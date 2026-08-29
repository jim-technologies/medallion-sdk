from __future__ import annotations

import base64
import ipaddress
import json
import math
import random
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from threading import Event
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, OpenerDirector, Request, build_opener

from google.protobuf import json_format, message

from .errors import (
    RETRYABLE_HTTP_STATUSES,
    MedallionAPIError,
    MedallionError,
    error_info_from_details,
)
from .tracing import (
    TracingConfig,
    inject_trace_context,
    normalize_tracing,
    record_span_exception,
    set_response_span,
    start_request_span,
)

DEFAULT_TIMEOUT_SECONDS = 30.0
MAX_CONNECT_TIMEOUT_MS = 9_999_999_999
MAX_RESPONSE_BYTES = 64 << 20
_WORKSPACE_ID_PATTERN = re.compile(r"^ws_[0-9a-hjkmnp-tv-z]{26}$", re.ASCII)
_CANONICAL_RPC_PATHS = frozenset(
    {
        "/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
        "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        "/medallion.connect.v1.MedallionConnectService/PublishAuditEvents",
        "/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
        "/medallion.ingest.v1.MedallionIngestService/Append",
        "/medallion.ingest.v1.MedallionIngestService/Query",
        "/medallion.ingest.v1.MedallionIngestService/GetQueryResults",
        "/medallion.ingest.v1.MedallionIngestService/CreateDataset",
        "/medallion.ingest.v1.MedallionIngestService/GetDataset",
        "/medallion.ingest.v1.MedallionIngestService/ListDatasets",
    }
)
_PROTECTED_HEADERS = frozenset(
    {
        "accept",
        "authorization",
        "connect-protocol-version",
        "connect-timeout-ms",
        "content-length",
        "content-type",
        "host",
        "idempotency-key",
        "x-medallion-api-key",
        "x-medallion-workspace-id",
    }
)


class _RejectRedirectHandler(HTTPRedirectHandler):
    # Run before any redirect handler already installed on a caller-supplied
    # opener. Raising the normal HTTPError keeps the 3xx response in the safe
    # error path without issuing a credential-bearing follow-up request.
    handler_order = 0

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class _ResponseTooLargeError(Exception):
    pass


@dataclass(frozen=True)
class RetryConfig:
    """Bounded retry settings for idempotent ingestion and readback calls."""

    max_attempts: int = 1
    initial_backoff: float = 0.2
    max_backoff: float = 2.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not 1 <= self.max_attempts <= 5:
            raise MedallionError(
                "retry.max_attempts must be between 1 and 5.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        if (
            not _nonnegative_finite(self.initial_backoff)
            or not _nonnegative_finite(self.max_backoff)
            or self.initial_backoff > self.max_backoff
            or self.max_backoff > 5
        ):
            raise MedallionError(
                "retry backoff values must be ordered, non-negative, and capped at 5 seconds.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        if (
            isinstance(self.jitter_ratio, bool)
            or not isinstance(self.jitter_ratio, int | float)
            or not math.isfinite(float(self.jitter_ratio))
            or not 0 <= self.jitter_ratio <= 1
        ):
            raise MedallionError(
                "retry.jitter_ratio must be between 0 and 1.",
                code="MEDALLION_INVALID_OPTIONS",
            )


@dataclass(frozen=True)
class ResponseEnvelope:
    body: Mapping[str, Any]
    request_id: str | None = None
    attempts: int = 1


class _RequestClient:
    def __init__(
        self,
        *,
        base_url: str,
        workspace_id: str,
        api_key: str | None = None,
        access_token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        retry: RetryConfig | None = None,
        tracing: bool | TracingConfig | None = None,
        opener: OpenerDirector | None = None,
    ) -> None:
        normalized_base_url = base_url.strip()
        if not normalized_base_url:
            raise MedallionError(
                "base_url is required.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        if any(
            ord(character) <= 0x20 or ord(character) == 0x7F
            for character in normalized_base_url
        ):
            raise MedallionError(
                "base_url must not contain embedded whitespace or control characters.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        try:
            parsed_base_url = urlsplit(normalized_base_url)
            _ = parsed_base_url.port
        except ValueError:
            raise MedallionError(
                "base_url must be an HTTPS origin (or loopback HTTP) without credentials, path, query, or fragment.",
                code="MEDALLION_INVALID_OPTIONS",
            ) from None
        if (
            not _base_url_scheme_allowed(parsed_base_url)
            or not parsed_base_url.netloc
            or parsed_base_url.hostname is None
            or parsed_base_url.username is not None
            or parsed_base_url.password is not None
            or parsed_base_url.path not in {"", "/"}
            or bool(parsed_base_url.query)
            or bool(parsed_base_url.fragment)
            or "?" in normalized_base_url
            or "#" in normalized_base_url
        ):
            raise MedallionError(
                "base_url must be an HTTPS origin (or loopback HTTP) without credentials, path, query, or fragment.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        hostname = parsed_base_url.hostname or ""
        authority = f"[{hostname}]" if ":" in hostname else hostname
        if parsed_base_url.port is not None:
            authority += f":{parsed_base_url.port}"
        normalized_base_url = f"{parsed_base_url.scheme.lower()}://{authority}"

        key = _optional_secret(api_key)
        token = _optional_secret(access_token)
        if bool(key) == bool(token):
            raise MedallionError(
                "Configure exactly one of api_key or access_token.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        self._timeout = _timeout_seconds(timeout)
        self._base_url = normalized_base_url
        self._api_key = key
        self._access_token = token
        self._workspace_id = _canonical_workspace_id(workspace_id)
        self._retry = retry or RetryConfig()
        self._tracing = normalize_tracing(tracing)
        if opener is None:
            self._opener = build_opener(_RejectRedirectHandler())
        else:
            # The opener is caller-owned, but adding an earlier response handler
            # is necessary to make its redirect policy safe for credentialed RPCs.
            opener.add_handler(_RejectRedirectHandler())
            self._opener = opener

    @property
    def workspace_id(self) -> str:
        return self._workspace_id

    def _post_proto(
        self,
        path: str,
        body: message.Message,
        response: message.Message,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
        retry_safe: bool = False,
        idempotency_key: str | None = None,
    ) -> ResponseEnvelope:
        # Serialize exactly once. Every retry sends the same bytes and ordering.
        payload = json_format.MessageToJson(
            body,
            preserving_proto_field_name=False,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        envelope = self._post(
            path,
            payload,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=retry_safe,
            idempotency_key=idempotency_key,
        )
        raw = envelope.body.get("__raw__")
        if not isinstance(raw, bytes) or not raw:
            raise MedallionError(
                "Medallion response body was empty.",
                code="MEDALLION_INVALID_JSON_RESPONSE",
                request_id=envelope.request_id,
            )
        try:
            json_format.Parse(
                raw.decode("utf-8"),
                response,
                ignore_unknown_fields=True,
            )
        except UnicodeDecodeError, json_format.ParseError:
            raise MedallionError(
                "Medallion response was not valid protobuf JSON.",
                code="MEDALLION_INVALID_JSON_RESPONSE",
                request_id=envelope.request_id,
            ) from None
        return ResponseEnvelope(
            body={},
            request_id=envelope.request_id,
            attempts=envelope.attempts,
        )

    def _post(
        self,
        path: str,
        payload: bytes,
        *,
        timeout: float | None,
        cancellation_event: Event | None,
        retry_safe: bool,
        idempotency_key: str | None = None,
    ) -> ResponseEnvelope:
        canonical_path = path if path.startswith("/") else "/" + path
        if canonical_path not in _CANONICAL_RPC_PATHS:
            raise MedallionError(
                "Only the canonical medallion.connect.v1 and medallion.ingest.v1 RPCs are supported.",
                code="MEDALLION_UNSUPPORTED_RPC",
            )
        request_path = canonical_path
        url = self._base_url + request_path
        sensitive_values = (
            self._api_key,
            self._access_token,
            *_payload_sensitive_values(payload),
        )
        total_timeout = self._timeout if timeout is None else _timeout_seconds(timeout)
        deadline = time.monotonic() + total_timeout
        attempts_allowed = self._retry.max_attempts if retry_safe else 1

        with start_request_span(self._tracing, "POST", request_path) as span:
            for attempt in range(1, attempts_allowed + 1):
                _raise_if_cancelled(cancellation_event)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    error = MedallionError(
                        f"Medallion request timed out after {total_timeout} seconds.",
                        code="MEDALLION_TIMEOUT",
                    )
                    record_span_exception(span, error)
                    raise error

                headers = self._headers(
                    remaining,
                )
                if idempotency_key is not None:
                    headers["Idempotency-Key"] = idempotency_key
                trace_headers: dict[str, str] = {}
                inject_trace_context(self._tracing, trace_headers)
                if any(name.lower() in _PROTECTED_HEADERS for name in trace_headers):
                    error = MedallionError(
                        "Tracing attempted to override a protected HTTP header.",
                        code="MEDALLION_PROTECTED_HEADER_OVERRIDE",
                    )
                    record_span_exception(span, error)
                    raise error
                headers.update(trace_headers)
                request = Request(url, data=payload, headers=headers, method="POST")
                try:
                    with self._opener.open(request, timeout=remaining) as http_response:
                        request_id = _request_id(
                            http_response.headers,
                            sensitive_values,
                        )
                        try:
                            raw = _read_bounded_response(http_response)
                        except _ResponseTooLargeError:
                            error = _response_too_large_error(request_id)
                            record_span_exception(span, error)
                            raise error from None
                        except Exception:
                            delay = _retry_delay(
                                self._retry,
                                attempt,
                                http_response.headers.get("retry-after"),
                            )
                            if (
                                retry_safe
                                and attempt < attempts_allowed
                                and _can_wait(delay, deadline)
                            ):
                                _wait(delay, cancellation_event, deadline)
                                continue
                            error = MedallionError(
                                "Medallion response body could not be read.",
                                code="MEDALLION_NETWORK_ERROR",
                                request_id=request_id,
                            )
                            set_response_span(span, http_response.status, request_id)
                            record_span_exception(span, error)
                            raise error from OSError(
                                "Medallion response body read failed."
                            )
                        set_response_span(span, http_response.status, request_id)
                        if span is not None:
                            span.set_attribute("medallion.retry_count", attempt - 1)
                        return ResponseEnvelope(
                            body={"__raw__": raw},
                            request_id=request_id,
                            attempts=attempt,
                        )
                except HTTPError as exc:
                    request_id = _request_id(
                        exc.headers,
                        sensitive_values,
                    )
                    try:
                        raw = _read_bounded_response(exc)
                    except _ResponseTooLargeError:
                        error = _response_too_large_error(request_id)
                        set_response_span(span, exc.code, request_id)
                        record_span_exception(span, error)
                        raise error from None
                    except Exception:
                        delay = _retry_delay(
                            self._retry,
                            attempt,
                            exc.headers.get("retry-after"),
                        )
                        if (
                            retry_safe
                            and attempt < attempts_allowed
                            and exc.code in RETRYABLE_HTTP_STATUSES
                            and _can_wait(delay, deadline)
                        ):
                            _wait(delay, cancellation_event, deadline)
                            continue
                        error = MedallionError(
                            "Medallion response body could not be read.",
                            code="MEDALLION_NETWORK_ERROR",
                            request_id=request_id,
                        )
                        set_response_span(span, exc.code, request_id)
                        record_span_exception(span, error)
                        raise error from OSError("Medallion response body read failed.")
                    finally:
                        exc.close()
                    error = _api_error(
                        exc.code,
                        request_id,
                        raw,
                        secrets=sensitive_values,
                    )
                    delay = _retry_delay(
                        self._retry,
                        attempt,
                        exc.headers.get("retry-after"),
                    )
                    if not (
                        retry_safe
                        and attempt < attempts_allowed
                        and _retryable_api_error(error)
                        and _can_wait(delay, deadline)
                    ):
                        set_response_span(span, exc.code, request_id)
                        record_span_exception(span, error)
                        raise error from OSError(
                            f"HTTP request failed with status {exc.code}."
                        )
                    _wait(delay, cancellation_event, deadline)
                except (TimeoutError, URLError) as exc:
                    delay = _retry_delay(self._retry, attempt, None)
                    if not (
                        retry_safe
                        and attempt < attempts_allowed
                        and _can_wait(delay, deadline)
                    ):
                        error = MedallionError(
                            (
                                f"Medallion request timed out after {total_timeout} seconds."
                                if isinstance(exc, TimeoutError)
                                else "Medallion request failed."
                            ),
                            code=(
                                "MEDALLION_TIMEOUT"
                                if isinstance(exc, TimeoutError)
                                else "MEDALLION_NETWORK_ERROR"
                            ),
                        )
                        record_span_exception(span, error)
                        safe_cause: BaseException = (
                            TimeoutError("Medallion request timed out.")
                            if isinstance(exc, TimeoutError)
                            else OSError("Medallion request transport failed.")
                        )
                        raise error from safe_cause
                    _wait(delay, cancellation_event, deadline)
        raise AssertionError("unreachable")

    def _headers(
        self,
        remaining: float,
    ) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Connect-Protocol-Version": "1",
            "Connect-Timeout-Ms": str(
                min(MAX_CONNECT_TIMEOUT_MS, max(1, math.ceil(remaining * 1000)))
            ),
            "Content-Type": "application/json",
            "X-Medallion-Workspace-Id": self._workspace_id,
        }
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        else:
            headers["X-Medallion-API-Key"] = self._api_key or ""
        return headers


def _api_error(
    status: int,
    request_id: str | None,
    raw: bytes,
    *,
    secrets: Sequence[str | None] = (),
) -> MedallionAPIError:
    body = _json_body(raw)
    connect_code = _safe_text(body.get("code"), maximum=128, secrets=secrets)
    details_value = body.get("details")
    raw_details = (
        tuple(dict(detail) for detail in details_value if isinstance(detail, Mapping))
        if isinstance(details_value, Sequence)
        and not isinstance(details_value, str | bytes)
        else ()
    )
    domain, reason, metadata = error_info_from_details(raw_details)
    domain = _redact(domain, secrets)
    reason = _redact(reason, secrets)
    metadata = {
        _redact(key, secrets) or "[REDACTED]": _redact(value, secrets) or ""
        for key, value in metadata.items()
    }
    details = tuple(_sanitize_detail(detail, secrets) for detail in raw_details)
    message = _api_error_message(status, request_id)
    sanitized_body = {
        key: value
        for key, value in {
            "code": connect_code,
            "details": list(details),
        }.items()
        if value
    }
    return MedallionAPIError(
        message,
        status=status,
        connect_code=connect_code,
        request_id=request_id,
        error_info_domain=domain,
        reason=reason,
        metadata=metadata,
        details=details,
        response_body=sanitized_body,
    )


def _json_body(raw: bytes) -> Mapping[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError, json.JSONDecodeError:
        return {}
    return value if isinstance(value, Mapping) else {}


def _api_error_message(status: int, request_id: str | None) -> str:
    suffix = f" Request ID: {request_id}." if request_id else ""
    return f"Medallion API request failed with HTTP {status}.{suffix}"


def _retryable_api_error(error: MedallionAPIError) -> bool:
    return error.is_retryable(idempotent=True)


def _read_bounded_response(response: Any) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length, 10)
        except ValueError:
            declared_length = None
        if declared_length is not None and declared_length > MAX_RESPONSE_BYTES:
            raise _ResponseTooLargeError

    raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise _ResponseTooLargeError
    return raw


def _response_too_large_error(request_id: str | None) -> MedallionError:
    return MedallionError(
        f"Medallion response exceeded the {MAX_RESPONSE_BYTES}-byte safety limit.",
        code="MEDALLION_RESPONSE_TOO_LARGE",
        request_id=request_id,
    )


def _retry_delay(
    config: RetryConfig,
    attempt: int,
    retry_after: str | None,
) -> float:
    parsed_retry_after = _parse_retry_after(retry_after)
    if parsed_retry_after is not None:
        # Retry-After is server guidance, not client backoff input. Honor it as
        # provided; the caller declines the retry when it cannot fit the total
        # request deadline.
        return parsed_retry_after
    base = min(config.max_backoff, config.initial_backoff * (2 ** (attempt - 1)))
    jitter = base * config.jitter_ratio
    return min(
        config.max_backoff,
        max(0.0, base + random.uniform(-jitter, jitter)),
    )


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value.strip())
        return seconds if math.isfinite(seconds) and seconds >= 0 else None
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return max(0.0, (parsed - datetime.now(UTC)).total_seconds())
        except TypeError, ValueError, OverflowError:
            return None


def _wait(delay: float, cancellation_event: Event | None, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0 or delay >= remaining:
        raise MedallionError(
            "Medallion request deadline expired before a safe retry.",
            code="MEDALLION_TIMEOUT",
        )
    if cancellation_event is not None:
        if cancellation_event.wait(delay):
            raise MedallionError(
                "Medallion request was cancelled.",
                code="MEDALLION_CANCELLED",
            )
    else:
        time.sleep(delay)


def _can_wait(delay: float, deadline: float) -> bool:
    return delay < deadline - time.monotonic()


def _raise_if_cancelled(cancellation_event: Event | None) -> None:
    if cancellation_event is not None and cancellation_event.is_set():
        raise MedallionError(
            "Medallion request was cancelled.",
            code="MEDALLION_CANCELLED",
        )


def _request_id(
    headers: Any,
    secrets: Sequence[str | None] = (),
) -> str | None:
    value = headers.get("x-request-id") or headers.get("x-medallion-request-id")
    return _safe_text(value, maximum=256, secrets=secrets)


def _optional_secret(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or not value.strip():
        raise MedallionError(
            "Configured credentials must be non-empty strings.",
            code="MEDALLION_INVALID_OPTIONS",
        )
    if value != value.strip() or any(
        ord(char) < 0x20 or ord(char) == 0x7F for char in value
    ):
        raise MedallionError(
            "Credentials must not contain surrounding whitespace or control characters.",
            code="MEDALLION_INVALID_OPTIONS",
        )
    return value


def _canonical_workspace_id(value: object) -> str:
    if value is None or value == "":
        raise MedallionError(
            "workspace_id is required when constructing MedallionClient.",
            code="MEDALLION_MISSING_WORKSPACE_ID",
        )
    if not isinstance(value, str) or _WORKSPACE_ID_PATTERN.fullmatch(value) is None:
        raise MedallionError(
            "workspace_id must match ^ws_[0-9a-hjkmnp-tv-z]{26}$.",
            code="MEDALLION_INVALID_WORKSPACE_ID",
        )
    return value


def _payload_sensitive_values(payload: bytes) -> tuple[str, ...]:
    """Collect payload values solely so a server echo can be redacted."""

    try:
        body = json.loads(payload.decode("utf-8"))
    except UnicodeDecodeError, json.JSONDecodeError:
        return ()
    if not isinstance(body, Mapping):
        return ()
    events = body.get("events")
    if not isinstance(events, list):
        return ()
    values: set[str] = set()
    for event in events:
        if not isinstance(event, Mapping):
            continue
        raw = event.get("payloadJson")
        if not isinstance(raw, str) or not raw:
            continue
        values.add(raw)
        try:
            decoded = json.loads(raw, parse_float=str, parse_int=str)
        except json.JSONDecodeError, RecursionError:
            continue
        try:
            _collect_sensitive_json_strings(decoded, values)
        except RecursionError:
            # The complete serialized payload remains in ``values`` and is
            # sufficient to redact a wholesale server echo.
            continue
    return tuple(sorted(values, key=len, reverse=True))


def _collect_sensitive_json_strings(value: Any, output: set[str]) -> None:
    if isinstance(value, str):
        if len(value) >= 4:
            output.add(value)
        return
    if value is None or isinstance(value, bool | int | float):
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"))
        if len(encoded) >= 4:
            output.add(encoded)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str) and len(key) >= 4:
                output.add(key)
            _collect_sensitive_json_strings(item, output)
        return
    if isinstance(value, list):
        for item in value:
            _collect_sensitive_json_strings(item, output)


def _base_url_scheme_allowed(parsed_base_url: Any) -> bool:
    if parsed_base_url.scheme == "https":
        return True
    if parsed_base_url.scheme != "http":
        return False
    hostname = parsed_base_url.hostname
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv4Address):
        return address.packed[0] == 127
    return address == ipaddress.IPv6Address("::1")


def _timeout_seconds(value: float) -> float:
    if not _positive_finite(value):
        raise MedallionError(
            "timeout must be a positive finite number of seconds.",
            code="MEDALLION_INVALID_OPTIONS",
        )
    return float(value)


def _positive_finite(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and math.isfinite(float(value))
        and value > 0
    )


def _nonnegative_finite(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and math.isfinite(float(value))
        and value >= 0
    )


def _safe_text(
    value: Any,
    *,
    maximum: int,
    secrets: Sequence[str | None] = (),
) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.replace("\r", " ").replace("\n", " ").strip()
    normalized = _redact(normalized, secrets) or ""
    return normalized[:maximum] or None


def _redact(value: str | None, secrets: Sequence[str | None]) -> str | None:
    if value is None:
        return None
    for secret in secrets:
        if secret:
            value = value.replace(secret, "[REDACTED]")
    return value


def _sanitize_detail(
    detail: Mapping[str, Any], secrets: Sequence[str | None]
) -> dict[str, Any]:
    return {
        _redact(str(key), secrets) or "[REDACTED]": _sanitize_detail_value(
            value,
            secrets,
        )
        for key, value in detail.items()
    }


def _sanitize_detail_value(value: Any, secrets: Sequence[str | None]) -> Any:
    if isinstance(value, str):
        redacted = _redact(value, secrets) or ""
        try:
            padded = value + "=" * ((4 - len(value) % 4) % 4)
            decoded = base64.b64decode(padded, validate=True)
        except ValueError, TypeError:
            return redacted
        if any(secret and secret.encode() in decoded for secret in secrets):
            return "[REDACTED]"
        return redacted
    if isinstance(value, Mapping):
        return _sanitize_detail(value, secrets)
    if isinstance(value, list):
        return [_sanitize_detail_value(item, secrets) for item in value]
    if value is None or isinstance(value, bool | int | float):
        return value
    return "[REDACTED]"
