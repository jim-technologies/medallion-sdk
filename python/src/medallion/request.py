from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from google.protobuf import json_format, message

from .errors import MedallionAPIError, MedallionError
from .tracing import (
    TracingConfig,
    inject_trace_context,
    normalize_tracing,
    record_span_exception,
    set_response_span,
    start_request_span,
)

DEFAULT_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class ResponseEnvelope:
    body: Mapping[str, Any]
    request_id: str | None = None


class RequestClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str | None = None,
        access_token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        tracing: bool | TracingConfig | None = None,
    ) -> None:
        normalized_base_url = base_url.strip().rstrip("/")
        if not normalized_base_url:
            raise MedallionError(
                "base_url is required.",
                code="MEDALLION_INVALID_OPTIONS",
            )
        parsed_base_url = urlsplit(normalized_base_url)
        if (
            parsed_base_url.scheme not in {"http", "https"}
            or not parsed_base_url.netloc
            or parsed_base_url.username is not None
            or parsed_base_url.password is not None
            or bool(parsed_base_url.query)
            or bool(parsed_base_url.fragment)
        ):
            raise MedallionError(
                "base_url must be an absolute HTTP(S) URL without credentials, query, or fragment.",
                code="MEDALLION_INVALID_OPTIONS",
            )

        token = next(
            (
                value.strip()
                for value in (access_token, api_key)
                if value is not None and value.strip()
            ),
            "",
        )
        if not token:
            raise MedallionError(
                "api_key or access_token is required.",
                code="MEDALLION_INVALID_OPTIONS",
            )

        self._base_url = normalized_base_url
        self._bearer_token = token
        self._timeout = timeout
        self._tracing = normalize_tracing(tracing)

    def post_json(
        self,
        path: str,
        body: Mapping[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> ResponseEnvelope:
        url = self._base_url + (path if path.startswith("/") else "/" + path)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._bearer_token}",
            "Connect-Protocol-Version": "1",
            "Content-Type": "application/json",
        }
        with start_request_span(self._tracing, "POST", path) as span:
            if idempotency_key:
                headers["Idempotency-Key"] = idempotency_key
            inject_trace_context(self._tracing, headers)

            try:
                payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
            except (TypeError, ValueError) as exc:
                record_span_exception(span, exc)
                raise MedallionError(
                    "Medallion request body must be JSON serializable.",
                    code="MEDALLION_INVALID_JSON_BODY",
                ) from exc

            request = Request(url, data=payload, headers=headers, method="POST")
            try:
                with urlopen(request, timeout=self._timeout) as response:
                    raw = response.read()
                    request_id = response.headers.get("x-request-id")
                    set_response_span(span, response.status, request_id)
                    return ResponseEnvelope(
                        body=_json_body(raw),
                        request_id=request_id,
                    )
            except HTTPError as exc:
                raw = exc.read()
                request_id = exc.headers.get("x-request-id")
                response_body = _json_body(raw)
                set_response_span(span, exc.code, request_id)
                record_span_exception(span, exc)
                raise MedallionAPIError(
                    _api_error_message(exc.code, request_id, response_body),
                    status=exc.code,
                    request_id=request_id,
                    response_body=response_body,
                ) from exc
            except TimeoutError as exc:
                record_span_exception(span, exc)
                raise MedallionError(
                    f"Medallion request timed out after {self._timeout} seconds.",
                    code="MEDALLION_TIMEOUT",
                ) from exc
            except URLError as exc:
                record_span_exception(span, exc)
                raise MedallionError(
                    "Medallion request failed.",
                    code="MEDALLION_NETWORK_ERROR",
                ) from exc

    def post_proto(
        self,
        path: str,
        body: message.Message,
        response: message.Message,
        *,
        idempotency_key: str | None = None,
    ) -> ResponseEnvelope:
        url = self._base_url + (path if path.startswith("/") else "/" + path)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._bearer_token}",
            "Connect-Protocol-Version": "1",
            "Content-Type": "application/json",
        }
        with start_request_span(self._tracing, "POST", path) as span:
            if idempotency_key:
                headers["Idempotency-Key"] = idempotency_key
            inject_trace_context(self._tracing, headers)

            payload = json_format.MessageToJson(
                body,
                preserving_proto_field_name=False,
            ).encode("utf-8")

            request = Request(url, data=payload, headers=headers, method="POST")
            try:
                with urlopen(request, timeout=self._timeout) as http_response:
                    raw = http_response.read()
                    request_id = http_response.headers.get("x-request-id")
                    set_response_span(span, http_response.status, request_id)
                    if raw:
                        try:
                            json_format.Parse(raw.decode("utf-8"), response)
                        except (UnicodeDecodeError, json_format.ParseError) as exc:
                            record_span_exception(span, exc)
                            raise MedallionError(
                                "Medallion response was not valid protobuf JSON.",
                                code="MEDALLION_INVALID_JSON_RESPONSE",
                                request_id=request_id,
                            ) from exc
                    return ResponseEnvelope(
                        body={},
                        request_id=request_id,
                    )
            except HTTPError as exc:
                raw = exc.read()
                request_id = exc.headers.get("x-request-id")
                response_body = _json_body(raw)
                set_response_span(span, exc.code, request_id)
                record_span_exception(span, exc)
                raise MedallionAPIError(
                    _api_error_message(exc.code, request_id, response_body),
                    status=exc.code,
                    request_id=request_id,
                    response_body=response_body,
                ) from exc
            except TimeoutError as exc:
                record_span_exception(span, exc)
                raise MedallionError(
                    f"Medallion request timed out after {self._timeout} seconds.",
                    code="MEDALLION_TIMEOUT",
                ) from exc
            except URLError as exc:
                record_span_exception(span, exc)
                raise MedallionError(
                    "Medallion request failed.",
                    code="MEDALLION_NETWORK_ERROR",
                ) from exc


def _json_body(raw: bytes) -> Mapping[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError, json.JSONDecodeError:
        return {"body": raw.decode("utf-8", errors="replace")}
    return value if isinstance(value, Mapping) else {"body": value}


def _api_error_message(
    status: int,
    request_id: str | None,
    response_body: Mapping[str, Any],
) -> str:
    message = response_body.get("message")
    if isinstance(message, str) and message:
        return message
    suffix = f" Request ID: {request_id}." if request_id else ""
    return f"Medallion API request failed with HTTP {status}.{suffix}"
