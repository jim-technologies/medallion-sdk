from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

from .error_policy_generated import (
    KNOWN_ERROR_DOMAIN,
    REASON_POLICIES,
    KnownErrorReason,
)

_AUTOMATIC_RETRY_CLASSIFICATIONS = frozenset(
    {
        "bounded_transient_retry",
        "bounded_transient_retry_idempotent_operations_only",
    }
)
RETRYABLE_CONNECT_CODES = frozenset(
    {"resource_exhausted", "unavailable", "deadline_exceeded"}
)
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 502, 503, 504})


class MedallionError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id


class MedallionAPIError(MedallionError):
    """A sanitized Connect error response.

    ``reason`` intentionally remains a string so callers retain unknown future
    ErrorInfo reasons. ``details`` likewise preserves additive Connect details.
    The raw HTTP response and credential-bearing request are never retained.
    """

    def __init__(
        self,
        message: str,
        *,
        status: int,
        connect_code: str | None = None,
        request_id: str | None = None,
        error_info_domain: str | None = None,
        reason: str | None = None,
        metadata: Mapping[str, str] | None = None,
        details: Sequence[Mapping[str, Any]] = (),
        response_body: Any = None,
    ) -> None:
        super().__init__(
            message,
            code="MEDALLION_API_ERROR",
            request_id=request_id,
        )
        self.status = status
        self.http_status = status
        self.connect_code = connect_code
        self.grpc_code = connect_code.upper() if connect_code else None
        self.error_info_domain = error_info_domain
        self.domain = error_info_domain
        self.reason = reason
        self.metadata = dict(metadata or {})
        self.details = tuple(dict(detail) for detail in details)
        # Kept for source compatibility, but intentionally sanitized by the
        # transport rather than containing an arbitrary raw response body.
        self.response_body = response_body

    @property
    def known_reason(self) -> KnownErrorReason | None:
        try:
            return KnownErrorReason(self.reason) if self.reason else None
        except ValueError:
            return None

    def is_retryable(self, *, idempotent: bool) -> bool:
        """Return retry guidance only when the caller's operation is safe."""

        if not idempotent:
            return False
        if self.reason:
            known = self.known_reason
            if self.error_info_domain != KNOWN_ERROR_DOMAIN or known is None:
                return False
            policy = REASON_POLICIES[known]
            return (
                self.connect_code is not None
                and self.connect_code.lower() == policy.grpc_code.lower()
                and policy.retry_classification in _AUTOMATIC_RETRY_CLASSIFICATIONS
                and policy.reuse_idempotency_key
            )
        # A decoded Connect code is more specific than the HTTP transport
        # status. Do not turn deterministic application failures into retries
        # merely because a gateway supplied a transient-looking HTTP status.
        if self.connect_code is not None:
            return self.connect_code.lower() in RETRYABLE_CONNECT_CODES
        return self.status in RETRYABLE_HTTP_STATUSES

    @property
    def cause(self) -> BaseException | None:
        return self.__cause__


def error_info_from_details(
    details: Sequence[Mapping[str, Any]],
) -> tuple[str | None, str | None, dict[str, str]]:
    """Decode the first google.rpc.ErrorInfo Connect detail without extra deps."""

    for detail in details:
        detail_type = detail.get("type")
        if detail_type not in {
            "google.rpc.ErrorInfo",
            "type.googleapis.com/google.rpc.ErrorInfo",
        }:
            continue
        encoded = detail.get("value")
        if not isinstance(encoded, str):
            continue
        try:
            padded = encoded + "=" * ((4 - len(encoded) % 4) % 4)
            raw = base64.b64decode(padded, validate=True)
            return _decode_error_info(raw)
        except ValueError, TypeError:
            continue
    return None, None, {}


def _decode_error_info(raw: bytes) -> tuple[str | None, str | None, dict[str, str]]:
    reason: str | None = None
    domain: str | None = None
    metadata: dict[str, str] = {}
    for field_number, wire_type, value in _protobuf_fields(raw):
        if wire_type != 2 or not isinstance(value, bytes):
            continue
        if field_number == 1:
            reason = _utf8(value)
        elif field_number == 2:
            domain = _utf8(value)
        elif field_number == 3:
            key: str | None = None
            item: str | None = None
            for entry_number, entry_wire_type, entry_value in _protobuf_fields(value):
                if entry_wire_type != 2 or not isinstance(entry_value, bytes):
                    continue
                if entry_number == 1:
                    key = _utf8(entry_value)
                elif entry_number == 2:
                    item = _utf8(entry_value)
            if key is not None and item is not None:
                metadata[key] = item
    return domain, reason, metadata


def _protobuf_fields(raw: bytes):
    offset = 0
    while offset < len(raw):
        tag, offset = _varint(raw, offset)
        field_number = tag >> 3
        wire_type = tag & 7
        if field_number == 0:
            raise ValueError("invalid protobuf field")
        if wire_type == 0:
            value, offset = _varint(raw, offset)
        elif wire_type == 1:
            end = offset + 8
            if end > len(raw):
                raise ValueError("truncated protobuf field")
            value, offset = raw[offset:end], end
        elif wire_type == 2:
            length, offset = _varint(raw, offset)
            end = offset + length
            if end > len(raw):
                raise ValueError("truncated protobuf field")
            value, offset = raw[offset:end], end
        elif wire_type == 5:
            end = offset + 4
            if end > len(raw):
                raise ValueError("truncated protobuf field")
            value, offset = raw[offset:end], end
        else:
            raise ValueError("unsupported protobuf wire type")
        yield field_number, wire_type, value


def _varint(raw: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(raw) and shift < 70:
        byte = raw[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("invalid protobuf varint")


def _utf8(raw: bytes) -> str:
    return raw.decode("utf-8", errors="strict")
