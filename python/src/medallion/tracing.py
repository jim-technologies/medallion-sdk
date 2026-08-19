from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

DEFAULT_TRACER_NAME = "medallion"
DEFAULT_SPAN_PREFIX = "medallion"


@dataclass(frozen=True)
class TracingConfig:
    enabled: bool = False
    tracer: Any | None = None
    tracer_name: str = DEFAULT_TRACER_NAME
    span_prefix: str = DEFAULT_SPAN_PREFIX


def normalize_tracing(config: bool | TracingConfig | None) -> TracingConfig:
    if config is None or config is False:
        return TracingConfig(enabled=False)
    if config is True:
        return TracingConfig(enabled=True, tracer=trace.get_tracer(DEFAULT_TRACER_NAME))
    return TracingConfig(
        enabled=config.enabled or config.tracer is not None,
        tracer=config.tracer or trace.get_tracer(config.tracer_name),
        tracer_name=config.tracer_name,
        span_prefix=config.span_prefix or DEFAULT_SPAN_PREFIX,
    )


def start_request_span(config: TracingConfig, method: str, path: str):
    if not config.enabled:
        return nullcontext(None)
    tracer = config.tracer or trace.get_tracer(config.tracer_name)
    return tracer.start_as_current_span(
        f"{config.span_prefix} {method} {path}",
        kind=SpanKind.CLIENT,
        attributes={
            "medallion.sdk.language": "python",
            "medallion.request.path": path,
            "http.request.method": method,
        },
        # Request failures are recorded below with stable metadata. Letting the
        # context manager record an escaping exception would copy arbitrary
        # server messages into exception.message and the span status.
        record_exception=False,
        set_status_on_exception=False,
    )


def inject_trace_context(config: TracingConfig, headers: dict[str, str]) -> None:
    if not config.enabled:
        return
    injected: dict[str, str] = {}
    propagate.inject(injected)
    for name, value in injected.items():
        if (
            isinstance(name, str)
            and isinstance(value, str)
            and _allowed_trace_header(name)
        ):
            headers[name] = value


def _allowed_trace_header(name: str) -> bool:
    normalized = name.lower()
    return normalized in {
        "b3",
        "baggage",
        "grpc-trace-bin",
        "traceparent",
        "tracestate",
        "uber-trace-id",
        "x-amzn-trace-id",
        "x-cloud-trace-context",
    } or normalized.startswith(("uberctx-", "x-b3-"))


def set_response_span(span: Any, status: int, request_id: str | None) -> None:
    if span is None:
        return
    span.set_attribute("http.response.status_code", status)
    if request_id:
        span.set_attribute("medallion.request_id", request_id)
    if 200 <= status < 300:
        span.set_status(Status(StatusCode.OK))
    else:
        span.set_status(Status(StatusCode.ERROR))


def record_span_exception(span: Any, exc: BaseException) -> None:
    if span is None:
        return
    code, error_type, request_id = _telemetry_error_fields(exc)
    span.set_attribute("medallion.error.code", code)
    span.set_attribute("medallion.error.type", error_type)
    if request_id:
        span.set_attribute("medallion.request_id", request_id)
    span.record_exception(_TelemetryError(code))
    span.set_status(Status(StatusCode.ERROR, code))


class _TelemetryError(Exception):
    """Stable exception projection that cannot contain response text."""


def _telemetry_error_fields(
    exc: BaseException,
) -> tuple[str, str, str | None]:
    raw_code = getattr(exc, "code", None)
    code = (
        raw_code
        if isinstance(raw_code, str)
        and raw_code.startswith("MEDALLION_")
        and all(char in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for char in raw_code)
        else "MEDALLION_TRANSPORT_ERROR"
    )
    error_type = (
        "api"
        if code == "MEDALLION_API_ERROR"
        else "sdk"
        if code != "MEDALLION_TRANSPORT_ERROR"
        else "transport"
    )
    raw_request_id = getattr(exc, "request_id", None)
    request_id = raw_request_id if isinstance(raw_request_id, str) else None
    return code, error_type, request_id
