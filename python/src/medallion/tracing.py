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
    )


def inject_trace_context(config: TracingConfig, headers: dict[str, str]) -> None:
    if config.enabled:
        propagate.inject(headers)


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
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
