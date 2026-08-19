package medallion

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTracerName         = "github.com/jim-technologies/medallion-sdk/go"
	defaultSpanPrefix         = "medallion"
	requestSpanFailureMessage = "Medallion request failed."
)

// requestSpanFailure is deliberately detached from the caller-facing error.
// Error messages can contain server responses, credentials, or event payloads,
// none of which belong in telemetry.
type requestSpanFailure struct{}

func (requestSpanFailure) Error() string {
	return requestSpanFailureMessage
}

func (c TracingConfig) enabled() bool {
	return c.Enabled || c.Tracer != nil
}

func (c TracingConfig) tracer() trace.Tracer {
	if c.Tracer != nil {
		return c.Tracer
	}
	name := c.TracerName
	if name == "" {
		name = defaultTracerName
	}
	return otel.Tracer(name)
}

func (c TracingConfig) spanPrefix() string {
	if c.SpanPrefix != "" {
		return c.SpanPrefix
	}
	return defaultSpanPrefix
}

func (c TracingConfig) startRequestSpan(ctx context.Context, method, path string) (context.Context, trace.Span) {
	if !c.enabled() {
		return ctx, nil
	}
	return c.tracer().Start(
		ctx,
		c.spanPrefix()+" "+method+" "+path,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("medallion.sdk.language", "go"),
			attribute.String("medallion.request.path", path),
			attribute.String("http.request.method", method),
		),
	)
}

func (c TracingConfig) inject(ctx context.Context, header http.Header) {
	if !c.enabled() {
		return
	}
	// Propagate only standard tracing headers from the process-wide propagator.
	// This request starts with no caller headers, so an allowlist keeps custom
	// propagators from smuggling authentication, scope, or transport metadata
	// into an SDK-owned request.
	injected := make(http.Header)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(injected))
	for name, values := range injected {
		if !allowedTracingHeader(name) {
			continue
		}
		header[name] = append([]string(nil), values...)
	}
}

func allowedTracingHeader(name string) bool {
	name = strings.ToLower(name)
	switch name {
	case "b3", "baggage", "grpc-trace-bin", "traceparent", "tracestate", "uber-trace-id", "x-amzn-trace-id", "x-cloud-trace-context":
		return true
	default:
		return strings.HasPrefix(name, "uberctx-") || strings.HasPrefix(name, "x-b3-")
	}
}

func setRequestSpanResponse(span trace.Span, status int, requestID string) {
	if span == nil {
		return
	}
	span.SetAttributes(attribute.Int("http.response.status_code", status))
	if requestID != "" {
		span.SetAttributes(attribute.String("medallion.request_id", requestID))
	}
	if status < 200 || status >= 300 {
		span.SetStatus(codes.Error, "")
	}
}

func recordRequestSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(requestSpanFailure{})
	span.SetStatus(codes.Error, "")
}

func setRequestSpanOK(span trace.Span) {
	if span != nil {
		span.SetStatus(codes.Ok, "")
	}
}
