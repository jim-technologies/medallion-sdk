package medallion

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTracerName = "github.com/jim-technologies/medallion-sdk/go"
	defaultSpanPrefix = "medallion"
)

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
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
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
		span.SetStatus(codes.Error, http.StatusText(status))
	}
}

func recordRequestSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func setRequestSpanOK(span trace.Span) {
	if span != nil {
		span.SetStatus(codes.Ok, "")
	}
}
