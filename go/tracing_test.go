package medallion

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type hostileHeaderPropagator struct{}

func (hostileHeaderPropagator) Inject(_ context.Context, carrier propagation.TextMapCarrier) {
	for name, value := range map[string]string{
		"Accept":                    "text/plain",
		"Authorization":             "Bearer attacker",
		"Connect-Protocol-Version":  "999",
		"Connect-Timeout-Ms":        "9999999999",
		"Content-Encoding":          "gzip",
		"Content-Length":            "1",
		"Content-Type":              "text/plain",
		"Transfer-Encoding":         "chunked",
		"Traceparent":               "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"X-Forbidden-Scope":         "scope_attacker",
		"X-Medallion-API-Key":       "attacker",
		"X-Medallion-Workspace-Id":  "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
		"X-Medallion-Trace-Fixture": "discarded",
	} {
		carrier.Set(name, value)
	}
}

func (hostileHeaderPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (hostileHeaderPropagator) Fields() []string { return nil }

func TestTracingCannotOverrideProtectedRequestHeaders(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(hostileHeaderPropagator{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	client, err := newRequestClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "service-key",
		WorkspaceID: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
		Tracing:     TracingConfig{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "https://api.example.com"+listCdcEventsPath, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	client.setRequestHeaders(request, ctx, client.workspaceID)

	for name, want := range map[string]string{
		"Accept":                    "application/json",
		"Authorization":             "",
		"Connect-Protocol-Version":  "1",
		"Content-Encoding":          "",
		"Content-Length":            "",
		"Content-Type":              "application/json",
		"Traceparent":               "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"Transfer-Encoding":         "",
		"X-Forbidden-Scope":         "",
		"X-Medallion-API-Key":       "service-key",
		"X-Medallion-Workspace-Id":  "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
		"X-Medallion-Trace-Fixture": "",
	} {
		if got := request.Header.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if got := request.Header.Get("Connect-Timeout-Ms"); got == "" || got == "9999999999" {
		t.Fatalf("Connect-Timeout-Ms = %q, want SDK-derived value", got)
	}
}

func TestTracingNeverRecordsArbitraryErrorMessages(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	_, span := (TracingConfig{
		Enabled: true,
		Tracer:  provider.Tracer("test"),
	}).startRequestSpan(context.Background(), "POST", "/v1/query")
	secret := "customer_payload_that_must_not_escape"
	callerError := &APIError{
		Status:    400,
		Code:      "invalid_argument",
		RequestID: "req_trace",
		Message:   "Invalid payload " + secret,
	}
	setRequestSpanResponse(span, callerError.Status, callerError.RequestID)
	recordRequestSpanError(span, callerError)
	span.End()

	if !strings.Contains(callerError.Error(), secret) {
		t.Fatalf("caller-facing error was unexpectedly changed: %q", callerError.Error())
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(spans))
	}
	got := spans[0]
	if got.Status.Code != codes.Error || got.Status.Description != "" {
		t.Fatalf("span status = %#v, want error without description", got.Status)
	}

	attributes := map[string]string{}
	for _, item := range got.Attributes {
		attributes[string(item.Key)] = item.Value.AsString()
	}
	if attributes["medallion.request_id"] != "req_trace" {
		t.Fatalf("request ID attribute = %q", attributes["medallion.request_id"])
	}
	if len(got.Events) != 1 || got.Events[0].Name != "exception" {
		t.Fatalf("span events = %#v, want one exception", got.Events)
	}
	eventAttributes := map[string]string{}
	for _, item := range got.Events[0].Attributes {
		eventAttributes[string(item.Key)] = item.Value.AsString()
	}
	if eventAttributes["exception.message"] != requestSpanFailureMessage {
		t.Fatalf("exception message = %q", eventAttributes["exception.message"])
	}
	if got := eventAttributes["exception.type"]; got != "github.com/jim-technologies/medallion-sdk/go.requestSpanFailure" {
		t.Fatalf("exception type = %q, want stable request failure type", got)
	}
	for key, value := range attributes {
		if strings.Contains(value, secret) {
			t.Fatalf("span attribute %q leaked arbitrary text: %q", key, value)
		}
	}
	for key, value := range eventAttributes {
		if strings.Contains(value, secret) {
			t.Fatalf("event attribute %q leaked arbitrary text: %q", key, value)
		}
	}
}
