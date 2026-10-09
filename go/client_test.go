package medallion

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const testWorkspaceID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"

func TestClientHasOnlyIngestBusinessSurface(t *testing.T) {
	shape := reflect.TypeFor[Client]()
	if shape.NumField() != 1 || shape.Field(0).Name != "Ingest" {
		t.Fatalf("unexpected Client shape %v", shape)
	}
	shape = reflect.TypeFor[*IngestClient]()
	if shape.NumMethod() != 7 {
		t.Fatalf("unexpected Ingest method count %d", shape.NumMethod())
	}
}

func TestTracingCreatesClientSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_trace")
		_, _ = w.Write([]byte(`{"tables":[]}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "test-api-key",
		WorkspaceID: testWorkspaceID,
		Tracing: TracingConfig{
			Enabled:    true,
			Tracer:     provider.Tracer("test"),
			SpanPrefix: "test-medallion",
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, _, err = client.Ingest.ListTables(context.Background(), &ingestv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("record audit: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name != "test-medallion POST "+listTablesPath {
		t.Fatalf("span name = %q", span.Name)
	}
	attrs := map[string]string{}
	for _, attr := range span.Attributes {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	if attrs["medallion.sdk.language"] != "go" || attrs["medallion.request.path"] != listTablesPath {
		t.Fatalf("unexpected span attributes: %#v", attrs)
	}
	if attrs["medallion.request_id"] != "req_trace" {
		t.Fatalf("request id attribute = %q", attrs["medallion.request_id"])
	}
}

func TestTransportErrorsPreserveClassificationAndCause(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_invalid")
		_, _ = w.Write([]byte(`{`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		AccessToken: "   ",
		APIKey:      "fallback-key",
		WorkspaceID: testWorkspaceID,
	})
	if err != nil {
		t.Fatalf("new client with fallback key: %v", err)
	}
	_, _, err = client.Ingest.ListTables(context.Background(), &ingestv1.ListTablesRequest{})
	var medallionErr *Error
	if !errors.As(err, &medallionErr) ||
		medallionErr.Code != "MEDALLION_INVALID_JSON_RESPONSE" ||
		medallionErr.RequestID != "req_invalid" ||
		!errors.Is(err, errInvalidJSONResponse) {
		t.Fatalf("invalid response error = %#v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = client.Ingest.ListTables(cancelled, &ingestv1.ListTablesRequest{})
	if !errors.As(err, &medallionErr) ||
		medallionErr.Code != "MEDALLION_ABORTED" ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %#v", err)
	}

	_, err = NewClient(ClientConfig{
		BaseURL:     "https://user:secret@example.com?unsafe=true",
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
	})
	if !errors.As(err, &medallionErr) || medallionErr.Code != "MEDALLION_INVALID_OPTIONS" {
		t.Fatalf("invalid base URL error = %#v", err)
	}
}
