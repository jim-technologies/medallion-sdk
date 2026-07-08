package medallion

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestAuditRecordUsesConnectProtoRouteAndHeaders(t *testing.T) {
	var seen struct {
		path    string
		headers http.Header
		body    map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.path = r.URL.Path
		seen.headers = r.Header.Clone()
		if err := json.NewDecoder(r.Body).Decode(&seen.body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_123")
		_, _ = w.Write([]byte(`{"accepted_count":1,"events":[{"idempotency_key":"audit_1","event_id":"42"}]}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "test-api-key",
		DefaultConnectorID: "conn_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: 123},
		Action:         "order.cancelled",
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		After:          map[string]any{"status": "cancelled"},
		IdempotencyKey: "audit_1",
	})
	if err != nil {
		t.Fatalf("record audit: %v", err)
	}

	if seen.path != publishCdcEventsPath {
		t.Fatalf("path = %q, want %q", seen.path, publishCdcEventsPath)
	}
	if got := seen.headers.Get("Authorization"); got != "Bearer test-api-key" {
		t.Fatalf("authorization = %q", got)
	}
	if got := seen.headers.Get("Idempotency-Key"); got != "audit_1" {
		t.Fatalf("idempotency key = %q", got)
	}
	events := seen.body["events"].([]any)
	event := events[0].(map[string]any)
	if got := event["actor_principal"]; got != "user:123" {
		t.Fatalf("actor_principal = %#v", got)
	}
	if got := event["kind"]; got != "EVENT_KIND_AUDIT" {
		t.Fatalf("kind = %#v", got)
	}
	if result.RequestID != "req_123" || result.Events[0].EventID != "42" || result.Proto == nil {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestAuditTrailFiltersSourceActorAndIngester(t *testing.T) {
	var seen map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != listCdcEventsPath {
			t.Fatalf("path = %q, want %q", r.URL.Path, listCdcEventsPath)
		}
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_123")
		_, _ = w.Write([]byte(`{
			"events":[{
				"id":"42",
				"organization_id":"org_123",
				"connector_id":"conn_123",
				"stream_name":"audit_log",
				"entity_type":"order",
				"entity_id":"order_123",
				"idempotency_key":"audit_1",
				"actor_principal":"user:user_123",
				"ingested_by_principal":"service_account:worker",
				"payload_json":"{\"actor\":{\"type\":\"user\",\"id\":\"user_123\"},\"after\":{\"status\":\"cancelled\"}}",
				"kind":"EVENT_KIND_AUDIT",
				"action":"order.cancelled"
			}],
			"next_page_cursor":"cursor_2"
		}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "test-api-key",
		OrganizationID:     "org_123",
		DefaultConnectorID: "conn_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Audit.Trail(context.Background(), AuditTrailQuery{
		ResourceType:      "order",
		ResourceID:        "order_123",
		Actor:             &ActorRef{Type: "user", ID: "user_123"},
		IngesterPrincipal: "service_account:worker",
	})
	if err != nil {
		t.Fatalf("trail: %v", err)
	}

	if got := seen["actor_principal"]; got != "user:user_123" {
		t.Fatalf("actor_principal = %#v", got)
	}
	if got := seen["ingested_by_principal"]; got != "service_account:worker" {
		t.Fatalf("ingested_by_principal = %#v", got)
	}
	if result.NextCursor != "cursor_2" || len(result.Events) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.Events[0].IngesterPrincipal != "service_account:worker" || result.Events[0].ActorPrincipal != "user:user_123" {
		t.Fatalf("unexpected event: %#v", result.Events[0])
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
		_, _ = w.Write([]byte(`{"accepted_count":1,"events":[{"idempotency_key":"audit_1","event_id":"42"}]}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "test-api-key",
		DefaultConnectorID: "conn_123",
		Tracing: TracingConfig{
			Enabled:    true,
			Tracer:     provider.Tracer("test"),
			SpanPrefix: "test-medallion",
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: "user_123"},
		Action:         "order.cancelled",
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: "audit_1",
	})
	if err != nil {
		t.Fatalf("record audit: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name != "test-medallion POST "+publishCdcEventsPath {
		t.Fatalf("span name = %q", span.Name)
	}
	attrs := map[string]string{}
	for _, attr := range span.Attributes {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	if attrs["medallion.sdk.language"] != "go" || attrs["medallion.request.path"] != publishCdcEventsPath {
		t.Fatalf("unexpected span attributes: %#v", attrs)
	}
	if attrs["medallion.request_id"] != "req_trace" {
		t.Fatalf("request id attribute = %q", attrs["medallion.request_id"])
	}
}
