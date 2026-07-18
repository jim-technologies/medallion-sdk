package medallion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/reflect/protoreflect"
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
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		After:          map[string]any{"status": "cancelled"},
		EvidenceURL:    "https://evidence.example/orders/order_123",
		IdempotencyKey: "audit_1",
	})
	if err != nil {
		t.Fatalf("record audit: %v", err)
	}

	if seen.path != publishAuditEventsPath {
		t.Fatalf("path = %q, want %q", seen.path, publishAuditEventsPath)
	}
	if got := seen.headers.Get("Authorization"); got != "Bearer test-api-key" {
		t.Fatalf("authorization = %q", got)
	}
	if got := seen.headers.Get("Idempotency-Key"); got != "audit_1" {
		t.Fatalf("idempotency key = %q", got)
	}
	events := seen.body["events"].([]any)
	event := events[0].(map[string]any)
	if got := event["actorPrincipal"]; got != "user:123" {
		t.Fatalf("actorPrincipal = %#v", got)
	}
	if got := event["resourceType"]; got != "order" {
		t.Fatalf("resourceType = %#v", got)
	}
	if got := event["resourceId"]; got != "order_123" {
		t.Fatalf("resourceId = %#v", got)
	}
	if got := event["outcome"]; got != "AUDIT_EVENT_OUTCOME_SUCCEEDED" {
		t.Fatalf("outcome = %#v", got)
	}
	if _, ok := event["kind"]; ok {
		t.Fatalf("dedicated audit event must not include kind: %#v", event)
	}
	if _, ok := event["operation"]; ok {
		t.Fatalf("dedicated audit event must not include operation: %#v", event)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event["payloadJson"].(string)), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["evidenceUrl"]; got != "https://evidence.example/orders/order_123" {
		t.Fatalf("evidenceUrl = %#v", got)
	}
	if result.RequestID != "req_123" || result.Events[0].EventID != "42" || result.Proto == nil {
		t.Fatalf("unexpected result: %#v", result)
	}

	_, err = client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: 123},
		Action:         "view",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: "audit_2",
	})
	if err != nil {
		t.Fatalf("record audit without evidence: %v", err)
	}
	event = seen.body["events"].([]any)[0].(map[string]any)
	if err := json.Unmarshal([]byte(event["payloadJson"].(string)), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if value, ok := payload["evidenceUrl"]; !ok || value != nil {
		t.Fatalf("absent evidenceUrl must encode as null: %#v", payload)
	}
}

func TestDatasourceRegistrationUsesCanonicalJSONAndResponse(t *testing.T) {
	var seen map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_register")
		_, _ = w.Write([]byte(`{
			"connector":{
				"id":"conn_123",
				"organization_id":"org_123",
				"kind":"postgres",
				"source_system":"primary_postgres",
				"display_name":"Primary Postgres"
			}
		}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:        server.URL,
		AccessToken:    "test-access-token",
		OrganizationID: "org_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Datasources.Register(context.Background(), DatasourceRegistration{
		Name:           "primary_postgres",
		Type:           "postgres",
		IdempotencyKey: "register_primary_postgres",
		DisplayName:    "Primary Postgres",
		ExternalID:     42,
		Metadata:       map[string]any{"environment": "test"},
	})
	if err != nil {
		t.Fatalf("register datasource: %v", err)
	}

	expected := map[string]any{
		"organizationId": "org_123",
		"kind":           "postgres",
		"sourceSystem":   "primary_postgres",
		"displayName":    "Primary Postgres",
		"externalId":     "42",
		"idempotencyKey": "register_primary_postgres",
	}
	for key, value := range expected {
		if seen[key] != value {
			t.Fatalf("%s = %#v, want %#v (body %#v)", key, seen[key], value, seen)
		}
	}
	if result.RequestID != "req_register" ||
		result.Datasource.ID != "conn_123" ||
		result.Datasource.SourceSystem != "primary_postgres" ||
		result.Datasource.Metadata["environment"] != "test" {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestDatasourceRegistrationRequiresVisibleASCIIIdempotencyKey(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:        "https://connect.example.com",
		APIKey:         "test-api-key",
		OrganizationID: "org_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Datasources.Register(context.Background(), DatasourceRegistration{
		Name:           "primary_postgres",
		Type:           "postgres",
		IdempotencyKey: "register primary postgres",
	})
	var medallionErr *Error
	if !errors.As(err, &medallionErr) || medallionErr.Code != "MEDALLION_INVALID_IDEMPOTENCY_KEY" {
		t.Fatalf("error = %#v", err)
	}
}

func TestAuditTrailFiltersSourceActorAndIngester(t *testing.T) {
	var seen map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != listAuditEventsPath {
			t.Fatalf("path = %q, want %q", r.URL.Path, listAuditEventsPath)
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
				"resource_type":"order",
				"resource_id":"order_123",
				"idempotency_key":"audit_1",
				"actor_principal":"user:user_123",
				"ingested_by_principal":"service_account:worker",
				"payload_json":"{\"actor\":{\"type\":\"user\",\"id\":\"payload_spoof\"},\"after\":{\"status\":\"cancelled\"},\"evidenceUrl\":\"https://evidence.example/orders/order_123\"}",
				"action":"cancel",
				"source_system":"orders",
				"origin":"AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
				"outcome":"AUDIT_EVENT_OUTCOME_SUCCEEDED"
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
		Action:            "cancel",
		Origin:            AuditOriginExternalProvider,
		Outcome:           AuditOutcomeSucceeded,
	})
	if err != nil {
		t.Fatalf("trail: %v", err)
	}

	if got := seen["actorPrincipal"]; got != "user:user_123" {
		t.Fatalf("actorPrincipal = %#v", got)
	}
	if got := seen["ingestedByPrincipal"]; got != "service_account:worker" {
		t.Fatalf("ingestedByPrincipal = %#v", got)
	}
	if got := seen["action"]; got != "cancel" {
		t.Fatalf("action = %#v", got)
	}
	if got := seen["origin"]; got != "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER" {
		t.Fatalf("origin = %#v", got)
	}
	if got := seen["outcome"]; got != "AUDIT_EVENT_OUTCOME_SUCCEEDED" {
		t.Fatalf("outcome = %#v", got)
	}
	if _, ok := seen["limit"]; ok {
		t.Fatalf("zero limit must be omitted for server-side defaulting: %#v", seen)
	}
	if result.NextCursor != "cursor_2" || len(result.Events) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.Events[0].IngesterPrincipal != "service_account:worker" || result.Events[0].ActorPrincipal != "user:user_123" {
		t.Fatalf("unexpected event: %#v", result.Events[0])
	}
	if result.Events[0].Actor == nil || result.Events[0].Actor.ID != "user_123" {
		t.Fatalf("authoritative actor principal was not used: %#v", result.Events[0].Actor)
	}
	if result.Events[0].EvidenceURL != "https://evidence.example/orders/order_123" {
		t.Fatalf("evidence URL = %q", result.Events[0].EvidenceURL)
	}
	if result.Events[0].SourceSystem != "orders" || result.Events[0].Origin != AuditOriginExternalProvider || result.Events[0].Outcome != AuditOutcomeSucceeded {
		t.Fatalf("audit provenance/outcome = %#v", result.Events[0])
	}
}

func TestAuditTrailStructuredActorPreservesMatchingColonID(t *testing.T) {
	event := auditEventFromConnect(&connectv1.AuditEvent{
		ActorPrincipal: "user:tenant:42",
		PayloadJson:    `{"actor":{"type":"user","id":"tenant:42"}}`,
	})
	if event.Actor == nil || event.Actor.Type != "user" || event.Actor.Provider != "" || event.Actor.ID != "tenant:42" {
		t.Fatalf("actor = %#v", event.Actor)
	}
}

func TestAuditTrailRejectsSpoofedStructuredActor(t *testing.T) {
	event := auditEventFromConnect(&connectv1.AuditEvent{
		ActorPrincipal: "user:tenant:42",
		PayloadJson:    `{"actor":{"type":"system","id":"attacker"}}`,
	})
	if event.Actor == nil || event.Actor.Type != "user" || event.Actor.Provider != "tenant" || event.Actor.ID != "42" {
		t.Fatalf("wire actor was not authoritative: %#v", event.Actor)
	}
}

func TestAuditTrailValidatesLimit(t *testing.T) {
	for input, want := range map[int]uint32{0: 0, 1: 1, 500: 500} {
		got, err := auditTrailLimit(input, 0)
		if err != nil || got != want {
			t.Fatalf("auditTrailLimit(%d, 0) = %d, %v; want %d", input, got, err, want)
		}
	}
	client, err := NewClient(ClientConfig{
		BaseURL:        "https://connect.example.com",
		APIKey:         "test-api-key",
		OrganizationID: "org_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	tests := []struct {
		name     string
		limit    int
		pageSize int
		code     string
	}{
		{name: "negative limit", limit: -1, code: "MEDALLION_INVALID_AUDIT_TRAIL_LIMIT"},
		{name: "limit too large", limit: 501, code: "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE"},
		{name: "page size too large", pageSize: 501, code: "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Audit.Trail(context.Background(), AuditTrailQuery{
				ResourceType: "order",
				ResourceID:   "order_123",
				Limit:        test.limit,
				PageSize:     test.pageSize,
			})
			medallionErr, ok := err.(*Error)
			if !ok || medallionErr.Code != test.code {
				t.Fatalf("error = %#v, want code %q", err, test.code)
			}
		})
	}
}

func TestActionExecutionStatusIncludesIndeterminate(t *testing.T) {
	if got := connectv1.ActionExecutionStatus_ACTION_EXECUTION_STATUS_INDETERMINATE.String(); got != "ACTION_EXECUTION_STATUS_INDETERMINATE" {
		t.Fatalf("status = %q", got)
	}
}

func TestCanonicalCdcProtoShapes(t *testing.T) {
	tests := []struct {
		name   string
		fields interface {
			Len() int
			Get(int) protoreflect.FieldDescriptor
		}
		expected []string
	}{
		{
			name:   "CdcEvent",
			fields: (&connectv1.CdcEvent{}).ProtoReflect().Descriptor().Fields(),
			expected: []string{
				"id", "organization_id", "connector_id", "stream_name",
				"entity_type", "entity_id", "operation", "source_event_id",
				"idempotency_key", "actor_principal", "payload_json",
				"occurred_at", "observed_at", "description", "source_system",
				"ingested_by_principal",
			},
		},
		{
			name:   "ListCdcEventsRequest",
			fields: (&connectv1.ListCdcEventsRequest{}).ProtoReflect().Descriptor().Fields(),
			expected: []string{
				"organization_id", "connector_id", "entity_type", "entity_id",
				"limit", "actor_principal", "occurred_at_from", "occurred_at_to",
				"source_system", "stream_name", "page_cursor",
				"ingested_by_principal",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.fields.Len() != len(test.expected) {
				t.Fatalf("field count = %d, want %d", test.fields.Len(), len(test.expected))
			}
			for index, name := range test.expected {
				field := test.fields.Get(index)
				if int(field.Number()) != index+1 || string(field.Name()) != name {
					t.Fatalf("field %d = (%d, %s), want (%d, %s)", index, field.Number(), field.Name(), index+1, name)
				}
			}
		})
	}
}

func TestAuditTrailRequiresResourceType(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:        "https://connect.example.com",
		APIKey:         "test-api-key",
		OrganizationID: "org_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Audit.Trail(context.Background(), AuditTrailQuery{ResourceID: "order_123"})
	if err == nil {
		t.Fatal("expected missing resource type error")
	}
	medallionErr, ok := err.(*Error)
	if !ok || medallionErr.Code != "MEDALLION_MISSING_RESOURCE_TYPE" {
		t.Fatalf("error = %#v", err)
	}
}

func TestSinglePrimaryKeyProjectionUsesItsValue(t *testing.T) {
	if single := entityIDFromPrimaryKey(map[string]string{"id": "1"}); single != "1" {
		t.Fatalf("single-key projection = %q", single)
	}
}

func TestCdcRecordStaysOnDedicatedCdcRoute(t *testing.T) {
	var seen struct {
		path string
		body map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.path = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&seen.body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"accepted_count":1,"events":[{"idempotency_key":"cdc_1","event_id":"43"}]}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "test-api-key", DefaultConnectorID: "conn_123"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"tenant_id": "tenant_a", "id": "1"},
		EntityID:       "tenant_a/order/1",
		IdempotencyKey: "cdc_1",
	})
	if err != nil {
		t.Fatalf("record CDC: %v", err)
	}

	if seen.path != publishCdcEventsPath {
		t.Fatalf("path = %q, want %q", seen.path, publishCdcEventsPath)
	}
	event := seen.body["events"].([]any)[0].(map[string]any)
	if got := event["entityId"]; got != "tenant_a/order/1" {
		t.Fatalf("entityId = %#v", got)
	}
	if _, ok := event["resourceType"]; ok {
		t.Fatalf("CDC event must not use audit fields: %#v", event)
	}
	if _, ok := event["kind"]; ok {
		t.Fatalf("canonical CDC event must not include deprecated kind: %#v", event)
	}
	if _, ok := event["action"]; ok {
		t.Fatalf("CDC event must not include audit action: %#v", event)
	}
}

func TestCdcRecordRejectsCompositePrimaryKeyWithoutEntityID(t *testing.T) {
	client, err := NewClient(ClientConfig{BaseURL: "https://connect.example.com", APIKey: "test-api-key", DefaultConnectorID: "conn_123"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"tenant_id": "tenant_a", "id": "1"},
		IdempotencyKey: "cdc_composite_without_entity",
	})
	var medallionErr *Error
	if !errors.As(err, &medallionErr) || medallionErr.Code != "MEDALLION_MISSING_CDC_ENTITY_ID" {
		t.Fatalf("error = %#v", err)
	}
}

func TestCdcRecordRejectsEmptyPrimaryKey(t *testing.T) {
	client, err := NewClient(ClientConfig{BaseURL: "https://connect.example.com", APIKey: "test-api-key", DefaultConnectorID: "conn_123"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		Source:     "postgres",
		Table:      "orders",
		Operation:  "update",
		PrimaryKey: map[string]IDInput{},
	})
	if err == nil {
		t.Fatal("expected empty primary key error")
	}
	medallionErr, ok := err.(*Error)
	if !ok || medallionErr.Code != "MEDALLION_EMPTY_CDC_PRIMARY_KEY" {
		t.Fatalf("error = %#v", err)
	}
}

func TestEventRecordsRequireStableIdempotencyKeys(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:            "https://connect.example.com",
		APIKey:             "test-api-key",
		DefaultConnectorID: "conn_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, auditErr := client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: "user_123"},
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: " ",
	})
	_, cdcErr := client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"id": "order_123"},
		IdempotencyKey: " ",
	})
	for name, got := range map[string]error{"audit": auditErr, "cdc": cdcErr} {
		medallionErr, ok := got.(*Error)
		if !ok || medallionErr.Code != "MEDALLION_MISSING_IDEMPOTENCY_KEY" {
			t.Fatalf("%s error = %#v, want MEDALLION_MISSING_IDEMPOTENCY_KEY", name, got)
		}
	}

	_, auditTooLong := client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: "user_123"},
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: strings.Repeat("x", 513),
	})
	_, cdcTooLong := client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"id": "order_123"},
		IdempotencyKey: strings.Repeat("x", 513),
	})
	for name, got := range map[string]error{
		"audit too long": auditTooLong,
		"cdc too long":   cdcTooLong,
	} {
		medallionErr, ok := got.(*Error)
		if !ok || medallionErr.Code != "MEDALLION_INVALID_IDEMPOTENCY_KEY" {
			t.Fatalf("%s error = %#v, want MEDALLION_INVALID_IDEMPOTENCY_KEY", name, got)
		}
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
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
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
	if span.Name != "test-medallion POST "+publishAuditEventsPath {
		t.Fatalf("span name = %q", span.Name)
	}
	attrs := map[string]string{}
	for _, attr := range span.Attributes {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	if attrs["medallion.sdk.language"] != "go" || attrs["medallion.request.path"] != publishAuditEventsPath {
		t.Fatalf("unexpected span attributes: %#v", attrs)
	}
	if attrs["medallion.request_id"] != "req_trace" {
		t.Fatalf("request id attribute = %q", attrs["medallion.request_id"])
	}
}

func TestSuccessfulResponsesRequireSemanticAcknowledgements(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_empty")
		_, _ = w.Write([]byte(`{}`))
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

	_, err = client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: "user_123"},
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: "audit_empty_ack",
	})
	var medallionErr *Error
	if !errors.As(err, &medallionErr) ||
		medallionErr.Code != "MEDALLION_INVALID_PUBLISH_RESPONSE" ||
		medallionErr.RequestID != "req_empty" {
		t.Fatalf("publish error = %#v", err)
	}

	_, err = client.Connect.RegisterDatasource(context.Background(), DatasourceRegistration{
		Name:           "primary_postgres",
		Type:           "postgres",
		IdempotencyKey: "register_primary_postgres",
	})
	if !errors.As(err, &medallionErr) ||
		medallionErr.Code != "MEDALLION_INVALID_DATASOURCE_RESPONSE" ||
		medallionErr.RequestID != "req_empty" {
		t.Fatalf("datasource error = %#v", err)
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
		BaseURL:            server.URL,
		AccessToken:        "   ",
		APIKey:             "fallback-key",
		DefaultConnectorID: "conn_123",
	})
	if err != nil {
		t.Fatalf("new client with fallback key: %v", err)
	}
	_, err = client.Audit.Record(context.Background(), AuditRecord{
		Actor:          ActorRef{Type: "user", ID: "user_123"},
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: "audit_malformed_response",
	})
	var medallionErr *Error
	if !errors.As(err, &medallionErr) ||
		medallionErr.Code != "MEDALLION_INVALID_JSON_RESPONSE" ||
		medallionErr.RequestID != "req_invalid" ||
		medallionErr.Unwrap() == nil {
		t.Fatalf("invalid response error = %#v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Audit.Record(cancelled, AuditRecord{
		Actor:          ActorRef{Type: "user", ID: "user_123"},
		Action:         "cancel",
		Outcome:        AuditOutcomeSucceeded,
		Resource:       ResourceRef{Type: "order", ID: "order_123"},
		IdempotencyKey: "audit_cancelled_request",
	})
	if !errors.As(err, &medallionErr) ||
		medallionErr.Code != "MEDALLION_ABORTED" ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %#v", err)
	}

	_, err = NewClient(ClientConfig{
		BaseURL: "https://user:secret@example.com?unsafe=true",
		APIKey:  "key",
	})
	if !errors.As(err, &medallionErr) || medallionErr.Code != "MEDALLION_INVALID_OPTIONS" {
		t.Fatalf("invalid base URL error = %#v", err)
	}
}
