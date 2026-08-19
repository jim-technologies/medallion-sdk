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
		key := seen.body["events"].([]any)[0].(map[string]any)["idempotencyKey"]
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accepted_count": 1,
			"events": []any{map[string]any{
				"idempotency_key": key,
				"event_id":        "42",
			}},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "test-api-key",
		WorkspaceID:        testWorkspaceID,
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
	if got := seen.headers.Get("X-Medallion-API-Key"); got != "test-api-key" {
		t.Fatalf("API key = %q", got)
	}
	if got := seen.headers.Get("Authorization"); got != "" {
		t.Fatalf("API-key authentication must not set authorization: %q", got)
	}
	if got := seen.headers.Get("X-Medallion-Workspace-Id"); got != testWorkspaceID {
		t.Fatalf("workspace ID = %q", got)
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
				"workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c3",
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
		WorkspaceID:        testWorkspaceID,
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
	if got := seen["workspaceId"]; got != testWorkspaceID {
		t.Fatalf("workspaceId = %#v", got)
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
		ActorPrincipal: "user:realm:42",
		PayloadJson:    `{"actor":{"type":"user","id":"realm:42"}}`,
	})
	if event.Actor == nil || event.Actor.Type != "user" || event.Actor.Provider != "" || event.Actor.ID != "realm:42" {
		t.Fatalf("actor = %#v", event.Actor)
	}
}

func TestAuditTrailRejectsSpoofedStructuredActor(t *testing.T) {
	event := auditEventFromConnect(&connectv1.AuditEvent{
		ActorPrincipal: "user:realm:42",
		PayloadJson:    `{"actor":{"type":"system","id":"attacker"}}`,
	})
	if event.Actor == nil || event.Actor.Type != "user" || event.Actor.Provider != "realm" || event.Actor.ID != "42" {
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
		BaseURL:     "https://api.example.com",
		APIKey:      "test-api-key",
		WorkspaceID: testWorkspaceID,
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

func TestGeneratedConnectDescriptorIsBounded(t *testing.T) {
	file := connectv1.File_medallion_connect_v1_connect_proto
	if file.Services().Len() != 1 {
		t.Fatalf("service count = %d, want 1", file.Services().Len())
	}
	service := file.Services().Get(0)
	if got := string(service.FullName()); got != "medallion.connect.v1.MedallionConnectService" {
		t.Fatalf("service = %q", got)
	}
	methodNames := make([]string, service.Methods().Len())
	for index := range methodNames {
		methodNames[index] = string(service.Methods().Get(index).Name())
	}
	if got, want := strings.Join(methodNames, ","), "PublishCdcEvents,ListCdcEvents,PublishAuditEvents,ListAuditEvents"; got != want {
		t.Fatalf("methods = %q, want %q", got, want)
	}

	messageNames := make([]string, file.Messages().Len())
	for index := range messageNames {
		messageNames[index] = string(file.Messages().Get(index).Name())
	}
	if got, want := strings.Join(messageNames, ","), "CdcEvent,AuditEvent,PublishCdcEventsRequest,PublishedCdcEvent,PublishCdcEventsResponse,ListCdcEventsRequest,ListCdcEventsResponse,PublishAuditEventsRequest,PublishedAuditEvent,PublishAuditEventsResponse,ListAuditEventsRequest,ListAuditEventsResponse"; got != want {
		t.Fatalf("messages = %q, want %q", got, want)
	}

	enumNames := make([]string, file.Enums().Len())
	for index := range enumNames {
		enumNames[index] = string(file.Enums().Get(index).Name())
	}
	if got, want := strings.Join(enumNames, ","), "CdcOperation,AuditEventOrigin,AuditEventOutcome"; got != want {
		t.Fatalf("enums = %q, want %q", got, want)
	}
}

func TestCanonicalCdcProtoShapes(t *testing.T) {
	tests := []struct {
		name   string
		fields interface {
			Len() int
			Get(int) protoreflect.FieldDescriptor
		}
		expected []struct {
			name   string
			number protoreflect.FieldNumber
		}
	}{
		{
			name:   "CdcEvent",
			fields: (&connectv1.CdcEvent{}).ProtoReflect().Descriptor().Fields(),
			expected: []struct {
				name   string
				number protoreflect.FieldNumber
			}{
				{"id", 1}, {"connector_id", 3}, {"stream_name", 4},
				{"entity_type", 5}, {"entity_id", 6}, {"operation", 7},
				{"source_event_id", 8}, {"idempotency_key", 9}, {"actor_principal", 10},
				{"payload_json", 11}, {"occurred_at", 12}, {"observed_at", 13},
				{"description", 14}, {"source_system", 15}, {"ingested_by_principal", 16},
				{"workspace_id", 17},
			},
		},
		{
			name:   "ListCdcEventsRequest",
			fields: (&connectv1.ListCdcEventsRequest{}).ProtoReflect().Descriptor().Fields(),
			expected: []struct {
				name   string
				number protoreflect.FieldNumber
			}{
				{"connector_id", 2}, {"entity_type", 3}, {"entity_id", 4},
				{"limit", 5}, {"actor_principal", 6}, {"occurred_at_from", 7},
				{"occurred_at_to", 8}, {"source_system", 9}, {"stream_name", 10},
				{"page_cursor", 11}, {"ingested_by_principal", 12}, {"workspace_id", 13},
			},
		},
		{
			name:   "AuditEvent",
			fields: (&connectv1.AuditEvent{}).ProtoReflect().Descriptor().Fields(),
			expected: []struct {
				name   string
				number protoreflect.FieldNumber
			}{
				{"id", 1}, {"connector_id", 3}, {"resource_type", 4}, {"resource_id", 5},
				{"action", 6}, {"source_event_id", 7}, {"idempotency_key", 8},
				{"actor_principal", 9}, {"payload_json", 10}, {"occurred_at", 11},
				{"observed_at", 12}, {"description", 13}, {"source_system", 14},
				{"ingested_by_principal", 15}, {"origin", 16}, {"outcome", 17},
				{"workspace_id", 18},
			},
		},
		{
			name:   "ListAuditEventsRequest",
			fields: (&connectv1.ListAuditEventsRequest{}).ProtoReflect().Descriptor().Fields(),
			expected: []struct {
				name   string
				number protoreflect.FieldNumber
			}{
				{"connector_id", 2}, {"resource_type", 3}, {"resource_id", 4}, {"limit", 5},
				{"actor_principal", 6}, {"action", 7}, {"occurred_at_from", 8},
				{"occurred_at_to", 9}, {"source_system", 10}, {"page_cursor", 11},
				{"ingested_by_principal", 12}, {"origin", 13}, {"outcome", 14},
				{"workspace_id", 15},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.fields.Len() != len(test.expected) {
				t.Fatalf("field count = %d, want %d", test.fields.Len(), len(test.expected))
			}
			for index, expected := range test.expected {
				field := test.fields.Get(index)
				if field.Number() != expected.number || string(field.Name()) != expected.name {
					t.Fatalf("field %d = (%d, %s), want (%d, %s)", index, field.Number(), field.Name(), expected.number, expected.name)
				}
			}
		})
	}
}

func TestWorkspaceCutoverFieldsAreReserved(t *testing.T) {
	for _, test := range []struct {
		name    string
		message protoreflect.MessageDescriptor
		number  protoreflect.FieldNumber
	}{
		{"CdcEvent", (&connectv1.CdcEvent{}).ProtoReflect().Descriptor(), 2},
		{"AuditEvent", (&connectv1.AuditEvent{}).ProtoReflect().Descriptor(), 2},
		{"ListCdcEventsRequest", (&connectv1.ListCdcEventsRequest{}).ProtoReflect().Descriptor(), 1},
		{"ListAuditEventsRequest", (&connectv1.ListAuditEventsRequest{}).ProtoReflect().Descriptor(), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !test.message.ReservedRanges().Has(test.number) || test.message.ReservedNames().Len() != 1 {
				t.Fatalf("reserved metadata missing for %s", test.name)
			}
		})
	}
}

func TestAuditTrailRequiresResourceType(t *testing.T) {
	client, err := NewClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "test-api-key",
		WorkspaceID: testWorkspaceID,
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

	client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "test-api-key", WorkspaceID: testWorkspaceID, DefaultConnectorID: "conn_123"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"account_id": "account_a", "id": "1"},
		EntityID:       "account_a/order/1",
		IdempotencyKey: "cdc_1",
	})
	if err != nil {
		t.Fatalf("record CDC: %v", err)
	}

	if seen.path != publishCdcEventsPath {
		t.Fatalf("path = %q, want %q", seen.path, publishCdcEventsPath)
	}
	event := seen.body["events"].([]any)[0].(map[string]any)
	if got := event["entityId"]; got != "account_a/order/1" {
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
	client, err := NewClient(ClientConfig{BaseURL: "https://api.example.com", APIKey: "test-api-key", WorkspaceID: testWorkspaceID, DefaultConnectorID: "conn_123"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"account_id": "account_a", "id": "1"},
		IdempotencyKey: "cdc_composite_without_entity",
	})
	var medallionErr *Error
	if !errors.As(err, &medallionErr) || medallionErr.Code != "MEDALLION_MISSING_CDC_ENTITY_ID" {
		t.Fatalf("error = %#v", err)
	}
}

func TestCdcRecordRejectsEmptyPrimaryKey(t *testing.T) {
	client, err := NewClient(ClientConfig{BaseURL: "https://api.example.com", APIKey: "test-api-key", WorkspaceID: testWorkspaceID, DefaultConnectorID: "conn_123"})
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
		BaseURL:            "https://api.example.com",
		APIKey:             "test-api-key",
		WorkspaceID:        testWorkspaceID,
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
		IdempotencyKey: "",
	})
	_, cdcErr := client.CDC.Record(context.Background(), CDCEvent{
		Source:         "postgres",
		Table:          "orders",
		Operation:      "update",
		PrimaryKey:     map[string]IDInput{"id": "order_123"},
		IdempotencyKey: "",
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
		WorkspaceID:        testWorkspaceID,
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
		WorkspaceID:        testWorkspaceID,
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
		WorkspaceID:        testWorkspaceID,
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
		!errors.Is(err, errInvalidJSONResponse) {
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
		BaseURL:     "https://user:secret@example.com?unsafe=true",
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
	})
	if !errors.As(err, &medallionErr) || medallionErr.Code != "MEDALLION_INVALID_OPTIONS" {
		t.Fatalf("invalid base URL error = %#v", err)
	}
}
