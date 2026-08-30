package medallion

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

var uuidShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

const testQueryName = "queries/01jz9q5g6rsf7r5ar4rah1b2c3"

func newIngestTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "test-api-key",
		WorkspaceID: testWorkspaceID,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client, server
}

func jsonRow(t *testing.T, values map[string]any) *ingestv1.Row {
	t.Helper()
	fields, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("build row: %v", err)
	}
	return &ingestv1.Row{Json: fields}
}

func TestIngestAppendRowsStampsBatchKeyAndCanonicalRoute(t *testing.T) {
	var seen struct {
		path    string
		headers http.Header
		body    map[string]any
	}
	client, _ := newIngestTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		seen.path = r.URL.Path
		seen.headers = r.Header.Clone()
		if err := json.NewDecoder(r.Body).Decode(&seen.body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_ingest_1")
		_ = json.NewEncoder(w).Encode(map[string]any{"acceptedRows": "2"})
	})

	first := jsonRow(t, map[string]any{"level": "info"})
	first.InsertId = "evt-1"
	request := &ingestv1.AppendRowsRequest{
		Table: "tables/events",
		Rows: []*ingestv1.Row{
			first,
			jsonRow(t, map[string]any{"level": "warn"}),
		},
	}
	response, requestID, err := client.Ingest.AppendRows(context.Background(), request)
	if err != nil {
		t.Fatalf("append rows: %v", err)
	}
	if seen.path != "/medallion.ingest.v1.MedallionIngestService/AppendRows" {
		t.Fatalf("unexpected path %q", seen.path)
	}
	key := seen.headers.Get("Idempotency-Key")
	if !uuidShape.MatchString(key) {
		t.Fatalf("expected a generated UUID Idempotency-Key, got %q", key)
	}
	// request_id is the field the contract deduplicates on, so the generated
	// key must reach the body as well as the header.
	if request.GetRequestId() != key {
		t.Fatalf("expected request_id %q to match the batch key %q", request.GetRequestId(), key)
	}
	if seen.body["requestId"] != key {
		t.Fatalf("expected the wire request_id to carry the batch key, got %v", seen.body["requestId"])
	}
	if seen.headers.Get("X-Medallion-Workspace-Id") != testWorkspaceID {
		t.Fatalf("workspace header missing")
	}
	rows := seen.body["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows on the wire, got %d", len(rows))
	}
	if response.GetAcceptedRows() != 2 {
		t.Fatalf("expected 2 accepted rows, got %d", response.GetAcceptedRows())
	}
	if requestID != "req_ingest_1" {
		t.Fatalf("unexpected request id %q", requestID)
	}
}

func TestIngestAppendRowsHonorsCallerIdempotencyKeyFromContext(t *testing.T) {
	var seenKey string
	client, _ := newIngestTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		seenKey = r.Header.Get("Idempotency-Key")
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"acceptedRows": "1",
			"rowErrors": []any{map[string]any{
				"index": "0",
				"error": map[string]any{"code": 3, "message": "level expects a string"},
			}},
		})
	})

	ctx := WithIngestIdempotencyKey(context.Background(), "outbox:batch:42")
	response, _, err := client.Ingest.AppendRows(ctx, &ingestv1.AppendRowsRequest{
		Table: "tables/events",
		ArrowRows: &ingestv1.ArrowRecordBatch{
			SerializedRecordBatch: []byte("ARROW"),
		},
	})
	if err != nil {
		t.Fatalf("append rows: %v", err)
	}
	if seenKey != "outbox:batch:42" {
		t.Fatalf("expected the pinned key, got %q", seenKey)
	}
	if len(response.GetRowErrors()) != 1 {
		t.Fatalf("expected one row error to survive decoding")
	}
	rowError := response.GetRowErrors()[0]
	if rowError.GetIndex() != 0 || rowError.GetError().GetCode() != 3 {
		t.Fatalf("unexpected row error %v", rowError)
	}
}

func TestIngestRejectsMalformedRequestsLocally(t *testing.T) {
	client, _ := newIngestTestClient(t, func(http.ResponseWriter, *http.Request) {
		t.Fatalf("no request may reach the network")
	})

	_, _, err := client.Ingest.AppendRows(context.Background(), &ingestv1.AppendRowsRequest{Table: "tables/events"})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_AMBIGUOUS_ROW_PAYLOAD" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.AppendRows(context.Background(), &ingestv1.AppendRowsRequest{
		Table: "events",
		Rows:  []*ingestv1.Row{jsonRow(t, map[string]any{"level": "info"})},
	})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_INVALID_TABLE_ID" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.CreateTable(context.Background(), &ingestv1.CreateTableRequest{TableId: "Events"})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_INVALID_TABLE_ID" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.CreateTable(context.Background(), &ingestv1.CreateTableRequest{
		TableId: "events",
		Table:   &ingestv1.Table{},
	})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_INVALID_SCHEMA" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.RunQuery(context.Background(), &ingestv1.RunQueryRequest{Query: "   "})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_INVALID_QUERY" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.GetQueryResults(context.Background(), &ingestv1.GetQueryResultsRequest{})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_INVALID_QUERY" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
}

func TestIngestQueryPollAndPaginateRoutes(t *testing.T) {
	var paths []string
	client, _ := newIngestTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("content-type", "application/json")
		if r.Header.Get("Idempotency-Key") != "" {
			t.Fatalf("reads must not carry an Idempotency-Key header")
		}
		switch len(paths) {
		case 1:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":  testQueryName,
				"state": "RUNNING",
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":      testQueryName,
				"state":     "SUCCEEDED",
				"rows":      []any{map[string]any{"n": 1}},
				"totalRows": "1",
			})
		}
	})

	first, _, err := client.Ingest.RunQuery(context.Background(), &ingestv1.RunQueryRequest{Query: "SELECT n FROM events"})
	if err != nil {
		t.Fatalf("run query: %v", err)
	}
	if first.GetState() != "RUNNING" {
		t.Fatalf("expected the first acknowledgement to still be running")
	}
	poll, _, err := client.Ingest.GetQueryResults(context.Background(), &ingestv1.GetQueryResultsRequest{
		Name: first.GetName(),
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if poll.GetState() != "SUCCEEDED" || len(poll.GetRows()) != 1 {
		t.Fatalf("expected one succeeded row page")
	}
	if poll.GetRows()[0].GetFields()["n"].GetNumberValue() != 1 {
		t.Fatalf("expected the result row to decode as a struct")
	}
	if paths[0] != "/medallion.ingest.v1.MedallionIngestService/RunQuery" ||
		paths[1] != "/medallion.ingest.v1.MedallionIngestService/GetQueryResults" {
		t.Fatalf("unexpected routes %v", paths)
	}
}

func TestIngestTableLifecycleRoutes(t *testing.T) {
	var paths []string
	var createKey string
	client, _ := newIngestTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if len(paths) == 1 {
			createKey = r.Header.Get("Idempotency-Key")
		}
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case createTablePath, getTablePath, updateTablePath:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"table": map[string]any{
					"name":       "tables/events",
					"timeColumn": "occurred_at",
				},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tables":        []any{map[string]any{"name": "tables/events"}},
				"nextPageToken": "",
			})
		}
	})

	schema := &ingestv1.TableSchema{Columns: []*ingestv1.ColumnSchema{
		{Name: "occurred_at", Type: "TIMESTAMP"},
		{Name: "level", Type: "STRING"},
	}}
	created, _, err := client.Ingest.CreateTable(context.Background(), &ingestv1.CreateTableRequest{
		TableId: "events",
		Table:   &ingestv1.Table{Schema: schema, TimeColumn: "occurred_at"},
	})
	if err != nil || created.GetTable().GetName() != "tables/events" {
		t.Fatalf("create table: %v", err)
	}
	if !uuidShape.MatchString(createKey) {
		t.Fatalf("table creation must carry a generated Idempotency-Key, got %q", createKey)
	}
	if _, _, err = client.Ingest.GetTable(context.Background(), &ingestv1.GetTableRequest{Name: "tables/events"}); err != nil {
		t.Fatalf("get table: %v", err)
	}
	evolved := &ingestv1.TableSchema{Columns: append(schema.GetColumns(), &ingestv1.ColumnSchema{
		Name:     "trace_id",
		Type:     "STRING",
		Nullable: true,
	})}
	if _, _, err = client.Ingest.UpdateTable(context.Background(), &ingestv1.UpdateTableRequest{
		Table: &ingestv1.Table{Name: "tables/events", Schema: evolved},
	}); err != nil {
		t.Fatalf("update table: %v", err)
	}
	listed, _, err := client.Ingest.ListTables(context.Background(), &ingestv1.ListTablesRequest{PageSize: 10})
	if err != nil || len(listed.GetTables()) != 1 {
		t.Fatalf("list tables: %v", err)
	}
}

func asSDKError(t *testing.T, err error) *Error {
	t.Helper()
	sdkError, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected a Medallion SDK error, got %T: %v", err, err)
	}
	return sdkError
}
