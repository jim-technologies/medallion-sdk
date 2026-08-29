package medallion

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
)

var uuidShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

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

func TestIngestAppendSendsIdempotencyKeyAndCanonicalRoute(t *testing.T) {
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
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted_rows": "2"})
	})

	response, requestID, err := client.Ingest.Append(context.Background(), &ingestv1.AppendRequest{
		DatasetId: "events",
		Rows: &ingestv1.AppendRequest_JsonRows{JsonRows: &ingestv1.JsonRows{Rows: []*ingestv1.Row{
			{InsertId: "evt-1", Json: `{"level":"info"}`},
			{Json: `{"level":"warn"}`},
		}}},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seen.path != "/medallion.ingest.v1.MedallionIngestService/Append" {
		t.Fatalf("unexpected path %q", seen.path)
	}
	if key := seen.headers.Get("Idempotency-Key"); !uuidShape.MatchString(key) {
		t.Fatalf("expected a generated UUID Idempotency-Key, got %q", key)
	}
	if seen.headers.Get("X-Medallion-Workspace-Id") != testWorkspaceID {
		t.Fatalf("workspace header missing")
	}
	rows := seen.body["jsonRows"].(map[string]any)["rows"].([]any)
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

func TestIngestAppendHonorsCallerIdempotencyKeyFromContext(t *testing.T) {
	var seenKey string
	client, _ := newIngestTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		seenKey = r.Header.Get("Idempotency-Key")
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"duplicate": true})
	})

	ctx := WithIngestIdempotencyKey(context.Background(), "outbox:batch:42")
	response, _, err := client.Ingest.Append(ctx, &ingestv1.AppendRequest{
		DatasetId: "events",
		Rows: &ingestv1.AppendRequest_ArrowRows{ArrowRows: &ingestv1.ArrowRecordBatch{
			SerializedRecordBatch: []byte("ARROW"),
		}},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seenKey != "outbox:batch:42" {
		t.Fatalf("expected the pinned key, got %q", seenKey)
	}
	if !response.GetDuplicate() {
		t.Fatalf("expected the duplicate acknowledgement to survive decoding")
	}
}

func TestIngestAppendRejectsAmbiguousOrMissingPayloadLocally(t *testing.T) {
	client, _ := newIngestTestClient(t, func(http.ResponseWriter, *http.Request) {
		t.Fatalf("no request may reach the network")
	})

	_, _, err := client.Ingest.Append(context.Background(), &ingestv1.AppendRequest{DatasetId: "events"})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_AMBIGUOUS_ROW_PAYLOAD" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.Append(context.Background(), &ingestv1.AppendRequest{
		Rows: &ingestv1.AppendRequest_JsonRows{JsonRows: &ingestv1.JsonRows{Rows: []*ingestv1.Row{{Json: "{}"}}}},
	})
	if sdkError := asSDKError(t, err); sdkError.Code != "MEDALLION_INVALID_DATASET_ID" {
		t.Fatalf("unexpected code %q", sdkError.Code)
	}
	_, _, err = client.Ingest.Query(context.Background(), &ingestv1.QueryRequest{Query: "   "})
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
				"results": map[string]any{"completed": false, "queryId": "q_1"},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": map[string]any{
					"completed": true,
					"queryId":   "q_1",
					"rowsJson":  []string{`{"n":1}`},
					"totalRows": "1",
				},
			})
		}
	})

	first, _, err := client.Ingest.Query(context.Background(), &ingestv1.QueryRequest{Query: "SELECT n FROM events"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if first.GetResults().GetCompleted() {
		t.Fatalf("expected the first acknowledgement to still be running")
	}
	poll, _, err := client.Ingest.GetQueryResults(context.Background(), &ingestv1.GetQueryResultsRequest{
		QueryId: first.GetResults().GetQueryId(),
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if !poll.GetResults().GetCompleted() || len(poll.GetResults().GetRowsJson()) != 1 {
		t.Fatalf("expected one completed row page")
	}
	if paths[0] != "/medallion.ingest.v1.MedallionIngestService/Query" ||
		paths[1] != "/medallion.ingest.v1.MedallionIngestService/GetQueryResults" {
		t.Fatalf("unexpected routes %v", paths)
	}
}

func TestIngestDatasetLifecycleRoutes(t *testing.T) {
	var paths []string
	var createKey string
	client, _ := newIngestTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if len(paths) == 1 {
			createKey = r.Header.Get("Idempotency-Key")
		}
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case createDatasetPath, getDatasetPath:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"dataset": map[string]any{"datasetId": "events"},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"datasets":      []any{map[string]any{"datasetId": "events"}},
				"nextPageToken": "",
			})
		}
	})

	created, _, err := client.Ingest.CreateDataset(context.Background(), &ingestv1.CreateDatasetRequest{DatasetId: "events"})
	if err != nil || created.GetDataset().GetDatasetId() != "events" {
		t.Fatalf("create dataset: %v", err)
	}
	if !uuidShape.MatchString(createKey) {
		t.Fatalf("dataset creation must carry a generated Idempotency-Key, got %q", createKey)
	}
	if _, _, err = client.Ingest.GetDataset(context.Background(), &ingestv1.GetDatasetRequest{DatasetId: "events"}); err != nil {
		t.Fatalf("get dataset: %v", err)
	}
	listed, _, err := client.Ingest.ListDatasets(context.Background(), &ingestv1.ListDatasetsRequest{PageSize: 10})
	if err != nil || len(listed.GetDatasets()) != 1 {
		t.Fatalf("list datasets: %v", err)
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
