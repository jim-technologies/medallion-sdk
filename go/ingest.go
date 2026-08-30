package medallion

import (
	"context"
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"

	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
)

const ingestService = "/medallion.ingest.v1.MedallionIngestService"

const (
	createTablePath     = ingestService + "/CreateTable"
	getTablePath        = ingestService + "/GetTable"
	listTablesPath      = ingestService + "/ListTables"
	updateTablePath     = ingestService + "/UpdateTable"
	appendRowsPath      = ingestService + "/AppendRows"
	runQueryPath        = ingestService + "/RunQuery"
	getQueryResultsPath = ingestService + "/GetQueryResults"
)

const (
	maxIngestIdempotencyKeyBytes = 255
	maxIngestQueryBytes          = 262144
)

var (
	ingestTableIDPattern   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
	ingestTableNamePattern = regexp.MustCompile(`^tables/[a-z][a-z0-9_]{0,62}$`)
	ingestQueryNamePattern = regexp.MustCompile(`^queries/[0-9a-hjkmnp-tv-z]{26}$`)
)

type ingestIdempotencyKeyContextKey struct{}

// WithIngestIdempotencyKey pins the batch idempotency key for the CreateTable,
// UpdateTable, or AppendRows call made with the returned context, so an exact
// replay of the same batch is absorbed server-side without duplication.
// Without it the SDK generates a fresh key per call.
func WithIngestIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ingestIdempotencyKeyContextKey{}, key)
}

// IngestClient is the thin generated-binding client for the seven
// medallion.ingest.v1 tabular ingestion and query RPCs. It passes protobuf
// requests through with header-based workspace identity; the richer
// convenience layer ships Python-first.
type IngestClient struct {
	requests *requestClient
}

// CreateTable declares one table: an ordered schema, a TIMESTAMP time column,
// and an optional sort key. Re-declaring an identical table returns the
// existing one, so retries are safe.
func (c *IngestClient) CreateTable(ctx context.Context, request *ingestv1.CreateTableRequest) (*ingestv1.CreateTableResponse, string, error) {
	if !ingestTableIDPattern.MatchString(request.GetTableId()) {
		return nil, "", invalidIngestTableID("table_id must be a lowercase identifier of at most 63 characters")
	}
	if err := requireIngestSchema(request.GetTable()); err != nil {
		return nil, "", err
	}
	key, err := stampIngestRequestID(ctx, request)
	if err != nil {
		return nil, "", err
	}
	response := &ingestv1.CreateTableResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, createTablePath, request, response, postOptions{
		retrySafe:      true,
		idempotencyKey: key,
	})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// UpdateTable evolves one table's schema. The request carries the FULL desired
// schema, and the only permitted change is appending new nullable columns
// after the existing ones. Resending the current schema is a no-op success.
func (c *IngestClient) UpdateTable(ctx context.Context, request *ingestv1.UpdateTableRequest) (*ingestv1.UpdateTableResponse, string, error) {
	if err := requireIngestTableName(request.GetTable().GetName()); err != nil {
		return nil, "", err
	}
	if err := requireIngestSchema(request.GetTable()); err != nil {
		return nil, "", err
	}
	key, err := stampIngestRequestID(ctx, request)
	if err != nil {
		return nil, "", err
	}
	response := &ingestv1.UpdateTableResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, updateTablePath, request, response, postOptions{
		retrySafe:      true,
		idempotencyKey: key,
	})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// GetTable reads one table by resource name, "tables/{table}".
func (c *IngestClient) GetTable(ctx context.Context, request *ingestv1.GetTableRequest) (*ingestv1.GetTableResponse, string, error) {
	if err := requireIngestTableName(request.GetName()); err != nil {
		return nil, "", err
	}
	response := &ingestv1.GetTableResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, getTablePath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// ListTables pages through the tables of the selected workspace, ordered by
// table id.
func (c *IngestClient) ListTables(ctx context.Context, request *ingestv1.ListTablesRequest) (*ingestv1.ListTablesResponse, string, error) {
	response := &ingestv1.ListTablesResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, listTablesPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// AppendRows appends one batch of JSON rows or one Arrow IPC record batch to a
// table; the insertAll analog. The acknowledged rows are durable and
// immediately queryable. Every call carries a batch idempotency key, sent as
// the Idempotency-Key header and stamped into request_id when the caller left
// that field empty; request_id is what the contract deduplicates on.
func (c *IngestClient) AppendRows(ctx context.Context, request *ingestv1.AppendRowsRequest) (*ingestv1.AppendRowsResponse, string, error) {
	if err := requireIngestTableName(request.GetTable()); err != nil {
		return nil, "", err
	}
	hasJSON := len(request.GetRows()) > 0
	hasArrow := request.GetArrowRows() != nil
	if hasJSON == hasArrow {
		return nil, "", &Error{
			Code:    "MEDALLION_AMBIGUOUS_ROW_PAYLOAD",
			Message: "an append batch requires exactly one of JSON rows or one Arrow record batch",
		}
	}
	key, err := stampIngestRequestID(ctx, request)
	if err != nil {
		return nil, "", err
	}
	response := &ingestv1.AppendRowsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, appendRowsPath, request, response, postOptions{
		retrySafe:      true,
		idempotencyKey: key,
	})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// RunQuery runs one read-only SQL statement in the declared ClickHouse
// dialect; the synchronous-first jobs.query analog. When the acknowledgement
// reports state RUNNING, poll GetQueryResults with the returned name.
func (c *IngestClient) RunQuery(ctx context.Context, request *ingestv1.RunQueryRequest) (*ingestv1.RunQueryResponse, string, error) {
	if strings.TrimSpace(request.GetQuery()) == "" {
		return nil, "", &Error{
			Code:    "MEDALLION_INVALID_QUERY",
			Message: "query requires one SQL statement",
		}
	}
	if len(request.GetQuery()) > maxIngestQueryBytes {
		return nil, "", &Error{
			Code:    "MEDALLION_INVALID_QUERY",
			Message: fmt.Sprintf("query must not exceed %d bytes", maxIngestQueryBytes),
		}
	}
	response := &ingestv1.RunQueryResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, runQueryPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// GetQueryResults polls a running query and paginates its result rows.
func (c *IngestClient) GetQueryResults(ctx context.Context, request *ingestv1.GetQueryResultsRequest) (*ingestv1.GetQueryResultsResponse, string, error) {
	if !ingestQueryNamePattern.MatchString(request.GetName()) {
		return nil, "", &Error{
			Code:    "MEDALLION_INVALID_QUERY",
			Message: `query results require the "queries/{query}" name returned by RunQuery`,
		}
	}
	response := &ingestv1.GetQueryResultsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, getQueryResultsPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// ingestRequest is the shape shared by the three writes that carry a batch
// idempotency identifier.
type ingestRequest interface {
	GetRequestId() string
}

// stampIngestRequestID resolves the batch key for one write and copies it into
// the request's request_id when the caller left that field empty.
func stampIngestRequestID(ctx context.Context, request ingestRequest) (string, error) {
	key, err := ingestIdempotencyKeyFromContext(ctx)
	if err != nil {
		return "", err
	}
	if request.GetRequestId() != "" {
		return key, nil
	}
	switch typed := request.(type) {
	case *ingestv1.CreateTableRequest:
		typed.RequestId = key
	case *ingestv1.UpdateTableRequest:
		typed.RequestId = key
	case *ingestv1.AppendRowsRequest:
		typed.RequestId = key
	}
	return key, nil
}

func requireIngestTableName(name string) error {
	if !ingestTableNamePattern.MatchString(name) {
		return invalidIngestTableID(`table must be a resource name of the form "tables/{table}"`)
	}
	return nil
}

func requireIngestSchema(table *ingestv1.Table) error {
	if table == nil || len(table.GetSchema().GetColumns()) == 0 {
		return &Error{
			Code:    "MEDALLION_INVALID_SCHEMA",
			Message: "a table declaration requires at least one column",
		}
	}
	return nil
}

func invalidIngestTableID(message string) error {
	return &Error{
		Code:    "MEDALLION_INVALID_TABLE_ID",
		Message: message,
	}
}

func ingestIdempotencyKeyFromContext(ctx context.Context) (string, error) {
	value := ctx.Value(ingestIdempotencyKeyContextKey{})
	if value == nil {
		return randomIngestIdempotencyKey()
	}
	key, ok := value.(string)
	if !ok || key == "" || len(key) > maxIngestIdempotencyKeyBytes {
		return "", &Error{
			Code:    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
			Message: fmt.Sprintf("Idempotency-Key must be a printable ASCII string of at most %d bytes", maxIngestIdempotencyKeyBytes),
		}
	}
	for _, char := range []byte(key) {
		if char <= 0x20 || char >= 0x7f {
			return "", &Error{
				Code:    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
				Message: fmt.Sprintf("Idempotency-Key must be a printable ASCII string of at most %d bytes", maxIngestIdempotencyKeyBytes),
			}
		}
	}
	return key, nil
}

func randomIngestIdempotencyKey() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", &Error{
			Code:    "MEDALLION_IDEMPOTENCY_KEY_UNAVAILABLE",
			Message: "the process entropy source failed while generating an Idempotency-Key",
			Cause:   err,
		}
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
