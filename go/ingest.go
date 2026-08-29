package medallion

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
)

const ingestService = "/medallion.ingest.v1.MedallionIngestService"

const (
	appendPath          = ingestService + "/Append"
	queryPath           = ingestService + "/Query"
	getQueryResultsPath = ingestService + "/GetQueryResults"
	createDatasetPath   = ingestService + "/CreateDataset"
	getDatasetPath      = ingestService + "/GetDataset"
	listDatasetsPath    = ingestService + "/ListDatasets"
)

const maxIngestIdempotencyKeyBytes = 255

type ingestIdempotencyKeyContextKey struct{}

// WithIngestIdempotencyKey pins the Stripe-style Idempotency-Key header for
// the Append or CreateDataset call made with the returned context, so an
// exact replay of the same batch is deduplicated server-side. Without it the
// SDK generates a fresh key per call.
func WithIngestIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ingestIdempotencyKeyContextKey{}, key)
}

// IngestClient is the thin generated-binding client for the six
// medallion.ingest.v1 tabular ingestion and query RPCs. It passes protobuf
// requests through with header-based workspace identity and idempotency;
// the richer convenience layer ships Python-first.
type IngestClient struct {
	requests *requestClient
}

// Append appends one batch of JSON rows or one Arrow IPC record batch to a
// dataset; the insertAll analog. Every call carries an Idempotency-Key
// header for whole-batch replay protection.
func (c *IngestClient) Append(ctx context.Context, request *ingestv1.AppendRequest) (*ingestv1.AppendResponse, string, error) {
	if err := requireIngestDatasetID(request.GetDatasetId()); err != nil {
		return nil, "", err
	}
	hasJSON := request.GetJsonRows() != nil
	hasArrow := request.GetArrowRows() != nil
	if hasJSON == hasArrow {
		return nil, "", &Error{
			Code:    "MEDALLION_AMBIGUOUS_ROW_PAYLOAD",
			Message: "an append batch requires exactly one of JSON rows or one Arrow record batch",
		}
	}
	key, err := ingestIdempotencyKeyFromContext(ctx)
	if err != nil {
		return nil, "", err
	}
	response := &ingestv1.AppendResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, appendPath, request, response, postOptions{
		retrySafe:      true,
		idempotencyKey: key,
	})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// Query runs one read-only SQL statement in the declared ClickHouse dialect;
// the synchronous-first jobs.query analog. When the acknowledgement reports
// completed=false, poll GetQueryResults with the returned query_id.
func (c *IngestClient) Query(ctx context.Context, request *ingestv1.QueryRequest) (*ingestv1.QueryResponse, string, error) {
	if strings.TrimSpace(request.GetQuery()) == "" {
		return nil, "", &Error{
			Code:    "MEDALLION_INVALID_QUERY",
			Message: "query requires one SQL statement",
		}
	}
	response := &ingestv1.QueryResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, queryPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// GetQueryResults polls a running query and paginates its result rows.
func (c *IngestClient) GetQueryResults(ctx context.Context, request *ingestv1.GetQueryResultsRequest) (*ingestv1.GetQueryResultsResponse, string, error) {
	if request.GetQueryId() == "" {
		return nil, "", &Error{
			Code:    "MEDALLION_INVALID_QUERY",
			Message: "query results require the query_id returned by Query",
		}
	}
	response := &ingestv1.GetQueryResultsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, getQueryResultsPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// CreateDataset creates one dataset; replay-safe through the Idempotency-Key
// header exactly like Append.
func (c *IngestClient) CreateDataset(ctx context.Context, request *ingestv1.CreateDatasetRequest) (*ingestv1.CreateDatasetResponse, string, error) {
	if err := requireIngestDatasetID(request.GetDatasetId()); err != nil {
		return nil, "", err
	}
	key, err := ingestIdempotencyKeyFromContext(ctx)
	if err != nil {
		return nil, "", err
	}
	response := &ingestv1.CreateDatasetResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, createDatasetPath, request, response, postOptions{
		retrySafe:      true,
		idempotencyKey: key,
	})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// GetDataset reads one dataset by identifier.
func (c *IngestClient) GetDataset(ctx context.Context, request *ingestv1.GetDatasetRequest) (*ingestv1.GetDatasetResponse, string, error) {
	if err := requireIngestDatasetID(request.GetDatasetId()); err != nil {
		return nil, "", err
	}
	response := &ingestv1.GetDatasetResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, getDatasetPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

// ListDatasets pages through the datasets of the selected workspace.
func (c *IngestClient) ListDatasets(ctx context.Context, request *ingestv1.ListDatasetsRequest) (*ingestv1.ListDatasetsResponse, string, error) {
	response := &ingestv1.ListDatasetsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, listDatasetsPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func requireIngestDatasetID(datasetID string) error {
	if strings.TrimSpace(datasetID) == "" {
		return &Error{
			Code:    "MEDALLION_INVALID_DATASET_ID",
			Message: "dataset_id is required",
		}
	}
	if len(datasetID) > 256 {
		return &Error{
			Code:    "MEDALLION_INVALID_DATASET_ID",
			Message: "dataset_id must not exceed 256 bytes",
		}
	}
	return nil
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
