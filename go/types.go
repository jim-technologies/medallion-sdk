package medallion

import (
	"net/http"
	"time"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	"go.opentelemetry.io/otel/trace"
)

type IDInput any

type ActorRef struct {
	Type     string
	ID       IDInput
	Provider string
}

type ResourceRef struct {
	Type string
	ID   IDInput
}

type ClientConfig struct {
	BaseURL            string
	APIKey             string
	AccessToken        string
	WorkspaceID        string
	DefaultConnectorID string
	Timeout            time.Duration
	Retry              RetryConfig
	HTTPClient         *http.Client
	Tracing            TracingConfig
}

// RetryConfig enables a small, bounded retry budget for requests that are
// intrinsically read-only or carry complete event-level idempotency keys.
// The zero value disables retries.
type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// JitterRatio is the proportional random spread applied to client
	// exponential backoff. Zero selects the safe default of 0.2 when retries
	// are enabled.
	JitterRatio float64
}

type TracingConfig struct {
	Enabled    bool
	Tracer     trace.Tracer
	TracerName string
	SpanPrefix string
}

type CdcEvent = connectv1.CdcEvent
type AuditEvent = connectv1.AuditEvent
type PublishCdcEventsRequest = connectv1.PublishCdcEventsRequest
type PublishCdcEventsResponseProto = connectv1.PublishCdcEventsResponse
type ListCdcEventsRequest = connectv1.ListCdcEventsRequest
type ListCdcEventsResponseProto = connectv1.ListCdcEventsResponse
type PublishAuditEventsRequest = connectv1.PublishAuditEventsRequest
type PublishAuditEventsResponseProto = connectv1.PublishAuditEventsResponse
type ListAuditEventsRequest = connectv1.ListAuditEventsRequest
type ListAuditEventsResponseProto = connectv1.ListAuditEventsResponse

type PublishedEventResult struct {
	IdempotencyKey string
	EventID        string
	Duplicate      bool
	Proto          *connectv1.PublishedCdcEvent
}

type EventRecordResponse struct {
	RequestID      string
	IdempotencyKey string
	Duplicate      bool
	Result         string
	AcceptedCount  uint32
	DuplicateCount uint32
	Events         []PublishedEventResult
	Proto          *connectv1.PublishCdcEventsResponse
}

type PublishedAuditEventResult struct {
	IdempotencyKey string
	EventID        string
	Duplicate      bool
	Proto          *connectv1.PublishedAuditEvent
}

type AuditOutcome string

const (
	AuditOutcomeSucceeded     AuditOutcome = "succeeded"
	AuditOutcomeFailed        AuditOutcome = "failed"
	AuditOutcomeIndeterminate AuditOutcome = "indeterminate"
)

type AuditOrigin string

const (
	AuditOriginExternalProvider AuditOrigin = "external_provider"
	AuditOriginConnect          AuditOrigin = "connect"
)

type AuditRecordResponse struct {
	RequestID      string
	IdempotencyKey string
	Duplicate      bool
	Result         string
	AcceptedCount  uint32
	DuplicateCount uint32
	Events         []PublishedAuditEventResult
	Proto          *connectv1.PublishAuditEventsResponse
}

// AuditBatchInput binds a batch of audit events to one request-level
// connector. ConnectorID overrides ClientConfig.DefaultConnectorID.
type AuditBatchInput struct {
	ConnectorID string
	Events      []AuditRecord
}

type AuditRecord struct {
	Actor   ActorRef
	Action  string
	Outcome AuditOutcome
	// ResourceType and ResourceID are the modern ingestion fields. Resource is
	// retained for legacy compatibility.
	ResourceType string
	ResourceID   IDInput
	Resource     ResourceRef
	// Payload and PayloadJSON are mutually exclusive. PayloadJSON is validated
	// and sent byte-for-byte; Payload is encoded as JSON. When both are absent,
	// modern records send {}, while legacy records synthesize the compatibility
	// actor/resource/before/after payload.
	Payload        any
	PayloadJSON    string
	Before         any
	After          any
	Metadata       map[string]any
	Description    string
	EvidenceURL    string
	IdempotencyKey string
	SourceEventID  IDInput
	OccurredAt     string
}

type AuditTrailQuery struct {
	ConnectorID       string
	ResourceType      string
	ResourceID        IDInput
	Action            string
	Cursor            string
	Limit             int
	PageSize          int
	Actor             *ActorRef
	IngesterPrincipal string
	OccurredAtFrom    string
	OccurredAtTo      string
	SourceSystem      string
	Origin            AuditOrigin
	Outcome           AuditOutcome
}

type AuditTrailResponse struct {
	RequestID  string
	NextCursor string
	Events     []AuditTrailEvent
	Proto      *connectv1.ListAuditEventsResponse
}

// CDCListQuery selects one canonical ListCdcEvents page.
type CDCListQuery struct {
	ConnectorID       string
	EntityType        string
	EntityID          IDInput
	Actor             *ActorRef
	OccurredAtFrom    string
	OccurredAtTo      string
	SourceSystem      string
	StreamName        string
	Cursor            string
	Limit             int
	PageSize          int
	IngesterPrincipal string
}

// CDCListResponse is a lossless projection of one ListCdcEvents response.
type CDCListResponse struct {
	RequestID  string
	NextCursor string
	Events     []CDCListEvent
	Proto      *connectv1.ListCdcEventsResponse
}

// CDCListEvent retains the canonical protobuf alongside a JSON projection
// whose numbers are json.Number values rather than lossy float64 values.
type CDCListEvent struct {
	ID                string
	WorkspaceID       string
	ConnectorID       string
	StreamName        string
	EntityType        string
	EntityID          string
	Operation         string
	SourceEventID     string
	IdempotencyKey    string
	Actor             *ActorRef
	ActorPrincipal    string
	OccurredAt        string
	ObservedAt        string
	Description       string
	SourceSystem      string
	IngesterPrincipal string
	Payload           any
	PayloadJSON       string
	Proto             *connectv1.CdcEvent
}

type AuditTrailEvent struct {
	ID                string
	EventID           string
	WorkspaceID       string
	ConnectorID       string
	Actor             *ActorRef
	IngesterPrincipal string
	ActorPrincipal    string
	Action            string
	Description       string
	IdempotencyKey    string
	TargetType        string
	TargetID          string
	EntityType        string
	EntityID          string
	Metadata          map[string]any
	CreatedAt         string
	OccurredAt        string
	ObservedAt        string
	Before            any
	After             any
	EvidenceURL       string
	SourceEventID     string
	SourceSystem      string
	Origin            AuditOrigin
	Outcome           AuditOutcome
	Payload           any
	PayloadJSON       string
	Proto             *connectv1.AuditEvent
}

// CDCBatchInput binds a batch of CDC events to one request-level connector.
// ConnectorID overrides ClientConfig.DefaultConnectorID.
type CDCBatchInput struct {
	ConnectorID string
	Events      []CDCEvent
}

type CDCEvent struct {
	// StreamName, EntityType, and EntityID are the modern ingestion fields.
	// Source, Table, and PrimaryKey are retained for legacy compatibility.
	StreamName string
	Source     string
	Table      string
	Operation  string
	PrimaryKey map[string]IDInput
	EntityType string
	EntityID   IDInput
	Actor      *ActorRef
	// Payload and PayloadJSON are mutually exclusive. PayloadJSON is validated
	// and sent byte-for-byte; Payload is encoded as JSON. When both are absent,
	// modern records send {}, while legacy records synthesize the compatibility
	// source/table/primary-key payload.
	Payload        any
	PayloadJSON    string
	Before         any
	After          any
	Metadata       map[string]any
	Description    string
	IdempotencyKey string
	SourceEventID  IDInput
	OccurredAt     string
}
