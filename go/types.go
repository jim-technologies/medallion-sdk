package medallion

import (
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
	ConnectBaseURL     string
	OrganizationID     string
	TenantID           string
	DefaultConnectorID string
	HTTPClient         HTTPDoer
	Tracing            TracingConfig
}

type TracingConfig struct {
	Enabled    bool
	Tracer     trace.Tracer
	TracerName string
	SpanPrefix string
}

type Connector = connectv1.Connector
type CdcEvent = connectv1.CdcEvent
type AuditEvent = connectv1.AuditEvent
type RegisterConnectorRequest = connectv1.RegisterConnectorRequest
type RegisterConnectorResponseProto = connectv1.RegisterConnectorResponse
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

type AuditRecord struct {
	ConnectorID    string
	Actor          ActorRef
	Action         string
	Resource       ResourceRef
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
	OrganizationID    string
	ConnectorID       string
	ResourceType      string
	ResourceID        IDInput
	Action            string
	Cursor            string
	Limit             int
	PageSize          int
	Actor             *ActorRef
	IngesterPrincipal string
}

type AuditTrailResponse struct {
	RequestID  string
	NextCursor string
	Events     []AuditTrailEvent
	Proto      *connectv1.ListAuditEventsResponse
}

type AuditTrailEvent struct {
	ID                string
	EventID           string
	OrganizationID    string
	ConnectorID       string
	Actor             *ActorRef
	IngesterPrincipal string
	ActorPrincipal    string
	Action            string
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
	Payload           any
	Proto             *connectv1.AuditEvent
}

type CDCEvent struct {
	ConnectorID    string
	Source         string
	Table          string
	Operation      string
	PrimaryKey     map[string]IDInput
	EntityType     string
	EntityID       IDInput
	Actor          *ActorRef
	Before         any
	After          any
	Metadata       map[string]any
	IdempotencyKey string
	SourceEventID  IDInput
	OccurredAt     string
}

type DatasourceRegistration struct {
	OrganizationID string
	Name           string
	Type           string
	DisplayName    string
	ExternalID     IDInput
	Metadata       map[string]any
}

type Datasource struct {
	ID             string
	OrganizationID string
	Kind           string
	Type           string
	SourceSystem   string
	Name           string
	DisplayName    string
	ExternalID     string
	Status         string
	CreatedAt      string
	UpdatedAt      string
	Metadata       map[string]any
	Proto          *connectv1.Connector
}

type RegisterDatasourceResponse struct {
	RequestID  string
	Datasource Datasource
	Connector  *connectv1.Connector
	Proto      *connectv1.RegisterConnectorResponse
}
