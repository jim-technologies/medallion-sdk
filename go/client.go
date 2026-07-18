package medallion

import (
	"context"
	"strings"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
)

const connectService = "/medallion.connect.v1.MedallionConnectService"

const (
	registerConnectorPath  = connectService + "/RegisterConnector"
	publishCdcEventsPath   = connectService + "/PublishCdcEvents"
	listCdcEventsPath      = connectService + "/ListCdcEvents"
	publishAuditEventsPath = connectService + "/PublishAuditEvents"
	listAuditEventsPath    = connectService + "/ListAuditEvents"
)

type Client struct {
	Connect     *ConnectClient
	Datasources *DatasourcesClient
	Audit       *AuditClient
	CDC         *CDCClient
}

func NewClient(cfg ClientConfig) (*Client, error) {
	requests, err := newRequestClient(cfg)
	if err != nil {
		return nil, err
	}
	org := firstNonEmpty(cfg.OrganizationID, cfg.TenantID)
	connect := &ConnectClient{requests: requests, organizationID: org}
	return &Client{
		Connect:     connect,
		Datasources: &DatasourcesClient{connect: connect},
		Audit:       &AuditClient{connect: connect, organizationID: org, defaultConnectorID: cfg.DefaultConnectorID},
		CDC:         &CDCClient{connect: connect, defaultConnectorID: cfg.DefaultConnectorID},
	}, nil
}

type ConnectClient struct {
	requests       *requestClient
	organizationID string
}

func (c *ConnectClient) RegisterConnector(ctx context.Context, request *connectv1.RegisterConnectorRequest) (*connectv1.RegisterConnectorResponse, string, error) {
	response := &connectv1.RegisterConnectorResponse{}
	envelope, err := c.requests.postProto(ctx, registerConnectorPath, request, response, request.GetIdempotencyKey())
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) PublishCdcEvents(ctx context.Context, request *connectv1.PublishCdcEventsRequest, idempotencyKey string) (*connectv1.PublishCdcEventsResponse, string, error) {
	response := &connectv1.PublishCdcEventsResponse{}
	envelope, err := c.requests.postProto(ctx, publishCdcEventsPath, request, response, idempotencyKey)
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) ListCdcEvents(ctx context.Context, request *connectv1.ListCdcEventsRequest) (*connectv1.ListCdcEventsResponse, string, error) {
	response := &connectv1.ListCdcEventsResponse{}
	envelope, err := c.requests.postProto(ctx, listCdcEventsPath, request, response, "")
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) PublishAuditEvents(ctx context.Context, request *connectv1.PublishAuditEventsRequest, idempotencyKey string) (*connectv1.PublishAuditEventsResponse, string, error) {
	response := &connectv1.PublishAuditEventsResponse{}
	envelope, err := c.requests.postProto(ctx, publishAuditEventsPath, request, response, idempotencyKey)
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) ListAuditEvents(ctx context.Context, request *connectv1.ListAuditEventsRequest) (*connectv1.ListAuditEventsResponse, string, error) {
	response := &connectv1.ListAuditEventsResponse{}
	envelope, err := c.requests.postProto(ctx, listAuditEventsPath, request, response, "")
	if err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) RegisterDatasource(ctx context.Context, input DatasourceRegistration) (RegisterDatasourceResponse, error) {
	org := firstNonEmpty(input.OrganizationID, c.organizationID)
	if strings.TrimSpace(org) == "" {
		return RegisterDatasourceResponse{}, missingOption("MEDALLION_MISSING_ORGANIZATION_ID", "organization ID is required to register a datasource")
	}
	idempotencyKey, err := requiredControlIdempotencyKey(input.IdempotencyKey, "datasource.idempotencyKey")
	if err != nil {
		return RegisterDatasourceResponse{}, err
	}
	displayName := input.DisplayName
	if displayName == "" {
		displayName = input.Name
	}
	externalID, err := normalizeOptionalID(input.ExternalID, "datasource.externalId")
	if err != nil {
		return RegisterDatasourceResponse{}, err
	}
	protoResponse, requestID, err := c.RegisterConnector(ctx, &connectv1.RegisterConnectorRequest{
		OrganizationId: org,
		Kind:           input.Type,
		SourceSystem:   input.Name,
		DisplayName:    displayName,
		ExternalId:     externalID,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return RegisterDatasourceResponse{}, err
	}
	connector := protoResponse.GetConnector()
	if connector == nil || strings.TrimSpace(connector.GetId()) == "" {
		return RegisterDatasourceResponse{}, &Error{
			Code:      "MEDALLION_INVALID_DATASOURCE_RESPONSE",
			RequestID: requestID,
			Message:   "Medallion returned a datasource registration without a connector ID",
		}
	}
	datasource := datasourceFromConnector(connector, input.Metadata)
	return RegisterDatasourceResponse{
		RequestID:  requestID,
		Datasource: datasource,
		Connector:  connector,
		Proto:      protoResponse,
	}, nil
}

type DatasourcesClient struct {
	connect *ConnectClient
}

func (c *DatasourcesClient) Register(ctx context.Context, input DatasourceRegistration) (RegisterDatasourceResponse, error) {
	return c.connect.RegisterDatasource(ctx, input)
}

type AuditClient struct {
	connect            *ConnectClient
	organizationID     string
	defaultConnectorID string
}

func (c *AuditClient) Record(ctx context.Context, input AuditRecord) (AuditRecordResponse, error) {
	connectorID := firstNonEmpty(input.ConnectorID, c.defaultConnectorID)
	if strings.TrimSpace(connectorID) == "" {
		return AuditRecordResponse{}, missingOption("MEDALLION_MISSING_CONNECTOR_ID", "connector ID is required to record an audit event")
	}
	actor, err := normalizeActor(input.Actor)
	if err != nil {
		return AuditRecordResponse{}, err
	}
	resource, err := normalizeResource(input.Resource)
	if err != nil {
		return AuditRecordResponse{}, err
	}
	payload, err := jsonString(map[string]any{
		"actor":       actor,
		"resource":    resource,
		"before":      input.Before,
		"after":       input.After,
		"metadata":    input.Metadata,
		"evidenceUrl": nullableString(input.EvidenceURL),
	})
	if err != nil {
		return AuditRecordResponse{}, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, "audit.sourceEventId")
	if err != nil {
		return AuditRecordResponse{}, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, "audit.occurredAt")
	if err != nil {
		return AuditRecordResponse{}, err
	}
	outcome, err := auditOutcomeProto(input.Outcome, false)
	if err != nil {
		return AuditRecordResponse{}, err
	}
	key, err := requiredEventIdempotencyKey(input.IdempotencyKey, "audit.idempotencyKey")
	if err != nil {
		return AuditRecordResponse{}, err
	}
	event := &connectv1.AuditEvent{
		ResourceType:   resource["type"],
		ResourceId:     resource["id"],
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipalFromRef(actor),
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
		Description:    input.Description,
		Action:         input.Action,
		SourceEventId:  sourceEventID,
		Outcome:        outcome,
	}
	response, requestID, err := c.connect.PublishAuditEvents(ctx, &connectv1.PublishAuditEventsRequest{
		ConnectorId: connectorID,
		Events:      []*connectv1.AuditEvent{event},
	}, key)
	if err != nil {
		return AuditRecordResponse{}, err
	}
	return auditEventResponse(response, key, requestID)
}

func (c *AuditClient) Trail(ctx context.Context, input AuditTrailQuery) (AuditTrailResponse, error) {
	org := firstNonEmpty(input.OrganizationID, c.organizationID)
	if strings.TrimSpace(org) == "" {
		return AuditTrailResponse{}, missingOption("MEDALLION_MISSING_ORGANIZATION_ID", "organization ID is required to read an audit trail")
	}
	resourceType := strings.TrimSpace(input.ResourceType)
	if resourceType == "" {
		return AuditTrailResponse{}, missingOption("MEDALLION_MISSING_RESOURCE_TYPE", "resource type is required to read an audit trail")
	}
	resourceID, err := normalizeID(input.ResourceID, "audit.resourceId")
	if err != nil {
		return AuditTrailResponse{}, err
	}
	limit, err := auditTrailLimit(input.Limit, input.PageSize)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	var sourceActor map[string]string
	if input.Actor != nil {
		sourceActor, err = normalizeActor(*input.Actor)
		if err != nil {
			return AuditTrailResponse{}, err
		}
	}
	request := &connectv1.ListAuditEventsRequest{
		OrganizationId:      org,
		ConnectorId:         firstNonEmpty(input.ConnectorID, c.defaultConnectorID),
		ResourceType:        resourceType,
		ResourceId:          resourceID,
		Limit:               limit,
		Action:              input.Action,
		PageCursor:          input.Cursor,
		IngestedByPrincipal: strings.TrimSpace(input.IngesterPrincipal),
	}
	request.Origin, err = auditOriginProto(input.Origin)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	request.Outcome, err = auditOutcomeProto(input.Outcome, true)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	if sourceActor != nil {
		request.ActorPrincipal = actorPrincipalFromRef(sourceActor)
	}
	protoResponse, requestID, err := c.connect.ListAuditEvents(ctx, request)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	events := make([]AuditTrailEvent, 0, len(protoResponse.GetEvents()))
	for _, item := range protoResponse.GetEvents() {
		event := auditEventFromConnect(item)
		if sourceActor == nil || sameActor(event.Actor, sourceActor) {
			events = append(events, event)
		}
	}
	return AuditTrailResponse{RequestID: requestID, NextCursor: protoResponse.GetNextPageCursor(), Events: events, Proto: protoResponse}, nil
}

type CDCClient struct {
	connect            *ConnectClient
	defaultConnectorID string
}

func (c *CDCClient) Record(ctx context.Context, input CDCEvent) (EventRecordResponse, error) {
	connectorID := firstNonEmpty(input.ConnectorID, c.defaultConnectorID)
	if strings.TrimSpace(connectorID) == "" {
		return EventRecordResponse{}, missingOption("MEDALLION_MISSING_CONNECTOR_ID", "connector ID is required to record a CDC event")
	}
	primaryKey, err := normalizeIDRecord(input.PrimaryKey, "primaryKey")
	if err != nil {
		return EventRecordResponse{}, err
	}
	if len(primaryKey) == 0 {
		return EventRecordResponse{}, &Error{Code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY", Message: "cdc.primaryKey must contain at least one field"}
	}
	if len(primaryKey) > 1 && input.EntityID == nil {
		return EventRecordResponse{}, &Error{Code: "MEDALLION_MISSING_CDC_ENTITY_ID", Message: "cdc.entityId is required when cdc.primaryKey contains more than one field"}
	}
	entityID := ""
	if input.EntityID != nil {
		entityID, err = normalizeID(input.EntityID, "cdc.entityId")
		if err != nil {
			return EventRecordResponse{}, err
		}
	} else {
		entityID = entityIDFromPrimaryKey(primaryKey)
	}
	var actor map[string]string
	var actorPrincipal string
	if input.Actor != nil {
		actor, err = normalizeActor(*input.Actor)
		if err != nil {
			return EventRecordResponse{}, err
		}
		actorPrincipal = actorPrincipalFromRef(actor)
	}
	payload, err := jsonString(map[string]any{
		"source":     input.Source,
		"table":      input.Table,
		"actor":      actor,
		"primaryKey": primaryKey,
		"before":     input.Before,
		"after":      input.After,
		"metadata":   input.Metadata,
	})
	if err != nil {
		return EventRecordResponse{}, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, "cdc.sourceEventId")
	if err != nil {
		return EventRecordResponse{}, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, "cdc.occurredAt")
	if err != nil {
		return EventRecordResponse{}, err
	}
	key, err := requiredEventIdempotencyKey(input.IdempotencyKey, "cdc.idempotencyKey")
	if err != nil {
		return EventRecordResponse{}, err
	}
	event := &connectv1.CdcEvent{
		StreamName:     input.Table,
		EntityType:     firstNonEmpty(input.EntityType, input.Table),
		EntityId:       entityID,
		Operation:      cdcOperation(input.Operation),
		SourceEventId:  sourceEventID,
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipal,
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
	}
	if event.Operation == connectv1.CdcOperation_CDC_OPERATION_UNSPECIFIED {
		return EventRecordResponse{}, &Error{Code: "MEDALLION_INVALID_CDC_OPERATION", Message: "operation must be insert, update, delete, or snapshot"}
	}
	response, requestID, err := c.connect.PublishCdcEvents(ctx, &connectv1.PublishCdcEventsRequest{
		ConnectorId: connectorID,
		Events:      []*connectv1.CdcEvent{event},
	}, key)
	if err != nil {
		return EventRecordResponse{}, err
	}
	return cdcEventResponse(response, key, requestID)
}
