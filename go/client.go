package medallion

import (
	"context"
	"strings"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
)

const connectService = "/medallion.connect.v1.MedallionConnectService"

const (
	registerConnectorPath = connectService + "/RegisterConnector"
	publishCdcEventsPath  = connectService + "/PublishCdcEvents"
	listCdcEventsPath     = connectService + "/ListCdcEvents"
)

type Client struct {
	Connect     *ConnectClient
	Datasources *DatasourcesClient
	Audit       *AuditClient
	Events      *EventsClient
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
		Events:      &EventsClient{connect: connect, defaultConnectorID: cfg.DefaultConnectorID},
		CDC:         &CDCClient{connect: connect, defaultConnectorID: cfg.DefaultConnectorID},
	}, nil
}

type ConnectClient struct {
	requests       *requestClient
	organizationID string
}

func (c *ConnectClient) RegisterConnector(ctx context.Context, request *connectv1.RegisterConnectorRequest) (*connectv1.RegisterConnectorResponse, string, error) {
	response := &connectv1.RegisterConnectorResponse{}
	envelope, err := c.requests.postProto(ctx, registerConnectorPath, request, response, "")
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

func (c *ConnectClient) RegisterDatasource(ctx context.Context, input DatasourceRegistration) (RegisterDatasourceResponse, error) {
	org := firstNonEmpty(input.OrganizationID, c.organizationID)
	if strings.TrimSpace(org) == "" {
		return RegisterDatasourceResponse{}, missingOption("MEDALLION_MISSING_ORGANIZATION_ID", "organization ID is required to register a datasource")
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
	})
	if err != nil {
		return RegisterDatasourceResponse{}, err
	}
	connector := protoResponse.GetConnector()
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

func (c *AuditClient) Record(ctx context.Context, input AuditRecord) (EventRecordResponse, error) {
	connectorID := firstNonEmpty(input.ConnectorID, c.defaultConnectorID)
	if strings.TrimSpace(connectorID) == "" {
		return EventRecordResponse{}, missingOption("MEDALLION_MISSING_CONNECTOR_ID", "connector ID is required to record an audit event")
	}
	actor, err := normalizeActor(input.Actor)
	if err != nil {
		return EventRecordResponse{}, err
	}
	resource, err := normalizeResource(input.Resource)
	if err != nil {
		return EventRecordResponse{}, err
	}
	payload, err := jsonString(map[string]any{
		"actor":    actor,
		"resource": resource,
		"before":   input.Before,
		"after":    input.After,
		"metadata": input.Metadata,
	})
	if err != nil {
		return EventRecordResponse{}, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, "audit.sourceEventId")
	if err != nil {
		return EventRecordResponse{}, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, "audit.occurredAt")
	if err != nil {
		return EventRecordResponse{}, err
	}
	key := idempotencyKey(input.IdempotencyKey)
	event := &connectv1.CdcEvent{
		StreamName:     firstNonEmpty(input.StreamName, "audit_log"),
		EntityType:     resource["type"],
		EntityId:       resource["id"],
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipalFromRef(actor),
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
		Description:    input.Description,
		Kind:           connectv1.EventKind_EVENT_KIND_AUDIT,
		Action:         input.Action,
		SourceEventId:  sourceEventID,
	}
	response, requestID, err := c.connect.PublishCdcEvents(ctx, &connectv1.PublishCdcEventsRequest{
		ConnectorId: connectorID,
		Events:      []*connectv1.CdcEvent{event},
	}, key)
	if err != nil {
		return EventRecordResponse{}, err
	}
	return eventResponse(response, key, requestID), nil
}

func (c *AuditClient) Trail(ctx context.Context, input AuditTrailQuery) (AuditTrailResponse, error) {
	org := firstNonEmpty(input.OrganizationID, c.organizationID)
	if strings.TrimSpace(org) == "" {
		return AuditTrailResponse{}, missingOption("MEDALLION_MISSING_ORGANIZATION_ID", "organization ID is required to read an audit trail")
	}
	resourceID, err := normalizeID(input.ResourceID, "audit.resourceId")
	if err != nil {
		return AuditTrailResponse{}, err
	}
	limit := input.Limit
	if limit == 0 {
		limit = input.PageSize
	}
	var sourceActor map[string]string
	if input.Actor != nil {
		sourceActor, err = normalizeActor(*input.Actor)
		if err != nil {
			return AuditTrailResponse{}, err
		}
	}
	request := &connectv1.ListCdcEventsRequest{
		OrganizationId:      org,
		ConnectorId:         firstNonEmpty(input.ConnectorID, c.defaultConnectorID),
		EntityType:          input.ResourceType,
		EntityId:            resourceID,
		Limit:               uint32(limit),
		Kind:                connectv1.EventKind_EVENT_KIND_AUDIT,
		Action:              input.Action,
		PageCursor:          input.Cursor,
		IngestedByPrincipal: strings.TrimSpace(input.IngesterPrincipal),
	}
	if sourceActor != nil {
		request.ActorPrincipal = actorPrincipalFromRef(sourceActor)
	}
	protoResponse, requestID, err := c.connect.ListCdcEvents(ctx, request)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	events := make([]AuditTrailEvent, 0, len(protoResponse.GetEvents()))
	for _, item := range protoResponse.GetEvents() {
		if !isAuditEvent(item) {
			continue
		}
		event := auditEventFromConnect(item)
		if sourceActor == nil || sameActor(event.Actor, sourceActor) {
			events = append(events, event)
		}
	}
	return AuditTrailResponse{RequestID: requestID, NextCursor: protoResponse.GetNextPageCursor(), Events: events, Proto: protoResponse}, nil
}

type EventsClient struct {
	connect            *ConnectClient
	defaultConnectorID string
}

func (c *EventsClient) Record(ctx context.Context, input GenericEvent) (EventRecordResponse, error) {
	connectorID := firstNonEmpty(input.ConnectorID, c.defaultConnectorID)
	if strings.TrimSpace(connectorID) == "" {
		return EventRecordResponse{}, missingOption("MEDALLION_MISSING_CONNECTOR_ID", "connector ID is required to record an event")
	}
	key := idempotencyKey(input.IdempotencyKey)
	var actor map[string]string
	var actorPrincipal string
	var err error
	if input.Actor != nil {
		actor, err = normalizeActor(*input.Actor)
		if err != nil {
			return EventRecordResponse{}, err
		}
		actorPrincipal = actorPrincipalFromRef(actor)
	}
	resource := map[string]string{"type": "event", "id": firstNonEmpty(input.IdempotencyKey, input.Type)}
	if input.Resource != nil {
		resource, err = normalizeResource(*input.Resource)
		if err != nil {
			return EventRecordResponse{}, err
		}
	}
	payload, err := jsonString(map[string]any{
		"type":     input.Type,
		"actor":    actor,
		"resource": resource,
		"payload":  input.Payload,
		"metadata": input.Metadata,
	})
	if err != nil {
		return EventRecordResponse{}, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, "event.sourceEventId")
	if err != nil {
		return EventRecordResponse{}, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, "event.occurredAt")
	if err != nil {
		return EventRecordResponse{}, err
	}
	event := &connectv1.CdcEvent{
		StreamName:     firstNonEmpty(input.StreamName, "events"),
		EntityType:     resource["type"],
		EntityId:       resource["id"],
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipal,
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
		SourceEventId:  sourceEventID,
		Kind:           connectv1.EventKind_EVENT_KIND_AUDIT,
		Action:         input.Type,
	}
	response, requestID, err := c.connect.PublishCdcEvents(ctx, &connectv1.PublishCdcEventsRequest{
		ConnectorId: connectorID,
		Events:      []*connectv1.CdcEvent{event},
	}, key)
	if err != nil {
		return EventRecordResponse{}, err
	}
	return eventResponse(response, key, requestID), nil
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
	key := idempotencyKey(input.IdempotencyKey)
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
		Kind:           connectv1.EventKind_EVENT_KIND_CDC,
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
	return eventResponse(response, key, requestID), nil
}
