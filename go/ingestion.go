package medallion

import (
	"context"
	"fmt"
	"strings"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
)

const (
	maxPublishBatchSize = 1000
	maxIteratorPages    = 10000
)

func (c *ConnectClient) preparePublishCdcEventsRequest(request *connectv1.PublishCdcEventsRequest) (*connectv1.PublishCdcEventsRequest, error) {
	if request == nil {
		return nil, &Error{Code: "MEDALLION_INVALID_PUBLISH_REQUEST", Message: "CDC publish request is required"}
	}
	out := cloneMessage(request)
	out.ConnectorId = firstNonBlank(out.GetConnectorId(), c.defaultConnectorID)
	if out.GetConnectorId() == "" {
		return nil, missingOption("MEDALLION_MISSING_CONNECTOR_ID", "connector ID is required to publish CDC events")
	}
	if err := validateMaxBytes(out.GetConnectorId(), "cdc.connectorId", 128); err != nil {
		return nil, err
	}
	if err := validateBatchSize(len(out.GetEvents()), "CDC"); err != nil {
		return nil, err
	}
	keys := make(map[string]struct{}, len(out.GetEvents()))
	for index, event := range out.GetEvents() {
		path := fmt.Sprintf("cdc.events[%d]", index)
		if event == nil {
			return nil, &Error{Code: "MEDALLION_INVALID_CDC_EVENT", Message: path + " is required"}
		}
		if event.GetId() != 0 || event.GetWorkspaceId() != "" || event.GetConnectorId() != "" || event.GetObservedAt() != nil || event.GetSourceSystem() != "" || event.GetIngestedByPrincipal() != "" {
			return nil, &Error{Code: "MEDALLION_SERVER_DERIVED_FIELD", Message: path + " contains a server-derived field"}
		}
		if strings.TrimSpace(event.GetStreamName()) == "" || strings.TrimSpace(event.GetEntityType()) == "" || strings.TrimSpace(event.GetEntityId()) == "" {
			return nil, &Error{Code: "MEDALLION_INVALID_CDC_EVENT", Message: path + " requires stream name, entity type, and entity ID"}
		}
		for _, field := range []struct {
			value string
			name  string
			max   int
		}{
			{event.GetStreamName(), ".streamName", 256},
			{event.GetEntityType(), ".entityType", 256},
			{event.GetEntityId(), ".entityId", 1024},
			{event.GetSourceEventId(), ".sourceEventId", 1024},
			{event.GetActorPrincipal(), ".actorPrincipal", 512},
			{event.GetDescription(), ".description", 4096},
		} {
			if err := validateMaxBytes(field.value, path+field.name, field.max); err != nil {
				return nil, err
			}
		}
		switch event.GetOperation() {
		case connectv1.CdcOperation_CDC_OPERATION_INSERT,
			connectv1.CdcOperation_CDC_OPERATION_UPDATE,
			connectv1.CdcOperation_CDC_OPERATION_DELETE,
			connectv1.CdcOperation_CDC_OPERATION_SNAPSHOT:
		default:
			return nil, &Error{Code: "MEDALLION_INVALID_CDC_OPERATION", Message: path + ".operation must be insert, update, delete, or snapshot"}
		}
		key, err := requiredEventIdempotencyKey(event.GetIdempotencyKey(), path+".idempotencyKey")
		if err != nil {
			return nil, err
		}
		event.IdempotencyKey = key
		if _, exists := keys[key]; exists {
			return nil, &Error{Code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY", Message: path + " repeats an idempotency key in the batch"}
		}
		keys[key] = struct{}{}
		if _, err := parsePayloadStrict(event.GetPayloadJson(), path+".payloadJson"); err != nil {
			return nil, err
		}
		if err := validateTimestamp(event.GetOccurredAt(), path+".occurredAt"); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *ConnectClient) preparePublishAuditEventsRequest(request *connectv1.PublishAuditEventsRequest) (*connectv1.PublishAuditEventsRequest, error) {
	if request == nil {
		return nil, &Error{Code: "MEDALLION_INVALID_PUBLISH_REQUEST", Message: "audit publish request is required"}
	}
	out := cloneMessage(request)
	out.ConnectorId = firstNonBlank(out.GetConnectorId(), c.defaultConnectorID)
	if out.GetConnectorId() == "" {
		return nil, missingOption("MEDALLION_MISSING_CONNECTOR_ID", "connector ID is required to publish audit events")
	}
	if err := validateMaxBytes(out.GetConnectorId(), "audit.connectorId", 128); err != nil {
		return nil, err
	}
	if err := validateBatchSize(len(out.GetEvents()), "audit"); err != nil {
		return nil, err
	}
	keys := make(map[string]struct{}, len(out.GetEvents()))
	for index, event := range out.GetEvents() {
		path := fmt.Sprintf("audit.events[%d]", index)
		if event == nil {
			return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_EVENT", Message: path + " is required"}
		}
		if event.GetId() != 0 || event.GetWorkspaceId() != "" || event.GetConnectorId() != "" || event.GetObservedAt() != nil || event.GetSourceSystem() != "" || event.GetIngestedByPrincipal() != "" || event.GetOrigin() != connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_UNSPECIFIED {
			return nil, &Error{Code: "MEDALLION_SERVER_DERIVED_FIELD", Message: path + " contains a server-derived field"}
		}
		if strings.TrimSpace(event.GetResourceType()) == "" || strings.TrimSpace(event.GetResourceId()) == "" || strings.TrimSpace(event.GetAction()) == "" {
			return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_EVENT", Message: path + " requires resource type, resource ID, and action"}
		}
		for _, field := range []struct {
			value string
			name  string
			max   int
		}{
			{event.GetResourceType(), ".resourceType", 256},
			{event.GetResourceId(), ".resourceId", 1024},
			{event.GetAction(), ".action", 256},
			{event.GetSourceEventId(), ".sourceEventId", 1024},
			{event.GetActorPrincipal(), ".actorPrincipal", 512},
			{event.GetDescription(), ".description", 4096},
		} {
			if err := validateMaxBytes(field.value, path+field.name, field.max); err != nil {
				return nil, err
			}
		}
		switch event.GetOutcome() {
		case connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED,
			connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_FAILED,
			connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_INDETERMINATE:
		default:
			return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_OUTCOME", Message: path + ".outcome must be succeeded, failed, or indeterminate"}
		}
		key, err := requiredEventIdempotencyKey(event.GetIdempotencyKey(), path+".idempotencyKey")
		if err != nil {
			return nil, err
		}
		event.IdempotencyKey = key
		if _, exists := keys[key]; exists {
			return nil, &Error{Code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY", Message: path + " repeats an idempotency key in the batch"}
		}
		keys[key] = struct{}{}
		if _, err := parsePayloadStrict(event.GetPayloadJson(), path+".payloadJson"); err != nil {
			return nil, err
		}
		if err := validateTimestamp(event.GetOccurredAt(), path+".occurredAt"); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *ConnectClient) prepareListCdcEventsRequest(request *connectv1.ListCdcEventsRequest) (*connectv1.ListCdcEventsRequest, error) {
	if request == nil {
		return nil, &Error{Code: "MEDALLION_INVALID_LIST_REQUEST", Message: "CDC list request is required"}
	}
	out := cloneMessage(request)
	if err := c.bindListWorkspace(out.GetWorkspaceId(), "CDC"); err != nil {
		return nil, err
	}
	out.WorkspaceId = c.requests.workspaceID
	out.ConnectorId = firstNonBlank(out.GetConnectorId(), c.defaultConnectorID)
	for _, field := range []struct {
		value string
		name  string
		max   int
	}{
		{out.GetConnectorId(), "cdc.list.connectorId", 128},
		{out.GetEntityType(), "cdc.list.entityType", 256},
		{out.GetEntityId(), "cdc.list.entityId", 1024},
		{out.GetActorPrincipal(), "cdc.list.actorPrincipal", 512},
		{out.GetSourceSystem(), "cdc.list.sourceSystem", 256},
		{out.GetStreamName(), "cdc.list.streamName", 256},
		{out.GetPageCursor(), "cdc.list.pageCursor", 2048},
		{out.GetIngestedByPrincipal(), "cdc.list.ingestedByPrincipal", 512},
	} {
		if err := validateMaxBytes(field.value, field.name, field.max); err != nil {
			return nil, err
		}
	}
	if out.GetLimit() > maxAuditTrailLimit {
		return nil, &Error{Code: "MEDALLION_LIST_LIMIT_TOO_LARGE", Message: "CDC list limit must be 500 or less"}
	}
	if err := validateTimestampBounds(out.GetOccurredAtFrom(), out.GetOccurredAtTo(), "cdc.list"); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *ConnectClient) prepareListAuditEventsRequest(request *connectv1.ListAuditEventsRequest) (*connectv1.ListAuditEventsRequest, error) {
	if request == nil {
		return nil, &Error{Code: "MEDALLION_INVALID_LIST_REQUEST", Message: "audit list request is required"}
	}
	out := cloneMessage(request)
	if err := c.bindListWorkspace(out.GetWorkspaceId(), "audit"); err != nil {
		return nil, err
	}
	out.WorkspaceId = c.requests.workspaceID
	out.ConnectorId = firstNonBlank(out.GetConnectorId(), c.defaultConnectorID)
	for _, field := range []struct {
		value string
		name  string
		max   int
	}{
		{out.GetConnectorId(), "audit.list.connectorId", 128},
		{out.GetResourceType(), "audit.list.resourceType", 256},
		{out.GetResourceId(), "audit.list.resourceId", 1024},
		{out.GetActorPrincipal(), "audit.list.actorPrincipal", 512},
		{out.GetAction(), "audit.list.action", 256},
		{out.GetSourceSystem(), "audit.list.sourceSystem", 256},
		{out.GetPageCursor(), "audit.list.pageCursor", 2048},
		{out.GetIngestedByPrincipal(), "audit.list.ingestedByPrincipal", 512},
	} {
		if err := validateMaxBytes(field.value, field.name, field.max); err != nil {
			return nil, err
		}
	}
	if (out.GetResourceType() == "") != (out.GetResourceId() == "") {
		return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_RESOURCE_FILTER", Message: "audit resource type and resource ID must be provided together"}
	}
	if out.GetLimit() > maxAuditTrailLimit {
		return nil, &Error{Code: "MEDALLION_LIST_LIMIT_TOO_LARGE", Message: "audit list limit must be 500 or less"}
	}
	if err := validateTimestampBounds(out.GetOccurredAtFrom(), out.GetOccurredAtTo(), "audit.list"); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *ConnectClient) bindListWorkspace(provided, family string) error {
	if provided != "" && provided != c.requests.workspaceID {
		return &Error{
			Code:    "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
			Message: family + " list workspace ID conflicts with this client's immutable workspace",
		}
	}
	return nil
}

func validateBatchSize(size int, kind string) error {
	if size == 0 {
		return &Error{Code: "MEDALLION_EMPTY_EVENT_BATCH", Message: kind + " publish batch must contain at least one event"}
	}
	if size > maxPublishBatchSize {
		return &Error{Code: "MEDALLION_EVENT_BATCH_TOO_LARGE", Message: kind + " publish batch must contain 1000 events or fewer"}
	}
	return nil
}

func validateMaxBytes(value, path string, maximum int) error {
	if len(value) > maximum {
		return &Error{Code: "MEDALLION_FIELD_TOO_LONG", Message: fmt.Sprintf("%s must not exceed %d bytes", path, maximum)}
	}
	return nil
}

func cdcEventKeys(events []*connectv1.CdcEvent) []string {
	keys := make([]string, len(events))
	for index, event := range events {
		keys[index] = event.GetIdempotencyKey()
	}
	return keys
}

func auditEventKeys(events []*connectv1.AuditEvent) []string {
	keys := make([]string, len(events))
	for index, event := range events {
		keys[index] = event.GetIdempotencyKey()
	}
	return keys
}

// RecordBatch publishes between one and 1000 audit events using
// ClientConfig.DefaultConnectorID.
func (c *AuditClient) RecordBatch(ctx context.Context, inputs []AuditRecord) (AuditRecordResponse, error) {
	return c.PublishBatch(ctx, AuditBatchInput{Events: inputs})
}

// PublishBatch publishes between one and 1000 audit events in one canonical
// RPC. Connector scope belongs to the request, never to a nested event.
func (c *AuditClient) PublishBatch(ctx context.Context, input AuditBatchInput) (AuditRecordResponse, error) {
	if err := validateBatchSize(len(input.Events), "audit"); err != nil {
		return AuditRecordResponse{}, err
	}
	events := make([]*connectv1.AuditEvent, 0, len(input.Events))
	keys := make([]string, 0, len(input.Events))
	for index, record := range input.Events {
		event, err := auditRecordProto(record, index)
		if err != nil {
			return AuditRecordResponse{}, err
		}
		events = append(events, event)
		keys = append(keys, event.GetIdempotencyKey())
	}
	response, requestID, err := c.connect.PublishAuditEvents(ctx, &connectv1.PublishAuditEventsRequest{
		ConnectorId: firstNonBlank(input.ConnectorID, c.defaultConnectorID),
		Events:      events,
	})
	if err != nil {
		return AuditRecordResponse{}, err
	}
	return auditEventResponse(response, keys, requestID)
}

func auditRecordProto(input AuditRecord, index int) (*connectv1.AuditEvent, error) {
	if auditUsesModernInput(input) {
		return modernAuditRecordProto(input, index)
	}
	return legacyAuditRecordProto(input, index)
}

func auditUsesModernInput(input AuditRecord) bool {
	return input.ResourceType != "" || input.ResourceID != nil
}

func modernAuditRecordProto(input AuditRecord, index int) (*connectv1.AuditEvent, error) {
	path := fmt.Sprintf("audit.records[%d]", index)
	if input.Resource.Type != "" || input.Resource.ID != nil || input.Before != nil || input.After != nil || input.Metadata != nil || input.EvidenceURL != "" {
		return nil, &Error{Code: "MEDALLION_AMBIGUOUS_EVENT_INPUT", Message: path + " mixes modern resource/payload fields with legacy audit fields"}
	}
	var actor map[string]string
	var actorPrincipal string
	if input.Actor.ID != nil || input.Actor.Type != "" || input.Actor.Provider != "" {
		var err error
		actor, err = normalizeActor(input.Actor)
		if err != nil {
			return nil, err
		}
		actorPrincipal = actorPrincipalFromRef(actor)
	}
	resourceType := input.ResourceType
	if strings.TrimSpace(resourceType) == "" {
		return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_EVENT", Message: path + ".resourceType is required"}
	}
	resourceID, err := normalizeID(input.ResourceID, path+".resourceId")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Action) == "" {
		return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_EVENT", Message: path + ".action is required"}
	}
	payload, err := rawOrJSONPayload(input.Payload, input.PayloadJSON, path+".payload", map[string]any{})
	if err != nil {
		return nil, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, path+".sourceEventId")
	if err != nil {
		return nil, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, path+".occurredAt")
	if err != nil {
		return nil, err
	}
	outcome, err := auditOutcomeProto(input.Outcome, false)
	if err != nil {
		return nil, err
	}
	key, err := requiredEventIdempotencyKey(input.IdempotencyKey, path+".idempotencyKey")
	if err != nil {
		return nil, err
	}
	return &connectv1.AuditEvent{
		ResourceType:   resourceType,
		ResourceId:     resourceID,
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipal,
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
		Description:    input.Description,
		Action:         input.Action,
		SourceEventId:  sourceEventID,
		Outcome:        outcome,
	}, nil
}

func legacyAuditRecordProto(input AuditRecord, index int) (*connectv1.AuditEvent, error) {
	path := fmt.Sprintf("audit.records[%d]", index)
	var actor map[string]string
	var actorPrincipal string
	if input.Actor.ID != nil || input.Actor.Type != "" || input.Actor.Provider != "" {
		var err error
		actor, err = normalizeActor(input.Actor)
		if err != nil {
			return nil, err
		}
		actorPrincipal = actorPrincipalFromRef(actor)
	}
	resource, err := normalizeResource(input.Resource)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Action) == "" {
		return nil, &Error{Code: "MEDALLION_INVALID_AUDIT_EVENT", Message: path + ".action is required"}
	}
	compatibilityPayload := map[string]any{
		"actor":       actor,
		"resource":    resource,
		"before":      input.Before,
		"after":       input.After,
		"metadata":    input.Metadata,
		"evidenceUrl": nullableString(input.EvidenceURL),
	}
	payload, err := rawOrJSONPayload(input.Payload, input.PayloadJSON, path+".payload", compatibilityPayload)
	if err != nil {
		return nil, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, path+".sourceEventId")
	if err != nil {
		return nil, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, path+".occurredAt")
	if err != nil {
		return nil, err
	}
	outcome, err := auditOutcomeProto(input.Outcome, false)
	if err != nil {
		return nil, err
	}
	key, err := requiredEventIdempotencyKey(input.IdempotencyKey, path+".idempotencyKey")
	if err != nil {
		return nil, err
	}
	return &connectv1.AuditEvent{
		ResourceType:   resource["type"],
		ResourceId:     resource["id"],
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipal,
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
		Description:    input.Description,
		Action:         input.Action,
		SourceEventId:  sourceEventID,
		Outcome:        outcome,
	}, nil
}

// RecordBatch publishes between one and 1000 CDC events using
// ClientConfig.DefaultConnectorID.
func (c *CDCClient) RecordBatch(ctx context.Context, inputs []CDCEvent) (EventRecordResponse, error) {
	return c.PublishBatch(ctx, CDCBatchInput{Events: inputs})
}

// PublishBatch publishes between one and 1000 CDC events in one canonical RPC.
// Connector scope belongs to the request, never to a nested event.
func (c *CDCClient) PublishBatch(ctx context.Context, input CDCBatchInput) (EventRecordResponse, error) {
	if err := validateBatchSize(len(input.Events), "CDC"); err != nil {
		return EventRecordResponse{}, err
	}
	events := make([]*connectv1.CdcEvent, 0, len(input.Events))
	keys := make([]string, 0, len(input.Events))
	for index, record := range input.Events {
		event, err := cdcRecordProto(record, index)
		if err != nil {
			return EventRecordResponse{}, err
		}
		events = append(events, event)
		keys = append(keys, event.GetIdempotencyKey())
	}
	response, requestID, err := c.connect.PublishCdcEvents(ctx, &connectv1.PublishCdcEventsRequest{
		ConnectorId: firstNonBlank(input.ConnectorID, c.defaultConnectorID),
		Events:      events,
	})
	if err != nil {
		return EventRecordResponse{}, err
	}
	return cdcEventResponse(response, keys, requestID)
}

func cdcRecordProto(input CDCEvent, index int) (*connectv1.CdcEvent, error) {
	if input.StreamName != "" {
		return modernCDCRecordProto(input, index)
	}
	return legacyCDCRecordProto(input, index)
}

func modernCDCRecordProto(input CDCEvent, index int) (*connectv1.CdcEvent, error) {
	path := fmt.Sprintf("cdc.records[%d]", index)
	if input.Source != "" || input.Table != "" || len(input.PrimaryKey) != 0 || input.Before != nil || input.After != nil || input.Metadata != nil {
		return nil, &Error{Code: "MEDALLION_AMBIGUOUS_EVENT_INPUT", Message: path + " mixes modern stream/payload fields with legacy CDC fields"}
	}
	if strings.TrimSpace(input.StreamName) == "" || strings.TrimSpace(input.EntityType) == "" {
		return nil, &Error{Code: "MEDALLION_INVALID_CDC_EVENT", Message: path + " requires streamName and entityType"}
	}
	entityID, err := normalizeID(input.EntityID, path+".entityId")
	if err != nil {
		return nil, err
	}
	var actor map[string]string
	var actorPrincipal string
	if input.Actor != nil {
		actor, err = normalizeActor(*input.Actor)
		if err != nil {
			return nil, err
		}
		actorPrincipal = actorPrincipalFromRef(actor)
	}
	payload, err := rawOrJSONPayload(input.Payload, input.PayloadJSON, path+".payload", map[string]any{})
	if err != nil {
		return nil, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, path+".sourceEventId")
	if err != nil {
		return nil, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, path+".occurredAt")
	if err != nil {
		return nil, err
	}
	key, err := requiredEventIdempotencyKey(input.IdempotencyKey, path+".idempotencyKey")
	if err != nil {
		return nil, err
	}
	event := &connectv1.CdcEvent{
		StreamName:     input.StreamName,
		EntityType:     input.EntityType,
		EntityId:       entityID,
		Operation:      cdcOperation(input.Operation),
		SourceEventId:  sourceEventID,
		IdempotencyKey: key,
		ActorPrincipal: actorPrincipal,
		PayloadJson:    payload,
		OccurredAt:     occurredAt,
		Description:    input.Description,
	}
	if event.Operation == connectv1.CdcOperation_CDC_OPERATION_UNSPECIFIED {
		return nil, &Error{Code: "MEDALLION_INVALID_CDC_OPERATION", Message: path + ".operation must be insert, update, delete, or snapshot"}
	}
	return event, nil
}

func legacyCDCRecordProto(input CDCEvent, index int) (*connectv1.CdcEvent, error) {
	path := fmt.Sprintf("cdc.records[%d]", index)
	primaryKey, err := normalizeIDRecord(input.PrimaryKey, path+".primaryKey")
	if err != nil {
		return nil, err
	}
	if len(primaryKey) == 0 {
		return nil, &Error{Code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY", Message: path + ".primaryKey must contain at least one field"}
	}
	if len(primaryKey) > 1 && input.EntityID == nil {
		return nil, &Error{Code: "MEDALLION_MISSING_CDC_ENTITY_ID", Message: path + ".entityId is required when primaryKey contains more than one field"}
	}
	if strings.TrimSpace(input.Table) == "" {
		return nil, &Error{Code: "MEDALLION_INVALID_CDC_EVENT", Message: path + ".table is required"}
	}
	entityID := ""
	if input.EntityID != nil {
		entityID, err = normalizeID(input.EntityID, path+".entityId")
		if err != nil {
			return nil, err
		}
	} else {
		entityID = entityIDFromPrimaryKey(primaryKey)
	}
	var actor map[string]string
	var actorPrincipal string
	if input.Actor != nil {
		actor, err = normalizeActor(*input.Actor)
		if err != nil {
			return nil, err
		}
		actorPrincipal = actorPrincipalFromRef(actor)
	}
	compatibilityPayload := map[string]any{
		"source":     input.Source,
		"table":      input.Table,
		"actor":      actor,
		"primaryKey": primaryKey,
		"before":     input.Before,
		"after":      input.After,
		"metadata":   input.Metadata,
	}
	payload, err := rawOrJSONPayload(input.Payload, input.PayloadJSON, path+".payload", compatibilityPayload)
	if err != nil {
		return nil, err
	}
	sourceEventID, err := normalizeOptionalID(input.SourceEventID, path+".sourceEventId")
	if err != nil {
		return nil, err
	}
	occurredAt, err := timestampFromString(input.OccurredAt, path+".occurredAt")
	if err != nil {
		return nil, err
	}
	key, err := requiredEventIdempotencyKey(input.IdempotencyKey, path+".idempotencyKey")
	if err != nil {
		return nil, err
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
		Description:    input.Description,
	}
	if event.Operation == connectv1.CdcOperation_CDC_OPERATION_UNSPECIFIED {
		return nil, &Error{Code: "MEDALLION_INVALID_CDC_OPERATION", Message: path + ".operation must be insert, update, delete, or snapshot"}
	}
	return event, nil
}

// List returns one canonical CDC page with lossless int64 and JSON projections.
func (c *CDCClient) List(ctx context.Context, input CDCListQuery) (CDCListResponse, error) {
	limit, err := cdcListLimit(input.Limit, input.PageSize)
	if err != nil {
		return CDCListResponse{}, err
	}
	entityID := ""
	if input.EntityID != nil {
		entityID, err = normalizeID(input.EntityID, "cdc.list.entityId")
		if err != nil {
			return CDCListResponse{}, err
		}
	}
	request := &connectv1.ListCdcEventsRequest{
		ConnectorId:         firstNonBlank(input.ConnectorID, c.defaultConnectorID),
		EntityType:          strings.TrimSpace(input.EntityType),
		EntityId:            entityID,
		Limit:               limit,
		SourceSystem:        strings.TrimSpace(input.SourceSystem),
		StreamName:          strings.TrimSpace(input.StreamName),
		PageCursor:          input.Cursor,
		IngestedByPrincipal: strings.TrimSpace(input.IngesterPrincipal),
		WorkspaceId:         c.connect.requests.workspaceID,
	}
	if input.Actor != nil {
		actor, err := normalizeActor(*input.Actor)
		if err != nil {
			return CDCListResponse{}, err
		}
		request.ActorPrincipal = actorPrincipalFromRef(actor)
	}
	request.OccurredAtFrom, err = timestampFromString(input.OccurredAtFrom, "cdc.list.occurredAtFrom")
	if err != nil {
		return CDCListResponse{}, err
	}
	request.OccurredAtTo, err = timestampFromString(input.OccurredAtTo, "cdc.list.occurredAtTo")
	if err != nil {
		return CDCListResponse{}, err
	}
	if err := validateTimestampBounds(request.GetOccurredAtFrom(), request.GetOccurredAtTo(), "cdc.list"); err != nil {
		return CDCListResponse{}, err
	}
	response, requestID, err := c.connect.ListCdcEvents(ctx, request)
	if err != nil {
		return CDCListResponse{}, err
	}
	return cdcListResponse(response, requestID, c.connect.requests.workspaceID)
}

// List returns one canonical audit page. Resource type and ID may both be
// omitted for callers authorized to perform wildcard reads.
func (c *AuditClient) List(ctx context.Context, input AuditTrailQuery) (AuditTrailResponse, error) {
	return c.list(ctx, input, false)
}

func (c *AuditClient) list(ctx context.Context, input AuditTrailQuery, requireResource bool) (AuditTrailResponse, error) {
	resourceType := strings.TrimSpace(input.ResourceType)
	resourceID := ""
	if input.ResourceID != nil {
		var err error
		resourceID, err = normalizeID(input.ResourceID, "audit.resourceId")
		if err != nil {
			return AuditTrailResponse{}, err
		}
	}
	if requireResource && resourceType == "" {
		return AuditTrailResponse{}, missingOption("MEDALLION_MISSING_RESOURCE_TYPE", "resource type is required to read an audit trail")
	}
	if (resourceType == "") != (resourceID == "") {
		return AuditTrailResponse{}, &Error{Code: "MEDALLION_INVALID_AUDIT_RESOURCE_FILTER", Message: "audit resource type and resource ID must be provided together"}
	}
	limit, err := auditTrailLimit(input.Limit, input.PageSize)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	request := &connectv1.ListAuditEventsRequest{
		ConnectorId:         firstNonBlank(input.ConnectorID, c.defaultConnectorID),
		ResourceType:        resourceType,
		ResourceId:          resourceID,
		Limit:               limit,
		Action:              strings.TrimSpace(input.Action),
		PageCursor:          input.Cursor,
		SourceSystem:        strings.TrimSpace(input.SourceSystem),
		IngestedByPrincipal: strings.TrimSpace(input.IngesterPrincipal),
		WorkspaceId:         c.connect.requests.workspaceID,
	}
	var sourceActor map[string]string
	if input.Actor != nil {
		sourceActor, err = normalizeActor(*input.Actor)
		if err != nil {
			return AuditTrailResponse{}, err
		}
		request.ActorPrincipal = actorPrincipalFromRef(sourceActor)
	}
	request.OccurredAtFrom, err = timestampFromString(input.OccurredAtFrom, "audit.list.occurredAtFrom")
	if err != nil {
		return AuditTrailResponse{}, err
	}
	request.OccurredAtTo, err = timestampFromString(input.OccurredAtTo, "audit.list.occurredAtTo")
	if err != nil {
		return AuditTrailResponse{}, err
	}
	if err := validateTimestampBounds(request.GetOccurredAtFrom(), request.GetOccurredAtTo(), "audit.list"); err != nil {
		return AuditTrailResponse{}, err
	}
	request.Origin, err = auditOriginProto(input.Origin)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	request.Outcome, err = auditOutcomeProto(input.Outcome, true)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	response, requestID, err := c.connect.ListAuditEvents(ctx, request)
	if err != nil {
		return AuditTrailResponse{}, err
	}
	return auditListResponse(response, requestID, c.connect.requests.workspaceID, request.GetActorPrincipal())
}

// AuditIterator walks cursor pages and stops with an error if a server repeats
// a cursor, preventing an accidental infinite loop.
type AuditIterator struct {
	client  *AuditClient
	ctx     context.Context
	query   AuditTrailQuery
	page    []AuditTrailEvent
	index   int
	current AuditTrailEvent
	seen    map[string]struct{}
	pages   int
	done    bool
	err     error
}

func (c *AuditClient) Iterate(ctx context.Context, query AuditTrailQuery) *AuditIterator {
	return &AuditIterator{client: c, ctx: ctx, query: query, seen: map[string]struct{}{}, index: -1}
}

func (iterator *AuditIterator) Next() bool {
	for iterator != nil && iterator.err == nil {
		if iterator.index+1 < len(iterator.page) {
			iterator.index++
			iterator.current = iterator.page[iterator.index]
			return true
		}
		if iterator.done {
			return false
		}
		cursor := iterator.query.Cursor
		if _, exists := iterator.seen[cursor]; exists {
			iterator.err = repeatedCursorError()
			return false
		}
		if iterator.pages >= maxIteratorPages {
			iterator.err = &Error{Code: "MEDALLION_PAGINATION_LIMIT", Message: "audit iterator exceeded 10000 pages"}
			return false
		}
		iterator.seen[cursor] = struct{}{}
		response, err := iterator.client.List(iterator.ctx, iterator.query)
		if err != nil {
			iterator.err = err
			return false
		}
		iterator.pages++
		iterator.page = response.Events
		iterator.index = -1
		iterator.query.Cursor = response.NextCursor
		iterator.done = response.NextCursor == ""
		if len(iterator.page) > 0 {
			iterator.index = 0
			iterator.current = iterator.page[0]
			return true
		}
	}
	return false
}

func (iterator *AuditIterator) Event() AuditTrailEvent {
	if iterator == nil {
		return AuditTrailEvent{}
	}
	return iterator.current
}

func (iterator *AuditIterator) Err() error {
	if iterator == nil {
		return nil
	}
	return iterator.err
}

// CDCIterator has the same cursor-safety guarantees as AuditIterator.
type CDCIterator struct {
	client  *CDCClient
	ctx     context.Context
	query   CDCListQuery
	page    []CDCListEvent
	index   int
	current CDCListEvent
	seen    map[string]struct{}
	pages   int
	done    bool
	err     error
}

func (c *CDCClient) Iterate(ctx context.Context, query CDCListQuery) *CDCIterator {
	return &CDCIterator{client: c, ctx: ctx, query: query, seen: map[string]struct{}{}, index: -1}
}

func (iterator *CDCIterator) Next() bool {
	for iterator != nil && iterator.err == nil {
		if iterator.index+1 < len(iterator.page) {
			iterator.index++
			iterator.current = iterator.page[iterator.index]
			return true
		}
		if iterator.done {
			return false
		}
		cursor := iterator.query.Cursor
		if _, exists := iterator.seen[cursor]; exists {
			iterator.err = repeatedCursorError()
			return false
		}
		if iterator.pages >= maxIteratorPages {
			iterator.err = &Error{Code: "MEDALLION_PAGINATION_LIMIT", Message: "CDC iterator exceeded 10000 pages"}
			return false
		}
		iterator.seen[cursor] = struct{}{}
		response, err := iterator.client.List(iterator.ctx, iterator.query)
		if err != nil {
			iterator.err = err
			return false
		}
		iterator.pages++
		iterator.page = response.Events
		iterator.index = -1
		iterator.query.Cursor = response.NextCursor
		iterator.done = response.NextCursor == ""
		if len(iterator.page) > 0 {
			iterator.index = 0
			iterator.current = iterator.page[0]
			return true
		}
	}
	return false
}

func (iterator *CDCIterator) Event() CDCListEvent {
	if iterator == nil {
		return CDCListEvent{}
	}
	return iterator.current
}

func (iterator *CDCIterator) Err() error {
	if iterator == nil {
		return nil
	}
	return iterator.err
}

func repeatedCursorError() error {
	return &Error{Code: "MEDALLION_REPEATED_CURSOR", Message: "Medallion returned a repeated non-empty cursor; iteration stopped to prevent an infinite loop"}
}
