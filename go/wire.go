package medallion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxAuditTrailLimit = 500
	maxListCursorBytes = 2048
)

func cdcEventResponse(body *connectv1.PublishCdcEventsResponse, expectedKeys []string, requestID string) (EventRecordResponse, error) {
	if err := validateCdcPublishAcknowledgement(body, expectedKeys, requestID); err != nil {
		return EventRecordResponse{}, err
	}
	results := make([]PublishedEventResult, 0, len(body.GetEvents()))
	for _, item := range body.GetEvents() {
		results = append(results, PublishedEventResult{
			IdempotencyKey: item.GetIdempotencyKey(),
			EventID:        strconv.FormatInt(item.GetEventId(), 10),
			Duplicate:      item.GetDuplicate(),
			Proto:          item,
		})
	}
	duplicate := body.GetDuplicateCount() == uint32(len(expectedKeys))
	key := ""
	if len(expectedKeys) == 1 {
		key = expectedKeys[0]
	}
	return EventRecordResponse{
		RequestID:      requestID,
		IdempotencyKey: key,
		Duplicate:      duplicate,
		Result:         publishResultString(body.GetAcceptedCount(), body.GetDuplicateCount()),
		AcceptedCount:  body.GetAcceptedCount(),
		DuplicateCount: body.GetDuplicateCount(),
		Events:         results,
		Proto:          body,
	}, nil
}

func auditEventResponse(body *connectv1.PublishAuditEventsResponse, expectedKeys []string, requestID string) (AuditRecordResponse, error) {
	if err := validateAuditPublishAcknowledgement(body, expectedKeys, requestID); err != nil {
		return AuditRecordResponse{}, err
	}
	results := make([]PublishedAuditEventResult, 0, len(body.GetEvents()))
	for _, item := range body.GetEvents() {
		results = append(results, PublishedAuditEventResult{
			IdempotencyKey: item.GetIdempotencyKey(),
			EventID:        strconv.FormatInt(item.GetEventId(), 10),
			Duplicate:      item.GetDuplicate(),
			Proto:          item,
		})
	}
	duplicate := body.GetDuplicateCount() == uint32(len(expectedKeys))
	key := ""
	if len(expectedKeys) == 1 {
		key = expectedKeys[0]
	}
	return AuditRecordResponse{
		RequestID:      requestID,
		IdempotencyKey: key,
		Duplicate:      duplicate,
		Result:         publishResultString(body.GetAcceptedCount(), body.GetDuplicateCount()),
		AcceptedCount:  body.GetAcceptedCount(),
		DuplicateCount: body.GetDuplicateCount(),
		Events:         results,
		Proto:          body,
	}, nil
}

func publishResultString(accepted, duplicate uint32) string {
	if accepted > 0 && duplicate > 0 {
		return "mixed"
	}
	if duplicate > 0 {
		return "duplicate"
	}
	return "accepted"
}

func validateCdcPublishAcknowledgement(body *connectv1.PublishCdcEventsResponse, expectedKeys []string, requestID string) error {
	if body == nil {
		return invalidPublishResponse(requestID, "Medallion returned no CDC publish acknowledgement")
	}
	if uint64(body.GetAcceptedCount())+uint64(body.GetDuplicateCount()) != uint64(len(expectedKeys)) || len(body.GetEvents()) != len(expectedKeys) {
		return invalidPublishResponse(requestID, "Medallion returned inconsistent CDC publish counts")
	}
	var accepted, duplicate uint32
	for index, event := range body.GetEvents() {
		if event == nil || event.GetEventId() <= 0 {
			return invalidPublishResponse(requestID, "Medallion returned a CDC acknowledgement without a positive event ID")
		}
		if event.GetIdempotencyKey() != expectedKeys[index] {
			return invalidPublishResponse(requestID, "Medallion returned CDC acknowledgements in an unexpected order")
		}
		if event.GetDuplicate() {
			duplicate++
		} else {
			accepted++
		}
	}
	if accepted != body.GetAcceptedCount() || duplicate != body.GetDuplicateCount() {
		return invalidPublishResponse(requestID, "Medallion returned inconsistent CDC duplicate flags")
	}
	return nil
}

func validateAuditPublishAcknowledgement(body *connectv1.PublishAuditEventsResponse, expectedKeys []string, requestID string) error {
	if body == nil {
		return invalidPublishResponse(requestID, "Medallion returned no audit publish acknowledgement")
	}
	if uint64(body.GetAcceptedCount())+uint64(body.GetDuplicateCount()) != uint64(len(expectedKeys)) || len(body.GetEvents()) != len(expectedKeys) {
		return invalidPublishResponse(requestID, "Medallion returned inconsistent audit publish counts")
	}
	var accepted, duplicate uint32
	for index, event := range body.GetEvents() {
		if event == nil || event.GetEventId() <= 0 {
			return invalidPublishResponse(requestID, "Medallion returned an audit acknowledgement without a positive event ID")
		}
		if event.GetIdempotencyKey() != expectedKeys[index] {
			return invalidPublishResponse(requestID, "Medallion returned audit acknowledgements in an unexpected order")
		}
		if event.GetDuplicate() {
			duplicate++
		} else {
			accepted++
		}
	}
	if accepted != body.GetAcceptedCount() || duplicate != body.GetDuplicateCount() {
		return invalidPublishResponse(requestID, "Medallion returned inconsistent audit duplicate flags")
	}
	return nil
}

func invalidPublishResponse(requestID, message string) error {
	return &Error{Code: "MEDALLION_INVALID_PUBLISH_RESPONSE", RequestID: requestID, Message: message}
}

func auditTrailLimit(limit, pageSize int) (uint32, error) {
	return listLimit(limit, pageSize, "MEDALLION_INVALID_AUDIT_TRAIL_LIMIT", "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE", "audit.trail")
}

func cdcListLimit(limit, pageSize int) (uint32, error) {
	return listLimit(limit, pageSize, "MEDALLION_INVALID_CDC_LIST_LIMIT", "MEDALLION_CDC_LIST_LIMIT_TOO_LARGE", "cdc.list")
}

func listLimit(limit, pageSize int, invalidCode, tooLargeCode, path string) (uint32, error) {
	value := limit
	if value == 0 {
		value = pageSize
	}
	if value < 0 {
		return 0, &Error{Code: invalidCode, Message: path + " limit/pageSize must be zero or a positive integer"}
	}
	if value > maxAuditTrailLimit {
		return 0, &Error{Code: tooLargeCode, Message: path + " limit/pageSize must be 500 or less; use cursor pagination for larger reads"}
	}
	return uint32(value), nil
}

func auditEventFromConnect(item *connectv1.AuditEvent) AuditTrailEvent {
	payload := parsePayload(item.GetPayloadJson())
	return auditEventProjection(item, payload)
}

func auditEventFromConnectStrict(item *connectv1.AuditEvent, index int, expectedWorkspace string) (AuditTrailEvent, error) {
	if item == nil {
		return AuditTrailEvent{}, &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: fmt.Sprintf("audit events[%d] is null", index)}
	}
	path := fmt.Sprintf("audit.events[%d]", index)
	if err := validateResponseWorkspace(item.GetWorkspaceId(), expectedWorkspace, path+".workspaceId", ""); err != nil {
		return AuditTrailEvent{}, err
	}
	if item.GetId() <= 0 || auditOriginFromProto(item.GetOrigin()) == "" || auditOutcomeFromProto(item.GetOutcome()) == "" {
		return AuditTrailEvent{}, &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: fmt.Sprintf("audit events[%d] is missing durable identity, origin, or outcome", index)}
	}
	for _, field := range []struct {
		value string
		name  string
		max   int
	}{
		{item.GetResourceType(), ".resourceType", 256},
		{item.GetResourceId(), ".resourceId", 1024},
		{item.GetAction(), ".action", 256},
	} {
		if err := validateRequiredListText(field.value, path+field.name, field.max); err != nil {
			return AuditTrailEvent{}, err
		}
	}
	if err := validateResponseEventKey(item.GetIdempotencyKey(), path+".idempotencyKey"); err != nil {
		return AuditTrailEvent{}, err
	}
	for _, field := range []struct {
		value string
		name  string
		max   int
	}{
		{item.GetConnectorId(), ".connectorId", 128},
		{item.GetSourceEventId(), ".sourceEventId", 1024},
		{item.GetActorPrincipal(), ".actorPrincipal", 512},
		{item.GetDescription(), ".description", 4096},
		{item.GetSourceSystem(), ".sourceSystem", 256},
		{item.GetIngestedByPrincipal(), ".ingestedByPrincipal", 512},
	} {
		if err := validateOptionalListText(field.value, path+field.name, field.max); err != nil {
			return AuditTrailEvent{}, err
		}
	}
	payload, err := parsePayloadStrict(item.GetPayloadJson(), path+".payloadJson")
	if err != nil {
		return AuditTrailEvent{}, responseProjectionError(err)
	}
	if err := validateTimestamp(item.GetOccurredAt(), path+".occurredAt"); err != nil {
		return AuditTrailEvent{}, responseProjectionError(err)
	}
	if err := validateTimestamp(item.GetObservedAt(), path+".observedAt"); err != nil {
		return AuditTrailEvent{}, responseProjectionError(err)
	}
	return auditEventProjection(item, payload), nil
}

func auditEventProjection(item *connectv1.AuditEvent, payload any) AuditTrailEvent {
	payloadRecord := asMap(payload)
	actor := actorFromPrincipal(item.GetActorPrincipal())
	payloadActor := actorFromPayload(payloadRecord["actor"])
	if payloadActor != nil {
		normalized, err := normalizeActor(*payloadActor)
		if err == nil && actorPrincipalFromRef(normalized) == item.GetActorPrincipal() {
			actor = payloadActor
		}
	}
	eventID := ""
	if item.GetId() != 0 {
		eventID = strconv.FormatInt(item.GetId(), 10)
	}
	return AuditTrailEvent{
		ID:                eventID,
		EventID:           eventID,
		WorkspaceID:       item.GetWorkspaceId(),
		ConnectorID:       item.GetConnectorId(),
		Actor:             actor,
		IngesterPrincipal: item.GetIngestedByPrincipal(),
		ActorPrincipal:    item.GetActorPrincipal(),
		Action:            item.GetAction(),
		Description:       item.GetDescription(),
		IdempotencyKey:    item.GetIdempotencyKey(),
		TargetType:        item.GetResourceType(),
		TargetID:          item.GetResourceId(),
		EntityType:        item.GetResourceType(),
		EntityID:          item.GetResourceId(),
		Metadata:          mapStringAny(payloadRecord["metadata"]),
		CreatedAt:         timestampString(item.GetObservedAt()),
		OccurredAt:        timestampString(item.GetOccurredAt()),
		ObservedAt:        timestampString(item.GetObservedAt()),
		Before:            payloadRecord["before"],
		After:             payloadRecord["after"],
		EvidenceURL:       stringValue(payloadRecord["evidenceUrl"]),
		SourceEventID:     item.GetSourceEventId(),
		SourceSystem:      item.GetSourceSystem(),
		Origin:            auditOriginFromProto(item.GetOrigin()),
		Outcome:           auditOutcomeFromProto(item.GetOutcome()),
		Payload:           payload,
		PayloadJSON:       item.GetPayloadJson(),
		Proto:             item,
	}
}

func auditListResponse(body *connectv1.ListAuditEventsResponse, requestID, expectedWorkspace, actorPrincipal string) (AuditTrailResponse, error) {
	if body == nil {
		return AuditTrailResponse{}, &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", RequestID: requestID, Message: "Medallion returned no audit list response"}
	}
	if err := validateResponseCursor(body.GetNextPageCursor(), requestID); err != nil {
		return AuditTrailResponse{}, err
	}
	events := make([]AuditTrailEvent, 0, len(body.GetEvents()))
	for index, item := range body.GetEvents() {
		event, err := auditEventFromConnectStrict(item, index, expectedWorkspace)
		if err != nil {
			setProjectionRequestID(err, requestID)
			return AuditTrailResponse{}, err
		}
		if actorPrincipal == "" || event.ActorPrincipal == actorPrincipal {
			events = append(events, event)
		}
	}
	return AuditTrailResponse{RequestID: requestID, NextCursor: body.GetNextPageCursor(), Events: events, Proto: body}, nil
}

func cdcListResponse(body *connectv1.ListCdcEventsResponse, requestID, expectedWorkspace string) (CDCListResponse, error) {
	if body == nil {
		return CDCListResponse{}, &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", RequestID: requestID, Message: "Medallion returned no CDC list response"}
	}
	if err := validateResponseCursor(body.GetNextPageCursor(), requestID); err != nil {
		return CDCListResponse{}, err
	}
	events := make([]CDCListEvent, 0, len(body.GetEvents()))
	for index, item := range body.GetEvents() {
		if item == nil {
			return CDCListResponse{}, &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", RequestID: requestID, Message: fmt.Sprintf("CDC events[%d] is null", index)}
		}
		path := fmt.Sprintf("cdc.events[%d]", index)
		if err := validateResponseWorkspace(item.GetWorkspaceId(), expectedWorkspace, path+".workspaceId", requestID); err != nil {
			return CDCListResponse{}, err
		}
		if item.GetId() <= 0 || cdcOperationFromProto(item.GetOperation()) == "" {
			return CDCListResponse{}, &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", RequestID: requestID, Message: fmt.Sprintf("CDC events[%d] is missing durable identity or operation", index)}
		}
		for _, field := range []struct {
			value string
			name  string
			max   int
		}{
			{item.GetStreamName(), ".streamName", 256},
			{item.GetEntityType(), ".entityType", 256},
			{item.GetEntityId(), ".entityId", 1024},
		} {
			if err := validateRequiredListText(field.value, path+field.name, field.max); err != nil {
				setProjectionRequestID(err, requestID)
				return CDCListResponse{}, err
			}
		}
		if err := validateResponseEventKey(item.GetIdempotencyKey(), path+".idempotencyKey"); err != nil {
			setProjectionRequestID(err, requestID)
			return CDCListResponse{}, err
		}
		for _, field := range []struct {
			value string
			name  string
			max   int
		}{
			{item.GetConnectorId(), ".connectorId", 128},
			{item.GetSourceEventId(), ".sourceEventId", 1024},
			{item.GetActorPrincipal(), ".actorPrincipal", 512},
			{item.GetDescription(), ".description", 4096},
			{item.GetSourceSystem(), ".sourceSystem", 256},
			{item.GetIngestedByPrincipal(), ".ingestedByPrincipal", 512},
		} {
			if err := validateOptionalListText(field.value, path+field.name, field.max); err != nil {
				setProjectionRequestID(err, requestID)
				return CDCListResponse{}, err
			}
		}
		payload, err := parsePayloadStrict(item.GetPayloadJson(), path+".payloadJson")
		if err != nil {
			err = responseProjectionError(err)
			setProjectionRequestID(err, requestID)
			return CDCListResponse{}, err
		}
		if err := validateTimestamp(item.GetOccurredAt(), path+".occurredAt"); err != nil {
			err = responseProjectionError(err)
			setProjectionRequestID(err, requestID)
			return CDCListResponse{}, err
		}
		if err := validateTimestamp(item.GetObservedAt(), path+".observedAt"); err != nil {
			err = responseProjectionError(err)
			setProjectionRequestID(err, requestID)
			return CDCListResponse{}, err
		}
		eventID := strconv.FormatInt(item.GetId(), 10)
		events = append(events, CDCListEvent{
			ID:                eventID,
			WorkspaceID:       item.GetWorkspaceId(),
			ConnectorID:       item.GetConnectorId(),
			StreamName:        item.GetStreamName(),
			EntityType:        item.GetEntityType(),
			EntityID:          item.GetEntityId(),
			Operation:         cdcOperationFromProto(item.GetOperation()),
			SourceEventID:     item.GetSourceEventId(),
			IdempotencyKey:    item.GetIdempotencyKey(),
			Actor:             actorFromPrincipal(item.GetActorPrincipal()),
			ActorPrincipal:    item.GetActorPrincipal(),
			OccurredAt:        timestampString(item.GetOccurredAt()),
			ObservedAt:        timestampString(item.GetObservedAt()),
			Description:       item.GetDescription(),
			SourceSystem:      item.GetSourceSystem(),
			IngesterPrincipal: item.GetIngestedByPrincipal(),
			Payload:           payload,
			PayloadJSON:       item.GetPayloadJson(),
			Proto:             item,
		})
	}
	return CDCListResponse{RequestID: requestID, NextCursor: body.GetNextPageCursor(), Events: events, Proto: body}, nil
}

func validateCdcListResponse(body *connectv1.ListCdcEventsResponse, expectedWorkspace, requestID string) error {
	_, err := cdcListResponse(body, requestID, expectedWorkspace)
	return err
}

func validateAuditListResponse(body *connectv1.ListAuditEventsResponse, expectedWorkspace, requestID string) error {
	_, err := auditListResponse(body, requestID, expectedWorkspace, "")
	return err
}

func validateResponseCursor(value, requestID string) error {
	if len(value) > maxListCursorBytes || !utf8.ValidString(value) {
		return &Error{
			Code:      "MEDALLION_INVALID_LIST_RESPONSE",
			RequestID: requestID,
			Message:   "Medallion returned an invalid pagination cursor",
		}
	}
	return nil
}

func validateResponseWorkspace(value, expected, path, requestID string) error {
	if !isCanonicalWorkspaceID(value) {
		return &Error{
			Code:      "MEDALLION_INVALID_LIST_RESPONSE",
			RequestID: requestID,
			Message:   path + " must contain a canonical workspace ID",
		}
	}
	if value != expected {
		return &Error{
			Code:      "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
			RequestID: requestID,
			Message:   "Medallion returned an event for a different workspace",
		}
	}
	return nil
}

func responseProjectionError(cause error) error {
	return &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: cause.Error(), Cause: cause}
}

func validateRequiredListText(value, path string, maximum int) error {
	if strings.TrimSpace(value) == "" {
		return &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: path + " is required"}
	}
	if len(value) > maximum {
		return &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: fmt.Sprintf("%s must not exceed %d bytes", path, maximum)}
	}
	return nil
}

func validateOptionalListText(value, path string, maximum int) error {
	if len(value) > maximum {
		return &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: fmt.Sprintf("%s must not exceed %d bytes", path, maximum)}
	}
	return nil
}

func validateResponseEventKey(value, path string) error {
	if value == "" {
		return &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: path + " is required"}
	}
	if len(value) > 512 || !utf8.ValidString(value) {
		return &Error{Code: "MEDALLION_INVALID_LIST_RESPONSE", Message: path + " must be valid UTF-8 between 1 and 512 bytes"}
	}
	return nil
}

func setProjectionRequestID(err error, requestID string) {
	if typed, ok := err.(*Error); ok {
		typed.RequestID = requestID
	}
}

func auditOutcomeProto(value AuditOutcome, optional bool) (connectv1.AuditEventOutcome, error) {
	switch value {
	case AuditOutcomeSucceeded:
		return connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, nil
	case AuditOutcomeFailed:
		return connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_FAILED, nil
	case AuditOutcomeIndeterminate:
		return connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_INDETERMINATE, nil
	case "":
		if optional {
			return connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_UNSPECIFIED, nil
		}
	}
	return connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_UNSPECIFIED, &Error{
		Code:    "MEDALLION_INVALID_AUDIT_OUTCOME",
		Message: "audit outcome must be succeeded, failed, or indeterminate",
	}
}

func auditOriginProto(value AuditOrigin) (connectv1.AuditEventOrigin, error) {
	switch value {
	case "":
		return connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_UNSPECIFIED, nil
	case AuditOriginExternalProvider:
		return connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER, nil
	case AuditOriginConnect:
		return connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_CONNECT, nil
	default:
		return connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_UNSPECIFIED, &Error{
			Code:    "MEDALLION_INVALID_AUDIT_ORIGIN",
			Message: "audit origin must be external_provider or connect",
		}
	}
}

func auditOutcomeFromProto(value connectv1.AuditEventOutcome) AuditOutcome {
	switch value {
	case connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED:
		return AuditOutcomeSucceeded
	case connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_FAILED:
		return AuditOutcomeFailed
	case connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_INDETERMINATE:
		return AuditOutcomeIndeterminate
	default:
		return ""
	}
}

func auditOriginFromProto(value connectv1.AuditEventOrigin) AuditOrigin {
	switch value {
	case connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER:
		return AuditOriginExternalProvider
	case connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_CONNECT:
		return AuditOriginConnect
	default:
		return ""
	}
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func actorFromPayload(value any) *ActorRef {
	record := asMap(value)
	rawID, ok := record["id"]
	if !ok {
		return nil
	}
	id, err := normalizeID(rawID, "actor.id")
	if err != nil {
		return nil
	}
	return &ActorRef{ID: id, Type: stringValue(record["type"]), Provider: stringValue(record["provider"])}
}

func parsePayload(value string) any {
	out, err := parsePayloadStrict(value, "payloadJson")
	if err != nil {
		return nil
	}
	return out
}

func parsePayloadStrict(value, path string) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader([]byte(value)))
	decoder.UseNumber()
	var out any
	if err := decoder.Decode(&out); err != nil {
		return nil, &Error{Code: "MEDALLION_INVALID_JSON_PAYLOAD", Message: path + " must contain exactly one valid JSON value", Cause: err}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return nil, &Error{Code: "MEDALLION_INVALID_JSON_PAYLOAD", Message: path + " must contain exactly one valid JSON value", Cause: err}
	}
	return out, nil
}

func rawOrJSONPayload(value any, raw, path string, fallback any) (string, error) {
	if value != nil && raw != "" {
		return "", &Error{Code: "MEDALLION_AMBIGUOUS_JSON_PAYLOAD", Message: path + " and " + path + "JSON are mutually exclusive"}
	}
	if raw != "" {
		if _, err := parsePayloadStrict(raw, path+"JSON"); err != nil {
			return "", &Error{Code: "MEDALLION_INVALID_JSON_BODY", Message: path + "JSON must contain exactly one valid JSON value", Cause: err}
		}
		return raw, nil
	}
	if value == nil {
		value = fallback
	}
	rendered, err := json.Marshal(value)
	if err != nil {
		return "", &Error{Code: "MEDALLION_INVALID_JSON_BODY", Message: path + " must be JSON serializable", Cause: err}
	}
	return string(rendered), nil
}

func cdcOperation(operation string) connectv1.CdcOperation {
	switch operation {
	case "insert":
		return connectv1.CdcOperation_CDC_OPERATION_INSERT
	case "update":
		return connectv1.CdcOperation_CDC_OPERATION_UPDATE
	case "delete":
		return connectv1.CdcOperation_CDC_OPERATION_DELETE
	case "snapshot":
		return connectv1.CdcOperation_CDC_OPERATION_SNAPSHOT
	default:
		return connectv1.CdcOperation_CDC_OPERATION_UNSPECIFIED
	}
}

func cdcOperationFromProto(operation connectv1.CdcOperation) string {
	switch operation {
	case connectv1.CdcOperation_CDC_OPERATION_INSERT:
		return "insert"
	case connectv1.CdcOperation_CDC_OPERATION_UPDATE:
		return "update"
	case connectv1.CdcOperation_CDC_OPERATION_DELETE:
		return "delete"
	case connectv1.CdcOperation_CDC_OPERATION_SNAPSHOT:
		return "snapshot"
	default:
		return ""
	}
}

func entityIDFromPrimaryKey(primaryKey map[string]string) string {
	for _, value := range primaryKey {
		return value
	}
	return ""
}

func normalizeOptionalID(value any, path string) (string, error) {
	if value == nil {
		return "", nil
	}
	return normalizeID(value, path)
}

func timestampFromString(value string, path string) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, &Error{Code: "MEDALLION_INVALID_TIMESTAMP", Message: path + " must be an RFC3339 timestamp"}
	}
	timestamp := timestamppb.New(parsed)
	if err := validateTimestamp(timestamp, path); err != nil {
		return nil, err
	}
	return timestamp, nil
}

func timestampString(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}

var (
	minimumAnalyticalTimestamp = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	maximumAnalyticalTimestamp = time.Date(2262, time.April, 11, 23, 47, 16, 854775807, time.UTC)
)

func validateTimestamp(value *timestamppb.Timestamp, path string) error {
	if value == nil {
		return nil
	}
	if err := value.CheckValid(); err != nil {
		return &Error{Code: "MEDALLION_INVALID_TIMESTAMP", Message: path + " must be a valid protobuf timestamp", Cause: err}
	}
	instant := value.AsTime()
	if instant.Before(minimumAnalyticalTimestamp) || instant.After(maximumAnalyticalTimestamp) {
		return &Error{Code: "MEDALLION_TIMESTAMP_OUT_OF_RANGE", Message: path + " must be between 1900-01-01 and 2262-04-11T23:47:16.854775807Z"}
	}
	return nil
}

func validateTimestampBounds(from, to *timestamppb.Timestamp, path string) error {
	if err := validateTimestamp(from, path+".occurredAtFrom"); err != nil {
		return err
	}
	if err := validateTimestamp(to, path+".occurredAtTo"); err != nil {
		return err
	}
	if from != nil && to != nil && !from.AsTime().Before(to.AsTime()) {
		return &Error{Code: "MEDALLION_INVALID_TIMESTAMP_RANGE", Message: path + ".occurredAtFrom must be earlier than occurredAtTo"}
	}
	return nil
}

func asMap(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}

func mapStringAny(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return nil
}

func stringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}

func requiredEventIdempotencyKey(value, field string) (string, error) {
	if value == "" {
		return "", &Error{
			Code:    "MEDALLION_MISSING_IDEMPOTENCY_KEY",
			Message: field + " is required for retry-safe delivery",
		}
	}
	if len(value) > 512 || !utf8.ValidString(value) {
		return "", &Error{
			Code:    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
			Message: field + " must be valid UTF-8 between 1 and 512 bytes",
		}
	}
	return value, nil
}
