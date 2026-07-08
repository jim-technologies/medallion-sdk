package medallion

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func eventResponse(body *connectv1.PublishCdcEventsResponse, fallbackKey, requestID string) EventRecordResponse {
	results := make([]PublishedEventResult, 0, len(body.GetEvents()))
	for _, item := range body.GetEvents() {
		eventID := ""
		if item.GetEventId() != 0 {
			eventID = strconv.FormatInt(item.GetEventId(), 10)
		}
		results = append(results, PublishedEventResult{
			IdempotencyKey: firstNonEmpty(item.GetIdempotencyKey(), fallbackKey),
			EventID:        eventID,
			Duplicate:      item.GetDuplicate(),
			Proto:          item,
		})
	}
	duplicate := body.GetDuplicateCount() > 0 && body.GetAcceptedCount() == 0
	if len(results) > 0 {
		duplicate = results[0].Duplicate
	}
	return EventRecordResponse{
		RequestID:      requestID,
		IdempotencyKey: firstEventKey(results, fallbackKey),
		Duplicate:      duplicate,
		Result:         resultString(duplicate),
		AcceptedCount:  body.GetAcceptedCount(),
		DuplicateCount: body.GetDuplicateCount(),
		Events:         results,
		Proto:          body,
	}
}

func firstEventKey(events []PublishedEventResult, fallbackKey string) string {
	if len(events) == 0 || events[0].IdempotencyKey == "" {
		return fallbackKey
	}
	return events[0].IdempotencyKey
}

func resultString(duplicate bool) string {
	if duplicate {
		return "duplicate"
	}
	return "accepted"
}

func auditEventFromConnect(item *connectv1.CdcEvent) AuditTrailEvent {
	payload := parsePayload(item.GetPayloadJson())
	payloadRecord := asMap(payload)
	actor := actorFromPayload(payloadRecord["actor"])
	if actor == nil {
		actor = actorFromPrincipal(item.GetActorPrincipal())
	}
	eventID := ""
	if item.GetId() != 0 {
		eventID = strconv.FormatInt(item.GetId(), 10)
	}
	return AuditTrailEvent{
		ID:                eventID,
		EventID:           eventID,
		OrganizationID:    item.GetOrganizationId(),
		ConnectorID:       item.GetConnectorId(),
		Actor:             actor,
		IngesterPrincipal: item.GetIngestedByPrincipal(),
		ActorPrincipal:    item.GetActorPrincipal(),
		Action:            item.GetAction(),
		TargetType:        item.GetEntityType(),
		TargetID:          item.GetEntityId(),
		EntityType:        item.GetEntityType(),
		EntityID:          item.GetEntityId(),
		Metadata:          mapStringAny(payloadRecord["metadata"]),
		CreatedAt:         timestampString(item.GetObservedAt()),
		OccurredAt:        timestampString(item.GetOccurredAt()),
		ObservedAt:        timestampString(item.GetObservedAt()),
		Before:            payloadRecord["before"],
		After:             payloadRecord["after"],
		SourceEventID:     item.GetSourceEventId(),
		Payload:           payload,
		Proto:             item,
	}
}

func datasourceFromConnector(connector *connectv1.Connector, metadata map[string]any) Datasource {
	return Datasource{
		ID:             connector.GetId(),
		OrganizationID: connector.GetOrganizationId(),
		Kind:           connector.GetKind(),
		Type:           connector.GetKind(),
		SourceSystem:   connector.GetSourceSystem(),
		Name:           connector.GetSourceSystem(),
		DisplayName:    connector.GetDisplayName(),
		ExternalID:     connector.GetExternalId(),
		Status:         connector.GetStatus().String(),
		CreatedAt:      timestampString(connector.GetCreatedAt()),
		UpdatedAt:      timestampString(connector.GetUpdatedAt()),
		Metadata:       metadata,
		Proto:          connector,
	}
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
	var out any
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return nil
	}
	return out
}

func isAuditEvent(item *connectv1.CdcEvent) bool {
	return item.GetKind() == connectv1.EventKind_EVENT_KIND_AUDIT || item.GetAction() != ""
}

func jsonString(value map[string]any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", &Error{Code: "MEDALLION_INVALID_JSON_BODY", Message: "Medallion event payload must be JSON serializable"}
	}
	return string(raw), nil
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

func entityIDFromPrimaryKey(primaryKey map[string]string) string {
	if id, ok := primaryKey["id"]; ok {
		return id
	}
	keys := make([]string, 0, len(primaryKey))
	for key := range primaryKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 1 {
		return primaryKey[keys[0]]
	}
	out := ""
	for index, key := range keys {
		if index > 0 {
			out += "|"
		}
		out += key + "=" + primaryKey[key]
	}
	return out
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
	return timestamppb.New(parsed), nil
}

func timestampString(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
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

func idempotencyKey(value string) string {
	if value != "" {
		return value
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "idem"
	}
	return hex.EncodeToString(raw[:])
}
