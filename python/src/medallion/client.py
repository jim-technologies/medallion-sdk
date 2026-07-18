from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from medallion.connect.v1 import connect_pb2

from .errors import MedallionError
from .ids import (
    actor_from_principal,
    actor_principal_from_ref,
    normalize_actor,
    normalize_id,
    normalize_id_record,
    normalize_resource,
    same_actor,
)
from .request import RequestClient
from .tracing import TracingConfig
from .types import (
    ActorInput,
    ActorRef,
    AuditOrigin,
    AuditOutcome,
    AuditRecordResponse,
    AuditTrailEvent,
    AuditTrailResponse,
    Datasource,
    EventRecordResponse,
    PublishedAuditEventResult,
    PublishedEventResult,
    RegisterDatasourceResponse,
    ResourceInput,
)

CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService"
REGISTER_CONNECTOR = f"{CONNECT_SERVICE}/RegisterConnector"
PUBLISH_CDC_EVENTS = f"{CONNECT_SERVICE}/PublishCdcEvents"
LIST_CDC_EVENTS = f"{CONNECT_SERVICE}/ListCdcEvents"
PUBLISH_AUDIT_EVENTS = f"{CONNECT_SERVICE}/PublishAuditEvents"
LIST_AUDIT_EVENTS = f"{CONNECT_SERVICE}/ListAuditEvents"


class MedallionClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str | None = None,
        access_token: str | None = None,
        connect_base_url: str | None = None,
        organization_id: str | None = None,
        tenant_id: str | None = None,
        default_connector_id: str | None = None,
        timeout: float = 30.0,
        tracing: bool | TracingConfig | None = None,
    ) -> None:
        requests = RequestClient(
            base_url=connect_base_url or base_url,
            api_key=api_key,
            access_token=access_token,
            timeout=timeout,
            tracing=tracing,
        )
        org = organization_id or tenant_id
        self.connect = ConnectClient(requests, organization_id=org)
        self.audit = AuditClient(
            self.connect,
            organization_id=org,
            default_connector_id=default_connector_id,
        )
        self.cdc = CdcClient(self.connect, default_connector_id=default_connector_id)
        self.datasources = DatasourcesClient(self.connect)


class ConnectClient:
    def __init__(
        self,
        requests: RequestClient,
        *,
        organization_id: str | None = None,
    ) -> None:
        self._requests = requests
        self._organization_id = organization_id

    def register_connector(
        self,
        request: connect_pb2.RegisterConnectorRequest,
    ) -> tuple[connect_pb2.RegisterConnectorResponse, str | None]:
        response = connect_pb2.RegisterConnectorResponse()
        envelope = self._requests.post_proto(
            REGISTER_CONNECTOR,
            request,
            response,
            idempotency_key=request.idempotency_key,
        )
        return response, envelope.request_id

    def publish_cdc_events(
        self,
        request: connect_pb2.PublishCdcEventsRequest,
        *,
        idempotency_key: str | None = None,
    ) -> tuple[connect_pb2.PublishCdcEventsResponse, str | None]:
        response = connect_pb2.PublishCdcEventsResponse()
        envelope = self._requests.post_proto(
            PUBLISH_CDC_EVENTS,
            request,
            response,
            idempotency_key=idempotency_key,
        )
        return response, envelope.request_id

    def list_cdc_events(
        self,
        request: connect_pb2.ListCdcEventsRequest,
    ) -> tuple[connect_pb2.ListCdcEventsResponse, str | None]:
        response = connect_pb2.ListCdcEventsResponse()
        envelope = self._requests.post_proto(LIST_CDC_EVENTS, request, response)
        return response, envelope.request_id

    def publish_audit_events(
        self,
        request: connect_pb2.PublishAuditEventsRequest,
        *,
        idempotency_key: str | None = None,
    ) -> tuple[connect_pb2.PublishAuditEventsResponse, str | None]:
        response = connect_pb2.PublishAuditEventsResponse()
        envelope = self._requests.post_proto(
            PUBLISH_AUDIT_EVENTS,
            request,
            response,
            idempotency_key=idempotency_key,
        )
        return response, envelope.request_id

    def list_audit_events(
        self,
        request: connect_pb2.ListAuditEventsRequest,
    ) -> tuple[connect_pb2.ListAuditEventsResponse, str | None]:
        response = connect_pb2.ListAuditEventsResponse()
        envelope = self._requests.post_proto(LIST_AUDIT_EVENTS, request, response)
        return response, envelope.request_id

    def register_datasource(
        self,
        *,
        name: str,
        type: str,
        idempotency_key: str,
        display_name: str | None = None,
        organization_id: str | None = None,
        external_id: object | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RegisterDatasourceResponse:
        org = organization_id or self._organization_id
        if not org or not org.strip():
            raise MedallionError(
                "organization_id is required to register a datasource.",
                code="MEDALLION_MISSING_ORGANIZATION_ID",
            )
        control_key = _required_control_idempotency_key(
            idempotency_key, "datasource.idempotency_key"
        )
        request = connect_pb2.RegisterConnectorRequest(
            organization_id=org,
            kind=type,
            source_system=name,
            display_name=display_name or name,
            external_id=(
                normalize_id(external_id, "datasource.externalId")
                if external_id is not None
                else ""
            ),
            idempotency_key=control_key,
        )
        response, request_id = self.register_connector(request)
        if not response.connector.id.strip():
            raise MedallionError(
                "Medallion returned a datasource registration without a connector ID.",
                code="MEDALLION_INVALID_DATASOURCE_RESPONSE",
                request_id=request_id,
            )
        datasource = _datasource_from_connector(response.connector, metadata)
        return RegisterDatasourceResponse(
            request_id=request_id,
            datasource=datasource,
            connector=response.connector,
            proto=response,
        )


class DatasourcesClient:
    def __init__(self, connect: ConnectClient) -> None:
        self._connect = connect

    def register(
        self,
        *,
        name: str,
        type: str,
        idempotency_key: str,
        display_name: str | None = None,
        organization_id: str | None = None,
        external_id: object | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RegisterDatasourceResponse:
        return self._connect.register_datasource(
            name=name,
            type=type,
            idempotency_key=idempotency_key,
            display_name=display_name,
            organization_id=organization_id,
            external_id=external_id,
            metadata=metadata,
        )


class AuditClient:
    def __init__(
        self,
        connect: ConnectClient,
        *,
        organization_id: str | None = None,
        default_connector_id: str | None = None,
    ) -> None:
        self._connect = connect
        self._organization_id = organization_id
        self._default_connector_id = default_connector_id

    def record(
        self,
        *,
        actor: ActorInput,
        action: str,
        outcome: AuditOutcome,
        resource: ResourceInput,
        idempotency_key: str,
        connector_id: str | None = None,
        before: Any = None,
        after: Any = None,
        metadata: Mapping[str, Any] | None = None,
        description: str | None = None,
        evidence_url: str | None = None,
        source_event_id: object | None = None,
        occurred_at: str | datetime | None = None,
    ) -> AuditRecordResponse:
        connector = connector_id or self._default_connector_id
        if not connector or not connector.strip():
            raise MedallionError(
                "connector_id is required to record an audit event.",
                code="MEDALLION_MISSING_CONNECTOR_ID",
            )
        normalized_actor = normalize_actor(actor)
        normalized_resource = normalize_resource(resource)
        key = _required_event_idempotency_key(idempotency_key, "audit.idempotency_key")
        event = connect_pb2.AuditEvent(
            resource_type=normalized_resource["type"],
            resource_id=normalized_resource["id"],
            idempotency_key=key,
            actor_principal=actor_principal_from_ref(normalized_actor),
            payload_json=_json_string(
                {
                    "actor": normalized_actor,
                    "resource": normalized_resource,
                    "before": before,
                    "after": after,
                    "metadata": metadata,
                    "evidenceUrl": evidence_url,
                }
            ),
            description=description or "",
            source_event_id=(
                normalize_id(source_event_id, "audit.sourceEventId")
                if source_event_id is not None
                else ""
            ),
            action=action,
            outcome=_audit_outcome(outcome),
        )
        timestamp = _timestamp(occurred_at, "audit.occurred_at")
        if timestamp is not None:
            event.occurred_at.CopyFrom(timestamp)
        response, request_id = self._connect.publish_audit_events(
            connect_pb2.PublishAuditEventsRequest(
                connector_id=connector,
                events=[event],
            ),
            idempotency_key=key,
        )
        return _audit_event_response(response, key, request_id)

    def trail(
        self,
        *,
        resource_id: object,
        resource_type: str,
        actor: ActorInput | None = None,
        ingester_principal: str | None = None,
        action: str | None = None,
        origin: AuditOrigin | None = None,
        outcome: AuditOutcome | None = None,
        organization_id: str | None = None,
        connector_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        page_size: int | None = None,
    ) -> AuditTrailResponse:
        org = organization_id or self._organization_id
        if not org or not org.strip():
            raise MedallionError(
                "organization_id is required to read an audit trail.",
                code="MEDALLION_MISSING_ORGANIZATION_ID",
            )
        if not resource_type.strip():
            raise MedallionError(
                "resource_type is required to read an audit trail.",
                code="MEDALLION_MISSING_RESOURCE_TYPE",
            )
        source_actor = normalize_actor(actor) if actor is not None else None
        trail_limit = _audit_trail_limit(limit, page_size)
        request = connect_pb2.ListAuditEventsRequest(
            organization_id=org,
            connector_id=connector_id or self._default_connector_id or "",
            resource_type=resource_type.strip(),
            resource_id=normalize_id(resource_id, "audit.resourceId"),
            limit=trail_limit,
            actor_principal=(
                actor_principal_from_ref(source_actor) if source_actor else ""
            ),
            ingested_by_principal=(ingester_principal or "").strip(),
            action=action or "",
            page_cursor=cursor or "",
            origin=_audit_origin(origin),
            outcome=_audit_outcome(outcome, optional=True),
        )
        response, request_id = self._connect.list_audit_events(request)
        events = [
            event
            for item in response.events
            for event in [_audit_event_from_connect(item)]
            if source_actor is None or same_actor(event.actor, source_actor)
        ]
        return AuditTrailResponse(
            events=events,
            next_cursor=response.next_page_cursor or None,
            request_id=request_id,
            proto=response,
        )


class CdcClient:
    def __init__(
        self,
        connect: ConnectClient,
        *,
        default_connector_id: str | None = None,
    ) -> None:
        self._connect = connect
        self._default_connector_id = default_connector_id

    def record(
        self,
        *,
        source: str,
        table: str,
        operation: str,
        primary_key: Mapping[str, object],
        idempotency_key: str,
        connector_id: str | None = None,
        entity_type: str | None = None,
        entity_id: object | None = None,
        actor: ActorInput | None = None,
        before: Any = None,
        after: Any = None,
        metadata: Mapping[str, Any] | None = None,
        source_event_id: object | None = None,
        occurred_at: str | datetime | None = None,
    ) -> EventRecordResponse:
        connector = connector_id or self._default_connector_id
        if not connector or not connector.strip():
            raise MedallionError(
                "connector_id is required to record a CDC event.",
                code="MEDALLION_MISSING_CONNECTOR_ID",
            )
        key = _required_event_idempotency_key(idempotency_key, "cdc.idempotency_key")
        normalized_primary_key = normalize_id_record(primary_key)
        if not normalized_primary_key:
            raise MedallionError(
                "cdc.primary_key must contain at least one field.",
                code="MEDALLION_EMPTY_CDC_PRIMARY_KEY",
            )
        if len(normalized_primary_key) > 1 and entity_id is None:
            raise MedallionError(
                "cdc.entity_id is required when cdc.primary_key contains more than one field.",
                code="MEDALLION_MISSING_CDC_ENTITY_ID",
            )
        normalized_actor = normalize_actor(actor) if actor is not None else None
        cdc_entity_id = (
            normalize_id(entity_id, "cdc.entityId")
            if entity_id is not None
            else _entity_id_from_primary_key(normalized_primary_key)
        )
        cdc_operation = _cdc_operation(operation)
        event = connect_pb2.CdcEvent(
            stream_name=table,
            entity_type=entity_type or table,
            entity_id=cdc_entity_id,
            operation=cdc_operation,
            source_event_id=(
                normalize_id(source_event_id, "cdc.sourceEventId")
                if source_event_id is not None
                else ""
            ),
            idempotency_key=key,
            actor_principal=(
                actor_principal_from_ref(normalized_actor) if normalized_actor else ""
            ),
            payload_json=_json_string(
                {
                    "source": source,
                    "table": table,
                    "actor": normalized_actor,
                    "primaryKey": normalized_primary_key,
                    "before": before,
                    "after": after,
                    "metadata": metadata,
                }
            ),
        )
        timestamp = _timestamp(occurred_at, "cdc.occurred_at")
        if timestamp is not None:
            event.occurred_at.CopyFrom(timestamp)
        response, request_id = self._connect.publish_cdc_events(
            connect_pb2.PublishCdcEventsRequest(connector_id=connector, events=[event]),
            idempotency_key=key,
        )
        return _cdc_event_response(response, key, request_id)


def _cdc_event_response(
    body: connect_pb2.PublishCdcEventsResponse,
    fallback_idempotency_key: str,
    request_id: str | None,
) -> EventRecordResponse:
    events = [
        PublishedEventResult(
            idempotency_key=item.idempotency_key or fallback_idempotency_key,
            event_id=str(item.event_id) if item.event_id else None,
            duplicate=item.duplicate,
            proto=item,
        )
        for item in body.events
    ]
    first = events[0] if events else None
    if not events and body.accepted_count == 0 and body.duplicate_count == 0:
        raise MedallionError(
            "Medallion returned an empty event publish acknowledgement.",
            code="MEDALLION_INVALID_PUBLISH_RESPONSE",
            request_id=request_id,
        )
    duplicate = (
        first.duplicate
        if first is not None
        else body.duplicate_count > 0 and body.accepted_count == 0
    )
    return EventRecordResponse(
        request_id=request_id,
        idempotency_key=(
            first.idempotency_key if first is not None else fallback_idempotency_key
        ),
        duplicate=duplicate,
        result="duplicate" if duplicate else "accepted",
        accepted_count=body.accepted_count,
        duplicate_count=body.duplicate_count,
        events=events,
        proto=body,
    )


def _audit_event_response(
    body: connect_pb2.PublishAuditEventsResponse,
    fallback_idempotency_key: str,
    request_id: str | None,
) -> AuditRecordResponse:
    events = [
        PublishedAuditEventResult(
            idempotency_key=item.idempotency_key or fallback_idempotency_key,
            event_id=str(item.event_id) if item.event_id else None,
            duplicate=item.duplicate,
            proto=item,
        )
        for item in body.events
    ]
    first = events[0] if events else None
    if not events and body.accepted_count == 0 and body.duplicate_count == 0:
        raise MedallionError(
            "Medallion returned an empty event publish acknowledgement.",
            code="MEDALLION_INVALID_PUBLISH_RESPONSE",
            request_id=request_id,
        )
    duplicate = (
        first.duplicate
        if first is not None
        else body.duplicate_count > 0 and body.accepted_count == 0
    )
    return AuditRecordResponse(
        request_id=request_id,
        idempotency_key=(
            first.idempotency_key if first is not None else fallback_idempotency_key
        ),
        duplicate=duplicate,
        result="duplicate" if duplicate else "accepted",
        accepted_count=body.accepted_count,
        duplicate_count=body.duplicate_count,
        events=events,
        proto=body,
    )


def _audit_event_from_connect(item: connect_pb2.AuditEvent) -> AuditTrailEvent:
    payload = _parse_payload(item.payload_json)
    payload_record = _mapping(payload)
    actor = actor_from_principal(item.actor_principal or None)
    payload_actor = _actor_from_payload(payload_record.get("actor"))
    if (
        payload_actor is not None
        and actor_principal_from_ref(normalize_actor(payload_actor))
        == item.actor_principal
    ):
        actor = payload_actor
    event_id = str(item.id) if item.id else None
    return AuditTrailEvent(
        id=event_id,
        event_id=event_id,
        organization_id=item.organization_id or None,
        connector_id=item.connector_id or None,
        actor=actor,
        ingester_principal=item.ingested_by_principal or None,
        actor_principal=item.actor_principal or None,
        action=item.action or None,
        target_type=item.resource_type or None,
        target_id=item.resource_id or None,
        entity_type=item.resource_type or None,
        entity_id=item.resource_id or None,
        metadata=_mapping_or_none(payload_record.get("metadata")),
        created_at=_timestamp_string(
            item.observed_at if item.HasField("observed_at") else None
        ),
        occurred_at=_timestamp_string(
            item.occurred_at if item.HasField("occurred_at") else None
        ),
        observed_at=_timestamp_string(
            item.observed_at if item.HasField("observed_at") else None
        ),
        before=payload_record.get("before"),
        after=payload_record.get("after"),
        evidence_url=_string(payload_record.get("evidenceUrl")),
        source_event_id=item.source_event_id or None,
        source_system=item.source_system or None,
        origin=_audit_origin_from_connect(item.origin),
        outcome=_audit_outcome_from_connect(item.outcome),
        payload=payload,
        proto=item,
    )


def _audit_outcome(
    value: AuditOutcome | None,
    *,
    optional: bool = False,
) -> int:
    outcomes = {
        "succeeded": connect_pb2.AUDIT_EVENT_OUTCOME_SUCCEEDED,
        "failed": connect_pb2.AUDIT_EVENT_OUTCOME_FAILED,
        "indeterminate": connect_pb2.AUDIT_EVENT_OUTCOME_INDETERMINATE,
    }
    if value is None and optional:
        return connect_pb2.AUDIT_EVENT_OUTCOME_UNSPECIFIED
    try:
        return outcomes[value]  # type: ignore[index]
    except KeyError, TypeError:
        raise MedallionError(
            "audit outcome must be succeeded, failed, or indeterminate.",
            code="MEDALLION_INVALID_AUDIT_OUTCOME",
        ) from None


def _audit_origin(value: AuditOrigin | None) -> int:
    origins = {
        None: connect_pb2.AUDIT_EVENT_ORIGIN_UNSPECIFIED,
        "external_provider": connect_pb2.AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER,
        "connect": connect_pb2.AUDIT_EVENT_ORIGIN_CONNECT,
    }
    try:
        return origins[value]
    except KeyError:
        raise MedallionError(
            "audit origin must be external_provider or connect.",
            code="MEDALLION_INVALID_AUDIT_ORIGIN",
        ) from None


def _audit_outcome_from_connect(value: int) -> AuditOutcome | None:
    return {
        connect_pb2.AUDIT_EVENT_OUTCOME_SUCCEEDED: "succeeded",
        connect_pb2.AUDIT_EVENT_OUTCOME_FAILED: "failed",
        connect_pb2.AUDIT_EVENT_OUTCOME_INDETERMINATE: "indeterminate",
    }.get(value)


def _audit_origin_from_connect(value: int) -> AuditOrigin | None:
    return {
        connect_pb2.AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER: "external_provider",
        connect_pb2.AUDIT_EVENT_ORIGIN_CONNECT: "connect",
    }.get(value)


def _datasource_from_connector(
    connector: connect_pb2.Connector,
    metadata: Mapping[str, Any] | None,
) -> Datasource:
    return Datasource(
        id=connector.id,
        organization_id=connector.organization_id or None,
        kind=connector.kind or None,
        type=connector.kind or None,
        source_system=connector.source_system or None,
        name=connector.source_system or None,
        display_name=connector.display_name or None,
        external_id=connector.external_id or None,
        status=connect_pb2.LifecycleStatus.Name(connector.status),
        created_at=_timestamp_string(
            connector.created_at if connector.HasField("created_at") else None
        ),
        updated_at=_timestamp_string(
            connector.updated_at if connector.HasField("updated_at") else None
        ),
        metadata=metadata,
        proto=connector,
    )


def _actor_from_payload(value: Any) -> ActorRef | None:
    record = _mapping(value)
    raw_id = record.get("id")
    if raw_id is None:
        return None
    try:
        normalized_id = normalize_id(raw_id, "actor.id")
    except MedallionError:
        return None
    return ActorRef(
        id=normalized_id,
        type=_string(record.get("type")),
        provider=_string(record.get("provider")),
    )


def _required_event_idempotency_key(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise MedallionError(
            f"{field} is required for retry-safe delivery.",
            code="MEDALLION_MISSING_IDEMPOTENCY_KEY",
        )
    normalized = value.strip()
    if not normalized:
        raise MedallionError(
            f"{field} is required for retry-safe delivery.",
            code="MEDALLION_MISSING_IDEMPOTENCY_KEY",
        )
    if len(normalized.encode("utf-8")) > 512:
        raise MedallionError(
            f"{field} must not exceed 512 bytes.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    return normalized


def _required_control_idempotency_key(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise MedallionError(
            f"{field} is required for retry-safe mutation.",
            code="MEDALLION_MISSING_IDEMPOTENCY_KEY",
        )
    if len(value.encode("utf-8")) > 256 or any(
        ord(character) < 0x21 or ord(character) > 0x7E for character in value
    ):
        raise MedallionError(
            f"{field} must be at most 256 bytes of visible ASCII without spaces.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    return value


def _audit_trail_limit(limit: int | None, page_size: int | None) -> int:
    value = limit if limit is not None else page_size
    if value is None or value == 0:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MedallionError(
            "audit.trail limit/page_size must be zero or a positive integer.",
            code="MEDALLION_INVALID_AUDIT_TRAIL_LIMIT",
        )
    if value > 500:
        raise MedallionError(
            "audit.trail limit/page_size must be 500 or less; use cursor pagination for larger reads.",
            code="MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE",
        )
    return value


def _timestamp(value: str | datetime | None, path: str) -> Timestamp | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MedallionError(
                f"{path} must be an RFC3339 timestamp.",
                code="MEDALLION_INVALID_TIMESTAMP",
            ) from exc
    else:
        parsed = value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    timestamp = Timestamp()
    timestamp.FromDatetime(parsed)
    return timestamp


def _timestamp_string(value: Timestamp | None) -> str | None:
    if value is None:
        return None
    return value.ToDatetime(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def _json_string(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(value, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise MedallionError(
            "Medallion event payload must be JSON serializable.",
            code="MEDALLION_INVALID_JSON_BODY",
        ) from exc


def _parse_payload(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _cdc_operation(value: str) -> int:
    operations = {
        "insert": connect_pb2.CDC_OPERATION_INSERT,
        "update": connect_pb2.CDC_OPERATION_UPDATE,
        "delete": connect_pb2.CDC_OPERATION_DELETE,
        "snapshot": connect_pb2.CDC_OPERATION_SNAPSHOT,
    }
    try:
        return operations[value]
    except KeyError as exc:
        raise MedallionError(
            "operation must be insert, update, delete, or snapshot.",
            code="MEDALLION_INVALID_CDC_OPERATION",
        ) from exc


def _entity_id_from_primary_key(primary_key: Mapping[str, str]) -> str:
    if not primary_key:
        raise MedallionError(
            "cdc.primary_key must contain at least one field.",
            code="MEDALLION_EMPTY_CDC_PRIMARY_KEY",
        )
    return next(iter(primary_key.values()))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None
