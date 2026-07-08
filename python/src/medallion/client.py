from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

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
    AuditTrailEvent,
    AuditTrailResponse,
    Datasource,
    EventRecordResponse,
    PublishedEventResult,
    RegisterDatasourceResponse,
    ResourceInput,
)

CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService"
REGISTER_CONNECTOR = f"{CONNECT_SERVICE}/RegisterConnector"
PUBLISH_CDC_EVENTS = f"{CONNECT_SERVICE}/PublishCdcEvents"
LIST_CDC_EVENTS = f"{CONNECT_SERVICE}/ListCdcEvents"


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
        self.events = EventsClient(self.connect, default_connector_id=default_connector_id)
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
        envelope = self._requests.post_proto(REGISTER_CONNECTOR, request, response)
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

    def register_datasource(
        self,
        *,
        name: str,
        type: str,
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
        )
        response, request_id = self.register_connector(request)
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

    def register(self, **kwargs: Any) -> RegisterDatasourceResponse:
        return self._connect.register_datasource(**kwargs)


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
        resource: ResourceInput,
        connector_id: str | None = None,
        before: Any = None,
        after: Any = None,
        metadata: Mapping[str, Any] | None = None,
        description: str | None = None,
        idempotency_key: str | None = None,
        source_event_id: object | None = None,
        stream_name: str | None = None,
        occurred_at: str | datetime | None = None,
    ) -> EventRecordResponse:
        connector = connector_id or self._default_connector_id
        if not connector or not connector.strip():
            raise MedallionError(
                "connector_id is required to record an audit event.",
                code="MEDALLION_MISSING_CONNECTOR_ID",
            )
        normalized_actor = normalize_actor(actor)
        normalized_resource = normalize_resource(resource)
        key = _idempotency_key(idempotency_key)
        event = connect_pb2.CdcEvent(
            stream_name=stream_name or "audit_log",
            entity_type=normalized_resource["type"],
            entity_id=normalized_resource["id"],
            idempotency_key=key,
            actor_principal=actor_principal_from_ref(normalized_actor),
            payload_json=_json_string(
                {
                    "actor": normalized_actor,
                    "resource": normalized_resource,
                    "before": before,
                    "after": after,
                    "metadata": metadata,
                }
            ),
            description=description or "",
            source_event_id=(
                normalize_id(source_event_id, "audit.sourceEventId")
                if source_event_id is not None
                else ""
            ),
            kind=connect_pb2.EVENT_KIND_AUDIT,
            action=action,
        )
        timestamp = _timestamp(occurred_at, "audit.occurred_at")
        if timestamp is not None:
            event.occurred_at.CopyFrom(timestamp)
        response, request_id = self._connect.publish_cdc_events(
            connect_pb2.PublishCdcEventsRequest(
                connector_id=connector,
                events=[event],
            ),
            idempotency_key=key,
        )
        return _event_response(response, key, request_id)

    def trail(
        self,
        *,
        resource_id: object,
        resource_type: str | None = None,
        actor: ActorInput | None = None,
        ingester_principal: str | None = None,
        action: str | None = None,
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
        source_actor = normalize_actor(actor) if actor is not None else None
        request = connect_pb2.ListCdcEventsRequest(
            organization_id=org,
            connector_id=connector_id or self._default_connector_id or "",
            entity_type=resource_type or "",
            entity_id=normalize_id(resource_id, "audit.resourceId"),
            limit=limit if limit is not None else page_size or 0,
            kind=connect_pb2.EVENT_KIND_AUDIT,
            actor_principal=(
                actor_principal_from_ref(source_actor) if source_actor else ""
            ),
            ingested_by_principal=(ingester_principal or "").strip(),
            action=action or "",
            page_cursor=cursor or "",
        )
        response, request_id = self._connect.list_cdc_events(request)
        events = [
            event
            for item in response.events
            if _is_audit_event(item)
            for event in [_audit_event_from_connect(item)]
            if source_actor is None or same_actor(event.actor, source_actor)
        ]
        return AuditTrailResponse(
            events=events,
            next_cursor=response.next_page_cursor or None,
            request_id=request_id,
            proto=response,
        )


class EventsClient:
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
        type: str,
        connector_id: str | None = None,
        actor: ActorInput | None = None,
        resource: ResourceInput | None = None,
        payload: Any = None,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        source_event_id: object | None = None,
        stream_name: str | None = None,
        occurred_at: str | datetime | None = None,
    ) -> EventRecordResponse:
        connector = connector_id or self._default_connector_id
        if not connector or not connector.strip():
            raise MedallionError(
                "connector_id is required to record an event.",
                code="MEDALLION_MISSING_CONNECTOR_ID",
            )
        key = _idempotency_key(idempotency_key)
        normalized_actor = normalize_actor(actor) if actor is not None else None
        normalized_resource = (
            normalize_resource(resource)
            if resource is not None
            else {"type": "event", "id": idempotency_key or type}
        )
        event = connect_pb2.CdcEvent(
            stream_name=stream_name or "events",
            entity_type=normalized_resource["type"],
            entity_id=normalized_resource["id"],
            idempotency_key=key,
            actor_principal=(
                actor_principal_from_ref(normalized_actor) if normalized_actor else ""
            ),
            payload_json=_json_string(
                {
                    "type": type,
                    "actor": normalized_actor,
                    "resource": normalized_resource,
                    "payload": payload,
                    "metadata": metadata,
                }
            ),
            source_event_id=(
                normalize_id(source_event_id, "event.sourceEventId")
                if source_event_id is not None
                else ""
            ),
            kind=connect_pb2.EVENT_KIND_AUDIT,
            action=type,
        )
        timestamp = _timestamp(occurred_at, "event.occurred_at")
        if timestamp is not None:
            event.occurred_at.CopyFrom(timestamp)
        response, request_id = self._connect.publish_cdc_events(
            connect_pb2.PublishCdcEventsRequest(connector_id=connector, events=[event]),
            idempotency_key=key,
        )
        return _event_response(response, key, request_id)


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
        connector_id: str | None = None,
        entity_type: str | None = None,
        entity_id: object | None = None,
        actor: ActorInput | None = None,
        before: Any = None,
        after: Any = None,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        source_event_id: object | None = None,
        occurred_at: str | datetime | None = None,
    ) -> EventRecordResponse:
        connector = connector_id or self._default_connector_id
        if not connector or not connector.strip():
            raise MedallionError(
                "connector_id is required to record a CDC event.",
                code="MEDALLION_MISSING_CONNECTOR_ID",
            )
        key = _idempotency_key(idempotency_key)
        normalized_primary_key = normalize_id_record(primary_key)
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
            kind=connect_pb2.EVENT_KIND_CDC,
        )
        timestamp = _timestamp(occurred_at, "cdc.occurred_at")
        if timestamp is not None:
            event.occurred_at.CopyFrom(timestamp)
        response, request_id = self._connect.publish_cdc_events(
            connect_pb2.PublishCdcEventsRequest(connector_id=connector, events=[event]),
            idempotency_key=key,
        )
        return _event_response(response, key, request_id)


def _event_response(
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


def _audit_event_from_connect(item: connect_pb2.CdcEvent) -> AuditTrailEvent:
    payload = _parse_payload(item.payload_json)
    payload_record = _mapping(payload)
    actor = _actor_from_payload(payload_record.get("actor")) or actor_from_principal(
        item.actor_principal or None
    )
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
        target_type=item.entity_type or None,
        target_id=item.entity_id or None,
        entity_type=item.entity_type or None,
        entity_id=item.entity_id or None,
        metadata=_mapping_or_none(payload_record.get("metadata")),
        created_at=_timestamp_string(item.observed_at),
        occurred_at=_timestamp_string(item.occurred_at),
        observed_at=_timestamp_string(item.observed_at),
        before=payload_record.get("before"),
        after=payload_record.get("after"),
        source_event_id=item.source_event_id or None,
        payload=payload,
        proto=item,
    )


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
        created_at=_timestamp_string(connector.created_at),
        updated_at=_timestamp_string(connector.updated_at),
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


def _is_audit_event(item: connect_pb2.CdcEvent) -> bool:
    return item.kind == connect_pb2.EVENT_KIND_AUDIT or bool(item.action)


def _idempotency_key(value: str | None) -> str:
    if value is not None and value.strip():
        return value
    return str(uuid.uuid4())


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
        parsed = parsed.replace(tzinfo=timezone.utc)
    timestamp = Timestamp()
    timestamp.FromDatetime(parsed)
    return timestamp


def _timestamp_string(value: Timestamp) -> str | None:
    if value.seconds == 0 and value.nanos == 0:
        return None
    return value.ToDatetime(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


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
    if isinstance(primary_key.get("id"), str):
        return primary_key["id"]
    keys = sorted(primary_key.keys())
    if len(keys) == 1:
        return primary_key[keys[0]]
    return "|".join(f"{key}={primary_key[key]}" for key in keys)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None
