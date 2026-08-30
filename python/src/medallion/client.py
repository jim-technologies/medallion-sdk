from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from threading import Event
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
from .request import RetryConfig, _canonical_workspace_id, _RequestClient
from .tables import IngestClient, TablesClient
from .tracing import TracingConfig
from .types import (
    PAYLOAD_UNSET,
    ActorInput,
    ActorRef,
    AuditEventInput,
    AuditOrigin,
    AuditOutcome,
    AuditRecordResponse,
    AuditTrailEvent,
    AuditTrailResponse,
    CdcEventInput,
    CdcPage,
    CdcReadEvent,
    EventRecordResponse,
    PublishedAuditEventResult,
    PublishedEventResult,
    ResourceInput,
)

CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService"
PUBLISH_CDC_EVENTS = f"{CONNECT_SERVICE}/PublishCdcEvents"
LIST_CDC_EVENTS = f"{CONNECT_SERVICE}/ListCdcEvents"
PUBLISH_AUDIT_EVENTS = f"{CONNECT_SERVICE}/PublishAuditEvents"
LIST_AUDIT_EVENTS = f"{CONNECT_SERVICE}/ListAuditEvents"
MAX_ITERATOR_PAGES = 10_000
MIN_ANALYTICAL_TIMESTAMP = (-2_208_988_800, 0)
MAX_ANALYTICAL_TIMESTAMP = (9_223_372_036, 854_775_807)
MIN_PROTOBUF_TIMESTAMP_SECONDS = -62_135_596_800
MAX_PROTOBUF_TIMESTAMP_SECONDS = 253_402_300_799


class MedallionClient:
    def __init__(
        self,
        *,
        base_url: str,
        workspace_id: str,
        api_key: str | None = None,
        access_token: str | None = None,
        default_connector_id: str | None = None,
        timeout: float = 30.0,
        retry: RetryConfig | None = None,
        tracing: bool | TracingConfig | None = None,
    ) -> None:
        workspace = _canonical_workspace_id(workspace_id)
        requests = _RequestClient(
            base_url=base_url,
            workspace_id=workspace,
            api_key=api_key,
            access_token=access_token,
            timeout=timeout,
            retry=retry,
            tracing=tracing,
        )
        self.ingest = IngestClient(requests)
        self.tables = TablesClient(self.ingest)
        self.connect = ConnectClient(requests)
        self.audit = AuditClient(
            self.connect,
            default_connector_id=default_connector_id,
        )
        self.cdc = CdcClient(
            self.connect,
            default_connector_id=default_connector_id,
        )

    @property
    def workspace_id(self) -> str:
        """The immutable workspace selected for this client."""

        return self.connect.workspace_id


class ConnectClient:
    def __init__(
        self,
        requests: _RequestClient,
    ) -> None:
        self._requests = requests

    @property
    def workspace_id(self) -> str:
        return self._requests.workspace_id

    def publish_cdc_events(
        self,
        request: connect_pb2.PublishCdcEventsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[connect_pb2.PublishCdcEventsResponse, str | None]:
        _required_text(request.connector_id, "connector_id", max_bytes=128)
        _batch_size(request.events, "cdc")
        seen: set[str] = set()
        for event in request.events:
            _validate_customer_cdc_proto(event)
            if event.idempotency_key in seen:
                raise MedallionError(
                    "CDC publish batch repeats an idempotency key.",
                    code="MEDALLION_DUPLICATE_IDEMPOTENCY_KEY",
                )
            seen.add(event.idempotency_key)
        response = connect_pb2.PublishCdcEventsResponse()
        envelope = self._requests._post_proto(
            PUBLISH_CDC_EVENTS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_publish_response(
            [event.idempotency_key for event in request.events],
            [event.idempotency_key for event in response.events],
            [_positive_receipt_event_id(event.event_id) for event in response.events],
            [event.duplicate for event in response.events],
            response.accepted_count,
            response.duplicate_count,
            envelope.request_id,
        )
        return response, envelope.request_id

    def list_cdc_events(
        self,
        request: connect_pb2.ListCdcEventsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[connect_pb2.ListCdcEventsResponse, str | None]:
        request = _prepare_list_cdc_request(request, self.workspace_id)
        response = connect_pb2.ListCdcEventsResponse()
        envelope = self._requests._post_proto(
            LIST_CDC_EVENTS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_list_cursor(response.next_page_cursor, envelope.request_id)
        for index, event in enumerate(response.events):
            _cdc_event_from_connect_strict(
                event,
                index,
                envelope.request_id,
                self.workspace_id,
            )
        return response, envelope.request_id

    def publish_audit_events(
        self,
        request: connect_pb2.PublishAuditEventsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[connect_pb2.PublishAuditEventsResponse, str | None]:
        _required_text(request.connector_id, "connector_id", max_bytes=128)
        _batch_size(request.events, "audit")
        seen: set[str] = set()
        for event in request.events:
            _validate_customer_audit_proto(event)
            if event.idempotency_key in seen:
                raise MedallionError(
                    "Audit publish batch repeats an idempotency key.",
                    code="MEDALLION_DUPLICATE_IDEMPOTENCY_KEY",
                )
            seen.add(event.idempotency_key)
        response = connect_pb2.PublishAuditEventsResponse()
        envelope = self._requests._post_proto(
            PUBLISH_AUDIT_EVENTS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_publish_response(
            [event.idempotency_key for event in request.events],
            [event.idempotency_key for event in response.events],
            [_positive_receipt_event_id(event.event_id) for event in response.events],
            [event.duplicate for event in response.events],
            response.accepted_count,
            response.duplicate_count,
            envelope.request_id,
        )
        return response, envelope.request_id

    def list_audit_events(
        self,
        request: connect_pb2.ListAuditEventsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[connect_pb2.ListAuditEventsResponse, str | None]:
        request = _prepare_list_audit_request(request, self.workspace_id)
        response = connect_pb2.ListAuditEventsResponse()
        envelope = self._requests._post_proto(
            LIST_AUDIT_EVENTS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_list_cursor(response.next_page_cursor, envelope.request_id)
        for index, event in enumerate(response.events):
            _audit_event_from_connect_strict(
                event,
                index,
                envelope.request_id,
                self.workspace_id,
            )
        return response, envelope.request_id


class AuditClient:
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
        action: str,
        outcome: AuditOutcome,
        idempotency_key: str,
        resource: ResourceInput | None = None,
        resource_type: str | None = None,
        resource_id: object | None = None,
        actor: ActorInput | None = None,
        connector_id: str | None = None,
        payload: Any = PAYLOAD_UNSET,
        payload_json: str | None = None,
        before: Any = None,
        after: Any = None,
        metadata: Mapping[str, Any] | None = None,
        description: str | None = None,
        evidence_url: str | None = None,
        source_event_id: object | None = None,
        occurred_at: str | datetime | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> AuditRecordResponse:
        if resource is not None and (
            resource_type is not None or resource_id is not None
        ):
            raise MedallionError(
                "Use either resource or resource_type/resource_id, not both.",
                code="MEDALLION_INVALID_AUDIT_EVENT",
            )
        if resource is not None:
            normalized_resource = normalize_resource(resource)
            resource_type = normalized_resource["type"]
            resource_id = normalized_resource["id"]
        if payload_json is None and payload is PAYLOAD_UNSET:
            payload = {
                "actor": normalize_actor(actor) if actor is not None else None,
                "resource": (
                    {
                        "type": resource_type,
                        "id": normalize_id(resource_id, "audit.resource_id"),
                    }
                    if resource_type and resource_id is not None
                    else None
                ),
                "before": before,
                "after": after,
                "metadata": metadata,
                "evidenceUrl": evidence_url,
            }
        return self.publish_batch(
            [
                AuditEventInput(
                    resource_type=resource_type or "",
                    resource_id=resource_id if resource_id is not None else "",
                    action=action,
                    outcome=outcome,
                    idempotency_key=idempotency_key,
                    payload=payload,
                    payload_json=payload_json,
                    source_event_id=source_event_id,
                    actor=actor,
                    occurred_at=occurred_at,
                    description=description,
                )
            ],
            connector_id=connector_id,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )

    def publish_batch(
        self,
        events: Sequence[AuditEventInput | Mapping[str, Any]],
        *,
        connector_id: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> AuditRecordResponse:
        connector = _connector_id(connector_id, self._default_connector_id, "audit")
        inputs = [_audit_input(item) for item in events]
        _batch_size(inputs, "audit")
        proto_events: list[connect_pb2.AuditEvent] = []
        keys: list[str] = []
        for index, item in enumerate(inputs):
            key = _required_event_idempotency_key(
                item.idempotency_key, f"audit.events[{index}].idempotency_key"
            )
            event = connect_pb2.AuditEvent(
                resource_type=_required_text(
                    item.resource_type,
                    f"audit.events[{index}].resource_type",
                    max_bytes=256,
                ),
                resource_id=_required_id(
                    item.resource_id,
                    f"audit.events[{index}].resource_id",
                    max_bytes=1024,
                ),
                idempotency_key=key,
                actor_principal=(
                    _actor_principal(item.actor, f"audit.events[{index}].actor")
                    if item.actor is not None
                    else ""
                ),
                payload_json=_payload_json(item.payload, item.payload_json),
                description=_optional_text(
                    item.description,
                    f"audit.events[{index}].description",
                    max_bytes=4096,
                ),
                source_event_id=(
                    _required_id(
                        item.source_event_id,
                        f"audit.events[{index}].source_event_id",
                        max_bytes=1024,
                    )
                    if item.source_event_id is not None
                    else ""
                ),
                action=_required_text(
                    item.action,
                    f"audit.events[{index}].action",
                    max_bytes=256,
                ),
                outcome=_audit_outcome(item.outcome),
            )
            timestamp = _timestamp(
                item.occurred_at, f"audit.events[{index}].occurred_at"
            )
            if timestamp is not None:
                event.occurred_at.CopyFrom(timestamp)
            proto_events.append(event)
            keys.append(key)
        response, request_id = self._connect.publish_audit_events(
            connect_pb2.PublishAuditEventsRequest(
                connector_id=connector,
                events=proto_events,
            ),
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _audit_event_response(response, keys, request_id)

    def list(
        self,
        *,
        resource_type: str | None = None,
        resource_id: object | None = None,
        actor: ActorInput | None = None,
        ingester_principal: str | None = None,
        action: str | None = None,
        origin: AuditOrigin | None = None,
        outcome: AuditOutcome | None = None,
        connector_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        page_size: int | None = None,
        occurred_at_from: str | datetime | None = None,
        occurred_at_to: str | datetime | None = None,
        source_system: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> AuditTrailResponse:
        if (resource_type is None) != (resource_id is None):
            raise MedallionError(
                "resource_type and resource_id must be provided together.",
                code="MEDALLION_INVALID_AUDIT_FILTER",
            )
        normalized_resource_type = (
            _required_text(resource_type, "audit.resource_type", max_bytes=256)
            if resource_type is not None
            else ""
        )
        source_actor = normalize_actor(actor) if actor is not None else None
        request = connect_pb2.ListAuditEventsRequest(
            workspace_id=self._connect.workspace_id,
            connector_id=(
                self._default_connector_id if connector_id is None else connector_id
            )
            or "",
            resource_type=normalized_resource_type,
            resource_id=(
                normalize_id(resource_id, "audit.resource_id")
                if resource_id is not None
                else ""
            ),
            limit=_page_limit(limit, page_size, "audit.list"),
            actor_principal=(
                actor_principal_from_ref(source_actor) if source_actor else ""
            ),
            ingested_by_principal=(ingester_principal or "").strip(),
            action=(action or "").strip(),
            page_cursor=cursor or "",
            source_system=(source_system or "").strip(),
            origin=_audit_origin(origin),
            outcome=_audit_outcome(outcome, optional=True),
        )
        for field, value in (
            ("occurred_at_from", occurred_at_from),
            ("occurred_at_to", occurred_at_to),
        ):
            timestamp = _timestamp(value, f"audit.{field}")
            if timestamp is not None:
                getattr(request, field).CopyFrom(timestamp)
        response, request_id = self._connect.list_audit_events(
            request,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return AuditTrailResponse(
            events=[
                _audit_event_from_connect_strict(
                    item,
                    index,
                    request_id,
                    self._connect.workspace_id,
                )
                for index, item in enumerate(response.events)
            ],
            next_cursor=response.next_page_cursor or None,
            request_id=request_id,
            proto=response,
        )

    def iterate(
        self,
        *,
        resource_type: str | None = None,
        resource_id: object | None = None,
        actor: ActorInput | None = None,
        ingester_principal: str | None = None,
        action: str | None = None,
        origin: AuditOrigin | None = None,
        outcome: AuditOutcome | None = None,
        connector_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        page_size: int | None = None,
        occurred_at_from: str | datetime | None = None,
        occurred_at_to: str | datetime | None = None,
        source_system: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Iterator[AuditTrailEvent]:
        filters = _freeze_iterator_filters(
            {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "actor": actor,
                "ingester_principal": ingester_principal,
                "action": action,
                "origin": origin,
                "outcome": outcome,
                "connector_id": connector_id,
                "limit": limit,
                "page_size": page_size,
                "occurred_at_from": occurred_at_from,
                "occurred_at_to": occurred_at_to,
                "source_system": source_system,
                "timeout": timeout,
                "cancellation_event": cancellation_event,
            }
        )
        seen: set[str] = {cursor} if cursor else set()
        pages = 0
        last_request_id: str | None = None
        while True:
            if pages >= MAX_ITERATOR_PAGES:
                raise MedallionError(
                    f"Audit iterator exceeded {MAX_ITERATOR_PAGES} pages.",
                    code="MEDALLION_PAGINATION_LIMIT",
                    request_id=last_request_id,
                )
            page = self.list(cursor=cursor, **filters)
            pages += 1
            last_request_id = page.request_id
            yield from page.events
            cursor = page.next_cursor
            if not cursor:
                return
            if cursor in seen:
                raise MedallionError(
                    "Medallion returned a repeated audit cursor.",
                    code="MEDALLION_REPEATED_CURSOR",
                    request_id=page.request_id,
                )
            seen.add(cursor)

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
        connector_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        page_size: int | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> AuditTrailResponse:
        if not resource_type.strip():
            raise MedallionError(
                "resource_type is required to read an audit trail.",
                code="MEDALLION_MISSING_RESOURCE_TYPE",
            )
        trail_limit = _audit_trail_limit(limit, page_size)
        response = self.list(
            resource_type=resource_type,
            resource_id=resource_id,
            actor=actor,
            ingester_principal=ingester_principal,
            action=action,
            origin=origin,
            outcome=outcome,
            connector_id=connector_id,
            cursor=cursor,
            limit=trail_limit,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        if actor is None:
            return response
        normalized_actor = normalize_actor(actor)
        return AuditTrailResponse(
            events=[
                event
                for event in response.events
                if same_actor(event.actor, normalized_actor)
            ],
            next_cursor=response.next_cursor,
            request_id=response.request_id,
            proto=response.proto,
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
        operation: str,
        idempotency_key: str,
        stream_name: str | None = None,
        entity_type: str | None = None,
        entity_id: object | None = None,
        payload: Any = PAYLOAD_UNSET,
        payload_json: str | None = None,
        source: str | None = None,
        table: str | None = None,
        primary_key: Mapping[str, object] | None = None,
        connector_id: str | None = None,
        actor: ActorInput | None = None,
        before: Any = None,
        after: Any = None,
        metadata: Mapping[str, Any] | None = None,
        source_event_id: object | None = None,
        occurred_at: str | datetime | None = None,
        description: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> EventRecordResponse:
        normalized_primary_key = (
            normalize_id_record(primary_key) if primary_key is not None else None
        )
        if primary_key is not None and not normalized_primary_key:
            raise MedallionError(
                "cdc.primary_key must contain at least one field.",
                code="MEDALLION_EMPTY_CDC_PRIMARY_KEY",
            )
        if (
            normalized_primary_key
            and len(normalized_primary_key) > 1
            and entity_id is None
        ):
            raise MedallionError(
                "cdc.entity_id is required when cdc.primary_key contains more than one field.",
                code="MEDALLION_MISSING_CDC_ENTITY_ID",
            )
        if entity_id is None and normalized_primary_key:
            entity_id = _entity_id_from_primary_key(normalized_primary_key)
        if (
            payload_json is None
            and payload is PAYLOAD_UNSET
            and (
                source is not None
                or table is not None
                or primary_key is not None
                or before is not None
                or after is not None
                or metadata is not None
            )
        ):
            payload = {
                "source": source,
                "table": table,
                "actor": normalize_actor(actor) if actor is not None else None,
                "primaryKey": normalized_primary_key,
                "before": before,
                "after": after,
                "metadata": metadata,
            }
        return self.publish_batch(
            [
                CdcEventInput(
                    stream_name=stream_name or table or "",
                    entity_type=entity_type or table or "",
                    entity_id=entity_id if entity_id is not None else "",
                    operation=operation,  # type: ignore[arg-type]
                    idempotency_key=idempotency_key,
                    payload=payload,
                    payload_json=payload_json,
                    source_event_id=source_event_id,
                    actor=actor,
                    occurred_at=occurred_at,
                    description=description,
                )
            ],
            connector_id=connector_id,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )

    def publish_batch(
        self,
        events: Sequence[CdcEventInput | Mapping[str, Any]],
        *,
        connector_id: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> EventRecordResponse:
        connector = _connector_id(connector_id, self._default_connector_id, "CDC")
        inputs = [_cdc_input(item) for item in events]
        _batch_size(inputs, "cdc")
        proto_events: list[connect_pb2.CdcEvent] = []
        keys: list[str] = []
        for index, item in enumerate(inputs):
            key = _required_event_idempotency_key(
                item.idempotency_key, f"cdc.events[{index}].idempotency_key"
            )
            event = connect_pb2.CdcEvent(
                stream_name=_required_text(
                    item.stream_name,
                    f"cdc.events[{index}].stream_name",
                    max_bytes=256,
                ),
                entity_type=_required_text(
                    item.entity_type,
                    f"cdc.events[{index}].entity_type",
                    max_bytes=256,
                ),
                entity_id=_required_id(
                    item.entity_id,
                    f"cdc.events[{index}].entity_id",
                    max_bytes=1024,
                ),
                operation=_cdc_operation(item.operation),
                source_event_id=(
                    _required_id(
                        item.source_event_id,
                        f"cdc.events[{index}].source_event_id",
                        max_bytes=1024,
                    )
                    if item.source_event_id is not None
                    else ""
                ),
                idempotency_key=key,
                actor_principal=(
                    _actor_principal(item.actor, f"cdc.events[{index}].actor")
                    if item.actor is not None
                    else ""
                ),
                payload_json=_payload_json(item.payload, item.payload_json),
                description=_optional_text(
                    item.description,
                    f"cdc.events[{index}].description",
                    max_bytes=4096,
                ),
            )
            timestamp = _timestamp(item.occurred_at, f"cdc.events[{index}].occurred_at")
            if timestamp is not None:
                event.occurred_at.CopyFrom(timestamp)
            proto_events.append(event)
            keys.append(key)
        response, request_id = self._connect.publish_cdc_events(
            connect_pb2.PublishCdcEventsRequest(
                connector_id=connector,
                events=proto_events,
            ),
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _cdc_event_response(response, keys, request_id)

    def list(
        self,
        *,
        connector_id: str | None = None,
        entity_type: str | None = None,
        entity_id: object | None = None,
        actor: ActorInput | None = None,
        ingester_principal: str | None = None,
        source_system: str | None = None,
        stream_name: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        page_size: int | None = None,
        occurred_at_from: str | datetime | None = None,
        occurred_at_to: str | datetime | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> CdcPage:
        normalized_actor = normalize_actor(actor) if actor is not None else None
        request = connect_pb2.ListCdcEventsRequest(
            workspace_id=self._connect.workspace_id,
            connector_id=(
                self._default_connector_id if connector_id is None else connector_id
            )
            or "",
            entity_type=(entity_type or "").strip(),
            entity_id=(
                normalize_id(entity_id, "cdc.entity_id")
                if entity_id is not None
                else ""
            ),
            limit=_page_limit(limit, page_size, "cdc.list"),
            actor_principal=(
                actor_principal_from_ref(normalized_actor) if normalized_actor else ""
            ),
            source_system=(source_system or "").strip(),
            stream_name=(stream_name or "").strip(),
            page_cursor=cursor or "",
            ingested_by_principal=(ingester_principal or "").strip(),
        )
        for field, value in (
            ("occurred_at_from", occurred_at_from),
            ("occurred_at_to", occurred_at_to),
        ):
            timestamp = _timestamp(value, f"cdc.{field}")
            if timestamp is not None:
                getattr(request, field).CopyFrom(timestamp)
        response, request_id = self._connect.list_cdc_events(
            request,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return CdcPage(
            events=[
                _cdc_event_from_connect_strict(
                    item,
                    index,
                    request_id,
                    self._connect.workspace_id,
                )
                for index, item in enumerate(response.events)
            ],
            next_cursor=response.next_page_cursor or None,
            request_id=request_id,
            proto=response,
        )

    def iterate(
        self,
        *,
        connector_id: str | None = None,
        entity_type: str | None = None,
        entity_id: object | None = None,
        actor: ActorInput | None = None,
        ingester_principal: str | None = None,
        source_system: str | None = None,
        stream_name: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        page_size: int | None = None,
        occurred_at_from: str | datetime | None = None,
        occurred_at_to: str | datetime | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Iterator[CdcReadEvent]:
        filters = _freeze_iterator_filters(
            {
                "connector_id": connector_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "actor": actor,
                "ingester_principal": ingester_principal,
                "source_system": source_system,
                "stream_name": stream_name,
                "limit": limit,
                "page_size": page_size,
                "occurred_at_from": occurred_at_from,
                "occurred_at_to": occurred_at_to,
                "timeout": timeout,
                "cancellation_event": cancellation_event,
            }
        )
        seen: set[str] = {cursor} if cursor else set()
        pages = 0
        last_request_id: str | None = None
        while True:
            if pages >= MAX_ITERATOR_PAGES:
                raise MedallionError(
                    f"CDC iterator exceeded {MAX_ITERATOR_PAGES} pages.",
                    code="MEDALLION_PAGINATION_LIMIT",
                    request_id=last_request_id,
                )
            page = self.list(cursor=cursor, **filters)
            pages += 1
            last_request_id = page.request_id
            yield from page.events
            cursor = page.next_cursor
            if not cursor:
                return
            if cursor in seen:
                raise MedallionError(
                    "Medallion returned a repeated CDC cursor.",
                    code="MEDALLION_REPEATED_CURSOR",
                    request_id=page.request_id,
                )
            seen.add(cursor)


def _cdc_event_response(
    body: connect_pb2.PublishCdcEventsResponse,
    expected_idempotency_keys: Sequence[str],
    request_id: str | None,
) -> EventRecordResponse:
    events = [
        PublishedEventResult(
            idempotency_key=item.idempotency_key,
            event_id=_positive_receipt_event_id(item.event_id),
            duplicate=item.duplicate,
            proto=item,
        )
        for item in body.events
    ]
    _validate_publish_response(
        expected_idempotency_keys,
        [item.idempotency_key for item in events],
        [item.event_id for item in events],
        [item.duplicate for item in events],
        body.accepted_count,
        body.duplicate_count,
        request_id,
    )
    first = events[0]
    duplicate = len(events) == body.duplicate_count
    result = (
        "duplicate" if duplicate else "mixed" if body.duplicate_count else "accepted"
    )
    return EventRecordResponse(
        request_id=request_id,
        idempotency_key=first.idempotency_key if len(events) == 1 else None,
        duplicate=duplicate,
        result=result,
        accepted_count=body.accepted_count,
        duplicate_count=body.duplicate_count,
        events=events,
        proto=body,
    )


def _positive_receipt_event_id(value: int) -> str | None:
    return str(value) if value > 0 else None


def _validate_publish_response(
    expected_keys: Sequence[str],
    actual_keys: Sequence[str],
    event_ids: Sequence[str | None],
    duplicate_flags: Sequence[bool],
    accepted_count: int,
    duplicate_count: int,
    request_id: str | None,
) -> None:
    if list(actual_keys) != list(expected_keys) or (
        any(event_id is None for event_id in event_ids)
        or sum(duplicate_flags) != duplicate_count
        or accepted_count + duplicate_count != len(expected_keys)
    ):
        raise MedallionError(
            "Medallion returned an invalid event publish acknowledgement.",
            code="MEDALLION_INVALID_PUBLISH_RESPONSE",
            request_id=request_id,
        )


def _audit_event_response(
    body: connect_pb2.PublishAuditEventsResponse,
    expected_idempotency_keys: Sequence[str],
    request_id: str | None,
) -> AuditRecordResponse:
    events = [
        PublishedAuditEventResult(
            idempotency_key=item.idempotency_key,
            event_id=_positive_receipt_event_id(item.event_id),
            duplicate=item.duplicate,
            proto=item,
        )
        for item in body.events
    ]
    _validate_publish_response(
        expected_idempotency_keys,
        [item.idempotency_key for item in events],
        [item.event_id for item in events],
        [item.duplicate for item in events],
        body.accepted_count,
        body.duplicate_count,
        request_id,
    )
    first = events[0]
    duplicate = len(events) == body.duplicate_count
    result = (
        "duplicate" if duplicate else "mixed" if body.duplicate_count else "accepted"
    )
    return AuditRecordResponse(
        request_id=request_id,
        idempotency_key=first.idempotency_key if len(events) == 1 else None,
        duplicate=duplicate,
        result=result,
        accepted_count=body.accepted_count,
        duplicate_count=body.duplicate_count,
        events=events,
        proto=body,
    )


def _audit_event_from_connect(item: connect_pb2.AuditEvent) -> AuditTrailEvent:
    payload = _parse_payload(item.payload_json)
    return _audit_event_projection(item, payload)


def _audit_event_from_connect_strict(
    item: connect_pb2.AuditEvent,
    index: int,
    request_id: str | None,
    workspace_id: str,
) -> AuditTrailEvent:
    if (
        item.id <= 0
        or item.workspace_id != workspace_id
        or _audit_origin_from_connect(item.origin) is None
        or _audit_outcome_from_connect(item.outcome) is None
    ):
        raise _invalid_list_response(
            f"audit events[{index}] has invalid identity, workspace, origin, or outcome.",
            request_id,
        )
    for value, field, maximum, required in (
        (item.workspace_id, "workspace_id", 29, True),
        (item.connector_id, "connector_id", 128, False),
        (item.resource_type, "resource_type", 256, True),
        (item.resource_id, "resource_id", 1024, True),
        (item.action, "action", 256, True),
        (item.source_event_id, "source_event_id", 1024, False),
        (item.actor_principal, "actor_principal", 512, False),
        (item.description, "description", 4096, False),
        (item.source_system, "source_system", 256, False),
        (item.ingested_by_principal, "ingested_by_principal", 512, False),
    ):
        _validate_list_response_text(
            value,
            f"audit.events[{index}].{field}",
            maximum,
            request_id,
            required=required,
        )
    _validate_list_response_idempotency_key(
        item.idempotency_key,
        f"audit.events[{index}].idempotency_key",
        request_id,
    )
    _validate_list_response_timestamps(item, f"audit.events[{index}]", request_id)
    payload = _parse_list_payload(
        item.payload_json,
        f"audit.events[{index}].payload_json",
        request_id,
    )
    return _audit_event_projection(item, payload)


def _audit_event_projection(
    item: connect_pb2.AuditEvent,
    payload: Any,
) -> AuditTrailEvent:
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
        workspace_id=item.workspace_id,
        connector_id=item.connector_id or None,
        actor=actor,
        ingester_principal=item.ingested_by_principal or None,
        actor_principal=item.actor_principal or None,
        action=item.action or None,
        description=item.description or None,
        idempotency_key=item.idempotency_key or None,
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
        payload_json=item.payload_json,
        proto=item,
    )


def _cdc_event_from_connect(item: connect_pb2.CdcEvent) -> CdcReadEvent:
    return _cdc_event_projection(item, _parse_payload(item.payload_json))


def _cdc_event_from_connect_strict(
    item: connect_pb2.CdcEvent,
    index: int,
    request_id: str | None,
    workspace_id: str,
) -> CdcReadEvent:
    if (
        item.id <= 0
        or item.workspace_id != workspace_id
        or _cdc_operation_from_connect(item.operation) is None
    ):
        raise _invalid_list_response(
            f"CDC events[{index}] has invalid identity, workspace, or operation.",
            request_id,
        )
    for value, field, maximum, required in (
        (item.workspace_id, "workspace_id", 29, True),
        (item.connector_id, "connector_id", 128, False),
        (item.stream_name, "stream_name", 256, True),
        (item.entity_type, "entity_type", 256, True),
        (item.entity_id, "entity_id", 1024, True),
        (item.source_event_id, "source_event_id", 1024, False),
        (item.actor_principal, "actor_principal", 512, False),
        (item.description, "description", 4096, False),
        (item.source_system, "source_system", 256, False),
        (item.ingested_by_principal, "ingested_by_principal", 512, False),
    ):
        _validate_list_response_text(
            value,
            f"cdc.events[{index}].{field}",
            maximum,
            request_id,
            required=required,
        )
    _validate_list_response_idempotency_key(
        item.idempotency_key,
        f"cdc.events[{index}].idempotency_key",
        request_id,
    )
    _validate_list_response_timestamps(item, f"cdc.events[{index}]", request_id)
    payload = _parse_list_payload(
        item.payload_json,
        f"cdc.events[{index}].payload_json",
        request_id,
    )
    return _cdc_event_projection(item, payload)


def _cdc_event_projection(item: connect_pb2.CdcEvent, payload: Any) -> CdcReadEvent:
    event_id = str(item.id) if item.id else None
    return CdcReadEvent(
        id=event_id,
        event_id=event_id,
        workspace_id=item.workspace_id,
        connector_id=item.connector_id or None,
        stream_name=item.stream_name or None,
        entity_type=item.entity_type or None,
        entity_id=item.entity_id or None,
        operation=_cdc_operation_from_connect(item.operation),
        idempotency_key=item.idempotency_key or None,
        actor=actor_from_principal(item.actor_principal or None),
        actor_principal=item.actor_principal or None,
        source_event_id=item.source_event_id or None,
        occurred_at=_timestamp_string(
            item.occurred_at if item.HasField("occurred_at") else None
        ),
        observed_at=_timestamp_string(
            item.observed_at if item.HasField("observed_at") else None
        ),
        description=item.description or None,
        source_system=item.source_system or None,
        ingester_principal=item.ingested_by_principal or None,
        payload=payload,
        payload_json=item.payload_json,
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
    if not isinstance(value, str) or not value:
        raise MedallionError(
            f"{field} is required for retry-safe delivery.",
            code="MEDALLION_MISSING_IDEMPOTENCY_KEY",
        )
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise MedallionError(
            f"{field} must contain valid Unicode scalar values.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        ) from None
    if len(encoded) > 512:
        raise MedallionError(
            f"{field} must be between 1 and 512 UTF-8 bytes.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    return value


def _validate_customer_cdc_proto(event: connect_pb2.CdcEvent) -> None:
    if (
        event.id
        or event.connector_id
        or event.workspace_id
        or event.HasField("observed_at")
        or event.source_system
        or event.ingested_by_principal
    ):
        raise MedallionError(
            "CDC event contains server-derived fields.",
            code="MEDALLION_INVALID_CDC_EVENT",
        )
    _required_text(event.stream_name, "cdc.stream_name", max_bytes=256)
    _required_text(event.entity_type, "cdc.entity_type", max_bytes=256)
    _required_text(event.entity_id, "cdc.entity_id", max_bytes=1024)
    _optional_text(event.source_event_id, "cdc.source_event_id", max_bytes=1024)
    _optional_text(event.actor_principal, "cdc.actor_principal", max_bytes=512)
    _optional_text(event.description, "cdc.description", max_bytes=4096)
    if _cdc_operation_from_connect(event.operation) is None:
        raise MedallionError(
            "CDC operation must be insert, update, delete, or snapshot.",
            code="MEDALLION_INVALID_CDC_OPERATION",
        )
    _required_event_idempotency_key(event.idempotency_key, "cdc.idempotency_key")
    _payload_json(PAYLOAD_UNSET, event.payload_json)
    if event.HasField("occurred_at"):
        _validate_proto_timestamp(event.occurred_at, "cdc.occurred_at")


def _validate_customer_audit_proto(event: connect_pb2.AuditEvent) -> None:
    if (
        event.id
        or event.connector_id
        or event.workspace_id
        or event.HasField("observed_at")
        or event.source_system
        or event.ingested_by_principal
        or event.origin != connect_pb2.AUDIT_EVENT_ORIGIN_UNSPECIFIED
    ):
        raise MedallionError(
            "Audit event contains server-derived fields.",
            code="MEDALLION_INVALID_AUDIT_EVENT",
        )
    _required_text(event.resource_type, "audit.resource_type", max_bytes=256)
    _required_text(event.resource_id, "audit.resource_id", max_bytes=1024)
    _required_text(event.action, "audit.action", max_bytes=256)
    _optional_text(event.source_event_id, "audit.source_event_id", max_bytes=1024)
    _optional_text(event.actor_principal, "audit.actor_principal", max_bytes=512)
    _optional_text(event.description, "audit.description", max_bytes=4096)
    _required_event_idempotency_key(event.idempotency_key, "audit.idempotency_key")
    _payload_json(PAYLOAD_UNSET, event.payload_json)
    _audit_outcome(_audit_outcome_from_connect(event.outcome))
    if event.HasField("occurred_at"):
        _validate_proto_timestamp(event.occurred_at, "audit.occurred_at")


def _audit_trail_limit(limit: int | None, page_size: int | None) -> int:
    return _page_limit(limit, page_size, "audit.trail")


def _page_limit(limit: int | None, page_size: int | None, name: str) -> int:
    if limit is not None and page_size is not None and limit != page_size:
        raise MedallionError(
            f"{name} limit and page_size must match when both are provided.",
            code="MEDALLION_INVALID_PAGE_SIZE",
        )
    value = limit if limit is not None else page_size
    if value is None or value == 0:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MedallionError(
            f"{name} limit/page_size must be zero or a positive integer.",
            code=(
                "MEDALLION_INVALID_AUDIT_TRAIL_LIMIT"
                if name == "audit.trail"
                else "MEDALLION_INVALID_PAGE_SIZE"
            ),
        )
    if value > 500:
        raise MedallionError(
            f"{name} limit/page_size must be 500 or less; use cursor pagination for larger reads.",
            code=(
                "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE"
                if name == "audit.trail"
                else "MEDALLION_PAGE_SIZE_TOO_LARGE"
            ),
        )
    return value


def _prepare_list_cdc_request(
    request: connect_pb2.ListCdcEventsRequest,
    workspace_id: str,
) -> connect_pb2.ListCdcEventsRequest:
    if not isinstance(request, connect_pb2.ListCdcEventsRequest):
        raise MedallionError(
            "CDC list request is required.",
            code="MEDALLION_INVALID_LIST_REQUEST",
        )
    prepared = connect_pb2.ListCdcEventsRequest()
    prepared.CopyFrom(request)
    _bind_list_workspace(prepared.workspace_id, workspace_id, "CDC")
    prepared.workspace_id = workspace_id
    for value, path, maximum in (
        (prepared.workspace_id, "cdc.list.workspace_id", 29),
        (prepared.connector_id, "cdc.list.connector_id", 128),
        (prepared.entity_type, "cdc.list.entity_type", 256),
        (prepared.entity_id, "cdc.list.entity_id", 1024),
        (prepared.actor_principal, "cdc.list.actor_principal", 512),
        (prepared.source_system, "cdc.list.source_system", 256),
        (prepared.stream_name, "cdc.list.stream_name", 256),
        (prepared.page_cursor, "cdc.list.page_cursor", 2048),
        (prepared.ingested_by_principal, "cdc.list.ingested_by_principal", 512),
    ):
        _list_request_text(value, path, maximum)
    _list_request_limit(prepared.limit, "CDC")
    _validate_request_timestamp_bounds(
        prepared,
        "cdc.list",
    )
    return prepared


def _prepare_list_audit_request(
    request: connect_pb2.ListAuditEventsRequest,
    workspace_id: str,
) -> connect_pb2.ListAuditEventsRequest:
    if not isinstance(request, connect_pb2.ListAuditEventsRequest):
        raise MedallionError(
            "Audit list request is required.",
            code="MEDALLION_INVALID_LIST_REQUEST",
        )
    prepared = connect_pb2.ListAuditEventsRequest()
    prepared.CopyFrom(request)
    _bind_list_workspace(prepared.workspace_id, workspace_id, "Audit")
    prepared.workspace_id = workspace_id
    for value, path, maximum in (
        (prepared.workspace_id, "audit.list.workspace_id", 29),
        (prepared.connector_id, "audit.list.connector_id", 128),
        (prepared.resource_type, "audit.list.resource_type", 256),
        (prepared.resource_id, "audit.list.resource_id", 1024),
        (prepared.actor_principal, "audit.list.actor_principal", 512),
        (prepared.action, "audit.list.action", 256),
        (prepared.source_system, "audit.list.source_system", 256),
        (prepared.page_cursor, "audit.list.page_cursor", 2048),
        (
            prepared.ingested_by_principal,
            "audit.list.ingested_by_principal",
            512,
        ),
    ):
        _list_request_text(value, path, maximum)
    if bool(prepared.resource_type) != bool(prepared.resource_id):
        raise MedallionError(
            "audit resource_type and resource_id must be provided together.",
            code="MEDALLION_INVALID_AUDIT_FILTER",
        )
    _list_request_limit(prepared.limit, "Audit")
    if prepared.origin not in {
        connect_pb2.AUDIT_EVENT_ORIGIN_UNSPECIFIED,
        connect_pb2.AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER,
        connect_pb2.AUDIT_EVENT_ORIGIN_CONNECT,
    }:
        raise MedallionError(
            "audit.list.origin must be unspecified, external_provider, or connect.",
            code="MEDALLION_INVALID_AUDIT_ORIGIN",
        )
    if prepared.outcome not in {
        connect_pb2.AUDIT_EVENT_OUTCOME_UNSPECIFIED,
        connect_pb2.AUDIT_EVENT_OUTCOME_SUCCEEDED,
        connect_pb2.AUDIT_EVENT_OUTCOME_FAILED,
        connect_pb2.AUDIT_EVENT_OUTCOME_INDETERMINATE,
    }:
        raise MedallionError(
            "audit.list.outcome must be unspecified, succeeded, failed, or indeterminate.",
            code="MEDALLION_INVALID_AUDIT_OUTCOME",
        )
    _validate_request_timestamp_bounds(
        prepared,
        "audit.list",
    )
    return prepared


def _bind_list_workspace(provided: str, configured: str, family: str) -> None:
    if provided and provided != configured:
        raise MedallionError(
            f"{family} list workspace_id conflicts with this client.",
            code="MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
        )


def _list_request_text(value: str, path: str, maximum: int) -> None:
    if len(value.encode("utf-8")) > maximum:
        raise MedallionError(
            f"{path} must not exceed {maximum} bytes.",
            code="MEDALLION_INVALID_LIST_REQUEST",
        )


def _list_request_limit(value: int, family: str) -> None:
    if value > 500:
        raise MedallionError(
            f"{family} list limit must be between 0 and 500.",
            code="MEDALLION_PAGE_SIZE_TOO_LARGE",
        )


def _validate_request_timestamp_bounds(
    request: connect_pb2.ListCdcEventsRequest | connect_pb2.ListAuditEventsRequest,
    path: str,
) -> None:
    lower = (
        _validate_proto_timestamp(request.occurred_at_from, f"{path}.occurred_at_from")
        if request.HasField("occurred_at_from")
        else None
    )
    upper = (
        _validate_proto_timestamp(request.occurred_at_to, f"{path}.occurred_at_to")
        if request.HasField("occurred_at_to")
        else None
    )
    if lower is not None and upper is not None and lower >= upper:
        raise MedallionError(
            f"{path}.occurred_at_from must be earlier than occurred_at_to.",
            code="MEDALLION_INVALID_TIMESTAMP_RANGE",
        )


def _validate_proto_timestamp(value: Timestamp, path: str) -> tuple[int, int]:
    instant = (value.seconds, value.nanos)
    if not (
        MIN_PROTOBUF_TIMESTAMP_SECONDS
        <= value.seconds
        <= MAX_PROTOBUF_TIMESTAMP_SECONDS
        and 0 <= value.nanos <= 999_999_999
    ):
        raise MedallionError(
            f"{path} must be a valid protobuf Timestamp.",
            code="MEDALLION_INVALID_TIMESTAMP",
        )
    if not MIN_ANALYTICAL_TIMESTAMP <= instant <= MAX_ANALYTICAL_TIMESTAMP:
        raise MedallionError(
            f"{path} must be between 1900-01-01 and 2262-04-11T23:47:16.854775807Z.",
            code="MEDALLION_TIMESTAMP_OUT_OF_RANGE",
        )
    return instant


def _timestamp(value: str | datetime | None, path: str) -> Timestamp | None:
    if value is None:
        return None
    if isinstance(value, str):
        timestamp = Timestamp()
        try:
            timestamp.FromJsonString(value)
        except ValueError as exc:
            raise MedallionError(
                f"{path} must be an RFC3339 timestamp.",
                code="MEDALLION_INVALID_TIMESTAMP",
            ) from exc
        _validate_proto_timestamp(timestamp, path)
        return timestamp
    if not isinstance(value, datetime):
        raise MedallionError(
            f"{path} must be an RFC3339 timestamp or datetime.",
            code="MEDALLION_INVALID_TIMESTAMP",
        )
    parsed = value
    if parsed.tzinfo is None:
        raise MedallionError(
            f"{path} must include a timezone.",
            code="MEDALLION_INVALID_TIMESTAMP",
        )
    timestamp = Timestamp()
    try:
        timestamp.FromDatetime(parsed)
    except (ValueError, OverflowError) as exc:
        raise MedallionError(
            f"{path} is outside the protobuf Timestamp range.",
            code="MEDALLION_INVALID_TIMESTAMP",
        ) from exc
    _validate_proto_timestamp(timestamp, path)
    return timestamp


def _timestamp_string(value: Timestamp | None) -> str | None:
    if value is None:
        return None
    return value.ToJsonString()


def _json_string(value: Any) -> str:
    try:
        _validate_json(value, set())
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except MedallionError:
        raise
    except TypeError, ValueError, RecursionError:
        raise MedallionError(
            "Medallion event payload must be JSON serializable.",
            code="MEDALLION_INVALID_JSON_BODY",
        ) from None


def _parse_payload(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _parse_list_payload(
    value: str,
    path: str,
    request_id: str | None,
) -> Any:
    try:
        return json.loads(value, parse_constant=_reject_json_constant)
    except json.JSONDecodeError, MedallionError, RecursionError, TypeError, ValueError:
        raise _invalid_list_response(
            f"{path} must contain exactly one valid JSON value.",
            request_id,
        ) from None


def _invalid_list_response(
    message: str,
    request_id: str | None,
) -> MedallionError:
    return MedallionError(
        message,
        code="MEDALLION_INVALID_LIST_RESPONSE",
        request_id=request_id,
    )


def _validate_list_cursor(value: str, request_id: str | None) -> None:
    if len(value.encode("utf-8")) > 2048:
        raise _invalid_list_response(
            "Medallion returned a continuation cursor larger than 2048 bytes.",
            request_id,
        )


def _validate_list_response_text(
    value: str,
    path: str,
    maximum: int,
    request_id: str | None,
    *,
    required: bool,
) -> None:
    if required and not value.strip():
        raise _invalid_list_response(f"{path} is required.", request_id)
    if len(value.encode("utf-8")) > maximum:
        raise _invalid_list_response(
            f"{path} must not exceed {maximum} bytes.",
            request_id,
        )


def _validate_list_response_idempotency_key(
    value: str,
    path: str,
    request_id: str | None,
) -> None:
    try:
        _required_event_idempotency_key(value, path)
    except MedallionError:
        raise _invalid_list_response(
            f"{path} must be a non-empty valid Unicode string of at most 512 UTF-8 bytes.",
            request_id,
        ) from None


def _validate_list_response_timestamps(
    item: connect_pb2.CdcEvent | connect_pb2.AuditEvent,
    path: str,
    request_id: str | None,
) -> None:
    for field in ("occurred_at", "observed_at"):
        if not item.HasField(field):
            continue
        try:
            _validate_proto_timestamp(getattr(item, field), f"{path}.{field}")
        except MedallionError:
            raise _invalid_list_response(
                f"{path}.{field} must be a valid Medallion timestamp.",
                request_id,
            ) from None


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


def _cdc_operation_from_connect(value: int):
    return {
        connect_pb2.CDC_OPERATION_INSERT: "insert",
        connect_pb2.CDC_OPERATION_UPDATE: "update",
        connect_pb2.CDC_OPERATION_DELETE: "delete",
        connect_pb2.CDC_OPERATION_SNAPSHOT: "snapshot",
    }.get(value)


def _audit_input(value: AuditEventInput | Mapping[str, Any]) -> AuditEventInput:
    if isinstance(value, AuditEventInput):
        return value
    if not isinstance(value, Mapping):
        raise MedallionError(
            "Each audit event must be an AuditEventInput or mapping.",
            code="MEDALLION_INVALID_AUDIT_EVENT",
        )
    try:
        return AuditEventInput(**dict(value))
    except TypeError as exc:
        raise MedallionError(
            "Audit event contains missing, unknown, or server-owned fields.",
            code="MEDALLION_INVALID_AUDIT_EVENT",
        ) from exc


def _cdc_input(value: CdcEventInput | Mapping[str, Any]) -> CdcEventInput:
    if isinstance(value, CdcEventInput):
        return value
    if not isinstance(value, Mapping):
        raise MedallionError(
            "Each CDC event must be a CdcEventInput or mapping.",
            code="MEDALLION_INVALID_CDC_EVENT",
        )
    try:
        return CdcEventInput(**dict(value))
    except TypeError as exc:
        raise MedallionError(
            "CDC event contains missing, unknown, or server-owned fields.",
            code="MEDALLION_INVALID_CDC_EVENT",
        ) from exc


def _connector_id(
    override: str | None,
    default: str | None,
    family: str,
) -> str:
    value = default if override is None else override
    if not value or not value.strip():
        raise MedallionError(
            f"connector_id is required to publish {family} events.",
            code="MEDALLION_MISSING_CONNECTOR_ID",
        )
    return _required_text(value, "connector_id", max_bytes=128)


def _batch_size(events: Sequence[Any], family: str) -> None:
    if not 1 <= len(events) <= 1000:
        raise MedallionError(
            f"{family} batches must contain between 1 and 1000 events.",
            code="MEDALLION_INVALID_BATCH_SIZE",
        )


def _required_text(value: object, field: str, *, max_bytes: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MedallionError(
            f"{field} is required.",
            code="MEDALLION_INVALID_EVENT",
        )
    normalized = value.strip()
    if len(normalized.encode("utf-8")) > max_bytes:
        raise MedallionError(
            f"{field} must not exceed {max_bytes} bytes.",
            code="MEDALLION_INVALID_EVENT",
        )
    return normalized


def _optional_text(value: str | None, field: str, *, max_bytes: int) -> str:
    if value is None:
        return ""
    if len(value.encode("utf-8")) > max_bytes:
        raise MedallionError(
            f"{field} must not exceed {max_bytes} bytes.",
            code="MEDALLION_INVALID_EVENT",
        )
    return value


def _required_id(value: object, field: str, *, max_bytes: int) -> str:
    normalized = normalize_id(value, field)
    if not normalized.strip():
        raise MedallionError(
            f"{field} is required.",
            code="MEDALLION_INVALID_EVENT",
        )
    if len(normalized.encode("utf-8")) > max_bytes:
        raise MedallionError(
            f"{field} must not exceed {max_bytes} bytes.",
            code="MEDALLION_INVALID_EVENT",
        )
    return normalized


def _actor_principal(actor: ActorInput, field: str) -> str:
    principal = actor_principal_from_ref(normalize_actor(actor))
    return _required_id(principal, field, max_bytes=512)


def _payload_json(value: Any, raw_json: str | None) -> str:
    if raw_json is not None:
        if value is not PAYLOAD_UNSET:
            raise MedallionError(
                "Use either payload or payload_json, not both.",
                code="MEDALLION_INVALID_JSON_BODY",
            )
        if not isinstance(raw_json, str) or not raw_json:
            raise MedallionError(
                "payload_json must contain valid JSON.",
                code="MEDALLION_INVALID_JSON_BODY",
            )
        try:
            parsed = json.loads(
                raw_json,
                parse_constant=_reject_json_constant,
                parse_float=str,
                parse_int=str,
            )
            _validate_json(parsed, set())
        except json.JSONDecodeError, RecursionError, ValueError:
            raise MedallionError(
                "payload_json must contain valid JSON.",
                code="MEDALLION_INVALID_JSON_BODY",
            ) from None
        return raw_json
    return _json_string({} if value is PAYLOAD_UNSET else value)


def _reject_json_constant(token: str) -> Any:
    raise ValueError(f"invalid JSON constant {token}")


def _validate_json(value: Any, ancestors: set[int]) -> None:
    if value is None or isinstance(value, bool | int):
        return
    if isinstance(value, str):
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise MedallionError(
                "Medallion event payload must contain valid Unicode scalar values.",
                code="MEDALLION_INVALID_JSON_BODY",
            ) from None
        return
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise MedallionError(
                "Medallion event payload must contain finite JSON numbers.",
                code="MEDALLION_INVALID_JSON_BODY",
            )
        return
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in ancestors:
            raise MedallionError(
                "Medallion event payload must not contain cycles.",
                code="MEDALLION_INVALID_JSON_BODY",
            )
        ancestors.add(identity)
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise MedallionError(
                        "Medallion event payload object keys must be strings.",
                        code="MEDALLION_INVALID_JSON_BODY",
                    )
                _validate_json(key, ancestors)
                _validate_json(item, ancestors)
        finally:
            ancestors.remove(identity)
        return
    if isinstance(value, list | tuple):
        identity = id(value)
        if identity in ancestors:
            raise MedallionError(
                "Medallion event payload must not contain cycles.",
                code="MEDALLION_INVALID_JSON_BODY",
            )
        ancestors.add(identity)
        try:
            for item in value:
                _validate_json(item, ancestors)
        finally:
            ancestors.remove(identity)
        return
    raise MedallionError(
        "Medallion event payload contains a value that JSON cannot represent.",
        code="MEDALLION_INVALID_JSON_BODY",
    )


def _entity_id_from_primary_key(primary_key: Mapping[str, str]) -> str:
    if not primary_key:
        raise MedallionError(
            "cdc.primary_key must contain at least one field.",
            code="MEDALLION_EMPTY_CDC_PRIMARY_KEY",
        )
    return next(iter(primary_key.values()))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _freeze_iterator_filters(filters: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return {
            key: value if key == "cancellation_event" else deepcopy(value)
            for key, value in filters.items()
        }
    except (TypeError, ValueError) as exc:
        raise MedallionError(
            "Iterator filters must be stable values.",
            code="MEDALLION_INVALID_FILTER",
        ) from exc


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None
