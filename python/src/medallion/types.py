from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeAlias

from medallion.connect.v1 import connect_pb2

IdInput = str | int
PAYLOAD_UNSET = object()


@dataclass(frozen=True)
class ActorRef:
    id: IdInput
    type: str | None = None
    provider: str | None = None


@dataclass(frozen=True)
class ResourceRef:
    type: str
    id: IdInput


ActorInput: TypeAlias = ActorRef | Mapping[str, Any]
ResourceInput: TypeAlias = ResourceRef | Mapping[str, Any]
AuditOutcome: TypeAlias = Literal["succeeded", "failed", "indeterminate"]
AuditOrigin: TypeAlias = Literal["external_provider", "connect"]
CdcOperation: TypeAlias = Literal["insert", "update", "delete", "snapshot"]


@dataclass(frozen=True)
class CdcEventInput:
    stream_name: str
    entity_type: str
    entity_id: IdInput
    operation: CdcOperation
    idempotency_key: str
    payload: Any = PAYLOAD_UNSET
    payload_json: str | None = None
    source_event_id: IdInput | None = None
    actor: ActorInput | None = None
    occurred_at: str | datetime | None = None
    description: str | None = None


@dataclass(frozen=True)
class AuditEventInput:
    resource_type: str
    resource_id: IdInput
    action: str
    outcome: AuditOutcome
    idempotency_key: str
    payload: Any = PAYLOAD_UNSET
    payload_json: str | None = None
    source_event_id: IdInput | None = None
    actor: ActorInput | None = None
    occurred_at: str | datetime | None = None
    description: str | None = None


CdcEvent: TypeAlias = connect_pb2.CdcEvent
AuditEvent: TypeAlias = connect_pb2.AuditEvent
PublishCdcEventsRequest: TypeAlias = connect_pb2.PublishCdcEventsRequest
PublishCdcEventsResponseProto: TypeAlias = connect_pb2.PublishCdcEventsResponse
ListCdcEventsRequest: TypeAlias = connect_pb2.ListCdcEventsRequest
ListCdcEventsResponseProto: TypeAlias = connect_pb2.ListCdcEventsResponse
PublishAuditEventsRequest: TypeAlias = connect_pb2.PublishAuditEventsRequest
PublishAuditEventsResponseProto: TypeAlias = connect_pb2.PublishAuditEventsResponse
ListAuditEventsRequest: TypeAlias = connect_pb2.ListAuditEventsRequest
ListAuditEventsResponseProto: TypeAlias = connect_pb2.ListAuditEventsResponse


@dataclass(frozen=True)
class PublishedEventResult:
    idempotency_key: str
    event_id: str | None = None
    duplicate: bool = False
    proto: connect_pb2.PublishedCdcEvent | None = None


@dataclass(frozen=True)
class EventRecordResponse:
    idempotency_key: str | None
    duplicate: bool
    result: str
    accepted_count: int
    duplicate_count: int
    events: list[PublishedEventResult] = field(default_factory=list)
    request_id: str | None = None
    proto: connect_pb2.PublishCdcEventsResponse | None = None


@dataclass(frozen=True)
class PublishedAuditEventResult:
    idempotency_key: str
    event_id: str | None = None
    duplicate: bool = False
    proto: connect_pb2.PublishedAuditEvent | None = None


@dataclass(frozen=True)
class AuditRecordResponse:
    idempotency_key: str | None
    duplicate: bool
    result: str
    accepted_count: int
    duplicate_count: int
    events: list[PublishedAuditEventResult] = field(default_factory=list)
    request_id: str | None = None
    proto: connect_pb2.PublishAuditEventsResponse | None = None


@dataclass(frozen=True)
class AuditTrailEvent:
    workspace_id: str
    id: str | None = None
    event_id: str | None = None
    connector_id: str | None = None
    actor: ActorRef | None = None
    ingester_principal: str | None = None
    actor_principal: str | None = None
    action: str | None = None
    description: str | None = None
    idempotency_key: str | None = None
    target_type: str | None = None
    target_id: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    metadata: Mapping[str, Any] | None = None
    created_at: str | None = None
    occurred_at: str | None = None
    observed_at: str | None = None
    before: Any = None
    after: Any = None
    evidence_url: str | None = None
    source_event_id: str | None = None
    source_system: str | None = None
    origin: AuditOrigin | None = None
    outcome: AuditOutcome | None = None
    payload: Any = None
    payload_json: str | None = None
    proto: connect_pb2.AuditEvent | None = None


@dataclass(frozen=True)
class AuditTrailResponse:
    events: list[AuditTrailEvent]
    next_cursor: str | None = None
    request_id: str | None = None
    proto: connect_pb2.ListAuditEventsResponse | None = None


@dataclass(frozen=True)
class CdcReadEvent:
    workspace_id: str
    id: str | None = None
    event_id: str | None = None
    connector_id: str | None = None
    stream_name: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    operation: CdcOperation | None = None
    idempotency_key: str | None = None
    actor: ActorRef | None = None
    actor_principal: str | None = None
    source_event_id: str | None = None
    occurred_at: str | None = None
    observed_at: str | None = None
    description: str | None = None
    source_system: str | None = None
    ingester_principal: str | None = None
    payload: Any = None
    payload_json: str | None = None
    proto: connect_pb2.CdcEvent | None = None


@dataclass(frozen=True)
class CdcPage:
    events: list[CdcReadEvent]
    next_cursor: str | None = None
    request_id: str | None = None
    proto: connect_pb2.ListCdcEventsResponse | None = None
