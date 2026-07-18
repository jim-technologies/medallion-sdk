from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias

from medallion.connect.v1 import connect_pb2

IdInput = str | int


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

Connector: TypeAlias = connect_pb2.Connector
CdcEvent: TypeAlias = connect_pb2.CdcEvent
AuditEvent: TypeAlias = connect_pb2.AuditEvent
RegisterConnectorRequest: TypeAlias = connect_pb2.RegisterConnectorRequest
RegisterConnectorResponseProto: TypeAlias = connect_pb2.RegisterConnectorResponse
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
    idempotency_key: str
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
    idempotency_key: str
    duplicate: bool
    result: str
    accepted_count: int
    duplicate_count: int
    events: list[PublishedAuditEventResult] = field(default_factory=list)
    request_id: str | None = None
    proto: connect_pb2.PublishAuditEventsResponse | None = None


@dataclass(frozen=True)
class Datasource:
    id: str
    organization_id: str | None = None
    kind: str | None = None
    type: str | None = None
    source_system: str | None = None
    name: str | None = None
    display_name: str | None = None
    external_id: str | None = None
    status: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    metadata: Mapping[str, Any] | None = None
    proto: connect_pb2.Connector | None = None


@dataclass(frozen=True)
class RegisterDatasourceResponse:
    datasource: Datasource
    request_id: str | None = None
    connector: connect_pb2.Connector | None = None
    proto: connect_pb2.RegisterConnectorResponse | None = None


@dataclass(frozen=True)
class AuditTrailEvent:
    id: str | None = None
    event_id: str | None = None
    organization_id: str | None = None
    connector_id: str | None = None
    actor: ActorRef | None = None
    ingester_principal: str | None = None
    actor_principal: str | None = None
    action: str | None = None
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
    proto: connect_pb2.AuditEvent | None = None


@dataclass(frozen=True)
class AuditTrailResponse:
    events: list[AuditTrailEvent]
    next_cursor: str | None = None
    request_id: str | None = None
    proto: connect_pb2.ListAuditEventsResponse | None = None
