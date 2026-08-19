from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .errors import MedallionError
from .types import ActorRef, ResourceRef


def stable_idempotency_key(namespace: str, *source_identity: object) -> str:
    """Create a deterministic UUID key from a durable source identity."""

    if not isinstance(namespace, str) or not namespace.strip() or not source_identity:
        raise MedallionError(
            "namespace and at least one stable source identity are required.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    parts = [normalize_id(item, "source_identity") for item in source_identity]
    logical_identity = "\x1f".join([namespace.strip(), *parts])
    try:
        logical_identity.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise MedallionError(
            "Idempotency source identities must contain valid Unicode scalar values.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        ) from None
    candidate = f"{namespace.strip()}:{uuid5(NAMESPACE_URL, logical_identity)}"
    if len(candidate.encode("utf-8")) > 512:
        raise MedallionError(
            "The generated idempotency key must not exceed 512 UTF-8 bytes.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    return candidate


def normalize_id(value: object, path: str = "id") -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        raise MedallionError(
            f"Invalid ID at {path}. Expected string or integer.",
            code="MEDALLION_INVALID_ID",
        )
    if isinstance(value, int):
        return str(value)
    raise MedallionError(
        f"Invalid ID at {path}. Expected string or integer.",
        code="MEDALLION_INVALID_ID",
    )


def normalize_actor(actor: ActorRef | Mapping[str, Any]) -> dict[str, str]:
    if isinstance(actor, ActorRef):
        raw_id = actor.id
        actor_type = actor.type
        provider = actor.provider
    else:
        raw_id = actor.get("id")
        actor_type = actor.get("type")
        provider = actor.get("provider")

    normalized: dict[str, str] = {"id": normalize_id(raw_id, "actor.id")}
    if isinstance(actor_type, str) and actor_type:
        normalized["type"] = actor_type
    if isinstance(provider, str) and provider:
        normalized["provider"] = provider
    return normalized


def actor_principal_from_ref(actor: Mapping[str, str]) -> str:
    parts = [
        value
        for value in (actor.get("type"), actor.get("provider"), actor["id"])
        if value
    ]
    return ":".join(parts)


def actor_from_principal(value: str | None) -> ActorRef | None:
    if not value:
        return None
    parts = value.split(":")
    if len(parts) < 2:
        return ActorRef(id=value)
    return ActorRef(
        type=parts[0],
        provider=":".join(parts[1:-1]) or None,
        id=parts[-1],
    )


def normalize_resource(resource: ResourceRef | Mapping[str, Any]) -> dict[str, str]:
    if isinstance(resource, ResourceRef):
        resource_type = resource.type
        raw_id = resource.id
    else:
        resource_type = resource.get("type")
        raw_id = resource.get("id")

    if not isinstance(resource_type, str) or not resource_type:
        raise MedallionError(
            "resource.type is required.",
            code="MEDALLION_INVALID_ID",
        )
    return {
        "type": resource_type,
        "id": normalize_id(raw_id, "resource.id"),
    }


def normalize_id_record(
    values: Mapping[str, object],
    path: str = "primaryKey",
) -> dict[str, str]:
    return {key: normalize_id(value, f"{path}.{key}") for key, value in values.items()}


def same_actor(left: ActorRef | None, right: Mapping[str, str]) -> bool:
    if left is None:
        return False
    try:
        left_id = normalize_id(left.id, "actor.id")
    except MedallionError:
        return False
    return (
        left_id == right.get("id")
        and left.type == right.get("type")
        and left.provider == right.get("provider")
    )
