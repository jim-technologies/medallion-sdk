from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from .errors import MedallionError


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
