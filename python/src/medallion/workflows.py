"""Medallion as a durable-execution backend for a Temporaless workflow runtime.

This module deliberately contains no storage client. Temporaless already ships
``ConnectStore`` and ``ConnectQueryStore`` for the ``temporaless.v1``
storage contract; this is only the workspace-bound factory that hands a caller
one of those clients pointed at their Medallion endpoint, with the SDK's
existing credential and workspace binding attached as request headers::

    medallion = MedallionClient(base_url=..., api_key=..., workspace_id=...)
    store = medallion.workflows.store()        # temporaless ConnectStore
    query = medallion.workflows.query_store()  # temporaless ConnectQueryStore

The returned objects are Temporaless's own, so ``temporaless.run(...)`` and the
rest of the runtime work against Medallion with no further setup.

These credentials are server-side only. The storage surface can delete runs;
never ship a Medallion API key in a browser or mobile bundle.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import MedallionError
from .request import _RequestClient

if TYPE_CHECKING:  # pragma: no cover - typing only
    from temporaless.connectstore import ConnectQueryStore, ConnectStore

# The exact Temporaless release this SDK is built and gated against. Changing
# either value requires re-running the compat check, which fails when the
# installed package no longer matches the contract recorded below.
TEMPORALESS_VERSION = "0.10.7"
TEMPORALESS_COMMIT = "03dbf90732a8a043d1de0587b16a1f163c6efcd1"

RECORD_STORE_SERVICE = "temporaless.v1.RecordStoreService"
RECORD_QUERY_SERVICE = "temporaless.v1.RecordQueryService"

#: Every RPC on the point store, as the pinned contract declares it.
RECORD_STORE_METHODS: frozenset[str] = frozenset(
    {
        "GetStoreCapabilities",
        "GetWorkflow",
        "PutWorkflow",
        "GetLatestWorkflowRun",
        "GetTimer",
        "PutTimer",
        "GetActivity",
        "PutActivity",
        "GetClaim",
        "TryCreateClaim",
        "DeleteClaim",
        "GetEvent",
        "PutEvent",
        "DeliverEvent",
        "ListActivities",
        "ListTimers",
        "ListEvents",
        "ListClaims",
        "DeleteWorkflow",
        "DeleteActivity",
        "DeleteTimer",
        "DeleteEvent",
        "DeleteRun",
        "DueTimers",
    }
)

#: Every RPC on the optional derived index.
RECORD_QUERY_METHODS: frozenset[str] = frozenset(
    {"ListWorkflows", "ListActivities", "Sweep", "DueTimers"}
)

#: RPCs that belong to an operator identity, not to a workflow runtime:
#: replace-semantics event writes, the bounded deletions, and indexed
#: retention. This SDK ships no operator client and adds no convenience for
#: them; the server authorizes every method independently, so a
#: workflow-runtime credential is refused when it calls one. Run operator
#: tasks from a separately provisioned operator credential and its own client.
OPERATOR_METHODS: frozenset[str] = frozenset(
    {
        "PutEvent",
        "DeleteWorkflow",
        "DeleteActivity",
        "DeleteTimer",
        "DeleteEvent",
        "DeleteRun",
        "Sweep",
    }
)

_CLAIM_CAPABILITY_NAMES = {
    0: "CLAIM_CAPABILITY_UNSPECIFIED",
    1: "CLAIM_CAPABILITY_NO_CLAIMS",
    2: "CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS",
    3: "CLAIM_CAPABILITY_CAS_CLAIMS",
}
_EVENT_DELIVERY_CAPABILITY_NAMES = {
    0: "EVENT_DELIVERY_CAPABILITY_UNSPECIFIED",
    1: "EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE",
    2: "EVENT_DELIVERY_CAPABILITY_CREATE_ONLY",
}
_CLAIM_CAPABILITY_NO_CLAIMS = 1
_CLAIM_CAPABILITY_CREATE_ONLY = 2
_EVENT_DELIVERY_CREATE_ONLY = 2


@dataclass(frozen=True)
class StoreCapabilities:
    """The ``GetStoreCapabilities`` handshake, decoded.

    A workflow runtime that needs claim coordination or exactly-once event
    delivery must check this before it relies on either. A backend that
    reports no atomic create-if-absent cannot serve those runtimes, and
    Temporaless refuses the delivery rather than substituting a
    check-then-write sequence.
    """

    claim_capability: int
    event_delivery_capability: int

    @property
    def claim_capability_name(self) -> str:
        return _CLAIM_CAPABILITY_NAMES.get(
            self.claim_capability, f"UNKNOWN({self.claim_capability})"
        )

    @property
    def event_delivery_capability_name(self) -> str:
        return _EVENT_DELIVERY_CAPABILITY_NAMES.get(
            self.event_delivery_capability,
            f"UNKNOWN({self.event_delivery_capability})",
        )

    @property
    def supports_claims(self) -> bool:
        """True when the backend can atomically create claims."""

        return self.claim_capability == _CLAIM_CAPABILITY_CREATE_ONLY

    @property
    def supports_atomic_event_delivery(self) -> bool:
        """True when ``DeliverEvent`` has create-once semantics."""

        return self.event_delivery_capability == _EVENT_DELIVERY_CREATE_ONLY


class _MedallionIdentity:
    """Attaches the client's credential and workspace to every storage RPC.

    Implements the connectrpc ``UnaryInterceptor`` Protocol structurally. The
    whole storage contract is unary, so this covers every RPC. It is installed
    last so a caller-supplied interceptor cannot displace the identity headers.
    """

    def __init__(self, headers: dict[str, str]) -> None:
        self._headers = dict(headers)

    async def intercept_unary(self, call_next: Any, request: Any, ctx: Any) -> Any:
        for name, value in self._headers.items():
            ctx.request_headers[name] = value
        return await call_next(request, ctx)


class WorkflowsClient:
    """Hands out Temporaless storage clients bound to one Medallion workspace.

    Construct the store once at application start and reuse it; each call
    builds a fresh client with its own connection pool.
    """

    def __init__(self, requests: _RequestClient) -> None:
        self._requests = requests

    @property
    def workspace_id(self) -> str:
        """The immutable workspace every storage RPC is scoped to."""

        return self._requests.workspace_id

    @property
    def address(self) -> str:
        """The Medallion origin the storage clients call."""

        return self._requests.base_url

    def store(
        self,
        *,
        timeout_ms: int | None = None,
        read_max_bytes: int | None = None,
        interceptors: Iterable[Any] = (),
    ) -> ConnectStore:
        """Return a Temporaless ``ConnectStore`` backed by Medallion.

        Pass the result straight to a workflow runtime; the SDK adds only the
        credential and workspace headers. Caller ``interceptors`` (retry,
        tracing, logging) run first and cannot override those headers.
        """

        connectstore = _import_connectstore()
        return connectstore.ConnectStore.from_address(
            self.address,
            interceptors=self._interceptors(interceptors),
            timeout_ms=timeout_ms,
            read_max_bytes=read_max_bytes,
        )

    def query_store(
        self,
        *,
        timeout_ms: int | None = None,
        read_max_bytes: int | None = None,
        interceptors: Iterable[Any] = (),
    ) -> ConnectQueryStore:
        """Return a Temporaless ``ConnectQueryStore`` backed by Medallion.

        The query service is the optional derived index used for cross-run
        listing. Its ``Sweep`` retention RPC is operator-only and the server
        refuses it for a workflow-runtime credential.
        """

        connectstore = _import_connectstore()
        return connectstore.ConnectQueryStore.from_address(
            self.address,
            interceptors=self._interceptors(interceptors),
            timeout_ms=timeout_ms,
            read_max_bytes=read_max_bytes,
        )

    async def capabilities(
        self,
        *,
        store: ConnectStore | None = None,
        timeout_ms: int | None = None,
    ) -> StoreCapabilities:
        """Run the ``GetStoreCapabilities`` handshake against the backend.

        Call this before relying on claim coordination or atomic event
        delivery. Pass an existing ``store`` to reuse its connection.
        """

        target = self.store(timeout_ms=timeout_ms) if store is None else store
        return StoreCapabilities(
            claim_capability=int(await target.claim_capability()),
            event_delivery_capability=int(await target.event_delivery_capability()),
        )

    async def require_capabilities(
        self,
        *,
        claims: bool = True,
        atomic_event_delivery: bool = True,
        store: ConnectStore | None = None,
        timeout_ms: int | None = None,
    ) -> StoreCapabilities:
        """Fail fast unless the backend advertises what the runtime needs.

        A runtime that coordinates claims or delivers events exactly once must
        not be pointed at a backend that reports neither; this turns that into
        a startup error instead of a correctness bug under concurrency.
        """

        capabilities = await self.capabilities(store=store, timeout_ms=timeout_ms)
        missing: list[str] = []
        if claims and not capabilities.supports_claims:
            missing.append(
                f"atomic claim creation (reported {capabilities.claim_capability_name})"
            )
        if atomic_event_delivery and not capabilities.supports_atomic_event_delivery:
            missing.append(
                "atomic create-if-absent event delivery "
                f"(reported {capabilities.event_delivery_capability_name})"
            )
        if missing:
            raise MedallionError(
                "This Medallion workspace does not advertise "
                + "; ".join(missing)
                + ". A workflow runtime that depends on it must not use this backend.",
                code="MEDALLION_STORE_CAPABILITY_UNAVAILABLE",
            )
        return capabilities

    def _interceptors(self, caller: Iterable[Any]) -> tuple[Any, ...]:
        # Identity goes last: connectrpc runs the final interceptor closest to
        # the wire, so these headers are always the ones sent.
        return (*tuple(caller), _MedallionIdentity(self._requests.identity_headers()))


def _import_connectstore() -> Any:
    try:
        from temporaless import connectstore
    except ImportError as error:
        raise MedallionError(
            "The workflows surface requires Temporaless "
            f"{TEMPORALESS_VERSION}; install 'medallion[workflows]'.",
            code="MEDALLION_TEMPORALESS_REQUIRED",
        ) from error
    except AttributeError as error:
        # Both packages own the top-level `buf` module. This SDK vendors a
        # reduced buf.validate from its attested contract bundle, and
        # Temporaless's protovalidate needs the complete one, so whichever
        # was installed last wins. Say so, instead of surfacing an unrelated
        # AttributeError from deep inside protovalidate.
        raise MedallionError(
            "Temporaless could not import because this SDK's vendored "
            "buf.validate is shadowing the complete one it needs. Reinstall "
            "Temporaless after medallion so its copy resolves last.",
            code="MEDALLION_TEMPORALESS_INCOMPATIBLE",
        ) from error
    return connectstore
