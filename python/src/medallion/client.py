from __future__ import annotations

from .request import RetryConfig, _canonical_workspace_id, _RequestClient
from .tables import IngestClient, TablesClient
from .tracing import TracingConfig
from .workflows import WorkflowsClient


class MedallionClient:
    def __init__(
        self,
        *,
        base_url: str,
        workspace_id: str,
        api_key: str | None = None,
        access_token: str | None = None,
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
        self.workflows = WorkflowsClient(requests)
        self._workspace_id = workspace

    @property
    def workspace_id(self) -> str:
        """The immutable workspace selected for this client."""

        return self._workspace_id
