from __future__ import annotations

from typing import Any


class MedallionError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id


class MedallionAPIError(MedallionError):
    def __init__(
        self,
        message: str,
        *,
        status: int,
        request_id: str | None = None,
        response_body: Any = None,
    ) -> None:
        super().__init__(
            message,
            code="MEDALLION_API_ERROR",
            request_id=request_id,
        )
        self.status = status
        self.response_body = response_body
