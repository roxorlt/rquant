"""Closed pre-read access decision for a future durable audit authority."""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import Field, StrictStr

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel


class ServiceLogAccessRecord(RuntimeContractModel):
    """An authorized read attempt, recorded before the journal is contacted."""

    operator: StrictStr = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    unit: Literal["rquant-daily.service", "rquant-backup.service"]
    result_class: Literal["admitted"] = "admitted"
    at: AwareUtcDatetime


class ServiceLogAccessAudit(Protocol):
    """Return only after durably recording the authorized read attempt."""

    def record(self, event: ServiceLogAccessRecord) -> None: ...


__all__ = ["ServiceLogAccessAudit", "ServiceLogAccessRecord"]
