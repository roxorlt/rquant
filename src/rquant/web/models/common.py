"""Building blocks shared by several responses."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from rquant.web.status import Status, UserState


class StatusInfo(BaseModel):
    """One user-level status: state (colour + icon), a short word, a one-line reason."""

    model_config = ConfigDict(frozen=True)

    state: UserState
    label: str
    reason: str

    @classmethod
    def of(cls, status: Status) -> StatusInfo:
        return cls(state=status.state, label=status.label, reason=status.reason)


class StateCounts(BaseModel):
    model_config = ConfigDict(frozen=True)

    total: int
    ok: int
    warn: int
    crit: int
    idle: int
    waiting: int
