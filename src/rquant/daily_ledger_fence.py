"""Production adapter from the daily ledger's public fence API to pipeline stages."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import datetime

from rquant.daily_pipeline_ledger import (
    DailyLedgerStageFence,
    DailyPipelineLedger,
    DailyStageAttempt,
    DailyWriterLease,
)


class DailyLedgerFenceGuard:
    """Bind one stable writer lease to stage-local fence contexts."""

    def __init__(self, *, ledger: DailyPipelineLedger, lease: DailyWriterLease) -> None:
        self._ledger = ledger
        self._lease = DailyWriterLease.model_validate(lease)

    def __call__(
        self,
        attempt: DailyStageAttempt,
        checked_at: datetime,
        /,
    ) -> AbstractContextManager[DailyLedgerStageFence]:
        return self._ledger.hold_stage_fence(
            self._lease,
            attempt,
            checked_at=checked_at,
        )


__all__ = ["DailyLedgerFenceGuard"]
