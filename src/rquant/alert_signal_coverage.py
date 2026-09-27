"""A bounded, positive proof for the notifier-owned signal source."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Annotated, Self

from pydantic import Field, StrictInt, StringConstraints, model_validator

from rquant.alert_ack import alert_window_start
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.serving_read_models import ServingSignalRecord

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


def signal_window_digest(records: Iterable[ServingSignalRecord]) -> str:
    """Bind full verified envelopes, not just IDs from the small Serving table."""
    return canonical_sha256(
        {
            "contract": "signal-alert-window/v1",
            "records": tuple(
                {
                    "global_sequence": record.global_sequence,
                    "signal": record.signal,
                }
                for record in records
            ),
        }
    )


class SignalSourceCoverageReceipt(RuntimeContractModel):
    """One inspected spool prefix verified against one notifier SQLite read transaction."""

    source_generation_id: Sha256
    first_global_sequence: StrictInt = Field(ge=1)
    source_high_watermark: StrictInt = Field(ge=0)
    source_inspected_at: AwareUtcDatetime
    window_start: AwareUtcDatetime
    window_end: AwareUtcDatetime
    window_row_count: StrictInt = Field(ge=0)
    window_rows_sha256: Sha256
    prefix_row_count: StrictInt = Field(ge=0)
    prefix_rows_sha256: Sha256

    @model_validator(mode="after")
    def validate_positive_proof(self) -> Self:
        if self.first_global_sequence != 1:
            raise ValueError("signal coverage starts after the source beginning")
        if self.prefix_row_count != self.source_high_watermark:
            raise ValueError("signal prefix count differs from high watermark")
        if self.window_row_count > self.prefix_row_count:
            raise ValueError("signal window count exceeds source prefix")
        if self.window_end != self.source_inspected_at:
            raise ValueError("signal window end differs from source observation")
        expected_start = alert_window_start(
            count_as_of=self.window_end,
            activated_at=datetime(1970, 1, 1, tzinfo=UTC),
        )
        if self.window_start != expected_start:
            raise ValueError("signal coverage window start is invalid")
        return self


__all__ = ["SignalSourceCoverageReceipt", "signal_window_digest"]
