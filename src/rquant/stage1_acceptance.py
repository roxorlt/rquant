"""Typed, single-strategy planning for Stage 1 research acceptance."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.backfill_manifest import MinuteBackfillPlan
from rquant.backfill_state import BackfillManifestInput, BackfillManifestStatus

Stage1Strategy = Literal["n_shape", "growth_board_surge", "auction_gap"]
Stage1AcceptanceDisposition = Literal["ready", "blocked", "retired"]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MIN_TEMPORARY_DISK_BYTES = 256 * 1024 * 1024
_EXPECTED_MINUTE_ROWS_PER_SESSION = 241
_ESTIMATED_BYTES_PER_MINUTE_ROW = 96


class Stage1AcceptanceIdentityError(RuntimeError):
    """The requested acceptance identity differs from persisted evidence."""


class _BackfillStateReader(Protocol):
    def load_manifest(self, manifest_id: str) -> BackfillManifestInput | None: ...

    def get_manifest_status(self, manifest_id: str) -> BackfillManifestStatus: ...


class Stage1AcceptanceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Stage1AcceptanceSpec(Stage1AcceptanceModel):
    strategy: Stage1Strategy
    manifest_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    start_date: date
    end_date: date
    expected_code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    @model_validator(mode="after")
    def validate_range(self) -> Stage1AcceptanceSpec:
        if self.start_date > self.end_date:
            raise ValueError("acceptance start_date cannot be after end_date")
        return self


class Stage1AcceptanceBudget(Stage1AcceptanceModel):
    estimated_snapshot_scan_rows: int = Field(ge=0)
    estimated_temporary_disk_bytes: int = Field(ge=0)
    estimated_static_seconds: float = Field(ge=0)
    formal_replay_sample_limit: int = Field(ge=0)
    next_protected_window_start: datetime


class Stage1AcceptancePlan(Stage1AcceptanceModel):
    status: Literal["dry_run"] = "dry_run"
    disposition: Stage1AcceptanceDisposition
    apply_required: bool
    spec: Stage1AcceptanceSpec
    manifest_status: BackfillManifestStatus
    budget: Stage1AcceptanceBudget
    blockers: tuple[str, ...] = ()


def _next_protected_window_start(now: datetime) -> datetime:
    local = now.astimezone(_SHANGHAI)
    candidate = local.date()
    start = datetime.combine(candidate, time(9, 15), tzinfo=_SHANGHAI)
    if local >= start:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return datetime.combine(candidate, time(9, 15), tzinfo=_SHANGHAI)


def _validate_identity(
    plan: MinuteBackfillPlan,
    spec: Stage1AcceptanceSpec,
    *,
    observed_code_commit: str,
    require_manifest_commit: bool,
) -> None:
    manifest = plan.manifest
    if observed_code_commit != spec.expected_code_commit:
        raise Stage1AcceptanceIdentityError(
            "observed code commit does not match expected commit"
        )
    if manifest.spec.strategy_id != spec.strategy:
        raise Stage1AcceptanceIdentityError(
            "persisted manifest strategy does not match acceptance strategy"
        )
    if manifest.manifest_id != spec.manifest_id:
        raise Stage1AcceptanceIdentityError(
            "persisted manifest id does not match acceptance manifest"
        )
    if require_manifest_commit and manifest.code_commit != spec.expected_code_commit:
        raise Stage1AcceptanceIdentityError(
            "persisted manifest commit does not match acceptance commit"
        )
    if (
        manifest.start_date != spec.start_date
        or manifest.end_date != spec.end_date
    ):
        raise Stage1AcceptanceIdentityError(
            "persisted manifest date range does not match acceptance range"
        )


def build_stage1_acceptance_plan(
    state: _BackfillStateReader,
    spec: Stage1AcceptanceSpec,
    *,
    observed_code_commit: str,
    now: datetime,
) -> Stage1AcceptancePlan:
    """Inspect exactly one manifest and return a no-write acceptance decision."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("acceptance planning time must be timezone-aware")
    persisted = state.load_manifest(spec.manifest_id)
    if persisted is None:
        raise Stage1AcceptanceIdentityError(
            f"unknown acceptance manifest: {spec.manifest_id}"
        )
    plan = MinuteBackfillPlan.model_validate(persisted.payload)
    manifest_status = state.get_manifest_status(spec.manifest_id)
    retired_historical_manifest = (
        manifest_status.status == "abandoned"
        and spec.strategy == "auction_gap"
    )
    _validate_identity(
        plan,
        spec,
        observed_code_commit=observed_code_commit,
        require_manifest_commit=not retired_historical_manifest,
    )

    if manifest_status.status == "completed":
        disposition: Stage1AcceptanceDisposition = "ready"
        blockers: tuple[str, ...] = ()
        apply_required = True
    elif manifest_status.status == "abandoned" and spec.strategy == "auction_gap":
        disposition = "retired"
        blockers = ()
        apply_required = False
    else:
        disposition = "blocked"
        blockers = (f"manifest_not_completed:{manifest_status.status}",)
        apply_required = False

    estimated_snapshot_scan_rows = sum(
        len(window.open_dates) for window in plan.windows
    ) * _EXPECTED_MINUTE_ROWS_PER_SESSION
    estimated_scan_bytes = (
        estimated_snapshot_scan_rows * _ESTIMATED_BYTES_PER_MINUTE_ROW
    )

    return Stage1AcceptancePlan(
        disposition=disposition,
        apply_required=apply_required,
        spec=spec,
        manifest_status=manifest_status,
        budget=Stage1AcceptanceBudget(
            estimated_snapshot_scan_rows=estimated_snapshot_scan_rows,
            estimated_temporary_disk_bytes=max(
                _MIN_TEMPORARY_DISK_BYTES,
                plan.estimate.estimated_disk_bytes,
                estimated_scan_bytes,
            ),
            estimated_static_seconds=plan.estimate.total_seconds,
            formal_replay_sample_limit=len(plan.manifest.eligibilities),
            next_protected_window_start=_next_protected_window_start(now),
        ),
        blockers=blockers,
    )
