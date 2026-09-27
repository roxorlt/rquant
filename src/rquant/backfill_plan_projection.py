"""Bounded Serving rows for sealed, unverified daily-bar backfill plans."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import zlib
from collections.abc import Sequence
from datetime import date, datetime
from typing import Literal

import duckdb
from pydantic import Field, model_validator

from rquant.backfill_plan_core import (
    BackfillPlanEstimate,
    BackfillPlanMonth,
    BackfillPlanSource,
    DailyBarBackfillPlan,
)
from rquant.backfill_plan_job_projection import BackfillPlanJobSnapshot
from rquant.data_audit_evidence import MAX_AUDIT_DAYS
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.serving_read_models import ServingProjectionPayload

BACKFILL_PLAN_PROJECTION_TABLES = frozenset(
    {
        "backfill_plan_catalog",
        "backfill_plan_index",
        "backfill_plan_preview",
        "backfill_plan_archive",
        "backfill_plan_progress",
        "backfill_plan_job",
        "backfill_plan_event",
    }
)
MAX_PREVIEW_BACKFILL_PLANS = 8
MAX_DISCOVERABLE_BACKFILL_PLANS = 4096
MAX_BACKFILL_ARCHIVE_BYTES = 4 * 1024 * 1024
MAX_BACKFILL_DETAIL_BYTES = 256 * 1024
MAX_BACKFILL_ENCODED_CELL_BYTES = 48 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def backfill_plan_index_row(
    plan: DailyBarBackfillPlan, *, rank: int, published_at: datetime
) -> dict[str, object]:
    """Expose estimates and provenance as claims, never as production authority."""
    plan = DailyBarBackfillPlan.model_validate(plan)
    return {
        "rank": rank,
        "plan_hash": plan.content_sha256,
        "published_at": published_at.isoformat(),
        "audit_start": plan.audit_start.isoformat(),
        "completed_through": plan.completed_through.isoformat(),
        "cutoff_observed_at": plan.cutoff_observed_at_utc.isoformat(),
        "missing_day_count": len(plan.missing_dates),
        "estimated_seconds": str(plan.estimate.estimated_seconds),
        "source_mode": plan.source.mode,
        "snapshot_label": plan.source.snapshot_label,
        "identity_verified": plan.source.identity_verified,
        "collection_complete_verified": plan.source.collection_complete_verified,
        "quota_status": plan.estimate.quota_status,
        "executable": plan.executable,
    }


def backfill_plan_preview_row(plan: DailyBarBackfillPlan) -> dict[str, object]:
    """Keep the complete date/month/estimate/source preview within one plan row."""
    plan = DailyBarBackfillPlan.model_validate(plan)
    return {
        "plan_hash": plan.content_sha256,
        "missing_dates_json": _compact_json([day.isoformat() for day in plan.missing_dates]),
        "monthly_json": _compact_json([month.model_dump(mode="json") for month in plan.monthly]),
        "estimate_json": _compact_json(plan.estimate.model_dump(mode="json")),
        "source_json": _compact_json(plan.source.model_dump(mode="json")),
        "gap_count": len(plan.gaps),
        "coverage_scope": plan.coverage_scope,
    }


class BackfillPlanServingDetail(RuntimeContractModel):
    """A bounded plan preview sealed inside a Serving generation."""

    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    audit_start: date
    completed_through: date
    cutoff_observed_at: AwareUtcDatetime
    published_at: AwareUtcDatetime
    missing_dates: tuple[date, ...] = Field(max_length=MAX_AUDIT_DAYS)
    monthly: tuple[BackfillPlanMonth, ...] = Field(max_length=MAX_AUDIT_DAYS)
    estimate: BackfillPlanEstimate
    source: BackfillPlanSource
    gap_count: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    coverage_scope: Literal["whole_day_presence_only"] = "whole_day_presence_only"
    executable: Literal[False] = False

    @model_validator(mode="after")
    def validate_detail(self) -> BackfillPlanServingDetail:
        if self.audit_start > self.completed_through or self.published_at < self.cutoff_observed_at:
            raise ValueError("backfill plan detail dates disagree")
        if (
            any(
                day < self.audit_start or day > self.completed_through for day in self.missing_dates
            )
            or tuple(sorted(set(self.missing_dates))) != self.missing_dates
        ):
            raise ValueError("backfill plan detail missing dates are invalid")
        if sum(month.missing_open_days for month in self.monthly) != len(self.missing_dates):
            raise ValueError("backfill plan detail month totals disagree")
        if self.estimate.logical_operations.daily != len(self.missing_dates):
            raise ValueError("backfill plan detail estimate disagrees")
        return self

    @classmethod
    def from_plan(
        cls, plan: DailyBarBackfillPlan, *, published_at: datetime
    ) -> BackfillPlanServingDetail:
        plan = DailyBarBackfillPlan.model_validate(plan)
        return cls(
            plan_hash=plan.content_sha256,
            audit_start=plan.audit_start,
            completed_through=plan.completed_through,
            cutoff_observed_at=plan.cutoff_observed_at_utc,
            published_at=published_at,
            missing_dates=plan.missing_dates,
            monthly=plan.monthly,
            estimate=plan.estimate,
            source=plan.source,
            gap_count=len(plan.gaps),
            coverage_scope=plan.coverage_scope,
        )


def _detail_bytes(detail: BackfillPlanServingDetail) -> bytes:
    return _compact_json(detail.model_dump(mode="json")).encode("utf-8")


def _archive_row(plan: DailyBarBackfillPlan, published_at: datetime) -> dict[str, object]:
    detail = BackfillPlanServingDetail.from_plan(plan, published_at=published_at)
    raw = _detail_bytes(detail)
    if len(raw) > MAX_BACKFILL_DETAIL_BYTES:
        raise ValueError("backfill plan detail exceeds its bound")
    encoded = base64.b64encode(zlib.compress(raw, level=9)).decode("ascii")
    if len(encoded) > MAX_BACKFILL_ENCODED_CELL_BYTES:
        raise ValueError("backfill plan archive cell exceeds its bound")
    return {
        "plan_hash": detail.plan_hash,
        "encoding": "zlib-json-v1",
        "detail_sha256": hashlib.sha256(raw).hexdigest(),
        "payload_base64": encoded,
    }


def decode_backfill_plan_archive_row(row: dict[str, object]) -> BackfillPlanServingDetail:
    """Bound decompression and verify one generation-bound detail payload."""
    if row.get("encoding") != "zlib-json-v1":
        raise ValueError("backfill plan archive encoding is invalid")
    encoded = row.get("payload_base64")
    digest = row.get("detail_sha256")
    plan_hash = row.get("plan_hash")
    if (
        not isinstance(encoded, str)
        or len(encoded) > MAX_BACKFILL_ENCODED_CELL_BYTES
        or not isinstance(digest, str)
        or _SHA256.fullmatch(digest) is None
        or not isinstance(plan_hash, str)
        or _SHA256.fullmatch(plan_hash) is None
    ):
        raise ValueError("backfill plan archive metadata is invalid")
    try:
        compressed = base64.b64decode(encoded, validate=True)
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, MAX_BACKFILL_DETAIL_BYTES + 1)
        if (
            len(raw) > MAX_BACKFILL_DETAIL_BYTES
            or not decoder.eof
            or decoder.unused_data
            or decoder.unconsumed_tail
        ):
            raise ValueError("backfill plan archive decompression is incomplete")
        raw += decoder.flush()
    except (ValueError, zlib.error) as exc:
        raise ValueError("backfill plan archive is invalid") from exc
    if len(raw) > MAX_BACKFILL_DETAIL_BYTES or hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("backfill plan archive detail hash disagrees")
    try:
        detail = BackfillPlanServingDetail.model_validate_json(raw)
    except ValueError as exc:
        raise ValueError("backfill plan archive detail is invalid") from exc
    if detail.plan_hash != plan_hash or _detail_bytes(detail) != raw:
        raise ValueError("backfill plan archive detail is not canonical")
    return detail


def read_backfill_plan_detail_from_serving(
    cursor: duckdb.DuckDBPyConnection, plan_hash: str
) -> BackfillPlanServingDetail | None:
    """Resolve an old plan from one borrowed immutable Serving generation only."""
    if _SHA256.fullmatch(plan_hash) is None:
        raise ValueError("backfill plan hash is invalid")
    index = cursor.execute(
        "SELECT plan_hash, audit_start, completed_through, cutoff_observed_at, "
        "published_at, missing_day_count, estimated_seconds, source_mode, "
        "snapshot_label, identity_verified, collection_complete_verified, "
        "quota_status, executable FROM backfill_plan_index WHERE plan_hash = ?",
        (plan_hash,),
    ).fetchone()
    if index is None:
        return None
    archive = cursor.execute(
        "SELECT plan_hash, encoding, detail_sha256, payload_base64 "
        "FROM backfill_plan_archive WHERE plan_hash = ?",
        (plan_hash,),
    ).fetchone()
    if archive is None:
        raise ValueError("backfill plan is indexed without a same-generation detail")
    detail = decode_backfill_plan_archive_row(
        dict(
            zip(
                ("plan_hash", "encoding", "detail_sha256", "payload_base64"),
                archive,
                strict=True,
            )
        )
    )
    if (
        detail.plan_hash != index[0]
        or detail.audit_start != index[1]
        or detail.completed_through != index[2]
        or detail.cutoff_observed_at != index[3]
        or detail.published_at != index[4]
        or len(detail.missing_dates) != index[5]
        or str(detail.estimate.estimated_seconds) != index[6]
        or detail.source.mode != index[7]
        or detail.source.snapshot_label != index[8]
        or detail.source.identity_verified != index[9]
        or detail.source.collection_complete_verified != index[10]
        or detail.estimate.quota_status != index[11]
        or detail.executable != index[12]
    ):
        raise ValueError("backfill plan index and same-generation detail disagree")
    return detail


def project_backfill_plans(
    plans: Sequence[tuple[DailyBarBackfillPlan, datetime]],
    *,
    available_at: datetime,
    job_snapshot: BackfillPlanJobSnapshot | None = None,
) -> tuple[ServingProjectionPayload, ...]:
    """Seal every catalogued plan in one generation or refuse that generation."""
    if len(plans) > MAX_DISCOVERABLE_BACKFILL_PLANS:
        raise ValueError("backfill plan catalog count exceeds its bound")
    preview = plans[:MAX_PREVIEW_BACKFILL_PLANS]
    archive = tuple(_archive_row(plan, published) for plan, published in plans)
    if len(_compact_json(archive).encode("utf-8")) > MAX_BACKFILL_ARCHIVE_BYTES:
        raise ValueError("backfill plan same-generation archive exceeds its bound")
    if job_snapshot is not None:
        job_snapshot.validate(plan_hashes=frozenset(plan.content_sha256 for plan, _ in plans))
    progress = job_snapshot.progress if job_snapshot is not None else None
    rows = {
        "backfill_plan_catalog": (
            {
                "catalog_key": "current",
                "total_plan_count": len(plans),
                "indexed_plan_count": len(plans),
                "preview_plan_count": len(preview),
                "has_older_plans": False,
                "oldest_indexed_hash": plans[-1][0].content_sha256 if plans else None,
            },
        ),
        "backfill_plan_index": tuple(
            backfill_plan_index_row(plan, rank=rank, published_at=published)
            for rank, (plan, published) in enumerate(plans)
        ),
        "backfill_plan_preview": tuple(backfill_plan_preview_row(plan) for plan, _ in preview),
        "backfill_plan_archive": archive,
        "backfill_plan_job": (
            progress.model_dump(mode="json")
            if progress is not None
            else {
                "status_key": "current",
                "availability": "unavailable",
                "event_history": "unavailable",
                "task_id": None,
                "status": None,
                "attempts": None,
                "created_at": None,
                "updated_at": None,
                "plan_hash": None,
                "error_code": None,
            },
        ),
        "backfill_plan_progress": (
            {"status_key": "current", "availability": "unavailable", "task_id": None},
        ),
        "backfill_plan_event": (
            tuple(event.model_dump(mode="json") for event in job_snapshot.events)
            if job_snapshot is not None
            else ()
        ),
    }
    return tuple(
        ServingProjectionPayload(table_name=name, available_at=available_at, rows=rows[name])
        for name in sorted(rows)
    )
