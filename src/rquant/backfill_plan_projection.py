"""Bounded Serving rows for sealed, unverified daily-bar backfill plans."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime

from rquant.backfill_plan_core import DailyBarBackfillPlan
from rquant.serving_read_models import ServingProjectionPayload

BACKFILL_PLAN_PROJECTION_TABLES = frozenset(
    {
        "backfill_plan_catalog",
        "backfill_plan_index",
        "backfill_plan_preview",
        "backfill_plan_progress",
    }
)
MAX_INDEXED_BACKFILL_PLANS = 256
MAX_PREVIEW_BACKFILL_PLANS = 8
MAX_DISCOVERABLE_BACKFILL_PLANS = 4096


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
        "monthly_json": _compact_json(
            [month.model_dump(mode="json") for month in plan.monthly]
        ),
        "estimate_json": _compact_json(plan.estimate.model_dump(mode="json")),
        "source_json": _compact_json(plan.source.model_dump(mode="json")),
        "gap_count": len(plan.gaps),
        "coverage_scope": plan.coverage_scope,
    }


def project_backfill_plans(
    plans: Sequence[tuple[DailyBarBackfillPlan, datetime]],
    *,
    total_count: int,
    has_older_plans: bool,
    available_at: datetime,
) -> tuple[ServingProjectionPayload, ...]:
    """Publish a bounded index, complete first-page previews, and unknown progress."""
    if not 0 <= len(plans) <= MAX_INDEXED_BACKFILL_PLANS <= MAX_DISCOVERABLE_BACKFILL_PLANS:
        raise ValueError("backfill plan index exceeds its bound")
    if total_count < len(plans) or total_count > MAX_DISCOVERABLE_BACKFILL_PLANS:
        raise ValueError("backfill plan catalog count exceeds its bound")
    preview = plans[:MAX_PREVIEW_BACKFILL_PLANS]
    rows = {
        "backfill_plan_catalog": (
            {
                "catalog_key": "current",
                "total_plan_count": total_count,
                "indexed_plan_count": len(plans),
                "preview_plan_count": len(preview),
                "has_older_plans": has_older_plans,
                "oldest_indexed_hash": plans[-1][0].content_sha256 if plans else None,
            },
        ),
        "backfill_plan_index": tuple(
            backfill_plan_index_row(plan, rank=rank, published_at=published)
            for rank, (plan, published) in enumerate(plans)
        ),
        "backfill_plan_preview": tuple(
            backfill_plan_preview_row(plan) for plan, _ in preview
        ),
        "backfill_plan_progress": (
            {"status_key": "current", "availability": "unavailable", "task_id": None},
        ),
    }
    return tuple(
        ServingProjectionPayload(table_name=name, available_at=available_at, rows=rows[name])
        for name in sorted(rows)
    )
