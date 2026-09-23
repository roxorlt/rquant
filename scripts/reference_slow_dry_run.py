"""Read-only real-data dry run of the reference-slow source's capture path (#293).

Runs exactly what `source.reference-slow` runs at 09:20 -- `stock_st`, `stock_basic` for L / D /
P, the prior daily universe out of the read-only replica, `adj_factor`, `suspend_d`, the fact
assembly, the batch payload and the serving payload the publisher would derive from it -- and
stops before the first write: no spool batch, no quota ledger, no registry, no authority.

    PYTHONPATH=<checkout>/src <venv>/bin/python <checkout>/scripts/reference_slow_dry_run.py \\
        --database /home/lighthouse/rquant/data/rquant_ro.duckdb --trade-date 2026-09-23

Run it from the directory whose `.env` holds the Tushare token (the adapter reads the token
from there, read-only). `--trade-date` must be a session whose pre-open sources already
exist, i.e. today or earlier; the capture clock is pinned to 09:20 of that date so the
09:25 cutoff checks pass the way they do in production. The calendar is built in memory from
Tushare `trade_cal` unless `--calendar` names the installed calendar authority file.

Exit status 0 means the whole path validated on real data; 1 means a step refused, and the
last line says which one and why.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

_SHANGHAI = ZoneInfo("Asia/Shanghai")
#: the capture clock: the source's window opens at 09:20 and its responses must complete by
#: 09:25, so the dry run observes at 09:20:00 and completes at 09:20:30 of the target date
_CAPTURE_AT = time(9, 20)
_COMPLETED_AT = time(9, 20, 30)
_DRY_RUN_COMMIT = "0" * 40


@dataclass
class _Recorded:
    call: str
    rows: int
    columns: tuple[str, ...]


@dataclass
class _RecordingAdapter:
    """Forwards each source call to the real adapter and prints what came back."""

    inner: Any
    out: Callable[[str], None]
    calls: list[_Recorded] = field(default_factory=list)
    frames: dict[str, pd.DataFrame] = field(default_factory=dict)

    def _record(self, call: str, frame: pd.DataFrame) -> pd.DataFrame:
        columns = tuple(str(column) for column in getattr(frame, "columns", ()))
        rows = len(frame) if isinstance(frame, pd.DataFrame) else -1
        self.calls.append(_Recorded(call=call, rows=rows, columns=columns))
        self.frames[call] = frame
        self.out(f"  {call:<28} rows={rows:<6} columns={','.join(columns)}")
        return frame

    def stock_basic(self, list_status: str = "L") -> pd.DataFrame:
        return self._record(
            f"stock_basic({list_status})", self.inner.stock_basic(list_status=list_status)
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        return self._record("stock_st", self.inner.stock_st_raw(trade_date))

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
        return self._record("suspend_d", self.inner.suspend_d_raw(trade_date))

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        return self._record("adj_factor", self.inner.adj_factor_by_date(trade_date))


def _calendar_from_trade_cal(adapter: Any, *, target: date, generated_at: datetime) -> Any:
    from rquant.runtime_market_session import MarketCalendarAuthority

    start = target - timedelta(days=45)
    end = target + timedelta(days=45)
    open_dates = tuple(sorted(adapter.trade_cal(start, end)))
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=_DRY_RUN_COMMIT,
        coverage_start=start,
        coverage_end=end,
        open_dates=open_dates,
        generated_at=generated_at,
    )


def _prior_universe_count(database: Path, prior: date) -> int:
    import duckdb

    connection = duckdb.connect(str(database), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT count(*)
            FROM daily_bar AS daily
            JOIN adj_factor AS adjustment
              ON adjustment.ts_code = daily.ts_code
             AND adjustment.trade_date = daily.trade_date
            WHERE daily.trade_date = ?
            """,
            [prior],
        ).fetchone()
    finally:
        connection.close()
    return int(row[0]) if row else 0


def run_dry_run(
    *,
    database: Path,
    trade_date: date,
    adapter: Any,
    calendar: Any | None = None,
    out: Callable[[str], None] = print,
) -> int:
    """Run the capture path end to end without writing anything; 0 on success."""

    from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
    from rquant.reference_slow_publisher import (
        ReferenceSlowPublishReceipt,
        ReferenceSlowSourceSnapshot,
        build_reference_slow_serving_result,
    )
    from rquant.reference_slow_source import (
        ReferenceSlowSourceLimits,
        _security_source_facts,
        _st_codes,
        capture_reference_slow_source_snapshot,
    )
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json

    captured_at = datetime.combine(trade_date, _CAPTURE_AT, tzinfo=_SHANGHAI).astimezone(UTC)
    completed_at = datetime.combine(trade_date, _COMPLETED_AT, tzinfo=_SHANGHAI).astimezone(UTC)
    step = "calendar"
    try:
        out(f"[1] calendar  target_trade_date={trade_date.isoformat()}")
        if calendar is None:
            calendar = _calendar_from_trade_cal(
                adapter, target=trade_date, generated_at=captured_at - timedelta(hours=1)
            )
            out("  built in memory from Tushare trade_cal (not the installed authority)")
        prior_dates = tuple(item for item in calendar.open_dates if item < trade_date)
        next_dates = tuple(item for item in calendar.open_dates if item > trade_date)
        if trade_date not in calendar.open_dates or not prior_dates or not next_dates:
            raise RuntimeError(f"{trade_date.isoformat()} is not an open session with neighbours")
        prior = prior_dates[-1]
        out(
            f"  open_dates={len(calendar.open_dates)} prior={prior.isoformat()} "
            f"next={next_dates[0].isoformat()}"
        )

        step = "prior daily universe"
        out(f"[2] prior daily universe  database={database} (read_only)")
        prior_rows = _prior_universe_count(database, prior)
        out(f"  daily_bar JOIN adj_factor on {prior.isoformat()}: rows={prior_rows}")

        step = "source capture"
        out("[3] source responses (real Tushare calls, no quota ledger)")
        recording = _RecordingAdapter(inner=adapter, out=out)
        #: `snapshot_max_bytes=1` rules the private copy out, so on any platform the replica
        #: is read through the pinned descriptor or in place and nothing is copied anywhere
        limits = ReferenceSlowSourceLimits(snapshot_max_bytes=1, snapshot_min_free_bytes=0)
        snapshot = capture_reference_slow_source_snapshot(
            database_path=database,
            adapter=recording,
            calendar=calendar,
            target_trade_date=trade_date,
            captured_at=captured_at,
            completion_clock=lambda: completed_at,
            producer_commit=_DRY_RUN_COMMIT,
            limits=limits,
        )

        step = "stock_basic facts"
        out("[4] stock_basic facts per list (the same validator the capture used)")
        st_codes = _st_codes(
            recording.frames["stock_st"], target_trade_date=trade_date, limits=limits
        )
        out(f"  stock_st codes={len(st_codes)}")
        for status in ("L", "D", "P"):
            parsed = _security_source_facts(
                recording.frames[f"stock_basic({status})"],
                list_status=status,
                st_codes=st_codes,
                limits=limits,
            )
            skipped = parsed.skipped_invalid_codes
            out(
                f"  {status}: facts={len(parsed.facts)} skipped_invalid_codes={len(skipped)}"
                + (f" ({', '.join(skipped[:10])})" if skipped else "")
            )

        step = "batch payload"
        out("[5] sealed snapshot and batch payload (built in memory, not written)")
        payload = canonical_json_bytes(snapshot.model_dump(mode="json"))
        replayed = strict_model_validate_canonical_json(ReferenceSlowSourceSnapshot, payload)
        if replayed.content_sha256 != snapshot.content_sha256:
            raise RuntimeError("snapshot does not survive its canonical payload")
        envelope = BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.REFERENCE_SLOW,
            dataset_id="reference_slow_source",
            source="rquant.reference_slow_source",
            source_request_id=canonical_sha256({"dry_run": snapshot.content_sha256}),
            batch_id=snapshot.content_sha256,
            sequence=0,
            revision=1,
            event_time_start=snapshot.captured_at,
            event_time_end=snapshot.captured_at,
            source_time=snapshot.captured_at,
            received_at=snapshot.captured_at,
            available_at=snapshot.captured_at,
            row_count=len(snapshot.security_facts),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            quality_status=BatchQualityStatus.PUBLISHED,
            producer_version="reference-slow-dry-run",
            producer_commit=_DRY_RUN_COMMIT,
        )
        out(
            f"  daily_facts={len(snapshot.daily_facts)} "
            f"security_facts={len(snapshot.security_facts)} "
            f"suspended_codes={len(snapshot.suspended_codes)} payload_bytes={len(payload)}"
        )
        out(
            f"  envelope quality_status={envelope.quality_status.value} "
            f"row_count={envelope.row_count}"
        )
        for projection in snapshot.projections:
            out(f"  projection {projection.table_name:<24} rows={len(projection.rows)}")

        step = "serving payload"
        out("[6] reference_slow_authority serving payload (built in memory, not written)")
        receipt = ReferenceSlowPublishReceipt(
            target_trade_date=snapshot.target_trade_date,
            generation_id="0" * 64,
            source_snapshot_id=snapshot.content_sha256,
            inserted_record_count=0,
            security_count=len(snapshot.security_facts),
            revision=1,
            available_at=snapshot.captured_at,
        )
        serving = build_reference_slow_serving_result(snapshot=snapshot, receipt=receipt)
        out(
            f"  serving dataset_id={serving.dataset_id} status={serving.status.value} "
            f"projections={len(serving.payload.projections)}"
        )
    except Exception as exc:  # noqa: BLE001 - the dry run reports every refusal the same way
        out(f"DRY RUN FAILED at step '{step}': {type(exc).__name__}: {exc}")
        return 1
    out(
        f"DRY RUN OK: target_trade_date={trade_date.isoformat()} "
        f"daily_facts={len(snapshot.daily_facts)} security_facts={len(snapshot.security_facts)}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--calendar",
        type=Path,
        default=None,
        help="installed calendar authority file (needs --calendar-commit); default: trade_cal",
    )
    parser.add_argument("--calendar-commit", default=None)
    arguments = parser.parse_args(argv)

    from rquant.adapter.tushare import TushareAdapter

    adapter = TushareAdapter()
    calendar = None
    if arguments.calendar is not None:
        from rquant.runtime_market_session import load_market_calendar_authority

        if not arguments.calendar_commit:
            parser.error("--calendar needs --calendar-commit")
        calendar = load_market_calendar_authority(
            arguments.calendar, expected_commit=arguments.calendar_commit
        )
    return run_dry_run(
        database=arguments.database,
        trade_date=arguments.trade_date,
        adapter=adapter,
        calendar=calendar,
    )


if __name__ == "__main__":
    sys.exit(main())
