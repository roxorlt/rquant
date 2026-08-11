from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from rquant.auction_universe_authority import AuctionUniverseAuthority
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import auction_match_source_builder
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.source_quota_store import SourceQuotaAttemptOutcome, SourceQuotaStore

COMMIT = "a" * 40
TRADE_DATE = date(2026, 7, 31)
CAPTURE_AT = datetime(2026, 7, 31, 1, 26, 5, tzinfo=UTC)


class _Adapter:
    def __init__(self, responses: list[pd.DataFrame | BaseException] | None = None) -> None:
        self.calls: list[date] = []
        self._responses = list(responses or [_frame()])

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "price": 10.2,
                "vol": 100_000.0,
                "amount": 1_020_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.1,
                "volume_ratio": 1.5,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": TRADE_DATE,
                "price": 9.1,
                "vol": 80_000.0,
                "amount": 728_000.0,
                "pre_close": 9.0,
                "turnover_rate": 0.08,
                "volume_ratio": 1.4,
            },
        ]
    )


def _write_authorities(
    tmp_path: Path,
    *,
    open_date: bool = True,
) -> tuple[Path, Path, MarketCalendarAuthority]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=TRADE_DATE,
        coverage_end=TRADE_DATE,
        open_dates=(TRADE_DATE,) if open_date else (),
        generated_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
    )
    calendar_path = tmp_path / "calendar.json"
    calendar_path.write_text(
        json.dumps(
            calendar.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    calendar_path.chmod(0o600)
    universe = AuctionUniverseAuthority.create(
        effective_trade_date=TRADE_DATE,
        reference_trade_date=date(2026, 7, 30),
        available_at=datetime(2026, 7, 30, 9, 0, tzinfo=UTC),
        producer_commit=COMMIT,
        source_snapshot_id="b" * 64,
        codes=("600000.SH", "000001.SZ"),
    )
    universe_path = tmp_path / "universe.json"
    universe_path.write_bytes(universe.canonical_json_bytes())
    universe_path.chmod(0o600)
    return calendar_path, universe_path, calendar


def _manifest(tmp_path: Path, *, open_date: bool = True) -> RuntimeServiceManifest:
    calendar_path, universe_path, calendar = _write_authorities(
        tmp_path,
        open_date=open_date,
    )
    return RuntimeServiceManifest(
        service_id="source.auction-match",
        service_kind=RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=10,
        stale_after_seconds=300,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(tmp_path / "auction-match"),
            "quota_path": str(tmp_path / "auction-match" / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "producer_version": "auction-match-v1",
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "universe_path": str(universe_path),
            "max_attempts": 3,
        },
    )


def test_source_fetches_once_and_publishes_full_market_batch(tmp_path: Path) -> None:
    adapter = _Adapter()
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: CAPTURE_AT,
    )(_manifest(tmp_path))

    first = step()
    second = step()

    assert adapter.calls == [TRADE_DATE]
    assert first.processed_count == 1
    assert second.processed_count == 0
    assert first.source_generations[LiveChannel.AUCTION_MATCH.value]
    records = LiveBatchSpool(tmp_path / "auction-match").list_after(
        LiveChannel.AUCTION_MATCH,
        sequence=-1,
    )
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.PUBLISHED
    assert records[0].envelope.row_count == 2


def test_source_does_not_fetch_before_window_or_on_closed_date(tmp_path: Path) -> None:
    before = _Adapter()
    before_step = auction_match_source_builder(
        adapter_factory=lambda: before,
        clock=lambda: datetime(2026, 7, 31, 1, 25, 59, tzinfo=UTC),
    )(_manifest(tmp_path / "before"))

    closed = _Adapter()
    closed_step = auction_match_source_builder(
        adapter_factory=lambda: closed,
        clock=lambda: CAPTURE_AT,
    )(_manifest(tmp_path / "closed", open_date=False))

    assert before_step().processed_count == 0
    assert closed_step().processed_count == 0
    assert before.calls == []
    assert closed.calls == []


def test_source_retries_degraded_capture_only_up_to_manifest_limit(tmp_path: Path) -> None:
    adapter = _Adapter([RuntimeError("down"), RuntimeError("down"), RuntimeError("down"), _frame()])
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: CAPTURE_AT,
    )(_manifest(tmp_path))

    results = [step(), step(), step(), step()]

    assert adapter.calls == [TRADE_DATE, TRADE_DATE, TRADE_DATE]
    attempts = SourceQuotaStore(tmp_path / "auction-match" / "quota.sqlite3").list_attempts(
        source="tushare.stk_auction"
    )
    source_request_id = canonical_sha256(
        {
            "source": "tushare.stk_auction",
            "trade_date": TRADE_DATE,
            "expected_codes": ("000001.SZ", "600000.SH"),
            "required_codes": (),
        }
    )
    expected_attempt_ids = {
        canonical_sha256(
            {
                "protocol": "auction-source-attempt-v2",
                "source": "tushare.stk_auction",
                "trade_date": TRADE_DATE,
                "session": "auction_match",
                "source_request_id": source_request_id,
                "retry_ordinal": retry_ordinal,
            }
        )
        for retry_ordinal in range(3)
    }
    assert len(attempts) == 3
    assert {attempt.attempt_id for attempt in attempts} == expected_attempt_ids
    assert {attempt.outcome for attempt in attempts} == {SourceQuotaAttemptOutcome.FAILURE}
    assert results[-1].processed_count == 0
    assert any(reason.startswith("auction_match:stale:") for reason in results[0].degraded_reasons)
    records = LiveBatchSpool(tmp_path / "auction-match").list_after(
        LiveChannel.AUCTION_MATCH,
        sequence=-1,
    )
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.STALE


def test_source_stops_retrying_after_capture_deadline(tmp_path: Path) -> None:
    adapter = _Adapter([RuntimeError("down"), _frame()])
    times = iter(
        [
            CAPTURE_AT,
            CAPTURE_AT,
            CAPTURE_AT,
            datetime(2026, 7, 31, 1, 30, 1, tzinfo=UTC),
        ]
    )
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: next(times),
    )(_manifest(tmp_path))

    first = step()
    second = step()

    assert len(adapter.calls) == 1
    assert second.output_sequence == first.output_sequence
    assert second.degraded_reasons == first.degraded_reasons
