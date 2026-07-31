from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MARKET_MINUTE_COLUMNS, MarketMinuteGateway
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_paper_quote import (
    PaperPitQuoteResolver,
    PaperQuoteCandidateMissingError,
    PaperQuoteIntegrityError,
    PaperQuoteResolverConfig,
    PaperQuoteStaleError,
    PaperQuoteUnavailableError,
    PaperTradeCalendarError,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope

COMMIT = "a" * 40
CODE = "600000.SH"
OTHER_CODE = "600001.SH"
TRADE_DAY = date(2026, 7, 31)
NEXT_TRADE_DAY = date(2026, 8, 3)
T0931 = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)


def _signal(action: SignalAction = SignalAction.B_INTENT) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint="b" * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=T0931,
        available_at=T0931 + timedelta(seconds=2),
        candidate_id=CODE,
        action=action,
        reason_codes=("paper-pit",),
        evidence={},
        expires_at=T0931 + timedelta(minutes=10),
        producer_commit="e" * 40,
    )


def _minute_row(
    *,
    ts_code: str = CODE,
    trade_time: datetime = T0931,
    close: float = 10.0,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_time": trade_time,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "vol": 1_000.0,
        "amount": close * 1_000,
    }


def _publish(
    spool: LiveBatchSpool,
    *,
    sequence: int,
    available_at: datetime,
    rows: list[dict[str, object]],
    quality: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
) -> BatchEnvelope:
    raw = pd.DataFrame(rows) if rows else pd.DataFrame(columns=MARKET_MINUTE_COLUMNS)
    frame = MarketMinuteGateway.normalize_frame(raw)
    payload = MarketMinuteGateway.encode_payload(frame)
    if frame.empty:
        event_start = event_end = available_at
    else:
        event_start = frame["trade_time"].min().to_pydatetime()
        event_end = frame["trade_time"].max().to_pydatetime()
    envelope = BatchEnvelope(
        schema_version=1,
        channel=LiveChannel.MARKET_MINUTE,
        dataset_id="market_minute",
        source="test.market-minute",
        source_request_id=f"request-{sequence}",
        batch_id=canonical_sha256(
            {
                "sequence": sequence,
                "available_at": available_at,
                "content_sha256": hashlib.sha256(payload).hexdigest(),
            }
        ),
        sequence=sequence,
        revision=1,
        event_time_start=event_start,
        event_time_end=event_end,
        source_time=event_end,
        received_at=available_at,
        available_at=available_at,
        row_count=len(frame),
        content_sha256=hashlib.sha256(payload).hexdigest(),
        quality_status=quality,
        degraded_reasons=("source_timeout",) if quality is BatchQualityStatus.STALE else (),
        producer_version="test-v1",
        producer_commit=COMMIT,
    )
    spool.publish(envelope, payload)
    return envelope


def _calendar_bytes() -> bytes:
    return json.dumps(
        [
            {"exchange": "SSE", "cal_date": "2026-07-31", "is_open": True},
            {"exchange": "SSE", "cal_date": "2026-08-01", "is_open": False},
            {"exchange": "SSE", "cal_date": "2026-08-02", "is_open": False},
            {"exchange": "SSE", "cal_date": "2026-08-03", "is_open": True},
        ],
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _resolver(tmp_path: Path, spool: LiveBatchSpool) -> PaperPitQuoteResolver:
    tmp_path.mkdir(parents=True, exist_ok=True)
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar_bytes()
    calendar_path.write_bytes(calendar)
    return PaperPitQuoteResolver(
        PaperQuoteResolverConfig(
            raw_spool_root=spool.root,
            trade_calendar_path=calendar_path,
            trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
        )
    )


def test_resolver_uses_latest_visible_sequence_and_latest_visible_minute(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    first = _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(seconds=5),
        rows=[_minute_row(close=10.0)],
    )
    second = _publish(
        spool,
        sequence=1,
        available_at=T0931 + timedelta(minutes=1, seconds=5),
        rows=[
            _minute_row(trade_time=T0931 + timedelta(minutes=1), close=10.5),
            _minute_row(trade_time=T0931 + timedelta(minutes=2), close=99.0),
        ],
    )
    resolver = _resolver(tmp_path, spool)

    before_second = resolver(_signal(), T0931 + timedelta(minutes=1))
    after_second = resolver(_signal(), T0931 + timedelta(minutes=1, seconds=30))

    assert before_second.context.executable_price == Decimal("10.0")
    assert before_second.available_at == first.available_at
    assert after_second.context.executable_price == Decimal("10.5")
    assert after_second.event_time == T0931 + timedelta(minutes=1)
    assert after_second.available_at == second.available_at
    assert after_second.producer_commit == COMMIT
    assert after_second.context.acquisition_available_date == NEXT_TRADE_DAY


def test_future_batch_and_future_rows_are_never_visible(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(minutes=1),
        rows=[_minute_row(trade_time=T0931 + timedelta(minutes=2), close=99.0)],
    )
    resolver = _resolver(tmp_path, spool)

    with pytest.raises(PaperQuoteUnavailableError, match="available"):
        resolver(_signal(), T0931 + timedelta(seconds=30))
    with pytest.raises(PaperQuoteCandidateMissingError, match="visible minute"):
        resolver(_signal(), T0931 + timedelta(minutes=1, seconds=30))


def test_latest_stale_or_candidate_missing_batch_never_falls_back(
    tmp_path: Path,
) -> None:
    stale_spool = LiveBatchSpool(tmp_path / "stale-spool")
    _publish(
        stale_spool,
        sequence=0,
        available_at=T0931,
        rows=[_minute_row(close=10.0)],
    )
    _publish(
        stale_spool,
        sequence=1,
        available_at=T0931 + timedelta(minutes=1),
        rows=[],
        quality=BatchQualityStatus.STALE,
    )
    with pytest.raises(PaperQuoteStaleError, match="sequence 1"):
        _resolver(tmp_path / "stale", stale_spool)(
            _signal(), T0931 + timedelta(minutes=1)
        )

    missing_spool = LiveBatchSpool(tmp_path / "missing-spool")
    _publish(
        missing_spool,
        sequence=0,
        available_at=T0931,
        rows=[_minute_row(close=10.0)],
    )
    _publish(
        missing_spool,
        sequence=1,
        available_at=T0931 + timedelta(minutes=1),
        rows=[_minute_row(ts_code=OTHER_CODE, close=11.0)],
    )
    with pytest.raises(PaperQuoteCandidateMissingError, match=CODE):
        _resolver(tmp_path / "missing", missing_spool)(
            _signal(), T0931 + timedelta(minutes=1)
        )


def test_buy_uses_frozen_sse_next_open_day_and_sell_has_no_acquisition_date(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    resolver = _resolver(tmp_path, spool)

    buy = resolver(_signal(SignalAction.B_INTENT), T0931)
    sell = resolver(_signal(SignalAction.S_INTENT), T0931)

    assert buy.context.acquisition_available_date == NEXT_TRADE_DAY
    assert sell.context.acquisition_available_date is None


@pytest.mark.parametrize("suffix", [".json", ".parquet"])
def test_calendar_content_is_hash_bound_for_json_and_parquet(
    tmp_path: Path,
    suffix: str,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    path = tmp_path / f"calendar{suffix}"
    if suffix == ".json":
        content = _calendar_bytes()
        path.write_bytes(content)
    else:
        pd.DataFrame(json.loads(_calendar_bytes())).to_parquet(path, index=False)
        content = path.read_bytes()

    resolver = PaperPitQuoteResolver(
        PaperQuoteResolverConfig(
            raw_spool_root=spool.root,
            trade_calendar_path=path,
            trade_calendar_sha256=hashlib.sha256(content).hexdigest(),
        )
    )
    assert resolver(_signal(), T0931).context.acquisition_available_date == NEXT_TRADE_DAY

    with pytest.raises(PaperQuoteIntegrityError, match="calendar content hash"):
        PaperPitQuoteResolver(
            PaperQuoteResolverConfig(
                raw_spool_root=spool.root,
                trade_calendar_path=path,
                trade_calendar_sha256="f" * 64,
            )
        )


def test_missing_next_open_day_fails_explicitly(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    calendar_path = tmp_path / "calendar.json"
    calendar = json.dumps(
        [{"exchange": "SSE", "cal_date": "2026-07-31", "is_open": True}]
    ).encode()
    calendar_path.write_bytes(calendar)
    resolver = PaperPitQuoteResolver(
        PaperQuoteResolverConfig(
            raw_spool_root=spool.root,
            trade_calendar_path=calendar_path,
            trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
        )
    )

    with pytest.raises(PaperTradeCalendarError, match="next SSE open day"):
        resolver(_signal(), T0931)


def test_paths_must_be_absolute_and_no_parent_symlink_is_followed(
    tmp_path: Path,
) -> None:
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar_bytes()
    calendar_path.write_bytes(calendar)
    with pytest.raises(ValidationError, match="absolute"):
        PaperQuoteResolverConfig(
            raw_spool_root=Path("relative/spool"),
            trade_calendar_path=calendar_path,
            trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
        )

    real_parent = tmp_path / "real"
    real_parent.mkdir()
    spool = LiveBatchSpool(real_parent / "spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(PaperQuoteIntegrityError, match="symlink"):
        PaperPitQuoteResolver(
            PaperQuoteResolverConfig(
                raw_spool_root=linked_parent / "spool",
                trade_calendar_path=calendar_path,
                trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
            )
        )

    calendar_parent = tmp_path / "calendar-linked"
    calendar_parent.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(PaperQuoteIntegrityError, match="symlink"):
        PaperPitQuoteResolver(
            PaperQuoteResolverConfig(
                raw_spool_root=spool.root,
                trade_calendar_path=calendar_parent / "calendar.json",
                trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
            )
        )
