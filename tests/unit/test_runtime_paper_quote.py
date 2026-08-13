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
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintBatch,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintSnapshot,
    PaperExecutionConstraintUnavailableError,
)
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
    producer_commit: str = COMMIT,
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
        producer_commit=producer_commit,
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


def _write_constraints(
    tmp_path: Path,
    *,
    constraint_code: str = CODE,
    suspended: bool = False,
    buy_limit_locked: bool = False,
    sell_limit_locked: bool = False,
    risk_rejected: bool = False,
    sequence: int = 1,
    available_at: datetime = T0931 - timedelta(minutes=1),
    published_at: datetime = T0931 - timedelta(minutes=1),
) -> tuple[Path, str]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    constraint_payload: dict[str, object] = {
        "ts_code": constraint_code,
        "trade_date": TRADE_DAY,
        "available_at": available_at,
        "expires_at": datetime(2026, 7, 31, 7, 1, tzinfo=UTC),
        "suspended": suspended,
        "buy_limit_locked": buy_limit_locked,
        "sell_limit_locked": sell_limit_locked,
        "risk_rejected": risk_rejected,
        "source_snapshot_ids": {
            "market_minute": "8" * 64,
            "security_status": "9" * 64,
        },
        "producer_commit": COMMIT,
    }
    constraint_payload["content_hash"] = canonical_sha256(constraint_payload)
    constraint = PaperExecutionConstraintSnapshot.model_validate(constraint_payload)
    batch_payload: dict[str, object] = {
        "schema_version": 1,
        "sequence": sequence,
        "producer_commit": COMMIT,
        "records": (constraint,),
    }
    batch_payload["content_hash"] = canonical_sha256(batch_payload)
    batch = PaperExecutionConstraintBatch.model_validate(batch_payload)
    constraint_root = tmp_path / "execution-constraints"
    pointer = PaperExecutionConstraintPublisher(
        root=constraint_root,
        producer_commit=COMMIT,
        clock=lambda: published_at,
    ).publish(batch)
    return constraint_root, pointer.file_sha256


def _resolver(
    tmp_path: Path,
    spool: LiveBatchSpool,
    *,
    timestamp_semantics: str = "bar_end",
    quote_max_age_seconds: int = 300,
    max_finalize_scan_batches: int = 32,
    max_visible_scan_batches: int = 120,
    constraint_code: str = CODE,
    suspended: bool = False,
    buy_limit_locked: bool = False,
    sell_limit_locked: bool = False,
    risk_rejected: bool = False,
) -> PaperPitQuoteResolver:
    tmp_path.mkdir(parents=True, exist_ok=True)
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar_bytes()
    calendar_path.write_bytes(calendar)
    constraint_root, _constraint_sha256 = _write_constraints(
        tmp_path,
        constraint_code=constraint_code,
        suspended=suspended,
        buy_limit_locked=buy_limit_locked,
        sell_limit_locked=sell_limit_locked,
        risk_rejected=risk_rejected,
    )
    return PaperPitQuoteResolver(
        PaperQuoteResolverConfig(
            raw_spool_root=spool.root,
            trade_calendar_path=calendar_path,
            trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
            execution_constraint_root=constraint_root,
            expected_producer_commit=COMMIT,
            timestamp_semantics=timestamp_semantics,
            quote_max_age_seconds=quote_max_age_seconds,
            max_finalize_scan_batches=max_finalize_scan_batches,
            max_visible_scan_batches=max_visible_scan_batches,
        )
    )


def test_quote_carries_directional_execution_constraints_and_authority_evidence(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    resolver = _resolver(
        tmp_path,
        spool,
        buy_limit_locked=True,
        sell_limit_locked=False,
        risk_rejected=True,
    )

    buy = resolver(_signal(SignalAction.B_INTENT), T0931)
    sell = resolver(_signal(SignalAction.S_INTENT), T0931)

    assert buy.context.limit_locked is True
    assert sell.context.limit_locked is False
    assert buy.context.risk_rejected is True
    assert buy.constraint_snapshot_id is not None
    pointer = json.loads((tmp_path / "execution-constraints" / "current.json").read_text())
    assert buy.constraint_authority_sha256 == pointer["file_sha256"]
    assert buy.constraint_source_snapshot_ids == {
        "market_minute": "8" * 64,
        "security_status": "9" * 64,
    }


def test_resolver_reads_new_constraint_generation_without_reinitialization(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    resolver = _resolver(tmp_path, spool, buy_limit_locked=True)

    first = resolver(_signal(), T0931)
    _root, second_file_sha = _write_constraints(
        tmp_path,
        buy_limit_locked=False,
        sequence=2,
        available_at=T0931 + timedelta(seconds=10),
        published_at=T0931 + timedelta(seconds=10),
    )
    second = resolver(_signal(), T0931 + timedelta(seconds=10))

    assert first.context.limit_locked is True
    assert second.context.limit_locked is False
    assert second.constraint_authority_sha256 == second_file_sha
    assert first.constraint_authority_sha256 != second.constraint_authority_sha256


def test_missing_execution_constraint_keeps_quote_unavailable(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    resolver = _resolver(tmp_path, spool, constraint_code=OTHER_CODE)

    with pytest.raises(PaperExecutionConstraintUnavailableError, match="not found"):
        resolver(_signal(), T0931)


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


def test_quote_batch_must_match_the_runtime_producer_commit(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931,
        rows=[_minute_row()],
        producer_commit="f" * 40,
    )
    resolver = _resolver(tmp_path, spool)

    with pytest.raises(PaperQuoteIntegrityError, match="producer commit"):
        resolver(_signal(), T0931)


def test_trade_date_resolver_accepts_only_an_sse_open_day(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    resolver = _resolver(tmp_path, spool)

    assert resolver.trade_date_at(T0931) == TRADE_DAY
    with pytest.raises(PaperTradeCalendarError, match="not an SSE open day"):
        resolver.trade_date_at(datetime(2026, 8, 1, 2, 0, tzinfo=UTC))


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
        _resolver(tmp_path / "stale", stale_spool)(_signal(), T0931 + timedelta(minutes=1))

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
    with pytest.raises(PaperQuoteStaleError, match=CODE):
        _resolver(tmp_path / "missing", missing_spool)(_signal(), T0931 + timedelta(minutes=1))


def test_provider_snapshot_candidate_missing_batch_never_uses_prior_sequence(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(minutes=1, seconds=5),
        rows=[_minute_row(trade_time=T0931 + timedelta(minutes=1), close=10.2)],
    )
    _publish(
        spool,
        sequence=1,
        available_at=T0931 + timedelta(minutes=1, seconds=25),
        rows=[
            _minute_row(
                ts_code=OTHER_CODE,
                trade_time=T0931 + timedelta(minutes=1),
                close=11.0,
            )
        ],
    )
    _publish(
        spool,
        sequence=2,
        available_at=T0931 + timedelta(minutes=2, seconds=5),
        rows=[_minute_row(trade_time=T0931 + timedelta(minutes=2), close=10.4)],
    )

    with pytest.raises(PaperQuoteStaleError, match="sequence 1|600000.SH"):
        _resolver(tmp_path, spool, timestamp_semantics="provider_snapshot")(
            _signal(),
            T0931 + timedelta(minutes=2, seconds=10),
        )


def test_current_visible_quote_does_not_scan_historical_manifests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    for sequence in range(50):
        _publish(
            spool,
            sequence=sequence,
            available_at=T0931 + timedelta(seconds=sequence),
            rows=[_minute_row(close=10.0 + sequence / 100)],
        )
    resolver = _resolver(tmp_path, spool)

    from rquant import runtime_paper_quote as quote_module

    original = quote_module._read_regular_file_no_symlinks
    reads: list[Path] = []

    def record_read(path: Path) -> bytes:
        reads.append(path)
        return original(path)

    monkeypatch.setattr(quote_module, "_read_regular_file_no_symlinks", record_read)

    quote = resolver(_signal(), T0931 + timedelta(seconds=60))

    manifests = [path for path in reads if path.suffix == ".json" and "batches" in path.parts]
    assert quote.context.executable_price == Decimal("10.49")
    assert len(manifests) == 1


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
    constraint_root, _constraint_sha256 = _write_constraints(tmp_path)

    resolver = PaperPitQuoteResolver(
        PaperQuoteResolverConfig(
            raw_spool_root=spool.root,
            trade_calendar_path=path,
            trade_calendar_sha256=hashlib.sha256(content).hexdigest(),
            execution_constraint_root=constraint_root,
            expected_producer_commit=COMMIT,
            timestamp_semantics="bar_end",
        )
    )
    assert resolver(_signal(), T0931).context.acquisition_available_date == NEXT_TRADE_DAY

    with pytest.raises(PaperQuoteIntegrityError, match="calendar content hash"):
        PaperPitQuoteResolver(
            PaperQuoteResolverConfig(
                raw_spool_root=spool.root,
                trade_calendar_path=path,
                trade_calendar_sha256="f" * 64,
                execution_constraint_root=constraint_root,
                expected_producer_commit=COMMIT,
            )
        )


def test_missing_next_open_day_fails_explicitly(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    calendar_path = tmp_path / "calendar.json"
    calendar = json.dumps([{"exchange": "SSE", "cal_date": "2026-07-31", "is_open": True}]).encode()
    calendar_path.write_bytes(calendar)
    constraint_root, _constraint_sha256 = _write_constraints(tmp_path)
    resolver = PaperPitQuoteResolver(
        PaperQuoteResolverConfig(
            raw_spool_root=spool.root,
            trade_calendar_path=calendar_path,
            trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
            execution_constraint_root=constraint_root,
            expected_producer_commit=COMMIT,
            timestamp_semantics="bar_end",
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
    constraint_root, _constraint_sha256 = _write_constraints(tmp_path)
    with pytest.raises(ValidationError, match="absolute"):
        PaperQuoteResolverConfig(
            raw_spool_root=Path("relative/spool"),
            trade_calendar_path=calendar_path,
            trade_calendar_sha256=hashlib.sha256(calendar).hexdigest(),
            execution_constraint_root=constraint_root,
            expected_producer_commit=COMMIT,
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
                execution_constraint_root=constraint_root,
                expected_producer_commit=COMMIT,
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
                execution_constraint_root=constraint_root,
                expected_producer_commit=COMMIT,
            )
        )


def test_provider_snapshot_requires_timestamp_advance_and_uses_last_revision(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    first = _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(seconds=5),
        rows=[_minute_row(close=10.0)],
    )
    _publish(
        spool,
        sequence=1,
        available_at=T0931 + timedelta(seconds=25),
        rows=[_minute_row(close=10.2)],
    )
    resolver = _resolver(
        tmp_path,
        spool,
        timestamp_semantics="provider_snapshot",
    )

    with pytest.raises(PaperQuoteUnavailableError, match="advance|final"):
        resolver(_signal(), T0931 + timedelta(seconds=30))

    advancing = _publish(
        spool,
        sequence=2,
        available_at=T0931 + timedelta(minutes=1, seconds=5),
        rows=[
            _minute_row(
                trade_time=T0931 + timedelta(minutes=1),
                close=10.3,
            )
        ],
    )
    quote = resolver(_signal(), T0931 + timedelta(minutes=1, seconds=10))

    assert quote.context.executable_price == Decimal("10.2")
    assert quote.event_time == T0931
    assert quote.available_at == advancing.available_at
    assert quote.available_at != first.available_at


def test_provider_snapshot_quote_age_boundary_is_inclusive(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(seconds=5),
        rows=[_minute_row(close=10.0)],
    )
    _publish(
        spool,
        sequence=1,
        available_at=T0931 + timedelta(minutes=1, seconds=5),
        rows=[_minute_row(trade_time=T0931 + timedelta(minutes=1), close=10.1)],
    )
    resolver = _resolver(
        tmp_path,
        spool,
        timestamp_semantics="provider_snapshot",
        quote_max_age_seconds=90,
    )

    assert resolver(_signal(), T0931 + timedelta(seconds=90)).event_time == T0931
    with pytest.raises(PaperQuoteStaleError, match="age|old"):
        resolver(_signal(), T0931 + timedelta(seconds=90, microseconds=1))


@pytest.mark.parametrize(
    "observed_at",
    (
        datetime(2026, 7, 31, 1, 29, 59, tzinfo=UTC),
        datetime(2026, 7, 31, 4, 0, tzinfo=UTC),
        datetime(2026, 7, 31, 7, 0, 0, 1, tzinfo=UTC),
    ),
)
def test_resolution_rejects_preopen_lunch_and_after_close(
    tmp_path: Path,
    observed_at: datetime,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(spool, sequence=0, available_at=T0931, rows=[_minute_row()])
    resolver = _resolver(tmp_path, spool)

    with pytest.raises(PaperTradeCalendarError, match="session"):
        resolver(_signal(), observed_at)


def test_provider_snapshot_finalize_scan_is_bounded(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(seconds=1),
        rows=[_minute_row(close=10.0)],
    )
    for sequence in range(1, 5):
        _publish(
            spool,
            sequence=sequence,
            available_at=T0931 + timedelta(minutes=1, seconds=sequence),
            rows=[
                _minute_row(
                    trade_time=T0931 + timedelta(minutes=1),
                    close=10.0 + sequence / 10,
                )
            ],
        )
    resolver = _resolver(
        tmp_path,
        spool,
        timestamp_semantics="provider_snapshot",
        max_finalize_scan_batches=3,
    )

    with pytest.raises(PaperQuoteUnavailableError, match="bounded|scan|final"):
        resolver(_signal(), T0931 + timedelta(minutes=1, seconds=10))


def test_provider_snapshot_requires_the_direct_next_trading_minute(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931 + timedelta(seconds=5),
        rows=[_minute_row(close=10.0)],
    )
    _publish(
        spool,
        sequence=1,
        available_at=T0931 + timedelta(minutes=2, seconds=5),
        rows=[
            _minute_row(
                trade_time=T0931 + timedelta(minutes=2),
                close=10.3,
            )
        ],
    )

    with pytest.raises(PaperQuoteUnavailableError, match="advance|final|next"):
        _resolver(tmp_path, spool, timestamp_semantics="provider_snapshot")(
            _signal(),
            T0931 + timedelta(minutes=2, seconds=10),
        )


def test_provider_snapshot_opening_minute_can_be_finalized_by_0931(
    tmp_path: Path,
) -> None:
    t0930 = T0931 - timedelta(minutes=1)
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=t0930 + timedelta(seconds=5),
        rows=[_minute_row(trade_time=t0930, close=9.9)],
    )
    advancing = _publish(
        spool,
        sequence=1,
        available_at=T0931 + timedelta(seconds=5),
        rows=[_minute_row(close=10.0)],
    )

    quote = _resolver(
        tmp_path,
        spool,
        timestamp_semantics="provider_snapshot",
        quote_max_age_seconds=90,
    )(_signal(), T0931 + timedelta(seconds=10))

    assert quote.event_time == t0930
    assert quote.available_at == advancing.available_at
    assert quote.context.executable_price == Decimal("9.9")


def test_provider_snapshot_lunch_boundary_requires_1130_before_1300(
    tmp_path: Path,
) -> None:
    t1129 = datetime(2026, 7, 31, 3, 29, tzinfo=UTC)
    t1300 = datetime(2026, 7, 31, 5, 0, tzinfo=UTC)
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=t1129 + timedelta(seconds=5),
        rows=[_minute_row(trade_time=t1129, close=10.0)],
    )
    _publish(
        spool,
        sequence=1,
        available_at=t1300 + timedelta(seconds=5),
        rows=[_minute_row(trade_time=t1300, close=10.2)],
    )

    with pytest.raises(PaperQuoteUnavailableError, match="advance|final|next"):
        _resolver(
            tmp_path,
            spool,
            timestamp_semantics="provider_snapshot",
            quote_max_age_seconds=300,
        )(_signal(), t1300 + timedelta(seconds=10))


def test_latest_visible_batch_scan_is_bounded(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "raw-spool")
    _publish(
        spool,
        sequence=0,
        available_at=T0931,
        rows=[_minute_row()],
    )
    for sequence in range(1, 5):
        _publish(
            spool,
            sequence=sequence,
            available_at=T0931 + timedelta(minutes=10 + sequence),
            rows=[_minute_row(close=10.0 + sequence / 10)],
        )

    resolver = _resolver(
        tmp_path,
        spool,
        max_visible_scan_batches=3,
    )

    with pytest.raises(PaperQuoteUnavailableError, match="bounded|scan|available"):
        resolver(_signal(), T0931 + timedelta(minutes=1))
