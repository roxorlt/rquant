"""#302 / #307: a Route A round costs what is new, and publishes what the full rebuild did.

Every consumer below used to re-read the whole day on every round. The tests pin both
halves of the change: a long-lived consumer (warm, restarted once mid-day) writes the same
bytes as one rebuilt from nothing on every round (cold), and a round with nothing new reads
nothing it has already read -- while a retained file that changes is still refused.
"""

from __future__ import annotations

import hashlib
import itertools
import math
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import rquant.feature_spool as feature_spool_module
import rquant.live_spool as live_spool_module
from rquant.feature_live_service import FeatureLiveInputCache, run_feature_live_batch
from rquant.feature_spool import FeatureBatchSpool, FeatureSpoolIntegrityError
from rquant.intraday_feature_engine import IntradayFeatureConfig, NormalizedHistoricalMinutes
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool, LiveSpoolIntegrityError
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.paper_execution_constraint_producer import (
    PaperExecutionConstraintNoEvidenceError,
    PaperExecutionConstraintProducer,
    PaperExecutionConstraintProductionRequest,
)
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintAuthority,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintUnavailableError,
)
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.runtime_builder_authority import (
    PAPER_CONSTRAINT_CODES_WITHOUT_EVIDENCE_OBSERVATION,
    PAPER_CONSTRAINT_STALE_CODES_OBSERVATION,
    paper_execution_constraint_publisher_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_contracts import SignalAction

SH = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 7, 31)
PRIOR = (date(2026, 7, 29), date(2026, 7, 30))
#: all day / until 11:10 then re-served / from 13:02 only (appears after the lunch break)
CODES = ("600000.SH", "600001.SH", "300001.SZ")
COMMIT = "c" * 40


def _cn(day: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime.combine(day, time(hh, mm, ss), tzinfo=SH).astimezone(UTC)


def _bar(code: str, stamp: datetime, day_index: int) -> dict[str, object]:
    index = int((stamp - _cn(stamp.astimezone(SH).date(), 9, 30)).total_seconds() // 60)
    base = {"600000.SH": 10.0, "600001.SH": 8.0, "300001.SZ": 25.0}[code]
    close = round(base * (1 + 0.004 * math.sin(index / 5.0 + day_index)), 3)
    open_ = round(base * (1 + 0.004 * math.sin((index - 1) / 5.0 + day_index)), 3)
    vol = float(1000 + (index * 37 + day_index * 101) % 900)
    return {
        "ts_code": code,
        "trade_time": stamp,
        "open": open_,
        "high": round(max(open_, close) * 1.001, 3),
        "low": round(min(open_, close) * 0.999, 3),
        "close": close,
        "vol": vol,
        "amount": round(vol * close, 2),
    }


def _minutes(day: date, first: time, last: time) -> list[datetime]:
    start, stop = _cn(day, first.hour, first.minute), _cn(day, last.hour, last.minute)
    return [
        start + timedelta(minutes=k) for k in range(int((stop - start).total_seconds() // 60) + 1)
    ]


def _trades(code: str, stamp: datetime) -> bool:
    local = stamp.astimezone(SH).time()
    if code == "600001.SH":
        return local <= time(11, 10)
    if code == "300001.SZ":
        return local >= time(13, 2)
    return True


#: the rounds cover the end of the morning, the lunch break and the start of the afternoon
DAY_MINUTES = _minutes(DAY, time(11, 5), time(11, 30)) + _minutes(DAY, time(13, 1), time(13, 12))
DAY_BARS = {
    code: [_bar(code, stamp, 9) for stamp in DAY_MINUTES if _trades(code, stamp)] for code in CODES
}


def _history() -> pd.DataFrame:
    rows = []
    for day_index, day in enumerate(PRIOR):
        for code in CODES:
            for stamp in _minutes(day, time(11, 0), time(11, 30)) + _minutes(
                day, time(13, 1), time(13, 20)
            ):
                row = _bar(code, stamp, day_index)
                row["available_at"] = stamp
                rows.append(row)
    return pd.DataFrame(rows)


@pytest.fixture
def deterministic_generations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spool generation ids are random; two worlds built side by side must share them."""

    counters: dict[str, Iterator[int]] = {}

    def token_hex(nbytes: int = 32) -> str:
        world = str(Path.cwd())
        index = next(counters.setdefault(world, itertools.count()))
        return hashlib.sha256(f"generation-{index}".encode()).hexdigest()[: 2 * nbytes]

    monkeypatch.setattr(feature_spool_module.secrets, "token_hex", token_hex)
    monkeypatch.setattr(live_spool_module.secrets, "token_hex", token_hex)


class _World:
    """One raw minute spool, its reference registry, and a round clock."""

    def __init__(self, root: Path) -> None:
        root.mkdir(mode=0o700, parents=True)
        self.root = root
        self.now = _cn(DAY, 11, 4, 5)

        def fetch() -> pd.DataFrame:
            rows = []
            for code in CODES:
                eligible = [row for row in DAY_BARS[code] if row["trade_time"] <= self.now]
                if eligible:
                    rows.append(dict(eligible[-1]))
            frame = pd.DataFrame(
                rows,
                columns=["ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"],
            )
            if not frame.empty:
                frame["trade_time"] = (
                    pd.to_datetime(frame["trade_time"], utc=True)
                    .dt.tz_convert(SH)
                    .dt.tz_localize(None)
                )
            return frame

        self.gateway = MarketMinuteGateway(
            spool=LiveBatchSpool(root / "market-minute"),
            fetcher=fetch,
            completion_clock=lambda: self.now,
            config=MarketMinuteGatewayConfig(
                producer_version="market-minute-v1",
                producer_commit="a" * 40,
            ),
        )
        self.registry_path = root / "reference.sqlite3"
        registry = ReferenceRegistry(self.registry_path)
        for code in CODES:
            for dataset, payload in (
                (ReferenceDataset.ST_STATUS, {"is_st": False}),
                (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": False}),
                (
                    ReferenceDataset.PRICE_LIMIT_REGIME,
                    {"limit_up_price": 60.0, "limit_down_price": 1.0},
                ),
                (
                    ReferenceDataset.LISTING_STATUS,
                    {
                        "market": "CN",
                        "exchange": "SZSE" if code.endswith(".SZ") else "SSE",
                        "instrument_class": "EQUITY",
                        "security_class": "A_SHARE",
                        "status": "listed",
                    },
                ),
            ):
                registry.append(
                    ReferenceRecord(
                        dataset_id=dataset,
                        key=code,
                        effective_from=_cn(DAY, 9, 25),
                        effective_to=_cn(DAY, 15, 5),
                        revision=1,
                        source="fixture",
                        first_available_at=_cn(DAY, 9, 20),
                        payload=payload,
                    )
                )
        self.generation_id = registry.publish(published_at=_cn(DAY, 9, 24)).generation_id

    def rounds(self) -> Iterator[datetime]:
        """Every 30 s from 11:04:05 to 13:14:35, with a capture on every :05 round."""

        tick = _cn(DAY, 11, 4, 5)
        while tick <= _cn(DAY, 13, 14, 35):
            self.now = tick
            if tick.astimezone(SH).second == 5:
                self.gateway.capture_once(received_at=tick)
            yield tick
            tick += timedelta(seconds=30)


def _tree(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.name.startswith(".") and not path.name.endswith(".lock")
    }


# LiveBatchSpool: an unchanged retained batch is recognised by lstat -----------------------


def _count_batch_reads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    original = live_spool_module._secure_read_regular_file_with_identity
    reads: list[str] = []

    def counting(path: Path, **kwargs: object) -> tuple[bytes, object]:
        if "batches" in path.parts:
            reads.append(path.name)
        return original(path, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(live_spool_module, "_secure_read_regular_file_with_identity", counting)
    return reads


def test_a_repeated_listing_reads_no_unchanged_minute_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = _World(tmp_path / "world")
    for _tick in itertools.islice(world.rounds(), 12):
        pass
    reader = LiveBatchSpool(tmp_path / "world" / "market-minute", read_only=True)
    first = reader.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    reads = _count_batch_reads(monkeypatch)

    again = reader.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    descriptor = reader.source_descriptor(LiveChannel.MARKET_MINUTE)

    assert again == first
    assert descriptor.high_watermark == first[-1].envelope.sequence
    assert reads == []


@pytest.mark.parametrize("kind", ("payload", "manifest", "receipt"))
def test_a_changed_retained_minute_batch_is_still_refused(
    tmp_path: Path, kind: str, deterministic_generations: None
) -> None:
    del deterministic_generations
    spool = LiveBatchSpool(tmp_path / "live")
    gateway_frames = [
        pd.DataFrame([_bar("600000.SH", _cn(DAY, 9, 31 + sequence), 9)]) for sequence in range(4)
    ]
    for frame in gateway_frames:
        clock = frame["trade_time"].iloc[0] + timedelta(seconds=5)
        MarketMinuteGateway(
            spool=spool,
            fetcher=lambda frame=frame: frame,
            completion_clock=lambda clock=clock: clock,
            config=MarketMinuteGatewayConfig(producer_version="v1", producer_commit="a" * 40),
        ).capture_once(received_at=clock)
    reader = LiveBatchSpool(tmp_path / "live", read_only=True)
    assert len(reader.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)) == 4

    target = {
        "payload": spool._payload_path(LiveChannel.MARKET_MINUTE, 1),
        "manifest": spool._manifest_path(LiveChannel.MARKET_MINUTE, 1),
        "receipt": spool._publication_receipt_path(LiveChannel.MARKET_MINUTE, 1),
    }[kind]
    if kind == "receipt" and not target.exists():
        #: the gateway published without a deadline, so there is no receipt: a stray
        #: one appearing is exactly as foreign
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        LiveBatchSpool._atomic_write(target, b"{}")
    else:
        body = bytearray(target.read_bytes())
        body[-2] ^= 0x01
        LiveBatchSpool._atomic_write(target, bytes(body))

    with pytest.raises(LiveSpoolIntegrityError):
        reader.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)


# FeatureBatchSpool: an idle listing reads no manifest --------------------------------------


def _feature_world(tmp_path: Path) -> tuple[_World, Path]:
    world = _World(tmp_path / "world")
    features = tmp_path / "world" / "features"
    history = NormalizedHistoricalMinutes(_history())
    cache = FeatureLiveInputCache()
    raw = LiveBatchSpool(
        world.root / "market-minute", source_read_only=True, cursor_root=features / "raw-cursors"
    )
    spool = FeatureBatchSpool(features)
    for tick in itertools.islice(world.rounds(), 16):
        run_feature_live_batch(
            raw_spool=raw,
            feature_spool=spool,
            historical_minutes=history,
            historical_snapshot_id="history",
            config=IntradayFeatureConfig(lookback_sessions=2, producer_commit="b" * 40),
            observed_at=tick,
            limit=128,
            input_cache=cache,
        )
    return world, features


def test_an_idle_feature_listing_reads_no_manifest_and_a_new_one_reads_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _world, features = _feature_world(tmp_path)
    reader = FeatureBatchSpool(features, cursor_root=tmp_path / "cursors", read_only=True)
    high = reader.source_descriptor().high_watermark
    assert high >= 5
    everything = reader.list_after(sequence=-1)
    original = feature_spool_module._read_control_model_with_identity
    reads: list[str] = []

    def counting(path: Path, model: type, *, label: str) -> tuple[object, object]:
        if path.parent == features / "batches":
            reads.append(path.name)
        return original(path, model, label=label)

    monkeypatch.setattr(feature_spool_module, "_read_control_model_with_identity", counting)

    idle = reader.list_after(sequence=high, through_sequence=high)
    tail = reader.list_after(sequence=high - 1, through_sequence=high)

    assert idle == ()
    assert [record.envelope for record in tail] == [everything[-1].envelope]
    assert reads == [f"{high:020d}.json"]


def test_a_changed_manifest_outside_the_listed_range_is_still_refused(tmp_path: Path) -> None:
    _world, features = _feature_world(tmp_path)
    reader = FeatureBatchSpool(features, cursor_root=tmp_path / "cursors", read_only=True)
    high = reader.source_descriptor().high_watermark
    reader.list_after(sequence=-1)
    manifest = features / "batches" / f"{0:020d}.json"
    manifest.write_bytes(manifest.read_bytes().replace(b'"sequence":0', b'"sequence": 0'))

    with pytest.raises(FeatureSpoolIntegrityError, match="canonical"):
        reader.list_after(sequence=high, through_sequence=high)


# feature_live: a warm process publishes what a cold rebuild publishes ----------------------


def _run_features(root: Path, *, warm: bool, restart_at: datetime | None) -> dict[str, bytes]:
    world = _World(root)
    features = root / "features"
    config = IntradayFeatureConfig(lookback_sessions=2, producer_commit="b" * 40)
    history_frame = _history()

    def consumers() -> tuple[LiveBatchSpool, FeatureBatchSpool, object, object]:
        raw = LiveBatchSpool(
            root / "market-minute", source_read_only=True, cursor_root=features / "raw-cursors"
        )
        return (
            raw,
            FeatureBatchSpool(features),
            NormalizedHistoricalMinutes(history_frame) if warm else history_frame,
            FeatureLiveInputCache() if warm else None,
        )

    raw, spool, history, cache = consumers()
    for tick in world.rounds():
        if not warm or tick == restart_at:
            raw, spool, history, cache = consumers()
        try:
            run_feature_live_batch(
                raw_spool=raw,
                feature_spool=spool,
                historical_minutes=history,  # type: ignore[arg-type]
                historical_snapshot_id="history",
                config=config,
                observed_at=tick,
                limit=128,
                input_cache=cache,  # type: ignore[arg-type]
            )
        except LiveSpoolIntegrityError as error:
            assert "source identity is missing" in str(error)
    return _tree(features)


def test_a_warm_feature_process_publishes_the_cold_rebuilds_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, deterministic_generations: None
) -> None:
    del deterministic_generations
    monkeypatch.chdir(tmp_path)
    (tmp_path / "cold").mkdir()
    monkeypatch.chdir(tmp_path / "cold")
    cold = _run_features(tmp_path / "cold" / "world", warm=False, restart_at=None)
    (tmp_path / "warm").mkdir()
    monkeypatch.chdir(tmp_path / "warm")
    warm = _run_features(tmp_path / "warm" / "world", warm=True, restart_at=_cn(DAY, 11, 20, 35))

    batches = [name for name in cold if name.startswith("batches/") and name.endswith(".json")]
    #: the 11:04 capture found no bar yet (an empty published batch), then one batch per
    #: minute, none over the lunch break, and 300001.SZ joining at 13:02
    assert len(batches) == 1 + len(DAY_MINUTES)
    assert warm == cold


def test_a_warm_feature_process_decodes_each_minute_batch_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    decoded: list[int] = []
    original = LiveBatchSpool.read_payload

    def counting(self: LiveBatchSpool, record: object) -> bytes:
        decoded.append(record.envelope.sequence)  # type: ignore[attr-defined]
        return original(self, record)  # type: ignore[arg-type]

    monkeypatch.setattr(LiveBatchSpool, "read_payload", counting)
    _run_features(tmp_path / "world", warm=True, restart_at=None)

    assert sorted(decoded) == sorted(set(decoded))


# paper constraints: #307, and a warm producer publishes the cold rebuild's generations -----


def _producer(world: _World, authority_root: Path) -> PaperExecutionConstraintProducer:
    return PaperExecutionConstraintProducer(
        reference_registry=ReadonlyReferenceRegistry(world.registry_path),
        minute_spool=LiveBatchSpool(world.root / "market-minute", read_only=True),
        publisher=PaperExecutionConstraintPublisher(
            root=authority_root, producer_commit=COMMIT, clock=lambda: world.now
        ),
        producer_commit=COMMIT,
        quote_ttl=timedelta(minutes=2),
    )


def _request(
    world: _World, producer: PaperExecutionConstraintProducer
) -> PaperExecutionConstraintProductionRequest:
    """The runtime step's request; the 11:04 batch has no code yet, so it waits for 11:05."""

    if world.now < _cn(DAY, 11, 5, 5):
        world.now = _cn(DAY, 11, 5, 5)
        world.gateway.capture_once(received_at=world.now)
    spool = producer.minute_spool
    latest = [
        record
        for record in spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
        if record.envelope.available_at <= world.now
    ][-1]
    frame = MarketMinuteGateway.decode_payload(spool.read_payload(latest))
    return PaperExecutionConstraintProductionRequest(
        trade_date=DAY,
        ts_codes=tuple(sorted({str(code) for code in frame["ts_code"]})),
        observed_at=world.now,
        reference_generation_id=world.generation_id,
        sequence=latest.envelope.sequence,
    )


def _run_constraints(
    root: Path, *, warm: bool, restart_at: datetime | None
) -> tuple[list[tuple[str, object]], dict[str, bytes]]:
    world = _World(root)
    authority_root = root / "paper-constraints"
    producer = _producer(world, authority_root)
    outcomes: list[tuple[str, object]] = []
    for tick in world.rounds():
        if not warm or tick == restart_at:
            producer = _producer(world, authority_root)
        publication, coverage = producer.produce_with_coverage(_request(world, producer))
        outcomes.append((tick.isoformat(), (publication.pointer, coverage)))
    return outcomes, _tree(authority_root)


def test_a_warm_producer_publishes_the_cold_rebuilds_generations(
    tmp_path: Path, deterministic_generations: None
) -> None:
    del deterministic_generations
    cold_outcomes, cold = _run_constraints(tmp_path / "cold", warm=False, restart_at=None)
    warm_outcomes, warm = _run_constraints(
        tmp_path / "warm", warm=True, restart_at=_cn(DAY, 11, 20, 35)
    )

    assert warm_outcomes == cold_outcomes
    assert warm == cold
    #: the lunch break is stale for every code and no longer a failure (#307); nothing new
    #: is written for it: the last morning generation stays current, expired
    lunch = [
        coverage
        for tick, (_pointer, coverage) in warm_outcomes
        if _cn(DAY, 11, 33) <= datetime.fromisoformat(tick) < _cn(DAY, 13, 1)
    ]
    assert lunch and all(set(coverage.stale_codes) == set(CODES[:2]) for coverage in lunch)
    generations = [name for name in warm if name.startswith("generations/")]
    assert len(generations) == len(DAY_MINUTES)


def test_an_idle_round_reads_no_minute_payload_and_no_reference_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = _World(tmp_path / "world")
    producer = _producer(world, tmp_path / "paper-constraints")
    rounds = world.rounds()
    for _tick in itertools.islice(rounds, 11):
        producer.produce_with_coverage(_request(world, producer))
    reads: list[int] = []
    snapshots: list[object] = []
    original_read = LiveBatchSpool.read_payload
    original_snapshot = ReferenceRegistry.as_of_snapshot

    def counting_read(self: LiveBatchSpool, record: object) -> bytes:
        reads.append(record.envelope.sequence)  # type: ignore[attr-defined]
        return original_read(self, record)  # type: ignore[arg-type]

    def counting_snapshot(self: ReferenceRegistry, **kwargs: object) -> object:
        snapshots.append(kwargs)
        return original_snapshot(self, **kwargs)  # type: ignore[arg-type]

    idle_tick = next(rounds)
    assert idle_tick.astimezone(SH).second == 35
    request = _request(world, producer)
    monkeypatch.setattr(LiveBatchSpool, "read_payload", counting_read)
    monkeypatch.setattr(ReferenceRegistry, "as_of_snapshot", counting_snapshot)

    producer.produce_with_coverage(request)

    assert reads == []
    assert snapshots == []


def test_a_lunch_break_round_is_stale_not_failed_and_not_tradable(tmp_path: Path) -> None:
    world = _World(tmp_path / "world")
    producer = _producer(world, tmp_path / "paper-constraints")
    last_morning = None
    for tick in world.rounds():
        if tick > _cn(DAY, 11, 40, 5):
            break
        publication, coverage = producer.produce_with_coverage(_request(world, producer))
        if tick <= _cn(DAY, 11, 30, 35):
            last_morning = publication.pointer
    assert last_morning is not None

    assert publication.pointer == last_morning
    assert set(coverage.stale_codes) == {"600000.SH", "600001.SH"}
    assert coverage.codes_without_evidence == ()
    authority = PaperExecutionConstraintAuthority(
        root=tmp_path / "paper-constraints", expected_producer_commit=COMMIT
    )
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="expired"):
        authority.resolve(
            ts_code="600000.SH",
            trade_date=DAY,
            observed_at=world.now,
            action=SignalAction.B_INTENT,
        )


def _one_batch_spool(root: Path, rows: list[dict[str, object]], *, received: datetime) -> None:
    frame = pd.DataFrame(rows)
    frame["trade_time"] = (
        pd.to_datetime(frame["trade_time"], utc=True).dt.tz_convert(SH).dt.tz_localize(None)
    )
    MarketMinuteGateway(
        spool=LiveBatchSpool(root / "market-minute"),
        fetcher=lambda: frame,
        completion_clock=lambda: received,
        config=MarketMinuteGatewayConfig(producer_version="v1", producer_commit="a" * 40),
    ).capture_once(received_at=received)


def test_a_code_without_a_same_day_minute_is_left_out_and_the_others_published(
    tmp_path: Path,
) -> None:
    """#307: a suspended code's re-served bar from yesterday no longer refuses every code."""

    world = _World(tmp_path / "world")
    received = _cn(DAY, 11, 6, 5)
    world.now = received
    _one_batch_spool(
        tmp_path / "world",
        [
            _bar("600000.SH", _cn(DAY, 11, 6), 9),
            _bar("600001.SH", _cn(PRIOR[-1], 15, 0), 1),
        ],
        received=received,
    )
    producer = _producer(world, tmp_path / "paper-constraints")

    publication, coverage = producer.produce_with_coverage(_request(world, producer))

    assert coverage.codes_without_evidence == ("600001.SH",)
    assert coverage.stale_codes == ()
    assert {record.ts_code for record in publication.batch.records} == {"600000.SH"}
    authority = PaperExecutionConstraintAuthority(
        root=tmp_path / "paper-constraints", expected_producer_commit=COMMIT
    )
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="not found"):
        authority.resolve(
            ts_code="600001.SH",
            trade_date=DAY,
            observed_at=received,
            action=SignalAction.B_INTENT,
        )


def test_no_code_with_a_same_day_minute_is_nothing_to_publish(tmp_path: Path) -> None:
    world = _World(tmp_path / "world")
    received = _cn(DAY, 11, 6, 5)
    world.now = received
    _one_batch_spool(
        tmp_path / "world", [_bar("600001.SH", _cn(PRIOR[-1], 15, 0), 1)], received=received
    )
    producer = _producer(world, tmp_path / "paper-constraints")

    with pytest.raises(PaperExecutionConstraintNoEvidenceError):
        producer.produce_with_coverage(_request(world, producer))
    assert not (tmp_path / "paper-constraints" / "current.json").exists()


def test_the_publisher_does_not_reread_the_generation_it_confirmed_and_heals_a_lost_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.paper_execution_constraints as constraints

    world = _World(tmp_path / "world")
    authority_root = tmp_path / "paper-constraints"
    producer = _producer(world, authority_root)
    for _tick in itertools.islice(world.rounds(), 6):
        first, _coverage = producer.produce_with_coverage(_request(world, producer))
    loads: list[object] = []
    original = constraints._load_current_for_publisher

    def counting(**kwargs: object) -> object:
        loads.append(kwargs)
        return original(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(constraints, "_load_current_for_publisher", counting)

    again, _coverage = producer.produce_with_coverage(_request(world, producer))
    assert again.pointer == first.pointer
    assert loads == []

    (authority_root / "current.json").unlink()
    healed, _coverage = producer.produce_with_coverage(_request(world, producer))
    assert healed.pointer.batch_hash == first.pointer.batch_hash
    assert (authority_root / "current.json").is_file()
    assert len(loads) == 1


def test_the_publisher_step_reports_stale_codes_as_an_observation_not_a_failure(
    tmp_path: Path,
) -> None:
    world = _World(tmp_path / "world")
    for _tick in itertools.islice(world.rounds(), 4):
        pass
    clock: list[datetime] = [world.now]
    manifest = RuntimeServiceManifest(
        service_id="paper-constraint.market.v1",
        service_kind=RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=2,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "minute_spool_root": str(world.root / "market-minute"),
            "reference_registry_path": str(world.registry_path),
            "authority_root": str(tmp_path / "paper-constraints"),
            "quote_ttl_seconds": 120,
        },
    )
    step = paper_execution_constraint_publisher_builder(clock=lambda: clock[0])(manifest)

    fresh = step()
    clock[0] = world.now + timedelta(minutes=5)
    stale = step()

    assert fresh.observations == {}
    assert stale.observations == {PAPER_CONSTRAINT_STALE_CODES_OBSERVATION: 2}
    assert stale.output_sequence == fresh.output_sequence
    assert PAPER_CONSTRAINT_CODES_WITHOUT_EVIDENCE_OBSERVATION not in stale.observations


def test_published_quality_filter_is_unchanged(tmp_path: Path) -> None:
    """A STALE latest minute batch still refuses the round: it says nothing about any code."""

    world = _World(tmp_path / "world")
    rounds = world.rounds()
    for _tick in itertools.islice(rounds, 2):
        pass
    failing: Callable[[], pd.DataFrame] = lambda: (_ for _ in ()).throw(TimeoutError("down"))  # noqa: E731
    world.now = world.now + timedelta(minutes=1)
    MarketMinuteGateway(
        spool=world.gateway.spool,
        fetcher=failing,
        completion_clock=lambda: world.now,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1", producer_commit="a" * 40
        ),
    ).capture_once(received_at=world.now)
    producer = _producer(world, tmp_path / "paper-constraints")
    latest = producer.minute_spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[-1]
    assert latest.envelope.quality_status is BatchQualityStatus.STALE

    with pytest.raises(Exception, match="latest visible market-minute batch is stale"):
        producer.produce_with_coverage(
            PaperExecutionConstraintProductionRequest(
                trade_date=DAY,
                ts_codes=("600000.SH",),
                observed_at=world.now,
                reference_generation_id=world.generation_id,
                sequence=latest.envelope.sequence,
            )
        )
