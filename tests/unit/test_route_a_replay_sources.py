"""`scripts/route_a_replay_sources.py`: a day's source inputs, from the replica and Tushare.

Nothing here reaches the network: every Tushare answer is a fake adapter's, or a cache the
case filled itself. The live capture code (`capture_reference_slow_source_snapshot`,
`capture_reference_slow_batch`, `AuctionMatchGateway.capture_once`,
`publish_auction_universe_from_daily_snapshot`) runs for real over those answers.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "route_a_replay_sources.py"
SHANGHAI = ZoneInfo("Asia/Shanghai")
COMMIT = "b" * 40
#: 2026-09: the week the host recorded one day of
OPEN_DATES = (
    date(2026, 9, 10),
    date(2026, 9, 11),
    date(2026, 9, 14),
    date(2026, 9, 15),
    date(2026, 9, 16),
    date(2026, 9, 17),
    date(2026, 9, 18),
    date(2026, 9, 21),
    date(2026, 9, 22),
)
DAY = date(2026, 9, 18)
PRIOR = date(2026, 9, 17)
TODAY = date(2026, 9, 25)
CODES = ("300001.SZ", "600000.SH")
#: named "ST ..." today, absent from stock_st(DAY): the one anachronism, counted
ST_TODAY = "600001.SH"


def _sources() -> ModuleType:
    name = "route_a_replay_sources_unit"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _Audit:
    def __init__(self) -> None:
        self.reads: list[str] = []

    def before(self, path: Path) -> None:
        self.reads.append(f"before:{path.name}")

    def after(self, path: Path) -> None:
        self.reads.append(f"after:{path.name}")

    def read_bytes(self, path: Path) -> bytes:
        self.reads.append(f"read:{path.name}")
        return path.read_bytes()


def _calendar(*, generated_at: datetime, commit: str = COMMIT) -> Any:
    from rquant.runtime_market_session import MarketCalendarAuthority

    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=commit,
        coverage_start=date(2026, 9, 1),
        coverage_end=date(2026, 12, 31),
        open_dates=OPEN_DATES,
        generated_at=generated_at,
    )


def _local(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute, second), tzinfo=SHANGHAI).astimezone(UTC)


def _write_generation(runtime_root: Path, calendar: Any) -> Path:
    from rquant.strict_json import canonical_json_bytes

    directory = runtime_root / "authorities" / "market-calendar" / "generations"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{calendar.content_sha256}.json"
    path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
    return path


# ---------------------------------------------------------------------------------------
# Tushare through the cache
# ---------------------------------------------------------------------------------------


class _FakeTushare:
    """The live adapter's methods, answering from tables, counting every call."""

    def __init__(self, **answers: Any) -> None:
        self.answers = answers
        self.calls: list[tuple[str, Any]] = []

    def _answer(self, endpoint: str, argument: Any) -> Any:
        self.calls.append((endpoint, argument))
        answer = self.answers[endpoint]
        if isinstance(answer, Exception):
            raise answer
        return answer(argument) if callable(answer) else answer.copy()

    def stock_basic(self, list_status: str = "L") -> Any:
        return self._answer("stock_basic", list_status)

    def stock_st_raw(self, trade_date: date) -> Any:
        return self._answer("stock_st", trade_date)

    def adj_factor_by_date(self, trade_date: date) -> Any:
        return self._answer("adj_factor", trade_date)

    def suspend_d_raw(self, trade_date: date) -> Any:
        return self._answer("suspend_d", trade_date)

    def stk_auction(self, trade_date: date) -> Any:
        return self._answer("stk_auction", trade_date)

    def stk_mins(self, code: str, freq: str, start: datetime, end: datetime) -> Any:
        return self._answer("stk_mins", code)


def _stock_basic(codes: tuple[str, ...], *, names: dict[str, str] | None = None) -> pd.DataFrame:
    from rquant.adapter.tushare import STOCK_BASIC_COLUMNS

    return pd.DataFrame(
        [
            {
                "ts_code": code,
                "symbol": code[:6],
                "name": (names or {}).get(code, "样本股份"),
                "area": "上海",
                "industry": "银行",
                "list_date": "20100104",
                "delist_date": None,
                "market": "主板",
                "list_status": "L",
            }
            for code in codes
        ],
        columns=list(STOCK_BASIC_COLUMNS),
    )


def _empty(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})


def _auction(codes: tuple[str, ...], *, nan_code: str | None = None) -> pd.DataFrame:
    from rquant.adapter.tushare import STK_AUCTION_COLUMNS

    rows = [
        {
            "ts_code": code,
            "trade_date": DAY,
            "price": 10.3,
            "vol": 30_000.0,
            "amount": 309_000.0,
            "pre_close": 10.0,
            "turnover_rate": 0.1,
            "volume_ratio": 1.2,
            "auction_type": "open_realtime",
            "source": "tushare",
        }
        for code in codes
    ]
    if nan_code is not None:
        rows.append({**rows[0], "ts_code": nan_code, "price": float("nan"), "vol": 0.0})
    return pd.DataFrame(rows, columns=[*STK_AUCTION_COLUMNS, "auction_type", "source"])


def _day_answers(codes: tuple[str, ...] = CODES) -> dict[str, Any]:
    basic_columns = (
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "list_date",
        "delist_date",
        "market",
        "list_status",
    )
    everything = (*codes, ST_TODAY)
    return {
        "stock_basic": lambda status: (
            _stock_basic(everything, names={ST_TODAY: "ST样本"})
            if status == "L"
            else _empty(basic_columns)
        ),
        "stock_st": pd.DataFrame(
            [
                {
                    "ts_code": "000004.SZ",
                    "name": "*ST其他",
                    "trade_date": "20260918",
                    "type": "ST",
                    "type_name": "风险警示",
                }
            ]
        ),
        "adj_factor": pd.DataFrame(
            [{"ts_code": code, "trade_date": DAY, "adj_factor": 1.5} for code in everything]
        ),
        "suspend_d": _empty(("ts_code", "trade_date", "suspend_timing", "suspend_type")),
        "stk_auction": _auction(codes, nan_code="000005.SZ"),
    }


def _source(tmp_path: Path, fake: _FakeTushare, *, offline: bool = False, day: date = DAY) -> Any:
    sources = _sources()
    cache = sources.TushareCache(
        tmp_path / "cache", token="dummy", offline=offline, adapter_factory=lambda _token: fake
    )
    return sources.CachedDaySource(cache, trade_date=day, today=TODAY)


def test_a_second_ask_is_answered_from_disk_and_offline_never_asks(tmp_path: Path) -> None:
    sources = _sources()
    fake = _FakeTushare(stk_auction=_auction(CODES))

    first = _source(tmp_path, fake).stk_auction(DAY)
    again = _source(tmp_path, fake)
    second = again.stk_auction(DAY)

    assert fake.calls == [("stk_auction", DAY)]
    assert list(first["ts_code"]) == list(second["ts_code"]) == list(CODES)
    assert [entry["origin"] for entry in again.cache.ledger] == ["cache"]
    receipt = json.loads((tmp_path / "cache" / "stk_auction" / "20260918.json").read_text())
    assert receipt["rows"] == len(CODES) and len(receipt["sha256"]) == 64
    #: offline: the cached day answers, an uncached one is a miss, and nothing is asked
    offline = _source(tmp_path, fake, offline=True)
    assert len(offline.stk_auction(DAY)) == len(CODES)
    other = _source(tmp_path, fake, offline=True, day=date(2026, 9, 21))
    with pytest.raises(sources.TushareCacheMissError, match="--tushare-offline"):
        other.stk_auction(date(2026, 9, 21))
    assert fake.calls == [("stk_auction", DAY)]


def test_an_empty_or_failed_historical_answer_refuses_and_is_not_cached(tmp_path: Path) -> None:
    sources = _sources()
    empty = _FakeTushare(stk_auction=_auction(()))
    with pytest.raises(
        sources.InputUnavailableError, match="stk_auction for 2026-09-18 returned no rows"
    ):
        sources.prefetch_day(_source(tmp_path, empty), reference=False, auction=True)
    assert not (tmp_path / "cache" / "stk_auction" / "20260918.parquet").exists()

    failing = _FakeTushare(
        **{**_day_answers(), "adj_factor": RuntimeError("抱歉，您没有接口访问权限")}
    )
    with pytest.raises(
        sources.InputUnavailableError, match="adj_factor for 2026-09-18 failed: RuntimeError"
    ):
        sources.prefetch_day(_source(tmp_path, failing), reference=True, auction=False)

    offline = _source(tmp_path / "cold", _FakeTushare(), offline=True)
    with pytest.raises(sources.InputUnavailableError, match="not in the Tushare cache"):
        sources.prefetch_day(offline, reference=False, auction=True)


def test_a_listing_is_reused_only_when_fetched_on_or_after_the_day(tmp_path: Path) -> None:
    sources = _sources()
    cache = sources.TushareCache(tmp_path / "cache", offline=True)
    cache.store("stock_basic", "L/20260917", _stock_basic(("300001.SZ",)))
    cache.store("stock_basic", "L/20260920", _stock_basic(CODES))

    on_the_day = sources.CachedDaySource(cache, trade_date=DAY, today=TODAY)
    assert list(on_the_day.stock_basic("L")["ts_code"]) == list(CODES)
    #: a listing fetched before 09-21 may lack a code listed since: asked again (offline: a miss)
    later = sources.CachedDaySource(cache, trade_date=date(2026, 9, 21), today=TODAY)
    with pytest.raises(sources.TushareCacheMissError, match="stock_basic/L/20260925"):
        later.stock_basic("L")


def test_a_question_about_another_day_than_the_replayed_one_is_refused(tmp_path: Path) -> None:
    sources = _sources()
    source = _source(tmp_path, _FakeTushare(**_day_answers()))
    with pytest.raises(sources.InputUnavailableError, match="asked for 2026-09-17"):
        source.stk_auction(PRIOR)


def test_minute_gaps_are_one_cached_answer_per_code_and_day(tmp_path: Path) -> None:
    sources = _sources()
    bars = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": datetime.combine(DAY, time(9, 31)),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "vol": 1.0,
                "amount": 1.0,
                "freq": "1min",
                "source": "tushare",
            }
        ]
    )
    fake = _FakeTushare(stk_mins=lambda code: bars if code == "600000.SH" else pd.DataFrame())
    cache = sources.TushareCache(tmp_path / "cache", token="dummy", adapter_factory=lambda _t: fake)
    fetch = sources.cached_minute_fetcher(cache, DAY)

    assert len(fetch("600000.SH")) == 1
    assert fetch("300001.SZ").empty
    fetch("600000.SH")
    fetch("300001.SZ")

    assert fake.calls == [("stk_mins", "600000.SH"), ("stk_mins", "300001.SZ")]
    assert (tmp_path / "cache" / "stk_mins" / "1min" / "20260918" / "600000.SH.parquet").is_file()
    assert cache.summary()["by_endpoint"]["stk_mins"] == {"cache": 2, "tushare": 2, "empty": 2}


# ---------------------------------------------------------------------------------------
# The calendar valid on the day
# ---------------------------------------------------------------------------------------


def test_the_calendar_is_the_generation_installed_before_the_day_opened(tmp_path: Path) -> None:
    sources = _sources()
    runtime = tmp_path / "runtime"
    earlier = _calendar(generated_at=_local(date(2026, 9, 10), 20))
    installed = _calendar(generated_at=_local(date(2026, 9, 15), 21))
    later = _calendar(generated_at=_local(date(2026, 9, 19), 11))
    for calendar in (earlier, installed, later):
        _write_generation(runtime, calendar)

    choice = sources.choose_calendar(runtime_root=runtime, trade_date=DAY, audit=_Audit())

    assert choice.calendar.content_sha256 == installed.content_sha256
    assert choice.provenance["origin"] == "recorded"
    assert choice.provenance["generations_seen"] == 3


def test_a_calendar_generated_after_the_day_is_re_dated_and_labelled(tmp_path: Path) -> None:
    sources = _sources()
    runtime = tmp_path / "runtime"
    after = _calendar(generated_at=_local(date(2026, 9, 24), 17))
    _write_generation(runtime, after)

    choice = sources.choose_calendar(runtime_root=runtime, trade_date=DAY, audit=_Audit())

    assert choice.path is None
    assert choice.calendar.open_dates == after.open_dates
    assert choice.calendar.producer_commit == after.producer_commit
    assert choice.calendar.generated_at == _local(PRIOR, 20)
    assert choice.calendar.content_sha256 != after.content_sha256
    assert choice.provenance["origin"] == "synthesized"
    assert "generated after 2026-09-18 09:10" in choice.provenance["why"]
    with pytest.raises(sources.InputUnavailableError, match="opens 2026-09-18"):
        sources.choose_calendar(runtime_root=runtime, trade_date=DAY, audit=_Audit(), redate=False)


def test_no_calendar_that_opens_the_day_refuses_it(tmp_path: Path) -> None:
    sources = _sources()
    runtime = tmp_path / "runtime"
    _write_generation(runtime, _calendar(generated_at=_local(date(2026, 9, 1), 9)))

    with pytest.raises(sources.InputUnavailableError, match="opens 2026-09-19"):
        sources.choose_calendar(runtime_root=runtime, trade_date=date(2026, 9, 19), audit=_Audit())
    #: the last open date has no session after it
    with pytest.raises(sources.InputUnavailableError, match="open session on both sides"):
        sources.choose_calendar(runtime_root=runtime, trade_date=OPEN_DATES[-1], audit=_Audit())


# ---------------------------------------------------------------------------------------
# The replica, as it stood before the day
# ---------------------------------------------------------------------------------------


def _replica(
    path: Path,
    *,
    daily_days: tuple[date, ...] = OPEN_DATES[:7],
    adj_factor_days: tuple[date, ...] = OPEN_DATES[:7],
    minute_day: date | None = DAY,
    codes: tuple[str, ...] = (*CODES, ST_TODAY),
) -> Path:
    """A replica that already carries the day's own close (99.0), as the host's does."""

    import duckdb

    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, open DOUBLE, "
            "high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, 10, 10.5, 9.5, ?, 1000, 10000)",
            [
                (code, day, 99.0 if day == DAY else 10.0 + index)
                for index, code in enumerate(codes)
                for day in daily_days
            ],
        )
        connection.execute(
            "CREATE TABLE adj_factor(ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE)"
        )
        if adj_factor_days:
            connection.executemany(
                "INSERT INTO adj_factor VALUES (?, ?, 1.2)",
                [(code, day) for code in codes for day in adj_factor_days],
            )
        connection.execute(
            "CREATE TABLE minute_bar(ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, "
            "close DOUBLE)"
        )
        if minute_day is not None:
            connection.executemany(
                "INSERT INTO minute_bar VALUES (?, ?, '1min', 10.0)",
                [(code, datetime.combine(minute_day, time(9, 31))) for code in codes],
            )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    path.chmod(0o644)
    return path


def test_the_probe_names_every_gap_the_day_would_hit(tmp_path: Path) -> None:
    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    gappy = _replica(
        tmp_path / "gappy.duckdb",
        daily_days=tuple(day for day in OPEN_DATES[:7] if day != date(2026, 9, 14)),
        adj_factor_days=(),
        minute_day=None,
    )
    audit = _Audit()

    probe = sources.probe_replica(replica=gappy, trade_date=DAY, calendar=calendar, audit=audit)
    reasons = sources.replica_refusals(
        probe, trade_date=DAY, synthesize_reference=True, tushare_minutes=False
    )

    assert probe["prior_five"]["2026-09-14"] == 0
    assert probe["trade_date_daily_rows"] == 3
    assert audit.reads == ["before:gappy.duckdb", "after:gappy.duckdb"]
    assert len(reasons) == 3
    assert "daily_bar has no rows for 2026-09-14" in reasons[0]
    assert "adj_factor has no rows for 2026-09-17" in reasons[1]
    assert "minute_bar has no 1min rows for 2026-09-18" in reasons[2]
    #: minutes can come from stk_mins, and a recorded reference batch needs no adj_factor
    assert sources.replica_refusals(
        probe, trade_date=DAY, synthesize_reference=False, tushare_minutes=True
    ) == [reasons[0]]


def _spool(tmp_path: Path) -> Any:
    from rquant.live_spool import (
        LiveBatchSpool,
        ReferenceSourceBatchSigner,
        ReferenceSourceBatchVerifier,
    )

    key = tmp_path / "keys" / "source"
    key.parent.mkdir(mode=0o700, parents=True)
    subprocess.run(("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)), check=True)
    key.chmod(0o600)
    return LiveBatchSpool(
        tmp_path / "live" / "reference-slow",
        source_signer=ReferenceSourceBatchSigner(key_id="k", private_key=key.read_text("ascii")),
        source_verifier=ReferenceSourceBatchVerifier(
            key_id="k", public_key=key.with_suffix(".pub").read_text("ascii").strip()
        ),
    )


def test_the_reference_batch_is_the_live_capture_over_the_replica_before_the_day(
    tmp_path: Path,
) -> None:
    from rquant.live_contracts import LiveChannel

    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    audit = _Audit()
    evidence = sources.extract_reference_evidence(
        replica=replica,
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=audit,
    )
    source = _source(tmp_path, _FakeTushare(**_day_answers()))
    sources.prefetch_day(source, reference=True, auction=False)

    snapshot = sources.synthesize_reference_snapshot(
        evidence_database=Path(evidence["path"]),
        source=source,
        calendar=calendar,
        trade_date=DAY,
        producer_commit=COMMIT,
    )

    #: the day's own close (99.0) never reached the capture: daily facts are the prior session's
    assert evidence["rows"] == {"daily_bar": 3 * 6, "adj_factor": 3}
    assert snapshot.target_trade_date == DAY
    assert snapshot.captured_at == _local(DAY, 9, 20, 22)
    assert {fact.trade_date for fact in snapshot.daily_facts} == {PRIOR}
    assert sorted(fact.close_raw for fact in snapshot.daily_facts) == [10.0, 11.0, 12.0]
    assert {fact.prior_adj_factor for fact in snapshot.daily_facts} == {1.2}
    assert {fact.adj_factor for fact in snapshot.daily_facts} == {1.5}
    assert snapshot.source_snapshot_ids["calendar"] == calendar.content_sha256
    by_code = {fact.ts_code: fact for fact in snapshot.security_facts}
    assert by_code[ST_TODAY].is_st is True
    assert by_code["600000.SH"].is_st is False
    assert sources.reference_anachronisms(snapshot, source)["st_by_todays_name_only"] == {
        "count": 1,
        "codes": [ST_TODAY],
    }

    spool = _spool(tmp_path)
    prepared = snapshot.captured_at + timedelta(seconds=5)
    result = sources.seal_reference_snapshot(
        spool=spool,
        calendar=calendar,
        snapshot=snapshot,
        producer_commit=COMMIT,
        producer_version="reference-slow-source-v1",
        prepared_at=prepared,
    )
    (record,) = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    assert result.output_sequence == 0
    assert record.envelope.batch_id == snapshot.content_sha256
    assert record.envelope.available_at == prepared + timedelta(seconds=30)
    spool.verify_reference_source_record(record)


def test_a_capture_the_live_code_refuses_is_a_refusal_naming_its_cause(tmp_path: Path) -> None:
    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    evidence = sources.extract_reference_evidence(
        replica=_replica(tmp_path / "rquant_ro.duckdb"),
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=_Audit(),
    )
    #: today's listing lacks a code the prior session traded
    listing = _day_answers()["stock_basic"]
    answers = {
        **_day_answers(),
        "stock_basic": lambda status: _stock_basic(CODES) if status == "L" else listing(status),
    }
    source = _source(tmp_path, _FakeTushare(**answers))

    with pytest.raises(
        sources.InputUnavailableError, match="stock_basic does not cover prior daily universe"
    ):
        sources.synthesize_reference_snapshot(
            evidence_database=Path(evidence["path"]),
            source=source,
            calendar=calendar,
            trade_date=DAY,
            producer_commit=COMMIT,
        )


def test_the_evidence_refuses_a_replica_without_adj_factor_rows_for_the_prior_session(
    tmp_path: Path,
) -> None:
    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    replica = _replica(tmp_path / "rquant_ro.duckdb", adj_factor_days=OPEN_DATES[:5])

    with pytest.raises(sources.InputUnavailableError, match="no adj_factor rows for 2026-09-17"):
        sources.extract_reference_evidence(
            replica=replica,
            target=tmp_path / "sandbox" / "a.duckdb",
            trade_date=DAY,
            calendar=calendar,
            audit=_Audit(),
        )
    #: the auction alone does not need it
    facts = sources.extract_reference_evidence(
        replica=replica,
        target=tmp_path / "sandbox" / "b.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=_Audit(),
        require_adj_factor=False,
    )
    assert facts["rows"]["adj_factor"] == 0


# ---------------------------------------------------------------------------------------
# The auction universe and the auction-match batch
# ---------------------------------------------------------------------------------------


def test_the_auction_batch_is_the_live_gateway_capture_with_pre_close_and_nan_rows_dropped(
    tmp_path: Path,
) -> None:
    from rquant.auction_match_gateway import AuctionMatchGateway
    from rquant.live_spool import LiveBatchSpool

    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    evidence = sources.extract_reference_evidence(
        replica=_replica(tmp_path / "rquant_ro.duckdb", codes=CODES),
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=_Audit(),
    )
    codes, universe = sources.synthesize_auction_universe(
        evidence_database=Path(evidence["path"]),
        authority_root=tmp_path / "sandbox" / "auction-universe",
        calendar=calendar,
        trade_date=DAY,
        producer_commit=COMMIT,
    )
    source = _source(tmp_path, _FakeTushare(**_day_answers()))
    sources.prefetch_day(source, reference=False, auction=True)

    facts = sources.synthesize_auction_batch(
        spool=LiveBatchSpool(tmp_path / "live" / "auction-match"),
        source=source,
        trade_date=DAY,
        expected_codes=codes,
        settings={"producer_version": "auction-match-source-v1", "min_coverage_ratio": 0.95},
        producer_commit=COMMIT,
        received_at=_local(DAY, 9, 29, 7),
    )

    assert codes == CODES
    assert universe["source"] == "replica daily_bar codes of 2026-09-17"
    assert facts["quality_status"] == "published"
    assert facts["rows"] == len(CODES)
    assert facts["rows_dropped_non_finite"] == 1
    assert facts["available_at"] == _local(DAY, 9, 29, 7)
    frame = AuctionMatchGateway.decode_payload(facts["payload"])
    assert list(frame["pre_close"]) == [10.0, 10.0]
    assert set(frame["trade_date"]) == {DAY}


def test_a_thin_auction_is_published_degraded_and_says_why(tmp_path: Path) -> None:
    from rquant.live_spool import LiveBatchSpool

    sources = _sources()
    answers = {**_day_answers(), "stk_auction": _auction(CODES[:1])}
    source = _source(tmp_path, _FakeTushare(**answers))

    facts = sources.synthesize_auction_batch(
        spool=LiveBatchSpool(tmp_path / "live" / "auction-match"),
        source=source,
        trade_date=DAY,
        expected_codes=CODES,
        settings={"producer_version": "auction-match-source-v1", "min_coverage_ratio": 0.95},
        producer_commit=COMMIT,
        received_at=_local(DAY, 9, 29, 7),
    )

    assert facts["quality_status"] == "degraded"
    assert facts["degraded_reasons"] == ["coverage_below_minimum"]
    assert facts["coverage_ratio"] == 0.5


# ---------------------------------------------------------------------------------------
# Synthesized against recorded
# ---------------------------------------------------------------------------------------


def test_the_fidelity_report_counts_what_differs(tmp_path: Path) -> None:
    from rquant.auction_match_gateway import AuctionMatchGateway
    from rquant.reference_slow_publisher import (
        ReferenceDailyFact,
        ReferenceSecurityFact,
        ReferenceSlowSourceSnapshot,
    )

    sources = _sources()

    def snapshot(close: float, codes: tuple[str, ...]) -> Any:
        return ReferenceSlowSourceSnapshot.create(
            target_trade_date=DAY,
            captured_at=_local(DAY, 9, 20, 22),
            producer_commit=COMMIT,
            source_snapshot_ids={
                key: "1" * 64 for key in ("calendar", "daily", "security", "suspension")
            },
            daily_facts=tuple(
                ReferenceDailyFact(
                    ts_code=code,
                    trade_date=PRIOR,
                    close_raw=close if code == codes[0] else 10.0,
                    prior_adj_factor=1.0,
                    adj_factor=1.0,
                )
                for code in codes
            ),
            security_facts=tuple(
                ReferenceSecurityFact(
                    ts_code=code, name="样本", list_date=date(2010, 1, 4), market="主板"
                )
                for code in codes
            ),
        )

    reference = sources.compare_reference(snapshot(10.0, CODES), snapshot(10.1, (*CODES, ST_TODAY)))
    assert reference["securities"] == {"recorded": 2, "synthesized": 3}
    assert reference["only_synthesized"] == [ST_TODAY]
    assert reference["daily_close_raw_differ"] == {"count": 1, "codes": ["300001.SZ"]}
    assert reference["security_name_differ"]["count"] == 0

    normalized = AuctionMatchGateway.normalize_frame(
        _auction(CODES), trade_date=DAY, expected_codes=CODES
    )
    moved = normalized.copy()
    moved.loc[0, "price"] = 10.4
    auction = sources.compare_auction(
        AuctionMatchGateway.encode_payload(normalized),
        AuctionMatchGateway.encode_payload(moved.iloc[:1]),
    )
    assert auction["rows"] == {"recorded": 2, "synthesized": 1}
    assert auction["only_recorded"] == ["600000.SH"]
    assert auction["price_differ"] == 1
    assert auction["pre_close_differ"] == 0
