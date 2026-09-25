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

    def namechange_raw(self, start_date: date, end_date: date, ts_code: str | None = None) -> Any:
        return self._answer("namechange", ts_code)


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


def _bars(code: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": code,
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


def test_minute_bars_are_cached_per_code_and_day_but_an_empty_answer_never_is(
    tmp_path: Path,
) -> None:
    sources = _sources()
    fake = _FakeTushare(
        stk_mins=lambda code: _bars(code) if code == "600000.SH" else pd.DataFrame()
    )
    cache = sources.TushareCache(tmp_path / "cache", token="dummy", adapter_factory=lambda _t: fake)
    fetch = sources.cached_minute_fetcher(cache, DAY)

    assert len(fetch("600000.SH")) == 1
    assert fetch("300001.SZ").empty
    fetch("600000.SH")
    fetch("300001.SZ")

    #: the bars are asked once; the empty answer is asked again, and nothing is kept for it
    assert fake.calls == [
        ("stk_mins", "600000.SH"),
        ("stk_mins", "300001.SZ"),
        ("stk_mins", "300001.SZ"),
    ]
    minutes = tmp_path / "cache" / "stk_mins" / "1min" / "20260918"
    assert (minutes / "600000.SH.parquet").is_file()
    assert not (minutes / "300001.SZ.parquet").exists()
    assert fetch.report()["empty_not_traded_in_auction"] == ["300001.SZ"]


def test_an_empty_answer_for_a_code_that_traded_is_retried_with_backoff(tmp_path: Path) -> None:
    """Host 2026-09-24: 920003.BJ matched in the auction, and one empty stk_mins answer --
    cached by the first cut -- removed it from every later replay of the day."""

    sources = _sources()
    answers = {"920003.BJ": [pd.DataFrame(), pd.DataFrame(), _bars("920003.BJ")]}
    fake = _FakeTushare(
        stk_mins=lambda code: answers[code].pop(0) if answers.get(code) else pd.DataFrame()
    )
    cache = sources.TushareCache(tmp_path / "cache", token="dummy", adapter_factory=lambda _t: fake)
    waits: list[float] = []
    fetch = sources.MinuteFetcher(
        cache=cache,
        trade_date=DAY,
        expected_codes=frozenset({"920003.BJ", "430017.BJ"}),
        retries=3,
        backoff_seconds=5.0,
        sleep=waits.append,
    )

    assert len(fetch("920003.BJ")) == 1
    assert fetch("430017.BJ").empty

    #: two empty answers, then the bars: waited 5 s and 10 s; the other code gave up after
    #: 1 + 3 attempts (5, 10, 20 s) and is named
    assert [call for call in fake.calls if call[1] == "920003.BJ"] == [
        ("stk_mins", "920003.BJ")
    ] * 3
    assert [call for call in fake.calls if call[1] == "430017.BJ"] == [
        ("stk_mins", "430017.BJ")
    ] * 4
    assert waits == [5.0, 10.0, 5.0, 10.0, 20.0]
    report = fetch.report()
    assert report["asked_more_than_once"] == {"430017.BJ": 4, "920003.BJ": 3}
    assert report["empty_after_retries"] == ["430017.BJ"]
    minutes = tmp_path / "cache" / "stk_mins" / "1min" / "20260918"
    assert (minutes / "920003.BJ.parquet").is_file()
    assert not (minutes / "430017.BJ.parquet").exists()


def test_an_empty_answer_an_earlier_version_cached_is_ignored_and_asked_again(
    tmp_path: Path,
) -> None:
    sources = _sources()
    cache = sources.TushareCache(tmp_path / "cache", token="dummy")
    #: what package AK's first cut left behind for 920003.BJ on the host
    cache.store("stk_mins", "1min/20260918/920003.BJ", pd.DataFrame())
    fake = _FakeTushare(stk_mins=_bars)
    cache.adapter_factory = lambda _token: fake
    fetch = sources.cached_minute_fetcher(cache, DAY, expected_codes={"920003.BJ"})

    assert len(fetch("920003.BJ")) == 1

    assert fake.calls == [("stk_mins", "920003.BJ")]
    assert fetch.report()["ignored_cached_empty"] == ["920003.BJ"]
    stored, receipt = cache.load("stk_mins", "1min/20260918/920003.BJ")
    assert len(stored) == 1 and receipt["rows"] == 1
    #: offline, a stale empty entry is not an answer either: the code stays missing, said so
    offline = sources.TushareCache(tmp_path / "cold", offline=True)
    offline.store("stk_mins", "1min/20260918/920003.BJ", pd.DataFrame())
    with pytest.raises(sources.TushareCacheMissError, match="--tushare-offline"):
        sources.cached_minute_fetcher(offline, DAY, expected_codes={"920003.BJ"})("920003.BJ")


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


# ---------------------------------------------------------------------------------------
# The evidence answers every capture query as the replica before the day did (2026-09-24)
# ---------------------------------------------------------------------------------------

#: a quarter and a half of weekday sessions, so a code can stop trading 140 sessions back
LONG_OPEN_DATES = tuple(
    day
    for day in (date(2026, 2, 2) + timedelta(days=offset) for offset in range(245))
    if day.weekday() < 5
)
LONG_DAY = date(2026, 9, 18)
LONG_PRIOR = max(day for day in LONG_OPEN_DATES if day < LONG_DAY)
#: every shape a calendar window cuts off, as the host's 54 market_liquidity rows were:
#: code -> (sessions before the day its daily_bar stops, sessions its daily_basic stops,
#: sessions it has traded at all)
LIQUIDITY_SHAPES = {
    "600000.SH": (0, 0, None),  # trades every session, before and after the day
    "000001.SZ": (0, 0, None),
    "920003.BJ": (0, 15, None),  # a BSE code whose daily_basic lags three weeks
    "430017.BJ": (40, 40, None),  # a pre-920 BSE code whose history ends at the migration
    "600999.SH": (140, 140, None),  # suspended for more than half a year
    "301999.SZ": (0, 0, 3),  # listed three sessions ago
}


def _liquidity_replica(path: Path, *, until: date | None = None) -> Path:
    """A replica holding the day and the sessions after it (`until=None`), or -- the
    ground truth of what the 09:20 capture read -- only the sessions before `until`."""

    import duckdb

    before = [day for day in LONG_OPEN_DATES if day < LONG_DAY]
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE, "
            "low DOUBLE, close DOUBLE, pre_close DOUBLE, pct_chg DOUBLE, vol DOUBLE, "
            "amount DOUBLE, PRIMARY KEY (ts_code, trade_date))"
        )
        connection.execute(
            "CREATE TABLE daily_basic(ts_code VARCHAR, trade_date DATE, turnover_rate DOUBLE, "
            "volume_ratio DOUBLE, total_mv DOUBLE, circ_mv DOUBLE, "
            "PRIMARY KEY (ts_code, trade_date))"
        )
        connection.execute(
            "CREATE TABLE adj_factor(ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE)"
        )
        connection.execute(
            "CREATE TABLE daily_state(ts_code VARCHAR, trade_date DATE, is_st BOOLEAN, "
            "is_bj BOOLEAN, board_type VARCHAR, is_limit_up BOOLEAN)"
        )
        connection.execute(
            "CREATE TABLE stock_basic(ts_code VARCHAR, name VARCHAR, industry VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE risk_blacklist(list_label VARCHAR, ts_code VARCHAR, "
            "expires_at DATE, imported_at DATE)"
        )
        bars, basics, factors, states = [], [], [], []
        for index, (code, (bar_gap, basic_gap, listed)) in enumerate(LIQUIDITY_SHAPES.items()):
            connection.execute(
                "INSERT INTO stock_basic VALUES (?, ?, ?)", [code, f"样本{index}", "银行"]
            )
            sessions = list(LONG_OPEN_DATES) if listed is None else list(before[-listed:])
            if listed is not None:
                sessions += [day for day in LONG_OPEN_DATES if day >= LONG_DAY]
            for position, day in enumerate(sessions):
                if day < LONG_DAY and before.index(day) >= len(before) - bar_gap:
                    continue
                close = 10.0 + index + 0.01 * position
                bars.append(
                    (
                        code,
                        day,
                        close,
                        close + 0.1,
                        close - 0.1,
                        close,
                        close - 0.05,
                        0.5,
                        1000.0 + position,
                        (1000.0 + position) * close * (index + 1),
                    )
                )
                factors.append((code, day, 1.0 + 0.001 * index))
                states.append((code, day, False, code.endswith(".BJ"), "main", False))
                if not (day < LONG_DAY and before.index(day) >= len(before) - basic_gap):
                    basics.append((code, day, 1.5, 1.1, 1e9 * (index + 1), 5e8 * (index + 1)))
        connection.executemany("INSERT INTO daily_bar VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", bars)
        connection.executemany("INSERT INTO daily_basic VALUES (?, ?, ?, ?, ?, ?)", basics)
        connection.executemany("INSERT INTO adj_factor VALUES (?, ?, ?)", factors)
        connection.executemany("INSERT INTO daily_state VALUES (?, ?, ?, ?, ?, ?)", states)
        connection.execute(
            "INSERT INTO risk_blacklist VALUES ('430黑名单', '600999.SH', ?, ?), "
            "('430黑名单', '000001.SZ', ?, ?)",
            [date(2026, 12, 31), date(2026, 6, 1), date(2026, 12, 31), date(2026, 9, 21)],
        )
        if until is not None:
            for table in ("daily_bar", "daily_basic", "adj_factor", "daily_state"):
                connection.execute(f"DELETE FROM {table} WHERE trade_date >= ?", [until])
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    path.chmod(0o600)
    return path


def _capture_reads(database: Path) -> Any:
    """Every query the live reference-slow capture makes of its database, answered."""

    from rquant.reference_slow_source import _query_database_reference_evidence

    return _query_database_reference_evidence(
        database, prior_trade_date=LONG_PRIOR, projection_as_of_date=LONG_DAY
    )


def _long_calendar() -> Any:
    from rquant.runtime_market_session import MarketCalendarAuthority

    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=LONG_OPEN_DATES[0],
        coverage_end=LONG_OPEN_DATES[-1],
        open_dates=LONG_OPEN_DATES,
        generated_at=_local(date(2026, 1, 5), 9),
    )


def test_the_evidence_answers_every_capture_query_as_the_replica_before_the_day_did(
    tmp_path: Path,
) -> None:
    """Host 2026-09-24: `market_liquidity` was 5,565 synthesized rows against 5,619 recorded.

    The capture reads each code's latest `daily_basic` row and last five `daily_bar` rows
    wherever they fall; the first cut kept ten sessions of one and 130 of the other, so every
    code whose history ends earlier -- a lagging BSE code, a pre-920 BSE code, a long
    suspension -- fell out. Now the extract of a replica that already holds the day and the
    sessions after it answers every query exactly as the replica did at 09:20.
    """

    sources = _sources()
    replica = _liquidity_replica(tmp_path / "rquant_ro.duckdb")
    truth = _liquidity_replica(tmp_path / "as-of-0920.duckdb", until=LONG_DAY)
    evidence = sources.extract_reference_evidence(
        replica=replica,
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=LONG_DAY,
        calendar=_long_calendar(),
        audit=_Audit(),
    )

    expected_rows, expected_projections = _capture_reads(truth)
    rows, projections = _capture_reads(Path(evidence["path"]))

    #: the fixture really has the shapes that were lost
    liquidity = {row["ts_code"] for row in expected_projections["market_liquidity"]}
    assert liquidity == set(LIQUIDITY_SHAPES)
    assert evidence["codes_not_trading_on_prior_session"] == 2
    #: and the extract answers every one of them, row for row
    assert rows == expected_rows
    assert projections.keys() == expected_projections.keys()
    for table in expected_projections:
        assert projections[table] == expected_projections[table], table
    assert {row["ts_code"] for row in projections["daily_bar"]} == set(LIQUIDITY_SHAPES)
    assert (
        max(
            sum(1 for row in projections["daily_bar"] if row["ts_code"] == code)
            for code in LIQUIDITY_SHAPES
        )
        == sources.REFERENCE_EVIDENCE_DAILY_ROWS_PER_CODE
    )


def test_a_calendar_window_is_what_lost_the_liquidity_rows(tmp_path: Path) -> None:
    """The negative control: the first cut's windows, applied to the same replica."""

    import duckdb

    replica = _liquidity_replica(tmp_path / "rquant_ro.duckdb")
    before = [day for day in LONG_OPEN_DATES if day < LONG_DAY]
    first_cut = tmp_path / "first-cut.duckdb"
    connection = duckdb.connect(str(first_cut))
    try:
        connection.execute(f"ATTACH '{replica}' AS source_replica (READ_ONLY)")
        for table, since in (("daily_bar", before[-130]), ("daily_basic", before[-10])):
            connection.execute(
                f"CREATE TABLE {table} AS SELECT * FROM source_replica.{table} "
                "WHERE trade_date >= ? AND trade_date < ?",
                [since, LONG_DAY],
            )
        for table in ("adj_factor", "daily_state"):
            connection.execute(
                f"CREATE TABLE {table} AS SELECT * FROM source_replica.{table} "
                "WHERE trade_date = ?",
                [LONG_PRIOR],
            )
        for table in ("stock_basic", "risk_blacklist"):
            connection.execute(f"CREATE TABLE {table} AS SELECT * FROM source_replica.{table}")
        connection.execute("DETACH source_replica")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    first_cut.chmod(0o600)

    _rows, projections = _capture_reads(first_cut)

    assert set(LIQUIDITY_SHAPES) - {row["ts_code"] for row in projections["market_liquidity"]} == {
        "920003.BJ",
        "430017.BJ",
        "600999.SH",
    }


def test_the_projection_report_names_the_rows_only_one_side_has() -> None:
    from types import SimpleNamespace

    sources = _sources()

    def snapshot(rows: list[dict[str, Any]]) -> Any:
        return SimpleNamespace(
            projections=[
                SimpleNamespace(table_name="market_liquidity", rows=tuple(rows)),
                SimpleNamespace(table_name="dc_board", rows=()),
            ]
        )

    recorded = snapshot(
        [
            {"ts_code": "920003.BJ", "circ_mv": 1.0, "avg_amount_5d": 2.0},
            {"ts_code": "600000.SH", "circ_mv": 3.0, "avg_amount_5d": 4.0},
            {"ts_code": "000001.SZ", "circ_mv": 5.0, "avg_amount_5d": 6.0},
        ]
    )
    synthesized = snapshot(
        [
            {"ts_code": "600000.SH", "circ_mv": 3.0, "avg_amount_5d": 4.0},
            {"ts_code": "000001.SZ", "circ_mv": 5.0, "avg_amount_5d": 6.5},
        ]
    )

    report = sources.compare_projections(recorded, synthesized)

    liquidity = report["market_liquidity"]
    assert liquidity["key"] == ["ts_code"]
    assert liquidity["only_recorded"] == ["920003.BJ"]
    assert liquidity["only_synthesized_count"] == 0
    assert liquidity["values_differ"] == ["000001.SZ"]
    assert liquidity["identical"] is False
    assert report["dc_board"]["identical"] is True


def test_the_candidate_input_report_names_each_code_that_reads_different_evidence() -> None:
    from rquant.auction_match_gateway import AuctionMatchGateway
    from rquant.reference_slow_publisher import (
        ReferenceDailyFact,
        ReferenceSecurityFact,
        ReferenceSlowSourceSnapshot,
    )

    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    codes = ("300001.SZ", "600000.SH", "920003.BJ")

    def snapshot(*, suspended: tuple[str, ...], market: dict[str, str]) -> Any:
        return ReferenceSlowSourceSnapshot.create(
            target_trade_date=DAY,
            captured_at=_local(DAY, 9, 20, 22),
            producer_commit=COMMIT,
            source_snapshot_ids={
                "calendar": calendar.content_sha256,
                "daily": "1" * 64,
                "security": "2" * 64,
                "suspension": "3" * 64,
            },
            daily_facts=tuple(
                ReferenceDailyFact(
                    ts_code=code,
                    trade_date=PRIOR,
                    close_raw=10.0,
                    prior_adj_factor=1.0,
                    adj_factor=1.0,
                )
                for code in codes
            ),
            security_facts=tuple(
                ReferenceSecurityFact(
                    ts_code=code,
                    name="样本",
                    list_date=date(2010, 1, 4),
                    market=market.get(code, "主板"),
                )
                for code in codes
            ),
            suspended_codes=suspended,
        )

    auction = AuctionMatchGateway.normalize_frame(
        _auction(codes), trade_date=DAY, expected_codes=codes
    )
    moved = auction.copy()
    moved.loc[moved["ts_code"] == "300001.SZ", "vol"] = 1.0

    report = sources.compare_candidate_inputs(
        recorded_snapshot=snapshot(suspended=(), market={}),
        recorded_calendar=calendar,
        synthesized_snapshot=snapshot(suspended=("920003.BJ",), market={"600000.SH": "科创板"}),
        synthesized_calendar=calendar,
        recorded_auction_payload=AuctionMatchGateway.encode_payload(auction),
        synthesized_auction_payload=AuctionMatchGateway.encode_payload(moved),
    )

    assert report["codes_compared"] == 3
    assert report["codes_differ"] == ["300001.SZ", "600000.SH", "920003.BJ"]
    assert report["by_code"]["300001.SZ"] == {"auction_columns": ["vol"]}
    assert report["by_code"]["600000.SH"]["reference_records"] == {
        "security_board_membership": {"market": ["主板", "科创板"]}
    }
    assert report["by_code"]["920003.BJ"]["reference_records"] == {
        "security_suspension_status": {"is_suspended": [False, True]}
    }
    assert report["calendar_differs"] is False


def test_two_runs_signals_are_diffed_up_to_the_earlier_runs_last_tick() -> None:
    sources = _sources()

    def signal(at: str, code: str, action: str = "watch") -> dict[str, str]:
        return {
            "event_time_local": f"2026-09-24T{at}",
            "strategy_id": "auction_gap",
            "candidate_id": code,
            "action": action,
        }

    recorded = [
        signal("09:30:00", "002819.SZ"),
        signal("09:30:00", "920003.BJ"),
        signal("09:31:00", "603937.SH", "b_intent"),
        signal("13:04:00", "002238.SZ"),
    ]
    synthesized = [
        signal("09:30:00", "002819.SZ"),
        signal("09:31:00", "603937.SH", "b_intent"),
    ]

    report = sources.compare_signal_lists(synthesized, recorded, until_local="09:45:00")

    assert report["common"] == 2
    assert report["only_this_run"] == []
    assert report["only_other_run"] == [
        {
            "event_time_local": "2026-09-24T09:30:00",
            "strategy_id": "auction_gap",
            "candidate_id": "920003.BJ",
            "action": "watch",
        }
    ]
    assert report["codes_differ"] == ["920003.BJ"]
    assert report["identical"] is False


def test_a_full_day_suspension_of_a_code_that_matched_in_the_auction_is_left_out(
    tmp_path: Path,
) -> None:
    """suspend_d(D), asked after the day, lists what happened during it. An intraday halt
    recorded without a timing reads as a full-day suspension and would mark a code the 09:20
    capture saw trading as suspended -- auction_gap then drops it. A code that matched in the
    opening auction was trading at 09:25: its full-day row is left out and listed."""

    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    evidence = sources.extract_reference_evidence(
        replica=_replica(tmp_path / "rquant_ro.duckdb"),
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=_Audit(),
    )
    suspensions = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_date": "20260918",
                "suspend_timing": None,
                "suspend_type": "S",
            },
            {
                "ts_code": ST_TODAY,
                "trade_date": "20260918",
                "suspend_timing": None,
                "suspend_type": "S",
            },
            {
                "ts_code": "300001.SZ",
                "trade_date": "20260918",
                "suspend_timing": "10:00-10:30",
                "suspend_type": "S",
            },
        ]
    )
    source = _source(tmp_path, _FakeTushare(**{**_day_answers(), "suspend_d": suspensions}))
    auction = source.stk_auction(DAY)
    known = sources.KnownAtCapture(source, traded_in_auction=sources.auction_traded_codes(auction))

    snapshot = sources.synthesize_reference_snapshot(
        evidence_database=Path(evidence["path"]),
        source=known,
        calendar=calendar,
        trade_date=DAY,
        producer_commit=COMMIT,
    )
    naive = sources.synthesize_reference_snapshot(
        evidence_database=Path(evidence["path"]),
        source=source,
        calendar=calendar,
        trade_date=DAY,
        producer_commit=COMMIT,
    )

    #: the NaN-priced auction row of 000005.SZ did not match; the two others did
    assert sources.auction_traded_codes(auction) == frozenset(CODES)
    assert naive.suspended_codes == ("600000.SH", ST_TODAY)
    assert snapshot.suspended_codes == (ST_TODAY,)
    report = sources.reference_anachronisms(snapshot, known)["suspend_d_asked_after_the_day"]
    assert report["left_out_traded_in_the_opening_auction"] == {
        "count": 1,
        "events": ["600000.SH S (no timing)"],
    }
    assert report["full_day_suspended"] == {"count": 1, "codes": [ST_TODAY]}
    assert report["partial_or_resumption_events"]["events"] == ["300001.SZ S 10:00-10:30"]


# ---------------------------------------------------------------------------------------
# Today's listing, put back to the day's
# ---------------------------------------------------------------------------------------


def _namechanges(code: str | None) -> pd.DataFrame:
    """ST_TODAY became `ST样本` on 09-21, after the day; 600000.SH was renamed on the day."""

    columns = ("ts_code", "name", "start_date", "end_date", "ann_date", "change_reason")
    window = [
        (ST_TODAY, "ST样本", "20260921", None, "20260919", "ST"),
        ("600000.SH", "样本新名", "20260918", None, "20260915", "改名"),
    ]
    histories = {
        ST_TODAY: [
            (ST_TODAY, "样本股份", "20100104", "20260920", "20100101", "上市"),
            (ST_TODAY, "ST样本", "20260921", None, "20260919", "ST"),
        ]
    }
    rows = window if code is None else histories.get(code, [])
    return pd.DataFrame(rows, columns=list(columns))


def test_renamed_codes_carry_the_name_they_had_on_the_day(tmp_path: Path) -> None:
    """Host 2026-09-24: security_name_differ, and the one candidate input that differed
    (601091.SH), came from today's names in stock_basic. A code renamed -- or made ST --
    after the day gets the name its history says it had; the name-derived ST follows."""

    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    evidence = sources.extract_reference_evidence(
        replica=_replica(tmp_path / "rquant_ro.duckdb"),
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=_Audit(),
    )
    source = _source(tmp_path, _FakeTushare(**{**_day_answers(), "namechange": _namechanges}))
    listing = source.stock_basic("L")

    names, facts = sources.names_as_of_day(source, set(listing["ts_code"]))

    assert names == {ST_TODAY: "样本股份"}
    assert facts["renamed_after_the_day"] == 1
    assert facts["unresolved"] == []
    assert facts["renamed_on_the_day_itself_keep_the_new_name"] == ["600000.SH"]
    known = sources.KnownAtCapture(source, traded_in_auction=frozenset(), names_on_day=names)
    snapshot = sources.synthesize_reference_snapshot(
        evidence_database=Path(evidence["path"]),
        source=known,
        calendar=calendar,
        trade_date=DAY,
        producer_commit=COMMIT,
    )
    by_code = {fact.ts_code: fact for fact in snapshot.security_facts}
    assert by_code[ST_TODAY].name == "样本股份"
    assert by_code[ST_TODAY].is_st is False
    assert sources.reference_anachronisms(snapshot, known)["st_by_todays_name_only"]["count"] == 0


def test_the_evidence_listing_gets_the_days_names_and_codes_delisted_since(
    tmp_path: Path,
) -> None:
    import duckdb

    sources = _sources()
    calendar = _calendar(generated_at=_local(date(2026, 9, 1), 9))
    replica = _replica(tmp_path / "rquant_ro.duckdb")
    replica.chmod(0o600)
    connection = duckdb.connect(str(replica))
    try:
        connection.execute(
            "CREATE TABLE stock_basic(ts_code VARCHAR, symbol VARCHAR, name VARCHAR, "
            "area VARCHAR, industry VARCHAR, list_date DATE, market VARCHAR)"
        )
        #: today's table: ST_TODAY under its new name, 600000.SH gone (delisted 09-22)
        connection.execute(
            "INSERT INTO stock_basic VALUES (?, '600001', 'ST样本', '上海', '银行', "
            "DATE '2010-01-04', '主板'), ('300001.SZ', '300001', '样本股份', '深圳', '软件', "
            "DATE '2010-01-04', '创业板')",
            [ST_TODAY],
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    evidence = sources.extract_reference_evidence(
        replica=replica,
        target=tmp_path / "sandbox" / "reference-evidence.duckdb",
        trade_date=DAY,
        calendar=calendar,
        audit=_Audit(),
    )
    delisted = _stock_basic(("600000.SH", "000009.SZ"))
    delisted["delist_date"] = ["20260922", "20260915"]
    delisted["list_status"] = "D"

    facts = sources.restore_listing_in_evidence(
        Path(evidence["path"]),
        listing=delisted,
        names_on_day={ST_TODAY: "样本股份"},
        trade_date=DAY,
        prior_trade_date=PRIOR,
    )

    assert facts["renamed_restored"] == 1
    assert facts["delisted_since_codes"] == ["600000.SH"]
    connection = duckdb.connect(str(evidence["path"]), read_only=True)
    try:
        rows = dict(connection.execute("SELECT ts_code, name FROM stock_basic").fetchall())
    finally:
        connection.close()
    #: 000009.SZ was delisted before the day and never traded on the prior session
    assert rows == {ST_TODAY: "样本股份", "300001.SZ": "样本股份", "600000.SH": "样本股份"}


def test_every_differing_projection_says_why() -> None:
    from types import SimpleNamespace

    sources = _sources()

    def snapshot(table: str, rows: list[dict[str, Any]]) -> Any:
        return SimpleNamespace(projections=[SimpleNamespace(table_name=table, rows=tuple(rows))])

    recorded = snapshot(
        "risk_blacklist",
        [{"list_label": "ST", "ts_code": ST_TODAY, "expires_at": None, "imported_at": "a"}],
    )
    synthesized = snapshot(
        "risk_blacklist",
        [{"list_label": "ST", "ts_code": ST_TODAY, "expires_at": None, "imported_at": "b"}],
    )

    report = sources.compare_projections(recorded, synthesized)["risk_blacklist"]

    assert report["values_differ_fields"] == {"imported_at": 1}
    assert report["explained_by"] == sources.PROJECTION_ANACHRONISMS["risk_blacklist"]
    assert set(sources.PROJECTION_ANACHRONISMS) >= {
        "stock_basic",
        "nl_screen_universe",
        "risk_blacklist",
        "kpl_concept_member",
        "market_liquidity",
    }
