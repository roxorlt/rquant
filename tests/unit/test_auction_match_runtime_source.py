"""auction-match 构建器：可配置的采集窗、摊开的重试、留得住的失败（#277）。

2026-09-21 生产现场：09:26:01/04/08 三次 `stk_auction` 全「返回空」，零批次、零报错，
09:50 的心跳还是 `running / last_error None / processed_count 0 / output_sequence -1`。
三件事合起来造成它：窗写死在 09:26-09:30（当天数据那时还没出），三次重试挤在七秒里，
早退分支用那份**从未被写过**的初始结果把心跳洗干净。这个文件逐条钉住修好之后的形状。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.auction_universe_authority import AuctionUniverseAuthority
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import (
    AUCTION_MATCH_DEFAULT_CAPTURE_END,
    AUCTION_MATCH_DEFAULT_CAPTURE_START,
    AuctionMatchSourceSettings,
    auction_match_source_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.source_quota_store import SourceQuotaAttemptOutcome, SourceQuotaStore

COMMIT = "a" * 40
SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 7, 31)
NEXT_TRADE_DATE = date(2026, 8, 3)


def at(hour: int, minute: int, second: int = 0, *, day: date = TRADE_DATE) -> datetime:
    """本地挂钟时刻，转成 UTC——构建器读的是 Asia/Shanghai 的墙上时间。"""

    return datetime.combine(day, time(hour, minute, second), tzinfo=SHANGHAI).astimezone(UTC)


#: 新默认窗的起点，也是本文件里「正常的第一次尝试」
CAPTURE_AT = at(9, 31)


class _Adapter:
    def __init__(self, responses: list[pd.DataFrame | BaseException] | None = None) -> None:
        self.calls: list[date] = []
        self._responses = list(responses or [_frame()])

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        response = self._responses.pop(0) if self._responses else _frame()
        if isinstance(response, BaseException):
            raise response
        return response


class _Clock:
    """一个可以拨的时钟：构建器每轮读一次，配额调度器还会再读几次。"""

    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def _frame(*, drop: str | None = None) -> pd.DataFrame:
    frame = pd.DataFrame(
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
    if drop is not None:
        frame = frame.drop(columns=[drop])
    return frame


def _empty_frame() -> pd.DataFrame:
    """适配器在「返回空」时给出的形状：零行，八列俱全。"""

    from rquant.adapter.tushare import _empty_stk_auction_frame

    return _empty_stk_auction_frame()


def _write_authorities(
    tmp_path: Path,
    *,
    open_date: bool = True,
    universe: bool = True,
) -> tuple[Path, Path, MarketCalendarAuthority]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=TRADE_DATE,
        coverage_end=NEXT_TRADE_DATE,
        open_dates=(TRADE_DATE, NEXT_TRADE_DATE) if open_date else (),
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
    universe_path = tmp_path / "universe.json"
    if universe:
        authority = AuctionUniverseAuthority.create(
            effective_trade_date=TRADE_DATE,
            reference_trade_date=date(2026, 7, 30),
            available_at=datetime(2026, 7, 30, 9, 0, tzinfo=UTC),
            producer_commit=COMMIT,
            source_snapshot_id="b" * 64,
            codes=("600000.SH", "000001.SZ"),
        )
        universe_path.write_bytes(authority.canonical_json_bytes())
        universe_path.chmod(0o600)
    return calendar_path, universe_path, calendar


def _manifest(
    tmp_path: Path,
    *,
    open_date: bool = True,
    universe: bool = True,
    **overrides: object,
) -> RuntimeServiceManifest:
    calendar_path, universe_path, calendar = _write_authorities(
        tmp_path,
        open_date=open_date,
        universe=universe,
    )
    settings: dict[str, object] = {
        "spool_root": str(tmp_path / "auction-match"),
        "quota_path": str(tmp_path / "auction-match" / "quota.sqlite3"),
        "quota_units_per_window": 500,
        "producer_version": "auction-match-v1",
        "calendar_path": str(calendar_path),
        "calendar_expected_commit": calendar.producer_commit,
        "calendar_content_sha256": calendar.content_sha256,
        "universe_path": str(universe_path),
        "max_attempts": 3,
    }
    settings.update(overrides)
    return RuntimeServiceManifest(
        service_id="source.auction-match",
        service_kind=RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=10,
        stale_after_seconds=300,
        producer_commit=COMMIT,
        settings=settings,
    )


def _records(tmp_path: Path) -> list:
    return LiveBatchSpool(tmp_path / "auction-match").list_after(
        LiveChannel.AUCTION_MATCH,
        sequence=-1,
    )


# ---------------------------------------------------------------------------------------
# 采集窗：默认值、可配置、摊开的重试
# ---------------------------------------------------------------------------------------


def test_the_interim_default_window_is_the_one_the_probe_will_replace() -> None:
    """09:26 是错的起点——当天数据那时还没出。临时默认写成 09:31-09:45。"""

    assert time(9, 31) == AUCTION_MATCH_DEFAULT_CAPTURE_START
    assert time(9, 45) == AUCTION_MATCH_DEFAULT_CAPTURE_END


def test_the_window_comes_from_settings_and_defaults_when_absent(tmp_path: Path) -> None:
    """冻结的 manifest 里没有这两项，所以默认值必须顶得上；给了就按给的走。"""

    absent = AuctionMatchSourceSettings.model_validate(
        dict(_manifest(tmp_path / "absent").settings)
    )
    explicit = AuctionMatchSourceSettings.model_validate(
        dict(
            _manifest(
                tmp_path / "explicit",
                capture_start="09:33:00",
                capture_end="09:41:00",
                retry_interval_seconds=120,
            ).settings
        )
    )

    assert absent.capture_start == AUCTION_MATCH_DEFAULT_CAPTURE_START
    assert absent.capture_end == AUCTION_MATCH_DEFAULT_CAPTURE_END
    #: 14 分钟摊给三次尝试，除的是 3 不是 2：09:31:00 / 09:35:40 / 09:40:20，
    #: 最后一次到期之后离窗口右界还有整整 280 秒
    assert absent.capture_retry_interval_seconds == 280
    assert explicit.capture_start == time(9, 33)
    assert explicit.capture_end == time(9, 41)
    assert explicit.capture_retry_interval_seconds == 120


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"capture_start": "09:45:00", "capture_end": "09:31:00"}, "precede"),
        ({"capture_start": "09:20:00"}, "09:26"),
    ],
)
def test_an_impossible_window_is_refused(
    tmp_path: Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        AuctionMatchSourceSettings.model_validate(
            dict(_manifest(tmp_path, **overrides).settings)
        )


def test_the_three_attempts_are_spread_across_the_window(tmp_path: Path) -> None:
    """三次重试不再挤在七秒里：09:31:00 / 09:35:40 / 09:40:20。

    #277 的现场就是三次全落在 09:26:01-09:26:08——等于只在同一秒问了一次，
    数据晚到一分钟就永远看不到。间隔是 `窗宽 // max_attempts`，所以第三次到期之后离
    窗口右界 09:45 还留着 280 秒（复核 MF-1）。
    """

    adapter = _Adapter([RuntimeError("down"), RuntimeError("down"), RuntimeError("down")])
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path))

    step()
    assert len(adapter.calls) == 1
    clock.now = at(9, 33)
    step()
    assert len(adapter.calls) == 1, "窗内但还没到下一次的时刻，不该再问"
    clock.now = at(9, 35, 40)
    step()
    assert len(adapter.calls) == 2
    clock.now = at(9, 40, 19)
    step()
    assert len(adapter.calls) == 2
    clock.now = at(9, 40, 20)
    step()
    assert len(adapter.calls) == 3
    clock.now = at(9, 44)
    step()
    assert len(adapter.calls) == 3, "次数用尽后不再问"

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
    assert {attempt.attempt_id for attempt in attempts} == expected_attempt_ids
    assert {attempt.outcome for attempt in attempts} == {SourceQuotaAttemptOutcome.FAILURE}


def test_every_polling_phase_gets_the_full_attempt_budget(tmp_path: Path) -> None:
    """复核 MF-1 的正面：**任何轮询相位**都拿得到 `max_attempts` 次尝试。

    改动前间隔按 `max_attempts - 1` 推，第三次的到期时刻正好等于 `capture_end`，而窗口闸门是
    「过了右界就整轮空转」，于是第三次只在右界那一整秒内可达。复核者在 HEAD 上实测：

        2s tick, calls by start-phase second: {0: 3, 1: 2, 2: 3, 3: 2, 4: 3, 5: 2}
        5s tick, calls by start-phase second: {0: 3, 1: 2, 2: 2, 3: 2, 4: 2}

    配置写着 3 次、实际常常只发 2 次，而被吃掉的恰恰是最晚那一次——正是为「数据晚到」准备的
    那一次。这条用例用真的构建器、真的配额台账，从几个不同相位按固定步长 tick 过整个窗，
    断言每个相位都恰好发出 `max_attempts` 次请求。
    """

    counts: dict[tuple[int, int], int] = {}
    for tick_seconds in (2, 5):
        for phase in range(tick_seconds):
            adapter = _Adapter([_empty_frame() for _ in range(6)])
            clock = _Clock(at(9, 30, phase))
            step = auction_match_source_builder(
                adapter_factory=lambda adapter=adapter: adapter,
                clock=clock,
            )(_manifest(tmp_path / f"tick{tick_seconds}-phase{phase}"))
            moment = at(9, 30, phase)
            deadline = at(9, 46)
            while moment <= deadline:
                clock.now = moment
                step()
                moment += timedelta(seconds=tick_seconds)
            counts[(tick_seconds, phase)] = len(adapter.calls)

    assert set(counts.values()) == {3}, counts



def test_nothing_is_fetched_before_the_window_or_on_a_closed_date(tmp_path: Path) -> None:
    before = _Adapter()
    before_step = auction_match_source_builder(
        adapter_factory=lambda: before,
        clock=lambda: at(9, 30, 59),
    )(_manifest(tmp_path / "before"))

    after = _Adapter()
    after_step = auction_match_source_builder(
        adapter_factory=lambda: after,
        clock=lambda: at(9, 45, 1),
    )(_manifest(tmp_path / "after"))

    closed = _Adapter()
    closed_step = auction_match_source_builder(
        adapter_factory=lambda: closed,
        clock=lambda: CAPTURE_AT,
    )(_manifest(tmp_path / "closed", open_date=False))

    assert before_step().processed_count == 0
    assert after_step().processed_count == 0
    assert closed_step().processed_count == 0
    assert before.calls == []
    assert after.calls == []
    assert closed.calls == []


def test_a_configured_window_moves_the_whole_schedule(tmp_path: Path) -> None:
    """探测拿到真值之后要改的就是这两项，改了立刻生效，不用动代码。"""

    adapter = _Adapter([RuntimeError("down")])
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path, capture_start="09:36:00", capture_end="09:50:00"))

    step()
    assert adapter.calls == []
    clock.now = at(9, 36)
    step()
    assert len(adapter.calls) == 1


# ---------------------------------------------------------------------------------------
# 采集本身
# ---------------------------------------------------------------------------------------


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
    assert first.degraded_reasons == ()
    assert second.degraded_reasons == ()
    assert first.source_generations[LiveChannel.AUCTION_MATCH.value]
    records = _records(tmp_path)
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.PUBLISHED
    assert records[0].envelope.row_count == 2


def test_an_empty_response_then_a_real_one_publishes_degraded_then_published(
    tmp_path: Path,
) -> None:
    """09-21 的形状加上 09-22 希望看到的结局：先空后有，两个批次都留了下来。"""

    adapter = _Adapter([_empty_frame(), _frame()])
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path))

    first = step()
    clock.now = at(9, 38)
    second = step()

    assert adapter.calls == [TRADE_DATE, TRADE_DATE]
    records = _records(tmp_path)
    assert [record.envelope.quality_status for record in records] == [
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.PUBLISHED,
    ]
    assert records[0].envelope.degraded_reasons[0] == "empty_source_result"
    assert any("empty_source_result" in reason for reason in first.degraded_reasons)
    assert second.degraded_reasons == ()
    assert second.processed_count == 1


def test_a_response_without_pre_close_is_a_visible_validation_failure(tmp_path: Path) -> None:
    """接口哪天少给一列，心跳上看得见 `validation_failed:`，而不是一次静默的抛出。"""

    adapter = _Adapter([_frame(drop="pre_close")])
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: CAPTURE_AT,
    )(_manifest(tmp_path))

    result = step()

    reasons = result.degraded_reasons
    assert any(reason.startswith("auction_match:degraded:validation_failed:") for reason in reasons)
    assert any("pre_close" in reason for reason in reasons)
    records = _records(tmp_path)
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.DEGRADED
    assert records[0].envelope.row_count == 0


# ---------------------------------------------------------------------------------------
# 今天没采到，就一直说没采到
# ---------------------------------------------------------------------------------------


def test_an_exhausted_day_keeps_capture_failed_until_the_trade_date_rolls_over(
    tmp_path: Path,
) -> None:
    """#277 第三个缺陷的正面：次数用尽之后，心跳一整天都带着 `capture_failed`。"""

    adapter = _Adapter([_empty_frame(), _empty_frame(), _empty_frame()])
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path))

    for moment in (at(9, 31), at(9, 38), at(9, 45)):
        clock.now = moment
        step()

    for moment in (at(9, 46), at(11, 30), at(15, 30), at(22, 0)):
        clock.now = moment
        result = step()
        assert "capture_failed" in result.degraded_reasons, moment
        assert result.processed_count == 0

    #: 下一个交易日重新开始：旗子落下（取窗之前的 08:00，这一轮不该再发请求）
    clock.now = at(8, 0, day=NEXT_TRADE_DATE)
    rolled = step()
    assert "capture_failed" not in rolled.degraded_reasons
    assert len(adapter.calls) == 3


def test_a_window_that_passes_with_attempts_made_is_also_capture_failed(tmp_path: Path) -> None:
    """次数没用尽但窗过去了，同样是「今天没采到」。"""

    adapter = _Adapter([_empty_frame()])
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path))

    step()
    clock.now = at(9, 46)
    result = step()

    assert len(adapter.calls) == 1
    assert "capture_failed" in result.degraded_reasons


def test_a_published_batch_never_leaves_capture_failed_behind(tmp_path: Path) -> None:
    """成了就是成了：发过一个 PUBLISHED 批次之后，一天都不会冒出 `capture_failed`。"""

    adapter = _Adapter([_frame()])
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path))

    step()
    for moment in (at(9, 45), at(9, 46), at(15, 0)):
        clock.now = moment
        assert "capture_failed" not in step().degraded_reasons


def test_a_missing_universe_authority_never_consumes_an_attempt(tmp_path: Path) -> None:
    """竞价全集读不出来时**每一轮都再试**，不烧尝试次数（复核 SF-2）。

    改动前计数放在读权威之前，于是「全集晚发了十分钟」会直接报销掉当天仅有的三次预算。
    「今天一次都没发出去」这件事由 `capture_missed` 留痕（复核 SF-3），不必靠烧掉次数来换。
    """

    adapter = _Adapter()
    clock = _Clock(at(9, 31))
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=clock,
    )(_manifest(tmp_path, universe=False))

    #: 窗内每一轮都抛，抛多少轮都不消耗次数
    for moment in (at(9, 31), at(9, 32), at(9, 33), at(9, 40), at(9, 44)):
        clock.now = moment
        with pytest.raises(Exception):  # noqa: B017 - 具体类型由权威加载器决定
            step()

    clock.now = at(9, 46)
    result = step()

    assert adapter.calls == []
    #: 一次请求都没发出去，所以是「没采成」不是「试过都没成」
    assert "capture_missed" in result.degraded_reasons
    assert "capture_failed" not in result.degraded_reasons


def test_a_window_that_passes_without_any_attempt_is_capture_missed(tmp_path: Path) -> None:
    """role 整个窗口都没起来（宕机 / 部署 / watchdog），心跳也必须说得出来（复核 SF-3）。"""

    adapter = _Adapter()
    step = auction_match_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: at(10, 0),
    )(_manifest(tmp_path))

    result = step()

    assert adapter.calls == []
    assert "capture_missed" in result.degraded_reasons


def test_an_idle_day_reports_nothing_at_all(tmp_path: Path) -> None:
    """非交易日不该凭空长出降级理由。"""

    clock = _Clock(at(9, 31) + timedelta(days=1))
    step = auction_match_source_builder(
        adapter_factory=_Adapter,
        clock=clock,
    )(_manifest(tmp_path, open_date=False))

    result = step()

    assert result.degraded_reasons == ()
    assert result.processed_count == 0
