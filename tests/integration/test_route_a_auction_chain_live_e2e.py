"""#277 / #278 acceptance: the opening-auction chain actually produces something.

2026-09-21 是 Route A 的验收日，v0.33.14、20 个 role 常驻。竞价链那一段是这样过去的：

* `auction-universe.publisher.v1` 09-20 10:18 就把当日全集发好了，`effective_trade_date`
  是 09-21，没有问题；
* `auction-match.source.v1` 在 09:26:01 / 09:26:04 / 09:26:08 三次请求 `stk_auction`，
  三次都「返回空」，随后**零批次、零报错**，09:50 的心跳是
  `running / last_error None / processed_count 0 / output_sequence -1`；
* 下游 `candidate.auction_gap.v1` 整天装不出东西，`watchlist-quote` 与 `market-minute`
  整天报 `auction_gap@1: required authority has no not_visible snapshot`。

协调者当天 15:10 用同一个生产主 token 只读探测：`stk_auction(20260921)` 已经有 6,073 行，
列里就有 `pre_close`；备用 token 的回答是「抱歉，您没有接口(stk_auction)访问权限」。
所以 09:26 不是接口坏了，是**当天的数据那时还没就绪**，而三个缺陷让这件事既没被重试到，
也没在心跳上留下痕迹（#277）。同一天还暴露出第二件事：n_shape / growth_board 的候选文档是
装机时封死的一份，`trade_date` 停在 2026-07-14，盘中 loader 要的是当日（#278）。

这个文件把那一天放进真实世界里跑：两代真安装、真 stage 与发布的权威链、wrapper 自己派生的
argv 与子环境、一份真的五分钟只读副本、一本开着这一场与它前面五场的市场日历，时钟落在
本场交易日上。夹具只替换 Tushare 的报文——空、缺列、含 `pre_close` 的正常报文、备用 token
的权限错误——其余每一层都是生产那一层。
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from threading import Event
from typing import Any
from unittest import mock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_deployment_bundle import acknowledge_runtime_schema_rollout_preparation
from rquant.runtime_service_control import RuntimeServiceControl
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshotSpool
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    _production_bundle,
)
from tests.integration.test_route_a_readside_replica_e2e import (
    CODE,
    OPEN_DATES,
    PREVIOUS_COMMIT,
    PRIOR_DATES,
    TRADE_DATE,
    ReplicaWorld,
    _instance_name,
    _publish_reference_generation,
    _seal_candidate_documents,
    _write_replica,
)
from tests.unit.test_runtime_authority_publish import World

pytestmark = pytest.mark.integration

_SHANGHAI = ZoneInfo("Asia/Shanghai")

AUCTION_MATCH_SERVICE_ID = "auction-match.source.v1"
AUCTION_UNIVERSE_SERVICE_ID = "auction-universe.publisher.v1"
AUCTION_GAP_SERVICE_ID = "candidate.auction_gap.v1"
N_SHAPE_SERVICE_ID = "candidate.n_shape.v1"
GROWTH_BOARD_SERVICE_ID = "candidate.growth_board_surge.v1"

AUCTION_MATCH_ROLE = "auction_match_source"
AUCTION_UNIVERSE_ROLE = "auction_universe_publisher"
CANDIDATE_ROLE = "candidate_publisher"
MINUTE_ROLE = "market_minute_source"
QUOTE_ROLE = "watchlist_quote_source"


def at(hour: int, minute: int, second: int = 0, *, day: date = TRADE_DATE) -> datetime:
    return datetime.combine(day, time(hour, minute, second), tzinfo=_SHANGHAI).astimezone(UTC)


#: 副本在开盘前最后一次被换掉的时刻。两个候选发布者与竞价全集发布者都拒绝未来的证据，
#: 所以它必须早于本文件里最早的那一次运行（08:45）。
REPLICA_SYNCED_AT = at(8, 40)
#: `session_document` 的起点，也是 #278 要求的「每个交易日 09:15 之前重建一次」
SESSION_DOCUMENT_AT = at(8, 45)
#: 竞价全集：09:15 的保护窗之前
UNIVERSE_AT = at(9, 0)
#: 采集窗 09:29-09:44 的三次尝试。间隔 = 窗宽 // max_attempts = 900 // 3 = 300 秒，
#: 到期时刻是 09:29 / 09:34 / 09:39；这里**故意取到期之后的偏相位时刻**（+1:37 / +1:29 /
#: +1:07），不再踩在到期秒或窗口右界上——复核 MF-1 指出的相位缺陷正是被「所有时刻都恰好
#: 踩在边界上」的测试放过去的。窗由 09-23 探测到的首次可用时刻 T = 09:27:14 定
#: （09:26:54 零行、09:27:14 六千余行）。
CAPTURE_FIRST = at(9, 30, 37)
CAPTURE_SECOND = at(9, 35, 29)
CAPTURE_THIRD = at(9, 40, 7)
#: 采集窗里的三轮，一个进程按顺序跑完就是「今天的次数用尽了」
CAPTURE_MOMENTS = (
    CAPTURE_FIRST,
    CAPTURE_SECOND,
    CAPTURE_THIRD,
)
#: 窗过去之后的任意一轮，以及当天更晚的时刻
AFTER_WINDOW = at(9, 45)
AFTER_CLOSE = at(15, 30)
#: 下一个交易日的盘前：交易日切换之后旗子应当落下
NEXT_SESSION = at(8, 30, day=OPEN_DATES[-1] + timedelta(days=1))
#: 装配窗（09:29-09:49）里的一轮，排在第一次成功采集（09:30:37）之后
ASSEMBLE_AT = at(9, 31, 11)
#: 两个源真正去读候选全集的时刻：早盘阶段，且晚于上面那一次装配
CONSUME_AT = at(9, 40)
#: 第二次安装打开 schema rollout 窗口的时刻。窗宽是生产画像的
#: `schema_rollout_stage_timeout_seconds` = 600 s，而**记一条 dual-write 的是两个消费者**
#: （`market-minute` / `watchlist-quote`，`runtime.strategy_candidate.snapshot` 这条
#: channel 的消费方），它们跑在 `CONSUME_AT`。所以窗开在 `CONSUME_AT` 前五分钟：
#: 早于窗的那些轮次（08:45 的候选文档、09:00 的竞价全集）不记 dual-write，不受影响。
SCHEMA_ROLLOUT_STARTED_AT = at(9, 35)


# ---------------------------------------------------------------------------------------
# 夹具报文
# ---------------------------------------------------------------------------------------


def auction_frame(*, drop: str | None = None) -> pd.DataFrame:
    """`stk_auction` 15:10 真实返回的那种一行，列与生产逐字相同。"""

    frame = pd.DataFrame(
        [
            {
                "ts_code": CODE,
                "trade_date": TRADE_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
        ]
    )
    if drop is not None:
        frame = frame.drop(columns=[drop])
    return frame


def auction_frame_with_unmatched_rows() -> pd.DataFrame:
    """2026-09-23 生产报文的形状：正常行里混着几行「今天没有集合竞价成交」。

    当天 `stk_auction(20260923)` 的 6,077 行里有 407 行 `price` 是 NaN，同一行的 `vol` 与
    `amount` 都是 0、`pre_close` 有数（例：`600289.SH pre_close=4.42`）。整张表因此被判
    `validation_failed:required numeric values must be finite`，批次发成零行 DEGRADED，
    `candidate.auction_gap` 一整天读不到 PUBLISHED 批次（③a 当天没过）。
    这两行的代码都不在竞价全集里——全集是「昨天有日线的代码」。
    """

    return pd.concat(
        [
            auction_frame(),
            pd.DataFrame(
                [
                    {
                        "ts_code": ts_code,
                        "trade_date": TRADE_DATE,
                        "price": float("nan"),
                        "vol": 0.0,
                        "amount": 0.0,
                        "pre_close": pre_close,
                        "turnover_rate": 0.0,
                        "volume_ratio": float("nan"),
                    }
                    for ts_code, pre_close in (("600289.SH", 4.42), ("000004.SZ", 12.8))
                ]
            ),
        ],
        ignore_index=True,
    )


def empty_auction_frame() -> pd.DataFrame:
    """09:26 三次「返回空」时适配器现在交出的形状：零行，八列俱全。"""

    from rquant.adapter.tushare import _empty_stk_auction_frame

    return _empty_stk_auction_frame()


BACKUP_TOKEN_REFUSAL = "抱歉，您没有接口(stk_auction)访问权限"


class _AuctionAdapter:
    """auction-match 的 Tushare 适配器，只把网络换成一串预置报文。"""

    def __init__(self, responses: list[pd.DataFrame | BaseException]) -> None:
        self.calls: list[date] = []
        self._responses = list(responses)

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        response = self._responses.pop(0) if self._responses else empty_auction_frame()
        if isinstance(response, BaseException):
            raise response
        return response


class _MinuteAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame:
        self.calls.append(tuple(codes))
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "trade_time": f"{TRADE_DATE.isoformat()} 09:30:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "vol": 1_000.0,
                    "amount": 10_100.0,
                }
                for code in codes
            ]
        )


class _QuoteProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def __call__(
        self,
        codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: Any,
    ) -> pd.DataFrame:
        self.calls.append(tuple(codes))
        on_started(CONSUME_AT)
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "observed_at": CONSUME_AT,
                    "price": 10.1,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "volume": 1_000.0,
                    "amount": 10_100.0,
                }
                for code in codes
            ]
        )


# ---------------------------------------------------------------------------------------
# 世界
# ---------------------------------------------------------------------------------------


@pytest.fixture
def auction_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ReplicaWorld:
    """两代真安装、真发布的权威链，时钟落在本场交易日的盘前。

    与 `test_route_a_readside_replica_e2e` 的世界同源，只把副本的同步时刻提早到 08:40：
    本文件最早的一次运行是 08:45 的 `session_document` 发布者，而每个读副本的角色都拒绝
    未来的证据。
    """

    world = World(tmp_path / "root", monkeypatch).build()
    runtime_root = tmp_path / "host" / "data" / "runtime"
    _production_bundle(
        tmp_path / "previous",
        monkeypatch,
        producer_commit=PREVIOUS_COMMIT,
        runtime_root=runtime_root,
        schema_bootstrap_reason="#277 acceptance bootstrap",
        market_calendar_open_dates=OPEN_DATES,
    )
    inputs, profile, receipt, sealed = _production_bundle(
        tmp_path / "target",
        monkeypatch,
        producer_commit=world.commit,
        runtime_root=runtime_root,
        schema_bootstrap_reason=None,
        definition_registry_root=runtime_root.parent / f"definitions-{world.commit[:7]}",
        market_calendar_open_dates=OPEN_DATES,
        schema_rollout_started_at=SCHEMA_ROLLOUT_STARTED_AT,
    )
    assert receipt.previous_generation_hash is not None

    route = ReplicaWorld(world, inputs.runtime_root)
    route.inputs = inputs
    route.profile = profile
    route.receipt = receipt
    route.sealed_credentials = sealed
    acknowledge_runtime_schema_rollout_preparation(
        route.runtime_root,
        now=SCHEMA_ROLLOUT_STARTED_AT,
    )
    route.stage_and_publish()

    _write_replica(
        inputs.readonly_replica_database_path,
        trade_dates=PRIOR_DATES,
        synced_at=REPLICA_SYNCED_AT,
    )
    _publish_reference_generation(
        route.setting(AUCTION_GAP_SERVICE_ID, "reference_registry_path")
    )
    #: 生产不再从这两份文档发布（#278），但 inputs 文档点了它们的名，装机校验要它们在场
    _seal_candidate_documents(inputs, producer_commit=world.commit)
    return route


class _StopAfterTheseMoments(Event):
    """让**同一个进程**按一串给定的时刻各跑一轮，中间不真的睡。

    这不是方便，是必须的：`auction-match` 的重试序号、当天的 `capture_failed` 旗子、
    `session_document` 的「今天发过了」记号，三样都活在 `build()` 建出来的闭包里，也就是
    活在一个 role **进程**里。每个时刻各起一次进程，序号会从 0 重来（第二次尝试于是撞上
    配额库里同一条 attempt 而被拒），旗子和记号也永远攒不起来——那是「把 role 重启一遍」，
    不是「role 跑了两轮」。循环自己的 `wait()` 是推进时钟的地方。
    """

    def __init__(self, moments: Sequence[datetime], clock: dict[str, datetime]) -> None:
        super().__init__()
        self.moments = list(moments)
        assert self.moments
        self.iterations = 0
        self._clock = clock
        clock["now"] = self.moments[0]

    def is_set(self) -> bool:
        return super().is_set() or self.iterations >= len(self.moments)

    def wait(self, timeout: float | None = None) -> bool:  # noqa: ARG002 - 不睡是要点
        self.iterations += 1
        if self.iterations < len(self.moments):
            self._clock["now"] = self.moments[self.iterations]
        else:
            self.set()
        return True


def run_role(
    route: ReplicaWorld,
    role: str,
    *,
    instance: str,
    now: datetime | None = None,
    moments: Sequence[datetime] | None = None,
    **extra: Any,
) -> Any:
    """一个 role 进程，按 `moments` 各跑一轮，用 wrapper 自己派生的 argv。

    `ReplicaWorld.run` 只认 `adapter_factory` 与 `watchlist_quote_provider_factory` 两个注入点，
    而 auction-match 的那一个叫 `auction_adapter_factory`（`build_builtin_registry` 给三类源
    各留了一个），所以这里把注入点做成透传；同时把「一轮」放开成「几轮」，理由见
    `_StopAfterTheseMoments`。
    """

    schedule = list(moments) if moments is not None else [now]
    assert schedule and schedule[0] is not None
    resolved = route.world.resolve(role, instance)
    argv = list(resolved["module_argv"])
    index = argv.index("--control-root") + 1
    argv[index] = str(route.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    arguments = service_main.build_parser().parse_args(argv)
    clock: dict[str, datetime] = {}
    stop = _StopAfterTheseMoments(schedule, clock)
    real_event = service_main.Event
    real_registry = builtin_module.build_builtin_registry
    service_main.Event = lambda: stop  # type: ignore[assignment]
    builtin_module.build_builtin_registry = (  # type: ignore[assignment]
        lambda **kwargs: real_registry(clock=lambda: clock["now"], **{**extra, **kwargs})
    )
    try:
        with mock.patch.dict(os.environ, dict(resolved["environment"]), clear=True):
            code = service_main.run(arguments)
    finally:
        service_main.Event = real_event  # type: ignore[assignment]
        builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]
    assert stop.iterations == len(schedule), f"{role} ran {stop.iterations} of {len(schedule)}"
    control_root = Path(argv[argv.index("--control-root") + 1])
    manifest = next(
        item for item in route.profile.manifests if _instance_name(item.service_id) == instance
    )
    heartbeat = RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)
    return code, heartbeat


def auction_spool(route: ReplicaWorld) -> LiveBatchSpool:
    return LiveBatchSpool(route.setting(AUCTION_GAP_SERVICE_ID, "auction_spool_root"))


def auction_records(route: ReplicaWorld) -> list:
    return auction_spool(route).list_after(LiveChannel.AUCTION_MATCH, sequence=-1)


def run_auction_match(
    route: ReplicaWorld,
    adapter: _AuctionAdapter,
    *,
    now: datetime | None = None,
    moments: Sequence[datetime] | None = None,
) -> Any:
    code, heartbeat = run_role(
        route,
        AUCTION_MATCH_ROLE,
        instance=_instance_name(AUCTION_MATCH_SERVICE_ID),
        now=now,
        moments=moments,
        auction_adapter_factory=lambda: adapter,
    )
    assert code == 0
    assert heartbeat is not None
    return heartbeat


def publish_auction_universe(route: ReplicaWorld) -> None:
    """竞价全集，由它自己的 role 在 09:15 保护窗之前发布。"""

    code, heartbeat = route.run(
        AUCTION_UNIVERSE_ROLE,
        instance=_instance_name(AUCTION_UNIVERSE_SERVICE_ID),
        now=UNIVERSE_AT,
    )
    assert code == 0
    assert heartbeat is not None and heartbeat.last_error is None
    current = route.setting(AUCTION_UNIVERSE_SERVICE_ID, "authority_root") / "current.json"
    assert current.is_file()


def publish_session_documents(route: ReplicaWorld) -> list:
    """#278：两个 document-driven 发布者在 09:15 之前重建当日文档。"""

    heartbeats = []
    for service_id in (N_SHAPE_SERVICE_ID, GROWTH_BOARD_SERVICE_ID):
        code, heartbeat = route.run(
            CANDIDATE_ROLE,
            instance=_instance_name(service_id),
            now=SESSION_DOCUMENT_AT,
        )
        assert code == 0, service_id
        assert heartbeat is not None and heartbeat.last_error is None, service_id
        assert heartbeat.degraded_reasons == (), service_id
        heartbeats.append(heartbeat)
    return heartbeats


def assemble_auction_gap(route: ReplicaWorld, *, now: datetime = ASSEMBLE_AT) -> Any:
    code, heartbeat = route.run(
        CANDIDATE_ROLE,
        instance=_instance_name(AUCTION_GAP_SERVICE_ID),
        now=now,
    )
    assert code == 0
    assert heartbeat is not None
    return heartbeat


# ---------------------------------------------------------------------------------------
# (a) 窗内先空后有
# ---------------------------------------------------------------------------------------


def test_an_empty_source_then_a_real_one_leaves_a_degraded_batch_then_a_published_one(
    auction_world: ReplicaWorld,
) -> None:
    """2026-09-21 的形状加上它应有的结局。

    改动前这两轮的结果都是「异常穿出去、零批次」：空表被 `normalize_frame` 当成缺八列，
    含 `pre_close` 的正常报文被当成缺 `pre_close`（适配器从来没要过这一列）。
    """

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([empty_auction_frame(), auction_frame()])

    #: 同一个进程的两轮，是三次尝试摊在窗里的头两次（取的是到期之后的偏相位时刻）
    second = run_auction_match(
        auction_world,
        adapter,
        moments=[CAPTURE_FIRST, CAPTURE_SECOND],
    )

    assert adapter.calls == [TRADE_DATE, TRADE_DATE]
    records = auction_records(auction_world)
    assert [record.envelope.quality_status for record in records] == [
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.PUBLISHED,
    ]
    assert records[0].envelope.degraded_reasons[0] == "empty_source_result"
    assert records[0].envelope.row_count == 0
    assert records[1].envelope.row_count == 1
    assert second.degraded_reasons == ()
    assert second.processed_count == 1
    assert second.last_error is None
    #: 下游读的是 `current` 指针，它现在指着那个真正发布出来的批次
    current = auction_spool(auction_world).current(LiveChannel.AUCTION_MATCH)
    assert current is not None
    assert current.quality_status is BatchQualityStatus.PUBLISHED


# ---------------------------------------------------------------------------------------
# (b) 缺 pre_close
# ---------------------------------------------------------------------------------------


def test_a_response_without_pre_close_is_a_degraded_batch_the_heartbeat_names(
    auction_world: ReplicaWorld,
) -> None:
    """接口少给一列时，心跳上看得见 `validation_failed:`，而不是一次静默的抛出。"""

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([auction_frame(drop="pre_close")])

    heartbeat = run_auction_match(auction_world, adapter, now=CAPTURE_FIRST)

    reasons = heartbeat.degraded_reasons
    assert any(
        reason.startswith("auction_match:degraded:validation_failed:") for reason in reasons
    ), reasons
    assert any("pre_close" in reason for reason in reasons), reasons
    records = auction_records(auction_world)
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.DEGRADED
    assert records[0].envelope.row_count == 0


# ---------------------------------------------------------------------------------------
# (c) 尝试耗尽
# ---------------------------------------------------------------------------------------


def test_an_exhausted_day_keeps_capture_failed_in_the_heartbeat_until_the_next_day(
    auction_world: ReplicaWorld,
) -> None:
    """#277 第三个缺陷：早退分支不许再把今天的失败洗成一条干净心跳。"""

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([empty_auction_frame() for _ in CAPTURE_MOMENTS])

    heartbeat = run_auction_match(
        auction_world,
        adapter,
        moments=[*CAPTURE_MOMENTS, AFTER_WINDOW, AFTER_CLOSE],
    )

    #: 三次尝试用完之后不再发请求
    assert adapter.calls == [TRADE_DATE] * len(CAPTURE_MOMENTS)
    #: 收盘之后那一轮的心跳仍然说得出「今天没采到」
    assert "capture_failed" in heartbeat.degraded_reasons


def test_a_window_the_role_slept_through_is_capture_missed(
    auction_world: ReplicaWorld,
) -> None:
    """复核 SF-3：role 整个窗口都没起来时，心跳也必须说得出来。

    宕机、部署、watchdog 重启都会造成这个形状。改动前 `capture_failed` 要求
    `attempts > 0`，于是「今天一次都没试过」反而留下一条干干净净的心跳——正是 #277
    最初的症状。现在这一种有它自己的理由 `capture_missed`。
    """

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([])

    heartbeat = run_auction_match(auction_world, adapter, now=AFTER_WINDOW)

    assert adapter.calls == []
    assert "capture_missed" in heartbeat.degraded_reasons
    assert "capture_failed" not in heartbeat.degraded_reasons
    #: 落盘的那一半同样是空的，两个证据一致（`list_after` 返回的是 tuple）
    assert not auction_records(auction_world)


def test_the_failure_flag_is_a_per_process_memory_that_a_restart_loses(
    auction_world: ReplicaWorld,
) -> None:
    """诚实记下来的边界：旗子活在进程里，重启之后今天的失败就不再被复述。

    构建器每次 `build()` 都从零开始，这一条在生产上意味着「auction-match 被重启之后，
    心跳不会再带 `capture_failed`」。落盘的证据仍然在——spool 里那几个 DEGRADED 批次和
    `quota.sqlite3` 里的尝试记录都在——但心跳这一路会失忆。做成持久化需要给这个 role 再
    加一份状态文件，那超出本包的边界（#277 只要求早退分支不洗掉当天的失败）。
    """

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([empty_auction_frame() for _ in CAPTURE_MOMENTS])
    exhausted = run_auction_match(
        auction_world,
        adapter,
        moments=[*CAPTURE_MOMENTS, AFTER_WINDOW],
    )
    assert "capture_failed" in exhausted.degraded_reasons

    #: 同一个进程跨到下一个交易日：旗子落下，这是设计
    rolled = run_auction_match(auction_world, adapter, now=NEXT_SESSION)

    assert "capture_failed" not in rolled.degraded_reasons
    #: 而**换一个进程**再看今天，旗子已经不在了——落盘的证据还在
    restarted = run_auction_match(auction_world, adapter, now=AFTER_CLOSE)
    assert "capture_failed" not in restarted.degraded_reasons
    assert len(auction_records(auction_world)) >= 1


# ---------------------------------------------------------------------------------------
# 反向：备用 token 没有权限，必须 fail closed 且看得见
# ---------------------------------------------------------------------------------------


def test_a_backup_token_permission_error_fails_closed_and_is_visible(
    auction_world: ReplicaWorld,
) -> None:
    """备用 token 无 `stk_auction` 权限（2026-09-21 实测），失败必须冒出来。

    适配器**不切备用 token**，网关把传输失败发成一个 STALE 的空批次，心跳里带
    `auction_match:stale:source_error:`——一行数据都没有被当成真的收下来。
    """

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([RuntimeError(BACKUP_TOKEN_REFUSAL)])

    heartbeat = run_auction_match(auction_world, adapter, now=CAPTURE_FIRST)

    reasons = heartbeat.degraded_reasons
    assert any(
        reason.startswith("auction_match:stale:source_error:") for reason in reasons
    ), reasons
    records = auction_records(auction_world)
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.STALE
    assert records[0].envelope.row_count == 0
    #: 而且没有任何东西被提升成 `current` 的已发布批次
    current = auction_spool(auction_world).current(LiveChannel.AUCTION_MATCH)
    assert current is None or current.quality_status is not BatchQualityStatus.PUBLISHED


# ---------------------------------------------------------------------------------------
# (d) 装配窗后移之后，auction_gap 装得出当日候选
# ---------------------------------------------------------------------------------------


def test_the_auction_gap_publisher_assembles_todays_candidates_in_the_moved_window(
    auction_world: ReplicaWorld,
) -> None:
    publish_auction_universe(auction_world)
    run_auction_match(auction_world, _AuctionAdapter([auction_frame()]), now=CAPTURE_FIRST)

    heartbeat = assemble_auction_gap(auction_world)

    assert heartbeat.last_error is None
    assert heartbeat.degraded_reasons == ()
    assert heartbeat.processed_count == 1
    snapshot_root = auction_world.setting(AUCTION_GAP_SERVICE_ID, "snapshot_root")
    settings = auction_world.manifest(AUCTION_GAP_SERVICE_ID).settings
    snapshot = StrategyCandidateSnapshotSpool(snapshot_root).read_strategy_as_of(
        CONSUME_AT,
        strategy_id="auction_gap",
        strategy_version="1",
        definition_fingerprint=str(settings["definition_fingerprint"]),
        executable_fingerprint=str(settings["executable_fingerprint"]),
        candidate_schema_fingerprint=str(settings["candidate_schema_fingerprint"]),
        static_feature_schema=settings["static_feature_schema"],
    )
    assert snapshot is not None
    assert snapshot.trade_date == TRADE_DATE
    assert [row.candidate_id for row in snapshot.rows] == [CODE]


def test_rows_without_an_auction_match_do_not_cost_the_day_its_candidates(
    auction_world: ReplicaWorld,
) -> None:
    """2026-09-23 的现场与它应有的结局：几行没有成交，当天的候选照样装得出来。

    当天 09:35 的第一次尝试真的拿到了 6,077 行（包 Y 的 `pre_close` 修复是有效的），却
    因为其中 407 行必填数值是 NaN 而被整批拒，`candidate.auction_gap` 于是报
    `auction_gap_input_unavailable`，watchlist / market-minute 一整天降级。现在这些行被丢掉，
    批次照常 PUBLISHED，丢了几行由心跳说出来，候选链往下走。
    """

    publish_auction_universe(auction_world)
    adapter = _AuctionAdapter([auction_frame_with_unmatched_rows()])

    capture_heartbeat = run_auction_match(auction_world, adapter, now=CAPTURE_FIRST)

    assert capture_heartbeat.last_error is None
    assert capture_heartbeat.degraded_reasons == ("auction_match:rows_dropped_non_finite:2",)
    records = auction_records(auction_world)
    assert len(records) == 1
    assert records[0].envelope.quality_status is BatchQualityStatus.PUBLISHED
    assert records[0].envelope.row_count == 1
    assert records[0].envelope.degraded_reasons == ()

    heartbeat = assemble_auction_gap(auction_world)

    assert heartbeat.last_error is None
    assert heartbeat.degraded_reasons == ()
    assert heartbeat.processed_count == 1
    snapshot_root = auction_world.setting(AUCTION_GAP_SERVICE_ID, "snapshot_root")
    settings = auction_world.manifest(AUCTION_GAP_SERVICE_ID).settings
    snapshot = StrategyCandidateSnapshotSpool(snapshot_root).read_strategy_as_of(
        CONSUME_AT,
        strategy_id="auction_gap",
        strategy_version="1",
        definition_fingerprint=str(settings["definition_fingerprint"]),
        executable_fingerprint=str(settings["executable_fingerprint"]),
        candidate_schema_fingerprint=str(settings["candidate_schema_fingerprint"]),
        static_feature_schema=settings["static_feature_schema"],
    )
    assert snapshot is not None
    assert [row.candidate_id for row in snapshot.rows] == [CODE]


def test_the_old_window_would_have_had_nothing_to_assemble(
    auction_world: ReplicaWorld,
) -> None:
    """反面：装配窗起点之前没有任何可装的料，这正是窗必须跟着采集窗一起挪的理由。

    09:28 落在老窗 09:26-09:30 里，却在新的装配窗 09:29-09:49 之外——采集窗最早 09:29
    才发第一次请求，09:28 这一轮手上什么批次都没有。
    """

    publish_auction_universe(auction_world)

    heartbeat = assemble_auction_gap(auction_world, now=at(9, 28))

    assert heartbeat.processed_count == 0
    assert heartbeat.output_sequence == -1
    #: 窗外的一轮什么都不做，也不把自己标成降级
    assert heartbeat.degraded_reasons == ()


# ---------------------------------------------------------------------------------------
# (e) 三个候选权威齐备之后，两个源接受它们
# ---------------------------------------------------------------------------------------


def test_the_rebuilt_documents_and_the_auction_gap_snapshot_are_accepted_by_both_sources(
    auction_world: ReplicaWorld,
) -> None:
    """#278 的正面，也是上线标准 ③ 的那一步：盘中链条真的有料往下走。

    三个候选权威都是 `required: True`：只要 n_shape / growth_board 还停在 2026-07-14，
    这两个源一整天都是 `snapshot trade date does not match required trade date`。
    """

    publish_session_documents(auction_world)
    publish_auction_universe(auction_world)
    run_auction_match(auction_world, _AuctionAdapter([auction_frame()]), now=CAPTURE_FIRST)
    assemble_auction_gap(auction_world)

    minute_adapter = _MinuteAdapter()
    minute_code, minute = auction_world.run(
        MINUTE_ROLE,
        instance=_instance_name("market-minute.source.v1"),
        now=CONSUME_AT,
        adapter_factory=lambda: minute_adapter,
    )
    provider = _QuoteProvider()
    quote_code, quote = auction_world.run(
        QUOTE_ROLE,
        instance=_instance_name("watchlist-quote.source.v1"),
        now=CONSUME_AT,
        watchlist_quote_provider_factory=lambda: provider,
    )

    assert minute_code == 0
    assert minute is not None and minute.last_error is None
    assert minute.degraded_reasons == ()
    assert minute_adapter.calls == [(CODE,)]
    assert "candidate_universe" in minute.source_generations
    assert quote_code == 0
    assert quote is not None and quote.last_error is None
    assert quote.degraded_reasons == ()
    assert provider.calls == [(CODE,)]


def test_the_two_session_documents_carry_todays_date_and_the_previous_sessions_basis(
    auction_world: ReplicaWorld,
) -> None:
    """重建出来的文档长什么样：当日的 `trade_date`，上一场的 `basis_trade_date`。"""

    publish_session_documents(auction_world)

    for service_id, strategy_id in (
        (N_SHAPE_SERVICE_ID, "n_shape"),
        (GROWTH_BOARD_SERVICE_ID, "growth_board_surge"),
    ):
        settings = auction_world.manifest(service_id).settings
        assert settings["input_mode"] == "session_document"
        snapshot = StrategyCandidateSnapshotSpool(
            Path(str(settings["snapshot_root"]))
        ).read_strategy_as_of(
            CONSUME_AT,
            strategy_id=strategy_id,
            strategy_version="1",
            definition_fingerprint=str(settings["definition_fingerprint"]),
            executable_fingerprint=str(settings["executable_fingerprint"]),
            candidate_schema_fingerprint=str(settings["candidate_schema_fingerprint"]),
            static_feature_schema=settings["static_feature_schema"],
        )
        assert snapshot is not None, strategy_id
        assert snapshot.trade_date == TRADE_DATE, strategy_id
        assert snapshot.captured_at == SESSION_DOCUMENT_AT, strategy_id


def test_a_second_pass_on_the_same_session_writes_nothing(
    auction_world: ReplicaWorld,
) -> None:
    """包 W 的纪律：内容没变就不写。这里的「没变」是「今天这一份已经发过了」。"""

    settings = auction_world.manifest(N_SHAPE_SERVICE_ID).settings
    root = Path(str(settings["snapshot_root"]))

    #: 同一个进程的两轮：08:45 与 09:10
    code, heartbeat = run_role(
        auction_world,
        CANDIDATE_ROLE,
        instance=_instance_name(N_SHAPE_SERVICE_ID),
        moments=[SESSION_DOCUMENT_AT, at(9, 10)],
    )

    assert code == 0
    #: 第二轮什么都没写：这一轮没有产出，`processed_count` 是 0，而输出序号照抄第一轮的
    #: （心跳不接受回退的序号，见 `test_an_idle_round_after_a_publish_does_not_regress…`）
    assert heartbeat is not None
    assert heartbeat.processed_count == 0
    assert heartbeat.output_sequence == 0
    #: 而盘上那一份仍然是第一轮发的那一代
    snapshot = StrategyCandidateSnapshotSpool(root).read_strategy_as_of(
        at(9, 10),
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=str(settings["definition_fingerprint"]),
        executable_fingerprint=str(settings["executable_fingerprint"]),
        candidate_schema_fingerprint=str(settings["candidate_schema_fingerprint"]),
        static_feature_schema=settings["static_feature_schema"],
    )
    assert snapshot is not None
    assert snapshot.sequence == 0
    assert snapshot.captured_at == SESSION_DOCUMENT_AT


# ---------------------------------------------------------------------------------------
# 前提本身
# ---------------------------------------------------------------------------------------


def test_the_world_is_two_generations_over_a_session_the_calendar_opens(
    auction_world: ReplicaWorld,
) -> None:
    installed = sorted((auction_world.runtime_root / "generations").iterdir())
    assert len(installed) >= 2, installed
    assert auction_world.receipt.previous_generation_hash is not None
    assert TRADE_DATE in OPEN_DATES
    assert auction_world.inputs.readonly_replica_database_path.is_file()
    #: 冻结的 manifest 里没有采集窗那几项，走的是代码里的默认 09:29-09:44；
    #: 尝试次数是画像写进 manifest 的那一项，读的是同一个常量（3）
    settings = auction_world.manifest(AUCTION_MATCH_SERVICE_ID).settings
    assert "capture_start" not in settings
    assert "capture_end" not in settings
    assert settings["max_attempts"] == 3


def test_the_instance_names_are_the_ones_the_units_carry(
    auction_world: ReplicaWorld,
) -> None:
    for service_id in (
        AUCTION_MATCH_SERVICE_ID,
        AUCTION_UNIVERSE_SERVICE_ID,
        AUCTION_GAP_SERVICE_ID,
        N_SHAPE_SERVICE_ID,
        GROWTH_BOARD_SERVICE_ID,
    ):
        manifest = auction_world.manifest(service_id)
        assert manifest.service_kind in {
            RuntimeServiceKind.AUCTION_MATCH_SOURCE,
            RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
        }
        assert _instance_name(service_id) == "svc-" + hashlib.sha256(
            service_id.encode("utf-8")
        ).hexdigest()
