"""每个交易日重建一次的 n_shape / growth_board_surge 候选文档（#278）。

到 v0.33.15 为止，这两个策略的候选文档是**装机时封好的一份**：
`scripts/build_runtime_production_inputs.py` 把 `trade_calendar` 表里最新的 `updated_at`
（生产副本上是 2026-07-14 18:13）当成生成时刻，据此封出 `trade_date 2026-07-14` 的文档，
发布者每轮把同一份文档重发一遍。而 watchlist-quote / market-minute 用
`required_trade_date = 当日` 去读这三个候选权威，且三个都是 `required: True`，于是任何
一个真实交易日这两份文档都对不上日期——只是因为 `auction_gap` 排在前面先失败（#277），
这条才一直没浮上来。

这里给出的是同一份文档的「按日重建」版本：**同一条查询**（装机脚本用的那条，现在两边共用
下面的 `candidate_input_batch`），`trade_date` = 本场交易日，`captured_at` = 生成时刻，
`basis_trade_date` = 只读副本里能读到的最新那一场日线结果的日期。09:15 之前能读到的最新
日线结果必然是**上一场**的（今天还没收盘），所以 `basis_trade_date` 正常就是上一个交易日，
这不是降级，而是这两个策略本来的口径；文档把它写出来，读的人不会误以为候选是用今天的
数据算的。

事实列表仍然是空的——装机脚本封的就是空列表，本包只搬日期，不改「候选从哪来」。真正的
筛选事实要接哪条查询，是另一件事，不在 #278 的范围里。
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from rquant.auction_gap_candidate_input import (
    #: 副本的安全规则只有一份：不是符号链接、属主是自己、组和其他不可写、单硬链接、
    #: 读前读后同一个 inode。auction-gap 的读者定义了它，这里原样复用，不另写一套。
    AuctionGapCandidateInputError,
    _private_snapshot_identity,
    _same_snapshot,
)
from rquant.live_contracts import BatchQualityStatus
from rquant.readside_replica_gate import ReplicaReadGate
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_read_interrupt import interruptible_read, is_read_interrupt
from rquant.strategy_candidate_producers import PublishedCandidateInputAuthority
from rquant.strategy_candidate_publish_service import (
    CandidatePublishBatch,
    GrowthBoardCandidateBatch,
    NShapeCandidateBatch,
)

SessionCandidateStrategyId = Literal["n_shape", "growth_board_surge"]

#: 装机脚本封存文档用的契约串。**不要改**：改了之后同一次装机重跑会得出不同的
#: `authority_snapshot_id`，而复现装机文档是装机流程的一条硬约定。
SEALED_CANDIDATE_CONTRACT = "route-a/sealed-candidate-input/v1"
#: 按日重建出来的那一份。与封存版分开命名，两种文档的 id 因此不会互相冒充。
SESSION_CANDIDATE_CONTRACT = "route-a/session-candidate-input/v1"


class SessionCandidateInputError(RuntimeError):
    """副本里读不出可信的日线基准日，就不要发今天的候选文档。"""


def candidate_input_authority_id(
    *,
    contract: str,
    strategy_id: SessionCandidateStrategyId,
    trade_date: date,
    basis_trade_date: date | None = None,
) -> str:
    """文档身份：寻址的是「这份（空的）事实列表」，重封同一份空得出同一个 id。

    `basis_trade_date` 为 `None` 时**不进哈希**，所以封存版的 id 与 v0.33.15 逐字相同。
    """

    payload: dict[str, object] = {
        "contract": contract,
        "strategy_id": strategy_id,
        "trade_date": trade_date,
        "facts": [],
    }
    if basis_trade_date is not None:
        payload["basis_trade_date"] = basis_trade_date
    return canonical_sha256(payload)


def candidate_input_batch(
    *,
    strategy_id: SessionCandidateStrategyId,
    producer_commit: str,
    trade_date: date,
    captured_at: datetime,
    basis_trade_date: date | None = None,
    contract: str = SEALED_CANDIDATE_CONTRACT,
) -> CandidatePublishBatch:
    """装机脚本与按日发布者共用的那一份构造。"""

    authority = PublishedCandidateInputAuthority(
        trade_date=trade_date,
        captured_at=normalize_aware_utc(captured_at),
        quality_status=BatchQualityStatus.PUBLISHED,
        authority_snapshot_id=candidate_input_authority_id(
            contract=contract,
            strategy_id=strategy_id,
            trade_date=trade_date,
            basis_trade_date=basis_trade_date,
        ),
        producer_commit=producer_commit,
        basis_trade_date=basis_trade_date,
    )
    if strategy_id == "n_shape":
        return NShapeCandidateBatch(authority=authority, facts=())
    if strategy_id == "growth_board_surge":
        return GrowthBoardCandidateBatch(authority=authority, facts=())
    raise SessionCandidateInputError(f"no session candidate document is defined for {strategy_id}")


_BasisRead = tuple[date | None, datetime]


def _snapshot_identity(path: Path) -> tuple[Any, Path]:
    """共用的安全规则，拒绝的理由按本模块的错误类型报出来。"""

    try:
        return _private_snapshot_identity(path)
    except (AuctionGapCandidateInputError, ValueError) as exc:
        raise SessionCandidateInputError(str(exc)) from exc


def _query_latest_daily_trade_date(path: Path, *, on_or_before: date) -> _BasisRead:
    before, normalized = _snapshot_identity(path)
    import duckdb

    connection = None
    try:
        connection = duckdb.connect(str(normalized), read_only=True)
        #: 一次 `max()`，不取任何行内容。停机时可中断（#268），中断按中断报，
        #: 不冒充成「副本查询失败」。
        with interruptible_read(connection):
            row = connection.execute(
                "SELECT max(trade_date) FROM daily_bar WHERE trade_date <= ?",
                [on_or_before],
            ).fetchone()
    except duckdb.Error as exc:
        if is_read_interrupt(exc):
            raise
        raise SessionCandidateInputError("daily snapshot query failed") from exc
    finally:
        if connection is not None:
            connection.close()
    try:
        after = normalized.lstat()
    except OSError as exc:
        raise SessionCandidateInputError("daily snapshot changed while reading") from exc
    _snapshot_identity(normalized)
    if not _same_snapshot(before, after):
        raise SessionCandidateInputError("daily snapshot changed while reading")

    observed = None if row is None else row[0]
    if observed is None:
        basis: date | None = None
    elif isinstance(observed, datetime):
        basis = observed.date()
    elif isinstance(observed, date):
        basis = observed
    else:
        raise SessionCandidateInputError("daily snapshot returned a non-date trade_date")
    available_at = datetime.fromtimestamp(before.st_mtime_ns / 1_000_000_000, tz=UTC)
    return basis, available_at


def latest_daily_trade_date(
    path: Path,
    *,
    on_or_before: date,
    read_gate: ReplicaReadGate[_BasisRead] | None = None,
) -> date | None:
    """副本里不晚于 `on_or_before` 的最新日线交易日。

    走 gate 时，同一个问题（同一个 `on_or_before`）在同一代副本上只开一次库。
    """

    if read_gate is None:
        return _query_latest_daily_trade_date(path, on_or_before=on_or_before)[0]
    return read_gate.read(
        lambda: _query_latest_daily_trade_date(path, on_or_before=on_or_before),
        key=("session-candidate-basis", on_or_before),
    ).value[0]


def assemble_session_candidate_batch(
    *,
    strategy_id: SessionCandidateStrategyId,
    daily_database_path: Path,
    calendar: MarketCalendarAuthority,
    trade_date: date,
    observed_at: datetime,
    producer_commit: str,
    read_gate: ReplicaReadGate[Any] | None = None,
) -> CandidatePublishBatch:
    """今天这一场的候选文档。"""

    if trade_date not in calendar.open_dates:
        raise SessionCandidateInputError("trade_date is not an open session")
    basis = latest_daily_trade_date(
        daily_database_path,
        on_or_before=trade_date,
        read_gate=read_gate,
    )
    if basis is None:
        raise SessionCandidateInputError(
            "the read-only replica has no daily result at or before this session"
        )
    return candidate_input_batch(
        strategy_id=strategy_id,
        producer_commit=producer_commit,
        trade_date=trade_date,
        captured_at=observed_at,
        #: 与本场同日时不写这个键：文档回到 v0.33.15 的形状（见 `serialize_candidate_input`）
        basis_trade_date=None if basis == trade_date else basis,
        contract=SESSION_CANDIDATE_CONTRACT,
    )


__all__ = [
    "SEALED_CANDIDATE_CONTRACT",
    "SESSION_CANDIDATE_CONTRACT",
    "SessionCandidateInputError",
    "SessionCandidateStrategyId",
    "assemble_session_candidate_batch",
    "candidate_input_authority_id",
    "candidate_input_batch",
    "latest_daily_trade_date",
]
