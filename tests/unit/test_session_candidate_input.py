"""#278：n_shape / growth_board_surge 的候选文档按交易日重建。

装机封存的那一份停在 `trade_date 2026-07-14`，盘中 loader 要的是当日，于是两份文档在任何
一个真实交易日都被拒。这里钉住重建出来的文档：当日的 `trade_date`、生成时刻的
`captured_at`、副本里读到的 `basis_trade_date`，以及「封存版的字节一个都没变」。
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pytest

from rquant.live_contracts import BatchQualityStatus
from rquant.runtime_builder_candidate import serialize_candidate_input
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.session_candidate_input import (
    SEALED_CANDIDATE_CONTRACT,
    SESSION_CANDIDATE_CONTRACT,
    SessionCandidateInputError,
    assemble_session_candidate_batch,
    candidate_input_authority_id,
    candidate_input_batch,
    latest_daily_trade_date,
)
from rquant.strict_json import strict_canonical_json_loads

SHANGHAI = ZoneInfo("Asia/Shanghai")
COMMIT = "a" * 40
OPEN_DATES = (date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12))
TRADE_DATE = OPEN_DATES[-1]
PRIOR_DATE = OPEN_DATES[-2]
OBSERVED_AT = datetime.combine(TRADE_DATE, time(8, 45), tzinfo=SHANGHAI).astimezone(UTC)


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=OPEN_DATES[0],
        coverage_end=OPEN_DATES[-1],
        open_dates=OPEN_DATES,
        generated_at=datetime(2026, 8, 9, 8, 0, tzinfo=UTC),
    )


def _replica(path: Path, *, trade_dates: tuple[date, ...]) -> Path:
    """`scripts/sync-readonly-replica.sh` 留下的那种文件：0644、无 WAL、单硬链接。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)")
        if trade_dates:
            connection.executemany(
                "INSERT INTO daily_bar VALUES (?, ?, ?)",
                [("600000.SH", item, 1000.0) for item in trade_dates],
            )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    assert not Path(f"{path}.wal").exists()
    path.chmod(0o644)
    return path


def test_the_sealed_document_id_is_byte_for_byte_the_v0_33_15_one() -> None:
    """封存契约串与哈希载荷不许动：同一次装机重跑必须得出同一份文档。"""

    assert candidate_input_authority_id(
        contract=SEALED_CANDIDATE_CONTRACT,
        strategy_id="n_shape",
        trade_date=date(2026, 7, 14),
    ) == "de5b9dc067cc46f267503f2995558fef37b1f4ff66af920b01f39fec6846cf78"


def test_a_document_without_a_basis_keeps_the_old_wire_shape() -> None:
    """`basis_trade_date` 是新加的字段，旧读者 `extra="forbid"`——没有值时键不写出来。"""

    payload = serialize_candidate_input(
        candidate_input_batch(
            strategy_id="n_shape",
            producer_commit=COMMIT,
            trade_date=TRADE_DATE,
            captured_at=OBSERVED_AT,
        )
    )

    decoded = strict_canonical_json_loads(payload)
    assert "basis_trade_date" not in decoded["batch"]["authority"]


def test_a_document_with_a_basis_carries_it_on_the_wire() -> None:
    payload = serialize_candidate_input(
        candidate_input_batch(
            strategy_id="growth_board_surge",
            producer_commit=COMMIT,
            trade_date=TRADE_DATE,
            captured_at=OBSERVED_AT,
            basis_trade_date=PRIOR_DATE,
            contract=SESSION_CANDIDATE_CONTRACT,
        )
    )

    decoded = strict_canonical_json_loads(payload)
    assert decoded["batch"]["authority"]["basis_trade_date"] == PRIOR_DATE.isoformat()


def test_a_basis_after_the_session_is_refused() -> None:
    with pytest.raises(ValueError, match="basis_trade_date"):
        candidate_input_batch(
            strategy_id="n_shape",
            producer_commit=COMMIT,
            trade_date=PRIOR_DATE,
            captured_at=datetime.combine(PRIOR_DATE, time(8, 45), tzinfo=SHANGHAI),
            basis_trade_date=TRADE_DATE,
        )


# ---------------------------------------------------------------------------------------
# 副本里那一条查询
# ---------------------------------------------------------------------------------------


def test_the_basis_is_the_previous_session_before_todays_results_land(tmp_path: Path) -> None:
    """09:15 之前副本里最新的日线结果必然是上一场的——今天还没收盘。"""

    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES[:-1])

    assert latest_daily_trade_date(replica, on_or_before=TRADE_DATE) == PRIOR_DATE


def test_the_basis_is_today_once_todays_results_are_in_the_replica(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES)

    assert latest_daily_trade_date(replica, on_or_before=TRADE_DATE) == TRADE_DATE


def test_a_replica_with_nothing_at_or_before_the_session_refuses(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=())

    with pytest.raises(SessionCandidateInputError, match="no daily result"):
        assemble_session_candidate_batch(
            strategy_id="n_shape",
            daily_database_path=replica,
            calendar=_calendar(),
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
            producer_commit=COMMIT,
        )


def test_a_group_writable_replica_is_refused(tmp_path: Path) -> None:
    """副本的安全规则与 auction-gap 读者共用一套，这里确认它真的在生效。"""

    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES[:-1])
    replica.chmod(0o664)

    with pytest.raises(SessionCandidateInputError, match="group or other writable"):
        latest_daily_trade_date(replica, on_or_before=TRADE_DATE)


def test_a_closed_date_is_refused_by_name(tmp_path: Path) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES[:-1])

    with pytest.raises(SessionCandidateInputError, match="not an open session"):
        assemble_session_candidate_batch(
            strategy_id="n_shape",
            daily_database_path=replica,
            calendar=_calendar(),
            trade_date=date(2026, 8, 15),
            observed_at=datetime(2026, 8, 15, 0, 45, tzinfo=UTC),
            producer_commit=COMMIT,
        )


# ---------------------------------------------------------------------------------------
# 装出来的那一份
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("strategy_id", ["n_shape", "growth_board_surge"])
def test_the_session_document_is_dated_today_and_stamped_now(
    tmp_path: Path,
    strategy_id: str,
) -> None:
    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES[:-1])

    batch = assemble_session_candidate_batch(
        strategy_id=strategy_id,
        daily_database_path=replica,
        calendar=_calendar(),
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
    )

    authority = batch.authority
    assert authority.trade_date == TRADE_DATE
    assert authority.captured_at == OBSERVED_AT
    assert authority.basis_trade_date == PRIOR_DATE
    assert authority.producer_commit == COMMIT
    assert authority.quality_status is BatchQualityStatus.PUBLISHED
    #: 事实列表仍然是空的：装机脚本封的就是空列表，本包只搬日期
    assert batch.facts == ()
    #: 两种文档的身份互不冒充
    assert authority.authority_snapshot_id != candidate_input_authority_id(
        contract=SEALED_CANDIDATE_CONTRACT,
        strategy_id=strategy_id,
        trade_date=TRADE_DATE,
    )


def test_two_sessions_in_a_row_are_two_different_documents(tmp_path: Path) -> None:
    """按日重建的意义就在这里：昨天那份不会被当成今天的。"""

    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES[:-1])
    calendar = _calendar()

    today = assemble_session_candidate_batch(
        strategy_id="n_shape",
        daily_database_path=replica,
        calendar=calendar,
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
    )
    yesterday = assemble_session_candidate_batch(
        strategy_id="n_shape",
        daily_database_path=replica,
        calendar=calendar,
        trade_date=PRIOR_DATE,
        observed_at=datetime.combine(PRIOR_DATE, time(8, 45), tzinfo=SHANGHAI).astimezone(UTC),
        producer_commit=COMMIT,
    )

    assert today.authority.trade_date != yesterday.authority.trade_date
    assert today.authority.authority_snapshot_id != yesterday.authority.authority_snapshot_id


def test_a_replica_replaced_mid_read_is_refused(tmp_path: Path) -> None:
    """副本每五分钟被整文件换掉；读到一半换了就不要用这次的答案。"""

    replica = _replica(tmp_path / "rquant_ro.duckdb", trade_dates=OPEN_DATES[:-1])
    import rquant.session_candidate_input as module

    real_identity = module._private_snapshot_identity
    state = {"calls": 0}

    def replacing_identity(path: Path):
        state["calls"] += 1
        result = real_identity(path)
        if state["calls"] == 1:
            #: 在查询之后、比对之前把 mtime 改掉，等价于 `mv` 换了一代
            os.utime(path, (0, 0))
        return result

    module._private_snapshot_identity = replacing_identity  # type: ignore[assignment]
    try:
        with pytest.raises(SessionCandidateInputError, match="changed while reading"):
            latest_daily_trade_date(replica, on_or_before=TRADE_DATE)
    finally:
        module._private_snapshot_identity = real_identity  # type: ignore[assignment]
