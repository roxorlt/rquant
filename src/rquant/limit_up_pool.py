"""涨停池每日采集（东方财富，via akshare）。

`ak.stock_zt_pool_em` 只有**当天**有数据，历史日期返回空——封板资金 /
首次封板时间 / 炸板次数这些盘口口径字段无法事后回补，必须每日采集落库。
云服务器上东财源被屏蔽，本采集只在本地跑（sync-from-cloud.sh 日终触发）。

封单/成交额、封单/流通市值等派生比值在查询侧算，不落库。
"""

from __future__ import annotations

import json
from datetime import date
from typing import Literal

import duckdb
import pandas as pd
from loguru import logger

from rquant.data_metadata import DataQualityIssue, stable_sha256, utc_now
from rquant.security_codes import to_ts_code
from rquant.storage.duckdb import DuckDBStore, _duckdb_transaction_is_active

# akshare stock_zt_pool_em 中文列 → limit_up_pool_daily 列
_COLUMN_MAP: dict[str, str] = {
    "名称": "name",
    "涨跌幅": "pct_chg",
    "最新价": "close",
    "成交额": "amount",
    "流通市值": "circ_mv",
    "总市值": "total_mv",
    "换手率": "turnover_rate",
    "封板资金": "seal_amount",
    "首次封板时间": "first_seal_time",
    "最后封板时间": "last_seal_time",
    "炸板次数": "break_count",
    "涨停统计": "limit_up_stat",
    "连板数": "consecutive_boards",
    "所属行业": "industry",
}

_INT_COLUMNS = ("break_count", "consecutive_boards")
_SEAL_TIME_COLUMNS = ("first_seal_time", "last_seal_time")
_CALENDAR_EXCHANGE = "SSE"
_DATASET_ID = "limit_up_pool_daily"
_RULE_CLOSED_DAY = "limit_up_pool.closed_day_capture"
_RULE_CALENDAR_UNKNOWN = "limit_up_pool.calendar_unknown"
_RULE_CALENDAR_CHANGED = "limit_up_pool.calendar_changed_during_capture"
_RULE_BUSINESS_WRITE = "limit_up_pool.concurrent_business_write"


class LimitUpPoolCaptureError(RuntimeError):
    """Base class for a capture that must return a non-zero CLI status."""


class LimitUpPoolCalendarGuardError(LimitUpPoolCaptureError):
    """Authoritative calendar cannot prove that this capture may write."""

    trade_date: date
    stage: Literal["pre_fetch", "pre_write"]

    def __init__(
        self,
        trade_date: date,
        *,
        stage: Literal["pre_fetch", "pre_write"],
        detail: str,
    ) -> None:
        self.trade_date = trade_date
        self.stage = stage
        super().__init__(detail)


class LimitUpPoolWriteConflictError(LimitUpPoolCaptureError):
    trade_date: date

    def __init__(self, trade_date: date) -> None:
        self.trade_date = trade_date
        super().__init__(
            f"limit-up-pool concurrent business write blocked capture: {trade_date.isoformat()}"
        )


def _record_data_quality_issue(
    store: DuckDBStore,
    issue: DataQualityIssue,
) -> None:
    if not _duckdb_transaction_is_active(store._conn):  # noqa: SLF001
        store.record_data_quality_issue(issue)
        return

    existing = store.get_data_quality_issue(issue.issue_id)
    if existing is not None and (
        existing.rule_id,
        existing.dataset_id,
        existing.scope_key,
    ) != (issue.rule_id, issue.dataset_id, issue.scope_key):
        raise ValueError(f"data quality issue id conflict: {issue.issue_id}")
    if existing is not None:
        effective_time = existing.resolved_at or existing.last_seen_at
        if issue.last_seen_at <= effective_time:
            return
        store._conn.execute(  # noqa: SLF001
            """
            UPDATE data_quality_issue
            SET severity = ?, status = 'open', message = ?,
                evidence = CAST(? AS JSON), last_seen_at = ?,
                resolved_at = NULL
            WHERE issue_id = ?
            """,
            [
                issue.severity,
                issue.message,
                json.dumps(issue.evidence, sort_keys=True),
                issue.last_seen_at,
                issue.issue_id,
            ],
        )
        return
    store._conn.execute(  # noqa: SLF001
        """
        INSERT INTO data_quality_issue
        (issue_id, rule_id, dataset_id, severity, status, scope_key,
         message, evidence, first_seen_at, last_seen_at, resolved_at)
        VALUES (?, ?, ?, ?, 'open', ?, ?, CAST(? AS JSON), ?, ?, NULL)
        """,
        [
            issue.issue_id,
            issue.rule_id,
            issue.dataset_id,
            issue.severity,
            issue.scope_key,
            issue.message,
            json.dumps(issue.evidence, sort_keys=True),
            issue.first_seen_at,
            issue.last_seen_at,
        ],
    )


def _normalize_seal_time(value: object) -> str | None:
    """东财封板时间统一存 VARCHAR。数值型（如 int 92500）丢首位 0，补齐 6 位。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    if s.isdigit() and len(s) < 6:
        s = s.zfill(6)
    return s


def _as_optional_str(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def normalize_zt_pool(raw: pd.DataFrame, trade_date: date) -> pd.DataFrame:
    """东财原始中文列映射为 limit_up_pool_daily 行。

    代码段无法映射交易所（非 6/0/3/4/8/9 开头）的行丢弃并 warning。
    """
    if raw.empty:
        return pd.DataFrame()
    if "代码" not in raw.columns:
        raise ValueError("stock_zt_pool_em 返回缺少 '代码' 列，东财列名可能变更")

    out = pd.DataFrame(index=raw.index)
    out["ts_code"] = raw["代码"].map(to_ts_code)
    for src, dst in _COLUMN_MAP.items():
        out[dst] = raw[src] if src in raw.columns else None

    dropped = int(out["ts_code"].isna().sum())
    if dropped:
        logger.warning(f"涨停池 {dropped} 行代码段无法映射交易所，已丢弃")
    out = out[out["ts_code"].notna()].copy()

    out["trade_date"] = trade_date
    for col in _SEAL_TIME_COLUMNS:
        out[col] = out[col].map(_normalize_seal_time)
    for col in _INT_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    out["limit_up_stat"] = out["limit_up_stat"].map(_as_optional_str)
    out["source"] = "eastmoney"
    return out.reset_index(drop=True)


def _fetch_zt_pool(ds: str) -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zt_pool_em(date=ds)


def _record_calendar_issue(
    store: DuckDBStore,
    trading_date: date,
    *,
    stage: Literal["pre_fetch", "pre_write"],
    state: Literal["closed", "unknown", "concurrent_change"],
) -> None:
    if stage == "pre_fetch" and state == "closed":
        rule_id = _RULE_CLOSED_DAY
        severity: Literal["P0", "P1"] = "P1"
        message = "limit-up-pool capture was scheduled for a known closed day"
    elif state == "unknown":
        rule_id = _RULE_CALENDAR_UNKNOWN
        severity = "P0"
        message = "limit-up-pool capture blocked because calendar is unknown"
    else:
        rule_id = _RULE_CALENDAR_CHANGED
        severity = "P0"
        message = "limit-up-pool calendar changed before the final write"
    _record_data_quality_issue(
        store,
        DataQualityIssue.detected(
            rule_id=rule_id,
            dataset_id=_DATASET_ID,
            severity=severity,
            scope_key=trading_date.isoformat(),
            message=message,
            evidence={
                "exchange": _CALENDAR_EXCHANGE,
                "trade_date": trading_date.isoformat(),
                "stage": stage,
                "calendar_state": state,
            },
        ),
    )


def _record_business_write_issue(
    store: DuckDBStore,
    trading_date: date,
) -> None:
    _record_data_quality_issue(
        store,
        DataQualityIssue.detected(
            rule_id=_RULE_BUSINESS_WRITE,
            dataset_id=_DATASET_ID,
            severity="P0",
            scope_key=trading_date.isoformat(),
            message="concurrent limit-up-pool business write blocked capture",
            evidence={
                "trade_date": trading_date.isoformat(),
                "stage": "pre_write",
            },
        ),
    )


def _record_conflict_issue(
    store: DuckDBStore,
    trading_date: date,
    *,
    conflict_domain: Literal["calendar", "business_write"],
    use_fresh_connection: bool,
) -> None:
    issue_store = DuckDBStore(store.path) if use_fresh_connection else None
    target = issue_store or store
    try:
        if conflict_domain == "calendar":
            _record_calendar_issue(
                target,
                trading_date,
                stage="pre_write",
                state="concurrent_change",
            )
        else:
            _record_business_write_issue(target, trading_date)
    finally:
        if issue_store is not None:
            issue_store.close()


def _resolve_open_issues(
    store: DuckDBStore,
    trading_date: date,
    rule_ids: tuple[str, ...],
) -> None:
    for rule_id in rule_ids:
        issue_id = stable_sha256(
            "data_quality_issue",
            {
                "rule_id": rule_id,
                "dataset_id": _DATASET_ID,
                "scope_key": trading_date.isoformat(),
            },
        )
        existing = store.get_data_quality_issue(issue_id)
        if existing is not None and existing.status == "open":
            if not _duckdb_transaction_is_active(store._conn):  # noqa: SLF001
                store.resolve_data_quality_issue(issue_id)
                continue
            resolved_at = utc_now()
            if resolved_at < existing.last_seen_at:
                raise ValueError(
                    "data quality issue resolved_at cannot be earlier than "
                    f"last_seen_at: {issue_id}"
                )
            updated = store._conn.execute(  # noqa: SLF001
                """
                UPDATE data_quality_issue
                SET status = 'resolved', resolved_at = ?
                WHERE issue_id = ? AND status = 'open'
                RETURNING issue_id
                """,
                [resolved_at, issue_id],
            ).fetchone()
            if updated is None:
                raise RuntimeError(
                    f"data quality issue resolution lost concurrent update: {issue_id}"
                )


def _allow_remote_fetch(store: DuckDBStore, trading_date: date) -> bool:
    calendar_day = store.get_trade_calendar_day(_CALENDAR_EXCHANGE, trading_date)
    if calendar_day is None:
        _record_calendar_issue(
            store,
            trading_date,
            stage="pre_fetch",
            state="unknown",
        )
        raise LimitUpPoolCalendarGuardError(
            trading_date,
            stage="pre_fetch",
            detail=(f"limit-up-pool calendar unknown before fetch: {trading_date.isoformat()}"),
        )
    if not calendar_day.is_open:
        _record_calendar_issue(
            store,
            trading_date,
            stage="pre_fetch",
            state="closed",
        )
        logger.warning(
            f"涨停池拒绝休市日采集: date={trading_date.isoformat()} exchange={_CALENDAR_EXCHANGE}"
        )
        return False
    _resolve_open_issues(
        store,
        trading_date,
        (_RULE_CLOSED_DAY, _RULE_CALENDAR_UNKNOWN),
    )
    return True


def _write_with_final_calendar_check(
    store: DuckDBStore,
    df: pd.DataFrame,
    trading_date: date,
) -> int:
    owns_transaction = not _duckdb_transaction_is_active(  # noqa: SLF001
        store._conn  # noqa: SLF001
    )
    transaction_open = False
    blocked_state: Literal["closed", "unknown"] | None = None
    conflict_domain: Literal["calendar", "business_write"] = "calendar"
    rows: int
    try:
        if owns_transaction:
            store._conn.execute("BEGIN")  # noqa: SLF001
            transaction_open = True
        # DuckDB optimizes `SET is_open = is_open` away. Two real writes restore
        # the fact before reading it while fencing same-row calendar corrections.
        store._conn.execute(  # noqa: SLF001
            """
            UPDATE trade_calendar
            SET is_open = NOT is_open
            WHERE exchange = ? AND cal_date = ?
            """,
            [_CALENDAR_EXCHANGE, trading_date],
        )
        store._conn.execute(  # noqa: SLF001
            """
            UPDATE trade_calendar
            SET is_open = NOT is_open
            WHERE exchange = ? AND cal_date = ?
            """,
            [_CALENDAR_EXCHANGE, trading_date],
        )
        calendar_day = store.get_trade_calendar_day(
            _CALENDAR_EXCHANGE,
            trading_date,
        )
        if calendar_day is None:
            blocked_state = "unknown"
        elif not calendar_day.is_open:
            blocked_state = "closed"
        if blocked_state is not None:
            if owns_transaction:
                store._conn.execute("ROLLBACK")  # noqa: SLF001
                transaction_open = False
            _record_calendar_issue(
                store,
                trading_date,
                stage="pre_write",
                state=blocked_state,
            )
            raise LimitUpPoolCalendarGuardError(
                trading_date,
                stage="pre_write",
                detail=(
                    "limit-up-pool calendar changed before write: "
                    f"{trading_date.isoformat()} state={blocked_state}"
                ),
            )
        conflict_domain = "business_write"
        rows = store.upsert_limit_up_pool(df, transaction_mode="existing")
        if owns_transaction:
            store._conn.execute("COMMIT")  # noqa: SLF001
            transaction_open = False
    except duckdb.TransactionException as original_error:
        if transaction_open:
            try:
                store._conn.execute("ROLLBACK")  # noqa: SLF001
            except BaseException as rollback_error:
                raise BaseExceptionGroup(
                    "calendar fence failed and rollback failed",
                    [original_error, rollback_error],
                ) from None
            transaction_open = False
        try:
            _record_conflict_issue(
                store,
                trading_date,
                conflict_domain=conflict_domain,
                use_fresh_connection=not owns_transaction,
            )
        except BaseException as issue_error:
            original_error.add_note(
                "failed to persist the capture conflict quality issue: "
                f"{type(issue_error).__name__}: {issue_error}"
            )
            logger.exception(
                "涨停池冲突已阻断，但 P0 质量问题落库失败: "
                f"date={trading_date.isoformat()} domain={conflict_domain}"
            )
        if conflict_domain == "business_write":
            raise LimitUpPoolWriteConflictError(trading_date) from original_error
        raise LimitUpPoolCalendarGuardError(
            trading_date,
            stage="pre_write",
            detail=(
                "limit-up-pool concurrent write blocked final calendar check: "
                f"{trading_date.isoformat()}"
            ),
        ) from original_error
    except BaseException as original_error:
        if transaction_open:
            try:
                store._conn.execute("ROLLBACK")  # noqa: SLF001
            except BaseException as rollback_error:
                raise BaseExceptionGroup(
                    "limit-up-pool write failed and rollback failed",
                    [original_error, rollback_error],
                ) from None
        raise
    try:
        _resolve_open_issues(
            store,
            trading_date,
            (_RULE_CALENDAR_CHANGED, _RULE_BUSINESS_WRITE),
        )
    except Exception as exc:
        logger.warning(
            "涨停池已写入，但旧质量问题暂未关闭，后续重跑会重试: "
            f"date={trading_date.isoformat()} err={exc}"
        )
    return rows


def capture_zt_pool(trade_date: date | None = None, store: DuckDBStore | None = None) -> int:
    """抓当日涨停池并落库，返回写入行数。

    权威日历明确休市时记录 P1 并跳过；日历未知或抓取期间发生变化时
    记录 P0 并抛出 ``LimitUpPoolCalendarGuardError``。东财源失败、空返回和
    列名变更仍保持原有的 warning + return 0 兼容行为。
    """
    trading_date = trade_date or date.today()
    ds = trading_date.strftime("%Y%m%d")

    owns_store = store is None
    if owns_store:
        with DuckDBStore() as calendar_store:
            if not _allow_remote_fetch(calendar_store, trading_date):
                return 0
    elif not _allow_remote_fetch(store, trading_date):
        return 0

    try:
        raw = _fetch_zt_pool(ds)
    except Exception as e:
        logger.warning(f"涨停池抓取失败（东财源偶发不可用）: date={ds} err={e}")
        return 0
    if raw is None or raw.empty:
        logger.warning(f"涨停池返回空: date={ds}（历史日期无数据 / 当日无涨停 / 源不可用）")
        return 0

    try:
        df = normalize_zt_pool(raw, trading_date)
    except Exception as e:
        logger.warning(f"涨停池字段归一化失败: date={ds} err={e}")
        return 0
    if df.empty:
        logger.warning(f"涨停池归一化后为空: date={ds}")
        return 0

    if owns_store:
        with DuckDBStore() as write_store:
            rows = _write_with_final_calendar_check(
                write_store,
                df,
                trading_date,
            )
    else:
        rows = _write_with_final_calendar_check(store, df, trading_date)
    logger.info(f"limit_up_pool_daily 写入 {rows} 行: date={ds}")
    return rows
