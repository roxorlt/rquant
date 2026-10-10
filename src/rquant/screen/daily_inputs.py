"""Bounded daily input preparation; evaluation stays in the original screen domain."""

from __future__ import annotations

import math
import hashlib
from functools import cache
from pathlib import Path
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.runtime_contracts import canonical_sha256
from rquant.pool_result_receipt import DailyInputEvidence, DailyScreenAuthority
from rquant.screen.core import _collect_aggregates, _infer_lookback, rule_state
from rquant.screen.dynamic_ma import MAX_DYNAMIC_MA_FACTS, dynamic_ma_day_count, requested_dynamic_ma
from rquant.screen.dynamic_rsi import daily_rsi_values, requested_dynamic_rsi
from rquant.screen.loader import (
    FUNDAMENTAL_COLS_MAP, ScreeningFactError, _load_fundamental_wide,
    _resolve_decision_at, _resolve_trading_dates, _selected_sources, load_universe,
)
from rquant.screen.ranking import RETURN_20D_COLUMN, load_twenty_day_adjusted_returns
from rquant.screen.replica_source import (
    MAX_AGGREGATE_FACTS, MAX_AGGREGATE_WINDOW, MAX_CONDITIONS, MAX_LOOKBACK,
    MAX_STOCKS, MAX_WIDE_CELLS, ScreenReplicaBudgetError,
)
from rquant.screen.rules import Rule, required_rule_columns

if TYPE_CHECKING:
    from rquant.storage.duckdb import DuckDBStore




class DailyScreenInputs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)
    frame: pd.DataFrame
    evidence: DailyInputEvidence


@cache
def daily_writer_contract_fingerprint() -> str:
    root = Path(__file__).resolve().parents[1]
    names = ("screen/daily_inputs.py", "screen/core.py", "screen/loader.py", "screen/dynamic_ma.py",
             "screen/dynamic_rsi.py", "screen/ranking.py", "fundamental_receipts.py",
             "pool_result_receipt.py", "pipeline.py", "daily_pool_stage.py", "storage/duckdb.py", "storage/migrations.py")
    return canonical_sha256({"contract": "daily-screen-writer/v1", "files": {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names
    }})


def validate_daily_columns(rules: list[Rule], columns: frozenset[str]) -> None:
    if len(rules) > MAX_CONDITIONS or _infer_lookback(rules) > MAX_LOOKBACK:
        raise ScreenReplicaBudgetError("daily screening request exceeds its budget")
    _selected_sources(columns - {RETURN_20D_COLUMN}, MAX_AGGREGATE_WINDOW)
    requested_dynamic_ma(columns)
    requested_dynamic_rsi(columns)
    aggregates = _collect_aggregates(rules)
    if any(request.window > MAX_AGGREGATE_WINDOW for request in aggregates):
        raise ScreenReplicaBudgetError("daily aggregate window exceeds its budget")


def _value(value: object) -> object:
    if value is None or bool(pd.isna(value)):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def prepare_daily_screen_inputs(
    trade_date: str, rules: list[Rule], *, store: DuckDBStore,
    include_columns: list[str] | None = None, ts_code_whitelist: list[str] | None = None,
    parent_scope_result_version: str | None = None,
    canonical_authority: DailyScreenAuthority | None = None,
) -> DailyScreenInputs:
    target = date.fromisoformat(trade_date)
    decision = _resolve_decision_at(trade_date, None)
    if decision > datetime.now(UTC):
        raise ScreeningFactError("daily decision is not yet visible")
    if canonical_authority is not None:
        canonical_authority = DailyScreenAuthority.model_validate(canonical_authority)
        if canonical_authority.trade_date != target or canonical_authority.available_at > datetime.now(UTC):
            raise ScreeningFactError("daily canonical authority is not visible for this date")
    columns = required_rule_columns(rules) | frozenset(include_columns or ())
    validate_daily_columns(rules, columns)
    count = int(store._conn.execute("SELECT COUNT(*) FROM daily_bar WHERE trade_date=?", [target]).fetchone()[0])
    aggregates = _collect_aggregates(rules)
    if count > MAX_STOCKS or count * (len(columns) + len(aggregates) + 5) > MAX_WIDE_CELLS:
        raise ScreenReplicaBudgetError("daily stock or wide budget exceeded")
    if ts_code_whitelist is not None and (len(ts_code_whitelist)>MAX_STOCKS or len(set(ts_code_whitelist))!=len(ts_code_whitelist)):
        raise ScreenReplicaBudgetError("daily scope exceeds its budget")
    if ts_code_whitelist == [] and parent_scope_result_version is not None:
        _resolve_trading_dates(store,trade_date,0)
        content=canonical_sha256({"date":target,"decision":decision,"parent_result_version":parent_scope_result_version,"codes":[],"columns":sorted(columns),"canonical_authority":canonical_authority})
        return DailyScreenInputs(frame=pd.DataFrame(columns=["ts_code","name",*sorted(columns|{"CLOSE[0]","PCT_CHG[0]"})]),evidence=DailyInputEvidence(
            trade_date=target,decision_at=decision,content_digest=content,
            source_identity=canonical_sha256({"kind":"empty_verified_parent","content":content}),
            required_columns=tuple(sorted(columns)),universe_count=0,unknown_count=0,unknown_steps=tuple(0 for _ in rules),
            parent_scope_result_version=parent_scope_result_version,writer_contract_fingerprint=daily_writer_contract_fingerprint(),
            canonical_authority=canonical_authority,
        ))
    if count * sum(request.window for request in aggregates) > MAX_AGGREGATE_FACTS:
        raise ScreenReplicaBudgetError("daily aggregate fact budget exceeded")
    ma = requested_dynamic_ma(columns)
    if count * dynamic_ma_day_count(ma) > MAX_DYNAMIC_MA_FACTS:
        raise ScreenReplicaBudgetError("daily moving-average fact budget exceeded")
    fundamentals = frozenset(f"{name}[0]" for name in FUNDAMENTAL_COLS_MAP.values())
    ordinary = columns - fundamentals - {RETURN_20D_COLUMN}
    frame = load_universe(trade_date, lookback=_infer_lookback(rules), store=store,
                          aggregate_requests=aggregates, required_columns=ordinary)
    if ts_code_whitelist is not None:
        frame = frame[frame.ts_code.isin(ts_code_whitelist)].reset_index(drop=True)
    codes = frame.ts_code.tolist()
    missing_scope = sorted(set(ts_code_whitelist or ())-set(codes))
    versions: tuple[str, ...] = ()
    selected_fundamental = columns & fundamentals
    if selected_fundamental and codes:
        sources = [name for name, label in FUNDAMENTAL_COLS_MAP.items() if f"{label}[0]" in selected_fundamental]
        facts = _load_fundamental_wide(store, trade_date=trade_date, ts_codes=codes, sources=sources, decision_at=decision)
        if facts.empty:
            raise ScreeningFactError("fundamental daily source is unavailable")
        frame = frame.merge(facts, on="ts_code", how="left", validate="one_to_one")
        slots = ",".join("?" for _ in codes)
        rows = store._conn.execute(
            f"SELECT h.ts_code,h.version_id FROM fundamental_daily_head h WHERE h.trade_date=? AND h.ts_code IN ({slots}) ORDER BY h.ts_code",
            [target, *codes],
        ).fetchall()
        versions = tuple(canonical_sha256(row) for row in rows)
    rsi_digest = None
    rsi = requested_dynamic_rsi(columns)
    if rsi and codes:
        values, rsi_digest = daily_rsi_values(store, target, codes, rsi)
        frame = frame.drop(columns=list(rsi), errors="ignore").merge(values, on="ts_code", how="left", validate="one_to_one")
    if RETURN_20D_COLUMN in columns:
        frame = frame.merge(load_twenty_day_adjusted_returns(store._conn, target, codes), on="ts_code", how="left", validate="one_to_one")
    frame=frame.replace([float("inf"),float("-inf")],float("nan"))
    _, unknowns = rule_state(frame, rules)
    ordered = frame.sort_values("ts_code")
    names = sorted(ordered.columns)
    content = canonical_sha256({
        "columns": names, "rows": [[_value(value) for value in row] for row in ordered[names].itertuples(index=False, name=None)],
        "fundamental_versions": versions, "rsi_source_digest": rsi_digest,
        "decision_at": decision,
        "scope_missing":missing_scope,"parent_scope_result_version":parent_scope_result_version,
        "canonical_authority":canonical_authority,
    })
    evidence = DailyInputEvidence(
        trade_date=target, decision_at=decision, content_digest=content,
        source_identity=canonical_sha256({"kind": "daily_writer", "date": target, "content": content}),
        required_columns=tuple(sorted(columns)), universe_count=len(frame)+len(missing_scope),
        unknown_count=(unknowns[-1] if unknowns else 0)+len(missing_scope), unknown_steps=tuple(value+len(missing_scope) for value in unknowns),
        scope_missing_count=len(missing_scope),parent_scope_result_version=parent_scope_result_version,
        rsi_source_digest=rsi_digest, fundamental_versions=versions,
        writer_contract_fingerprint=daily_writer_contract_fingerprint(),
        canonical_authority=canonical_authority,
    )
    return DailyScreenInputs(frame=frame, evidence=evidence)
