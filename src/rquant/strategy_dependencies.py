"""Typed data dependency closure for formal strategy execution."""

from __future__ import annotations

from datetime import date
from typing import Protocol

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_lake import ResearchDataset


class _DependencyModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)


class StrategyTableDependency(_DependencyModel):
    dataset_id: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    date_column: str | None = None
    code_column: str | None = None
    available_at_column: str | None = None


class BoundStrategyEligibility(_DependencyModel):
    eligibility_id: str = Field(min_length=1)
    strategy_id: str = Field(min_length=1)
    ts_code: str = Field(min_length=1)
    eligibility_date: date
    entry_date: date
    variant: str = Field(min_length=1)


class _EligibilityStore(Protocol):
    _conn: duckdb.DuckDBPyConnection


def query_bound_strategy_eligibility(
    store: _EligibilityStore,
    *,
    strategy_id: str,
    start_date: date,
    end_date: date,
) -> tuple[BoundStrategyEligibility, ...] | None:
    """Return exact bound keys, or None for ordinary operational stores."""
    try:
        rows = store._conn.execute(
            """
            SELECT eligibility_id, strategy_id, ts_code, eligibility_date,
                   entry_date, variant
            FROM strategy_eligibility
            WHERE strategy_id = ?
              AND eligibility_date BETWEEN ? AND ?
            ORDER BY eligibility_date, ts_code, variant, eligibility_id
            """,
            [strategy_id, start_date, end_date],
        ).fetchall()
    except duckdb.CatalogException:
        return None
    return tuple(
        BoundStrategyEligibility(
            eligibility_id=str(eligibility_id),
            strategy_id=strategy,
            ts_code=str(ts_code),
            eligibility_date=eligibility_date,
            entry_date=entry_date,
            variant=str(variant),
        )
        for (
            eligibility_id,
            strategy,
            ts_code,
            eligibility_date,
            entry_date,
            variant,
        ) in rows
    )


class StrategyExecutionDependencies(_DependencyModel):
    strategy_id: str = Field(min_length=1)
    contract_version: str = Field(min_length=1)
    lake_datasets: tuple[ResearchDataset, ...] = Field(min_length=1)
    materialized_tables: tuple[StrategyTableDependency, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_dependencies(self) -> StrategyExecutionDependencies:
        if len(self.lake_datasets) != len(set(self.lake_datasets)):
            raise ValueError("lake_datasets must be unique")
        table_names = [item.table_name for item in self.materialized_tables]
        dataset_ids = [item.dataset_id for item in self.materialized_tables]
        if len(table_names) != len(set(table_names)):
            raise ValueError("materialized table_name values must be unique")
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("materialized dataset_id values must be unique")
        if set(self.lake_datasets) & set(dataset_ids):
            raise ValueError("a dataset cannot be both lake and materialized")
        return self


def _daily(dataset_id: str) -> StrategyTableDependency:
    return StrategyTableDependency(
        dataset_id=dataset_id,
        table_name=dataset_id,
        date_column="trade_date",
        code_column="ts_code",
    )


_COMMON_DAILY_TABLES = (
    _daily("daily_bar"),
    _daily("adj_factor"),
    _daily("daily_state"),
    _daily("daily_indicator"),
    _daily("daily_basic"),
    StrategyTableDependency(
        dataset_id="stock_status_daily",
        table_name="stock_status_daily",
        date_column="trade_date",
        code_column="ts_code",
        available_at_column="available_at",
    ),
    StrategyTableDependency(
        dataset_id="trade_calendar",
        table_name="trade_calendar",
        date_column="cal_date",
    ),
    StrategyTableDependency(
        dataset_id="stock_basic",
        table_name="stock_basic",
        code_column="ts_code",
    ),
    StrategyTableDependency(
        dataset_id="index_daily_bar",
        table_name="index_daily_bar",
        date_column="trade_date",
    ),
)


STRATEGY_EXECUTION_DEPENDENCIES: dict[str, StrategyExecutionDependencies] = {
    "n_shape": StrategyExecutionDependencies(
        strategy_id="n_shape",
        contract_version="stage1-v1",
        lake_datasets=("minute_bar", "auction_bar"),
        materialized_tables=(
            *_COMMON_DAILY_TABLES,
            _daily("limit_list_daily"),
            StrategyTableDependency(
                dataset_id="market_sentiment_daily",
                table_name="market_sentiment_daily",
                date_column="trade_date",
            ),
        ),
    ),
    "growth_board_surge": StrategyExecutionDependencies(
        strategy_id="growth_board_surge",
        contract_version="stage1-v1",
        lake_datasets=("minute_bar",),
        materialized_tables=(
            *_COMMON_DAILY_TABLES,
            _daily("moneyflow_daily"),
            StrategyTableDependency(
                dataset_id="market_sentiment_daily",
                table_name="market_sentiment_daily",
                date_column="trade_date",
            ),
        ),
    ),
    "auction_gap": StrategyExecutionDependencies(
        strategy_id="auction_gap",
        contract_version="stage1-v1",
        lake_datasets=("minute_bar", "auction_bar"),
        materialized_tables=(
            *_COMMON_DAILY_TABLES,
            _daily("limit_list_daily"),
        ),
    ),
}


def strategy_execution_dependencies(
    strategy_id: str,
) -> StrategyExecutionDependencies:
    try:
        return STRATEGY_EXECUTION_DEPENDENCIES[strategy_id]
    except KeyError as exc:
        raise ValueError(f"unknown strategy execution dependency: {strategy_id}") from exc
