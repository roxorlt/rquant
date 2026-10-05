"""Typed data dependency closure for formal strategy execution."""

from __future__ import annotations

from datetime import date
from typing import Protocol

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_lake import ResearchDataset
from rquant.strategy_template_definition import StrategyTemplateExecutionVersion

SUSPENSION_SESSION_EVIDENCE_DATASET = "stock_suspend_session_evidence"
FACTOR_EVAL_CONTRACT_VERSION = "factor-eval-v1"


class _DependencyModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)


class StrategyTableDependency(_DependencyModel):
    dataset_id: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    date_column: str | None = None
    code_column: str | None = None
    available_at_column: str | None = None


PORTFOLIO_BACKTEST_CONTRACT_VERSION = "portfolio-daily-v1"
_PORTFOLIO_TABLE_DEPENDENCIES = (
    StrategyTableDependency(
        dataset_id="portfolio_backtest_input", table_name="portfolio_backtest_input"
    ),
)

_PAPER_TABLE_DEPENDENCIES = (
    StrategyTableDependency(dataset_id="paper_research_input", table_name="paper_research_input"),
)


_FACTOR_TABLE_DEPENDENCIES = (
    StrategyTableDependency(
        dataset_id="daily_bar",
        table_name="daily_bar",
        date_column="trade_date",
        code_column="ts_code",
    ),
    StrategyTableDependency(
        dataset_id="adj_factor",
        table_name="adj_factor",
        date_column="trade_date",
        code_column="ts_code",
    ),
    StrategyTableDependency(
        dataset_id="trade_calendar",
        table_name="trade_calendar",
        date_column="cal_date",
    ),
)


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
    lake_datasets: tuple[ResearchDataset, ...]
    materialized_tables: tuple[StrategyTableDependency, ...] = Field(min_length=1)
    template_definition: StrategyTemplateExecutionVersion | None = None

    @model_validator(mode="after")
    def validate_unique_dependencies(self) -> StrategyExecutionDependencies:
        if self.template_definition is not None:
            if (self.strategy_id, self.contract_version, self.lake_datasets, self.materialized_tables) != (self.template_definition.strategy_id, "strategy-template-input/v1", (), (StrategyTableDependency(dataset_id="strategy_template_input", table_name="strategy_template_input"),)):
                raise ValueError("template source requires its exact committed definition and input table")
        materialized_only = (
            self.strategy_id == "factor_eval"
            and self.contract_version == FACTOR_EVAL_CONTRACT_VERSION
            and self.materialized_tables == _FACTOR_TABLE_DEPENDENCIES
        ) or (
            self.strategy_id == "portfolio_backtest"
            and self.contract_version == PORTFOLIO_BACKTEST_CONTRACT_VERSION
            and self.materialized_tables == _PORTFOLIO_TABLE_DEPENDENCIES
        ) or (
            self.strategy_id in {"paper_reconcile", "paper_backtest_band"}
            and self.contract_version == "paper-research-input/v1"
            and self.materialized_tables == _PAPER_TABLE_DEPENDENCIES
        ) or self.template_definition is not None
        if not self.lake_datasets and not materialized_only:
            raise ValueError(
                "lake_datasets may be empty only for an exact approved materialized contract"
            )
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
    "paper_reconcile": StrategyExecutionDependencies(strategy_id="paper_reconcile", contract_version="paper-research-input/v1",
                                                      lake_datasets=(), materialized_tables=_PAPER_TABLE_DEPENDENCIES),
    "paper_backtest_band": StrategyExecutionDependencies(strategy_id="paper_backtest_band", contract_version="paper-research-input/v1",
                                                          lake_datasets=(), materialized_tables=_PAPER_TABLE_DEPENDENCIES),
    "portfolio_backtest": StrategyExecutionDependencies(
        strategy_id="portfolio_backtest",
        contract_version=PORTFOLIO_BACKTEST_CONTRACT_VERSION,
        lake_datasets=(),
        materialized_tables=_PORTFOLIO_TABLE_DEPENDENCIES,
    ),
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
        contract_version="stage1-v2",
        lake_datasets=("minute_bar",),
        materialized_tables=(
            *_COMMON_DAILY_TABLES,
            _daily("moneyflow_daily"),
            StrategyTableDependency(
                dataset_id=SUSPENSION_SESSION_EVIDENCE_DATASET,
                table_name=SUSPENSION_SESSION_EVIDENCE_DATASET,
            ),
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

FACTOR_EVAL_DEPENDENCIES = StrategyExecutionDependencies(
    strategy_id="factor_eval",
    contract_version=FACTOR_EVAL_CONTRACT_VERSION,
    lake_datasets=(),
    materialized_tables=_FACTOR_TABLE_DEPENDENCIES,
)


def factor_execution_dependencies() -> StrategyExecutionDependencies:
    return FACTOR_EVAL_DEPENDENCIES


def strategy_execution_dependencies(
    strategy_id: str,
) -> StrategyExecutionDependencies:
    try:
        return STRATEGY_EXECUTION_DEPENDENCIES[strategy_id]
    except KeyError as exc:
        raise ValueError(f"unknown strategy execution dependency: {strategy_id}") from exc
