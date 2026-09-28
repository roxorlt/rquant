"""One bounded, owner-private cycle for a saved formula pool day."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.formula_pool_batch import (
    FormulaPoolBatchCoordinator,
    FormulaPoolBatchDayResult,
    FormulaPoolBatchPrivateConfig,
    load_private_formula_pool_batch_config,
)
from rquant.formula_pool_daily import _run_identity
from rquant.runtime_contracts import RuntimeContractModel
from rquant.screen.formula_market_jobs import (
    FormulaMarketJobReceipt,
    FormulaMarketJobRequest,
    FormulaMarketJobWorker,
)
from rquant.strict_json import canonical_json_bytes

_MAX_WORKER_CALLS = 8


class FormulaPoolAutoCycleResult(RuntimeContractModel):
    schema_version: Literal[1] = 1
    trade_date: date
    worker_calls: int = Field(ge=0, le=_MAX_WORKER_CALLS, strict=True)
    day: FormulaPoolBatchDayResult

    @model_validator(mode="after")
    def require_same_day(self) -> FormulaPoolAutoCycleResult:
        if self.day.trade_date != self.trade_date:
            raise ValueError("formula pool cycle result differs from target day")
        return self


class FormulaPoolAutoCycle:
    """Reconcile, execute at most eight shared tasks, and recheck after each."""

    def __init__(
        self,
        *,
        config: FormulaPoolBatchPrivateConfig,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = FormulaPoolBatchPrivateConfig.model_validate(config)
        self.coordinator = FormulaPoolBatchCoordinator(config=self.config, clock=clock)
        self.worker = FormulaMarketJobWorker(
            self.coordinator.runner.task_store,
            trusted_source_roots=(
                self.config.market.universe_root,
                self.config.market.projection_root,
            ),
        )

    def _require_worker_binding(self) -> None:
        store = self.worker.store
        if (
            store.state_path,
            store.artifact_directory,
            self.worker.trusted_source_roots,
        ) != (
            self.config.market.state_path,
            self.config.market.artifact_directory,
            (self.config.market.universe_root, self.config.market.projection_root),
        ):
            raise ValueError("formula pool worker differs from private configuration")

    @staticmethod
    def _identity(day: FormulaPoolBatchDayResult) -> tuple[date, str, str, str]:
        return (
            day.trade_date,
            day.catalog_identity,
            day.universe_identity,
            day.projection_identity,
        )

    @staticmethod
    def _is_pool_request(
        day: FormulaPoolBatchDayResult, request: FormulaMarketJobRequest
    ) -> bool:
        return any(
            request.idempotency_key
            == _run_identity(
                item.pool_name,
                item.definition_version,
                request.trade_date,
                request.expected_universe_sha256,
                request.expected_projection_identity,
            )
            for item in day.pools
        )

    def _require_receipt_identity(
        self,
        *,
        receipt: FormulaMarketJobReceipt,
        initial: FormulaPoolBatchDayResult,
    ) -> None:
        request = self.worker.store.read_request(receipt.task_id)
        if not self._is_pool_request(initial, request):
            return
        if (
            request.trade_date != initial.trade_date
            or request.expected_universe_sha256 != initial.universe_identity
            or request.expected_projection_identity != initial.projection_identity
            or receipt.error_code == "source_changed"
        ):
            raise ValueError("formula pool worker task source identity changed")

    def run(
        self, trade_date: date, *, max_worker_calls: int = _MAX_WORKER_CALLS
    ) -> FormulaPoolAutoCycleResult:
        if type(max_worker_calls) is not int or not 1 <= max_worker_calls <= _MAX_WORKER_CALLS:
            raise ValueError("formula pool worker calls must be between 1 and 8")
        self._require_worker_binding()
        initial = self.coordinator.run_day(trade_date)
        day = initial
        worker_calls = 0
        while not day.all_complete and worker_calls < max_worker_calls:
            worker_calls += 1
            receipt = self.worker.run_one()
            current = self.coordinator.run_day(trade_date)
            if self._identity(current) != self._identity(initial):
                raise ValueError("formula pool cycle sources or catalog changed")
            if receipt is not None:
                self._require_receipt_identity(receipt=receipt, initial=initial)
            day = current
            if receipt is None:
                break
        return FormulaPoolAutoCycleResult(
            trade_date=trade_date,
            worker_calls=worker_calls,
            day=day,
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Advance one private formula pool day")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--max-worker-calls", type=int, default=_MAX_WORKER_CALLS)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if not 1 <= args.max_worker_calls <= _MAX_WORKER_CALLS:
            raise ValueError("formula pool worker call limit is invalid")
        config = load_private_formula_pool_batch_config(args.config)
        target = date.fromisoformat(args.trade_date)
    except (OSError, ValueError):
        print("formula_pool_cycle_input_invalid", file=sys.stderr)
        return 2
    try:
        result = FormulaPoolAutoCycle(config=config).run(
            target, max_worker_calls=args.max_worker_calls
        )
    except Exception:
        print("formula_pool_cycle_unavailable", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(
        canonical_json_bytes(result.model_dump(mode="json"), trailing_newline=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
