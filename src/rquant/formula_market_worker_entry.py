"""Run at most one locally queued formula market job from explicit private configuration."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.formula_market_private_config import load_private_formula_market_config
from rquant.runtime_contracts import RuntimeContractModel
from rquant.screen.formula_market_jobs import (
    FormulaMarketJobReceipt,
    FormulaMarketJobStore,
    FormulaMarketJobWorker,
    JobErrorCode,
)
from rquant.strict_json import canonical_json_bytes


class FormulaMarketWorkerCommandResult(RuntimeContractModel):
    status: Literal["idle", "succeeded", "failed"]
    task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    error_code: JobErrorCode | None = None

    @model_validator(mode="after")
    def require_terminal_shape(self) -> FormulaMarketWorkerCommandResult:
        if (self.status == "idle") != (self.task_id is None):
            raise ValueError("worker task identity and status disagree")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("worker error category and status disagree")
        return self


def _result(receipt: FormulaMarketJobReceipt | None) -> FormulaMarketWorkerCommandResult:
    if receipt is None:
        return FormulaMarketWorkerCommandResult(status="idle")
    if receipt.status == "succeeded":
        return FormulaMarketWorkerCommandResult(status="succeeded", task_id=receipt.task_id)
    if receipt.status == "failed":
        return FormulaMarketWorkerCommandResult(
            status="failed", task_id=receipt.task_id, error_code=receipt.error_code
        )
    raise RuntimeError("formula worker did not finish the claimed task")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one locally queued formula market task")
    parser.add_argument("--config", required=True, type=Path)
    arguments = parser.parse_args(list(argv) if argv is not None else None)
    try:
        config = load_private_formula_market_config(arguments.config)
    except (OSError, ValueError):
        print("worker_config_invalid", file=sys.stderr)
        return 2
    try:
        store = FormulaMarketJobStore(
            state_path=config.state_path,
            artifact_directory=config.artifact_directory,
        )
        receipt = FormulaMarketJobWorker(
            store,
            trusted_source_roots=(config.universe_root, config.projection_root),
        ).run_one()
        result = _result(receipt)
    except Exception:
        print("worker_run_unavailable", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(
        canonical_json_bytes(
            result.model_dump(mode="json", exclude_none=True), trailing_newline=True
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
