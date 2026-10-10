"""Configuration-free diagnostics over explicitly supplied frozen historical JSON."""

from __future__ import annotations

import argparse
import hashlib
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, NoReturn

from pydantic import BaseModel, ConfigDict, ValidationError

from rquant.minute_backtest_artifact import MinuteSealedReplayResult
from rquant.minute_backtest_contracts import Sha256
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
from rquant.minute_historical_comparison import (
    HistoricalExecutionComparison,
    HistoricalRowMatch,
    HistoricalSideEvidence,
    compare_frozen_executions,
)
from rquant.minute_historical_reconstruction import (
    HistoricalReconstructionPreparation,
    HistoricalReconstructionRequest,
    prepare_historical_minute_reconstruction,
)
from rquant.strict_json import StrictJsonError, strict_json_loads

Action = Literal["prepare", "compare"]


class _CliModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HistoricalPrepareCommand(_CliModel):
    action: Literal["prepare"]
    request: HistoricalReconstructionRequest


class HistoricalCompareCommand(_CliModel):
    action: Literal["compare"]
    native_result: MinuteSealedReplayResult | MinuteRuntimeReplayResult
    new_evidence: HistoricalSideEvidence
    old_evidence: HistoricalSideEvidence
    matches: tuple[HistoricalRowMatch, ...]


class HistoricalCliError(_CliModel):
    code: Literal["invalid_json", "invalid_request", "input_unavailable", "output_unavailable"]
    detail: str
    field_path: str = ""


class HistoricalCliReceipt(_CliModel):
    action: Action
    status: Literal[
        "complete", "unavailable", "execution_incomplete", "blocked", "diagnostic_complete"
    ]
    input_sha256: Sha256 | None = None
    diagnostic_only: Literal[True] = True
    formal_history_passed: Literal[False] = False
    formal_authorization: Literal["not_assessed"] = "not_assessed"
    policy_installation: Literal["not_assessed"] = "not_assessed"
    preparation: HistoricalReconstructionPreparation | None = None
    comparison: HistoricalExecutionComparison | None = None
    errors: tuple[HistoricalCliError, ...] = ()


def _nonfinite_json(value: str) -> NoReturn:
    raise StrictJsonError(f"nonfinite JSON constant: {value}")


def _failure(
    action: Action,
    code: Literal["invalid_json", "input_unavailable", "output_unavailable"],
    detail: str,
    *,
    input_sha256: Sha256 | None = None,
) -> HistoricalCliReceipt:
    return HistoricalCliReceipt(
        action=action,
        status="unavailable",
        input_sha256=input_sha256,
        errors=(HistoricalCliError(code=code, detail=detail),),
    )


def _diagnose(action: Action, payload: bytes) -> HistoricalCliReceipt:
    input_hash = hashlib.sha256(payload).hexdigest()
    try:
        strict_json_loads(payload, parse_constant=_nonfinite_json)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        return _failure(action, "invalid_json", str(exc), input_sha256=input_hash)
    try:
        # Validate the original JSON, retaining exact numeric digits. The strict
        # decode above only rejects ambiguity; it does not rewrite frozen input.
        if action == "prepare":
            request = HistoricalPrepareCommand.model_validate_json(payload)
            preparation = prepare_historical_minute_reconstruction(request.request)
            return HistoricalCliReceipt(
                action=action,
                status=preparation.preparation_status,
                input_sha256=input_hash,
                preparation=preparation,
            )
        request = HistoricalCompareCommand.model_validate_json(payload)
        comparison = compare_frozen_executions(
            native_result=request.native_result,
            new_evidence=request.new_evidence,
            old_evidence=request.old_evidence,
            matches=request.matches,
        )
        return HistoricalCliReceipt(
            action=action, status=comparison.status, input_sha256=input_hash, comparison=comparison
        )
    except ValidationError as exc:
        errors = tuple(
            HistoricalCliError(
                code="invalid_request",
                detail=error["msg"],
                field_path=".".join(str(part) for part in error["loc"]),
            )
            for error in exc.errors(include_input=False, include_context=False, include_url=False)
        )
    except ValueError as exc:
        errors = (HistoricalCliError(code="invalid_request", detail=str(exc)),)
    return HistoricalCliReceipt(
        action=action, status="unavailable", input_sha256=input_hash, errors=errors
    )


def _emit(receipt: HistoricalCliReceipt, output: Path) -> int:
    payload = receipt.model_dump_json().encode("utf-8") + b"\n"
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(output, flags, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
    except OSError as exc:
        failed = HistoricalCliReceipt(
            action=receipt.action,
            status="unavailable",
            input_sha256=receipt.input_sha256,
            preparation=receipt.preparation,
            comparison=receipt.comparison,
            errors=(
                *receipt.errors,
                HistoricalCliError(code="output_unavailable", detail=str(exc)),
            ),
        )
        print(failed.model_dump_json())
        return 2
    print(payload.decode("utf-8"), end="")
    return (
        0 if receipt.status in {"complete", "diagnostic_complete"} else 2 if receipt.errors else 1
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rquant minute-historical", description="离线历史资料准备与冻结结果诊断。"
    )
    actions = parser.add_subparsers(dest="action", required=True)
    for name, help_text in (("prepare", "准备历史重建资料"), ("compare", "比较独立冻结的新旧结果")):
        command = actions.add_parser(name, help=help_text)
        command.add_argument(
            "--input", type=Path, required=True, help="包含原资料与明确引用的 JSON 请求"
        )
        command.add_argument("--output", type=Path, required=True, help="保存新的诊断 JSON 文件")
    args = parser.parse_args(argv)
    if args.output.exists():
        failed = _failure(args.action, "output_unavailable", "输出文件已存在；请选择新路径。")
        print(failed.model_dump_json())
        return 2
    try:
        payload = args.input.read_bytes()
    except OSError as exc:
        receipt = _failure(args.action, "input_unavailable", str(exc))
    else:
        receipt = _diagnose(args.action, payload)
    return _emit(receipt, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
