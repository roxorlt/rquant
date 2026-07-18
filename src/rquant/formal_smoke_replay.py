"""Formal-only fixed Stage 1 strategy smoke replays."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, computed_field, model_validator

from rquant.auction_gap_strategy import AuctionGapMinuteReplayConfig
from rquant.dashboard.strategy_lab_runs import (
    _canonical_json_bytes,
    _hash_json_value,
    _strategy_spec_hash,
    build_strategy_lab_run,
    save_strategy_lab_run,
)
from rquant.growth_board_surge_strategy import GrowthBoardSurgeConfig
from rquant.minute_replay import DEFAULT_FACTOR_SCORE_THRESHOLD
from rquant.paper import PaperTradeConfig
from rquant.research_gate import (
    ResearchGateRequest,
    build_gate_research_manifest,
    open_gated_research_store,
)

FormalSmokeStrategy = Literal[
    "n_shape",
    "growth_board_surge",
    "auction_gap",
]
FormalSmokeRunType = Literal[
    "n_shape_compare",
    "growth_board_surge",
    "auction_gap",
]
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_COMMIT_PATTERN = r"^[0-9a-f]{40}$"


class FormalSmokeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    fixed_spec_version: Literal["stage1-smoke-v1"] = "stage1-smoke-v1"
    strategy: FormalSmokeStrategy
    run_type: FormalSmokeRunType
    start_date: date
    end_date: date
    parameters: dict[str, Any]

    @model_validator(mode="after")
    def validate_range(self) -> FormalSmokeSpec:
        if self.start_date > self.end_date:
            raise ValueError("formal smoke start_date cannot be after end_date")
        return self

    @computed_field
    @property
    def spec_hash(self) -> str:
        return _strategy_spec_hash(
            self.run_type,
            self.run_parameters,
        )

    @property
    def run_parameters(self) -> dict[str, Any]:
        return {
            "fixed_spec_version": self.fixed_spec_version,
            "strategy": self.strategy,
            "start_date": self.start_date,
            "end_date": self.end_date,
            **self.parameters,
        }


class FormalSmokeReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy: FormalSmokeStrategy
    start_date: date
    end_date: date
    audit_run_id: str
    dataset_snapshot_id: str
    dataset_binding_hash: str
    code_commit: str

    @model_validator(mode="after")
    def validate_request(self) -> FormalSmokeReplayRequest:
        if self.start_date > self.end_date:
            raise ValueError("formal smoke start_date cannot be after end_date")
        import re

        for field_name in (
            "audit_run_id",
            "dataset_snapshot_id",
            "dataset_binding_hash",
        ):
            if re.fullmatch(_HASH_PATTERN, getattr(self, field_name)) is None:
                raise ValueError(
                    f"formal smoke {field_name} must be a 64-character hash"
                )
        if re.fullmatch(_COMMIT_PATTERN, self.code_commit) is None:
            raise ValueError(
                "formal smoke code_commit must be a clean 40-character commit"
            )
        return self


class FormalSmokeComputation(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    metrics: dict[str, Any]
    tables: dict[str, pd.DataFrame]
    sample_count: int


class FormalSmokeReplayResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["comparable"] = "comparable"
    strategy: FormalSmokeStrategy
    fixed_spec_version: Literal["stage1-smoke-v1"]
    run_id: str
    audit_run_id: str
    dataset_snapshot_id: str
    dataset_binding_hash: str
    code_commit: str
    strategy_spec_hash: str
    result_hash: str
    sample_count: int
    metrics: dict[str, Any]
    missing_evidence: tuple[str, ...]
    json_path: Path
    markdown_path: Path


def _n_shape_parameters() -> dict[str, Any]:
    return {
        "preset_name": "n-shape-combined",
        "entry_modes": ["first_break"],
        "profile_variants": ["baseline"],
        "max_hold_days": 1,
        "freq": "1min",
        "factor_score_threshold": DEFAULT_FACTOR_SCORE_THRESHOLD,
        "paper": PaperTradeConfig().model_dump(mode="python"),
    }


def _growth_parameters() -> dict[str, Any]:
    return GrowthBoardSurgeConfig(
        lookback_days=20,
        min_hist_days=10,
        min_cum_amount_ratio=1.4,
        min_same_minute_amount_ratio=2.0,
        min_amount_accel_5m=2.0,
        require_vwap_strength=True,
        use_same_minute_surge=True,
        use_accel_surge=True,
        max_hold_days=1,
    ).model_dump(mode="python")


def _auction_parameters(
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    return AuctionGapMinuteReplayConfig(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        gap_mode="close",
        st_filter="case_insensitive",
        min_auction_vol_ratio_5d=0.15,
        max_auction_vol_ratio_5d=5.0,
        max_hold_days=1,
    ).model_dump(
        mode="python",
        exclude={"start_date", "end_date"},
    )


def build_formal_smoke_spec(
    strategy: str,
    *,
    start_date: date,
    end_date: date,
) -> FormalSmokeSpec:
    if strategy == "n_shape":
        run_type: FormalSmokeRunType = "n_shape_compare"
        parameters = _n_shape_parameters()
    elif strategy == "growth_board_surge":
        run_type = "growth_board_surge"
        parameters = _growth_parameters()
    elif strategy == "auction_gap":
        run_type = "auction_gap"
        parameters = _auction_parameters(start_date, end_date)
    else:
        raise ValueError(f"unsupported formal smoke strategy: {strategy}")
    return FormalSmokeSpec(
        strategy=strategy,
        run_type=run_type,
        start_date=start_date,
        end_date=end_date,
        parameters=parameters,
    )


def _execute_formal_smoke_spec(
    store: object,
    spec: FormalSmokeSpec,
) -> FormalSmokeComputation:
    if spec.strategy == "n_shape":
        return _execute_n_shape(store, spec)
    if spec.strategy == "growth_board_surge":
        return _execute_growth_board_surge(store, spec)
    if spec.strategy == "auction_gap":
        return _execute_auction_gap(store, spec)
    raise ValueError(
        f"unsupported formal smoke strategy: {spec.strategy}"
    )


def _return_metrics(
    trades: pd.DataFrame,
) -> tuple[float | None, float | None]:
    if "ret_pct" not in trades.columns:
        return None, None
    returns = pd.to_numeric(trades["ret_pct"], errors="coerce").dropna()
    if returns.empty:
        return None, None
    return (
        round(float(returns.mean()), 4),
        round(float((returns > 0).mean() * 100), 4),
    )


def _canonicalize_formal_tables(
    tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    canonical: dict[str, pd.DataFrame] = {}
    for name, frame in tables.items():
        if len(frame) < 2:
            canonical[name] = frame.reset_index(drop=True)
            continue
        row_keys = [
            _canonical_json_bytes(_hash_json_value(row))
            for row in frame.itertuples(index=False, name=None)
        ]
        order = sorted(range(len(frame)), key=row_keys.__getitem__)
        canonical[name] = frame.iloc[order].reset_index(drop=True)
    return canonical


def _execute_n_shape(
    store: object,
    spec: FormalSmokeSpec,
) -> FormalSmokeComputation:
    from rquant.strategy_compare import run_entry_mode_comparison

    parameters = spec.parameters
    result = run_entry_mode_comparison(
        store,
        start_date=spec.start_date,
        end_date=spec.end_date,
        entry_modes=list(parameters["entry_modes"]),
        profile_variants=list(parameters["profile_variants"]),
        preset_name=str(parameters["preset_name"]),
        max_hold_days=int(parameters["max_hold_days"]),
        freq=str(parameters["freq"]),
        paper_config=PaperTradeConfig.model_validate(parameters["paper"]),
        factor_score_threshold=float(
            parameters["factor_score_threshold"]
        ),
    )
    mean_ret_pct, win_rate_pct = _return_metrics(result.trades)
    return FormalSmokeComputation(
        metrics={
            "candidate_count": result.candidates_count,
            "trade_count": len(result.trades),
            "mean_ret_pct": mean_ret_pct,
            "win_rate_pct": win_rate_pct,
        },
        tables={
            "strategy_summary": result.summary,
            "trades": result.trades,
        },
        sample_count=len(result.trades),
    )


def _execute_growth_board_surge(
    store: object,
    spec: FormalSmokeSpec,
) -> FormalSmokeComputation:
    from rquant.dashboard.strategy_lab_data import growth_board_metric_rows
    from rquant.growth_board_surge_strategy import (
        run_growth_board_surge_replay,
    )

    config = GrowthBoardSurgeConfig.model_validate(spec.parameters)
    trades = run_growth_board_surge_replay(
        store,
        start_date=spec.start_date,
        end_date=spec.end_date,
        config=config,
    )
    summary = growth_board_metric_rows(trades)
    mean_ret_pct, win_rate_pct = _return_metrics(trades)
    return FormalSmokeComputation(
        metrics={
            "trade_count": len(trades),
            "mean_ret_pct": mean_ret_pct,
            "win_rate_pct": win_rate_pct,
        },
        tables={
            "strategy_summary": summary,
            "trades": trades,
        },
        sample_count=len(trades),
    )


def _execute_auction_gap(
    store: object,
    spec: FormalSmokeSpec,
) -> FormalSmokeComputation:
    from rquant.auction_gap_strategy import (
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
    )
    from rquant.dashboard.strategy_lab_data import auction_gap_metric_rows

    config = AuctionGapMinuteReplayConfig(
        start_date=spec.start_date.isoformat(),
        end_date=spec.end_date.isoformat(),
        **spec.parameters,
    )
    baseline = run_auction_gap_replay(store, config.auction_config())
    trades = run_auction_gap_minute_replay(
        store,
        config,
        persist_positions=False,
        candidates=baseline,
    )
    comparison = auction_gap_metric_rows(baseline, trades)
    mean_ret_pct, win_rate_pct = _return_metrics(trades)
    return FormalSmokeComputation(
        metrics={
            "candidate_count": len(baseline),
            "trade_count": len(trades),
            "mean_ret_pct": mean_ret_pct,
            "win_rate_pct": win_rate_pct,
        },
        tables={
            "strategy_comparison": comparison,
            "baseline_candidates": baseline,
            "trades": trades,
        },
        sample_count=len(trades),
    )


def _verify_gate_evidence(
    request: FormalSmokeReplayRequest,
    decision: object,
) -> None:
    if not decision.allowed or decision.research_status != "comparable":
        raise PermissionError(
            "formal smoke gate did not return comparable research status"
        )
    expected = {
        "audit_run_id": request.audit_run_id,
        "dataset_snapshot_id": request.dataset_snapshot_id,
        "dataset_binding_hash": request.dataset_binding_hash,
    }
    for field_name, expected_value in expected.items():
        if getattr(decision, field_name) != expected_value:
            raise PermissionError(
                f"formal smoke gate {field_name} does not match request"
            )


def run_formal_smoke_replay(
    request: FormalSmokeReplayRequest,
    *,
    base_dir: Path | None = None,
) -> FormalSmokeReplayResult:
    spec = build_formal_smoke_spec(
        request.strategy,
        start_date=request.start_date,
        end_date=request.end_date,
    )
    gate_request = ResearchGateRequest(
        mode="formal",
        strategy_name=request.strategy,
        start_date=request.start_date,
        end_date=request.end_date,
        audit_run_id=request.audit_run_id,
        dataset_snapshot_id=request.dataset_snapshot_id,
        dataset_binding_hash=request.dataset_binding_hash,
        code_commit=request.code_commit,
    )
    with open_gated_research_store(gate_request) as (store, decision):
        _verify_gate_evidence(request, decision)
        computation = _execute_formal_smoke_spec(store, spec)

    if "formal_evidence" in computation.tables:
        raise ValueError("formal smoke computation uses reserved evidence table")
    evidence = pd.DataFrame(
        [
            {
                "audit_run_id": request.audit_run_id,
                "dataset_snapshot_id": request.dataset_snapshot_id,
                "dataset_binding_hash": request.dataset_binding_hash,
                "code_commit": request.code_commit,
            }
        ]
    )
    tables = _canonicalize_formal_tables({
        **computation.tables,
        "formal_evidence": evidence,
    })
    manifest = build_gate_research_manifest(gate_request, decision)
    title = (
        f"Stage 1 {request.strategy} formal smoke "
        f"{request.start_date.isoformat()} to {request.end_date.isoformat()}"
    )
    saved = save_strategy_lab_run(
        build_strategy_lab_run(
            run_type=spec.run_type,
            title=title,
            params=spec.run_parameters,
            metrics=computation.metrics,
            tables=tables,
            manifest=manifest,
        ),
        base_dir=base_dir,
    )
    strategy_spec_hash = saved.manifest.strategy_spec_hash
    result_hash = saved.manifest.result_hash
    if strategy_spec_hash != spec.spec_hash:
        raise RuntimeError("formal smoke persisted strategy spec hash mismatch")
    if result_hash is None:
        raise RuntimeError("formal smoke persisted result hash is missing")
    if saved.json_path is None or saved.markdown_path is None:
        raise RuntimeError("formal smoke persisted result paths are missing")
    return FormalSmokeReplayResult(
        strategy=request.strategy,
        fixed_spec_version=spec.fixed_spec_version,
        run_id=saved.run_id,
        audit_run_id=request.audit_run_id,
        dataset_snapshot_id=request.dataset_snapshot_id,
        dataset_binding_hash=request.dataset_binding_hash,
        code_commit=request.code_commit,
        strategy_spec_hash=strategy_spec_hash,
        result_hash=result_hash,
        sample_count=computation.sample_count,
        metrics=saved.metrics,
        missing_evidence=tuple(saved.manifest.missing_evidence),
        json_path=saved.json_path,
        markdown_path=saved.markdown_path,
    )
