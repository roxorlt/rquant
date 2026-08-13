from __future__ import annotations

import multiprocessing
import os
import signal
import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    LabShardTable,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
)


class _BlockingReduceValue:
    def __init__(self, marker_path: Path) -> None:
        self._marker_path = marker_path

    def __reduce__(self) -> object:
        self._marker_path.write_text("child result reduce invoked", encoding="ascii")
        threading.Event().wait()
        raise AssertionError("unreachable")


_LegacyProfile = Literal["uint64_max", "table_context", "boundary_terminal_bytes"]


def _legacy_profile_frame(profile: _LegacyProfile) -> pd.DataFrame:
    if profile == "uint64_max":
        return pd.DataFrame({"hold_days": pd.Series([2**64 - 1], dtype="uint64")})
    if profile == "table_context":
        return pd.DataFrame(
            {
                "a\x00b": pd.Series([np.finfo(np.float16).tiny], dtype="float16"),
                "float32_rounding": pd.Series([np.float32(-394.478118896484375)], dtype="float32"),
                "legacy_bytes": pd.Series([b"\xc3"], dtype=object),
                "duration": pd.Series(
                    [pd.Timedelta("-1 days 23:56:21.971770440")], dtype="timedelta64[ns]"
                ),
            }
        )
    if profile == "boundary_terminal_bytes":
        return pd.DataFrame(
            {"legacy_bytes": pd.Series([b"x" * 4096 + bytes.fromhex("d0")], dtype=object)}
        )
    raise AssertionError(f"unknown legacy test profile: {profile}")


def _counter_increment(path_value: object) -> int:
    path = Path(str(path_value))
    current = int(path.read_text(encoding="ascii")) if path.exists() else 0
    current += 1
    path.write_text(str(current), encoding="ascii")
    return current


def _wait_for_release(path_value: object, *, timeout_seconds: float = 10) -> None:
    path = Path(str(path_value))
    deadline = time.monotonic() + timeout_seconds
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        raise TimeoutError("test shard fixture was not released")


def _spawn_descendant(path_value: object) -> int:
    path = Path(str(path_value))
    child_pid = os.fork()
    if child_pid == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
        os._exit(1)
    deadline = time.monotonic() + 2
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        raise TimeoutError("test shard descendant did not start")
    return child_pid


def prepare_lab_shard_fixture(configuration: object) -> None:
    assert isinstance(configuration, dict)
    session = configuration["session"]
    assert isinstance(session, dict)
    kind = session["kind"]
    if kind in {"record", "block"}:
        Path(str(session["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
    if kind == "block":
        _wait_for_release(session["release_path"], timeout_seconds=5)
    if kind == "failure":
        raise PermissionError("setsid denied")


def execute_lab_shard_fixture(
    configuration: object,
    validated: ValidatedStrategyShard,
    *,
    runtime_code_sha: str,
) -> LabShardExecutionResult:
    assert isinstance(configuration, dict)
    formal = configuration["formal"]
    if formal is not None:
        from rquant.research_gate import ResearchGateRequest

        assert isinstance(formal, dict)
        identity = validated.spec.dataset_snapshot
        if identity is None or configuration["lake_root"] is None:
            raise PermissionError("formal worker execution requires dataset snapshot and lake")
        if formal.get("unregistered") is True:
            raise LabDaemonConfigurationError("formal test store is not registered")
        if formal.get("binding_hash") != identity.binding_hash:
            raise PermissionError("formal dataset snapshot binding hash mismatch")
        registry = default_strategy_job_adapter_registry()
        strategy_name = registry.for_spec(validated.spec).snapshot_strategy_name
        if formal.get("snapshot_strategy_name") != strategy_name or formal.get("p0_count") != 0:
            raise PermissionError("formal research gate rejected fixture evidence")
        gate_request = ResearchGateRequest(
            mode="formal",
            strategy_name=strategy_name,
            start_date=validated.spec.parameters.start_date,
            end_date=validated.spec.parameters.end_date,
            audit_run_id=identity.audit_run_id,
            dataset_snapshot_id=identity.snapshot_id,
            dataset_binding_hash=identity.binding_hash,
            code_commit=runtime_code_sha,
        )
        if "request_path" in formal:
            Path(str(formal["request_path"])).write_text(
                gate_request.model_dump_json(), encoding="utf-8"
            )
        if "opened_path" in formal:
            Path(str(formal["opened_path"])).write_text("opened", encoding="ascii")
    store = configuration["store"]
    assert isinstance(store, dict)
    if store["kind"] == "slow":
        time.sleep(float(store["delay_seconds"]))
    adapter = configuration["adapter"]
    assert isinstance(adapter, dict)
    kind = adapter["kind"]
    if kind == "unregistered":
        raise LabDaemonConfigurationError(str(adapter["message"]))
    _counter_increment(configuration["counter_path"])
    if kind == "failure":
        raise RuntimeError(str(adapter["message"]))
    if kind == "recording":
        time.sleep(float(adapter["delay_seconds"]))
        failure = adapter["failure"]
        if isinstance(failure, dict):
            raise RuntimeError(str(failure["message"]))
    elif kind == "slow":
        Path(str(adapter["path"])).write_text(str(os.getpid()), encoding="ascii")
        time.sleep(float(adapter["delay_seconds"]))
    elif kind == "spawn-method":
        Path(str(adapter["path"])).write_text(multiprocessing.get_start_method(), encoding="ascii")
    elif kind == "hung":
        Path(str(adapter["path"])).write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
    elif kind == "blocking":
        Path(str(adapter["entered_path"])).write_text("entered", encoding="ascii")
        _wait_for_release(adapter["release_path"])
    elif kind == "deadline":
        Path(str(adapter["path"])).write_text("reached", encoding="ascii")
    elif kind == "malicious-result":
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame(
                        {
                            "payload": pd.Series(
                                [_BlockingReduceValue(Path(str(adapter["path"])))],
                                dtype=object,
                            )
                        }
                    ),
                ),
            ),
        )
    if adapter.get("artifact_profile") == "nshape_projection":
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="summary",
                    frame=pd.DataFrame(
                        [
                            {
                                "entry_mode": "first_break",
                                "profile_variant": "baseline",
                                "candidates": 1,
                                "trades": 1,
                                "trigger_rate_pct": 100.0,
                                "mean_ret_pct": 2.0,
                                "median_ret_pct": 2.0,
                                "win_rate_pct": 100.0,
                                "best_ret_pct": 2.0,
                                "worst_ret_pct": 2.0,
                                "gap_stop_rate_pct": 0.0,
                            }
                        ]
                    ),
                ),
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame(
                        [
                            {
                                "entry_mode": "first_break",
                                "profile_variant": "baseline",
                                "signal_date": date(2026, 6, 30),
                                "ts_code": "600000.SH",
                                "name": "PF Bank",
                                "entry_time": datetime(2026, 6, 30, 1, 31, tzinfo=UTC),
                                "entry_price_raw": 10.0,
                                "entry_price": 10.0,
                                "stop_loss_basis": 9.5,
                                "take_profit_basis": 11.0,
                                "volume_profile_lookbacks": "90",
                                "volume_profile_rr": 2.0,
                                "exit_time": datetime(2026, 6, 30, 7, 0, tzinfo=UTC),
                                "exit_price": 10.2,
                                "exit_reason": "close",
                                "ret_pct": 2.0,
                            }
                        ]
                    ),
                ),
            ),
        )
    if kind in {"sigterm-tree", "term-exit-tree", "successful-tree"}:
        if kind == "sigterm-tree":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        elif kind == "term-exit-tree":
            signal.signal(signal.SIGTERM, lambda *_args: os._exit(0))
        _spawn_descendant(adapter["descendant_path"])
        Path(str(adapter["path"])).write_text(str(os.getpid()), encoding="ascii")
        if kind != "successful-tree":
            threading.Event().wait()
    profile = adapter.get("legacy_profile")
    if profile is not None:
        if profile not in {"uint64_max", "table_context", "boundary_terminal_bytes"}:
            raise LabDaemonConfigurationError("legacy test profile is not registered")
        frame = _legacy_profile_frame(profile)
    else:
        frame = pd.DataFrame(
            [
                {
                    "hold_days": getattr(validated.shard, "hold_days", 1),
                    "ret_pct": 1.25,
                }
            ]
        )
    return LabShardExecutionResult.from_validated(
        validated,
        tables=(
            LabShardTable(
                name="trades",
                frame=frame,
            ),
        ),
    )
