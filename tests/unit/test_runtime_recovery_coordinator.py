from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import resource
import shutil
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import tracemalloc
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.executable_dependencies import (
    DependencyFingerprintLimits,
    ExecutableBinding,
    ExecutableDependencyError,
    capture_executable_dependency_guard,
    fingerprint_dependency_value,
)
from rquant.recovery_manifest import (
    RecoveryArtifactRole,
    RecoveryFaultPoint,
    RecoveryInventoryPlan,
    RecoveryInventoryRequirement,
    RecoveryWatermarkSummary,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_production_profile import ProductionStrategyBinding
from rquant.runtime_recovery_coordinator import (
    RecoveryArtifactVerificationContext,
    RecoveryAuthorityExpectation,
    RecoveryPlane,
    RuntimeRecoveryCoordinator,
    RuntimeRecoveryCoordinatorError,
    RuntimeRecoveryFixedReplayExpectation,
    RuntimeRecoveryFixedReplayVerifier,
    append_recovery_authority_manifest,
    build_recovery_authority_manifest,
    build_runtime_recovery_fixed_replay_expectations,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
TOPOLOGY_ID = canonical_sha256({"deployment": "mixed-v1"})
DEPLOYMENT_PROFILE_ID = canonical_sha256({"profile": "runtime-v1"})
DEPLOYMENT_PROFILE_GENERATION = canonical_sha256({"generation": "runtime-v1"})
STRATEGY_REGISTRATION_FINGERPRINTS = {
    strategy_id: canonical_sha256({"registered_strategy": strategy_id, "version": 1})
    for strategy_id in ("n_shape", "growth_board_surge", "auction_gap")
}
AS_OF = datetime(2026, 7, 31, 7, 30, tzinfo=UTC)


class _StatefulPolicy:
    def __init__(self, decision: str) -> None:
        self.decision = decision

    def allows(self) -> bool:
        return self.decision == "allow"


_STATEFUL_POLICY = _StatefulPolicy("allow")


def _stateful_policy_probe() -> bool:
    return _STATEFUL_POLICY.allows()


class _CountingItemsMapping(Mapping[str, int]):
    def __init__(self, size: int) -> None:
        self.size = size
        self.consumed = 0

    def __getitem__(self, key: str) -> int:
        return int(key)

    def __iter__(self) -> Iterator[str]:
        return (str(index) for index in range(self.size))

    def __len__(self) -> int:
        return self.size

    def items(self) -> Iterator[tuple[str, int]]:
        for index in range(self.size):
            self.consumed += 1
            yield str(index), index


_BOUNDED_MAPPING_DEPENDENCY = _CountingItemsMapping(1)


def _bounded_mapping_probe() -> int | None:
    return _BOUNDED_MAPPING_DEPENDENCY.get("0")


_CAPTURE_PROPERTY_READS = 0


class _CapturePropertyOwner:
    @property
    def policy(self) -> str:
        global _CAPTURE_PROPERTY_READS
        _CAPTURE_PROPERTY_READS += 1
        return "allow"


_CAPTURE_PROPERTY_OWNER = _CapturePropertyOwner()


def _capture_property_probe() -> str:
    return _CAPTURE_PROPERTY_OWNER.policy


_CALLABLE_DESCRIPTOR_READS = 0


class _CallableModuleDescriptor:
    @property
    def __module__(self) -> str:
        global _CALLABLE_DESCRIPTOR_READS
        _CALLABLE_DESCRIPTOR_READS += 1
        return "side_effect_module"

    def __call__(self) -> str:
        return "allow"


_CALLABLE_MODULE_DESCRIPTOR = _CallableModuleDescriptor()


def _callable_module_descriptor_probe() -> str:
    return _CALLABLE_MODULE_DESCRIPTOR()


_REVERIFY_PROPERTY_READS = 0


class _ReverifyPropertyOwner:
    policy = "allow"


_REVERIFY_PROPERTY_OWNER = _ReverifyPropertyOwner()


def _reverify_property_probe() -> str:
    return _REVERIFY_PROPERTY_OWNER.policy


def _counting_reverify_property(_owner: object) -> str:
    global _REVERIFY_PROPERTY_READS
    _REVERIFY_PROPERTY_READS += 1
    return "deny"


def _strategy_bindings() -> tuple[ProductionStrategyBinding, ...]:
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT_A)
    return tuple(
        ProductionStrategyBinding(
            strategy_id=strategy_id,
            strategy_version=1,
            registration_fingerprint=STRATEGY_REGISTRATION_FINGERPRINTS[strategy_id],
            candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
            strategy_spec_fingerprint=definition.spec.spec_fingerprint,
            executable_fingerprint=definition.executable_fingerprint,
        )
        for strategy_id in ("auction_gap", "growth_board_surge", "n_shape")
        for definition in (registry.load_definition(strategy_id, 1),)
    )


def _send_spawned_n_shape_fingerprint(connection: object) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    sender = connection
    try:
        sender.send(recovery_module._n_shape_strategy_executable_sha256())
    finally:
        sender.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _file_set_hashes(directory: Path) -> dict[str, str]:
    return {path.name: _sha256(path) for path in sorted(directory.glob("*.json"))}


ROLE_VERIFIERS: dict[RecoveryArtifactRole, object] = {}


def _requirements() -> tuple[tuple[str, RecoveryArtifactRole, RecoveryPlane], ...]:
    return (
        ("production.duckdb", RecoveryArtifactRole.PRODUCTION_DUCKDB, RecoveryPlane.DATA),
        ("live.state", RecoveryArtifactRole.SQLITE_STATE, RecoveryPlane.LIVE),
        ("research.catalog", RecoveryArtifactRole.RESEARCH_CATALOG, RecoveryPlane.RESEARCH),
        ("research.lake", RecoveryArtifactRole.LAKE_MANIFEST, RecoveryPlane.RESEARCH),
        (
            "research.artifacts",
            RecoveryArtifactRole.ARTIFACT_METADATA,
            RecoveryPlane.RESEARCH,
        ),
        ("serving.current", RecoveryArtifactRole.SERVING_CURRENT, RecoveryPlane.SERVING),
        ("serving.manifest", RecoveryArtifactRole.SERVING_MANIFEST, RecoveryPlane.SERVING),
    )


def _write_json_artifact(path: Path, *, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"max_date": "2026-07-31", "row_count": rows},
            separators=(",", ":"),
        )
        + "\n"
    )


def _write_duckdb_artifact(path: Path, *, rows: int) -> None:
    from rquant.storage.duckdb import DuckDBStore

    del rows
    path.parent.mkdir(parents=True, exist_ok=True)
    store = DuckDBStore(path)
    try:
        store.upsert_daily(
            pd.DataFrame(
                [
                    {
                        "ts_code": "600000.SH",
                        "trade_date": date(2026, 6, 24),
                        "open": 9.8,
                        "high": 10.2,
                        "low": 9.7,
                        "close": 10.0,
                        "pre_close": 9.8,
                        "change": 0.2,
                        "pct_chg": 2.04,
                        "vol": 1,
                        "amount": 1,
                    },
                    {
                        "ts_code": "600000.SH",
                        "trade_date": date(2026, 6, 25),
                        "open": 10.0,
                        "high": 10.6,
                        "low": 10.0,
                        "close": 10.5,
                        "pre_close": 10.0,
                        "change": 0.5,
                        "pct_chg": 5.0,
                        "vol": 1,
                        "amount": 1,
                    },
                    {
                        "ts_code": "600000.SH",
                        "trade_date": date(2026, 6, 26),
                        "open": 10.5,
                        "high": 10.88,
                        "low": 10.4,
                        "close": 10.6,
                        "pre_close": 10.5,
                        "change": 0.1,
                        "pct_chg": 0.95,
                        "vol": 1,
                        "amount": 1,
                    },
                    {
                        "ts_code": "000002.SZ",
                        "trade_date": date(2026, 7, 31),
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "pre_close": 1.0,
                        "change": 0.0,
                        "pct_chg": 0.0,
                        "vol": 1,
                        "amount": 1,
                    },
                ]
            )
        )
        store.upsert_screen_result(
            pd.DataFrame(
                [
                    {
                        "trade_date": date(2026, 6, 24),
                        "preset_name": "n-shape-pool1",
                        "ts_code": "600000.SH",
                        "name": "fixture",
                        "close": 10.0,
                        "pct_chg": 2.04,
                        "extra": None,
                    }
                ]
            )
        )
        store.upsert_minute_bars(
            pd.DataFrame(
                [
                    {
                        "ts_code": "600000.SH",
                        "trade_time": trade_time,
                        "freq": "1min",
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "close": close,
                        "vol": 10_000,
                        "amount": close * 10_000,
                        "source": "fixture",
                    }
                    for trade_time, open_price, high, low, close in (
                        (datetime(2026, 6, 25, 9, 30), 10.00, 10.10, 10.00, 10.05),
                        (datetime(2026, 6, 25, 9, 31), 10.05, 10.18, 10.03, 10.16),
                        (datetime(2026, 6, 25, 9, 32), 10.16, 10.26, 10.15, 10.24),
                        (datetime(2026, 6, 25, 9, 33), 10.24, 10.30, 10.22, 10.28),
                        (datetime(2026, 6, 26, 9, 30), 10.50, 10.88, 10.50, 10.76),
                        (datetime(2026, 6, 26, 9, 31), 10.76, 10.78, 10.58, 10.62),
                    )
                ]
            )
        )
        store._conn.execute("CHECKPOINT")
    finally:
        store.close()


def _real_strategy_replay_result(path: Path, strategy_id: str) -> dict[str, object]:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import formal_smoke_replay
    from rquant.storage.duckdb import DuckDBStore

    spec = formal_smoke_replay.build_formal_smoke_spec(
        strategy_id,
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    )
    store = DuckDBStore(path, read_only=True)
    try:
        computation = formal_smoke_replay._execute_formal_smoke_spec(store, spec)
    finally:
        store.close()
    trades = computation.tables.get("trades", pd.DataFrame())
    returns_bps = [
        int(
            (Decimal(str(value)) * Decimal(100)).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_EVEN,
            )
        )
        for value in trades.get("ret_pct", pd.Series(dtype=float))
    ]
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for value in returns_bps:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    executable_sha256 = recovery_module._strategy_fixed_replay_executable_sha256(strategy_id)
    strategy_binding = next(
        item for item in _strategy_bindings() if item.strategy_id == strategy_id
    )
    return {
        "dataset_snapshot_id": _sha256(path),
        "strategy_registration_fingerprint": strategy_binding.registration_fingerprint,
        "strategy_definition_sha256": spec.spec_hash,
        "strategy_executable_sha256": executable_sha256,
        "engine_version": "rquant.formal-smoke.stage1-smoke-v1",
        "start_date": "2026-06-24",
        "end_date": "2026-06-24",
        "trade_count": len(returns_bps),
        "winning_trade_count": sum(value > 0 for value in returns_bps),
        "total_return_bps": sum(returns_bps),
        "max_drawdown_bps": max_drawdown,
    }


def _real_n_shape_replay_result(path: Path) -> dict[str, object]:
    return _real_strategy_replay_result(path, "n_shape")


def _write_replay_metadata(artifacts: dict[str, Path]) -> None:
    metadata_path = artifacts["research.artifacts"]
    replay_evidence = []
    for strategy_id in ("auction_gap", "growth_board_surge", "n_shape"):
        replay_result = _real_strategy_replay_result(
            artifacts["production.duckdb"],
            strategy_id,
        )
        replay_result_sha256 = _fixed_replay_result_sha256(replay_result)
        replay_evidence.append(
            {
                "strategy_id": strategy_id,
                "expected_result_sha256": replay_result_sha256,
                "result": {
                    **replay_result,
                    "result_sha256": replay_result_sha256,
                },
                "status": "passed",
            }
        )
    metadata_path.write_text(
        json.dumps(
            {
                "max_date": "2026-07-31",
                "row_count": 5,
                "references": [
                    {
                        "logical_role": "production.duckdb",
                        "sha256": _sha256(artifacts["production.duckdb"]),
                    }
                ],
                "fixed_replays": replay_evidence,
            },
            separators=(",", ":"),
        )
        + "\n"
    )


def _fixed_replay_result_sha256(result: dict[str, object]) -> str:
    normalized = dict(result)
    for field_name in ("start_date", "end_date"):
        value = normalized[field_name]
        if isinstance(value, str):
            normalized[field_name] = date.fromisoformat(value)
    normalized.pop("result_sha256", None)
    return canonical_sha256(normalized)


def _fixed_replay_evidence(
    payload: dict[str, object],
    strategy_id: str,
) -> dict[str, object]:
    return next(item for item in payload["fixed_replays"] if item["strategy_id"] == strategy_id)


def _runtime_replay_expectations(
    artifacts: Mapping[str, Path],
) -> tuple[RuntimeRecoveryFixedReplayExpectation, ...]:
    payload = json.loads(artifacts["research.artifacts"].read_text(encoding="utf-8"))
    return tuple(
        RuntimeRecoveryFixedReplayExpectation.model_validate(item)
        for item in payload["fixed_replays"]
    )


def _write_sqlite_artifact(path: Path, *, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        connection.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, trade_date TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO events(id, trade_date) VALUES (?, ?)",
            [(index, "2026-07-31") for index in range(1, rows + 1)],
        )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        connection.close()
    assert not path.with_name(f"{path.name}-wal").exists()


def _write_parquet_artifact(path: Path, *, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        escaped_path = str(path).replace("'", "''")
        connection.execute(
            "COPY (SELECT range AS id, DATE '2026-07-31' AS trade_date "
            f"FROM range(?)) TO '{escaped_path}' (FORMAT PARQUET)",
            [rows],
        )
    finally:
        connection.close()


def _fixture(
    tmp_path: Path,
    *,
    mixed_commits: bool = False,
) -> tuple[
    RuntimeRecoveryCoordinator,
    tuple[RecoveryAuthorityExpectation, ...],
    dict[str, Path],
    Path,
    Path,
]:
    sources_root = tmp_path / "sources"
    authority_root = tmp_path / "authority-manifests"
    manifest_store = tmp_path / "recovery-manifests"
    object_store = tmp_path / "backup-objects"
    authority_root.mkdir(parents=True)
    artifacts: dict[str, Path] = {}

    for index, (logical_role, artifact_role, plane) in enumerate(_requirements(), start=1):
        suffix = ".sqlite3" if artifact_role is RecoveryArtifactRole.SQLITE_STATE else ".bin"
        if artifact_role is RecoveryArtifactRole.PRODUCTION_DUCKDB:
            suffix = ".duckdb"
        if artifact_role is RecoveryArtifactRole.LAKE_MANIFEST:
            suffix = ".parquet"
        artifact = sources_root / plane.value / f"{logical_role}{suffix}"
        if artifact_role is RecoveryArtifactRole.PRODUCTION_DUCKDB:
            _write_duckdb_artifact(artifact, rows=index)
        elif artifact_role is RecoveryArtifactRole.SQLITE_STATE:
            _write_sqlite_artifact(artifact, rows=index)
        elif artifact_role is RecoveryArtifactRole.LAKE_MANIFEST:
            _write_parquet_artifact(artifact, rows=index)
        else:
            _write_json_artifact(artifact, rows=index)
        artifacts[logical_role] = artifact

    _write_replay_metadata(artifacts)

    requirements: list[RecoveryInventoryRequirement] = []
    expectations: list[RecoveryAuthorityExpectation] = []
    for index, (logical_role, artifact_role, plane) in enumerate(_requirements(), start=1):
        artifact = artifacts[logical_role]
        watermark = RecoveryWatermarkSummary(
            max_date=date(2026, 7, 31),
            row_count=4 if artifact_role is RecoveryArtifactRole.PRODUCTION_DUCKDB else index,
        )
        producer_commit = COMMIT_B if mixed_commits and index % 2 == 0 else COMMIT_A
        authority = build_recovery_authority_manifest(
            plane=plane,
            logical_role=logical_role,
            artifact_role=artifact_role,
            artifact_path=artifact,
            producer_commit=producer_commit,
            generation_id=f"generation-{index}",
            schema_version="v1",
            available_at=AS_OF - timedelta(minutes=index),
            watermark=watermark,
        )
        authority_path = append_recovery_authority_manifest(authority_root, authority)
        requirements.append(
            RecoveryInventoryRequirement(
                logical_role=logical_role,
                artifact_role=artifact_role,
                restore_path=f"{plane.value}/{artifact.name}",
            )
        )
        expectations.append(
            RecoveryAuthorityExpectation(
                plane=plane,
                logical_role=logical_role,
                artifact_role=artifact_role,
                authority_manifest_path=str(authority_path),
                allowed_root=str(sources_root / plane.value),
                expected_producer_commit=producer_commit,
                expected_generation_id=f"generation-{index}",
            )
        )

    coordinator = RuntimeRecoveryCoordinator(
        inventory_plan=RecoveryInventoryPlan(
            plan_version=1,
            requirements=tuple(requirements),
        ),
        manifest_store=manifest_store,
        backup_object_store=object_store,
        deployment_topology_id=TOPOLOGY_ID,
        deployment_profile_id=DEPLOYMENT_PROFILE_ID,
        deployment_profile_generation=DEPLOYMENT_PROFILE_GENERATION,
        strategy_producer_commit=COMMIT_A,
        strategy_bindings=_strategy_bindings(),
    )
    return coordinator, tuple(expectations), artifacts, sources_root, authority_root


def _measure_recovery_run_in_spawned_process(
    coordinator: RuntimeRecoveryCoordinator,
    expectations: tuple[RecoveryAuthorityExpectation, ...],
    connection: object,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    sender = connection
    try:
        baseline_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_scale = 1 if sys.platform == "darwin" else 1024
        rss_checkpoints: dict[str, int] = {}
        real_call_verifier = recovery_module._call_verifier

        def measured_call_verifier(*args, **kwargs):
            result = real_call_verifier(*args, **kwargs)
            context = args[0]
            current_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_checkpoints[context.logical_role] = max(0, current_rss - baseline_rss) * rss_scale
            return result

        recovery_module._call_verifier = measured_call_verifier
        tracemalloc.start()
        coordinator.run_once(expectations=expectations, as_of=AS_OF)
        _, python_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        sender.send(
            {
                "python_peak": python_peak,
                "rss_delta": max(0, peak_rss - baseline_rss) * rss_scale,
                "rss_checkpoints": rss_checkpoints,
            }
        )
    except BaseException as exc:
        sender.send({"error": f"{type(exc).__name__}: {exc}"})
        raise
    finally:
        sender.close()


def test_n_shape_executable_fingerprint_is_stable_across_fresh_processes() -> None:
    command = (
        "import rquant.runtime_recovery_coordinator as module; "
        "print(module._n_shape_strategy_executable_sha256())"
    )

    observed = {
        subprocess.check_output(
            [sys.executable, "-c", command],
            text=True,
        ).strip()
        for _ in range(3)
    }

    assert len(observed) == 1


def test_parent_and_fresh_spawn_child_use_identical_n_shape_fingerprint() -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_send_spawned_n_shape_fingerprint,
        args=(child_connection,),
    )
    parent_fingerprint = recovery_module._n_shape_strategy_executable_sha256()

    process.start()
    child_connection.close()
    try:
        assert parent_connection.poll(30), "spawned fingerprint probe timed out"
        child_fingerprint = parent_connection.recv()
        process.join(timeout=10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
        parent_connection.close()

    assert child_fingerprint == parent_fingerprint


def test_run_once_seals_objects_and_source_deletion_keeps_preflight_valid(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, sources_root, authority_root = _fixture(tmp_path)

    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    shutil.rmtree(sources_root)
    shutil.rmtree(authority_root)

    result = coordinator.restore_preflight(
        manifest_id=str(manifest.manifest_id),
        expected_as_of=AS_OF,
    )

    assert result.passed is True
    assert result.deployment_topology_id == TOPOLOGY_ID
    assert all(
        Path(receipt.artifact_path).parent == coordinator.backup_object_store
        for receipt in manifest.authorities
    )
    assert all(
        Path(receipt.artifact_path).name == f"{receipt.artifact_sha256}.blob"
        for receipt in manifest.authorities
    )


def test_manifest_current_and_audit_bind_profile_generation_and_three_replays(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert manifest.deployment_profile_id == DEPLOYMENT_PROFILE_ID
    assert manifest.deployment_profile_generation == DEPLOYMENT_PROFILE_GENERATION
    assert {item.strategy_id for item in manifest.strategy_lineage} == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }
    assert all(item.fixed_replay_fingerprint for item in manifest.strategy_lineage)
    expected_bindings = {item.strategy_id: item for item in _strategy_bindings()}
    assert all(
        item.registration_fingerprint
        == expected_bindings[item.strategy_id].registration_fingerprint
        and item.candidate_schema_fingerprint
        == expected_bindings[item.strategy_id].candidate_schema_fingerprint
        and item.strategy_spec_fingerprint
        == expected_bindings[item.strategy_id].strategy_spec_fingerprint
        and item.executable_fingerprint
        == expected_bindings[item.strategy_id].executable_fingerprint
        for item in manifest.strategy_lineage
    )
    assert all(item.fixed_replay_definition_fingerprint for item in manifest.strategy_lineage)
    assert all(item.fixed_replay_executable_fingerprint for item in manifest.strategy_lineage)

    restore_root = tmp_path / "profile-bound-restore"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in manifest.authorities},
    )
    current = json.loads((restore_root / "current.json").read_text())
    audits = [json.loads(path.read_text()) for path in (restore_root / "audits").glob("*.json")]

    assert current["deployment_profile_id"] == DEPLOYMENT_PROFILE_ID
    assert current["deployment_profile_generation"] == DEPLOYMENT_PROFILE_GENERATION
    assert current["strategy_lineage"] == [
        item.model_dump(mode="json") for item in manifest.strategy_lineage
    ]
    assert audits
    assert all(audit["deployment_profile_id"] == DEPLOYMENT_PROFILE_ID for audit in audits)
    assert all(
        audit["deployment_profile_generation"] == DEPLOYMENT_PROFILE_GENERATION for audit in audits
    )
    assert all(
        audit["strategy_lineage"]
        == [item.model_dump(mode="json") for item in manifest.strategy_lineage]
        for audit in audits
    )


def test_mixed_role_producer_commits_are_bound_without_global_commit(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path, mixed_commits=True)

    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )

    assert {receipt.producer_commit for receipt in manifest.authorities} == {COMMIT_A, COMMIT_B}
    assert manifest.deployment_topology_id == TOPOLOGY_ID


def test_all_external_role_verifier_overrides_fail_closed(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    missing: dict[RecoveryArtifactRole, object] = {}
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="untrusted verifier"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF, role_verifiers=missing)

    wrong = {RecoveryArtifactRole.SQLITE_STATE: lambda _: RecoveryWatermarkSummary(row_count=0)}
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="untrusted verifier"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF, role_verifiers=wrong)


def test_nested_manifest_and_receipts_are_strictly_cross_checked(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    entry = manifest.recovery_manifest.entries[0]
    forged = manifest.model_copy(
        update={
            "recovery_manifest": manifest.recovery_manifest.model_copy(
                update={
                    "manifest_id": None,
                    "entries": (
                        entry.model_copy(update={"schema_version": "forged"}),
                        *manifest.recovery_manifest.entries[1:],
                    ),
                }
            )
        },
        deep=True,
    )
    forged = forged.model_copy(update={"manifest_id": None})
    with pytest.raises(ValueError, match="receipt"):
        type(manifest).model_validate(forged.model_dump())


def test_object_tamper_is_rejected_after_source_deletion(tmp_path: Path) -> None:
    coordinator, expectations, _, sources_root, authority_root = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    shutil.rmtree(sources_root)
    shutil.rmtree(authority_root)
    target = Path(manifest.authorities[0].artifact_path)
    target.chmod(stat.S_IRUSR | stat.S_IWUSR)
    target.write_bytes(b"tampered")

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="hash/size"):
        coordinator.restore_preflight(
            manifest_id=str(manifest.manifest_id),
            expected_as_of=AS_OF,
        )


@pytest.mark.parametrize("store_name", ["manifest", "object"])
def test_stores_nested_with_allowed_roots_are_rejected(tmp_path: Path, store_name: str) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    root = Path(expectations[0].allowed_root)
    kwargs = {
        "inventory_plan": coordinator.inventory_plan,
        "manifest_store": coordinator.manifest_store,
        "backup_object_store": coordinator.backup_object_store,
        "deployment_topology_id": TOPOLOGY_ID,
        "deployment_profile_id": DEPLOYMENT_PROFILE_ID,
        "deployment_profile_generation": DEPLOYMENT_PROFILE_GENERATION,
        "strategy_producer_commit": COMMIT_A,
        "strategy_bindings": _strategy_bindings(),
    }
    kwargs["manifest_store" if store_name == "manifest" else "backup_object_store"] = (
        root / "nested"
    )
    nested = RuntimeRecoveryCoordinator(**kwargs)

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="store.*isolated"):
        nested.run_once(
            expectations=expectations,
            as_of=AS_OF,
        )


def test_manifest_and_object_stores_must_be_disjoint(tmp_path: Path) -> None:
    plan = RecoveryInventoryPlan(
        plan_version=1,
        requirements=tuple(
            RecoveryInventoryRequirement(
                logical_role=logical_role,
                artifact_role=artifact_role,
                restore_path=f"role/{index}",
            )
            for index, (logical_role, artifact_role, _) in enumerate(_requirements())
        ),
    )
    with pytest.raises(ValueError, match="stores must be physically isolated"):
        RuntimeRecoveryCoordinator(
            inventory_plan=plan,
            manifest_store=tmp_path / "store",
            backup_object_store=tmp_path / "store" / "objects",
            deployment_topology_id=TOPOLOGY_ID,
            deployment_profile_id=DEPLOYMENT_PROFILE_ID,
            deployment_profile_generation=DEPLOYMENT_PROFILE_GENERATION,
            strategy_producer_commit=COMMIT_A,
            strategy_bindings=_strategy_bindings(),
        )


def test_concurrent_idempotent_runs_publish_one_manifest_and_one_object_per_hash(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)

    def execute() -> str:
        result = coordinator.run_once(
            expectations=expectations,
            as_of=AS_OF,
        )
        return str(result.manifest_id)

    with ThreadPoolExecutor(max_workers=4) as pool:
        manifest_ids = tuple(pool.map(lambda _: execute(), range(8)))

    assert len(set(manifest_ids)) == 1
    assert len(tuple(coordinator.manifest_store.glob("*.json"))) == 1
    expected_hashes = {_sha256(path) for path in _fixture_paths_from_expectations(expectations)}
    assert {path.stem for path in coordinator.backup_object_store.glob("*.blob")} == expected_hashes


def _fixture_paths_from_expectations(
    expectations: tuple[RecoveryAuthorityExpectation, ...],
) -> tuple[Path, ...]:
    paths = []
    for expectation in expectations:
        authority = json.loads(Path(expectation.authority_manifest_path).read_text())
        paths.append(Path(authority["artifact_path"]))
    return tuple(paths)


def test_each_source_artifact_is_opened_once_during_run(tmp_path: Path) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    source_names = {path.name for path in artifacts.values()}
    counts = {name: 0 for name in source_names}

    def count_source_open(event: str, arguments: tuple[object, ...]) -> None:
        if event != "open" or not arguments:
            return
        name = str(arguments[0])
        if name in counts:
            counts[name] += 1

    sys.addaudithook(count_source_open)
    coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )

    assert counts == {name: 1 for name in source_names}


def test_four_hundred_thousand_rows_use_bounded_replay_and_verifier_memory(
    tmp_path: Path,
) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    production = artifacts["production.duckdb"]
    connection = duckdb.connect(str(production))
    try:
        connection.execute(
            """
            INSERT INTO daily_bar
            SELECT
                printf('X%09d.SZ', range), DATE '2020-01-01',
                1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0
            FROM range(400000)
            """
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    lake = artifacts["research.lake"]
    lake.unlink()
    _write_parquet_artifact(lake, rows=400_001)
    _write_replay_metadata(artifacts)
    production_authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.DATA,
        logical_role="production.duckdb",
        artifact_role=RecoveryArtifactRole.PRODUCTION_DUCKDB,
        artifact_path=production,
        producer_commit=COMMIT_A,
        generation_id="generation-1",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=1),
        watermark=RecoveryWatermarkSummary(
            max_date=date(2026, 7, 31),
            row_count=400_004,
        ),
    )
    metadata_authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.RESEARCH,
        logical_role="research.artifacts",
        artifact_role=RecoveryArtifactRole.ARTIFACT_METADATA,
        artifact_path=artifacts["research.artifacts"],
        producer_commit=COMMIT_A,
        generation_id="generation-5",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=5),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=5),
    )
    lake_authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.RESEARCH,
        logical_role="research.lake",
        artifact_role=RecoveryArtifactRole.LAKE_MANIFEST,
        artifact_path=lake,
        producer_commit=COMMIT_A,
        generation_id="generation-4",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=4),
        watermark=RecoveryWatermarkSummary(
            max_date=date(2026, 7, 31),
            row_count=400_001,
        ),
    )
    authority_root = tmp_path / "large-authority"
    changed = list(expectations)
    changed[0] = changed[0].model_copy(
        update={
            "authority_manifest_path": str(
                append_recovery_authority_manifest(authority_root, production_authority)
            )
        }
    )
    changed[4] = changed[4].model_copy(
        update={
            "authority_manifest_path": str(
                append_recovery_authority_manifest(authority_root, metadata_authority)
            )
        }
    )
    changed[3] = changed[3].model_copy(
        update={
            "authority_manifest_path": str(
                append_recovery_authority_manifest(authority_root, lake_authority)
            )
        }
    )
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_measure_recovery_run_in_spawned_process,
        args=(coordinator, tuple(changed), child_connection),
    )
    process.start()
    child_connection.close()
    try:
        assert parent_connection.poll(60), "spawned memory probe timed out"
        result = parent_connection.recv()
        process.join(timeout=10)
        assert process.exitcode == 0, result
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
        parent_connection.close()
    assert "error" not in result
    assert result["python_peak"] <= 32 * 1024 * 1024
    assert result["rss_delta"] <= 32 * 1024 * 1024, result


@pytest.mark.parametrize(
    "strategy_id",
    ["n_shape", "growth_board_surge", "auction_gap"],
)
def test_fixed_replay_rejects_oversized_working_set_before_pandas(
    strategy_id: str,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant.formal_smoke_replay import build_formal_smoke_spec

    class OversizedConnection:
        def execute(self, _query: str, _parameters: object = None) -> OversizedConnection:
            return self

        def fetchone(self) -> tuple[int, int]:
            return 1_000_000, 1_000_000_000

        def fetchdf(self) -> pd.DataFrame:
            raise AssertionError("oversized fixed replay reached pandas materialization")

    class OversizedStore:
        _conn = OversizedConnection()

    start_date = date(2026, 4, 1)
    end_date = date(2026, 7, 2)
    spec = build_formal_smoke_spec(
        strategy_id,
        start_date=start_date,
        end_date=end_date,
    )
    expected = recovery_module._FixedReplayResult(
        dataset_snapshot_id="a" * 64,
        strategy_registration_fingerprint="b" * 64,
        strategy_definition_sha256=spec.spec_hash,
        strategy_executable_sha256=(
            recovery_module._strategy_fixed_replay_executable_sha256(strategy_id)
        ),
        engine_version="test-fixed-replay-v1",
        start_date=start_date,
        end_date=end_date,
        trade_count=0,
        winning_trade_count=0,
        total_return_bps=0,
        max_drawdown_bps=0,
    )

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="working set"):
        recovery_module._compute_strategy_fixed_replay_v1(
            strategy_id=strategy_id,
            dataset_sha256="a" * 64,
            expected=expected,
            store=OversizedStore(),
        )


class _AuctionBudgetProbeConnection:
    def __init__(self, *, oversized_status: bool) -> None:
        self.oversized_status = oversized_status
        self.queries: list[tuple[str, object]] = []

    def execute(
        self,
        query: str,
        parameters: object = None,
    ) -> _AuctionBudgetProbeConnection:
        self.queries.append((query, parameters))
        return self

    def fetchone(self) -> tuple[object, ...]:
        query = self.queries[-1][0]
        if "current_database()" in query:
            return ("budget_probe",)
        if self.oversized_status and "FROM stock_status_daily" in query:
            return (65_537, 33 * 1024 * 1024)
        return (1, 16)


class _AuctionBudgetProbeStore:
    def __init__(self, *, oversized_status: bool) -> None:
        self._conn = _AuctionBudgetProbeConnection(
            oversized_status=oversized_status,
        )

    def close(self) -> None:
        pass


def _auction_budget_expected(recovery_module: object) -> object:
    from rquant.formal_smoke_replay import build_formal_smoke_spec

    start_date = date(2026, 7, 1)
    end_date = date(2026, 7, 2)
    spec = build_formal_smoke_spec(
        "auction_gap",
        start_date=start_date,
        end_date=end_date,
    )
    return recovery_module._FixedReplayResult(
        dataset_snapshot_id="a" * 64,
        strategy_registration_fingerprint="b" * 64,
        strategy_definition_sha256=spec.spec_hash,
        strategy_executable_sha256="c" * 64,
        engine_version="test-fixed-replay-v1",
        start_date=start_date,
        end_date=end_date,
        trade_count=0,
        winning_trade_count=0,
        total_return_bps=0,
        max_drawdown_bps=0,
    )


def test_auction_status_budget_rejects_before_pandas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import formal_smoke_replay

    store = _AuctionBudgetProbeStore(oversized_status=True)
    entered_pandas = False

    def enter_pandas(_store: object, _spec: object) -> object:
        nonlocal entered_pandas
        entered_pandas = True
        raise AssertionError("oversized auction status reached pandas")

    monkeypatch.setattr(
        recovery_module,
        "_strategy_fixed_replay_executable_sha256",
        lambda _strategy_id: "c" * 64,
    )
    monkeypatch.setattr(formal_smoke_replay, "_execute_formal_smoke_spec", enter_pandas)

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="auction_status working set"):
        recovery_module._compute_strategy_fixed_replay_v1(
            strategy_id="auction_gap",
            dataset_sha256="a" * 64,
            expected=_auction_budget_expected(recovery_module),
            store=store,
        )

    assert entered_pandas is False


def test_auction_bounded_working_set_reaches_pandas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import formal_smoke_replay

    store = _AuctionBudgetProbeStore(oversized_status=False)
    entered_pandas = False

    class EmptyComputation:
        tables: dict[str, pd.DataFrame] = {}

    def enter_pandas(_store: object, _spec: object) -> EmptyComputation:
        nonlocal entered_pandas
        entered_pandas = True
        return EmptyComputation()

    monkeypatch.setattr(
        recovery_module,
        "_strategy_fixed_replay_executable_sha256",
        lambda _strategy_id: "c" * 64,
    )
    monkeypatch.setattr(
        recovery_module,
        "_open_auction_gap_bounded_replay_store",
        lambda *_args, **_kwargs: store,
    )
    monkeypatch.setattr(formal_smoke_replay, "_execute_formal_smoke_spec", enter_pandas)

    recovery_module._compute_strategy_fixed_replay_v1(
        strategy_id="auction_gap",
        dataset_sha256="a" * 64,
        expected=_auction_budget_expected(recovery_module),
        store=store,
    )

    assert entered_pandas is True


def _auction_unbudgeted_query_from_helper(relation: str) -> str:
    return f"SELECT * FROM {relation}"


@pytest.mark.parametrize(
    "query_source",
    ("literal", "variable", "f_string", "helper"),
)
def test_auction_replay_rejects_unbudgeted_relation_for_every_sql_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    query_source: str,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import formal_smoke_replay
    from rquant.storage.duckdb import DuckDBStore

    dataset_path = tmp_path / "auction-relation-contract.duckdb"
    _write_duckdb_artifact(dataset_path, rows=1)
    writable = duckdb.connect(str(dataset_path))
    try:
        writable.execute("CREATE TABLE unbudgeted_relation(value INTEGER)")
        writable.execute("INSERT INTO unbudgeted_relation VALUES (1)")
    finally:
        writable.close()

    def query_unbudgeted_relation(store: object, _spec: object) -> object:
        if query_source == "literal":
            store._conn.execute("SELECT * FROM unbudgeted_relation").fetchdf()
        elif query_source == "variable":
            variable_query = "SELECT * FROM unbudgeted_relation"
            store._conn.execute(variable_query).fetchdf()
        elif query_source == "f_string":
            relation = "unbudgeted_relation"
            store._conn.execute(f"SELECT * FROM {relation}").fetchdf()
        else:
            store._conn.execute(
                _auction_unbudgeted_query_from_helper("unbudgeted_relation")
            ).fetchdf()
        raise AssertionError("unbudgeted auction relation reached pandas")

    monkeypatch.setattr(
        recovery_module,
        "_strategy_fixed_replay_executable_sha256",
        lambda _strategy_id: "c" * 64,
    )
    monkeypatch.setattr(
        formal_smoke_replay,
        "_execute_formal_smoke_spec",
        query_unbudgeted_relation,
    )
    store = DuckDBStore(dataset_path, read_only=True)
    try:
        with pytest.raises(duckdb.CatalogException, match="unbudgeted_relation"):
            recovery_module._compute_strategy_fixed_replay_v1(
                strategy_id="auction_gap",
                dataset_sha256="a" * 64,
                expected=_auction_budget_expected(recovery_module),
                store=store,
            )
    finally:
        store.close()


def test_auction_replay_exposes_exact_declared_relation_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import formal_smoke_replay
    from rquant.storage.duckdb import DuckDBStore

    dataset_path = tmp_path / "auction-declared-relations.duckdb"
    _write_duckdb_artifact(dataset_path, rows=1)
    writable = duckdb.connect(str(dataset_path))
    try:
        writable.execute(
            """
            CREATE TABLE strategy_eligibility(
                eligibility_id VARCHAR,
                strategy_id VARCHAR,
                ts_code VARCHAR,
                eligibility_date DATE,
                entry_date DATE,
                variant VARCHAR
            )
            """
        )
    finally:
        writable.close()

    class EmptyComputation:
        tables: dict[str, pd.DataFrame] = {}

    def assert_declared_relations(store: object, _spec: object) -> EmptyComputation:
        relations = {
            str(row[0])
            for row in store._conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = current_database()
                  AND table_schema = 'main'
                """
            ).fetchall()
        }
        assert relations == set(recovery_module._AUCTION_GAP_REPLAY_RELATION_BUDGET_LABELS)
        store._conn.execute("SELECT * FROM stock_status_daily").fetchdf()
        assert store._conn.execute(
            "SELECT current_setting('enable_external_access')"
        ).fetchone() == (False,)
        with pytest.raises(duckdb.PermissionException, match="file system operations are disabled"):
            forbidden_path = dataset_path.with_name("forbidden-source.duckdb")
            store._conn.execute(f"ATTACH '{forbidden_path}' AS forbidden_source")
        return EmptyComputation()

    monkeypatch.setattr(
        recovery_module,
        "_strategy_fixed_replay_executable_sha256",
        lambda _strategy_id: "c" * 64,
    )
    monkeypatch.setattr(
        formal_smoke_replay,
        "_execute_formal_smoke_spec",
        assert_declared_relations,
    )
    store = DuckDBStore(dataset_path, read_only=True)
    try:
        recovery_module._compute_strategy_fixed_replay_v1(
            strategy_id="auction_gap",
            dataset_sha256="a" * 64,
            expected=_auction_budget_expected(recovery_module),
            store=store,
        )
    finally:
        store.close()


def test_large_parquet_verifier_streams_without_duckdb_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    lake = tmp_path / "trusted" / "large.parquet"
    _write_parquet_artifact(lake, rows=400_001)
    context = RecoveryArtifactVerificationContext.for_source(
        logical_role="research.lake",
        artifact_role=RecoveryArtifactRole.LAKE_MANIFEST,
        artifact_path=lake,
        generation_id="generation-large",
        schema_version="v1",
        as_of=AS_OF,
        related_artifacts=(),
        trust_root=lake.parent,
    )

    def forbidden_connect(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Parquet verifier must not use DuckDB")

    monkeypatch.setattr(recovery_module.duckdb, "connect", forbidden_connect)
    watermark, fixed_replay_verified = recovery_module._verify_lake_manifest(context)

    assert watermark == RecoveryWatermarkSummary(
        max_date=date(2026, 7, 31),
        row_count=400_001,
    )
    assert fixed_replay_verified is False


def test_parent_directory_replacement_during_open_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    target = artifacts["production.duckdb"]
    original_parent = target.parent
    moved_parent = original_parent.with_name(f"{original_parent.name}-moved")
    real_open = os.open
    triggered = False

    def racing_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal triggered
        if not triggered and dir_fd is not None and str(path) == target.name:
            triggered = True
            original_parent.rename(moved_parent)
            original_parent.mkdir()
            (original_parent / target.name).write_bytes(b"replacement")
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", racing_open)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="directory binding changed"):
        coordinator.run_once(
            expectations=expectations,
            as_of=AS_OF,
        )


def test_ancestor_symlink_replacement_during_open_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, artifacts, sources_root, _ = _fixture(tmp_path)
    target = artifacts["serving.manifest"]
    moved_root = sources_root.with_name("sources-moved")
    real_open = os.open
    triggered = False

    def racing_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal triggered
        if not triggered and dir_fd is not None and str(path) == target.name:
            triggered = True
            sources_root.rename(moved_root)
            sources_root.symlink_to(moved_root, target_is_directory=True)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", racing_open)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="directory binding changed"):
        coordinator.run_once(
            expectations=expectations,
            as_of=AS_OF,
        )


def test_restore_rehearsal_succeeds_without_sources_and_records_rpo_rto(tmp_path: Path) -> None:
    coordinator, expectations, _, sources_root, authority_root = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    shutil.rmtree(sources_root)
    shutil.rmtree(authority_root)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={
            receipt.logical_role: receipt.watermark for receipt in manifest.authorities
        },
    )

    assert report.passed is True
    assert report.started_at <= report.finished_at
    assert report.duration_seconds >= 0
    assert report.rto_target_seconds == 30.0
    assert report.rto_met is True
    assert report.rpo_met is True
    assert all(loss.met for loss in report.rpo_loss)
    assert report.rpo_as_of == AS_OF
    assert {item.logical_role for item in report.recovered_watermarks} == {
        item.logical_role for item in expectations
    }
    current = json.loads((restore_root / "current.json").read_text())
    generation_root = restore_root / current["generation_path"]
    assert all(
        (generation_root / item.restore_path).is_file()
        for item in manifest.recovery_manifest.entries
    )


def test_restore_rehearsal_reports_real_per_role_rpo_loss(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    targets = {receipt.logical_role: receipt.watermark for receipt in manifest.authorities}
    targets["production.duckdb"] = RecoveryWatermarkSummary(
        max_date=date(2026, 8, 1),
        row_count=6,
    )
    restore_root = tmp_path / "rpo-loss-restore"
    restore_root.mkdir()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks=targets,
    )

    assert report.rto_met is True
    assert report.rpo_met is False
    assert report.passed is False
    production_loss = next(
        loss for loss in report.rpo_loss if loss.logical_role == "production.duckdb"
    )
    assert production_loss.met is False
    assert production_loss.max_date_loss_days == 1
    assert production_loss.row_count_loss == 2
    assert not (restore_root / "current.json").exists()
    assert tuple((restore_root / ".failed").iterdir())
    assert any(
        json.loads(path.read_text())["failure_point"] == "rpo_rto_gate"
        for path in (restore_root / "audits").glob("*.json")
    )


def test_restore_rehearsal_with_one_of_seven_rpo_targets_fails_closed(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    assert len(manifest.authorities) == 7
    target = manifest.authorities[0]
    restore_root = tmp_path / "one-of-seven-rpo-targets"
    restore_root.mkdir()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={target.logical_role: target.watermark},
    )

    assert report.rto_met is True
    assert report.rpo_met is False
    assert report.passed is False
    assert report.rpo_missing_target_roles == tuple(
        item.logical_role
        for item in manifest.authorities
        if item.logical_role != target.logical_role
    )


def test_restore_rehearsal_with_one_missing_rpo_target_fails_closed(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    omitted = manifest.authorities[-1]
    targets = {
        item.logical_role: item.watermark
        for item in manifest.authorities
        if item.logical_role != omitted.logical_role
    }
    restore_root = tmp_path / "one-missing-rpo-target"
    restore_root.mkdir()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks=targets,
    )

    assert report.rto_met is True
    assert report.rpo_met is False
    assert report.passed is False
    assert report.rpo_missing_target_roles == (omitted.logical_role,)


def test_restore_rehearsal_without_explicit_rpo_targets_fails_closed(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    restore_root = tmp_path / "missing-rpo-target-restore"
    restore_root.mkdir()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
    )

    assert report.rto_met is True
    assert report.rpo_met is False
    assert report.rpo_loss == ()
    assert report.passed is False


def test_restore_target_binding_is_leased_from_empty_check_through_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    restore_root = tmp_path / "leased-restore"
    moved_root = tmp_path / "leased-restore-moved"
    restore_root.mkdir()
    restore_identity = (restore_root.stat().st_dev, restore_root.stat().st_ino)
    real_listdir = os.listdir
    triggered = False

    def racing_listdir(path: int | str | bytes | os.PathLike[str]) -> list[str]:
        nonlocal triggered
        entries = real_listdir(path)
        if isinstance(path, int) and not triggered:
            observed = os.fstat(path)
            if (observed.st_dev, observed.st_ino) == restore_identity:
                triggered = True
                restore_root.rename(moved_root)
                restore_root.mkdir()
        return entries

    monkeypatch.setattr(os, "listdir", racing_listdir)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="directory binding changed"):
        coordinator.rehearse_restore(
            manifest_id=str(manifest.manifest_id),
            target_root=restore_root,
            expected_as_of=AS_OF,
            rto_target_seconds=30.0,
            rpo_target_watermarks={
                receipt.logical_role: receipt.watermark for receipt in manifest.authorities
            },
        )


@pytest.mark.parametrize("entry_kind", ["file", "directory"])
def test_restore_final_entries_must_exactly_match_manifest_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_kind: str,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    restore_root = tmp_path / f"extra-{entry_kind}-restore"
    restore_root.mkdir()
    restore_identity = (restore_root.stat().st_dev, restore_root.stat().st_ino)
    real_listdir = os.listdir
    triggered = False

    def racing_listdir(path: int | str | bytes | os.PathLike[str]) -> list[str]:
        nonlocal triggered
        entries = real_listdir(path)
        if isinstance(path, int) and not triggered:
            observed = os.fstat(path)
            if (observed.st_dev, observed.st_ino) == restore_identity:
                triggered = True
                extra = restore_root / "unexpected"
                if entry_kind == "file":
                    extra.write_text("injected")
                else:
                    extra.mkdir()
        return entries

    monkeypatch.setattr(os, "listdir", racing_listdir)
    with pytest.raises(
        RuntimeRecoveryCoordinatorError,
        match="manifest outputs|directory binding changed",
    ):
        coordinator.rehearse_restore(
            manifest_id=str(manifest.manifest_id),
            target_root=restore_root,
            expected_as_of=AS_OF,
            rto_target_seconds=30.0,
            rpo_target_watermarks={
                receipt.logical_role: receipt.watermark for receipt in manifest.authorities
            },
        )


@pytest.mark.parametrize("race_kind", ["hardlink", "replacement"])
def test_backup_object_rebinding_during_restore_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race_kind: str,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    receipt = next(item for item in manifest.authorities if item.logical_role == "serving.manifest")
    object_path = Path(receipt.artifact_path)
    object_content = object_path.read_bytes()
    real_open = os.open
    real_read = os.read
    object_open_count = 0
    restore_descriptor: int | None = None
    triggered = False

    def tracking_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal object_open_count, restore_descriptor
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if dir_fd is not None and str(path) == object_path.name:
            object_open_count += 1
            if object_open_count == 1:
                restore_descriptor = descriptor
        return descriptor

    def racing_read(descriptor: int, length: int) -> bytes:
        nonlocal triggered
        if descriptor == restore_descriptor and not triggered:
            triggered = True
            if race_kind == "hardlink":
                os.link(object_path, object_path.with_name(f"linked-{object_path.name}"))
            else:
                object_path.rename(object_path.with_name(f"replaced-{object_path.name}"))
                object_path.write_bytes(object_content)
        return real_read(descriptor, length)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "read", racing_read)
    restore_root = tmp_path / f"object-{race_kind}-restore"
    restore_root.mkdir()

    import rquant.runtime_recovery_coordinator as recovery_module

    target_lease = recovery_module._open_physical_directory(
        restore_root,
        create=False,
        label="restore target",
    )
    try:
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="hardlink|changed"):
            coordinator._restore_entries(manifest=manifest, target_lease=target_lease)
    finally:
        target_lease.close()


@pytest.mark.parametrize("mutation_kind", ["chmod", "nlink"])
def test_backup_object_parent_metadata_mutation_during_restore_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation_kind: str,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    receipt = next(item for item in manifest.authorities if item.logical_role == "serving.manifest")
    object_path = Path(receipt.artifact_path)
    object_parent = object_path.parent
    original_parent = object_parent.stat()
    real_open = os.open
    real_read = os.read
    object_open_count = 0
    restore_descriptor: int | None = None
    triggered = False

    def tracking_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal object_open_count, restore_descriptor
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if dir_fd is not None and str(path) == object_path.name:
            object_open_count += 1
            if object_open_count == 1:
                restore_descriptor = descriptor
        return descriptor

    def racing_read(descriptor: int, length: int) -> bytes:
        nonlocal triggered
        if descriptor == restore_descriptor and not triggered:
            triggered = True
            if mutation_kind == "chmod":
                original_mode = stat.S_IMODE(original_parent.st_mode)
                object_parent.chmod(original_mode ^ stat.S_IXGRP)
                assert stat.S_IMODE(object_parent.stat().st_mode) != original_mode
            else:
                (object_parent / "injected-directory").mkdir()
                assert object_parent.stat().st_nlink != original_parent.st_nlink
        return real_read(descriptor, length)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "read", racing_read)
    restore_root = tmp_path / f"object-parent-{mutation_kind}-restore"
    restore_root.mkdir()

    import rquant.runtime_recovery_coordinator as recovery_module

    target_lease = recovery_module._open_physical_directory(
        restore_root,
        create=False,
        label="restore target",
    )
    try:
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="directory binding changed"):
            coordinator._restore_entries(manifest=manifest, target_lease=target_lease)
    finally:
        target_lease.close()
    assert triggered is True


def test_restore_rehearsal_never_overwrites_existing_paths(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    restore_root = tmp_path / "nonempty"
    restore_root.mkdir()
    sentinel = restore_root / "sentinel"
    sentinel.write_text("keep")

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="must be empty"):
        coordinator.rehearse_restore(
            manifest_id=str(manifest.manifest_id),
            target_root=restore_root,
            expected_as_of=AS_OF,
            rto_target_seconds=30.0,
        )
    assert sentinel.read_text() == "keep"


def test_artifact_metadata_reference_hash_is_verified(tmp_path: Path) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    metadata = json.loads(artifacts["research.artifacts"].read_text())
    metadata["references"][0]["sha256"] = "f" * 64
    artifacts["research.artifacts"].write_text(json.dumps(metadata, sort_keys=True))
    changed = list(expectations)
    index = next(i for i, item in enumerate(changed) if item.logical_role == "research.artifacts")
    authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.RESEARCH,
        logical_role="research.artifacts",
        artifact_role=RecoveryArtifactRole.ARTIFACT_METADATA,
        artifact_path=artifacts["research.artifacts"],
        producer_commit=COMMIT_A,
        generation_id="generation-5",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=5),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=5),
    )
    authority_path = append_recovery_authority_manifest(tmp_path / "bad-metadata", authority)
    changed[index] = changed[index].model_copy(
        update={"authority_manifest_path": str(authority_path)}
    )

    with pytest.raises((AssertionError, RuntimeRecoveryCoordinatorError)):
        coordinator.run_once(
            expectations=tuple(changed),
            as_of=AS_OF,
        )


@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_linked_source_artifacts_fail_closed(tmp_path: Path, link_kind: str) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    source = artifacts["production.duckdb"]
    linked = source.with_name(f"linked-{source.name}")
    if link_kind == "symlink":
        source.rename(linked)
        source.symlink_to(linked)
    else:
        os.link(source, linked)

    with pytest.raises(RuntimeRecoveryCoordinatorError, match=link_kind):
        coordinator.run_once(
            expectations=expectations,
            as_of=AS_OF,
        )


def test_hardlinked_backup_object_fails_preflight(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    object_path = Path(manifest.authorities[0].artifact_path)
    os.link(object_path, object_path.with_name(f"linked-{object_path.name}"))

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="hardlink|isolated"):
        coordinator.restore_preflight(
            manifest_id=str(manifest.manifest_id),
            expected_as_of=AS_OF,
        )


def test_concurrent_reader_waits_for_atomic_link_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    real_link = os.link
    first_object_linked = threading.Event()
    release_first_publisher = threading.Event()
    guarded = False

    def delayed_link(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        nonlocal guarded
        real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        if not guarded and str(dst).endswith(".blob"):
            guarded = True
            first_object_linked.set()
            assert release_first_publisher.wait(timeout=5)

    monkeypatch.setattr(os, "link", delayed_link)

    def execute() -> str:
        return str(
            coordinator.run_once(
                expectations=expectations,
                as_of=AS_OF,
            ).manifest_id
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(execute)
        assert first_object_linked.wait(timeout=5)
        second = pool.submit(execute)
        time.sleep(0.05)
        release_first_publisher.set()
        assert first.result(timeout=5) == second.result(timeout=5)


def test_concurrent_manifest_reader_waits_for_atomic_link_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    initial = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    (coordinator.manifest_store / f"{initial.manifest_id}.json").unlink()
    real_link = os.link
    first_manifest_linked = threading.Event()
    release_first_publisher = threading.Event()
    guarded = False

    def delayed_link(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        nonlocal guarded
        real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        if not guarded and str(dst).endswith(".json"):
            guarded = True
            first_manifest_linked.set()
            assert release_first_publisher.wait(timeout=5)

    monkeypatch.setattr(os, "link", delayed_link)

    def execute() -> str:
        return str(
            coordinator.run_once(
                expectations=expectations,
                as_of=AS_OF,
            ).manifest_id
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(execute)
        assert first_manifest_linked.wait(timeout=5)
        second = pool.submit(execute)
        time.sleep(0.05)
        release_first_publisher.set()
        assert first.result(timeout=5) == second.result(timeout=5)


def test_restore_publishes_verified_generation_before_switching_current(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(
        expectations=expectations,
        as_of=AS_OF,
    )
    restore_root = tmp_path / "published-restore"
    restore_root.mkdir()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in manifest.authorities},
    )

    current_path = restore_root / "current.json"
    current = json.loads(current_path.read_text())
    generation = restore_root / current["generation_path"]
    assert report.passed is True
    assert current["manifest_id"] == manifest.manifest_id
    assert current["report_sha256"] == canonical_sha256(report.model_dump(mode="python"))
    assert generation.parent == restore_root / "generations"
    assert all(
        (generation / item.restore_path).is_file() for item in manifest.recovery_manifest.entries
    )
    assert tuple((restore_root / "audits").glob("*.json"))
    assert not tuple((restore_root / ".candidates").iterdir())


def test_untrusted_role_callback_cannot_echo_expected_duckdb_watermark(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    malicious = dict(ROLE_VERIFIERS)
    malicious[RecoveryArtifactRole.PRODUCTION_DUCKDB] = lambda _: RecoveryWatermarkSummary(
        max_date=date(2026, 7, 31),
        row_count=1,
    )

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="untrusted verifier"):
        coordinator.run_once(
            expectations=expectations,
            as_of=AS_OF,
            role_verifiers=malicious,
        )


def test_transient_source_hardlink_is_detected_even_after_link_is_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    source.write_text('{"max_date":"2026-07-31","row_count":1}')
    transient = tmp_path / "transient-link.json"
    real_read = os.read
    triggered = False

    def racing_read(descriptor: int, length: int) -> bytes:
        nonlocal triggered
        if not triggered:
            triggered = True
            os.link(source, transient)
            transient.unlink()
        return real_read(descriptor, length)

    monkeypatch.setattr(os, "read", racing_read)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="changed while reading"):
        RecoveryArtifactVerificationContext.for_source(
            logical_role="serving.current",
            artifact_role=RecoveryArtifactRole.SERVING_CURRENT,
            artifact_path=source,
            generation_id="generation-1",
            schema_version="v1",
            as_of=AS_OF,
            related_artifacts=(),
        )
    assert triggered is True


def test_external_ancestor_nlink_change_during_read_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ancestor = tmp_path / "trusted"
    source_parent = ancestor / "nested" / "leaf"
    source_parent.mkdir(parents=True)
    source = source_parent / "source.json"
    source.write_text('{"max_date":"2026-07-31","row_count":1}')
    injected = ancestor / "external-directory"
    real_read = os.read
    triggered = False

    def racing_read(descriptor: int, length: int) -> bytes:
        nonlocal triggered
        if not triggered:
            triggered = True
            injected.mkdir()
        return real_read(descriptor, length)

    monkeypatch.setattr(os, "read", racing_read)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="directory binding changed"):
        RecoveryArtifactVerificationContext.for_source(
            logical_role="serving.current",
            artifact_role=RecoveryArtifactRole.SERVING_CURRENT,
            artifact_path=source,
            generation_id="generation-1",
            schema_version="v1",
            as_of=AS_OF,
            related_artifacts=(),
            trust_root=ancestor,
        )
    assert triggered is True


@pytest.mark.parametrize(
    "fault_point",
    [
        RecoveryFaultPoint.AFTER_COPY,
        RecoveryFaultPoint.AFTER_HASH_VERIFY,
        RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH,
        RecoveryFaultPoint.AFTER_CURRENT_SWITCH,
    ],
)
def test_failed_candidate_never_replaces_current_and_persists_failure_audit(
    tmp_path: Path,
    fault_point: RecoveryFaultPoint,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / f"fault-{fault_point.value}"
    restore_root.mkdir()
    targets = {item.logical_role: item.watermark for item in manifest.authorities}
    coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks=targets,
    )
    previous_current = (restore_root / "current.json").read_bytes()
    next_as_of = AS_OF + timedelta(minutes=1)
    next_manifest = coordinator.run_once(expectations=expectations, as_of=next_as_of)
    next_targets = {item.logical_role: item.watermark for item in next_manifest.authorities}

    def inject(observed: RecoveryFaultPoint) -> None:
        if observed is fault_point:
            raise RuntimeError(f"fault:{fault_point.value}")

    with pytest.raises(RuntimeRecoveryCoordinatorError, match=fault_point.value):
        coordinator.rehearse_restore(
            manifest_id=str(next_manifest.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks=next_targets,
            fault_injector=inject,
        )

    assert (restore_root / "current.json").read_bytes() == previous_current
    assert not tuple((restore_root / ".candidates").iterdir())
    failed = tuple((restore_root / ".failed").iterdir())
    audits = tuple((restore_root / "audits").glob("*.json"))
    assert failed
    assert any(json.loads(path.read_text())["status"] == "failed" for path in audits)


def test_production_duckdb_verifier_rejects_non_duckdb_with_matching_watermark(
    tmp_path: Path,
) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    production = artifacts["production.duckdb"]
    production.unlink()
    _write_json_artifact(production, rows=1)
    authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.DATA,
        logical_role="production.duckdb",
        artifact_role=RecoveryArtifactRole.PRODUCTION_DUCKDB,
        artifact_path=production,
        producer_commit=COMMIT_A,
        generation_id="generation-1",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=1),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=1),
    )
    authority_path = append_recovery_authority_manifest(
        tmp_path / "invalid-duckdb-authority",
        authority,
    )
    changed = list(expectations)
    changed[0] = changed[0].model_copy(update={"authority_manifest_path": str(authority_path)})

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="production DuckDB"):
        coordinator.run_once(expectations=tuple(changed), as_of=AS_OF)


def test_artifact_metadata_requires_typed_fixed_replay_evidence(tmp_path: Path) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    metadata = artifacts["research.artifacts"]
    metadata.write_text(
        json.dumps(
            {
                "max_date": "2026-07-31",
                "row_count": 5,
                "references": [
                    {
                        "logical_role": "research.lake",
                        "sha256": _sha256(artifacts["research.lake"]),
                    }
                ],
            },
            separators=(",", ":"),
        )
        + "\n"
    )
    authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.RESEARCH,
        logical_role="research.artifacts",
        artifact_role=RecoveryArtifactRole.ARTIFACT_METADATA,
        artifact_path=metadata,
        producer_commit=COMMIT_A,
        generation_id="generation-5",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=5),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=5),
    )
    authority_path = append_recovery_authority_manifest(
        tmp_path / "missing-replay-authority",
        authority,
    )
    changed = list(expectations)
    changed[4] = changed[4].model_copy(update={"authority_manifest_path": str(authority_path)})

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="fixed_replay"):
        coordinator.run_once(expectations=tuple(changed), as_of=AS_OF)


def test_fixed_replay_result_must_bind_the_referenced_dataset_snapshot(
    tmp_path: Path,
) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    metadata = artifacts["research.artifacts"]
    payload = json.loads(metadata.read_text())
    replay = _fixed_replay_evidence(payload, "n_shape")
    forged_result = replay["result"]
    forged_result["dataset_snapshot_id"] = "f" * 64
    forged_result["result_sha256"] = _fixed_replay_result_sha256(forged_result)
    replay["expected_result_sha256"] = forged_result["result_sha256"]
    metadata.write_text(json.dumps(payload, separators=(",", ":")) + "\n")
    authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.RESEARCH,
        logical_role="research.artifacts",
        artifact_role=RecoveryArtifactRole.ARTIFACT_METADATA,
        artifact_path=metadata,
        producer_commit=COMMIT_A,
        generation_id="generation-5",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=5),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=5),
    )
    authority_path = append_recovery_authority_manifest(
        tmp_path / "forged-replay-dataset-authority",
        authority,
    )
    changed = list(expectations)
    changed[4] = changed[4].model_copy(update={"authority_manifest_path": str(authority_path)})

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="dataset snapshot"):
        coordinator.run_once(expectations=tuple(changed), as_of=AS_OF)


def test_fixed_replay_recomputes_metrics_instead_of_accepting_self_signed_json(
    tmp_path: Path,
) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    metadata = artifacts["research.artifacts"]
    payload = json.loads(metadata.read_text())
    replay = _fixed_replay_evidence(payload, "n_shape")
    forged_result = replay["result"]
    forged_result["total_return_bps"] += 999
    forged_result["result_sha256"] = _fixed_replay_result_sha256(forged_result)
    replay["expected_result_sha256"] = forged_result["result_sha256"]
    metadata.write_text(json.dumps(payload, separators=(",", ":")) + "\n")
    authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.RESEARCH,
        logical_role="research.artifacts",
        artifact_role=RecoveryArtifactRole.ARTIFACT_METADATA,
        artifact_path=metadata,
        producer_commit=COMMIT_A,
        generation_id="generation-5",
        schema_version="v1",
        available_at=AS_OF - timedelta(minutes=5),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=5),
    )
    authority_path = append_recovery_authority_manifest(
        tmp_path / "self-signed-replay-authority",
        authority,
    )
    changed = list(expectations)
    changed[4] = changed[4].model_copy(update={"authority_manifest_path": str(authority_path)})

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="trusted fixed replay"):
        coordinator.run_once(expectations=tuple(changed), as_of=AS_OF)


def test_unknown_artifact_schema_has_no_trusted_verifier(tmp_path: Path) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    production = artifacts["production.duckdb"]
    authority = build_recovery_authority_manifest(
        plane=RecoveryPlane.DATA,
        logical_role="production.duckdb",
        artifact_role=RecoveryArtifactRole.PRODUCTION_DUCKDB,
        artifact_path=production,
        producer_commit=COMMIT_A,
        generation_id="generation-1",
        schema_version="v2-unregistered",
        available_at=AS_OF - timedelta(minutes=1),
        watermark=RecoveryWatermarkSummary(max_date=date(2026, 7, 31), row_count=1),
    )
    authority_path = append_recovery_authority_manifest(
        tmp_path / "unknown-schema-authority",
        authority,
    )
    changed = list(expectations)
    changed[0] = changed[0].model_copy(update={"authority_manifest_path": str(authority_path)})

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="no trusted verifier"):
        coordinator.run_once(expectations=tuple(changed), as_of=AS_OF)


def test_trusted_verifier_registry_is_not_module_mutable() -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    assert not hasattr(recovery_module, "_TRUSTED_VERIFIERS")


def test_trusted_verifier_binds_referenced_duckdb_module_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    resolve = recovery_module._build_trusted_verifier_resolver()
    definition = resolve(RecoveryArtifactRole.PRODUCTION_DUCKDB, "v1")
    assert definition is not None

    monkeypatch.setattr(recovery_module, "duckdb", object())

    with pytest.raises(
        RuntimeRecoveryCoordinatorError,
        match="implementation fingerprint|dependency graph is unsafe",
    ):
        definition.assert_implementation_is_trusted()


def test_bound_method_dependency_binds_owner_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = capture_executable_dependency_guard(
        (ExecutableBinding.from_callable(_stateful_policy_probe),),
        contract="test-stateful-policy-owner/v1",
    )

    monkeypatch.setattr(sys.modules[__name__], "_STATEFUL_POLICY", _StatefulPolicy("deny"))

    assert guard.current_fingerprint() != guard.fingerprint
    with pytest.raises(ExecutableDependencyError, match="fingerprint changed"):
        guard.assert_unchanged()


def test_external_bound_method_dependency_cannot_downgrade_owner_to_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_StatefulPolicy.allows, "__module__", "external_policy")
    guard = capture_executable_dependency_guard(
        (ExecutableBinding.from_callable(_stateful_policy_probe),),
        contract="test-external-stateful-policy-owner/v1",
    )

    monkeypatch.setattr(sys.modules[__name__], "_STATEFUL_POLICY", _StatefulPolicy("deny"))

    assert guard.current_fingerprint() != guard.fingerprint
    with pytest.raises(ExecutableDependencyError, match="fingerprint changed"):
        guard.assert_unchanged()


def test_mapping_snapshot_stops_consuming_at_graph_budget() -> None:
    mapping = _CountingItemsMapping(10_000)
    limits = DependencyFingerprintLimits(max_nodes=16, max_depth=8, max_bytes=512)

    with pytest.raises(ExecutableDependencyError, match="node|byte|budget"):
        fingerprint_dependency_value(
            mapping,
            contract="test-bounded-mapping/v1",
            limits=limits,
        )

    assert mapping.consumed <= limits.max_nodes + 1


def test_mapping_probe_reuses_capture_limits() -> None:
    dependency = _BOUNDED_MAPPING_DEPENDENCY
    original_size = dependency.size
    limits = DependencyFingerprintLimits(max_nodes=24, max_depth=8, max_bytes=64 * 1024)
    guard = capture_executable_dependency_guard(
        (ExecutableBinding.from_callable(_bounded_mapping_probe),),
        contract="test-bounded-mapping-probe/v1",
        limits=limits,
    )
    dependency.consumed = 0
    dependency.size = 10_000
    try:
        with pytest.raises(ExecutableDependencyError, match="node|byte|budget"):
            guard.assert_unchanged()
        assert dependency.consumed <= limits.max_nodes + 1
    finally:
        dependency.size = original_size
        dependency.consumed = 0


def test_oversized_dict_dependency_fails_closed() -> None:
    with pytest.raises(ExecutableDependencyError, match="node|byte|budget"):
        fingerprint_dependency_value(
            {str(index): index for index in range(10_000)},
            contract="test-oversized-dict/v1",
            limits=DependencyFingerprintLimits(
                max_nodes=32,
                max_depth=8,
                max_bytes=2048,
            ),
        )


def test_dependency_capture_rejects_property_without_executing_it() -> None:
    global _CAPTURE_PROPERTY_READS
    _CAPTURE_PROPERTY_READS = 0

    with pytest.raises(ExecutableDependencyError, match="descriptor|unsafe"):
        capture_executable_dependency_guard(
            (ExecutableBinding.from_callable(_capture_property_probe),),
            contract="test-property-capture/v1",
        )

    assert _CAPTURE_PROPERTY_READS == 0


def test_dependency_capture_rejects_callable_metadata_descriptor_without_executing_it() -> None:
    global _CALLABLE_DESCRIPTOR_READS
    _CALLABLE_DESCRIPTOR_READS = 0

    with pytest.raises(ExecutableDependencyError):
        capture_executable_dependency_guard(
            (ExecutableBinding.from_callable(_callable_module_descriptor_probe),),
            contract="test-callable-property-capture/v1",
        )

    assert _CALLABLE_DESCRIPTOR_READS == 0


def test_dependency_reverify_rejects_property_without_executing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _REVERIFY_PROPERTY_READS
    _REVERIFY_PROPERTY_READS = 0
    guard = capture_executable_dependency_guard(
        (ExecutableBinding.from_callable(_reverify_property_probe),),
        contract="test-property-reverify/v1",
    )
    monkeypatch.setattr(
        _ReverifyPropertyOwner,
        "policy",
        property(_counting_reverify_property),
    )

    with pytest.raises(ExecutableDependencyError, match="descriptor|unsafe|fingerprint changed"):
        guard.assert_unchanged()
    assert _REVERIFY_PROPERTY_READS == 0


def _build_test_trusted_verifier_with_closure(
    state: list[object],
    *,
    monkeypatch: pytest.MonkeyPatch,
):
    import rquant.runtime_recovery_coordinator as recovery_module

    def verifier(context):
        bool(state)
        return context, False

    binding_name = "_test_trusted_verifier_with_closure"
    verifier.__name__ = binding_name
    monkeypatch.setattr(recovery_module, binding_name, verifier, raising=False)
    return recovery_module._TrustedVerifierDefinition.build(
        artifact_role=RecoveryArtifactRole.PRODUCTION_DUCKDB,
        schema_version="test-v1",
        verifier_id="test.trusted-verifier.closure",
        verifier_version=1,
        verify=verifier,
    )


@pytest.mark.parametrize("phase", ["build", "reverify"])
def test_trusted_verifier_rejects_cyclic_closure_dependency(
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: list[object] = []
    if phase == "build":
        state.append(state)
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency graph is unsafe"):
            _build_test_trusted_verifier_with_closure(state, monkeypatch=monkeypatch)
        return

    definition = _build_test_trusted_verifier_with_closure(state, monkeypatch=monkeypatch)
    state.append(state)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency graph is unsafe"):
        definition.assert_implementation_is_trusted()


@pytest.mark.parametrize("phase", ["build", "reverify"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_trusted_verifier_rejects_nonfinite_closure_dependency(
    phase: str,
    value: float,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: list[object] = []
    if phase == "build":
        state.append(value)
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency graph is unsafe"):
            _build_test_trusted_verifier_with_closure(state, monkeypatch=monkeypatch)
        return

    definition = _build_test_trusted_verifier_with_closure(state, monkeypatch=monkeypatch)
    state.append(value)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency graph is unsafe"):
        definition.assert_implementation_is_trusted()


@pytest.mark.parametrize("phase", ["build", "reverify"])
def test_trusted_verifier_rejects_opaque_closure_dependency(
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: list[object] = []
    if phase == "build":
        state.append(object())
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency graph is unsafe"):
            _build_test_trusted_verifier_with_closure(state, monkeypatch=monkeypatch)
        return

    definition = _build_test_trusted_verifier_with_closure(state, monkeypatch=monkeypatch)
    state.append(object())
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency graph is unsafe"):
        definition.assert_implementation_is_trusted()


def test_trusted_verifier_callable_code_mutation_fails_fingerprint_check(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    trusted = recovery_module._verify_production_duckdb
    original_code = trusted.__code__

    def forged(context):
        return context, False

    trusted.__code__ = forged.__code__
    try:
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="implementation fingerprint"):
            coordinator.run_once(expectations=expectations, as_of=AS_OF)
    finally:
        trusted.__code__ = original_code


def test_trusted_verifier_module_rebind_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)

    def forged(context):
        return context, False

    monkeypatch.setattr(recovery_module, "_verify_production_duckdb", forged)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="verifier binding changed"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF)


def test_trusted_fixed_replay_dependency_rebind_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)

    def forged_replay(*, expected, **kwargs):
        del kwargs
        return expected

    monkeypatch.setattr(recovery_module, "_run_n_shape_fixed_replay_v1", forged_replay)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency binding changed"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF)


def test_trusted_verifier_resolver_rebind_cannot_replace_closed_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    monkeypatch.setattr(
        recovery_module,
        "_resolve_trusted_verifier",
        lambda *args: None,
        raising=False,
    )

    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert manifest.authorities


def test_fixed_replay_uses_real_n_shape_signal_trade_and_pnl_chain(
    tmp_path: Path,
) -> None:
    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    payload = json.loads(artifacts["research.artifacts"].read_text())
    result = _fixed_replay_evidence(payload, "n_shape")["result"]

    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert result["strategy_definition_sha256"] != "c" * 64
    assert result["strategy_executable_sha256"] != "c" * 64
    assert result["engine_version"] == "rquant.formal-smoke.stage1-smoke-v1"
    assert result["trade_count"] == 1
    assert result["winning_trade_count"] == 1
    assert result["total_return_bps"] == 352
    assert any(receipt.fixed_replay_verified for receipt in manifest.authorities)


def test_fixed_replay_fails_closed_when_real_strategy_executable_is_rebound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_compare as strategy_compare

    coordinator, expectations, _, _, _ = _fixture(tmp_path)

    def forged_replay(*args, **kwargs):
        del args, kwargs
        raise AssertionError("forged strategy executable must not be trusted")

    monkeypatch.setattr(strategy_compare, "run_entry_mode_comparison", forged_replay)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="strategy executable"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF)


def test_fixed_replay_fails_closed_when_strategy_compare_replay_binding_is_rebound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_compare as strategy_compare

    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    real_replay = strategy_compare.run_minute_strong_carry_replay

    def rebound_replay(*args, **kwargs):
        return real_replay(*args, **kwargs)

    monkeypatch.setattr(strategy_compare, "run_minute_strong_carry_replay", rebound_replay)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="strategy executable"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert not tuple(coordinator.manifest_store.glob("*.json"))


def test_fixed_replay_fails_closed_when_result_model_binding_is_rebound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_compare as strategy_compare

    coordinator, expectations, _, _, _ = _fixture(tmp_path)

    class ForgedStrategyComparisonResult:
        def __init__(self, **_kwargs: object) -> None:
            self.candidates_count = 1
            self.trades = pd.DataFrame([{"ret_pct": 3.52}])
            self.summary = pd.DataFrame()

    monkeypatch.setattr(
        strategy_compare,
        "StrategyComparisonResult",
        ForgedStrategyComparisonResult,
    )

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="strategy executable"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert not tuple(coordinator.manifest_store.glob("*.json"))


def test_fixed_replay_fingerprint_binds_mutable_global_content_and_can_be_restored() -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import minute_replay

    original_labels = dict(minute_replay.INDEX_CONTEXT_LABELS)
    original_graph = recovery_module._build_strategy_replay_executable_fingerprint()()
    original_fingerprint = recovery_module._strategy_fixed_replay_executable_sha256("n_shape")
    try:
        minute_replay.INDEX_CONTEXT_LABELS["000001.SH"] = "rebound-sse"
        changed_graph = recovery_module._build_strategy_replay_executable_fingerprint()()

        assert changed_graph != original_graph
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="global.*fingerprint changed"):
            recovery_module._strategy_fixed_replay_executable_sha256("n_shape")
    finally:
        minute_replay.INDEX_CONTEXT_LABELS.clear()
        minute_replay.INDEX_CONTEXT_LABELS.update(original_labels)

    assert recovery_module._build_strategy_replay_executable_fingerprint()() == original_graph
    assert (
        recovery_module._strategy_fixed_replay_executable_sha256("n_shape") == original_fingerprint
    )


def test_fixed_replay_fingerprint_binds_referenced_mutable_list_content() -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import auction_gap_strategy

    columns = auction_gap_strategy._AUCTION_GAP_MINIMUM_COLUMNS
    original_columns = list(columns)
    original_graph = recovery_module._build_strategy_replay_executable_fingerprint()()
    try:
        columns.append("forged_column")

        assert recovery_module._build_strategy_replay_executable_fingerprint()() != original_graph
        with pytest.raises(RuntimeRecoveryCoordinatorError, match="global.*fingerprint changed"):
            recovery_module._strategy_fixed_replay_executable_sha256("auction_gap")
    finally:
        columns[:] = original_columns

    assert recovery_module._build_strategy_replay_executable_fingerprint()() == original_graph


def test_fixed_replay_fails_closed_when_hash_helper_and_executor_are_rebound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module
    from rquant import formal_smoke_replay

    coordinator, expectations, artifacts, _, _ = _fixture(tmp_path)
    payload = json.loads(artifacts["research.artifacts"].read_text())
    expected_executable_sha256 = _fixed_replay_evidence(payload, "n_shape")["result"][
        "strategy_executable_sha256"
    ]

    monkeypatch.setattr(
        recovery_module,
        "_n_shape_strategy_executable_sha256",
        lambda: expected_executable_sha256,
    )

    def forged_executor(store, spec):
        del store, spec
        return formal_smoke_replay.FormalSmokeComputation(
            metrics={
                "candidate_count": 1,
                "trade_count": 1,
                "mean_ret_pct": 3.52,
                "win_rate_pct": 100.0,
            },
            tables={
                "strategy_summary": pd.DataFrame(),
                "trades": pd.DataFrame([{"ret_pct": 3.52}]),
            },
            sample_count=1,
        )

    monkeypatch.setattr(formal_smoke_replay, "_execute_formal_smoke_spec", forged_executor)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="dependency binding changed"):
        coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert not tuple(coordinator.manifest_store.glob("*.json"))


def test_atomic_pointer_replace_failure_retains_previous_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    first = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "pointer-replace-failure"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(first.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in first.authorities},
    )
    previous_current = (restore_root / "current.json").read_bytes()
    next_as_of = AS_OF + timedelta(minutes=1)
    second = coordinator.run_once(expectations=expectations, as_of=next_as_of)
    real_replace = os.replace

    def failing_replace(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        destination: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        if str(destination) == "current.json":
            raise OSError("injected pointer replace failure")
        real_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )

    monkeypatch.setattr(os, "replace", failing_replace)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="pointer replace failure"):
        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks={
                item.logical_role: item.watermark for item in second.authorities
            },
        )

    assert (restore_root / "current.json").read_bytes() == previous_current
    assert any(
        json.loads(path.read_text())["status"] == "failed"
        for path in (restore_root / "audits").glob("*.json")
    )


def test_exception_after_current_replace_rolls_back_using_durable_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    first = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "post-replace-exception"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(first.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in first.authorities},
    )
    previous_current = (restore_root / "current.json").read_bytes()
    next_as_of = AS_OF + timedelta(minutes=1)
    second = coordinator.run_once(expectations=expectations, as_of=next_as_of)
    real_replace = os.replace
    injected = False

    def replace_then_raise(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        destination: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal injected
        real_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        if str(destination) == "current.json" and not injected:
            injected = True
            raise OSError("injected after durable current replace")

    monkeypatch.setattr(os, "replace", replace_then_raise)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="after durable current replace"):
        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks={
                item.logical_role: item.watermark for item in second.authorities
            },
        )

    assert injected is True
    assert (restore_root / "current.json").read_bytes() == previous_current
    assert not tuple((restore_root / "transactions").glob("*.json"))
    failed_audits = [
        json.loads(path.read_text())
        for path in (restore_root / "audits").glob("*.json")
        if json.loads(path.read_text())["status"] == "failed"
    ]
    assert any(
        audit["failure_point"] == "interrupted_current_publish_recovered" for audit in failed_audits
    )


@pytest.mark.parametrize("interrupt_type", [SystemExit, KeyboardInterrupt])
def test_base_exception_after_current_switch_rolls_back_before_propagating(
    tmp_path: Path,
    interrupt_type: type[BaseException],
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    first = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / f"base-exception-{interrupt_type.__name__}"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(first.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in first.authorities},
    )
    previous_current = (restore_root / "current.json").read_bytes()
    previous_audits = _file_set_hashes(restore_root / "audits")
    next_as_of = AS_OF + timedelta(minutes=1)
    second = coordinator.run_once(expectations=expectations, as_of=next_as_of)

    def interrupt_after_switch(point: RecoveryFaultPoint) -> None:
        if point is RecoveryFaultPoint.AFTER_CURRENT_SWITCH:
            raise interrupt_type("injected publication interrupt")

    with pytest.raises(interrupt_type, match="publication interrupt"):
        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks={
                item.logical_role: item.watermark for item in second.authorities
            },
            fault_injector=interrupt_after_switch,
        )

    assert (restore_root / "current.json").read_bytes() == previous_current
    assert not tuple((restore_root / "transactions").glob("*.json"))
    current_audits = _file_set_hashes(restore_root / "audits")
    assert current_audits.items() > previous_audits.items()
    failed_audits = [
        json.loads(path.read_text())
        for path in (restore_root / "audits").glob("*.json")
        if json.loads(path.read_text())["status"] == "failed"
    ]
    assert any(audit["failure_type"] == interrupt_type.__name__ for audit in failed_audits)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork fault injection")
def test_hard_exit_after_current_replace_is_recovered_on_next_start(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    first = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "hard-exit-recovery"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(first.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in first.authorities},
    )
    previous_current = (restore_root / "current.json").read_bytes()
    next_as_of = AS_OF + timedelta(minutes=1)
    second = coordinator.run_once(expectations=expectations, as_of=next_as_of)
    second_targets = {item.logical_role: item.watermark for item in second.authorities}

    child = os.fork()
    if child == 0:
        real_replace = recovery_module.os.replace

        def replace_then_exit(
            source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
            destination: str | bytes | os.PathLike[str] | os.PathLike[bytes],
            *,
            src_dir_fd: int | None = None,
            dst_dir_fd: int | None = None,
        ) -> None:
            real_replace(
                source,
                destination,
                src_dir_fd=src_dir_fd,
                dst_dir_fd=dst_dir_fd,
            )
            if str(destination) == "current.json":
                os._exit(77)

        recovery_module.os.replace = replace_then_exit
        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks=second_targets,
        )
        os._exit(78)

    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 77
    assert (restore_root / "current.json").read_bytes() != previous_current

    def stop_before_republish(point: RecoveryFaultPoint) -> None:
        if point is RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH:
            raise RuntimeError("stop after startup recovery")

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="stop after startup recovery"):
        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks=second_targets,
            fault_injector=stop_before_republish,
        )

    assert (restore_root / "current.json").read_bytes() == previous_current
    assert not tuple((restore_root / "transactions").glob("*.json"))
    audits = [json.loads(path.read_text()) for path in (restore_root / "audits").glob("*.json")]
    assert any(
        audit["failure_point"] == "interrupted_current_publish_recovered" for audit in audits
    )


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork fault injection")
def test_hard_exit_after_success_audit_completes_publish_on_next_start(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    first = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "hard-exit-after-success-audit"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(first.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in first.authorities},
    )
    previous_current = (restore_root / "current.json").read_bytes()
    next_as_of = AS_OF + timedelta(minutes=1)
    second = coordinator.run_once(expectations=expectations, as_of=next_as_of)
    second_targets = {item.logical_role: item.watermark for item in second.authorities}

    child = os.fork()
    if child == 0:
        recovery_module._remove_publish_intent = lambda target: os._exit(79)
        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks=second_targets,
        )
        os._exit(80)

    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 79
    published_current = (restore_root / "current.json").read_bytes()
    assert published_current != previous_current
    assert (restore_root / "transactions" / "active.json").is_file()

    recovered_report = coordinator.rehearse_restore(
        manifest_id=str(second.manifest_id),
        target_root=restore_root,
        expected_as_of=next_as_of,
        rto_target_seconds=30.0,
        rpo_target_watermarks=second_targets,
    )

    assert recovered_report.passed is True
    assert (restore_root / "current.json").read_bytes() == published_current
    assert not tuple((restore_root / "transactions").glob("*.json"))
    current = json.loads(published_current)
    passed_audits = [
        json.loads(path.read_text())
        for path in (restore_root / "audits").glob("*.json")
        if json.loads(path.read_text())["status"] == "passed"
    ]
    assert any(audit["report_sha256"] == current["report_sha256"] for audit in passed_audits)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork fault injection")
def test_restore_copy_starts_only_after_owner_intent_is_durable(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "intent-before-copy"
    restore_root.mkdir()
    targets = {item.logical_role: item.watermark for item in manifest.authorities}

    child = os.fork()
    if child == 0:

        def exit_when_copy_starts(*args, **kwargs) -> None:
            del args, kwargs
            intent_path = restore_root / "transactions" / "active.json"
            if not intent_path.is_file():
                os._exit(84)
            intent = json.loads(intent_path.read_text())
            if (
                intent["manifest_id"] != str(manifest.manifest_id)
                or intent["candidate_name"] != intent["owner_id"]
                or intent["stage"] != "candidate_prepared"
            ):
                os._exit(85)
            os._exit(86)

        coordinator._restore_entries = exit_when_copy_starts
        coordinator.rehearse_restore(
            manifest_id=str(manifest.manifest_id),
            target_root=restore_root,
            expected_as_of=AS_OF,
            rto_target_seconds=30.0,
            rpo_target_watermarks=targets,
        )
        os._exit(87)

    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 86
    assert (restore_root / "transactions" / "active.json").is_file()

    report = coordinator.rehearse_restore(
        manifest_id=str(manifest.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks=targets,
    )

    assert report.passed is True
    assert not tuple((restore_root / "transactions").glob("*.json"))


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork fault injection")
@pytest.mark.parametrize(
    ("fault_point", "exit_code"),
    [
        (RecoveryFaultPoint.AFTER_COPY, 88),
        (RecoveryFaultPoint.AFTER_HASH_VERIFY, 89),
        (RecoveryFaultPoint.AFTER_GENERATION_STAGE, 90),
        (RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH, 91),
        (RecoveryFaultPoint.AFTER_CURRENT_SWITCH, 92),
    ],
)
def test_hard_exit_at_each_restore_stage_is_recovered_without_orphan_candidate(
    tmp_path: Path,
    fault_point: RecoveryFaultPoint,
    exit_code: int,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    first = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "hard-exit-after-copy"
    restore_root.mkdir()
    coordinator.rehearse_restore(
        manifest_id=str(first.manifest_id),
        target_root=restore_root,
        expected_as_of=AS_OF,
        rto_target_seconds=30.0,
        rpo_target_watermarks={item.logical_role: item.watermark for item in first.authorities},
    )
    next_as_of = AS_OF + timedelta(minutes=1)
    second = coordinator.run_once(expectations=expectations, as_of=next_as_of)
    second_targets = {item.logical_role: item.watermark for item in second.authorities}

    child = os.fork()
    if child == 0:

        def exit_at_stage(point: RecoveryFaultPoint) -> None:
            if point is fault_point:
                os._exit(exit_code)

        coordinator.rehearse_restore(
            manifest_id=str(second.manifest_id),
            target_root=restore_root,
            expected_as_of=next_as_of,
            rto_target_seconds=30.0,
            rpo_target_watermarks=second_targets,
            fault_injector=exit_at_stage,
        )
        os._exit(93)

    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == exit_code
    assert (restore_root / "transactions" / "active.json").is_file()

    report = coordinator.rehearse_restore(
        manifest_id=str(second.manifest_id),
        target_root=restore_root,
        expected_as_of=next_as_of,
        rto_target_seconds=30.0,
        rpo_target_watermarks=second_targets,
    )

    assert report.passed is True
    assert not tuple((restore_root / ".candidates").iterdir())
    assert not tuple((restore_root / "transactions").glob("*.json"))
    audits = [json.loads(path.read_text()) for path in (restore_root / "audits").glob("*.json")]
    assert any(
        audit["failure_type"] in {"InterruptedRecoveryStage", "InterruptedRecoveryPublish"}
        for audit in audits
    )


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork fault injection")
def test_hard_exit_during_object_copy_is_cleaned_before_retry(tmp_path: Path) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    child = os.fork()
    if child == 0:
        real_open = os.open
        real_write = os.write
        exited = False
        object_descriptor = -1

        def track_object_open(
            path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
            flags: int,
            mode: int = 0o777,
            *,
            dir_fd: int | None = None,
        ) -> int:
            nonlocal object_descriptor
            descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
            name = str(path)
            if name.startswith(".object.") and name.endswith(".tmp"):
                object_descriptor = descriptor
            return descriptor

        def write_then_exit(descriptor: int, content: bytes | memoryview) -> int:
            nonlocal exited
            if descriptor == object_descriptor and not exited:
                exited = True
                real_write(descriptor, content[: min(len(content), 1024)])
                os._exit(83)
            return real_write(descriptor, content)

        os.open = track_object_open
        os.write = write_then_exit
        coordinator.run_once(expectations=expectations, as_of=AS_OF)
        os._exit(84)

    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 83
    assert tuple(coordinator.backup_object_store.glob(".object.*.tmp"))
    assert tuple(coordinator.backup_object_store.glob(".object.*.intent.json"))

    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert manifest.manifest_id is not None
    assert not tuple(coordinator.backup_object_store.glob(".object.*.tmp"))
    assert not tuple(coordinator.backup_object_store.glob(".object.*.intent.json"))


def test_object_copy_recovery_preserves_live_and_unowned_temporary_files(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_coordinator as recovery_module

    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    object_store = coordinator.backup_object_store
    owner_id = "f" * 32
    live_temporary = object_store / f".object.{owner_id}.tmp"
    live_temporary.parent.mkdir(parents=True, exist_ok=True)
    live_temporary.write_bytes(b"live-owner")
    live_temporary.chmod(0o600)
    intent = recovery_module._ObjectCopyIntent(
        owner_id=owner_id,
        owner_pid=os.getpid(),
        temporary_name=live_temporary.name,
        logical_role="foreign.live",
        expected_size=10,
        expected_sha256="f" * 64,
        created_at=datetime.now(UTC),
    )
    recovery_module._write_object_copy_intent(object_store, intent)
    unowned_temporary = object_store / f".object.{'e' * 32}.tmp"
    unowned_temporary.write_bytes(b"no-intent")
    unowned_temporary.chmod(0o600)

    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)

    assert manifest.manifest_id is not None
    assert live_temporary.read_bytes() == b"live-owner"
    assert (object_store / f".object.{owner_id}.intent.json").is_file()
    assert unowned_temporary.read_bytes() == b"no-intent"


def test_repeating_same_manifest_is_idempotent_for_generation_and_current(
    tmp_path: Path,
) -> None:
    coordinator, expectations, _, _, _ = _fixture(tmp_path)
    manifest = coordinator.run_once(expectations=expectations, as_of=AS_OF)
    restore_root = tmp_path / "idempotent-restore"
    restore_root.mkdir()
    kwargs = {
        "manifest_id": str(manifest.manifest_id),
        "target_root": restore_root,
        "expected_as_of": AS_OF,
        "rto_target_seconds": 30.0,
        "rpo_target_watermarks": {
            item.logical_role: item.watermark for item in manifest.authorities
        },
    }

    first_report = coordinator.rehearse_restore(**kwargs)
    first_current = (restore_root / "current.json").read_bytes()
    first_audits = _file_set_hashes(restore_root / "audits")
    first_reports = _file_set_hashes(restore_root / "reports")
    second_report = coordinator.rehearse_restore(**kwargs)

    assert (restore_root / "current.json").read_bytes() == first_current
    assert second_report == first_report
    assert len(tuple((restore_root / "generations").iterdir())) == 1
    assert _file_set_hashes(restore_root / "audits") == first_audits
    assert _file_set_hashes(restore_root / "reports") == first_reports
    current = json.loads(first_current)
    audits = [json.loads(path.read_text()) for path in (restore_root / "audits").glob("*.json")]
    assert len(audits) == 1
    assert {audit["report_sha256"] for audit in audits} == {current["report_sha256"]}
    report_path = restore_root / "reports" / f"{current['report_sha256']}.json"
    assert report_path.is_file()
    assert json.loads(report_path.read_text()) == first_report.model_dump(mode="json")


def test_transient_source_rebind_is_detected_after_original_name_is_restored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    source.write_text('{"max_date":"2026-07-31","row_count":1}')
    moved = tmp_path / "moved.json"
    real_read = os.read
    triggered = False

    def racing_read(descriptor: int, length: int) -> bytes:
        nonlocal triggered
        if not triggered:
            triggered = True
            source.rename(moved)
            moved.rename(source)
        return real_read(descriptor, length)

    monkeypatch.setattr(os, "read", racing_read)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="changed"):
        RecoveryArtifactVerificationContext.for_source(
            logical_role="serving.current",
            artifact_role=RecoveryArtifactRole.SERVING_CURRENT,
            artifact_path=source,
            generation_id="generation-1",
            schema_version="v1",
            as_of=AS_OF,
            related_artifacts=(),
        )
    assert triggered is True


def test_source_group_change_during_read_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    source.write_text('{"max_date":"2026-07-31","row_count":1}')
    current_gid = source.stat().st_gid
    alternate_gids = tuple(group for group in os.getgroups() if group != current_gid)
    if not alternate_gids:
        pytest.skip("current user has no alternate supplementary group")
    real_read = os.read
    triggered = False

    def racing_read(descriptor: int, length: int) -> bytes:
        nonlocal triggered
        if not triggered:
            triggered = True
            os.chown(source, -1, alternate_gids[0])
        return real_read(descriptor, length)

    monkeypatch.setattr(os, "read", racing_read)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="changed"):
        RecoveryArtifactVerificationContext.for_source(
            logical_role="serving.current",
            artifact_role=RecoveryArtifactRole.SERVING_CURRENT,
            artifact_path=source,
            generation_id="generation-1",
            schema_version="v1",
            as_of=AS_OF,
            related_artifacts=(),
        )
    assert triggered is True


def test_public_fixed_replay_verifier_runs_all_real_strategy_contracts(
    tmp_path: Path,
) -> None:
    _, _, artifacts, sources_root, _ = _fixture(tmp_path)
    expectations = _runtime_replay_expectations(artifacts)
    verifier = RuntimeRecoveryFixedReplayVerifier(expectations=expectations)

    receipts = verifier.verify(
        target_root=sources_root,
        dataset_path=artifacts["production.duckdb"],
    )

    assert {receipt.strategy_id for receipt in receipts} == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }
    assert {receipt.replay_fingerprint for receipt in receipts} == {
        expectation.expected_result_sha256 for expectation in expectations
    }
    assert (
        verifier.fingerprint
        == RuntimeRecoveryFixedReplayVerifier(
            expectations=expectations,
        ).fingerprint
    )


def test_public_fixed_replay_expectation_builder_uses_frozen_dataset_and_bindings(
    tmp_path: Path,
) -> None:
    _, _, artifacts, sources_root, _ = _fixture(tmp_path)

    expectations = build_runtime_recovery_fixed_replay_expectations(
        target_root=sources_root,
        dataset_path=artifacts["production.duckdb"],
        strategy_bindings=_strategy_bindings(),
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    )

    assert {item.strategy_id for item in expectations} == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }
    assert {item.result.strategy_registration_fingerprint for item in expectations} == set(
        STRATEGY_REGISTRATION_FINGERPRINTS.values()
    )
    verifier = RuntimeRecoveryFixedReplayVerifier(expectations=expectations)
    assert (
        len(
            verifier.verify(
                target_root=sources_root,
                dataset_path=artifacts["production.duckdb"],
            )
        )
        == 3
    )


def test_public_fixed_replay_verifier_binds_expected_results_and_dataset(
    tmp_path: Path,
) -> None:
    _, _, artifacts, sources_root, _ = _fixture(tmp_path)
    expectations = _runtime_replay_expectations(artifacts)
    forged_payload = expectations[0].model_dump(mode="python")
    forged_result = dict(forged_payload["result"])
    forged_result["total_return_bps"] = int(forged_result["total_return_bps"]) + 1
    forged_result.pop("result_sha256", None)
    forged_payload["result"] = forged_result
    forged_payload["expected_result_sha256"] = canonical_sha256(forged_result)
    forged = RuntimeRecoveryFixedReplayExpectation.model_validate(forged_payload)
    verifier = RuntimeRecoveryFixedReplayVerifier(
        expectations=(forged, *expectations[1:]),
    )

    with pytest.raises(RuntimeRecoveryCoordinatorError, match="recomputed metrics"):
        verifier.verify(
            target_root=sources_root,
            dataset_path=artifacts["production.duckdb"],
        )

    copied = sources_root / "data" / "copied-production.duckdb"
    shutil.copyfile(artifacts["production.duckdb"], copied)
    with copied.open("ab") as handle:
        handle.write(b"tampered")
    trusted = RuntimeRecoveryFixedReplayVerifier(expectations=expectations)
    with pytest.raises(RuntimeRecoveryCoordinatorError, match="hash"):
        trusted.verify(target_root=sources_root, dataset_path=copied)
