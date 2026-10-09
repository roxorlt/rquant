from __future__ import annotations

from pathlib import Path
from uuid import uuid4
import hashlib

import pytest

from rquant.lab_worker import build_builtin_shard_runtime_manifest
from rquant.lab_worker_registry import (
    BuiltinLabShardRuntimeConfig, builtin_lab_shard_configuration,
    resolve_builtin_adapter_registry, unconfigured_builtin_lab_shard_configuration,
)
from rquant.minute_backtest_installation import load_minute_replay_installation
from rquant.minute_backtest_producer import MinutePrivateFileReference, MinutePublicationReference, MinuteReplayCatalog
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from rquant.experiment_registry import ExperimentRegistry, ExperimentRegistryError, ExperimentRegistryReadonlyReader


def reference_catalog(root: Path) -> MinuteReplayCatalog:
    def reference(name: str) -> MinutePrivateFileReference:
        return MinutePrivateFileReference(path=root / name, device=1, inode=1, size_bytes=1,
            mtime_ns=1, owner_uid=0, content_sha256="a" * 64)
    return MinuteReplayCatalog(entries=(MinutePublicationReference(source_key="source", source_version=1,
        owner_id="researcher", receipt=reference("receipt.json"), source=reference("source.duckdb")),))


def test_uninstalled_registry_and_configuration_bytes_remain_exact() -> None:
    config = unconfigured_builtin_lab_shard_configuration()
    assert "minute_registry_mode" not in config.model_dump(mode="json")
    assert "minute_catalog" not in config.model_dump(mode="json")
    restored = BuiltinLabShardRuntimeConfig.model_validate_json(config.model_dump_json())
    assert resolve_builtin_adapter_registry(restored).closed_descriptor() == default_strategy_job_adapter_registry().closed_descriptor()
    assert len(resolve_builtin_adapter_registry(restored).closed_descriptor().adapters) == 5


def test_installed_additive_registry_retains_five_and_has_one_minute_adapter(tmp_path: Path) -> None:
    catalog = reference_catalog(tmp_path)
    kwargs = dict(catalog_path=tmp_path / "metadata", forbidden_paths=(), snapshot_root=tmp_path / "copies",
        research_lake_root=tmp_path / "lake", minute_catalog=catalog)
    isolated = builtin_lab_shard_configuration(**kwargs)
    installed = builtin_lab_shard_configuration(**kwargs, minute_registry_mode="installed")
    assert "minute_registry_mode" not in isolated.model_dump(mode="json")
    assert len(resolve_builtin_adapter_registry(isolated).closed_descriptor().adapters) == 1
    descriptor = resolve_builtin_adapter_registry(installed).closed_descriptor()
    assert descriptor.adapters[:5] == default_strategy_job_adapter_registry().closed_descriptor().adapters
    assert [(x.adapter_id, x.adapter_version) for x in descriptor.adapters[5:]] == [("minute-runtime-replay", "2")]
    wire = build_builtin_shard_runtime_manifest(**kwargs, minute_registry_mode="installed")
    restored = BuiltinLabShardRuntimeConfig.model_validate_json(wire.registry.configuration_json)
    assert restored == installed
    assert resolve_builtin_adapter_registry(restored).closed_descriptor() == descriptor


def test_additive_mode_cannot_exist_without_its_complete_catalog() -> None:
    with pytest.raises(ValueError, match="installed.*catalog"):
        BuiltinLabShardRuntimeConfig(configured=False,
            adapter_manifest_hash=default_strategy_job_adapter_registry().closed_descriptor().manifest_hash,
            minute_registry_mode="installed")


@pytest.mark.parametrize("unsafe", ["public", "symlink", "hardlink"])
def test_private_installation_is_checked_before_json_or_source_open(tmp_path: Path, unsafe: str) -> None:
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    path = root / "installation.json"
    path.write_bytes(b"{}")
    path.chmod(0o600)
    if unsafe == "public":
        path.chmod(0o644)
    elif unsafe == "symlink":
        target = root / "original.json"
        path.rename(target)
        path.symlink_to(target)
    else:
        (root / "second.json").hardlink_to(path)
    with pytest.raises((PermissionError, OSError)):
        load_minute_replay_installation(path)


def test_original_readonly_intent_lookup_returns_absence_without_mutating_ledger(tmp_path: Path) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    assert reader.get_submission_intent_for_job(uuid4()) is None
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_original_readonly_intent_lookup_keeps_private_file_authority(tmp_path: Path) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    path.chmod(0o644)
    with pytest.raises(ExperimentRegistryError, match="path"):
        reader.get_submission_intent_for_job(uuid4())
