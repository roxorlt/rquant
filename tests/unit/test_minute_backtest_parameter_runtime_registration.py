from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import TypeAdapter

from rquant.lab_job_center import ResearchRunInput, _run_identity
from rquant.lab_worker import build_builtin_shard_runtime_manifest
from rquant.lab_worker_registry import BuiltinLabShardRuntimeConfig, builtin_lab_shard_configuration, resolve_builtin_adapter_registry
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalRunInput
from rquant.minute_backtest_parameter_producer import MinuteParameterPublicationReference, MinuteParameterReplayCatalog
from rquant.minute_backtest_producer import MinutePrivateFileReference
from rquant.research_run_spec import ResearchJobType
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from tests.unit.test_minute_backtest_parameter_adapter import complete_seed, complete_source, source_root


def parameter_catalog(root: Path) -> MinuteParameterReplayCatalog:
    def reference(name: str) -> MinutePrivateFileReference:
        return MinutePrivateFileReference(path=root / name, device=1, inode=1, size_bytes=1,
            mtime_ns=1, owner_uid=0, content_sha256="a" * 64)
    return MinuteParameterReplayCatalog(entries=(MinuteParameterPublicationReference(
        source_key="parameters", source_version=1, owner_id="researcher",
        receipt=reference("receipt.json"), source=reference("source.duckdb")),))


def test_parameter_job_decode_does_not_alias_the_original_native_input(complete_source: object) -> None:
    value = MinuteParameterFormalRunInput.from_frozen(complete_source)
    restored = TypeAdapter(ResearchRunInput).validate_json(value.model_dump_json())
    assert type(restored) is MinuteParameterFormalRunInput
    identity = _run_identity(restored)
    assert identity[:3] == ("minute_parameter_replay", ResearchJobType.STRATEGY_REPLAY, "minute-parameter-replay")
    assert identity[3] == value.parameters


def test_parameter_installed_worker_configuration_keeps_the_original_five(tmp_path: Path) -> None:
    catalog = parameter_catalog(tmp_path)
    kwargs = dict(catalog_path=tmp_path / "metadata", forbidden_paths=(), snapshot_root=tmp_path / "copies",
        research_lake_root=tmp_path / "lake", parameter_catalog=catalog)
    config = builtin_lab_shard_configuration(**kwargs, minute_registry_mode="installed")
    descriptor = resolve_builtin_adapter_registry(config).closed_descriptor()
    assert descriptor.adapters[:5] == default_strategy_job_adapter_registry().closed_descriptor().adapters
    assert [(x.adapter_id, x.adapter_version) for x in descriptor.adapters[5:]] == [("minute-parameter-replay", "1")]
    wire = build_builtin_shard_runtime_manifest(**kwargs, minute_registry_mode="installed")
    restored = BuiltinLabShardRuntimeConfig.model_validate_json(wire.registry.configuration_json)
    assert restored == config
    assert resolve_builtin_adapter_registry(restored).closed_descriptor() == descriptor
    with pytest.raises(ValueError):
        BuiltinLabShardRuntimeConfig.model_validate_json(config.model_dump_json().replace(
            '"minute-parameter-replay-catalog/v1"', '"minute-replay-catalog/v1"'))


def test_omitted_parameter_catalog_preserves_original_configuration_bytes() -> None:
    from rquant.lab_worker_registry import unconfigured_builtin_lab_shard_configuration

    config = unconfigured_builtin_lab_shard_configuration()
    assert "parameter_catalog" not in config.model_dump(mode="json")
    assert "parameter_catalog" not in config.model_dump_json()
    restored = BuiltinLabShardRuntimeConfig.model_validate_json(config.model_dump_json())
    assert resolve_builtin_adapter_registry(restored).closed_descriptor() == default_strategy_job_adapter_registry().closed_descriptor()
