"""The catalog only publishes three definitions from one verified runtime generation."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.runtime_builder_serving import serving_publisher_builder
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.runtime_generation_lineage import load_runtime_generation_tree
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_publisher import ServingReader
from rquant.strategy_catalog_source import (
    StrategyCatalogAuthorityPublisher,
    StrategyCatalogSourceReader,
    _display_parameter,
)
from tests.unit.test_runtime_builder_serving import _authority_settings
from tests.unit.test_runtime_builder_serving import _manifest as _serving_manifest
from tests.unit.test_runtime_deployment_bundle import (
    _manifest,
    install_runtime_deployment_bundle,
    isolated_root_credential_sealer,  # noqa: F401 -- isolated test install
)

COMMIT = "a" * 40
NOW = datetime(2026, 9, 29, 8, tzinfo=UTC)


@pytest.mark.parametrize("value", [float("inf"), 10**9])
def test_catalog_rejects_unbounded_parameter_values(value: float | int) -> None:
    with pytest.raises(ValueError, match="bound"):
        _display_parameter("min_rel_cumulative", value)


def _install_catalog(
    tmp_path: Path,
    *,
    missing: str | None = None,
    wrong_field: tuple[str, str] | None = None,
) -> Path:
    runtime_root = tmp_path / "runtime"
    registry_root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=COMMIT)
    bootstrap_builtin_definitions(
        registry_root,
        producer_commit=COMMIT,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=plan.plan_id,
    )
    manifests: list[RuntimeServiceManifest] = []
    for binding in plan.strategies:
        if binding.strategy_id == missing:
            continue
        service_id = f"strategy.{binding.strategy_id}.v1"
        base = _manifest(
            runtime_root,
            service_id=service_id,
            kind=RuntimeServiceKind.STRATEGY_LIVE,
            plane=RuntimeServicePlane.LIVE,
        )
        settings = {
            **dict(base.settings),
            "definition_registry_root": str(registry_root),
            "strategy_id": binding.strategy_id,
            "strategy_version": binding.strategy_version,
            "strategy_registration_fingerprint": binding.registration_fingerprint,
            "strategy_spec_fingerprint": binding.strategy_spec_fingerprint,
            "strategy_executable_fingerprint": binding.executable_fingerprint,
            "evaluator_contract_fingerprint": binding.executable_fingerprint,
            "candidate_schema_fingerprint": binding.candidate_schema_fingerprint,
        }
        if wrong_field is not None and binding.strategy_id == "n_shape":
            settings[wrong_field[0]] = wrong_field[1]
        manifests.append(
            RuntimeServiceManifest.model_validate(
                {
                    **base.model_dump(mode="python"),
                    "settings": settings,
                }
            )
        )
    install_runtime_deployment_bundle(
        runtime_root,
        producer_commit=COMMIT,
        manifests=tuple(manifests),
        capability_env={manifest.service_id: {} for manifest in manifests},
    )
    return runtime_root


def test_catalog_reads_three_real_registered_specs_without_private_identity(tmp_path: Path) -> None:
    root = _install_catalog(tmp_path)
    result = StrategyCatalogSourceReader(runtime_root=root)(NOW)
    tables = {projection.table_name: projection for projection in result.payload.projections}
    assert set(tables) == {"strategy_catalog", "strategy_catalog_parameter"}
    rows = tables["strategy_catalog"].rows
    assert {row["strategy_id"] for row in rows} == {"auction_gap", "growth_board_surge", "n_shape"}
    assert all(row["version"] == 1 for row in rows)
    assert len(tables["strategy_catalog_parameter"].rows) >= 3
    public_values = str(tuple(tables.values()))
    assert str(tmp_path) not in public_values
    assert COMMIT not in public_values
    assert "a" * 64 not in public_values


@pytest.mark.parametrize(
    ("missing", "wrong_field"),
    [
        ("n_shape", None),
        (None, ("strategy_registration_fingerprint", "f" * 64)),
        (None, ("strategy_executable_fingerprint", "f" * 64)),
        (None, ("definition_registry_root", "FOREIGN_ROOT")),
    ],
)
def test_catalog_rejects_missing_or_mismatched_definition(
    tmp_path: Path,
    missing: str | None,
    wrong_field: tuple[str, str] | None,
) -> None:
    if wrong_field == ("definition_registry_root", "FOREIGN_ROOT"):
        wrong_field = ("definition_registry_root", str(tmp_path / "foreign-definitions"))
    root = _install_catalog(tmp_path, missing=missing, wrong_field=wrong_field)
    with pytest.raises((ValueError, RuntimeError)):
        StrategyCatalogSourceReader(runtime_root=root)(NOW)


def test_catalog_publishes_and_reads_one_verified_source_authority(tmp_path: Path) -> None:
    runtime_root = _install_catalog(tmp_path)
    authority_root = tmp_path / "source"
    reader = StrategyCatalogSourceReader(runtime_root=runtime_root)
    publisher = ServingSourceAuthorityPublisher(
        root=authority_root,
        producer_commit=COMMIT,
        dataset_id="strategy_catalog",
        payload_kind="strategy_catalog",
        clock=lambda: NOW,
    )
    pointer = StrategyCatalogAuthorityPublisher(reader=reader, publisher=publisher).publish(NOW)
    selected = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id="strategy_catalog",
        expected_payload_kind="strategy_catalog",
    )(NOW)
    assert selected.generation_id == pointer.generation_id
    assert len(selected.payload.projections[0].rows) == 3


def test_catalog_rejects_tampered_executable_binding(tmp_path: Path) -> None:
    root = _install_catalog(tmp_path)
    records = (tmp_path / "definitions" / "strategies").rglob("*.json")
    target = next(
        path
        for path in records
        if json.loads(path.read_text(encoding="utf-8")).get("logical_id") == "n_shape"
    )
    target.chmod(0o600)
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["execution_binding"]["runtime_evaluator_fingerprint"] = "f" * 64
    target.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    target.chmod(0o400)
    with pytest.raises((ValueError, RuntimeError)):
        StrategyCatalogSourceReader(runtime_root=root)(NOW)


def test_catalog_source_projects_into_one_serving_generation(tmp_path: Path) -> None:
    runtime_root = _install_catalog(tmp_path)
    settings, _roots = _authority_settings(tmp_path)
    source_root = tmp_path / "authorities" / "strategy_catalog"
    source_reader = StrategyCatalogSourceReader(runtime_root=runtime_root)
    source_publisher = ServingSourceAuthorityPublisher(
        root=source_root,
        producer_commit=COMMIT,
        dataset_id="strategy_catalog",
        payload_kind="strategy_catalog",
        clock=lambda: NOW,
    )
    StrategyCatalogAuthorityPublisher(reader=source_reader, publisher=source_publisher).publish(NOW)
    settings["source_authorities"].append(
        {"dataset_id": "strategy_catalog", "root": str(source_root)}
    )
    step = serving_publisher_builder(
        snapshot_loader=None, clock=lambda: NOW, runtime_root=runtime_root
    )(_serving_manifest(tmp_path, settings=settings))
    result = step()
    assert result.generation_published is True
    with ServingReader(tmp_path / "serving").acquire_generation() as lease:
        assert lease.manifest.source_generations["strategy_catalog"]
        assert lease.connection.execute(
            "SELECT name, version FROM strategy_catalog ORDER BY strategy_id"
        ).fetchall() == [
            ("集合竞价跳空", 1),
            ("科创及创业板放量", 1),
            ("N 字形态", 1),
        ]
        assert lease.connection.execute(
            "SELECT count(*) FROM strategy_catalog_parameter"
        ).fetchone() == (19,)


def test_serving_drops_old_catalog_after_current_runtime_removes_strategy(tmp_path: Path) -> None:
    runtime_root = _install_catalog(tmp_path)
    settings, _roots = _authority_settings(tmp_path)
    source_root = tmp_path / "authorities" / "strategy_catalog"
    source_reader = StrategyCatalogSourceReader(runtime_root=runtime_root)
    source_publisher = ServingSourceAuthorityPublisher(
        root=source_root,
        producer_commit=COMMIT,
        dataset_id="strategy_catalog",
        payload_kind="strategy_catalog",
        clock=lambda: NOW,
    )
    StrategyCatalogAuthorityPublisher(reader=source_reader, publisher=source_publisher).publish(NOW)
    settings["source_authorities"].append(
        {"dataset_id": "strategy_catalog", "root": str(source_root)}
    )
    step = serving_publisher_builder(
        snapshot_loader=None, clock=lambda: NOW, runtime_root=runtime_root
    )(_serving_manifest(tmp_path, settings=settings))
    first = step()
    assert first.generation_published is True

    tree = load_runtime_generation_tree(runtime_root)
    retained = tuple(
        tree.lineage(f"strategy.{strategy_id}.v1").current.manifest
        for strategy_id in ("auction_gap", "growth_board_surge")
    )
    install_runtime_deployment_bundle(
        runtime_root,
        producer_commit=COMMIT,
        manifests=retained,
        capability_env={manifest.service_id: {} for manifest in retained},
    )
    with pytest.raises(ValueError):
        source_reader(NOW)
    old_source = ServingSourceAuthorityReader(
        root=source_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id="strategy_catalog",
        expected_payload_kind="strategy_catalog",
    )(NOW)
    assert len(old_source.payload.projections[0].rows) == 3

    second = step()
    assert second.generation_published is True
    assert any(
        reason.startswith("serving:strategy_catalog:unavailable:")
        for reason in second.degraded_reasons
    )
    with ServingReader(tmp_path / "serving").acquire_generation() as lease:
        assert lease.connection.execute("SELECT COUNT(*) FROM strategy_catalog").fetchone() == (0,)
        mark = next(
            item for item in lease.manifest.watermarks if item.dataset_id == "strategy_catalog"
        )
        assert mark.status is FreshnessStatus.UNAVAILABLE


def test_catalog_rejects_current_pointer_switch_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.strategy_catalog_source as source_module

    runtime_root = _install_catalog(tmp_path)
    first = load_runtime_generation_tree(runtime_root)
    manifests = [
        first.lineage(f"strategy.{strategy_id}.v1").current.manifest
        for strategy_id in ("auction_gap", "growth_board_surge", "n_shape")
    ]
    changed = RuntimeServiceManifest.model_validate(
        {
            **manifests[0].model_dump(mode="python"),
            "settings": {**dict(manifests[0].settings), "batch_limit": 127},
        }
    )
    calls = 0

    def switching_tree(root: Path):
        nonlocal calls
        calls += 1
        if calls == 2:
            replacement = (changed, *manifests[1:])
            install_runtime_deployment_bundle(
                runtime_root,
                producer_commit=COMMIT,
                manifests=replacement,
                capability_env={manifest.service_id: {} for manifest in replacement},
            )
        return load_runtime_generation_tree(root)

    monkeypatch.setattr(source_module, "load_runtime_generation_tree", switching_tree)
    with pytest.raises(ValueError, match="changed during publication"):
        StrategyCatalogSourceReader(runtime_root=runtime_root)(NOW)
    assert calls == 2
