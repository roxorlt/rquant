"""Health lease/owner/privacy rules over offline fixtures, not production proof."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rquant.paper_portfolio_projection import PaperPortfolioPublishedAccount
    from rquant.runtime_serving_snapshot import SourceReadResult

from tests.support.web_serving_fixture import build_web_fixture
from tests.unit.test_web_overview_health import AFTER_CLOSE, HOLIDAY, _get


def test_old_generation_has_six_unknown_layers_without_fake_zero(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    data = _get(root, "/api/v1/health", AFTER_CLOSE)["data"]
    assert [item["key"] for item in data["layers"]] == [
        "host",
        "market",
        "strategy",
        "orders",
        "risk",
        "comparison",
    ]
    assert all(item["metrics"] == [] for item in data["layers"])
    assert all(item["status"]["state"] != "ok" for item in data["layers"])
    assert all(item["links"] for item in data["layers"])


def test_holiday_missing_observation_does_not_turn_layers_red(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    data = _get(root, "/api/v1/health", HOLIDAY)["data"]
    assert all(item["status"]["state"] != "crit" for item in data["layers"])


def _publish_private_health(
    root: Path,
    read: SourceReadResult,
    *,
    paper_generation: str | None = None,
    account_ids: tuple[str, ...] | None = None,
) -> None:
    """Real paper-owner output plus the separately verified synthetic core rules."""
    from datetime import timedelta

    from rquant.runtime_builder_paper import paper_health_metrics_for_publication
    from rquant.runtime_health_details import (
        RuntimeHealthHeartbeatMaterial,
        build_runtime_health_detail_graph,
        validate_runtime_health_detail_graph,
    )
    from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
    from rquant.serving_publisher import ServingPublisher
    from rquant.serving_read_models import (
        SERVING_TABLE_SPECS,
        ServingProjectionInput,
        ServingReadModelInput,
        build_serving_read_models,
    )
    from tests.unit.test_runtime_health_details import (
        GENERATION,
        context,
        heartbeat,
        legacy,
        spec,
        witness,
    )

    at = read.published_at
    account_id = read.payload.paper_accounts[0].account_id
    facts = tuple(
        metric
        for key in account_ids or (account_id,)
        for metric in paper_health_metrics_for_publication(
            read, account_id=key, fallback_configuration_identity="f" * 64
        )
    )
    hb = heartbeat(started_at=at - timedelta(seconds=60), heartbeat_at=at, last_success_at=at)
    binding = witness(hb, sampled_at=hb.started_at)
    hb = type(hb).model_validate(
        hb.model_dump(mode="python") | {"startup_witness": binding, "health_metrics": facts}
    )
    source = RuntimeHealthHeartbeatMaterial.from_read(
        control_root=root / "fixture-control",
        spec=spec(),
        heartbeat=hb,
        observed_at=at,
        startup_witness=binding,
        metrics=facts,
    )
    values = dict(
        legacy_services=(legacy(hb, at=at),),
        source_receipts={hb.service_id: source.source_receipt},
        context=context(sampled_at=at),
        owner_generation_id=GENERATION,
        observed_at=at,
    )
    graph = build_runtime_health_detail_graph(materials=(source,), enabled=True, **values)
    verified = validate_runtime_health_detail_graph(graph, **values)
    generation = paper_generation or read.generation_id
    inputs = ServingReadModelInput(
        observed_at=at,
        runtime_services=values["legacy_services"],
        runtime_health_details=verified,
        projections=tuple(
            ServingProjectionInput(**p.model_dump(mode="python")) for p in graph.projections
        )
        + tuple(
            ServingProjectionInput.bind(
                p, owner_dataset_id="paper_accounts", owner_generation_id=generation
            )
            for p in read.payload.projections
        ),
    )
    publisher = ServingPublisher(
        root, producer_commit="a" * 40, schema_version=1, table_specs=SERVING_TABLE_SPECS
    )
    publisher.publish(
        build_serving_read_models(inputs),
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="paper_accounts",
                generation_id=generation,
                event_time=at,
                published_at=at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
            ServingDatasetWatermark(
                dataset_id="runtime_health",
                generation_id=GENERATION,
                event_time=at,
                published_at=at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"paper_accounts": generation, "runtime_health": GENERATION},
        built_at=at,
    )


def _private_get(root: Path, at: datetime, viewer: str | None) -> dict[str, object]:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.security import current_user
    from rquant.web.settings import WebSettings

    app = create_app(WebSettings(serving_root=root), clock=lambda: at, background=False)
    # Explicit synthetic identity, not a proxy ingress or real authentication claim.
    app.dependency_overrides[current_user] = lambda: viewer
    with TestClient(app) as client:
        response = client.get("/api/v1/health")
    assert response.status_code == 200, response.text
    return response.json()["data"]


def _exposure_read(
    tmp_path: Path, *, unknown: bool = False, publication_at: datetime | None = None
) -> tuple[PaperPortfolioPublishedAccount, SourceReadResult]:
    import sqlite3
    from contextlib import closing

    from tests.unit.test_runtime_health_owner_metrics import _exposure_fixture, _selected_paper

    source, snapshot, benchmark = _exposure_fixture(tmp_path)
    source.health_metrics_enabled = True
    at = publication_at or snapshot.available_at
    if publication_at is not None:
        from datetime import timedelta

        source.runtime.exposure_store.publish_benchmark(
            type(benchmark).model_validate(
                benchmark.model_dump(mode="python")
                | {
                    "observed_at": at,
                    "available_at": at,
                    "valid_through": at + timedelta(seconds=5),
                }
            )
        )
    if unknown or publication_at is not None:
        from datetime import timedelta

        original = snapshot.accounts[0].market_material
        if publication_at is None:
            at += timedelta(microseconds=1)
        source.runtime.materials.publish(
            type(original).model_validate(
                original.model_dump(mode="python")
                | {
                    "observed_at": at,
                    "available_at": at,
                    "facts": tuple(
                        f.model_copy(
                            update={
                                "industry_l1": None if unknown else f.industry_l1,
                                "observed_at": at,
                                "available_at": at,
                            }
                        )
                        for f in original.facts
                    ),
                }
            )
        )
    with closing(sqlite3.connect(source.broker.path)) as pinned:
        pinned.execute("SELECT count(*) FROM paper_order").fetchone()
        account = source.read(as_of=at)
    return account, _selected_paper(source, account, at)


def test_health_keeps_complete_original_exposure_and_scope_for_its_owner(tmp_path: Path) -> None:
    account, read = _exposure_read(tmp_path / "paper", unknown=True)
    root = tmp_path / "serving"
    _publish_private_health(root, read)
    data = _private_get(root, read.published_at, account.configuration.binding.owner_id)
    risk = next(row for row in data["layers"] if row["key"] == "risk")
    original = account.exposure.exposure.rows
    assert {row["kind"] for row in risk["exposure"]} == {"industry", "unknown", "cash"}
    assert [
        (row["portfolio_weight"], row["benchmark_weight"], row["deviation"])
        for row in risk["exposure"]
    ] == [
        (str(row.portfolio_weight), str(row.benchmark_weight), str(row.deviation))
        for row in original
    ]
    cash = next(row for row in risk["metrics"] if row["name"] == "现金权重")
    assert all(row["scope_detail"] == cash["scope_detail"] for row in risk["exposure"])
    assert all(row["source_generation_id"] == read.generation_id for row in risk["exposure"])
    assert all(row["valid_until"] == cash["valid_until"] for row in risk["exposure"])
    for actor in (None, "other-owner"):
        hidden = _private_get(root, read.published_at, actor)
        assert all(not row["metrics"] and not row["exposure"] for row in hidden["layers"])
        assert all(not item["detail"]["observations"] for item in hidden["services"])


def test_realtime_exposure_expires_at_original_benchmark_while_as_of_orders_survive(
    tmp_path: Path,
) -> None:
    from datetime import timedelta

    account, read = _exposure_read(tmp_path / "paper")
    root = tmp_path / "serving"
    _publish_private_health(root, read)
    data = _private_get(
        root, read.published_at + timedelta(seconds=6), account.configuration.binding.owner_id
    )
    risk = next(row for row in data["layers"] if row["key"] == "risk")
    cash = next(row for row in risk["metrics"] if row["name"] == "现金权重")
    assert not cash["available"] and cash["value"] is None and risk["exposure"] == []
    orders = next(row for row in data["layers"] if row["key"] == "orders")
    assert orders["metrics"][0]["temporal_basis"] == "as_of" and orders["metrics"][0]["available"]


def test_changed_paper_owner_generation_makes_old_bound_values_unknown(tmp_path: Path) -> None:
    account, read = _exposure_read(tmp_path / "paper")
    root = tmp_path / "serving"
    _publish_private_health(root, read, paper_generation="e" * 64)
    data = _private_get(root, read.published_at, account.configuration.binding.owner_id)
    assert all(
        not metric["available"] and metric["value"] is None
        for layer in data["layers"]
        for metric in layer["metrics"]
    )
    assert all(layer["exposure"] == [] for layer in data["layers"])


def test_verified_sealed_comparison_survives_without_a_fake_expiry(tmp_path: Path) -> None:
    from datetime import timedelta

    from rquant.paper_portfolio_view_source import publish_paper_band_position
    from tests.unit.test_runtime_health_owner_metrics import _closed_comparison, _selected_paper

    source, original, at = _closed_comparison(tmp_path / "paper")
    account = publish_paper_band_position(original)
    read = _selected_paper(source, account, at)
    root = tmp_path / "serving"
    _publish_private_health(root, read)
    data = _private_get(root, at + timedelta(days=1), account.configuration.binding.owner_id)
    comparison = next(layer for layer in data["layers"] if layer["key"] == "comparison")
    item = comparison["metrics"][0]
    assert item["available"] and item["value"] == account.band_position
    assert item["temporal_basis"] == "as_of" and item["valid_until"] is None


def _multi_exposure_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, publication_at: datetime | None = None
) -> tuple[tuple[PaperPortfolioPublishedAccount, ...], SourceReadResult]:
    """Two original configured brokers/quotes, published as one complete typed graph."""
    from rquant.paper_portfolio_projection import (
        PaperPortfolioSnapshot,
        paper_portfolio_projections,
    )
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_serving_snapshot import PaperAccountsPayload, SourceReadResult
    from tests.unit import test_paper_portfolio_core as core
    from tests.unit import test_paper_portfolio_pause_chain as pause
    from tests.unit import test_paper_signal_worker as worker

    first, first_read = _exposure_read(
        tmp_path / "first", unknown=True, publication_at=publication_at
    )
    original_materials = core.materials
    with monkeypatch.context() as context:
        context.setattr(core, "ACCOUNT_ID", "paper-second")
        context.setattr(worker, "ACCOUNT_ID", "paper-second")
        context.setattr(
            pause,
            "materials",
            lambda path: original_materials(
                path, weight={"method": "equal", "max_positions": 1, "cash_reserve": ".5"}
            ),
        )
        second, second_read = _exposure_read(
            tmp_path / "second", unknown=True, publication_at=publication_at
        )
    assert first_read.published_at == second_read.published_at
    accounts = tuple(
        sorted((first, second), key=lambda item: item.configuration.binding.account_id)
    )
    snapshot = PaperPortfolioSnapshot(available_at=first_read.published_at, accounts=accounts)
    history = tuple(
        row
        for row in first_read.payload.projections
        if row.table_name.startswith(("paper_order_", "paper_fill_"))
    )
    values = first_read.model_dump(mode="python", exclude={"payload", "generation_id"}) | {
        "payload": PaperAccountsPayload(
            paper_accounts=tuple(item.frame.account for item in accounts),
            projections=history + paper_portfolio_projections(snapshot),
        )
    }
    # This is the one newly published owner version, not either single-account version.
    read = SourceReadResult(generation_id=canonical_sha256(values), **values)
    assert read.generation_id not in (first_read.generation_id, second_read.generation_id)
    return accounts, read


def test_same_owner_full_multi_account_exposure_never_copies_retained_orders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.runtime_builder_paper import paper_health_metrics_for_publication

    accounts, read = _multi_exposure_read(tmp_path / "paper", monkeypatch)
    keys = tuple(item.configuration.binding.account_id for item in accounts)
    facts = {
        key: paper_health_metrics_for_publication(
            read, account_id=key, fallback_configuration_identity="f" * 64
        )
        for key in keys
    }
    retained_account = next(
        row.rows[0]["account_id"]
        for row in read.payload.projections
        if row.table_name == "paper_order_window"
    )
    for account in accounts:
        key = account.configuration.binding.account_id
        assert all(
            item.scope.account_id == key and item.source_generation_id == read.generation_id
            for item in facts[key]
        )
        assert any(item.metric_id == "order_rejection_ratio" for item in facts[key]) == (
            key == retained_account
        )
        cash = next(item for item in facts[key] if item.metric_id == "portfolio_exposure")
        assert cash.value == next(
            row.portfolio_weight for row in account.exposure.exposure.rows if row.kind == "cash"
        )
    root = tmp_path / "serving"
    _publish_private_health(root, read, account_ids=keys)
    data = _private_get(root, read.published_at, accounts[0].configuration.binding.owner_id)
    risk = next(layer for layer in data["layers"] if layer["key"] == "risk")
    scopes = {row["scope_key"] for row in risk["exposure"]}
    assert len(scopes) == 2
    assert {row["scope_label"] for row in risk["exposure"]} == {"组合 1", "组合 2"}
    for index, account in enumerate(accounts):
        rows = [row for row in risk["exposure"] if row["scope_label"] == f"组合 {index + 1}"]
        assert [
            (row["portfolio_weight"], row["benchmark_weight"], row["deviation"]) for row in rows
        ] == [
            (str(row.portfolio_weight), str(row.benchmark_weight), str(row.deviation))
            for row in account.exposure.exposure.rows
        ]
        assert {row["source_generation_id"] for row in rows} == {read.generation_id}
    assert len(next(layer for layer in data["layers"] if layer["key"] == "orders")["metrics"]) == 1


def test_full_multi_account_graph_rejects_unknown_account_and_foreign_retained_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.runtime_builder_paper import paper_health_metrics_for_publication
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_serving_snapshot import SourceReadResult

    accounts, read = _multi_exposure_read(tmp_path / "paper", monkeypatch)
    with pytest.raises(ValueError, match="absent"):
        paper_health_metrics_for_publication(
            read, account_id="unpublished", fallback_configuration_identity="f" * 64
        )
    projections = tuple(
        type(row).model_validate(
            row.model_dump(mode="python")
            | {"rows": (dict(row.rows[0]) | {"account_id": "foreign-account"},)}
        )
        if row.table_name == "paper_order_window"
        else row
        for row in read.payload.projections
    )
    values = read.model_dump(mode="python", exclude={"generation_id"}) | {
        "payload": type(read.payload)(
            paper_accounts=read.payload.paper_accounts, projections=projections
        )
    }
    with pytest.raises(ValueError, match="account|retained"):
        paper_health_metrics_for_publication(
            SourceReadResult(generation_id=canonical_sha256(values), **values),
            account_id=accounts[0].configuration.binding.account_id,
            fallback_configuration_identity="f" * 64,
        )


def test_single_account_paper_helper_preserves_exact_frozen_original_results(
    tmp_path: Path,
) -> None:
    import ast
    import hashlib

    import rquant.runtime_builder_paper as current
    from rquant.runtime_contracts import canonical_sha256

    path = (
        Path(__file__).resolve().parents[1]
        / "fixtures/react_platform_health/runtime_builder_paper.py"
    )
    raw = path.read_bytes()
    assert (
        hashlib.sha256(raw).hexdigest()
        == "eaf0ef79c6ea1791f3ddb3de3110468aa4646477af0ce368164b29af883bd156"
    )
    function = next(
        node
        for node in ast.parse(raw).body
        if isinstance(node, ast.FunctionDef) and node.name == "paper_health_metrics_for_publication"
    )
    namespace = vars(current).copy()
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    account, read = _exposure_read(tmp_path / "paper")
    options = dict(
        account_id=account.configuration.binding.account_id,
        fallback_configuration_identity="f" * 64,
    )
    before = namespace["paper_health_metrics_for_publication"](read, **options)
    after = current.paper_health_metrics_for_publication(read, **options)
    assert before == after and canonical_sha256(before) == canonical_sha256(after)


def test_native_fixture_uses_original_start_witness_owner_reader_and_business_observations(
    tmp_path: Path,
) -> None:
    import importlib.util

    from tests.unit.test_runtime_serving_snapshot import NOW

    path = (
        Path(__file__).resolve().parents[1]
        / "fixtures/react_platform_health/native_health_fixture.py"
    )
    spec = importlib.util.spec_from_file_location("native_health_fixture", path)
    assert spec is not None and spec.loader is not None
    factory = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(factory)
    root = tmp_path / "serving"
    factory.publish_native_health_fixture(root, tmp_path / "native-domain")
    data = _private_get(root, NOW, "alice")
    risk = next(layer for layer in data["layers"] if layer["key"] == "risk")
    assert len({item["scope_key"] for item in risk["exposure"]}) == 2
    assert any(
        item["detail"]["available"] and item["detail"]["observations"] for item in data["services"]
    )
    strategy = next(layer for layer in data["layers"] if layer["key"] == "strategy")
    count = next(item for item in strategy["metrics"] if item["name"] == "已处理候选")
    assert count["available"] and count["value"] == 1
    assert all(item["detail"]["started_at"] is not None for item in data["services"])
