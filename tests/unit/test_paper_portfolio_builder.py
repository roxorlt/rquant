"""Exact original manifest, route cursor and applied control share one role."""

from datetime import timedelta
from pathlib import Path
from decimal import Decimal
from contextlib import closing
import sqlite3

import pytest

from rquant.paper_broker import PaperBrokerStore
from rquant.runtime_builder_paper import paper_broker_builder
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_portfolio_core import config_data
from tests.unit.test_runtime_builder_paper import NOW, _manifest, _publish_signal
from tests.unit.test_paper_signal_worker import _quote


def fixture(tmp_path: Path, *, serving_authority_root: Path | None = None):
    from rquant.paper_operator import PaperOperatorControlStore
    from rquant.paper_operator_commands import SetPaperAccountPaused
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntime, PaperPortfolioRuntimeCatalog
    from rquant.paper_portfolio_source import PaperPortfolioMaterialStore, PaperPortfolioMarketSnapshot, PaperPortfolioRawFact
    from rquant.paper_portfolio_state import PaperPortfolioStateStore
    from uuid import uuid4

    old = _manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER)
    data = old.model_dump(mode="python")
    data["service_id"] = "paper.main.v1"
    data["settings"]["initial_cash"] = "1000"
    if serving_authority_root is not None:
        data["settings"]["serving_authority_root"] = str(serving_authority_root)
    manifest = RuntimeServiceManifest.model_validate(data)
    data = config_data()
    data["binding"]["manifest_fingerprint"] = manifest.manifest_fingerprint
    data["configured_at"] = NOW - timedelta(days=1)
    configuration = PaperPortfolioConfiguration.model_validate(data)
    state = PaperPortfolioStateStore(tmp_path / "portfolio.sqlite", configuration=configuration)
    operator = PaperOperatorControlStore(state, root=tmp_path / "control" / "operator", clock=lambda: NOW)
    source = PaperPortfolioMaterialStore(state)
    cutoff = NOW + timedelta(minutes=1)
    source.publish(PaperPortfolioMarketSnapshot(binding=configuration.binding, configuration_fingerprint=configuration.fingerprint,
                                                dataset_snapshot_id="c" * 64, feature_snapshot_id="d" * 64,
                                                observed_at=cutoff, available_at=cutoff,
                                                facts=(PaperPortfolioRawFact(ts_code="600000.SH", rank_score="1", industry_l1="银行",
                                                                            valuation_price="1", trading_status="normal", observed_at=cutoff,
                                                                            available_at=cutoff, source_snapshot_id="f" * 64),)))
    request = SetPaperAccountPaused(command_id=str(uuid4()), requested_at=NOW, generation_id="generation-a",
                                    account_id=configuration.binding.account_id, configuration_fingerprint=configuration.fingerprint,
                                    expected_sequence=0, expected_paused=True, paused=False)
    control = operator.commit_confirmed_control(request, authenticated_actor_id="alice", original_command_id=request.command_id)
    operator.publish(control)
    runtime = PaperPortfolioRuntime(state, operator=operator, materials=source, producer_commit=manifest.producer_commit)
    return manifest, operator, PaperPortfolioRuntimeCatalog((runtime,)), control


def test_original_route_role_applies_control_and_runs_target_quantity(tmp_path: Path) -> None:
    manifest, operator, catalog, control = fixture(tmp_path)
    _publish_signal(tmp_path)
    now = [NOW]
    step = paper_broker_builder(clock=lambda: now[0], quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff),
                                trade_date_resolver=lambda cutoff: cutoff.date(), portfolio_catalog=catalog)(manifest)
    assert operator.current().status == "waiting"
    first = step()
    assert first.output_sequence == 1 and first.processed_count == 0
    assert operator.current().status == "applied" and not operator.current().paused
    assert first.source_generations["paper_operator_control"] == control.fingerprint
    assert first.observations["paper_operator_applied_sequence"] == 1
    now[0] += timedelta(minutes=1)
    assert step().processed_count == 1
    with PaperBrokerStore.open_readonly(Path(manifest.settings["broker_path"]), account_id="paper-main",
                                        initial_cash=Decimal("1000"), cost_policy=paper_cost_policy()) as reader:
        result = reader.reconcile()
        assert result.order_count == 1 and result.open_lot_quantity == 800 and result.cash == 195


def test_changed_manifest_or_untyped_catalog_rejects_before_broker_creation(tmp_path: Path) -> None:
    manifest, _, catalog, _ = fixture(tmp_path)
    data = manifest.model_dump(mode="python")
    data["settings"]["broker_path"] = str(tmp_path / "foreign.sqlite")
    altered = RuntimeServiceManifest.model_validate(data)
    with pytest.raises(ValueError):
        paper_broker_builder(clock=lambda: NOW, quote_resolver=lambda signal, cutoff: _quote(price="1"),
                             trade_date_resolver=lambda cutoff: cutoff.date(), portfolio_catalog=catalog)(altered)
    assert not Path(altered.settings["broker_path"]).exists()
    with pytest.raises(TypeError):
        paper_broker_builder(clock=lambda: NOW, portfolio_catalog={})


def test_original_builtin_registry_dispatches_the_exact_configured_role(tmp_path: Path) -> None:
    from rquant.runtime_service_builtin import build_builtin_registry

    manifest, operator, catalog, control = fixture(tmp_path)
    _publish_signal(tmp_path)
    registry = build_builtin_registry(clock=lambda: NOW, paper_quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff),
                                      trade_date_resolver=lambda cutoff: cutoff.date(), paper_portfolio_catalog=catalog)
    result = registry.build(manifest)()
    assert result.source_generations["paper_operator_control"] == control.fingerprint
    assert result.observations["paper_operator_applied_sequence"] == 1
    assert operator.current().status == "applied" and not operator.current().paused


def test_original_host_passes_optional_catalog_and_preserves_default_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.runtime_service_main as host
    import rquant.runtime_service_builtin as builtin

    _, _, catalog, _ = fixture(tmp_path)
    captured = []
    marker = object()
    def factory(**kwargs):
        captured.append(kwargs)
        return marker
    monkeypatch.setattr(builtin, "build_builtin_registry", factory)
    assert host.build_builtin_registry(runtime_capabilities={}, paper_portfolio_catalog=catalog) is marker
    assert captured[-1]["paper_portfolio_catalog"] is catalog
    assert host.build_builtin_registry(runtime_capabilities={}) is marker
    assert captured[-1].get("paper_portfolio_catalog") is None


def test_original_role_publisher_adds_complete_owned_graph_to_unchanged_old_window(tmp_path: Path) -> None:
    from rquant.runtime_serving_authority import ServingSourceAuthorityReader
    from rquant.paper_portfolio_projection import validate_paper_portfolio_projections

    root = tmp_path/"authority"
    manifest, _, catalog, control = fixture(tmp_path, serving_authority_root=root)
    _publish_signal(tmp_path)
    now = [NOW]
    step = paper_broker_builder(clock=lambda: now[0], quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff),
                               trade_date_resolver=lambda cutoff: cutoff.date(), portfolio_catalog=catalog)(manifest)
    with closing(sqlite3.connect(Path(manifest.settings["broker_path"]))) as pinned_writer:
        pinned_writer.execute("SELECT count(*) FROM paper_order").fetchone()
        step()
        now[0] += timedelta(minutes=1)
        try:
            completed = step()
        except Exception:
            from rquant.paper_signal_worker import PaperSignalQueueStore
            from rquant.runtime_builder_paper import PaperBrokerSettings
            from tests.unit.test_runtime_builder_paper import _signal

            settings = PaperBrokerSettings.model_validate(dict(manifest.settings))
            record = PaperSignalQueueStore(settings.queue_path, policy=settings.signal_policy(manifest.producer_commit)).record(str(_signal().signal_id))
            print("ORIGINAL_QUEUE_ON_PUBLISH_FAILURE=", record.status, record.last_error)
            raise
        read = ServingSourceAuthorityReader(root=root, expected_producer_commit=manifest.producer_commit,
                                           expected_dataset_id="paper_accounts", expected_payload_kind="paper_accounts")(now[0])
        tables = {item.table_name: item for item in read.payload.projections}
        assert set(tables) == {"paper_order_window", "paper_order_history", "paper_fill_history",
                               "paper_portfolio_state", "paper_portfolio_account", "paper_portfolio_material"}
        published = validate_paper_portfolio_projections(tables)
        assert published.available_at == now[0]
        value = published.accounts[0]
        assert value.frame.account.cash == 195 and value.frame.account.holdings[0].quantity == 800
        assert value.operator.status == "applied" and value.operator.control_fingerprint == control.fingerprint
        assert tables["paper_order_window"].rows[0]["total_orders"] == 1
        assert len(tables["paper_order_history"].rows) == 1 and len(value.frame.history) == 1
        assert completed.source_generations["paper_accounts"] == read.generation_id
