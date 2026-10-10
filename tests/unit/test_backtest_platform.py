from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
from datetime import UTC, date, datetime, timedelta
from datetime import timedelta as duration
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from uuid import uuid4

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backtest import run_portfolio_backtest
from rquant.portfolio.drawdown import DrawdownRule
from rquant.runtime_contracts import canonical_sha256
from tests.unit.test_portfolio_backtest import _CODES, _request


def platform() -> ModuleType:
    name = "rquant.portfolio_backtest_models"
    assert importlib.util.find_spec(name) is not None, "typed portfolio product contract is missing"
    return importlib.import_module(name)


def config() -> Any:
    return platform().PortfolioBacktestConfig.from_request(_request((_CODES[0], _CODES[1])))


def frozen() -> Any:
    model = platform()
    request = _request((_CODES[0], _CODES[1]))
    return model.FrozenPortfolioInput(
        config=config(),
        request=request,
        sources=model.PortfolioSourceManifest(
            market_hash="1" * 64,
            reference_hash="2" * 64,
            opening_hash="3" * 64,
            source_mode="captured_with_retrospective_prices",
        ),
        benchmark_closes=(
            (date(2026, 8, 7), 100.0),
            (date(2026, 8, 10), 101.0),
            (date(2026, 8, 11), 102.0),
        ),
    )


def test_pb03_frozen_input_binds_request_config_and_source_identity() -> None:
    value = frozen()
    assert value.input_hash == canonical_sha256(
        value.model_dump(mode="python", exclude={"input_hash"})
    )
    bad = value.model_dump(mode="python")
    bad["input_hash"] = "0" * 64
    with pytest.raises(ValidationError, match="input hash"):
        platform().FrozenPortfolioInput.model_validate(bad)
    bad = value.model_dump(mode="python")
    bad["config"] = config().model_copy(update={"initial_cash": Decimal("99")})
    bad["input_hash"] = None
    with pytest.raises(ValidationError, match="config"):
        platform().FrozenPortfolioInput.model_validate(bad)


@pytest.mark.parametrize(
    "extra", [{"owner": "admin"}, {"path": "/secret"}, {"expression": "x"}, {"code": "pass"}]
)
def test_pb01_config_has_no_executable_or_identity_fields(extra: dict) -> None:
    bad = config().model_dump(mode="python") | extra
    with pytest.raises(ValidationError, match="Extra inputs"):
        platform().PortfolioBacktestConfig.model_validate(bad)


@pytest.mark.parametrize("money", ["NaN", "Infinity", "-1", "0", "1.001", "1000000000001"])
def test_pb07_money_is_finite_bounded_and_exact_to_cent(money: str) -> None:
    bad = config().model_dump(mode="python") | {"initial_cash": Decimal(money)}
    with pytest.raises(ValidationError):
        platform().PortfolioBacktestConfig.model_validate(bad)


def test_pb07_date_span_limit() -> None:
    bad = config().model_dump(mode="python")
    bad["end_date"] = bad["start_date"] + timedelta(days=5 * 366)
    with pytest.raises(ValidationError, match="date"):
        platform().PortfolioBacktestConfig.model_validate(bad)


def test_pb06_missing_baseline_and_benchmark_date_are_not_filled() -> None:
    value = frozen()
    for rows in (
        value.benchmark_closes[1:],
        value.benchmark_closes[:2],
        (*value.benchmark_closes, value.benchmark_closes[-1]),
    ):
        bad = value.model_dump(mode="python") | {"benchmark_closes": rows, "input_hash": None}
        with pytest.raises(ValidationError, match="benchmark"):
            platform().FrozenPortfolioInput.model_validate(bad)


def test_pb12_legacy_request_bytes_stay_exact_when_drawdown_disabled() -> None:
    request = _request((_CODES[0], _CODES[1]))
    assert "drawdown_rule" not in request.model_dump(mode="python")
    assert request.request_id == "b4ccf3fe6d0c3a4ff4f1b64e4c2bfcfc5f7ccb95a0e7e9062849627525cb1094"


def test_pb12_legacy_result_bytes_remain_exact(tmp_path: Path) -> None:
    result = run_portfolio_backtest(_request((_CODES[0], _CODES[1])), research_root=tmp_path)
    assert result.content_hash == "a35a96480046361620fa278cc9d0d4b703ed664237bf9e2a81ecae65567b9100"
    assert (
        hashlib.sha256(result.model_dump_json().encode()).hexdigest()
        == "9ba78cfeff6eddada490471c89655d6cf8c0d09933085482fce748da71a178ea"
    )


def test_pb04_drawdown_is_bound_and_does_not_use_today_close(tmp_path: Path) -> None:
    old = _request((_CODES[0], _CODES[1], _CODES[1]))
    data = old.model_dump(mode="python")
    data["drawdown_rule"] = DrawdownRule(
        trigger_drawdown=Decimal("0.001"), action="block_new_positions"
    )
    guarded = type(old).model_validate(data)
    assert guarded.request_id != old.request_id
    normal = run_portfolio_backtest(old, research_root=tmp_path)
    result = run_portfolio_backtest(guarded, research_root=tmp_path)
    assert result.days[0].account == normal.days[0].account
    assert [(o.intent.side, o.intent.quantity) for o in result.days[0].orders] == [
        (o.intent.side, o.intent.quantity) for o in normal.days[0].orders
    ]
    assert not any(
        order.intent.side.value == "BUY" and order.intent.ts_code == _CODES[1]
        for order in result.days[1].orders
    )
    assert any(order.intent.side.value == "SELL" for order in result.days[1].orders)
    assert list(tmp_path.iterdir()) == []


def _drawdown_request(action: str = "block_new_positions") -> Any:
    from tests.paper_cost_fixtures import paper_execution_cost_spec

    original = _request((_CODES[0], _CODES[0], _CODES[1]))
    data = original.model_dump(mode="python")
    data["execution_cost_spec"] = paper_execution_cost_spec(
        commission_bps=Decimal("0"), minimum_commission=Decimal("0"), stamp_duty_bps=Decimal("0")
    )
    data["days"][0]["instruments"][0]["close_price"] = Decimal("7")
    data["drawdown_rule"] = DrawdownRule(
        trigger_drawdown=Decimal("0.1"),
        action=action,
        total_risk_weight_cap=Decimal("0.1") if action == "cap_total_risk_weight" else None,
    )
    return type(original).model_validate(data)


def test_pb04_risk_exposes_prior_day_exact_threshold_and_release(tmp_path: Path) -> None:
    request = _drawdown_request()
    result = run_portfolio_backtest(request, research_root=tmp_path)
    risks = [day.risk for day in result.days]
    assert [risk.state.active for risk in risks] == [False, True, False]
    assert [risk.state.last_nav for risk in risks] == [
        Decimal("3000"),
        Decimal("2700"),
        Decimal("3000"),
    ]
    assert risks[1].drawdown == Decimal("0.1")
    assert risks[1].allow_new_positions is False and risks[2].allow_new_positions is True
    assert all(
        risk.state.last_at.date() < day.trade_date
        for day, risk in zip(result.days, risks, strict=True)
    )
    assert any(
        order.intent.side.value == "BUY" and order.intent.ts_code == _CODES[1]
        for order in result.days[2].orders
    )
    altered = request.model_dump(mode="python")
    altered["days"][1]["instruments"][0]["close_price"] = Decimal("2")
    changed = run_portfolio_backtest(type(request).model_validate(altered), research_root=tmp_path)
    assert changed.days[1].risk == result.days[1].risk
    assert changed.days[1].decisions == result.days[1].decisions
    assert list(tmp_path.iterdir()) == []


def test_pb04_total_risk_cap_rebalances_between_scheduled_dates(tmp_path: Path) -> None:
    from rquant.backtest import RebalanceRule

    request = _drawdown_request("cap_total_risk_weight")
    request = type(request).model_validate(
        request.model_dump(mode="python") | {"rebalance_rule": RebalanceRule(kind="monthly")}
    )
    result = run_portfolio_backtest(request, research_root=tmp_path)
    assert result.days[1].risk.max_total_risk_weight == Decimal("0.1")
    assert result.days[1].rebalanced is True
    assert result.days[1].account.holdings == ()
    assert any(order.intent.side.value == "SELL" for order in result.days[1].orders)
    assert not result.days[2].rebalanced


def test_pb03_bundle_benchmark_cannot_swap_code_or_frozen_close(tmp_path: Path) -> None:
    product = importlib.import_module("rquant.portfolio_backtest_product")
    bundle = product.execute_portfolio_input(frozen(), research_root=tmp_path)
    alternatives = (
        bundle.frozen.model_dump(mode="python")
        | {
            "config": bundle.frozen.config.model_copy(update={"benchmark_code": "000905.SH"}),
            "input_hash": None,
        },
        bundle.frozen.model_dump(mode="python")
        | {
            "benchmark_closes": ((date(2026, 8, 7), 99.0), *bundle.frozen.benchmark_closes[1:]),
            "input_hash": None,
        },
    )
    for alternative in alternatives:
        changed = platform().FrozenPortfolioInput.model_validate(alternative)
        benchmark = product._benchmark(changed, bundle.result)
        bad = bundle.model_dump(mode="python") | {"benchmark": benchmark, "bundle_hash": None}
        with pytest.raises(ValidationError, match="benchmark|基准"):
            platform().PortfolioBundle.model_validate(bad)


def test_pb10_full_bundle_seals_html_and_lossless_result(tmp_path: Path) -> None:
    name = "rquant.portfolio_backtest_product"
    assert importlib.util.find_spec(name) is not None, "portfolio product executor is missing"
    product = importlib.import_module(name)
    value = frozen()
    result = product.execute_portfolio_input(value, research_root=tmp_path)
    payload = result.json_bytes()
    restored = platform().PortfolioBundle.model_validate_json(payload)
    assert restored == result
    assert restored.result.request_id == value.request.request_id
    assert restored.html_sha256 == hashlib.sha256(restored.html.encode()).hexdigest()
    assert restored.html.lower().startswith("<!doctype html>")
    assert restored.benchmark.backtest_content_hash == restored.result.content_hash
    assert set(product.bundle_tables(restored)) == {
        "portfolio_bundle",
        "portfolio_nav",
        "portfolio_trades",
        "portfolio_holdings",
        "portfolio_daily",
        "portfolio_monthly",
        "portfolio_log",
    }
    assert all(day.account.cash + day.market_value == day.account.nav for day in result.result.days)
    assert list(tmp_path.iterdir()) == []


def test_pb03_bundle_rejects_changed_result_or_html(tmp_path: Path) -> None:
    name = "rquant.portfolio_backtest_product"
    assert importlib.util.find_spec(name) is not None, "portfolio product executor is missing"
    product = importlib.import_module(name)
    bundle = product.execute_portfolio_input(frozen(), research_root=tmp_path)
    for change in ({"html": bundle.html + "x"}, {"request_id": "0" * 64}):
        payload = json.loads(bundle.json_bytes()) | change
        with pytest.raises(ValidationError):
            platform().PortfolioBundle.model_validate(payload)


def adapter_module() -> ModuleType:
    name = "rquant.portfolio_backtest_adapter"
    assert importlib.util.find_spec(name) is not None, "portfolio Lab execution adapter is missing"
    return importlib.import_module(name)


def adapter_spec(value: Any) -> Any:
    from rquant.research_run_spec import (
        ParameterKind,
        ResearchJobType,
        ResearchParameter,
        ResearchRunParameters,
        ResearchRunSpec,
        ResourceClass,
    )
    from rquant.strategy_job_adapters import build_adapter_execution_contract

    parameters = adapter_module().PortfolioBacktestParameters.from_frozen(value)
    return ResearchRunSpec(
        parameters=ResearchRunParameters(
            strategy_name="portfolio_backtest",
            start_date=value.config.start_date,
            end_date=value.config.end_date,
            arguments=tuple(
                ResearchParameter(
                    name=name,
                    kind=ParameterKind.INTEGER if type(item) is int else ParameterKind.TEXT,
                    value=item,
                )
                for name, item in parameters.model_dump().items()
            ),
        ),
        job_type=ResearchJobType.STRATEGY_REPLAY,
        code_sha=value.request.producer_commit,
        execution_costs=value.request.execution_cost_spec,
        feature_contract=build_adapter_execution_contract(
            "portfolio-backtest", "1", value.request.producer_commit
        ),
        dataset_snapshot=None,
        research_status="exploratory",
        random_seed=1,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 12, 1, tzinfo=UTC),
    )


def portfolio_claim(registry: Any, spec: Any) -> Any:
    from rquant.lab_shard_protocol import LabShardClaim

    now = datetime(2026, 10, 5, tzinfo=UTC)
    return LabShardClaim(
        job_id=uuid4(),
        spec_hash=spec.spec_hash,
        definition=registry.plan(spec)[0],
        worker_id="portfolio-test",
        claim_token=uuid4(),
        claim_generation=1,
        scheduler_fencing_token=1,
        claimed_at=now,
        lease_expires_at=now + duration(minutes=5),
    )


def test_pb03_adapter_is_one_full_range_shard_and_binds_work_to_source(tmp_path: Path) -> None:
    from rquant.strategy_job_adapters import StrategyJobAdapterRegistry

    module = adapter_module()
    value = frozen()
    registry = StrategyJobAdapterRegistry((module.PortfolioBacktestAdapter(),))
    spec = adapter_spec(value)
    definitions = registry.plan(spec)
    assert len(definitions) == 1
    assert definitions[0].work_plan.work_units == 6
    with duckdb.connect() as connection:
        module.write_portfolio_input_table(connection, value)
        before = connection.execute("SELECT * FROM portfolio_backtest_input").fetchone()
        claim = portfolio_claim(registry, spec)
        validated = registry.validate_claim(claim)
        first = registry.for_spec(spec).execute_shard(validated, SimpleNamespace(_conn=connection))
        second = registry.for_spec(spec).execute_shard(validated, SimpleNamespace(_conn=connection))
        assert set(table.name for table in first.tables) == set(platform().PORTFOLIO_TABLE_NAMES)
        assert first.tables[0].frame.iloc[0, 0] == second.tables[0].frame.iloc[0, 0]
        assert connection.execute("SELECT * FROM portfolio_backtest_input").fetchone() == before
        bundle = platform().PortfolioBundle.model_validate_json(first.tables[0].frame.iloc[0, 0])
        assert bundle.frozen.input_hash == value.input_hash


@pytest.mark.parametrize(
    "field,wrong",
    [
        ("input_hash", "0" * 64),
        ("config_hash", "0" * 64),
        ("request_id", "0" * 64),
        ("work_units", 1),
    ],
)
def test_pb03_adapter_rejects_identity_and_work_drift(field: str, wrong: object) -> None:
    from rquant.research_run_spec import ResearchRunSpec
    from rquant.strategy_job_adapters import StrategyJobAdapterRegistry

    module = adapter_module()
    value = frozen()
    spec = adapter_spec(value)
    data = spec.model_dump(mode="python")
    data["parameters"]["arguments"] = tuple(
        {**p, "value": wrong} if p["name"] == field else p for p in data["parameters"]["arguments"]
    )
    drifted = ResearchRunSpec.model_validate(data)
    registry = StrategyJobAdapterRegistry((module.PortfolioBacktestAdapter(),))
    with duckdb.connect() as connection:
        module.write_portfolio_input_table(connection, value)
        validated = registry.validate_claim(portfolio_claim(registry, drifted))
        with pytest.raises(ValueError, match="frozen|source|identity|work"):
            registry.for_spec(drifted).execute_shard(validated, SimpleNamespace(_conn=connection))


def test_pb03_snapshot_source_rejects_additional_rows_or_columns() -> None:
    module = adapter_module()
    value = frozen()
    with duckdb.connect() as connection:
        module.write_portfolio_input_table(connection, value)
        assert module.read_portfolio_input_table(connection).input_hash == value.input_hash
        connection.execute("ALTER TABLE portfolio_backtest_input ADD COLUMN private_notes VARCHAR")
        with pytest.raises(ValueError, match="schema"):
            module.read_portfolio_input_table(connection)
    with duckdb.connect() as connection:
        module.write_portfolio_input_table(connection, value)
        connection.execute(
            "INSERT INTO portfolio_backtest_input VALUES (?, ?)",
            ["0" * 64, value.model_dump_json()],
        )
        with pytest.raises(ValueError, match="one"):
            module.read_portfolio_input_table(connection)


def artifact_module() -> ModuleType:
    name = "rquant.portfolio_backtest_artifact"
    assert importlib.util.find_spec(name) is not None, (
        "portfolio sealed reader and report export are missing"
    )
    return importlib.import_module(name)


def sealed_portfolio(tmp_path: Path) -> tuple[Any, Any, Any, Any]:
    from rquant.lab_artifacts import LabArtifactIndexEvidence, LabJobArtifactStore
    from rquant.lab_jobs import (
        ControlIntent,
        JobStatus,
        LabArtifactPreviewAuthority,
        LabJobRecord,
        LabResultState,
    )
    from rquant.portfolio_backtest_product import bundle_tables, execute_portfolio_input

    bundle = execute_portfolio_input(frozen(), research_root=tmp_path)
    spec = adapter_spec(bundle.frozen)
    job_id, now = uuid4(), datetime(2026, 10, 5, tzinfo=UTC)
    artifacts = LabJobArtifactStore(tmp_path / "sealed-artifacts")
    candidate = artifacts.prepare_candidate(
        job_id=job_id,
        spec=spec,
        plan_hash="1" * 64,
        adapter_id="portfolio-backtest",
        adapter_version="1",
        result_contract_version="p14b1-v1",
        metrics={"status": "complete"},
        report_markdown="# 组合回测\n",
        tables=bundle_tables(bundle),
    )
    sealed = artifacts.seal_candidate(candidate)
    evidence = LabArtifactIndexEvidence(
        job_id=job_id,
        sealed_path=sealed.path,
        manifest_hash=sealed.manifest_hash,
        complete_result_hash=sealed.manifest.complete_result_hash,
        bundle_device=sealed.device,
        bundle_inode=sealed.inode,
        file_identities=sealed.file_identities,
        indexed_at=now,
    )
    job = LabJobRecord(
        job_id=job_id,
        spec=spec,
        spec_hash=spec.spec_hash,
        job_type=spec.job_type,
        resource_class=spec.resource_class,
        deadline=spec.deadline,
        status=JobStatus.SUCCEEDED,
        control_intent=ControlIntent.NONE,
        version=1,
        attempt_count=1,
        max_attempts=2,
        recoverable=True,
        requires_complete_result=True,
        result_state=LabResultState.SEALED,
        created_at=now,
        updated_at=now,
    )
    authority = LabArtifactPreviewAuthority(job=job, evidence=evidence)

    class LedgerFixture:
        def get_artifact_preview_authority(self, selected: Any) -> Any:
            return authority if selected == job_id else None

    return LedgerFixture(), artifacts, bundle, authority


def test_pb09_sealed_portfolio_reader_checks_full_artifact_and_result_identity(
    tmp_path: Path,
) -> None:
    module = artifact_module()
    ledger, artifacts, bundle, authority = sealed_portfolio(tmp_path)
    try:
        reader = module.PortfolioResultReader(reader=ledger, artifact_root=artifacts.root)
        read = reader.read(
            authority.job.job_id, expected_result_hash=authority.evidence.complete_result_hash
        )
        assert read.bundle == bundle
        assert read.html_bytes() == bundle.html.encode()
        with pytest.raises(ValueError, match="result"):
            reader.read(authority.job.job_id, expected_result_hash="0" * 64)
        with pytest.raises(Exception, match="sealed|unavailable"):
            reader.read(uuid4())
    finally:
        artifacts.close()


def test_pb10_new_zip_preserves_old_entries_and_exact_report_bytes(tmp_path: Path) -> None:
    import zipfile

    from rquant.lab_artifact_export import LabJobZipExportFacade

    module = artifact_module()
    ledger, artifacts, bundle, authority = sealed_portfolio(tmp_path)
    original_root, new_root = tmp_path / "old-exports", tmp_path / "portfolio-exports"
    original_root.mkdir(mode=0o700)
    new_root.mkdir(mode=0o700)
    try:
        original = LabJobZipExportFacade(
            reader=ledger, artifact_store=artifacts, export_root=original_root
        )
        reader = module.PortfolioResultReader(reader=ledger, artifact_root=artifacts.root)
        exports = module.PortfolioZipExportFacade(
            reader=ledger,
            artifact_store=artifacts,
            result_reader=reader,
            original_exports=original,
            export_root=new_root,
        )
        receipt = exports.export_portfolio(
            authority.job.job_id, expected_result_hash=authority.evidence.complete_result_hash
        )
        assert receipt.html_sha256 == bundle.html_sha256
        assert receipt.byte_size <= platform().MAX_ZIP_BYTES
        original_file = next(original_root.rglob("result.zip"))
        original_bytes = original_file.read_bytes()
        with zipfile.ZipFile(original_file) as old, zipfile.ZipFile(receipt.path) as new:
            assert set(new.namelist()) == set(old.namelist()) | {"report.html"}
            assert all(new.read(name) == old.read(name) for name in old.namelist())
            assert new.read("report.html") == reader.read(authority.job.job_id).html_bytes()
        assert original_file.read_bytes() == original_bytes
        assert exports.read_bytes(receipt) == receipt.path.read_bytes()
    finally:
        artifacts.close()


def test_pb10_zip_budget_failure_cleans_only_own_temporary_file(tmp_path: Path) -> None:
    from rquant.lab_artifact_export import LabJobZipExportFacade

    module = artifact_module()
    ledger, artifacts, bundle, authority = sealed_portfolio(tmp_path)
    original_root, new_root = tmp_path / "old-exports", tmp_path / "portfolio-exports"
    original_root.mkdir(mode=0o700)
    new_root.mkdir(mode=0o700)
    try:
        exports = module.PortfolioZipExportFacade(
            reader=ledger,
            artifact_store=artifacts,
            result_reader=module.PortfolioResultReader(reader=ledger, artifact_root=artifacts.root),
            original_exports=LabJobZipExportFacade(
                reader=ledger, artifact_store=artifacts, export_root=original_root
            ),
            export_root=new_root,
            max_zip_bytes=128,
        )
        with pytest.raises(ValueError, match="budget"):
            exports.export_portfolio(
                authority.job.job_id, expected_result_hash=authority.evidence.complete_result_hash
            )
        assert not list(new_root.rglob("*.tmp"))
        assert not list(new_root.rglob("result.zip"))
        assert list(original_root.rglob("result.zip"))
        assert bundle.html_sha256 == hashlib.sha256(bundle.html.encode()).hexdigest()
    finally:
        artifacts.close()


def test_pb02_portfolio_export_recovers_original_request_without_second_zip(tmp_path: Path) -> None:
    from rquant.lab_artifact_export import LabJobZipExportFacade

    module = artifact_module()
    ledger, artifacts, bundle, authority = sealed_portfolio(tmp_path)
    old_root, new_root = tmp_path / "old-exports", tmp_path / "portfolio-exports"
    old_root.mkdir(mode=0o700)
    new_root.mkdir(mode=0o700)
    request_id = uuid4()
    try:
        exports = module.PortfolioZipExportFacade(
            reader=ledger,
            artifact_store=artifacts,
            result_reader=module.PortfolioResultReader(reader=ledger, artifact_root=artifacts.root),
            original_exports=LabJobZipExportFacade(
                reader=ledger, artifact_store=artifacts, export_root=old_root
            ),
            export_root=new_root,
        )
        first = exports.export_portfolio(
            authority.job.job_id,
            request_id=request_id,
            expected_result_hash=authority.evidence.complete_result_hash,
        )
        recovered = exports.recover_portfolio(
            authority.job.job_id,
            request_id=request_id,
            expected_result_hash=authority.evidence.complete_result_hash,
        )
        assert recovered == first
        assert (
            exports.export_portfolio(
                authority.job.job_id,
                request_id=request_id,
                expected_result_hash=authority.evidence.complete_result_hash,
            )
            == first
        )
        assert len(list(old_root.rglob("result.zip"))) == 1
        assert len(list(new_root.rglob("result.zip"))) == 1
        assert exports.read_bytes(recovered) == first.path.read_bytes()
        with pytest.raises(ValueError):
            exports.recover_portfolio(
                authority.job.job_id, request_id=request_id, expected_result_hash="0" * 64
            )
    finally:
        artifacts.close()


def test_pb08_zip_restart_rebuilds_only_owned_interrupted_request(tmp_path: Path) -> None:
    import errno

    from rquant.lab_artifact_export import LabJobZipExportFacade

    module = artifact_module()
    ledger, artifacts, bundle, authority = sealed_portfolio(tmp_path)
    old_root, new_root = tmp_path / "old-exports", tmp_path / "portfolio-exports"
    old_root.mkdir(mode=0o700)
    new_root.mkdir(mode=0o700)
    request_id = uuid4()
    slot = new_root / authority.job.job_id.hex / request_id.hex
    slot.parent.mkdir(mode=0o700)
    slot.mkdir(mode=0o700)
    temporary = slot / "result.tmp"
    temporary.write_bytes(b"partial zip from an interrupted writer")
    temporary.chmod(0o600)
    unrelated = slot / "unrelated-note"
    unrelated.write_bytes(b"must survive")
    try:
        exports = module.PortfolioZipExportFacade(
            reader=ledger,
            artifact_store=artifacts,
            result_reader=module.PortfolioResultReader(reader=ledger, artifact_root=artifacts.root),
            original_exports=LabJobZipExportFacade(
                reader=ledger, artifact_store=artifacts, export_root=old_root
            ),
            export_root=new_root,
        )
        receipt = exports.export_portfolio(
            authority.job.job_id,
            request_id=request_id,
            expected_result_hash=authority.evidence.complete_result_hash,
        )
        assert exports.read_bytes(receipt) == receipt.path.read_bytes()
        assert not temporary.exists() and unrelated.read_bytes() == b"must survive"
        other_id = uuid4()
        other = slot.parent / other_id.hex
        other.mkdir(mode=0o700)
        (other / "result.tmp").symlink_to(unrelated)
        with pytest.raises(OSError) as blocked:
            exports.export_portfolio(
                authority.job.job_id,
                request_id=other_id,
                expected_result_hash=authority.evidence.complete_result_hash,
            )
        assert blocked.value.errno == errno.ELOOP
        assert unrelated.read_bytes() == b"must survive" and (other / "result.tmp").is_symlink()
    finally:
        artifacts.close()
