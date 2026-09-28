"""Only a verified, completed formula task may create a replayable formula pool."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.formula_market_page_backend import FormulaMarketPageBackend
from rquant.formula_market_private_config import FormulaMarketPrivateConfig
from rquant.llm.schemas import RuleCall
from rquant.page_control import PageControlStatus, SaveUserPoolV2, parse_page_control_command
from rquant.page_control_service import build_page_control_service
from rquant.runtime_contracts import canonical_sha256
from rquant.screen.formula_market_jobs import FormulaMarketJobResult, FormulaMarketJobWorker
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_admission import _command
from tests.unit.test_formula_market_run import _history, _market


def _save(
    task_id: str,
    *,
    command_id: str = "save-formula-pool-1",
    base_name: str = "research",
    actor_id: str = "researcher",
    expected_version: str | None = None,
) -> object:
    return parse_page_control_command(
        {
            "kind": "save_formula_pool_v1",
            "command_id": command_id,
            "requested_at": datetime.now(UTC).isoformat(),
            "base_name": base_name,
            "display_name": "研究池",
            "task_id": task_id,
            "actor_id": actor_id,
            "expected_version": expected_version,
        }
    )


def _setup(tmp_path: Path) -> tuple[object, object, object, Path]:
    from rquant.formula_pool_definition import FormulaPoolDefinitionStore, FormulaPoolSaveBackend

    market, history = _market(tmp_path), _history(tmp_path)
    config = FormulaMarketPrivateConfig(
        universe_root=market[0],
        projection_root=history[0],
        state_path=tmp_path / "tasks" / "formula-jobs.sqlite",
        artifact_directory=tmp_path / "results",
    )
    admission = FormulaMarketPageBackend(config)
    data_dir = tmp_path / "page"
    definitions = FormulaPoolDefinitionStore(
        definition_root=data_dir / "formula_pools",
        rule_pool_root=data_dir / "user_presets",
    )
    backend = FormulaPoolSaveBackend(task_store=admission.store, definitions=definitions)
    service = build_page_control_service(
        outbox_path=tmp_path / "control" / "page-control.sqlite",
        data_dir=data_dir,
        log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(),
        formula_market_backend=admission,
        formula_pool_backend=backend,
        load_default_lab_backend=False,
    )
    return service, admission, definitions, data_dir


def _queued(service: object, command_id: str = "formula-pool-run") -> str:
    queued = service.submit(_command(command_id))
    assert queued.status is PageControlStatus.SUCCEEDED
    assert queued.result is not None and queued.result["outcome"] == "task_queued"
    return queued.result["task_id"]


def test_save_command_rejects_browser_formula_codes_and_paths() -> None:
    command = _save("a" * 32)
    assert command.kind == "save_formula_pool_v1"
    for forbidden, value in (
        ("formula", "CLOSE>0"),
        ("match_codes", ["000001.SZ"]),
        ("artifact_directory", "/tmp/results"),
        ("universe_root", "/tmp/market"),
    ):
        with pytest.raises(ValidationError):
            parse_page_control_command(command.model_dump(mode="json") | {forbidden: value})
    with pytest.raises(ValidationError):
        parse_page_control_command(command.model_dump(mode="json") | {"base_name": "x" * 81})


def test_save_command_requires_actor_identity() -> None:
    payload = _save("a" * 32).model_dump(mode="json")
    payload.pop("actor_id")
    with pytest.raises(ValidationError):
        parse_page_control_command(payload)
    command = parse_page_control_command(payload | {"actor_id": "researcher"})
    assert command.actor_id == "researcher"


def test_original_save_command_id_cannot_be_retried_as_another_actor(tmp_path: Path) -> None:
    from rquant.page_control import PageControlCommandConflictError

    service, admission, _definitions, _data_dir = _setup(tmp_path)
    task_id = _queued(service)
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    original = _save(task_id)
    assert service.submit(original).status is PageControlStatus.SUCCEEDED
    changed_actor = parse_page_control_command(
        original.model_dump(mode="json") | {"actor_id": "another-user"}
    )
    with pytest.raises(PageControlCommandConflictError):
        service.submit(changed_actor)


def test_page_control_queues_worker_completes_then_saves_replayable_definition(
    tmp_path: Path,
) -> None:
    service, admission, definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    completed = FormulaMarketJobWorker(
        admission.store,
        trusted_source_roots=(admission.config.universe_root, admission.config.projection_root),
    ).run_one()
    assert completed is not None and completed.status == "succeeded"

    command = _save(task_id)
    saved = service.submit(command)
    assert saved.status is PageControlStatus.SUCCEEDED
    assert saved.result is not None
    assert saved.result["pool_name"] == "user/research"
    version = saved.result["version"]
    definition = definitions.read("research", expected_version=version)
    assert definition.schema_version == 1
    assert definition.syntax_version == "tdx-v1"
    assert definition.pool_name == "user/research"
    assert definition.display_name == "研究池"
    assert definition.formula == "CLOSE>2"
    assert definition.creation.task_id == task_id
    assert definition.creation.result_sha256 == completed.result_sha256
    request, _result = admission.store.read_succeeded_task(task_id)
    assert definition.creation.trade_date == request.trade_date
    assert definition.creation.universe_identity == request.expected_universe_sha256
    assert definition.creation.projection_identity == request.expected_projection_identity
    assert definition.version == version
    with pytest.raises(ValueError, match="version"):
        definitions.read("research", expected_version="0" * 64)
    assert "match_codes" not in definition.model_dump(mode="json")
    definition_path = data_dir / "formula_pools" / "research.json"
    assert definition_path.stat().st_mode & 0o777 == 0o600
    assert definition_path.parent.stat().st_mode & 0o777 == 0o700
    assert service.submit(command) == saved


@pytest.mark.parametrize("task_state", ("queued", "failed", "broken_result"))
def test_incomplete_or_invalid_task_never_creates_definition(
    tmp_path: Path,
    task_state: str,
) -> None:
    service, admission, definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    if task_state == "failed":
        claim = admission.store._claim()
        assert claim is not None
        admission.store._finish_failure(claim, "source_changed")
    elif task_state == "broken_result":
        completed = FormulaMarketJobWorker(admission.store).run_one()
        assert completed is not None and completed.result_sha256 is not None
        artifact = next(admission.config.artifact_directory.iterdir())
        artifact.write_bytes(b"{}")

    saved = service.submit(_save(task_id))
    assert saved.status is PageControlStatus.FAILED
    assert not (data_dir / "formula_pools" / "research.json").exists()
    with pytest.raises(FileNotFoundError):
        definitions.read("research")


def test_name_conflict_expected_version_and_unsafe_existing_file_never_overwrite(
    tmp_path: Path,
) -> None:
    service, admission, definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    first = service.submit(_save(task_id))
    assert first.status is PageControlStatus.SUCCEEDED
    existing = data_dir / "formula_pools" / "research.json"
    original = existing.read_bytes()

    assert (
        service.submit(_save(task_id, command_id="new-command")).status is PageControlStatus.FAILED
    )
    assert (
        service.submit(
            _save(task_id, command_id="update-command", expected_version=first.result["version"])
        ).status
        is PageControlStatus.FAILED
    )
    assert existing.read_bytes() == original

    rules = data_dir / "user_presets"
    rules.mkdir(mode=0o700)
    (rules / "rule-name.json").write_bytes(b"{}")
    assert (
        service.submit(_save(task_id, command_id="rule-conflict", base_name="rule-name")).status
        is PageControlStatus.FAILED
    )
    assert not (data_dir / "formula_pools" / "rule-name.json").exists()

    existing.write_bytes(b"corrupt")
    assert (
        service.submit(_save(task_id, command_id="bad-existing")).status is PageControlStatus.FAILED
    )
    assert existing.read_bytes() == b"corrupt"
    existing.unlink()
    existing.symlink_to(rules / "rule-name.json")
    assert (
        service.submit(_save(task_id, command_id="linked-existing")).status
        is PageControlStatus.FAILED
    )
    assert existing.is_symlink()


def test_coherent_result_with_wrong_formula_digest_cannot_be_saved(tmp_path: Path) -> None:
    service, admission, _definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    original = admission.store.read_result(task_id)
    altered_data = original.model_dump(mode="python")
    altered_data["formula_sha256"] = "0" * 64
    altered_data["content_sha256"] = canonical_sha256(
        {key: value for key, value in altered_data.items() if key != "content_sha256"}
    )
    altered = FormulaMarketJobResult.model_validate(altered_data)
    new_path = admission.config.artifact_directory / admission.store._artifact_name(
        task_id, altered.content_sha256
    )
    new_path.write_bytes(canonical_json_bytes(altered.model_dump(mode="json")))
    os.chmod(new_path, 0o600)
    with sqlite3.connect(admission.config.state_path) as connection:
        connection.execute(
            "UPDATE formula_market_job SET result_sha256 = ? WHERE task_id = ?",
            (altered.content_sha256, task_id),
        )

    refused = service.submit(_save(task_id))
    assert refused.status is PageControlStatus.FAILED
    assert not (data_dir / "formula_pools" / "research.json").exists()


def test_unsafe_definition_directory_rejects_save(tmp_path: Path) -> None:
    service, admission, _definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    root = data_dir / "formula_pools"
    root.mkdir(mode=0o755, parents=True)
    os.chmod(root, 0o755)

    refused = service.submit(_save(task_id))
    assert refused.status is PageControlStatus.FAILED
    assert not (root / "research.json").exists()


def test_lost_receipt_recovers_exact_original_definition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, admission, definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    command = _save(task_id)
    backend = service.consumer.formula_pool_backend
    original_submit = backend.submit

    def lose_receipt(value: object) -> object:
        original_submit(value)
        raise KeyboardInterrupt("after definition link")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "submit", lose_receipt)
        with pytest.raises(KeyboardInterrupt, match="after definition link"):
            service.submit(command)
    service.consumer.clock = lambda: datetime.now(UTC) + timedelta(seconds=60)
    recovered = service.submit(command)
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result is not None
    definition = definitions.read("research", expected_version=recovered.result["version"])
    assert definition.command_id == command.command_id
    assert len(tuple((data_dir / "formula_pools").iterdir())) == 1


def test_rule_pool_cannot_reuse_existing_formula_pool_name(tmp_path: Path) -> None:
    service, admission, _definitions, data_dir = _setup(tmp_path)
    task_id = _queued(service)
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    assert service.submit(_save(task_id)).status is PageControlStatus.SUCCEEDED

    rule_save = SaveUserPoolV2(
        command_id="rule-after-formula",
        requested_at=datetime.now(UTC),
        base_name="research",
        display_name="规则池",
        rule_calls=(RuleCall(name="not_st", args={}),),
        include_columns=("CLOSE[0]",),
        expected_version=None,
    )
    refused = service.submit(rule_save)
    assert refused.status is PageControlStatus.FAILED
    assert "formula pool" in (refused.error or "")
    assert not (data_dir / "user_presets" / "research.json").exists()
