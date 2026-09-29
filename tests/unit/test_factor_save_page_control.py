"""Only the original actor and draft can resume the fenced save command."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.factor.draft import FactorSaveDraft
from rquant.factor.page_control_backend import FactorDefinitionPageControlBackend
from rquant.factor.registry import FactorDefinitionRegistry
from rquant.page_control import (
    PageControlCommandConflictError,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
)

NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)


def _draft(**changes: object) -> FactorSaveDraft:
    fields = {
        "generation_id": "a" * 64,
        "command_id": "save-original-1",
        "requested_at": NOW,
        "mode": "create",
        "factor_id": None,
        "expected_head": None,
        "name_zh": "价量强度",
        "category": "technical",
        "direction": "higher_is_better",
        "expression": "ts_mean(close, 5)",
    }
    fields.update(changes)
    return FactorSaveDraft.model_validate(fields)


def _service(tmp_path: Path, registry: FactorDefinitionRegistry) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            factor_definition_backend=FactorDefinitionPageControlBackend(registry),
            clock=lambda: NOW,
        ),
    )


def test_save_is_actor_and_full_original_request_bound_and_targeted(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, registry)
    draft = _draft()
    receipt = service._submit_trusted_factor_save(
        draft,
        authenticated_actor_id="researcher",
        verified_registry_instance_id=identity.instance_id,
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result["action"] == "save"
    assert receipt.result["version"] == 1
    assert (
        service._lookup_trusted_factor_save(draft, authenticated_actor_id="researcher") == receipt
    )
    assert (
        service._resume_trusted_factor_save(draft, authenticated_actor_id="researcher") == receipt
    )
    for changed in (
        _draft(generation_id="b" * 64),
        _draft(name_zh="不同名称"),
        _draft(requested_at=NOW.replace(minute=1)),
    ):
        with pytest.raises(PageControlCommandConflictError):
            service._lookup_trusted_factor_save(changed, authenticated_actor_id="researcher")
    with pytest.raises(PageControlCommandConflictError):
        service._lookup_trusted_factor_save(draft, authenticated_actor_id="other")
    assert len(registry.list_current(expected_identity=identity)) == 1


def test_save_retains_original_registry_identity_after_file_replacement(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, registry)
    draft = _draft()
    original_settle = service._settle
    service._settle = lambda _command, receipt, **_kwargs: receipt  # type: ignore[method-assign]
    try:
        pending = service._submit_trusted_factor_save(
            draft,
            authenticated_actor_id="researcher",
            verified_registry_instance_id=identity.instance_id,
        )
    finally:
        service._settle = original_settle  # type: ignore[method-assign]
    assert pending.status is PageControlStatus.PENDING
    registry.path.unlink()
    replacement = FactorDefinitionRegistry(registry.path)
    replacement.initialize()
    resumed = service._resume_trusted_factor_save(draft, authenticated_actor_id="researcher")
    assert resumed.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}
    assert replacement.list_current(expected_identity=replacement.identity()) == ()


def test_delayed_duplicate_submission_and_post_commit_recovery_write_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, registry)
    draft = _draft()
    original_save = registry.save
    interrupted = False

    def save_then_lose_reply(*args: object, **kwargs: object) -> object:
        nonlocal interrupted
        receipt = original_save(*args, **kwargs)
        if not interrupted:
            interrupted = True
            raise OSError("simulated lost registry reply")
        return receipt

    monkeypatch.setattr(registry, "save", save_then_lose_reply)

    def submit() -> PageControlStatus:
        return service._submit_trusted_factor_save(
            draft,
            authenticated_actor_id="researcher",
            verified_registry_instance_id=identity.instance_id,
        ).status

    with ThreadPoolExecutor(max_workers=2) as workers:
        statuses = tuple(workers.map(lambda _index: submit(), range(2)))
    assert statuses
    terminal = service._resume_trusted_factor_save(draft, authenticated_actor_id="researcher")
    assert terminal.status is PageControlStatus.SUCCEEDED
    assert terminal.result["version"] == 1
    assert len(registry.list_current(expected_identity=identity)) == 1
    assert registry.get_version(terminal.result["factor_id"], 2, expected_identity=identity) is None
