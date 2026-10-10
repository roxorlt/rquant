"""Trusted PageControl commands and a fenced factor definition store."""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pytest

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.page_control_backend import FactorDefinitionPageControlBackend
from rquant.factor.registry import (
    FactorConflictError,
    FactorDefinitionRegistry,
    FactorHeadRef,
    FactorIntegrityError,
    FactorRegistryIdentity,
    FactorRegistryIdentityError,
    SaveFactorDefinitionRequest,
)
from rquant.page_control import (
    ArchiveFactor,
    PageControlClient,
    PageControlCommandConflictError,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SaveFactorDefinition,
    _owned_factor_definition_command,
    parse_page_control_command,
)
from rquant.page_control_service import build_page_control_service, handler_for

NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)


def _definition(*, version: int = 1, name: str = "价量强度") -> FactorDefinition:
    return build_factor_definition(
        factor_id="price_volume_1",
        name_zh=name,
        category="technical",
        direction="higher_is_better",
        version=version,
        earliest_available_date=date(2024, 1, 2),
        expression="ts_mean(close, 5) / ref(volume, 2)",
        feature_catalog=FeatureCatalog(columns=("close", "volume")),
    )


def test_factor_registry_requires_preinitialization_and_fences_every_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = FactorDefinitionRegistry(path)
    first = SaveFactorDefinitionRequest(
        command_id="factor-first", definition=_definition(), expected_head=None
    )
    with pytest.raises(FactorRegistryIdentityError):
        registry.identity()
    assert not path.exists()

    identity = registry.initialize()
    assert identity.path == str(path)
    assert identity.instance_id
    assert identity.st_dev == path.stat().st_dev
    assert identity.st_ino == path.stat().st_ino
    receipt = registry.save(first, expected_identity=identity)
    assert registry.lookup_command(first, expected_identity=identity) == receipt

    second = SaveFactorDefinitionRequest(
        command_id="factor-second",
        definition=_definition(version=2, name="新版价量强度"),
        expected_head=FactorHeadRef(version=1, content_sha256=receipt.content_sha256),
    )
    assert registry.lookup_command(second, expected_identity=identity) is None
    registry.save(second, expected_identity=identity)
    assert registry.lookup_command(first, expected_identity=identity) == receipt


def test_missing_or_replaced_registry_never_replays_or_writes(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = FactorDefinitionRegistry(path)
    identity = registry.initialize()
    request = SaveFactorDefinitionRequest(
        command_id="factor-first", definition=_definition(), expected_head=None
    )
    registry.save(request, expected_identity=identity)
    moved = tmp_path / "original.sqlite3"
    path.rename(moved)
    with pytest.raises(FactorRegistryIdentityError):
        registry.lookup_command(request, expected_identity=identity)
    with pytest.raises(FactorRegistryIdentityError):
        registry.save(request, expected_identity=identity)
    assert not path.exists()

    replacement = FactorDefinitionRegistry(path)
    replacement_identity = replacement.initialize()
    assert replacement_identity.instance_id != identity.instance_id
    with pytest.raises(FactorRegistryIdentityError):
        replacement.lookup_command(request, expected_identity=identity)
    with pytest.raises(FactorRegistryIdentityError):
        replacement.save(request, expected_identity=identity)
    assert replacement.lookup_command(request, expected_identity=replacement_identity) is None


def _service(
    tmp_path: Path,
    *,
    backend: FactorDefinitionPageControlBackend | None,
    now: datetime = NOW,
) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            factor_definition_backend=backend,
            clock=lambda: now,
            lease_seconds=1,
        ),
    )


def _save_command(
    *,
    command_id: str = "factor-first",
    version: int = 1,
    expected_head: FactorHeadRef | None = None,
) -> SaveFactorDefinition:
    return SaveFactorDefinition(
        command_id=command_id,
        requested_at=NOW,
        definition=_definition(version=version),
        expected_head=expected_head,
    )


def test_archive_admission_binds_registry_and_only_drains_its_command(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    registry.save(
        SaveFactorDefinitionRequest(
            command_id="initial-save", definition=_definition(), expected_head=None
        ),
        expected_identity=identity,
    )
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    head = registry.get_head("price_volume_1", expected_identity=identity)
    assert head is not None
    archive = ArchiveFactor(
        command_id="archive-web-1",
        requested_at=NOW,
        factor_id="price_volume_1",
        expected_head=FactorHeadRef(version=1, content_sha256=head.content_sha256),
    )
    unrelated = _save_command(command_id="unrelated-save")
    service.outbox.enqueue_trusted_factor_definition(
        _owned_factor_definition_command(unrelated, authenticated_actor_id="research-admin")
    )

    receipt = service._submit_trusted_factor_archive(
        archive,
        authenticated_actor_id="research-admin",
        verified_registry_instance_id=identity.instance_id,
    )

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert service.outbox.receipt(unrelated.command_id).status is PageControlStatus.PENDING
    with sqlite3.connect(service.outbox.path) as connection:
        payload = connection.execute(
            "SELECT payload_json FROM page_control_command WHERE command_id = ?",
            (archive.command_id,),
        ).fetchone()[0]
    assert identity.instance_id in payload
    assert str(identity.st_ino) in payload
    assert (
        service._lookup_trusted_factor_archive(archive, authenticated_actor_id="research-admin")
        == receipt
    )
    with pytest.raises(PageControlCommandConflictError):
        service._resume_trusted_factor_archive(archive, authenticated_actor_id="another-user")
    assert registry.get_head("price_volume_1", expected_identity=identity).head.archived


def test_archive_admission_rejects_serving_registry_mismatch_before_enqueue(
    tmp_path: Path,
) -> None:
    source = FactorDefinitionRegistry(tmp_path / "serving-factor.sqlite3")
    source_identity = source.initialize()
    destination = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    destination_identity = destination.initialize()
    for registry, identity in (
        (source, source_identity),
        (destination, destination_identity),
    ):
        registry.save(
            SaveFactorDefinitionRequest(
                command_id="same-definition", definition=_definition(), expected_head=None
            ),
            expected_identity=identity,
        )
    source_head = source.get_head("price_volume_1", expected_identity=source_identity)
    destination_head = destination.get_head(
        "price_volume_1", expected_identity=destination_identity
    )
    assert source_head is not None and destination_head is not None
    assert source_head.content_sha256 == destination_head.content_sha256
    assert source_identity.instance_id != destination_identity.instance_id
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(destination))
    archive = ArchiveFactor(
        command_id="archive-web-2",
        requested_at=NOW,
        factor_id="price_volume_1",
        expected_head=FactorHeadRef(version=1, content_sha256=source_head.content_sha256),
    )
    with pytest.raises(ValueError, match="registry"):
        service._submit_trusted_factor_archive(
            archive,
            authenticated_actor_id="research-admin",
            verified_registry_instance_id=source_identity.instance_id,
        )
    assert service.outbox.receipt(archive.command_id) is None
    assert not destination.get_head(
        "price_volume_1", expected_identity=destination_identity
    ).head.archived


def test_archive_admission_does_not_write_replacement_registry_after_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = FactorDefinitionRegistry(path)
    identity = registry.initialize()
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="initial-save", definition=_definition(), expected_head=None
        ),
        expected_identity=identity,
    )
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    command = ArchiveFactor(
        command_id="archive-before-replace",
        requested_at=NOW,
        factor_id="price_volume_1",
        expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
    )
    enqueue = service.outbox.enqueue_trusted_factor_definition

    def replace_after_enqueue(owned: object) -> object:
        receipt = enqueue(owned)
        path.rename(tmp_path / "original.sqlite3")
        replacement = FactorDefinitionRegistry(path)
        replacement_identity = replacement.initialize()
        replacement.save(
            SaveFactorDefinitionRequest(
                command_id="replacement-save", definition=_definition(), expected_head=None
            ),
            expected_identity=replacement_identity,
        )
        return receipt

    monkeypatch.setattr(service.outbox, "enqueue_trusted_factor_definition", replace_after_enqueue)
    receipt = service._submit_trusted_factor_archive(
        command,
        authenticated_actor_id="research-admin",
        verified_registry_instance_id=identity.instance_id,
    )
    assert receipt.status is not PageControlStatus.SUCCEEDED
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM factor_archive_events").fetchone()[0] == 0
    with pytest.raises(PageControlCommandConflictError):
        service._lookup_trusted_factor_archive(
            command.model_copy(
                update={"expected_head": FactorHeadRef(version=1, content_sha256="0" * 64)}
            ),
            authenticated_actor_id="research-admin",
        )


def test_archive_admission_rechecks_registry_before_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = FactorDefinitionRegistry(path)
    identity = registry.initialize()
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="initial-save", definition=_definition(), expected_head=None
        ),
        expected_identity=identity,
    )
    backend = FactorDefinitionPageControlBackend(registry)
    service = _service(tmp_path, backend=backend)
    original_identity = backend.identity
    calls = 0

    def replace_before_recheck() -> FactorRegistryIdentity:
        nonlocal calls
        calls += 1
        if calls == 2:
            path.rename(tmp_path / "original.sqlite3")
            replacement = FactorDefinitionRegistry(path)
            replacement_identity = replacement.initialize()
            replacement.save(
                SaveFactorDefinitionRequest(
                    command_id="replacement-save", definition=_definition(), expected_head=None
                ),
                expected_identity=replacement_identity,
            )
        return original_identity()

    monkeypatch.setattr(backend, "identity", replace_before_recheck)
    command = ArchiveFactor(
        command_id="archive-before-recheck",
        requested_at=NOW,
        factor_id="price_volume_1",
        expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
    )
    with pytest.raises(ValueError, match="registry"):
        service._submit_trusted_factor_archive(
            command,
            authenticated_actor_id="research-admin",
            verified_registry_instance_id=identity.instance_id,
        )
    assert service.outbox.receipt(command.command_id) is None
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM factor_archive_events").fetchone()[0] == 0


def test_generic_admission_rejects_factor_commands_and_trusted_actor_is_durable(
    tmp_path: Path,
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    command = _save_command()
    archive = ArchiveFactor(
        command_id="factor-archive",
        requested_at=NOW,
        factor_id="price_volume_1",
        expected_head=FactorHeadRef(version=1, content_sha256="0" * 64),
    )
    for candidate in (command, archive):
        for operation in (
            lambda candidate=candidate: parse_page_control_command(
                candidate.model_dump(mode="json")
            ),
            lambda candidate=candidate: parse_page_control_command(candidate),
            lambda candidate=candidate: service.submit(candidate),
            lambda candidate=candidate: service.outbox.enqueue(candidate),
            lambda candidate=candidate: PageControlClient(
                transport=lambda _payload: pytest.fail("HTTP called")
            ).submit(candidate),
        ):
            with pytest.raises(ValueError, match="trusted"):
                operation()
        assert service.outbox.receipt(candidate.command_id) is None

    receipt = service._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result is not None
    with sqlite3.connect(service.outbox.path) as connection:
        payload = connection.execute(
            "SELECT payload_json FROM page_control_command WHERE command_id = ?",
            (command.command_id,),
        ).fetchone()[0]
    assert '"actor_id":"research-admin"' in payload
    assert (
        registry.get_head(
            "price_volume_1", expected_identity=registry.identity()
        ).definition.version
        == 1
    )


def test_trusted_factor_save_archive_replay_and_payload_conflict(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    first = _save_command()
    first_receipt = service._submit_trusted_factor_definition(
        first, authenticated_actor_id="research-admin"
    )
    head = registry.get_head("price_volume_1", expected_identity=identity)
    expected = FactorHeadRef(version=1, content_sha256=head.content_sha256)
    second = _save_command(command_id="factor-second", version=2, expected_head=expected)
    second_receipt = service._submit_trusted_factor_definition(
        second, authenticated_actor_id="research-admin"
    )
    archive = ArchiveFactor(
        command_id="factor-archive",
        requested_at=NOW,
        factor_id="price_volume_1",
        expected_head=FactorHeadRef(
            version=2,
            content_sha256=registry.get_head(
                "price_volume_1", expected_identity=identity
            ).content_sha256,
        ),
    )
    archived = service._submit_trusted_factor_definition(
        archive, authenticated_actor_id="research-admin"
    )
    assert (
        first_receipt.status
        is second_receipt.status
        is archived.status
        is PageControlStatus.SUCCEEDED
    )
    assert (
        service._submit_trusted_factor_definition(
            first, authenticated_actor_id="research-admin"
        ).result
        == first_receipt.result
    )
    assert (
        registry.get_version("price_volume_1", 1, expected_identity=identity).definition
        == _definition()
    )
    assert registry.get_head("price_volume_1", expected_identity=identity).head.archived
    assert registry.list_current(expected_identity=identity) == ()
    changed = first.model_copy(update={"definition": _definition(name="重放异载荷")})
    with pytest.raises(ValueError, match="different payload"):
        service._submit_trusted_factor_definition(changed, authenticated_actor_id="research-admin")


def test_crash_after_registry_commit_recovers_original_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    command = _save_command()
    original_finish = service.outbox.finish_effect
    crashed = False

    def crash_once(command_id: str, **kwargs: object):
        nonlocal crashed
        if not crashed:
            crashed = True
            raise KeyboardInterrupt("crash after registry commit")
        return original_finish(command_id, **kwargs)

    monkeypatch.setattr(service.outbox, "finish_effect", crash_once)
    with pytest.raises(KeyboardInterrupt, match="crash after registry commit"):
        service._submit_trusted_factor_definition(command, authenticated_actor_id="research-admin")
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 1
    effect = service.outbox.effect(command.command_id)
    assert effect is not None and effect.result is not None
    assert effect.result["identity"]["instance_id"] == identity.instance_id

    restarted = _service(
        tmp_path,
        backend=FactorDefinitionPageControlBackend(FactorDefinitionRegistry(registry.path)),
        now=NOW + timedelta(seconds=2),
    )
    recovered = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result == registry.lookup_command(
        SaveFactorDefinitionRequest(
            command_id=command.command_id,
            definition=command.definition,
            expected_head=command.expected_head,
        ),
        expected_identity=identity,
    ).model_dump(mode="json")
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 1


def test_recovery_waits_for_original_registry_and_never_writes_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    command = _save_command()
    monkeypatch.setattr(
        service.outbox,
        "finish_effect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt("crash")),
    )
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_definition(command, authenticated_actor_id="research-admin")
    moved = tmp_path / "original.sqlite3"
    registry.path.rename(moved)
    restarted = _service(
        tmp_path,
        backend=FactorDefinitionPageControlBackend(FactorDefinitionRegistry(registry.path)),
        now=NOW + timedelta(seconds=2),
    )
    missing = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert missing.status is PageControlStatus.PENDING
    assert not registry.path.exists()

    replacement = FactorDefinitionRegistry(registry.path)
    replacement_identity = replacement.initialize()
    replaced = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert replaced.status is PageControlStatus.PENDING
    assert replacement.get_head("price_volume_1", expected_identity=replacement_identity) is None
    replacement.path.rename(tmp_path / "replacement.sqlite3")
    moved.rename(registry.path)
    restored = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert restored.status is PageControlStatus.SUCCEEDED
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 1


def test_first_factor_command_without_backend_fails_without_registry_write(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=None)
    receipt = service._submit_trusted_factor_definition(
        _save_command(), authenticated_actor_id="research-admin"
    )
    assert receipt.status is PageControlStatus.FAILED
    assert registry.get_head("price_volume_1", expected_identity=identity) is None


def test_raw_http_post_and_archive_parser_cannot_enqueue_factor_write(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    for command in (
        _save_command(),
        ArchiveFactor(
            command_id="archive-request",
            requested_at=NOW,
            factor_id="price_volume_1",
            expected_head=FactorHeadRef(version=1, content_sha256="0" * 64),
        ),
    ):
        with pytest.raises(ValueError, match="trusted"):
            parse_page_control_command(command.model_dump(mode="json"))
        body = command.model_dump_json().encode()
        handler = object.__new__(handler_for(service))
        handler.path = "/v1/commands"
        handler.headers = {"Content-Length": str(len(body))}
        handler.rfile = BytesIO(body)
        handler.wfile = BytesIO()
        statuses: list[int] = []
        handler.send_response = statuses.append
        handler.send_header = lambda _name, _value: None
        handler.end_headers = lambda: None
        handler.do_POST()
        assert statuses == [400]
        assert service.outbox.receipt(command.command_id) is None


def test_service_factory_injects_backend_only_when_explicit(tmp_path: Path) -> None:
    registry_path = tmp_path / "factors.sqlite3"
    backend = FactorDefinitionPageControlBackend(FactorDefinitionRegistry(registry_path))
    shared = {
        "outbox_path": tmp_path / "outbox.sqlite3",
        "data_dir": tmp_path / "data",
        "log_dir": tmp_path / "logs",
        "allowed_lab_export_roots": (tmp_path / "exports",),
        "load_default_lab_backend": False,
    }
    injected = build_page_control_service(**shared, factor_definition_backend=backend)
    assert injected.consumer.factor_definition_backend is backend
    default = build_page_control_service(**shared)
    assert default.consumer.factor_definition_backend is None
    assert not registry_path.exists()


def test_started_without_identity_can_bind_same_preinitialized_store_before_first_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    command = _save_command()
    monkeypatch.setattr(
        service.outbox,
        "record_started_effect_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt("before identity")),
    )
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_definition(command, authenticated_actor_id="research-admin")
    effect = service.outbox.effect(command.command_id)
    assert effect is not None and effect.result is None
    assert registry.get_head("price_volume_1", expected_identity=identity) is None

    restarted = _service(
        tmp_path,
        backend=FactorDefinitionPageControlBackend(FactorDefinitionRegistry(registry.path)),
        now=NOW + timedelta(seconds=2),
    )
    receipt = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 1


def test_same_store_absent_command_can_execute_after_started_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    backend = FactorDefinitionPageControlBackend(registry)
    service = _service(tmp_path, backend=backend)
    command = _save_command()
    monkeypatch.setattr(
        backend,
        "submit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt("before write")),
    )
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_definition(command, authenticated_actor_id="research-admin")
    assert registry.get_head("price_volume_1", expected_identity=identity) is None

    restarted = _service(
        tmp_path,
        backend=FactorDefinitionPageControlBackend(FactorDefinitionRegistry(registry.path)),
        now=NOW + timedelta(seconds=2),
    )
    receipt = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 1


def test_lookup_conflict_and_bad_receipt_fail_closed(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    request = SaveFactorDefinitionRequest(
        command_id="factor-first", definition=_definition(), expected_head=None
    )
    registry.save(request, expected_identity=identity)
    with pytest.raises(FactorConflictError):
        registry.lookup_command(
            SaveFactorDefinitionRequest(
                command_id="factor-first",
                definition=_definition(name="不同定义"),
                expected_head=None,
            ),
            expected_identity=identity,
        )
    with sqlite3.connect(registry.path) as connection:
        connection.execute(
            "UPDATE factor_commands SET receipt_json = '{bad' WHERE command_id = 'factor-first'"
        )
    with pytest.raises(FactorIntegrityError):
        registry.lookup_command(request, expected_identity=identity)


def test_recovery_keeps_started_effect_pending_when_registry_receipt_is_damaged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    command = _save_command()
    monkeypatch.setattr(
        service.outbox,
        "finish_effect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt("crash")),
    )
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_definition(command, authenticated_actor_id="research-admin")
    with sqlite3.connect(registry.path) as connection:
        original = connection.execute(
            "SELECT receipt_json FROM factor_commands WHERE command_id = ?", (command.command_id,)
        ).fetchone()[0]
        connection.execute(
            "UPDATE factor_commands SET receipt_json = '{bad' WHERE command_id = ?",
            (command.command_id,),
        )
    restarted = _service(
        tmp_path,
        backend=FactorDefinitionPageControlBackend(FactorDefinitionRegistry(registry.path)),
        now=NOW + timedelta(seconds=2),
    )
    pending = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert pending.status is PageControlStatus.PENDING
    with sqlite3.connect(registry.path) as connection:
        connection.execute(
            "UPDATE factor_commands SET receipt_json = ? WHERE command_id = ?",
            (original, command.command_id),
        )
    recovered = restarted._submit_trusted_factor_definition(
        command, authenticated_actor_id="research-admin"
    )
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 1


def test_old_schema_is_not_migrated_or_used_for_lookup(tmp_path: Path) -> None:
    path = tmp_path / "old.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 2")
    registry = FactorDefinitionRegistry(path)
    with pytest.raises(FactorIntegrityError):
        registry.identity()
    with pytest.raises(FactorRegistryIdentityError):
        registry.initialize()


def test_two_factor_commands_with_same_expected_head_do_not_overwrite(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    assert (
        service._submit_trusted_factor_definition(
            _save_command(), authenticated_actor_id="research-admin"
        ).status
        is PageControlStatus.SUCCEEDED
    )
    head = registry.get_head("price_volume_1", expected_identity=identity)
    expected = FactorHeadRef(version=1, content_sha256=head.content_sha256)
    assert (
        service._submit_trusted_factor_definition(
            _save_command(command_id="v2-a", version=2, expected_head=expected),
            authenticated_actor_id="research-admin",
        ).status
        is PageControlStatus.SUCCEEDED
    )
    rejected = service._submit_trusted_factor_definition(
        _save_command(command_id="v2-b", version=2, expected_head=expected),
        authenticated_actor_id="research-admin",
    )
    assert rejected.status is PageControlStatus.FAILED
    assert registry.get_head("price_volume_1", expected_identity=identity).definition.version == 2


def test_owned_factor_outbox_rejects_changed_stored_payload_before_retry_or_claim(
    tmp_path: Path,
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factors.sqlite3")
    identity = registry.initialize()
    service = _service(tmp_path, backend=FactorDefinitionPageControlBackend(registry))
    owned = _owned_factor_definition_command(
        _save_command(), authenticated_actor_id="research-admin"
    )
    service.outbox.enqueue_trusted_factor_definition(owned)
    with sqlite3.connect(service.outbox.path) as connection:
        connection.execute(
            "UPDATE page_control_command SET payload_json = replace(payload_json, ?, ?) "
            "WHERE command_id = ?",
            ('"actor_id":"research-admin"', '"actor_id":"other-actor"', owned.command_id),
        )
    with pytest.raises(PageControlCommandConflictError):
        service.outbox.enqueue_trusted_factor_definition(owned)
    assert service.consumer.drain(limit=1) == ()
    assert registry.get_head("price_volume_1", expected_identity=identity) is None
