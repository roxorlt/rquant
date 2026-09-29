"""Private archive admission is actor scoped and bound to a verified registry."""

from __future__ import annotations

import os
import tempfile
import threading
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from rquant.factor.definition import build_factor_definition
from rquant.factor.draft import FactorSaveDraft
from rquant.factor.expression import FeatureCatalog
from rquant.factor.page_control_backend import FactorDefinitionPageControlBackend
from rquant.factor.registry import (
    FactorDefinitionRegistry,
    FactorHeadRef,
    SaveFactorDefinitionRequest,
)
from rquant.factor_definition_admission import (
    FactorDefinitionAdmission,
    FactorDefinitionAdmissionClient,
    FactorDefinitionAdmissionRejectedError,
    FactorDefinitionAdmissionUnavailableError,
    build_factor_definition_admission_server,
)
from rquant.page_control import (
    ArchiveFactor,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
)
from rquant.page_control_service import build_parser

NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)


def test_private_archive_submit_lookup_and_actor_allowlist(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    identity = registry.initialize()
    definition = build_factor_definition(
        factor_id="factor_one",
        name_zh="因子一",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=date(2024, 1, 2),
        expression="ts_mean(close, 5)",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="initial-factor", definition=definition, expected_head=None
        ),
        expected_identity=identity,
    )
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            factor_definition_backend=FactorDefinitionPageControlBackend(registry),
            clock=lambda: NOW,
        ),
    )
    command = ArchiveFactor(
        command_id="archive-private-1",
        requested_at=NOW,
        factor_id="factor_one",
        expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
    )
    web_uid = os.geteuid() + 1
    socket_root = Path(tempfile.mkdtemp(prefix="fa-", dir="/private/tmp"))
    os.chown(socket_root, os.geteuid(), os.getegid())
    socket_root.chmod(0o710)
    socket_path = socket_root / "factor.sock"
    server = build_factor_definition_admission_server(
        FactorDefinitionAdmission(service, editor_users=frozenset({"research-admin"})),
        socket_path=socket_path,
        trusted_web_uid=web_uid,
        shared_gid=os.getegid(),
        peer_uid=lambda _connection: web_uid,
    )
    assert server is not None
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        client = FactorDefinitionAdmissionClient(
            socket_path,
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: web_uid,
        )
        with pytest.raises(FactorDefinitionAdmissionRejectedError):
            client.submit(
                command,
                authenticated_actor_id="other-user",
                verified_registry_instance_id=identity.instance_id,
            )
        assert outbox.receipt(command.command_id) is None
        receipt = client.submit(
            command,
            authenticated_actor_id="research-admin",
            verified_registry_instance_id=identity.instance_id,
        )
        assert receipt.receipt.status is PageControlStatus.SUCCEEDED
        assert receipt.registry_instance_id == identity.instance_id
        assert client.lookup(command, authenticated_actor_id="research-admin") == receipt
        assert client.resume(command, authenticated_actor_id="research-admin") == receipt
        with pytest.raises(FactorDefinitionAdmissionRejectedError):
            client.lookup(command, authenticated_actor_id="other-user")
        repeat = command.model_copy(update={"command_id": "archive-private-2"})
        failed = client.submit(
            repeat,
            authenticated_actor_id="research-admin",
            verified_registry_instance_id=identity.instance_id,
        )
        assert failed.receipt.status is PageControlStatus.FAILED
        assert failed.receipt.error == "归档未完成"
        server.peer_uid = lambda _connection: -1
        with pytest.raises(FactorDefinitionAdmissionUnavailableError):
            client.lookup(command, authenticated_actor_id="research-admin")
    finally:
        server.shutdown()
        worker.join(timeout=3)
        server.server_close()
        socket_root.rmdir()


def test_private_archive_listener_disabled_without_editor_list(tmp_path: Path) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    registry.initialize()
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            factor_definition_backend=FactorDefinitionPageControlBackend(registry),
        ),
    )
    assert (
        build_factor_definition_admission_server(
            FactorDefinitionAdmission(service, editor_users=frozenset()),
            socket_path=tmp_path / "private" / "factor.sock",
            trusted_web_uid=os.geteuid() + 1,
            shared_gid=os.getegid(),
        )
        is None
    )


def test_private_save_requires_independent_enablement_and_original_draft(
    tmp_path: Path,
) -> None:
    registry = FactorDefinitionRegistry(tmp_path / "factor.sqlite3")
    identity = registry.initialize()
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            factor_definition_backend=FactorDefinitionPageControlBackend(registry),
            clock=lambda: NOW,
        ),
    )
    draft = FactorSaveDraft(
        generation_id="a" * 64,
        command_id="private-save-1",
        requested_at=NOW,
        mode="create",
        factor_id=None,
        expected_head=None,
        name_zh="价量强度",
        category="technical",
        direction="higher_is_better",
        expression="ts_mean(close, 5)",
    )
    disabled = FactorDefinitionAdmission(service, editor_users=frozenset({"research-admin"}))
    with pytest.raises(FactorDefinitionAdmissionRejectedError):
        disabled.submit_save(
            draft,
            authenticated_actor_id="research-admin",
            verified_registry_instance_id=identity.instance_id,
        )
    web_uid = os.geteuid() + 1
    socket_root = Path(tempfile.mkdtemp(prefix="fs-", dir="/private/tmp"))
    os.chown(socket_root, os.geteuid(), os.getegid())
    socket_root.chmod(0o710)
    server = build_factor_definition_admission_server(
        FactorDefinitionAdmission(
            service, editor_users=frozenset({"research-admin"}), save_enabled=True
        ),
        socket_path=socket_root / "factor.sock",
        trusted_web_uid=web_uid,
        shared_gid=os.getegid(),
        peer_uid=lambda _connection: web_uid,
    )
    assert server is not None
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        client = FactorDefinitionAdmissionClient(
            socket_root / "factor.sock",
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: web_uid,
        )
        result = client.submit_save(
            draft,
            authenticated_actor_id="research-admin",
            verified_registry_instance_id=identity.instance_id,
        )
        assert result.receipt.status is PageControlStatus.SUCCEEDED
        assert result.receipt.result["action"] == "save"
        assert result.registry_instance_id == identity.instance_id
        assert client.lookup_save(draft, authenticated_actor_id="research-admin") == result
        assert client.resume_save(draft, authenticated_actor_id="research-admin") == result
        with pytest.raises(FactorDefinitionAdmissionRejectedError):
            client.lookup_save(
                draft.model_copy(update={"generation_id": "b" * 64}),
                authenticated_actor_id="research-admin",
            )
        server.peer_uid = lambda _connection: -1
        with pytest.raises(FactorDefinitionAdmissionUnavailableError):
            client.lookup_save(draft, authenticated_actor_id="research-admin")
    finally:
        server.shutdown()
        worker.join(timeout=3)
        server.server_close()
        socket_root.rmdir()


def test_page_control_cli_accepts_only_explicit_archive_listener_configuration() -> None:
    argv = [
        "--manifest",
        "/private/tmp/manifest.json",
        "--control-root",
        "/private/tmp/control",
        "--expected-commit",
        "a" * 40,
        "--expected-generation",
        "b" * 64,
        "--factor-archive-socket",
        "/private/tmp/factor-archive/factor.sock",
        "--factor-archive-registry",
        "/private/tmp/factor-registry.sqlite3",
        "--factor-archive-web-uid",
        "501",
        "--factor-archive-shared-gid",
        "20",
        "--factor-archive-editors",
        "research-admin",
    ]
    args = build_parser().parse_args([*argv, "--factor-save-enabled"])
    assert args.factor_archive_socket == Path("/private/tmp/factor-archive/factor.sock")
    assert args.factor_archive_editors == "research-admin"
    assert args.factor_save_enabled is True
    assert build_parser().parse_args(argv).factor_save_enabled is False
