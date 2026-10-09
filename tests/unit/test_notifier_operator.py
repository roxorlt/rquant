"""Manual notify-test uses a signed exact service and actual absent timer facts."""

from __future__ import annotations

import base64
import hashlib
import os
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

import pytest

from tests.unit.test_task_unit_control import BOOT, NOW, REQUEST, _manifest, systemd_window

UNIT = "rquant-notify-test.service"
COMMIT = "c" * 40
PROFILE = "d" * 64


def signed_install(tmp_path: Path) -> tuple[Path, bytes, object, bytes]:
    from rquant.notifier_operator import NotifierManualServiceInstall, SignedNotifierManualServiceInstall

    fragment = tmp_path / UNIT
    content = (
        "[Unit]\nDescription=Notification channel test\n[Service]\nType=oneshot\n"
        "User=rquant\nWorkingDirectory=/opt/rquant\nExecStart=/opt/rquant/.venv/bin/rquant notify-test\n"
        "Restart=no\nEnvironmentFile=/opt/rquant/test-credentials.env\n"
    ).encode()
    fragment.write_bytes(content)
    fragment.chmod(0o600)
    install = NotifierManualServiceInstall(
        host_name="rquant-test", manifest_digest=_manifest().digest,
        profile_sha256=PROFILE, runtime_commit=COMMIT, installation_id="notification-test/v1", revision=1,
        fragment_path=str(fragment), fragment_sha256=hashlib.sha256(content).hexdigest(),
        user="rquant", working_directory="/opt/rquant", cli_path="/opt/rquant/.venv/bin/rquant",
        environment_file="/opt/rquant/test-credentials.env", enabled=True,
    )
    openssl = shutil.which("openssl")
    assert openssl is not None
    private, public = tmp_path / "synthetic-private.pem", tmp_path / "synthetic-public.pem"
    body, signature = tmp_path / "signed.body", tmp_path / "signature"
    subprocess.run((openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private)), check=True, capture_output=True)
    subprocess.run((openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)), check=True, capture_output=True)
    body.write_bytes(install.signing_bytes())
    subprocess.run((openssl, "pkeyutl", "-sign", "-inkey", str(private), "-rawin", "-in", str(body), "-out", str(signature)), check=True, capture_output=True)
    path = tmp_path / "manual-install.json"
    envelope = SignedNotifierManualServiceInstall(install=install, signature=base64.b64encode(signature.read_bytes()).decode())
    path.write_bytes(envelope.canonical_bytes())
    path.chmod(0o600)
    return path, public.read_bytes(), install, content


def raw_manager(install: object, **changes: str) -> tuple[bytes, bytes, bytes]:
    fields = {
        "Id": UNIT, "Names": UNIT, "LoadState": "loaded", "ActiveState": "inactive",
        "MainPID": "0", "InvocationID": "", "FragmentPath": install.fragment_path,
        "DropInPaths": "", "TriggeredBy": "", "Type": "oneshot", "User": "rquant",
        "WorkingDirectory": "/opt/rquant", "Restart": "no",
        "EnvironmentFiles": "/opt/rquant/test-credentials.env (ignore_errors=no)",
        "ExecStart": "{ path=/opt/rquant/.venv/bin/rquant ; argv[]=/opt/rquant/.venv/bin/rquant notify-test ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }",
        "Job": "0", "Result": "success", "ExecMainStatus": "0",
        "ExecMainStartTimestamp": "", "ExecMainExitTimestamp": "",
        "ExecMainStartTimestampMonotonic": "0", "ExecMainExitTimestampMonotonic": "0",
    } | changes
    return (
        "\n".join(f"{key}={value}" for key, value in fields.items()).encode(),
        b"Id=rquant-notify-test.timer\nLoadState=not-found\nFragmentPath=\n",
        b'{"type":"a(usssoo)","data":[[]]}',
    )


def receipt(tmp_path: Path, **changes: str) -> object:
    from rquant.notifier_operator import capture_manual_service_receipt

    path, key, install, _ = signed_install(tmp_path)
    show, timer, jobs = raw_manager(install, **changes)
    return capture_manual_service_receipt(
        install_path=path, public_key_pem=key, manifest=_manifest(), expected_profile_sha256=PROFILE,
        expected_runtime_commit=COMMIT, expected_host="rquant-test", boot_before=BOOT, boot_after=BOOT,
        service_show=show, timer_show=timer, list_jobs=jobs,
        observed_at=NOW, observed_monotonic_ns=500_000_000, expected_uid=os.getuid(),
    )


def test_manual_signed_install_does_not_expand_original_timer_policy(tmp_path: Path) -> None:
    from rquant.notifier_operator import load_notifier_manual_install
    from rquant.task_unit_control import TaskUnitRunPolicy

    path, key, installed, _ = signed_install(tmp_path)
    actual, source = load_notifier_manual_install(
        path, public_key_pem=key, manifest=_manifest(), expected_profile_sha256=PROFILE,
        expected_runtime_commit=COMMIT, expected_uid=os.getuid(),
    )
    assert actual == installed
    assert source.raw_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="manifest|exact"):
        TaskUnitRunPolicy(version=1, host_name="rquant-test", manifest_digest=_manifest().digest,
                          enabled=True, units=({"unit": UNIT, "mode": "readonly", "enabled": True},)).bind_manifest(_manifest())
    old = path.read_bytes()
    path.write_bytes(old.replace(b'"revision":1', b'"revision":2'))
    with pytest.raises(ValueError, match="signature"):
        load_notifier_manual_install(path, public_key_pem=key, manifest=_manifest(),
                                     expected_profile_sha256=PROFILE, expected_runtime_commit=COMMIT, expected_uid=os.getuid())
    path.write_bytes(old)
    alias = tmp_path / "alias.json"
    alias.symlink_to(path)
    with pytest.raises((ValueError, OSError)):
        load_notifier_manual_install(alias, public_key_pem=key, manifest=_manifest(),
                                     expected_profile_sha256=PROFILE, expected_runtime_commit=COMMIT, expected_uid=os.getuid())


@pytest.mark.parametrize("changes", [{"MainPID": ""}, {"MainPID": "1"}, {"DropInPaths": "/evil.conf"},
                                     {"TriggeredBy": "some.timer"}, {"Names": UNIT + " alias.service"},
                                     {"ExecStart": "{ path=/bin/sh ; argv[]=/bin/sh -c true ; }"}])
def test_manual_complete_manager_definition_and_idle_are_required(tmp_path: Path, changes: dict[str, str]) -> None:
    from rquant.notifier_operator import guard_manual_service_run

    with pytest.raises(ValueError):
        guard_manual_service_run(receipt(tmp_path, **changes), now=NOW)


def test_absence_is_not_disabled_timer_or_missing_field_or_claimed_hash(tmp_path: Path) -> None:
    from rquant.notifier_operator import ManualServiceReadReceipt, guard_manual_service_run

    actual = receipt(tmp_path)
    assert actual.runtime_state.unit == UNIT and actual.main_pid == 0
    assert "timer_before_usec" not in actual.model_dump()
    assert guard_manual_service_run(actual, now=NOW).unit == UNIT
    with pytest.raises(ValueError, match="stale|future"):
        guard_manual_service_run(actual, now=NOW + timedelta(seconds=120))
    raw = actual.model_dump()
    for body in ("Id=rquant-notify-test.timer\nLoadState=loaded\nFragmentPath=\n",
                 "Id=rquant-notify-test.timer\nLoadState=not-found\n",
                 "Id=rquant-other.timer\nLoadState=not-found\nFragmentPath=\n"):
        material = dict(raw["material"]) | {"timer_show": body}
        with pytest.raises(ValueError):
            ManualServiceReadReceipt.model_validate(raw | {"material": material})
    with pytest.raises(ValueError):
        ManualServiceReadReceipt.model_validate(raw | {"main_pid": 999})


def manual_window(tmp_path: Path, *, before: object | None = None) -> object:
    from rquant.task_unit_control import ManualServiceRunWindow

    before = receipt(tmp_path) if before is None else before
    raw = systemd_window()
    for key in ("timer_before_usec", "timer_after_usec"):
        raw.pop(key)
    raw["unit"] = UNIT
    for key in ("calls", "events"):
        raw[key] = tuple(dict(value) | {"unit": UNIT} for value in raw[key])
    raw["invocation"] = dict(raw["invocation"]) | {"unit": UNIT}
    after_data = before.model_dump()
    material = dict(after_data["material"])
    material["service_show"] = material["service_show"].replace("InvocationID=\n", "InvocationID=" + "a" * 32 + "\n")
    material["service_show"] = material["service_show"].replace("ExecMainStartTimestamp=\n", "ExecMainStartTimestamp=Tue 2026-10-06 01:14:00 UTC\n")
    material["service_show"] = material["service_show"].replace("ExecMainExitTimestamp=\n", "ExecMainExitTimestamp=Tue 2026-10-06 01:14:02 UTC\n")
    material["service_show"] = material["service_show"].replace("ExecMainStartTimestampMonotonic=0", "ExecMainStartTimestampMonotonic=1000000")
    material["service_show"] = material["service_show"].replace("ExecMainExitTimestampMonotonic=0", "ExecMainExitTimestampMonotonic=3000000")
    material["observed_at"] = NOW + timedelta(seconds=3)
    material["observed_monotonic_ns"] = 4_000_000_000
    from rquant.notifier_operator import ManualServiceReadReceipt

    after = ManualServiceReadReceipt.from_material(type(before.material).model_validate(material))
    return ManualServiceRunWindow(**raw, before_receipt=before, after_receipt=after)


def test_manual_window_uses_exact_call_job_invocation_without_zero_timer(tmp_path: Path) -> None:
    from rquant.task_unit_control import ManualServiceRunWindow, bind_manual_service_run

    window = manual_window(tmp_path)
    run = bind_manual_service_run(window)
    assert run.request_id == REQUEST and run.status == "succeeded"
    assert run.duration_ns == 2_000_000_000
    assert len(window.model_dump_json().encode()) <= 16 * 1024
    assert "timer_before_usec" not in window.model_dump()
    for update in ({"caller_pid": 201}, {"previous_invocation_id": "a" * 32},
                   {"calls": window.calls * 2}, {"events": window.events[:-1]},
                   {"boot_id": "b" * 8 + "-1234-1234-1234-123456789abc"}):
        with pytest.raises(ValueError):
            bind_manual_service_run(ManualServiceRunWindow.model_validate(window.model_dump() | update))


def manual_owned(tmp_path: Path, outbox: object, journal: object, *, command_id: str = REQUEST,
                 confirmation_id: str | None = None, leaf: object | None = None,
                 enqueue: bool = True, at: object = NOW) -> object:
    from rquant.task_control_commands import ManualServiceAcceptedContext, OwnedRequestUnitRun, RequestUnitRun

    leaf = receipt(tmp_path) if leaf is None else leaf
    context = ManualServiceAcceptedContext(
        host_name="rquant-test", boot_id=BOOT, manifest_digest=_manifest().digest,
        policy_digest=leaf.install_digest, source_payload_hash="c" * 64, generation_id="generation-a",
        observed_at=NOW, mode="readonly", runtime_state=leaf.runtime_state,
        manual_identity_sha256=leaf.start_identity_sha256,
    )
    original = RequestUnitRun(command_id=command_id, requested_at=at, generation_id="generation-a",
                              unit=UNIT, confirmation_id=confirmation_id)
    checked = OwnedRequestUnitRun.model_validate(original.model_dump() | {
        "owner_id": "alice", "accepted_at": at, "metadata_identity": journal.identity(),
        "original_request_hash": original.request_hash, "context": context,
    })
    if enqueue:
        outbox.enqueue_trusted_task_control(checked)
    return checked


def test_manual_readonly_still_requires_confirm_and_unknown_survives_boot(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal

    leaf_root = tmp_path / "leaf"
    leaf_root.mkdir()
    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    command = manual_owned(leaf_root, outbox, journal)
    journal.prepare_run(command, now=NOW)
    with pytest.raises(ValueError, match="confirm"):
        journal.start_intent(command, now=NOW, monotonic_ns=800_000_000)
    assert journal.run_effect(command).intent_at is None


def confirm_manual(outbox: object, journal: object, base: object, *, prepare_id: str,
                   at: object = NOW) -> object:
    from rquant.task_control_commands import OwnedPrepareUnitRun, OwnedRequestUnitRun, PrepareUnitRun, RequestUnitRun, TaskUnitRunDraft

    draft = TaskUnitRunDraft.model_validate(base.original().model_dump(exclude={"kind", "confirmation_id"}))
    public = PrepareUnitRun(command_id=prepare_id, requested_at=at, generation_id=base.generation_id, run=draft)
    prepare = OwnedPrepareUnitRun.model_validate(public.model_dump() | {
        "owner_id": base.owner_id, "accepted_at": at, "metadata_identity": journal.identity(),
        "original_request_hash": public.request_hash, "context": base.context,
    })
    outbox.enqueue_trusted_task_control(prepare)
    challenge = journal.prepare_confirmation(prepare)
    original = RequestUnitRun.model_validate(base.original().model_dump() | {"confirmation_id": challenge.confirmation_id})
    command = OwnedRequestUnitRun.model_validate(base.model_dump() | original.model_dump() | {
        "original_request_hash": original.request_hash,
    })
    outbox.enqueue_trusted_task_control(command)
    return command


def test_manual_confirmed_intent_persistent_600_seconds_and_unknown_exclusion(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_unit_control import ManualServiceRunWindow, bind_manual_service_run

    leaf_dir = tmp_path / "leaf"
    leaf_dir.mkdir()
    leaf = receipt(leaf_dir)
    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    base = manual_owned(leaf_dir, outbox, journal, leaf=leaf, enqueue=False)
    command = confirm_manual(outbox, journal, base, prepare_id="848d1d8b-3b70-4258-a971-58b0beaa8f4d")
    journal.prepare_run(command, now=NOW)
    intent = journal.start_intent(command, now=NOW, monotonic_ns=800_000_000)
    assert journal.start_intent(command, now=NOW + timedelta(seconds=1), monotonic_ns=900_000_000) == intent
    assert TaskControlJournal(outbox).manual_next_allowed_at() == NOW + timedelta(seconds=600)
    window = manual_window(leaf_dir, before=leaf)
    window = ManualServiceRunWindow.model_validate(window.model_dump() | {"request_hash": command.original_request_hash})
    run = bind_manual_service_run(window)
    journal.record_run(command, now=NOW + timedelta(seconds=3), stage="completed", job_path=run.job_witness.job_path, run=run, window=window)
    base2 = manual_owned(leaf_dir, outbox, journal, leaf=leaf, enqueue=False,
        command_id="948d1d8b-3b70-4258-a971-58b0beaa8f4d", at=NOW + timedelta(seconds=599))
    command2 = confirm_manual(outbox, journal, base2, prepare_id="a48d1d8b-3b70-4258-a971-58b0beaa8f4d", at=NOW + timedelta(seconds=599))
    reopened = TaskControlJournal(outbox)
    reopened.prepare_run(command2, now=NOW + timedelta(seconds=599))
    with pytest.raises(ValueError, match="600|cooldown"):
        reopened.start_intent(command2, now=NOW + timedelta(seconds=599), monotonic_ns=600_000_000_000)
    assert reopened.run_effect(command2).intent_at is None
    reopened.start_intent(command2, now=NOW + timedelta(seconds=600), monotonic_ns=601_000_000_000)
    reopened.record_run(command2, now=NOW + timedelta(seconds=601), stage="unknown", reason="original_reply_missing")
    with pytest.raises(ValueError, match="unresolved"):
        TaskControlJournal(outbox).assert_unit_available(UNIT, "22345678-1234-1234-1234-123456789abc")


def test_manual_monitor_rejects_restart_reload_and_extra_call_without_timer(tmp_path: Path) -> None:
    import json
    from types import SimpleNamespace
    from rquant.task_unit_control import SystemdUnitRunExecutor

    for member, data, kind in (("RestartUnit", [UNIT, "fail"], "method_call"),
                               ("Reload", [], "method_call"), ("Reloading", [True], "signal")):
        raw = {"type": kind, "member": member, "interface": "org.freedesktop.systemd1.Manager",
               "sender": ":1.1", "monotonic_usec": 900_000, "cookie": 1,
               "credentials": {"pid": 200}, "payload": {"type": "ss", "data": data}}
        with pytest.raises(ValueError):
            SystemdUnitRunExecutor._messages(SimpleNamespace(manual=True, lines=[json.dumps(raw).encode()]))


def test_notifier_mode_uses_two_original_uuids_one_confirmation_and_persistent_cas(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_control_commands import (
        NotifierModeDraft, PrepareNotifierDeliveryMode, SetNotifierDeliveryMode,
        NotifierControlAcceptedContext, OwnedPrepareNotifierDeliveryMode, OwnedSetNotifierDeliveryMode,
    )
    from tests.unit.test_task_control_admission import PREPARE, REQUEST

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    context = NotifierControlAcceptedContext(host_name="rquant-test", boot_id=BOOT,
        manifest_digest=_manifest().digest, installation_sha256="a" * 64,
        notifier_manifest_sha256="b" * 64, producer_manifest_sha256="c" * 64,
        source_payload_hash="d" * 64, generation_id="generation-a", observed_at=NOW,
        initial_mode="shadow", builtin_definitions=())
    draft = NotifierModeDraft(command_id=REQUEST, requested_at=NOW, generation_id="generation-a", mode="live", expected_revision=0)
    prepare = PrepareNotifierDeliveryMode(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=draft)
    common = dict(owner_id="admin", accepted_at=NOW, metadata_identity=journal.identity(), context=context)
    accepted = OwnedPrepareNotifierDeliveryMode(**prepare.model_dump(), **common, original_request_hash=prepare.request_hash)
    outbox.enqueue_trusted_task_control(accepted)
    confirmation = journal.prepare_notifier_mode(accepted)
    assert confirmation.run.command_id == REQUEST and confirmation.expires_at == NOW + timedelta(minutes=5)
    request = SetNotifierDeliveryMode(**draft.model_dump(), confirmation_id=confirmation.confirmation_id)
    command = OwnedSetNotifierDeliveryMode(**request.model_dump(), **common, original_request_hash=request.request_hash)
    outbox.enqueue_trusted_task_control(command)
    state = journal.set_notifier_mode(command, now=NOW)
    assert state.revision == 1 and state.mode == "live" and state.command_id == REQUEST
    reopened = TaskControlJournal(outbox)
    assert reopened.set_notifier_mode(command, now=NOW + timedelta(seconds=1)) == state
    assert reopened.notifier_mode(context) == state
    # The original confirmation belongs to this exact submitted UUID and actor.
    other = SetNotifierDeliveryMode(**(draft.model_dump() | {"command_id": "79c41b9b-c8fb-40b3-81bd-1f4cfe5f20b5"}), confirmation_id=confirmation.confirmation_id)
    forged = OwnedSetNotifierDeliveryMode(**other.model_dump(), **common, original_request_hash=other.request_hash)
    outbox.enqueue_trusted_task_control(forged)
    with pytest.raises(ValueError, match="confirmation|revision|consumed"):
        reopened.set_notifier_mode(forged, now=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError):
        PrepareNotifierDeliveryMode(**(prepare.model_dump() | {"command_id": REQUEST}))
    with pytest.raises(ValueError):
        SetNotifierDeliveryMode(**(request.model_dump() | {"expected_revision": True}))


def test_builtin_toggle_keeps_exact_owner_installation_and_original_journal(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_control_commands import SetMonitorBuiltinEnabled, OwnedSetMonitorBuiltinEnabled, NotifierControlAcceptedContext
    from rquant.monitor_builtin_contracts import MonitorBuiltinDefinition
    from rquant.monitor_builtin_runtime import builtin_source_contract_sha256
    from rquant.delivery_contracts import DeliveryChannel
    from tests.unit.test_task_control_admission import REQUEST

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    definition = MonitorBuiltinDefinition(owner_id="alice", builtin_id="pool_attack", channels=(DeliveryChannel.PUSHDEER,),
        code_contract_sha256=builtin_source_contract_sha256())
    context = NotifierControlAcceptedContext(host_name="rquant-test", boot_id=BOOT, manifest_digest=_manifest().digest,
        installation_sha256="a" * 64, notifier_manifest_sha256="b" * 64, producer_manifest_sha256="c" * 64,
        source_payload_hash="d" * 64, generation_id="generation-a", observed_at=NOW, initial_mode="shadow", builtin_definitions=(definition,))
    request = SetMonitorBuiltinEnabled(command_id=REQUEST, requested_at=NOW, generation_id="generation-a",
        builtin_id="pool_attack", expected_revision=0, enabled=True)
    command = OwnedSetMonitorBuiltinEnabled(**request.model_dump(), owner_id="alice", accepted_at=NOW,
        metadata_identity=journal.identity(), original_request_hash=request.request_hash, context=context)
    outbox.enqueue_trusted_task_control(command)
    state = journal.set_builtin_enabled(command, now=NOW)
    assert state.owner_id == "alice" and state.definition.enabled and state.definition.version == 2
    assert state.revision == 1 and state.definition.builtin_id == "pool_attack"
    assert TaskControlJournal(outbox).set_builtin_enabled(command, now=NOW) == state
    with pytest.raises(ValueError):
        SetMonitorBuiltinEnabled(**(request.model_dump() | {"owner_id": "bob"}))
    with pytest.raises(ValueError, match="owner|installed"):
        OwnedSetMonitorBuiltinEnabled(**request.model_dump(), owner_id="bob", accepted_at=NOW,
            metadata_identity=journal.identity(), original_request_hash=request.request_hash, context=context)


def test_mode_backend_without_installed_binding_never_accepts_public_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.test_task_control_admission import control_service, PREPARE, REQUEST
    from rquant.task_control_commands import NotifierModeDraft, PrepareNotifierDeliveryMode

    service, backend, _, _, starts = control_service(tmp_path, monkeypatch)
    assert backend.monitor_control is None
    draft = NotifierModeDraft(command_id=REQUEST, requested_at=NOW, generation_id="generation-a", mode="live", expected_revision=0)
    request = PrepareNotifierDeliveryMode(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=draft)
    with pytest.raises((PermissionError, ValueError), match="role|installed|unavailable"):
        service._submit_trusted_task_control(request, authenticated_actor_id="admin", verified_metadata_identity=backend.journal.identity())
    assert service.outbox.receipt(PREPARE) is None and starts == []


def test_monitor_control_read_binding_is_complete_and_keeps_physical_original(tmp_path: Path) -> None:
    from rquant.notifier_operator import MonitorControlReadSettings, read_monitor_control_state
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal

    journal = TaskControlJournal(PageControlOutbox(tmp_path / "control.db"))
    identity = journal.identity()
    raw = dict(outbox_path=journal.path, outbox_device=identity.device, outbox_inode=identity.inode,
        outbox_instance_id=identity.instance_id, notifier_service_id="notifier", condition_service_id="conditions")
    settings = MonitorControlReadSettings(**raw)
    with pytest.raises(ValueError, match="generation|profile|installed|current"):
        read_monitor_control_state(settings, runtime_root=tmp_path, now=NOW)
    for changed in ({"outbox_device": True}, {"outbox_inode": 0}, {"outbox_path": Path("relative")},
                    {"outbox_instance_id": "unknown"}, {"condition_service_id": "notifier"}):
        with pytest.raises(ValueError):
            MonitorControlReadSettings(**(raw | changed))


def test_monitor_control_uses_original_verified_composed_peer_outside_profile(tmp_path: Path) -> None:
    from rquant.notifier_operator import inspect_monitor_control_installation, read_monitor_control_state
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_generation_lineage import load_runtime_generation_tree
    from tests.support.monitor_completion_fixture import build_original_monitor_control_fixture

    fixture = build_original_monitor_control_fixture(tmp_path / "runtime")
    tree = load_runtime_generation_tree(fixture.runtime_root)
    assert fixture.producer not in fixture.profile.manifests
    with pytest.raises(ValueError):
        tree.lineage(fixture.producer.service_id)
    actual = inspect_monitor_control_installation(fixture.controls, runtime_root=fixture.runtime_root)
    assert actual.profile_sha256 == canonical_sha256(fixture.profile)
    assert actual.producer_manifest_sha256 == fixture.producer.manifest_fingerprint
    assert actual.producer_membership == "verified_condition_peer"
    snapshot = read_monitor_control_state(fixture.controls, runtime_root=fixture.runtime_root, now=fixture.now)
    assert snapshot.mode.mode == "shadow" and snapshot.mode.revision == 0
    assert tuple(item.definition for item in snapshot.builtins) == fixture.definitions
    fixture.producer_path.write_bytes(fixture.producer_path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        inspect_monitor_control_installation(fixture.controls, runtime_root=fixture.runtime_root)


def test_live_mode_requires_current_loaded_recipient_capability(tmp_path: Path) -> None:
    import rquant.notifier_operator as operator
    from rquant.delivery_contracts import NotificationMergeBinding, NotificationRuntimeWindow
    from tests.support.monitor_completion_fixture import build_original_monitor_control_fixture

    fixture = build_original_monitor_control_fixture(tmp_path / "runtime")
    installed = operator.inspect_monitor_control_installation(fixture.controls, runtime_root=fixture.runtime_root)
    mode = operator.read_monitor_control_state(
        fixture.controls, runtime_root=fixture.runtime_root, now=fixture.now
    ).mode
    assert hasattr(operator, "require_monitor_live_capability"), "live mode lacks its original loader capability fence"
    binding = NotificationMergeBinding(owner_id="alice", source_id="signal-route-spool/v1",
        installation_sha256=fixture.notifier.manifest_fingerprint,
        role_revision=operator.notifier_mode_role_revision(
            fixture.notifier.service_spec.identity, mode
        ),
        generation_id=fixture.generation_id, mode="shadow")
    window = NotificationRuntimeWindow(state="unavailable", reason="no_observed_window", observed_at=fixture.now,
        binding=binding, source_receipt_sha256="e" * 64, covered_from=None, covered_through=fixture.now,
        complete=False, history_count=0, returned_history_count=0, truncated=False,
        applied_revision=mode.revision, applied_command_id=mode.command_id,
        monitor_installation_sha256=installed.installation_sha256)
    for candidate in (None, window):
        with pytest.raises(ValueError, match="capability"):
            operator.require_monitor_live_capability(
                candidate, installed=installed, current_mode=mode, now=fixture.now
            )
    actual = window.model_copy(update={"capability_observed_at": fixture.now, "available_targets": installed.delivery_targets})
    operator.require_monitor_live_capability(
        actual, installed=installed, current_mode=mode, now=fixture.now
    )
    for changed in ({"available_targets": ()}, {"capability_observed_at": fixture.now - timedelta(seconds=120)},
                    {"binding": binding.model_copy(update={"generation_id": "other"})}):
        with pytest.raises(ValueError, match="capability"):
            operator.require_monitor_live_capability(
                actual.model_copy(update=changed), installed=installed,
                current_mode=mode, now=fixture.now
            )


def test_original_factory_capability_binds_actual_role_application_and_installation(
    tmp_path: Path,
) -> None:
    from rquant.condition_alert_runtime_projection import validate_monitor_runtime_projections
    from rquant.runtime_serving_authority import ServingSourceAuthorityReader
    from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
    from tests.support.monitor_completion_fixture import COMMIT, build_original_monitor_pipeline

    from rquant.notifier_operator import read_monitor_control_state, require_monitor_live_capability

    pipeline = build_original_monitor_pipeline(tmp_path / "original-runtime")
    try:
        pipeline.tick(timedelta(0))
        captured = ServingSourceAuthorityReader(
            root=pipeline.authority_root, expected_producer_commit=COMMIT,
            expected_dataset_id="signals", expected_payload_kind="signal_delivery",
        )(pipeline.now)
        payload = SignalDeliveryReadPayload.model_validate(captured.payload)
        window = validate_monitor_runtime_projections(
            {row.table_name: row for row in payload.projections}
        ).notification_window
        actual = read_monitor_control_state(
            pipeline.control.controls, runtime_root=pipeline.control.runtime_root, now=pipeline.now
        )
        require_monitor_live_capability(
            window, installed=actual.installation, current_mode=actual.mode, now=pipeline.now
        )
        changed_mode = type(actual.mode).model_validate({
            **actual.mode.model_dump(), "revision": 1, "command_id": "later-original-command",
            "actor_id": "alice", "accepted_at": pipeline.now,
        })
        for changed in (
            {"current_mode": changed_mode},
            {"installed": actual.installation.model_copy(
                update={"notifier_role_revision": "e" * 64}
            )},
            {"installed": actual.installation.model_copy(update={"installation_sha256": "e" * 64})},
            {"installed": actual.installation.model_copy(
                update={"runtime_generation_id": "e" * 64}
            )},
            {"window": window.model_copy(update={"applied_command_id": "other"})},
            {"now": pipeline.now + timedelta(seconds=120)},
        ):
            args = {"window": window, "installed": actual.installation,
                    "current_mode": actual.mode, "now": pipeline.now} | changed
            with pytest.raises(ValueError, match="capability"):
                require_monitor_live_capability(**args)
    finally:
        pipeline.close()


def test_original_owner_application_receipt_is_distinct_from_saved_desired_state(
    tmp_path: Path,
) -> None:
    from types import SimpleNamespace

    from rquant.condition_alert_runtime_projection import validate_monitor_runtime_projections
    from rquant.page_control import PageControlStatus
    from rquant.runtime_serving_authority import ServingSourceAuthorityReader
    from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
    from rquant.task_control_commands import (
        NotifierControlAcceptedContext,
        NotifierModeDraft,
        OwnedPrepareNotifierDeliveryMode,
        OwnedSetMonitorBuiltinEnabled,
        OwnedSetNotifierDeliveryMode,
        PrepareNotifierDeliveryMode,
        SetMonitorBuiltinEnabled,
        SetNotifierDeliveryMode,
    )
    from tests.support.monitor_completion_fixture import COMMIT, build_original_monitor_pipeline
    from tests.unit.test_task_control_admission import PREPARE, REQUEST

    from rquant.notifier_operator import read_monitor_control_state
    from rquant.web.models.monitor import MonitorRuntimeData
    from rquant.web.routes.monitor import _runtime_data
    from rquant.web.routes.task_center_controls import _public

    pipeline = build_original_monitor_pipeline(tmp_path / "actual-owner")
    try:
        reader = ServingSourceAuthorityReader(
            root=pipeline.authority_root, expected_producer_commit=COMMIT,
            expected_dataset_id="signals", expected_payload_kind="signal_delivery",
        )

        def runtime() -> MonitorRuntimeData:
            payload = SignalDeliveryReadPayload.model_validate(reader(pipeline.now).payload)
            facts = validate_monitor_runtime_projections(
                {row.table_name: row for row in payload.projections}
            )
            return _runtime_data(facts, viewer="alice", now=pipeline.now)

        pipeline.tick(timedelta(0))
        before = runtime()
        installed = read_monitor_control_state(
            pipeline.control.controls, runtime_root=pipeline.control.runtime_root, now=pipeline.now
        ).installation
        context = NotifierControlAcceptedContext(
            host_name="rquant-test", boot_id=BOOT, manifest_digest=_manifest().digest,
            installation_sha256=installed.installation_sha256,
            notifier_manifest_sha256=installed.notifier_manifest_sha256,
            producer_manifest_sha256=installed.producer_manifest_sha256,
            source_payload_hash="d" * 64, generation_id=pipeline.control.generation_id,
            observed_at=pipeline.now, initial_mode=installed.initial_mode,
            builtin_definitions=tuple(
                row for row in installed.builtin_definitions if row.owner_id == "alice"
            ),
        )
        journal = pipeline.control.journal
        common = dict(owner_id="alice", accepted_at=pipeline.now,
                      metadata_identity=journal.identity(), context=context)
        draft = NotifierModeDraft(
            command_id=REQUEST, requested_at=pipeline.now,
            generation_id=pipeline.control.generation_id,
            mode="shadow", expected_revision=0,
        )
        prepare = PrepareNotifierDeliveryMode(
            command_id=PREPARE, requested_at=pipeline.now,
            generation_id=pipeline.control.generation_id, run=draft,
        )
        accepted = OwnedPrepareNotifierDeliveryMode(
            **prepare.model_dump(), **common, original_request_hash=prepare.request_hash,
        )
        journal.outbox.enqueue_trusted_task_control(accepted)
        confirmation = journal.prepare_notifier_mode(accepted)
        request = SetNotifierDeliveryMode(
            **draft.model_dump(), confirmation_id=confirmation.confirmation_id
        )
        command = OwnedSetNotifierDeliveryMode(
            **request.model_dump(), **common, original_request_hash=request.request_hash,
        )
        journal.outbox.enqueue_trusted_task_control(command)
        desired_mode = journal.set_notifier_mode(command, now=pipeline.now)
        toggle = SetMonitorBuiltinEnabled(
            command_id="79c41b9b-c8fb-40b3-81bd-1f4cfe5f20b5", requested_at=pipeline.now,
            generation_id=pipeline.control.generation_id, builtin_id="pool_attack",
            expected_revision=0, enabled=False,
        )
        owned_toggle = OwnedSetMonitorBuiltinEnabled(
            **toggle.model_dump(), **common, original_request_hash=toggle.request_hash,
        )
        journal.outbox.enqueue_trusted_task_control(owned_toggle)
        desired_builtin = journal.set_builtin_enabled(owned_toggle, now=pipeline.now)
        receipt = SimpleNamespace(status=PageControlStatus.SUCCEEDED)
        for body, state in ((request, desired_mode), (toggle, desired_builtin)):
            saved = _public(body, SimpleNamespace(receipt=receipt, monitor_state=state))
            assert saved.status == "submitted" and saved.desired_revision == 1
            assert saved.desired_installation_sha256 == installed.installation_sha256
        still_old = runtime()
        assert still_old == before
        assert still_old.applied_revision == 0 and still_old.applied_command_id is None
        old_builtin = next(row for row in still_old.builtins if row.builtin_id == "pool_attack")
        assert old_builtin.enabled and old_builtin.applied_revision == 0
        pipeline.tick(timedelta(seconds=5))
        applied = runtime()
        assert (
            applied.applied_revision, applied.applied_command_id,
            applied.monitor_installation_sha256
        ) == (
            1, request.command_id, installed.installation_sha256,
        )
        head = next(row for row in applied.builtins if row.builtin_id == "pool_attack")
        assert not head.enabled and head.state == "disabled"
        assert (
            head.applied_revision, head.applied_command_id, head.monitor_installation_sha256
        ) == (
            1, toggle.command_id, installed.installation_sha256,
        )
        payload = SignalDeliveryReadPayload.model_validate(reader(pipeline.now).payload)
        facts = validate_monitor_runtime_projections(
            {row.table_name: row for row in payload.projections}
        )
        bob = _runtime_data(facts, viewer="bob", now=pipeline.now)
        other = next(row for row in bob.builtins if row.builtin_id == "pool_attack")
        assert other.enabled and other.applied_revision == 0 and other.applied_command_id is None
    finally:
        pipeline.close()
