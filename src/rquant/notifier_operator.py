"""Exact signed manual notification service and same-read installation facts."""

from __future__ import annotations

import base64
import hashlib
import os
import re
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal
from collections.abc import Callable

from pydantic import ConfigDict, Field, StrictBool, StrictInt, StrictStr, TypeAdapter, field_validator, model_validator

from rquant.backtest.contracts import Sha256
from rquant.ed25519_verify import verify_ed25519_signature
from rquant.ops_status import OpsInstallManifest
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads
from rquant.monitor_builtin_contracts import BuiltinId, MonitorBuiltinDefinition
from rquant.delivery_contracts import DeliveryTarget, NotificationRuntimeWindow

if TYPE_CHECKING:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.task_unit_control import TaskUnitRuntimeState
    from rquant.task_control import TaskControlPageControlBackend

MANUAL_SERVICE = "rquant-notify-test.service"
MANUAL_TIMER = "rquant-notify-test.timer"
_DOMAIN = b"rquant.notifier-manual-service-install/v1\x00"
_BOOT = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")]
_COUNTER = Annotated[StrictInt, Field(ge=0, le=2**63 - 1)]
SERVICE_PROPERTIES = (
    "Id", "Names", "LoadState", "ActiveState", "MainPID", "InvocationID", "FragmentPath",
    "DropInPaths", "TriggeredBy", "Type", "User", "WorkingDirectory", "Restart",
    "EnvironmentFiles", "ExecStart", "Job", "Result", "ExecMainStatus",
    "ExecMainStartTimestamp", "ExecMainExitTimestamp", "ExecMainStartTimestampMonotonic",
    "ExecMainExitTimestampMonotonic",
)
TIMER_PROPERTIES = ("Id", "LoadState", "FragmentPath")


class NotifierModeState(RuntimeContractModel):
    contract: Literal["rquant.notifier-mode-state/v1"] = "rquant.notifier-mode-state/v1"
    installation_sha256: Sha256
    revision: StrictInt = Field(ge=0, le=2**63 - 1)
    mode: Literal["shadow", "live"]
    command_id: StrictStr | None = None
    actor_id: StrictStr | None = None
    accepted_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def original_command(self) -> NotifierModeState:
        if (self.revision == 0) != (self.command_id is None) or len({self.command_id is None, self.actor_id is None, self.accepted_at is None}) != 1:
            raise ValueError("notifier mode revision requires its complete original command")
        return self


class MonitorBuiltinControlState(RuntimeContractModel):
    contract: Literal["rquant.monitor-builtin-control/v1"] = "rquant.monitor-builtin-control/v1"
    installation_sha256: Sha256
    owner_id: StrictStr = Field(min_length=1, max_length=128)
    builtin_id: BuiltinId
    revision: StrictInt = Field(ge=0, le=2**63 - 1)
    definition: MonitorBuiltinDefinition
    command_id: StrictStr | None = None
    accepted_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def exact_definition(self) -> MonitorBuiltinControlState:
        if (self.owner_id, self.builtin_id) != (self.definition.owner_id, self.definition.builtin_id):
            raise ValueError("builtin control differs from its original installed owner")
        if (self.revision == 0) != (self.command_id is None) or (self.command_id is None) != (self.accepted_at is None):
            raise ValueError("builtin control revision requires its original accepted command")
        return self


class MonitorControlReadSettings(RuntimeContractModel):
    outbox_path: Path
    outbox_device: StrictInt = Field(ge=0, le=2**63 - 1)
    outbox_inode: StrictInt = Field(ge=1, le=2**63 - 1)
    outbox_instance_id: StrictStr = Field(pattern=r"^[0-9a-f]{32}$")
    notifier_service_id: StrictStr = Field(min_length=1, max_length=256)
    condition_service_id: StrictStr = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def original_path(self) -> MonitorControlReadSettings:
        path = self.outbox_path
        if (not path.is_absolute() or path != Path(os.path.abspath(path))
                or self.notifier_service_id == self.condition_service_id):
            raise ValueError("monitor control needs the exact original path and separate installed roles")
        return self


class MonitorControlInstallation(RuntimeContractModel):
    installation_sha256: Sha256
    profile_sha256: Sha256
    notifier_manifest_sha256: Sha256
    notifier_role_revision: Sha256
    producer_manifest_sha256: Sha256
    runtime_generation_id: Sha256
    producer_membership: Literal["verified_condition_peer"] = "verified_condition_peer"
    producer_activation_sha256: Sha256
    producer_source_sha256: Sha256
    initial_mode: Literal["shadow", "live"]
    builtin_definitions: tuple[MonitorBuiltinDefinition, ...] = Field(max_length=128)
    delivery_targets: tuple[DeliveryTarget, ...] = Field(max_length=64)


def inspect_monitor_control_installation(settings: MonitorControlReadSettings, *, runtime_root: Path,
    borrowed_condition: ConditionAlertRuntimeStore | None = None,
    source_connection: sqlite3.Connection | None = None,
) -> MonitorControlInstallation:
    from contextlib import nullcontext
    from rquant.condition_alert_runtime import verify_condition_runtime_namespace
    from rquant.condition_alert_runtime_contracts import require_condition_alert_activation
    from rquant.monitor_builtin_runtime import MonitorBuiltinOwnerSettings
    from rquant.price_alert_runtime_contracts import _activation_bytes
    from rquant.runtime_builder_condition_alert import ConditionAlertPeerSettings, open_condition_role_peer
    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile
    from rquant.runtime_generation_lineage import load_runtime_generation_tree
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, load_runtime_service_manifest

    profile = load_current_runtime_deployment_profile(runtime_root)
    tree = load_runtime_generation_tree(runtime_root)
    notifier = tree.lineage(settings.notifier_service_id).current.manifest
    if (profile.page_control is None or profile.page_control.outbox_path != settings.outbox_path
            or notifier not in profile.manifests
            or notifier.service_kind is not RuntimeServiceKind.NOTIFIER
            or notifier.settings.get("monitor_control") != settings.model_dump(mode="json")
            or notifier.settings.get("paused", False)):
        raise ValueError("monitor controls differ from the original installed profile/roles")
    peer_settings = ConditionAlertPeerSettings.model_validate_json(canonical_json_bytes(notifier.model_dump(mode="json")["settings"].get("condition_alert_peer")))
    raw = _activation_bytes(peer_settings.producer_manifest_path, runtime_root)
    producer = load_runtime_service_manifest(peer_settings.producer_manifest_path, expected_commit=notifier.producer_commit)
    if (producer.service_id != settings.condition_service_id
            or producer.service_kind is not RuntimeServiceKind.PRICE_ALERT_RUNTIME
            or producer.settings.get("monitor_control") != settings.model_dump(mode="json")
            or producer.settings.get("paused", False)):
        raise ValueError("monitor producer differs from the original notifier's composed peer")
    owner = MonitorBuiltinOwnerSettings.model_validate_json(canonical_json_bytes(producer.model_dump(mode="json")["settings"].get("monitor_builtin")))
    if not owner.enabled:
        raise ValueError("monitor original owner is unavailable or paused")
    activation, peer, policy = open_condition_role_peer(notifier, peer_settings, runtime_root=runtime_root,
        borrowed_condition=borrowed_condition, source_connection=source_connection)
    try:
        require_condition_alert_activation(activation, "delivery")
        binding = require_condition_alert_activation(peer.activation, "evaluation")
        require_condition_alert_activation(peer.activation, "event_write")
        raw_sha = hashlib.sha256(raw).hexdigest()
        if binding.producer_manifest_sha256 != raw_sha:
            raise ValueError("monitor producer activation differs from the exact original manifest read")
        with (peer.ledger._connection() if source_connection is None else nullcontext(source_connection)) as connection:
            condition_source = verify_condition_runtime_namespace(connection)
            price_source = peer.ledger._source(connection)
            physical = peer.ledger._file_identity()
            condition_body = condition_source.model_dump(mode="json", exclude={"first_sequence", "high_watermark"})
            price_body = price_source.model_dump(mode="json", exclude={"first_sequence", "high_watermark"})
            if any(condition_body[key] != getattr(binding, key) for key in condition_body):
                raise ValueError("monitor source receipt differs from the actual producer activation")
            source_sha = canonical_sha256({"contract": "rquant.monitor-composed-source-receipt/v1",
                "condition": condition_body, "price": price_body, "recipient_policy": policy.sha256,
                "path": str(peer.ledger.path), "device": physical[0], "inode": physical[1]})
        if (_activation_bytes(peer_settings.producer_manifest_path, runtime_root) != raw
                or load_current_runtime_deployment_profile(runtime_root) != profile
                or load_runtime_generation_tree(runtime_root).current_generation_id != tree.current_generation_id):
            raise ValueError("monitor original producer/profile changed during verification")
    finally:
        if borrowed_condition is None:
            peer.ledger.close()
    profile_sha = canonical_sha256(profile)
    installation = canonical_sha256({"contract": "rquant.monitor-control-installation/v1", "profile": profile_sha,
        "runtime_generation": tree.current_generation_id, "notifier": notifier.manifest_fingerprint,
        "producer_membership": "verified_condition_peer", "producer": producer.manifest_fingerprint,
        "producer_activation": raw_sha, "producer_source": source_sha})
    return MonitorControlInstallation(installation_sha256=installation, profile_sha256=profile_sha,
        notifier_manifest_sha256=notifier.manifest_fingerprint, notifier_role_revision=notifier.service_spec.identity,
        producer_manifest_sha256=producer.manifest_fingerprint,
        runtime_generation_id=tree.current_generation_id, producer_activation_sha256=raw_sha,
        producer_source_sha256=source_sha,
        initial_mode="shadow" if notifier.settings.get("suppress_delivery", False) else "live",
        builtin_definitions=owner.definitions,
        delivery_targets=tuple(sorted({target for definition in owner.definitions
            for target in policy.targets_for(definition.owner_id) if target.channel in definition.channels},
            key=lambda row: (row.channel.value, row.recipient_id))))


def notifier_mode_role_revision(role: str, mode: NotifierModeState) -> str:
    return canonical_sha256({"role": role, "revision": mode.revision, "command": mode.command_id})


def require_monitor_live_capability(
    window: NotificationRuntimeWindow | None, *, installed: MonitorControlInstallation,
    current_mode: NotifierModeState, now: datetime,
) -> None:
    if (type(window) is not NotificationRuntimeWindow or window.binding is None
            or type(current_mode) is not NotifierModeState
            or current_mode.installation_sha256 != installed.installation_sha256
            or (window.monitor_installation_sha256,
                window.applied_revision, window.applied_command_id)
            != (installed.installation_sha256, current_mode.revision, current_mode.command_id)
            or window.binding.mode != current_mode.mode
            or window.source_receipt_sha256 is None or window.capability_observed_at is None
            or not 0 <= (now - window.capability_observed_at).total_seconds() < 120
            or not 0 <= (now - window.observed_at).total_seconds() < 120
            or (window.binding.installation_sha256, window.binding.role_revision, window.binding.generation_id)
            != (installed.notifier_manifest_sha256,
                notifier_mode_role_revision(installed.notifier_role_revision, current_mode),
                installed.runtime_generation_id)
            or not installed.delivery_targets or window.available_targets is None
            or not set(installed.delivery_targets) <= set(window.available_targets)):
        raise ValueError("monitor live mode requires its current original loaded capability")


class MonitorControlSnapshot(RuntimeContractModel):
    installation: MonitorControlInstallation
    observed_at: AwareUtcDatetime
    mode: NotifierModeState
    builtins: tuple[MonitorBuiltinControlState, ...] = Field(max_length=128)


def read_monitor_control_state(settings: MonitorControlReadSettings, *, runtime_root: Path, now: datetime,
    borrowed_condition: ConditionAlertRuntimeStore | None = None,
    source_connection: sqlite3.Connection | None = None,
) -> MonitorControlSnapshot:
    from rquant.task_control import validate_task_journal_command
    from rquant.task_control_commands import OwnedTaskControl, OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled, TaskControlIdentity

    installed = inspect_monitor_control_installation(settings, runtime_root=runtime_root,
        borrowed_condition=borrowed_condition, source_connection=source_connection)
    physical = settings.outbox_path.lstat()
    if (not stat.S_ISREG(physical.st_mode) or physical.st_nlink != 1 or physical.st_mode & 0o077
            or (physical.st_dev, physical.st_ino) != (settings.outbox_device, settings.outbox_inode)):
        raise ValueError("monitor controls lost their original private journal identity")
    identity = TaskControlIdentity(path=str(settings.outbox_path), device=physical.st_dev, inode=physical.st_ino,
        instance_id=settings.outbox_instance_id)
    connection = sqlite3.connect(settings.outbox_path.as_uri() + "?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        authority = connection.execute("SELECT instance_id FROM page_control_task_authority WHERE singleton=1").fetchone()
        if authority is None or authority[0] != settings.outbox_instance_id:
            raise ValueError("monitor controls differ from the original journal instance")
        rows = [] if not verify_monitor_control_metadata(connection) else connection.execute(
            "SELECT owner_id,control_id,CASE WHEN length(body)<=32768 THEN body END FROM page_control_monitor_state ORDER BY owner_id,control_id").fetchall()
        mode = NotifierModeState(installation_sha256=installed.installation_sha256, revision=0, mode=installed.initial_mode)
        definitions = {(item.owner_id, item.builtin_id): item for item in installed.builtin_definitions}
        builtins = {key: MonitorBuiltinControlState(installation_sha256=installed.installation_sha256,
            owner_id=key[0], builtin_id=key[1], revision=0, definition=value) for key, value in definitions.items()}
        for owner, control, body in rows:
            if body is None:
                raise ValueError("monitor state exceeds its original task metadata cell")
            state = (NotifierModeState if (owner, control) == ("", "mode") else MonitorBuiltinControlState).model_validate_json(bytes(body))
            original = connection.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=32768 THEN payload_json END FROM page_control_command WHERE command_id=?", (state.command_id,)).fetchone()
            if original is None or original[0] is None or state.installation_sha256 != installed.installation_sha256:
                raise ValueError("monitor state has no original current-installation command")
            command = TypeAdapter(OwnedTaskControl).validate_json(original[0])
            validate_task_journal_command(connection, command, identity)
            if type(command) not in {OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled}:
                raise ValueError("monitor state uses another protected command")
            context = command.context
            operation = connection.execute("SELECT body FROM page_control_monitor_operation WHERE command_id=?", (state.command_id,)).fetchone()
            if (state.revision != command.expected_revision + 1 or state.accepted_at != command.accepted_at
                    or state.accepted_at > now or operation is None or bytes(operation[0]) != bytes(body)
                    or (context.installation_sha256, context.notifier_manifest_sha256, context.producer_manifest_sha256)
                    != (installed.installation_sha256, installed.notifier_manifest_sha256, installed.producer_manifest_sha256)):
                raise ValueError("monitor applied state differs from its exact original operation/source")
            if type(command) is OwnedSetNotifierDeliveryMode and (owner, control) == ("", "mode"):
                if state.mode != command.mode or state.actor_id != command.owner_id:
                    raise ValueError("monitor mode differs from the original administrator command")
                mode = state
            elif type(command) is OwnedSetMonitorBuiltinEnabled and (owner, control) in builtins:
                expected = definitions[(owner, control)]
                desired = type(expected).model_validate(expected.model_dump() | {"enabled": command.enabled, "version": expected.version + state.revision})
                if command.owner_id != owner or command.builtin_id != control or state.definition != desired:
                    raise ValueError("builtin desired state differs from the original owner definition")
                builtins[(owner, control)] = state
            else:
                raise ValueError("monitor state uses another owner or protected command")
        snapshot = MonitorControlSnapshot(installation=installed, observed_at=now, mode=mode,
            builtins=tuple(builtins[key] for key in sorted(builtins)))
        connection.commit()
    finally:
        connection.close()
    after = settings.outbox_path.lstat()
    if (after.st_dev, after.st_ino) != (physical.st_dev, physical.st_ino) or inspect_monitor_control_installation(
        settings, runtime_root=runtime_root, borrowed_condition=borrowed_condition,
        source_connection=source_connection) != installed:
        raise ValueError("monitor original installation changed during the same read")
    return snapshot


MONITOR_CONTROL_SQL = (
    "CREATE TABLE page_control_monitor_state(owner_id TEXT NOT NULL,control_id TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(owner_id,control_id))",
    "CREATE TABLE page_control_monitor_operation(command_id TEXT PRIMARY KEY REFERENCES page_control_command(command_id),kind TEXT NOT NULL,body BLOB NOT NULL)",
)


def verify_monitor_control_metadata(connection: object, *, install: bool = False) -> bool:
    actual = {row[0] for row in connection.execute("SELECT sql FROM sqlite_master WHERE name LIKE 'page_control_monitor_%' AND sql IS NOT NULL")}
    if not actual:
        if not install:
            return False
        for sql in MONITOR_CONTROL_SQL:
            connection.execute(sql)
        actual = set(MONITOR_CONTROL_SQL)
    if actual != set(MONITOR_CONTROL_SQL):
        raise ValueError("original monitor control metadata is incomplete or changed")
    if (connection.execute("SELECT COUNT(*) FROM page_control_monitor_state").fetchone()[0] > 129
            or connection.execute("SELECT COUNT(*) FROM page_control_monitor_operation").fetchone()[0] > 4096):
        raise ValueError("monitor control exceeds the installed owner domain")
    return True


class NotifierManualServiceInstall(RuntimeContractModel):
    contract: Literal["rquant.notifier-manual-service-install/v1"] = "rquant.notifier-manual-service-install/v1"
    unit: Literal["rquant-notify-test.service"] = MANUAL_SERVICE
    host_name: StrictStr = Field(min_length=1, max_length=253)
    manifest_digest: Sha256
    profile_sha256: Sha256
    runtime_commit: StrictStr = Field(pattern=r"^[0-9a-f]{40}$")
    installation_id: StrictStr = Field(min_length=1, max_length=128)
    revision: StrictInt = Field(ge=1, le=2**63 - 1)
    fragment_path: StrictStr = Field(min_length=1, max_length=512)
    fragment_sha256: Sha256
    user: StrictStr = Field(pattern=r"^[a-z_][a-z0-9_-]{0,31}$")
    working_directory: StrictStr = Field(min_length=1, max_length=512)
    cli_path: StrictStr = Field(min_length=1, max_length=512)
    environment_file: StrictStr = Field(min_length=1, max_length=512)
    enabled: StrictBool = False
    mode: Literal["readonly"] = "readonly"
    requires_confirmation: Literal[True] = True

    @field_validator("fragment_path", "working_directory", "cli_path", "environment_file")
    @classmethod
    def exact_path(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or ".." in path.parts or str(path) != value or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value):
            raise ValueError("manual install requires canonical absolute paths without expansions")
        return value

    @model_validator(mode="after")
    def exact_definition(self) -> NotifierManualServiceInstall:
        if Path(self.fragment_path).name != self.unit or Path(self.cli_path).name != "rquant":
            raise ValueError("manual install requires the exact service and original rquant CLI")
        if len(self.model_dump_json().encode()) > 4096:
            raise ValueError("manual install exceeds 4 KiB")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self)

    def signing_bytes(self) -> bytes:
        return _DOMAIN + canonical_json_bytes(self.model_dump(mode="json"))


class SignedNotifierManualServiceInstall(RuntimeContractModel):
    install: NotifierManualServiceInstall
    signature: StrictStr = Field(min_length=88, max_length=88)

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class ManualInstallSourceReceipt(RuntimeContractModel):
    raw_sha256: Sha256
    device: _COUNTER
    inode: StrictInt = Field(ge=1, le=2**63 - 1)
    size: StrictInt = Field(ge=1, le=4096)
    mtime_ns: _COUNTER
    owner_uid: _COUNTER


@dataclass(frozen=True, slots=True)
class NotifierManualServiceBinding:
    install_path: Path
    public_key_pem: bytes
    profile_sha256: str
    runtime_commit: str
    expected_uid: int = 0


def _read_install_file(path: Path, *, expected_uid: int) -> tuple[bytes, ManualInstallSourceReceipt]:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != expected_uid
                or before.st_mode & 0o022 or not 0 < before.st_size <= 4096):
            raise ValueError("manual install requires a bounded single-link trusted regular file")
        raw = os.read(descriptor, 4097)
        after = os.fstat(descriptor)
        identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
                                  value.st_ctime_ns, value.st_uid, value.st_mode, value.st_nlink)
        if identity(before) != identity(after) or len(raw) != before.st_size:
            raise ValueError("manual installation file changed during read")
    finally:
        os.close(descriptor)
    return raw, ManualInstallSourceReceipt(raw_sha256=hashlib.sha256(raw).hexdigest(), device=before.st_dev,
        inode=before.st_ino, size=before.st_size, mtime_ns=before.st_mtime_ns, owner_uid=before.st_uid)


def _verify_install(raw: bytes, *, public_key_pem: bytes, manifest: OpsInstallManifest,
                    expected_profile_sha256: str, expected_runtime_commit: str) -> NotifierManualServiceInstall:
    signed = SignedNotifierManualServiceInstall.model_validate(strict_canonical_json_loads(raw))
    if len(raw) > 4096 or signed.canonical_bytes() != raw:
        raise ValueError("manual install requires bounded canonical JSON")
    try:
        signature = base64.b64decode(signed.signature, validate=True)
    except ValueError as exc:
        raise ValueError("manual install signature is invalid") from exc
    if not verify_ed25519_signature(public_key_pem=public_key_pem, message=signed.install.signing_bytes(), signature=signature):
        raise ValueError("manual install signature is invalid")
    install = signed.install
    if (install.host_name, install.manifest_digest, install.profile_sha256, install.runtime_commit) != (
            manifest.host_name, manifest.digest, expected_profile_sha256, expected_runtime_commit):
        raise ValueError("manual install differs from the actual signed Ops/profile/commit")
    return install


def load_notifier_manual_install(path: Path, *, public_key_pem: bytes, manifest: OpsInstallManifest,
                                 expected_profile_sha256: str, expected_runtime_commit: str,
                                 expected_uid: int = 0) -> tuple[NotifierManualServiceInstall, ManualInstallSourceReceipt]:
    raw, source = _read_install_file(Path(path), expected_uid=expected_uid)
    install = _verify_install(raw, public_key_pem=public_key_pem, manifest=manifest,
        expected_profile_sha256=expected_profile_sha256, expected_runtime_commit=expected_runtime_commit)
    return install, source


def _properties(body: str, names: tuple[str, ...]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in body.splitlines():
        name, separator, value = line.partition("=")
        if not separator or name not in names or name in fields:
            raise ValueError("manual manager read requires exact complete properties")
        fields[name] = value
    if set(fields) != set(names):
        raise ValueError("manual manager read has missing properties")
    return fields


def _fragment(body: str, install: NotifierManualServiceInstall) -> None:
    values: dict[tuple[str, str], str] = {}
    section = ""
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line in ("[Unit]", "[Service]"):
            section = line[1:-1]
            continue
        key, separator, value = line.partition("=")
        if not separator or (section, key) in values or not section:
            raise ValueError("manual service fragment is not the exact simple definition")
        values[section, key] = value
    expected = {
        ("Service", "Type"): "oneshot", ("Service", "User"): install.user,
        ("Service", "WorkingDirectory"): install.working_directory,
        ("Service", "ExecStart"): install.cli_path + " notify-test",
        ("Service", "Restart"): "no", ("Service", "EnvironmentFile"): install.environment_file,
    }
    if {key: value for key, value in values.items() if key[0] == "Service"} != expected:
        raise ValueError("manual service fragment differs from the signed exact definition")
    if any(section_name != "Service" and (section_name, key) != ("Unit", "Description") for section_name, key in values):
        raise ValueError("manual service has an automatic trigger or unverified directive")


class ManualServiceReadMaterial(RuntimeContractModel):
    model_config = ConfigDict(str_strip_whitespace=False)
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_before: _BOOT
    boot_after: _BOOT
    ops_manifest_digest: Sha256
    install_source: ManualInstallSourceReceipt
    install_json: StrictStr = Field(max_length=4096)
    fragment_source: ManualInstallSourceReceipt
    fragment_text: StrictStr = Field(max_length=4096)
    service_show: StrictStr = Field(max_length=4096)
    timer_show: StrictStr = Field(max_length=1024)
    list_jobs: StrictStr = Field(max_length=16 * 1024)
    observed_at: AwareUtcDatetime
    observed_monotonic_ns: _COUNTER

    @model_validator(mode="after")
    def complete_material(self) -> ManualServiceReadMaterial:
        if self.boot_before != self.boot_after:
            raise ValueError("manual read crossed a boot")
        if len(self.model_dump_json().encode()) > 16 * 1024:
            raise ValueError("manual same-read material exceeds original 16 KiB")
        for text, source in ((self.install_json, self.install_source), (self.fragment_text, self.fragment_source)):
            raw = text.encode("utf-8")
            if len(raw) != source.size or hashlib.sha256(raw).hexdigest() != source.raw_sha256:
                raise ValueError("manual material differs from its actual file receipt")
        install = self.install
        if (install.host_name, install.manifest_digest, install.fragment_sha256) != (
                self.host_name, self.ops_manifest_digest, self.fragment_source.raw_sha256):
            raise ValueError("manual material differs from signed source identity")
        _fragment(self.fragment_text, install)
        _manual_facts(self)
        return self

    @property
    def install(self) -> NotifierManualServiceInstall:
        signed = SignedNotifierManualServiceInstall.model_validate(strict_canonical_json_loads(self.install_json))
        if signed.canonical_bytes() != self.install_json.encode():
            raise ValueError("manual material requires complete canonical signed source")
        return signed.install

    @property
    def digest(self) -> str:
        return canonical_sha256(self)


def _manual_facts(material: ManualServiceReadMaterial) -> dict[str, object]:
    install = material.install
    service = _properties(material.service_show, SERVICE_PROPERTIES)
    timer = _properties(material.timer_show, TIMER_PROPERTIES)
    fixed = {"Id": MANUAL_SERVICE, "Names": MANUAL_SERVICE, "LoadState": "loaded", "DropInPaths": "",
             "TriggeredBy": "", "Type": "oneshot", "User": install.user,
             "WorkingDirectory": install.working_directory, "Restart": "no", "FragmentPath": install.fragment_path,
             "EnvironmentFiles": install.environment_file + " (ignore_errors=no)"}
    if any(service[key] != value for key, value in fixed.items()):
        raise ValueError("loaded manual service differs from its complete signed definition")
    if timer != {"Id": MANUAL_TIMER, "LoadState": "not-found", "FragmentPath": ""}:
        raise ValueError("manual service lacks actual same-name timer absence")
    executable = re.fullmatch(
        r"\{ path=([^ ;]+) ; argv\[\]=([^;]+) ; ignore_errors=no ; start_time=\[[^\]\n]*\] ; "
        r"stop_time=\[[^\]\n]*\] ; pid=[0-9]+ ; code=\([^;]*\) ; status=[^;}]+ \}", service["ExecStart"])
    if executable is None or executable.group(1) != install.cli_path or executable.group(2).strip() != install.cli_path + " notify-test":
        raise ValueError("loaded ExecStart differs from the original fixed CLI")
    if not re.fullmatch(r"[0-9]{1,10}", service["MainPID"]):
        raise ValueError("manual service MainPID is unavailable")
    jobs = strict_json_loads(material.list_jobs)
    if not isinstance(jobs, dict) or jobs.get("type") != "a(usssoo)" or not isinstance(jobs.get("data"), list) or len(jobs["data"]) != 1 or not isinstance(jobs["data"][0], list) or len(jobs["data"][0]) > 128:
        raise ValueError("manual ListJobs requires original complete bounded rows")
    starts: list[str] = []
    for row in jobs["data"][0]:
        if not isinstance(row, list) or len(row) != 6 or type(row[0]) is not int or not 1 <= row[0] <= 2**32 - 1 or any(not isinstance(value, str) for value in row[1:]):
            raise ValueError("manual ListJobs contains invalid raw facts")
        if row[1] in (MANUAL_SERVICE, MANUAL_TIMER):
            if row[4] != f"/org/freedesktop/systemd1/job/{row[0]}":
                raise ValueError("manual ListJobs identity differs")
            starts.append(row[4])
    if service["Job"] != "0" and not starts:
        raise ValueError("manual service pending job is missing from same-read ListJobs")
    return {"host_name": material.host_name, "boot_id": material.boot_after, "unit": MANUAL_SERVICE,
            "load_state": service["LoadState"], "active_state": service["ActiveState"],
            "invocation_id": service["InvocationID"] or None, "start_jobs": tuple(starts),
            "observed_at": material.observed_at, "main_pid": int(service["MainPID"])}


class ManualTimerAbsenceProof(RuntimeContractModel):
    unit: Literal["rquant-notify-test.timer"] = MANUAL_TIMER
    load_state: Literal["not-found"] = "not-found"
    fragment_path: Literal[""] = ""
    service_triggered_by: tuple[()] = ()
    material_sha256: Sha256


class ManualServiceReadReceipt(RuntimeContractModel):
    contract: Literal["rquant.manual-service-read/v1"] = "rquant.manual-service-read/v1"
    material: ManualServiceReadMaterial
    material_sha256: Sha256
    install_digest: Sha256
    main_pid: StrictInt = Field(ge=0, le=2**31 - 1)
    timer_absence: ManualTimerAbsenceProof

    @model_validator(mode="after")
    def original_values(self) -> ManualServiceReadReceipt:
        facts = _manual_facts(self.material)
        if (self.material_sha256, self.install_digest, self.main_pid, self.timer_absence.material_sha256) != (
                self.material.digest, self.material.install.digest, facts["main_pid"], self.material.digest):
            raise ValueError("manual receipt differs from the complete original values")
        self.runtime_state
        return self

    @classmethod
    def from_material(cls, material: ManualServiceReadMaterial) -> ManualServiceReadReceipt:
        return cls(material=material, material_sha256=material.digest, install_digest=material.install.digest,
                   main_pid=_manual_facts(material)["main_pid"],
                   timer_absence=ManualTimerAbsenceProof(material_sha256=material.digest))

    @property
    def runtime_state(self) -> TaskUnitRuntimeState:
        from rquant.task_unit_control import TaskUnitRuntimeState

        facts = _manual_facts(self.material)
        return TaskUnitRuntimeState.model_validate({key: value for key, value in facts.items() if key != "main_pid"})

    def verify_source(self, *, public_key_pem: bytes, manifest: OpsInstallManifest,
                      profile_sha256: str, runtime_commit: str) -> None:
        _verify_install(self.material.install_json.encode(), public_key_pem=public_key_pem, manifest=manifest,
                        expected_profile_sha256=profile_sha256, expected_runtime_commit=runtime_commit)

    @property
    def start_identity_sha256(self) -> str:
        return canonical_sha256({
            "installation": self.material.model_dump(exclude={"service_show", "list_jobs", "observed_at", "observed_monotonic_ns"}),
            "runtime_state": self.runtime_state.model_dump(exclude={"observed_at"}), "main_pid": self.main_pid,
        })


def capture_manual_service_receipt(*, install_path: Path, public_key_pem: bytes, manifest: OpsInstallManifest,
                                   expected_profile_sha256: str, expected_runtime_commit: str, expected_host: str,
                                   boot_before: str, boot_after: str, service_show: bytes, timer_show: bytes,
                                   list_jobs: bytes, observed_at: datetime, observed_monotonic_ns: int,
                                   expected_uid: int = 0) -> ManualServiceReadReceipt:
    raw, source = _read_install_file(install_path, expected_uid=expected_uid)
    install = _verify_install(raw, public_key_pem=public_key_pem, manifest=manifest,
                             expected_profile_sha256=expected_profile_sha256, expected_runtime_commit=expected_runtime_commit)
    if install.host_name != expected_host:
        raise ValueError("manual leaf actual host differs from the signed installation")
    fragment, fragment_source = _read_install_file(Path(install.fragment_path), expected_uid=expected_uid)
    material = ManualServiceReadMaterial(host_name=expected_host, boot_before=boot_before, boot_after=boot_after,
        ops_manifest_digest=manifest.digest, install_source=source, install_json=raw.decode("utf-8"),
        fragment_source=fragment_source, fragment_text=fragment.decode("utf-8"),
        service_show=service_show.decode("utf-8"), timer_show=timer_show.decode("utf-8"),
        list_jobs=list_jobs.decode("utf-8"), observed_at=observed_at, observed_monotonic_ns=observed_monotonic_ns)
    return ManualServiceReadReceipt.from_material(material)


def guard_manual_service_run(receipt: ManualServiceReadReceipt, *, now: datetime) -> NotifierManualServiceInstall:
    receipt = ManualServiceReadReceipt.model_validate(receipt)
    age = (now - receipt.material.observed_at).total_seconds()
    if not 0 <= age < 120:
        raise ValueError("manual service source is future or stale")
    if not receipt.material.install.enabled:
        raise ValueError("manual service running is disabled")
    state = receipt.runtime_state
    if state.active_state not in ("inactive", "failed") or receipt.main_pid != 0 or state.start_jobs:
        raise ValueError("manual service is running or has a pending job")
    return receipt.material.install


class NotifierOperatorConfig(RuntimeContractModel):
    contract: Literal["rquant.notifier-operator-config/v1"] = "rquant.notifier-operator-config/v1"
    runtime_root: Path
    serving_root: Path
    manifest_path: Path
    policy_path: Path
    manifest_public_key_pem: StrictStr = Field(min_length=1, max_length=512)
    policy_public_key_pem: StrictStr = Field(min_length=1, max_length=512)
    manual_install_path: Path | None = None
    manual_public_key_pem: StrictStr | None = Field(default=None, min_length=1, max_length=512)
    operators: tuple[StrictStr, ...] = Field(max_length=16)
    monitor_control: MonitorControlReadSettings | None = None
    notifier_admins: tuple[StrictStr, ...] = Field(default=(), max_length=16)
    socket_path: Path | None = None
    trusted_web_uid: StrictInt | None = Field(default=None, ge=0)
    shared_gid: StrictInt | None = Field(default=None, ge=0)
    enabled: StrictBool = False

    @field_validator("runtime_root", "serving_root", "manifest_path", "policy_path", "manual_install_path", "socket_path")
    @classmethod
    def normalized_path(cls, value: Path | None) -> Path | None:
        if value is not None:
            NotifierManualServiceInstall.exact_path(str(value))
        return value

    @model_validator(mode="after")
    def complete_config(self) -> NotifierOperatorConfig:
        if (self.manual_install_path is None) != (self.manual_public_key_pem is None):
            raise ValueError("manual operator needs both the exact install and trusted public key")
        if len(set(self.operators)) != len(self.operators) or any(not 1 <= len(actor) <= 128 for actor in self.operators):
            raise ValueError("manual operators require a finite exact actor set")
        if (len(set(self.notifier_admins)) != len(self.notifier_admins) or any(not 1 <= len(actor) <= 128 for actor in self.notifier_admins)
                or self.monitor_control is not None and self.manual_install_path is None):
            raise ValueError("monitor operator requires finite admins and its signed original installation")
        peer = (self.socket_path, self.trusted_web_uid, self.shared_gid)
        if any(value is not None for value in peer) and not all(value is not None for value in peer):
            raise ValueError("notifier task peer requires its exact socket/Web UID/GID together")
        if len(self.model_dump_json().encode()) > 4096:
            raise ValueError("operator configuration exceeds 4 KiB")
        return self


def read_notifier_operator_config(path: Path, *, expected_uid: int = 0) -> NotifierOperatorConfig:
    raw, _receipt = _read_install_file(Path(path), expected_uid=expected_uid)
    document = strict_canonical_json_loads(raw)
    config = NotifierOperatorConfig.model_validate(document)
    complete = config.model_dump(mode="json")
    for key in ("monitor_control", "notifier_admins", "socket_path", "trusted_web_uid", "shared_gid"):
        if key not in document:
            complete.pop(key)
    if canonical_json_bytes(complete) != raw:
        raise ValueError("operator config requires complete canonical JSON")
    return config


def build_notifier_operator_backend(config: NotifierOperatorConfig, *, outbox_path: Path,
                                    clock: Callable[[], datetime] | None = None,
                                    expected_commit: str | None = None,
                                    expected_uid: int = 0) -> TaskControlPageControlBackend:
    from rquant.page_control import PageControlOutbox
    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile
    from rquant.task_control import TaskControlJournal, TaskControlPageControlBackend
    from rquant.task_control_admission import TaskCenterServingSource
    from rquant.task_unit_control import SystemdUnitRunExecutor

    config = NotifierOperatorConfig.model_validate(config)
    profile = load_current_runtime_deployment_profile(config.runtime_root)
    if (expected_commit is not None and profile.producer_commit != expected_commit
            or profile.page_control is None or profile.page_control.outbox_path != outbox_path):
        raise ValueError("operator bootstrap differs from the actual installed profile/PageControl")
    binding = None
    if config.manual_install_path is not None:
        binding = NotifierManualServiceBinding(install_path=config.manual_install_path,
            public_key_pem=config.manual_public_key_pem.encode(), profile_sha256=canonical_sha256(profile),
            runtime_commit=profile.producer_commit, expected_uid=expected_uid)
    executor = SystemdUnitRunExecutor(manifest_path=config.manifest_path, policy_path=config.policy_path,
        manifest_public_key_pem=config.manifest_public_key_pem.encode(),
        policy_public_key_pem=config.policy_public_key_pem.encode(), manual_binding=binding,
        **({} if clock is None else {"clock": clock}))
    executor.configuration()
    if binding is not None:
        executor.manual_configuration()
    journal = TaskControlJournal(PageControlOutbox(outbox_path))
    if config.monitor_control is not None:
        installed = inspect_monitor_control_installation(config.monitor_control, runtime_root=config.runtime_root)
        if installed.profile_sha256 != canonical_sha256(profile):
            raise ValueError("monitor operator profile changed during bootstrap")
        read_monitor_control_state(config.monitor_control, runtime_root=config.runtime_root, now=executor.clock())
    return TaskControlPageControlBackend(journal=journal, source=TaskCenterServingSource(config.serving_root,
        clock=executor.clock), executor=executor, operators=config.operators, enabled=config.enabled, clock=clock,
        monitor_control=config.monitor_control, monitor_runtime_root=None if config.monitor_control is None else config.runtime_root,
        notifier_admins=config.notifier_admins)
