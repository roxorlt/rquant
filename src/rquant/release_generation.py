"""Crash-persistent authority for one deployable checkout generation.

This module intentionally uses only the Python standard library so startup wrappers can
load it by physical file path before importing the :mod:`rquant` package.
"""

from __future__ import annotations

import fcntl
import fnmatch
import hashlib
import importlib.util
import io
import json
import math
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tarfile
import time
import tomllib
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import asdict, dataclass, fields, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _load_strict_json() -> tuple[
    type[ValueError],
    Callable[[str | bytes | bytearray], Any],
    Callable[..., Any],
    Callable[..., bytes],
]:
    path = Path(__file__).resolve().parents[2] / "scripts" / "strict_json.py"
    spec = importlib.util.spec_from_file_location("_rquant_strict_json", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("strict JSON authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return (
        module.StrictJsonError,
        module.strict_json_loads,
        module.strict_canonical_json_loads,
        module.canonical_json_bytes,
    )


(
    StrictJsonError,
    strict_json_loads,
    strict_canonical_json_loads,
    canonical_json_bytes,
) = _load_strict_json()


def _load_contained_runner() -> tuple[
    Callable[..., subprocess.CompletedProcess[Any]],
    type[RuntimeError],
]:
    path = Path(__file__).resolve().with_name("contained_subprocess.py")
    spec = importlib.util.spec_from_file_location("_rquant_contained_subprocess", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("contained subprocess authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run_contained, module.ContainedProcessError


run_contained, ContainedProcessError = _load_contained_runner()

MARKER_SCHEMA_VERSION = 1
INTENT_SCHEMA_VERSION = 1
COMMIT_SCHEMA_VERSION = 1
ENVIRONMENT_SCHEMA_VERSION = 1
RELEASE_CODE_DIRECTORY = "release"
LAB_HANDOFF_SCHEMA_VERSION = 1
MAX_MARKER_BYTES = 32 * 1024
MAX_INTENT_BYTES = 128 * 1024
MAX_ENVIRONMENT_MANIFEST_BYTES = 64 * 1024 * 1024
DEFAULT_GENERATION_GC_GRACE_SECONDS = 7 * 24 * 60 * 60
DEFAULT_GENERATION_MINIMUM_FREE_BYTES = 2 * 1024 * 1024 * 1024
GENERATION_ID_PATTERN = re.compile(r"[0-9a-f]{64}")
BUILDING_GENERATION_PATTERN = re.compile(r"\.([0-9a-f]{64})\.[0-9a-f]{16}\.building")
_VENV_RELATIVE_SYMLINKS = frozenset({"bin/python3", "lib64"})
ALL_LONG_RUNNING_SERVICES = (
    "rquant-canvas.service",
    "rquant-dashboard.service",
    "rquant-daily-receipt-signer.socket",
    "rquant-page-control.service",
    "rquant-monitor.service",
    "rquant-nl-screen.service",
    "rquant-panorama-auth.service",
    "rquant-panorama.service",
    "rquant-surge-watch.service",
)
LAB_LAUNCHD_HANDOFF_LABELS = (
    "com.roxor.rquant-lab-scheduler",
    "com.roxor.rquant-lab-worker",
    "com.roxor.rquant-lab-finalizer",
)
LINUX_RELEASE_PROFILE = "linux-production"
MACOS_LAB_RELEASE_PROFILE = "macos-lab"
RELEASE_PROFILES = (LINUX_RELEASE_PROFILE, MACOS_LAB_RELEASE_PROFILE)
SERVICE_TIMERS: dict[str, tuple[str, ...]] = {
    "rquant-monitor.service": (
        "rquant-monitor.timer",
        "rquant-monitor-watchdog.timer",
    ),
    "rquant-surge-watch.service": ("rquant-surge-watch.timer",),
}
PRIVILEGED_PREFIXES = (
    "deploy/launchd/",
    "deploy/root-runtime/",
    "deploy/systemd/",
    "deploy/nginx/",
    "deploy/frp/",
    "deploy/sudoers/",
)
NO_RESTART_SOURCE_PATTERNS = (
    "src/rquant/__init__.py",
    "src/rquant/cli.py",
    "src/rquant/preflight.py",
    "src/rquant/ops/*",
)
SHARED_RUNTIME_PATTERNS = (
    "src/rquant/config.py",
    "src/rquant/storage/*",
)
SERVICE_PATTERNS: dict[str, tuple[str, ...]] = {
    "rquant-canvas.service": (
        "src/rquant/dashboard/nl_canvas.py",
        "src/rquant/llm/*",
        "src/rquant/screen/*",
        "src/rquant/presets.py",
    ),
    "rquant-dashboard.service": (
        "src/rquant/dashboard/app.py",
        "src/rquant/health.py",
        "src/rquant/risk/*",
        "src/rquant/state.py",
    ),
    "rquant-page-control.service": (
        "src/rquant/page_control.py",
        "src/rquant/page_control_service.py",
        "src/rquant/canvas_publication_receipt.py",
        "src/rquant/serving_page_projection_source.py",
    ),
    "rquant-daily-receipt-signer.socket": (
        "deploy/root-runtime/daily_receipt_authority.py",
        "deploy/libexec/rquant-daily-receipt-signer",
        "scripts/install-runtime-credential-infra.sh",
        "deploy/systemd/rquant-daily-receipt-signer.socket",
        "deploy/systemd/rquant-daily-receipt-signer.service",
    ),
    "rquant-monitor.service": (
        "src/rquant/monitor.py",
        "src/rquant/notify/*",
        "src/rquant/risk/*",
        "src/rquant/state.py",
        "src/rquant/presets.py",
        "src/rquant/screen/*",
        "src/rquant/indicator.py",
    ),
    "rquant-nl-screen.service": (
        "src/rquant/dashboard/nl_screen.py",
        "src/rquant/llm/*",
        "src/rquant/screen/*",
        "src/rquant/presets.py",
        "src/rquant/state.py",
    ),
    "rquant-panorama-auth.service": ("src/rquant/panorama_auth.py",),
    "rquant-panorama.service": (
        "src/rquant/dashboard/market_panorama.py",
        "src/rquant/panorama_*",
    ),
    "rquant-surge-watch.service": (
        "src/rquant/surge_watch.py",
        "src/rquant/intraday_*",
        "src/rquant/notify/*",
    ),
}
_DEPLOYMENT_STAGE_PATTERN = re.compile(
    r"(?:planned|initializing|recovery_started|timers_stopped|services_transitioning|"
    r"services_ready|post_restart_preflight_ready|timers_restored|marker_published|"
    r"awaiting_readiness|completed|"
    r"handoff_rebound|(?:deploy|resume|rollback)_(?:checkout|dependencies|preflight)_ready)"
)
_TARGET_REF_PATTERN = re.compile(r"(?:v\d+\.\d+\.\d+|[0-9a-f]{40})")


class ReleaseGenerationError(RuntimeError):
    """The release generation cannot be trusted."""


class ReleaseGenerationRecordMissingError(ReleaseGenerationError):
    """A private release record is absent from its bound authority directory."""


@dataclass(frozen=True)
class PathIdentity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int

    @classmethod
    def capture(cls, value: os.stat_result) -> PathIdentity:
        return cls(value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink)

    @classmethod
    def from_payload(cls, payload: object, *, label: str) -> PathIdentity:
        fields = {"device", "inode", "mode", "owner", "links"}
        if (
            type(payload) is not dict
            or set(payload) != fields
            or any(type(payload[field]) is not int for field in fields)
        ):
            raise ReleaseGenerationError(f"{label} identity is malformed")
        return cls(
            device=payload["device"],
            inode=payload["inode"],
            mode=payload["mode"],
            owner=payload["owner"],
            links=payload["links"],
        )


@dataclass(frozen=True)
class LabInstallationIdentity:
    path: str
    sha256: str
    device: int
    inode: int

    @classmethod
    def from_payload(cls, payload: object) -> LabInstallationIdentity:
        expected_fields = {"path", "sha256", "device", "inode"}
        if (
            type(payload) is not dict
            or set(payload) != expected_fields
            or type(payload["path"]) is not str
            or type(payload["sha256"]) is not str
            or type(payload["device"]) is not int
            or type(payload["inode"]) is not int
            or re.fullmatch(r"[0-9a-f]{64}", payload["sha256"]) is None
        ):
            raise ReleaseGenerationError("Lab installation identity is malformed")
        return cls(**payload)


@dataclass(frozen=True)
class LabRuntimePreparedIdentity:
    runtime_authority_id: str
    runtime_root: str
    runtime_device: int
    runtime_inode: int

    @classmethod
    def from_payload(cls, payload: object) -> LabRuntimePreparedIdentity:
        expected = {
            "runtime_authority_id",
            "runtime_root",
            "runtime_device",
            "runtime_inode",
        }
        if (
            type(payload) is not dict
            or set(payload) != expected
            or type(payload["runtime_authority_id"]) is not str
            or re.fullmatch(r"[0-9a-f]{32}", payload["runtime_authority_id"]) is None
            or type(payload["runtime_root"]) is not str
            or type(payload["runtime_device"]) is not int
            or type(payload["runtime_inode"]) is not int
        ):
            raise ReleaseGenerationError("registered Lab prepared authority is invalid")
        return cls(**payload)


def _lab_installation_plists(
    payload: object,
    *,
    expected_names: tuple[str, ...],
    label: str,
) -> tuple[tuple[str, LabInstallationIdentity], ...]:
    if type(payload) is not dict or set(payload) != set(expected_names):
        raise ReleaseGenerationError(f"{label} plist bindings are invalid")
    return tuple(
        (name, LabInstallationIdentity.from_payload(payload[name])) for name in expected_names
    )


@dataclass(frozen=True)
class LabRegisteredInstallationAuthority:
    checkout_root: str
    labels: tuple[str, ...]
    plists: tuple[tuple[str, LabInstallationIdentity], ...]
    runtime_root: str
    readiness_root: str
    registered_by_commit: str
    prepared_authority: LabRuntimePreparedIdentity
    installed_at: str
    environment_generation_id: str
    handoff_operation_id: str

    @classmethod
    def from_payload(cls, payload: object) -> LabRegisteredInstallationAuthority:
        base_fields = {
            "schema_version",
            "checkout_root",
            "labels",
            "plists",
            "runtime_root",
            "readiness_root",
            "registered_by_commit",
            "prepared_authority",
            "installed_at",
        }
        installed_fields = {"environment_generation_id", "handoff_operation_id"}
        if type(payload) is not dict or set(payload) not in (
            base_fields,
            base_fields | installed_fields,
        ):
            raise ReleaseGenerationError("registered Lab installation authority is invalid")
        labels = payload.get("labels")
        installed = installed_fields <= set(payload)
        try:
            installed_at = datetime.fromisoformat(payload["installed_at"])
        except (TypeError, ValueError):
            installed_at = None
        if (
            type(payload["schema_version"]) is not int
            or payload["schema_version"] != 2
            or type(payload["checkout_root"]) is not str
            or type(labels) is not list
            or tuple(labels) != LAB_LAUNCHD_HANDOFF_LABELS
            or any(type(value) is not str for value in labels)
            or type(payload["runtime_root"]) is not str
            or type(payload["readiness_root"]) is not str
            or type(payload["registered_by_commit"]) is not str
            or re.fullmatch(r"[0-9a-f]{40}", payload["registered_by_commit"]) is None
            or type(payload["installed_at"]) is not str
            or installed_at is None
            or installed_at.tzinfo is None
            or installed_at.utcoffset() is None
        ):
            raise ReleaseGenerationError("registered Lab installation authority is invalid")
        generation_id = payload.get("environment_generation_id", "")
        handoff_id = payload.get("handoff_operation_id", "")
        if installed and (
            type(generation_id) is not str
            or GENERATION_ID_PATTERN.fullmatch(generation_id) is None
            or type(handoff_id) is not str
            or (handoff_id != "" and re.fullmatch(r"[0-9a-f]{32}", handoff_id) is None)
        ):
            raise ReleaseGenerationError("registered Lab installation authority is invalid")
        return cls(
            checkout_root=payload["checkout_root"],
            labels=tuple(labels),
            plists=_lab_installation_plists(
                payload["plists"],
                expected_names=LAB_LAUNCHD_HANDOFF_LABELS,
                label="registered Lab installation",
            ),
            runtime_root=payload["runtime_root"],
            readiness_root=payload["readiness_root"],
            registered_by_commit=payload["registered_by_commit"],
            prepared_authority=LabRuntimePreparedIdentity.from_payload(
                payload["prepared_authority"]
            ),
            installed_at=payload["installed_at"],
            environment_generation_id=generation_id,
            handoff_operation_id=handoff_id,
        )

    @property
    def is_installed(self) -> bool:
        return bool(self.environment_generation_id)

    def plist_map(self) -> dict[str, LabInstallationIdentity]:
        return dict(self.plists)


@dataclass(frozen=True)
class LabLocalInstallationAuthority:
    code_sha: str
    environment_generation_id: str
    handoff_operation_id: str
    launch_agents_dir: str
    plists: tuple[tuple[str, LabInstallationIdentity], ...]

    @classmethod
    def from_payload(cls, payload: object) -> LabLocalInstallationAuthority:
        expected = {
            "schema_version",
            "code_sha",
            "environment_generation_id",
            "handoff_operation_id",
            "launch_agents_dir",
            "plists",
        }
        if (
            type(payload) is not dict
            or set(payload) != expected
            or type(payload["schema_version"]) is not int
            or payload["schema_version"] != 2
            or type(payload["code_sha"]) is not str
            or re.fullmatch(r"[0-9a-f]{40}", payload["code_sha"]) is None
            or type(payload["environment_generation_id"]) is not str
            or GENERATION_ID_PATTERN.fullmatch(payload["environment_generation_id"]) is None
            or type(payload["handoff_operation_id"]) is not str
            or (
                payload["handoff_operation_id"] != ""
                and re.fullmatch(r"[0-9a-f]{32}", payload["handoff_operation_id"]) is None
            )
            or type(payload["launch_agents_dir"]) is not str
        ):
            raise ReleaseGenerationError("local Lab installation authority is invalid")
        names = tuple(f"{label}.plist" for label in LAB_LAUNCHD_HANDOFF_LABELS)
        return cls(
            code_sha=payload["code_sha"],
            environment_generation_id=payload["environment_generation_id"],
            handoff_operation_id=payload["handoff_operation_id"],
            launch_agents_dir=payload["launch_agents_dir"],
            plists=_lab_installation_plists(
                payload["plists"],
                expected_names=names,
                label="local Lab installation",
            ),
        )

    def plist_map(self) -> dict[str, LabInstallationIdentity]:
        return dict(self.plists)


@dataclass(frozen=True)
class LabHandoffRecord:
    schema_version: int
    operation_id: str
    checkout_root: str
    labels: tuple[str, ...]
    loaded_labels: tuple[str, ...]
    stopped_labels: tuple[str, ...]
    restarted_labels: tuple[str, ...]
    target_ref: str
    target_sha: str
    action: str
    release_profile: str
    lifecycle_mode: str
    installation_identity: LabInstallationIdentity
    supersedes_operation_id: str
    stage: str
    updated_at: str
    generation_operation_id: str = ""
    environment_generation_id: str = ""
    code_sha: str = ""

    @classmethod
    def from_payload(
        cls,
        payload: object,
        *,
        completed: bool,
    ) -> LabHandoffRecord:
        base_fields = {
            "schema_version",
            "operation_id",
            "checkout_root",
            "labels",
            "loaded_labels",
            "stopped_labels",
            "restarted_labels",
            "target_ref",
            "target_sha",
            "action",
            "release_profile",
            "lifecycle_mode",
            "installation_identity",
            "supersedes_operation_id",
            "stage",
            "updated_at",
        }
        completion_fields = {
            "generation_operation_id",
            "environment_generation_id",
            "code_sha",
        }
        expected_fields = base_fields | (completion_fields if completed else set())
        string_fields = expected_fields - {
            "schema_version",
            "labels",
            "loaded_labels",
            "stopped_labels",
            "restarted_labels",
            "installation_identity",
        }
        list_fields = {"labels", "loaded_labels", "stopped_labels", "restarted_labels"}
        if (
            type(payload) is not dict
            or set(payload) != expected_fields
            or type(payload["schema_version"]) is not int
            or any(type(payload[field]) is not str for field in string_fields)
            or any(
                type(payload[field]) is not list
                or any(type(value) is not str for value in payload[field])
                for field in list_fields
            )
        ):
            raise ReleaseGenerationError("Lab handoff record fields are malformed")
        try:
            updated_at = datetime.fromisoformat(payload["updated_at"])
        except ValueError as exc:
            raise ReleaseGenerationError("Lab handoff timestamp is malformed") from exc
        record = cls(
            schema_version=payload["schema_version"],
            operation_id=payload["operation_id"],
            checkout_root=payload["checkout_root"],
            labels=tuple(payload["labels"]),
            loaded_labels=tuple(payload["loaded_labels"]),
            stopped_labels=tuple(payload["stopped_labels"]),
            restarted_labels=tuple(payload["restarted_labels"]),
            target_ref=payload["target_ref"],
            target_sha=payload["target_sha"],
            action=payload["action"],
            release_profile=payload["release_profile"],
            lifecycle_mode=payload["lifecycle_mode"],
            installation_identity=LabInstallationIdentity.from_payload(
                payload["installation_identity"]
            ),
            supersedes_operation_id=payload["supersedes_operation_id"],
            stage=payload["stage"],
            updated_at=payload["updated_at"],
            generation_operation_id=(payload["generation_operation_id"] if completed else ""),
            environment_generation_id=(payload["environment_generation_id"] if completed else ""),
            code_sha=payload["code_sha"] if completed else "",
        )
        record.validate(completed=completed, updated_at=updated_at)
        return record

    def validate(self, *, completed: bool, updated_at: datetime) -> None:
        label_set = set(self.labels)
        chain_invalid = (self.action == "deploy" and self.supersedes_operation_id != "") or (
            self.action in {"resume", "rollback"}
            and (
                re.fullmatch(r"[0-9a-f]{32}", self.supersedes_operation_id) is None
                or self.supersedes_operation_id == self.operation_id
            )
        )
        if (
            self.schema_version != LAB_HANDOFF_SCHEMA_VERSION
            or re.fullmatch(r"[0-9a-f]{32}", self.operation_id) is None
            or re.fullmatch(r"[0-9a-f]{40}", self.target_sha) is None
            or _TARGET_REF_PATTERN.fullmatch(self.target_ref) is None
            or self.action not in {"deploy", "resume", "rollback"}
            or self.release_profile not in RELEASE_PROFILES
            or self.lifecycle_mode not in {"installed", "uninstalled"}
            or self.release_profile != MACOS_LAB_RELEASE_PROFILE
            or self.lifecycle_mode != "installed"
            or not self.labels
            or len(self.labels) != len(label_set)
            or self.loaded_labels != self.labels
            or not set(self.stopped_labels).issubset(label_set)
            or not set(self.restarted_labels).issubset(label_set)
            or len(self.stopped_labels) != len(set(self.stopped_labels))
            or len(self.restarted_labels) != len(set(self.restarted_labels))
            or chain_invalid
            or updated_at.tzinfo is None
            or updated_at.utcoffset() is None
        ):
            raise ReleaseGenerationError("Lab handoff record binding is invalid")
        if completed:
            if (
                self.stage != "completed"
                or set(self.stopped_labels) != label_set
                or set(self.restarted_labels) != label_set
                or re.fullmatch(r"[0-9a-f]{32}", self.generation_operation_id) is None
                or re.fullmatch(r"[0-9a-f]{64}", self.environment_generation_id) is None
                or re.fullmatch(r"[0-9a-f]{40}", self.code_sha) is None
            ):
                raise ReleaseGenerationError("completed Lab handoff proof is invalid")
            return
        allowed_stages = {"planned", "stopping", "stopped", "restarting", "aborted"}
        if self.stage not in allowed_stages:
            raise ReleaseGenerationError("partial Lab handoff stage is invalid")
        if self.stage == "planned" and (self.stopped_labels or self.restarted_labels):
            raise ReleaseGenerationError("planned Lab handoff state is invalid")
        if self.stage == "stopping" and self.restarted_labels:
            raise ReleaseGenerationError("stopping Lab handoff state is invalid")
        if self.stage == "stopped" and (
            set(self.stopped_labels) != label_set or self.restarted_labels
        ):
            raise ReleaseGenerationError("stopped Lab handoff state is invalid")
        if self.stage == "aborted" and (
            self.action != "deploy"
            or self.supersedes_operation_id
            or set(self.restarted_labels) != label_set
        ):
            raise ReleaseGenerationError("aborted Lab handoff state is invalid")


@dataclass(frozen=True)
class GenerationGcMetrics:
    scanned_generations: int
    deleted_generations: int
    reclaimed_bytes: int
    free_bytes_before: int
    free_bytes_after: int
    required_free_bytes: int
    retained_generation_ids: tuple[str, ...]


@dataclass
class GenerationReferenceCollector:
    values: set[str]
    sources: dict[str, set[str]]

    @classmethod
    def create(cls) -> GenerationReferenceCollector:
        return cls(values=set(), sources={})

    def add(self, value: object, *, source: str, optional: bool = False) -> None:
        if value in {None, ""} and optional:
            return
        if type(value) is not str or GENERATION_ID_PATTERN.fullmatch(value) is None:
            raise ReleaseGenerationError(f"{source} generation reference is invalid")
        self.values.add(value)
        self.sources.setdefault(value, set()).add(source)


@dataclass(frozen=True)
class DeploymentChangePlan:
    changed_files: tuple[str, ...]
    blocked_files: tuple[str, ...]
    restart_services: tuple[str, ...]
    handoff_daemons: tuple[str, ...] = LAB_LAUNCHD_HANDOFF_LABELS


def _matches_deployment_path(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def build_deployment_change_plan(
    changed_files: list[str] | tuple[str, ...],
    *,
    release_profile: str = LINUX_RELEASE_PROFILE,
) -> DeploymentChangePlan:
    files = tuple(sorted({path.strip() for path in changed_files if path.strip()}))
    blocked = tuple(
        path for path in files if any(path.startswith(prefix) for prefix in PRIVILEGED_PREFIXES)
    )
    if release_profile == MACOS_LAB_RELEASE_PROFILE:
        handoff = LAB_LAUNCHD_HANDOFF_LABELS if files else ()
        return DeploymentChangePlan(files, blocked, (), handoff)
    if release_profile != LINUX_RELEASE_PROFILE:
        raise ReleaseGenerationError(f"unknown release profile: {release_profile!r}")
    services: set[str] = set()
    for path in files:
        if path in {"pyproject.toml", "uv.lock"} or _matches_deployment_path(
            path,
            SHARED_RUNTIME_PATTERNS,
        ):
            services.update(ALL_LONG_RUNNING_SERVICES)
            continue
        if _matches_deployment_path(path, NO_RESTART_SOURCE_PATTERNS):
            continue
        matched = False
        for service, patterns in SERVICE_PATTERNS.items():
            if _matches_deployment_path(path, patterns):
                services.add(service)
                matched = True
        if path.startswith("src/rquant/") and not matched:
            services.update(ALL_LONG_RUNNING_SERVICES)
    ordered_services = tuple(
        service for service in ALL_LONG_RUNNING_SERVICES if service in services
    )
    return DeploymentChangePlan(files, blocked, ordered_services, ())


def validate_deployment_change_policy(
    changed_files: list[str] | tuple[str, ...],
    *,
    release_profile: str,
    lifecycle_mode: str,
) -> DeploymentChangePlan:
    """Classify one target with the same fail-closed policy used by recovery."""
    if lifecycle_mode not in {"installed", "uninstalled"}:
        raise ReleaseGenerationError("deployment lifecycle mode is invalid")
    plan = build_deployment_change_plan(changed_files, release_profile=release_profile)
    if plan.blocked_files:
        raise ReleaseGenerationError("deployment contains privileged changed files")
    if lifecycle_mode != "installed" and plan.handoff_daemons:
        return replace(plan, handoff_daemons=())
    return plan


def deployment_timers_for_services(services: tuple[str, ...]) -> tuple[str, ...]:
    selected = {timer for service in services for timer in SERVICE_TIMERS.get(service, ())}
    return tuple(sorted(selected))


@dataclass(frozen=True)
class ReleaseGenerationMarker:
    schema_version: int
    operation_id: str
    transaction_kind: str
    commit: str
    uv_lock_sha256: str
    pyproject_sha256: str
    package_version: str
    python_version: str
    python_abi: str
    venv_path: str
    venv_identity: PathIdentity
    pyvenv_cfg_sha256: str
    python_path: str
    python_identity: PathIdentity
    site_packages_path: str
    site_packages_identity: PathIdentity
    environment_generation_id: str
    previous_generation_id: str
    environment_manifest_sha256: str
    published_at: str

    def content_hash(self) -> str:
        payload = canonical_json_bytes(asdict(self))
        return hashlib.sha256(payload).hexdigest()

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ReleaseGenerationMarker:
        identity_fields = {"venv_identity", "python_identity", "site_packages_identity"}
        expected_fields = {field.name for field in fields(cls)}
        string_fields = expected_fields - {"schema_version", *identity_fields}
        if (
            type(payload) is not dict
            or set(payload) != expected_fields
            or type(payload["schema_version"]) is not int
            or any(type(payload[field]) is not str for field in string_fields)
        ):
            raise ReleaseGenerationError("release generation marker is malformed")
        return cls(
            schema_version=payload["schema_version"],
            operation_id=payload["operation_id"],
            transaction_kind=payload["transaction_kind"],
            commit=payload["commit"],
            uv_lock_sha256=payload["uv_lock_sha256"],
            pyproject_sha256=payload["pyproject_sha256"],
            package_version=payload["package_version"],
            python_version=payload["python_version"],
            python_abi=payload["python_abi"],
            venv_path=payload["venv_path"],
            venv_identity=PathIdentity.from_payload(payload["venv_identity"], label="release venv"),
            pyvenv_cfg_sha256=payload["pyvenv_cfg_sha256"],
            python_path=payload["python_path"],
            python_identity=PathIdentity.from_payload(
                payload["python_identity"], label="release Python"
            ),
            site_packages_path=payload["site_packages_path"],
            site_packages_identity=PathIdentity.from_payload(
                payload["site_packages_identity"], label="release site-packages"
            ),
            environment_generation_id=payload["environment_generation_id"],
            previous_generation_id=payload["previous_generation_id"],
            environment_manifest_sha256=payload["environment_manifest_sha256"],
            published_at=payload["published_at"],
        )


@dataclass(frozen=True)
class DeploymentIntent:
    schema_version: int
    operation_id: str
    previous_sha: str
    target_sha: str
    target_ref: str
    stage: str
    changed_files: tuple[str, ...]
    restart_services: tuple[str, ...]
    active_services: tuple[str, ...]
    active_timers: tuple[str, ...]
    restarted_services: tuple[str, ...]
    handoff_operation_id: str
    initial_handoff_operation_id: str
    handoff_labels: tuple[str, ...]
    marker_generation: str
    previous_generation_id: str
    created_at: str
    updated_at: str
    stage_history: tuple[dict[str, str], ...]

    def content_hash(self) -> str:
        values = asdict(self)
        if not self.handoff_operation_id:
            values.pop("handoff_operation_id")
            values.pop("initial_handoff_operation_id")
        if not self.handoff_labels:
            values.pop("handoff_labels")
        payload = canonical_json_bytes(values)
        return hashlib.sha256(payload).hexdigest()

    @classmethod
    def create(
        cls,
        *,
        previous_sha: str,
        target_sha: str,
        target_ref: str,
        changed_files: tuple[str, ...],
        restart_services: tuple[str, ...],
        active_services: tuple[str, ...],
        active_timers: tuple[str, ...],
        marker_generation: str = "",
        previous_generation_id: str = "",
        handoff_operation_id: str = "",
        handoff_labels: tuple[str, ...] = (),
        operation_id: str | None = None,
        stage: str = "planned",
    ) -> DeploymentIntent:
        if stage not in {"planned", "initializing"}:
            raise ReleaseGenerationError("deployment intent initial stage is invalid")
        for label, value in (("previous", previous_sha), ("target", target_sha)):
            if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
                raise ReleaseGenerationError(f"deployment intent {label} SHA is invalid")
        timestamp = datetime.now(UTC).isoformat()
        return cls(
            schema_version=INTENT_SCHEMA_VERSION,
            operation_id=operation_id or secrets.token_hex(16),
            previous_sha=previous_sha,
            target_sha=target_sha,
            target_ref=target_ref,
            stage=stage,
            changed_files=tuple(changed_files),
            restart_services=tuple(restart_services),
            active_services=tuple(active_services),
            active_timers=tuple(active_timers),
            restarted_services=(),
            handoff_operation_id=handoff_operation_id,
            initial_handoff_operation_id=handoff_operation_id,
            handoff_labels=tuple(handoff_labels),
            marker_generation=marker_generation,
            previous_generation_id=previous_generation_id,
            created_at=timestamp,
            updated_at=timestamp,
            stage_history=({"stage": stage, "timestamp": timestamp},),
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> DeploymentIntent:
        expected_fields = {
            "schema_version",
            "operation_id",
            "previous_sha",
            "target_sha",
            "target_ref",
            "stage",
            "changed_files",
            "restart_services",
            "active_services",
            "active_timers",
            "restarted_services",
            "handoff_operation_id",
            "initial_handoff_operation_id",
            "handoff_labels",
            "marker_generation",
            "previous_generation_id",
            "created_at",
            "updated_at",
            "stage_history",
        }
        if type(payload) is not dict or set(payload) != expected_fields:
            raise ReleaseGenerationError("deployment intent fields are malformed")
        if type(payload["schema_version"]) is not int:
            raise ReleaseGenerationError("deployment intent schema type is malformed")
        string_fields = expected_fields - {
            "schema_version",
            "changed_files",
            "restart_services",
            "active_services",
            "active_timers",
            "restarted_services",
            "handoff_labels",
            "stage_history",
        }
        if any(type(payload[field]) is not str for field in string_fields):
            raise ReleaseGenerationError("deployment intent string field is malformed")
        list_fields = (
            "changed_files",
            "restart_services",
            "active_services",
            "active_timers",
            "restarted_services",
            "handoff_labels",
        )
        if any(
            type(payload[field]) is not list
            or any(type(value) is not str for value in payload[field])
            for field in list_fields
        ):
            raise ReleaseGenerationError("deployment intent list field is malformed")
        raw_history = payload["stage_history"]
        if type(raw_history) is not list or not raw_history:
            raise ReleaseGenerationError("deployment intent stage history is malformed")
        for value in raw_history:
            if type(value) is not dict or type(value.get("stage")) is not str:
                raise ReleaseGenerationError("deployment intent stage history is malformed")
            expected_history_fields = (
                {
                    "stage",
                    "timestamp",
                    "previous_handoff_operation_id",
                    "handoff_operation_id",
                }
                if value["stage"] == "handoff_rebound"
                else {"stage", "timestamp"}
            )
            if set(value) != expected_history_fields or any(
                type(value[field]) is not str for field in expected_history_fields
            ):
                raise ReleaseGenerationError("deployment intent stage history is malformed")
        intent = cls(
            schema_version=payload["schema_version"],
            operation_id=payload["operation_id"],
            previous_sha=payload["previous_sha"],
            target_sha=payload["target_sha"],
            target_ref=payload["target_ref"],
            stage=payload["stage"],
            changed_files=tuple(payload["changed_files"]),
            restart_services=tuple(payload["restart_services"]),
            active_services=tuple(payload["active_services"]),
            active_timers=tuple(payload["active_timers"]),
            restarted_services=tuple(payload["restarted_services"]),
            handoff_operation_id=payload["handoff_operation_id"],
            initial_handoff_operation_id=payload["initial_handoff_operation_id"],
            handoff_labels=tuple(payload["handoff_labels"]),
            marker_generation=payload["marker_generation"],
            previous_generation_id=payload["previous_generation_id"],
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
            stage_history=tuple(dict(value) for value in raw_history),
        )
        if (
            intent.schema_version != INTENT_SCHEMA_VERSION
            or re.fullmatch(r"[0-9a-f]{32}", intent.operation_id) is None
        ):
            raise ReleaseGenerationError("deployment intent schema or operation id is invalid")
        for label, value in (("previous", intent.previous_sha), ("target", intent.target_sha)):
            if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
                raise ReleaseGenerationError(f"deployment intent {label} SHA is invalid")
        if _TARGET_REF_PATTERN.fullmatch(intent.target_ref) is None:
            raise ReleaseGenerationError("deployment intent target ref is invalid")
        initialization = intent.stage_history[0]["stage"] == "initializing"
        if (
            intent.marker_generation
            and re.fullmatch(r"[0-9a-f]{64}", intent.marker_generation) is None
        ):
            raise ReleaseGenerationError("deployment intent marker generation is invalid")
        if not initialization and not intent.marker_generation:
            raise ReleaseGenerationError("deployment intent marker generation is invalid")
        if (
            intent.previous_generation_id
            and re.fullmatch(r"[0-9a-f]{64}", intent.previous_generation_id) is None
        ):
            raise ReleaseGenerationError("deployment intent previous generation is invalid")
        if intent.handoff_operation_id and not re.fullmatch(
            r"[0-9a-f]{32}", intent.handoff_operation_id
        ):
            raise ReleaseGenerationError("deployment handoff operation is invalid")
        if intent.initial_handoff_operation_id and not re.fullmatch(
            r"[0-9a-f]{32}", intent.initial_handoff_operation_id
        ):
            raise ReleaseGenerationError("deployment initial handoff operation is invalid")
        if bool(intent.initial_handoff_operation_id) != bool(intent.handoff_labels):
            raise ReleaseGenerationError("deployment initial handoff binding is incomplete")
        if bool(intent.handoff_operation_id) != bool(intent.handoff_labels):
            raise ReleaseGenerationError("deployment handoff binding is incomplete")
        try:
            created_at = datetime.fromisoformat(intent.created_at)
            updated_at = datetime.fromisoformat(intent.updated_at)
            history_times = [
                datetime.fromisoformat(value["timestamp"]) for value in intent.stage_history
            ]
        except ValueError as exc:
            raise ReleaseGenerationError("deployment intent stage history is malformed") from exc
        if (
            created_at.tzinfo is None
            or created_at.utcoffset() is None
            or updated_at.tzinfo is None
            or updated_at.utcoffset() is None
            or any(value.tzinfo is None or value.utcoffset() is None for value in history_times)
            or history_times != sorted(history_times)
            or intent.stage_history[0]["stage"] not in {"planned", "initializing"}
            or intent.stage_history[0]["timestamp"] != intent.created_at
            or intent.stage_history[-1]["timestamp"] != intent.updated_at
            or any(
                _DEPLOYMENT_STAGE_PATTERN.fullmatch(value["stage"]) is None
                for value in intent.stage_history
            )
        ):
            raise ReleaseGenerationError("deployment intent stage history is invalid")
        history_stages = [value["stage"] for value in intent.stage_history]
        semantic_stages = [stage for stage in history_stages if stage != "handoff_rebound"]
        if not semantic_stages or semantic_stages[-1] != intent.stage:
            raise ReleaseGenerationError("deployment intent stage history is inconsistent")
        if (
            intent.handoff_operation_id
            and intent.stage == "completed"
            and "awaiting_readiness" not in semantic_stages
        ):
            raise ReleaseGenerationError("installed deployment skipped readiness")
        _validate_deployment_stage_sequence(history_stages)
        rebound_events = [
            value for value in intent.stage_history if value["stage"] == "handoff_rebound"
        ]
        if rebound_events:
            current_operation = intent.initial_handoff_operation_id
            for event in rebound_events:
                if (
                    event["previous_handoff_operation_id"] != current_operation
                    or event["handoff_operation_id"] == current_operation
                    or re.fullmatch(r"[0-9a-f]{32}", event["handoff_operation_id"]) is None
                    or re.fullmatch(r"[0-9a-f]{32}", event["previous_handoff_operation_id"]) is None
                ):
                    raise ReleaseGenerationError("deployment handoff rebound history is invalid")
                current_operation = event["handoff_operation_id"]
            if current_operation != intent.handoff_operation_id:
                raise ReleaseGenerationError("deployment handoff rebound history is stale")
        elif intent.handoff_operation_id != intent.initial_handoff_operation_id:
            raise ReleaseGenerationError("deployment initial handoff binding changed")
        return intent

    def advance(
        self,
        *,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        if (
            stage == "completed"
            and self.handoff_operation_id
            and self.stage != "awaiting_readiness"
        ):
            raise ReleaseGenerationError("installed deployment must await readiness")
        history_stages = [value["stage"] for value in self.stage_history]
        _validate_deployment_stage_sequence([*history_stages, stage])
        timestamp = datetime.now(UTC).isoformat()
        return replace(
            self,
            stage=stage,
            restarted_services=(
                self.restarted_services if restarted_services is None else tuple(restarted_services)
            ),
            updated_at=timestamp,
            stage_history=(*self.stage_history, {"stage": stage, "timestamp": timestamp}),
        )

    def rebind_handoff(
        self,
        *,
        handoff_operation_id: str,
        handoff_labels: tuple[str, ...],
    ) -> DeploymentIntent:
        if re.fullmatch(r"[0-9a-f]{32}", handoff_operation_id) is None or not handoff_labels:
            raise ReleaseGenerationError("deployment handoff binding is invalid")
        if self.stage == "completed":
            raise ReleaseGenerationError("completed deployment cannot rebind its handoff")
        if (
            self.stage != "recovery_started"
            or self.stage_history[-1]["stage"] != "recovery_started"
        ):
            raise ReleaseGenerationError("deployment handoff rebind requires adjacent recovery")
        if handoff_operation_id == self.handoff_operation_id:
            raise ReleaseGenerationError("deployment handoff operation must change")
        if tuple(handoff_labels) != self.handoff_labels:
            raise ReleaseGenerationError("deployment handoff labels cannot change during recovery")
        _validate_deployment_stage_sequence(
            [*[value["stage"] for value in self.stage_history], "handoff_rebound"]
        )
        timestamp = datetime.now(UTC).isoformat()
        return replace(
            self,
            handoff_operation_id=handoff_operation_id,
            handoff_labels=tuple(handoff_labels),
            updated_at=timestamp,
            stage_history=(
                *self.stage_history,
                {
                    "stage": "handoff_rebound",
                    "timestamp": timestamp,
                    "previous_handoff_operation_id": self.handoff_operation_id,
                    "handoff_operation_id": handoff_operation_id,
                },
            ),
        )


def _validate_deployment_stage_sequence(stages: list[str]) -> None:
    if not stages or stages[0] not in {"planned", "initializing"}:
        raise ReleaseGenerationError("deployment intent stage history is invalid")
    previous = stages[0]
    raw_previous = stages[0]
    for current in stages[1:]:
        if current == "handoff_rebound":
            if raw_previous != "recovery_started":
                raise ReleaseGenerationError(
                    f"deployment intent stage transition is invalid: {raw_previous} -> {current}"
                )
            raw_previous = current
            continue
        if previous == "initializing":
            allowed = {"completed"}
        elif previous == "completed":
            allowed = set()
        elif current == "recovery_started" and previous != "initializing":
            allowed = {"recovery_started"}
        elif previous in {"planned", "recovery_started"}:
            allowed = {"timers_stopped"}
        elif previous == "timers_stopped":
            allowed = {
                "deploy_checkout_ready",
                "resume_checkout_ready",
                "rollback_checkout_ready",
            }
        elif previous.endswith("_checkout_ready"):
            allowed = {previous.replace("_checkout_ready", "_dependencies_ready")}
        elif previous.endswith("_dependencies_ready"):
            allowed = {previous.replace("_dependencies_ready", "_preflight_ready")}
        elif previous == "post_restart_preflight_ready":
            allowed = {"timers_restored"}
        elif previous.endswith("_preflight_ready"):
            allowed = {"services_transitioning"}
        elif previous == "services_transitioning":
            allowed = {"services_transitioning", "services_ready"}
        elif previous == "services_ready":
            allowed = {"post_restart_preflight_ready"}
        elif previous == "timers_restored":
            allowed = {"marker_published"}
        elif previous == "marker_published":
            allowed = {"awaiting_readiness", "completed"}
        elif previous == "awaiting_readiness":
            allowed = {"completed"}
        else:
            allowed = set()
        if current not in allowed:
            raise ReleaseGenerationError(
                f"deployment intent stage transition is invalid: {previous} -> {current}"
            )
        previous = current
        raw_previous = current


@dataclass(frozen=True)
class EnvironmentSelector:
    schema_version: int
    operation_id: str
    transaction_kind: str
    commit: str
    generation_id: str
    previous_generation_id: str
    environment_path: str
    manifest_name: str
    manifest_sha256: str
    published_at: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> EnvironmentSelector:
        expected_fields = {field.name for field in fields(cls)}
        if (
            type(payload) is not dict
            or set(payload) != expected_fields
            or type(payload["schema_version"]) is not int
            or any(
                type(payload[field]) is not str for field in expected_fields - {"schema_version"}
            )
        ):
            raise ReleaseGenerationError("environment selector is malformed")
        selector = cls(**payload)
        if (
            selector.schema_version != ENVIRONMENT_SCHEMA_VERSION
            or len(selector.operation_id) != 32
            or selector.transaction_kind not in {"deployment", "initialization"}
            or len(selector.commit) != 40
            or len(selector.generation_id) != 64
            or (
                selector.previous_generation_id != "" and len(selector.previous_generation_id) != 64
            )
            or len(selector.manifest_sha256) != 64
        ):
            raise ReleaseGenerationError("environment selector is invalid")
        return selector


@dataclass(frozen=True)
class ReleaseGenerationCommit:
    schema_version: int
    operation_id: str
    transaction_kind: str
    commit: str
    marker_sha256: str
    transaction_sha256: str
    environment_generation_id: str
    previous_generation_id: str
    environment_manifest_sha256: str
    committed_at: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ReleaseGenerationCommit:
        expected_fields = {field.name for field in fields(cls)}
        if (
            type(payload) is not dict
            or set(payload) != expected_fields
            or type(payload["schema_version"]) is not int
            or any(
                type(payload[field]) is not str for field in expected_fields - {"schema_version"}
            )
        ):
            raise ReleaseGenerationError("release generation commit record is malformed")
        record = cls(**payload)
        if (
            record.schema_version != COMMIT_SCHEMA_VERSION
            or len(record.operation_id) != 32
            or record.transaction_kind not in {"deployment", "initialization"}
            or len(record.commit) != 40
            or any(
                len(value) != 64
                for value in (
                    record.marker_sha256,
                    record.transaction_sha256,
                    record.environment_generation_id,
                    record.environment_manifest_sha256,
                )
            )
            or (record.previous_generation_id != "" and len(record.previous_generation_id) != 64)
        ):
            raise ReleaseGenerationError("release generation commit record is invalid")
        return record


def validate_deployment_intent_policy(
    intent: DeploymentIntent,
    *,
    release_profile: str,
    lifecycle_mode: str,
    expected_handoff_operation_id: str | None = None,
) -> DeploymentChangePlan:
    plan = validate_deployment_change_policy(
        intent.changed_files,
        release_profile=release_profile,
        lifecycle_mode=lifecycle_mode,
    )
    if (
        intent.changed_files != plan.changed_files
        or intent.restart_services != plan.restart_services
    ):
        raise ReleaseGenerationError("deployment intent change classification is inconsistent")
    for values in (
        intent.restart_services,
        intent.active_services,
        intent.active_timers,
        intent.restarted_services,
    ):
        if len(values) != len(set(values)):
            raise ReleaseGenerationError("deployment intent service state contains duplicates")
    if not set(intent.active_services).issubset(intent.restart_services):
        raise ReleaseGenerationError("deployment intent active service state is invalid")
    if not set(intent.restarted_services).issubset(intent.active_services):
        raise ReleaseGenerationError("deployment intent restarted service state is invalid")
    if not set(intent.active_timers).issubset(
        deployment_timers_for_services(intent.restart_services)
    ):
        raise ReleaseGenerationError("deployment intent active timer state is invalid")
    expected_labels = (
        plan.handoff_daemons
        if lifecycle_mode == "installed" and release_profile == MACOS_LAB_RELEASE_PROFILE
        else ()
    )
    if intent.handoff_labels != expected_labels or bool(intent.handoff_operation_id) != bool(
        expected_labels
    ):
        raise ReleaseGenerationError("deployment intent handoff policy is inconsistent")
    if (
        expected_handoff_operation_id is not None
        and intent.handoff_operation_id != expected_handoff_operation_id
    ):
        raise ReleaseGenerationError("deployment intent handoff operation changed")
    return plan


def validate_lab_handoff_record_authority(
    *,
    record: LabHandoffRecord,
    intent: DeploymentIntent,
    installation_identity: LabInstallationIdentity,
    checkout_root: str,
    expected_labels: tuple[str, ...],
) -> None:
    expected_sha = intent.previous_sha if record.action == "rollback" else intent.target_sha
    allowed_refs = (
        {intent.previous_sha}
        if record.action == "rollback"
        else {intent.target_sha, intent.target_ref}
    )
    if (
        record.checkout_root != checkout_root
        or record.labels != expected_labels
        or record.installation_identity != installation_identity
        or record.target_sha != expected_sha
        or record.target_ref not in allowed_refs
        or record.release_profile != MACOS_LAB_RELEASE_PROFILE
        or record.lifecycle_mode != "installed"
    ):
        raise ReleaseGenerationError("Lab handoff record does not match deployment intent")


def validate_lab_handoff_supersede_chain(
    *,
    record: LabHandoffRecord,
    ancestors: tuple[LabHandoffRecord, ...],
    intent: DeploymentIntent,
    installation_identity: LabInstallationIdentity,
    checkout_root: str,
    expected_labels: tuple[str, ...],
    completed_proofs: tuple[LabHandoffRecord, ...] = (),
) -> None:
    if record.operation_id != intent.handoff_operation_id:
        raise ReleaseGenerationError("Lab handoff supersede chain is stale")
    physical_chain = (record, *ancestors)
    history_chain = (
        intent.initial_handoff_operation_id,
        *(
            event["handoff_operation_id"]
            for event in intent.stage_history
            if event["stage"] == "handoff_rebound"
        ),
    )
    if tuple(item.operation_id for item in reversed(physical_chain)) != history_chain:
        raise ReleaseGenerationError("Lab handoff supersede chain does not match rebound history")
    validate_lab_handoff_record_authority(
        record=record,
        intent=intent,
        installation_identity=installation_identity,
        checkout_root=checkout_root,
        expected_labels=expected_labels,
    )
    current = record
    seen = {record.operation_id}
    for ancestor in ancestors:
        if (
            current.action == "deploy"
            or current.supersedes_operation_id != ancestor.operation_id
            or ancestor.operation_id in seen
        ):
            raise ReleaseGenerationError("Lab handoff supersede chain is discontinuous")
        validate_lab_handoff_supersede_action(
            action=current.action,
            superseded_action=ancestor.action,
        )
        validate_lab_handoff_record_authority(
            record=ancestor,
            intent=intent,
            installation_identity=ancestor.installation_identity,
            checkout_root=checkout_root,
            expected_labels=expected_labels,
        )
        seen.add(ancestor.operation_id)
        current = ancestor
    if current.action != "deploy" or current.supersedes_operation_id:
        raise ReleaseGenerationError("Lab handoff supersede chain has no deploy root")
    if current.operation_id != intent.initial_handoff_operation_id:
        raise ReleaseGenerationError("Lab handoff supersede chain root is stale")
    if record.action == "deploy" and ancestors:
        raise ReleaseGenerationError("deploy handoff cannot have a supersede chain")
    if record.action != "deploy" and not ancestors:
        raise ReleaseGenerationError("recovery handoff supersede chain is missing")
    records_by_operation = {item.operation_id: item for item in physical_chain}
    seen_proofs: set[str] = set()
    for proof in completed_proofs:
        operation = records_by_operation.get(proof.operation_id)
        if (
            operation is None
            or proof.operation_id in seen_proofs
            or operation.stage != "completed"
            or proof != operation
        ):
            raise ReleaseGenerationError("completed Lab handoff proof records are inconsistent")
        seen_proofs.add(proof.operation_id)


def validate_lab_handoff_supersede_action(
    *,
    action: str,
    superseded_action: str,
) -> None:
    allowed_ancestor_actions = {
        "resume": {"deploy"},
        "rollback": {"deploy", "resume", "rollback"},
    }.get(action, set())
    if superseded_action not in allowed_ancestor_actions:
        raise ReleaseGenerationError("Lab handoff supersede action edge is invalid")


def validate_ready_deployment_handoff_authority(
    *,
    intent: DeploymentIntent,
    marker: ReleaseGenerationMarker,
    selector: EnvironmentSelector,
    handoff_operation_id: str,
    handoff_labels: tuple[str, ...],
    generation_operation_id: str,
    environment_generation_id: str,
    code_sha: str,
    action: str,
    target_ref: str,
    target_sha: str,
    release_profile: str,
    lifecycle_mode: str,
    allowed_intent_stages: tuple[str, ...] = ("awaiting_readiness", "completed"),
) -> None:
    validate_deployment_intent_policy(
        intent,
        release_profile=release_profile,
        lifecycle_mode=lifecycle_mode,
        expected_handoff_operation_id=handoff_operation_id,
    )
    expected_code_sha = intent.previous_sha if action == "rollback" else intent.target_sha
    allowed_target_refs = (
        {intent.previous_sha} if action == "rollback" else {intent.target_sha, intent.target_ref}
    )
    if (
        action not in {"deploy", "resume", "rollback"}
        or intent.stage not in allowed_intent_stages
        or intent.handoff_labels != handoff_labels
        or target_ref not in allowed_target_refs
        or target_sha != expected_code_sha
        or code_sha != expected_code_sha
        or generation_operation_id != intent.operation_id
        or environment_generation_id != marker.environment_generation_id
    ):
        raise ReleaseGenerationError("completed deployment handoff binding is stale")
    if (
        marker.transaction_kind != "deployment"
        or selector.transaction_kind != "deployment"
        or marker.operation_id != intent.operation_id
        or selector.operation_id != intent.operation_id
        or marker.commit != code_sha
        or selector.commit != code_sha
        or selector.generation_id != environment_generation_id
        or marker.previous_generation_id != intent.previous_generation_id
        or selector.previous_generation_id != intent.previous_generation_id
        or marker.environment_manifest_sha256 != selector.manifest_sha256
    ):
        raise ReleaseGenerationError("ready deployment generation authority is inconsistent")


def validate_completed_deployment_authority(
    *,
    intent: DeploymentIntent,
    marker: ReleaseGenerationMarker,
    selector: EnvironmentSelector,
    committed: ReleaseGenerationCommit,
    handoff_operation_id: str,
    handoff_labels: tuple[str, ...],
    generation_operation_id: str,
    environment_generation_id: str,
    code_sha: str,
    action: str,
    target_ref: str,
    target_sha: str,
    release_profile: str,
    lifecycle_mode: str,
) -> None:
    validate_ready_deployment_handoff_authority(
        intent=intent,
        marker=marker,
        selector=selector,
        handoff_operation_id=handoff_operation_id,
        handoff_labels=handoff_labels,
        generation_operation_id=generation_operation_id,
        environment_generation_id=environment_generation_id,
        code_sha=code_sha,
        action=action,
        target_ref=target_ref,
        target_sha=target_sha,
        release_profile=release_profile,
        lifecycle_mode=lifecycle_mode,
        allowed_intent_stages=("completed",),
    )
    if (
        committed.transaction_kind != "deployment"
        or committed.operation_id != intent.operation_id
        or committed.commit != code_sha
        or committed.environment_generation_id != environment_generation_id
        or committed.previous_generation_id != intent.previous_generation_id
        or committed.environment_manifest_sha256 != selector.manifest_sha256
        or committed.marker_sha256 != marker.content_hash()
        or committed.transaction_sha256 != intent.content_hash()
    ):
        raise ReleaseGenerationError("completed deployment generation authority is inconsistent")


def marker_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.complete.json")


def intent_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.intent.json")


def prepared_intent_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.intent.prepared.json")


def initialization_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.initialized.json")


def commit_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.commit.json")


def environment_selector_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.environment.json")


def environment_root_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.venvs")


def environment_manifest_path_for_lock(lock_path: Path, generation_id: str) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.venv-{generation_id}.manifest.json")


def generation_code_root(environment_path: Path) -> Path:
    """Return the immutable source/config authority inside one environment generation."""
    return environment_path / RELEASE_CODE_DIRECTORY


def _canonical(path: Path, *, label: str) -> Path:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ReleaseGenerationError(f"{label} must be an absolute canonical path")
    return path


def _identity(path: Path, *, label: str, directory: bool) -> PathIdentity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} is unavailable") from exc
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not expected_type(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or (not directory and observed.st_nlink != 1)
        or observed.st_mode & 0o022
        or path.resolve(strict=True) != path
    ):
        raise ReleaseGenerationError(f"{label} has unsafe identity")
    return PathIdentity.capture(observed)


def _private_lock_root(path: Path) -> tuple[int, PathIdentity]:
    identity = _identity(path, label="deployment authority root", directory=True)
    if stat.S_IMODE(identity.mode) != 0o700:
        raise ReleaseGenerationError("deployment authority root must have mode 0700")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    opened = PathIdentity.capture(os.fstat(descriptor))
    if _object_key(opened) != _object_key(identity):
        os.close(descriptor)
        raise ReleaseGenerationError("deployment authority root identity changed")
    return descriptor, identity


def _object_key(identity: PathIdentity) -> tuple[int, int, int, int]:
    return identity.device, identity.inode, identity.mode, identity.owner


def _hash_file(
    path: Path,
    *,
    label: str,
    checkpoint: Callable[[], None] | None = None,
) -> str:
    _identity(path, label=label, directory=False)
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                if checkpoint is not None:
                    checkpoint()
                digest.update(chunk)
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} cannot be read") from exc
    return digest.hexdigest()


def _trusted_executable_binding(path: Path, *, label: str) -> dict[str, object]:
    canonical = _canonical(path, label=label)
    try:
        observed = canonical.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid not in {0, os.getuid()}
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
        or canonical.resolve(strict=True) != canonical
    ):
        raise ReleaseGenerationError(f"{label} has unsafe identity")
    digest = hashlib.sha256()
    try:
        with canonical.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        active = canonical.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} cannot be read") from exc
    if PathIdentity.capture(active) != PathIdentity.capture(observed):
        raise ReleaseGenerationError(f"{label} identity changed while reading")
    return {
        "physical_path": str(canonical),
        "identity": asdict(PathIdentity.capture(observed)),
        "sha256": digest.hexdigest(),
    }


def _blocking_deadline(
    cap_seconds: float,
    timeout_provider: Callable[[float], float] | None,
) -> float:
    now = time.monotonic()
    deadline = now + cap_seconds if timeout_provider is None else timeout_provider(cap_seconds)
    if not math.isfinite(deadline) or deadline <= now:
        raise ReleaseGenerationError("release generation command deadline is invalid")
    return deadline


def _contained_run(
    arguments: list[str],
    *,
    cwd: Path,
    cap_seconds: float,
    timeout_provider: Callable[[float], float] | None,
    check: bool = True,
    text: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[Any]:
    deadline = _blocking_deadline(cap_seconds, timeout_provider)
    try:
        return run_contained(
            arguments,
            cwd=cwd,
            deadline_monotonic=deadline,
            check=check,
            text=text,
            env=env,
            may_spawn_background_descendants=False,
        )
    except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
        raise ReleaseGenerationError("release generation command failed") from exc


def _git_output(
    repo: Path,
    git_path: Path,
    *arguments: str,
    timeout_provider: Callable[[float], float] | None = None,
) -> str:
    try:
        result = _contained_run(
            [str(git_path), *arguments],
            cwd=repo,
            check=True,
            text=True,
            cap_seconds=10,
            timeout_provider=timeout_provider,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("release generation Git verification failed") from exc
    return result.stdout.strip()


def _assert_tracked_clean(
    repo: Path,
    git_path: Path,
    *,
    timeout_provider: Callable[[float], float] | None = None,
) -> None:
    try:
        status = _contained_run(
            [str(git_path), "status", "--porcelain=v1", "--untracked-files=no"],
            cwd=repo,
            check=True,
            text=True,
            cap_seconds=10,
            timeout_provider=timeout_provider,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
        diff = _contained_run(
            [str(git_path), "diff", "--quiet", "HEAD", "--"],
            cwd=repo,
            check=False,
            text=True,
            cap_seconds=10,
            timeout_provider=timeout_provider,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("tracked checkout verification failed") from exc
    if status.stdout or diff.returncode != 0:
        raise ReleaseGenerationError("tracked checkout is dirty")


_RELEASE_CODE_EXACT_FILES = frozenset(
    {
        ".env.example",
        "pyproject.toml",
        "uv.lock",
        "scripts/bootstrap-lab-daemon.py",
        "scripts/preflight-lab-runtime.py",
        "scripts/run-lab-daemon.py",
        "scripts/strict_json.py",
    }
)


def _release_code_member(path: str) -> bool:
    return (
        path in _RELEASE_CODE_EXACT_FILES
        or ("/" not in path and path.endswith(".py"))
        or path.startswith("src/rquant/")
        or (path.startswith("deploy/launchd/com.roxor.rquant-lab-") and path.endswith(".plist"))
    )


def _release_code_directory(path: str) -> bool:
    prefixes = (
        "src",
        "src/rquant",
        "scripts",
        "deploy",
        "deploy/launchd",
    )
    return path in prefixes


def _write_private_payload(path: Path, payload: bytes, *, executable: bool = False) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o700 if executable else 0o600,
    )
    try:
        _write_all(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _materialize_release_code(
    *,
    repo: Path,
    git_path: Path,
    expected_commit: str,
    destination: Path,
    checkpoint: Callable[[], None],
    timeout_provider: Callable[[float], float] | None = None,
) -> None:
    """Extract a non-recursive exact-commit runtime payload into a private generation."""
    listing = _git_output(
        repo,
        git_path,
        "ls-tree",
        "-r",
        "--name-only",
        expected_commit,
        timeout_provider=timeout_provider,
    )
    members = tuple(path for path in listing.splitlines() if _release_code_member(path))
    required = {"pyproject.toml", "uv.lock"}
    if not required.issubset(members) or not any(
        path.startswith("src/rquant/") for path in members
    ):
        raise ReleaseGenerationError("release code payload is incomplete")
    checkpoint()
    try:
        result = _contained_run(
            [str(git_path), "archive", "--format=tar", expected_commit, "--", *members],
            cwd=repo,
            check=True,
            text=False,
            cap_seconds=30,
            timeout_provider=timeout_provider,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("exact release code payload could not be exported") from exc
    checkpoint()
    destination.mkdir(mode=0o700)
    try:
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:") as archive:
            for member in archive.getmembers():
                checkpoint()
                relative = Path(member.name)
                if (
                    relative.is_absolute()
                    or ".." in relative.parts
                    or not (
                        _release_code_member(relative.as_posix())
                        or (member.isdir() and _release_code_directory(relative.as_posix()))
                    )
                    or member.issym()
                    or member.islnk()
                    or not (member.isdir() or member.isfile())
                ):
                    raise ReleaseGenerationError("release code archive contains an unsafe member")
                target = destination / relative
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True, mode=0o700)
                    target.chmod(0o700)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise ReleaseGenerationError("release code archive member is unreadable")
                _write_private_payload(
                    target,
                    extracted.read(),
                    executable=bool(member.mode & stat.S_IXUSR),
                )
        dotenv = repo / ".env"
        if os.path.lexists(dotenv):
            observed = _identity(dotenv, label="release .env", directory=False)
            if stat.S_IMODE(observed.mode) != 0o600:
                raise ReleaseGenerationError("release .env must have mode 0600")
            _write_private_payload(destination / ".env", dotenv.read_bytes())
    except BaseException:
        raise
    checkpoint()


def _python_facts(
    python_path: Path,
    *,
    timeout_provider: Callable[[float], float] | None = None,
) -> tuple[str, str]:
    program = (
        "import json,sys,sysconfig;"
        "print(json.dumps({'version': '.'.join(map(str, sys.version_info[:3])),"
        "'cache_tag': sys.implementation.cache_tag or '',"
        "'soabi': sysconfig.get_config_var('SOABI') or ''}, sort_keys=True))"
    )
    try:
        result = _contained_run(
            [str(python_path), "-I", "-S", "-c", program],
            cwd=python_path.parent,
            check=True,
            text=True,
            cap_seconds=10,
            timeout_provider=timeout_provider,
        )
        payload = strict_json_loads(result.stdout)
        version = str(payload["version"])
        abi = f"{payload['cache_tag']}:{payload['soabi']}"
    except (OSError, subprocess.SubprocessError, StrictJsonError, KeyError) as exc:
        raise ReleaseGenerationError("release Python ABI cannot be verified") from exc
    if not version or abi == ":":
        raise ReleaseGenerationError("release Python ABI is incomplete")
    return version, abi


def _verified_interpreter(path: Path, *, label: str) -> tuple[Path, PathIdentity, str]:
    try:
        resolved = path.resolve(strict=True)
        observed = resolved.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} cannot be resolved") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
    ):
        raise ReleaseGenerationError(f"{label} has unsafe identity")
    return resolved, PathIdentity.capture(observed), _hash_file(resolved, label=label)


def _venv_system_interpreter(
    python_path: Path,
    *,
    preselected_system_python: Path | None = None,
    timeout_provider: Callable[[float], float] | None = None,
) -> tuple[Path, PathIdentity, str]:
    try:
        result = _contained_run(
            [
                str(python_path),
                "-I",
                "-S",
                "-c",
                "import sys; print(sys._base_executable)",
            ],
            cwd=python_path.parent,
            check=True,
            text=True,
            cap_seconds=10,
            timeout_provider=timeout_provider,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("deployment system Python cannot be discovered") from exc
    raw_path = result.stdout.strip()
    if not raw_path:
        raise ReleaseGenerationError("deployment system Python is empty")
    if preselected_system_python is None:
        return _verified_interpreter(Path(raw_path), label="deployment system Python")
    try:
        expected = preselected_system_python.resolve(strict=True)
        reported = Path(raw_path).resolve(strict=True)
    except OSError as exc:
        raise ReleaseGenerationError("deployment system Python is unavailable") from exc
    if reported != expected:
        raise ReleaseGenerationError(
            "deployment system Python differs from preselected interpreter"
        )
    return _verified_interpreter(expected, label="deployment system Python")


def _write_all(
    descriptor: int,
    payload: bytes,
    *,
    checkpoint: Callable[[], None] | None = None,
) -> None:
    offset = 0
    while offset < len(payload):
        if checkpoint is not None:
            checkpoint()
        try:
            written = os.write(descriptor, payload[offset:])
        except OSError as exc:
            raise ReleaseGenerationError("release generation marker cannot be written") from exc
        if written <= 0:
            raise ReleaseGenerationError("release generation marker write made no progress")
        offset += written


def _verify_temporary_payload(
    descriptor: int,
    *,
    expected_payload: bytes,
    expected_marker: ReleaseGenerationMarker,
) -> None:
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, MAX_MARKER_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_MARKER_BYTES:
                raise ReleaseGenerationError("temporary release marker is too large")
        observed_payload = b"".join(chunks)
        observed_marker = ReleaseGenerationMarker.from_payload(strict_json_loads(observed_payload))
    except ReleaseGenerationError:
        raise
    except (OSError, StrictJsonError) as exc:
        raise ReleaseGenerationError("temporary release marker cannot be verified") from exc
    if (
        len(observed_payload) != len(expected_payload)
        or hashlib.sha256(observed_payload).digest() != hashlib.sha256(expected_payload).digest()
        or observed_marker != expected_marker
    ):
        raise ReleaseGenerationError("temporary release marker content mismatch")


def _read_private_json(
    *,
    root_fd: int,
    root_path: Path,
    name: str,
    maximum_bytes: int,
    checkpoint: Callable[[], None] | None = None,
) -> tuple[dict[str, Any], PathIdentity]:
    descriptor = -1
    try:
        if checkpoint is not None:
            checkpoint()
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ReleaseGenerationError(f"private deployment record {name} is unsafe")
        chunks: list[bytes] = []
        total = 0
        while True:
            if checkpoint is not None:
                checkpoint()
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise ReleaseGenerationError(f"private deployment record {name} is too large")
        active = (root_path / name).lstat()
        identity = PathIdentity.capture(opened)
        if identity != PathIdentity.capture(active):
            raise ReleaseGenerationError(f"private deployment record {name} identity changed")
        if checkpoint is not None:
            checkpoint()
        payload = strict_canonical_json_loads(
            b"".join(chunks),
            trailing_newline=True,
        )
        if checkpoint is not None:
            checkpoint()
        if not isinstance(payload, dict):
            raise ReleaseGenerationError(f"private deployment record {name} is malformed")
        return payload, identity
    except ReleaseGenerationError:
        raise
    except FileNotFoundError as exc:
        raise ReleaseGenerationRecordMissingError(
            f"private deployment record {name} is missing"
        ) from exc
    except StrictJsonError as exc:
        raise ReleaseGenerationError(f"private deployment record {name} is invalid: {exc}") from exc
    except OSError as exc:
        raise ReleaseGenerationError(f"private deployment record {name} cannot be read") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _write_private_json(
    *,
    root_fd: int,
    root_path: Path,
    name: str,
    payload: dict[str, Any],
    require_absent: bool,
    expected_identity: PathIdentity | None = None,
    maximum_bytes: int = MAX_INTENT_BYTES,
    checkpoint: Callable[[], None] | None = None,
) -> None:
    temporary_name = f".{name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    published = False
    try:
        descriptor = os.open(
            temporary_name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        expected_digest = hashlib.sha256()
        expected_size = 0
        for chunk in _canonical_json_chunks(payload, checkpoint=checkpoint):
            expected_size += len(chunk)
            if expected_size + 1 > maximum_bytes:
                raise ReleaseGenerationError(f"private deployment record {name} is too large")
            expected_digest.update(chunk)
            _write_all(descriptor, chunk, checkpoint=checkpoint)
        expected_digest.update(b"\n")
        expected_size += 1
        _write_all(descriptor, b"\n", checkpoint=checkpoint)
        if checkpoint is not None:
            checkpoint()
        os.fsync(descriptor)
        if checkpoint is not None:
            checkpoint()
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed_chunks: list[bytes] = []
        observed_digest = hashlib.sha256()
        observed_size = 0
        while True:
            if checkpoint is not None:
                checkpoint()
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - observed_size))
            if not chunk:
                break
            observed_chunks.append(chunk)
            observed_digest.update(chunk)
            observed_size += len(chunk)
            if observed_size > maximum_bytes:
                raise ReleaseGenerationError(f"private deployment record {name} is too large")
        if checkpoint is not None:
            checkpoint()
        observed_payload = b"".join(observed_chunks)
        parsed = strict_json_loads(observed_payload)
        if checkpoint is not None:
            checkpoint()
        if (
            observed_size != expected_size
            or observed_digest.digest() != expected_digest.digest()
            or not isinstance(parsed, dict)
        ):
            raise ReleaseGenerationError(f"private deployment record {name} verification failed")
        if checkpoint is not None:
            checkpoint()
        if require_absent:
            try:
                os.link(
                    temporary_name,
                    name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise ReleaseGenerationError(
                    f"private deployment record {name} already exists"
                ) from exc
            os.unlink(temporary_name, dir_fd=root_fd)
        else:
            if expected_identity is None:
                raise ReleaseGenerationError("deployment record update lacks an identity fence")
            active = (root_path / name).lstat()
            if PathIdentity.capture(active) != expected_identity:
                raise ReleaseGenerationError(f"private deployment record {name} changed")
            os.replace(temporary_name, name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
        published = True
        os.fsync(root_fd)
        if checkpoint is not None:
            checkpoint()
        active = (root_path / name).lstat()
        if PathIdentity.capture(os.fstat(descriptor)) != PathIdentity.capture(active):
            raise ReleaseGenerationError(f"private deployment record {name} publish changed")
    except ReleaseGenerationError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseGenerationError(f"private deployment record {name} cannot be written") from exc
    finally:
        if not published:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)
        if descriptor >= 0:
            os.close(descriptor)


def _canonical_json_chunks(
    payload: dict[str, Any],
    *,
    checkpoint: Callable[[], None] | None = None,
) -> Iterator[bytes]:
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    for encoded_text in encoder.iterencode(payload):
        if checkpoint is not None:
            checkpoint()
        encoded = encoded_text.encode()
        for offset in range(0, len(encoded), 64 * 1024):
            if checkpoint is not None:
                checkpoint()
            yield encoded[offset : offset + 64 * 1024]


def _payload_hash(
    payload: dict[str, Any],
    *,
    checkpoint: Callable[[], None] | None = None,
) -> str:
    digest = hashlib.sha256()
    for chunk in _canonical_json_chunks(payload, checkpoint=checkpoint):
        digest.update(chunk)
    if checkpoint is not None:
        checkpoint()
    return digest.hexdigest()


def _environment_generation_id(*, operation_id: str, commit: str) -> str:
    return hashlib.sha256(f"{operation_id}:{commit}".encode()).hexdigest()


def _nonnegative_float_setting(value: float | None, *, env_name: str, default: float) -> float:
    raw: object = os.environ.get(env_name, default) if value is None else value
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative number")
    return parsed


def _nonnegative_int_setting(value: int | None, *, env_name: str, default: int) -> int:
    raw: object = os.environ.get(env_name, default) if value is None else value
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative integer") from exc
    if parsed < 0:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative integer")
    return parsed


def _private_tree_size(
    root: Path,
    *,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
    checkpoint: Callable[[], None] | None = None,
    timeout_provider: Callable[[float], float] | None = None,
) -> int:
    total = 0
    for current_root, directory_names, file_names in os.walk(root):
        if checkpoint is not None:
            checkpoint()
        current = Path(current_root)
        observed_root = current.lstat()
        if (
            not stat.S_ISDIR(observed_root.st_mode)
            or stat.S_ISLNK(observed_root.st_mode)
            or observed_root.st_uid != os.getuid()
        ):
            raise ReleaseGenerationError("source release venv contains an unsafe directory")
        for name in (*directory_names, *file_names):
            if checkpoint is not None:
                checkpoint()
            path = current / name
            observed = path.lstat()
            if observed.st_uid != os.getuid():
                raise ReleaseGenerationError("source release venv contains an unsafe object")
            if stat.S_ISLNK(observed.st_mode):
                _environment_entry(
                    path,
                    root,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                    checkpoint=checkpoint,
                    timeout_provider=timeout_provider,
                )
                continue
            if stat.S_ISREG(observed.st_mode):
                if observed.st_nlink != 1:
                    raise ReleaseGenerationError("source release venv contains a hardlink")
                total += observed.st_size
            elif not stat.S_ISDIR(observed.st_mode):
                raise ReleaseGenerationError("source release venv contains an unsafe object")
    return total


def _remove_private_tree_at(
    parent_fd: int,
    name: str,
    *,
    checkpoint: Callable[[], None] | None = None,
) -> int:
    if checkpoint is not None:
        checkpoint()
    descriptor = os.open(
        name,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )
    reclaimed = 0
    try:
        opened = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            PathIdentity.capture(opened) != PathIdentity.capture(active)
            or not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.getuid()
        ):
            raise ReleaseGenerationError("orphan environment generation is unsafe")
        os.fchmod(descriptor, 0o700)
        for child_name in os.listdir(descriptor):
            if checkpoint is not None:
                checkpoint()
            child = os.stat(child_name, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(child.st_mode):
                if child.st_uid != os.getuid():
                    raise ReleaseGenerationError(
                        "orphan environment generation contains unsafe data"
                    )
                os.unlink(child_name, dir_fd=descriptor)
                continue
            if stat.S_ISDIR(child.st_mode) and not stat.S_ISLNK(child.st_mode):
                reclaimed += _remove_private_tree_at(
                    descriptor,
                    child_name,
                    checkpoint=checkpoint,
                )
                continue
            if (
                not stat.S_ISREG(child.st_mode)
                or stat.S_ISLNK(child.st_mode)
                or child.st_uid != os.getuid()
                or child.st_nlink != 1
            ):
                raise ReleaseGenerationError("orphan environment generation contains unsafe data")
            child_fd = os.open(
                child_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            try:
                if PathIdentity.capture(os.fstat(child_fd)) != PathIdentity.capture(child):
                    raise ReleaseGenerationError("orphan environment generation changed")
                os.fchmod(child_fd, 0o600)
                reclaimed += child.st_size
            finally:
                os.close(child_fd)
            os.unlink(child_name, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_fd)
    return reclaimed


def _environment_entry(
    path: Path,
    root: Path,
    *,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
    checkpoint: Callable[[], None] | None = None,
    timeout_provider: Callable[[float], float] | None = None,
) -> dict[str, Any]:
    if checkpoint is not None:
        checkpoint()
    observed = path.lstat()
    relative = path.relative_to(root).as_posix()
    if stat.S_ISLNK(observed.st_mode):
        version, _abi = _python_facts(
            system_python,
            timeout_provider=timeout_provider,
        )
        major_minor = ".".join(version.split(".")[:2])
        allowed = _VENV_RELATIVE_SYMLINKS | {
            "bin/python",
            f"bin/python{major_minor}",
        }
        if relative not in allowed or observed.st_uid != os.getuid():
            raise ReleaseGenerationError("environment generation contains an unsafe symlink")
        target_text = os.readlink(path)
        target = Path(target_text)
        if relative != "bin/python" and (target.is_absolute() or ".." in target.parts):
            raise ReleaseGenerationError("environment generation symlink target is unsafe")
        try:
            resolved = path.resolve(strict=True)
            resolved_observed = resolved.lstat()
        except OSError as exc:
            raise ReleaseGenerationError("environment generation symlink is broken") from exc
        if relative.startswith("bin/python"):
            if (
                resolved != system_python
                or PathIdentity.capture(resolved_observed) != system_python_identity
                or _hash_file(
                    resolved,
                    label="system Python",
                    checkpoint=checkpoint,
                )
                != system_python_sha256
            ):
                raise ReleaseGenerationError("environment generation Python symlink target changed")
        elif relative == "lib64" and not resolved.is_relative_to(root):
            raise ReleaseGenerationError("environment generation lib64 escapes its root")
        return {
            "path": relative,
            "kind": "symlink",
            "mode": stat.S_IMODE(observed.st_mode),
            "size": observed.st_size,
            "mtime_ns": observed.st_mtime_ns,
            "ctime_ns": observed.st_ctime_ns,
            "sha256": "",
            "link_target": target_text,
            "resolved_target": str(resolved),
        }
    if stat.S_ISDIR(observed.st_mode):
        kind = "directory"
        digest = ""
        size = 0
    elif stat.S_ISREG(observed.st_mode) and not stat.S_ISLNK(observed.st_mode):
        if observed.st_nlink != 1:
            raise ReleaseGenerationError("environment generation contains a hardlink")
        kind = "file"
        digest = _hash_file(
            path,
            label=f"environment file {relative}",
            checkpoint=checkpoint,
        )
        size = observed.st_size
    else:
        raise ReleaseGenerationError("environment generation contains an unsafe object")
    if observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise ReleaseGenerationError("environment generation is not owner-private")
    return {
        "path": relative,
        "kind": kind,
        "mode": stat.S_IMODE(observed.st_mode),
        "size": size,
        "mtime_ns": observed.st_mtime_ns,
        "ctime_ns": observed.st_ctime_ns,
        "sha256": digest,
    }


def _freeze_environment(
    root: Path,
    *,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
    checkpoint: Callable[[], None] | None = None,
    timeout_provider: Callable[[float], float] | None = None,
) -> None:
    for current_root, directory_names, file_names in os.walk(root, topdown=False):
        if checkpoint is not None:
            checkpoint()
        current = Path(current_root)
        for name in file_names:
            if checkpoint is not None:
                checkpoint()
            path = current / name
            observed = path.lstat()
            if stat.S_ISLNK(observed.st_mode):
                _environment_entry(
                    path,
                    root,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                    timeout_provider=timeout_provider,
                )
                continue
            if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
                raise ReleaseGenerationError("environment generation contains a symlink")
            path.chmod(0o500 if observed.st_mode & stat.S_IXUSR else 0o400)
        for name in directory_names:
            if checkpoint is not None:
                checkpoint()
            path = current / name
            observed = path.lstat()
            if stat.S_ISLNK(observed.st_mode):
                _environment_entry(
                    path,
                    root,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                    timeout_provider=timeout_provider,
                )
                continue
            if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
                raise ReleaseGenerationError("environment generation contains a symlink")
            path.chmod(0o500)
    root.chmod(0o500)


def _rebind_environment_console_scripts(
    staging_path: Path,
    final_path: Path,
    source_venv: Path,
    *,
    checkpoint: Callable[[], None] | None = None,
) -> None:
    bin_path = staging_path / "bin"
    for path in bin_path.iterdir():
        if checkpoint is not None:
            checkpoint()
        observed = path.lstat()
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
            continue
        payload = path.read_bytes()
        lines = payload.splitlines(keepends=True)
        if lines and lines[0].startswith(b"#!"):
            interpreter = lines[0][2:].rstrip(b"\r\n")
            for root in (staging_path, source_venv):
                source_bin = str(root / "bin").encode() + b"/"
                if interpreter.startswith(source_bin):
                    name = interpreter[len(source_bin) :]
                    if name.startswith(b"python") and b"/" not in name:
                        suffix = lines[0][2 + len(interpreter) :]
                        lines[0] = b"#!" + str(final_path / "bin").encode() + b"/" + name + suffix
                        path.write_bytes(b"".join(lines))
                    break
        rebound = path.read_bytes()
        if str(staging_path).encode() in rebound or str(source_venv).encode() in rebound:
            raise ReleaseGenerationError(
                "environment console script retains a staging or source path"
            )


def _environment_manifest(
    root: Path,
    *,
    operation_id: str,
    transaction_kind: str,
    commit: str,
    generation_id: str,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
    uv_binding: dict[str, object] | None,
    checkpoint: Callable[[], None] | None = None,
    timeout_provider: Callable[[float], float] | None = None,
) -> dict[str, Any]:
    entry_arguments = {
        "system_python": system_python,
        "system_python_identity": system_python_identity,
        "system_python_sha256": system_python_sha256,
        "timeout_provider": timeout_provider,
    }
    entries = [_environment_entry(root, root, checkpoint=checkpoint, **entry_arguments)]
    paths: list[Path] = []
    for current_root, directory_names, file_names in os.walk(root):
        if checkpoint is not None:
            checkpoint()
        current = Path(current_root)
        for name in (*directory_names, *file_names):
            if checkpoint is not None:
                checkpoint()
            paths.append(current / name)
    for path in sorted(paths, key=lambda value: value.as_posix()):
        entries.append(_environment_entry(path, root, checkpoint=checkpoint, **entry_arguments))
    return {
        "schema_version": ENVIRONMENT_SCHEMA_VERSION,
        "operation_id": operation_id,
        "transaction_kind": transaction_kind,
        "commit": commit,
        "generation_id": generation_id,
        "environment_path": str(root),
        "system_python_path": str(system_python),
        "system_python_identity": asdict(system_python_identity),
        "system_python_sha256": system_python_sha256,
        "uv_binding": uv_binding or {},
        "entries": entries,
    }


def _verify_environment_manifest(
    root: Path,
    manifest: dict[str, Any],
    *,
    checkpoint: Callable[[], None] | None = None,
    timeout_provider: Callable[[float], float] | None = None,
) -> None:
    if int(manifest.get("schema_version", 0)) != ENVIRONMENT_SCHEMA_VERSION or manifest.get(
        "environment_path"
    ) != str(root):
        raise ReleaseGenerationError("environment generation manifest is invalid")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ReleaseGenerationError("environment generation manifest has no entries")
    try:
        system_python = _canonical(
            Path(str(manifest["system_python_path"])),
            label="system Python",
        )
        expected_system_identity = PathIdentity(**manifest["system_python_identity"])
        expected_system_sha256 = str(manifest["system_python_sha256"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ReleaseGenerationError("environment generation interpreter is malformed") from exc
    resolved_system, system_identity, system_sha256 = _verified_interpreter(
        system_python,
        label="system Python",
    )
    if (
        resolved_system != system_python
        or system_identity != expected_system_identity
        or system_sha256 != expected_system_sha256
    ):
        raise ReleaseGenerationError("environment generation interpreter changed")
    uv_binding = manifest.get("uv_binding", {})
    if not isinstance(uv_binding, dict):
        raise ReleaseGenerationError("environment generation uv binding is malformed")
    if uv_binding:
        try:
            uv_path = Path(str(uv_binding["physical_path"]))
            uv_identity = PathIdentity(**uv_binding["identity"])
            uv_sha256 = str(uv_binding["sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("environment generation uv binding is malformed") from exc
        observed_uv = _trusted_executable_binding(uv_path, label="release uv")
        if (
            PathIdentity(**observed_uv["identity"]) != uv_identity
            or observed_uv["sha256"] != uv_sha256
        ):
            raise ReleaseGenerationError("environment generation uv binding changed")
    expected_paths: set[str] = set()
    for entry in entries:
        if checkpoint is not None:
            checkpoint()
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise ReleaseGenerationError("environment generation manifest is malformed")
        relative = str(entry["path"])
        path = root if relative == "." else root / relative
        if relative in expected_paths or (relative and Path(relative).is_absolute()):
            raise ReleaseGenerationError("environment generation manifest path is invalid")
        expected_paths.add(relative)
        observed = _environment_entry(
            path,
            root,
            system_python=system_python,
            system_python_identity=system_identity,
            system_python_sha256=system_sha256,
            checkpoint=checkpoint,
            timeout_provider=timeout_provider,
        )
        if observed != entry:
            raise ReleaseGenerationError("environment generation content changed")
    actual_paths = {"."}
    for current_root, directory_names, file_names in os.walk(root):
        if checkpoint is not None:
            checkpoint()
        current = Path(current_root)
        for name in (*directory_names, *file_names):
            if checkpoint is not None:
                checkpoint()
            actual_paths.add((current / name).relative_to(root).as_posix())
    if actual_paths != expected_paths:
        raise ReleaseGenerationError("environment generation namespace changed")


class ReleaseGenerationAuthority:
    def __init__(
        self,
        *,
        repo: Path,
        lock_path: Path,
        lock_fd: int,
        python_path: Path,
        git_path: Path,
        writable: bool = False,
        mutation_hook: Callable[[str], None] | None = None,
        gc_grace_seconds: float | None = None,
        minimum_free_bytes: int | None = None,
        uv_path: Path | None = None,
        environment_builder: Callable[[Path], None] | None = None,
        immutable_code_root: Path | None = None,
        command_timeout_seconds: float = 900,
        overall_deadline_monotonic: float | None = None,
        cancellation_check: Callable[[], bool] | None = None,
    ) -> None:
        self.repo = _canonical(repo, label="release checkout")
        self.lock_path = _canonical(lock_path, label="deployment lock")
        self.marker_path = marker_path_for_lock(self.lock_path)
        self.intent_path = intent_path_for_lock(self.lock_path)
        self.prepared_intent_path = prepared_intent_path_for_lock(self.lock_path)
        self.initialization_path = initialization_path_for_lock(self.lock_path)
        self.commit_path = commit_path_for_lock(self.lock_path)
        self.handoff_path = self.lock_path.with_name(f"{self.lock_path.stem}.lab-handoff.json")
        self.environment_selector_path = environment_selector_path_for_lock(self.lock_path)
        self.environment_root = environment_root_for_lock(self.lock_path)
        self.lock_fd = lock_fd
        self.python_path = _canonical(python_path, label="release Python")
        self.git_path = _canonical(git_path, label="trusted Git")
        self.uv_path = None if uv_path is None else _canonical(uv_path, label="release uv")
        self.uv_binding = (
            None
            if self.uv_path is None
            else _trusted_executable_binding(self.uv_path, label="release uv")
        )
        self._environment_builder = environment_builder
        self.immutable_code_root = (
            None
            if immutable_code_root is None
            else _canonical(immutable_code_root, label="immutable release code root")
        )
        self._cancellation_check = cancellation_check or (lambda: False)
        if (
            not math.isfinite(command_timeout_seconds)
            or command_timeout_seconds <= 0
            or command_timeout_seconds > 7200
        ):
            raise ReleaseGenerationError("release environment command timeout is invalid")
        self.command_timeout_seconds = command_timeout_seconds
        self.overall_deadline_monotonic = (
            time.monotonic() + 1800
            if overall_deadline_monotonic is None
            else overall_deadline_monotonic
        )
        if not math.isfinite(self.overall_deadline_monotonic):
            raise ReleaseGenerationError("release environment overall deadline is invalid")
        self.writable = writable
        self._mutation_hook = mutation_hook or (lambda _stage: None)
        self.gc_grace_seconds = _nonnegative_float_setting(
            gc_grace_seconds,
            env_name="RQUANT_RELEASE_GENERATION_GC_GRACE_SECONDS",
            default=DEFAULT_GENERATION_GC_GRACE_SECONDS,
        )
        self.minimum_free_bytes = _nonnegative_int_setting(
            minimum_free_bytes,
            env_name="RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES",
            default=DEFAULT_GENERATION_MINIMUM_FREE_BYTES,
        )
        self._assert_lock()
        if self.writable:
            self._assert_exclusive_lock()

    def for_recovery(self, overall_deadline_monotonic: float) -> ReleaseGenerationAuthority:
        return ReleaseGenerationAuthority(
            repo=self.repo,
            lock_path=self.lock_path,
            lock_fd=self.lock_fd,
            python_path=self.python_path,
            git_path=self.git_path,
            writable=self.writable,
            mutation_hook=self._mutation_hook,
            gc_grace_seconds=self.gc_grace_seconds,
            minimum_free_bytes=self.minimum_free_bytes,
            uv_path=self.uv_path,
            environment_builder=self._environment_builder,
            immutable_code_root=self.immutable_code_root,
            command_timeout_seconds=self.command_timeout_seconds,
            overall_deadline_monotonic=min(
                self.overall_deadline_monotonic,
                overall_deadline_monotonic,
            ),
            cancellation_check=self._cancellation_check,
        )

    def _checkpoint(self) -> None:
        if self._cancellation_check():
            raise ReleaseGenerationError("immutable release environment build was cancelled")
        if time.monotonic() >= self.overall_deadline_monotonic:
            raise ReleaseGenerationError("immutable release environment build timed out")

    def _absolute_command_deadline(self, _cap_seconds: float) -> float:
        self._checkpoint()
        return self.overall_deadline_monotonic

    def _build_environment(
        self,
        destination: Path,
        *,
        system_python: Path,
        project_root: Path | None = None,
    ) -> None:
        self._checkpoint()
        if self._environment_builder is not None:
            self._environment_builder(destination)
            self._checkpoint()
            return
        if self.uv_path is None:
            raise ReleaseGenerationError("writable generation authority requires release uv")
        environment = {
            **os.environ,
            "VIRTUAL_ENV": str(destination),
            "UV_PROJECT_ENVIRONMENT": str(destination),
        }
        commands = (
            [
                str(self.uv_path),
                "venv",
                "--allow-existing",
                "--relocatable",
                "--python",
                str(system_python),
                str(destination),
            ],
            [str(self.uv_path), "sync", "--frozen", "--active"],
        )
        for command in commands:
            self._checkpoint()
            remaining = self.overall_deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise ReleaseGenerationError("immutable release environment build timed out")
            try:
                result = run_contained(
                    command,
                    cwd=self.repo if project_root is None else project_root,
                    deadline_monotonic=self.overall_deadline_monotonic,
                    env=environment,
                    cancellation_check=self._cancellation_check,
                    may_spawn_background_descendants=False,
                )
            except ReleaseGenerationError:
                raise
            except subprocess.TimeoutExpired as exc:
                raise ReleaseGenerationError(
                    "immutable release environment build timed out"
                ) from exc
            except (OSError, subprocess.SubprocessError) as exc:
                raise ReleaseGenerationError("immutable release environment build failed") from exc
            except ContainedProcessError as exc:
                if self._cancellation_check():
                    raise ReleaseGenerationError(
                        "immutable release environment build was cancelled"
                    ) from exc
                raise ReleaseGenerationError(
                    "immutable release environment build timed out or escaped containment"
                ) from exc
            if result.returncode != 0:
                diagnostic = (result.stderr or result.stdout or "no command output").strip()
                raise ReleaseGenerationError(
                    f"immutable release environment build failed: {diagnostic[:1000]}"
                )

    def _assert_lock(self) -> None:
        try:
            opened = os.fstat(self.lock_fd)
            active = self.lock_path.lstat()
        except OSError as exc:
            raise ReleaseGenerationError("deployment generation lock is unavailable") from exc
        if (
            PathIdentity.capture(opened) != PathIdentity.capture(active)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ReleaseGenerationError("deployment generation lock identity changed")

    def _assert_exclusive_lock(self) -> None:
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReleaseGenerationError(
                "writable generation authority requires the exclusive deployment lock"
            ) from exc

    def _facts(
        self,
        *,
        expected_commit: str,
        selector: EnvironmentSelector,
        manifest: dict[str, Any],
        verify_checkout: bool = True,
    ) -> ReleaseGenerationMarker:
        if len(expected_commit) != 40 or any(c not in "0123456789abcdef" for c in expected_commit):
            raise ReleaseGenerationError("release commit must be a lowercase full SHA")
        venv = _canonical(Path(selector.environment_path), label="release environment")
        code_root = generation_code_root(venv)
        _identity(code_root, label="immutable release code root", directory=True)
        if self.immutable_code_root is not None and self.immutable_code_root != code_root:
            raise ReleaseGenerationError("immutable release code selector is stale")
        commit = expected_commit
        if self.immutable_code_root is None and verify_checkout:
            commit = _git_output(
                self.repo,
                self.git_path,
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
                timeout_provider=self._absolute_command_deadline,
            )
            if commit != expected_commit:
                raise ReleaseGenerationError("release checkout commit does not match marker")
        uv_lock = code_root / "uv.lock"
        pyproject = code_root / "pyproject.toml"
        uv_hash = _hash_file(uv_lock, label="uv.lock", checkpoint=self._checkpoint)
        pyproject_hash = _hash_file(
            pyproject,
            label="pyproject.toml",
            checkpoint=self._checkpoint,
        )
        try:
            package_version = str(
                tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["version"]
            )
        except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
            raise ReleaseGenerationError("package version cannot be verified") from exc
        if selector.commit != commit or str(manifest.get("commit")) != commit:
            raise ReleaseGenerationError("environment generation commit is stale")
        _verify_environment_manifest(
            venv,
            manifest,
            checkpoint=self._checkpoint,
            timeout_provider=self._absolute_command_deadline,
        )
        venv_identity = _identity(venv, label="release venv", directory=True)
        selected_python = venv / "bin" / "python"
        try:
            system_python = Path(str(manifest["system_python_path"]))
            expected_system_identity = PathIdentity(**manifest["system_python_identity"])
            expected_system_sha256 = str(manifest["system_python_sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("release interpreter binding is malformed") from exc
        selected_observed = selected_python.lstat()
        if stat.S_ISLNK(selected_observed.st_mode):
            resolved_python, python_identity, python_sha256 = _verified_interpreter(
                selected_python,
                label="release venv Python",
            )
            if (
                resolved_python != system_python
                or python_identity != expected_system_identity
                or python_sha256 != expected_system_sha256
            ):
                raise ReleaseGenerationError("release venv Python binding changed")
        else:
            python_identity = _identity(
                selected_python,
                label="release venv Python",
                directory=False,
            )
        version, abi = _python_facts(
            selected_python,
            timeout_provider=self._absolute_command_deadline,
        )
        major_minor = ".".join(version.split(".")[:2])
        site_packages = venv / "lib" / f"python{major_minor}" / "site-packages"
        site_identity = _identity(
            site_packages,
            label="release site-packages",
            directory=True,
        )
        return ReleaseGenerationMarker(
            schema_version=MARKER_SCHEMA_VERSION,
            operation_id=selector.operation_id,
            transaction_kind=selector.transaction_kind,
            commit=commit,
            uv_lock_sha256=uv_hash,
            pyproject_sha256=pyproject_hash,
            package_version=package_version,
            python_version=version,
            python_abi=abi,
            venv_path=str(venv),
            venv_identity=venv_identity,
            pyvenv_cfg_sha256=_hash_file(venv / "pyvenv.cfg", label="pyvenv.cfg"),
            python_path=str(selected_python),
            python_identity=python_identity,
            site_packages_path=str(site_packages),
            site_packages_identity=site_identity,
            environment_generation_id=selector.generation_id,
            previous_generation_id=selector.previous_generation_id,
            environment_manifest_sha256=selector.manifest_sha256,
            published_at=datetime.now(UTC).isoformat(),
        )

    def _assert_root(self, descriptor: int, expected: PathIdentity) -> None:
        active = _identity(
            self.lock_path.parent,
            label="deployment authority root",
            directory=True,
        )
        if _object_key(active) != _object_key(expected) or _object_key(
            PathIdentity.capture(os.fstat(descriptor))
        ) != _object_key(expected):
            raise ReleaseGenerationError("deployment authority root identity changed")
        self._assert_lock()

    def _optional_private_payload(
        self,
        path: Path,
        *,
        maximum_bytes: int,
    ) -> dict[str, Any] | None:
        self._checkpoint()
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            try:
                payload, _identity_value = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=path.name,
                    maximum_bytes=maximum_bytes,
                    checkpoint=self._checkpoint,
                )
            except ReleaseGenerationRecordMissingError:
                self._assert_root(root_fd, root_identity)
                self._checkpoint()
                return None
            self._assert_root(root_fd, root_identity)
            self._checkpoint()
            return payload
        finally:
            os.close(root_fd)

    def _verify_installation_file_binding(
        self,
        binding: LabInstallationIdentity,
        *,
        expected_path: Path,
        private: bool,
    ) -> None:
        self._checkpoint()
        path = _canonical(expected_path, label="Lab installation plist")
        if binding.path != str(path):
            raise ReleaseGenerationError("Lab installation plist binding path changed")
        try:
            if path.resolve(strict=True) != path:
                raise ReleaseGenerationError("Lab installation plist path is not physical")
            parent_fd = os.open(
                path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise ReleaseGenerationError("Lab installation plist is unavailable") from exc
        descriptor = -1
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.getuid()
                or before.st_nlink != 1
                or (private and stat.S_IMODE(before.st_mode) != 0o600)
            ):
                raise ReleaseGenerationError("Lab installation plist identity is unsafe")
            digest = hashlib.sha256()
            total = 0
            while True:
                self._checkpoint()
                chunk = os.read(descriptor, min(64 * 1024, 1024 * 1024 + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > 1024 * 1024:
                    raise ReleaseGenerationError("Lab installation plist is too large")
                digest.update(chunk)
            after = os.fstat(descriptor)
            active = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            expected_identity = (binding.device, binding.inode)
            if (
                (before.st_dev, before.st_ino) != expected_identity
                or (after.st_dev, after.st_ino) != expected_identity
                or (active.st_dev, active.st_ino) != expected_identity
                or digest.hexdigest() != binding.sha256
            ):
                raise ReleaseGenerationError("Lab installation plist binding changed")
        except OSError as exc:
            raise ReleaseGenerationError("Lab installation plist binding is unavailable") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(parent_fd)
        self._checkpoint()

    def _validate_lab_installation_authorities(
        self,
        *,
        local_payload: dict[str, Any] | None,
        registered_payload: dict[str, Any] | None,
    ) -> tuple[LabLocalInstallationAuthority | None, LabRegisteredInstallationAuthority | None]:
        local = (
            None
            if local_payload is None
            else LabLocalInstallationAuthority.from_payload(local_payload)
        )
        registered = (
            None
            if registered_payload is None
            else LabRegisteredInstallationAuthority.from_payload(registered_payload)
        )
        if local is not None and registered is None:
            raise ReleaseGenerationError("registered Lab installation authority is missing")
        if registered is None:
            return local, None
        checkout_root = _canonical(
            Path(registered.checkout_root),
            label="Lab installation checkout",
        )
        if checkout_root != self.repo or checkout_root.resolve(strict=True) != checkout_root:
            raise ReleaseGenerationError("registered Lab installation checkout binding changed")
        runtime_root = _canonical(Path(registered.runtime_root), label="Lab runtime root")
        readiness_root = _canonical(Path(registered.readiness_root), label="Lab readiness root")
        runtime_fd, runtime_identity = _private_lock_root(runtime_root)
        os.close(runtime_fd)
        readiness_fd, _readiness_identity = _private_lock_root(readiness_root)
        os.close(readiness_fd)
        prepared = registered.prepared_authority
        if (
            readiness_root.parent != runtime_root
            or prepared.runtime_root != str(runtime_root)
            or (prepared.runtime_device, prepared.runtime_inode)
            != (runtime_identity.device, runtime_identity.inode)
        ):
            raise ReleaseGenerationError("registered Lab runtime authority binding changed")
        registered_plists = registered.plist_map()
        if local is None:
            if registered.is_installed:
                raise ReleaseGenerationError("local Lab installation authority is missing")
            for label in LAB_LAUNCHD_HANDOFF_LABELS:
                self._verify_installation_file_binding(
                    registered_plists[label],
                    expected_path=checkout_root / "deploy" / "launchd" / f"{label}.plist",
                    private=False,
                )
            return None, registered
        if (
            not registered.is_installed
            or local.code_sha != registered.registered_by_commit
            or local.environment_generation_id != registered.environment_generation_id
            or local.handoff_operation_id != registered.handoff_operation_id
        ):
            raise ReleaseGenerationError(
                "local and registered Lab installation authorities diverged"
            )
        launch_agents = _canonical(
            Path(local.launch_agents_dir),
            label="Lab launch agents directory",
        )
        launch_fd, _launch_identity = _private_lock_root(launch_agents)
        os.close(launch_fd)
        local_plists = local.plist_map()
        for label in LAB_LAUNCHD_HANDOFF_LABELS:
            local_binding = local_plists[f"{label}.plist"]
            if local_binding != registered_plists[label]:
                raise ReleaseGenerationError(
                    "local and registered Lab installation plist bindings diverged"
                )
            self._verify_installation_file_binding(
                local_binding,
                expected_path=launch_agents / f"{label}.plist",
                private=True,
            )
        return local, registered

    def _retained_environment_ids(self, environment_fd: int) -> set[str]:
        """Collect every generation named by a durable release/Lab authority."""

        del environment_fd
        self._checkpoint()
        references = GenerationReferenceCollector.create()
        deployment_intents: list[DeploymentIntent] = []
        selector_payload = self._optional_private_payload(
            self.environment_selector_path,
            maximum_bytes=MAX_MARKER_BYTES,
        )
        if selector_payload is not None:
            self._checkpoint()
            selector = EnvironmentSelector.from_payload(selector_payload)
            references.add(selector.generation_id, source="environment selector current")
            references.add(
                selector.previous_generation_id,
                source="environment selector previous",
                optional=True,
            )
        marker_payload = self._optional_private_payload(
            self.marker_path,
            maximum_bytes=MAX_MARKER_BYTES,
        )
        if marker_payload is not None:
            self._checkpoint()
            marker = ReleaseGenerationMarker.from_payload(marker_payload)
            references.add(
                marker.environment_generation_id,
                source="release marker current",
            )
            references.add(
                marker.previous_generation_id,
                source="release marker previous",
                optional=True,
            )
        commit_payload = self._optional_private_payload(
            self.commit_path,
            maximum_bytes=MAX_MARKER_BYTES,
        )
        if commit_payload is not None:
            self._checkpoint()
            commit_record = ReleaseGenerationCommit.from_payload(commit_payload)
            references.add(
                commit_record.environment_generation_id,
                source="release commit current",
            )
            references.add(
                commit_record.previous_generation_id,
                source="release commit previous",
                optional=True,
            )
        for path in (
            self.intent_path,
            self.prepared_intent_path,
            self.initialization_path,
        ):
            payload = self._optional_private_payload(path, maximum_bytes=MAX_INTENT_BYTES)
            if payload is None:
                continue
            self._checkpoint()
            intent = DeploymentIntent.from_payload(payload)
            if path in {self.intent_path, self.prepared_intent_path}:
                deployment_intents.append(intent)
            references.add(
                intent.previous_generation_id,
                source=f"{path.name} previous",
                optional=True,
            )
            references.add(
                _environment_generation_id(
                    operation_id=intent.operation_id,
                    commit=intent.previous_sha,
                ),
                source=f"{path.name} derived previous",
            )
            references.add(
                _environment_generation_id(
                    operation_id=intent.operation_id,
                    commit=intent.target_sha,
                ),
                source=f"{path.name} derived target",
            )

        intent_archive_pattern = re.compile(
            rf"{re.escape(self.intent_path.stem)}\.([0-9a-f]{{32}})\.completed\.json"
        )
        intent_archive_prefix = f"{self.intent_path.stem}."
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            for name in sorted(os.listdir(root_fd)):
                self._checkpoint()
                archive_match = intent_archive_pattern.fullmatch(name)
                if archive_match is None:
                    if name.startswith(intent_archive_prefix) and name.endswith(".completed.json"):
                        raise ReleaseGenerationError(
                            "completed deployment intent archive name is invalid"
                        )
                    continue
                payload, _archive_identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=name,
                    maximum_bytes=MAX_INTENT_BYTES,
                    checkpoint=self._checkpoint,
                )
                intent = DeploymentIntent.from_payload(payload)
                if intent.operation_id != archive_match.group(1) or intent.stage != "completed":
                    raise ReleaseGenerationError(
                        "completed deployment intent archive binding is invalid"
                    )
                deployment_intents.append(intent)
                references.add(
                    intent.previous_generation_id,
                    source=f"{name} previous",
                    optional=True,
                )
                references.add(
                    _environment_generation_id(
                        operation_id=intent.operation_id,
                        commit=intent.previous_sha,
                    ),
                    source=f"{name} derived previous",
                )
                references.add(
                    _environment_generation_id(
                        operation_id=intent.operation_id,
                        commit=intent.target_sha,
                    ),
                    source=f"{name} derived target",
                )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

        local_install_path = self.lock_path.with_name(
            f"{self.lock_path.stem}.lab-local-install.json"
        )
        registered_install_path = self.lock_path.with_name(
            f"{self.lock_path.stem}.lab-install.json"
        )
        local_install = self._optional_private_payload(
            local_install_path,
            maximum_bytes=MAX_INTENT_BYTES,
        )
        registered_install = self._optional_private_payload(
            registered_install_path,
            maximum_bytes=MAX_INTENT_BYTES,
        )
        local_authority, registered_authority = self._validate_lab_installation_authorities(
            local_payload=local_install,
            registered_payload=registered_install,
        )
        if local_authority is not None:
            references.add(
                local_authority.environment_generation_id,
                source="local Lab installation",
            )
        if registered_authority is not None and registered_authority.is_installed:
            references.add(
                registered_authority.environment_generation_id,
                source="registered Lab installation",
            )

        install_transaction_path = self.lock_path.with_name(
            f"{self.lock_path.stem}.lab-install-transaction.json"
        )
        install_transaction = self._optional_private_payload(
            install_transaction_path,
            maximum_bytes=MAX_INTENT_BYTES,
        )
        if install_transaction is not None:
            raise ReleaseGenerationError(
                "unfinished Lab installation transaction blocks generation GC"
            )

        if registered_authority is not None:
            readiness_root = _canonical(
                Path(registered_authority.readiness_root),
                label="Lab readiness root",
            )
            readiness_fd, readiness_identity = _private_lock_root(readiness_root)
            try:
                for label in LAB_LAUNCHD_HANDOFF_LABELS:
                    self._checkpoint()
                    try:
                        heartbeat, _heartbeat_identity = _read_private_json(
                            root_fd=readiness_fd,
                            root_path=readiness_root,
                            name=f"{label}.json",
                            maximum_bytes=MAX_MARKER_BYTES,
                            checkpoint=self._checkpoint,
                        )
                    except ReleaseGenerationRecordMissingError:
                        continue
                    expected_heartbeat_fields = {
                        "label",
                        "pid",
                        "operation_id",
                        "environment_generation_id",
                        "code_sha",
                        "started_at",
                        "heartbeat_at",
                        "heartbeat_monotonic",
                        "generation_lock_device",
                        "generation_lock_inode",
                    }
                    if (
                        set(heartbeat) != expected_heartbeat_fields
                        or heartbeat.get("label") != label
                        or type(heartbeat.get("pid")) is not int
                    ):
                        raise ReleaseGenerationError("Lab readiness authority is invalid")
                    references.add(
                        heartbeat.get("environment_generation_id"),
                        source=f"Lab readiness {label}",
                    )
                active_readiness = _identity(
                    readiness_root,
                    label="Lab readiness root",
                    directory=True,
                )
                if _object_key(active_readiness) != _object_key(readiness_identity):
                    raise ReleaseGenerationError("Lab readiness root changed during GC")
            finally:
                os.close(readiness_fd)

        operation_pattern = re.compile(
            rf"{re.escape(self.lock_path.stem)}\.lab-handoff\.([0-9a-f]{{32}})\.json"
        )
        completed_pattern = re.compile(
            rf"{re.escape(self.lock_path.stem)}\.lab-handoff\.([0-9a-f]{{32}})"
            rf"\.completed\.json"
        )
        bootout_pattern = re.compile(
            rf"{re.escape(self.lock_path.stem)}\.lab-handoff\.([0-9a-f]{{32}})"
            rf"\.[0-9a-f]{{16}}\.bootout\.json"
        )
        active_name = f"{self.lock_path.stem}.lab-handoff.json"
        handoff_records: dict[str, LabHandoffRecord] = {}
        bootout_operations: set[str] = set()
        for entry in sorted(self.lock_path.parent.iterdir()):
            self._checkpoint()
            operation_match = operation_pattern.fullmatch(entry.name)
            completed_match = completed_pattern.fullmatch(entry.name)
            bootout_match = bootout_pattern.fullmatch(entry.name)
            if entry.name != active_name and operation_match is None and completed_match is None:
                if bootout_match is not None:
                    evidence = self._optional_private_payload(
                        entry,
                        maximum_bytes=MAX_MARKER_BYTES,
                    )
                    expected_evidence_fields = {
                        "schema_version",
                        "operation_id",
                        "label",
                        "domain",
                        "action",
                    }
                    if (
                        evidence is None
                        or set(evidence) != expected_evidence_fields
                        or evidence.get("schema_version") != 1
                        or evidence.get("operation_id") != bootout_match.group(1)
                        or evidence.get("label") not in LAB_LAUNCHD_HANDOFF_LABELS
                        or evidence.get("action") != "bootout"
                        or type(evidence.get("domain")) is not str
                    ):
                        raise ReleaseGenerationError("Lab bootout evidence is invalid")
                    bootout_operations.add(bootout_match.group(1))
                continue
            payload = self._optional_private_payload(entry, maximum_bytes=MAX_INTENT_BYTES)
            if payload is None:
                raise ReleaseGenerationError("Lab handoff authority disappeared during GC")
            completed = payload.get("stage") == "completed"
            record = LabHandoffRecord.from_payload(payload, completed=completed)
            existing = handoff_records.get(record.operation_id)
            if existing is not None and existing != record:
                raise ReleaseGenerationError("Lab handoff authority records are inconsistent")
            handoff_records[record.operation_id] = record
            if completed:
                references.add(
                    record.environment_generation_id,
                    source=f"Lab handoff {record.operation_id}",
                )
            else:
                matching_intents = [
                    intent
                    for intent in deployment_intents
                    if record.operation_id
                    in {
                        intent.initial_handoff_operation_id,
                        *(
                            event["handoff_operation_id"]
                            for event in intent.stage_history
                            if event["stage"] == "handoff_rebound"
                        ),
                    }
                    and record.target_sha in {intent.previous_sha, intent.target_sha}
                ]
                if len(matching_intents) != 1:
                    raise ReleaseGenerationError(
                        "partial Lab handoff has no unique deployment intent authority"
                    )
        for record in handoff_records.values():
            if (
                record.supersedes_operation_id
                and record.supersedes_operation_id not in handoff_records
            ):
                raise ReleaseGenerationError("Lab handoff supersede ancestor is missing")
        if not bootout_operations.issubset(handoff_records):
            raise ReleaseGenerationError("Lab bootout evidence operation is missing")
        self._checkpoint()
        return references.values

    def _append_generation_gc_audit(self, payload: dict[str, Any]) -> None:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        name = f"{self.lock_path.stem}.generation-gc.jsonl"
        descriptor = -1
        try:
            self._assert_root(root_fd, root_identity)
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            opened = os.fstat(descriptor)
            active = (self.lock_path.parent / name).lstat()
            if (
                PathIdentity.capture(opened) != PathIdentity.capture(active)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise ReleaseGenerationError("generation GC audit is unsafe")
            _write_all(
                descriptor,
                canonical_json_bytes(payload, trailing_newline=True),
            )
            os.fsync(descriptor)
            self._assert_root(root_fd, root_identity)
        except ReleaseGenerationError:
            raise
        except OSError as exc:
            raise ReleaseGenerationError("generation GC audit cannot be written") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)

    def _remove_orphan_environment_manifest(self, generation_id: str) -> None:
        self._checkpoint()
        manifest_path = environment_manifest_path_for_lock(self.lock_path, generation_id)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            try:
                payload, identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=manifest_path.name,
                    maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                    checkpoint=self._checkpoint,
                )
            except ReleaseGenerationRecordMissingError:
                self._assert_root(root_fd, root_identity)
                self._checkpoint()
                return
            self._checkpoint()
            if payload.get("generation_id") != generation_id or payload.get(
                "environment_path"
            ) != str(self.environment_root / generation_id):
                raise ReleaseGenerationError("orphan environment manifest binding changed")
            self._mutation_hook("before_environment_gc_manifest_delete")
            self._checkpoint()
            active = manifest_path.lstat()
            if PathIdentity.capture(active) != identity:
                raise ReleaseGenerationError("orphan environment manifest identity changed")
            self._assert_root(root_fd, root_identity)
            os.unlink(manifest_path.name, dir_fd=root_fd)
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def garbage_collect_environments(
        self,
        *,
        reason: str,
        required_bytes: int = 0,
    ) -> GenerationGcMetrics:
        if not self.writable:
            raise ReleaseGenerationError(
                "read-only generation authority cannot collect environments"
            )
        if required_bytes < 0:
            raise ReleaseGenerationError("generation disk requirement cannot be negative")
        self._assert_lock()
        self._assert_exclusive_lock()
        environment_fd, environment_identity = self._ensure_environment_root()
        free_before = shutil.disk_usage(self.environment_root).free
        scanned = 0
        deleted = 0
        reclaimed = 0
        try:
            self._checkpoint()
            retained = self._retained_environment_ids(environment_fd)
            self._checkpoint()
            cutoff = datetime.now(UTC).timestamp() - self.gc_grace_seconds
            for name in sorted(os.listdir(environment_fd)):
                self._checkpoint()
                generation_match = GENERATION_ID_PATTERN.fullmatch(name)
                building_match = BUILDING_GENERATION_PATTERN.fullmatch(name)
                if generation_match is None and building_match is None:
                    continue
                generation_id = name if generation_match is not None else building_match.group(1)
                scanned += 1
                observed = os.stat(name, dir_fd=environment_fd, follow_symlinks=False)
                if (
                    not stat.S_ISDIR(observed.st_mode)
                    or stat.S_ISLNK(observed.st_mode)
                    or observed.st_uid != os.getuid()
                ):
                    raise ReleaseGenerationError("release environment generation is unsafe")
                if generation_id in retained or observed.st_mtime > cutoff:
                    continue
                self._mutation_hook("before_environment_gc_delete")
                active_environment = _identity(
                    self.environment_root,
                    label="release environment root",
                    directory=True,
                )
                if _object_key(active_environment) != _object_key(
                    environment_identity
                ) or _object_key(PathIdentity.capture(os.fstat(environment_fd))) != _object_key(
                    environment_identity
                ):
                    raise ReleaseGenerationError("release environment root identity changed")
                if generation_match is not None:
                    self._remove_orphan_environment_manifest(generation_id)
                reclaimed += _remove_private_tree_at(
                    environment_fd,
                    name,
                    checkpoint=self._checkpoint,
                )
                os.fsync(environment_fd)
                deleted += 1
            free_after = shutil.disk_usage(self.environment_root).free
            required_free = self.minimum_free_bytes + required_bytes
            status = "ok" if free_after >= required_free else "disk_budget_blocked"
            metrics = GenerationGcMetrics(
                scanned_generations=scanned,
                deleted_generations=deleted,
                reclaimed_bytes=reclaimed,
                free_bytes_before=free_before,
                free_bytes_after=free_after,
                required_free_bytes=required_free,
                retained_generation_ids=tuple(sorted(retained)),
            )
            self._append_generation_gc_audit(
                {
                    "deleted_generations": deleted,
                    "free_bytes_after": free_after,
                    "free_bytes_before": free_before,
                    "reason": reason,
                    "reclaimed_bytes": reclaimed,
                    "required_free_bytes": required_free,
                    "retained_generation_ids": list(metrics.retained_generation_ids),
                    "scanned_generations": scanned,
                    "status": status,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )
            if free_after < required_free:
                raise ReleaseGenerationError(
                    "release environment disk budget is insufficient: "
                    f"free={free_after} required={required_free}"
                )
            return metrics
        finally:
            os.close(environment_fd)

    def _read_marker(self) -> ReleaseGenerationMarker:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        descriptor = -1
        try:
            descriptor = os.open(
                self.marker_path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise ReleaseGenerationError("release generation marker is unsafe")
            payload = os.read(descriptor, MAX_MARKER_BYTES + 1)
            if len(payload) > MAX_MARKER_BYTES:
                raise ReleaseGenerationError("release generation marker is too large")
            active = self.marker_path.lstat()
            if PathIdentity.capture(opened) != PathIdentity.capture(active):
                raise ReleaseGenerationError("release generation marker identity changed")
            self._assert_root(root_fd, root_identity)
            return ReleaseGenerationMarker.from_payload(
                strict_canonical_json_loads(payload, trailing_newline=True)
            )
        except FileNotFoundError as exc:
            raise ReleaseGenerationError("release generation marker is missing") from exc
        except (OSError, StrictJsonError) as exc:
            raise ReleaseGenerationError("release generation marker cannot be read") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)

    def commit_generation(
        self,
        *,
        operation_id: str,
        transaction_kind: str,
    ) -> ReleaseGenerationCommit:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot commit")
        self._assert_lock()
        marker = self._read_marker()
        if marker.operation_id != operation_id or marker.transaction_kind != transaction_kind:
            raise ReleaseGenerationError("release marker transaction binding changed")
        transaction = self._transaction_record(
            operation_id=operation_id,
            transaction_kind=transaction_kind,
        )
        if transaction.stage != "completed":
            raise ReleaseGenerationError("release transaction is not completed")
        selector, _selector_identity = self._read_selector()
        manifest, _manifest_identity = self._read_environment_manifest(selector)
        current = self._facts(
            expected_commit=marker.commit,
            selector=selector,
            manifest=manifest,
        )
        if self._comparable(current) != self._comparable(marker):
            raise ReleaseGenerationError("release marker changed before commit")
        record = ReleaseGenerationCommit(
            schema_version=COMMIT_SCHEMA_VERSION,
            operation_id=operation_id,
            transaction_kind=transaction_kind,
            commit=marker.commit,
            marker_sha256=marker.content_hash(),
            transaction_sha256=transaction.content_hash(),
            environment_generation_id=marker.environment_generation_id,
            previous_generation_id=marker.previous_generation_id,
            environment_manifest_sha256=marker.environment_manifest_sha256,
            committed_at=datetime.now(UTC).isoformat(),
        )
        try:
            existing = self._read_commit_record()
        except ReleaseGenerationRecordMissingError:
            existing_identity = None
        else:
            if replace(existing, committed_at=record.committed_at) == record:
                return existing
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                _payload, existing_identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=self.commit_path.name,
                    maximum_bytes=MAX_MARKER_BYTES,
                )
                self._assert_root(root_fd, root_identity)
            finally:
                os.close(root_fd)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._mutation_hook("before_generation_commit")
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.commit_path.name,
                payload=asdict(record),
                require_absent=existing_identity is None,
                expected_identity=existing_identity,
                maximum_bytes=MAX_MARKER_BYTES,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        self._mutation_hook("generation_committed")
        return record

    @staticmethod
    def _comparable(marker: ReleaseGenerationMarker) -> dict[str, Any]:
        payload = asdict(marker)
        payload.pop("published_at", None)
        return payload

    def _read_selector(self) -> tuple[EnvironmentSelector, PathIdentity]:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.environment_selector_path.name,
                maximum_bytes=MAX_MARKER_BYTES,
            )
            self._assert_root(root_fd, root_identity)
            return EnvironmentSelector.from_payload(payload), identity
        finally:
            os.close(root_fd)

    def _read_environment_manifest(
        self,
        selector: EnvironmentSelector,
    ) -> tuple[dict[str, Any], PathIdentity]:
        expected = environment_manifest_path_for_lock(self.lock_path, selector.generation_id)
        if selector.manifest_name != expected.name:
            raise ReleaseGenerationError("environment manifest name is not generation-bound")
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=selector.manifest_name,
                maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                checkpoint=self._checkpoint,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        if _payload_hash(payload, checkpoint=self._checkpoint) != selector.manifest_sha256:
            raise ReleaseGenerationError("environment generation manifest hash changed")
        if (
            str(payload.get("operation_id")) != selector.operation_id
            or str(payload.get("transaction_kind")) != selector.transaction_kind
            or str(payload.get("generation_id")) != selector.generation_id
        ):
            raise ReleaseGenerationError("environment generation manifest binding changed")
        return payload, identity

    def selected_environment(self) -> EnvironmentSelector:
        self._assert_lock()
        selector, _identity_value = self._read_selector()
        manifest, _manifest_identity = self._read_environment_manifest(selector)
        _verify_environment_manifest(
            Path(selector.environment_path),
            manifest,
            checkpoint=self._checkpoint,
            timeout_provider=self._absolute_command_deadline,
        )
        self._assert_lock()
        return selector

    def _read_commit_record(self) -> ReleaseGenerationCommit:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, _identity_value = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.commit_path.name,
                maximum_bytes=MAX_MARKER_BYTES,
            )
            self._assert_root(root_fd, root_identity)
            return ReleaseGenerationCommit.from_payload(payload)
        finally:
            os.close(root_fd)

    def _transaction_record(
        self,
        *,
        operation_id: str,
        transaction_kind: str,
    ) -> DeploymentIntent:
        if transaction_kind == "initialization":
            try:
                active, _active_identity = self._read_intent_record(self.intent_path)
            except ReleaseGenerationRecordMissingError:
                pass
            else:
                raise ReleaseGenerationError(
                    "active deployment intent blocks initialization generation: "
                    f"stage={active.stage}"
                )
            record, _identity_value = self._read_intent_record(self.initialization_path)
        elif transaction_kind == "deployment":
            try:
                record, _identity_value = self._read_intent_record(self.intent_path)
            except ReleaseGenerationRecordMissingError:
                archive = self.intent_path.with_name(
                    f"{self.intent_path.stem}.{operation_id}.completed.json"
                )
                record, _identity_value = self._read_intent_record(archive)
        else:
            raise ReleaseGenerationError("release transaction kind is invalid")
        if record.operation_id != operation_id:
            raise ReleaseGenerationError("release transaction operation id changed")
        return record

    def _verify_deployment_handoff(
        self,
        transaction: DeploymentIntent,
        *,
        marker: ReleaseGenerationMarker,
        selector: EnvironmentSelector,
        provisional_label: str | None,
        provisional_installation_operation_id: str | None,
    ) -> None:
        if not transaction.handoff_operation_id:
            return
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            install_name = f"{self.lock_path.stem}.lab-install.json"
            installation_payload, installation_file_identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=install_name,
                maximum_bytes=MAX_INTENT_BYTES,
            )
            installation_checkout_root = installation_payload.get("checkout_root")
            if type(installation_checkout_root) is not str:
                raise ReleaseGenerationError("Lab installation checkout authority is invalid")
            installation = LabInstallationIdentity(
                path=str(self.lock_path.with_name(install_name)),
                sha256=hashlib.sha256(canonical_json_bytes(installation_payload)).hexdigest(),
                device=installation_file_identity.device,
                inode=installation_file_identity.inode,
            )
            operation_name = (
                f"{self.lock_path.stem}.lab-handoff.{transaction.handoff_operation_id}.json"
            )
            operation_payload, _operation_identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=operation_name,
                maximum_bytes=MAX_INTENT_BYTES,
            )
            active_payload, _active_identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=f"{self.lock_path.stem}.lab-handoff.json",
                maximum_bytes=MAX_INTENT_BYTES,
            )
            records_completed = operation_payload.get("stage") == "completed"
            if provisional_label is None and not records_completed:
                raise ReleaseGenerationError("deployment handoff record is not completed")
            if records_completed:
                proof_payload, _proof_identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=(
                        f"{self.lock_path.stem}.lab-handoff."
                        f"{transaction.handoff_operation_id}.completed.json"
                    ),
                    maximum_bytes=MAX_INTENT_BYTES,
                )
                record = LabHandoffRecord.from_payload(proof_payload, completed=True)
                operation = LabHandoffRecord.from_payload(operation_payload, completed=True)
                if record != operation:
                    raise ReleaseGenerationError(
                        "completed Lab handoff proof records are inconsistent"
                    )
            else:
                record = LabHandoffRecord.from_payload(operation_payload, completed=False)
            active_completed = active_payload.get("stage") == "completed"
            active = LabHandoffRecord.from_payload(
                active_payload,
                completed=active_completed,
            )
            if active.operation_id != record.operation_id or record != active:
                raise ReleaseGenerationError("active Lab handoff record is inconsistent")
            ancestors: list[LabHandoffRecord] = []
            superseded_operation_id = record.supersedes_operation_id
            while superseded_operation_id:
                ancestor_payload, _ancestor_identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=(f"{self.lock_path.stem}.lab-handoff.{superseded_operation_id}.json"),
                    maximum_bytes=MAX_INTENT_BYTES,
                )
                ancestor_completed = ancestor_payload.get("stage") == "completed"
                ancestor = LabHandoffRecord.from_payload(
                    ancestor_payload,
                    completed=ancestor_completed,
                )
                ancestors.append(ancestor)
                superseded_operation_id = ancestor.supersedes_operation_id
            completed_proofs: list[LabHandoffRecord] = []
            for chain_record in (record, *ancestors):
                try:
                    proof_payload, _proof_identity = _read_private_json(
                        root_fd=root_fd,
                        root_path=self.lock_path.parent,
                        name=(
                            f"{self.lock_path.stem}.lab-handoff."
                            f"{chain_record.operation_id}.completed.json"
                        ),
                        maximum_bytes=MAX_INTENT_BYTES,
                    )
                except ReleaseGenerationRecordMissingError:
                    continue
                completed_proofs.append(
                    LabHandoffRecord.from_payload(proof_payload, completed=True)
                )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        if record.operation_id != transaction.handoff_operation_id:
            raise ReleaseGenerationError("deployment handoff operation is stale")
        validate_lab_handoff_supersede_chain(
            record=record,
            ancestors=tuple(ancestors),
            intent=transaction,
            installation_identity=installation,
            checkout_root=installation_checkout_root,
            expected_labels=transaction.handoff_labels,
            completed_proofs=tuple(completed_proofs),
        )
        if records_completed:
            validate_ready_deployment_handoff_authority(
                intent=transaction,
                marker=marker,
                selector=selector,
                handoff_operation_id=record.operation_id,
                handoff_labels=record.labels,
                generation_operation_id=record.generation_operation_id,
                environment_generation_id=record.environment_generation_id,
                code_sha=record.code_sha,
                action=record.action,
                target_ref=record.target_ref,
                target_sha=record.target_sha,
                release_profile=record.release_profile,
                lifecycle_mode=record.lifecycle_mode,
            )
            return
        if provisional_label is not None and (
            provisional_label in record.restarted_labels and record.stage == "restarting"
        ):
            return
        if (
            provisional_installation_operation_id == record.operation_id
            and record.stage == "stopped"
            and record.stopped_labels == record.labels
            and not record.restarted_labels
        ):
            return
        raise ReleaseGenerationError("deployment handoff is not completed")

    def verify(
        self,
        *,
        expected_commit: str,
        provisional_handoff_label: str | None = None,
        provisional_installation_operation_id: str | None = None,
    ) -> ReleaseGenerationMarker:
        self._checkpoint()
        self._assert_lock()
        published = self._read_marker()
        self._checkpoint()
        if published.schema_version != MARKER_SCHEMA_VERSION:
            raise ReleaseGenerationError("release generation marker schema is unsupported")
        if published.commit != expected_commit:
            raise ReleaseGenerationError("release generation marker commit is stale")
        transaction = self._transaction_record(
            operation_id=published.operation_id,
            transaction_kind=published.transaction_kind,
        )
        provisional = (
            provisional_handoff_label is not None
            and published.transaction_kind == "deployment"
            and transaction.stage == "awaiting_readiness"
        )
        if transaction.stage != "completed" and not provisional:
            raise ReleaseGenerationError("release transaction is not completed")
        selector, _selector_identity = self._read_selector()
        self._verify_deployment_handoff(
            transaction,
            marker=published,
            selector=selector,
            provisional_label=provisional_handoff_label,
            provisional_installation_operation_id=provisional_installation_operation_id,
        )
        committed: ReleaseGenerationCommit | None = None
        if not provisional:
            try:
                committed = self._read_commit_record()
            except ReleaseGenerationError as exc:
                raise ReleaseGenerationError("release generation commit record is missing") from exc
        manifest, _manifest_identity = self._read_environment_manifest(selector)
        self._checkpoint()
        current = self._facts(
            expected_commit=expected_commit,
            selector=selector,
            manifest=manifest,
        )
        if self._comparable(published) != self._comparable(current):
            if published.uv_lock_sha256 != current.uv_lock_sha256:
                raise ReleaseGenerationError("uv.lock no longer matches release marker")
            if published.venv_identity != current.venv_identity:
                raise ReleaseGenerationError("release venv identity no longer matches marker")
            raise ReleaseGenerationError("release generation marker is stale")
        if committed is not None and (
            committed.operation_id != published.operation_id
            or committed.transaction_kind != published.transaction_kind
            or committed.commit != published.commit
            or committed.marker_sha256 != published.content_hash()
            or committed.transaction_sha256 != transaction.content_hash()
            or committed.environment_generation_id != published.environment_generation_id
            or committed.previous_generation_id != published.previous_generation_id
            or committed.environment_manifest_sha256 != published.environment_manifest_sha256
        ):
            raise ReleaseGenerationError("release generation commit record is stale")
        if self.immutable_code_root is None:
            if _hash_file(self.repo / "uv.lock", label="uv.lock") != published.uv_lock_sha256:
                raise ReleaseGenerationError("uv.lock no longer matches release marker")
            _assert_tracked_clean(
                self.repo,
                self.git_path,
                timeout_provider=self._absolute_command_deadline,
            )
        self._assert_lock()
        return published

    def _read_intent_record(self, path: Path) -> tuple[DeploymentIntent, PathIdentity]:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=path.name,
                maximum_bytes=MAX_INTENT_BYTES,
            )
            self._assert_root(root_fd, root_identity)
            return DeploymentIntent.from_payload(payload), identity
        finally:
            os.close(root_fd)

    def _create_intent_record(self, path: Path, intent: DeploymentIntent) -> None:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=path.name,
                payload=asdict(intent),
                require_absent=True,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def _update_intent_record(
        self,
        path: Path,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        current, identity = self._read_intent_record(path)
        if current.operation_id != operation_id:
            raise ReleaseGenerationError("deployment intent operation id changed")
        updated = current.advance(stage=stage, restarted_services=restarted_services)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=path.name,
                payload=asdict(updated),
                require_absent=False,
                expected_identity=identity,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        return updated

    def begin_deployment_intent(
        self,
        *,
        previous_sha: str,
        target_sha: str,
        target_ref: str,
        changed_files: tuple[str, ...],
        restart_services: tuple[str, ...],
        active_services: tuple[str, ...],
        active_timers: tuple[str, ...],
        marker_generation: str = "",
        previous_generation_id: str = "",
        handoff_operation_id: str = "",
        handoff_labels: tuple[str, ...] = (),
    ) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot create intent")
        self._assert_lock()
        if not marker_generation:
            marker = self.verify(expected_commit=previous_sha)
            marker_generation = marker.content_hash()
            previous_generation_id = marker.environment_generation_id
        if len(previous_generation_id) != 64:
            raise ReleaseGenerationError("deployment previous generation is unavailable")
        try:
            current, completed_identity = self._read_intent_record(self.intent_path)
        except ReleaseGenerationRecordMissingError:
            pass
        else:
            if current.stage != "completed":
                raise ReleaseGenerationError("an incomplete deployment intent already exists")
            archive = self.intent_path.with_name(
                f"{self.intent_path.stem}.{current.operation_id}.completed.json"
            )
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                self._assert_root(root_fd, root_identity)
                if PathIdentity.capture(self.intent_path.lstat()) != completed_identity:
                    raise ReleaseGenerationError("completed deployment intent changed")
                os.replace(
                    self.intent_path.name,
                    archive.name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                )
                os.fsync(root_fd)
                self._assert_root(root_fd, root_identity)
            finally:
                os.close(root_fd)
        intent = DeploymentIntent.create(
            previous_sha=previous_sha,
            target_sha=target_sha,
            target_ref=target_ref,
            changed_files=changed_files,
            restart_services=restart_services,
            active_services=active_services,
            active_timers=active_timers,
            marker_generation=marker_generation,
            previous_generation_id=previous_generation_id,
            handoff_operation_id=handoff_operation_id,
            handoff_labels=handoff_labels,
        )
        self._create_intent_record(self.intent_path, intent)
        return intent

    def read_deployment_intent(self) -> DeploymentIntent:
        self._assert_lock()
        intent, _identity_value = self._read_intent_record(self.intent_path)
        return intent

    def read_prepared_deployment_intent(self) -> DeploymentIntent:
        self._assert_lock()
        intent, _identity_value = self._read_intent_record(self.prepared_intent_path)
        return intent

    def adopt_prepared_deployment_intent(self, *, operation_id: str) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot adopt intent")
        self._assert_lock()
        try:
            current, current_identity = self._read_intent_record(self.intent_path)
        except ReleaseGenerationRecordMissingError:
            current = None
            current_identity = None
        if current is not None and current.operation_id == operation_id:
            try:
                self._read_intent_record(self.prepared_intent_path)
            except ReleaseGenerationRecordMissingError:
                return current
            raise ReleaseGenerationError("adopted deployment intent left a prepared record")
        prepared, prepared_identity = self._read_intent_record(self.prepared_intent_path)
        if prepared.operation_id != operation_id or prepared.stage not in {
            "planned",
            "recovery_started",
        }:
            raise ReleaseGenerationError("prepared deployment intent binding changed")
        if current is not None and current.stage != "completed":
            raise ReleaseGenerationError("an incomplete deployment intent already exists")
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._assert_root(root_fd, root_identity)
            if current is not None:
                assert current_identity is not None
                archive = self.intent_path.with_name(
                    f"{self.intent_path.stem}.{current.operation_id}.completed.json"
                )
                if archive.exists() or archive.is_symlink():
                    archived, _archived_identity = self._read_intent_record(archive)
                    if archived != current:
                        raise ReleaseGenerationError("completed deployment intent archive changed")
                    os.unlink(self.intent_path.name, dir_fd=root_fd)
                else:
                    if PathIdentity.capture(self.intent_path.lstat()) != current_identity:
                        raise ReleaseGenerationError("completed deployment intent changed")
                    os.rename(
                        self.intent_path.name,
                        archive.name,
                        src_dir_fd=root_fd,
                        dst_dir_fd=root_fd,
                    )
                os.fsync(root_fd)
            if PathIdentity.capture(self.prepared_intent_path.lstat()) != prepared_identity:
                raise ReleaseGenerationError("prepared deployment intent changed")
            os.rename(
                self.prepared_intent_path.name,
                self.intent_path.name,
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
        except OSError as exc:
            raise ReleaseGenerationError("prepared deployment intent cannot be adopted") from exc
        finally:
            os.close(root_fd)
        adopted, _identity_value = self._read_intent_record(self.intent_path)
        if adopted != prepared:
            raise ReleaseGenerationError("adopted deployment intent changed")
        return adopted

    def update_deployment_intent(
        self,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot update intent")
        self._assert_lock()
        return self._update_intent_record(
            self.intent_path,
            operation_id=operation_id,
            stage=stage,
            restarted_services=restarted_services,
        )

    def rebind_deployment_handoff(
        self,
        *,
        operation_id: str,
        handoff_operation_id: str,
        handoff_labels: tuple[str, ...],
    ) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot update intent")
        self._assert_lock()
        current, identity = self._read_intent_record(self.intent_path)
        if current.operation_id != operation_id:
            raise ReleaseGenerationError("deployment intent operation id changed")
        updated = current.rebind_handoff(
            handoff_operation_id=handoff_operation_id,
            handoff_labels=handoff_labels,
        )
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.intent_path.name,
                payload=asdict(updated),
                require_absent=False,
                expected_identity=identity,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        return updated

    def begin_initialization(self, *, target_sha: str) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot initialize")
        self._assert_lock()
        try:
            current, _identity_value = self._read_intent_record(self.initialization_path)
        except ReleaseGenerationRecordMissingError:
            pass
        else:
            if current.stage == "completed":
                raise ReleaseGenerationError("release generation initialization already completed")
            if current.target_sha != target_sha:
                raise ReleaseGenerationError("initialization target is already pinned")
            return current
        intent = DeploymentIntent.create(
            previous_sha=target_sha,
            target_sha=target_sha,
            target_ref=target_sha,
            changed_files=(),
            restart_services=(),
            active_services=(),
            active_timers=(),
            previous_generation_id="",
            stage="initializing",
        )
        self._create_intent_record(self.initialization_path, intent)
        return intent

    def read_initialization(self) -> DeploymentIntent:
        self._assert_lock()
        initialization, _identity_value = self._read_intent_record(self.initialization_path)
        return initialization

    def complete_initialization(self, *, operation_id: str) -> DeploymentIntent:
        return self._update_intent_record(
            self.initialization_path,
            operation_id=operation_id,
            stage="completed",
        )

    def _ensure_environment_root(self) -> tuple[int, PathIdentity]:
        if self.environment_root.exists() or self.environment_root.is_symlink():
            identity = _identity(
                self.environment_root,
                label="release environment root",
                directory=True,
            )
            if stat.S_IMODE(identity.mode) != 0o700:
                raise ReleaseGenerationError("release environment root must have mode 0700")
        else:
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                self._assert_root(root_fd, root_identity)
                os.mkdir(self.environment_root.name, 0o700, dir_fd=root_fd)
                os.fsync(root_fd)
                self._assert_root(root_fd, root_identity)
            except OSError as exc:
                raise ReleaseGenerationError("release environment root cannot be created") from exc
            finally:
                os.close(root_fd)
        descriptor = os.open(
            self.environment_root,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        identity = _identity(
            self.environment_root,
            label="release environment root",
            directory=True,
        )
        if PathIdentity.capture(os.fstat(descriptor)) != identity:
            os.close(descriptor)
            raise ReleaseGenerationError("release environment root identity changed")
        return descriptor, identity

    def _publish_environment(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        transaction_kind: str,
        previous_generation_id: str,
        activate_selector: bool = True,
        allow_gc: bool = True,
    ) -> tuple[EnvironmentSelector, dict[str, Any]]:
        self._checkpoint()
        source_venv = self.repo / ".venv"
        _identity(source_venv, label="source release venv", directory=True)
        if not self.python_path.is_relative_to(source_venv):
            raise ReleaseGenerationError("deployment Python is outside source release venv")
        system_python, system_python_identity, system_python_sha256 = _venv_system_interpreter(
            self.python_path,
            timeout_provider=self._absolute_command_deadline,
        )
        source_bytes = _private_tree_size(
            source_venv,
            system_python=system_python,
            system_python_identity=system_python_identity,
            system_python_sha256=system_python_sha256,
            checkpoint=self._checkpoint,
            timeout_provider=self._absolute_command_deadline,
        )
        if allow_gc:
            self.garbage_collect_environments(
                reason=f"pre-publish:{transaction_kind}",
                required_bytes=source_bytes,
            )
        elif shutil.disk_usage(self.environment_root.parent).free < (
            source_bytes + self.minimum_free_bytes
        ):
            raise ReleaseGenerationError("release generation disk budget is insufficient")
        generation_id = _environment_generation_id(
            operation_id=operation_id,
            commit=expected_commit,
        )
        final_path = self.environment_root / generation_id
        environment_fd, environment_identity = self._ensure_environment_root()
        staging_name = f".{generation_id}.{secrets.token_hex(8)}.building"
        staging_path = self.environment_root / staging_name
        manifest_path = environment_manifest_path_for_lock(self.lock_path, generation_id)
        try:
            manifest: dict[str, Any] | None = None
            if final_path.exists() or final_path.is_symlink():
                _identity(final_path, label="release environment generation", directory=True)
                try:
                    root_fd, root_identity = _private_lock_root(self.lock_path.parent)
                    try:
                        manifest, _manifest_identity = _read_private_json(
                            root_fd=root_fd,
                            root_path=self.lock_path.parent,
                            name=manifest_path.name,
                            maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                            checkpoint=self._checkpoint,
                        )
                        self._assert_root(root_fd, root_identity)
                    finally:
                        os.close(root_fd)
                except ReleaseGenerationRecordMissingError:
                    pass
            else:
                os.mkdir(staging_name, 0o700, dir_fd=environment_fd)
                try:
                    self._checkpoint()
                    code_root = generation_code_root(staging_path)
                    _materialize_release_code(
                        repo=self.repo,
                        git_path=self.git_path,
                        expected_commit=expected_commit,
                        destination=code_root,
                        checkpoint=self._checkpoint,
                        timeout_provider=self._absolute_command_deadline,
                    )
                    self._build_environment(
                        staging_path,
                        system_python=system_python,
                        project_root=code_root,
                    )
                    _rebind_environment_console_scripts(
                        staging_path,
                        final_path,
                        source_venv,
                        checkpoint=self._checkpoint,
                    )
                    self._checkpoint()
                    self._mutation_hook("environment_staged")
                    active_environment = _identity(
                        self.environment_root,
                        label="release environment root",
                        directory=True,
                    )
                    if _object_key(active_environment) != _object_key(
                        environment_identity
                    ) or _object_key(PathIdentity.capture(os.fstat(environment_fd))) != _object_key(
                        environment_identity
                    ):
                        raise ReleaseGenerationError("release environment root identity changed")
                    os.rename(
                        staging_name,
                        generation_id,
                        src_dir_fd=environment_fd,
                        dst_dir_fd=environment_fd,
                    )
                    os.fsync(environment_fd)
                    _freeze_environment(
                        final_path,
                        system_python=system_python,
                        system_python_identity=system_python_identity,
                        system_python_sha256=system_python_sha256,
                        checkpoint=self._checkpoint,
                        timeout_provider=self._absolute_command_deadline,
                    )
                except BaseException:
                    if staging_path.exists() and not staging_path.is_symlink():
                        _remove_private_tree_at(environment_fd, staging_name)
                    if manifest is None and final_path.exists() and not final_path.is_symlink():
                        _remove_private_tree_at(environment_fd, generation_id)
                    raise
            if manifest is None:
                _freeze_environment(
                    final_path,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                    checkpoint=self._checkpoint,
                    timeout_provider=self._absolute_command_deadline,
                )
                active_environment = _identity(
                    self.environment_root,
                    label="release environment root",
                    directory=True,
                )
                if _object_key(active_environment) != _object_key(
                    environment_identity
                ) or _object_key(PathIdentity.capture(os.fstat(environment_fd))) != _object_key(
                    environment_identity
                ):
                    raise ReleaseGenerationError("release environment root identity changed")
                self._mutation_hook("environment_generation_ready")
                manifest = _environment_manifest(
                    final_path,
                    operation_id=operation_id,
                    transaction_kind=transaction_kind,
                    commit=expected_commit,
                    generation_id=generation_id,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                    uv_binding=self.uv_binding,
                    checkpoint=self._checkpoint,
                    timeout_provider=self._absolute_command_deadline,
                )
                manifest_hash = _payload_hash(manifest, checkpoint=self._checkpoint)
                root_fd, root_identity = _private_lock_root(self.lock_path.parent)
                try:
                    self._assert_root(root_fd, root_identity)
                    _write_private_json(
                        root_fd=root_fd,
                        root_path=self.lock_path.parent,
                        name=manifest_path.name,
                        payload=manifest,
                        require_absent=True,
                        maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                        checkpoint=self._checkpoint,
                    )
                    self._assert_root(root_fd, root_identity)
                finally:
                    os.close(root_fd)
                self._mutation_hook("environment_sealed")
            else:
                if (
                    str(manifest.get("operation_id")) != operation_id
                    or str(manifest.get("transaction_kind")) != transaction_kind
                    or str(manifest.get("commit")) != expected_commit
                    or str(manifest.get("generation_id")) != generation_id
                ):
                    raise ReleaseGenerationError("existing environment generation is stale")
                _verify_environment_manifest(
                    final_path,
                    manifest,
                    checkpoint=self._checkpoint,
                    timeout_provider=self._absolute_command_deadline,
                )
                manifest_hash = _payload_hash(manifest, checkpoint=self._checkpoint)
            selector = EnvironmentSelector(
                schema_version=ENVIRONMENT_SCHEMA_VERSION,
                operation_id=operation_id,
                transaction_kind=transaction_kind,
                commit=expected_commit,
                generation_id=generation_id,
                previous_generation_id=previous_generation_id,
                environment_path=str(final_path),
                manifest_name=manifest_path.name,
                manifest_sha256=manifest_hash,
                published_at=datetime.now(UTC).isoformat(),
            )
            if activate_selector:
                try:
                    _prior, selector_identity = self._read_selector()
                except ReleaseGenerationRecordMissingError:
                    selector_identity = None
                root_fd, root_identity = _private_lock_root(self.lock_path.parent)
                try:
                    self._assert_root(root_fd, root_identity)
                    _write_private_json(
                        root_fd=root_fd,
                        root_path=self.lock_path.parent,
                        name=self.environment_selector_path.name,
                        payload=asdict(selector),
                        require_absent=selector_identity is None,
                        expected_identity=selector_identity,
                        maximum_bytes=MAX_MARKER_BYTES,
                        checkpoint=self._checkpoint,
                    )
                    self._assert_root(root_fd, root_identity)
                finally:
                    os.close(root_fd)
                self._mutation_hook("environment_selector_published")
            return selector, manifest
        finally:
            os.close(environment_fd)

    def prepare_environment_candidate(
        self,
        *,
        expected_commit: str,
        operation_id: str,
    ) -> ReleaseGenerationMarker:
        """Build and verify an unselected deployment generation while readers stay live."""

        self._assert_lock()
        intent, _identity_value = self._read_intent_record(self.prepared_intent_path)
        if (
            intent.operation_id != operation_id
            or intent.target_sha != expected_commit
            or intent.stage != "planned"
        ):
            raise ReleaseGenerationError("prepared generation candidate intent changed")
        selector, manifest = self._publish_environment(
            expected_commit=expected_commit,
            operation_id=operation_id,
            transaction_kind="deployment",
            previous_generation_id=intent.previous_generation_id,
            activate_selector=False,
            allow_gc=False,
        )
        return self._facts(
            expected_commit=expected_commit,
            selector=selector,
            manifest=manifest,
            verify_checkout=False,
        )

    def invalidate(self) -> None:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot invalidate")
        self._assert_lock()
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._mutation_hook("before_marker_invalidate")
            self._assert_root(root_fd, root_identity)
            with suppress(FileNotFoundError):
                os.unlink(self.marker_path.name, dir_fd=root_fd)
            with suppress(FileNotFoundError):
                os.unlink(self.commit_path.name, dir_fd=root_fd)
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def publish(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        transaction_kind: str,
    ) -> ReleaseGenerationMarker:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot publish")
        self._assert_lock()
        _assert_tracked_clean(
            self.repo,
            self.git_path,
            timeout_provider=self._absolute_command_deadline,
        )
        transaction = self._transaction_record(
            operation_id=operation_id,
            transaction_kind=transaction_kind,
        )
        expected_stage = (
            "initializing" if transaction_kind == "initialization" else "timers_restored"
        )
        if transaction.stage != expected_stage:
            raise ReleaseGenerationError("release transaction is not ready for marker publication")
        target_commit = (
            transaction.previous_sha
            if transaction_kind == "deployment" and expected_commit == transaction.previous_sha
            else transaction.target_sha
        )
        if target_commit != expected_commit:
            raise ReleaseGenerationError("release transaction target does not match marker")
        selector, manifest = self._publish_environment(
            expected_commit=expected_commit,
            operation_id=operation_id,
            transaction_kind=transaction_kind,
            previous_generation_id=transaction.previous_generation_id,
        )
        marker = self._facts(
            expected_commit=expected_commit,
            selector=selector,
            manifest=manifest,
        )
        payload = canonical_json_bytes(asdict(marker), trailing_newline=True)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        temporary_name = f".{self.marker_path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        descriptor = -1
        renamed = False
        completed = False
        try:
            self._assert_root(root_fd, root_identity)
            descriptor = os.open(
                temporary_name,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            _write_all(descriptor, payload)
            os.fsync(descriptor)
            self._mutation_hook("marker_temp_fsynced")
            self._assert_root(root_fd, root_identity)
            _verify_temporary_payload(
                descriptor,
                expected_payload=payload,
                expected_marker=marker,
            )
            self._assert_root(root_fd, root_identity)
            os.replace(
                temporary_name,
                self.marker_path.name,
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            renamed = True
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
            active = self.marker_path.lstat()
            if PathIdentity.capture(os.fstat(descriptor)) != PathIdentity.capture(active):
                raise ReleaseGenerationError("published generation marker identity changed")
            completed = True
            self._mutation_hook("marker_published")
            return marker
        finally:
            if not completed and renamed:
                try:
                    active = self.marker_path.lstat()
                    if descriptor >= 0 and PathIdentity.capture(
                        os.fstat(descriptor)
                    ) == PathIdentity.capture(active):
                        os.unlink(self.marker_path.name, dir_fd=root_fd)
                        os.fsync(root_fd)
                except FileNotFoundError:
                    pass
            elif not renamed:
                with suppress(FileNotFoundError):
                    os.unlink(temporary_name, dir_fd=root_fd)
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)


if __name__ == "__main__":
    raise SystemExit("release_generation is a library, not a command")
