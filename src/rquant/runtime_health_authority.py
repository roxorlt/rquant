"""Point-in-time runtime health aggregation for serving publication."""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import ValidationError, field_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.runtime_service_control import (
    RuntimeServiceHealth,
    RuntimeServiceHeartbeat,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
)
from rquant.runtime_serving_snapshot import (
    RUNTIME_HEALTH_DATASET_ID,
    RuntimeHealthPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload

_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_DEFAULT_MAX_BYTES = 1024 * 1024


def _service_summary(
    services: tuple[RuntimeServiceHealth, ...],
    *tokens: str,
) -> tuple[str, str | None, datetime | None]:
    selected = next(
        (
            service
            for service in services
            if any(token in service.service_id.lower() for token in tokens)
        ),
        None,
    )
    if selected is None:
        return "unavailable", None, None
    substate = "stale" if selected.stale else "healthy"
    last_at = None if selected.heartbeat is None else selected.heartbeat.heartbeat_at
    return selected.status.value, substate, last_at


def _dashboard_summary_projection(
    *,
    services: tuple[RuntimeServiceHealth, ...],
    observed_at: datetime,
    source_receipts: dict[str, str],
) -> tuple[ServingProjectionPayload, str]:
    def timestamp(value: datetime | None) -> str | None:
        return None if value is None else normalize_aware_utc(value).isoformat()

    monitor_state, monitor_substate, monitor_last_at = _service_summary(
        services,
        "monitor",
        "market-minute",
    )
    daily_state, daily_exec_status, daily_last_at = _service_summary(
        services,
        "daily",
        "close",
    )
    dashboard_state, _dashboard_substate, _dashboard_last_at = _service_summary(
        services,
        "dashboard",
    )
    projection = ServingProjectionPayload(
        table_name="dashboard_summary",
        available_at=observed_at,
        rows=(
            {
                "snapshot_key": "current",
                "latest_daily_bar": None,
                "latest_screen": None,
                "daily_bar_rows": None,
                "monitor_event_rows": None,
                "minute_bar_rows": None,
                "minute_codes": None,
                "minute_min_time": None,
                "minute_max_time": None,
                "host_name": os.uname().nodename,
                "monitor_state": monitor_state,
                "monitor_substate": monitor_substate,
                "monitor_next_at": None,
                "monitor_last_at": timestamp(monitor_last_at),
                "daily_state": daily_state,
                "daily_exec_status": daily_exec_status,
                "daily_next_at": None,
                "daily_last_at": timestamp(daily_last_at),
                "dashboard_state": dashboard_state,
                "backup_snapshot_at": None,
                "backup_source_bytes": None,
                "backup_compressed_bytes": None,
                "backup_last_download_at": None,
                "backup_last_download_ip": None,
                "backup_last_download_bytes": None,
            },
        ),
    )
    generation_id = canonical_sha256(
        {
            "contract": "runtime-health-dashboard-summary/v1",
            "observed_at": observed_at,
            "source_receipts": dict(sorted(source_receipts.items())),
            "projection": projection,
        }
    )
    return projection, generation_id


class RuntimeHealthAuthorityIntegrityError(RuntimeError):
    """A heartbeat authority cannot be trusted as a point-in-time source."""


class RuntimeHealthControlSource(RuntimeContractModel):
    control_root: Path
    spec: RuntimeServiceSpec

    @field_validator("control_root")
    @classmethod
    def normalize_control_root(cls, value: Path) -> Path:
        path = Path(value)
        if not path.is_absolute():
            raise ValueError("control_root must be absolute")
        return Path(os.path.abspath(path))


@dataclass(frozen=True)
class _DirectoryHandle:
    descriptor: int
    parent_descriptor: int | None
    name: str
    identity: tuple[int, int, int, int]


def _directory_identity(observed: os.stat_result) -> tuple[int, int, int, int]:
    return (observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid)


def _file_identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _close_directory_chain(chain: list[_DirectoryHandle]) -> None:
    for handle in reversed(chain):
        with suppress(OSError):
            os.close(handle.descriptor)


def _open_directory_chain(path: Path) -> list[_DirectoryHandle] | None:
    chain: list[_DirectoryHandle] = []
    try:
        anchor_fd = os.open(path.anchor, _DIRECTORY_FLAGS)
        anchor_stat = os.fstat(anchor_fd)
        chain.append(
            _DirectoryHandle(
                descriptor=anchor_fd,
                parent_descriptor=None,
                name=path.anchor,
                identity=_directory_identity(anchor_stat),
            )
        )
        for component in path.parts[1:]:
            parent_fd = chain[-1].descriptor
            descriptor = -1
            try:
                descriptor = os.open(component, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            except FileNotFoundError:
                _close_directory_chain(chain)
                return None
            try:
                observed = os.fstat(descriptor)
                entry = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                if not stat.S_ISDIR(observed.st_mode) or _directory_identity(observed) != (
                    _directory_identity(entry)
                ):
                    raise RuntimeHealthAuthorityIntegrityError(
                        "runtime control directory changed during read"
                    )
                chain.append(
                    _DirectoryHandle(
                        descriptor=descriptor,
                        parent_descriptor=parent_fd,
                        name=component,
                        identity=_directory_identity(observed),
                    )
                )
                descriptor = -1
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
        return chain
    except RuntimeHealthAuthorityIntegrityError:
        _close_directory_chain(chain)
        raise
    except OSError as exc:
        _close_directory_chain(chain)
        raise RuntimeHealthAuthorityIntegrityError(
            "runtime control path is unsafe or contains a symlink"
        ) from exc


def _open_child_directory(
    chain: list[_DirectoryHandle],
    name: str,
) -> bool:
    parent_fd = chain[-1].descriptor
    try:
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise RuntimeHealthAuthorityIntegrityError(
            "runtime control path is unsafe or contains a symlink"
        ) from exc
    try:
        observed = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(observed.st_mode) or _directory_identity(observed) != (
            _directory_identity(entry)
        ):
            raise RuntimeHealthAuthorityIntegrityError(
                "runtime control directory changed during read"
            )
        chain.append(
            _DirectoryHandle(
                descriptor=descriptor,
                parent_descriptor=parent_fd,
                name=name,
                identity=_directory_identity(observed),
            )
        )
        return True
    except Exception:
        os.close(descriptor)
        raise


def _verify_directory_chain(chain: list[_DirectoryHandle]) -> None:
    for handle in chain:
        try:
            observed = os.fstat(handle.descriptor)
        except OSError as exc:
            raise RuntimeHealthAuthorityIntegrityError(
                "runtime control directory changed during read"
            ) from exc
        if _directory_identity(observed) != handle.identity:
            raise RuntimeHealthAuthorityIntegrityError(
                "runtime control directory changed during read"
            )
        if handle.parent_descriptor is None:
            continue
        try:
            entry = os.stat(
                handle.name,
                dir_fd=handle.parent_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise RuntimeHealthAuthorityIntegrityError(
                "runtime control directory changed during read"
            ) from exc
        if _directory_identity(entry) != handle.identity:
            raise RuntimeHealthAuthorityIntegrityError(
                "runtime control directory changed during read"
            )


def _read_descriptor_bytes(descriptor: int, *, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    remaining = max_bytes + 1
    while remaining:
        chunk = os.read(descriptor, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _heartbeat_name(spec: RuntimeServiceSpec) -> str:
    identity = canonical_sha256({"service_id": spec.service_id})
    return f"{identity}.json"


def _read_heartbeat(
    source: RuntimeHealthControlSource,
    *,
    max_bytes: int,
) -> RuntimeServiceHeartbeat | None:
    chain = _open_directory_chain(source.control_root)
    if chain is None:
        return None
    descriptor = -1
    try:
        if not _open_child_directory(chain, "heartbeats"):
            return None
        parent_fd = chain[-1].descriptor
        name = _heartbeat_name(source.spec)
        try:
            descriptor = os.open(name, _FILE_FLAGS, dir_fd=parent_fd)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RuntimeHealthAuthorityIntegrityError(
                f"runtime heartbeat is unsafe or contains a symlink: {source.spec.service_id}"
            ) from exc
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise RuntimeHealthAuthorityIntegrityError(
                f"runtime heartbeat is unsafe: {source.spec.service_id}"
            )
        payload = _read_descriptor_bytes(descriptor, max_bytes=max_bytes)
        after = os.fstat(descriptor)
        try:
            entry = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise RuntimeHealthAuthorityIntegrityError(
                f"runtime heartbeat changed during read: {source.spec.service_id}"
            ) from exc
        if (
            len(payload) > max_bytes
            or _file_identity(before) != _file_identity(after)
            or _file_identity(after) != _file_identity(entry)
        ):
            raise RuntimeHealthAuthorityIntegrityError(
                f"runtime heartbeat changed during read: {source.spec.service_id}"
            )
        _verify_directory_chain(chain)
        try:
            heartbeat = RuntimeServiceHeartbeat.model_validate_json(payload)
        except (ValidationError, ValueError) as exc:
            raise RuntimeHealthAuthorityIntegrityError(
                f"runtime heartbeat is invalid: {source.spec.service_id}"
            ) from exc
        if (
            heartbeat.service_id != source.spec.service_id
            or heartbeat.spec_fingerprint != source.spec.identity
        ):
            raise RuntimeHealthAuthorityIntegrityError(
                f"runtime heartbeat does not match service spec: {source.spec.service_id}"
            )
        return heartbeat
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        _close_directory_chain(chain)


def _validate_heartbeat_time(
    heartbeat: RuntimeServiceHeartbeat,
    *,
    observed_at: datetime,
) -> None:
    timestamps = (
        heartbeat.started_at,
        heartbeat.heartbeat_at,
        heartbeat.last_success_at,
        heartbeat.stopped_at,
    )
    if any(value is not None and value > observed_at for value in timestamps):
        raise RuntimeHealthAuthorityIntegrityError(
            f"runtime heartbeat contains future evidence: {heartbeat.service_id}"
        )
    if heartbeat.heartbeat_at < heartbeat.started_at:
        raise RuntimeHealthAuthorityIntegrityError(
            f"runtime heartbeat precedes service start: {heartbeat.service_id}"
        )
    if heartbeat.last_success_at is not None and heartbeat.last_success_at > heartbeat.heartbeat_at:
        raise RuntimeHealthAuthorityIntegrityError(
            f"runtime success time exceeds heartbeat time: {heartbeat.service_id}"
        )
    if heartbeat.stopped_at is not None and not (
        heartbeat.started_at <= heartbeat.stopped_at <= heartbeat.heartbeat_at
    ):
        raise RuntimeHealthAuthorityIntegrityError(
            f"runtime stop time is outside heartbeat lifetime: {heartbeat.service_id}"
        )


class RuntimeHealthSourceReader:
    """Read dedicated service controls into one deterministic PIT health result."""

    def __init__(
        self,
        *,
        sources: tuple[RuntimeHealthControlSource, ...],
        serving_service_id: str,
        max_heartbeat_bytes: int = _DEFAULT_MAX_BYTES,
    ) -> None:
        if not serving_service_id.strip():
            raise ValueError("serving_service_id cannot be empty")
        if max_heartbeat_bytes < 1:
            raise ValueError("max_heartbeat_bytes must be positive")
        if any(not isinstance(source, RuntimeHealthControlSource) for source in sources):
            raise TypeError("sources must contain RuntimeHealthControlSource values")
        visible = tuple(
            source for source in sources if source.spec.service_id != serving_service_id
        )
        if not visible:
            raise ValueError("at least one non-serving runtime health source is required")
        service_ids = tuple(source.spec.service_id for source in visible)
        if len(service_ids) != len(set(service_ids)):
            raise ValueError("runtime health sources must have unique service ids")
        control_roots = tuple(source.control_root for source in visible)
        if len(control_roots) != len(set(control_roots)):
            raise ValueError("runtime health sources must have exclusive control roots")
        self.sources = tuple(sorted(visible, key=lambda source: source.spec.service_id))
        self.max_heartbeat_bytes = max_heartbeat_bytes

    def __call__(self, observed_at: datetime, /) -> SourceReadResult:
        observed = normalize_aware_utc(observed_at)
        services: list[RuntimeServiceHealth] = []
        reasons: list[str] = []
        event_times: list[datetime] = []
        source_receipts: dict[str, str] = {}
        for source in self.sources:
            heartbeat = _read_heartbeat(source, max_bytes=self.max_heartbeat_bytes)
            source_receipts[source.spec.service_id] = canonical_sha256(
                {
                    "contract": "runtime-health-source-receipt/v1",
                    "control_root": str(source.control_root),
                    "spec": source.spec.model_dump(mode="json"),
                    "heartbeat": (None if heartbeat is None else heartbeat.model_dump(mode="json")),
                    "observed_at": observed,
                }
            )
            if heartbeat is None:
                services.append(
                    RuntimeServiceHealth(
                        service_id=source.spec.service_id,
                        plane=source.spec.plane,
                        status=RuntimeServiceStatus.MISSING,
                        stale=True,
                        observed_at=observed,
                    )
                )
                reasons.append(f"missing:{source.spec.service_id}")
                continue
            _validate_heartbeat_time(heartbeat, observed_at=observed)
            stale = observed - heartbeat.heartbeat_at > source.spec.stale_after
            status = RuntimeServiceStatus.DEGRADED if stale else heartbeat.status
            services.append(
                RuntimeServiceHealth(
                    service_id=source.spec.service_id,
                    plane=source.spec.plane,
                    status=status,
                    stale=stale,
                    observed_at=observed,
                    heartbeat=heartbeat,
                )
            )
            event_times.append(heartbeat.heartbeat_at)
            if stale:
                reasons.append(f"stale:{source.spec.service_id}")
            elif heartbeat.status is not RuntimeServiceStatus.RUNNING:
                reasons.append(f"{heartbeat.status.value}:{source.spec.service_id}")

        status = FreshnessStatus.FRESH if not reasons else FreshnessStatus.DEGRADED
        reason = None if not reasons else ",".join(sorted(reasons))
        service_snapshot = tuple(services)
        dashboard_projection, dashboard_generation_id = _dashboard_summary_projection(
            services=service_snapshot,
            observed_at=observed,
            source_receipts=source_receipts,
        )
        values: dict[str, object] = {
            "dataset_id": RUNTIME_HEALTH_DATASET_ID,
            "sequence": int(observed.timestamp() * 1_000_000),
            "event_time": max(event_times, default=observed),
            "published_at": observed,
            "status": status,
            "reason": reason,
            "payload": RuntimeHealthPayload(
                runtime_services=service_snapshot,
                projections=(dashboard_projection,),
                dashboard_summary_observed_at=observed,
                dashboard_summary_generation_id=dashboard_generation_id,
                dashboard_summary_source_receipts=source_receipts,
            ),
        }
        values["generation_id"] = canonical_sha256(values)
        return SourceReadResult.model_validate(values)


class RuntimeHealthAuthorityPublisher:
    """Publish one verified runtime-health read through the generic source authority."""

    def __init__(
        self,
        *,
        reader: RuntimeHealthSourceReader,
        publisher: ServingSourceAuthorityPublisher,
    ) -> None:
        if not isinstance(reader, RuntimeHealthSourceReader):
            raise TypeError("reader must be RuntimeHealthSourceReader")
        if not isinstance(publisher, ServingSourceAuthorityPublisher):
            raise TypeError("publisher must be ServingSourceAuthorityPublisher")
        if publisher.dataset_id != RUNTIME_HEALTH_DATASET_ID:
            raise ValueError("publisher must own the runtime_health dataset")
        if publisher.payload_kind != "runtime_health":
            raise ValueError("publisher must own the runtime_health payload kind")
        self.reader = reader
        self.publisher = publisher

    def publish(self, observed_at: datetime) -> ServingSourceAuthorityPointer:
        return self.publisher.publish(self.reader(observed_at))


__all__ = [
    "RuntimeHealthAuthorityIntegrityError",
    "RuntimeHealthAuthorityPublisher",
    "RuntimeHealthControlSource",
    "RuntimeHealthSourceReader",
]
