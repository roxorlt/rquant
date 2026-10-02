"""Immutable bounded source snapshots for page-only Serving projections."""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from tempfile import mkdtemp
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Self

if TYPE_CHECKING:
    from rquant.factor.tracking import FactorTrackingIdentity
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
from loguru import logger
from pydantic import (
    ConfigDict,
    Field,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.backfill_plan_artifact import (
    MAX_BACKFILL_PLAN_BYTES,
    parse_daily_bar_backfill_plan_bytes,
)
from rquant.backfill_plan_core import DailyBarBackfillPlan
from rquant.backfill_plan_job_projection import (
    BackfillPlanProgressEvent,
    BackfillPlanProgressState,
    read_backfill_plan_job_snapshot,
    validate_backfill_plan_progress,
)
from rquant.backfill_plan_projection import (
    BACKFILL_PLAN_PROJECTION_TABLES,
    MAX_DISCOVERABLE_BACKFILL_PLANS,
    MAX_PREVIEW_BACKFILL_PLANS,
    decode_backfill_plan_archive_row,
    project_backfill_plans,
)
from rquant.builtin_presets import BUILTIN_PRESET_SCREENS
from rquant.canvas_publication_receipt import (
    CanvasPublicationCatalogRecord,
    CanvasPublicationKeyring,
    CanvasPublicationReceipt,
    CanvasPublicationReceiptStore,
    canvas_catalog_record_hash,
    canvas_command_hash,
    canvas_publication_effect_id,
    canvas_publication_generation_id,
    canvas_publication_receipt_id,
    canvas_source_identity_hash,
)
from rquant.data_audit_contracts import REPORT_JOB_PROJECTION_TABLES, REPORT_PROJECTION_TABLES
from rquant.data_audit_projection import (
    MAX_AUDIT_ISSUES as _MAX_AUDIT_ISSUES,
)
from rquant.data_audit_projection import (
    DataAuditIssueProjectionRow,
    DataAuditStatusProjectionRow,
)
from rquant.data_audit_report import MAX_REPORT_BYTES, parse_data_audit_report_bytes
from rquant.data_audit_report_job_projection import (
    DataAuditReportJobProgress,
    DataAuditReportSuccessfulTask,
    project_data_audit_report_job,
    read_data_audit_report_job_snapshot,
    validate_data_audit_report_job_progress,
)
from rquant.data_audit_report_jobs import DataAuditReportJobEvent
from rquant.data_audit_report_projection import project_data_audit_report
from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
from rquant.factor.registry import (
    FactorDefinitionRegistry,
    FactorRegistryError,
    FactorRegistryIdentity,
)
from rquant.factor.serving_projection import (
    FACTOR_DEFINITION_PROJECTION_TABLES,
    project_factor_definition_projections,
    validate_factor_definition_projections,
)
from rquant.formula_market_job_projection import (
    FORMULA_MARKET_PROJECTION_TABLES,
    project_formula_market_job,
    read_formula_market_job_snapshot,
    validate_formula_market_projections,
)
from rquant.formula_pool_serving_projection import (
    FormulaPoolServingConfig,
    FormulaPoolSourceWatch,
    read_formula_pool_projections,
    validate_formula_pool_projections,
)
from rquant.notification_state import (
    NotificationProjectionAuthoritySnapshot,
    NotificationProjectionPublication,
    NotificationProjectionSourceReceipt,
    NotificationStateStore,
)
from rquant.page_control import (
    AlertAcknowledgment,
    CanvasCurrentHead,
    PageControlOutbox,
    PageControlStatus,
    parse_page_control_command,
    read_canvas_current_head,
)
from rquant.pool_definition_projection import PoolMutation, build_pool_definition_rows
from rquant.pool_member_return import calculate_adjusted_pool_return
from rquant.pool_membership import PoolDayEvidence, PoolMemberClose, compute_pool_membership
from rquant.pool_result_receipt import ScreenRunReceipt, member_price_digest, member_set_digest
from rquant.price_alert_rule_store import _SCHEMA_COLUMNS, PriceAlertRuleRepository
from rquant.readside_replica_gate import (
    UNLIMITED_READ_PROFILE,
    ReplicaRead,
    ReplicaReadGate,
    ReplicaReadProfile,
)
from rquant.research_gate import (
    ResearchGateFailure,
    ResearchGateRequest,
    evaluate_store_research_gate,
    research_gate_metadata_ready,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_read_interrupt import READ_INTERRUPTS, interruptible_read
from rquant.serving_alert_projection import (
    AlertAckAuthoritySnapshot,
    build_ack_source_projections,
)
from rquant.serving_manual_watchlist_projection import (
    MAX_MANUAL_WATCHLIST_ROWS as _MAX_MANUAL_WATCHLIST_ROWS,
)
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    build_manual_watchlist_projections,
    validate_manual_watchlist_projections,
)
from rquant.serving_price_alert_rule_projection import (
    MAX_PRICE_ALERT_RULE_ROWS as _MAX_PRICE_ALERT_RULE_ROWS,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleAuthoritySnapshot,
    PriceAlertRuleProjectionRow,
    build_price_alert_rule_projections,
    validate_price_alert_rule_projections,
)
from rquant.serving_read_models import ProjectionScalar, ServingProjectionPayload
from rquant.storage.duckdb import DuckDBStore
from rquant.strict_json import StrictJsonError, strict_json_loads

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_SCREEN_PRESETS = 256
_MAX_MINUTE_SOURCES = 127
_MAX_CANVAS_HITS = 20_000
_MAX_CANVAS_DEFINITIONS = 512
_MAX_CANVAS_DEFINITION_BYTES = 64 * 1024
_MAX_CANVAS_CATALOG_BYTES = 2 * 1024 * 1024
_MAX_POOL_DEFINITIONS = 512
_MAX_POOL_DEFINITION_BYTES = 64 * 1024
_MAX_POOL_CATALOG_BYTES = 2 * 1024 * 1024
_MAX_POOL_MUTATIONS = 8_192
_MAX_POOL_AUDIT_BYTES = 8 * 1024 * 1024
_MAX_POOL_AUDIT_CELL_CHARS = 128 * 1024
_MAX_RUN_RECEIPTS = 512
_MAX_RUN_RECEIPT_SOURCE_ROWS = 16_384
_MAX_RUN_LINEAGE_NODES = 512
_MAX_RUN_MEMBERS_PER_POOL = 20_000
_MAX_RUN_TOTAL_MEMBERS = 100_000
_RUN_RECEIPT_LOOKBACK_DAYS = 30
_MAX_POOL_MEMBERSHIP_SOURCE_RECEIPTS = 128
_MAX_POOL_MEMBERSHIP_SOURCE_MEMBERS = 4_096
_MAX_EXACT_PARENT_CALENDAR_SPAN_DAYS = 730
_RUN_RECEIPT_COLUMNS = (
    "trade_date",
    "preset_name",
    "definition_version",
    "parent_trade_date",
    "parent_result_version",
    "hit_count",
    "member_digest",
    "lineage_complete",
    "completed_at",
    "result_version",
)
_RUN_PRICE_RECEIPT_COLUMNS = ("contract", "price_digest")
_MAX_RESEARCH_GATES = 512
_MAX_AUDIT_FINDING_LIST_BYTES = 32 * 1024
_MAX_PULSE_ROWS = 512
_MAX_PULSE_FILE_BYTES = 256 * 1024
_MAX_ALERT_FILE_BYTES = 512 * 1024
_MAX_RUNTIME_CONFIG_BYTES = 16 * 1024
_MAX_EVENT_ROWS = 10_000
_MAX_ALERT_ACK_ROWS = 10_000
_MAX_SURGE_EVENT_BYTES = 8 * 1024 * 1024
_MAX_LEGACY_NOTIFICATION_BYTES = 8 * 1024 * 1024
_EVENT_WINDOW_DAYS = 30
_SURGE_EVENT_TIME = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")
_STOCK_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ|BJ)")
_LEGACY_SCENE_LABELS = MappingProxyType(
    {
        "price_level": "价位提醒",
        "pool2_exit": "二池退出",
        "daily_summary": "每日汇总",
        "error": "运行异常",
        "heartbeat": "运行心跳",
        "morning_pulse": "早盘脉搏",
        "midday_report": "午间报告",
        "surge_watch": "爆量提醒",
        "pulse_alert": "脉搏异动",
    }
)
_LEGACY_CHANNEL_LABELS = MappingProxyType({"pushdeer": "PushDeer", "pushplus": "PushPlus"})
_CANVAS_CATALOG_SCHEMA_VERSION = 1
_PAGE_CONTROL_PROTOCOL_MARKER = "safe-effect-journal-v2"
_PAGE_CONTROL_PROTOCOL_VERSION = 2
_COMPANION_SIGNAL_TABLES = frozenset(
    {
        "screen_result",
        "pool2_watch",
        "monitor_event",
        "surge_event",
        "market_snapshot",
        "market_overview",
        "intraday_kline",
    }
)
_EMPTY_PROJECTION_AVAILABLE_AT = datetime(1970, 1, 1, tzinfo=UTC)


class PageProjectionSourceIntegrityError(RuntimeError):
    """A mutable or malformed operational snapshot cannot become Serving evidence."""


class _PageControlAuditSnapshotUnavailableError(PageProjectionSourceIntegrityError):
    """The shared audit transaction itself could not enter or finish safely."""


@dataclass(frozen=True)
class _BoundReadonlyDirectory:
    path: Path
    descriptors: tuple[int, ...]
    component_names: tuple[str, ...]
    label: str

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]

    def verify(self) -> None:
        for parent, child, component in zip(
            self.descriptors[:-1],
            self.descriptors[1:],
            self.component_names,
            strict=True,
        ):
            try:
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError as exc:
                raise PageProjectionSourceIntegrityError(
                    f"{self.label} ancestor changed while bound"
                ) from exc
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                raise PageProjectionSourceIntegrityError(
                    f"{self.label} ancestors must be regular non-symlink directories"
                )
            if (entry.st_dev, entry.st_ino) != (
                os.fstat(child).st_dev,
                os.fstat(child).st_ino,
            ):
                raise PageProjectionSourceIntegrityError(
                    f"{self.label} ancestor rotated while bound"
                )

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            with suppress(OSError):
                os.close(descriptor)


def _bind_readonly_directory(path: Path, *, label: str) -> _BoundReadonlyDirectory:
    normalized = Path(os.path.abspath(path))
    flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    component_names: list[str] = []
    try:
        descriptors.append(os.open(normalized.anchor, flags))
        for component in normalized.parts[1:]:
            parent = descriptors[-1]
            entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                raise PageProjectionSourceIntegrityError(
                    f"{label} ancestors must be regular non-symlink directories"
                )
            descriptor = os.open(component, flags, dir_fd=parent)
            opened = os.fstat(descriptor)
            if (entry.st_dev, entry.st_ino) != (opened.st_dev, opened.st_ino):
                os.close(descriptor)
                raise PageProjectionSourceIntegrityError(f"{label} ancestor rotated while open")
            descriptors.append(descriptor)
            component_names.append(component)
        binding = _BoundReadonlyDirectory(
            path=normalized,
            descriptors=tuple(descriptors),
            component_names=tuple(component_names),
            label=label,
        )
        binding.verify()
        return binding
    except FileNotFoundError:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise
    except Exception:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _copy_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    """The same, plus `st_ctime_ns`, for the window a byte copy is exposed to.

    `_file_identity` answers "is this still the file I opened", which is what a rename
    changes. A copy has to answer the narrower question "was this file touched *at all*
    while I was reading it", and an in-place write that leaves the size alone moves
    `st_ctime_ns` even where a filesystem's mtime granularity hides it (review SF-1).
    """

    return (*_file_identity(value), value.st_ctime_ns)


def _read_bound_optional_file(
    binding: _BoundReadonlyDirectory,
    name: str,
    *,
    max_bytes: int,
    label: str = "surge live source",
) -> tuple[bytes, os.stat_result] | None:
    try:
        item = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(item.st_mode) or not stat.S_ISREG(item.st_mode):
        raise PageProjectionSourceIntegrityError(f"{label} must be a regular non-symlink file")
    if item.st_size > max_bytes:
        raise PageProjectionSourceIntegrityError(f"{label} exceeds size bound")
    descriptor = os.open(
        name,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=binding.descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        if _copy_identity(opened) != _copy_identity(item):
            raise PageProjectionSourceIntegrityError(f"{label} rotated while read")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(max_bytes + 1)
        after = os.fstat(descriptor)
        if _copy_identity(after) != _copy_identity(opened):
            raise PageProjectionSourceIntegrityError(f"{label} changed while read")
        try:
            named = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
        except FileNotFoundError as error:
            raise PageProjectionSourceIntegrityError(f"{label} rotated while read") from error
        if _copy_identity(named) != _copy_identity(opened):
            raise PageProjectionSourceIntegrityError(f"{label} rotated while read")
    finally:
        os.close(descriptor)
    if len(raw) > max_bytes:
        raise PageProjectionSourceIntegrityError(f"{label} exceeds size bound")
    binding.verify()
    return raw, opened


_BACKFILL_PLAN_NAME = re.compile(r"daily-bar-backfill-plan-v1-([0-9a-f]{64})\.json\Z")
_BACKFILL_TEMP_NAME = re.compile(r"\.backfill-plan-[0-9a-f]{32}\Z")
MAX_BACKFILL_CATALOG_READ_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class _BackfillPlanDirectoryEntry:
    name: str
    identity: tuple[int, int, int, int, int]
    published_ns: int

    @property
    def plan_hash(self) -> str:
        match = _BACKFILL_PLAN_NAME.fullmatch(self.name)
        if match is None:
            raise PageProjectionSourceIntegrityError("backfill plan filename is invalid")
        return match.group(1)


def _list_bound_backfill_plan_entries(
    binding: _BoundReadonlyDirectory,
) -> tuple[_BackfillPlanDirectoryEntry, ...]:
    """List a bounded immutable catalogue without reading large old plans."""
    names = []
    for name in os.listdir(binding.descriptor):
        if _BACKFILL_TEMP_NAME.fullmatch(name):
            continue
        if _BACKFILL_PLAN_NAME.fullmatch(name) is None:
            raise PageProjectionSourceIntegrityError("backfill plan filename is unexpected")
        names.append(name)
    if len(names) > MAX_DISCOVERABLE_BACKFILL_PLANS:
        raise PageProjectionSourceIntegrityError("backfill plan directory exceeds its bound")
    entries = []
    for name in names:
        if _BACKFILL_PLAN_NAME.fullmatch(name) is None:
            raise PageProjectionSourceIntegrityError("backfill plan filename is invalid")
        observed = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode) or not (
            0 < observed.st_size <= MAX_BACKFILL_PLAN_BYTES
        ):
            raise PageProjectionSourceIntegrityError("backfill plan must be a bounded regular file")
        if os.name != "posix" or any(
            value <= 0 for value in (observed.st_mtime_ns, observed.st_ctime_ns)
        ):
            raise PageProjectionSourceIntegrityError(
                "backfill plan publication time is unavailable"
            )
        entries.append(
            _BackfillPlanDirectoryEntry(
                name=name,
                identity=_copy_identity(observed),
                published_ns=max(observed.st_mtime_ns, observed.st_ctime_ns),
            )
        )
    binding.verify()
    return tuple(sorted(entries, key=lambda item: (-item.published_ns, item.plan_hash)))


def _read_bound_backfill_plan(
    binding: _BoundReadonlyDirectory, entry: _BackfillPlanDirectoryEntry
) -> DailyBarBackfillPlan:
    found = _read_bound_optional_file(
        binding,
        entry.name,
        max_bytes=MAX_BACKFILL_PLAN_BYTES,
        label="backfill plan",
    )
    if found is None or _copy_identity(found[1]) != entry.identity:
        raise PageProjectionSourceIntegrityError("backfill plan rotated while read")
    return parse_daily_bar_backfill_plan_bytes(found[0], filename=entry.name)


def _verify_bound_backfill_catalogue(
    binding: _BoundReadonlyDirectory,
    entries: tuple[_BackfillPlanDirectoryEntry, ...],
) -> None:
    binding.verify()
    if _list_bound_backfill_plan_entries(binding) != entries:
        raise PageProjectionSourceIntegrityError("backfill plan directory changed while read")


def _pool_definition_projection(
    root: Path,
    audit: _ReadonlyPageControlAuditReader,
) -> ServingProjectionPayload:
    """Read every managed pool through pinned paths in one PageControl audit snapshot."""
    root = Path(os.path.abspath(root))
    mutations = audit.pool_mutations()
    files: dict[str, Mapping[str, object] | None] = {}
    try:
        binding = _bind_readonly_directory(root, label="user pool catalog")
    except FileNotFoundError:
        binding = None
    if binding is not None:
        try:
            names = sorted(os.listdir(binding.descriptor))
            definition_names = [name for name in names if name.endswith(".json")]
            if len(definition_names) + len(BUILTIN_PRESET_SCREENS) > _MAX_POOL_DEFINITIONS:
                raise PageProjectionSourceIntegrityError("user pools exceed row bound")
            total_bytes = 0
            identities: dict[str, os.stat_result] = {}
            for name in definition_names:
                base_name = name.removesuffix(".json")
                if not re.fullmatch(r"[\w\u4e00-\u9fff-]+", base_name):
                    raise PageProjectionSourceIntegrityError("user pool filename is invalid")
                found = _read_bound_optional_file(
                    binding,
                    name,
                    max_bytes=_MAX_POOL_DEFINITION_BYTES,
                    label="user pool definition",
                )
                if found is None:
                    raise PageProjectionSourceIntegrityError("user pool rotated while read")
                raw_bytes, identity = found
                identities[name] = identity
                total_bytes += len(raw_bytes)
                if total_bytes > _MAX_POOL_CATALOG_BYTES:
                    raise PageProjectionSourceIntegrityError("user pool catalog exceeds byte bound")
                try:
                    value = strict_json_loads(raw_bytes)
                except (UnicodeDecodeError, StrictJsonError, ValueError):
                    value = None
                files[base_name] = value if isinstance(value, dict) else None
            if sorted(os.listdir(binding.descriptor)) != names:
                raise PageProjectionSourceIntegrityError("user pool catalog rotated while read")
            for name, before in identities.items():
                after = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
                if _copy_identity(after) != _copy_identity(before):
                    raise PageProjectionSourceIntegrityError(
                        "user pool definition changed while read"
                    )
            binding.verify()
        finally:
            binding.close()
    rows = build_pool_definition_rows(files, mutations, root_path=str(root))
    # A client request time is not a server publication time. Verified command and
    # file versions live in the rows, whose content hash changes only with facts.
    return ServingProjectionPayload(
        table_name="pool_definition",
        available_at=_EMPTY_PROJECTION_AVAILABLE_AT,
        rows=rows,
    )


def _local_naive(value: datetime) -> datetime:
    return normalize_aware_utc(value).astimezone(_SHANGHAI).replace(tzinfo=None)


def _database_timestamp(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise PageProjectionSourceIntegrityError("projection timestamp is not a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=_SHANGHAI)
    return value.astimezone(UTC)


#: Where a still-open descriptor can be re-opened by name. Linux publishes `/proc/self/fd`
#: and the BSDs `/dev/fd`; opening an entry there re-opens the *inode* the descriptor holds,
#: not the path it was reached through, so it pins a generation without creating anything.
_DESCRIPTOR_DIRECTORIES = ("/proc/self/fd", "/dev/fd")

#: How large a generation may be before copying it stops being a way to pin it. The
#: production replica the notifier is pointed at (`data/rquant_ro.duckdb`, #250) is about
#: 10 GB -- two orders of magnitude over this -- so on that host the copy branch is never
#: taken and the in-place branch is what runs, which is exactly why it exists. A serving
#: or research projection is a few tens of MB, well inside it.
_MAX_PINNED_COPY_BYTES = 256 * 1024 * 1024

#: What DuckDB says when another process holds the write lock, lowercased. Two markers,
#: because the wording differs across builds and only the first half is stable.
_WRITE_LOCK_MARKERS = ("could not set lock", "conflicting lock")


def _descriptor_path(descriptor: int) -> str | None:
    """The name that re-opens exactly this descriptor's inode, or None if the OS has none."""

    for directory in _DESCRIPTOR_DIRECTORIES:
        if os.path.isdir(directory):
            return f"{directory}/{descriptor}"
    return None


class _StableReadonlyDuckDB:
    """Open one regular immutable-generation file and reject pointer rotation mid-read."""

    #: #241: this used to pin the generation with a hard link into a scratch directory.
    #: There is no directory on a runtime host where that can work: systemd builds every
    #: `ReadWritePaths=` and `ReadOnlyPaths=` entry as its own bind mount, and Linux
    #: `link()` refuses across mounts (`do_linkat` compares `mnt`, not the superblock), so
    #: beside the database is `EROFS` and anywhere the role may write is `EXDEV`. A
    #: descriptor pins the same generation and creates nothing at all.
    def __init__(
        self,
        path: Path,
        *,
        control_root: Path | None = None,
        max_copy_bytes: int = _MAX_PINNED_COPY_BYTES,
        atomically_published: bool = False,
    ) -> None:
        normalized = Path(os.path.abspath(path))
        if not normalized.is_absolute():
            raise ValueError("projection database path must be absolute")
        self.path = normalized
        #: a directory this reader owns and may write, used only when the engine refuses
        #: the descriptor path: the copy goes here, never beside the database (#255).
        self.control_root = None if control_root is None else Path(os.path.abspath(control_root))
        if not isinstance(max_copy_bytes, int) or isinstance(max_copy_bytes, bool):
            raise TypeError("max_copy_bytes must be an integer")
        if max_copy_bytes < 0:
            raise ValueError("max_copy_bytes cannot be negative")
        self.max_copy_bytes = max_copy_bytes
        #: the caller states that this artifact's owner replaces it with `rename()` and
        #: never writes it in place. Without that, copying it can serve the last
        #: checkpoint of a database somebody is writing (review SF-1), so the copy branch
        #: is off unless it is said out loud.
        if not isinstance(atomically_published, bool):
            raise TypeError("atomically_published must be a bool")
        self.atomically_published = atomically_published
        self._before: os.stat_result | None = None
        self._descriptor = -1
        self._generation_path: str | None = None
        self._bound_directory: Path | None = None
        self._bound_path: Path | None = None
        #: which opener took: `"descriptor"` (no writes; the branch a runtime host takes),
        #: `"copy"` (a private copy of the pinned inode inside this reader's own control
        #: root) or `"in_place"` (no pinning; the identity check after the read is what
        #: says the generation did not move). Recorded so every branch can be asserted
        #: rather than discovered at run time.
        self.opened_through: str | None = None
        self.connection: duckdb.DuckDBPyConnection | None = None
        #: this read's entry in the process-wide interrupt registry, -1 while none (#268)
        self._interrupt_token = -1

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        before = os.lstat(self.path)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise PageProjectionSourceIntegrityError(
                "projection database must be a regular non-symlink file"
            )
        self._before = before
        try:
            self._descriptor = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as exc:
            raise PageProjectionSourceIntegrityError(
                f"projection database {self.path} cannot be opened "
                f"(errno {exc.errno} {errno.errorcode.get(exc.errno or 0, '?')} {exc.strerror})"
            ) from exc
        try:
            opened = os.fstat(self._descriptor)
            if _file_identity(opened) != _file_identity(before):
                raise PageProjectionSourceIntegrityError(
                    "projection database rotated while its generation was being opened"
                )
            descriptor_path = _descriptor_path(self._descriptor)
            self.connection = self._connect_generation(descriptor_path)
            #: for as long as this reader is open, a stop abandons the query in flight
            #: rather than waiting a multi-gigabyte scan out (#268). Released in
            #: `_release()`, which every exit path runs.
            self._interrupt_token = READ_INTERRUPTS.register(self.connection)
            after_open = os.fstat(self._descriptor)
            if _file_identity(after_open) != _file_identity(before):
                raise PageProjectionSourceIntegrityError(
                    "projection database rotated while the generation was being opened"
                )
        except BaseException:
            self._release()
            raise
        return self.connection

    @property
    def generation_modified_at(self) -> datetime | None:
        """When the inode this reader has open was last written, or None before it opens.

        Taken from the *descriptor*, not the name, so it answers for the generation being
        read however the name moves under it. Every row inside a replica generation was
        written before the file itself was, so a point-in-time cutoff at or after this is a
        cutoff every row already satisfies -- which is what lets `_minute_coverage` drop a
        predicate rather than evaluate it (review SF-7).
        """

        if self._descriptor < 0:
            return None
        return datetime.fromtimestamp(
            os.fstat(self._descriptor).st_mtime_ns / 1_000_000_000, tz=UTC
        )

    def _connect_generation(self, descriptor_path: str | None) -> duckdb.DuckDBPyConnection:
        """Open the exact generation this descriptor holds, writing nothing if possible.

        Three openers, in this order, and the order is the whole point (#241, #255):

        1. **the descriptor** -- `duckdb.connect("/proc/self/fd/<n>")` opens the inode the
           descriptor holds, so a `rename()` over the name during the open cannot swap the
           generation, and nothing is created anywhere. **A failure here is not one thing.**
           Package L's message said "this engine refused the descriptor", and the v0.33.5
           code reached it through a bare `except Exception` -- so a database another
           process holds the write lock on produced exactly the same sentence as an engine
           that does not understand the path. With the pinned duckdb 1.5.2 on Linux the
           descriptor path *is* accepted, so on the production host the far likelier cause
           of #255 is the write lock `rquant-monitor` holds on the main database from
           09:25 (#250). A lock is therefore reported as a lock and stops the read here;
           only a genuine "this path means nothing to me" falls through.
        2. **a private copy inside this reader's own control root** -- for artifacts whose
           publisher replaces them by `rename()`, and only those (`atomically_published`).
           The pinned inode is copied out through the descriptor itself, its identity
           checked before and after. A live writer must never reach this branch: DuckDB's
           uncommitted state lives in a `.wal` beside the database, so a byte copy of the
           main file alone would open *successfully* and serve the last checkpoint --
           older data, with nothing to say it is old. The hard link this replaced failed
           closed there, because it shared the inode and so shared the lock.
        3. **the database in place, unpinned** -- a generation too large to copy, or a
           reader with no control root. There is no pinning and the class does not pretend
           otherwise: `__exit__` compares the descriptor's inode and the name's inode
           against the ones this read started with, so a generation replaced by `rename()`
           under the read is *reported*, after the fact, rather than silently mixed. An
           in-place rewrite of the same inode is outside what that can see.

        What is gone is the branch #255 is about: a hard link beside the database. On a
        runtime host every granted path is its own bind mount, so a link out of the
        database's directory is `EXDEV` and one inside it is `EROFS`.
        """

        if descriptor_path is not None:
            try:
                connection = duckdb.connect(descriptor_path, read_only=True)
            except Exception as error:  # noqa: BLE001 - classified, then re-raised or passed
                self._refuse_if_write_locked(error, opened_path=descriptor_path)
            else:
                self.opened_through = "descriptor"
                self._generation_path = descriptor_path
                return connection
        copied = self._connect_through_copy()
        if copied is not None:
            return copied
        connection = duckdb.connect(str(self.path), read_only=True)
        self.opened_through = "in_place"
        self._generation_path = str(self.path)
        return connection

    @staticmethod
    def _refuse_if_write_locked(error: BaseException, *, opened_path: str) -> None:
        """Re-raise a "somebody else holds the write lock" as itself, not as a path refusal.

        DuckDB says `IO Error: Could not set lock on file "...": Conflicting lock is held
        in <exe> (PID n)`. Swallowing that into the same silence as "the engine does not
        understand this path" is what made #255's message name the wrong cause for a whole
        window, and it is also what would let the copy branch below quietly serve the last
        checkpoint of a database somebody is writing.
        """

        text = str(error).lower()
        if any(marker in text for marker in _WRITE_LOCK_MARKERS):
            raise PageProjectionSourceIntegrityError(
                f"projection database {opened_path} is held by another process's write "
                f"lock: {error}"
            ) from error

    def _connect_through_copy(self) -> duckdb.DuckDBPyConnection | None:
        """A copy of the pinned inode in this reader's own root, or `None` if it cannot be.

        Two conditions the caller has to earn, because getting either wrong turns a
        fail-closed read into a silently stale one (review SF-1):

        * `atomically_published` -- the owner replaces this artifact with `rename()` and
          never writes it in place. Anything else may have a writer in it right now.
        * no `.wal` beside it -- DuckDB keeps uncommitted state there, and a byte copy of
          the main file alone would drop it and open cleanly on the previous checkpoint.
        """

        if self.control_root is None or not self.atomically_published:
            return None
        if self.path.with_name(f"{self.path.name}.wal").exists():
            raise PageProjectionSourceIntegrityError(
                f"projection database {self.path} has an uncommitted write-ahead log "
                "beside it; a copy of the database alone would serve its last checkpoint"
            )
        opened = os.fstat(self._descriptor)
        if opened.st_size > self.max_copy_bytes:
            return None
        bound_directory: Path | None = None
        bound_path: Path | None = None
        try:
            self.control_root.mkdir(parents=True, exist_ok=True)
            bound_directory = Path(
                mkdtemp(prefix=f".{self.path.name}.{uuid4().hex}.", dir=self.control_root)
            )
            os.chmod(bound_directory, 0o700)
            bound_path = bound_directory / "generation.duckdb"
            self._copy_descriptor(bound_path, size=opened.st_size)
        except OSError:
            self._discard_copy(bound_directory, bound_path)
            return None
        #: the same inode *and the same content* all the way through. Size alone is not
        #: enough: a writer that replaces a page in place leaves the size where it was, and
        #: the copy would then be a mixture of before and after. `st_mtime_ns` /
        #: `st_ctime_ns` are what says the file was touched at all (review SF-1).
        after = os.fstat(self._descriptor)
        if _copy_identity(after) != _copy_identity(opened):
            self._discard_copy(bound_directory, bound_path)
            raise PageProjectionSourceIntegrityError(
                "projection database changed while its generation was being copied"
            )
        if os.lstat(bound_path).st_size != opened.st_size:
            self._discard_copy(bound_directory, bound_path)
            raise PageProjectionSourceIntegrityError(
                "projection database copy does not match the opened generation"
            )
        try:
            connection = duckdb.connect(str(bound_path), read_only=True)
        except BaseException:
            self._discard_copy(bound_directory, bound_path)
            return None
        self.opened_through = "copy"
        self._generation_path = str(bound_path)
        self._bound_directory = bound_directory
        self._bound_path = bound_path
        return connection

    def _copy_descriptor(self, destination: Path, *, size: int) -> None:
        """Copy the bytes the descriptor holds, reading only through the descriptor."""

        os.lseek(self._descriptor, 0, os.SEEK_SET)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        target = os.open(destination, flags, 0o600)
        try:
            remaining = size
            while remaining > 0:
                chunk = os.read(self._descriptor, min(remaining, 4 * 1024 * 1024))
                if not chunk:
                    break
                os.write(target, chunk)
                remaining -= len(chunk)
        finally:
            os.close(target)

    @staticmethod
    def _discard_copy(directory: Path | None, path: Path | None) -> None:
        if path is not None:
            with suppress(FileNotFoundError):
                os.unlink(path)
        if directory is not None:
            with suppress(FileNotFoundError):
                os.rmdir(directory)

    @property
    def generation_path(self) -> str:
        """The name a second reader can use to open the very same pinned generation."""

        if self._generation_path is None or self.connection is None:
            raise RuntimeError("projection database generation is not currently open")
        return self._generation_path

    def _release(self) -> None:
        if self._interrupt_token >= 0:
            READ_INTERRUPTS.release(self._interrupt_token)
            self._interrupt_token = -1
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        self._discard_copy(self._bound_directory, self._bound_path)
        self._bound_directory = None
        self._bound_path = None
        if self._descriptor >= 0:
            os.close(self._descriptor)
            self._descriptor = -1

    def __exit__(self, *_error: object) -> None:
        assert self._before is not None
        try:
            opened = os.fstat(self._descriptor)
            if _file_identity(opened) != _file_identity(self._before):
                raise PageProjectionSourceIntegrityError(
                    "projection database opened generation rotated while read"
                )
            after = os.lstat(self.path)
            if _file_identity(after) != _file_identity(self._before):
                raise PageProjectionSourceIntegrityError(
                    "projection database rotated while the snapshot was being read"
                )
        finally:
            self._release()


@dataclass(frozen=True)
class _ReadonlyPageControlAudit:
    command_id: str
    command_kind: str
    command_hash: str
    payload: Mapping[str, object]
    status: PageControlStatus


class _ReadonlyPageControlAuditReader:
    _REQUIRED_TABLES = {
        "page_control_command": {
            "command_id": ("TEXT", 0, None, 1),
            "command_kind": ("TEXT", 1, None, 0),
            "command_hash": ("TEXT", 1, None, 0),
            "payload_json": ("TEXT", 1, None, 0),
            "status": ("TEXT", 1, None, 0),
            "enqueued_at": ("TEXT", 1, None, 0),
            "completed_at": ("TEXT", 0, None, 0),
            "result_json": ("TEXT", 0, None, 0),
            "error": ("TEXT", 0, None, 0),
            "processing_owner": ("TEXT", 0, None, 0),
            "lease_expires_at": ("TEXT", 0, None, 0),
            "attempt_count": ("INTEGER", 1, "0", 0),
            "claim_token": ("TEXT", 0, None, 0),
        },
        "page_control_effect": {
            "command_id": ("TEXT", 0, None, 1),
            "command_hash": ("TEXT", 1, None, 0),
            "effect_kind": ("TEXT", 1, None, 0),
            "status": ("TEXT", 1, None, 0),
            "owner_id": ("TEXT", 1, None, 0),
            "claim_token": ("TEXT", 1, None, 0),
            "started_at": ("TEXT", 1, None, 0),
            "completed_at": ("TEXT", 0, None, 0),
            "result_json": ("TEXT", 0, None, 0),
            "error": ("TEXT", 0, None, 0),
        },
        "page_control_protocol_activation": {
            "marker_name": ("TEXT", 0, None, 1),
            "protocol_version": ("INTEGER", 1, None, 0),
            "activated_at": ("TEXT", 1, None, 0),
        },
    }

    #: #241: `snapshot()` holds the generation it reads. It used to do that with a hard
    #: link, beside the outbox -- a directory that belongs to `rquant-page-control.service`
    #: and is read-only in every other unit, so on the host the notifier got `EROFS` every
    #: iteration. Moving the link into a directory the notifier owns does not fix it:
    #: systemd builds each granted path as its own bind mount and Linux `link()` refuses
    #: across mounts, so that is `EXDEV` instead. An open descriptor needs no target at
    #: all, and creates nothing anywhere, which is the only shape that holds here.
    #:
    #: What the descriptor buys is exact, and less than "pins the inode": the *open*
    #: connection reads the generation it opened, and `os.fstat` on the descriptor answers
    #: for that generation however the name moves. But sqlite resolves
    #: `/proc/self/fd/<n>` **by name**, so once the outbox has been replaced a *later*
    #: open through it is `unable to open database file`. That is fail-closed, never a
    #: silent mix of two generations -- and the revalidation below reports the rotation
    #: from the identity comparison rather than from that open, so the wording names the
    #: generation that moved. (DuckDB re-opens the inode, so its reader does pin.)
    def __init__(self, path: Path, *, reject_sidecars: bool = False) -> None:
        self.path = Path(os.path.abspath(path))
        self._snapshot_connection: sqlite3.Connection | None = None
        self._reject_sidecars = reject_sidecars
        self._price_rule_read = False
        self._require_no_sidecars()
        validated = self._validate_schema()
        self._require_no_sidecars()
        self._validated_node_identity = (validated.st_dev, validated.st_ino)

    def _require_no_sidecars(self) -> None:
        if not (self._reject_sidecars or self._price_rule_read):
            return
        for suffix in ("-wal", "-shm"):
            try:
                os.lstat(Path(f"{self.path}{suffix}"))
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise PageProjectionSourceIntegrityError(
                    "PageControl audit sidecar cannot be checked"
                ) from exc
            raise PageProjectionSourceIntegrityError(
                "PageControl audit sidecar is not allowed for verified projections"
            )

    def _connect(self, path: Path | str | None = None) -> sqlite3.Connection:
        database_path = self.path if path is None else Path(path)
        uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=0)
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                f"PageControl audit cannot be opened read-only: {exc}"
            ) from exc
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        #: sqlite has its own `interrupt()` and the same reason to use it (#268): this
        #: audit is read inside the notifier's iteration, on the same stop path.
        if self._snapshot_connection is not None:
            with interruptible_read(self._snapshot_connection):
                yield self._snapshot_connection
            return
        with self._connect() as connection, interruptible_read(connection):
            yield connection

    @contextmanager
    def snapshot(self) -> Iterator[None]:
        if self._snapshot_connection is not None:
            raise RuntimeError("PageControl audit snapshot is already active")
        self._require_no_sidecars()
        before = os.lstat(self.path)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or (before.st_dev, before.st_ino) != self._validated_node_identity
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database rotated or is not a regular non-symlink file"
            )
        try:
            descriptor = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as exc:
            raise PageProjectionSourceIntegrityError(
                f"PageControl audit {self.path} cannot be opened "
                f"(errno {exc.errno} {errno.errorcode.get(exc.errno or 0, '?')} {exc.strerror})"
            ) from exc
        bound_path = _descriptor_path(descriptor)
        if bound_path is None:
            os.close(descriptor)
            raise PageProjectionSourceIntegrityError(
                f"PageControl audit {self.path} cannot be pinned: this platform publishes "
                f"neither {' nor '.join(_DESCRIPTOR_DIRECTORIES)}, and a reader that may not "
                f"write anywhere has no other way to hold one generation"
            )
        connection: sqlite3.Connection | None = None
        try:
            bound = os.fstat(descriptor)
            after_link = os.lstat(self.path)
            if _file_identity(bound) != _file_identity(before) or _file_identity(
                after_link
            ) != _file_identity(before):
                raise PageProjectionSourceIntegrityError(
                    "PageControl audit rotated while binding its exact generation"
                )
            self._require_no_sidecars()
            connection = self._connect(bound_path)
            self._validate_schema_connection(connection)
            connection.execute("BEGIN")
            before_data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
            after_data_version = before_data_version
            self._snapshot_connection = connection
            self.assert_quiescent()
            yield
            self._require_no_sidecars()
            after_data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
        except BaseException:
            self._snapshot_connection = None
            if connection is not None:
                connection.rollback()
                connection.close()
            os.close(descriptor)
            raise
        else:
            self._snapshot_connection = None
            assert connection is not None
            connection.rollback()
            connection.close()
        integrity_error: PageProjectionSourceIntegrityError | None = None
        try:
            self._require_no_sidecars()
            after = os.lstat(self.path)
            bound_after = os.fstat(descriptor)
            if _file_identity(after) != _file_identity(before):
                #: The generation rotated under us. Say so from the identity comparison
                #: rather than from the re-open below: on Linux `/proc/self/fd/<n>` re-opens
                #: through the descriptor's *name*, and once that name has been replaced the
                #: re-open is `ENOENT` -- fail-closed either way, but with wording about
                #: opening a file rather than about the generation that moved.
                raise PageProjectionSourceIntegrityError(
                    "PageControl audit generation changed or entered in-flight state"
                )
            with self._connect(bound_path) as current:
                inflight = current.execute(
                    """
                    SELECT 1 FROM page_control_command
                    WHERE status IN (?, ?)
                    LIMIT 1
                    """,
                    (PageControlStatus.PENDING.value, PageControlStatus.PROCESSING.value),
                ).fetchone()
            if (
                inflight is not None
                or _file_identity(after) != _file_identity(before)
                or _file_identity(bound_after) != _file_identity(bound)
                or after_data_version != before_data_version
            ):
                integrity_error = PageProjectionSourceIntegrityError(
                    "PageControl audit generation changed or entered in-flight state"
                )
            self._require_no_sidecars()
        except PageProjectionSourceIntegrityError as exc:
            integrity_error = exc
        except (OSError, sqlite3.Error) as exc:
            integrity_error = PageProjectionSourceIntegrityError(
                f"PageControl audit generation cannot be revalidated: {exc}"
            )
        finally:
            os.close(descriptor)
        if integrity_error is not None:
            raise integrity_error

    def _validate_schema(self) -> os.stat_result:
        try:
            before = os.lstat(self.path)
        except FileNotFoundError as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database does not exist"
            ) from exc
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database must be a regular non-symlink file"
            )
        try:
            with self._connect() as connection:
                self._validate_schema_connection(connection)
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                f"PageControl audit schema cannot be read: {exc}"
            ) from exc
        after = os.lstat(self.path)
        if _file_identity(after) != _file_identity(before):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit database rotated while validating schema"
            )
        return after

    def _validate_schema_connection(self, connection: sqlite3.Connection) -> None:
        for table_name, expected in self._REQUIRED_TABLES.items():
            rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
            observed = {
                str(row[1]): (
                    str(row[2]).upper(),
                    int(row[3]),
                    None if row[4] is None else str(row[4]),
                    int(row[5]),
                )
                for row in rows
            }
            if observed != expected:
                raise PageProjectionSourceIntegrityError(
                    "PageControl audit schema is invalid or incomplete"
                )
        foreign_keys = connection.execute("PRAGMA foreign_key_list(page_control_effect)").fetchall()
        if not any(
            str(row[2]) == "page_control_command"
            and str(row[3]) == "command_id"
            and str(row[4]) == "command_id"
            for row in foreign_keys
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit schema lacks its effect authority constraint"
            )
        marker = connection.execute(
            """
            SELECT protocol_version FROM page_control_protocol_activation
            WHERE marker_name = ?
            """,
            (_PAGE_CONTROL_PROTOCOL_MARKER,),
        ).fetchone()
        if marker is None or int(marker[0]) != _PAGE_CONTROL_PROTOCOL_VERSION:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit schema lacks its activation authority"
            )

    def assert_quiescent(self) -> None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT command_id FROM page_control_command
                WHERE status IN (?, ?)
                LIMIT 1
                """,
                (PageControlStatus.PENDING.value, PageControlStatus.PROCESSING.value),
            ).fetchone()
        if row is not None:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit contains an in-flight mutating command"
            )

    def alert_ack_snapshot(self) -> AlertAckAuthoritySnapshot | None:
        """Read activation and every acknowledgment in the pinned SQLite transaction."""
        if self._snapshot_connection is None:
            raise RuntimeError("PageControl alert read requires an active audit snapshot")
        try:
            return self._read_alert_ack_snapshot()
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl alert authority cannot be read"
            ) from exc

    def _read_alert_ack_snapshot(self) -> AlertAckAuthoritySnapshot | None:
        with self._read_connection() as connection:
            present = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('page_control_alert_activation', 'page_control_alert_ack')"
                ).fetchall()
            }
            if not present:
                return None
            if present != {"page_control_alert_activation", "page_control_alert_ack"}:
                raise PageProjectionSourceIntegrityError(
                    "PageControl alert authority tables are incomplete"
                )
            expected = {
                "page_control_alert_activation": {
                    "marker_name": ("TEXT", 0, None, 1),
                    "activated_at": ("TEXT", 1, None, 0),
                },
                "page_control_alert_ack": {
                    "alert_id": ("TEXT", 0, None, 1),
                    "confirmation_id": ("TEXT", 1, None, 0),
                    "actor_id": ("TEXT", 1, None, 0),
                    "confirmed_at": ("TEXT", 1, None, 0),
                    "generation_id": ("TEXT", 1, None, 0),
                },
            }
            for table_name, columns in expected.items():
                observed = {
                    str(row[1]): (
                        str(row[2]).upper(),
                        int(row[3]),
                        None if row[4] is None else str(row[4]),
                        int(row[5]),
                    )
                    for row in connection.execute(f"PRAGMA table_info({table_name})")
                }
                if observed != columns:
                    raise PageProjectionSourceIntegrityError(
                        "PageControl alert authority schema is invalid"
                    )
            unique_confirmation = False
            for index in connection.execute("PRAGMA index_list(page_control_alert_ack)"):
                if int(index[2]) != 1:
                    continue
                index_name = str(index[1]).replace('"', '""')
                index_columns = tuple(
                    str(column[2])
                    for column in connection.execute(f'PRAGMA index_info("{index_name}")')
                )
                if index_columns == ("confirmation_id",):
                    unique_confirmation = True
                    break
            if not unique_confirmation:
                raise PageProjectionSourceIntegrityError(
                    "PageControl alert confirmation uniqueness is missing"
                )
            activation_rows = connection.execute(
                "SELECT marker_name, activated_at FROM page_control_alert_activation LIMIT 2"
            ).fetchall()
            rows = connection.execute(
                "SELECT alert_id, confirmation_id, actor_id, confirmed_at, generation_id "
                "FROM page_control_alert_ack ORDER BY alert_id LIMIT ?",
                (_MAX_ALERT_ACK_ROWS + 1,),
            ).fetchall()
        if len(activation_rows) > 1 or len(rows) > _MAX_ALERT_ACK_ROWS:
            raise PageProjectionSourceIntegrityError(
                "PageControl alert authority exceeds its bounded snapshot"
            )
        if not activation_rows:
            if rows:
                raise PageProjectionSourceIntegrityError(
                    "PageControl alert rows exist without activation"
                )
            return None
        if activation_rows[0]["marker_name"] != "alert_ack":
            raise PageProjectionSourceIntegrityError(
                "PageControl alert activation marker is invalid"
            )
        try:
            acknowledgments = tuple(AlertAcknowledgment.model_validate(dict(row)) for row in rows)
            return AlertAckAuthoritySnapshot.create(
                activated_at=datetime.fromisoformat(activation_rows[0]["activated_at"]),
                rows=acknowledgments,
            )
        except (TypeError, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl alert authority snapshot is invalid"
            ) from exc

    def manual_watchlist_snapshot(self) -> ManualWatchlistAuthoritySnapshot | None:
        """Read activation and the full bounded row set in the pinned audit transaction."""
        if self._snapshot_connection is None:
            raise RuntimeError("PageControl watchlist read requires an active audit snapshot")
        try:
            return self._read_manual_watchlist_snapshot()
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl manual watchlist authority cannot be read"
            ) from exc

    def _read_manual_watchlist_snapshot(self) -> ManualWatchlistAuthoritySnapshot | None:
        with self._read_connection() as connection:
            marker = connection.execute(
                "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
                "WHERE marker_name = ?",
                ("manual-watchlist/v1",),
            ).fetchone()
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'manual_watchlist'"
            ).fetchone()
            if marker is None and table is None:
                return None
            if marker is None or table is None or marker["protocol_version"] != 1:
                raise PageProjectionSourceIntegrityError(
                    "PageControl manual watchlist activation and table are inconsistent"
                )
            expected = {
                "owner_id": ("TEXT", 1, None, 1),
                "ts_code": ("TEXT", 1, None, 2),
                "version": ("INTEGER", 1, None, 0),
                "deleted": ("INTEGER", 1, None, 0),
                "source": ("TEXT", 0, None, 0),
                "price_levels_json": ("TEXT", 1, None, 0),
                "expires_at_utc": ("TEXT", 0, None, 0),
                "updated_at_utc": ("TEXT", 0, None, 0),
            }
            observed = {
                str(row[1]): (
                    str(row[2]).upper(),
                    int(row[3]),
                    None if row[4] is None else str(row[4]),
                    int(row[5]),
                )
                for row in connection.execute("PRAGMA table_info(manual_watchlist)")
            }
            if observed != expected:
                raise PageProjectionSourceIntegrityError(
                    "PageControl manual watchlist schema is invalid"
                )
            rows = connection.execute(
                "SELECT owner_id, ts_code, version, deleted, source, price_levels_json, "
                "expires_at_utc, updated_at_utc FROM manual_watchlist "
                "ORDER BY owner_id, ts_code LIMIT ?",
                (_MAX_MANUAL_WATCHLIST_ROWS + 1,),
            ).fetchall()
        if len(rows) > _MAX_MANUAL_WATCHLIST_ROWS:
            raise PageProjectionSourceIntegrityError(
                "PageControl manual watchlist exceeds its bounded snapshot"
            )
        try:
            entries = []
            for row in rows:
                if type(row["deleted"]) is not int or row["deleted"] not in (0, 1):
                    raise ValueError("manual watchlist deletion marker is invalid")
                entries.append(
                    ManualWatchlistProjectionRow(
                        owner_id=row["owner_id"],
                        ts_code=row["ts_code"],
                        version=row["version"],
                        deleted=bool(row["deleted"]),
                        source=row["source"],
                        price_levels_json=row["price_levels_json"],
                        expires_at=row["expires_at_utc"],
                        updated_at=row["updated_at_utc"],
                    )
                )
            return ManualWatchlistAuthoritySnapshot.create(
                activated_at=datetime.fromisoformat(marker["activated_at"]),
                rows=entries,
            )
        except (TypeError, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl manual watchlist snapshot is invalid"
            ) from exc

    def price_alert_rule_snapshot(self) -> PriceAlertRuleAuthoritySnapshot | None:
        """Read all current heads in the same pinned transaction as the watchlist."""
        if self._snapshot_connection is None:
            raise RuntimeError("PageControl price rules require an active audit snapshot")
        self._price_rule_read = True
        self._require_no_sidecars()
        try:
            with self._read_connection() as connection:
                marker = connection.execute(
                    "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
                    "WHERE marker_name = ?",
                    ("price-alert-rule/v1",),
                ).fetchone()
                table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'price_alert_rule'"
                ).fetchone()
                if marker is None and table is None:
                    return None
                if marker is None or table is None or marker["protocol_version"] != 1:
                    raise PageProjectionSourceIntegrityError(
                        "PageControl price rule activation and table are inconsistent"
                    )
                actual = tuple(
                    (row[1], row[2], row[3], row[5])
                    for row in connection.execute("PRAGMA table_info(price_alert_rule)")
                )
                if actual != _SCHEMA_COLUMNS:
                    raise PageProjectionSourceIntegrityError(
                        "PageControl price rule schema is invalid"
                    )
                rows = connection.execute(
                    "SELECT owner_id, rule_id, version, deleted, ts_code, "
                    "membership_version, rule_json, updated_at_utc FROM price_alert_rule "
                    "ORDER BY owner_id, rule_id LIMIT ?",
                    (_MAX_PRICE_ALERT_RULE_ROWS + 1,),
                ).fetchall()
            self._require_no_sidecars()
            if len(rows) > _MAX_PRICE_ALERT_RULE_ROWS:
                raise PageProjectionSourceIntegrityError(
                    "PageControl price rules exceed their bounded snapshot"
                )
            activated = marker["activated_at"]
            if not isinstance(activated, str):
                raise ValueError("price rule activation time is invalid")
            activated_at = normalize_aware_utc(datetime.fromisoformat(activated))
            if activated_at.isoformat(timespec="microseconds") != activated:
                raise ValueError("price rule activation time is not canonical")
            return PriceAlertRuleAuthoritySnapshot.create(
                activated_at=activated_at,
                rows=(
                    PriceAlertRuleProjectionRow.from_entry(PriceAlertRuleRepository._entry(row))
                    for row in rows
                ),
            )
        except sqlite3.Error as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl price rule authority cannot be read"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                "PageControl price rule snapshot is invalid"
            ) from exc

    def audit(self, command_id: str) -> _ReadonlyPageControlAudit | None:
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT c.command_id, c.command_kind, c.command_hash,
                       c.payload_json, c.status,
                       e.command_id AS effect_command_id,
                       e.command_hash AS effect_command_hash,
                       e.effect_kind, e.status AS effect_status
                FROM page_control_command AS c
                LEFT JOIN page_control_effect AS e USING (command_id)
                WHERE c.command_id = ?
                """,
                (command_id,),
            ).fetchone()
        if row is None:
            return None
        return self._audit_row(row)

    def canvas_mutations(self) -> Mapping[str, _ReadonlyPageControlAudit]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT c.command_id, c.command_kind, c.command_hash,
                       c.payload_json, c.status,
                       e.command_id AS effect_command_id,
                       e.command_hash AS effect_command_hash,
                       e.effect_kind, e.status AS effect_status
                FROM page_control_command AS c
                LEFT JOIN page_control_effect AS e USING (command_id)
                WHERE c.status = ?
                  AND c.command_kind IN (?, ?, ?, ?, ?, ?, ?)
                ORDER BY c.rowid
                """,
                (
                    PageControlStatus.SUCCEEDED.value,
                    "save_canvas",
                    "create_canvas",
                    "delete_canvas",
                    "set_canvas_pool_refs",
                    "add_pool_to_canvas",
                    "save_user_pool",
                    "fork_builtin_pool",
                ),
            ).fetchall()
        latest: dict[str, _ReadonlyPageControlAudit] = {}
        for row in rows:
            audit = self._audit_row(row)
            canvas_name = self._canvas_name_for_audit(audit)
            if canvas_name is not None and canvas_name != "__default__":
                latest[canvas_name] = audit
        return MappingProxyType(latest)

    def pool_mutations(self) -> Mapping[str, PoolMutation]:
        """Newest successful mutation per pool, from this pinned audit generation."""
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT c.command_id, c.command_kind, c.command_hash,
                       CASE WHEN length(c.payload_json) <= ?
                            THEN c.payload_json ELSE NULL END AS payload_json,
                       c.status,
                       CASE WHEN length(c.result_json) <= ?
                            THEN c.result_json ELSE NULL END AS result_json,
                       e.command_id AS effect_command_id,
                       e.command_hash AS effect_command_hash,
                       e.effect_kind, e.status AS effect_status,
                       CASE WHEN length(e.result_json) <= ?
                            THEN e.result_json ELSE NULL END AS effect_result_json
                FROM page_control_command AS c
                LEFT JOIN page_control_effect AS e USING (command_id)
                WHERE c.status = ?
                  AND c.command_kind IN (?, ?, ?, ?, ?, ?)
                ORDER BY c.rowid DESC
                LIMIT ?
                """,
                (
                    _MAX_POOL_AUDIT_CELL_CHARS,
                    _MAX_POOL_AUDIT_CELL_CHARS,
                    _MAX_POOL_AUDIT_CELL_CHARS,
                    PageControlStatus.SUCCEEDED.value,
                    "save_user_pool",
                    "save_user_pool_v2",
                    "save_user_pool_v3",
                    "save_nl_preset",
                    "fork_builtin_pool",
                    "delete_user_pool",
                    _MAX_POOL_MUTATIONS + 1,
                ),
            )
            return self._pool_mutations_from_rows(rows)

    def formula_pool_saves(self) -> Mapping[str, PoolMutation]:
        """Successful formula saves from the same pinned, read-only audit generation."""
        if self._snapshot_connection is None:
            raise RuntimeError("formula pool audit requires an active snapshot")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT c.command_id, c.command_kind, c.command_hash,
                       CASE WHEN length(c.payload_json) <= ?
                            THEN c.payload_json ELSE NULL END AS payload_json,
                       c.status,
                       CASE WHEN length(c.result_json) <= ?
                            THEN c.result_json ELSE NULL END AS result_json,
                       e.command_id AS effect_command_id,
                       e.command_hash AS effect_command_hash,
                       e.effect_kind, e.status AS effect_status,
                       CASE WHEN length(e.result_json) <= ?
                            THEN e.result_json ELSE NULL END AS effect_result_json
                FROM page_control_command AS c
                LEFT JOIN page_control_effect AS e USING (command_id)
                WHERE c.status = ? AND c.command_kind = ?
                ORDER BY c.rowid DESC LIMIT ?
                """,
                (
                    _MAX_POOL_AUDIT_CELL_CHARS,
                    _MAX_POOL_AUDIT_CELL_CHARS,
                    _MAX_POOL_AUDIT_CELL_CHARS,
                    PageControlStatus.SUCCEEDED.value,
                    "save_formula_pool_v1",
                    _MAX_POOL_MUTATIONS + 1,
                ),
            ).fetchall()
        saves = self._pool_mutations_from_rows(rows)
        if len(saves) != len(rows):
            raise PageProjectionSourceIntegrityError("formula pool has repeated save authority")
        return saves

    @classmethod
    def _pool_mutations_from_rows(cls, rows: Iterable[sqlite3.Row]) -> Mapping[str, PoolMutation]:
        latest: dict[str, PoolMutation] = {}
        audit_bytes = 0
        for position, row in enumerate(rows):
            if position >= _MAX_POOL_MUTATIONS:
                raise PageProjectionSourceIntegrityError(
                    "PageControl pool audit exceeds event bound"
                )
            if any(
                row[field] is None
                for field in ("payload_json", "result_json", "effect_result_json")
            ):
                raise PageProjectionSourceIntegrityError(
                    "PageControl pool audit exceeds cell bound or lacks result"
                )
            audit_bytes += sum(
                len(value.encode("utf-8"))
                for value in (
                    row["payload_json"],
                    row["result_json"],
                    row["effect_result_json"],
                )
                if isinstance(value, str)
            )
            if audit_bytes > _MAX_POOL_AUDIT_BYTES:
                raise PageProjectionSourceIntegrityError(
                    "PageControl pool audit exceeds byte bound"
                )
            audit = cls._audit_row(row)
            try:
                command = parse_page_control_command(dict(audit.payload))
                receipt = strict_json_loads(row["result_json"])
                effect = strict_json_loads(row["effect_result_json"])
            except (TypeError, ValueError, StrictJsonError) as exc:
                raise PageProjectionSourceIntegrityError(
                    "PageControl pool audit is malformed"
                ) from exc
            if (
                command.kind != audit.command_kind
                or canonical_sha256(command.model_dump(mode="json")) != audit.command_hash
                or not isinstance(receipt, dict)
                or receipt != effect
            ):
                raise PageProjectionSourceIntegrityError(
                    "PageControl pool effect result mismatches command"
                )
            base_name = (
                command.target_base_name
                if command.kind == "fork_builtin_pool"
                else command.name
                if command.kind == "save_nl_preset"
                else command.base_name
            )
            if base_name not in latest:
                latest[base_name] = PoolMutation(
                    command_id=audit.command_id,
                    command_kind=audit.command_kind,
                    command_hash=audit.command_hash,
                    payload=audit.payload,
                    result=receipt,
                )
            if len(latest) > _MAX_POOL_DEFINITIONS:
                raise PageProjectionSourceIntegrityError("PageControl pools exceed row bound")
        return MappingProxyType(latest)

    @staticmethod
    def _canvas_name_for_audit(audit: _ReadonlyPageControlAudit) -> str | None:
        field_name = (
            "name"
            if audit.command_kind
            in {"save_canvas", "create_canvas", "delete_canvas", "set_canvas_pool_refs"}
            else "canvas_name"
        )
        value = audit.payload.get(field_name)
        if value is None:
            return None
        if not isinstance(value, str):
            raise PageProjectionSourceIntegrityError(
                "PageControl canvas mutation has an invalid canvas name"
            )
        return value

    @staticmethod
    def _audit_row(row: sqlite3.Row) -> _ReadonlyPageControlAudit:
        try:
            payload = strict_json_loads(row["payload_json"])
            status = PageControlStatus(row["status"])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PageProjectionSourceIntegrityError("PageControl audit row is malformed") from exc
        if not isinstance(payload, dict):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit command payload is not an object"
            )
        command_id = str(row["command_id"])
        if payload.get("command_id") != command_id:
            raise PageProjectionSourceIntegrityError("PageControl audit command identity mismatch")
        command_hash = str(row["command_hash"])
        if canonical_sha256(payload) != command_hash:
            raise PageProjectionSourceIntegrityError(
                "PageControl audit command payload hash mismatch"
            )
        if (
            row["effect_command_id"] != command_id
            or row["effect_command_hash"] != command_hash
            or row["effect_kind"] != row["command_kind"]
            or row["effect_status"] != "succeeded"
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl audit lacks its matching succeeded effect authority"
            )
        return _ReadonlyPageControlAudit(
            command_id=command_id,
            command_kind=str(row["command_kind"]),
            command_hash=command_hash,
            payload=MappingProxyType(payload),
            status=status,
        )


@dataclass(frozen=True, slots=True)
class _VerifiedRunReceipts:
    latest: tuple[ScreenRunReceipt, ...]
    lineage: tuple[ScreenRunReceipt, ...]
    price_digest_verified: frozenset[str]
    newest_candidate_day: date | None
    newest_candidate_at: datetime | None
    exact_parent_steps: tuple[tuple[str, int], ...]
    membership_daily: tuple[ScreenRunReceipt, ...] | None
    membership_candidate_keys: frozenset[tuple[date, str]]


def _exact_parent_steps(
    connection: duckdb.DuckDBPyConnection,
    *,
    receipt: ScreenRunReceipt,
    observed: datetime,
) -> int | None:
    parent_day = receipt.parent_trade_date
    if parent_day is None:
        return None
    span = (receipt.trade_date - parent_day).days
    if span <= 0 or span > _MAX_EXACT_PARENT_CALENDAR_SPAN_DAYS:
        return None
    calendar = connection.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT cal_date),
               COUNT(*) FILTER (WHERE cal_date IN (?, ?) AND is_open),
               COUNT(*) FILTER (WHERE cal_date > ? AND is_open),
               COUNT(*) FILTER (WHERE updated_at <= ?)
        FROM trade_calendar
        WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ?
        """,
        (parent_day, receipt.trade_date, parent_day, observed, parent_day, receipt.trade_date),
    ).fetchone()
    assert calendar is not None
    expected_rows = span + 1
    if (
        calendar[0] != expected_rows
        or calendar[1] != expected_rows
        or calendar[2] != 2
        or calendar[4] != expected_rows
    ):
        return None
    target_bar = connection.execute(
        "SELECT 1 FROM daily_bar WHERE trade_date = ? LIMIT 1", (parent_day,)
    ).fetchone()
    return int(calendar[3]) if target_bar is not None else None


def _read_verified_run_receipts(
    connection: duckdb.DuckDBPyConnection,
    *,
    cutoff: datetime,
    observed: datetime,
    generation_sealed_before_cutoff: bool,
) -> _VerifiedRunReceipts | None:
    present = connection.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = 'screen_run_receipt'"
    ).fetchone()
    if present is None:
        return None
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info('screen_run_receipt')").fetchall()
    }
    if not set(_RUN_RECEIPT_COLUMNS).issubset(columns):
        raise PageProjectionSourceIntegrityError("screen run receipt table is incomplete")
    price_columns = set(_RUN_PRICE_RECEIPT_COLUMNS)
    if columns & price_columns and not price_columns <= columns:
        raise PageProjectionSourceIntegrityError("screen run price receipt columns are incomplete")
    receipt_columns = (
        _RUN_RECEIPT_COLUMNS + _RUN_PRICE_RECEIPT_COLUMNS
        if price_columns <= columns
        else _RUN_RECEIPT_COLUMNS
    )
    receipt_select = ", ".join(receipt_columns)
    candidate_rows = connection.execute(
        f"""
        SELECT {receipt_select}
        FROM screen_run_receipt
        WHERE trade_date BETWEEN ? AND ? AND completed_at <= ?
        ORDER BY trade_date DESC, preset_name
        LIMIT ?
        """,
        (
            cutoff.date() - timedelta(days=_RUN_RECEIPT_LOOKBACK_DAYS),
            cutoff.date(),
            observed,
            _MAX_RUN_RECEIPT_SOURCE_ROWS + 1,
        ),
    ).fetchall()
    if len(candidate_rows) > _MAX_RUN_RECEIPT_SOURCE_ROWS:
        raise PageProjectionSourceIntegrityError("screen run receipt source exceeds bound")
    latest_by_pool: dict[str, tuple[object, ...]] = {}
    for raw in candidate_rows:
        latest_by_pool.setdefault(str(raw[1]), raw)
    if len(latest_by_pool) > _MAX_RUN_RECEIPTS:
        raise PageProjectionSourceIntegrityError("screen run receipt exceeds row bound")

    candidates = tuple(latest_by_pool[name] for name in sorted(latest_by_pool))
    newest = max((row[0] for row in candidates), default=None)
    newest_at = max((_database_timestamp(row[8]) for row in candidate_rows), default=None)
    verified: dict[tuple[date, str], ScreenRunReceipt | None] = {}
    price_digest_verified: set[str] = set()
    member_total = 0

    def verify(raw: tuple[object, ...]) -> ScreenRunReceipt | None:
        nonlocal member_total
        key = (raw[0], raw[1])
        if key in verified:
            return verified[key]
        if len(verified) >= _MAX_RUN_LINEAGE_NODES:
            raise PageProjectionSourceIntegrityError("screen run receipt lineage exceeds bound")
        verified[key] = None
        try:
            receipt = ScreenRunReceipt.model_validate(dict(zip(receipt_columns, raw, strict=True)))
        except ValueError:
            return None
        if receipt.trade_date > cutoff.date() or receipt.completed_at > observed:
            return None
        member_rows = connection.execute(
            """
            SELECT ts_code, close FROM screen_result
            WHERE trade_date = ? AND preset_name = ? AND created_at <= ?
            ORDER BY ts_code LIMIT ?
            """,
            (receipt.trade_date, receipt.preset_name, cutoff, _MAX_RUN_MEMBERS_PER_POOL + 1),
        ).fetchall()
        member_total += len(member_rows)
        if len(member_rows) > _MAX_RUN_MEMBERS_PER_POOL or member_total > _MAX_RUN_TOTAL_MEMBERS:
            raise PageProjectionSourceIntegrityError("screen run receipt members exceed bound")
        codes = [str(row[0]) for row in member_rows]
        try:
            matches = (
                len(codes) == receipt.hit_count
                and member_set_digest(codes) == receipt.member_digest
            )
        except ValueError:
            return None
        if not matches:
            return None
        if receipt.parent_trade_date is not None:
            if receipt.parent_trade_date >= receipt.trade_date:
                return None
            parent_rows = connection.execute(
                f"""
                SELECT {receipt_select}
                FROM screen_run_receipt
                WHERE trade_date = ? AND result_version = ? AND completed_at <= ?
                LIMIT 2
                """,
                (receipt.parent_trade_date, receipt.parent_result_version, observed),
            ).fetchall()
            if len(parent_rows) != 1:
                return None
            parent = verify(parent_rows[0])
            if (
                parent is None
                or parent.completed_at > receipt.completed_at
                or parent.lineage_complete != receipt.lineage_complete
            ):
                return None
        if (
            receipt.contract == "screen-run-receipt/v2"
            and receipt.price_digest
            == member_price_digest([(str(code), close) for code, close in member_rows])
        ):
            assert receipt.result_version is not None
            price_digest_verified.add(receipt.result_version)
        verified[key] = receipt
        return receipt

    latest = tuple(item for raw in candidates if (item := verify(raw)) is not None)
    membership_candidate_keys = frozenset((raw[0], str(raw[1])) for raw in candidate_rows)
    membership_daily: tuple[ScreenRunReceipt, ...] | None = None
    try:
        source_members = sum(int(raw[5]) for raw in candidate_rows)
    except (TypeError, ValueError):
        source_members = _MAX_POOL_MEMBERSHIP_SOURCE_MEMBERS + 1
    if (
        len(candidate_rows) <= _MAX_POOL_MEMBERSHIP_SOURCE_RECEIPTS
        and 0 <= source_members <= _MAX_POOL_MEMBERSHIP_SOURCE_MEMBERS
    ):
        try:
            membership_daily = tuple(
                item for raw in candidate_rows if (item := verify(raw)) is not None
            )
        except PageProjectionSourceIntegrityError:
            membership_daily = None
    lineage = tuple(item for item in verified.values() if item is not None)
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name IN ('trade_calendar', 'daily_bar')"
        ).fetchall()
    }
    exact_parent_steps: list[tuple[str, int]] = []
    # daily_bar has no row timestamp, so its target-day presence proves a PIT fact
    # only after the immutable replica generation was already sealed.
    if generation_sealed_before_cutoff and tables == {"trade_calendar", "daily_bar"}:
        step_cache: dict[tuple[date, date], int | None] = {}
        for item in lineage:
            if item.parent_trade_date is None:
                continue
            key = (item.parent_trade_date, item.trade_date)
            if key not in step_cache:
                step_cache[key] = _exact_parent_steps(connection, receipt=item, observed=observed)
            steps = step_cache[key]
            if steps is not None and item.result_version is not None:
                exact_parent_steps.append((item.result_version, steps))
    return _VerifiedRunReceipts(
        latest=latest,
        lineage=lineage,
        price_digest_verified=frozenset(price_digest_verified),
        newest_candidate_day=newest,
        newest_candidate_at=newest_at,
        exact_parent_steps=tuple(exact_parent_steps),
        membership_daily=membership_daily,
        membership_candidate_keys=membership_candidate_keys,
    )


def _current_receipt_versions(
    receipts: _VerifiedRunReceipts,
    definitions: ServingProjectionPayload,
) -> frozenset[str]:
    by_name = {str(row["pool_name"]): row for row in definitions.rows}
    by_version = {item.result_version: item for item in receipts.lineage}
    exact_parent_steps = dict(receipts.exact_parent_steps)
    current_cache: dict[str, bool] = {}

    def current(receipt: ScreenRunReceipt) -> bool:
        assert receipt.result_version is not None
        if receipt.result_version in current_cache:
            return current_cache[receipt.result_version]
        row = by_name.get(receipt.preset_name)
        matches = bool(
            row is not None
            and row["state"] == "available"
            and row["version"] == receipt.definition_version
            and receipt.lineage_complete
        )
        if matches and row is not None:
            parent_name = row["depends_on"]
            if parent_name is None:
                matches = receipt.parent_result_version is None
            else:
                parent = by_version.get(receipt.parent_result_version)
                matches = bool(
                    row["delay_mode"] == "exact"
                    and parent is not None
                    and parent.preset_name == parent_name
                    and parent.trade_date == receipt.parent_trade_date
                    and exact_parent_steps.get(receipt.result_version) == row["delay_days"]
                    and current(parent)
                )
        current_cache[receipt.result_version] = matches
        return matches

    return frozenset(
        item.result_version
        for item in receipts.lineage
        if item.result_version is not None and current(item)
    )


def _receipt_projection(
    receipts: _VerifiedRunReceipts,
    definitions: ServingProjectionPayload,
) -> ServingProjectionPayload:
    current_versions = _current_receipt_versions(receipts, definitions)
    rows = tuple(
        {
            "trade_date": item.trade_date.isoformat(),
            "preset_name": item.preset_name,
            "definition_version": item.definition_version,
            "result_version": item.result_version,
            "parent_trade_date": (
                None if item.parent_trade_date is None else item.parent_trade_date.isoformat()
            ),
            "parent_result_version": item.parent_result_version,
            "hit_count": item.hit_count,
            "member_digest": item.member_digest,
            "lineage_complete": item.lineage_complete,
            "current_definition": item.result_version in current_versions,
            "completed_at": item.completed_at.isoformat(),
        }
        for item in receipts.latest
    )
    available = max(
        (item.completed_at for item in receipts.latest), default=_EMPTY_PROJECTION_AVAILABLE_AT
    )
    return ServingProjectionPayload(
        table_name="screen_run_receipt", available_at=available, rows=rows
    )


@dataclass(frozen=True, slots=True)
class _MembershipSource:
    target_date: date | None
    trading_days: tuple[date, ...]
    calendar_complete: bool
    receipt_table_present: bool
    source_limited: bool
    days: tuple[PoolDayEvidence, ...]
    candidate_keys: frozenset[tuple[date, str]]
    return_facts: tuple[_MemberReturnFact, ...]


@dataclass(frozen=True, slots=True)
class _MemberReturnFact:
    pool_name: str
    trade_date: date
    ts_code: str
    close: float | None
    volume: float | None
    factor: float | None


def _membership_calendar(
    connection: duckdb.DuckDBPyConnection,
    *,
    start: date,
    end: date,
    observed: datetime,
) -> tuple[tuple[date, ...], bool]:
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info('trade_calendar')").fetchall()
    }
    if not {"exchange", "cal_date", "is_open", "updated_at"}.issubset(columns):
        return (), False
    expected = (end - start).days + 1
    if expected < 1 or expected > _RUN_RECEIPT_LOOKBACK_DAYS + 1:
        return (), False
    rows = connection.execute(
        """
        SELECT cal_date, is_open, updated_at FROM trade_calendar
        WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ?
        ORDER BY cal_date LIMIT ?
        """,
        (start, end, expected + 1),
    ).fetchall()
    if len(rows) != expected:
        return (), False
    if any(
        day != start + timedelta(days=index) or _database_timestamp(updated_at) > observed
        for index, (day, _is_open, updated_at) in enumerate(rows)
    ):
        return (), False
    trading_days = tuple(day for day, is_open, _updated_at in rows if is_open)
    return trading_days, bool(trading_days and trading_days[-1] == end)


def _read_membership_source(
    connection: duckdb.DuckDBPyConnection,
    *,
    receipts: _VerifiedRunReceipts | None,
    target_date: date | None,
    cutoff: datetime,
    observed: datetime,
) -> _MembershipSource:
    if receipts is None or target_date is None:
        return _MembershipSource(
            target_date=target_date,
            trading_days=(),
            calendar_complete=False,
            receipt_table_present=receipts is not None,
            source_limited=False,
            days=(),
            candidate_keys=frozenset(),
            return_facts=(),
        )
    if receipts.membership_daily is None:
        return _MembershipSource(
            target_date=target_date,
            trading_days=(),
            calendar_complete=False,
            receipt_table_present=True,
            source_limited=True,
            days=(),
            candidate_keys=receipts.membership_candidate_keys,
            return_facts=(),
        )
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' "
            "AND table_name IN ('trade_calendar', 'daily_bar', 'adj_factor')"
        ).fetchall()
    }
    daily_columns = (
        {str(row[1]) for row in connection.execute("PRAGMA table_info('daily_bar')").fetchall()}
        if "daily_bar" in tables
        else set()
    )
    daily_has_close = {"trade_date", "ts_code", "close"} <= daily_columns
    factor_columns = (
        {str(row[1]) for row in connection.execute("PRAGMA table_info('adj_factor')").fetchall()}
        if "adj_factor" in tables
        else set()
    )
    has_factor = {"trade_date", "ts_code", "adj_factor"} <= factor_columns
    start = min(
        (day for day, _pool in receipts.membership_candidate_keys if day <= target_date),
        default=target_date,
    )
    calendar, complete = (
        _membership_calendar(connection, start=start, end=target_date, observed=observed)
        if "trade_calendar" in tables
        else ((), False)
    )
    days: list[PoolDayEvidence] = []
    return_facts: list[_MemberReturnFact] = []
    member_total = 0
    for receipt in receipts.membership_daily:
        daily_values = (
            "db.close AS daily_close, "
            + ("db.vol" if "vol" in daily_columns else "NULL::DOUBLE")
            + " AS daily_volume"
            if daily_has_close
            else "NULL::DOUBLE AS daily_close, NULL::DOUBLE AS daily_volume"
        )
        daily_join = (
            "LEFT JOIN daily_bar AS db ON db.trade_date = sr.trade_date "
            "AND db.ts_code = sr.ts_code "
            if daily_has_close
            else ""
        )
        member_rows = connection.execute(
            "SELECT sr.ts_code, sr.close, "
            + daily_values
            + " FROM screen_result AS sr "
            + daily_join
            + "WHERE sr.trade_date = ? AND sr.preset_name = ? AND sr.created_at <= ? "
            + "ORDER BY sr.ts_code LIMIT ?",
            (
                receipt.trade_date,
                receipt.preset_name,
                cutoff,
                _MAX_POOL_MEMBERSHIP_SOURCE_MEMBERS + 1,
            ),
        ).fetchall()
        member_total += len(member_rows)
        if member_total > _MAX_POOL_MEMBERSHIP_SOURCE_MEMBERS:
            return _MembershipSource(
                target_date=target_date,
                trading_days=(),
                calendar_complete=False,
                receipt_table_present=True,
                source_limited=True,
                days=(),
                candidate_keys=receipts.membership_candidate_keys,
                return_facts=(),
            )
        prices_match_receipt = receipt.result_version in receipts.price_digest_verified
        factors: dict[str, float] = {}
        if has_factor and prices_match_receipt and member_rows:
            try:
                factor_rows = connection.execute(
                    "SELECT af.ts_code, af.adj_factor FROM adj_factor AS af "
                    "JOIN screen_result AS sr ON sr.trade_date = af.trade_date "
                    "AND sr.ts_code = af.ts_code "
                    "WHERE sr.trade_date = ? AND sr.preset_name = ? "
                    "AND sr.created_at <= ? ORDER BY af.ts_code LIMIT ?",
                    (
                        receipt.trade_date,
                        receipt.preset_name,
                        cutoff,
                        len(member_rows) + 1,
                    ),
                ).fetchall()
            except duckdb.Error:
                factor_rows = []
            factor_codes = [str(code) for code, _factor in factor_rows]
            if len(factor_rows) <= len(member_rows) and len(set(factor_codes)) == len(factor_rows):
                factors = {str(code): factor for code, factor in factor_rows}
        members: list[PoolMemberClose] = []
        for code, close, daily_close, daily_volume in member_rows:
            trusted_close = (
                close
                if prices_match_receipt
                and close is not None
                and daily_close is not None
                and math.isfinite(close)
                and close > 0
                and close == daily_close
                else None
            )
            members.append(PoolMemberClose(ts_code=str(code), close=trusted_close))
            return_facts.append(
                _MemberReturnFact(
                    pool_name=receipt.preset_name,
                    trade_date=receipt.trade_date,
                    ts_code=str(code),
                    close=trusted_close,
                    volume=daily_volume,
                    factor=factors.get(str(code)),
                )
            )
        days.append(
            PoolDayEvidence(trade_date=receipt.trade_date, receipt=receipt, members=tuple(members))
        )
    return _MembershipSource(
        target_date=target_date,
        trading_days=calendar,
        calendar_complete=complete,
        receipt_table_present=True,
        source_limited=False,
        days=tuple(days),
        candidate_keys=receipts.membership_candidate_keys,
        return_facts=tuple(return_facts),
    )


def _membership_projection(
    source: _MembershipSource,
    *,
    screen_bounds: tuple[ScreenBoundsProjectionRow, ...],
    definitions: ServingProjectionPayload,
    receipts: _VerifiedRunReceipts | None,
    available_at: datetime,
) -> ServingProjectionPayload:
    definition_rows = tuple(
        row
        for row in definitions.rows
        if row["state"] == "available" and isinstance(row["version"], str)
    )
    target = source.target_date
    published_trade_date = (
        target.isoformat()
        if target is not None and target <= available_at.astimezone(_SHANGHAI).date()
        else None
    )
    raw_pool_dates = {row.preset_name: row.max_date for row in screen_bounds}
    current_versions = (
        _current_receipt_versions(receipts, definitions) if receipts is not None else frozenset()
    )
    days_by_pool: dict[str, list[PoolDayEvidence]] = {}
    for day in source.days:
        assert day.receipt is not None
        days_by_pool.setdefault(day.receipt.preset_name, []).append(day)
    rows: list[dict[str, object]] = []
    for definition in definition_rows:
        pool_name = str(definition["pool_name"])
        pool_days = days_by_pool.get(pool_name, [])
        current = next((day for day in pool_days if day.trade_date == target), None)
        result_version = current.receipt.result_version if current and current.receipt else None
        member_rows: tuple[dict[str, object], ...] = ()
        if source.source_limited:
            status = "source_limited"
        elif not source.receipt_table_present:
            status = "legacy_unproven" if raw_pool_dates.get(pool_name) == target else "not_run"
        elif target is None:
            status = "not_run"
        elif published_trade_date is None or (
            (target, pool_name) in source.candidate_keys and current is None
        ):
            status = "unverified"
        elif current is None:
            status = "legacy_unproven" if raw_pool_dates.get(pool_name) == target else "not_run"
        elif not source.calendar_complete:
            status = "calendar_incomplete"
        elif result_version not in current_versions:
            status = "definition_mismatch"
        else:
            trusted_days = [
                day
                for day in pool_days
                if day.receipt is not None and day.receipt.result_version in current_versions
            ]
            trusted_keys = {(day.trade_date, pool_name) for day in trusted_days}
            trusted_days.extend(
                PoolDayEvidence(
                    trade_date=trade_date,
                    receipt=None,
                    missing_receipt_reason="legacy_unproven",
                )
                for trade_date, name in source.candidate_keys
                if name == pool_name and (trade_date, name) not in trusted_keys
            )
            result = compute_pool_membership(
                pool_name=pool_name,
                published_definition_version=str(definition["version"]),
                trading_days=source.trading_days,
                days=trusted_days,
                calendar_complete=True,
            )
            status = result.status
            member_rows = tuple(
                {
                    "pool_name": pool_name,
                    "trade_date": published_trade_date,
                    "result_version": result.result_version,
                    "row_kind": "member",
                    "ts_code": member.ts_code,
                    "status": status,
                    "entry_trade_date": (
                        member.entry_trade_date.isoformat()
                        if member.entry_trade_date is not None
                        else None
                    ),
                    "entry_close": member.entry_close,
                    "entry_result_version": member.entry_result_version,
                    "unknown_reason": member.unknown_reason,
                }
                for member in result.members
            )
        rows.append(
            {
                "pool_name": pool_name,
                "trade_date": published_trade_date,
                "result_version": result_version,
                "row_kind": "status",
                "ts_code": "",
                "status": status,
                "entry_trade_date": None,
                "entry_close": None,
                "entry_result_version": None,
                "unknown_reason": None,
            }
        )
        rows.extend(member_rows)
    try:
        return ServingProjectionPayload(
            table_name="pool_membership", available_at=available_at, rows=tuple(rows)
        )
    except ValueError as error:
        if "byte budget" not in str(error) and "row budget" not in str(error):
            raise
        limited = tuple(
            {**row, "status": "source_limited"} for row in rows if row["row_kind"] == "status"
        )
        return ServingProjectionPayload(
            table_name="pool_membership", available_at=available_at, rows=limited
        )


def _return_projection(
    source: _MembershipSource,
    *,
    membership: ServingProjectionPayload,
    receipts: _VerifiedRunReceipts | None,
    observed: datetime,
) -> ServingProjectionPayload | None:
    if receipts is None:
        return None
    facts = {(fact.pool_name, fact.trade_date, fact.ts_code): fact for fact in source.return_facts}
    if len(facts) != len(source.return_facts):
        facts = {}
    current_receipts = {receipt.preset_name: receipt for receipt in receipts.latest}
    history_receipts = {
        (receipt.preset_name, receipt.trade_date): receipt
        for receipt in receipts.membership_daily or ()
    }
    current_status = {
        str(row["pool_name"]): row
        for row in membership.rows
        if row["row_kind"] == "status" and row["status"] == "verified"
    }
    rows: list[dict[str, object]] = []
    for member in membership.rows:
        if member["row_kind"] != "member" or member["status"] != "verified":
            continue
        pool_name = str(member["pool_name"])
        status = current_status.get(pool_name)
        current = current_receipts.get(pool_name)
        entry_day_raw = member["entry_trade_date"]
        entry_version = member["entry_result_version"]
        entry_close = member["entry_close"]
        if (
            status is None
            or current is None
            or entry_day_raw is None
            or entry_version is None
            or entry_close is None
            or status["trade_date"] != current.trade_date.isoformat()
            or status["result_version"] != current.result_version
            or member["trade_date"] != status["trade_date"]
            or member["result_version"] != current.result_version
            or current.contract != "screen-run-receipt/v2"
            or current.result_version not in receipts.price_digest_verified
        ):
            continue
        market_close = datetime.combine(current.trade_date, time(15), tzinfo=_SHANGHAI).astimezone(
            UTC
        )
        if observed < market_close or current.completed_at < market_close:
            continue
        entry_day = date.fromisoformat(str(entry_day_raw))
        entry = history_receipts.get((pool_name, entry_day))
        if (
            entry is None
            or entry.result_version != entry_version
            or entry.contract != "screen-run-receipt/v2"
            or entry.result_version not in receipts.price_digest_verified
        ):
            continue
        code = str(member["ts_code"])
        entry_fact = facts.get((pool_name, entry_day, code))
        current_fact = facts.get((pool_name, current.trade_date, code))
        if entry_fact is None or current_fact is None or entry_fact.close != entry_close:
            continue
        adjusted = calculate_adjusted_pool_return(
            entry_close=entry_close,
            current_close=current_fact.close,
            entry_factor=entry_fact.factor,
            current_factor=current_fact.factor,
            current_volume=current_fact.volume,
        )
        if adjusted is None:
            continue
        rows.append(
            {
                "pool_name": pool_name,
                "trade_date": current.trade_date.isoformat(),
                "result_version": current.result_version,
                "ts_code": code,
                "entry_trade_date": entry_day.isoformat(),
                "entry_result_version": entry.result_version,
                "gain_pct": adjusted.gain_pct,
                "entry_line_price": adjusted.entry_line_price,
            }
        )
    try:
        return ServingProjectionPayload(
            table_name="pool_member_return", available_at=membership.available_at, rows=tuple(rows)
        )
    except ValueError as error:
        if "byte budget" not in str(error) and "row budget" not in str(error):
            raise
        return ServingProjectionPayload(
            table_name="pool_member_return", available_at=membership.available_at, rows=()
        )


@dataclass(frozen=True, slots=True)
class _DatabaseProjection:
    """Everything one page projection takes out of the replica, in one open (#256)."""

    screen_bounds: tuple[ScreenBoundsProjectionRow, ...]
    minute_coverage: tuple[MinuteCoverageProjectionRow, ...]
    latest_trade_date: date | None
    canvas_diagnostics: tuple[CanvasDiagnosticProjectionRow, ...]
    canvas_hits: tuple[CanvasHitProjectionRow, ...]
    run_receipts: _VerifiedRunReceipts | None
    membership: _MembershipSource
    monitor_event: ServingProjectionPayload
    available_at: datetime


class DuckDBSignalPageProjectionSource:
    """Build bounded point-in-time page projections from an atomic read replica."""

    def __init__(
        self,
        database_path: Path,
        *,
        canvas_catalog_root: Path | None = None,
        canvas_receipt_root: Path | None = None,
        canvas_publication_keyring: CanvasPublicationKeyring | None = None,
        page_control_outbox: PageControlOutbox | Path | None = None,
        formula_pool_config: FormulaPoolServingConfig | None = None,
        user_presets_root: Path | None = None,
        surge_live_root: Path | None = None,
        notification_log_path: Path | None = None,
        control_root: Path | None = None,
        atomically_published: bool = False,
        read_profile: ReplicaReadProfile = UNLIMITED_READ_PROFILE,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database_path = Path(os.path.abspath(database_path))
        #: this role's own state directory, the only place it may write. Used solely as
        #: the destination for a pinned copy when the engine refuses the descriptor
        #: path (#255); nothing is ever written beside the database.
        self.control_root = None if control_root is None else Path(os.path.abspath(control_root))
        #: whether this database's owner replaces it with `rename()` (review SF-1)
        self.atomically_published = atomically_published
        self.canvas_catalog_root = (
            None if canvas_catalog_root is None else Path(os.path.abspath(canvas_catalog_root))
        )
        self.canvas_receipt_root = (
            None if canvas_receipt_root is None else Path(os.path.abspath(canvas_receipt_root))
        )
        self.canvas_publication_keyring = canvas_publication_keyring
        self.user_presets_root = (
            None if user_presets_root is None else Path(os.path.abspath(user_presets_root))
        )
        self.surge_live_root = (
            None if surge_live_root is None else Path(os.path.abspath(surge_live_root))
        )
        self.notification_log_path = (
            None if notification_log_path is None else Path(os.path.abspath(notification_log_path))
        )
        self.formula_pool_config = (
            None
            if formula_pool_config is None
            else FormulaPoolServingConfig.model_validate(formula_pool_config)
        )
        if page_control_outbox is None:
            self.page_control_outbox = None
        else:
            audit_path = (
                page_control_outbox.path
                if isinstance(page_control_outbox, PageControlOutbox)
                else Path(page_control_outbox)
            )
            self.page_control_outbox = _ReadonlyPageControlAuditReader(
                audit_path, reject_sidecars=self.formula_pool_config is not None
            )

        if self.canvas_catalog_root is not None and self.page_control_outbox is None:
            raise PageProjectionSourceIntegrityError(
                "configured canvas catalog requires readonly PageControl audit authority"
            )
        if self.user_presets_root is not None and self.page_control_outbox is None:
            raise PageProjectionSourceIntegrityError(
                "configured user pools require readonly PageControl audit authority"
            )
        if self.formula_pool_config is not None and self.page_control_outbox is None:
            raise PageProjectionSourceIntegrityError(
                "configured formula pools require readonly PageControl audit authority"
            )
        self._formula_pool_watch: FormulaPoolSourceWatch | None = None
        self._formula_pool_identity: tuple[tuple[int, ...] | None, ...] | None = None
        self._cached_formula_pool_projections: tuple[ServingProjectionPayload, ...] | None = None
        if self.canvas_catalog_root is not None and (
            self.canvas_receipt_root is None or self.canvas_publication_keyring is None
        ):
            raise PageProjectionSourceIntegrityError(
                "configured canvas catalog requires receipt root and keyring authority"
            )
        #: this reader's memory of which replica generation it has already read (#256),
        #: and of how often its role is allowed to open a newer one (#268). The default
        #: profile is inert: a caller that has not said which role this is -- a test, a
        #: CLI -- keeps package Q's behaviour, and `runtime_builder_signal` is where the
        #: notifier's fifteen minutes and its 09:20-09:40 window are attached.
        gate_arguments: dict[str, object] = {"profile": read_profile}
        if clock is not None:
            gate_arguments["clock"] = clock
        self._replica_gate: ReplicaReadGate[_DatabaseProjection] = ReplicaReadGate(
            self.database_path,
            **gate_arguments,  # type: ignore[arg-type]
        )

    @property
    def last_replica_read(self) -> ReplicaRead[_DatabaseProjection] | None:
        """What the most recent projection did with the replica, for the heartbeat."""

        return self._replica_gate.last_read

    def begin_replica_iteration(self) -> None:
        """Start a new loop iteration's accounting (review MF-1)."""

        self._replica_gate.begin_iteration()

    def replica_iteration_summary(self) -> tuple[bool, int | None]:
        """`(opened, read_bytes)` for this iteration, for the heartbeat."""

        return self._replica_gate.iteration_summary()

    def replica_iteration_skipped_by_floor(self) -> bool:
        """Whether this iteration kept an older generation on purpose (#268)."""

        return self._replica_gate.iteration_skipped_by_floor()

    def __call__(self, observed_at: datetime, /) -> SignalPageProjectionSnapshot:
        if self.page_control_outbox is None:
            return self._build_snapshot(observed_at)
        entered = False
        built = False
        try:
            with self.page_control_outbox.snapshot():
                entered = True
                result = self._build_snapshot(observed_at)
                built = True
            return result
        except (PageProjectionSourceIntegrityError, OSError, sqlite3.Error, ValueError) as exc:
            if not entered or built:
                raise _PageControlAuditSnapshotUnavailableError(
                    f"PageControl shared audit snapshot is unavailable: {exc}"
                ) from exc
            raise

    def _read_formula_pool_projections(
        self, observed: datetime
    ) -> tuple[ServingProjectionPayload, ...]:
        config = self.formula_pool_config
        if config is None:
            return ()
        assert self.page_control_outbox is not None
        watch = self._formula_pool_watch
        cached = self._cached_formula_pool_projections
        if watch is not None and cached is not None:
            current = watch.identity()
            if current == self._formula_pool_identity:
                if watch.identity() != current:
                    raise PageProjectionSourceIntegrityError(
                        "formula pool authority changed while reusing verified projection"
                    )
                if cached[0].available_at > observed:
                    raise PageProjectionSourceIntegrityError(
                        "formula pool authority is newer than observation"
                    )
                return cached
        catalog_before = FormulaPoolSourceWatch.catalog_identity(
            config, self.page_control_outbox.path
        )
        projections = read_formula_pool_projections(
            config,
            self.page_control_outbox.formula_pool_saves(),
            observed_at=observed,
        )
        watch = FormulaPoolSourceWatch.from_projections(
            config, self.page_control_outbox.path, projections
        )
        identity = watch.identity()
        if (
            FormulaPoolSourceWatch.catalog_identity(config, self.page_control_outbox.path)
            != catalog_before
            or watch.identity() != identity
        ):
            raise PageProjectionSourceIntegrityError(
                "formula pool authority changed while binding verified projection"
            )
        self._formula_pool_watch = watch
        self._formula_pool_identity = identity
        self._cached_formula_pool_projections = projections
        return projections

    def _build_snapshot(self, observed_at: datetime) -> SignalPageProjectionSnapshot:
        observed = normalize_aware_utc(observed_at)
        cutoff = _local_naive(observed)
        #: One `lstat`, and the database is opened only if this generation of the replica
        #: has not already been read (#256). This role's interval is two seconds and the
        #: replica is replaced every five minutes, so before this it scanned all of
        #: `minute_bar` in a 10 GB file about a hundred and fifty times per generation.
        #:
        #: `key` carries `cutoff.date()`, which is the **local** date (`_local_naive` is
        #: this module's own Asia/Shanghai conversion), because that is the granularity
        #: every predicate below is written against; the gate itself compares instants and
        #: knows nothing about a local zone (review SF-3). This matters in production
        #: rather than in theory: `rquant-replica-sync.timer`'s last run of the day is
        #: 17:30 and the next is 09:00, so one generation spans local midnight, and
        #: without the date in the key a projection taken at 23:59 would still be served
        #: at 00:01 with `trade_date <= yesterday`.
        #:
        #: `cutoff=observed` carries the instant, so an answer is reused only when it was
        #: taken at or after the generation's own mtime -- every row in the file was
        #: written before the file was, so a later cutoff admits exactly the same rows.
        read = self._replica_gate.read(
            lambda: self._read_database_projection(cutoff, observed=observed),
            key=("signal-page-projection", cutoff.date()),
            cutoff=observed,
        )
        database = read.value
        screen_bounds = database.screen_bounds
        minute_coverage = database.minute_coverage
        latest_date = database.latest_trade_date
        diagnostics = database.canvas_diagnostics
        hits = database.canvas_hits
        available = database.available_at
        canvas_definitions = self._canvas_definitions(observed=observed)
        pool_definition = (
            _pool_definition_projection(
                self.user_presets_root,
                self.page_control_outbox,
            )
            if self.user_presets_root is not None
            else (
                ServingProjectionPayload(
                    table_name="pool_definition",
                    available_at=_EMPTY_PROJECTION_AVAILABLE_AT,
                    rows=build_pool_definition_rows({}, {}, root_path=""),
                )
                if database.run_receipts is not None
                else None
            )
        )
        run_receipt_projection = (
            _receipt_projection(database.run_receipts, pool_definition)
            if database.run_receipts is not None and pool_definition is not None
            else None
        )
        membership_definitions = pool_definition or ServingProjectionPayload(
            table_name="pool_definition",
            available_at=_EMPTY_PROJECTION_AVAILABLE_AT,
            rows=build_pool_definition_rows({}, {}, root_path=""),
        )
        pool_membership = _membership_projection(
            database.membership,
            screen_bounds=screen_bounds,
            definitions=membership_definitions,
            receipts=database.run_receipts,
            available_at=max(
                available,
                run_receipt_projection.available_at
                if run_receipt_projection is not None
                else _EMPTY_PROJECTION_AVAILABLE_AT,
                database.run_receipts.newest_candidate_at
                if database.run_receipts is not None
                and database.run_receipts.newest_candidate_at is not None
                else _EMPTY_PROJECTION_AVAILABLE_AT,
            ),
        )
        pool_member_return = _return_projection(
            database.membership,
            membership=pool_membership,
            receipts=database.run_receipts,
            observed=observed,
        )
        pulse_history, pulse_alerts, runtime_config = _read_surge_live_projection_sources(
            self.surge_live_root,
            observed=observed,
        )
        try:
            surge_event = _read_surge_event_projection(self.surge_live_root, observed=observed)
        except (OSError, PageProjectionSourceIntegrityError, ValueError) as error:
            logger.warning("爆量事件来源暂不可用：{}", error)
            surge_event = None
        legacy_notification, legacy_notification_status = self.legacy_notification_projections(
            observed
        )
        alert_ack_projections = build_ack_source_projections(
            None
            if self.page_control_outbox is None
            else self.page_control_outbox.alert_ack_snapshot(),
            observed_at=observed,
        )
        try:
            manual_watchlist_projections = build_manual_watchlist_projections(
                None
                if self.page_control_outbox is None
                else self.page_control_outbox.manual_watchlist_snapshot(),
                observed_at=observed,
            )
        except (OSError, sqlite3.Error, PageProjectionSourceIntegrityError, ValueError) as exc:
            logger.warning("手动盯盘名单来源暂不可用：{}", exc)
            manual_watchlist_projections = build_manual_watchlist_projections(
                None, observed_at=observed
            )
        try:
            price_alert_rule_projections = build_price_alert_rule_projections(
                None
                if self.page_control_outbox is None
                else self.page_control_outbox.price_alert_rule_snapshot(),
                observed_at=observed,
                unavailable=self.page_control_outbox is None,
            )
        except (OSError, sqlite3.Error, PageProjectionSourceIntegrityError, ValueError) as exc:
            logger.warning("价格提醒规则来源暂不可用：{}", exc)
            price_alert_rule_projections = build_price_alert_rule_projections(
                None, observed_at=observed, unavailable=True
            )
        formula_pool_projections: tuple[ServingProjectionPayload, ...] = ()
        if self.formula_pool_config is not None:
            try:
                formula_pool_projections = self._read_formula_pool_projections(observed)
            except (OSError, sqlite3.Error, KeyError, ValueError) as exc:
                raise PageProjectionSourceIntegrityError(
                    "configured formula pool authority is invalid"
                ) from exc
        if canvas_definitions:
            available = max(
                available,
                max(item.updated_at for item in canvas_definitions),
            )
        return SignalPageProjectionSnapshot.create(
            available_at=available,
            screen_bounds=screen_bounds,
            minute_coverage=minute_coverage,
            canvas_diagnostics=diagnostics,
            canvas_latest_trade_date=(
                None
                if latest_date is None
                else CanvasLatestTradeDateProjectionRow(trade_date=latest_date)
            ),
            canvas_hits=hits,
            canvas_definitions=canvas_definitions,
            pool_definition=pool_definition,
            formula_pool_state=(formula_pool_projections[0] if formula_pool_projections else None),
            formula_pool_definition=(
                formula_pool_projections[1] if formula_pool_projections else None
            ),
            formula_pool_latest_result=(
                formula_pool_projections[2] if formula_pool_projections else None
            ),
            screen_run_receipt=run_receipt_projection,
            pool_membership=pool_membership,
            pool_member_return=pool_member_return,
            pulse_history=pulse_history,
            pulse_alerts=pulse_alerts,
            surge_runtime_config=runtime_config,
            monitor_event=database.monitor_event,
            surge_event=surge_event,
            legacy_notification=legacy_notification,
            legacy_notification_status=legacy_notification_status,
            alert_ack_state=alert_ack_projections[0],
            alert_ack=(alert_ack_projections[1] if len(alert_ack_projections) > 1 else None),
            manual_watchlist_state=manual_watchlist_projections[0],
            manual_watchlist=(
                manual_watchlist_projections[1] if len(manual_watchlist_projections) > 1 else None
            ),
            price_alert_rule_state=price_alert_rule_projections[0],
            price_alert_rule=(
                price_alert_rule_projections[1] if len(price_alert_rule_projections) > 1 else None
            ),
        )

    def legacy_notification_projections(
        self, observed: datetime
    ) -> tuple[ServingProjectionPayload | None, ServingProjectionPayload | None]:
        if self.notification_log_path is None:
            return None, None
        try:
            result = _read_legacy_notification_projections(
                self.notification_log_path, observed=observed
            )
        except (OSError, PageProjectionSourceIntegrityError, ValueError):
            logger.warning("旧通知记录来源暂不可用")
            result = None
        if result is None:
            return None, _legacy_notification_status(
                state="unavailable", skipped=0, available_at=_EMPTY_PROJECTION_AVAILABLE_AT
            )
        return result

    def _read_database_projection(
        self,
        cutoff: datetime,
        *,
        observed: datetime | None = None,
    ) -> _DatabaseProjection:
        """Everything this projection takes out of the replica, in one open.

        Split out of `_build_snapshot` so the gate above has something to remember. What
        stays outside it is what does not live in the replica and changes on its own: the
        canvas catalog, the PageControl audit, and the `surge_live` JSONL -- caching those
        with the database would delay a canvas by up to a replica period.
        """

        stable = _StableReadonlyDuckDB(
            self.database_path,
            control_root=self.control_root,
            atomically_published=self.atomically_published,
        )
        with stable as connection:
            #: Whether every row in this generation was already written when the cutoff was
            #: taken. The replica is *replaced* whole, never written in place, so a cutoff
            #: at or after the file's own mtime is one that `created_at <= cutoff` cannot
            #: exclude a single row by -- and a predicate that cannot exclude anything is a
            #: column DuckDB does not have to read (review SF-7). False when the reader
            #: cannot say, or when the file carries a stamp from the future, in which case
            #: the predicate is evaluated exactly as before.
            written_at = stable.generation_modified_at
            sealed_before_cutoff = (
                observed is not None and written_at is not None and observed >= written_at
            )
            self._require_tables(connection)
            screen_rows = connection.execute(
                """
                SELECT preset_name, MIN(trade_date), MAX(trade_date), COUNT(*)
                FROM screen_result
                WHERE trade_date <= ? AND created_at <= ?
                GROUP BY preset_name
                ORDER BY preset_name
                LIMIT ?
                """,
                (cutoff.date(), cutoff, _MAX_SCREEN_PRESETS + 1),
            ).fetchall()
            if len(screen_rows) > _MAX_SCREEN_PRESETS:
                raise PageProjectionSourceIntegrityError(
                    "screen bounds exceed the bounded projection limit"
                )
            screen_bounds = tuple(
                ScreenBoundsProjectionRow(
                    preset_name=str(preset),
                    min_date=minimum,
                    max_date=maximum,
                    candidate_count=int(count),
                )
                for preset, minimum, maximum, count in screen_rows
            )
            minute_coverage = self._minute_coverage(
                connection,
                cutoff=cutoff,
                generation_sealed_before_cutoff=sealed_before_cutoff,
            )
            latest_row = connection.execute(
                """
                SELECT MAX(trade_date)
                FROM screen_result
                WHERE trade_date <= ? AND created_at <= ?
                """,
                (cutoff.date(), cutoff),
            ).fetchone()
            latest_date = None if latest_row is None else latest_row[0]
            membership_target_date = latest_date
            run_receipts = _read_verified_run_receipts(
                connection,
                cutoff=cutoff,
                observed=observed or cutoff.replace(tzinfo=_SHANGHAI).astimezone(UTC),
                generation_sealed_before_cutoff=sealed_before_cutoff,
            )
            if run_receipts is not None:
                if run_receipts.newest_candidate_day is not None:
                    membership_target_date = (
                        max(
                            membership_target_date,
                            run_receipts.newest_candidate_day,
                        )
                        if membership_target_date is not None
                        else run_receipts.newest_candidate_day
                    )
                trusted_date = max((item.trade_date for item in run_receipts.latest), default=None)
                if trusted_date is not None:
                    latest_date = max(latest_date, trusted_date) if latest_date else trusted_date
                if run_receipts.newest_candidate_day is not None and (
                    latest_date is None or run_receipts.newest_candidate_day > latest_date
                ):
                    # A newer, invalid run cannot make yesterday's members look current.
                    latest_date = None
            membership_source = _read_membership_source(
                connection,
                receipts=run_receipts,
                target_date=membership_target_date,
                cutoff=cutoff,
                observed=observed or cutoff.replace(tzinfo=_SHANGHAI).astimezone(UTC),
            )
            diagnostics: tuple[CanvasDiagnosticProjectionRow, ...] = ()
            hits: tuple[CanvasHitProjectionRow, ...] = ()
            if latest_date is not None:
                diagnostic_rows = connection.execute(
                    """
                    SELECT preset_name, COUNT(*)
                    FROM screen_result
                    WHERE trade_date = ? AND created_at <= ?
                    GROUP BY preset_name
                    ORDER BY preset_name
                    LIMIT ?
                    """,
                    (latest_date, cutoff, _MAX_SCREEN_PRESETS + 1),
                ).fetchall()
                if len(diagnostic_rows) > _MAX_SCREEN_PRESETS:
                    raise PageProjectionSourceIntegrityError(
                        "canvas diagnostics exceed the bounded projection limit"
                    )
                diagnostics = tuple(
                    CanvasDiagnosticProjectionRow(
                        trade_date=latest_date,
                        preset_name=str(preset),
                        step_index=0,
                        rule_label="final",
                        remaining_count=int(count),
                    )
                    for preset, count in diagnostic_rows
                )
                hit_rows = connection.execute(
                    """
                    SELECT preset_name, ts_code, name, close, pct_chg
                    FROM screen_result
                    WHERE trade_date = ? AND created_at <= ?
                    ORDER BY preset_name, ts_code
                    LIMIT ?
                    """,
                    (latest_date, cutoff, _MAX_CANVAS_HITS + 1),
                ).fetchall()
                if len(hit_rows) > _MAX_CANVAS_HITS:
                    raise PageProjectionSourceIntegrityError(
                        "canvas hits exceed the bounded projection limit"
                    )
                hits = tuple(
                    CanvasHitProjectionRow(
                        trade_date=latest_date,
                        preset_name=str(preset),
                        ts_code=str(ts_code),
                        row_json=json.dumps(
                            {
                                "close": close,
                                "name": name,
                                "pct_chg": pct_chg,
                                "ts_code": ts_code,
                            },
                            ensure_ascii=True,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    )
                    for preset, ts_code, name, close, pct_chg in hit_rows
                )
            available_row = connection.execute(
                """
                SELECT MAX(created_at) FROM (
                    SELECT MAX(created_at) AS created_at
                    FROM screen_result WHERE trade_date <= ? AND created_at <= ?
                    UNION ALL
                    SELECT MAX(created_at) AS created_at
                    FROM minute_bar WHERE trade_time <= ? AND created_at <= ?
                )
                """,
                (cutoff.date(), cutoff, cutoff, cutoff),
            ).fetchone()
            window_start = cutoff.date() - timedelta(days=_EVENT_WINDOW_DAYS - 1)
            monitor_rows = connection.execute(
                """
                SELECT trade_date, trigger_time, ts_code, level, trigger_price,
                       level_price, trigger_type, pool
                FROM monitor_event
                WHERE trade_date BETWEEN ? AND ?
                  AND trigger_time >= ? AND trigger_time <= ?
                ORDER BY trade_date DESC, trigger_time DESC, ts_code, level
                LIMIT ?
                """,
                (
                    window_start,
                    cutoff.date(),
                    datetime.combine(window_start, time.min),
                    cutoff,
                    _MAX_EVENT_ROWS + 1,
                ),
            ).fetchall()
        receipt_available = (
            max(
                (item.completed_at for item in run_receipts.latest),
                default=None,
            )
            if run_receipts is not None
            else None
        )
        if (available_row is None or available_row[0] is None) and receipt_available is None:
            raise PageProjectionSourceIntegrityError("projection database has no PIT evidence")
        database_available = max(
            (
                _database_timestamp(available_row[0])
                if available_row is not None and available_row[0] is not None
                else _EMPTY_PROJECTION_AVAILABLE_AT
            ),
            receipt_available or _EMPTY_PROJECTION_AVAILABLE_AT,
        )
        if len(monitor_rows) > _MAX_EVENT_ROWS:
            raise PageProjectionSourceIntegrityError("monitor events exceed the row bound")
        published_monitor: list[dict[str, object]] = []
        monitor_available = database_available
        for (
            trade_day,
            trigger_at,
            code,
            level,
            price,
            level_price,
            trigger_type,
            pool,
        ) in monitor_rows:
            if not isinstance(trigger_at, datetime) or trigger_at.tzinfo is not None:
                raise PageProjectionSourceIntegrityError(
                    "monitor event time must be local naive time"
                )
            if trade_day != trigger_at.date():
                raise PageProjectionSourceIntegrityError(
                    "monitor event trade date differs from time"
                )
            at = _database_timestamp(trigger_at)
            if at > observed:
                raise PageProjectionSourceIntegrityError("monitor events contain future evidence")
            monitor_available = max(monitor_available, at)
            published_monitor.append(
                {
                    "trade_date": trade_day.isoformat(),
                    "trigger_time": at.isoformat(),
                    "ts_code": str(code),
                    "level": str(level),
                    "trigger_price": price,
                    "level_price": level_price,
                    "trigger_type": trigger_type,
                    "pool": pool,
                }
            )
        monitor_projection = ServingProjectionPayload(
            table_name="monitor_event",
            available_at=monitor_available,
            rows=tuple(published_monitor),
        )
        return _DatabaseProjection(
            screen_bounds=screen_bounds,
            minute_coverage=minute_coverage,
            latest_trade_date=latest_date,
            canvas_diagnostics=diagnostics,
            canvas_hits=hits,
            run_receipts=run_receipts,
            membership=membership_source,
            monitor_event=monitor_projection,
            available_at=database_available,
        )

    def _canvas_definitions(
        self,
        *,
        observed: datetime,
    ) -> tuple[CanvasDefinitionProjectionRow, ...]:
        root = self.canvas_catalog_root
        if root is None:
            return ()
        try:
            catalog_binding = _bind_readonly_directory(
                root,
                label="canvas catalog",
            )
        except FileNotFoundError:
            self._verify_canvas_current_heads((), observed=observed)
            return ()
        if self.canvas_receipt_root is None:
            catalog_binding.close()
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt authority is unavailable"
            )
        root_descriptor = catalog_binding.descriptor
        try:
            catalog_binding.verify()
            rows: list[CanvasDefinitionProjectionRow] = []
            total_bytes = 0
            for child_name in sorted(os.listdir(root_descriptor)):
                if not child_name.endswith(".json") or Path(child_name).name != child_name:
                    continue
                item = os.stat(child_name, dir_fd=root_descriptor, follow_symlinks=False)
                if stat.S_ISLNK(item.st_mode) or not stat.S_ISREG(item.st_mode):
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record must be a regular non-symlink file"
                    )
                if item.st_size > _MAX_CANVAS_DEFINITION_BYTES:
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record exceeds size bound"
                    )
                total_bytes += item.st_size
                if total_bytes > _MAX_CANVAS_CATALOG_BYTES:
                    raise PageProjectionSourceIntegrityError("canvas catalog exceeds size bound")
                descriptor = os.open(
                    child_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_descriptor,
                )
                try:
                    opened = os.fstat(descriptor)
                    if _file_identity(opened) != _file_identity(item):
                        raise PageProjectionSourceIntegrityError(
                            "canvas catalog record rotated while read"
                        )
                    with os.fdopen(descriptor, "rb", closefd=False) as handle:
                        raw_bytes = handle.read(_MAX_CANVAS_DEFINITION_BYTES + 1)
                finally:
                    os.close(descriptor)
                if len(raw_bytes) > _MAX_CANVAS_DEFINITION_BYTES:
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record exceeds size bound"
                    )
                try:
                    raw = json.loads(raw_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise PageProjectionSourceIntegrityError(
                        "canvas catalog record is not valid JSON"
                    ) from exc
                row = CanvasDefinitionProjectionRow.from_catalog_record(
                    file_name=child_name,
                    raw=raw,
                    observed=observed,
                )
                self._verify_canvas_page_control_receipt(
                    row,
                    observed=observed,
                )
                rows.append(row)
                if len(rows) > _MAX_CANVAS_DEFINITIONS:
                    raise PageProjectionSourceIntegrityError("canvas catalog exceeds row bound")
            catalog_binding.verify()
        finally:
            catalog_binding.close()
        self._verify_canvas_current_heads(tuple(rows), observed=observed)
        return tuple(rows)

    def _verify_canvas_current_heads(
        self,
        rows: tuple[CanvasDefinitionProjectionRow, ...],
        *,
        observed: datetime,
    ) -> None:
        if self.canvas_catalog_root is None or self.canvas_publication_keyring is None:
            if rows:
                raise PageProjectionSourceIntegrityError(
                    "canvas current head authority is unavailable"
                )
            return
        head_root = self.canvas_catalog_root.parent / "canvas-publication-heads"
        heads = self._read_canvas_head_authority(
            head_root,
            label="canvas current head",
            observed=observed,
        )
        watermark_root = self.canvas_catalog_root.parent / "canvas-publication-watermarks"
        watermarks = self._read_canvas_head_authority(
            watermark_root,
            label="canvas immutable watermark",
            observed=observed,
        )
        if set(heads) != set(watermarks):
            raise PageProjectionSourceIntegrityError(
                "canvas current heads do not match immutable watermark authority"
            )
        for name, head in heads.items():
            watermark = watermarks[name]
            if (
                head.receipt.receipt_id != watermark.receipt.receipt_id
                or head.sequence != watermark.sequence
                or head.state != watermark.state
                or head.publication_receipt_id != watermark.publication_receipt_id
            ):
                raise PageProjectionSourceIntegrityError(
                    "canvas authority rollback detected by immutable watermark"
                )
        rows_by_name = {row.name: row for row in rows}
        active_names = {name for name, head in heads.items() if head.state == "active"}
        if active_names != set(rows_by_name):
            raise PageProjectionSourceIntegrityError(
                "canvas catalog does not match the complete current head authority"
            )
        if self.page_control_outbox is not None:
            audit_names = set(self.page_control_outbox.canvas_mutations())
            if audit_names != set(heads):
                raise PageProjectionSourceIntegrityError(
                    "canvas current heads do not match PageControl mutation authority"
                )
        for name, head in heads.items():
            if head.state == "deleted" and name in rows_by_name:
                raise PageProjectionSourceIntegrityError(
                    "canvas tombstone conflicts with a catalog definition"
                )
            self._verify_canvas_head_page_control_audit(head)

    def _read_canvas_head_authority(
        self,
        root: Path,
        *,
        label: str,
        observed: datetime,
    ) -> dict[str, CanvasCurrentHead]:
        try:
            binding = _bind_readonly_directory(root, label=label)
        except FileNotFoundError as exc:
            has_audit_authority = self.page_control_outbox is not None and bool(
                self.page_control_outbox.canvas_mutations()
            )
            if has_audit_authority:
                raise PageProjectionSourceIntegrityError(f"{label} authority is missing") from exc
            return {}
        descriptor = binding.descriptor
        try:
            binding.verify()
            names = sorted(os.listdir(descriptor))
            heads: dict[str, CanvasCurrentHead] = {}
            for name in names:
                item = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISLNK(item.st_mode) or not stat.S_ISDIR(item.st_mode):
                    raise PageProjectionSourceIntegrityError(
                        f"{label} entry must be a regular non-symlink directory"
                    )
                child_descriptor = os.open(
                    name,
                    os.O_RDONLY
                    | os.O_CLOEXEC
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=descriptor,
                )
                try:
                    opened = os.fstat(child_descriptor)
                    if (opened.st_dev, opened.st_ino) != (item.st_dev, item.st_ino):
                        raise PageProjectionSourceIntegrityError(
                            f"{label} entry rotated while open"
                        )
                    head = read_canvas_current_head(
                        root,
                        name,
                        self.canvas_publication_keyring,
                        observed_at=observed,
                        directory_descriptor=child_descriptor,
                    )
                except Exception as exc:
                    raise PageProjectionSourceIntegrityError(
                        f"{label} cannot be verified: {exc}"
                    ) from exc
                finally:
                    os.close(child_descriptor)
                if head is not None:
                    heads[name] = head
            binding.verify()
        finally:
            binding.close()
        return heads

    def _verify_canvas_head_page_control_audit(self, head: CanvasCurrentHead) -> None:
        if self.page_control_outbox is None:
            return
        audit = self.page_control_outbox.audit(head.receipt.claims.command.command_id)
        if audit is None or audit.status != PageControlStatus.SUCCEEDED:
            raise PageProjectionSourceIntegrityError(
                "PageControl receipt is missing or not succeeded for canvas current head"
            )
        if (
            audit.command_kind != head.authority_command_kind
            or audit.command_hash != head.authority_command_hash
            or audit.payload.get("kind") != audit.command_kind
        ):
            raise PageProjectionSourceIntegrityError(
                "PageControl command authority mismatch for canvas current head"
            )
        latest = self.page_control_outbox.canvas_mutations().get(head.receipt.claims.command.name)
        if latest is None or latest.command_id != audit.command_id:
            raise PageProjectionSourceIntegrityError(
                "canvas current head is not the latest PageControl mutation authority"
            )

    def _verify_canvas_page_control_receipt(
        self,
        row: CanvasDefinitionProjectionRow,
        *,
        observed: datetime,
    ) -> None:
        if self.canvas_receipt_root is None or self.canvas_publication_keyring is None:
            raise PageProjectionSourceIntegrityError(
                "CanvasPublicationReceipt keyring and root are required for canvas definitions"
            )
        try:
            receipt_binding = _bind_readonly_directory(
                self.canvas_receipt_root,
                label="canvas publication receipt root",
            )
            try:
                publication = CanvasPublicationReceiptStore(
                    self.canvas_receipt_root,
                    directory_descriptor=receipt_binding.descriptor,
                ).read(row.publication_receipt_id)
                receipt_binding.verify()
            finally:
                receipt_binding.close()
        except Exception as exc:
            raise PageProjectionSourceIntegrityError(
                f"canvas publication receipt cannot be read safely: {exc}"
            ) from exc
        if not self.canvas_publication_keyring.verify_publication_receipt(
            publication,
            require_active=True,
        ):
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt active signature verification failed"
            )
        self._verify_canvas_receipt_time(publication, observed=observed)
        self._verify_canvas_publication_receipt_semantics(row, publication)
        if self.canvas_catalog_root is None:
            raise PageProjectionSourceIntegrityError("canvas current head root cannot be derived")
        try:
            head = read_canvas_current_head(
                self.canvas_catalog_root.parent / "canvas-publication-heads",
                row.name,
                self.canvas_publication_keyring,
                observed_at=observed,
            )
        except Exception as exc:
            raise PageProjectionSourceIntegrityError(
                f"canvas current head cannot be verified: {exc}"
            ) from exc
        if (
            head is None
            or head.state != "active"
            or head.publication_receipt_id != publication.receipt_id
        ):
            raise PageProjectionSourceIntegrityError(
                "canvas catalog does not match its exact current head"
            )
        self._verify_canvas_head_page_control_audit(head)

    @staticmethod
    def _verify_canvas_receipt_time(
        publication: CanvasPublicationReceipt,
        *,
        observed: datetime,
    ) -> None:
        claims = publication.claims
        timestamps = (
            ("requested_at", claims.command.requested_at),
            ("created_at", claims.created_at),
            ("catalog created_at", claims.catalog_record.created_at),
            ("catalog updated_at", claims.catalog_record.updated_at),
        )
        for label, value in timestamps:
            if value > observed:
                raise PageProjectionSourceIntegrityError(
                    f"canvas publication receipt {label} contains future evidence"
                )

    @staticmethod
    def _verify_canvas_publication_receipt_semantics(
        row: CanvasDefinitionProjectionRow,
        publication: CanvasPublicationReceipt,
    ) -> None:
        claims = publication.claims
        command = claims.command
        if canvas_command_hash(command) != row.command_hash:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt command hash mismatch"
            )
        expected_source_identity_hash = canvas_source_identity_hash(
            command_id=command.command_id,
            command_hash=row.command_hash,
            source=command.source,
        )
        if expected_source_identity_hash != row.source_identity_hash:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt source identity mismatch"
            )
        expected_effect_id = canvas_publication_effect_id(
            command_hash=row.command_hash,
            source_identity_hash=row.source_identity_hash,
            consumer_service_id=claims.consumer_service_id,
            consumer_instance_id=claims.consumer_instance_id,
        )
        if claims.effect_id != expected_effect_id:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt effect identity mismatch"
            )
        expected_generation_id = canvas_publication_generation_id(
            command_hash=row.command_hash,
            source_identity_hash=row.source_identity_hash,
            effect_id=claims.effect_id,
        )
        if row.publication_generation_id != expected_generation_id:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt generation mismatch"
            )
        expected_receipt_id = canvas_publication_receipt_id(
            command_hash=row.command_hash,
            source_identity_hash=row.source_identity_hash,
            effect_id=claims.effect_id,
            generation_id=row.publication_generation_id,
        )
        if publication.receipt_id != expected_receipt_id:
            raise PageProjectionSourceIntegrityError("canvas publication receipt identity mismatch")
        catalog = claims.catalog_record
        if catalog.model_dump(mode="json") != {
            "schema_version": row.schema_version,
            "name": row.name,
            "description": row.description,
            "pool_refs": list(row.pool_refs),
            "created_at": row.created_at.isoformat().replace("+00:00", "Z"),
            "updated_at": row.updated_at.isoformat().replace("+00:00", "Z"),
            "source": row.source,
            "command_id": row.command_id,
            "command_hash": row.command_hash,
            "source_identity_hash": row.source_identity_hash,
            "publication_generation_id": row.publication_generation_id,
            "publication_receipt_id": row.publication_receipt_id,
            "record_hash": row.record_hash,
        }:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt catalog semantics do not match catalog"
            )
        command_fields = {
            "name": row.name,
            "description": row.description,
            "pool_refs": row.pool_refs,
            "source": row.source,
            "command_id": row.command_id,
        }
        for field_name, expected in command_fields.items():
            if getattr(command, field_name) != expected:
                raise PageProjectionSourceIntegrityError(
                    "canvas publication receipt command payload does not match catalog"
                )
        if claims.catalog_record_hash != row.record_hash:
            raise PageProjectionSourceIntegrityError(
                "canvas publication receipt record hash does not match catalog"
            )

    @staticmethod
    def _require_tables(connection: duckdb.DuckDBPyConnection) -> None:
        rows = connection.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name IN ('screen_result', 'minute_bar', 'monitor_event')
            """
        ).fetchall()
        if {str(row[0]) for row in rows} != {"screen_result", "minute_bar", "monitor_event"}:
            raise PageProjectionSourceIntegrityError(
                "projection database is missing screen_result, minute_bar or monitor_event"
            )

    @staticmethod
    def _minute_coverage(
        connection: duckdb.DuckDBPyConnection,
        *,
        cutoff: datetime,
        generation_sealed_before_cutoff: bool = False,
    ) -> tuple[MinuteCoverageProjectionRow, ...]:
        """Per-source and total 1-minute coverage, in **one** pass over `minute_bar` (#256).

        This projection was, and after this package still is, the whole of what the
        notifier reads from the replica -- 44,052,711 of 44,052,711 bytes on the package Q
        measurement replica. It used to run *two* independent aggregates over the same
        table, one grouped by source and one ungrouped, so every generation was scanned
        twice. `GROUPING SETS ((COALESCE(source,'unknown')), ())` computes both from one
        `SEQ_SCAN`, and `COUNT(DISTINCT ...)` is evaluated per grouping set, so the numbers
        are the same numbers (review SF-1).

        **Nothing about the published projection changes**: same rows, same values, same
        order (the total first, then the sources ascending), and an empty table still
        yields no rows at all -- the `()` grouping set does produce one row there, with
        `COUNT(*) = 0`, and the same `> 0` guard as before drops it.

        The row limit keeps its meaning too. `_MAX_MINUTE_SOURCES + 2` is fetched because
        the total shares the result set; ordering by the grouping flag first puts the
        source rows ahead of it, so more sources than the budget still overflows the count
        and still refuses.

        **`generation_sealed_before_cutoff` drops the `created_at` predicate, and nothing
        else** (review SF-7). A replica generation is *replaced* whole and never written in
        place, so every row inside it was written before the file was: a cutoff at or after
        the file's own mtime is one that `created_at <= cutoff` cannot exclude a single row
        by. The predicate is then not a cheaper filter, it is a column -- `created_at` is
        one of five this scan reads, and dropping the clause takes it out of the plan
        entirely. The published values are identical, which is the whole point and is
        asserted directly rather than argued: the two forms are compared row by row on
        several fixtures, and the scanned column list is read out of `EXPLAIN ANALYZE`.

        The caller passes False whenever it cannot establish that -- no descriptor, or a
        replica stamped in the future -- and then this runs exactly the query it ran
        before. Ruling 30 item 3 asked for a *trade-date* narrowing, which is not available
        (it changes three published fields, `test_the_minute_coverage_projection_cannot_be
        _narrowed_to_the_current_trade_date`); this is the half of it that is.
        """

        if generation_sealed_before_cutoff:
            rows = connection.execute(
                """
                SELECT GROUPING(COALESCE(source, 'unknown')) AS is_total,
                       COALESCE(source, 'unknown') AS source_label,
                       COUNT(*), COUNT(DISTINCT ts_code),
                       COUNT(DISTINCT CAST(trade_time AS DATE)),
                       MIN(trade_time), MAX(trade_time)
                FROM minute_bar
                WHERE freq = '1min' AND trade_time <= ?
                GROUP BY GROUPING SETS ((COALESCE(source, 'unknown')), ())
                ORDER BY is_total, source_label
                LIMIT ?
                """,
                (cutoff, _MAX_MINUTE_SOURCES + 2),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT GROUPING(COALESCE(source, 'unknown')) AS is_total,
                       COALESCE(source, 'unknown') AS source_label,
                       COUNT(*), COUNT(DISTINCT ts_code),
                       COUNT(DISTINCT CAST(trade_time AS DATE)),
                       MIN(trade_time), MAX(trade_time)
                FROM minute_bar
                WHERE freq = '1min' AND trade_time <= ? AND created_at <= ?
                GROUP BY GROUPING SETS ((COALESCE(source, 'unknown')), ())
                ORDER BY is_total, source_label
                LIMIT ?
                """,
                (cutoff, cutoff, _MAX_MINUTE_SOURCES + 2),
            ).fetchall()
        grouped = [row for row in rows if not int(row[0])]
        if len(grouped) > _MAX_MINUTE_SOURCES:
            raise PageProjectionSourceIntegrityError(
                "minute sources exceed the bounded projection limit"
            )
        total = next((row for row in rows if int(row[0])), None)
        values: list[MinuteCoverageProjectionRow] = []
        if total is not None and int(total[2]) > 0:
            values.append(
                MinuteCoverageProjectionRow(
                    is_total=True,
                    source="all",
                    rows_count=int(total[2]),
                    codes_count=int(total[3]),
                    trade_dates=int(total[4]),
                    min_time=_database_timestamp(total[5]),
                    max_time=_database_timestamp(total[6]),
                )
            )
        values.extend(
            MinuteCoverageProjectionRow(
                is_total=False,
                source=str(source),
                rows_count=int(count),
                codes_count=int(codes),
                trade_dates=int(trade_dates),
                min_time=_database_timestamp(minimum),
                max_time=_database_timestamp(maximum),
            )
            for _flag, source, count, codes, trade_dates, minimum, maximum in grouped
        )
        return tuple(values)


class DuckDBLabPageProjectionSource:
    """Project formal research gate metadata from one stable research replica."""

    def __init__(
        self,
        database_path: Path,
        *,
        control_root: Path | None = None,
        audit_report_path: Path | None = None,
        audit_report_job_state_path: Path | None = None,
        audit_report_job_directory: Path | None = None,
        formula_market_job_state_path: Path | None = None,
        formula_market_job_directory: Path | None = None,
        backfill_plan_directory: Path | None = None,
        backfill_plan_job_state_path: Path | None = None,
        factor_registry: FactorDefinitionRegistry | None = None,
        factor_registry_identity: FactorRegistryIdentity | None = None,
        factor_tracking_identity: FactorTrackingIdentity | None = None,
    ) -> None:
        self.database_path = Path(os.path.abspath(database_path))
        #: this role's own state directory; see `_StableReadonlyDuckDB` (#255)
        self.control_root = None if control_root is None else Path(os.path.abspath(control_root))
        if audit_report_path is not None and not audit_report_path.is_absolute():
            raise ValueError("audit report path must be absolute")
        if (audit_report_job_state_path is None) != (audit_report_job_directory is None):
            raise ValueError("audit report job state and directory require paired paths")
        if audit_report_path is not None and audit_report_job_state_path is not None:
            raise ValueError(
                "audit report job and explicit file modes are exclusive, not ambiguous"
            )
        if audit_report_job_state_path is not None and (
            not audit_report_job_state_path.is_absolute()
            or not audit_report_job_directory.is_absolute()
        ):
            raise ValueError("audit report job paths must be absolute")
        self.audit_report_path = audit_report_path
        self.audit_report_job_state_path = audit_report_job_state_path
        self.audit_report_job_directory = audit_report_job_directory
        if (formula_market_job_state_path is None) != (formula_market_job_directory is None):
            raise ValueError("formula market job state and result directory require paired paths")
        if formula_market_job_state_path is not None and (
            not formula_market_job_state_path.is_absolute()
            or not formula_market_job_directory.is_absolute()
        ):
            raise ValueError("formula market job paths must be absolute")
        self.formula_market_job_state_path = formula_market_job_state_path
        self.formula_market_job_directory = formula_market_job_directory
        if backfill_plan_directory is not None and not backfill_plan_directory.is_absolute():
            raise ValueError("backfill plan directory must be absolute")
        self.backfill_plan_directory = backfill_plan_directory
        if backfill_plan_job_state_path is not None:
            if not backfill_plan_job_state_path.is_absolute():
                raise ValueError("backfill plan job state path must be absolute")
            if backfill_plan_directory is None:
                raise ValueError("backfill plan job state requires a plan directory")
        self.backfill_plan_job_state_path = backfill_plan_job_state_path
        if (factor_registry is None) != (factor_registry_identity is None):
            raise ValueError("factor registry and fixed identity require paired configuration")
        if factor_registry is not None and not isinstance(
            factor_registry, FactorDefinitionRegistry
        ):
            raise TypeError("factor registry must be FactorDefinitionRegistry")
        if factor_registry_identity is not None and not isinstance(
            factor_registry_identity, FactorRegistryIdentity
        ):
            raise TypeError("factor registry identity must be FactorRegistryIdentity")
        self.factor_registry = factor_registry
        self.factor_registry_identity = factor_registry_identity
        if factor_tracking_identity is not None and factor_registry_identity is None:
            raise ValueError("tracking projection requires its fixed factor registry")
        self.factor_tracking_identity = factor_tracking_identity

    def _factor_tracking_projections(
        self, observed: datetime
    ) -> tuple[ServingProjectionPayload, ...]:
        from rquant.factor.tracking_serving import (
            project_factor_tracking_projections,
            project_factor_tracking_snapshot,
        )

        if self.factor_tracking_identity is None:
            return ()
        return project_factor_tracking_projections(
            project_factor_tracking_snapshot(
                self.factor_tracking_identity,
                registry_identity=self.factor_registry_identity,
                available_at=observed,
            )
        )

    def _backfill_plan_projections(
        self, observed_at: datetime
    ) -> tuple[ServingProjectionPayload, ...]:
        """Seal every discoverable plan in this source generation or refuse it."""
        directory = self.backfill_plan_directory
        if directory is None:
            return ()
        observed = normalize_aware_utc(observed_at)
        try:
            job_snapshot = read_backfill_plan_job_snapshot(
                self.backfill_plan_job_state_path, observed_at=observed
            )
        except (OSError, sqlite3.Error, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                f"backfill plan job state invalid: {exc}"
            ) from exc
        try:
            binding = _bind_readonly_directory(directory, label="backfill plan directory")
        except FileNotFoundError:
            try:
                os.stat(directory, follow_symlinks=False)
            except FileNotFoundError:
                try:
                    return project_backfill_plans(
                        (),
                        available_at=max(_EMPTY_PROJECTION_AVAILABLE_AT, job_snapshot.available_at),
                        job_snapshot=job_snapshot,
                    )
                except ValueError as exc:
                    raise PageProjectionSourceIntegrityError(
                        f"backfill plan invalid: {exc}"
                    ) from exc
            raise PageProjectionSourceIntegrityError(
                "backfill plan directory appeared while read"
            ) from None
        try:
            entries = _list_bound_backfill_plan_entries(binding)
            if sum(entry.identity[2] for entry in entries) > MAX_BACKFILL_CATALOG_READ_BYTES:
                raise ValueError("backfill plan complete catalogue exceeds read bound")
            directory_stat = os.fstat(binding.descriptor)
            if os.name != "posix" or directory_stat.st_ctime_ns <= 0:
                raise ValueError("backfill plan directory publication time is unavailable")
            available_ns = max(
                (directory_stat.st_ctime_ns, *(entry.published_ns for entry in entries))
            )
            if available_ns > int(observed.timestamp() * 1_000_000_000):
                raise ValueError("backfill plan is not yet available")
            indexed = tuple(
                (
                    _read_bound_backfill_plan(binding, entry),
                    datetime.fromtimestamp(entry.published_ns / 1_000_000_000, tz=UTC),
                )
                for entry in entries
            )
            _verify_bound_backfill_catalogue(binding, entries)
            return project_backfill_plans(
                indexed,
                available_at=max(
                    datetime.fromtimestamp(available_ns / 1_000_000_000, tz=UTC),
                    job_snapshot.available_at,
                ),
                job_snapshot=job_snapshot,
            )
        except (OSError, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(f"backfill plan invalid: {exc}") from exc
        finally:
            binding.close()

    def _report_projections(
        self, observed: datetime, *, success: DataAuditReportSuccessfulTask | None = None
    ) -> tuple[ServingProjectionPayload, ...]:
        path = self.audit_report_path
        if self.audit_report_job_state_path is not None:
            if success is None:
                return ()
            assert self.audit_report_job_directory is not None
            path = self.audit_report_job_directory / (
                f"data-audit-v1-{success.receipt.report_hash}.json"
            )
        if path is None:
            return ()
        try:
            binding = _bind_readonly_directory(path.parent, label="audit report directory")
        except FileNotFoundError:
            if success is not None:
                raise PageProjectionSourceIntegrityError(
                    "audit report directory is missing"
                ) from None
            return ()
        try:
            found = _read_bound_optional_file(
                binding,
                path.name,
                max_bytes=MAX_REPORT_BYTES,
                label="audit report",
            )
            if found is None:
                binding.verify()
                try:
                    os.stat(path.name, dir_fd=binding.descriptor, follow_symlinks=False)
                except FileNotFoundError:
                    if success is not None:
                        raise PageProjectionSourceIntegrityError(
                            "successful audit report is missing"
                        ) from None
                    return ()
                raise PageProjectionSourceIntegrityError("audit report appeared while read")
            raw, identity = found
            report = parse_data_audit_report_bytes(raw, filename=path.name)
            if (
                report.source.mode != "production_unverified"
                or report.source.namespace != "production"
            ):
                raise ValueError("synthetic test audit report cannot enter production Serving")
            if success is not None and (
                report.content_hash != success.receipt.report_hash
                or report.source.snapshot_label != f"sha256:{success.replica_sha256}"
                or report.audit_start != success.request.audit_start
                or report.observed_through != success.request.observed_through
                or report.null_fields
                != tuple(sorted(success.request.null_fields, key=lambda field: field.field_name))
                or report.collection_status != "collection_unconfirmed"
            ):
                raise ValueError("audit report differs from successful task")
            # mtime is caller-settable. Inode ctime and the containing directory's
            # ctime bound when this content/name could first have been published;
            # they do not establish collection completion or replica identity.
            directory_stat = os.fstat(binding.descriptor)
            if os.name != "posix" or any(
                not isinstance(value, int) or value <= 0
                for value in (
                    identity.st_mtime_ns,
                    identity.st_ctime_ns,
                    directory_stat.st_ctime_ns,
                )
            ):
                raise ValueError("audit report publication time cannot be established")
            available_ns = max(
                identity.st_mtime_ns,
                identity.st_ctime_ns,
                directory_stat.st_ctime_ns,
            )
            report_available = datetime.fromtimestamp(available_ns / 1_000_000_000, tz=UTC)
            if report_available > observed:
                raise ValueError("audit report is not yet available")
            projections = project_data_audit_report(report, available_at=report_available)
            binding.verify()
            named = os.stat(path.name, dir_fd=binding.descriptor, follow_symlinks=False)
            if _copy_identity(named) != _copy_identity(identity):
                raise PageProjectionSourceIntegrityError("audit report rotated while read")
            return projections
        except (OSError, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(f"audit report invalid: {exc}") from exc
        finally:
            binding.close()

    def _audit_report_bundle(
        self, observed: datetime
    ) -> tuple[tuple[ServingProjectionPayload, ...], tuple[ServingProjectionPayload, ...]]:
        if self.audit_report_job_state_path is None:
            return self._report_projections(observed), ()
        try:
            job_snapshot = read_data_audit_report_job_snapshot(
                self.audit_report_job_state_path, observed_at=observed
            )
            report = self._report_projections(observed, success=job_snapshot.successful)
            available = max((job_snapshot.available_at, *(item.available_at for item in report)))
            report = tuple(
                ServingProjectionPayload(
                    table_name=item.table_name, available_at=available, rows=item.rows
                )
                for item in report
            )
            job = project_data_audit_report_job(job_snapshot, available_at=available)
            return report, job
        except (OSError, sqlite3.Error, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                f"audit report job state invalid: {exc}"
            ) from exc

    def _formula_market_projections(
        self, observed: datetime
    ) -> tuple[ServingProjectionPayload, ...]:
        if self.formula_market_job_state_path is None:
            return ()
        try:
            snapshot = read_formula_market_job_snapshot(
                self.formula_market_job_state_path,
                self.formula_market_job_directory,
                observed_at=observed,
            )
            return project_formula_market_job(snapshot)
        except (OSError, sqlite3.Error, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                f"formula market job source invalid: {exc}"
            ) from exc

    def _factor_definition_projections(
        self, observed: datetime
    ) -> tuple[ServingProjectionPayload, ...]:
        if self.factor_registry is None or self.factor_registry_identity is None:
            return ()
        try:
            snapshot = project_factor_definition_serving_snapshot(
                self.factor_registry,
                expected_identity=self.factor_registry_identity,
                available_at=observed,
            )
            return project_factor_definition_projections(snapshot)
        except (FactorRegistryError, OSError, sqlite3.Error, ValueError) as exc:
            raise PageProjectionSourceIntegrityError(
                f"factor definition source invalid: {exc}"
            ) from exc

    def __call__(self, observed_at: datetime, /) -> LabPageProjectionSnapshot:
        observed = normalize_aware_utc(observed_at)
        stable = _StableReadonlyDuckDB(self.database_path, control_root=self.control_root)
        with stable as connection:
            self._require_tables(connection)
            audit_status, audit_issues = self._audit_results(connection, observed=observed)
            candidates = connection.execute(
                """
                SELECT snapshot.snapshot_id, snapshot.strategy_name, snapshot.code_commit,
                       TRY_CAST(json_extract_string(
                           table_watermarks, '$.manifest_start_date') AS DATE),
                       TRY_CAST(json_extract_string(
                           table_watermarks, '$.manifest_end_date') AS DATE),
                       snapshot.as_of_time, snapshot.completed_at, binding.binding_hash,
                       binding.completed_at
                FROM dataset_snapshot AS snapshot
                JOIN dataset_snapshot_binding AS binding USING (snapshot_id)
                WHERE snapshot.status = 'ready' AND binding.status = 'ready'
                  AND snapshot.as_of_time <= ?
                  AND snapshot.completed_at <= ?
                  AND binding.completed_at <= ?
                ORDER BY snapshot.completed_at DESC, snapshot_id
                LIMIT ?
                """,
                (observed, observed, observed, _MAX_RESEARCH_GATES + 1),
            ).fetchall()
            if len(candidates) > _MAX_RESEARCH_GATES:
                raise PageProjectionSourceIntegrityError(
                    "research gates exceed the bounded projection limit"
                )
            rows: list[ResearchGateProjectionRow] = []
            with DuckDBStore(stable.generation_path, read_only=True) as store:
                for (
                    snapshot_id,
                    strategy_name,
                    code_commit,
                    range_start,
                    range_end,
                    as_of_time,
                    snapshot_completed_at,
                    binding_hash,
                    binding_completed_at,
                ) in candidates:
                    if range_start is None or range_end is None:
                        raise PageProjectionSourceIntegrityError(
                            "ready research snapshot lacks its manifest range"
                        )
                    audit_row = connection.execute(
                        """
                        SELECT audit_run_id, completed_at
                        FROM data_audit_run
                        WHERE status = 'completed' AND completed_at <= ?
                          AND as_of_date >= ? AND range_start <= ? AND range_end >= ?
                        ORDER BY as_of_date DESC, completed_at DESC
                        LIMIT 1
                        """,
                        (observed, range_end, range_start, range_end),
                    ).fetchone()
                    audit_run_id = None if audit_row is None else str(audit_row[0])
                    decision = evaluate_store_research_gate(
                        store,
                        ResearchGateRequest(
                            mode="formal",
                            strategy_name=str(strategy_name),
                            start_date=range_start,
                            end_date=range_end,
                            audit_run_id=audit_run_id,
                            dataset_snapshot_id=str(snapshot_id),
                            dataset_binding_hash=str(binding_hash),
                            code_commit=str(code_commit),
                        ),
                        binding_verified=False,
                    )
                    completion_candidates = [
                        _database_timestamp(snapshot_completed_at),
                        _database_timestamp(binding_completed_at),
                    ]
                    if audit_row is not None:
                        completion_candidates.append(_database_timestamp(audit_row[1]))
                    rows.append(
                        ResearchGateProjectionRow(
                            strategy_name=str(strategy_name),
                            range_start=range_start,
                            range_end=range_end,
                            as_of_time=_database_timestamp(as_of_time),
                            completed_at=max(completion_candidates),
                            code_commit=str(code_commit),
                            audit_run_id=decision.audit_run_id,
                            dataset_snapshot_id=decision.dataset_snapshot_id,
                            dataset_binding_hash=decision.dataset_binding_hash,
                            coverage_ratios=decision.coverage_ratios,
                            coverage_counts=decision.coverage_counts,
                            failures=decision.failures,
                            metadata_ready=research_gate_metadata_ready(decision),
                        )
                    )
        available_at = max(
            (
                _EMPTY_PROJECTION_AVAILABLE_AT,
                *(row.completed_at for row in rows),
                *(
                    time
                    for time in (
                        audit_status.latest_observed_at,
                        audit_status.latest_completed_at,
                        audit_status.successful_completed_at,
                    )
                    if time is not None
                ),
            )
        )
        audit_report, audit_job = self._audit_report_bundle(observed)
        return LabPageProjectionSnapshot.create(
            available_at=available_at,
            rows=tuple(rows),
            audit_status=audit_status,
            audit_issues=audit_issues,
            audit_report_projections=audit_report,
            audit_job_projections=audit_job,
            backfill_plan_projections=self._backfill_plan_projections(observed),
            formula_market_projections=self._formula_market_projections(observed),
            factor_definition_projections=self._factor_definition_projections(observed),
            factor_tracking_projections=self._factor_tracking_projections(observed),
        )

    @staticmethod
    def _audit_results(
        connection: duckdb.DuckDBPyConnection, *, observed: datetime
    ) -> tuple[DataAuditStatusProjectionRow, tuple[DataAuditIssueProjectionRow, ...]]:
        latest = connection.execute(
            """
            SELECT audit_run_id, status, observed_at, completed_at
            FROM data_audit_run
            WHERE observed_at <= ? AND (status = 'running' OR completed_at <= ?)
            ORDER BY observed_at DESC, audit_run_id DESC LIMIT 1
            """,
            (observed, observed),
        ).fetchone()
        successful = connection.execute(
            """
            SELECT audit_run_id, as_of_date, range_start, range_end,
                   observed_at, completed_at, p0_count,
                   json_array_length(finding_issue_ids),
                   json_type(finding_issue_ids),
                   octet_length(encode(CAST(finding_issue_ids AS VARCHAR)))
            FROM data_audit_run
            WHERE status = 'completed' AND observed_at <= ? AND completed_at <= ?
            ORDER BY observed_at DESC, audit_run_id DESC LIMIT 1
            """,
            (observed, observed),
        ).fetchone()
        issues: tuple[DataAuditIssueProjectionRow, ...] = ()
        if successful is not None:
            if successful[8] != "ARRAY" or int(successful[9]) > _MAX_AUDIT_FINDING_LIST_BYTES:
                raise PageProjectionSourceIntegrityError("audit finding list is malformed or large")
            expected = int(successful[7])
            if expected > _MAX_AUDIT_ISSUES:
                raise PageProjectionSourceIntegrityError("audit issue limit exceeded")
            rows = connection.execute(
                """
                SELECT json_extract_string(finding.value, '$') AS finding_id,
                       issue.issue_id, issue.dataset_id, issue.rule_id,
                       issue.severity, issue.status
                FROM data_audit_run AS audit,
                     json_each(audit.finding_issue_ids) AS finding
                LEFT JOIN data_quality_issue AS issue
                  ON issue.issue_id = json_extract_string(finding.value, '$')
                WHERE audit.audit_run_id = ?
                ORDER BY finding_id LIMIT ?
                """,
                (str(successful[0]), _MAX_AUDIT_ISSUES + 1),
            ).fetchall()
            if len(rows) != expected or any(row[1] is None for row in rows):
                raise PageProjectionSourceIntegrityError("audit issue is missing")
            if len({str(row[0]) for row in rows}) != expected:
                raise PageProjectionSourceIntegrityError("audit issue ids are duplicated")
            issues = tuple(
                DataAuditIssueProjectionRow(
                    audit_run_id=str(successful[0]),
                    issue_id=str(issue_id),
                    dataset_id=str(dataset_id),
                    rule_id=str(rule_id),
                    severity=str(severity),
                    status=str(status),
                )
                for _finding_id, issue_id, dataset_id, rule_id, severity, status in rows
            )
            # Issue severity can change after this completed run; p0_count is its
            # historical result, while issue rows show the current classification.
        status = DataAuditStatusProjectionRow(
            latest_status="never_run" if latest is None else str(latest[1]),
            latest_observed_at=None if latest is None else _database_timestamp(latest[2]),
            latest_completed_at=(
                None if latest is None or latest[3] is None else _database_timestamp(latest[3])
            ),
            successful_audit_id=None if successful is None else str(successful[0]),
            successful_as_of_date=None if successful is None else successful[1],
            successful_range_start=None if successful is None else successful[2],
            successful_range_end=None if successful is None else successful[3],
            successful_completed_at=(
                None if successful is None else _database_timestamp(successful[5])
            ),
            finding_count=0 if successful is None else int(successful[7]),
            p0_count=0 if successful is None else int(successful[6]),
        )
        return status, issues

    @staticmethod
    def _require_tables(connection: duckdb.DuckDBPyConnection) -> None:
        required = {
            "data_audit_run",
            "data_quality_issue",
            "dataset_coverage",
            "dataset_snapshot",
            "dataset_snapshot_binding",
        }
        rows = connection.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ANY(?)
            """,
            (list(sorted(required)),),
        ).fetchall()
        if {str(row[0]) for row in rows} != required:
            raise PageProjectionSourceIntegrityError(
                "research metadata database is missing gate authority tables"
            )


class SignalPageProjectionProducer:
    """Publish the signal-owned page source into notification authority state."""

    def __init__(
        self,
        *,
        source: DuckDBSignalPageProjectionSource,
        store: NotificationStateStore,
        companion_projections: tuple[ServingProjectionPayload, ...] | None = None,
    ) -> None:
        self.source = source
        self.store = store
        if (
            companion_projections is not None
            and {item.table_name for item in companion_projections} != _COMPANION_SIGNAL_TABLES
        ):
            raise ValueError("signal companion projections are incomplete")
        self.companion_projections = companion_projections

    def publish(self, observed_at: datetime) -> NotificationProjectionPublication:
        """Publish this iteration's page projection, and say whether it wrote anything.

        The snapshot it assembles is a function of the replica generation, the canvas
        catalog and the `surge_live` files -- none of which move on this role's two-second
        interval -- so the ordinary answer is the content that is already published, and
        `written` is False (#271).
        """

        observed = normalize_aware_utc(observed_at)
        try:
            snapshot = self.source(observed)
        except (PageProjectionSourceIntegrityError, OSError, duckdb.Error, ValueError) as error:
            if self.source.formula_pool_config is not None:
                raise
            previous = self.store.serving_snapshot(observed_at=observed, history_limit=1)
            if previous.projection_generation_id is None:
                raise
            logger.warning("盯盘事件来源暂不可用：{}", error)
            page_projections = tuple(
                item
                for item in previous.payload.projections
                if item.table_name not in _COMPANION_SIGNAL_TABLES
                and item.table_name
                not in {
                    "surge_event",
                    "legacy_notification",
                    "legacy_notification_status",
                    "pool_definition",
                    "screen_run_receipt",
                    "pool_membership",
                    "pool_member_return",
                    "formula_pool_state",
                    "formula_pool_definition",
                    "formula_pool_latest_result",
                    "alert_ack_state",
                    "alert_ack",
                    "manual_watchlist_state",
                    "manual_watchlist",
                    "price_alert_rule_state",
                    "price_alert_rule",
                }
            )
            try:
                surge = _read_surge_event_projection(self.source.surge_live_root, observed=observed)
            except (PageProjectionSourceIntegrityError, OSError, ValueError) as surge_error:
                logger.warning("爆量事件来源暂不可用：{}", surge_error)
                surge = None
            if surge is not None:
                page_projections += (surge,)
            legacy_notification, legacy_status = self.source.legacy_notification_projections(
                observed
            )
            page_projections += tuple(
                item for item in (legacy_notification, legacy_status) if item is not None
            )
            page_projections += build_ack_source_projections(None, observed_at=observed)
            page_projections += build_manual_watchlist_projections(None, observed_at=observed)
            page_projections += build_price_alert_rule_projections(
                None, observed_at=observed, unavailable=True
            )
            page_available_at = max(item.available_at for item in page_projections)
            page_generation_id = canonical_sha256(
                {"source": "signal-page-projections-partial", "projections": page_projections}
            )
        else:
            page_projections = snapshot.projections
            page_available_at = snapshot.available_at
            page_generation_id = snapshot.content_sha256
        if self.companion_projections is not None:
            injected_names = {item.table_name for item in self.companion_projections}
            page_projections = tuple(
                item for item in page_projections if item.table_name not in injected_names
            )
        page_source = NotificationProjectionSourceReceipt.create(
            dataset_id="signal-page-projections",
            generation_id=page_generation_id,
            sequence=int(page_available_at.timestamp() * 1_000_000),
            event_time=page_available_at,
            published_at=observed,
            projections=page_projections,
        )
        if self.companion_projections is None:
            previous = self.store.serving_snapshot(observed_at=observed, history_limit=1)
            previous_by_name = {
                projection.table_name: projection for projection in previous.payload.projections
            }
            companion_projections = tuple(
                previous_by_name.get(table_name)
                or ServingProjectionPayload(
                    table_name=table_name,
                    available_at=_EMPTY_PROJECTION_AVAILABLE_AT,
                    rows=(),
                )
                for table_name in sorted(
                    _COMPANION_SIGNAL_TABLES - {"monitor_event", "surge_event"}
                )
            )
        else:
            companion_projections = self.companion_projections
        companion_identity = {
            "dataset_id": "signal-companion-projections",
            "projections": companion_projections,
        }
        companion_source = NotificationProjectionSourceReceipt.create(
            dataset_id="signal-companion-projections",
            generation_id=canonical_sha256(companion_identity),
            sequence=int(
                max(item.available_at for item in companion_projections).timestamp() * 1_000_000
            ),
            event_time=max(item.available_at for item in companion_projections),
            published_at=observed,
            projections=companion_projections,
        )
        authority = NotificationProjectionAuthoritySnapshot.create_from_sources(
            observed_at=observed,
            sources=(page_source, companion_source),
        )
        return self.store.publish_projection_authority(authority)


class ScreenBoundsProjectionRow(RuntimeContractModel):
    preset_name: str = Field(min_length=1)
    min_date: date
    max_date: date
    candidate_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.min_date > self.max_date:
            raise ValueError("screen bounds min_date exceeds max_date")
        return self


class MinuteCoverageProjectionRow(RuntimeContractModel):
    is_total: bool
    source: str = Field(min_length=1)
    rows_count: int = Field(ge=0)
    codes_count: int = Field(ge=0)
    trade_dates: int = Field(ge=0)
    min_time: AwareUtcDatetime | None = None
    max_time: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if (self.min_time is None) != (self.max_time is None):
            raise ValueError("minute coverage timestamps must be bound together")
        if (
            self.min_time is not None
            and self.max_time is not None
            and self.min_time > self.max_time
        ):
            raise ValueError("minute coverage min_time exceeds max_time")
        return self


class CanvasDiagnosticProjectionRow(RuntimeContractModel):
    trade_date: date
    preset_name: str = Field(min_length=1)
    step_index: int = Field(ge=0)
    rule_label: str = Field(min_length=1)
    remaining_count: int = Field(ge=0)


class CanvasLatestTradeDateProjectionRow(RuntimeContractModel):
    trade_date: date


class CanvasHitProjectionRow(RuntimeContractModel):
    trade_date: date
    preset_name: str = Field(min_length=1)
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    row_json: str = Field(min_length=2)

    @field_validator("row_json")
    @classmethod
    def validate_row_json(cls, value: str) -> str:
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("canvas hit row_json must contain a JSON object")
        return json.dumps(parsed, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


class CanvasDefinitionProjectionRow(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=128, pattern=r"^[\w\u4e00-\u9fff-]+$")
    description: str = Field(default="", max_length=8_192)
    pool_refs: tuple[str, ...] = Field(default_factory=tuple, max_length=256)
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    source: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=128)
    command_hash: Sha256
    source_identity_hash: Sha256
    publication_generation_id: Sha256
    publication_receipt_id: Sha256
    record_hash: Sha256
    version_hash: Sha256

    @model_validator(mode="after")
    def validate_definition(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("canvas definition updated_at precedes created_at")
        expected_source_identity_hash = canvas_source_identity_hash(
            command_id=self.command_id,
            command_hash=self.command_hash,
            source=self.source,
        )
        if self.source_identity_hash != expected_source_identity_hash:
            raise ValueError("canvas definition source identity hash mismatch")
        expected = canonical_sha256(
            {
                "schema_version": self.schema_version,
                "name": self.name,
                "description": self.description,
                "pool_refs": self.pool_refs,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "source": self.source,
                "command_id": self.command_id,
                "command_hash": self.command_hash,
                "source_identity_hash": self.source_identity_hash,
                "publication_generation_id": self.publication_generation_id,
                "publication_receipt_id": self.publication_receipt_id,
                "record_hash": self.record_hash,
            }
        )
        if self.version_hash != expected:
            raise ValueError("canvas definition version hash mismatch")
        return self

    @classmethod
    def from_catalog_record(
        cls,
        *,
        file_name: str,
        raw: object,
        observed: datetime,
    ) -> Self:
        if not isinstance(raw, dict):
            raise PageProjectionSourceIntegrityError("canvas catalog record must be a JSON object")
        allowed = {
            "schema_version",
            "name",
            "description",
            "pool_refs",
            "created_at",
            "updated_at",
            "source",
            "command_id",
            "command_hash",
            "source_identity_hash",
            "publication_generation_id",
            "publication_receipt_id",
            "record_hash",
        }
        if set(raw) != allowed:
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record must bind command identity and record hash"
            )
        if raw.get("schema_version") != _CANVAS_CATALOG_SCHEMA_VERSION:
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record schema version is unsupported"
            )
        record_hash = raw.get("record_hash")
        if not isinstance(record_hash, str):
            raise PageProjectionSourceIntegrityError("canvas catalog record hash is invalid")
        name = raw.get("name")
        if not isinstance(name, str) or file_name != f"{name}.json":
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record name does not match its path"
            )
        pool_refs = raw.get("pool_refs", [])
        if not isinstance(pool_refs, list) or not all(isinstance(item, str) for item in pool_refs):
            raise PageProjectionSourceIntegrityError("canvas catalog record pool_refs are invalid")

        def parse_time(field: str) -> datetime:
            value = raw.get(field)
            if not isinstance(value, str):
                raise PageProjectionSourceIntegrityError(
                    f"canvas catalog record {field} is missing"
                )
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                parsed = normalize_aware_utc(parsed)
            except ValueError as exc:
                raise PageProjectionSourceIntegrityError(
                    f"canvas catalog record {field} is invalid"
                ) from exc
            if parsed > observed:
                raise PageProjectionSourceIntegrityError(
                    "canvas catalog record contains future evidence"
                )
            return parsed

        created_at = parse_time("created_at")
        updated_at = parse_time("updated_at")
        description = raw.get("description", "")
        source = raw.get("source", "page_control")
        command_id = raw.get("command_id")
        command_hash = raw.get("command_hash")
        source_identity_hash = raw.get("source_identity_hash")
        publication_generation_id = raw.get("publication_generation_id")
        publication_receipt_id = raw.get("publication_receipt_id")
        if not all(
            isinstance(value, str)
            for value in (
                description,
                source,
                command_id,
                command_hash,
                source_identity_hash,
                publication_generation_id,
                publication_receipt_id,
            )
        ):
            raise PageProjectionSourceIntegrityError(
                "canvas catalog record has invalid text or command identity fields"
            )
        catalog_record = CanvasPublicationCatalogRecord(
            schema_version=_CANVAS_CATALOG_SCHEMA_VERSION,
            name=name,
            description=description,
            pool_refs=tuple(pool_refs),
            created_at=created_at,
            updated_at=updated_at,
            source=source,
            command_id=command_id,
            command_hash=command_hash,
            source_identity_hash=source_identity_hash,
            publication_generation_id=publication_generation_id,
            publication_receipt_id=publication_receipt_id,
            record_hash=record_hash,
        )
        identity = {
            "schema_version": _CANVAS_CATALOG_SCHEMA_VERSION,
            "name": name,
            "description": description,
            "pool_refs": tuple(pool_refs),
            "created_at": created_at,
            "updated_at": updated_at,
            "source": source,
            "command_id": command_id,
            "command_hash": command_hash,
            "source_identity_hash": source_identity_hash,
            "publication_generation_id": publication_generation_id,
            "publication_receipt_id": publication_receipt_id,
            "record_hash": record_hash,
        }
        if record_hash != canvas_catalog_record_hash(catalog_record):
            raise PageProjectionSourceIntegrityError("canvas catalog record hash mismatch")
        try:
            return cls(**identity, version_hash=canonical_sha256(identity))
        except ValueError as exc:
            raise PageProjectionSourceIntegrityError("canvas catalog record is invalid") from exc


class PulseHistoryProjectionRow(RuntimeContractModel):
    model_config = ConfigDict(strict=True)

    trade_date: date
    as_of: AwareUtcDatetime
    t: str = Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
    limit_up: int = Field(ge=0)
    limit_down: int = Field(ge=0)
    broken: int = Field(ge=0)
    up: int = Field(ge=0)
    down: int = Field(ge=0)
    up_ratio_pct: float | None = None
    total: int = Field(ge=0)


class PulseAlertProjectionRow(RuntimeContractModel):
    model_config = ConfigDict(strict=True)

    trade_date: date
    as_of: AwareUtcDatetime
    t: str = Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
    kind: str = Field(min_length=1, max_length=64)
    kind_label: str = Field(min_length=1, max_length=64)
    before: float
    after: float
    window_minutes: int = Field(ge=1, le=241)
    message: str = Field(min_length=1, max_length=2_048)


class SurgeRuntimeConfigProjectionRow(RuntimeContractModel):
    model_config = ConfigDict(strict=True)

    trade_date: date
    as_of: AwareUtcDatetime
    boards: tuple[str, ...] = Field(min_length=1, max_length=4)
    k_rough: float
    k_cum: float
    ratio_cap: float
    skip_first_minutes: int = Field(ge=0, le=240)
    tushare_rate_per_min: int = Field(ge=1, le=1_000)
    require_price_strength: bool
    max_room_to_limit_pct: float

    @field_validator("boards")
    @classmethod
    def validate_boards(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        allowed = {"main", "gem", "star", "bj"}
        if len(value) != len(set(value)) or not set(value).issubset(allowed):
            raise ValueError("surge runtime config boards are invalid")
        return value


class PulseHistoryProjectionSource(RuntimeContractModel):
    available_at: AwareUtcDatetime
    rows: tuple[PulseHistoryProjectionRow, ...] = Field(max_length=_MAX_PULSE_ROWS)


class PulseAlertProjectionSource(RuntimeContractModel):
    available_at: AwareUtcDatetime
    rows: tuple[PulseAlertProjectionRow, ...] = Field(max_length=_MAX_PULSE_ROWS)


class SurgeRuntimeConfigProjectionSource(RuntimeContractModel):
    available_at: AwareUtcDatetime
    row: SurgeRuntimeConfigProjectionRow


def _source_file_time(
    item: os.stat_result,
    *,
    observed: datetime,
    name: str,
) -> datetime:
    value = datetime.fromtimestamp(item.st_mtime_ns / 1_000_000_000, tz=UTC)
    if value > observed:
        raise PageProjectionSourceIntegrityError(
            f"surge live source {name} contains future file evidence"
        )
    return value


def _parse_jsonl_objects(raw: bytes, *, name: str) -> tuple[dict[str, object], ...]:
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PageProjectionSourceIntegrityError(
            f"surge live source {name} is not valid UTF-8"
        ) from exc
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PageProjectionSourceIntegrityError(
                f"surge live source {name} has invalid JSON at line {line_number}"
            ) from exc
        if not isinstance(value, dict):
            raise PageProjectionSourceIntegrityError(
                f"surge live source {name} line {line_number} must be an object"
            )
        rows.append(value)
        if len(rows) > _MAX_PULSE_ROWS:
            raise PageProjectionSourceIntegrityError(f"surge live source {name} exceeds row bound")
    return tuple(rows)


def _read_surge_event_projection(
    root: Path | None, *, observed: datetime
) -> ServingProjectionPayload | None:
    """Read the writer's complete daily JSONL files for a bounded 30-day window."""

    if root is None:
        return None
    try:
        binding = _bind_readonly_directory(root, label="surge event source")
    except FileNotFoundError:
        return None
    from rquant.runtime_shadow_sources import LegacySurgeEvent

    local_day = observed.astimezone(_SHANGHAI).date()
    first_day = local_day - timedelta(days=_EVENT_WINDOW_DAYS - 1)
    remaining = _MAX_SURGE_EVENT_BYTES
    seen = 0
    duplicates = 0
    selected: dict[tuple[str, str, str], dict[str, object]] = {}
    available = _EMPTY_PROJECTION_AVAILABLE_AT
    try:
        for offset in range(_EVENT_WINDOW_DAYS):
            day = first_day + timedelta(days=offset)
            name = f"events-{day.isoformat()}.jsonl"
            file = _read_bound_optional_file(binding, name, max_bytes=remaining)
            if file is None:
                continue
            raw, item = file
            try:
                current = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
            except FileNotFoundError as error:
                raise PageProjectionSourceIntegrityError(
                    f"surge event source {name} rotated while read"
                ) from error
            if _copy_identity(current) != _copy_identity(item):
                raise PageProjectionSourceIntegrityError(
                    f"surge event source {name} changed while read"
                )
            binding.verify()
            remaining -= len(raw)
            available = max(available, _source_file_time(item, observed=observed, name=name))
            if raw and not raw.endswith(b"\n"):
                raise PageProjectionSourceIntegrityError(
                    f"surge event source {name} has an incomplete line"
                )
            try:
                lines = raw.decode("utf-8").splitlines()
            except UnicodeDecodeError as error:
                raise PageProjectionSourceIntegrityError(
                    f"surge event source {name} is not UTF-8"
                ) from error
            for line_number, line in enumerate(lines, start=1):
                seen += 1
                if seen > _MAX_EVENT_ROWS:
                    raise PageProjectionSourceIntegrityError("surge events exceed the row bound")
                if not line:
                    raise PageProjectionSourceIntegrityError(
                        f"surge event source {name} has an empty line"
                    )
                try:
                    value = strict_json_loads(line)
                    if not isinstance(value, dict):
                        raise ValueError("event record must be an object")
                    required = {
                        "ts_code",
                        "name",
                        "theme",
                        "confirmed_at",
                        "price",
                        "pct_chg",
                        "cum_amount",
                        "rel_cum",
                        "room_to_limit_pct",
                        "status",
                    }
                    if not required.issubset(value):
                        raise ValueError("event fields are incomplete")
                    if set(value) - (set(LegacySurgeEvent.model_fields) | {"push_count_5d"}):
                        raise ValueError("event fields are unknown")
                    event = LegacySurgeEvent.model_validate(
                        {
                            field: value[field]
                            for field in LegacySurgeEvent.model_fields
                            if field in value
                        }
                    )
                    if (
                        _STOCK_CODE.fullmatch(event.ts_code) is None
                        or _SURGE_EVENT_TIME.fullmatch(event.confirmed_at) is None
                    ):
                        raise ValueError("event code or time is invalid")
                    event_at = datetime.combine(
                        day, time.fromisoformat(event.confirmed_at), tzinfo=_SHANGHAI
                    ).astimezone(UTC)
                    if event_at > observed:
                        raise ValueError("event is from the future")
                    row: dict[str, object] = {
                        "trade_date": day.isoformat(),
                        "confirmed_at": event.confirmed_at,
                        "ts_code": event.ts_code,
                        "name": event.name,
                        "theme": event.theme,
                        "price": event.price,
                        "pct_chg": event.pct_chg,
                        "cum_amount": event.cum_amount,
                        "rel_cum": event.rel_cum,
                        "room_to_limit_pct": event.room_to_limit_pct,
                        "status": event.status,
                    }
                except (StrictJsonError, ValueError) as error:
                    raise PageProjectionSourceIntegrityError(
                        f"surge event source {name} line {line_number} is invalid"
                    ) from error
                key = (day.isoformat(), event.confirmed_at, event.ts_code)
                old = selected.get(key)
                if old is not None:
                    duplicates += 1
                    if canonical_sha256(row) <= canonical_sha256(old):
                        continue
                selected[key] = row
        binding.verify()
    finally:
        binding.close()
    if duplicates:
        logger.warning("爆量事件有 {} 条重复记录，已按内容稳定去重", duplicates)
    return ServingProjectionPayload(
        table_name="surge_event",
        available_at=available,
        rows=tuple(selected[key] for key in sorted(selected)),
    )


def _legacy_notification_status(
    *, state: str, skipped: int, available_at: datetime
) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name="legacy_notification_status",
        available_at=available_at,
        rows=({"snapshot_key": "current", "state": state, "skipped": skipped},),
    )


def _read_legacy_notification_projections(
    path: Path | None, *, observed: datetime
) -> tuple[ServingProjectionPayload, ServingProjectionPayload] | None:
    """Extract only safe submission facts from one bounded, stable legacy JSONL file."""

    if path is None:
        return None
    observed = normalize_aware_utc(observed)
    try:
        binding = _bind_readonly_directory(path.parent, label="legacy notification source")
    except FileNotFoundError:
        return None
    try:
        file = _read_bound_optional_file(
            binding,
            path.name,
            max_bytes=_MAX_LEGACY_NOTIFICATION_BYTES,
            label="legacy notification source",
        )
        if file is None:
            return None
        raw, item = file
        if raw and not raw.endswith(b"\n"):
            raise PageProjectionSourceIntegrityError(
                "legacy notification source has an incomplete line"
            )
        try:
            lines = raw.decode("utf-8").splitlines()
        except UnicodeDecodeError:
            raise PageProjectionSourceIntegrityError(
                "legacy notification source is not UTF-8"
            ) from None
        if len(lines) > _MAX_EVENT_ROWS:
            raise PageProjectionSourceIntegrityError("legacy notification source exceeds row bound")
        file_time = datetime.fromtimestamp(item.st_mtime_ns / 1_000_000_000, tz=UTC)
        if file_time > observed:
            raise PageProjectionSourceIntegrityError(
                "legacy notification source has future file time"
            )
        local_start = observed.astimezone(_SHANGHAI).date() - timedelta(days=_EVENT_WINDOW_DAYS - 1)
        window_start = datetime.combine(local_start, time.min, tzinfo=_SHANGHAI).astimezone(UTC)
        rows: list[dict[str, ProjectionScalar]] = []
        skipped = 0
        for line_number, line in enumerate(lines, start=1):
            try:
                value = strict_json_loads(line)
            except (StrictJsonError, ValueError):
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source has invalid JSON"
                ) from None
            if not isinstance(value, dict):
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source row is invalid"
                )
            sent_at_raw = value.get("sent_at")
            if (
                not isinstance(sent_at_raw, str)
                or "T" not in sent_at_raw
                or type(value.get("success")) is not bool
            ):
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source row is invalid"
                )
            try:
                local_time = datetime.fromisoformat(sent_at_raw)
            except ValueError:
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source time is invalid"
                ) from None
            if local_time.tzinfo is not None or local_time.utcoffset() is not None:
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source time is invalid"
                )
            try:
                event_at = local_time.replace(tzinfo=_SHANGHAI).astimezone(UTC)
            except (OverflowError, ValueError):
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source time is invalid"
                ) from None
            if event_at > observed:
                raise PageProjectionSourceIntegrityError(
                    "legacy notification source has future event"
                )
            scene = value.get("scene")
            channel = value.get("channel")
            scene_label = _LEGACY_SCENE_LABELS.get(scene) if isinstance(scene, str) else None
            channel_label = (
                _LEGACY_CHANNEL_LABELS.get(channel) if isinstance(channel, str) else None
            )
            if scene_label is None or channel_label is None:
                skipped += 1
                continue
            if event_at < window_start:
                continue
            sent_at = event_at.isoformat()
            identity = json.dumps(
                (line_number, sent_at, scene, channel, value["success"]),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            rows.append(
                {
                    "record_key": hashlib.sha256(identity.encode("utf-8")).hexdigest(),
                    "sent_at": sent_at,
                    "scene_label": scene_label,
                    "channel_label": channel_label,
                    "submitted": value["success"],
                }
            )
        try:
            named = os.stat(path.name, dir_fd=binding.descriptor, follow_symlinks=False)
        except FileNotFoundError:
            raise PageProjectionSourceIntegrityError(
                "legacy notification source rotated while read"
            ) from None
        if _copy_identity(named) != _copy_identity(item):
            raise PageProjectionSourceIntegrityError(
                "legacy notification source changed while read"
            )
        binding.verify()
    finally:
        binding.close()
    available_at = max((file_time, *(datetime.fromisoformat(str(row["sent_at"])) for row in rows)))
    try:
        records = ServingProjectionPayload(
            table_name="legacy_notification", available_at=available_at, rows=tuple(rows)
        )
        status = _legacy_notification_status(
            state="partial" if skipped else "complete", skipped=skipped, available_at=available_at
        )
    except ValueError:
        raise PageProjectionSourceIntegrityError(
            "legacy notification source exceeds projection bound"
        ) from None
    return records, status


def _pulse_as_of(trade_date: date, minute: str) -> datetime:
    try:
        parsed_time = time.fromisoformat(minute)
    except ValueError as exc:
        raise PageProjectionSourceIntegrityError("pulse minute is invalid") from exc
    return datetime.combine(trade_date, parsed_time, tzinfo=_SHANGHAI).astimezone(UTC)


def _pulse_history_source_row(
    value: dict[str, object],
    *,
    trade_date: date,
) -> PulseHistoryProjectionRow:
    expected = {
        "t",
        "limit_up",
        "limit_down",
        "broken",
        "up",
        "down",
        "up_ratio_pct",
        "total",
    }
    if set(value) != expected:
        raise PageProjectionSourceIntegrityError("pulse history fields are invalid")
    try:
        return PulseHistoryProjectionRow(
            trade_date=trade_date,
            as_of=_pulse_as_of(trade_date, str(value["t"])),
            **value,
        )
    except ValueError as exc:
        raise PageProjectionSourceIntegrityError("pulse history row is invalid") from exc


def _pulse_alert_source_row(
    value: dict[str, object],
    *,
    trade_date: date,
) -> PulseAlertProjectionRow:
    expected = {
        "t",
        "kind",
        "kind_label",
        "before",
        "after",
        "window_minutes",
        "message",
    }
    if set(value) != expected:
        raise PageProjectionSourceIntegrityError("pulse alert fields are invalid")
    try:
        return PulseAlertProjectionRow(
            trade_date=trade_date,
            as_of=_pulse_as_of(trade_date, str(value["t"])),
            **value,
        )
    except ValueError as exc:
        raise PageProjectionSourceIntegrityError("pulse alert row is invalid") from exc


def _read_surge_live_projection_sources(
    root: Path | None,
    *,
    observed: datetime,
) -> tuple[
    PulseHistoryProjectionSource | None,
    PulseAlertProjectionSource | None,
    SurgeRuntimeConfigProjectionSource | None,
]:
    if root is None:
        return None, None, None
    try:
        binding = _bind_readonly_directory(root, label="surge live projection source")
    except FileNotFoundError:
        return None, None, None
    trade_date = observed.astimezone(_SHANGHAI).date()
    try:
        history_file = _read_bound_optional_file(
            binding,
            f"pulse-{trade_date.isoformat()}.jsonl",
            max_bytes=_MAX_PULSE_FILE_BYTES,
        )
        alert_file = _read_bound_optional_file(
            binding,
            f"pulse_alerts-{trade_date.isoformat()}.jsonl",
            max_bytes=_MAX_ALERT_FILE_BYTES,
        )
        config_file = _read_bound_optional_file(
            binding,
            "runtime_config.json",
            max_bytes=_MAX_RUNTIME_CONFIG_BYTES,
        )
    finally:
        binding.close()

    history_source = None
    if history_file is not None:
        raw, item = history_file
        file_available_at = _source_file_time(item, observed=observed, name="pulse history")
        rows = tuple(
            _pulse_history_source_row(value, trade_date=trade_date)
            for value in _parse_jsonl_objects(
                raw,
                name=f"pulse-{trade_date.isoformat()}.jsonl",
            )
        )
        if any(row.as_of > observed for row in rows):
            raise PageProjectionSourceIntegrityError("pulse history contains future evidence")
        history_source = PulseHistoryProjectionSource(
            available_at=file_available_at,
            rows=rows,
        )

    alert_source = None
    if alert_file is not None:
        raw, item = alert_file
        file_available_at = _source_file_time(item, observed=observed, name="pulse alerts")
        rows = tuple(
            _pulse_alert_source_row(value, trade_date=trade_date)
            for value in _parse_jsonl_objects(
                raw,
                name=f"pulse_alerts-{trade_date.isoformat()}.jsonl",
            )
        )
        if any(row.as_of > observed for row in rows):
            raise PageProjectionSourceIntegrityError("pulse alerts contain future evidence")
        alert_source = PulseAlertProjectionSource(available_at=file_available_at, rows=rows)

    config_source = None
    if config_file is not None:
        raw, item = config_file
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PageProjectionSourceIntegrityError(
                "surge runtime config is not valid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise PageProjectionSourceIntegrityError("surge runtime config must be an object")
        expected_fields = {
            "day",
            "boards",
            "k_rough",
            "k_cum",
            "ratio_cap",
            "skip_first_minutes",
            "tushare_rate_per_min",
            "require_price_strength",
            "max_room_to_limit_pct",
        }
        if set(value) != expected_fields:
            raise PageProjectionSourceIntegrityError(
                "surge runtime config fields do not match the writer contract"
            )
        file_time = _source_file_time(item, observed=observed, name="runtime config")
        normalized = dict(value)
        try:
            day_value = normalized.pop("day")
            boards_value = normalized.get("boards")
            if not isinstance(day_value, str) or not isinstance(boards_value, list):
                raise ValueError("runtime config source types are invalid")
            normalized["boards"] = tuple(boards_value)
            config_day = date.fromisoformat(day_value)
            row = SurgeRuntimeConfigProjectionRow(
                trade_date=config_day,
                as_of=file_time,
                **normalized,
            )
        except ValueError as exc:
            raise PageProjectionSourceIntegrityError("surge runtime config is invalid") from exc
        if config_day > trade_date:
            raise PageProjectionSourceIntegrityError(
                "surge runtime config contains a future trade date"
            )
        config_source = SurgeRuntimeConfigProjectionSource(
            available_at=file_time,
            row=row,
        )
    return history_source, alert_source, config_source


class ResearchGateProjectionRow(RuntimeContractModel):
    strategy_name: str = Field(min_length=1)
    range_start: date
    range_end: date
    as_of_time: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    code_commit: CommitSha
    audit_run_id: str | None = None
    dataset_snapshot_id: str | None = None
    dataset_binding_hash: str | None = None
    coverage_ratios: Mapping[str, float | None]
    coverage_counts: Mapping[str, tuple[int, int]]
    failures: tuple[ResearchGateFailure, ...] = ()
    metadata_ready: bool

    @field_validator("coverage_ratios", "coverage_counts", mode="after")
    @classmethod
    def freeze_mapping(cls, value: Mapping[str, object]) -> Mapping[str, object]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("coverage_ratios", "coverage_counts")
    def serialize_mapping(self, value: Mapping[str, object]) -> dict[str, object]:
        return dict(value)

    @model_validator(mode="after")
    def validate_evidence(self) -> Self:
        if self.range_start > self.range_end:
            raise ValueError("research gate range_start exceeds range_end")
        if self.completed_at < self.as_of_time:
            raise ValueError("research gate completion precedes as_of_time")
        if any(total < covered or covered < 0 for covered, total in self.coverage_counts.values()):
            raise ValueError("research gate coverage counts are invalid")
        return self


class SignalPageProjectionSnapshot(RuntimeContractModel):
    available_at: AwareUtcDatetime
    projections: tuple[ServingProjectionPayload, ...]
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        required_names = {
            "screen_bounds",
            "minute_coverage",
            "canvas_diagnostic",
            "canvas_latest_trade_date",
            "canvas_hit",
            "canvas_definition",
        }
        optional_names = {
            "pool_definition",
            "formula_pool_state",
            "formula_pool_definition",
            "formula_pool_latest_result",
            "screen_run_receipt",
            "pool_membership",
            "pool_member_return",
            "pulse_history",
            "pulse_alert",
            "surge_runtime_config",
            "monitor_event",
            "surge_event",
            "alert_ack_state",
            "alert_ack",
            "manual_watchlist_state",
            "manual_watchlist",
            "price_alert_rule_state",
            "price_alert_rule",
            "legacy_notification",
            "legacy_notification_status",
        }
        published_names = {item.table_name for item in self.projections}
        if not required_names.issubset(published_names) or not published_names.issubset(
            required_names | optional_names
        ):
            raise ValueError("signal page projection snapshot is incomplete")
        validate_formula_pool_projections({item.table_name: item for item in self.projections})
        validate_manual_watchlist_projections({item.table_name: item for item in self.projections})
        validate_price_alert_rule_projections({item.table_name: item for item in self.projections})
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("signal page projection snapshot hash mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        available_at: datetime,
        screen_bounds: tuple[ScreenBoundsProjectionRow, ...] = (),
        minute_coverage: tuple[MinuteCoverageProjectionRow, ...] = (),
        canvas_diagnostics: tuple[CanvasDiagnosticProjectionRow, ...] = (),
        canvas_latest_trade_date: CanvasLatestTradeDateProjectionRow | None = None,
        canvas_hits: tuple[CanvasHitProjectionRow, ...] = (),
        canvas_definitions: tuple[CanvasDefinitionProjectionRow, ...] = (),
        pool_definition: ServingProjectionPayload | None = None,
        formula_pool_state: ServingProjectionPayload | None = None,
        formula_pool_definition: ServingProjectionPayload | None = None,
        formula_pool_latest_result: ServingProjectionPayload | None = None,
        screen_run_receipt: ServingProjectionPayload | None = None,
        pool_membership: ServingProjectionPayload | None = None,
        pool_member_return: ServingProjectionPayload | None = None,
        pulse_history: PulseHistoryProjectionSource | None = None,
        pulse_alerts: PulseAlertProjectionSource | None = None,
        surge_runtime_config: SurgeRuntimeConfigProjectionSource | None = None,
        monitor_event: ServingProjectionPayload | None = None,
        surge_event: ServingProjectionPayload | None = None,
        alert_ack_state: ServingProjectionPayload | None = None,
        alert_ack: ServingProjectionPayload | None = None,
        manual_watchlist_state: ServingProjectionPayload | None = None,
        manual_watchlist: ServingProjectionPayload | None = None,
        price_alert_rule_state: ServingProjectionPayload | None = None,
        price_alert_rule: ServingProjectionPayload | None = None,
        legacy_notification: ServingProjectionPayload | None = None,
        legacy_notification_status: ServingProjectionPayload | None = None,
    ) -> SignalPageProjectionSnapshot:
        available = normalize_aware_utc(available_at)
        rows = {
            "screen_bounds": tuple(_screen_bounds_row(row) for row in screen_bounds),
            "minute_coverage": tuple(_minute_coverage_row(row) for row in minute_coverage),
            "canvas_diagnostic": tuple(_canvas_diagnostic_row(row) for row in canvas_diagnostics),
            "canvas_latest_trade_date": (
                ()
                if canvas_latest_trade_date is None
                else (
                    {
                        "snapshot_key": "current",
                        "trade_date": canvas_latest_trade_date.trade_date.isoformat(),
                    },
                )
            ),
            "canvas_hit": tuple(_canvas_hit_row(row) for row in canvas_hits),
            "canvas_definition": tuple(_canvas_definition_row(row) for row in canvas_definitions),
        }
        projections: tuple[ServingProjectionPayload, ...] = tuple(
            ServingProjectionPayload(
                table_name=table_name,
                available_at=available,
                rows=table_rows,
            )
            for table_name, table_rows in sorted(rows.items())
        )
        optional: list[ServingProjectionPayload] = []
        if pulse_history is not None:
            optional.append(
                ServingProjectionPayload(
                    table_name="pulse_history",
                    available_at=pulse_history.available_at,
                    rows=tuple(_pulse_history_row(row) for row in pulse_history.rows),
                )
            )
        if pulse_alerts is not None:
            optional.append(
                ServingProjectionPayload(
                    table_name="pulse_alert",
                    available_at=pulse_alerts.available_at,
                    rows=tuple(_pulse_alert_row(row) for row in pulse_alerts.rows),
                )
            )
        if surge_runtime_config is not None:
            optional.append(
                ServingProjectionPayload(
                    table_name="surge_runtime_config",
                    available_at=surge_runtime_config.available_at,
                    rows=(_surge_runtime_config_row(surge_runtime_config.row),),
                )
            )
        for table_name, projection in (
            ("pool_definition", pool_definition),
            ("formula_pool_state", formula_pool_state),
            ("formula_pool_definition", formula_pool_definition),
            ("formula_pool_latest_result", formula_pool_latest_result),
            ("screen_run_receipt", screen_run_receipt),
            ("pool_membership", pool_membership),
            ("pool_member_return", pool_member_return),
            ("monitor_event", monitor_event),
            ("surge_event", surge_event),
            ("alert_ack_state", alert_ack_state),
            ("alert_ack", alert_ack),
            ("manual_watchlist_state", manual_watchlist_state),
            ("manual_watchlist", manual_watchlist),
            ("price_alert_rule_state", price_alert_rule_state),
            ("price_alert_rule", price_alert_rule),
            ("legacy_notification", legacy_notification),
            ("legacy_notification_status", legacy_notification_status),
        ):
            if projection is not None:
                if projection.table_name != table_name:
                    raise ValueError("signal event projection has the wrong table")
                optional.append(projection)
        projections = tuple(sorted((*projections, *optional), key=lambda item: item.table_name))
        snapshot_available = max(item.available_at for item in projections)
        identity = {"available_at": snapshot_available, "projections": projections}
        return cls(**identity, content_sha256=canonical_sha256(identity))


class LabPageProjectionSnapshot(RuntimeContractModel):
    available_at: AwareUtcDatetime
    projections: tuple[ServingProjectionPayload, ...]
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        from rquant.factor.result_serving import (
            FACTOR_RESULT_PROJECTION_TABLES,
            validate_factor_result_projections,
        )
        from rquant.factor.tracking_serving import (
            FACTOR_TRACKING_PROJECTION_TABLES,
            validate_factor_tracking_projections,
        )

        names = {item.table_name for item in self.projections}
        required = {"data_audit_issue", "data_audit_status", "research_gate_metadata"}
        optional_groups = (
            REPORT_PROJECTION_TABLES,
            REPORT_JOB_PROJECTION_TABLES,
            BACKFILL_PLAN_PROJECTION_TABLES,
            FORMULA_MARKET_PROJECTION_TABLES,
            FACTOR_DEFINITION_PROJECTION_TABLES,
            FACTOR_RESULT_PROJECTION_TABLES,
            FACTOR_TRACKING_PROJECTION_TABLES,
        )
        if (
            not required.issubset(names)
            or any(names & group not in (set(), group) for group in optional_groups)
            or names - required - set().union(*optional_groups)
            or len(names) != len(self.projections)
            or tuple(item.table_name for item in self.projections) != tuple(sorted(names))
        ):
            raise ValueError("lab page projection snapshot is incomplete")
        projections = {item.table_name: item for item in self.projections}
        if names >= FORMULA_MARKET_PROJECTION_TABLES:
            validate_formula_market_projections(projections)
        if names >= FACTOR_DEFINITION_PROJECTION_TABLES:
            validate_factor_definition_projections(projections)
        if names >= FACTOR_RESULT_PROJECTION_TABLES:
            validate_factor_result_projections(projections)
        if names >= FACTOR_TRACKING_PROJECTION_TABLES:
            validate_factor_tracking_projections(projections)
        status = projections["data_audit_status"].rows
        issues = projections["data_audit_issue"].rows
        if len(status) != 1 or len(issues) != status[0]["finding_count"]:
            raise ValueError("lab audit projection row count differs")
        if any(item["audit_run_id"] != status[0]["successful_audit_id"] for item in issues):
            raise ValueError("lab audit projection mixes audit runs")
        if names >= REPORT_JOB_PROJECTION_TABLES:
            job_rows = projections["audit_report_job"].rows
            if len(job_rows) != 1 or (
                projections["audit_report_job"].available_at
                != projections["audit_report_job_event"].available_at
            ):
                raise ValueError("audit report task projection is incomplete")
            try:
                progress = DataAuditReportJobProgress.model_validate(dict(job_rows[0]))
                events = tuple(
                    DataAuditReportJobEvent.model_validate(dict(row))
                    for row in projections["audit_report_job_event"].rows
                )
                report_hash = (
                    projections["audit_report_overview"].rows[0]["report_hash"]
                    if names >= REPORT_PROJECTION_TABLES
                    else None
                )
                validate_data_audit_report_job_progress(
                    progress,
                    events,
                    available_at=projections["audit_report_job"].available_at,
                    report_hash=report_hash,
                )
                if names >= REPORT_PROJECTION_TABLES and any(
                    projections[name].available_at != projections["audit_report_job"].available_at
                    for name in REPORT_PROJECTION_TABLES
                ):
                    raise ValueError("audit report and task projection times disagree")
            except (IndexError, KeyError, TypeError, ValueError) as exc:
                raise ValueError("audit report task projection is invalid") from exc
        if names >= REPORT_PROJECTION_TABLES:
            overview = projections["audit_report_overview"].rows
            if len(overview) != 1:
                raise ValueError("audit report overview row is missing")
            summary = overview[0]
            if (
                summary["current"] is not False
                or summary["collection_status"] != "collection_unconfirmed"
                or summary["collection_completed_through"] is not None
                or summary["coverage_conclusion"] != "unconfirmed"
                or summary["source_mode"] != "production_unverified"
                or summary["source_namespace"] != "production"
            ):
                raise ValueError("audit report source and completion remain unconfirmed")
            report_hash = summary["report_hash"]
            for name in REPORT_PROJECTION_TABLES - {"audit_report_overview"}:
                if any(row["report_hash"] != report_hash for row in projections[name].rows):
                    raise ValueError("audit report projection mixes reports")
            if (
                len(projections["audit_report_month"].rows) != summary["monthly_count"]
                or len(projections["audit_report_rule"].rows) != summary["rule_count"]
                or len(projections["audit_report_issue"].rows) != summary["indexed_issue_count"]
                or summary["quality_issue_count"]
                != summary["indexed_issue_count"] + summary["omitted_issue_count"]
                or sum(row["issue_count"] for row in projections["audit_report_rule"].rows)
                != summary["quality_issue_count"]
            ):
                raise ValueError("audit report projection row counts disagree")
        if names >= BACKFILL_PLAN_PROJECTION_TABLES:
            catalog_rows = projections["backfill_plan_catalog"].rows
            progress_rows = projections["backfill_plan_progress"].rows
            job_rows = projections["backfill_plan_job"].rows
            if (
                len(catalog_rows) != 1
                or progress_rows
                != ({"status_key": "current", "availability": "unavailable", "task_id": None},)
                or len(job_rows) != 1
            ):
                raise ValueError("backfill plan progress is incomplete")
            catalog = catalog_rows[0]
            index = projections["backfill_plan_index"].rows
            preview = projections["backfill_plan_preview"].rows
            archive = projections["backfill_plan_archive"].rows
            try:
                progress = BackfillPlanProgressState.model_validate(dict(job_rows[0]))
                events = tuple(
                    BackfillPlanProgressEvent.model_validate(dict(row))
                    for row in projections["backfill_plan_event"].rows
                )
                if (
                    len(
                        {projections[name].available_at for name in BACKFILL_PLAN_PROJECTION_TABLES}
                    )
                    != 1
                ):
                    raise ValueError("backfill plan projection times disagree")
                validate_backfill_plan_progress(
                    progress,
                    events,
                    available_at=projections["backfill_plan_job"].available_at,
                    plan_hashes=frozenset(row["plan_hash"] for row in index),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("backfill plan progress is invalid") from exc
            if (
                catalog["catalog_key"] != "current"
                or catalog["total_plan_count"] != len(index)
                or catalog["indexed_plan_count"] != len(index)
                or catalog["preview_plan_count"] != len(preview)
                or catalog["has_older_plans"] is not False
                or catalog["oldest_indexed_hash"] != (index[-1]["plan_hash"] if index else None)
                or [row["rank"] for row in index] != list(range(len(index)))
                or len(preview) != min(MAX_PREVIEW_BACKFILL_PLANS, len(index))
                or {row["plan_hash"] for row in preview}
                != {row["plan_hash"] for row in index[: len(preview)]}
                or len(archive) != len(index)
                or {row["plan_hash"] for row in archive} != {row["plan_hash"] for row in index}
            ):
                raise ValueError("backfill plan catalog rows disagree")
            if any(
                row["source_mode"] != "production_unverified"
                or row["identity_verified"] is not False
                or row["collection_complete_verified"] is not False
                or row["quota_status"] != "unverified"
                or row["executable"] is not False
                for row in index
            ):
                raise ValueError("backfill plan cannot become verified or executable")
            try:
                index_by_hash = {row["plan_hash"]: row for row in index}
                for row in archive:
                    detail = decode_backfill_plan_archive_row(dict(row))
                    summary = index_by_hash[detail.plan_hash]
                    if (
                        detail.audit_start.isoformat() != summary["audit_start"]
                        or detail.completed_through.isoformat() != summary["completed_through"]
                        or detail.cutoff_observed_at.isoformat() != summary["cutoff_observed_at"]
                        or detail.published_at.isoformat() != summary["published_at"]
                        or len(detail.missing_dates) != summary["missing_day_count"]
                        or str(detail.estimate.estimated_seconds) != summary["estimated_seconds"]
                        or detail.source.mode != summary["source_mode"]
                        or detail.source.snapshot_label != summary["snapshot_label"]
                    ):
                        raise ValueError("backfill plan archive and index disagree")
                for row in preview:
                    source = json.loads(str(row["source_json"]))
                    estimate = json.loads(str(row["estimate_json"]))
                    if (
                        source["mode"] != "production_unverified"
                        or source["identity_verified"] is not False
                        or source["collection_complete_verified"] is not False
                        or estimate["quota_status"] != "unverified"
                    ):
                        raise ValueError("backfill plan preview cannot promote trust")
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise ValueError("backfill plan preview is invalid") from exc
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("lab page projection snapshot hash mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        available_at: datetime,
        rows: tuple[ResearchGateProjectionRow, ...] = (),
        audit_status: DataAuditStatusProjectionRow | None = None,
        audit_issues: tuple[DataAuditIssueProjectionRow, ...] = (),
        audit_report_projections: tuple[ServingProjectionPayload, ...] = (),
        audit_job_projections: tuple[ServingProjectionPayload, ...] = (),
        backfill_plan_projections: tuple[ServingProjectionPayload, ...] = (),
        formula_market_projections: tuple[ServingProjectionPayload, ...] = (),
        factor_definition_projections: tuple[ServingProjectionPayload, ...] = (),
        factor_result_projections: tuple[ServingProjectionPayload, ...] = (),
        factor_tracking_projections: tuple[ServingProjectionPayload, ...] = (),
    ) -> LabPageProjectionSnapshot:
        from rquant.factor.result_serving import FACTOR_RESULT_PROJECTION_TABLES

        available = normalize_aware_utc(available_at)
        status = audit_status or DataAuditStatusProjectionRow(
            latest_status="never_run", finding_count=0, p0_count=0
        )
        projections = (
            ServingProjectionPayload(
                table_name="data_audit_issue",
                available_at=available,
                rows=tuple(item.model_dump(mode="json") for item in audit_issues),
            ),
            ServingProjectionPayload(
                table_name="data_audit_status",
                available_at=available,
                rows=({"status_key": "current", **status.model_dump(mode="json")},),
            ),
            ServingProjectionPayload(
                table_name="research_gate_metadata",
                available_at=available,
                rows=tuple(_research_gate_row(row) for row in rows),
            ),
        )
        if (
            audit_report_projections
            and {item.table_name for item in audit_report_projections} != REPORT_PROJECTION_TABLES
        ):
            raise ValueError("audit report projections must be complete")
        if (
            audit_job_projections
            and {item.table_name for item in audit_job_projections} != REPORT_JOB_PROJECTION_TABLES
        ):
            raise ValueError("audit report task projections must be complete")
        if (
            backfill_plan_projections
            and {item.table_name for item in backfill_plan_projections}
            != BACKFILL_PLAN_PROJECTION_TABLES
        ):
            raise ValueError("backfill plan projections must be complete")
        if (
            formula_market_projections
            and {item.table_name for item in formula_market_projections}
            != FORMULA_MARKET_PROJECTION_TABLES
        ):
            raise ValueError("formula market projections must be complete")
        if (
            factor_definition_projections
            and {item.table_name for item in factor_definition_projections}
            != FACTOR_DEFINITION_PROJECTION_TABLES
        ):
            raise ValueError("factor definition projections must be complete")
        if (
            factor_result_projections
            and {item.table_name for item in factor_result_projections}
            != FACTOR_RESULT_PROJECTION_TABLES
        ):
            raise ValueError("factor result projections must be complete")
        projections = tuple(
            sorted(
                (
                    *projections,
                    *audit_report_projections,
                    *audit_job_projections,
                    *backfill_plan_projections,
                    *formula_market_projections,
                    *factor_definition_projections,
                    *factor_result_projections,
                    *factor_tracking_projections,
                ),
                key=lambda item: item.table_name,
            )
        )
        identity = {
            "available_at": max(item.available_at for item in projections),
            "projections": projections,
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))


def _screen_bounds_row(row: ScreenBoundsProjectionRow) -> dict[str, object]:
    return {
        "preset_name": row.preset_name,
        "min_date": row.min_date.isoformat(),
        "max_date": row.max_date.isoformat(),
        "candidate_count": row.candidate_count,
    }


def _minute_coverage_row(row: MinuteCoverageProjectionRow) -> dict[str, object]:
    return {
        "is_total": row.is_total,
        "source": row.source,
        "rows_count": row.rows_count,
        "codes_count": row.codes_count,
        "trade_dates": row.trade_dates,
        "min_time": None if row.min_time is None else row.min_time.isoformat(),
        "max_time": None if row.max_time is None else row.max_time.isoformat(),
    }


def _canvas_diagnostic_row(row: CanvasDiagnosticProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "preset_name": row.preset_name,
        "step_index": row.step_index,
        "rule_label": row.rule_label,
        "remaining_count": row.remaining_count,
    }


def _canvas_hit_row(row: CanvasHitProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "preset_name": row.preset_name,
        "ts_code": row.ts_code,
        "row_json": row.row_json,
    }


def _canvas_definition_row(row: CanvasDefinitionProjectionRow) -> dict[str, object]:
    return {
        "name": row.name,
        "description": row.description,
        "pool_refs_json": json.dumps(list(row.pool_refs), ensure_ascii=True, separators=(",", ":")),
        "created_at": row.created_at.isoformat().replace("+00:00", "Z"),
        "updated_at": row.updated_at.isoformat().replace("+00:00", "Z"),
        "source": row.source,
        "command_id": row.command_id,
        "command_hash": row.command_hash,
        "source_identity_hash": row.source_identity_hash,
        "record_hash": row.record_hash,
        "version_hash": row.version_hash,
    }


def _pulse_history_row(row: PulseHistoryProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "as_of": row.as_of.isoformat(),
        "t": row.t,
        "limit_up": row.limit_up,
        "limit_down": row.limit_down,
        "broken": row.broken,
        "up": row.up,
        "down": row.down,
        "up_ratio_pct": row.up_ratio_pct,
        "total": row.total,
    }


def _pulse_alert_row(row: PulseAlertProjectionRow) -> dict[str, object]:
    return {
        "trade_date": row.trade_date.isoformat(),
        "as_of": row.as_of.isoformat(),
        "t": row.t,
        "kind": row.kind,
        "kind_label": row.kind_label,
        "before": row.before,
        "after": row.after,
        "window_minutes": row.window_minutes,
        "message": row.message,
    }


def _surge_runtime_config_row(
    row: SurgeRuntimeConfigProjectionRow,
) -> dict[str, object]:
    return {
        "snapshot_key": "current",
        "trade_date": row.trade_date.isoformat(),
        "as_of": row.as_of.isoformat(),
        "boards_json": json.dumps(list(row.boards), ensure_ascii=True, separators=(",", ":")),
        "k_rough": row.k_rough,
        "k_cum": row.k_cum,
        "ratio_cap": row.ratio_cap,
        "skip_first_minutes": row.skip_first_minutes,
        "tushare_rate_per_min": row.tushare_rate_per_min,
        "require_price_strength": row.require_price_strength,
        "max_room_to_limit_pct": row.max_room_to_limit_pct,
    }


def _research_gate_row(row: ResearchGateProjectionRow) -> dict[str, object]:
    return {
        "strategy_name": row.strategy_name,
        "range_start": row.range_start.isoformat(),
        "range_end": row.range_end.isoformat(),
        "as_of_time": row.as_of_time.isoformat(),
        "completed_at": row.completed_at.isoformat(),
        "code_commit": row.code_commit,
        "audit_run_id": row.audit_run_id,
        "dataset_snapshot_id": row.dataset_snapshot_id,
        "dataset_binding_hash": row.dataset_binding_hash,
        "coverage_ratios_json": json.dumps(
            dict(row.coverage_ratios), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ),
        "coverage_counts_json": json.dumps(
            dict(row.coverage_counts), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ),
        "failures_json": json.dumps(
            [item.model_dump(mode="json") for item in row.failures],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "metadata_ready": row.metadata_ready,
    }


__all__ = [
    "CanvasDiagnosticProjectionRow",
    "CanvasDefinitionProjectionRow",
    "CanvasHitProjectionRow",
    "CanvasLatestTradeDateProjectionRow",
    "DuckDBLabPageProjectionSource",
    "DuckDBSignalPageProjectionSource",
    "LabPageProjectionSnapshot",
    "MinuteCoverageProjectionRow",
    "PulseAlertProjectionRow",
    "PulseAlertProjectionSource",
    "PulseHistoryProjectionRow",
    "PulseHistoryProjectionSource",
    "ResearchGateProjectionRow",
    "ScreenBoundsProjectionRow",
    "SignalPageProjectionProducer",
    "SignalPageProjectionSnapshot",
    "SurgeRuntimeConfigProjectionRow",
    "SurgeRuntimeConfigProjectionSource",
]
