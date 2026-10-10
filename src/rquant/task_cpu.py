"""Complete kernel observations and the sole task CPU calculation contract."""

from __future__ import annotations

import json
import contextlib
import hashlib
import os
import re
import stat
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, StrictInt, StrictStr, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

SLICES = (
    "rquant.slice", "rquant-live.slice", "rquant-serving.slice",
    "rquant-research.slice", "rquant-maintenance.slice",
)
MAX_PAIR_BYTES = 128 * 1024
_CGROUP_ROOT = Path("/sys/fs/cgroup")
_PROC_ROOT = Path("/proc")
_ONLINE_PATH = Path("/sys/devices/system/cpu/online")
Counter = Annotated[StrictInt, Field(ge=0, le=2**63 - 1)]
Index = Annotated[StrictInt, Field(ge=0, le=2047)]
Digest = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
CpuList = Annotated[StrictStr, Field(max_length=4096)]
ReadError = Literal["absent", "permission", "invalid", "changed", "deadline"]
CpuReason = Literal[
    "first_sample", "identity_changed", "capacity_changed", "capacity_incomplete",
    "capacity_mismatch", "clock_discontinuity", "counter_reset", "empty_members",
    "capture_unavailable", "stale", "budget_exceeded",
]
# Compact pools retain before/after raw identities without repeating all thread fields per slice.
ThreadIdentity = tuple[Counter, Counter, Counter, Counter, Counter, Index]
ThreadReferences = tuple[Index, Index, Index, Index]


def parse_cpu_list(value: str) -> tuple[int, ...]:
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise ValueError("CPU list is missing or exceeds budget")
    cpus: set[int] = set()
    for part in value.split(","):
        if not re.fullmatch(r"[0-9]{1,4}(?:-[0-9]{1,4})?", part):
            raise ValueError("CPU list contains invalid bounded range")
        start, _, end = part.partition("-")
        low, high = int(start), int(end or start)
        if low > high or high >= 4096:
            raise ValueError("CPU list contains invalid bounded range")
        values = set(range(low, high + 1))
        if cpus & values:
            raise ValueError("CPU list contains duplicate CPUs")
        cpus.update(values)
    return tuple(sorted(cpus))


def parse_kernel_counter(value: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,19}", value):
        raise ValueError("kernel counter exceeds numeric budget")
    result = int(value)
    if result > 2**63 - 1:
        raise ValueError("kernel counter exceeds numeric budget")
    return result


def _quota(value: str) -> Fraction | None:
    parts = value.split(" ")
    if len(parts) != 2:
        raise ValueError("cpu.max is invalid")
    period = parse_kernel_counter(parts[1])
    if not period:
        raise ValueError("cpu.max period must be positive")
    if parts[0] == "max":
        return None
    quota = parse_kernel_counter(parts[0])
    if not quota:
        raise ValueError("cpu.max quota must be positive")
    return Fraction(quota, period)


class TaskCpuNode(RuntimeContractModel):
    path: StrictStr = Field(min_length=1, max_length=1024)
    device: Counter
    inode: Counter
    controllers: tuple[StrictStr, ...] = Field(max_length=16)
    subtree_control: tuple[StrictStr, ...] = Field(max_length=16)
    cpu_max: StrictStr | None = Field(default=None, max_length=41)
    cpu_max_error: ReadError | None = None
    cpuset_effective: CpuList | None = None
    cpuset_error: ReadError | None = None

    @model_validator(mode="after")
    def validate_kernel_text(self) -> TaskCpuNode:
        if not self.path.startswith("/") or any(part in {"", ".", ".."} for part in self.path[1:].split("/") if self.path != "/"):
            raise ValueError("cgroup path is not canonical")
        if len(set(self.controllers)) != len(self.controllers) or len(set(self.subtree_control)) != len(self.subtree_control):
            raise ValueError("cgroup controllers contain duplicates")
        if any(not re.fullmatch(r"[a-z_]{1,20}", name) for name in self.controllers + self.subtree_control):
            raise ValueError("cgroup controller is invalid")
        for text, error in [(self.cpu_max, self.cpu_max_error), (self.cpuset_effective, self.cpuset_error)]:
            if (text is None) == (error is None):
                raise ValueError("kernel read must carry text or its original error")
        if self.cpu_max is not None:
            _quota(self.cpu_max)
        if self.cpuset_effective is not None:
            parse_cpu_list(self.cpuset_effective)
        return self


class TaskCpuGroup(RuntimeContractModel):
    slice_name: StrictStr
    node_index: Index | None = None
    invocation_id: StrictStr | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    usage_usec: Counter | None = None
    observed_at: AwareUtcDatetime
    monotonic_ns: Counter
    boottime_ns: Counter
    members_before: tuple[Index, ...] = Field(max_length=1024)
    members_after: tuple[Index, ...] = Field(max_length=1024)
    error: ReadError | None = None

    @model_validator(mode="after")
    def validate_members(self) -> TaskCpuGroup:
        if len(set(self.members_before)) != len(self.members_before) or self.members_before != self.members_after:
            raise ValueError("CPU member enumeration changed or contains duplicates")
        if self.error is None and (self.node_index is None or self.usage_usec is None or self.invocation_id is None):
            raise ValueError("complete CPU group requires node/counter/invocation")
        return self


class TaskCpuObservation(RuntimeContractModel):
    contract: Literal["task-cpu/v1"] = "task-cpu/v1"
    collector_digest: Digest
    host_name: StrictStr = Field(min_length=1, max_length=255)
    boot_id: StrictStr = Field(pattern=r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    manifest_digest: Digest
    observed_at: AwareUtcDatetime
    monotonic_ns: Counter
    boottime_ns: Counter
    mount_id: Counter
    mount_device: StrictStr = Field(pattern=r"^[0-9]{1,10}:[0-9]{1,10}$")
    mount_root: Literal["/"]
    mount_path: Literal["/sys/fs/cgroup"]
    filesystem: Literal["cgroup2"]
    online_cpus: CpuList
    nodes: tuple[TaskCpuNode, ...] = Field(min_length=1, max_length=256)
    identity_table: tuple[ThreadIdentity, ...] = Field(max_length=2048)
    affinity_table: tuple[CpuList, ...] = Field(max_length=1024)
    threads: tuple[ThreadReferences, ...] = Field(max_length=1024)
    groups: tuple[TaskCpuGroup, ...] = Field(min_length=5, max_length=5)

    @model_validator(mode="after")
    def validate_raw_graph(self) -> TaskCpuObservation:
        parse_cpu_list(self.online_cpus)
        if tuple(g.slice_name for g in self.groups) != SLICES:
            raise ValueError("CPU observation requires all exact five slices")
        paths = tuple(n.path for n in self.nodes)
        if len(set(paths)) != len(paths) or paths[0] != "/":
            raise ValueError("CPU nodes must have a unique true root")
        for mask in self.affinity_table:
            parse_cpu_list(mask)
        actual_tids: set[int] = set()
        for before, after, mask_before, mask_after in self.threads:
            if max(before, after) >= len(self.identity_table) or max(mask_before, mask_after) >= len(self.affinity_table):
                raise ValueError("CPU identity/affinity reference is absent")
            identity = self.identity_table[before]
            if identity != self.identity_table[after] or identity[5] >= len(self.nodes):
                raise ValueError("CPU thread identity changed during observation")
            if identity[0] < 1 or identity[1] < 1 or identity[1] in actual_tids:
                raise ValueError("CPU thread identity is duplicated or invalid")
            actual_tids.add(identity[1])
            if parse_cpu_list(self.affinity_table[mask_before]) != parse_cpu_list(self.affinity_table[mask_after]):
                raise ValueError("CPU thread affinity changed during observation")
        for group in self.groups:
            if group.observed_at > self.observed_at or group.monotonic_ns > self.monotonic_ns or group.boottime_ns > self.boottime_ns:
                raise ValueError("CPU group is beyond observation cutoff")
            if group.node_index is not None:
                if group.node_index >= len(self.nodes):
                    raise ValueError("CPU group node reference is absent")
                expected = "/rquant.slice" if group.slice_name == "rquant.slice" else "/rquant.slice/" + group.slice_name
                if self.nodes[group.node_index].path != expected:
                    raise ValueError("CPU group is outside its exact physical path")
                for member in group.members_before:
                    if member >= len(self.threads):
                        raise ValueError("CPU member reference is absent")
                    identity = self.identity_table[self.threads[member][0]]
                    path = self.nodes[identity[5]].path
                    if path != expected and not path.startswith(expected + "/"):
                        raise ValueError("CPU member is outside measured group")
        return self


class TaskCpuPair(RuntimeContractModel):
    previous: TaskCpuObservation | None
    current: TaskCpuObservation

    @model_validator(mode="after")
    def bound_complete_pair(self) -> TaskCpuPair:
        if len(json.dumps(self.model_dump(mode="json"), ensure_ascii=True, separators=(",", ":")).encode()) > MAX_PAIR_BYTES:
            raise ValueError("complete raw CPU pair exceeds byte budget")
        return self


class TaskCpuResult(RuntimeContractModel):
    slice_name: StrictStr
    percent: StrictStr | None = Field(default=None, pattern=r"^(?:[0-9]|[1-9][0-9]|100)\.[0-9]$")
    reason: CpuReason | None = None
    capacity: StrictStr | None = Field(default=None, max_length=40)
    effective_cpus: tuple[StrictInt, ...] = Field(default=(), max_length=4096)

    @model_validator(mode="after")
    def unknown_has_no_number(self) -> TaskCpuResult:
        if (self.percent is None) != (self.reason is not None):
            raise ValueError("unknown CPU requires a reason and no numeric percentage")
        return self


def _chain(sample: TaskCpuObservation, group: TaskCpuGroup) -> tuple[TaskCpuNode, ...]:
    assert group.node_index is not None
    by_path = {n.path: n for n in sample.nodes}
    current = sample.nodes[group.node_index].path
    result: list[TaskCpuNode] = []
    while True:
        if current not in by_path or len(result) >= 16:
            raise ValueError("incomplete ancestor chain")
        result.append(by_path[current])
        if current == "/":
            return tuple(result)
        current = current.rsplit("/", 1)[0] or "/"


def _capacity(sample: TaskCpuObservation, group: TaskCpuGroup) -> tuple[Fraction, tuple[int, ...], tuple[object, ...]]:
    chain = _chain(sample, group)
    limits: list[Fraction] = []
    cpuset: tuple[int, ...] | None = None
    for index, node in enumerate(chain):
        if "cpu" not in node.controllers:
            raise ValueError("cpu controller is unavailable")
        if node.cpu_max is None:
            if node.path != "/" or node.cpu_max_error != "absent":
                raise ValueError("missing non-root quota")
        elif (quota := _quota(node.cpu_max)) is not None:
            limits.append(quota)
        if cpuset is None:
            if node.cpuset_effective is not None:
                cpuset = parse_cpu_list(node.cpuset_effective)
            elif node.cpuset_error != "absent" or "cpuset" in node.controllers or index + 1 == len(chain) or "cpuset" in chain[index + 1].subtree_control:
                raise ValueError("missing delegated cpuset")
    if cpuset is None:
        raise ValueError("cpuset ancestor proof is absent")
    affinities: set[int] = set()
    thread_affinities: list[tuple[tuple[object, ...], tuple[int, ...]]] = []
    for member in group.members_before:
        fact = sample.identity_table[sample.threads[member][0]]
        node = sample.nodes[fact[5]]
        mask = parse_cpu_list(sample.affinity_table[sample.threads[member][2]])
        thread_affinities.append(((*fact[:5], node.path, node.device, node.inode), mask))
        affinities.update(mask)
    effective = tuple(sorted(set(parse_cpu_list(sample.online_cpus)) & set(cpuset) & affinities))
    if not effective:
        raise ValueError("effective CPU capacity is empty")
    capacity = min((Fraction(len(effective)), *limits))
    material = (effective, tuple((n.path, n.controllers, n.subtree_control, n.cpu_max, n.cpu_max_error, n.cpuset_effective, n.cpuset_error) for n in chain), tuple(sorted(thread_affinities)))
    return capacity, effective, material


def _member_identities(sample: TaskCpuObservation, group: TaskCpuGroup) -> tuple[object, ...]:
    identities: list[object] = []
    for member in group.members_before:
        fact = sample.identity_table[sample.threads[member][0]]
        node = sample.nodes[fact[5]]
        identities.append((*fact[:5], node.path, node.device, node.inode))
    return tuple(sorted(identities))


def _rows(pair: TaskCpuPair, cutoff: datetime) -> tuple[TaskCpuResult, ...]:
    current, previous = pair.current, pair.previous
    if current.observed_at > cutoff or (previous is not None and previous.observed_at > cutoff):
        raise ValueError("CPU raw facts exceed owner cutoff")
    identity_fields = ("host_name", "boot_id", "manifest_digest", "collector_digest", "contract", "mount_id", "mount_device", "mount_root", "mount_path", "filesystem")
    rows: list[TaskCpuResult] = []
    for index, group in enumerate(current.groups):
        reason: CpuReason | None = None
        capacity: Fraction | None = None
        effective: tuple[int, ...] = ()
        percent: str | None = None
        if (cutoff - current.observed_at).total_seconds() >= 120:
            reason = "stale"
        elif previous is None:
            reason = "first_sample"
        else:
            before = previous.groups[index]
            if any(getattr(previous, f) != getattr(current, f) for f in identity_fields):
                reason = "identity_changed"
            elif group.error is not None or before.error is not None:
                reason = "capture_unavailable"
            elif not group.members_before or not before.members_before:
                reason = "empty_members"
            else:
                delta = group.boottime_ns - before.boottime_ns
                mono_delta = group.monotonic_ns - before.monotonic_ns
                if not 500_000_000 <= delta <= 120_000_000_000 or mono_delta <= 0 or abs(mono_delta - delta) > 5_000_000 or group.observed_at < before.observed_at or current.observed_at < previous.observed_at:
                    reason = "clock_discontinuity"
                elif group.invocation_id != before.invocation_id or _member_identities(current, group) != _member_identities(previous, before):
                    reason = "identity_changed"
                else:
                    try:
                        chain = _chain(current, group)
                        old_chain = _chain(previous, before)
                        if tuple((n.path, n.device, n.inode) for n in chain) != tuple((n.path, n.device, n.inode) for n in old_chain):
                            reason = "identity_changed"
                        else:
                            capacity, effective, material = _capacity(current, group)
                            _, _, old_material = _capacity(previous, before)
                            if material != old_material:
                                reason = "capacity_changed"
                    except ValueError:
                        reason = "capacity_incomplete"
                    if reason is None:
                        assert group.usage_usec is not None and before.usage_usec is not None and capacity is not None
                        usage_delta = group.usage_usec - before.usage_usec
                        if usage_delta < 0:
                            reason = "counter_reset"
                        else:
                            ratio = Fraction(usage_delta * 100_000, delta) / capacity
                            if ratio > 100:
                                reason = "capacity_mismatch"
                            else:
                                tenths = (ratio.numerator * 20 + ratio.denominator) // (2 * ratio.denominator)
                                percent = f"{tenths // 10}.{tenths % 10}"
        rows.append(TaskCpuResult(slice_name=group.slice_name, percent=percent, reason=reason, capacity=str(capacity) if capacity is not None else None, effective_cpus=effective))
    return tuple(rows)


class TaskCpuEvidence(RuntimeContractModel):
    pair: TaskCpuPair
    cutoff: AwareUtcDatetime
    groups: tuple[TaskCpuResult, ...] = Field(min_length=5, max_length=5)
    material_hash: Digest

    @model_validator(mode="after")
    def verify_material_and_calculation(self) -> TaskCpuEvidence:
        if self.material_hash != canonical_sha256(self.pair.model_dump(mode="json")) or self.groups != _rows(self.pair, self.cutoff):
            raise ValueError("CPU projection does not match complete trusted raw material")
        return self


def compute_task_cpu(pair: TaskCpuPair, *, cutoff: datetime) -> TaskCpuEvidence:
    pair = TaskCpuPair.model_validate(pair)
    return TaskCpuEvidence(pair=pair, cutoff=cutoff, groups=_rows(pair, cutoff), material_hash=canonical_sha256(pair.model_dump(mode="json")))


def _boottime_ns() -> int:
    return time.clock_gettime_ns(time.CLOCK_BOOTTIME)


@contextlib.contextmanager
def _directory(path: Path) -> Iterator[int]:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open("/", flags)
    try:
        for part in path.parts[1:]:
            if part in {"", ".", ".."}:
                raise ValueError("kernel directory path is invalid")
            following = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise ValueError("kernel directory is not a directory")
        yield descriptor
    finally:
        os.close(descriptor)


class _CpuCapture:
    def __init__(self, *, max_seconds: float) -> None:
        if not 0 < max_seconds <= 20:
            raise TimeoutError("kernel capture has no remaining deadline")
        self.deadline = time.monotonic() + max_seconds
        self.nodes: list[TaskCpuNode] = []
        self.identities: list[ThreadIdentity] = []
        self.affinities: list[str] = []
        self.threads: list[ThreadReferences] = []

    def check(self) -> None:
        if time.monotonic() >= self.deadline:
            raise TimeoutError("kernel capture exceeded total deadline")

    def read(self, path: Path, *, limit: int = 4096) -> str:
        self.check()
        with _directory(path.parent) as parent:
            descriptor = os.open(path.name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0), dir_fd=parent)
            try:
                before = os.fstat(descriptor)
                if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
                    raise ValueError("kernel read is not a bounded regular file")
                payload = os.read(descriptor, limit + 1)
                after = os.fstat(descriptor)
                if len(payload) > limit or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                    raise ValueError("kernel read changed identity or exceeded budget")
                text = payload.decode("ascii", "strict").strip()
            finally:
                os.close(descriptor)
        self.check()
        return text

    def optional(self, path: Path) -> tuple[str | None, ReadError | None]:
        try:
            return self.read(path), None
        except FileNotFoundError:
            return None, "absent"
        except PermissionError:
            return None, "permission"

    def physical(self, path: Path) -> tuple[int, int]:
        self.check()
        with _directory(path) as descriptor:
            identity = os.fstat(descriptor)
            return identity.st_dev, identity.st_ino

    def node(self, path: str) -> int:
        physical = _CGROUP_ROOT / path.lstrip("/")
        before = self.physical(physical)
        quota, quota_error = self.optional(physical / "cpu.max")
        cpuset, cpuset_error = self.optional(physical / "cpuset.cpus.effective")
        node = TaskCpuNode(path=path, device=before[0], inode=before[1], controllers=tuple(self.read(physical / "cgroup.controllers").split()), subtree_control=tuple(self.read(physical / "cgroup.subtree_control").split()), cpu_max=quota, cpu_max_error=quota_error, cpuset_effective=cpuset, cpuset_error=cpuset_error)
        if before != self.physical(physical):
            raise ValueError("kernel cgroup physical identity changed")
        for index, existing in enumerate(self.nodes):
            if existing.path == path:
                if existing != node:
                    raise ValueError("kernel capacity changed during capture")
                return index
        if len(self.nodes) >= 256:
            raise ValueError("kernel node budget exceeded")
        self.nodes.append(node)
        return len(self.nodes) - 1

    def ancestors(self, path: str) -> int:
        chain = [path]
        while chain[-1] != "/":
            if len(chain) >= 16:
                raise ValueError("kernel ancestor budget exceeded")
            chain.append(chain[-1].rsplit("/", 1)[0] or "/")
        for ancestor in reversed(chain):
            self.node(ancestor)
        return self.node(path)

    def membership(self, measured: str) -> tuple[tuple[tuple[str, int, int], ...], tuple[int, ...], tuple[tuple[int, tuple[int, ...]], ...]]:
        pending = [measured]
        directories: list[tuple[str, int, int]] = []
        pids: set[int] = set()
        while pending:
            self.check()
            path = pending.pop()
            if len(directories) >= 64 or len(path.split("/")) > 17:
                raise ValueError("kernel membership directory budget exceeded")
            physical = _CGROUP_ROOT / path.lstrip("/")
            with _directory(physical) as descriptor:
                identity = os.fstat(descriptor)
                directories.append((path, identity.st_dev, identity.st_ino))
                entries = os.listdir(descriptor)
                if len(entries) > 512:
                    raise ValueError("kernel membership entry budget exceeded")
                for name in sorted(entries, reverse=True):
                    item = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    if stat.S_ISLNK(item.st_mode):
                        raise ValueError("kernel cgroup contains a symlink")
                    if stat.S_ISDIR(item.st_mode):
                        if not re.fullmatch(r"[A-Za-z0-9_@.:-]{1,128}", name) or name in {".", ".."}:
                            raise ValueError("kernel cgroup child name is invalid")
                        pending.append(path.rstrip("/") + "/" + name)
            for value in self.read(physical / "cgroup.procs").split():
                pid = parse_kernel_counter(value)
                if not 0 < pid <= 2**31 - 1:
                    raise ValueError("kernel PID is invalid")
                pids.add(pid)
            if len(pids) > 256:
                raise ValueError("kernel PID budget exceeded")
        task_ids: list[tuple[int, tuple[int, ...]]] = []
        total = 0
        for pid in sorted(pids):
            with _directory(_PROC_ROOT / str(pid) / "task") as descriptor:
                tids = tuple(sorted(parse_kernel_counter(name) for name in os.listdir(descriptor)))
            if not tids or any(not 0 < tid <= 2**31 - 1 for tid in tids):
                raise ValueError("kernel thread enumeration is incomplete")
            total += len(tids)
            if total > 1024:
                raise ValueError("kernel thread budget exceeded")
            task_ids.append((pid, tids))
        return tuple(sorted(directories)), tuple(sorted(pids)), tuple(task_ids)

    def thread_identity(self, pid: int, tid: int, measured: str) -> ThreadIdentity:
        task = _PROC_ROOT / str(pid) / "task" / str(tid)
        device, inode = self.physical(task)
        value = self.read(task / "stat")
        try:
            tick = parse_kernel_counter(value.rsplit(")", 1)[1].split()[19])
            if int(value.split(" ", 1)[0]) != tid:
                raise ValueError("kernel thread stat identity mismatch")
        except (IndexError, ValueError) as exc:
            raise ValueError("kernel thread stat is invalid") from exc
        cgroups = self.read(task / "cgroup").splitlines()
        if len(cgroups) != 1 or not cgroups[0].startswith("0::/"):
            raise ValueError("kernel thread cgroup is not unified")
        actual = cgroups[0][3:]
        if actual != measured and not actual.startswith(measured + "/"):
            raise ValueError("kernel thread moved outside measured cgroup")
        node = self.node(actual)
        if (device, inode) != self.physical(task):
            raise ValueError("kernel task physical identity changed")
        return pid, tid, tick, device, inode, node

    def thread(self, pid: int, tid: int, measured: str) -> int:
        self.check()
        before = self.thread_identity(pid, tid, measured)
        first_mask = tuple(sorted(os.sched_getaffinity(tid)))
        after = self.thread_identity(pid, tid, measured)
        last_mask = tuple(sorted(os.sched_getaffinity(tid)))
        if before != after or first_mask != last_mask or not first_mask:
            raise ValueError("kernel thread identity/affinity changed")
        mask = ",".join(str(cpu) for cpu in first_mask)
        parse_cpu_list(mask)
        if before not in self.identities:
            if len(self.identities) >= 2048:
                raise ValueError("kernel identity budget exceeded")
            self.identities.append(before)
        if mask not in self.affinities:
            self.affinities.append(mask)
        identity_index, mask_index = self.identities.index(before), self.affinities.index(mask)
        reference = identity_index, identity_index, mask_index, mask_index
        for index, existing in enumerate(self.threads):
            if self.identities[existing[0]][1] == tid:
                if existing != reference:
                    raise ValueError("kernel thread changed between group reads")
                return index
        if len(self.threads) >= 1024:
            raise ValueError("kernel thread budget exceeded")
        self.threads.append(reference)
        return len(self.threads) - 1

    def counter(self, path: str) -> int:
        values: dict[str, int] = {}
        for line in self.read(_CGROUP_ROOT / path.lstrip("/") / "cpu.stat").splitlines():
            parts = line.split(" ")
            if len(parts) != 2 or parts[0] in values:
                raise ValueError("kernel cpu.stat is invalid")
            values[parts[0]] = parse_kernel_counter(parts[1])
        if "usage_usec" not in values:
            raise ValueError("kernel cpu.stat lacks usage_usec")
        return values["usage_usec"]


class LinuxTaskCpuReader:
    """Fixed read-only kernel leaf; no paths, command arguments or identity from HTTP."""

    def capture(self, *, host_name: str, boot_id: str, manifest_digest: str, properties: Mapping[str, Mapping[str, str]], max_seconds: float, clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> TaskCpuObservation:
        io = _CpuCapture(max_seconds=max_seconds)
        original_boot = io.read(_PROC_ROOT / "sys/kernel/random/boot_id", limit=128)
        if original_boot != boot_id:
            raise ValueError("kernel boot differs from trusted ops sample")
        if tuple(properties) != SLICES:
            raise ValueError("kernel capture requires exact cgroup properties")
        root_identity = io.physical(_CGROUP_ROOT)
        mounts: list[tuple[str, str]] = []
        for line in io.read(_PROC_ROOT / str(os.getpid()) / "mountinfo", limit=64 * 1024).splitlines():
            head, separator, tail = line.partition(" - ")
            fields, options = head.split(), tail.split()
            if separator and len(fields) >= 6 and fields[4] == "/sys/fs/cgroup":
                if len(options) < 3 or options[0] != "cgroup2" or fields[3] != "/":
                    raise ValueError("kernel cgroup mount/root proof is incomplete")
                mounts.append((fields[0], fields[2]))
        if len(mounts) != 1 or mounts[0][1] != f"{os.major(root_identity[0])}:{os.minor(root_identity[0])}":
            raise ValueError("kernel cgroup mount physical identity is invalid")
        online = io.read(_ONLINE_PATH)
        parse_cpu_list(online)
        groups: list[TaskCpuGroup] = []
        for name in SLICES:
            properties_for_group = properties[name]
            expected = "/rquant.slice" if name == "rquant.slice" else "/rquant.slice/" + name
            if properties_for_group.get("ControlGroup") != expected or properties_for_group.get("LoadState") != "loaded":
                raise ValueError("kernel unit cgroup is not the exact installed path")
            node = io.ancestors(expected)
            membership_before = io.membership(expected)
            members = tuple(io.thread(pid, tid, expected) for pid, tids in membership_before[2] for tid in tids)
            if membership_before != io.membership(expected):
                raise ValueError("kernel membership changed during capture")
            usage = io.counter(expected)
            groups.append(TaskCpuGroup(slice_name=name, node_index=node, invocation_id=properties_for_group.get("InvocationID"), usage_usec=usage, observed_at=clock(), monotonic_ns=time.monotonic_ns(), boottime_ns=_boottime_ns(), members_before=members, members_after=members))
        for node in tuple(io.nodes):
            io.node(node.path)
        if io.read(_PROC_ROOT / "sys/kernel/random/boot_id", limit=128) != original_boot or io.physical(_CGROUP_ROOT) != root_identity or io.read(_ONLINE_PATH) != online:
            raise ValueError("kernel host/boot/capacity changed during capture")
        io.check()
        return TaskCpuObservation(collector_digest=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), host_name=host_name, boot_id=boot_id, manifest_digest=manifest_digest, observed_at=clock(), monotonic_ns=time.monotonic_ns(), boottime_ns=_boottime_ns(), mount_id=parse_kernel_counter(mounts[0][0]), mount_device=mounts[0][1], mount_root="/", mount_path="/sys/fs/cgroup", filesystem="cgroup2", online_cpus=online, nodes=tuple(io.nodes), identity_table=tuple(io.identities), affinity_table=tuple(io.affinities), threads=tuple(io.threads), groups=tuple(groups))
