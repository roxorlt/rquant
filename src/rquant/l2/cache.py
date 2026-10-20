"""Private immutable observation batches; directory rename is the commit point."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shutil
import stat
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock

from rquant.l2.models import BatchPage, FeedStatus, MarketObservation, RawL2Batch, SubscriptionReceipt

_ACTIVE = {"starting", "connected", "seeded_waiting", "listening_candidate"}
_STALE_SECONDS = 60
_MAX_BATCH_BYTES = 20 * 1024 * 1024
MAX_BATCH_PAGE = 128
MAX_BATCH_PAGE_BYTES = 24 * 1024 * 1024
_MAX_RUNTIME_BYTES = 256 * 1024


class CacheError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise CacheError("unsafe_path")


def _read(path: Path, maximum: int) -> bytes:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
        raise CacheError("unsafe_path")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size > maximum:
            raise CacheError("unsafe_path")
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise CacheError("invalid_cache")
    return data


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_new(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _replace_snapshot(root: Path, name: str, payload: bytes) -> None:
    temporary = None
    try:
        if (root / name).is_symlink():
            raise CacheError('unsafe_path')
        fd, value = tempfile.mkstemp(prefix='.status-', dir=root)
        temporary = Path(value)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, root / name)
        temporary = None
        _fsync_dir(root)
    except OSError:
        raise CacheError('cache_io') from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class RawL2Cache:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._writer_fd: int | None = None
        self._highwater = 0
        self._last_batch: RawL2Batch | None = None
        self._head_receipt: tuple[tuple[int, int, str], int, datetime] | None = None
        self._head_lock = Lock()

    def _require_writer(self) -> None:
        if self._writer_fd is None:
            raise CacheError("writer_required")

    @contextmanager
    def writer(self) -> Iterator[RawL2Cache]:
        if self._writer_fd is not None:
            raise CacheError("writer_busy")
        fd: int | None = None
        try:
            self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
            _directory(self.root)
            fd = os.open(self.root / ".writer.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CacheError("writer_busy") from None
            self._writer_fd = fd
            batches = self.root / "batches"
            batches.mkdir(mode=0o700, exist_ok=True)
            _directory(batches)
            committed = self._directories()
            for expected, directory in enumerate(committed, 1):
                self._last_batch = self._read_batch(directory, expected)
            self._highwater = len(committed)
            head = self._published_head()
            if committed and (head is None or head[0] != self._highwater):
                self._publish_head(committed[-1])
                _fsync_dir(batches)
            previous = self.read_status()
            if previous.state in _ACTIVE or committed:
                self.write_status(previous.model_copy(update={
                    "state": "disconnected", "gap": True, "error_code": "restart_gap",
                }))
            yield self
        except CacheError:
            raise
        except OSError:
            raise CacheError("cache_io") from None
        finally:
            self._writer_fd = None
            self._highwater = 0
            self._last_batch = None
            if fd is not None:
                os.close(fd)

    def _directories(self) -> tuple[Path, ...]:
        path = self.root / "batches"
        if self.root.is_symlink() or path.is_symlink():
            raise CacheError("unsafe_path")
        minimum_committed = self._stored_status().last_sequence
        if not path.exists():
            if minimum_committed:
                raise CacheError("sequence_gap")
            return ()
        _directory(self.root)
        _directory(path)
        try:
            head = self._published_head()
            names = [p.name for p in path.iterdir()
                     if not p.name.startswith(".pending-") and p.name != ".head"]
            for name in names:
                if not re.fullmatch(r"[0-9]{20}", name):
                    raise CacheError("invalid_cache")
                if int(name) == 0:
                    raise CacheError("sequence_gap")
            # A readdir may see a pending rename and a later batch but omit the
            # renamed middle entry. Check every path within this finite bound.
            upper = max(minimum_committed, head[0] if head else 0,
                        max((int(name) for name in names), default=0))
            directories = []
            for expected in range(1, upper + 1):
                directory = path / f"{expected:020d}"
                if not directory.exists() and not directory.is_symlink():
                    raise CacheError("sequence_gap")
                _directory(directory)
                try:
                    if (not stat.S_ISREG((directory / "batch.json").lstat().st_mode)
                            or not stat.S_ISREG((directory / "sha256").lstat().st_mode)):
                        raise CacheError("invalid_batch")
                except OSError:
                    raise CacheError("invalid_batch") from None
                directories.append(directory)
            return tuple(directories)
        except OSError:
            raise CacheError("cache_io") from None

    @staticmethod
    def _read_batch(directory: Path, expected: int, *, maximum: int = _MAX_BATCH_BYTES) -> RawL2Batch:
        try:
            _directory(directory)
            payload = _read(directory / "batch.json", min(maximum, _MAX_BATCH_BYTES))
            digest = _read(directory / "sha256", 65)
            if digest != hashlib.sha256(payload).hexdigest().encode() + b"\n":
                raise CacheError("invalid_batch")
            batch = RawL2Batch.model_validate_json(payload)
            if batch.local_batch_sequence != expected:
                raise CacheError("invalid_batch")
            return batch
        except (OSError, ValueError):
            raise CacheError("invalid_batch") from None

    def _published_head(self, *, minimum: int = 0) -> tuple[int, datetime] | None:
        parent = self.root / "batches"
        if self.root.is_symlink() or parent.is_symlink():
            raise CacheError("unsafe_path")
        if not parent.exists():
            if minimum:
                raise CacheError("sequence_gap")
            return None
        _directory(self.root)
        _directory(parent)
        try:
            fd = os.open(parent / ".head", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        except OSError:
            raise CacheError("unsafe_path") from None
        try:
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_BATCH_BYTES
                        or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
                    raise CacheError("invalid_batch")
                payload = stream.read(_MAX_BATCH_BYTES + 1)
                if len(payload) > _MAX_BATCH_BYTES:
                    raise CacheError("invalid_batch")
                digest = hashlib.sha256(payload).hexdigest()
                key = (info.st_dev, info.st_ino, digest)
                # Only the validated sequence/time are reused. Concurrent first
                # readers must not build the same large head model in parallel.
                with self._head_lock:
                    receipt = self._head_receipt
                    if receipt is None or receipt[0] != key:
                        batch = RawL2Batch.model_validate_json(payload)
                        count, last = batch.local_batch_sequence, batch.received_at
                        del batch
                    else:
                        _, count, last = receipt
                    if count >= 10**20:
                        raise CacheError("invalid_batch")
                    directory = parent / f"{count:020d}"
                    _directory(directory)
                    actual = (directory / "batch.json").lstat()
                    if (not stat.S_ISREG(actual.st_mode)
                            or (actual.st_dev, actual.st_ino) != (info.st_dev, info.st_ino)
                            or _read(directory / "sha256", 65) != digest.encode() + b"\n"):
                        raise CacheError("invalid_batch")
                    if count < minimum:
                        raise CacheError("sequence_gap")
                    self._head_receipt = (key, count, last)
                    return count, last
        except (OSError, ValueError):
            raise CacheError("invalid_batch") from None

    def _publish_head(self, directory: Path) -> None:
        parent = self.root / "batches"
        temporary = parent / f".pending-head-{uuid.uuid4().hex}"
        created = False
        try:
            os.link(directory / "batch.json", temporary, follow_symlinks=False)
            created = True
            os.replace(temporary, parent / ".head")
        finally:
            if created:
                temporary.unlink(missing_ok=True)

    def _snapshot(self) -> tuple[FeedStatus, int, datetime | None]:
        status = self._stored_status()
        head = self._published_head(minimum=status.last_sequence)
        if head is not None:
            return status, *head
        # Legacy caches have no publication head. Audit every raw record before
        # exposing them; only a fully audited writer recovery may create .head.
        directories = self._directories()
        last = None
        for expected, directory in enumerate(directories, 1):
            last = self._read_batch(directory, expected).received_at
        return status, len(directories), last

    def _position(self) -> tuple[int, datetime | None]:
        if self._writer_fd is not None:
            return self._highwater, self._last_batch.received_at if self._last_batch else None
        _, count, last = self._snapshot()
        return count, last

    def append(self, observation: MarketObservation) -> RawL2Batch:
        self._require_writer()
        if not isinstance(observation, MarketObservation):
            raise ValueError("typed market observation required")
        batch = RawL2Batch.model_validate({
            **observation.model_dump(mode="python", serialize_as_any=True),
            "local_batch_sequence": self._highwater + 1,
            "cached_at": datetime.now(UTC),
        })
        payload = batch.model_dump_json().encode() + b"\n"
        if len(payload) > _MAX_BATCH_BYTES:
            raise CacheError("invalid_batch")
        try:
            space = os.statvfs(self.root)
            if (type(space.f_bavail) is not int or type(space.f_frsize) is not int
                    or space.f_bavail < 0 or space.f_frsize <= 0):
                raise CacheError("cache_io")
            block = space.f_frsize
            needed = 5 * 1024**3 + ((len(payload) + block - 1) // block
                      + (65 + block - 1) // block) * block + 1024**2
            if space.f_bavail * block < needed:
                raise CacheError("cache_io")
        except (OSError, AttributeError, TypeError, ValueError):
            raise CacheError("cache_io") from None
        pending: Path | None = None
        try:
            parent = self.root / "batches"
            pending = Path(tempfile.mkdtemp(prefix=".pending-", dir=parent))
            _write_new(pending / "batch.json", payload)
            _write_new(pending / "sha256", hashlib.sha256(payload).hexdigest().encode() + b"\n")
            _fsync_dir(pending)
            target = parent / f"{batch.local_batch_sequence:020d}"
            if target.exists():
                raise CacheError("sequence_gap")
            os.rename(pending, target)
            pending = None
            self._highwater = batch.local_batch_sequence
            self._last_batch = batch
            self._publish_head(target)
            _fsync_dir(parent)
            return batch
        except OSError:
            raise CacheError("cache_io") from None
        finally:
            if pending is not None:
                shutil.rmtree(pending, ignore_errors=True)

    def _stored_status(self) -> FeedStatus:
        path = self.root / "status.json"
        if self.root.is_symlink() or path.is_symlink():
            raise CacheError("unsafe_path")
        if not path.exists():
            return FeedStatus()
        try:
            return FeedStatus.model_validate_json(_read(path, 65536))
        except (OSError, ValueError):
            raise CacheError("invalid_status") from None

    @staticmethod
    def _with_committed_receipt(status: FeedStatus, count: int, last: datetime | None,
                                now: datetime) -> FeedStatus:
        age = max(0.0, (now - last).total_seconds()) if last else None
        return FeedStatus.model_validate({
            **status.model_dump(mode="python"),
            "last_market_received_at": last,
            "last_sequence": count, "market_batch_count": count,
            "cache_age_seconds": age,
            "cache_stale": (age is None or age > _STALE_SECONDS
                            or status.state in {"unconfigured", "disconnected", "blocked", "stopped"}),
        })

    def write_status(self, status: FeedStatus) -> None:
        self._require_writer()
        if not isinstance(status, FeedStatus):
            raise ValueError("typed feed status required")
        previous = self._stored_status()
        status = status.model_copy(update={"gap": status.gap or previous.gap})
        status = self._with_committed_receipt(status, *self._position(), datetime.now(UTC))
        _replace_snapshot(self.root, 'status.json', status.model_dump_json().encode() + b'\n')

    def read_runtime_status(self) -> FeedStatus:
        """Bounded writer snapshot for control routes; does not audit the batch prefix."""
        status = self._stored_status()
        last = status.last_market_received_at
        age = max(0.0, (datetime.now(UTC) - last).total_seconds()) if last else None
        return status.model_copy(update={
            'cache_age_seconds': age,
            'cache_stale': (age is None or age > _STALE_SECONDS or status.state in {
                'unconfigured', 'disconnected', 'blocked', 'stopped'}),
        })

    def write_subscription_status(self, receipt: SubscriptionReceipt) -> None:
        self._require_writer()
        if not isinstance(receipt, SubscriptionReceipt):
            raise ValueError('typed subscription receipt required')
        payload = receipt.model_dump_json().encode() + b'\n'
        if len(payload) > _MAX_RUNTIME_BYTES:
            raise CacheError('invalid_status')
        _replace_snapshot(self.root, 'subscription-status.json', payload)

    def read_subscription_status(self) -> SubscriptionReceipt | None:
        path = self.root / 'subscription-status.json'
        if self.root.is_symlink() or path.is_symlink():
            raise CacheError('unsafe_path')
        if not path.exists():
            return None
        try:
            info = path.lstat()
            if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                raise CacheError('unsafe_path')
            return SubscriptionReceipt.model_validate_json(_read(path, _MAX_RUNTIME_BYTES))
        except (OSError, ValueError):
            raise CacheError('invalid_status') from None

    def read_status(self, *, now: datetime | None = None) -> FeedStatus:
        clock = now if now is not None else datetime.now(UTC)
        if clock.tzinfo is None or clock.utcoffset() != datetime.now(UTC).utcoffset():
            raise ValueError("status clock must be aware UTC")
        if self._writer_fd is not None:
            status = self._stored_status()
            count, last = self._position()
        else:
            status, count, last = self._snapshot()
        return self._with_committed_receipt(status, count, last, clock)

    def read_batches(self, *, after_sequence: int = 0, limit: int = 1) -> BatchPage:
        if (type(after_sequence) is not int or after_sequence < 0 or type(limit) is not int
                or not 1 <= limit <= MAX_BATCH_PAGE):
            raise ValueError("invalid local batch cursor")
        stored_status, count, last = self._snapshot()
        if after_sequence > count:
            raise ValueError("invalid local batch cursor")
        selected_list = []
        payload_bytes = 0
        for sequence in range(after_sequence + 1, min(count, after_sequence + limit) + 1):
            path = self.root / 'batches' / f'{sequence:020d}'
            try:
                size = (path / 'batch.json').lstat().st_size
            except OSError:
                raise CacheError('invalid_batch') from None
            if size > _MAX_BATCH_BYTES:
                raise CacheError('invalid_batch')
            if payload_bytes + size > MAX_BATCH_PAGE_BYTES:
                break
            batch = self._read_batch(path, sequence, maximum=MAX_BATCH_PAGE_BYTES - payload_bytes)
            # Include canonical model bytes as well as committed payload bytes in
            # the budget, including noncanonical older JSON that expands on output.
            size = max(size, len(batch.model_dump_json().encode()) + 1)
            if payload_bytes + size > MAX_BATCH_PAGE_BYTES:
                break
            selected_list.append(batch)
            payload_bytes += size
        selected = tuple(selected_list)
        status = self._with_committed_receipt(stored_status, count, last, datetime.now(UTC))
        return BatchPage(batches=selected,
                         next_sequence=selected[-1].local_batch_sequence if selected else after_sequence,
                         status=status)
