"""Private immutable observation batches; directory rename is the commit point."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from rquant.l2.models import BatchPage, FeedStatus, MarketObservation, RawL2Batch

_ACTIVE = {"starting", "connected", "seeded_waiting", "listening_candidate"}
_STALE_SECONDS = 60
_MAX_BATCH_BYTES = 8 * 1024 * 1024


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
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
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


class RawL2Cache:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._writer_fd: int | None = None
        self._highwater = 0
        self._last_batch: RawL2Batch | None = None

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
            directories = sorted(p for p in path.iterdir() if not p.name.startswith(".pending-"))
            for expected, directory in enumerate(directories, 1):
                if not re.fullmatch(r"[0-9]{20}", directory.name):
                    raise CacheError("invalid_cache")
                if int(directory.name) != expected:
                    raise CacheError("sequence_gap")
                _directory(directory)
                try:
                    if (not stat.S_ISREG((directory / "batch.json").lstat().st_mode)
                            or not stat.S_ISREG((directory / "sha256").lstat().st_mode)):
                        raise CacheError("invalid_batch")
                except OSError:
                    raise CacheError("invalid_batch") from None
            if minimum_committed > len(directories):
                raise CacheError("sequence_gap")
            return tuple(directories)
        except OSError:
            raise CacheError("cache_io") from None

    @staticmethod
    def _read_batch(directory: Path, expected: int) -> RawL2Batch:
        try:
            payload = _read(directory / "batch.json", _MAX_BATCH_BYTES)
            digest = _read(directory / "sha256", 65)
            if digest != hashlib.sha256(payload).hexdigest().encode() + b"\n":
                raise CacheError("invalid_batch")
            batch = RawL2Batch.model_validate_json(payload)
            if batch.local_batch_sequence != expected:
                raise CacheError("invalid_batch")
            return batch
        except (OSError, ValueError):
            raise CacheError("invalid_batch") from None

    def _position(self) -> tuple[int, RawL2Batch | None]:
        if self._writer_fd is not None:
            return self._highwater, self._last_batch
        directories = self._directories()
        count = len(directories)
        return count, self._read_batch(directories[-1], count) if count else None

    def append(self, observation: MarketObservation) -> RawL2Batch:
        self._require_writer()
        if not isinstance(observation, MarketObservation):
            raise ValueError("typed market observation required")
        batch = RawL2Batch.model_validate({
            **observation.model_dump(mode="python"),
            "local_batch_sequence": self._highwater + 1,
            "cached_at": datetime.now(UTC),
        })
        payload = batch.model_dump_json().encode() + b"\n"
        if len(payload) > _MAX_BATCH_BYTES:
            raise CacheError("invalid_batch")
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
    def _with_committed_receipt(status: FeedStatus, count: int, last_batch: RawL2Batch | None,
                                now: datetime) -> FeedStatus:
        last = last_batch.received_at if last_batch else None
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
        temporary: Path | None = None
        try:
            fd, name = tempfile.mkstemp(prefix=".status-", dir=self.root)
            temporary = Path(name)
            with os.fdopen(fd, "wb") as stream:
                stream.write(status.model_dump_json().encode() + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.root / "status.json")
            temporary = None
            _fsync_dir(self.root)
        except OSError:
            raise CacheError("cache_io") from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def read_status(self, *, now: datetime | None = None) -> FeedStatus:
        clock = now if now is not None else datetime.now(UTC)
        if clock.tzinfo is None or clock.utcoffset() != datetime.now(UTC).utcoffset():
            raise ValueError("status clock must be aware UTC")
        return self._with_committed_receipt(self._stored_status(), *self._position(), clock)

    def read_batches(self, *, after_sequence: int = 0, limit: int = 1) -> BatchPage:
        if (type(after_sequence) is not int or after_sequence < 0 or type(limit) is not int
                or not 1 <= limit <= 8):
            raise ValueError("invalid local batch cursor")
        directories = self._directories()
        count = len(directories)
        if after_sequence > count:
            raise ValueError("invalid local batch cursor")
        selected = tuple(self._read_batch(path, after_sequence + index + 1)
                         for index, path in enumerate(directories[after_sequence:after_sequence + limit]))
        if selected and selected[-1].local_batch_sequence == count:
            last = selected[-1]
        else:
            last = self._read_batch(directories[-1], count) if count else None
        status = self._with_committed_receipt(self._stored_status(), count, last, datetime.now(UTC))
        return BatchPage(batches=selected,
                         next_sequence=selected[-1].local_batch_sequence if selected else after_sequence,
                         status=status)
