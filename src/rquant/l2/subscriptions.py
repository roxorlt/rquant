"""Small durable additive target set; never reads member inputs or opens sockets."""
from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
from collections.abc import Sequence
from pathlib import Path

from .cache import _fsync_dir
from .models import CapacityPolicy, SubscriptionTargets, SubscriptionUpdate
from .symbols import MAX_SYMBOLS, normalize_symbols


class SubscriptionError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate field')
        result[key] = value
    return result


def _safe_directory(path: Path) -> None:
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise SubscriptionError('unsafe_control')


def _safe_file(path: Path, maximum: int = 65536) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > maximum):
            raise SubscriptionError('unsafe_control')
        value = stream.read(maximum + 1)
    if len(value) > maximum:
        raise SubscriptionError('invalid_control')
    return value


def load_capacity_policy(path: Path) -> CapacityPolicy:
    try:
        return CapacityPolicy.model_validate(json.loads(_safe_file(Path(path)), object_pairs_hook=_unique_object))
    except SubscriptionError:
        raise
    except (OSError, ValueError, TypeError):
        raise SubscriptionError('invalid_control') from None


class SubscriptionStore:
    """The existing parent owns the private control directory's durable entry."""
    def __init__(self, root: Path, *, max_symbols: int = 20) -> None:
        if type(max_symbols) is not int or not 1 <= max_symbols <= MAX_SYMBOLS:
            raise ValueError('invalid symbol capacity')
        self.root, self.max_symbols = Path(root), max_symbols

    def read(self) -> SubscriptionTargets:
        try:
            if not self.root.exists() and not self.root.is_symlink():
                return SubscriptionTargets(revision=0, symbols=(), max_symbols=self.max_symbols)
            _safe_directory(self.root)
            path = self.root / 'desired.json'
            if not path.exists() and not path.is_symlink():
                return SubscriptionTargets(revision=0, symbols=(), max_symbols=self.max_symbols)
            values = json.loads(_safe_file(path), object_pairs_hook=_unique_object)
            if not isinstance(values, dict) or set(values) != {'revision', 'symbols'}:
                raise ValueError
            if len(values['symbols']) > self.max_symbols:
                raise SubscriptionError('capacity_exceeded')
            return SubscriptionTargets.model_validate({**values, 'max_symbols': self.max_symbols})
        except SubscriptionError:
            raise
        except (OSError, ValueError, TypeError):
            raise SubscriptionError('invalid_control') from None

    def add(self, symbols: Sequence[str]) -> SubscriptionUpdate:
        try:
            incoming = normalize_symbols(symbols)
        except ValueError:
            raise SubscriptionError('invalid_symbols') from None
        fd = None
        temporary = None
        try:
            self.root.mkdir(mode=0o700, exist_ok=True)
            _safe_directory(self.root)
            _fsync_dir(self.root.parent)
            fd = os.open(self.root / '.subscriptions.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise SubscriptionError('unsafe_control')
            fcntl.flock(fd, fcntl.LOCK_EX)
            current = self.read()
            added = tuple(s for s in incoming if s not in current.symbols)
            union = current.symbols + added
            if len(union) > self.max_symbols:
                raise SubscriptionError('capacity_exceeded')
            if not added:
                return SubscriptionUpdate(**current.model_dump(), added=())
            update = SubscriptionUpdate(revision=current.revision + 1, symbols=union,
                                        max_symbols=self.max_symbols, added=added)
            payload = json.dumps({'revision': update.revision, 'symbols': union}, separators=(',', ':')).encode() + b'\n'
            out, name = tempfile.mkstemp(prefix='.desired-', dir=self.root)
            temporary = Path(name)
            with os.fdopen(out, 'wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.root / 'desired.json')
            temporary = None
            _fsync_dir(self.root)
            return update
        except SubscriptionError:
            raise
        except OSError:
            raise SubscriptionError('control_io') from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            if fd is not None:
                os.close(fd)
