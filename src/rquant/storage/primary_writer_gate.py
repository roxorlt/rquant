"""One configured physical-primary lock shared by writers and snapshot readers."""
from __future__ import annotations

import fcntl
import os
import stat
from pathlib import Path
from types import TracebackType

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PrimaryWriterIdentityError(RuntimeError):
    """The installed primary or lock identity changed."""


class PrimaryWriterBusy(RuntimeError):
    """Another participating writer owns the physical primary."""


def _canonical(path: Path) -> Path:
    value = Path(path)
    if not value.is_absolute() or value != value.resolve(strict=True):
        raise PrimaryWriterIdentityError('writer paths must be canonical and present')
    return value


class PrimaryWriterGateConfig(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    primary_path: Path
    primary_device: int = Field(strict=True, ge=0)
    primary_inode: int = Field(strict=True, gt=0)
    lock_path: Path
    lock_device: int = Field(strict=True, ge=0)
    lock_inode: int = Field(strict=True, gt=0)
    parent_device: int = Field(strict=True, ge=0)
    parent_inode: int = Field(strict=True, gt=0)
    owner_uid: int = Field(strict=True, ge=0)

    @model_validator(mode='after')
    def distinct_paths(self) -> PrimaryWriterGateConfig:
        if self.primary_path == self.lock_path:
            raise ValueError('primary and writer lock must be distinct')
        return self

    @classmethod
    def capture(cls, *, primary_path: Path, lock_path: Path) -> PrimaryWriterGateConfig:
        """Inspect already installed files. This does not create a lock or enable a policy."""
        primary, lock = _canonical(primary_path), _canonical(lock_path)
        p, l, parent = primary.stat(follow_symlinks=False), lock.stat(follow_symlinks=False), lock.parent.stat()
        if not stat.S_ISREG(p.st_mode) or not stat.S_ISREG(l.st_mode):
            raise PrimaryWriterIdentityError('primary and writer lock must be regular files')
        if l.st_nlink != 1 or stat.S_IMODE(l.st_mode) != 0o600 or l.st_uid != os.geteuid():
            raise PrimaryWriterIdentityError('writer lock requires owner, mode 0600 and one link')
        return cls(primary_path=primary,primary_device=p.st_dev,primary_inode=p.st_ino,
                   lock_path=lock,lock_device=l.st_dev,lock_inode=l.st_ino,
                   parent_device=parent.st_dev,parent_inode=parent.st_ino,owner_uid=l.st_uid)


class PrimaryWriterLease:
    def __init__(self, config: PrimaryWriterGateConfig, descriptor: int) -> None:
        self.config = config
        self._descriptor: int | None = descriptor

    def verify(self, primary_path: Path | None = None) -> None:
        if self._descriptor is None:
            raise PrimaryWriterIdentityError('writer lease is closed')
        if primary_path is not None and Path(primary_path) != self.config.primary_path:
            raise PrimaryWriterIdentityError('writer lease belongs to another primary path')
        PrimaryWriterGate(self.config)._verify(os.fstat(self._descriptor))

    def close(self) -> None:
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is not None:
            os.close(descriptor)

    def __enter__(self) -> PrimaryWriterLease:
        self.verify()
        return self

    def __exit__(self, _type: type[BaseException] | None, _value: BaseException | None,
                 _traceback: TracebackType | None) -> None:
        self.close()


class PrimaryWriterGate:
    def __init__(self, config: PrimaryWriterGateConfig) -> None:
        self.config = config

    def _verify(self, opened: os.stat_result) -> None:
        c = self.config
        try:
            if _canonical(c.primary_path) != c.primary_path or _canonical(c.lock_path) != c.lock_path:
                raise PrimaryWriterIdentityError('writer path changed')
            primary = c.primary_path.stat(follow_symlinks=False)
            current = c.lock_path.stat(follow_symlinks=False)
            parent = c.lock_path.parent.stat(follow_symlinks=False)
            if (primary.st_dev,primary.st_ino) != (c.primary_device,c.primary_inode):
                raise PrimaryWriterIdentityError('primary file identity changed')
            if (parent.st_dev,parent.st_ino) != (c.parent_device,c.parent_inode):
                raise PrimaryWriterIdentityError('writer lock parent identity changed')
            for item in (current,opened):
                if (not stat.S_ISREG(item.st_mode) or item.st_nlink != 1 or
                    stat.S_IMODE(item.st_mode) != 0o600 or item.st_uid != c.owner_uid or
                    (item.st_dev,item.st_ino) != (c.lock_device,c.lock_inode)):
                    raise PrimaryWriterIdentityError('writer lock identity or permissions changed')
            if c.owner_uid != os.geteuid():
                raise PrimaryWriterIdentityError('writer lock owner differs from process')
        except OSError as error:
            raise PrimaryWriterIdentityError('writer files unavailable') from error

    def acquire(self) -> PrimaryWriterLease:
        c = self.config
        parent_fd: int | None = None
        descriptor: int | None = None
        try:
            parent_fd = os.open(c.lock_path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            parent = os.fstat(parent_fd)
            if (parent.st_dev,parent.st_ino) != (c.parent_device,c.parent_inode):
                raise PrimaryWriterIdentityError('writer lock parent changed')
            descriptor = os.open(c.lock_path.name,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent_fd)
            self._verify(os.fstat(descriptor))
            try:
                fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise PrimaryWriterBusy('primary writer is occupied') from error
            self._verify(os.fstat(descriptor))
            result = PrimaryWriterLease(c,descriptor)
            descriptor = None
            return result
        except OSError as error:
            raise PrimaryWriterIdentityError('writer lock unavailable') from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if parent_fd is not None:
                os.close(parent_fd)


def configured_primary_gate(path: Path, config_path: Path | None) -> PrimaryWriterGateConfig | None:
    if config_path is None:
        return None
    configured = Path(config_path)
    if configured.is_symlink() or configured.stat().st_size > 16_384:
        raise PrimaryWriterIdentityError('invalid writer profile')
    config = PrimaryWriterGateConfig.model_validate_json(configured.read_bytes())
    requested = Path(path)
    if requested == config.primary_path:
        return config
    if requested.exists() and os.path.samefile(requested,config.primary_path):
        raise PrimaryWriterIdentityError('primary path alias cannot bypass writer gate')
    return None
