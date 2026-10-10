"""Owner-private, explicit local source and task paths for formula market jobs."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from pydantic import field_validator, model_validator

from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import strict_canonical_json_loads

_MAX_CONFIG_BYTES = 8192


def _config_file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


class FormulaMarketPrivateConfig(RuntimeContractModel):
    """Four trusted local paths shared by command admission and offline execution."""

    universe_root: Path
    projection_root: Path
    state_path: Path
    artifact_directory: Path

    @field_validator("universe_root", "projection_root", "state_path", "artifact_directory")
    @classmethod
    def require_canonical_absolute_path(cls, path: Path) -> Path:
        if not path.is_absolute() or path != Path(os.path.abspath(path)):
            raise ValueError("formula task paths must be absolute and canonical")
        if path.resolve(strict=False) != path:
            raise ValueError("formula task paths must not follow symbolic links")
        return path

    @model_validator(mode="after")
    def validate_distinct_paths(self) -> FormulaMarketPrivateConfig:
        roots = (self.universe_root, self.projection_root, self.artifact_directory)
        if len(set(roots)) != len(roots) or self.state_path.parent in roots:
            raise ValueError("formula task sources and state must use distinct directories")
        return self


def load_private_formula_market_config(path: Path) -> FormulaMarketPrivateConfig:
    """Load an explicit owner-private config, rejecting links, swaps and ambiguous JSON."""
    candidate = Path(path)
    if (
        not candidate.is_absolute()
        or candidate != Path(os.path.abspath(candidate))
        or candidate.resolve(strict=False) != candidate
    ):
        raise ValueError("formula market config path must be absolute and canonical")
    try:
        before = candidate.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_CONFIG_BYTES
        ):
            raise ValueError("formula market config must be an owner-private regular file")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(candidate, flags)
        try:
            if _config_file_identity(os.fstat(descriptor)) != _config_file_identity(before):
                raise ValueError("formula market config changed while opening")
            payload = os.read(descriptor, _MAX_CONFIG_BYTES + 1)
            if (
                len(payload) != before.st_size
                or _config_file_identity(os.fstat(descriptor)) != _config_file_identity(before)
                or _config_file_identity(candidate.lstat()) != _config_file_identity(before)
            ):
                raise ValueError("formula market config changed while reading")
        finally:
            os.close(descriptor)
        return FormulaMarketPrivateConfig.model_validate(strict_canonical_json_loads(payload))
    except OSError as error:
        raise ValueError("formula market config is unavailable or unsafe") from error
