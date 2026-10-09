"""One bounded reconciliation operation on the original research runtime loop."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Annotated

from pydantic import StringConstraints, field_validator

from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)


class MinuteStudyProjectionSettings(RuntimeContractModel):
    installation_path: Path
    expected_code_sha: Annotated[str, StringConstraints(
        pattern=r"^[0-9a-f]{40}$", strict=True, strip_whitespace=False)]
    projection_authority_path: Path
    projection_expected_sha256: Annotated[str, StringConstraints(
        pattern=r"^[0-9a-f]{64}$", strict=True, strip_whitespace=False)]

    @field_validator("installation_path", "projection_authority_path", mode="before")
    @classmethod
    def canonical_absolute_path(cls, value: object) -> Path:
        if not isinstance(value, (str, Path)):
            raise ValueError("minute projection paths must be canonical absolute paths")
        path = Path(value)
        if (not path.is_absolute() or path.anchor != "/" or "\x00" in str(path)
            or path != Path(os.path.abspath(path))
            or isinstance(value, str) and value != str(path)):
            raise ValueError("minute projection paths must be canonical absolute paths")
        # Physical path and no-symlink proof belongs to the original loaders at each step.
        return path


class _MinuteStudyProjectionStep:
    def __init__(self, settings: MinuteStudyProjectionSettings, clock: Callable[[], datetime]) -> None:
        self._settings, self._clock, self._closed = settings, clock, False

    def close(self) -> None:
        self._closed = True

    @staticmethod
    def _check_stop() -> None:
        if read_interrupt_requested():
            raise ReadInterruptedError("minute study projection read stopped")

    def __call__(self) -> RuntimeStepResult:
        if self._closed:
            raise RuntimeError("minute study projection step is closed")
        self._check_stop()
        now = normalize_aware_utc(self._clock())
        from rquant.minute_backtest_installation import load_minute_replay_installation
        from rquant.minute_backtest_parameter_study_projection import (
            load_minute_study_projection,
        )

        settings = self._settings
        installation = load_minute_replay_installation(settings.installation_path,
            expected_code_sha=settings.expected_code_sha, writable=False, clock=self._clock)
        self._check_stop()
        with load_minute_study_projection(installation, settings.projection_authority_path,
            expected_sha256=settings.projection_expected_sha256, writable=True) as projection:
            self._check_stop()
            receipt = projection.reconcile_one(as_of=now)
            self._check_stop()
            return RuntimeStepResult(input_sequence=receipt.sequence, output_sequence=receipt.sequence,
                processed_count=int(receipt.published), backlog_count=receipt.pending_jobs,
                projection_published=receipt.published)


def minute_study_projection_builder(*, clock: Callable[[], datetime]) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.MINUTE_STUDY_PROJECTION:
            raise ValueError("runtime service kind must be minute_study_projection")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("minute study projection must run on the research plane")
        settings = MinuteStudyProjectionSettings.model_validate(dict(manifest.settings))
        if settings.expected_code_sha != manifest.producer_commit:
            raise ValueError("minute projection installed code must match manifest producer commit")
        return _MinuteStudyProjectionStep(settings, clock)

    return build
