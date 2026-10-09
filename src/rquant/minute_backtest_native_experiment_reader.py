"""Readonly native-family bridge to the original complete minute result reader."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from rquant.experiment_platform import ExperimentPreparationReceipt
from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_jobs import LabJobReader, LabJobRecord
from rquant.minute_backtest_artifact import (
    MinuteSealedReplayIntegrityError,
    MinuteSealedReplayReader,
    MinuteSealedReplayResult,
)
from rquant.minute_backtest_contracts import Sha256
from rquant.minute_backtest_formal import PreparedMinuteRequest
from rquant.runtime_contracts import normalize_aware_utc


class MinuteNativeExperimentReplayReader:
    """Use actual private preparation; current roles remain at the trusted caller."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        artifact_reader: ArtifactPreviewReader,
        submission_facade: LabCommandSubmissionFacade,
        private_authority: ExperimentPrivateResultAuthority,
        clock: Callable[[], datetime],
    ) -> None:
        self.reader = reader
        self.artifact_reader = artifact_reader
        self.submission_facade = submission_facade
        self.private_authority = private_authority
        self.clock = clock
        self._registry = private_authority.registry
        self._require_binding()
        if not callable(clock):
            raise TypeError("native experiment reader requires an explicit clock")

    def _require_binding(self) -> None:
        if (
            self.artifact_reader.reader is not self.reader
            or self.submission_facade.reader is not self.reader
            or self.private_authority.registry is not self._registry
            or self.submission_facade.experiment_registry is not self._registry
            or self.submission_facade.definition_registry is None
        ):
            raise ValueError("native experiment reader requires the same original Lab/Registry")

    def _capture(
        self,
        job_id: UUID,
        owner_id: str,
    ) -> tuple[LabJobRecord, ExperimentPreparationReceipt]:
        self._require_binding()
        job = self.reader.get_job(job_id)
        if job is None:
            raise LookupError("native experiment job is unavailable")
        preparation = self.private_authority.authorize(job, owner_id)
        if (
            type(preparation.prepared) is not PreparedMinuteRequest
            or job.spec.parameters.strategy_name != "minute_runtime_replay"
        ):
            raise PermissionError("native experiment reader only accepts original native families")
        return job, preparation

    def job(self, job_id: UUID, *, owner_id: str) -> LabJobRecord:
        job, preparation = self._capture(job_id, owner_id)
        if self._capture(job_id, owner_id) != (job, preparation):
            raise PermissionError("native experiment job/preparation changed during authorization")
        return job

    def read(
        self,
        job_id: UUID,
        *,
        owner_id: str,
        expected_spec_hash: Sha256,
        expected_result_hash: Sha256 | None = None,
        as_of: datetime,
    ) -> MinuteSealedReplayResult | None:
        visible_at = normalize_aware_utc(as_of)
        if visible_at > normalize_aware_utc(self.clock()):
            raise ValueError("native experiment read time cannot be in the future")
        job, preparation = self._capture(job_id, owner_id)
        if job.spec_hash != expected_spec_hash:
            raise PermissionError("native experiment accepted spec differs from the request")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None:
            return None
        if authority.job != job:
            raise MinuteSealedReplayIntegrityError("native experiment sealed job identity changed")
        if authority.evidence.indexed_at > visible_at or job.updated_at > visible_at:
            return None
        if (
            expected_result_hash is not None
            and authority.evidence.complete_result_hash != expected_result_hash
        ):
            raise PermissionError("native experiment sealed result differs from the request")
        prepared = preparation.prepared
        native = prepared.frozen.runtime.strategy
        full_reader = MinuteSealedReplayReader(
            reader=self.reader,
            artifact_reader=self.artifact_reader,
            submission_facade=self.submission_facade,
            catalog=prepared.catalog,
        )
        result = full_reader.read(
            job_id,
            owner_id=owner_id,
            native_id=native.strategy_id,
            native_version=native.strategy_version,
            as_of=visible_at,
        )
        if (
            self._capture(job_id, owner_id) != (job, preparation)
            or self.reader.get_artifact_preview_authority(job_id) != authority
        ):
            raise MinuteSealedReplayIntegrityError(
                "native experiment authority changed during full read"
            )
        if result is None:
            return None
        # Public result hashes identify the physical complete result, not the
        # separate canonical hash of its decoded replay model.
        if type(result) is not MinuteSealedReplayResult or (
            result.job_id != job_id
            or result.owner_id != owner_id
            or result.spec_hash != job.spec_hash
            or result.accepted_spec != job.spec
            or result.formal_plan != prepared.formal_plan
            or result.manifest_hash != authority.evidence.manifest_hash
            or result.complete_result_hash != authority.evidence.complete_result_hash
            or result.completed_at != authority.evidence.indexed_at
            or result.result.publication != prepared.published.receipt
        ):
            raise MinuteSealedReplayIntegrityError(
                "native experiment complete result binding differs"
            )
        return result
