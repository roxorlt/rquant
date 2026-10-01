"""Run one already submitted factor evaluation through its dedicated ledger."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Literal

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.job_ledger import (
    FactorEvaluationJobLedger,
    FactorJobFailureCode,
    FactorJobRecord,
)
from rquant.factor.job_runner import FactorEvaluationCompletion, run_factor_evaluation_job
from rquant.factor.stream_job_runner import FactorStreamCompletion, run_factor_stream_job
from rquant.factor.stream_job_spec import FactorStreamJobSpec
from rquant.factor_snapshot_admission import (
    FactorSnapshotAdmissionError,
    FactorSnapshotMetadataStore,
)

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


class FactorJobWorkerResult(BaseModel):
    """One durable terminal receipt or an explicit absence of terminal authority."""

    model_config = _IMMUTABLE

    status: Literal["idle", "succeeded", "failed", "lease_lost"]
    job_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    record: FactorJobRecord | None = None

    @model_validator(mode="after")
    def _valid_result(self) -> FactorJobWorkerResult:
        if self.status == "idle":
            valid = self.job_id is None and self.record is None
        elif self.status == "lease_lost":
            valid = self.job_id is not None and self.record is None
        else:
            valid = (
                self.job_id is not None
                and self.record is not None
                and self.record.job_id == self.job_id
                and self.record.status == self.status
            )
        if not valid:
            raise ValueError("factor worker result lacks matching durable evidence")
        return self


def _bounded_timing(
    lease_seconds: int, heartbeat_interval_seconds: float, heartbeat_join_timeout_seconds: float
) -> None:
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
        raise ValueError("factor worker lease duration is invalid")
    if (
        isinstance(heartbeat_interval_seconds, bool)
        or not isinstance(heartbeat_interval_seconds, (int, float))
        or not math.isfinite(heartbeat_interval_seconds)
        or not 0.01 <= heartbeat_interval_seconds <= min(30, lease_seconds / 3)
    ):
        raise ValueError("factor worker heartbeat interval is invalid")
    if (
        isinstance(heartbeat_join_timeout_seconds, bool)
        or not isinstance(heartbeat_join_timeout_seconds, (int, float))
        or not math.isfinite(heartbeat_join_timeout_seconds)
        or not 0.01 <= heartbeat_join_timeout_seconds <= 30
    ):
        raise ValueError("factor worker heartbeat join timeout is invalid")


def _failure_code(error: Exception) -> FactorJobFailureCode:
    if isinstance(error, FactorSnapshotAdmissionError):
        return "source_unavailable"
    if isinstance(error, TimeoutError):
        return "evaluation_failed"
    if isinstance(error, OSError):
        return "internal_error"
    return "evaluation_failed"


def run_one_factor_job(
    ledger: FactorEvaluationJobLedger,
    *,
    metadata_store: FactorSnapshotMetadataStore,
    lake_root: Path,
    artifact_root: Path,
    runner_now: Callable[[], datetime],
    member_root: Path | None = None,
    lease_seconds: int = 30,
    heartbeat_interval_seconds: float = 5.0,
    heartbeat_join_timeout_seconds: float = 5.0,
) -> FactorJobWorkerResult:
    """Take at most one factor job; never create or repair its ledger."""
    _bounded_timing(lease_seconds, heartbeat_interval_seconds, heartbeat_join_timeout_seconds)
    if not callable(runner_now):
        raise TypeError("factor worker requires a runner clock")
    claimed = ledger.claim(lease_seconds=lease_seconds)
    if claimed is None:
        return FactorJobWorkerResult(status="idle")

    latest = claimed
    stop = Event()
    lost = Event()
    lock = Lock()

    def heartbeat() -> None:
        nonlocal latest
        while not stop.wait(heartbeat_interval_seconds):
            with lock:
                if stop.is_set():
                    return
                try:
                    latest = ledger.heartbeat(
                        claimed.job.job_id,
                        latest.lease_token,
                        latest.version,
                        lease_seconds,
                    )
                except BaseException as exc:
                    lost.set()
                    stop.set()
                    logger.opt(exception=exc).error("factor evaluation heartbeat failed")
                    return

    thread = Thread(target=heartbeat, name="factor-job-heartbeat", daemon=True)
    try:
        thread.start()
    except RuntimeError as exc:
        logger.opt(exception=exc).error("factor evaluation heartbeat could not start")
        return FactorJobWorkerResult(status="lease_lost", job_id=claimed.job.job_id)

    completion: FactorEvaluationCompletion | FactorStreamCompletion | None = None
    prepared: object | None = None
    runner_error: Exception | None = None
    try:
        try:
            if isinstance(claimed.job.spec, FactorStreamJobSpec):
                if member_root is None:
                    raise ValueError("stream job worker requires its member archive root")
                completion = run_factor_stream_job(
                    claimed.job.spec,
                    metadata_store=metadata_store,
                    lake_root=lake_root,
                    artifact_root=artifact_root,
                    member_root=member_root,
                    now=runner_now,
                )
                prepared = ledger.prepare_stream_completion(
                    claimed.job.job_id, claimed.lease_token, completion, artifact_root, member_root
                )
            else:
                completion = run_factor_evaluation_job(
                    claimed.job.spec,
                    metadata_store=metadata_store,
                    lake_root=lake_root,
                    artifact_root=artifact_root,
                    now=runner_now,
                )
        except Exception as exc:
            runner_error = exc
            logger.opt(exception=exc).error("factor evaluation execution failed")
        finally:
            stop.set()
            thread.join(timeout=heartbeat_join_timeout_seconds)

        if thread.is_alive() or lost.is_set():
            return FactorJobWorkerResult(status="lease_lost", job_id=claimed.job.job_id)
        with lock:
            if runner_error is None:
                try:
                    if prepared is not None:
                        record = ledger.complete_prepared_stream(
                            claimed.job.job_id, latest.lease_token, latest.version, prepared
                        )
                    else:
                        record = ledger.complete(
                            claimed.job.job_id,
                            latest.lease_token,
                            latest.version,
                            completion,
                            artifact_root,
                        )
                except Exception as exc:
                    logger.opt(exception=exc).error(
                        "factor evaluation completion was not committed"
                    )
                    return FactorJobWorkerResult(status="lease_lost", job_id=claimed.job.job_id)
                return FactorJobWorkerResult(
                    status="succeeded", job_id=claimed.job.job_id, record=record
                )
            try:
                record = ledger.fail(
                    claimed.job.job_id,
                    latest.lease_token,
                    latest.version,
                    _failure_code(runner_error),
                )
            except Exception as exc:
                logger.opt(exception=exc).error("factor evaluation failure was not committed")
                return FactorJobWorkerResult(status="lease_lost", job_id=claimed.job.job_id)
            return FactorJobWorkerResult(status="failed", job_id=claimed.job.job_id, record=record)
    finally:
        ledger._discard_job_prepared(claimed.job.job_id)
