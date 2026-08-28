"""Whole-ledger Lab audit executed in a killable child process.

The scheduler must never block its writer lease behind an unbounded audit and
must never accumulate abandoned daemon threads.  This module runs the full
graph audit in a separate interpreter: the parent enforces a hard wall-clock
timeout, kills and reaps the child on expiry, and validates the returned
generation-bound receipt.  Binding the receipt into the external high-water
authority stays in the parent so the child needs no authority credentials.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from rquant.lab_jobs import LabGraphIntegrityReceipt, LabJobReader
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_json,
)

_MAX_RESPONSE_BYTES = 1024 * 1024
_AUDIT_FAILURE_EXIT_CODE = 2


class LabFullAuditTimeoutError(RuntimeError):
    """The child audit exceeded its hard budget and was killed."""


class LabFullAuditChildError(RuntimeError):
    """The child audit failed or returned an invalid receipt."""


class LabFullAuditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1, le=1)
    jobs_path: Path
    busy_timeout_ms: int = Field(default=5_000, ge=1)


def run_child_audit(payload: bytes) -> bytes:
    """Child-side entry: audit the ledger named by the request, return receipt bytes."""

    request = strict_model_validate_json(LabFullAuditRequest, payload)
    reader = LabJobReader(
        Path(request.jobs_path),
        busy_timeout_ms=request.busy_timeout_ms,
    )
    receipt = reader.audit_integrity()
    return canonical_model_json_bytes(receipt)


def main() -> int:
    payload = sys.stdin.buffer.read(_MAX_RESPONSE_BYTES + 1)
    try:
        response = run_child_audit(payload)
    except Exception as exc:
        message = " ".join((str(exc) or type(exc).__name__).split())[:400]
        print(f"{type(exc).__name__}: {message}", file=sys.stderr)
        return _AUDIT_FAILURE_EXIT_CODE
    sys.stdout.buffer.write(response)
    return 0


@dataclass(frozen=True)
class LabFullAuditChildRunner:
    """Run the full ledger audit in a killable child with a hard timeout.

    ``subprocess.run`` kills and reaps the child when the timeout expires, so a
    hung audit can neither pile up daemon threads nor leave zombie processes.
    """

    jobs_path: Path
    busy_timeout_ms: int = 5_000
    timeout_seconds: float = 120.0
    python_executable: str = sys.executable

    def __post_init__(self) -> None:
        if not 0.1 <= self.timeout_seconds <= 3_600:
            raise ValueError("full audit timeout_seconds is outside the safe range")

    def audit_integrity(self) -> LabGraphIntegrityReceipt:
        request = LabFullAuditRequest(
            jobs_path=Path(self.jobs_path),
            busy_timeout_ms=self.busy_timeout_ms,
        )
        try:
            result = subprocess.run(
                [self.python_executable, "-m", "rquant.lab_full_audit"],
                input=canonical_json_bytes(request.model_dump(mode="json")),
                capture_output=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise LabFullAuditTimeoutError(
                "full ledger audit exceeded its resource budget and was killed"
            ) from exc
        except OSError as exc:
            raise LabFullAuditChildError("full ledger audit child is unavailable") from exc
        if result.returncode != 0 or not result.stdout:
            detail = " ".join(result.stderr.decode("utf-8", "replace").split())[:400]
            raise LabFullAuditChildError(f"full ledger audit degraded: {detail}")
        if len(result.stdout) > _MAX_RESPONSE_BYTES:
            raise LabFullAuditChildError("full ledger audit receipt size is unsafe")
        try:
            receipt = strict_model_validate_json(LabGraphIntegrityReceipt, result.stdout)
        except Exception as exc:
            raise LabFullAuditChildError("full ledger audit returned an invalid receipt") from exc
        return receipt


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "LabFullAuditChildError",
    "LabFullAuditChildRunner",
    "LabFullAuditRequest",
    "LabFullAuditTimeoutError",
    "main",
    "run_child_audit",
]
