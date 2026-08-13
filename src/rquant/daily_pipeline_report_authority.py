"""Independent monotonic authority for immutable daily-DAG completion reports."""

from __future__ import annotations

import json
import os
import stat
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self
from uuid import uuid4

from pydantic import Field, StringConstraints, model_validator

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageBinding,
    DailyPipelineStorageProfile,
)
from rquant.lab_highwater_authority import (
    PRODUCTION_LAB_HIGHWATER_COMMAND,
    LabHighWaterAuthorityClient,
    LabHighWaterAuthorityConfig,
    LabHighWaterAuthorityError,
    load_highwater_trusted_keys,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
RunId = Annotated[str, StringConstraints(pattern=r"^daily-[a-z0-9_-]{1,121}$")]
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_MAX_REPORT_BYTES = 1024 * 1024
_RUNNER_OWNED_AUTHORITY_ENV = (
    "RQUANT_DAILY_REPORT_AUTHORITY_COMMAND",
    "RQUANT_DAILY_REPORT_AUTHORITY_COMMAND_JSON",
    "RQUANT_DAILY_REPORT_AUTHORITY_ARGV",
    "RQUANT_DAILY_REPORT_AUTHORITY_ARGUMENTS",
    "RQUANT_DAILY_REPORT_AUTHORITY_ROOT",
    "RQUANT_DAILY_REPORT_AUTHORITY_STATE_ROOT",
    "RQUANT_DAILY_REPORT_AUTHORITY_KEYS_FILE",
    "RQUANT_DAILY_REPORT_AUTHORITY_SECRET",
)


class DailyPipelineReportAuthorityError(RuntimeError):
    """Daily completion evidence cannot prove independent monotonic ordering."""


class DailyPipelineRunReport(RuntimeContractModel):
    """Content-addressed completion evidence for one immutable daily run."""

    contract: Literal["daily-pipeline-run-report/v2"] = "daily-pipeline-run-report/v2"
    report_id: Sha256 | None = None
    mode: DailyPipelineMode
    profile_hash: Sha256
    namespace_id: Sha256
    run_id: RunId
    plan_hash: Sha256
    trade_date: date
    receipt_ids: tuple[Sha256, ...] = Field(min_length=1, max_length=64)
    generated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def bind_report_identity(self) -> Self:
        if tuple(sorted(set(self.receipt_ids))) != self.receipt_ids:
            raise ValueError("daily pipeline report receipt ids must be sorted and unique")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"report_id"}))
        if self.report_id is None:
            object.__setattr__(self, "report_id", expected)
        elif self.report_id != expected:
            raise ValueError("daily pipeline report id does not match canonical content")
        return self

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        mode: DailyPipelineMode,
        profile_hash: str,
        namespace_id: str,
        plan_hash: str,
        trade_date: date,
        receipt_ids: tuple[str, ...],
        generated_at: datetime,
    ) -> DailyPipelineRunReport:
        return cls(
            mode=mode,
            profile_hash=profile_hash,
            namespace_id=namespace_id,
            run_id=run_id,
            plan_hash=plan_hash,
            trade_date=trade_date,
            receipt_ids=tuple(sorted(receipt_ids)),
            generated_at=generated_at,
        )


def _canonical_bytes(report: DailyPipelineRunReport) -> bytes:
    return (
        json.dumps(
            report.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
    )


def _read_bound_file(directory_fd: int, name: str, *, label: str) -> bytes:
    descriptor = -1
    try:
        before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > _MAX_REPORT_BYTES
        ):
            raise DailyPipelineReportAuthorityError(f"{label} is unsafe")
        descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
        opened = os.fstat(descriptor)
        active = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _identity(before) != _identity(opened) or _identity(active) != _identity(opened):
            raise DailyPipelineReportAuthorityError(f"{label} changed while opening")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, _MAX_REPORT_BYTES + 1):
            chunks.append(chunk)
            if sum(map(len, chunks)) > _MAX_REPORT_BYTES:
                raise DailyPipelineReportAuthorityError(f"{label} exceeds size limit")
        after = os.fstat(descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _identity(after) != _identity(opened) or _identity(current) != _identity(after):
            raise DailyPipelineReportAuthorityError(f"{label} changed while reading")
        return b"".join(chunks)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise DailyPipelineReportAuthorityError(f"{label} cannot be read safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


class DailyPipelineReportAuthorityCapability(Protocol):
    """Only the CAS operation exposed to a daily runner."""

    def compare_and_advance(self, report: DailyPipelineRunReport, /) -> int: ...


@dataclass(frozen=True)
class DailyPipelineDevelopmentTestReportAuthority:
    """Explicit, Python-only test seam; production profiles cannot construct it via CLI."""

    capability: DailyPipelineReportAuthorityCapability

    def __post_init__(self) -> None:
        if not callable(getattr(self.capability, "compare_and_advance", None)):
            raise TypeError("development-test report authority requires a CAS capability")


class DailyPipelineReportAuthorityClient:
    """Daily-report adapter for an independently privileged socket authority."""

    def __init__(self, client: LabHighWaterAuthorityClient) -> None:
        self._client = client

    @classmethod
    def from_production_profile(
        cls,
        *,
        code_identity: str,
        profile_identity: str,
        mode: DailyPipelineMode,
        namespace_id: str,
    ) -> DailyPipelineReportAuthorityClient:
        injected = tuple(name for name in _RUNNER_OWNED_AUTHORITY_ENV if os.environ.get(name))
        if injected:
            raise DailyPipelineReportAuthorityError(
                "daily report authority rejects runner-owned command, arguments, state, or secret"
            )
        from rquant.config import settings

        if settings.app_env != "prod":
            raise DailyPipelineReportAuthorityError(
                "daily report authority requires the production profile"
            )
        verified_mode = DailyPipelineMode(mode)
        if (
            canonical_sha256(
                {
                    "contract": "daily-pipeline-storage-namespace/v1",
                    "mode": verified_mode,
                    "profile_hash": profile_identity,
                }
            )
            != namespace_id
        ):
            raise DailyPipelineReportAuthorityError(
                "daily report authority storage namespace is invalid"
            )
        keyring_path = settings.lab_highwater_trusted_keyring_path
        if keyring_path is None:
            raise DailyPipelineReportAuthorityError(
                "daily report authority public verification keyring is unavailable"
            )
        try:
            trusted_keys = load_highwater_trusted_keys(Path(keyring_path))
            client = LabHighWaterAuthorityClient(
                LabHighWaterAuthorityConfig(
                    command=PRODUCTION_LAB_HIGHWATER_COMMAND,
                    stable_identity=(
                        f"daily-pipeline-report:daily-close:{verified_mode.value}:{namespace_id}:v2"
                    ),
                    code_identity=code_identity,
                    profile_identity=profile_identity,
                    trusted_key_provider=trusted_keys.get,
                    timeout_seconds=settings.lab_highwater_timeout_seconds,
                    allow_identity_rotation=True,
                    production_mode=True,
                )
            )
        except (OSError, ValueError) as exc:
            raise DailyPipelineReportAuthorityError(
                "daily report authority fixed capability is unavailable or invalid"
            ) from exc
        return cls(client)

    def compare_and_advance(self, report: DailyPipelineRunReport, /) -> int:
        verified = DailyPipelineRunReport.model_validate(report)
        generation = verified.trade_date.toordinal()
        try:
            receipt = self._client.observe(
                database_generation=(1, 1),
                schema_generation=1,
                mutation_epoch=generation,
                chain_generation=generation,
                chain_head_hash=verified.report_id,
                receipt_kind="full",
                receipt_hash=verified.report_id,
            )
        except LabHighWaterAuthorityError as exc:
            raise DailyPipelineReportAuthorityError(
                "monotonic report authority refused compare-and-advance"
            ) from exc
        state = receipt.high_water
        if state is None or (
            state.mutation_epoch != generation
            or state.chain_generation != generation
            or state.chain_head_hash != verified.report_id
            or state.receipt_hash != verified.report_id
        ):
            raise DailyPipelineReportAuthorityError(
                "monotonic report authority returned a conflicting state"
            )
        return state.sequence + 1


class DailyPipelineReportStore:
    """Immutable reports anchored by a physically independent CAS authority."""

    def __init__(
        self,
        *,
        storage_profile: DailyPipelineStorageProfile,
        authority: DailyPipelineReportAuthorityCapability,
    ) -> None:
        self.storage_profile = DailyPipelineStorageProfile.model_validate(storage_profile)
        self.report_root = self.storage_profile.report_root
        try:
            self._storage_binding = DailyPipelineStorageBinding.open(
                self.storage_profile,
                leaf="reports",
            )
        except DailyPipelineLedgerError as exc:
            raise DailyPipelineReportAuthorityError(
                "daily pipeline report storage binding is unsafe"
            ) from exc
        if not callable(getattr(authority, "compare_and_advance", None)):
            raise TypeError("daily report store requires a compare-and-advance capability")
        self.authority = authority

    def publish(self, report: DailyPipelineRunReport) -> Path:
        verified = DailyPipelineRunReport.model_validate(report)
        if (
            verified.mode is not self.storage_profile.mode
            or verified.profile_hash != self.storage_profile.profile_hash
            or verified.namespace_id != self.storage_profile.namespace_id
        ):
            raise DailyPipelineReportAuthorityError(
                "daily pipeline report storage profile mismatch"
            )
        try:
            self._storage_binding.assert_current()
        except DailyPipelineLedgerError as exc:
            raise DailyPipelineReportAuthorityError(
                "daily pipeline report storage binding changed"
            ) from exc
        sequence = self.authority.compare_and_advance(verified)
        name = f"{sequence:08d}-{verified.trade_date.isoformat()}-{verified.report_id}.json"
        payload = _canonical_bytes(verified)
        try:
            directory_fd = self._storage_binding.leaf_fd
            try:
                current = _read_bound_file(directory_fd, name, label="daily pipeline report")
            except FileNotFoundError:
                self._write_once(directory_fd, name, payload)
            else:
                if current != payload:
                    raise DailyPipelineReportAuthorityError(
                        "daily pipeline immutable report conflicts"
                    )
            self._storage_binding.assert_current()
        except DailyPipelineLedgerError as exc:
            raise DailyPipelineReportAuthorityError(
                "daily pipeline report storage binding changed"
            ) from exc
        return self.report_root / name

    @staticmethod
    def _write_once(directory_fd: int, name: str, payload: bytes) -> None:
        if len(payload) > _MAX_REPORT_BYTES:
            raise DailyPipelineReportAuthorityError("daily pipeline report exceeds size limit")
        temporary = f".{name}.{uuid4().hex}.tmp"
        descriptor = -1
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            os.write(descriptor, payload)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            try:
                os.link(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
            except FileExistsError:
                current = _read_bound_file(directory_fd, name, label="daily pipeline report")
                if current != payload:
                    raise DailyPipelineReportAuthorityError(
                        "daily pipeline immutable report conflicts"
                    ) from None
            finally:
                os.unlink(temporary, dir_fd=directory_fd)
            os.fsync(directory_fd)
        except OSError as exc:
            raise DailyPipelineReportAuthorityError(
                "daily pipeline report publication failed"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=directory_fd)


__all__ = [
    "DailyPipelineDevelopmentTestReportAuthority",
    "DailyPipelineReportAuthorityCapability",
    "DailyPipelineReportAuthorityClient",
    "DailyPipelineReportAuthorityError",
    "DailyPipelineReportStore",
    "DailyPipelineRunReport",
]
