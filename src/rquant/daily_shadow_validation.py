"""Immutable no-lookahead comparison evidence for legacy and daily DAG shadows."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
from collections import Counter
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_market_session import MarketCalendarAuthority

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
_MAX_REPORT_BYTES = 16 * 1024 * 1024


class DailyShadowValidationError(RuntimeError):
    """Daily shadow evidence is incomplete, future-aware, or not trustworthy."""


class DailyShadowReportConflictError(DailyShadowValidationError):
    """A trading day already has immutable shadow evidence."""


class DailyShadowDataset(StrEnum):
    DAILY_BAR = "daily_bar"
    DAILY_STATE = "daily_state"
    POOL = "pool"
    SCREEN = "screen"
    SUMMARY = "summary"
    NOTIFICATIONS = "notifications"
    OUTBOX = NOTIFICATIONS


class DailyShadowRecord(RuntimeContractModel):
    key: tuple[str, ...] = Field(min_length=1, max_length=16)
    content_hash: Sha256
    available_at: AwareUtcDatetime
    revises_content_hash: Sha256 | None = None


class DailyShadowSnapshot(RuntimeContractModel):
    source: Literal["legacy", "dag"]
    trade_date: date
    dataset: DailyShadowDataset
    source_generation_id: Sha256
    available_at: AwareUtcDatetime
    records: tuple[DailyShadowRecord, ...] = Field(max_length=1_000_000)

    @field_validator("records")
    @classmethod
    def _unique_keys(cls, records: tuple[DailyShadowRecord, ...]) -> tuple[DailyShadowRecord, ...]:
        keys = tuple(record.key for record in records)
        if len(keys) != len(set(keys)):
            raise ValueError("daily shadow snapshot record keys must be unique")
        return records

    @model_validator(mode="after")
    def _records_not_before_snapshot(self) -> Self:
        if any(record.available_at < self.available_at for record in self.records):
            raise ValueError("daily shadow record is available before its snapshot")
        return self


class DailyShadowSession(RuntimeContractModel):
    contract: Literal["daily-shadow-session/v1"] = "daily-shadow-session/v1"
    evidence_origin: Literal["production", "historical_fixture"]
    trade_date: date
    captured_at: AwareUtcDatetime
    session_closed_at: AwareUtcDatetime
    deadline_at: AwareUtcDatetime
    code_commit: CommitSha
    legacy_profile_hash: Sha256
    dag_profile_hash: Sha256
    legacy_data_generation: Sha256
    dag_data_generation: Sha256
    snapshots: tuple[DailyShadowSnapshot, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def _bind_trade_date(self) -> Self:
        if self.session_closed_at > self.deadline_at:
            raise ValueError("daily shadow deadline precedes session close")
        if any(snapshot.trade_date != self.trade_date for snapshot in self.snapshots):
            raise ValueError("daily shadow snapshot trade date changed")
        pairs = tuple((item.source, item.dataset) for item in self.snapshots)
        if len(pairs) != len(set(pairs)):
            raise ValueError("daily shadow source dataset snapshots must be unique")
        return self

    @property
    def freeze_identity(self) -> str:
        return canonical_sha256(
            {
                "code_commit": self.code_commit,
                "legacy_profile_hash": self.legacy_profile_hash,
                "dag_profile_hash": self.dag_profile_hash,
                "legacy_data_generation": self.legacy_data_generation,
                "dag_data_generation": self.dag_data_generation,
            }
        )


class DailyShadowDiscrepancy(RuntimeContractModel):
    dataset: DailyShadowDataset
    key: tuple[str, ...]
    category: Literal["data_delay", "legitimate_revision", "semantic_mismatch", "unknown"]
    legacy_content_hash: Sha256 | None = None
    dag_content_hash: Sha256 | None = None
    legacy_available_at: AwareUtcDatetime | None = None
    dag_available_at: AwareUtcDatetime | None = None
    data_delay_seconds: float | None = Field(default=None, ge=0.0)
    detail: str = Field(min_length=1, max_length=4_096)


class DailyShadowThresholds(RuntimeContractModel):
    max_data_delay_seconds: int = Field(default=30 * 60, ge=0, le=24 * 60 * 60)
    max_semantic_mismatches: int = Field(default=0, ge=0, le=10_000)
    max_unknown_differences: int = Field(default=0, ge=0, le=10_000)
    max_samples_per_category: int = Field(default=100, ge=1, le=1_000)


class DailyShadowReport(RuntimeContractModel):
    contract: Literal["daily-shadow-report/v1"] = "daily-shadow-report/v1"
    report_id: Sha256 | None = None
    session: DailyShadowSession
    thresholds: DailyShadowThresholds
    passed: bool
    discrepancy_counts: dict[str, int]
    discrepancies: tuple[DailyShadowDiscrepancy, ...] = ()
    generated_at: AwareUtcDatetime
    key_id: str | None = Field(default=None, min_length=1)
    signature: Sha256 | None = None

    @model_validator(mode="after")
    def _bind_report(self) -> Self:
        if (self.key_id is None) != (self.signature is None):
            raise ValueError("daily shadow report signature metadata is incomplete")
        expected = canonical_sha256(self._identity_payload())
        if self.report_id is None:
            object.__setattr__(self, "report_id", expected)
        elif self.report_id != expected:
            raise ValueError("daily shadow report id does not match content")
        return self

    def _identity_payload(self) -> dict[str, object]:
        return self.model_dump(
            mode="json",
            exclude={"report_id", "key_id", "signature"},
        )

    def signing_payload(self) -> bytes:
        return _canonical_bytes(
            {
                "contract": "daily-shadow-signature/v1",
                "report_id": self.report_id,
                "identity": self._identity_payload(),
            }
        )


class DailyShadowSigner(Protocol):
    key_id: str

    def sign(self, payload: bytes) -> str: ...

    def verify(self, payload: bytes, signature: str) -> bool: ...


class DailyShadowHmacSigner:
    """Injected signing capability; report roots never contain this secret."""

    def __init__(self, *, key_id: str, secret: bytes) -> None:
        if not key_id.strip():
            raise ValueError("daily shadow key id must not be empty")
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("daily shadow signing secret must contain at least 32 bytes")
        self.key_id = key_id
        self._secret = secret

    def sign(self, payload: bytes) -> str:
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)


class DailyRetirementGateConfig(RuntimeContractModel):
    minimum_real_trading_days: int = Field(default=10, ge=10, le=120)


class DailyRetirementGateDecision(RuntimeContractModel):
    eligible: bool
    counted_trade_dates: tuple[date, ...] = ()
    reasons: tuple[str, ...] = ()
    freeze_identity: Sha256 | None = None


class DailyShadowComparator:
    """Compare pre-materialized as-of snapshots; it never opens a live database."""

    def __init__(
        self,
        thresholds: DailyShadowThresholds | None = None,
        *,
        required_datasets: tuple[DailyShadowDataset, ...] = tuple(DailyShadowDataset),
    ) -> None:
        if len(required_datasets) != len(set(required_datasets)):
            raise ValueError("daily shadow required datasets must be unique")
        self._thresholds = thresholds or DailyShadowThresholds()
        self._required_datasets = required_datasets

    def compare(
        self, session: DailyShadowSession, *, generated_at: datetime | None = None
    ) -> DailyShadowReport:
        verified = DailyShadowSession.model_validate(session)
        observed = normalize_aware_utc(generated_at or verified.captured_at)
        self._assert_no_future_inputs(verified, observed)
        snapshots = {(item.dataset, item.source): item for item in verified.snapshots}
        discrepancies: list[DailyShadowDiscrepancy] = []
        datasets = tuple(
            sorted(
                {*self._required_datasets, *(item.dataset for item in verified.snapshots)}, key=str
            )
        )
        for dataset in datasets:
            left = snapshots.get((dataset, "legacy"))
            right = snapshots.get((dataset, "dag"))
            discrepancies.extend(self._compare_dataset(dataset, left, right))
        counts = Counter(item.category for item in discrepancies)
        stored = self._sample(discrepancies)
        passed = (
            counts["semantic_mismatch"] <= self._thresholds.max_semantic_mismatches
            and counts["unknown"] <= self._thresholds.max_unknown_differences
            and all(
                item.category != "data_delay"
                or item.data_delay_seconds is None
                or item.data_delay_seconds <= self._thresholds.max_data_delay_seconds
                for item in discrepancies
            )
        )
        return DailyShadowReport(
            session=verified,
            thresholds=self._thresholds,
            passed=passed,
            discrepancy_counts={key: int(value) for key, value in sorted(counts.items())},
            discrepancies=stored,
            generated_at=observed,
        )

    def _assert_no_future_inputs(self, session: DailyShadowSession, generated_at: datetime) -> None:
        if session.captured_at > session.deadline_at:
            raise DailyShadowValidationError(
                "daily shadow evidence was captured after its deadline"
            )
        if generated_at > session.deadline_at:
            raise DailyShadowValidationError("daily shadow report was generated after its deadline")
        if session.captured_at < session.session_closed_at:
            raise DailyShadowValidationError("daily shadow evidence predates the closed session")
        for snapshot in session.snapshots:
            if snapshot.available_at > session.captured_at:
                raise DailyShadowValidationError("daily shadow snapshot contains future data")
            for record in snapshot.records:
                if record.available_at > session.captured_at:
                    raise DailyShadowValidationError("daily shadow record contains future data")

    def _compare_dataset(
        self,
        dataset: DailyShadowDataset,
        legacy: DailyShadowSnapshot | None,
        dag: DailyShadowSnapshot | None,
    ) -> list[DailyShadowDiscrepancy]:
        if legacy is None or dag is None:
            return [
                DailyShadowDiscrepancy(
                    dataset=dataset,
                    key=("__dataset__",),
                    category="unknown",
                    detail="legacy or DAG dataset snapshot is missing",
                )
            ]
        left = {item.key: item for item in legacy.records}
        right = {item.key: item for item in dag.records}
        discrepancies: list[DailyShadowDiscrepancy] = []
        for key in sorted(set(left) | set(right)):
            legacy_row = left.get(key)
            dag_row = right.get(key)
            if legacy_row is None or dag_row is None:
                discrepancies.append(
                    DailyShadowDiscrepancy(
                        dataset=dataset,
                        key=key,
                        category="unknown",
                        legacy_content_hash=None if legacy_row is None else legacy_row.content_hash,
                        dag_content_hash=None if dag_row is None else dag_row.content_hash,
                        legacy_available_at=None if legacy_row is None else legacy_row.available_at,
                        dag_available_at=None if dag_row is None else dag_row.available_at,
                        detail="record is present in only one pipeline",
                    )
                )
                continue
            if legacy_row.content_hash == dag_row.content_hash:
                delay = (dag_row.available_at - legacy_row.available_at).total_seconds()
                if delay > 0:
                    discrepancies.append(
                        DailyShadowDiscrepancy(
                            dataset=dataset,
                            key=key,
                            category="data_delay",
                            legacy_content_hash=legacy_row.content_hash,
                            dag_content_hash=dag_row.content_hash,
                            legacy_available_at=legacy_row.available_at,
                            dag_available_at=dag_row.available_at,
                            data_delay_seconds=delay,
                            detail="DAG record became available after the legacy record",
                        )
                    )
                continue
            category: Literal["legitimate_revision", "semantic_mismatch"]
            if (
                dag_row.revises_content_hash == legacy_row.content_hash
                or legacy_row.revises_content_hash == dag_row.content_hash
            ):
                category = "legitimate_revision"
            else:
                category = "semantic_mismatch"
            discrepancies.append(
                DailyShadowDiscrepancy(
                    dataset=dataset,
                    key=key,
                    category=category,
                    legacy_content_hash=legacy_row.content_hash,
                    dag_content_hash=dag_row.content_hash,
                    legacy_available_at=legacy_row.available_at,
                    dag_available_at=dag_row.available_at,
                    detail="record content differs between legacy and DAG snapshots",
                )
            )
        return discrepancies

    def _sample(
        self,
        discrepancies: list[DailyShadowDiscrepancy],
    ) -> tuple[DailyShadowDiscrepancy, ...]:
        remaining: Counter[str] = Counter()
        selected: list[DailyShadowDiscrepancy] = []
        for item in sorted(
            discrepancies, key=lambda value: (value.category, value.dataset, value.key)
        ):
            if remaining[item.category] >= self._thresholds.max_samples_per_category:
                continue
            remaining[item.category] += 1
            selected.append(item)
        return tuple(selected)


class DailyShadowReportStore:
    """One signed, immutable report per trade date; reruns cannot replace evidence."""

    def __init__(
        self,
        root: Path,
        *,
        signer: DailyShadowSigner,
        create: bool = True,
    ) -> None:
        self.root = Path(root)
        self._signer = signer
        if create:
            self.root.mkdir(parents=True, exist_ok=True)

    def publish(self, report: DailyShadowReport) -> DailyShadowReport:
        unsigned = DailyShadowReport.model_validate(report)
        if unsigned.generated_at > unsigned.session.deadline_at:
            raise DailyShadowValidationError("daily shadow report was generated after its deadline")
        if unsigned.key_id is not None:
            self._verify(unsigned)
            signed = unsigned
        else:
            signed = DailyShadowReport.model_validate(
                {
                    **unsigned.model_dump(mode="python"),
                    "key_id": self._signer.key_id,
                    "signature": self._signer.sign(unsigned.signing_payload()),
                }
            )
        target = self._path(signed.session.trade_date)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise DailyShadowReportConflictError(
                "daily shadow report already exists for this trade date; reruns cannot replace it"
            )
        payload = _canonical_bytes(signed.model_dump(mode="json"))
        _atomic_create(target, payload)
        return signed

    def load(self, trade_date: date) -> DailyShadowReport:
        target = self._path(trade_date)
        if not target.exists():
            raise DailyShadowValidationError("daily shadow report is missing")
        report = _load_report(target)
        if report.session.trade_date != trade_date:
            raise DailyShadowValidationError("daily shadow report trade date path changed")
        self._verify(report)
        return report

    def load_optional(self, trade_date: date) -> DailyShadowReport | None:
        try:
            return self.load(trade_date)
        except DailyShadowValidationError as exc:
            if str(exc) == "daily shadow report is missing":
                return None
            raise

    def _path(self, trade_date: date) -> Path:
        return self.root / trade_date.isoformat() / "report.json"

    def _verify(self, report: DailyShadowReport) -> None:
        if report.key_id != self._signer.key_id or report.signature is None:
            raise DailyShadowValidationError("daily shadow report signing identity is invalid")
        if not self._signer.verify(report.signing_payload(), report.signature):
            raise DailyShadowValidationError("daily shadow report signature is invalid")


class DailyRetirementGate:
    """Count only immutable, real-session evidence under one frozen identity."""

    def __init__(self, config: DailyRetirementGateConfig | None = None) -> None:
        self._config = config or DailyRetirementGateConfig()

    def evaluate(
        self,
        store: DailyShadowReportStore,
        *,
        expected_trade_dates: tuple[date, ...],
        calendar: MarketCalendarAuthority,
    ) -> DailyRetirementGateDecision:
        authority = MarketCalendarAuthority.model_validate(calendar)
        expected = tuple(sorted(set(expected_trade_dates)))
        non_trading_dates = tuple(item for item in expected if item not in authority.open_dates)
        if non_trading_dates:
            return DailyRetirementGateDecision(
                eligible=False,
                reasons=tuple(
                    f"non_sse_open_date:{item.isoformat()}" for item in non_trading_dates
                ),
            )
        if not expected:
            return DailyRetirementGateDecision(
                eligible=False,
                reasons=("insufficient_authoritative_trade_dates",),
            )
        available_open_dates = tuple(item for item in authority.open_dates if item <= expected[-1])
        required = available_open_dates[-self._config.minimum_real_trading_days :]
        required_suffix = tuple(expected[-len(required) :])
        if len(required) < self._config.minimum_real_trading_days or required_suffix != required:
            return DailyRetirementGateDecision(
                eligible=False,
                reasons=("insufficient_authoritative_trade_dates",),
            )
        reports: list[DailyShadowReport] = []
        reasons: list[str] = []
        for trade_date in required:
            report = store.load_optional(trade_date)
            if report is None:
                reasons.append(f"missing_report:{trade_date.isoformat()}")
                continue
            if report.session.evidence_origin != "production":
                reasons.append(f"non_production_evidence:{trade_date.isoformat()}")
                continue
            if not report.passed:
                reasons.append(f"failed_parity:{trade_date.isoformat()}")
                continue
            reports.append(report)
        if reasons:
            return DailyRetirementGateDecision(
                eligible=False,
                counted_trade_dates=tuple(report.session.trade_date for report in reports),
                reasons=tuple(reasons),
            )
        frozen = reports[0].session.freeze_identity
        drifted = [
            report.session.trade_date.isoformat()
            for report in reports
            if report.session.freeze_identity != frozen
        ]
        if drifted:
            return DailyRetirementGateDecision(
                eligible=False,
                counted_trade_dates=tuple(report.session.trade_date for report in reports),
                reasons=tuple(f"frozen_identity_changed:{value}" for value in drifted),
                freeze_identity=frozen,
            )
        return DailyRetirementGateDecision(
            eligible=True,
            counted_trade_dates=tuple(report.session.trade_date for report in reports),
            freeze_identity=frozen,
        )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _atomic_create(path: Path, payload: bytes) -> None:
    if len(payload) > _MAX_REPORT_BYTES:
        raise DailyShadowValidationError("daily shadow report exceeds its size budget")
    if path.exists():
        raise DailyShadowReportConflictError("daily shadow report already exists")
    descriptor = None
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        os.unlink(temporary)
    except FileExistsError as exc:
        raise DailyShadowReportConflictError("daily shadow report already exists") from exc
    except OSError as exc:
        raise DailyShadowValidationError("daily shadow report publication failed") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _load_report(path: Path) -> DailyShadowReport:
    try:
        observed = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
            raise DailyShadowValidationError("daily shadow report path is unsafe")
        if observed.st_size > _MAX_REPORT_BYTES:
            raise DailyShadowValidationError("daily shadow report exceeds its size budget")
        payload = path.read_bytes()
        return DailyShadowReport.model_validate_json(payload)
    except (OSError, ValueError) as exc:
        if isinstance(exc, DailyShadowValidationError):
            raise
        raise DailyShadowValidationError("daily shadow report is corrupt") from exc


__all__ = [
    "DailyRetirementGate",
    "DailyRetirementGateConfig",
    "DailyRetirementGateDecision",
    "DailyShadowComparator",
    "DailyShadowDataset",
    "DailyShadowDiscrepancy",
    "DailyShadowHmacSigner",
    "DailyShadowRecord",
    "DailyShadowReport",
    "DailyShadowReportConflictError",
    "DailyShadowReportStore",
    "DailyShadowSession",
    "DailyShadowSnapshot",
    "DailyShadowThresholds",
    "DailyShadowValidationError",
]
