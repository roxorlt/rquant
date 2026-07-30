"""Immutable experiment evidence and append-only promotion governance."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Self
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
Probability = Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)]
FiniteDecimal = Annotated[Decimal, Field(allow_inf_nan=False)]
SHANGHAI = ZoneInfo("Asia/Shanghai")


class ExperimentStatus(StrEnum):
    REGISTERED = "registered"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PromotionStage(StrEnum):
    EXPLORATORY = "exploratory"
    COMPARABLE = "comparable"
    PAPER_CANDIDATE = "paper_candidate"
    MONITOR_APPROVED = "monitor_approved"


class ExperimentRegistryError(RuntimeError):
    """Base class for registry integrity failures."""


class ExperimentIdentityConflictError(ExperimentRegistryError):
    """An experiment id was reused with different immutable content."""


class TerminalExperimentError(ExperimentRegistryError):
    """A terminal attempt was asked to accept different evidence."""


class IncompleteHypothesisFamilyError(ExperimentRegistryError):
    """A frozen family is not complete enough for multiplicity adjustment."""


class DateRange(RuntimeContractModel):
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("date range start_date must not be after end_date")
        return self


class HypothesisFamilyManifest(RuntimeContractModel):
    manifest_id: Sha256 | None = None
    hypothesis_family: str = Field(min_length=1)
    experiment_ids: tuple[Sha256, ...]
    search_space_fingerprint: Sha256
    metric_definition_fingerprint: Sha256
    preregistered_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if not self.experiment_ids:
            raise ValueError("hypothesis family requires experiment ids")
        if len(set(self.experiment_ids)) != len(self.experiment_ids):
            raise ValueError("hypothesis family experiment ids must be unique")
        ordered = tuple(sorted(self.experiment_ids))
        if ordered != self.experiment_ids:
            object.__setattr__(self, "experiment_ids", ordered)
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"manifest_id"}))
        if self.manifest_id is None:
            object.__setattr__(self, "manifest_id", expected)
        elif self.manifest_id != expected:
            raise ValueError("manifest_id does not match canonical family content")
        return self

    @property
    def hypothesis_count(self) -> int:
        return len(self.experiment_ids)


class EvaluationArtifactEvidence(RuntimeContractModel):
    artifact_hash: Sha256
    metric_definition_fingerprint: Sha256
    evaluation_range: DateRange
    available_at: AwareUtcDatetime
    trade_count: int = Field(ge=0)
    net_return: FiniteDecimal
    max_drawdown: Probability

    @model_validator(mode="after")
    def validate_availability(self) -> Self:
        if self.available_at.astimezone(SHANGHAI) < datetime.combine(
            self.evaluation_range.end_date,
            time(15, 0),
            tzinfo=SHANGHAI,
        ):
            raise ValueError("artifact cannot be available before its evaluation range closes")
        return self


class ForwardArtifactEvidence(RuntimeContractModel):
    artifact_hash: Sha256
    metric_definition_fingerprint: Sha256
    observation_range: DateRange
    available_at: AwareUtcDatetime
    trading_days: int = Field(ge=0)
    fill_count: int = Field(ge=0)
    net_return: FiniteDecimal
    max_drawdown: Probability

    @model_validator(mode="after")
    def validate_availability(self) -> Self:
        if self.available_at.astimezone(SHANGHAI) < datetime.combine(
            self.observation_range.end_date,
            time(15, 0),
            tzinfo=SHANGHAI,
        ):
            raise ValueError("artifact cannot be available before its observation range closes")
        return self


class ExperimentSpec(RuntimeContractModel):
    experiment_id: Sha256 | None = None
    strategy_spec_fingerprint: Sha256
    dataset_snapshot_id: Sha256
    code_commit: CommitSha
    parameter_fingerprint: Sha256
    hypothesis_family: str = Field(min_length=1)
    metric_definition_fingerprint: Sha256
    train_range: DateRange
    validation_range: DateRange
    frozen_outer_test_range: DateRange
    cost_model_fingerprint: Sha256
    execution_model_fingerprint: Sha256
    seed: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_identity_and_ranges(self) -> Self:
        if self.train_range.end_date >= self.validation_range.start_date:
            raise ValueError("train and validation ranges must be disjoint and chronological")
        if self.validation_range.end_date >= self.frozen_outer_test_range.start_date:
            raise ValueError(
                "validation and frozen outer test ranges must be disjoint and chronological"
            )
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"experiment_id"}))
        if self.experiment_id is None:
            object.__setattr__(self, "experiment_id", expected)
        elif self.experiment_id != expected:
            raise ValueError("experiment_id does not match canonical experiment identity")
        return self


class ExperimentOutcome(RuntimeContractModel):
    experiment_id: Sha256
    trade_count: int = Field(ge=0)
    net_return: FiniteDecimal
    max_drawdown: Probability
    win_rate: Probability
    confidence_lower: FiniteDecimal
    confidence_upper: FiniteDecimal
    attempted_configuration_count: int = Field(ge=1)
    selected_rank: int = Field(ge=1)
    raw_p_value: Probability
    adjusted_p_value: Probability | None = None
    artifact_hash: Sha256
    outer_test_completed: bool
    outer_evidence: EvaluationArtifactEvidence | None = None

    @model_validator(mode="after")
    def validate_statistics(self) -> Self:
        if self.confidence_lower > self.confidence_upper:
            raise ValueError("confidence lower bound cannot exceed upper bound")
        if not self.confidence_lower <= self.net_return <= self.confidence_upper:
            raise ValueError("net_return must fall within the confidence interval")
        if self.selected_rank > self.attempted_configuration_count:
            raise ValueError("selected_rank cannot exceed attempted_configuration_count")
        if self.adjusted_p_value is not None and self.adjusted_p_value < self.raw_p_value:
            raise ValueError("adjusted_p_value cannot be below raw_p_value")
        if self.outer_test_completed != (self.outer_evidence is not None):
            raise ValueError("outer_test_completed requires matching immutable outer_evidence")
        if self.outer_evidence is not None:
            if self.artifact_hash != self.outer_evidence.artifact_hash:
                raise ValueError("artifact_hash must match outer_evidence")
            if self.trade_count != self.outer_evidence.trade_count:
                raise ValueError("trade_count must match outer_evidence")
            if self.net_return != self.outer_evidence.net_return:
                raise ValueError("net_return must match outer_evidence")
            if self.max_drawdown != self.outer_evidence.max_drawdown:
                raise ValueError("max_drawdown must match outer_evidence")
        return self


class ExperimentAttempt(RuntimeContractModel):
    spec: ExperimentSpec
    status: ExperimentStatus
    registered_at: AwareUtcDatetime
    started_at: AwareUtcDatetime | None = None
    completed_at: AwareUtcDatetime | None = None
    first_error: str | None = None
    outcome: ExperimentOutcome | None = None

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.started_at is not None and self.started_at < self.registered_at:
            raise ValueError("started_at cannot precede registered_at")
        if self.completed_at is not None:
            reference = self.started_at or self.registered_at
            if self.completed_at < reference:
                raise ValueError("completed_at cannot precede attempt activity")
        if self.status is ExperimentStatus.REGISTERED:
            if any(
                value is not None
                for value in (self.started_at, self.completed_at, self.first_error, self.outcome)
            ):
                raise ValueError("registered attempt cannot contain execution evidence")
        elif self.status is ExperimentStatus.RUNNING:
            if self.started_at is None or any(
                value is not None for value in (self.completed_at, self.first_error, self.outcome)
            ):
                raise ValueError("running attempt requires only started_at")
        elif self.status is ExperimentStatus.SUCCEEDED:
            if self.started_at is None or self.completed_at is None or self.outcome is None:
                raise ValueError("succeeded attempt requires timing and outcome evidence")
            if self.first_error is not None:
                raise ValueError("succeeded attempt cannot contain first_error")
        else:
            if self.completed_at is None or not self.first_error or self.outcome is not None:
                raise ValueError("failed or cancelled attempt requires first_error and completion")
        return self


class PromotionPolicy(RuntimeContractModel):
    policy_fingerprint: Sha256 | None = None
    minimum_comparable_trades: int = Field(ge=1)
    significance_level: Probability
    minimum_forward_days: int = Field(ge=1)
    minimum_forward_fills: int = Field(ge=1)
    maximum_forward_drawdown: Probability

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"policy_fingerprint"}))
        if self.policy_fingerprint is None:
            object.__setattr__(self, "policy_fingerprint", expected)
        elif self.policy_fingerprint != expected:
            raise ValueError("policy_fingerprint does not match promotion policy")
        return self


class PromotionDecision(RuntimeContractModel):
    decision_id: Sha256 | None = None
    stage: PromotionStage
    experiment_ids: tuple[Sha256, ...]
    evidence_artifact_hash: Sha256
    decided_at: AwareUtcDatetime
    approved: bool
    gate_failures: tuple[str, ...] = ()
    minimum_trade_count: int = Field(ge=1)
    significance_level: Probability
    forward_trading_days: int = Field(ge=0)
    forward_fills: int = Field(ge=0)
    minimum_forward_days: int = Field(ge=1)
    minimum_forward_fills: int = Field(ge=1)
    maximum_forward_drawdown: Probability
    policy_fingerprint: Sha256
    forward_evidence_artifact_hash: Sha256 | None = None
    forward_net_return: FiniteDecimal | None = None
    forward_max_drawdown: Probability | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if not self.experiment_ids:
            raise ValueError("promotion decision requires at least one experiment")
        ordered_ids = tuple(sorted(set(self.experiment_ids)))
        if ordered_ids != self.experiment_ids:
            object.__setattr__(self, "experiment_ids", ordered_ids)
        failures = tuple(dict.fromkeys(self.gate_failures))
        if failures != self.gate_failures:
            object.__setattr__(self, "gate_failures", failures)
        if self.approved == bool(self.gate_failures):
            raise ValueError("approved must be true exactly when gate_failures is empty")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"decision_id"}))
        if self.decision_id is None:
            object.__setattr__(self, "decision_id", expected)
        elif self.decision_id != expected:
            raise ValueError("decision_id does not match canonical decision content")
        return self


def _json_payload(model: RuntimeContractModel) -> str:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _utc_iso(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _adjusted_outcome(outcome: ExperimentOutcome, value: Decimal | None) -> ExperimentOutcome:
    payload = outcome.model_dump(mode="python")
    payload["adjusted_p_value"] = value
    return ExperimentOutcome.model_validate(payload)


class ExperimentRegistry:
    """Serialize experiment evidence and promotion decisions through SQLite WAL."""

    def __init__(
        self,
        path: Path,
        *,
        minimum_comparable_trades: int = 30,
        significance_level: Decimal = Decimal("0.05"),
        minimum_forward_days: int = 10,
        minimum_forward_fills: int = 20,
        maximum_forward_drawdown: Decimal = Decimal("0.10"),
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if minimum_comparable_trades < 1:
            raise ValueError("minimum_comparable_trades must be positive")
        if not Decimal("0") < significance_level <= Decimal("1"):
            raise ValueError("significance_level must be in (0, 1]")
        if minimum_forward_days < 1 or minimum_forward_fills < 1:
            raise ValueError("forward evidence minimums must be positive")
        if not Decimal("0") <= maximum_forward_drawdown <= Decimal("1"):
            raise ValueError("maximum_forward_drawdown must be in [0, 1]")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.minimum_comparable_trades = minimum_comparable_trades
        self.significance_level = significance_level
        self.minimum_forward_days = minimum_forward_days
        self.minimum_forward_fills = minimum_forward_fills
        self.maximum_forward_drawdown = maximum_forward_drawdown
        self.policy = PromotionPolicy(
            minimum_comparable_trades=minimum_comparable_trades,
            significance_level=significance_level,
            minimum_forward_days=minimum_forward_days,
            minimum_forward_fills=minimum_forward_fills,
            maximum_forward_drawdown=maximum_forward_drawdown,
        )
        self.busy_timeout_ms = busy_timeout_ms
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        except BaseException:
            connection.close()
            raise
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS experiment_attempt (
                    experiment_id TEXT PRIMARY KEY,
                    hypothesis_family TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    first_error TEXT
                );
                CREATE INDEX IF NOT EXISTS experiment_attempt_family_idx
                    ON experiment_attempt(hypothesis_family, experiment_id);

                CREATE TABLE IF NOT EXISTS hypothesis_family_manifest (
                    hypothesis_family TEXT PRIMARY KEY,
                    manifest_id TEXT NOT NULL UNIQUE,
                    preregistered_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS registry_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS experiment_outcome (
                    experiment_id TEXT PRIMARY KEY
                        REFERENCES experiment_attempt(experiment_id),
                    outcome_json TEXT NOT NULL,
                    attempted_configuration_count INTEGER NOT NULL,
                    selected_rank INTEGER NOT NULL,
                    raw_p_value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS family_adjustment (
                    experiment_id TEXT PRIMARY KEY
                        REFERENCES experiment_outcome(experiment_id),
                    hypothesis_family TEXT NOT NULL,
                    adjusted_p_value TEXT NOT NULL,
                    adjusted_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS family_adjustment_family_idx
                    ON family_adjustment(hypothesis_family, experiment_id);

                CREATE TABLE IF NOT EXISTS promotion_decision (
                    decision_id TEXT PRIMARY KEY,
                    stage TEXT NOT NULL,
                    approved INTEGER NOT NULL CHECK(approved IN (0, 1)),
                    decided_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS promotion_decision_time_idx
                    ON promotion_decision(decided_at, decision_id);
                """
            )
            policy_payload = _json_payload(self.policy)
            existing = connection.execute(
                "SELECT value FROM registry_metadata WHERE key = 'promotion_policy'"
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO registry_metadata(key, value) VALUES ('promotion_policy', ?)",
                    (policy_payload,),
                )
            elif existing["value"] != policy_payload:
                raise ExperimentRegistryError(
                    "promotion policy fingerprint conflicts with the registry"
                )

    def register_hypothesis_family(
        self, manifest: HypothesisFamilyManifest
    ) -> HypothesisFamilyManifest:
        expected = canonical_sha256(manifest.model_dump(mode="python", exclude={"manifest_id"}))
        if manifest.manifest_id != expected:
            raise ExperimentIdentityConflictError(
                "manifest_id does not match the supplied immutable content"
            )
        manifest = HypothesisFamilyManifest.model_validate(manifest.model_dump(mode="python"))
        payload = _json_payload(manifest)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    """
                    SELECT payload_json FROM hypothesis_family_manifest
                    WHERE hypothesis_family = ?
                    """,
                    (manifest.hypothesis_family,),
                ).fetchone()
                if existing is not None:
                    if existing["payload_json"] != payload:
                        raise ExperimentIdentityConflictError(
                            "hypothesis family manifest is immutable"
                        )
                    connection.rollback()
                    return manifest
                attempts = connection.execute(
                    "SELECT COUNT(*) FROM experiment_attempt WHERE hypothesis_family = ?",
                    (manifest.hypothesis_family,),
                ).fetchone()[0]
                if attempts:
                    raise IncompleteHypothesisFamilyError(
                        "hypothesis family must be preregistered before attempts"
                    )
                connection.execute(
                    """
                    INSERT INTO hypothesis_family_manifest(
                        hypothesis_family, manifest_id, preregistered_at, payload_json
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        manifest.hypothesis_family,
                        manifest.manifest_id,
                        _utc_iso(manifest.preregistered_at),
                        payload,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return manifest

    def get_hypothesis_family(self, hypothesis_family: str) -> HypothesisFamilyManifest:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM hypothesis_family_manifest WHERE hypothesis_family = ?",
                (hypothesis_family,),
            ).fetchone()
        if row is None:
            raise IncompleteHypothesisFamilyError(
                f"hypothesis family {hypothesis_family!r} is not preregistered"
            )
        return HypothesisFamilyManifest.model_validate_json(row["payload_json"])

    @staticmethod
    def _validated_spec(spec: ExperimentSpec) -> ExperimentSpec:
        expected = canonical_sha256(spec.model_dump(mode="python", exclude={"experiment_id"}))
        if spec.experiment_id != expected:
            raise ExperimentIdentityConflictError(
                "experiment_id does not match the supplied immutable content"
            )
        return ExperimentSpec.model_validate(spec.model_dump(mode="python"))

    def register_attempt(
        self,
        spec: ExperimentSpec,
        *,
        registered_at: datetime,
    ) -> ExperimentAttempt:
        spec = self._validated_spec(spec)
        registered_at = normalize_aware_utc(registered_at)
        payload = _json_payload(spec)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                manifest = self._required_manifest(connection, spec.hypothesis_family)
                if spec.experiment_id not in manifest.experiment_ids:
                    raise IncompleteHypothesisFamilyError(
                        "experiment id was not preregistered in the family manifest"
                    )
                if spec.metric_definition_fingerprint != manifest.metric_definition_fingerprint:
                    raise IncompleteHypothesisFamilyError(
                        "experiment metric does not match the preregistered manifest"
                    )
                if registered_at < manifest.preregistered_at:
                    raise ValueError("registered_at cannot precede family preregistration")
                existing = connection.execute(
                    "SELECT spec_json FROM experiment_attempt WHERE experiment_id = ?",
                    (spec.experiment_id,),
                ).fetchone()
                if existing is not None:
                    if existing["spec_json"] != payload:
                        raise ExperimentIdentityConflictError(
                            f"experiment_id {spec.experiment_id} has conflicting content"
                        )
                    connection.rollback()
                    return self.get_attempt(spec.experiment_id)

                connection.execute(
                    """
                    INSERT INTO experiment_attempt(
                        experiment_id, hypothesis_family, spec_json, status, registered_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        spec.experiment_id,
                        spec.hypothesis_family,
                        payload,
                        ExperimentStatus.REGISTERED.value,
                        _utc_iso(registered_at),
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self.get_attempt(spec.experiment_id)

    def start_attempt(self, experiment_id: str, *, started_at: datetime) -> ExperimentAttempt:
        started_at = normalize_aware_utc(started_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_attempt_row(connection, experiment_id)
                status = ExperimentStatus(row["status"])
                if status is ExperimentStatus.REGISTERED:
                    if started_at < _parse_utc(row["registered_at"]):  # type: ignore[operator]
                        raise ValueError("started_at cannot precede registered_at")
                    connection.execute(
                        """
                        UPDATE experiment_attempt SET status = ?, started_at = ?
                        WHERE experiment_id = ? AND status = ?
                        """,
                        (
                            ExperimentStatus.RUNNING.value,
                            _utc_iso(started_at),
                            experiment_id,
                            ExperimentStatus.REGISTERED.value,
                        ),
                    )
                    connection.commit()
                elif status is ExperimentStatus.RUNNING:
                    if _parse_utc(row["started_at"]) != started_at:
                        raise ExperimentIdentityConflictError(
                            "running attempt has a different started_at"
                        )
                    connection.rollback()
                else:
                    connection.rollback()
            except BaseException:
                connection.rollback()
                raise
        return self.get_attempt(experiment_id)

    def record_success(
        self,
        outcome: ExperimentOutcome,
        *,
        completed_at: datetime,
    ) -> ExperimentOutcome:
        outcome = ExperimentOutcome.model_validate(outcome.model_dump(mode="python"))
        if outcome.adjusted_p_value is not None:
            raise ValueError("adjusted p-values may only be written by family adjustment")
        completed_at = normalize_aware_utc(completed_at)
        payload = _json_payload(outcome)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_attempt_row(connection, outcome.experiment_id)
                status = ExperimentStatus(row["status"])
                if status is ExperimentStatus.SUCCEEDED:
                    existing = connection.execute(
                        "SELECT outcome_json FROM experiment_outcome WHERE experiment_id = ?",
                        (outcome.experiment_id,),
                    ).fetchone()
                    if (
                        existing is None
                        or existing["outcome_json"] != payload
                        or _parse_utc(row["completed_at"]) != completed_at
                    ):
                        raise TerminalExperimentError("succeeded outcome is immutable")
                    connection.rollback()
                    return self.get_attempt(outcome.experiment_id).outcome  # type: ignore[return-value]
                if status in {ExperimentStatus.FAILED, ExperimentStatus.CANCELLED}:
                    raise TerminalExperimentError("terminal attempt cannot become succeeded")
                if status is not ExperimentStatus.RUNNING:
                    raise ExperimentRegistryError("attempt must be running before success")
                if completed_at < _parse_utc(row["started_at"]):  # type: ignore[operator]
                    raise ValueError("completed_at cannot precede started_at")

                family = row["hypothesis_family"]
                manifest = self._required_manifest(connection, family)
                if outcome.experiment_id not in manifest.experiment_ids:
                    raise IncompleteHypothesisFamilyError(
                        "experiment is outside the preregistered family manifest"
                    )
                if outcome.attempted_configuration_count != manifest.hypothesis_count:
                    raise IncompleteHypothesisFamilyError(
                        "attempted_configuration_count must equal the preregistered manifest size"
                    )
                spec = ExperimentSpec.model_validate_json(row["spec_json"])
                evidence = outcome.outer_evidence
                if evidence is not None:
                    if evidence.evaluation_range != spec.frozen_outer_test_range:
                        raise ExperimentRegistryError(
                            "outer evidence range does not match the frozen outer test range"
                        )
                    if (
                        evidence.metric_definition_fingerprint
                        != manifest.metric_definition_fingerprint
                    ):
                        raise ExperimentRegistryError(
                            "outer evidence metric does not match the family manifest"
                        )
                    if evidence.available_at > completed_at:
                        raise ExperimentRegistryError(
                            "outer evidence was not available at experiment completion"
                        )
                rank_owner = connection.execute(
                    """
                    SELECT o.experiment_id
                    FROM experiment_outcome AS o
                    JOIN experiment_attempt AS a USING(experiment_id)
                    WHERE a.hypothesis_family = ? AND o.selected_rank = ?
                    """,
                    (family, outcome.selected_rank),
                ).fetchone()
                if rank_owner is not None:
                    raise ExperimentRegistryError("selected_rank must be unique within a family")

                connection.execute(
                    """
                    INSERT INTO experiment_outcome(
                        experiment_id, outcome_json, attempted_configuration_count,
                        selected_rank, raw_p_value
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        outcome.experiment_id,
                        payload,
                        outcome.attempted_configuration_count,
                        outcome.selected_rank,
                        format(outcome.raw_p_value, "f"),
                    ),
                )
                connection.execute(
                    """
                    UPDATE experiment_attempt
                    SET status = ?, completed_at = ?
                    WHERE experiment_id = ? AND status = ?
                    """,
                    (
                        ExperimentStatus.SUCCEEDED.value,
                        _utc_iso(completed_at),
                        outcome.experiment_id,
                        ExperimentStatus.RUNNING.value,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return outcome

    def record_failure(
        self,
        experiment_id: str,
        *,
        first_error: str,
        completed_at: datetime,
    ) -> ExperimentAttempt:
        return self._record_unsuccessful(
            experiment_id,
            status=ExperimentStatus.FAILED,
            first_error=first_error,
            completed_at=completed_at,
        )

    def cancel_attempt(
        self,
        experiment_id: str,
        *,
        first_error: str,
        completed_at: datetime,
    ) -> ExperimentAttempt:
        return self._record_unsuccessful(
            experiment_id,
            status=ExperimentStatus.CANCELLED,
            first_error=first_error,
            completed_at=completed_at,
        )

    def _record_unsuccessful(
        self,
        experiment_id: str,
        *,
        status: ExperimentStatus,
        first_error: str,
        completed_at: datetime,
    ) -> ExperimentAttempt:
        first_error = first_error.strip()
        if not first_error:
            raise ValueError("first_error must not be empty")
        completed_at = normalize_aware_utc(completed_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_attempt_row(connection, experiment_id)
                current = ExperimentStatus(row["status"])
                if current is status:
                    if (
                        row["first_error"] != first_error
                        or _parse_utc(row["completed_at"]) != completed_at
                    ):
                        raise TerminalExperimentError("terminal failure evidence is immutable")
                    connection.rollback()
                    return self.get_attempt(experiment_id)
                if current in {
                    ExperimentStatus.SUCCEEDED,
                    ExperimentStatus.FAILED,
                    ExperimentStatus.CANCELLED,
                }:
                    raise TerminalExperimentError("terminal attempt evidence is immutable")
                reference = _parse_utc(row["started_at"]) or _parse_utc(row["registered_at"])
                if completed_at < reference:  # type: ignore[operator]
                    raise ValueError("completed_at cannot precede attempt activity")
                connection.execute(
                    """
                    UPDATE experiment_attempt
                    SET status = ?, completed_at = ?, first_error = ?
                    WHERE experiment_id = ?
                    """,
                    (status.value, _utc_iso(completed_at), first_error, experiment_id),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self.get_attempt(experiment_id)

    def get_attempt(self, experiment_id: str) -> ExperimentAttempt:
        with self._connect() as connection:
            row = self._required_attempt_row(connection, experiment_id)
            return self._attempt_from_row(connection, row)

    def list_family_attempts(self, hypothesis_family: str) -> tuple[ExperimentAttempt, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM experiment_attempt
                WHERE hypothesis_family = ? ORDER BY experiment_id
                """,
                (hypothesis_family,),
            ).fetchall()
            return tuple(self._attempt_from_row(connection, row) for row in rows)

    def adjust_hypothesis_family(
        self,
        hypothesis_family: str,
        *,
        adjusted_at: datetime,
    ) -> tuple[ExperimentOutcome, ...]:
        adjusted_at = normalize_aware_utc(adjusted_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                attempt_rows = connection.execute(
                    """
                    SELECT * FROM experiment_attempt
                    WHERE hypothesis_family = ? ORDER BY experiment_id
                    """,
                    (hypothesis_family,),
                ).fetchall()
                if not attempt_rows:
                    raise IncompleteHypothesisFamilyError("hypothesis family is empty")
                manifest = self._required_manifest(connection, hypothesis_family)
                attempt_ids = {row["experiment_id"] for row in attempt_rows}
                if attempt_ids != set(manifest.experiment_ids):
                    raise IncompleteHypothesisFamilyError(
                        "registered attempts do not match the preregistered family manifest"
                    )
                if any(
                    ExperimentStatus(row["status"])
                    in {ExperimentStatus.REGISTERED, ExperimentStatus.RUNNING}
                    for row in attempt_rows
                ):
                    raise IncompleteHypothesisFamilyError(
                        "hypothesis family still has non-terminal attempts"
                    )
                expected_count = manifest.hypothesis_count
                completed_times = [
                    _parse_utc(row["completed_at"])
                    for row in attempt_rows
                    if row["completed_at"] is not None
                ]
                if completed_times and adjusted_at < max(completed_times):  # type: ignore[arg-type]
                    raise ValueError("adjusted_at cannot precede family completion")
                outcome_rows = connection.execute(
                    """
                    SELECT o.*
                    FROM experiment_outcome AS o
                    JOIN experiment_attempt AS a USING(experiment_id)
                    WHERE a.hypothesis_family = ?
                    ORDER BY o.experiment_id
                    """,
                    (hypothesis_family,),
                ).fetchall()
                succeeded_count = sum(
                    ExperimentStatus(row["status"]) is ExperimentStatus.SUCCEEDED
                    for row in attempt_rows
                )
                if not outcome_rows or len(outcome_rows) != succeeded_count:
                    raise IncompleteHypothesisFamilyError(
                        "successful family attempts are missing outcome evidence"
                    )
                existing = connection.execute(
                    """
                    SELECT experiment_id, adjusted_p_value, adjusted_at
                    FROM family_adjustment WHERE hypothesis_family = ?
                    """,
                    (hypothesis_family,),
                ).fetchall()
                if existing:
                    if len(existing) != len(outcome_rows) or any(
                        _parse_utc(row["adjusted_at"]) != adjusted_at for row in existing
                    ):
                        raise TerminalExperimentError("family adjustment evidence is immutable")
                    connection.rollback()
                    return self._family_outcomes(hypothesis_family)

                adjusted = self._benjamini_hochberg(
                    tuple(
                        (row["experiment_id"], Decimal(row["raw_p_value"])) for row in outcome_rows
                    ),
                    hypothesis_count=expected_count,
                )
                connection.executemany(
                    """
                    INSERT INTO family_adjustment(
                        experiment_id, hypothesis_family, adjusted_p_value, adjusted_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        (
                            experiment_id,
                            hypothesis_family,
                            format(value, "f"),
                            _utc_iso(adjusted_at),
                        )
                        for experiment_id, value in adjusted.items()
                    ],
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self._family_outcomes(hypothesis_family)

    @staticmethod
    def _benjamini_hochberg(
        p_values: tuple[tuple[str, Decimal], ...],
        *,
        hypothesis_count: int,
    ) -> dict[str, Decimal]:
        ordered = sorted(p_values, key=lambda item: (item[1], item[0]))
        adjusted: dict[str, Decimal] = {}
        running = Decimal("1")
        for rank in range(len(ordered), 0, -1):
            experiment_id, raw = ordered[rank - 1]
            candidate = min(Decimal("1"), raw * hypothesis_count / rank)
            running = min(running, candidate)
            adjusted[experiment_id] = running
        return adjusted

    def evaluate_promotion(
        self,
        stage: PromotionStage,
        *,
        experiment_ids: tuple[str, ...],
        evidence_artifact_hash: str,
        decided_at: datetime,
        forward_evidence: ForwardArtifactEvidence | None = None,
    ) -> PromotionDecision:
        stage = PromotionStage(stage)
        experiment_ids = tuple(sorted(set(experiment_ids)))
        if not experiment_ids:
            raise ValueError("promotion requires at least one experiment")
        decided_at = normalize_aware_utc(decided_at)

        attempts = tuple(self.get_attempt(experiment_id) for experiment_id in experiment_ids)
        families = {attempt.spec.hypothesis_family for attempt in attempts}
        if len(families) != 1:
            raise ExperimentRegistryError("promotion experiments must belong to one family")
        manifest = self.get_hypothesis_family(next(iter(families)))
        relevant_times = [
            attempt.completed_at for attempt in attempts if attempt.completed_at is not None
        ]
        decisions = tuple(
            decision
            for decision in self.list_promotion_decisions()
            if decision.experiment_ids == experiment_ids
        )
        relevant_times.extend(decision.decided_at for decision in decisions)
        with self._connect() as connection:
            adjustment_rows = connection.execute(
                f"""
                SELECT adjusted_at FROM family_adjustment
                WHERE experiment_id IN ({",".join("?" for _ in experiment_ids)})
                """,
                experiment_ids,
            ).fetchall()
        relevant_times.extend(_parse_utc(row["adjusted_at"]) for row in adjustment_rows)
        visible_times = [time for time in relevant_times if time is not None]
        if visible_times and decided_at < max(visible_times):
            raise ValueError("decided_at cannot precede experiment or governance evidence")

        if forward_evidence is not None:
            forward_evidence = ForwardArtifactEvidence.model_validate(
                forward_evidence.model_dump(mode="python")
            )
            if forward_evidence.available_at > decided_at:
                raise ValueError("forward evidence was not available at decided_at")
            if forward_evidence.artifact_hash != evidence_artifact_hash:
                raise ExperimentRegistryError(
                    "forward evidence artifact does not match promotion evidence"
                )
            if (
                forward_evidence.metric_definition_fingerprint
                != manifest.metric_definition_fingerprint
            ):
                raise ExperimentRegistryError(
                    "forward evidence metric does not match the family manifest"
                )

        failures: list[str] = []
        outcomes = tuple(attempt.outcome for attempt in attempts if attempt.outcome is not None)
        if stage is not PromotionStage.EXPLORATORY:
            if len(outcomes) != len(attempts) or any(
                attempt.status is not ExperimentStatus.SUCCEEDED for attempt in attempts
            ):
                failures.append("experiment_not_succeeded")
            if any(not outcome.outer_test_completed for outcome in outcomes):
                failures.append("outer_test_incomplete")
            if any(outcome.trade_count < self.minimum_comparable_trades for outcome in outcomes):
                failures.append("insufficient_trade_count")

        if stage in {PromotionStage.PAPER_CANDIDATE, PromotionStage.MONITOR_APPROVED}:
            required_prior = (
                PromotionStage.COMPARABLE
                if stage is PromotionStage.PAPER_CANDIDATE
                else PromotionStage.PAPER_CANDIDATE
            )
            if not self._has_approved_stage(
                required_prior,
                experiment_ids,
                no_later_than=decided_at,
            ):
                failures.append(f"{required_prior.value}_approval_missing")
            if any(
                outcome.adjusted_p_value is None
                or outcome.adjusted_p_value > self.significance_level
                for outcome in outcomes
            ):
                failures.append("adjusted_significance_failed")
            if any(outcome.confidence_lower <= 0 for outcome in outcomes):
                failures.append("non_positive_confidence_lower_bound")
            if any(outcome.net_return <= 0 for outcome in outcomes):
                failures.append("non_positive_cost_adjusted_return")

        if stage is PromotionStage.MONITOR_APPROVED:
            if forward_evidence is None:
                failures.append("forward_evidence_missing")
            else:
                paper_approvals = tuple(
                    decision
                    for decision in decisions
                    if decision.stage is PromotionStage.PAPER_CANDIDATE
                    and decision.approved
                    and decision.policy_fingerprint == self.policy.policy_fingerprint
                    and decision.decided_at <= decided_at
                )
                if paper_approvals:
                    selection_date = (
                        min(decision.decided_at for decision in paper_approvals)
                        .astimezone(SHANGHAI)
                        .date()
                    )
                    if forward_evidence.observation_range.start_date <= selection_date:
                        raise ExperimentRegistryError(
                            "forward observation must start after paper candidate selection"
                        )
                if forward_evidence.trading_days < self.minimum_forward_days:
                    failures.append("insufficient_forward_days")
                if forward_evidence.fill_count < self.minimum_forward_fills:
                    failures.append("insufficient_forward_fills")
                if forward_evidence.net_return <= 0:
                    failures.append("non_positive_forward_return")
                if forward_evidence.max_drawdown > self.maximum_forward_drawdown:
                    failures.append("forward_drawdown_budget_exceeded")

        forward_trading_days = forward_evidence.trading_days if forward_evidence else 0
        forward_fills = forward_evidence.fill_count if forward_evidence else 0

        decision = PromotionDecision(
            stage=stage,
            experiment_ids=experiment_ids,
            evidence_artifact_hash=evidence_artifact_hash,
            decided_at=decided_at,
            approved=not failures,
            gate_failures=tuple(failures),
            minimum_trade_count=self.minimum_comparable_trades,
            significance_level=self.significance_level,
            forward_trading_days=forward_trading_days,
            forward_fills=forward_fills,
            minimum_forward_days=self.minimum_forward_days,
            minimum_forward_fills=self.minimum_forward_fills,
            maximum_forward_drawdown=self.maximum_forward_drawdown,
            policy_fingerprint=self.policy.policy_fingerprint,
            forward_evidence_artifact_hash=(
                forward_evidence.artifact_hash if forward_evidence else None
            ),
            forward_net_return=(forward_evidence.net_return if forward_evidence else None),
            forward_max_drawdown=(forward_evidence.max_drawdown if forward_evidence else None),
        )
        payload = _json_payload(decision)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT payload_json FROM promotion_decision WHERE decision_id = ?",
                    (decision.decision_id,),
                ).fetchone()
                if existing is not None:
                    if existing["payload_json"] != payload:
                        raise ExperimentIdentityConflictError(
                            "promotion decision id has conflicting content"
                        )
                    connection.rollback()
                    return decision
                connection.execute(
                    """
                    INSERT INTO promotion_decision(
                        decision_id, stage, approved, decided_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        decision.decision_id,
                        stage.value,
                        int(decision.approved),
                        _utc_iso(decided_at),
                        payload,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return decision

    def list_promotion_decisions(self) -> tuple[PromotionDecision, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM promotion_decision ORDER BY decided_at, decision_id"
            ).fetchall()
        return tuple(PromotionDecision.model_validate_json(row["payload_json"]) for row in rows)

    @staticmethod
    def _required_attempt_row(connection: sqlite3.Connection, experiment_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM experiment_attempt WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown experiment_id: {experiment_id}")
        return row

    @staticmethod
    def _required_manifest(
        connection: sqlite3.Connection, hypothesis_family: str
    ) -> HypothesisFamilyManifest:
        row = connection.execute(
            "SELECT payload_json FROM hypothesis_family_manifest WHERE hypothesis_family = ?",
            (hypothesis_family,),
        ).fetchone()
        if row is None:
            raise IncompleteHypothesisFamilyError(
                f"hypothesis family {hypothesis_family!r} is not preregistered"
            )
        return HypothesisFamilyManifest.model_validate_json(row["payload_json"])

    def _attempt_from_row(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> ExperimentAttempt:
        outcome_row = connection.execute(
            """
            SELECT o.outcome_json, a.adjusted_p_value
            FROM experiment_outcome AS o
            LEFT JOIN family_adjustment AS a USING(experiment_id)
            WHERE o.experiment_id = ?
            """,
            (row["experiment_id"],),
        ).fetchone()
        outcome: ExperimentOutcome | None = None
        if outcome_row is not None:
            outcome = ExperimentOutcome.model_validate_json(outcome_row["outcome_json"])
            adjusted = (
                Decimal(outcome_row["adjusted_p_value"])
                if outcome_row["adjusted_p_value"] is not None
                else None
            )
            outcome = _adjusted_outcome(outcome, adjusted)
        return ExperimentAttempt(
            spec=ExperimentSpec.model_validate_json(row["spec_json"]),
            status=ExperimentStatus(row["status"]),
            registered_at=_parse_utc(row["registered_at"]),
            started_at=_parse_utc(row["started_at"]),
            completed_at=_parse_utc(row["completed_at"]),
            first_error=row["first_error"],
            outcome=outcome,
        )

    @staticmethod
    def _family_targets(connection: sqlite3.Connection, hypothesis_family: str) -> set[int]:
        return {
            int(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT o.attempted_configuration_count
                FROM experiment_outcome AS o
                JOIN experiment_attempt AS a USING(experiment_id)
                WHERE a.hypothesis_family = ?
                """,
                (hypothesis_family,),
            ).fetchall()
        }

    def _family_outcomes(self, hypothesis_family: str) -> tuple[ExperimentOutcome, ...]:
        return tuple(
            attempt.outcome
            for attempt in self.list_family_attempts(hypothesis_family)
            if attempt.outcome is not None
        )

    def _has_approved_stage(
        self,
        stage: PromotionStage,
        experiment_ids: tuple[str, ...],
        *,
        no_later_than: datetime,
    ) -> bool:
        return any(
            decision.stage is stage
            and decision.approved
            and decision.experiment_ids == experiment_ids
            and decision.decided_at <= no_later_than
            and decision.policy_fingerprint == self.policy.policy_fingerprint
            for decision in self.list_promotion_decisions()
        )
