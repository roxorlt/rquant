"""Immutable experiment evidence and append-only promotion governance."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Annotated, Literal, Self
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.artifact_retention import (
    PrivateSqlitePathAuthority,
    close_verified_sqlite_connection,
    execute_sqlite_setup_statement,
    raise_preserving_cleanup_errors,
    verified_sqlite_connection_scope,
)
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
    EXECUTED = "executed"
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
    strategy_executable_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
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


class FormalExperimentPlan(RuntimeContractModel):
    """Immutable, preregistered experiment identity eligible for formal submission."""

    schema_version: Literal[1, 2] = 1
    plan_id: Sha256 | None = None
    spec: ExperimentSpec
    hypothesis_variant: str = Field(min_length=1)
    strategy_definition_fingerprint: Sha256 | None = None
    definition_registration_record_hash: Sha256 | None = None
    preregistered_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        definition_receipt = (
            self.strategy_definition_fingerprint,
            self.definition_registration_record_hash,
        )
        if self.schema_version == 2 and any(value is None for value in definition_receipt):
            raise ValueError("current formal plan requires complete Definition Registry receipts")
        if self.schema_version == 1 and any(value is not None for value in definition_receipt):
            raise ValueError("legacy formal plan cannot carry current definition receipts")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"plan_id"}))
        if self.plan_id is None:
            object.__setattr__(self, "plan_id", expected)
        elif self.plan_id != expected:
            raise ValueError("plan_id does not match canonical formal experiment plan")
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
        elif self.status is ExperimentStatus.EXECUTED:
            if self.started_at is None or self.completed_at is None:
                raise ValueError("executed attempt requires timing evidence")
            if self.first_error is not None or self.outcome is not None:
                raise ValueError("executed attempt cannot contain statistical evidence")
        elif self.status is ExperimentStatus.SUCCEEDED:
            if self.started_at is None or self.completed_at is None or self.outcome is None:
                raise ValueError("succeeded attempt requires timing and outcome evidence")
            if self.first_error is not None:
                raise ValueError("succeeded attempt cannot contain first_error")
        else:
            if self.completed_at is None or not self.first_error or self.outcome is not None:
                raise ValueError("failed or cancelled attempt requires first_error and completion")
        return self


class ExperimentSubmissionIntent(RuntimeContractModel):
    """Durable outbox payload atomically owned by an experiment attempt."""

    schema_version: Literal[1, 2] = 1
    request_id: UUID
    job_id: UUID
    experiment_id: Sha256
    attempt_identity: Sha256
    hypothesis_variant: str = Field(min_length=1)
    formal_plan_id: Sha256 | None = None
    strategy_definition_fingerprint: Sha256 | None = None
    definition_registration_record_hash: Sha256 | None = None
    command_content_hash: Sha256
    envelope_json: str = Field(min_length=2)
    envelope_sha256: Sha256

    @model_validator(mode="after")
    def validate_canonical_envelope(self) -> Self:
        formal_receipts = (
            self.formal_plan_id,
            self.strategy_definition_fingerprint,
            self.definition_registration_record_hash,
        )
        if self.schema_version == 2 and any(value is None for value in formal_receipts):
            raise ValueError("current experiment submission requires complete formal receipts")
        if self.schema_version == 1 and any(value is not None for value in formal_receipts):
            raise ValueError("legacy experiment submission cannot carry current formal receipts")
        try:
            parsed = json.loads(self.envelope_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("envelope_json must be canonical JSON") from exc
        canonical = json.dumps(
            parsed,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if canonical != self.envelope_json:
            raise ValueError("envelope_json must use exact canonical JSON")
        expected = canonical_sha256({"canonical_envelope_json": canonical})
        if self.envelope_sha256 != expected:
            raise ValueError("envelope_sha256 does not match canonical envelope JSON")
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


class PromotionDecisionReadSnapshot(RuntimeContractModel):
    """One bounded point-in-time view of the append-only promotion ledger."""

    decisions: tuple[PromotionDecision, ...] = ()
    sequence: int = Field(ge=0)
    event_time: AwareUtcDatetime | None = None


def _validate_readonly_registry_schema(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT type FROM sqlite_master WHERE name = 'promotion_decision'"
    ).fetchone()
    if row is None or row[0] != "table":
        raise ExperimentRegistryError("promotion_decision table is missing")
    columns = tuple(
        item[1] for item in connection.execute("PRAGMA table_info(promotion_decision)").fetchall()
    )
    expected = ("decision_id", "stage", "approved", "decided_at", "payload_json")
    if columns != expected:
        raise ExperimentRegistryError("promotion_decision schema is incompatible")


def _bind_readonly_registry_authority(
    path: Path,
    *,
    managed_trust_root: Path,
) -> PrivateSqlitePathAuthority:
    transient_error: ValueError | None = None
    for _attempt in range(3):
        try:
            return PrivateSqlitePathAuthority(
                path,
                label="experiment registry",
                create_if_missing=False,
                managed_trust_root=managed_trust_root,
            )
        except ValueError as exc:
            if str(exc) != "experiment registry managed trust root identity changed":
                raise
            transient_error = exc
    assert transient_error is not None
    raise transient_error


class ExperimentRegistryReadonlyReader:
    """Read promotion governance without initializing or mutating its SQLite ledger."""

    def __init__(
        self,
        path: Path,
        *,
        managed_trust_root: Path,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self._path_authority = _bind_readonly_registry_authority(
            path,
            managed_trust_root=managed_trust_root,
        )
        self.path = self._path_authority.path
        self.busy_timeout_ms = busy_timeout_ms

    @contextmanager
    def _read_snapshot(self) -> Iterator[sqlite3.Connection]:
        try:
            _ = self._path_authority.database_generation
            has_wal_sidecars = any(self._path_authority.sqlite_sidecar_state())
            immutable_generation = (
                None if has_wal_sidecars else self._path_authority.begin_immutable_read()
            )
        except ValueError as exc:
            if not self.path.exists():
                message = f"experiment registry does not exist: {self.path}"
            else:
                message = "experiment registry path changed, has an active WAL, or is not quiescent"
            raise ExperimentRegistryError(message) from exc
        uri = (
            self._path_authority.readonly_uri()
            if has_wal_sidecars
            else self._path_authority.immutable_readonly_uri()
        )
        try:
            connection = self._path_authority.open_verified_connection(
                lambda _path: sqlite3.connect(
                    uri,
                    uri=True,
                    timeout=self.busy_timeout_ms / 1_000,
                    isolation_level=None,
                )
            )
        except (sqlite3.Error, ValueError) as exc:
            raise ExperimentRegistryError(
                "experiment registry path is unsafe while opening read-only"
            ) from exc
        with verified_sqlite_connection_scope(connection, self._path_authority):
            try:
                connection.row_factory = sqlite3.Row
                execute_sqlite_setup_statement(
                    connection,
                    f"PRAGMA busy_timeout = {self.busy_timeout_ms}",
                )
                execute_sqlite_setup_statement(connection, "PRAGMA query_only = ON")
                execute_sqlite_setup_statement(connection, "PRAGMA trusted_schema = OFF")
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                _validate_readonly_registry_schema(connection)
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                connection.execute("BEGIN")
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                self._assert_read_generation(immutable_generation)
                yield connection
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                self._assert_read_generation(immutable_generation)
                connection.execute("COMMIT")
                self._assert_read_generation(immutable_generation)
            except ValueError as exc:
                if connection.in_transaction:
                    connection.rollback()
                raise ExperimentRegistryError(
                    "experiment registry path changed during read"
                ) from exc
            except sqlite3.Error as exc:
                if connection.in_transaction:
                    connection.rollback()
                raise ExperimentRegistryError("experiment registry read failed") from exc
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def _assert_read_generation(
        self,
        immutable_generation: tuple[int, int, int, int, int] | None,
    ) -> None:
        if immutable_generation is None:
            self._path_authority.assert_current()
        else:
            self._path_authority.assert_immutable_read_current(immutable_generation)

    def read_promotion_decisions(
        self,
        *,
        observed_at: datetime,
        limit: int = 1_000,
    ) -> PromotionDecisionReadSnapshot:
        observed = normalize_aware_utc(observed_at)
        if limit < 1:
            raise ValueError("limit must be positive")
        cutoff = _utc_iso(observed)
        with self._read_snapshot() as connection:
            metadata = connection.execute(
                """
                SELECT COALESCE(MAX(rowid), 0) AS sequence,
                       MAX(decided_at) AS event_time
                FROM promotion_decision
                WHERE decided_at <= ?
                """,
                (cutoff,),
            ).fetchone()
            rows = connection.execute(
                """
                SELECT rowid, decision_id, stage, approved, decided_at, payload_json
                FROM promotion_decision
                WHERE decided_at <= ?
                ORDER BY decided_at DESC, decision_id DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()

        decisions: list[PromotionDecision] = []
        for row in rows:
            try:
                decision = PromotionDecision.model_validate_json(row["payload_json"])
                stored_time = _parse_utc(row["decided_at"])
            except (TypeError, ValueError) as exc:
                raise ExperimentRegistryError("promotion decision evidence is invalid") from exc
            if (
                decision.decision_id != row["decision_id"]
                or decision.stage.value != row["stage"]
                or int(decision.approved) != row["approved"]
                or decision.decided_at != stored_time
            ):
                raise ExperimentRegistryError(
                    "promotion decision payload does not match indexed evidence"
                )
            if decision.decided_at > observed:
                raise ExperimentRegistryError("promotion decision contains future evidence")
            decisions.append(decision)

        decisions.sort(key=lambda item: (item.decided_at, item.decision_id))
        event_time = _parse_utc(metadata["event_time"])
        if event_time is not None and event_time > observed:
            raise ExperimentRegistryError("promotion registry contains future evidence")
        return PromotionDecisionReadSnapshot(
            decisions=tuple(decisions),
            sequence=int(metadata["sequence"]),
            event_time=event_time,
        )

    def list_promotion_decisions(
        self,
        *,
        observed_at: datetime,
        limit: int = 1_000,
    ) -> tuple[PromotionDecision, ...]:
        return self.read_promotion_decisions(
            observed_at=observed_at,
            limit=limit,
        ).decisions

    def get_attempt(self, experiment_id: str) -> ExperimentAttempt:
        """Read one terminal-attempt authority without acquiring a writer."""

        with self._read_snapshot() as connection:
            row = ExperimentRegistry._required_attempt_row(connection, experiment_id)
            return ExperimentRegistry._attempt_from_row(connection, row)

    def resolve_formal_plan(
        self,
        *,
        strategy_spec_fingerprint: str,
        strategy_executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        dataset_snapshot_id: str,
        code_commit: str,
        parameter_fingerprint: str,
        cost_model_fingerprint: str,
        execution_model_fingerprint: str,
        seed: int,
        as_of: datetime,
    ) -> FormalExperimentPlan:
        """Resolve one exact visible formal plan without opening a writer."""

        visible_at = normalize_aware_utc(as_of)
        resolution_key = ExperimentRegistry._formal_plan_resolution_key(
            strategy_spec_fingerprint=strategy_spec_fingerprint,
            strategy_executable_fingerprint=strategy_executable_fingerprint,
            candidate_schema_fingerprint=candidate_schema_fingerprint,
            dataset_snapshot_id=dataset_snapshot_id,
            code_commit=code_commit,
            parameter_fingerprint=parameter_fingerprint,
            cost_model_fingerprint=cost_model_fingerprint,
            execution_model_fingerprint=execution_model_fingerprint,
            seed=seed,
        )
        with self._read_snapshot() as connection:
            rows = connection.execute(
                """
                SELECT plan_json FROM formal_experiment_plan
                WHERE resolution_key = ? AND preregistered_at <= ?
                ORDER BY preregistered_at, plan_id
                """,
                (resolution_key, _utc_iso(visible_at)),
            ).fetchall()
        if len(rows) != 1:
            raise IncompleteHypothesisFamilyError(
                "exactly one visible preregistered formal plan is required"
            )
        return FormalExperimentPlan.model_validate_json(rows[0]["plan_json"])

    def resolve_formal_plan_by_id(
        self,
        plan_id: str,
        *,
        as_of: datetime,
    ) -> FormalExperimentPlan:
        """Read one exact visible plan without exposing mutation APIs."""

        visible_at = normalize_aware_utc(as_of)
        with self._read_snapshot() as connection:
            rows = connection.execute(
                """
                SELECT plan_json FROM formal_experiment_plan
                WHERE plan_id = ? AND preregistered_at <= ?
                """,
                (plan_id, _utc_iso(visible_at)),
            ).fetchall()
        if len(rows) != 1:
            raise IncompleteHypothesisFamilyError(
                "exactly one visible preregistered formal plan is required"
            )
        return FormalExperimentPlan.model_validate_json(rows[0]["plan_json"])


class _ExperimentRegistryConnection(sqlite3.Connection):
    identity_authority: PrivateSqlitePathAuthority | None = None
    _identity_failed: bool = False

    def _assert_identity_current(self) -> None:
        authority = self.identity_authority
        if authority is None:
            return
        try:
            authority.assert_current()
        except BaseException:
            self._identity_failed = True
            raise

    def _rebind_identity_after_sqlite_change(self) -> None:
        authority = self.identity_authority
        if authority is None:
            return
        last_error: BaseException | None = None
        for _attempt in range(3):
            try:
                authority.rebind_and_assert_current_after_trusted_sqlite_change()
                return
            except BaseException as exc:
                last_error = exc
        self._identity_failed = True
        assert last_error is not None
        raise last_error

    def execute(
        self,
        sql: str,
        parameters: tuple[object, ...] = (),
        /,
    ) -> sqlite3.Cursor:
        self._rebind_identity_after_sqlite_change()
        result = super().execute(sql, parameters)
        self._rebind_identity_after_sqlite_change()
        return result

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:
        self._rebind_identity_after_sqlite_change()
        result = super().executescript(sql_script)
        self._rebind_identity_after_sqlite_change()
        return result

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterator[tuple[object, ...]],
        /,
    ) -> sqlite3.Cursor:
        self._rebind_identity_after_sqlite_change()
        result = super().executemany(sql, seq_of_parameters)
        self._rebind_identity_after_sqlite_change()
        return result

    def commit(self) -> None:
        self._rebind_identity_after_sqlite_change()
        super().commit()
        self._rebind_identity_after_sqlite_change()

    def rollback(self) -> None:
        self._rebind_identity_after_sqlite_change()
        super().rollback()
        self._rebind_identity_after_sqlite_change()

    def _close_underlying(self) -> None:
        super().close()

    def close(self, *, primary_error: BaseException | None = None) -> None:
        authority = self.identity_authority
        if authority is None:
            self._close_verified(primary_error=primary_error, authority=None)
            return
        with authority.identity_boundary():
            self._close_verified(primary_error=primary_error, authority=authority)

    def _close_verified(
        self,
        *,
        primary_error: BaseException | None,
        authority: PrivateSqlitePathAuthority | None,
    ) -> None:
        pre_rebind_error: BaseException | None = None
        identity_error: BaseException | None = None
        close_error: BaseException | None = None
        rebind_error: BaseException | None = None
        postcheck_error: BaseException | None = None
        if authority is not None:
            try:
                authority.rebind_ctime_after_trusted_sqlite_setup()
            except BaseException as exc:
                pre_rebind_error = exc
        try:
            self._assert_identity_current()
        except BaseException as exc:
            identity_error = exc
        try:
            self._close_underlying()
        except BaseException as exc:
            close_error = exc
        if authority is not None and pre_rebind_error is None and identity_error is None:
            try:
                authority.rebind_ctime_after_trusted_sqlite_setup()
            except BaseException as exc:
                rebind_error = exc
        try:
            self._assert_identity_current()
        except BaseException as exc:
            postcheck_error = exc
        raise_preserving_cleanup_errors(
            primary_error=primary_error,
            cleanup_errors=[
                *([pre_rebind_error] if pre_rebind_error is not None else []),
                *([identity_error] if identity_error is not None else []),
                *([close_error] if close_error is not None else []),
                *([rebind_error] if rebind_error is not None else []),
                *([postcheck_error] if postcheck_error is not None else []),
            ],
            message="experiment registry operation and close failed",
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        operation_error: BaseException | None = None
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        except BaseException as operation_exc:
            operation_error = operation_exc
        close_primary = exc
        if operation_error is not None:
            close_primary = (
                operation_error
                if exc is None
                else BaseExceptionGroup(
                    "experiment transaction body and completion failed",
                    [exc, operation_error],
                )
            )
        self.close(primary_error=close_primary)
        if operation_error is not None:
            raise close_primary
        return False


class ExperimentRegistry:
    """Serialize experiment evidence and promotion decisions through SQLite WAL."""

    def __init__(
        self,
        path: Path,
        *,
        managed_trust_root: Path,
        minimum_comparable_trades: int = 30,
        significance_level: Decimal = Decimal("0.05"),
        minimum_forward_days: int = 10,
        minimum_forward_fills: int = 20,
        maximum_forward_drawdown: Decimal = Decimal("0.10"),
        busy_timeout_ms: int = 5_000,
        artifact_terminal_hook: Callable[[str, str, datetime], None] | None = None,
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
        self._path_authority = PrivateSqlitePathAuthority(
            path,
            label="experiment registry",
            create_if_missing=True,
            create_parent_if_missing=True,
            managed_trust_root=managed_trust_root,
        )
        self.path = self._path_authority.path
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
        self._artifact_terminal_hook = artifact_terminal_hook
        self._initialize()
        self._path_authority.durably_sync_current_database()

    def _emit_artifact_terminal(self, experiment_id: str, completed_at: datetime) -> None:
        if self._artifact_terminal_hook is not None:
            self._artifact_terminal_hook("experiment", experiment_id, completed_at)

    def _connect(self) -> sqlite3.Connection:
        def open_writable(path: Path) -> sqlite3.Connection:
            connection = sqlite3.connect(
                self._path_authority.writable_uri(),
                uri=True,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
                factory=_ExperimentRegistryConnection,
            )
            assert isinstance(connection, _ExperimentRegistryConnection)
            connection.identity_authority = self._path_authority
            return connection

        connection = self._path_authority.open_verified_connection(open_writable)
        try:
            connection.row_factory = sqlite3.Row
            execute_sqlite_setup_statement(
                connection,
                f"PRAGMA busy_timeout = {self.busy_timeout_ms}",
            )
            execute_sqlite_setup_statement(connection, "PRAGMA foreign_keys = ON")
            execute_sqlite_setup_statement(connection, "PRAGMA journal_mode = WAL")
            execute_sqlite_setup_statement(connection, "PRAGMA synchronous = FULL")
            self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
        except BaseException as exc:
            close_verified_sqlite_connection(
                connection,
                self._path_authority,
                primary_error=exc,
            )
            raise
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
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

                CREATE TABLE IF NOT EXISTS experiment_submission_outbox (
                    request_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL UNIQUE,
                    experiment_id TEXT NOT NULL REFERENCES experiment_attempt(experiment_id),
                    attempt_identity TEXT NOT NULL,
                    intent_json TEXT NOT NULL,
                    command_content_hash TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('prepared', 'published')),
                    prepared_at TEXT NOT NULL,
                    published_at TEXT
                );
                CREATE INDEX IF NOT EXISTS experiment_submission_pending_idx
                    ON experiment_submission_outbox(state, prepared_at, request_id);

                CREATE TABLE IF NOT EXISTS formal_experiment_plan (
                    plan_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL UNIQUE,
                    resolution_key TEXT NOT NULL UNIQUE,
                    preregistered_at TEXT NOT NULL,
                    plan_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS formal_experiment_plan_resolution_idx
                    ON formal_experiment_plan(resolution_key, preregistered_at, plan_id);
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
    def _formal_plan_resolution_key(
        *,
        strategy_spec_fingerprint: str,
        strategy_executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        dataset_snapshot_id: str,
        code_commit: str,
        parameter_fingerprint: str,
        cost_model_fingerprint: str,
        execution_model_fingerprint: str,
        seed: int,
    ) -> str:
        return canonical_sha256(
            {
                "contract": "formal-experiment-resolution/v1",
                "strategy_spec_fingerprint": strategy_spec_fingerprint,
                "strategy_executable_fingerprint": strategy_executable_fingerprint,
                "candidate_schema_fingerprint": candidate_schema_fingerprint,
                "dataset_snapshot_id": dataset_snapshot_id,
                "code_commit": code_commit,
                "parameter_fingerprint": parameter_fingerprint,
                "cost_model_fingerprint": cost_model_fingerprint,
                "execution_model_fingerprint": execution_model_fingerprint,
                "seed": seed,
            }
        )

    def register_formal_plan(
        self,
        plan: FormalExperimentPlan,
        *,
        family_manifest: HypothesisFamilyManifest,
    ) -> FormalExperimentPlan:
        selected = FormalExperimentPlan.model_validate(plan.model_dump(mode="python"))
        manifest = self.register_hypothesis_family(family_manifest)
        spec = selected.spec
        if spec.experiment_id not in manifest.experiment_ids:
            raise IncompleteHypothesisFamilyError(
                "formal plan is outside its preregistered hypothesis family"
            )
        if (
            spec.hypothesis_family != manifest.hypothesis_family
            or spec.metric_definition_fingerprint != manifest.metric_definition_fingerprint
            or selected.preregistered_at < manifest.preregistered_at
        ):
            raise IncompleteHypothesisFamilyError(
                "formal plan conflicts with its preregistered hypothesis family"
            )
        assert selected.plan_id is not None
        assert spec.experiment_id is not None
        resolution_key = self._formal_plan_resolution_key(
            strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
            strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
            candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
            dataset_snapshot_id=spec.dataset_snapshot_id,
            code_commit=spec.code_commit,
            parameter_fingerprint=spec.parameter_fingerprint,
            cost_model_fingerprint=spec.cost_model_fingerprint,
            execution_model_fingerprint=spec.execution_model_fingerprint,
            seed=spec.seed,
        )
        payload = _json_payload(selected)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = connection.execute(
                    """
                    SELECT plan_json FROM formal_experiment_plan
                    WHERE plan_id = ? OR experiment_id = ? OR resolution_key = ?
                    """,
                    (selected.plan_id, spec.experiment_id, resolution_key),
                ).fetchall()
                if rows:
                    if len(rows) != 1 or rows[0]["plan_json"] != payload:
                        raise ExperimentIdentityConflictError(
                            "formal experiment plan identity has conflicting content"
                        )
                    connection.rollback()
                    return selected
                connection.execute(
                    """
                    INSERT INTO formal_experiment_plan(
                        plan_id, experiment_id, resolution_key,
                        preregistered_at, plan_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        selected.plan_id,
                        spec.experiment_id,
                        resolution_key,
                        _utc_iso(selected.preregistered_at),
                        payload,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return selected

    def resolve_formal_plan(
        self,
        *,
        strategy_spec_fingerprint: str,
        strategy_executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        dataset_snapshot_id: str,
        code_commit: str,
        parameter_fingerprint: str,
        cost_model_fingerprint: str,
        execution_model_fingerprint: str,
        seed: int,
        as_of: datetime,
    ) -> FormalExperimentPlan:
        visible_at = normalize_aware_utc(as_of)
        resolution_key = self._formal_plan_resolution_key(
            strategy_spec_fingerprint=strategy_spec_fingerprint,
            strategy_executable_fingerprint=strategy_executable_fingerprint,
            candidate_schema_fingerprint=candidate_schema_fingerprint,
            dataset_snapshot_id=dataset_snapshot_id,
            code_commit=code_commit,
            parameter_fingerprint=parameter_fingerprint,
            cost_model_fingerprint=cost_model_fingerprint,
            execution_model_fingerprint=execution_model_fingerprint,
            seed=seed,
        )
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT plan_json FROM formal_experiment_plan
                WHERE resolution_key = ? AND preregistered_at <= ?
                ORDER BY preregistered_at, plan_id
                """,
                (resolution_key, _utc_iso(visible_at)),
            ).fetchall()
        if len(rows) != 1:
            raise IncompleteHypothesisFamilyError(
                "exactly one visible preregistered formal plan is required"
            )
        return FormalExperimentPlan.model_validate_json(rows[0]["plan_json"])

    def resolve_formal_plan_by_id(
        self,
        plan_id: str,
        *,
        as_of: datetime,
    ) -> FormalExperimentPlan:
        """Read one exact visible plan from this authoritative registry."""

        visible_at = normalize_aware_utc(as_of)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT plan_json FROM formal_experiment_plan
                WHERE plan_id = ? AND preregistered_at <= ?
                """,
                (plan_id, _utc_iso(visible_at)),
            ).fetchone()
        if row is None:
            raise IncompleteHypothesisFamilyError(
                "exact visible preregistered formal plan is required"
            )
        return FormalExperimentPlan.model_validate_json(row["plan_json"])

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
        submission: ExperimentSubmissionIntent | None = None,
    ) -> ExperimentAttempt:
        spec = self._validated_spec(spec)
        registered_at = normalize_aware_utc(registered_at)
        payload = _json_payload(spec)
        if submission is not None:
            submission = ExperimentSubmissionIntent.model_validate(
                submission.model_dump(mode="python")
            )
            expected_attempt_identity = canonical_sha256(
                {
                    "contract": "research-experiment-attempt/v1",
                    "experiment_id": spec.experiment_id,
                    "hypothesis_family": spec.hypothesis_family,
                    "hypothesis_variant": submission.hypothesis_variant,
                }
            )
            if (
                submission.experiment_id != spec.experiment_id
                or submission.attempt_identity != expected_attempt_identity
            ):
                raise ExperimentIdentityConflictError(
                    "submission intent conflicts with immutable experiment ownership"
                )
            if submission.schema_version != 2:
                raise ExperimentIdentityConflictError(
                    "formal job submission requires current formal plan receipts"
                )
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
                if submission is not None:
                    plan_row = connection.execute(
                        """
                        SELECT plan_json FROM formal_experiment_plan
                        WHERE plan_id = ? AND preregistered_at <= ?
                        """,
                        (submission.formal_plan_id, _utc_iso(registered_at)),
                    ).fetchone()
                    if plan_row is None:
                        raise IncompleteHypothesisFamilyError(
                            "exact visible preregistered formal plan is required"
                        )
                    plan = FormalExperimentPlan.model_validate_json(plan_row["plan_json"])
                    if plan.schema_version != 2:
                        raise ExperimentIdentityConflictError(
                            "legacy formal plan cannot own a current job submission"
                        )
                    exact_plan_receipts = (
                        plan.plan_id,
                        plan.spec,
                        plan.hypothesis_variant,
                        plan.strategy_definition_fingerprint,
                        plan.definition_registration_record_hash,
                    )
                    submitted_plan_receipts = (
                        submission.formal_plan_id,
                        spec,
                        submission.hypothesis_variant,
                        submission.strategy_definition_fingerprint,
                        submission.definition_registration_record_hash,
                    )
                    if exact_plan_receipts != submitted_plan_receipts:
                        raise ExperimentIdentityConflictError(
                            "submission intent conflicts with authoritative formal plan receipts"
                        )
                existing = connection.execute(
                    "SELECT spec_json FROM experiment_attempt WHERE experiment_id = ?",
                    (spec.experiment_id,),
                ).fetchone()
                if existing is not None:
                    if existing["spec_json"] != payload:
                        raise ExperimentIdentityConflictError(
                            f"experiment_id {spec.experiment_id} has conflicting content"
                        )
                else:
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
                if submission is not None:
                    intent_json = _json_payload(submission)
                    existing_submission = connection.execute(
                        """
                        SELECT intent_json FROM experiment_submission_outbox
                        WHERE request_id = ? OR job_id = ?
                        """,
                        (str(submission.request_id), str(submission.job_id)),
                    ).fetchall()
                    if existing_submission:
                        if (
                            len(existing_submission) != 1
                            or existing_submission[0]["intent_json"] != intent_json
                        ):
                            raise ExperimentIdentityConflictError(
                                "job submission identity has conflicting immutable content"
                            )
                    else:
                        connection.execute(
                            """
                            INSERT INTO experiment_submission_outbox(
                                request_id, job_id, experiment_id, attempt_identity,
                                intent_json, command_content_hash, state, prepared_at
                            ) VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?)
                            """,
                            (
                                str(submission.request_id),
                                str(submission.job_id),
                                submission.experiment_id,
                                submission.attempt_identity,
                                intent_json,
                                submission.command_content_hash,
                                _utc_iso(registered_at),
                            ),
                        )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self.get_attempt(spec.experiment_id)

    def list_pending_submissions(
        self,
        *,
        limit: int = 100,
    ) -> tuple[ExperimentSubmissionIntent, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("submission outbox limit must be from 1 through 1000")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT intent_json FROM experiment_submission_outbox
                WHERE state = 'prepared'
                ORDER BY prepared_at, request_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(
            ExperimentSubmissionIntent.model_validate_json(row["intent_json"]) for row in rows
        )

    def list_submission_intents(
        self,
        *,
        limit: int = 1_000,
    ) -> tuple[ExperimentSubmissionIntent, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10_000:
            raise ValueError("submission intent limit must be from 1 through 10000")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT intent_json FROM experiment_submission_outbox
                ORDER BY prepared_at, request_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(
            ExperimentSubmissionIntent.model_validate_json(row["intent_json"]) for row in rows
        )

    def list_recoverable_submission_intents(
        self,
        *,
        limit: int = 1_000,
    ) -> tuple[ExperimentSubmissionIntent, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("recoverable submission limit must be from 1 through 1000")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT outbox.intent_json
                FROM experiment_submission_outbox AS outbox
                JOIN experiment_attempt AS attempt
                  ON attempt.experiment_id = outbox.experiment_id
                WHERE outbox.state = 'prepared'
                   OR attempt.status IN (?, ?)
                ORDER BY outbox.prepared_at, outbox.request_id
                LIMIT ?
                """,
                (
                    ExperimentStatus.REGISTERED.value,
                    ExperimentStatus.RUNNING.value,
                    limit,
                ),
            ).fetchall()
        return tuple(
            ExperimentSubmissionIntent.model_validate_json(row["intent_json"]) for row in rows
        )

    def get_submission_intent_for_job(
        self,
        job_id: UUID,
    ) -> ExperimentSubmissionIntent | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT intent_json FROM experiment_submission_outbox
                WHERE job_id = ?
                """,
                (str(job_id),),
            ).fetchone()
        return (
            ExperimentSubmissionIntent.model_validate_json(row["intent_json"])
            if row is not None
            else None
        )

    def mark_submission_published(
        self,
        request_id: UUID,
        *,
        command_content_hash: str,
        published_at: datetime,
    ) -> None:
        published_at = normalize_aware_utc(published_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT command_content_hash, state, prepared_at, published_at
                    FROM experiment_submission_outbox WHERE request_id = ?
                    """,
                    (str(request_id),),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown experiment submission request: {request_id}")
                if row["command_content_hash"] != command_content_hash:
                    raise ExperimentIdentityConflictError(
                        "published command hash conflicts with experiment outbox"
                    )
                if published_at < _parse_utc(row["prepared_at"]):  # type: ignore[operator]
                    raise ValueError("published_at cannot precede outbox preparation")
                if row["state"] == "published":
                    connection.rollback()
                    return
                connection.execute(
                    """
                    UPDATE experiment_submission_outbox
                    SET state = 'published', published_at = ?
                    WHERE request_id = ? AND state = 'prepared'
                    """,
                    (_utc_iso(published_at), str(request_id)),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

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

    def ensure_attempt_started(
        self,
        experiment_id: str,
        *,
        started_at: datetime,
    ) -> ExperimentAttempt:
        """Start once; retries retain the first immutable experiment start time."""

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
                    existing_outcome = self.get_attempt(outcome.experiment_id).outcome
                    self._emit_artifact_terminal(outcome.experiment_id, completed_at)
                    return existing_outcome  # type: ignore[return-value]
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
        self._emit_artifact_terminal(outcome.experiment_id, completed_at)
        return outcome

    def record_execution_completed(
        self,
        experiment_id: str,
        *,
        completed_at: datetime,
    ) -> ExperimentAttempt:
        """Seal successful compute without inventing statistical outcome evidence."""

        completed_at = normalize_aware_utc(completed_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_attempt_row(connection, experiment_id)
                status = ExperimentStatus(row["status"])
                if status is ExperimentStatus.EXECUTED:
                    if _parse_utc(row["completed_at"]) != completed_at:
                        raise TerminalExperimentError("executed attempt completion is immutable")
                    connection.rollback()
                    return self.get_attempt(experiment_id)
                if status in {
                    ExperimentStatus.SUCCEEDED,
                    ExperimentStatus.FAILED,
                    ExperimentStatus.CANCELLED,
                }:
                    raise TerminalExperimentError(
                        "terminal attempt cannot accept execution completion"
                    )
                if status is not ExperimentStatus.RUNNING:
                    raise ExperimentRegistryError(
                        "attempt must be running before execution completion"
                    )
                started_at = _parse_utc(row["started_at"])
                if started_at is None or completed_at < started_at:
                    raise ValueError("completed_at cannot precede started_at")
                connection.execute(
                    """
                    UPDATE experiment_attempt
                    SET status = ?, completed_at = ?
                    WHERE experiment_id = ? AND status = ?
                    """,
                    (
                        ExperimentStatus.EXECUTED.value,
                        _utc_iso(completed_at),
                        experiment_id,
                        ExperimentStatus.RUNNING.value,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self.get_attempt(experiment_id)

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
                    existing = self.get_attempt(experiment_id)
                    self._emit_artifact_terminal(experiment_id, completed_at)
                    return existing
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
        result = self.get_attempt(experiment_id)
        self._emit_artifact_terminal(experiment_id, completed_at)
        return result

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

    def read_promotion_decisions(
        self,
        *,
        observed_at: datetime,
        limit: int = 1_000,
    ) -> PromotionDecisionReadSnapshot:
        """Read the promotion ledger through this lifecycle-owned authority.

        Serving publishers receive this narrow method from the live registry
        composition.  They do not reopen an independent read-only registry
        handle whose identity lifecycle could drift from terminal hooks.
        """

        observed = normalize_aware_utc(observed_at)
        if limit < 1:
            raise ValueError("limit must be positive")
        cutoff = _utc_iso(observed)
        with self._connect() as connection:
            metadata = connection.execute(
                """
                SELECT COALESCE(MAX(rowid), 0) AS sequence,
                       MAX(decided_at) AS event_time
                FROM promotion_decision
                WHERE decided_at <= ?
                """,
                (cutoff,),
            ).fetchone()
            rows = connection.execute(
                """
                SELECT rowid, decision_id, stage, approved, decided_at, payload_json
                FROM promotion_decision
                WHERE decided_at <= ?
                ORDER BY decided_at DESC, decision_id DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()

        decisions: list[PromotionDecision] = []
        for row in rows:
            try:
                decision = PromotionDecision.model_validate_json(row["payload_json"])
                stored_time = _parse_utc(row["decided_at"])
            except (TypeError, ValueError) as exc:
                raise ExperimentRegistryError("promotion decision evidence is invalid") from exc
            if (
                decision.decision_id != row["decision_id"]
                or decision.stage.value != row["stage"]
                or int(decision.approved) != row["approved"]
                or decision.decided_at != stored_time
            ):
                raise ExperimentRegistryError(
                    "promotion decision payload does not match indexed evidence"
                )
            if decision.decided_at > observed:
                raise ExperimentRegistryError("promotion decision contains future evidence")
            decisions.append(decision)

        decisions.sort(key=lambda item: (item.decided_at, item.decision_id))
        event_time = _parse_utc(metadata["event_time"])
        if event_time is not None and event_time > observed:
            raise ExperimentRegistryError("promotion registry contains future evidence")
        return PromotionDecisionReadSnapshot(
            decisions=tuple(decisions),
            sequence=int(metadata["sequence"]),
            event_time=event_time,
        )

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

    @staticmethod
    def _attempt_from_row(connection: sqlite3.Connection, row: sqlite3.Row) -> ExperimentAttempt:
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
