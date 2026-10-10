"""Typed, bounded detail material for the original runtime-health owner.

The owner supplies an already verified Ops context and one heartbeat read. This
module verifies bindings; it does not collect Ops facts, verify an installation,
read a file, or calculate business metrics. An optional witness must be saved by
the original producer at start. A current collector context cannot create one for
an existing heartbeat. Publication remains off until the reader is installed.

Keep the original heartbeat imports local: its file model can later import the
independent startup-witness type without changing its frozen wire projection.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import (
    BeforeValidator,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import (
    canonical_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

if TYPE_CHECKING:
    from rquant.runtime_service_control import (
        RuntimeServiceHealth,
        RuntimeServiceHeartbeat,
        RuntimeServiceSpec,
    )
    from rquant.runtime_serving_snapshot import SourceReadResult

Sha256 = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]
Text = Annotated[str, StringConstraints(strict=True, min_length=1)]
Count = Annotated[int, Field(strict=True, ge=0)]
PositiveCount = Annotated[int, Field(strict=True, ge=1)]
Scalar = StrictStr | StrictInt | StrictFloat | StrictBool | None
_OPS_FRESHNESS = timedelta(seconds=120)
_MAX_CELL_BYTES = 64 * 1024
_MAX_CONTEXT_BYTES = 4 * 1024
_MAX_OWNER_BYTES = 7 * 1024 * 1024
_MAX_SERVICES = 500
_CONTEXT_TABLE = "runtime_health_detail_context"
_DETAIL_TABLE = "runtime_service_detail"


class RuntimeHealthOpsBinding(RuntimeContractModel):
    authority_root: Path
    install_manifest_path: Path
    install_public_key_pem: StrictStr = Field(min_length=1, max_length=4096)
    producer_commit: StrictStr = Field(pattern=r"^[0-9a-f]{40}$")

    @field_validator("authority_root", "install_manifest_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("runtime health Ops paths must be absolute")
        return Path(os.path.abspath(value))

    @field_validator("install_public_key_pem")
    @classmethod
    def require_public_key(cls, value: str) -> str:
        if not value.isascii() or "PRIVATE KEY" in value:
            raise ValueError("runtime health Ops binding requires a public key")
        if not value.startswith("-----BEGIN PUBLIC KEY-----") or not value.rstrip().endswith(
            "-----END PUBLIC KEY-----"
        ):
            raise ValueError("runtime health Ops binding requires a PEM public key")
        return value


class RuntimeHealthOpsContext(RuntimeContractModel):
    """Explicit input from the original trusted Ops reader, after install checks."""

    host_name: Text = Field(max_length=253)
    boot_id: Text
    manifest_digest: Sha256
    ops_source_generation_id: Sha256
    source_identity: Sha256
    sampled_at: AwareUtcDatetime

    @property
    def context_source_identity(self) -> str:
        return canonical_sha256({"contract": "runtime-health-ops-context/v1", **self.model_dump()})


class RuntimeHealthStartupWitness(RuntimeContractModel):
    service_id: Text
    spec_fingerprint: Sha256
    run_id: Sha256
    generation: PositiveCount
    started_at: AwareUtcDatetime
    ops_context: RuntimeHealthOpsContext

    @model_validator(mode="after")
    def validate_start_window(self) -> Self:
        age = self.started_at - self.ops_context.sampled_at
        if age < timedelta(0):
            raise ValueError("startup context contains future evidence")
        if age > _OPS_FRESHNESS:
            raise ValueError("startup context exceeds the original Ops freshness window")
        return self


def startup_witness_for_run(
    *,
    context: RuntimeHealthOpsContext | None,
    spec: RuntimeServiceSpec,
    run_id: str,
    generation: int,
    started_at: datetime,
) -> RuntimeHealthStartupWitness | None:
    """Called once by the original start path; never by the health collector."""

    from rquant.runtime_service_control import RuntimeServiceSpec

    if not isinstance(spec, RuntimeServiceSpec):
        raise TypeError("spec must be the original RuntimeServiceSpec")
    started = normalize_aware_utc(started_at)
    if context is None:
        return None
    context = RuntimeHealthOpsContext.model_validate(context)
    if context.sampled_at > started:
        raise ValueError("startup context contains future evidence")
    if started - context.sampled_at > _OPS_FRESHNESS:
        return None
    return RuntimeHealthStartupWitness(
        service_id=spec.service_id,
        spec_fingerprint=spec.identity,
        run_id=run_id,
        generation=generation,
        started_at=started,
        ops_context=context,
    )


class RuntimeHealthHostScope(RuntimeContractModel):
    kind: Literal["host", "slice"]
    host_name: Text
    boot_id: Text
    slice_id: Text | None = None

    @model_validator(mode="after")
    def validate_slice(self) -> Self:
        if (self.kind == "slice") != (self.slice_id is not None):
            raise ValueError("slice scope requires exactly one slice id")
        return self


class RuntimeHealthMinuteScope(RuntimeContractModel):
    kind: Literal["minute_batch"] = "minute_batch"
    batch_id: Sha256
    expected_universe_identity: Sha256 | None = None
    scope_complete: StrictBool


class RuntimeHealthStrategyScope(RuntimeContractModel):
    kind: Literal["strategy_batch"] = "strategy_batch"
    batch_id: Sha256
    processed: StrictBool


class RuntimeHealthRetainedOrderScope(RuntimeContractModel):
    kind: Literal["retained_orders"] = "retained_orders"
    account_id: Text
    configuration_identity: Sha256
    ledger_revision: Count
    retained_count: Count
    total_orders: Count
    has_more: StrictBool

    @model_validator(mode="after")
    def validate_retained_window(self) -> Self:
        if self.total_orders < self.retained_count:
            raise ValueError("retained window exceeds total orders")
        if self.has_more != (self.total_orders > self.retained_count):
            raise ValueError("retained window has_more does not match its count")
        return self


class RuntimeHealthPortfolioScope(RuntimeContractModel):
    kind: Literal["portfolio"] = "portfolio"
    account_id: Text
    configuration_identity: Sha256
    ledger_revision: Count


class RuntimeHealthComparisonScope(RuntimeContractModel):
    kind: Literal["backtest_comparison"] = "backtest_comparison"
    account_id: Text
    strategy_version: Text
    parameter_fingerprint: Sha256
    cost_identity: Sha256
    calendar_identity: Sha256
    baseline_identity: Sha256 | None = None
    comparison_dates: tuple[date, ...] = ()
    baseline_dates: tuple[date, ...] = ()
    complete: StrictBool

    @field_validator("comparison_dates", "baseline_dates")
    @classmethod
    def validate_dates(cls, dates: tuple[date, ...]) -> tuple[date, ...]:
        if dates != tuple(sorted(set(dates))):
            raise ValueError("comparison dates must be unique and ordered")
        return dates


MetricScope = Annotated[
    RuntimeHealthHostScope
    | RuntimeHealthMinuteScope
    | RuntimeHealthStrategyScope
    | RuntimeHealthRetainedOrderScope
    | RuntimeHealthPortfolioScope
    | RuntimeHealthComparisonScope,
    Field(discriminator="kind"),
]


def _metric_number(value: object) -> object:
    if isinstance(value, bool):
        raise ValueError("metric values cannot be bool")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metric values must be finite")
        return Decimal(str(value))
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError("metric values must be finite")
    if isinstance(value, str) and value not in {"inside", "outside", "clear", "breached"}:
        try:
            decimal = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError("metric value is not a supported value") from exc
        if not decimal.is_finite():
            raise ValueError("metric values must be finite")
        return decimal
    return value


MetricValue = Annotated[
    StrictInt | Decimal | Literal["inside", "outside", "clear", "breached"] | None,
    BeforeValidator(_metric_number),
]
MetricId = Literal[
    "host_cpu",
    "slice_cpu",
    "host_memory",
    "slice_memory",
    "minute_delay",
    "minute_missing_codes",
    "strategy_duration",
    "strategy_candidates",
    "order_rejection_ratio",
    "portfolio_risk",
    "portfolio_exposure",
    "return_comparison",
]
_METRIC_SHAPES: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "host_cpu": ("host", "ratio"),
        "slice_cpu": ("slice", "ratio"),
        "host_memory": ("host", "bytes"),
        "slice_memory": ("slice", "bytes"),
        "minute_delay": ("minute_batch", "seconds"),
        "minute_missing_codes": ("minute_batch", "count"),
        "strategy_duration": ("strategy_batch", "seconds"),
        "strategy_candidates": ("strategy_batch", "count"),
        "order_rejection_ratio": ("retained_orders", "ratio"),
        "portfolio_risk": ("portfolio", "risk_state"),
        "portfolio_exposure": ("portfolio", "fraction"),
        "return_comparison": ("backtest_comparison", "band_position"),
    }
)


class RuntimeHealthRealtimeValidity(RuntimeContractModel):
    kind: Literal["realtime"] = "realtime"
    rule_identity: Sha256
    valid_until: AwareUtcDatetime
    boundary: Literal["inclusive", "exclusive"]

    def expired_at(self, cutoff: datetime) -> bool:
        return cutoff > self.valid_until or (
            cutoff == self.valid_until and self.boundary == "exclusive"
        )


class RuntimeHealthAsOfValidity(RuntimeContractModel):
    """References an immutable owner's material; this declaration is not trust."""

    kind: Literal["as_of"] = "as_of"
    basis: Literal["batch", "retained_window", "sealed_comparison", "past_risk_decision"]
    basis_identity: Sha256
    as_of: AwareUtcDatetime
    owner_dataset_id: Text
    source_generation_id: Sha256
    source_identity: Sha256
    scope_identity: Sha256
    event_time_start: AwareUtcDatetime
    event_time_end: AwareUtcDatetime
    available_at: AwareUtcDatetime


MetricValidity = Annotated[
    RuntimeHealthRealtimeValidity | RuntimeHealthAsOfValidity, Field(discriminator="kind")
]


class RuntimeHealthMetric(RuntimeContractModel):
    """One owner-published fact; validation does not derive a measurement."""

    metric_id: MetricId
    owner_dataset_id: Text
    source_generation_id: Sha256
    source_identity: Sha256
    scope: MetricScope
    event_time_start: AwareUtcDatetime
    event_time_end: AwareUtcDatetime
    available_at: AwareUtcDatetime
    observed_at: AwareUtcDatetime
    fresh_until: AwareUtcDatetime | None = None
    validity: MetricValidity | None = Field(default=None, exclude_if=lambda value: value is None)
    unit: Literal["count", "seconds", "ratio", "bytes", "fraction", "band_position", "risk_state"]
    completeness: Literal["complete", "partial", "unavailable"]
    value: MetricValue = None
    numerator: Count | None = None
    denominator: Count | None = None
    verdict: Literal["normal", "attention", "abnormal", "unassessed", "unavailable"]
    reason_code: Text
    policy_identity: Sha256 | None = None

    @model_validator(mode="after")
    def validate_fact(self) -> Self:
        if not (
            self.event_time_start <= self.event_time_end <= self.available_at <= self.observed_at
        ):
            raise ValueError("metric contains an unordered or future event window")
        if _METRIC_SHAPES[self.metric_id] != (self.scope.kind, self.unit):
            raise ValueError("metric unit or object scope does not match its declared meaning")
        if self.completeness != "complete":
            if self.value is not None or self.verdict != "unavailable":
                raise ValueError("unknown or partial metric must have a null unavailable value")
        elif self.value is None:
            raise ValueError("complete metric requires a measured value")
        elif self.verdict == "unavailable":
            raise ValueError("complete metric cannot have an unavailable verdict")
        if (
            self.completeness == "complete"
            and self.fresh_until is not None
            and self.fresh_until < self.observed_at
        ):
            raise ValueError("complete metric was already stale at its owner observation")
        if self.verdict in {"normal", "attention", "abnormal"} and self.policy_identity is None:
            raise ValueError("metric verdict requires its original policy identity")
        if self.value is not None:
            if self.unit in {"count", "bytes"}:
                if type(self.value) is not int or self.value < 0:
                    raise ValueError("metric count requires a measured nonnegative integer")
            elif self.unit in {"seconds", "ratio", "fraction"}:
                if not isinstance(self.value, (Decimal, int)) or self.value < 0:
                    raise ValueError("metric numeric value must be finite and nonnegative")
            elif self.unit == "band_position" and self.value not in {"inside", "outside"}:
                raise ValueError("comparison must use the original band position")
            elif self.unit == "risk_state" and self.value not in {"clear", "breached"}:
                raise ValueError("risk must use the original business decision")
        if self.metric_id == "minute_delay" and self.completeness == "complete":
            elapsed = self.available_at - self.event_time_end
            expected = Decimal(elapsed.days * 86400 + elapsed.seconds) + Decimal(
                elapsed.microseconds
            ) / Decimal(1_000_000)
            if Decimal(self.value) != expected:
                raise ValueError(
                    "minute delay does not match its original batch event/available times"
                )
        if self.metric_id == "minute_missing_codes" and self.completeness == "complete":
            scope = self.scope
            if not isinstance(scope, RuntimeHealthMinuteScope) or not (
                scope.expected_universe_identity is not None and scope.scope_complete
            ):
                raise ValueError("missing codes require the same batch's complete expected scope")
        if (
            isinstance(self.scope, RuntimeHealthStrategyScope)
            and self.completeness == "complete"
            and not self.scope.processed
        ):
            raise ValueError("idle strategy loop cannot publish an actual batch metric")
        if self.metric_id == "order_rejection_ratio":
            self._validate_order_ratio()
        elif self.numerator is not None or self.denominator is not None:
            raise ValueError("order ratio counts cannot attach to a different metric")
        if isinstance(self.scope, RuntimeHealthComparisonScope) and self.completeness == "complete":
            scope = self.scope
            if not (
                scope.complete
                and scope.baseline_identity
                and scope.comparison_dates
                and scope.comparison_dates == scope.baseline_dates
            ):
                raise ValueError(
                    "comparison requires the exact baseline and complete matching dates"
                )
            if scope.comparison_dates[-1] > self.event_time_end.date():
                raise ValueError("comparison contains future completed dates")
        self._validate_validity()
        return self

    def _validate_validity(self) -> None:
        validity = self.validity
        if isinstance(validity, RuntimeHealthRealtimeValidity):
            if self.fresh_until is not None and self.fresh_until != validity.valid_until:
                raise ValueError("metric expiry conflicts with its declared original owner rule")
            if self.completeness == "complete" and validity.expired_at(self.observed_at):
                raise ValueError("complete metric was already stale under its owner rule")
        elif isinstance(validity, RuntimeHealthAsOfValidity):
            if self.fresh_until is not None:
                raise ValueError("as-of facts cannot declare a competing realtime expiry")
            if (
                validity.owner_dataset_id,
                validity.source_generation_id,
                validity.source_identity,
                validity.scope_identity,
                validity.event_time_start,
                validity.event_time_end,
                validity.available_at,
                validity.as_of,
            ) != (
                self.owner_dataset_id,
                self.source_generation_id,
                self.source_identity,
                canonical_sha256(self.scope),
                self.event_time_start,
                self.event_time_end,
                self.available_at,
                self.observed_at,
            ):
                raise ValueError(
                    "as-of declaration is detached from its owner source, scope or window"
                )
            scope = self.scope
            if isinstance(scope, (RuntimeHealthMinuteScope, RuntimeHealthStrategyScope)):
                expected = "batch", scope.batch_id
            elif isinstance(scope, RuntimeHealthRetainedOrderScope):
                expected = "retained_window", self.source_identity
            elif isinstance(scope, RuntimeHealthComparisonScope):
                expected = "sealed_comparison", scope.baseline_identity
            elif (
                isinstance(scope, RuntimeHealthPortfolioScope)
                and self.metric_id == "portfolio_risk"
                and self.policy_identity is not None
            ):
                expected = "past_risk_decision", self.source_identity
            else:
                raise ValueError("current resources cannot be declared as-of historical facts")
            if (validity.basis, validity.basis_identity) != expected:
                raise ValueError("as-of basis does not name its exact original material")

    def _validate_order_ratio(self) -> None:
        scope = self.scope
        if not isinstance(scope, RuntimeHealthRetainedOrderScope):
            raise ValueError("order ratio requires the original retained order scope")
        if self.numerator is None or self.denominator is None:
            if self.completeness == "complete":
                raise ValueError("order ratio requires same-window numerator and denominator")
            return
        if self.denominator != scope.retained_count:
            raise ValueError("order ratio denominator must be the retained count")
        if self.numerator > self.denominator:
            raise ValueError("order ratio numerator exceeds its denominator")
        if self.denominator == 0:
            if self.value is not None or self.completeness == "complete":
                raise ValueError("empty retained order window cannot have a ratio")
        elif self.completeness == "complete" and (
            Decimal(self.value) != Decimal(self.numerator) / Decimal(self.denominator)
        ):
            # Check the original owner's exact Decimal result; do not publish a derived rate.
            raise ValueError("order ratio does not match its original same-window counts")


class RuntimeHealthHeartbeatMaterial(RuntimeContractModel):
    """Same-read heartbeat plus its optional original trusted Ops owner record.

    Canonical original-model JSON keeps this type independent of the future file
    model's import of RuntimeHealthStartupWitness. The original source receipt
    hashes the original parsed file model, including any future accepted fields.
    The authority extracts file inputs from that same physical read. An Ops
    supplement carries the complete record from its one original owner capture,
    so serialized details can verify the mapped values against the trusted hash.
    """

    control_root: Path
    spec_json: Text
    heartbeat_json: Text | None = None
    observed_at: AwareUtcDatetime
    read_kind: Literal["readable", "missing", "superseded", "unreadable"]
    unreadable_error: Text | None = None
    startup_witness: RuntimeHealthStartupWitness | None = None
    metrics: tuple[RuntimeHealthMetric, ...] = ()
    ops_source_json: Text | None = Field(default=None, exclude_if=lambda value: value is None)

    @field_validator("control_root")
    @classmethod
    def validate_control_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("control_root must be absolute")
        return Path(os.path.abspath(value))

    @property
    def spec(self) -> RuntimeServiceSpec:
        from rquant.runtime_service_control import RuntimeServiceSpec

        return strict_model_validate_canonical_json(RuntimeServiceSpec, self.spec_json)

    @property
    def heartbeat(self) -> RuntimeServiceHeartbeat | None:
        from rquant.runtime_service_control import RuntimeServiceHeartbeat

        if self.heartbeat_json is None:
            return None
        return strict_model_validate_canonical_json(RuntimeServiceHeartbeat, self.heartbeat_json)

    @property
    def ops_source(self) -> SourceReadResult | None:
        from rquant.runtime_serving_snapshot import SourceReadResult

        if self.ops_source_json is None:
            return None
        return strict_model_validate_canonical_json(SourceReadResult, self.ops_source_json)

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        spec, heartbeat = self.spec, self.heartbeat
        if (self.read_kind == "readable") != (heartbeat is not None):
            raise ValueError("readable source requires exactly one raw heartbeat")
        if (self.read_kind == "unreadable") != (self.unreadable_error is not None):
            raise ValueError("unreadable source requires exactly one original error class")
        if heartbeat is None:
            if self.startup_witness is not None or self.metrics or self.ops_source_json is not None:
                raise ValueError("absent heartbeat cannot carry a witness or metric")
        else:
            if (
                heartbeat.service_id != spec.service_id
                or heartbeat.spec_fingerprint != spec.identity
            ):
                raise ValueError("raw heartbeat does not match the registered service spec")
            _validate_heartbeat_time(heartbeat, self.observed_at)
            if self.startup_witness is not None:
                binding = self.startup_witness
                if (
                    binding.service_id,
                    binding.spec_fingerprint,
                    binding.run_id,
                    binding.generation,
                    binding.started_at,
                ) != (
                    heartbeat.service_id,
                    heartbeat.spec_fingerprint,
                    heartbeat.run_id,
                    heartbeat.generation,
                    heartbeat.started_at,
                ):
                    raise ValueError("startup witness does not match the exact heartbeat run")
        ids = tuple(
            (metric.metric_id, metric.owner_dataset_id, canonical_sha256(metric.scope))
            for metric in self.metrics
        )
        if len(ids) != len(set(ids)):
            raise ValueError("service metric ids and object scopes must be unique")
        if any(metric.observed_at > self.observed_at for metric in self.metrics):
            raise ValueError("metric exceeds the same-read health cutoff")
        if heartbeat is not None:
            if self.startup_witness != heartbeat.startup_witness:
                raise ValueError("startup witness differs from the original heartbeat witness")
            producer_metrics = heartbeat.health_metrics or ()
            if self.metrics[: len(producer_metrics)] != producer_metrics or any(
                metric.owner_dataset_id != "ops_status"
                for metric in self.metrics[len(producer_metrics) :]
            ):
                raise ValueError("producer metrics differ from the original heartbeat metrics")
        _validate_ops_owner_facts(self)
        return self

    @property
    def source_receipt(self) -> str:
        summary: object = None
        if self.read_kind == "unreadable":
            summary = {"unreadable": self.unreadable_error}
        elif self.read_kind == "superseded":
            summary = {"superseded": True}
        elif self.heartbeat is not None:
            summary = self.heartbeat.model_dump(mode="json")
        return canonical_sha256(
            {
                "contract": "runtime-health-source-receipt/v1",
                "control_root": str(self.control_root),
                "spec": self.spec.model_dump(mode="json"),
                "heartbeat": summary,
                "observed_at": self.observed_at,
            }
        )

    @classmethod
    def from_read(
        cls,
        *,
        control_root: Path,
        spec: RuntimeServiceSpec,
        heartbeat: RuntimeServiceHeartbeat | None,
        observed_at: datetime,
        read_kind: Literal["readable", "missing", "superseded", "unreadable"] | None = None,
        unreadable_error: str | None = None,
        startup_witness: RuntimeHealthStartupWitness | None = None,
        metrics: tuple[RuntimeHealthMetric, ...] = (),
        ops_source: SourceReadResult | None = None,
    ) -> Self:
        from rquant.runtime_service_control import RuntimeServiceHeartbeat, RuntimeServiceSpec
        from rquant.runtime_serving_snapshot import SourceReadResult

        if not isinstance(spec, RuntimeServiceSpec) or (
            heartbeat is not None and not isinstance(heartbeat, RuntimeServiceHeartbeat)
        ):
            raise TypeError("same-read material requires the original spec and file model")
        if ops_source is not None and not isinstance(ops_source, SourceReadResult):
            raise TypeError("Ops source material requires the original typed owner read")
        return cls(
            control_root=control_root,
            spec_json=canonical_json_bytes(spec.model_dump(mode="json")).decode("utf-8"),
            heartbeat_json=None
            if heartbeat is None
            else canonical_json_bytes(heartbeat.model_dump(mode="json")).decode("utf-8"),
            observed_at=observed_at,
            read_kind=read_kind or ("missing" if heartbeat is None else "readable"),
            unreadable_error=unreadable_error,
            startup_witness=startup_witness,
            metrics=metrics,
            ops_source_json=None
            if ops_source is None
            else canonical_json_bytes(ops_source.model_dump(mode="json")).decode("utf-8"),
        )


def _validate_heartbeat_time(heartbeat: RuntimeServiceHeartbeat, observed_at: datetime) -> None:
    # Same temporal invariant as the original health reader; no second physical read.
    times = (
        heartbeat.started_at,
        heartbeat.heartbeat_at,
        heartbeat.last_success_at,
        heartbeat.stopped_at,
    )
    if any(value is not None and value > observed_at for value in times):
        raise ValueError("raw heartbeat contains future evidence")
    if heartbeat.heartbeat_at < heartbeat.started_at:
        raise ValueError("raw heartbeat precedes service start")
    if heartbeat.last_success_at is not None and heartbeat.last_success_at > heartbeat.heartbeat_at:
        raise ValueError("raw heartbeat success exceeds heartbeat time")
    if heartbeat.stopped_at is not None and not (
        heartbeat.started_at <= heartbeat.stopped_at <= heartbeat.heartbeat_at
    ):
        raise ValueError("raw heartbeat stop is outside its lifetime")


class RuntimeHealthDetailContextRow(RuntimeContractModel):
    host_name: Text
    boot_id: Text
    sampled_at: AwareUtcDatetime
    context_source_identity: Sha256
    ops_source_generation_id: Sha256
    observed_at: AwareUtcDatetime


class RuntimeHealthServiceDetailRow(RuntimeContractModel):
    service_id: Text
    source_receipt: Sha256
    source_material_json: Text
    context_source_identity: Sha256
    observed_at: AwareUtcDatetime


def _projection_bytes(value: object) -> int:
    """Match the accepted Serving byte accounting, including ASCII escaping."""

    def normalize(item: object) -> object:
        if isinstance(item, RuntimeContractModel):
            return normalize(item.model_dump(mode="python"))
        if isinstance(item, Mapping):
            return {str(key): normalize(child) for key, child in item.items()}
        if isinstance(item, (tuple, list)):
            return [normalize(child) for child in item]
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        return item

    return len(
        json.dumps(
            normalize(value),
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )


class RuntimeHealthDetailProjection(RuntimeContractModel):
    table_name: Literal["runtime_health_detail_context", "runtime_service_detail"]
    available_at: AwareUtcDatetime
    rows: tuple[RuntimeHealthDetailContextRow | RuntimeHealthServiceDetailRow, ...]
    owner_dataset_id: Literal["runtime_health"] = "runtime_health"
    owner_generation_id: Sha256

    @field_serializer("rows")
    def serialize_rows(
        self, rows: tuple[RuntimeHealthDetailContextRow | RuntimeHealthServiceDetailRow, ...]
    ) -> list[dict[str, Scalar]]:
        # Generic Serving rows contain ISO strings, not Python datetime objects.
        return [row.model_dump(mode="json") for row in rows]

    @model_validator(mode="after")
    def validate_table(self) -> Self:
        context = self.table_name == _CONTEXT_TABLE
        row_type = RuntimeHealthDetailContextRow if context else RuntimeHealthServiceDetailRow
        if any(not isinstance(row, row_type) for row in self.rows):
            raise ValueError("detail projection rows do not match the exact column contract")
        if context and len(self.rows) != 1:
            raise ValueError("complete detail context requires exactly one row")
        if not context and len(self.rows) > _MAX_SERVICES:
            raise ValueError("detail service row budget exceeds the original 500 services")
        for row in self.rows:
            for cell in row.model_dump(mode="json").values():
                if _projection_bytes(cell) > _MAX_CELL_BYTES:
                    raise ValueError("detail projection cell exceeds its original byte budget")
            if row.observed_at != self.available_at:
                raise ValueError("detail row cutoff must match projection availability")
            if (
                isinstance(row, RuntimeHealthDetailContextRow)
                and row.sampled_at > self.available_at
            ):
                raise ValueError("detail context contains future evidence")
        serialized_rows = self.serialize_rows(self.rows)
        if context and _projection_bytes(serialized_rows) > _MAX_CONTEXT_BYTES:
            raise ValueError("detail context exceeds its 4 KiB byte budget")
        if not context:
            ids = tuple(row.service_id for row in self.rows)
            if ids != tuple(sorted(set(ids))):
                raise ValueError("detail services must be unique and ordered")
            if _projection_bytes(serialized_rows) > _MAX_OWNER_BYTES:
                raise ValueError(
                    "health detail service table exceeds its original 7 MiB byte budget"
                )
        return self


class RuntimeHealthDetailGraph(RuntimeContractModel):
    projections: tuple[RuntimeHealthDetailProjection, ...]

    @model_validator(mode="after")
    def validate_complete_tables(self) -> Self:
        names = tuple(p.table_name for p in self.projections)
        if len(names) != 2 or set(names) != {_CONTEXT_TABLE, _DETAIL_TABLE}:
            raise ValueError("detail graph requires both complete tables exactly once")
        generations = {(p.owner_generation_id, p.available_at) for p in self.projections}
        if len(generations) != 1:
            raise ValueError("detail graph has mixed owner generation or observation cutoff")
        require_runtime_health_detail_capacity(self)
        return self


class RuntimeHealthOwnerProjection(RuntimeContractModel):
    """Original, already validated owner projections supplied for total accounting."""

    table_name: Text
    available_at: AwareUtcDatetime
    rows: tuple[Mapping[StrictStr, Scalar], ...] = ()
    owner_dataset_id: Literal["runtime_health"] = "runtime_health"
    owner_generation_id: Sha256

    @field_validator("rows")
    @classmethod
    def freeze_rows(
        cls, rows: tuple[Mapping[str, Scalar], ...]
    ) -> tuple[Mapping[str, Scalar], ...]:
        if any(
            isinstance(value, float) and not math.isfinite(value)
            for row in rows
            for value in row.values()
        ):
            raise ValueError("original projection contains a nonfinite cell")
        return tuple(MappingProxyType(dict(row)) for row in rows)

    @field_serializer("rows")
    def serialize_rows(self, rows: tuple[Mapping[str, Scalar], ...]) -> list[dict[str, Scalar]]:
        return [dict(row) for row in rows]


def require_runtime_health_detail_capacity(
    graph: RuntimeHealthDetailGraph,
    *,
    existing_projections: tuple[RuntimeHealthOwnerProjection, ...] = (),
) -> None:
    names = tuple(projection.table_name for projection in existing_projections)
    if len(names) != len(set(names)) or {_CONTEXT_TABLE, _DETAIL_TABLE}.intersection(names):
        raise ValueError("original owner projections duplicate a detail table")
    first = graph.projections[0]
    if any(
        (p.owner_generation_id, p.available_at) != (first.owner_generation_id, first.available_at)
        for p in existing_projections
    ):
        raise ValueError("original projections do not bind the same owner generation and cutoff")
    if (
        sum(
            _projection_bytes(projection)
            for projection in (*graph.projections, *existing_projections)
        )
        > _MAX_OWNER_BYTES
    ):
        raise ValueError("runtime health owner projections exceed the original 7 MiB budget")


class RuntimeHealthValidatedServiceDetail(RuntimeContractModel):
    service_id: Text
    availability: Literal["available", "unavailable"]
    reason_code: Text
    legacy_status: Literal["missing", "starting", "running", "degraded", "stopped"]
    legacy_stale: StrictBool
    source_receipt: Sha256
    material: RuntimeHealthHeartbeatMaterial

    @property
    def heartbeat(self) -> RuntimeServiceHeartbeat | None:
        return self.material.heartbeat if self.availability == "available" else None

    @property
    def metrics(self) -> tuple[RuntimeHealthValidatedMetric, ...]:
        return self.metrics_at(self.material.observed_at)

    def metrics_at(self, cutoff: datetime) -> tuple[RuntimeHealthValidatedMetric, ...]:
        observed = normalize_aware_utc(cutoff)
        if observed < self.material.observed_at:
            raise ValueError("consumer cutoff precedes the verified owner read")
        return tuple(
            _visible_metric(metric, observed, self.availability, self.reason_code)
            for metric in self.material.metrics
        )


class RuntimeHealthValidatedMetric(RuntimeContractModel):
    metric: RuntimeHealthMetric
    availability: Literal["available", "unavailable"]
    reason_code: Text

    @property
    def value(self) -> int | Decimal | str | None:
        return self.metric.value if self.availability == "available" else None

    @property
    def verdict(self) -> str:
        return self.metric.verdict if self.availability == "available" else "unavailable"


def _visible_metric(
    metric: RuntimeHealthMetric, cutoff: datetime, availability: str, reason: str
) -> RuntimeHealthValidatedMetric:
    as_of = isinstance(metric.validity, RuntimeHealthAsOfValidity)
    if availability == "available" or (as_of and reason == "heartbeat_stale"):
        if metric.completeness != "complete":
            reason = metric.reason_code
        elif as_of:
            return RuntimeHealthValidatedMetric(
                metric=metric, availability="available", reason_code=metric.reason_code
            )
        elif isinstance(metric.validity, RuntimeHealthRealtimeValidity):
            if not metric.validity.expired_at(cutoff):
                return RuntimeHealthValidatedMetric(
                    metric=metric, availability="available", reason_code=metric.reason_code
                )
            reason = "metric_stale"
        elif metric.fresh_until is None:
            reason = "metric_freshness_missing"
        elif metric.fresh_until < cutoff:
            reason = "metric_stale"
        else:
            return RuntimeHealthValidatedMetric(
                metric=metric, availability="available", reason_code=metric.reason_code
            )
    return RuntimeHealthValidatedMetric(
        metric=metric, availability="unavailable", reason_code=reason
    )


class RuntimeHealthValidatedDetails(RuntimeContractModel):
    owner_generation_id: Sha256
    observed_at: AwareUtcDatetime
    context: RuntimeHealthOpsContext
    services: tuple[RuntimeHealthValidatedServiceDetail, ...]


class RuntimeHealthServiceView(RuntimeContractModel):
    """Verified bounded view retained by Serving, without the raw file document."""

    service_id: Text
    availability: Literal["available", "unavailable"]
    reason_code: Text
    observed_at: AwareUtcDatetime
    source_receipt: Sha256
    context: RuntimeHealthOpsContext
    startup_witness: RuntimeHealthStartupWitness | None = None
    metrics: tuple[RuntimeHealthValidatedMetric, ...] = ()
    observations: Mapping[StrictStr, Count] | None = None
    degraded_reasons: tuple[Text, ...] = ()

    @field_validator("observations")
    @classmethod
    def freeze_observations(cls, value: Mapping[str, int] | None) -> Mapping[str, int] | None:
        return None if value is None else MappingProxyType(dict(value))

    def metrics_at(self, cutoff: datetime) -> tuple[RuntimeHealthValidatedMetric, ...]:
        observed = normalize_aware_utc(cutoff)
        if observed < self.observed_at:
            raise ValueError("consumer cutoff precedes the verified owner read")
        return tuple(
            _visible_metric(item.metric, observed, self.availability, self.reason_code)
            for item in self.metrics
        )

    @field_serializer("observations")
    def observations_json(self, value: Mapping[str, int] | None) -> dict[str, int] | None:
        return None if value is None else dict(value)

    @classmethod
    def from_verified(
        cls, detail: RuntimeHealthValidatedServiceDetail, context: RuntimeHealthOpsContext
    ) -> Self:
        hb = detail.heartbeat
        return cls(
            service_id=detail.service_id,
            availability=detail.availability,
            reason_code=detail.reason_code,
            observed_at=detail.material.observed_at,
            source_receipt=detail.source_receipt,
            context=context,
            startup_witness=detail.material.startup_witness,
            metrics=detail.metrics,
            observations=None if hb is None else hb.observations,
            degraded_reasons=() if hb is None else hb.degraded_reasons,
        )


def _current_context(
    context: RuntimeHealthOpsContext | None, observed_at: datetime
) -> RuntimeHealthOpsContext | None:
    if context is None:
        return None
    result = RuntimeHealthOpsContext.model_validate(context)
    if result.sampled_at > observed_at:
        raise ValueError("current Ops context contains future evidence")
    return result if observed_at - result.sampled_at <= _OPS_FRESHNESS else None


def _context_row(
    context: RuntimeHealthOpsContext, observed_at: datetime
) -> RuntimeHealthDetailContextRow:
    return RuntimeHealthDetailContextRow(
        host_name=context.host_name,
        boot_id=context.boot_id,
        sampled_at=context.sampled_at,
        context_source_identity=context.context_source_identity,
        ops_source_generation_id=context.ops_source_generation_id,
        observed_at=observed_at,
    )


def build_runtime_health_detail_graph(
    *,
    materials: tuple[RuntimeHealthHeartbeatMaterial, ...],
    legacy_services: tuple[RuntimeServiceHealth, ...],
    source_receipts: Mapping[str, str],
    context: RuntimeHealthOpsContext | None,
    owner_generation_id: str,
    observed_at: datetime,
    existing_projections: tuple[RuntimeHealthOwnerProjection, ...] = (),
    enabled: bool = False,
) -> RuntimeHealthDetailGraph | None:
    """Build the complete optional extension from the original owner's one read."""

    if type(enabled) is not bool:
        raise TypeError("detail enablement must be an explicit boolean")
    if not enabled:
        return None
    observed = normalize_aware_utc(observed_at)
    current = _current_context(context, observed)
    if current is None:
        return None
    rows = tuple(
        RuntimeHealthServiceDetailRow(
            service_id=source.spec.service_id,
            source_receipt=source.source_receipt,
            source_material_json=canonical_json_bytes(source.model_dump(mode="json")).decode(
                "utf-8"
            ),
            context_source_identity=current.context_source_identity,
            observed_at=observed,
        )
        for source in sorted(materials, key=lambda item: item.spec.service_id)
    )
    graph = RuntimeHealthDetailGraph(
        projections=(
            RuntimeHealthDetailProjection(
                table_name=_CONTEXT_TABLE,
                available_at=observed,
                rows=(_context_row(current, observed),),
                owner_generation_id=owner_generation_id,
            ),
            RuntimeHealthDetailProjection(
                table_name=_DETAIL_TABLE,
                available_at=observed,
                rows=rows,
                owner_generation_id=owner_generation_id,
            ),
        )
    )
    validate_runtime_health_detail_graph(
        graph,
        legacy_services=legacy_services,
        source_receipts=source_receipts,
        context=current,
        owner_generation_id=owner_generation_id,
        observed_at=observed,
        existing_projections=existing_projections,
    )
    return graph


def validate_runtime_health_detail_graph(
    graph: RuntimeHealthDetailGraph | None,
    *,
    legacy_services: tuple[RuntimeServiceHealth, ...],
    source_receipts: Mapping[str, str],
    context: RuntimeHealthOpsContext | None,
    owner_generation_id: str,
    observed_at: datetime,
    existing_projections: tuple[RuntimeHealthOwnerProjection, ...] = (),
) -> RuntimeHealthValidatedDetails | None:
    """Reject bad present material; absent optional material remains unavailable."""

    from rquant.runtime_service_control import RuntimeServiceHealth

    if graph is None:
        return None
    graph = RuntimeHealthDetailGraph.model_validate(graph)
    observed = normalize_aware_utc(observed_at)
    current = _current_context(context, observed)
    if current is None:
        raise ValueError("present detail graph has no fresh trusted current context")
    if any(
        (p.owner_generation_id, p.available_at) != (owner_generation_id, observed)
        for p in graph.projections
    ):
        raise ValueError("detail graph does not match the original owner generation and cutoff")
    require_runtime_health_detail_capacity(graph, existing_projections=existing_projections)
    ctx = next(p for p in graph.projections if p.table_name == _CONTEXT_TABLE).rows[0]
    if ctx != _context_row(current, observed):
        raise ValueError("detail context does not match the original trusted Ops source")
    if any(not isinstance(service, RuntimeServiceHealth) for service in legacy_services):
        raise TypeError("legacy_services must contain original RuntimeServiceHealth models")
    ids = tuple(service.service_id for service in legacy_services)
    if len(ids) > _MAX_SERVICES or len(ids) != len(set(ids)) or set(source_receipts) != set(ids):
        raise ValueError("original service set and receipt graph must be exact and unique")
    rows = next(p for p in graph.projections if p.table_name == _DETAIL_TABLE).rows
    if tuple(row.service_id for row in rows) != tuple(sorted(ids)):
        raise ValueError("detail graph does not contain the complete registered service set")
    services = {service.service_id: service for service in legacy_services}
    result: list[RuntimeHealthValidatedServiceDetail] = []
    for row in rows:
        if row.context_source_identity != current.context_source_identity:
            raise ValueError("detail service row is detached from its context")
        strict_canonical_json_loads(row.source_material_json)
        source = strict_model_validate_canonical_json(
            RuntimeHealthHeartbeatMaterial, row.source_material_json
        )
        if source.spec.service_id != row.service_id or source.observed_at != observed:
            raise ValueError("detail material is detached from its service or cutoff")
        if (
            source.source_receipt != row.source_receipt
            or row.source_receipt != source_receipts[row.service_id]
        ):
            raise ValueError(
                "detail source receipt does not match the exact original heartbeat read"
            )
        service = services[row.service_id]
        _validate_same_read(source, service, observed)
        reason = _detail_reason(source, service, current)
        _validate_ops_metrics(source, current)
        result.append(
            RuntimeHealthValidatedServiceDetail(
                service_id=row.service_id,
                availability="available" if reason == "available" else "unavailable",
                reason_code=reason,
                legacy_status=service.status.value,
                legacy_stale=service.stale,
                source_receipt=row.source_receipt,
                material=source,
            )
        )
    return RuntimeHealthValidatedDetails(
        owner_generation_id=owner_generation_id,
        observed_at=observed,
        context=current,
        services=tuple(result),
    )


def _validate_same_read(
    source: RuntimeHealthHeartbeatMaterial, service: RuntimeServiceHealth, observed: datetime
) -> None:
    from rquant.runtime_service_control import RuntimeServiceStatus, project_heartbeat

    heartbeat = source.heartbeat
    stale = heartbeat is None or observed - heartbeat.heartbeat_at > source.spec.stale_after
    if heartbeat is None:
        status = (
            RuntimeServiceStatus.DEGRADED
            if source.read_kind == "unreadable"
            else RuntimeServiceStatus.MISSING
        )
    else:
        status = RuntimeServiceStatus.DEGRADED if stale else heartbeat.status
    if (service.plane, service.observed_at, service.stale, service.status, service.heartbeat) != (
        source.spec.plane,
        observed,
        stale,
        status,
        project_heartbeat(heartbeat),
    ):
        raise ValueError("legacy health and detail did not come from the same read")


def _detail_reason(
    source: RuntimeHealthHeartbeatMaterial,
    service: RuntimeServiceHealth,
    context: RuntimeHealthOpsContext,
) -> str:
    if source.read_kind != "readable":
        return "heartbeat_" + source.read_kind
    binding = source.startup_witness
    if binding is None:
        return "startup_witness_missing"
    startup = binding.ops_context
    if startup.host_name != context.host_name:
        return "host_mismatch"
    if startup.boot_id != context.boot_id:
        return "boot_mismatch"
    if startup.manifest_digest != context.manifest_digest:
        return "installation_mismatch"
    if service.stale:
        return "heartbeat_stale"
    return "available"


def _validate_ops_owner_facts(material: RuntimeHealthHeartbeatMaterial) -> None:
    from rquant.ops_status_serving import ops_runtime_health_metrics
    from rquant.runtime_serving_snapshot import OpsStatusPayload
    from rquant.serving_contracts import FreshnessStatus

    facts = tuple(metric for metric in material.metrics if metric.owner_dataset_id == "ops_status")
    for fact in facts:
        scope = fact.scope
        if (
            not isinstance(scope, RuntimeHealthHostScope)
            or (fact.metric_id, scope.kind)
            not in {
                ("host_cpu", "host"),
                ("host_memory", "host"),
                ("slice_cpu", "slice"),
                ("slice_memory", "slice"),
            }
        ):
            raise ValueError("Ops source supplements require original host or slice metrics")
    source = material.ops_source
    if source is None:
        if facts:
            raise ValueError("Ops source supplement lacks its original typed owner record")
        return
    if (
        source.dataset_id != "ops_status"
        or source.status is not FreshnessStatus.FRESH
        or not isinstance(source.payload, OpsStatusPayload)
        or source.payload.snapshot is None
        or source.payload.snapshot.sampled_at != source.event_time
        or source.published_at > material.observed_at
    ):
        raise ValueError("Ops source supplement requires the original current owner record")
    expected = ops_runtime_health_metrics(source)
    if any(fact not in expected for fact in facts):
        raise ValueError("Ops source supplement differs from its original owner metric")


def _validate_ops_metrics(
    material: RuntimeHealthHeartbeatMaterial, context: RuntimeHealthOpsContext
) -> None:
    source = material.ops_source
    if source is not None:
        sample = source.payload.snapshot
        if (
            sample.host_name,
            sample.boot_id,
            sample.manifest_digest,
            sample.sampled_at,
            source.generation_id,
            canonical_sha256(source),
        ) != (
            context.host_name,
            context.boot_id,
            context.manifest_digest,
            context.sampled_at,
            context.ops_source_generation_id,
            context.source_identity,
        ):
            raise ValueError("Ops source record differs from the actual trusted current context")
    for metric in material.metrics:
        scope = metric.scope
        if isinstance(scope, RuntimeHealthHostScope) and (
            scope.host_name != context.host_name
            or scope.boot_id != context.boot_id
            or metric.owner_dataset_id != "ops_status"
            or metric.source_generation_id != context.ops_source_generation_id
            or metric.source_identity != context.source_identity
            or metric.event_time_end > context.sampled_at
        ):
            raise ValueError("host or slice metric does not bind the actual current Ops source")


_IDLE_HEARTBEAT_FIELDS = (
    "heartbeat_at",
    "last_success_at",
    "input_sequence",
    "output_sequence",
    "source_generations",
    "consecutive_failures",
    "total_failures",
    "total_successes",
    "last_step_duration_seconds",
    "p95_step_duration_seconds",
    "recent_step_durations_seconds",
)


def runtime_health_detail_state_identity(details: RuntimeHealthValidatedDetails | None) -> str:
    """Material extension identity for the original health publication gate."""

    if details is None:
        return canonical_sha256({"contract": "runtime-health-detail-state/v1", "details": None})
    services: list[dict[str, object]] = []
    for detail in details.services:
        source = detail.material
        heartbeat = source.heartbeat
        raw = None if heartbeat is None else heartbeat.model_dump(mode="json")
        if raw is not None:
            for field in _IDLE_HEARTBEAT_FIELDS:
                raw.pop(field, None)
        metrics = [metric.model_dump(mode="json") for metric in detail.metrics]
        for metric in metrics:
            metric["metric"].pop("observed_at", None)
        services.append(
            {
                "service_id": detail.service_id,
                "availability": detail.availability,
                "reason_code": detail.reason_code,
                "legacy_status": detail.legacy_status,
                "legacy_stale": detail.legacy_stale,
                "control_root": str(source.control_root),
                "spec": source.spec.model_dump(mode="json"),
                "read_kind": source.read_kind,
                "unreadable_error": source.unreadable_error,
                "heartbeat": raw,
                "startup_witness": source.startup_witness,
                "metrics": metrics,
            }
        )
    return canonical_sha256(
        {
            "contract": "runtime-health-detail-state/v1",
            "host_name": details.context.host_name,
            "boot_id": details.context.boot_id,
            "manifest_digest": details.context.manifest_digest,
            "services": services,
        }
    )


def runtime_health_graph_from_projections(
    projections: tuple[object, ...], *, owner_generation_id: str
) -> RuntimeHealthDetailGraph | None:
    """Adapt the existing generic extension; present partial material is invalid."""
    selected = tuple(p for p in projections if p.table_name in {_CONTEXT_TABLE, _DETAIL_TABLE})
    if not selected:
        return None
    return RuntimeHealthDetailGraph(
        projections=tuple(
            RuntimeHealthDetailProjection(
                table_name=p.table_name,
                available_at=p.available_at,
                rows=tuple(
                    (
                        RuntimeHealthDetailContextRow
                        if p.table_name == _CONTEXT_TABLE
                        else RuntimeHealthServiceDetailRow
                    ).model_validate(dict(row))
                    for row in p.rows
                ),
                owner_generation_id=owner_generation_id,
            )
            for p in selected
        )
    )
