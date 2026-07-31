"""Small deterministic read models for the read-only serving generation."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from types import MappingProxyType

import pandas as pd
from pydantic import Field, TypeAdapter, model_validator

from rquant.delivery_contracts import OutboxRecord
from rquant.experiment_registry import PromotionDecision
from rquant.lab_eta import LabEtaEstimate
from rquant.lab_jobs import LabJobSummary
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.serving_publisher import ServingTableSpec
from rquant.signal_bus import SignalRouteReceipt
from rquant.signal_contracts import SignalEnvelope


class ServingSignalRecord(RuntimeContractModel):
    global_sequence: int = Field(ge=1)
    signal: SignalEnvelope


class ServingLabJobRecord(RuntimeContractModel):
    summary: LabJobSummary
    eta: LabEtaEstimate | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> ServingLabJobRecord:
        if self.eta is not None and self.eta.job_id != self.summary.job_id:
            raise ValueError("ETA job_id does not match job summary")
        return self


class ServingReadModelInput(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    signals: tuple[ServingSignalRecord, ...] = ()
    routes: tuple[SignalRouteReceipt, ...] = ()
    deliveries: tuple[OutboxRecord, ...] = ()
    paper_accounts: tuple[PaperAccountSnapshot, ...] = ()
    lab_jobs: tuple[ServingLabJobRecord, ...] = ()
    promotions: tuple[PromotionDecision, ...] = ()

    @model_validator(mode="after")
    def validate_snapshot(self) -> ServingReadModelInput:
        self._require_unique(
            (record.global_sequence for record in self.signals),
            "signal global_sequence",
        )
        self._require_unique(
            (record.signal.signal_id for record in self.signals),
            "signal_id",
        )
        self._require_unique(
            ((record.source_id, record.source_sequence) for record in self.routes),
            "route source sequence",
        )
        self._require_unique((record.outbox_id for record in self.deliveries), "outbox_id")
        self._require_unique(
            (record.account_id for record in self.paper_accounts),
            "paper account_id",
        )
        self._require_unique(
            (str(record.summary.job_id) for record in self.lab_jobs),
            "job_id",
        )
        self._require_unique(
            (record.decision_id for record in self.promotions),
            "promotion decision_id",
        )

        times = [record.signal.available_at for record in self.signals]
        times.extend(record.routed_at for record in self.routes)
        times.extend(record.updated_at for record in self.deliveries)
        times.extend(record.as_of_time for record in self.paper_accounts)
        times.extend(record.summary.updated_at for record in self.lab_jobs)
        times.extend(record.eta.as_of for record in self.lab_jobs if record.eta is not None)
        times.extend(record.decided_at for record in self.promotions)
        if any(value > self.observed_at for value in times):
            raise ValueError("serving snapshot contains future evidence")

        signal_ids = {record.signal.signal_id for record in self.signals}
        if any(record.signal_id not in signal_ids for record in self.routes):
            raise ValueError("route references a signal outside the serving snapshot")
        routes = {record.signal_id: record for record in self.routes}
        for delivery in self.deliveries:
            route = routes.get(delivery.signal_id)
            if route is None:
                raise ValueError("delivery references a signal without a route receipt")
            if delivery.target not in route.targets:
                raise ValueError("delivery target is outside the frozen route manifest")
        return self

    @staticmethod
    def _require_unique(values: Iterable[object], label: str) -> None:
        materialized = tuple(values)
        if len(materialized) != len(set(materialized)):
            raise ValueError(f"{label} values must be unique")


SERVING_TABLE_SPECS: Mapping[str, ServingTableSpec] = MappingProxyType(
    {
        "deliveries": ServingTableSpec(sort_keys=("outbox_id",)),
        "lab_jobs": ServingTableSpec(sort_keys=("job_id",)),
        "paper_accounts": ServingTableSpec(sort_keys=("account_id",)),
        "paper_holdings": ServingTableSpec(sort_keys=("account_id", "ts_code")),
        "promotions": ServingTableSpec(sort_keys=("decision_id",)),
        "serving_status": ServingTableSpec(sort_keys=("snapshot_key",)),
        "signal_routes": ServingTableSpec(sort_keys=("source_id", "source_sequence")),
        "signals": ServingTableSpec(sort_keys=("global_sequence",)),
    }
)


def _json(value: object) -> str:
    jsonable = TypeAdapter(object).dump_python(value, mode="json")
    return json.dumps(jsonable, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _frame(rows: list[dict[str, object]], columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns)


def build_serving_read_models(
    source: ServingReadModelInput,
) -> Mapping[str, pd.DataFrame]:
    """Build a complete, internally coherent serving generation in memory."""

    signals = _frame(
        [
            {
                "global_sequence": record.global_sequence,
                "signal_id": record.signal.signal_id,
                "strategy_id": record.signal.strategy_id,
                "strategy_version": record.signal.strategy_version,
                "candidate_id": record.signal.candidate_id,
                "action": record.signal.action.value,
                "event_time": record.signal.event_time,
                "available_at": record.signal.available_at,
                "expires_at": record.signal.expires_at,
                "reason_codes_json": _json(record.signal.reason_codes),
                "evidence_json": _json(dict(record.signal.evidence)),
                "dataset_snapshot_id": record.signal.dataset_snapshot_id,
                "feature_snapshot_id": record.signal.feature_snapshot_id,
            }
            for record in source.signals
        ],
        (
            "global_sequence",
            "signal_id",
            "strategy_id",
            "strategy_version",
            "candidate_id",
            "action",
            "event_time",
            "available_at",
            "expires_at",
            "reason_codes_json",
            "evidence_json",
            "dataset_snapshot_id",
            "feature_snapshot_id",
        ),
    )
    routes = _frame(
        [
            {
                "source_id": record.source_id,
                "source_sequence": record.source_sequence,
                "signal_id": record.signal_id,
                "disposition": record.disposition.value,
                "reason_code": record.reason_code,
                "target_count": record.target_count,
                "target_manifest_hash": record.target_manifest_hash,
                "targets_json": _json(record.targets),
                "routed_at": record.routed_at,
            }
            for record in source.routes
        ],
        (
            "source_id",
            "source_sequence",
            "signal_id",
            "disposition",
            "reason_code",
            "target_count",
            "target_manifest_hash",
            "targets_json",
            "routed_at",
        ),
    )
    deliveries = _frame(
        [
            {
                "outbox_id": record.outbox_id,
                "signal_id": record.signal_id,
                "recipient_id": record.target.recipient_id,
                "channel": record.target.channel.value,
                "status": record.status.value,
                "attempt_count": record.attempt_count,
                "next_attempt_at": record.next_attempt_at,
                "last_error": record.last_error,
                "updated_at": record.updated_at,
                "expires_at": record.expires_at,
            }
            for record in source.deliveries
        ],
        (
            "outbox_id",
            "signal_id",
            "recipient_id",
            "channel",
            "status",
            "attempt_count",
            "next_attempt_at",
            "last_error",
            "updated_at",
            "expires_at",
        ),
    )
    accounts = _frame(
        [
            {
                "account_id": record.account_id,
                "snapshot_id": record.snapshot_id,
                "as_of_time": record.as_of_time,
                "cash": record.cash,
                "available_cash": record.available_cash,
                "frozen_cash": record.frozen_cash,
                "realized_pnl": record.realized_pnl,
                "unrealized_pnl": record.unrealized_pnl,
                "nav": record.nav,
            }
            for record in source.paper_accounts
        ],
        (
            "account_id",
            "snapshot_id",
            "as_of_time",
            "cash",
            "available_cash",
            "frozen_cash",
            "realized_pnl",
            "unrealized_pnl",
            "nav",
        ),
    )
    holdings = _frame(
        [
            {
                "account_id": account.account_id,
                "ts_code": holding.code,
                "quantity": holding.quantity,
                "available_quantity": holding.available_quantity,
                "frozen_quantity": holding.frozen_quantity,
                "average_cost": holding.average_cost,
                "market_price": holding.market_price,
                "market_value": holding.market_price * holding.quantity,
                "unrealized_pnl": (holding.market_price - holding.average_cost) * holding.quantity,
                "as_of_time": account.as_of_time,
            }
            for account in source.paper_accounts
            for holding in account.holdings
        ],
        (
            "account_id",
            "ts_code",
            "quantity",
            "available_quantity",
            "frozen_quantity",
            "average_cost",
            "market_price",
            "market_value",
            "unrealized_pnl",
            "as_of_time",
        ),
    )
    jobs = _frame(
        [
            {
                "job_id": str(record.summary.job_id),
                "strategy_name": record.summary.strategy_name,
                "job_type": record.summary.job_type.value,
                "resource_class": record.summary.resource_class.value,
                "status": record.summary.status.value,
                "control_intent": record.summary.control_intent.value,
                "result_state": record.summary.result_state.value,
                "progress_fraction": record.summary.progress.fraction,
                "phase": record.summary.progress.phase,
                "terminal_shards": record.summary.progress.terminal_shards,
                "total_shards": record.summary.progress.total_shards,
                "eta_status": record.eta.status.value if record.eta is not None else None,
                "eta_finish_low": (
                    record.eta.finish_at.low
                    if record.eta is not None and record.eta.finish_at is not None
                    else None
                ),
                "eta_finish_center": (
                    record.eta.finish_at.center
                    if record.eta is not None and record.eta.finish_at is not None
                    else None
                ),
                "eta_finish_high": (
                    record.eta.finish_at.high
                    if record.eta is not None and record.eta.finish_at is not None
                    else None
                ),
                "deadline": record.summary.deadline,
                "updated_at": record.summary.updated_at,
                "spec_hash": record.summary.spec_hash,
            }
            for record in source.lab_jobs
        ],
        (
            "job_id",
            "strategy_name",
            "job_type",
            "resource_class",
            "status",
            "control_intent",
            "result_state",
            "progress_fraction",
            "phase",
            "terminal_shards",
            "total_shards",
            "eta_status",
            "eta_finish_low",
            "eta_finish_center",
            "eta_finish_high",
            "deadline",
            "updated_at",
            "spec_hash",
        ),
    )
    promotions = _frame(
        [
            {
                "decision_id": record.decision_id,
                "stage": record.stage.value,
                "approved": record.approved,
                "experiment_ids_json": _json(record.experiment_ids),
                "gate_failures_json": _json(record.gate_failures),
                "evidence_artifact_hash": record.evidence_artifact_hash,
                "policy_fingerprint": record.policy_fingerprint,
                "decided_at": record.decided_at,
            }
            for record in source.promotions
        ],
        (
            "decision_id",
            "stage",
            "approved",
            "experiment_ids_json",
            "gate_failures_json",
            "evidence_artifact_hash",
            "policy_fingerprint",
            "decided_at",
        ),
    )
    status = _frame(
        [
            {
                "snapshot_key": "current",
                "observed_at": source.observed_at,
                "signal_count": len(source.signals),
                "route_count": len(source.routes),
                "delivery_count": len(source.deliveries),
                "paper_account_count": len(source.paper_accounts),
                "lab_job_count": len(source.lab_jobs),
                "promotion_count": len(source.promotions),
            }
        ],
        (
            "snapshot_key",
            "observed_at",
            "signal_count",
            "route_count",
            "delivery_count",
            "paper_account_count",
            "lab_job_count",
            "promotion_count",
        ),
    )
    return MappingProxyType(
        {
            "deliveries": deliveries,
            "lab_jobs": jobs,
            "paper_accounts": accounts,
            "paper_holdings": holdings,
            "promotions": promotions,
            "serving_status": status,
            "signal_routes": routes,
            "signals": signals,
        }
    )


__all__ = [
    "SERVING_TABLE_SPECS",
    "ServingReadModelInput",
    "ServingLabJobRecord",
    "ServingSignalRecord",
    "build_serving_read_models",
]
