"""Small deterministic read models for the read-only serving generation."""

from __future__ import annotations

import json
import math
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from types import MappingProxyType
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import (
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    StringConstraints,
    TypeAdapter,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.delivery_contracts import OutboxRecord
from rquant.experiment_registry import PromotionDecision
from rquant.lab_eta import LabEtaEstimate
from rquant.lab_jobs import LabJobSummary
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_service_control import RuntimeServiceHealth
from rquant.serving_publisher import (
    DuckDBColumnType,
    ServingTableSpec,
    validate_serving_column_identifier,
)
from rquant.signal_bus import SignalRouteReceipt
from rquant.signal_contracts import SignalEnvelope

GenerationId = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ProjectionScalar = StrictStr | StrictInt | StrictFloat | StrictBool | None
ProjectionColumnKind = Literal["string", "int", "float", "bool", "date", "timestamp"]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_PROJECTION_CELL_BYTES = 64 * 1024
_MAX_OWNER_PROJECTION_BYTES = 7 * 1024 * 1024
_NL_SCREEN_CURSOR_TYPE = "nl_screen_page"
_NL_SCREEN_ORDER_VERSION = "trade_date_ts_code_v1"
_DUCKDB_PROJECTION_TYPES: Mapping[ProjectionColumnKind, DuckDBColumnType] = MappingProxyType(
    {
        "string": "VARCHAR",
        "int": "BIGINT",
        "float": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMPTZ",
    }
)


@dataclass(frozen=True)
class ServingProjectionContract:
    owner_dataset_id: str
    columns: tuple[tuple[str, ProjectionColumnKind], ...]
    sort_keys: tuple[str, ...]
    max_rows: int
    max_bytes: int
    max_columns: int | None = None
    allow_dynamic_columns: bool = False
    event_date_columns: tuple[str, ...] = ()
    event_time_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        column_names = self.column_names
        if len(column_names) != len(set(column_names)):
            raise ValueError("projection contract column names must be unique")
        for column_name in column_names:
            validate_serving_column_identifier(column_name)
        for sort_key in self.sort_keys:
            validate_serving_column_identifier(sort_key)
        if not set(self.sort_keys).issubset(column_names):
            raise ValueError("projection contract sort keys must reference declared columns")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _kind in self.columns)

    @property
    def column_kinds(self) -> Mapping[str, ProjectionColumnKind]:
        return MappingProxyType(dict(self.columns))


def _contract(
    owner_dataset_id: str,
    columns: tuple[tuple[str, ProjectionColumnKind], ...],
    sort_keys: tuple[str, ...],
    *,
    max_rows: int,
    max_bytes: int,
    max_columns: int | None = None,
    allow_dynamic_columns: bool = False,
    event_date_columns: tuple[str, ...] = (),
    event_time_columns: tuple[str, ...] = (),
) -> ServingProjectionContract:
    return ServingProjectionContract(
        owner_dataset_id=owner_dataset_id,
        columns=columns,
        sort_keys=sort_keys,
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_columns=max_columns,
        allow_dynamic_columns=allow_dynamic_columns,
        event_date_columns=event_date_columns,
        event_time_columns=event_time_columns,
    )


PAGE_PROJECTION_CONTRACTS: Mapping[str, ServingProjectionContract] = MappingProxyType(
    {
        "dashboard_summary": _contract(
            "runtime_health",
            (
                ("snapshot_key", "string"),
                ("latest_daily_bar", "date"),
                ("latest_screen", "date"),
                ("daily_bar_rows", "int"),
                ("monitor_event_rows", "int"),
                ("minute_bar_rows", "int"),
                ("minute_codes", "int"),
                ("minute_min_time", "timestamp"),
                ("minute_max_time", "timestamp"),
                ("host_name", "string"),
                ("monitor_state", "string"),
                ("monitor_substate", "string"),
                ("monitor_next_at", "timestamp"),
                ("monitor_last_at", "timestamp"),
                ("daily_state", "string"),
                ("daily_exec_status", "string"),
                ("daily_next_at", "timestamp"),
                ("daily_last_at", "timestamp"),
                ("dashboard_state", "string"),
                ("backup_snapshot_at", "timestamp"),
                ("backup_source_bytes", "int"),
                ("backup_compressed_bytes", "int"),
                ("backup_last_download_at", "timestamp"),
                ("backup_last_download_ip", "string"),
                ("backup_last_download_bytes", "int"),
            ),
            ("snapshot_key",),
            max_rows=1,
            max_bytes=64 * 1024,
            event_date_columns=("latest_daily_bar", "latest_screen"),
            event_time_columns=(
                "minute_min_time",
                "minute_max_time",
                "monitor_last_at",
                "daily_last_at",
                "backup_snapshot_at",
                "backup_last_download_at",
            ),
        ),
        "screen_result": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("ts_code", "string"),
                ("preset_name", "string"),
                ("name", "string"),
                ("close", "float"),
                ("pct_chg", "float"),
            ),
            ("trade_date", "preset_name", "ts_code"),
            max_rows=20_000,
            max_bytes=2 * 1024 * 1024,
            event_date_columns=("trade_date",),
        ),
        "pool2_watch": _contract(
            "signals",
            (
                ("ts_code", "string"),
                ("entry_date", "date"),
                ("body_lower", "float"),
                ("body_upper", "float"),
                ("level_40", "float"),
                ("level_30", "float"),
                ("level_20", "float"),
                ("stop_strong", "float"),
                ("stop_weak", "float"),
                ("status", "string"),
            ),
            ("status", "entry_date", "ts_code"),
            max_rows=2_000,
            max_bytes=512 * 1024,
            event_date_columns=("entry_date",),
        ),
        "stock_basic": _contract(
            "reference_slow_authority",
            (("ts_code", "string"), ("name", "string"), ("industry", "string")),
            ("ts_code",),
            max_rows=8_000,
            max_bytes=1024 * 1024,
        ),
        "risk_blacklist": _contract(
            "reference_slow_authority",
            (
                ("ts_code", "string"),
                ("list_label", "string"),
                ("expires_at", "date"),
                ("imported_at", "timestamp"),
            ),
            ("list_label", "ts_code"),
            max_rows=10_000,
            max_bytes=1024 * 1024,
        ),
        "monitor_event": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("trigger_time", "timestamp"),
                ("ts_code", "string"),
                ("level", "string"),
                ("trigger_price", "float"),
                ("level_price", "float"),
                ("trigger_type", "string"),
                ("pool", "string"),
            ),
            ("trade_date", "trigger_time", "ts_code", "level"),
            max_rows=10_000,
            max_bytes=2 * 1024 * 1024,
            event_date_columns=("trade_date",),
            event_time_columns=("trigger_time",),
        ),
        "surge_event": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("confirmed_at", "string"),
                ("ts_code", "string"),
                ("name", "string"),
                ("theme", "string"),
                ("price", "float"),
                ("pct_chg", "float"),
                ("cum_amount", "float"),
                ("rel_cum", "float"),
                ("room_to_limit_pct", "float"),
                ("status", "string"),
            ),
            ("trade_date", "confirmed_at", "ts_code"),
            max_rows=10_000,
            max_bytes=2 * 1024 * 1024,
            event_date_columns=("trade_date",),
        ),
        "pulse_history": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("as_of", "timestamp"),
                ("t", "string"),
                ("limit_up", "int"),
                ("limit_down", "int"),
                ("broken", "int"),
                ("up", "int"),
                ("down", "int"),
                ("up_ratio_pct", "float"),
                ("total", "int"),
            ),
            ("trade_date", "as_of"),
            max_rows=512,
            max_bytes=256 * 1024,
            event_date_columns=("trade_date",),
            event_time_columns=("as_of",),
        ),
        "pulse_alert": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("as_of", "timestamp"),
                ("t", "string"),
                ("kind", "string"),
                ("kind_label", "string"),
                ("before", "float"),
                ("after", "float"),
                ("window_minutes", "int"),
                ("message", "string"),
            ),
            ("trade_date", "as_of", "kind"),
            max_rows=512,
            max_bytes=512 * 1024,
            event_date_columns=("trade_date",),
            event_time_columns=("as_of",),
        ),
        "surge_runtime_config": _contract(
            "signals",
            (
                ("snapshot_key", "string"),
                ("trade_date", "date"),
                ("as_of", "timestamp"),
                ("boards_json", "string"),
                ("k_rough", "float"),
                ("k_cum", "float"),
                ("ratio_cap", "float"),
                ("skip_first_minutes", "int"),
                ("tushare_rate_per_min", "int"),
                ("require_price_strength", "bool"),
                ("max_room_to_limit_pct", "float"),
            ),
            ("snapshot_key",),
            max_rows=1,
            max_bytes=16 * 1024,
            event_date_columns=("trade_date",),
            event_time_columns=("as_of",),
        ),
        "dc_board": _contract(
            "reference_slow_authority",
            (("ts_code", "string"), ("name", "string"), ("idx_type", "string")),
            ("ts_code",),
            max_rows=2_000,
            max_bytes=256 * 1024,
        ),
        "dc_board_member": _contract(
            "reference_slow_authority",
            (("board_code", "string"), ("con_code", "string")),
            ("board_code", "con_code"),
            max_rows=50_000,
            max_bytes=5 * 1024 * 1024,
        ),
        "kpl_concept_member": _contract(
            "reference_slow_authority",
            (
                ("board_code", "string"),
                ("board_name", "string"),
                ("con_code", "string"),
            ),
            ("board_code", "con_code"),
            max_rows=50_000,
            max_bytes=5 * 1024 * 1024,
        ),
        "market_liquidity": _contract(
            "reference_slow_authority",
            (
                ("ts_code", "string"),
                ("circ_mv", "float"),
                ("avg_amount_5d", "float"),
            ),
            ("ts_code",),
            max_rows=8_000,
            max_bytes=1024 * 1024,
        ),
        "daily_bar": _contract(
            "reference_slow_authority",
            (
                ("ts_code", "string"),
                ("trade_date", "date"),
                ("open", "float"),
                ("high", "float"),
                ("low", "float"),
                ("close", "float"),
                ("vol", "float"),
            ),
            ("ts_code", "trade_date"),
            max_rows=30_000,
            max_bytes=6 * 1024 * 1024,
            event_date_columns=("trade_date",),
        ),
        "market_overview": _contract(
            "signals",
            (
                ("as_of", "timestamp"),
                ("system", "string"),
                ("board_code", "string"),
                ("board_name", "string"),
                ("amount", "float"),
                ("main_net_amount", "float"),
                ("main_net_rate", "float"),
                ("pct_chg_median", "float"),
                ("limit_up_count", "int"),
                ("broken_count", "int"),
                ("stock_count", "int"),
                ("limit_up_ratio_pct", "float"),
                ("leading_stock", "string"),
            ),
            ("as_of", "system", "board_code"),
            max_rows=3_000,
            max_bytes=1024 * 1024,
            event_time_columns=("as_of",),
        ),
        "market_snapshot": _contract(
            "signals",
            (
                ("as_of", "timestamp"),
                ("ts_code", "string"),
                ("name", "string"),
                ("price", "float"),
                ("open", "float"),
                ("high", "float"),
                ("low", "float"),
                ("pre_close", "float"),
                ("pct_chg", "float"),
                ("volume", "float"),
                ("amount", "float"),
            ),
            ("as_of", "ts_code"),
            max_rows=8_000,
            max_bytes=3 * 1024 * 1024,
            event_time_columns=("as_of",),
        ),
        "intraday_kline": _contract(
            "signals",
            (
                ("ts_code", "string"),
                ("trade_time", "timestamp"),
                ("open", "float"),
                ("high", "float"),
                ("low", "float"),
                ("close", "float"),
                ("vol", "float"),
            ),
            ("ts_code", "trade_time"),
            max_rows=10_000,
            max_bytes=2 * 1024 * 1024,
            event_time_columns=("trade_time",),
        ),
        "strategy_summary": _contract(
            "lab_jobs",
            (
                ("run_id", "string"),
                ("computed_at", "timestamp"),
                ("start_date", "date"),
                ("end_date", "date"),
                ("max_hold_days", "int"),
                ("entry_mode", "string"),
                ("profile_variant", "string"),
                ("candidates", "int"),
                ("trades", "int"),
                ("trigger_rate_pct", "float"),
                ("mean_ret_pct", "float"),
                ("median_ret_pct", "float"),
                ("win_rate_pct", "float"),
                ("best_ret_pct", "float"),
                ("worst_ret_pct", "float"),
                ("gap_stop_rate_pct", "float"),
            ),
            ("run_id", "entry_mode", "profile_variant"),
            max_rows=2_000,
            max_bytes=1024 * 1024,
            event_date_columns=("end_date",),
            event_time_columns=("computed_at",),
        ),
        "strategy_trade": _contract(
            "lab_jobs",
            (
                ("run_id", "string"),
                ("trade_id", "string"),
                ("entry_mode", "string"),
                ("profile_variant", "string"),
                ("signal_date", "date"),
                ("ts_code", "string"),
                ("name", "string"),
                ("entry_time", "timestamp"),
                ("entry_price_raw", "float"),
                ("entry_price", "float"),
                ("stop_loss_basis", "float"),
                ("take_profit_basis", "float"),
                ("volume_profile_lookbacks", "string"),
                ("volume_profile_rr", "float"),
                ("exit_time", "timestamp"),
                ("exit_price", "float"),
                ("exit_reason", "string"),
                ("ret_pct", "float"),
            ),
            ("run_id", "trade_id"),
            max_rows=10_000,
            max_bytes=5 * 1024 * 1024,
            event_date_columns=("signal_date",),
            event_time_columns=("entry_time", "exit_time"),
        ),
        "trade_calendar": _contract(
            "reference_slow_authority",
            (("trade_date", "date"), ("exchange", "string"), ("is_open", "bool")),
            ("trade_date", "exchange"),
            max_rows=5_000,
            max_bytes=256 * 1024,
        ),
        "screen_bounds": _contract(
            "signals",
            (
                ("preset_name", "string"),
                ("min_date", "date"),
                ("max_date", "date"),
                ("candidate_count", "int"),
            ),
            ("preset_name",),
            max_rows=256,
            max_bytes=128 * 1024,
            event_date_columns=("min_date", "max_date"),
        ),
        "minute_coverage": _contract(
            "signals",
            (
                ("is_total", "bool"),
                ("source", "string"),
                ("rows_count", "int"),
                ("codes_count", "int"),
                ("trade_dates", "int"),
                ("min_time", "timestamp"),
                ("max_time", "timestamp"),
            ),
            ("is_total", "source"),
            max_rows=128,
            max_bytes=128 * 1024,
            event_time_columns=("min_time", "max_time"),
        ),
        "research_gate_metadata": _contract(
            "lab_jobs",
            (
                ("strategy_name", "string"),
                ("range_start", "date"),
                ("range_end", "date"),
                ("as_of_time", "timestamp"),
                ("completed_at", "timestamp"),
                ("code_commit", "string"),
                ("audit_run_id", "string"),
                ("dataset_snapshot_id", "string"),
                ("dataset_binding_hash", "string"),
                ("coverage_ratios_json", "string"),
                ("coverage_counts_json", "string"),
                ("failures_json", "string"),
                ("metadata_ready", "bool"),
            ),
            ("strategy_name", "range_start", "range_end", "as_of_time"),
            max_rows=512,
            max_bytes=2 * 1024 * 1024,
            event_date_columns=("range_start", "range_end"),
            event_time_columns=("as_of_time", "completed_at"),
        ),
        "canvas_diagnostic": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("preset_name", "string"),
                ("step_index", "int"),
                ("rule_label", "string"),
                ("remaining_count", "int"),
            ),
            ("trade_date", "preset_name", "step_index"),
            max_rows=20_000,
            max_bytes=2 * 1024 * 1024,
            event_date_columns=("trade_date",),
        ),
        "canvas_latest_trade_date": _contract(
            "signals",
            (("snapshot_key", "string"), ("trade_date", "date")),
            ("snapshot_key",),
            max_rows=1,
            max_bytes=4 * 1024,
            event_date_columns=("trade_date",),
        ),
        "canvas_hit": _contract(
            "signals",
            (
                ("trade_date", "date"),
                ("preset_name", "string"),
                ("ts_code", "string"),
                ("row_json", "string"),
            ),
            ("trade_date", "preset_name", "ts_code"),
            max_rows=20_000,
            max_bytes=6 * 1024 * 1024,
            event_date_columns=("trade_date",),
        ),
        "canvas_definition": _contract(
            "signals",
            (
                ("name", "string"),
                ("description", "string"),
                ("pool_refs_json", "string"),
                ("created_at", "timestamp"),
                ("updated_at", "timestamp"),
                ("source", "string"),
                ("command_id", "string"),
                ("command_hash", "string"),
                ("source_identity_hash", "string"),
                ("record_hash", "string"),
                ("version_hash", "string"),
            ),
            ("name",),
            max_rows=512,
            max_bytes=2 * 1024 * 1024,
            event_time_columns=("created_at", "updated_at"),
        ),
        "nl_screen_universe": _contract(
            "reference_slow_authority",
            (
                ("trade_date", "date"),
                ("ts_code", "string"),
                ("name", "string"),
                ("is_st", "bool"),
                ("is_bj", "bool"),
                ("board_type", "string"),
                ("CLOSE[0]", "float"),
                ("PCT_CHG[0]", "float"),
            ),
            ("trade_date", "ts_code"),
            max_rows=8_000,
            max_bytes=6 * 1024 * 1024,
            max_columns=512,
            allow_dynamic_columns=True,
            event_date_columns=("trade_date",),
        ),
    }
)


def _projection_json_bytes(value: object) -> int:
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

    payload = normalize(value)
    return len(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
    )


def _event_date(value: ProjectionScalar, *, column: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ValueError(f"projection column {column} must contain ISO dates") from exc


def _event_time(value: ProjectionScalar, *, column: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"projection column {column} must contain ISO timestamps") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"projection column {column} timestamps must be timezone-aware")
    return parsed


class ServingProjectionPayload(RuntimeContractModel):
    table_name: StrictStr = Field(min_length=1)
    available_at: AwareUtcDatetime
    rows: tuple[Mapping[str, ProjectionScalar], ...] = ()

    @field_validator("rows")
    @classmethod
    def freeze_rows(
        cls,
        value: tuple[Mapping[str, ProjectionScalar], ...],
    ) -> tuple[Mapping[str, ProjectionScalar], ...]:
        return tuple(MappingProxyType(dict(row)) for row in value)

    @field_serializer("rows")
    def serialize_rows(
        self,
        value: tuple[Mapping[str, ProjectionScalar], ...],
    ) -> list[dict[str, ProjectionScalar]]:
        return [dict(row) for row in value]

    @model_validator(mode="after")
    def validate_projection(self) -> Self:
        contract = PAGE_PROJECTION_CONTRACTS.get(self.table_name)
        if contract is None:
            raise ValueError("serving projection table is not registered")
        if len(self.rows) > contract.max_rows:
            raise ValueError(f"projection {self.table_name} exceeds its row budget")
        required = set(contract.column_names)
        observed_columns: set[str] = set()
        seen_keys: set[tuple[ProjectionScalar, ...]] = set()
        available_date = self.available_at.astimezone(_SHANGHAI).date()
        for row in self.rows:
            columns = set(row)
            if contract.allow_dynamic_columns:
                if not required.issubset(columns):
                    raise ValueError(f"projection {self.table_name} rows have invalid columns")
                try:
                    for column in columns:
                        validate_serving_column_identifier(column)
                except ValueError as exc:
                    raise ValueError(
                        f"projection {self.table_name} has an unsafe dynamic column"
                    ) from exc
                if contract.max_columns is not None and len(columns) > contract.max_columns:
                    raise ValueError(f"projection {self.table_name} exceeds its column budget")
            elif columns != required:
                raise ValueError(f"projection {self.table_name} rows have invalid columns")
            observed_columns.update(columns)
            for value in row.values():
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError("projection cells must contain finite values")
                if _projection_json_bytes(value) > _MAX_PROJECTION_CELL_BYTES:
                    raise ValueError("projection cell exceeds its byte budget")
            key = tuple(row.get(column) for column in contract.sort_keys)
            if any(value is None for value in key):
                raise ValueError("projection sort keys cannot be null")
            if key in seen_keys:
                raise ValueError("projection sort keys must be unique")
            seen_keys.add(key)
            for column in contract.event_date_columns:
                observed = _event_date(row.get(column), column=column)
                if observed is not None and observed > available_date:
                    raise ValueError("projection contains future dated evidence")
            for column in contract.event_time_columns:
                observed_time = _event_time(row.get(column), column=column)
                if observed_time is not None and observed_time > self.available_at:
                    raise ValueError("projection contains future timestamp evidence")
        if contract.allow_dynamic_columns and len(self.rows) > 1:
            for row in self.rows:
                if set(row) != observed_columns:
                    raise ValueError("dynamic projection rows must use one column schema")
        if _projection_json_bytes(self.rows) > contract.max_bytes:
            raise ValueError(f"projection {self.table_name} exceeds its byte budget")
        return self


class ServingProjectionInput(ServingProjectionPayload):
    owner_dataset_id: StrictStr = Field(min_length=1)
    owner_generation_id: GenerationId

    @model_validator(mode="after")
    def validate_owner(self) -> Self:
        contract = PAGE_PROJECTION_CONTRACTS[self.table_name]
        if self.owner_dataset_id != contract.owner_dataset_id:
            raise ValueError(f"projection {self.table_name} owner does not match its contract")
        return self

    @classmethod
    def bind(
        cls,
        payload: ServingProjectionPayload,
        *,
        owner_dataset_id: str,
        owner_generation_id: str,
    ) -> ServingProjectionInput:
        if not isinstance(payload, ServingProjectionPayload):
            raise TypeError("payload must be ServingProjectionPayload")
        return cls(
            **payload.model_dump(mode="python"),
            owner_dataset_id=owner_dataset_id,
            owner_generation_id=owner_generation_id,
        )


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
    runtime_services: tuple[RuntimeServiceHealth, ...] = ()
    lab_jobs: tuple[ServingLabJobRecord, ...] = ()
    promotions: tuple[PromotionDecision, ...] = ()
    projections: tuple[ServingProjectionInput, ...] = ()

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
            (record.service_id for record in self.runtime_services),
            "runtime service_id",
        )
        self._require_unique(
            (str(record.summary.job_id) for record in self.lab_jobs),
            "job_id",
        )
        self._require_unique(
            (record.decision_id for record in self.promotions),
            "promotion decision_id",
        )
        self._require_unique(
            (projection.table_name for projection in self.projections),
            "projection table_name",
        )

        times = [record.signal.available_at for record in self.signals]
        times.extend(record.routed_at for record in self.routes)
        times.extend(record.updated_at for record in self.deliveries)
        times.extend(record.as_of_time for record in self.paper_accounts)
        times.extend(record.observed_at for record in self.runtime_services)
        times.extend(record.summary.updated_at for record in self.lab_jobs)
        times.extend(record.eta.as_of for record in self.lab_jobs if record.eta is not None)
        times.extend(record.decided_at for record in self.promotions)
        times.extend(projection.available_at for projection in self.projections)
        if any(value > self.observed_at for value in times):
            raise ValueError("serving snapshot contains future evidence")

        owner_sizes: dict[str, int] = {}
        for projection in self.projections:
            owner_sizes[projection.owner_dataset_id] = owner_sizes.get(
                projection.owner_dataset_id,
                0,
            ) + _projection_json_bytes(projection)
        oversized_owners = tuple(
            sorted(
                owner for owner, size in owner_sizes.items() if size > _MAX_OWNER_PROJECTION_BYTES
            )
        )
        if oversized_owners:
            raise ValueError(
                "serving owner projections exceed their authority byte budget: "
                + ", ".join(oversized_owners)
            )

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
        "runtime_services": ServingTableSpec(sort_keys=("service_id",)),
        "serving_status": ServingTableSpec(sort_keys=("snapshot_key",)),
        "signal_routes": ServingTableSpec(sort_keys=("source_id", "source_sequence")),
        "signals": ServingTableSpec(sort_keys=("global_sequence",)),
        "projection_status": ServingTableSpec(sort_keys=("table_name",)),
        **{
            table_name: ServingTableSpec(
                sort_keys=contract.sort_keys,
                column_types=tuple(
                    (column, _DUCKDB_PROJECTION_TYPES[kind]) for column, kind in contract.columns
                ),
            )
            for table_name, contract in PAGE_PROJECTION_CONTRACTS.items()
        },
    }
)


def serving_physical_table_specs_fingerprint() -> str:
    """Return the canonical identity of the complete serving table contract."""

    tables: dict[str, object] = {}
    for table_name, spec in sorted(SERVING_TABLE_SPECS.items()):
        contract = PAGE_PROJECTION_CONTRACTS.get(table_name)
        tables[table_name] = {
            "sort_keys": spec.sort_keys,
            "projection": (
                None
                if contract is None
                else {
                    "owner_dataset_id": contract.owner_dataset_id,
                    "columns": contract.columns,
                    "sort_keys": contract.sort_keys,
                    "max_rows": contract.max_rows,
                    "max_bytes": contract.max_bytes,
                    "max_columns": contract.max_columns,
                    "allow_dynamic_columns": contract.allow_dynamic_columns,
                    "event_date_columns": contract.event_date_columns,
                    "event_time_columns": contract.event_time_columns,
                }
            ),
        }
    return canonical_sha256(
        {
            "contract": "serving-physical-table-specs/v1",
            "tables": tables,
        }
    )


def _json(value: object) -> str:
    jsonable = TypeAdapter(object).dump_python(value, mode="json")
    return json.dumps(jsonable, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _frame(rows: list[dict[str, object]], columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns)


def _coerce_projection_column(
    values: pd.Series,
    kind: ProjectionColumnKind,
) -> pd.Series:
    if kind == "string":
        return values.astype("string")
    if kind == "int":
        return pd.to_numeric(values, errors="coerce").astype("Int64")
    if kind == "float":
        return pd.to_numeric(values, errors="coerce").astype("float64")
    if kind == "bool":
        return values.astype("boolean")
    if kind == "date":
        return pd.to_datetime(values, errors="coerce").dt.date
    if kind == "timestamp":
        return pd.to_datetime(values, errors="coerce", utc=True)
    raise ValueError(f"unsupported projection column kind: {kind}")


def _projection_frame(
    projection: ServingProjectionInput | None,
    contract: ServingProjectionContract,
) -> pd.DataFrame:
    rows = [dict(row) for row in projection.rows] if projection is not None else []
    columns = list(contract.column_names)
    if projection is not None and contract.allow_dynamic_columns and projection.rows:
        dynamic = sorted(set(projection.rows[0]).difference(columns))
        columns.extend(dynamic)
    frame = pd.DataFrame(rows, columns=columns)
    kinds = contract.column_kinds
    for column in columns:
        if column in kinds:
            frame[column] = _coerce_projection_column(frame[column], kinds[column])
    return frame


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
                "evidence_json": _json(record.signal.model_dump(mode="json")["evidence"]),
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
    runtime_services = _frame(
        [
            {
                "service_id": record.service_id,
                "plane": record.plane.value,
                "status": record.status.value,
                "stale": record.stale,
                "observed_at": record.observed_at,
                "heartbeat_at": (
                    record.heartbeat.heartbeat_at if record.heartbeat is not None else None
                ),
                "input_sequence": (
                    record.heartbeat.input_sequence if record.heartbeat is not None else -1
                ),
                "output_sequence": (
                    record.heartbeat.output_sequence if record.heartbeat is not None else -1
                ),
                "backlog_count": (
                    record.heartbeat.backlog_count if record.heartbeat is not None else 0
                ),
                "consecutive_failures": (
                    record.heartbeat.consecutive_failures if record.heartbeat is not None else 0
                ),
                "last_error": (
                    record.heartbeat.last_error if record.heartbeat is not None else None
                ),
                "spec_fingerprint": (
                    record.heartbeat.spec_fingerprint if record.heartbeat is not None else None
                ),
            }
            for record in source.runtime_services
        ],
        (
            "service_id",
            "plane",
            "status",
            "stale",
            "observed_at",
            "heartbeat_at",
            "input_sequence",
            "output_sequence",
            "backlog_count",
            "consecutive_failures",
            "last_error",
            "spec_fingerprint",
        ),
    )
    projections = {projection.table_name: projection for projection in source.projections}
    projection_tables = {
        table_name: _projection_frame(projections.get(table_name), contract)
        for table_name, contract in PAGE_PROJECTION_CONTRACTS.items()
    }
    projection_status = _frame(
        [
            {
                "table_name": table_name,
                "available": table_name in projections,
                "reason": None if table_name in projections else "projection_not_published",
                "owner_dataset_id": contract.owner_dataset_id,
                "owner_generation_id": (
                    projections[table_name].owner_generation_id
                    if table_name in projections
                    else None
                ),
                "available_at": (
                    projections[table_name].available_at if table_name in projections else None
                ),
                "row_count": len(projection_tables[table_name]),
                "max_rows": contract.max_rows,
                "max_bytes": contract.max_bytes,
            }
            for table_name, contract in PAGE_PROJECTION_CONTRACTS.items()
        ],
        (
            "table_name",
            "available",
            "reason",
            "owner_dataset_id",
            "owner_generation_id",
            "available_at",
            "row_count",
            "max_rows",
            "max_bytes",
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
                "runtime_service_count": len(source.runtime_services),
                "lab_job_count": len(source.lab_jobs),
                "promotion_count": len(source.promotions),
                "projection_count": len(source.projections),
                "unavailable_projection_count": len(PAGE_PROJECTION_CONTRACTS)
                - len(source.projections),
            }
        ],
        (
            "snapshot_key",
            "observed_at",
            "signal_count",
            "route_count",
            "delivery_count",
            "paper_account_count",
            "runtime_service_count",
            "lab_job_count",
            "promotion_count",
            "projection_count",
            "unavailable_projection_count",
        ),
    )
    return MappingProxyType(
        {
            "deliveries": deliveries,
            "lab_jobs": jobs,
            "paper_accounts": accounts,
            "paper_holdings": holdings,
            "promotions": promotions,
            "projection_status": projection_status,
            "runtime_services": runtime_services,
            "serving_status": status,
            "signal_routes": routes,
            "signals": signals,
            **projection_tables,
        }
    )


class NlScreenPageError(ValueError):
    """A cursor or bounded NL candidate read cannot safely continue."""


class NlScreenCursor(RuntimeContractModel):
    cursor_type: Literal["nl_screen_page"] = _NL_SCREEN_CURSOR_TYPE
    generation_id: GenerationId
    query_digest: GenerationId
    last_trade_date: date
    last_ts_code: StrictStr = Field(min_length=1)
    order_version: Literal["trade_date_ts_code_v1"] = _NL_SCREEN_ORDER_VERSION


@dataclass(frozen=True)
class NlScreenPage:
    rows: pd.DataFrame
    diagnostics: tuple[tuple[str, int], ...]
    next_cursor: str | None
    generation_id: str
    query_digest: str


def nl_screen_query_digest(
    normalized_plan: Mapping[str, object],
    include_columns: Sequence[str] = (),
) -> str:
    """Bind a cursor to canonical plan and output-column intent."""

    return canonical_sha256(
        {
            "contract": "nl-screen-page-query/v1",
            "plan": dict(normalized_plan),
            "include_columns": tuple(include_columns),
        }
    )


def encode_nl_screen_cursor(cursor: NlScreenCursor) -> str:
    payload = cursor.model_dump(mode="json")
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def decode_nl_screen_cursor(token: str) -> NlScreenCursor:
    if not isinstance(token, str) or not token:
        raise NlScreenPageError("nl screen cursor requires rerun: cursor is invalid")
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = json.loads(urlsafe_b64decode(padded.encode("ascii")))
        return NlScreenCursor.model_validate(payload)
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError) as exc:
        raise NlScreenPageError("nl screen cursor requires rerun: cursor is invalid") from exc


def validate_nl_screen_cursor(
    cursor: NlScreenCursor,
    *,
    generation_id: str,
    query_digest: str,
) -> None:
    if cursor.generation_id != generation_id:
        raise NlScreenPageError("nl screen cursor requires rerun: generation changed")
    if cursor.query_digest != query_digest:
        raise NlScreenPageError("nl screen cursor requires rerun: query changed")
    if cursor.order_version != _NL_SCREEN_ORDER_VERSION:
        raise NlScreenPageError("nl screen cursor requires rerun: ordering changed")


def screen_nl_projection(
    universe: pd.DataFrame,
    *,
    trade_date: str,
    rules: Sequence[Callable[[pd.DataFrame], pd.Series]],
    rule_labels: Sequence[str],
    include_columns: Sequence[str] = (),
) -> tuple[pd.DataFrame, tuple[tuple[str, int], ...]]:
    """Apply NL rules to one bounded, point-in-time serving universe in memory."""

    if len(rules) != len(rule_labels):
        raise ValueError("rules and rule_labels must have the same length")
    base_columns = ("ts_code", "name", "CLOSE[0]", "PCT_CHG[0]")
    required_columns = ("trade_date", *base_columns, *include_columns)
    missing = tuple(column for column in required_columns if column not in universe.columns)
    if missing:
        raise ValueError("nl serving projection is missing required columns: " + ", ".join(missing))

    normalized_dates = pd.to_datetime(universe["trade_date"], errors="coerce").dt.date
    requested_date = date.fromisoformat(trade_date)
    frame = universe.loc[normalized_dates.eq(requested_date)].copy()
    candidate_keys = frame.loc[:, ["trade_date", "ts_code"]]
    if candidate_keys.duplicated().any():
        raise NlScreenPageError("nl screen snapshot contains duplicate trade_date and ts_code")
    mask = pd.Series(True, index=frame.index, dtype="boolean")
    diagnostics: list[tuple[str, int]] = []
    for rule, label in zip(rules, rule_labels, strict=True):
        try:
            rule_mask = rule(frame)
        except KeyError as error:
            missing_feature = str(error.args[0]) if error.args else "unknown"
            raise ValueError(
                f"nl serving projection is missing required feature {missing_feature}"
            ) from error
        if not isinstance(rule_mask, pd.Series) or not rule_mask.index.equals(frame.index):
            raise ValueError("nl rule must return a Series aligned to the serving projection")
        mask &= rule_mask.astype("boolean").fillna(False)
        diagnostics.append((str(label), int(mask.sum())))

    result_columns = list(dict.fromkeys((*base_columns, *include_columns)))
    selected = frame.loc[mask.fillna(False)]
    selected = selected.sort_values(["trade_date", "ts_code"], kind="stable")
    result = selected.loc[:, result_columns].reset_index(drop=True)
    return result, tuple(diagnostics)


def paginate_nl_screen_projection(
    universe: pd.DataFrame,
    *,
    generation_id: str,
    trade_date: str,
    rules: Sequence[Callable[[pd.DataFrame], pd.Series]],
    rule_labels: Sequence[str],
    normalized_plan: Mapping[str, object],
    include_columns: Sequence[str] = (),
    page_size: int,
    cursor: str | None = None,
) -> NlScreenPage:
    """Screen a complete immutable universe, then apply keyset pagination."""

    if type(page_size) is not int or not 1 <= page_size <= 1_000:
        raise ValueError("nl screen page_size must be an integer between 1 and 1000")
    query_digest = nl_screen_query_digest(normalized_plan, include_columns)
    decoded = None if cursor is None else decode_nl_screen_cursor(cursor)
    if decoded is not None:
        validate_nl_screen_cursor(
            decoded,
            generation_id=generation_id,
            query_digest=query_digest,
        )
    screened, diagnostics = screen_nl_projection(
        universe,
        trade_date=trade_date,
        rules=rules,
        rule_labels=rule_labels,
        include_columns=include_columns,
    )
    requested_date = date.fromisoformat(trade_date)
    if decoded is not None:
        after = screened["ts_code"].astype("string").gt(decoded.last_ts_code)
        screened = screened.loc[after.fillna(False)]
    rows = screened.iloc[:page_size].reset_index(drop=True)
    has_next = len(screened) > len(rows)
    next_cursor = None
    if has_next:
        next_cursor = encode_nl_screen_cursor(
            NlScreenCursor(
                generation_id=generation_id,
                query_digest=query_digest,
                last_trade_date=requested_date,
                last_ts_code=str(rows.iloc[-1]["ts_code"]),
            )
        )
    return NlScreenPage(
        rows=rows,
        diagnostics=diagnostics,
        next_cursor=next_cursor,
        generation_id=generation_id,
        query_digest=query_digest,
    )


__all__ = [
    "SERVING_TABLE_SPECS",
    "PAGE_PROJECTION_CONTRACTS",
    "ServingProjectionContract",
    "ServingProjectionInput",
    "ServingProjectionPayload",
    "NlScreenCursor",
    "NlScreenPage",
    "NlScreenPageError",
    "ServingReadModelInput",
    "ServingLabJobRecord",
    "ServingSignalRecord",
    "build_serving_read_models",
    "decode_nl_screen_cursor",
    "encode_nl_screen_cursor",
    "nl_screen_query_digest",
    "paginate_nl_screen_projection",
    "screen_nl_projection",
    "serving_physical_table_specs_fingerprint",
]
