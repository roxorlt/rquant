"""Point-in-time contracts for research datasets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import cast
from zoneinfo import ZoneInfo

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EXCHANGE_TIMEZONE = ZoneInfo("Asia/Shanghai")


class PriceBasis(StrEnum):
    """Price representation persisted by a dataset."""

    RAW = "raw"
    ADJUSTMENT_FACTOR = "adjustment_factor"
    NOT_APPLICABLE = "not_applicable"


class VisibilityRule(StrEnum):
    """Earliest point at which an event may be used by a PIT query."""

    MINUTE_AS_OF = "minute_as_of"
    AUCTION_0925 = "auction_0925"
    PANEL_CLOSE_NEXT_SESSION = "panel_close_next_session"
    UNKNOWN = "unknown"


class ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class FreshnessRule(ContractModel):
    watermark_column: str = Field(min_length=1)
    max_trading_session_lag: int | None = Field(default=None, ge=0)
    max_wall_clock_lag: timedelta | None = None
    required_on_open_day: bool

    @field_validator("max_wall_clock_lag")
    @classmethod
    def validate_wall_clock_lag(cls, value: timedelta | None) -> timedelta | None:
        if value is not None and value <= timedelta(0):
            raise ValueError("max_wall_clock_lag must be positive")
        return value

    @model_validator(mode="after")
    def validate_lag_kind(self) -> FreshnessRule:
        if self.max_trading_session_lag is not None and self.max_wall_clock_lag is not None:
            raise ValueError("freshness rule must use exactly one known lag kind")
        return self

    @property
    def has_known_lag(self) -> bool:
        return self.max_trading_session_lag is not None or self.max_wall_clock_lag is not None


class SourceAvailability(ContractModel):
    source: str = Field(min_length=1)
    available_at: time

    @field_validator("available_at")
    @classmethod
    def validate_exchange_local_time(cls, value: time) -> time:
        if value.tzinfo is not None:
            raise ValueError("available_at must be an Asia/Shanghai local wall-clock time")
        return value


class DatasetContract(ContractModel):
    dataset_id: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    sources: tuple[str, ...] = Field(min_length=1)
    physical_primary_key: tuple[str, ...] = Field(min_length=1)
    logical_key: tuple[str, ...] = Field(min_length=1)
    event_date_column: str | None = None
    event_time_column: str | None = None
    ingested_at_column: str | None = None
    source_availability: tuple[SourceAvailability, ...] = ()
    price_basis: PriceBasis
    visibility: VisibilityRule
    freshness: FreshnessRule
    historized: bool
    earliest_date: date | None = None
    allowed_missing_reasons: tuple[str, ...] = ()
    backfill_dataset_id: str | None = None

    @field_validator(
        "sources",
        "physical_primary_key",
        "logical_key",
        "allowed_missing_reasons",
    )
    @classmethod
    def validate_unique_tuple(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("contract tuples cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("contract tuples cannot contain duplicates")
        return values

    @model_validator(mode="after")
    def validate_contract_invariants(self) -> DatasetContract:
        if not set(self.logical_key) <= set(self.physical_primary_key):
            raise ValueError("logical_key must be contained in physical_primary_key")
        if len(self.sources) > 1 and "source" not in self.physical_primary_key:
            raise ValueError("multi-source contracts require source in the physical primary key")

        if self.visibility is VisibilityRule.MINUTE_AS_OF:
            if self.event_time_column is None:
                raise ValueError("MINUTE_AS_OF visibility requires an event timestamp column")
        elif self.visibility in {
            VisibilityRule.AUCTION_0925,
            VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        }:
            if self.event_date_column is None:
                raise ValueError(f"{self.visibility.name} visibility requires an event date column")
        else:
            if self.event_date_column is not None or self.event_time_column is not None:
                raise ValueError("UNKNOWN visibility is reserved for undated current snapshots")
            if self.historized:
                raise ValueError("UNKNOWN visibility cannot claim a historized dataset")

        availability_sources = tuple(item.source for item in self.source_availability)
        if self.visibility is VisibilityRule.AUCTION_0925:
            if len(availability_sources) != len(set(availability_sources)) or set(
                availability_sources
            ) != set(self.sources):
                raise ValueError(
                    "source availability must exactly cover contract sources without duplicates"
                )
        elif self.source_availability:
            raise ValueError("source availability is only valid for AUCTION_0925 contracts")

        if self.visibility is not VisibilityRule.UNKNOWN and not self.freshness.has_known_lag:
            raise ValueError("known visibility requires a known freshness lag")
        return self

    def is_visible(
        self,
        *,
        as_of_time: datetime,
        event_date: date | None = None,
        event_time: datetime | None = None,
        source: str | None = None,
    ) -> bool:
        return is_visible(
            self,
            as_of_time=as_of_time,
            event_date=event_date,
            event_time=event_time,
            source=source,
        )


def _require_aware_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of_time must be timezone-aware")
    return value.astimezone(EXCHANGE_TIMEZONE)


def _require_event_date(value: date | None) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError("event_date must be a civil date")
    return value


def _localize_event_time(value: datetime | None) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("event_time must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=EXCHANGE_TIMEZONE)
    if value.utcoffset() is None:
        raise ValueError("event_time has a nonsensical timezone")
    return value.astimezone(EXCHANGE_TIMEZONE)


def is_visible(
    contract: DatasetContract,
    *,
    as_of_time: datetime,
    event_date: date | None = None,
    event_time: datetime | None = None,
    source: str | None = None,
) -> bool:
    """Evaluate one event using conservative Asia/Shanghai PIT visibility."""

    local_as_of = _require_aware_as_of(as_of_time)

    if contract.visibility is VisibilityRule.UNKNOWN:
        return False
    if contract.visibility is VisibilityRule.MINUTE_AS_OF:
        return _localize_event_time(event_time) <= local_as_of

    local_event_date = _require_event_date(event_date)
    if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION:
        return local_event_date < local_as_of.date()

    availability = next(
        (item for item in contract.source_availability if item.source == source),
        None,
    )
    if availability is None:
        raise ValueError(f"unknown or missing source for {contract.dataset_id}: {source!r}")
    if local_event_date < local_as_of.date():
        return True
    if local_event_date > local_as_of.date():
        return False
    source_cutoff = datetime.combine(
        local_event_date,
        availability.available_at,
        tzinfo=EXCHANGE_TIMEZONE,
    )
    return local_as_of >= source_cutoff


def _session_freshness(
    watermark_column: str,
    *,
    lag: int = 1,
    required_on_open_day: bool = True,
) -> FreshnessRule:
    return FreshnessRule(
        watermark_column=watermark_column,
        max_trading_session_lag=lag,
        required_on_open_day=required_on_open_day,
    )


def _unknown_freshness(watermark_column: str) -> FreshnessRule:
    return FreshnessRule(
        watermark_column=watermark_column,
        required_on_open_day=False,
    )


DATASET_CONTRACTS: tuple[DatasetContract, ...] = (
    DatasetContract(
        dataset_id="daily_bar",
        table_name="daily_bar",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        ingested_at_column=None,
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
    ),
    DatasetContract(
        dataset_id="minute_bar",
        table_name="minute_bar",
        sources=("tushare", "tushare_rt", "tushare_rt_daily"),
        physical_primary_key=("ts_code", "trade_time", "freq", "source"),
        logical_key=("ts_code", "trade_time", "freq"),
        event_time_column="trade_time",
        ingested_at_column="created_at",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.MINUTE_AS_OF,
        freshness=FreshnessRule(
            watermark_column="trade_time",
            max_wall_clock_lag=timedelta(minutes=5),
            required_on_open_day=True,
        ),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
    ),
    DatasetContract(
        dataset_id="auction_bar",
        table_name="auction_bar",
        sources=("tushare", "minute_0930_fallback"),
        physical_primary_key=("ts_code", "trade_date", "auction_type", "source"),
        logical_key=("ts_code", "trade_date", "auction_type"),
        event_date_column="trade_date",
        ingested_at_column="created_at",
        source_availability=(
            SourceAvailability(source="tushare", available_at=time(9, 26)),
            SourceAvailability(
                source="minute_0930_fallback",
                available_at=time(9, 31),
            ),
        ),
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.AUCTION_0925,
        freshness=_session_freshness("trade_date", lag=0),
        historized=True,
        earliest_date=date(2025, 1, 1),
        allowed_missing_reasons=(),
    ),
    DatasetContract(
        dataset_id="adj_factor",
        table_name="adj_factor",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.ADJUSTMENT_FACTOR,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
    ),
    DatasetContract(
        dataset_id="limit_list_daily",
        table_name="limit_list_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date", "limit_status"),
        logical_key=("ts_code", "trade_date", "limit_status"),
        event_date_column="trade_date",
        ingested_at_column="created_at",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=date(2020, 1, 1),
        allowed_missing_reasons=(),
    ),
    DatasetContract(
        dataset_id="ths_daily",
        table_name="ths_index_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="ths_daily",
    ),
    DatasetContract(
        dataset_id="dc_daily",
        table_name="dc_index_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=date(2020, 1, 1),
        allowed_missing_reasons=(),
        backfill_dataset_id="dc_daily",
    ),
    DatasetContract(
        dataset_id="ths_index",
        table_name="ths_board",
        sources=("tushare",),
        physical_primary_key=("ts_code",),
        logical_key=("ts_code",),
        event_date_column=None,
        event_time_column=None,
        ingested_at_column="updated_at",
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.UNKNOWN,
        freshness=_unknown_freshness("updated_at"),
        historized=False,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="ths_index",
    ),
    DatasetContract(
        dataset_id="ths_member",
        table_name="ths_board_member",
        sources=("tushare",),
        physical_primary_key=("board_code", "con_code"),
        logical_key=("board_code", "con_code"),
        event_date_column=None,
        event_time_column=None,
        ingested_at_column="updated_at",
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.UNKNOWN,
        freshness=_unknown_freshness("updated_at"),
        historized=False,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="ths_member",
    ),
    DatasetContract(
        dataset_id="dc_index",
        table_name="dc_board",
        sources=("tushare",),
        physical_primary_key=("ts_code",),
        logical_key=("ts_code",),
        event_date_column="trade_date",
        ingested_at_column="updated_at",
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=False,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="dc_index",
    ),
    DatasetContract(
        dataset_id="dc_member",
        table_name="dc_board_member",
        sources=("tushare",),
        physical_primary_key=("board_code", "con_code"),
        logical_key=("board_code", "con_code"),
        event_date_column="trade_date",
        ingested_at_column="updated_at",
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=False,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="dc_member",
    ),
    DatasetContract(
        dataset_id="kpl_concept",
        table_name="kpl_concept_member",
        sources=("tushare",),
        physical_primary_key=("board_code", "con_code"),
        logical_key=("board_code", "con_code"),
        event_date_column="trade_date",
        ingested_at_column="created_at",
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness(
            "trade_date",
            lag=30,
            required_on_open_day=False,
        ),
        historized=False,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="kpl_concept",
    ),
    DatasetContract(
        dataset_id="kpl_concept_daily",
        table_name="kpl_concept_member_daily",
        sources=("tushare",),
        physical_primary_key=("trade_date", "board_code", "con_code"),
        logical_key=("trade_date", "board_code", "con_code"),
        event_date_column="trade_date",
        ingested_at_column="created_at",
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness(
            "trade_date",
            lag=30,
            required_on_open_day=False,
        ),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="kpl_concept_daily",
    ),
    DatasetContract(
        dataset_id="moneyflow",
        table_name="moneyflow_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date", "source"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        ingested_at_column="created_at",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=date(2010, 1, 1),
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow",
    ),
    DatasetContract(
        dataset_id="moneyflow_dc",
        table_name="moneyflow_dc_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=date(2023, 9, 11),
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow_dc",
    ),
    DatasetContract(
        dataset_id="moneyflow_ths",
        table_name="moneyflow_ths_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow_ths",
    ),
    DatasetContract(
        dataset_id="moneyflow_ind_ths",
        table_name="moneyflow_ind_ths_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow_ind_ths",
    ),
    DatasetContract(
        dataset_id="moneyflow_ind_dc",
        table_name="moneyflow_ind_dc_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow_ind_dc",
    ),
    DatasetContract(
        dataset_id="moneyflow_cnt_ths",
        table_name="moneyflow_cnt_ths_daily",
        sources=("tushare",),
        physical_primary_key=("ts_code", "trade_date"),
        logical_key=("ts_code", "trade_date"),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow_cnt_ths",
    ),
    DatasetContract(
        dataset_id="moneyflow_mkt_dc",
        table_name="moneyflow_mkt_daily",
        sources=("tushare",),
        physical_primary_key=("trade_date",),
        logical_key=("trade_date",),
        event_date_column="trade_date",
        price_basis=PriceBasis.RAW,
        visibility=VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        freshness=_session_freshness("trade_date"),
        historized=True,
        earliest_date=None,
        allowed_missing_reasons=(),
        backfill_dataset_id="moneyflow_mkt_dc",
    ),
)


def _duplicate_dataset_ids(contracts: Sequence[DatasetContract]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for contract in contracts:
        if contract.dataset_id in seen and contract.dataset_id not in duplicates:
            duplicates.append(contract.dataset_id)
        seen.add(contract.dataset_id)
    return tuple(duplicates)


def build_contract_registry(
    contracts: Sequence[DatasetContract],
) -> Mapping[str, DatasetContract]:
    """Build a read-only id mapping without losing duplicate-id evidence."""

    duplicates = _duplicate_dataset_ids(contracts)
    if duplicates:
        raise ValueError(f"duplicate dataset_id values: {', '.join(duplicates)}")
    return MappingProxyType({contract.dataset_id: contract for contract in contracts})


def validate_contract_registry_shape(
    contracts: Sequence[DatasetContract],
    registry: Mapping[str, DatasetContract],
) -> None:
    """Validate only immutable model and registry identity invariants."""

    for contract in contracts:
        DatasetContract.model_validate(contract.model_dump())
    for contract in registry.values():
        DatasetContract.model_validate(contract.model_dump())

    duplicates = _duplicate_dataset_ids(contracts)
    if duplicates:
        raise ValueError(f"duplicate dataset_id values: {', '.join(duplicates)}")
    for key, contract in registry.items():
        if key != contract.dataset_id:
            raise ValueError(
                f"registry key {key!r} does not match model id {contract.dataset_id!r}"
            )
    declared_by_id = {contract.dataset_id: contract for contract in contracts}
    for key, contract in registry.items():
        if declared_by_id.get(key) != contract:
            raise ValueError(f"registry value for {key!r} differs from declared contract")
    expected_ids = {contract.dataset_id for contract in contracts}
    if len(registry) != len(contracts) or set(registry) != expected_ids:
        raise ValueError("registry keys do not exactly cover the declared dataset ids")


def _validate_backfill_mappings(
    contracts: Sequence[DatasetContract],
    backfill_tables: Mapping[str, str],
) -> None:
    for contract in contracts:
        if contract.backfill_dataset_id is None:
            continue
        backfill_table = backfill_tables.get(contract.backfill_dataset_id)
        if backfill_table is None:
            raise ValueError(f"unknown backfill dataset id: {contract.backfill_dataset_id}")
        if backfill_table != contract.table_name:
            raise ValueError(
                "backfill table mismatch for "
                f"{contract.dataset_id}: {backfill_table} != {contract.table_name}"
            )


def _validate_schema_contracts(
    contracts: Sequence[DatasetContract],
    connection: duckdb.DuckDBPyConnection,
) -> None:
    for contract in contracts:
        columns = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = ?
                """,
                [contract.table_name],
            ).fetchall()
        }
        if not columns:
            raise ValueError(
                f"contract table does not exist in a fresh schema: {contract.table_name}"
            )
        declared_columns = {
            *contract.physical_primary_key,
            *contract.logical_key,
            contract.freshness.watermark_column,
        }
        declared_columns.update(
            column
            for column in (
                contract.event_date_column,
                contract.event_time_column,
                contract.ingested_at_column,
            )
            if column is not None
        )
        missing_columns = declared_columns - columns
        if missing_columns:
            raise ValueError(
                f"contract {contract.dataset_id} declares missing columns: "
                f"{', '.join(sorted(missing_columns))}"
            )

        primary_key_row = connection.execute(
            """
            SELECT constraint_column_names
            FROM duckdb_constraints()
            WHERE schema_name = 'main'
              AND table_name = ?
              AND constraint_type = 'PRIMARY KEY'
            """,
            [contract.table_name],
        ).fetchone()
        if primary_key_row is None:
            raise ValueError(f"contract table has no primary key: {contract.table_name}")
        primary_key_value = primary_key_row[0]
        if not isinstance(primary_key_value, list) or not all(
            isinstance(column, str) for column in primary_key_value
        ):
            raise ValueError(f"invalid primary key metadata for {contract.table_name}")
        actual_primary_key = tuple(cast(list[str], primary_key_value))
        if actual_primary_key != contract.physical_primary_key:
            raise ValueError(
                f"primary key mismatch for {contract.dataset_id}: "
                f"{actual_primary_key} != {contract.physical_primary_key}"
            )


def validate_contract_registry(
    contracts: Sequence[DatasetContract],
    registry: Mapping[str, DatasetContract],
    *,
    connection: duckdb.DuckDBPyConnection,
    backfill_tables: Mapping[str, str],
) -> None:
    """Fully validate registry shape, backfill links, columns, and physical PKs."""

    validate_contract_registry_shape(contracts, registry)
    _validate_backfill_mappings(contracts, backfill_tables)
    _validate_schema_contracts(contracts, connection)


CONTRACTS_BY_ID: Mapping[str, DatasetContract] = build_contract_registry(DATASET_CONTRACTS)
validate_contract_registry_shape(DATASET_CONTRACTS, CONTRACTS_BY_ID)
