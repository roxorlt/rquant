"""Point-in-time dataset contract registry tests."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.data_contracts import (
    CONTRACTS_BY_ID,
    DATASET_CONTRACTS,
    DatasetContract,
    FreshnessRule,
    PriceBasis,
    VisibilityRule,
    build_contract_registry,
    is_visible,
    validate_contract_registry,
)
from rquant.dataset_backfill import DATASETS
from rquant.storage.duckdb import DuckDBStore

SHANGHAI = ZoneInfo("Asia/Shanghai")

EXPECTED_DATASET_IDS = (
    "daily_bar",
    "minute_bar",
    "auction_bar",
    "adj_factor",
    "limit_list_daily",
    "ths_daily",
    "dc_daily",
    "ths_index",
    "ths_member",
    "dc_index",
    "dc_member",
    "kpl_concept",
    "kpl_concept_daily",
    "moneyflow",
    "moneyflow_dc",
    "moneyflow_ths",
    "moneyflow_ind_ths",
    "moneyflow_ind_dc",
    "moneyflow_cnt_ths",
    "moneyflow_mkt_dc",
)


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    with DuckDBStore(tmp_path / "contracts.duckdb") as fresh_store:
        yield fresh_store


def _contract(**overrides: object) -> DatasetContract:
    values: dict[str, object] = {
        "dataset_id": "test_daily",
        "table_name": "daily_bar",
        "sources": ("test",),
        "physical_primary_key": ("ts_code", "trade_date"),
        "logical_key": ("ts_code", "trade_date"),
        "event_date_column": "trade_date",
        "event_time_column": None,
        "ingested_at_column": None,
        "price_basis": PriceBasis.RAW,
        "visibility": VisibilityRule.PANEL_CLOSE_NEXT_SESSION,
        "freshness": FreshnessRule(
            watermark_column="trade_date",
            max_trading_session_lag=1,
            required_on_open_day=True,
        ),
        "historized": True,
        "earliest_date": None,
        "allowed_missing_reasons": (),
        "backfill_dataset_id": None,
    }
    values.update(overrides)
    return DatasetContract.model_validate(values)


def test_registry_contains_exact_initial_twenty_contracts() -> None:
    assert isinstance(DATASET_CONTRACTS, tuple)
    assert tuple(contract.dataset_id for contract in DATASET_CONTRACTS) == (EXPECTED_DATASET_IDS)
    assert tuple(CONTRACTS_BY_ID) == EXPECTED_DATASET_IDS
    assert set(CONTRACTS_BY_ID) == {contract.dataset_id for contract in DATASET_CONTRACTS}


def test_registry_has_representative_mappings_and_known_earliest_dates() -> None:
    assert CONTRACTS_BY_ID["daily_bar"].table_name == "daily_bar"
    assert CONTRACTS_BY_ID["ths_daily"].table_name == "ths_index_daily"
    assert CONTRACTS_BY_ID["dc_daily"].table_name == "dc_index_daily"
    assert CONTRACTS_BY_ID["kpl_concept"].table_name == "kpl_concept_member"
    assert CONTRACTS_BY_ID["moneyflow_mkt_dc"].table_name == "moneyflow_mkt_daily"

    assert CONTRACTS_BY_ID["auction_bar"].earliest_date == date(2025, 1, 1)
    assert CONTRACTS_BY_ID["dc_daily"].earliest_date == date(2020, 1, 1)
    assert CONTRACTS_BY_ID["limit_list_daily"].earliest_date == date(2020, 1, 1)
    assert CONTRACTS_BY_ID["moneyflow"].earliest_date == date(2010, 1, 1)
    assert CONTRACTS_BY_ID["moneyflow_dc"].earliest_date == date(2023, 9, 11)
    assert CONTRACTS_BY_ID["daily_bar"].earliest_date is None


def test_registry_records_raw_facts_and_source_aware_physical_keys() -> None:
    assert CONTRACTS_BY_ID["daily_bar"].price_basis is PriceBasis.RAW
    assert CONTRACTS_BY_ID["adj_factor"].price_basis is PriceBasis.ADJUSTMENT_FACTOR
    with pytest.raises(ValidationError):
        _contract(price_basis="qfq")

    minute = CONTRACTS_BY_ID["minute_bar"]
    assert len(minute.sources) > 1
    assert minute.physical_primary_key == ("ts_code", "trade_time", "freq", "source")
    assert minute.logical_key == ("ts_code", "trade_time", "freq")
    assert "source" not in minute.logical_key


def test_board_and_moneyflow_daily_contracts_are_never_same_day_intraday() -> None:
    panel_ids = {
        "daily_bar",
        "adj_factor",
        "limit_list_daily",
        "ths_daily",
        "dc_daily",
        "dc_index",
        "dc_member",
        "kpl_concept",
        "kpl_concept_daily",
        "moneyflow",
        "moneyflow_dc",
        "moneyflow_ths",
        "moneyflow_ind_ths",
        "moneyflow_ind_dc",
        "moneyflow_cnt_ths",
        "moneyflow_mkt_dc",
    }
    assert {
        contract.dataset_id
        for contract in DATASET_CONTRACTS
        if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION
    } == panel_ids


def test_current_snapshots_are_not_misrepresented_as_historized() -> None:
    for dataset_id in ("ths_index", "ths_member"):
        contract = CONTRACTS_BY_ID[dataset_id]
        assert contract.visibility is VisibilityRule.UNKNOWN
        assert contract.event_date_column is None
        assert contract.event_time_column is None
        assert contract.historized is False

    for dataset_id in ("dc_index", "dc_member", "kpl_concept"):
        assert CONTRACTS_BY_ID[dataset_id].historized is False
    assert CONTRACTS_BY_ID["kpl_concept_daily"].historized is True


def test_models_forbid_extra_fields_and_are_deeply_immutable() -> None:
    contract = DATASET_CONTRACTS[0]
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DatasetContract.model_validate({**contract.model_dump(), "unexpected": True})
    with pytest.raises(ValidationError, match="frozen"):
        contract.table_name = "other"  # type: ignore[misc]
    with pytest.raises(TypeError):
        CONTRACTS_BY_ID["other"] = contract  # type: ignore[index]
    assert isinstance(CONTRACTS_BY_ID, MappingProxyType)


def test_freshness_rule_accepts_one_known_lag_kind_or_unknown() -> None:
    session_rule = FreshnessRule(
        watermark_column="trade_date",
        max_trading_session_lag=0,
        required_on_open_day=True,
    )
    wall_clock_rule = FreshnessRule(
        watermark_column="trade_time",
        max_wall_clock_lag=timedelta(minutes=1),
        required_on_open_day=True,
    )
    unknown_rule = FreshnessRule(
        watermark_column="updated_at",
        required_on_open_day=False,
    )

    assert session_rule.max_trading_session_lag == 0
    assert wall_clock_rule.max_wall_clock_lag == timedelta(minutes=1)
    assert unknown_rule.max_trading_session_lag is None
    assert unknown_rule.max_wall_clock_lag is None
    assert (
        _contract(
            visibility=VisibilityRule.UNKNOWN,
            event_date_column=None,
            historized=False,
            freshness=unknown_rule,
        ).freshness
        == unknown_rule
    )


@pytest.mark.parametrize(
    "values",
    [
        {"max_trading_session_lag": -1},
        {"max_wall_clock_lag": timedelta(0)},
        {
            "max_trading_session_lag": 1,
            "max_wall_clock_lag": timedelta(minutes=1),
        },
    ],
)
def test_freshness_rule_rejects_invalid_lags(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        FreshnessRule(
            watermark_column="watermark",
            required_on_open_day=True,
            **values,
        )


def test_known_visibility_rejects_unknown_freshness() -> None:
    with pytest.raises(ValidationError, match="freshness"):
        _contract(
            freshness=FreshnessRule(
                watermark_column="trade_date",
                required_on_open_day=False,
            )
        )


@pytest.mark.parametrize(
    ("visibility", "event_date_column", "event_time_column"),
    [
        (VisibilityRule.MINUTE_AS_OF, None, None),
        (VisibilityRule.AUCTION_0925, None, None),
        (VisibilityRule.PANEL_CLOSE_NEXT_SESSION, None, None),
    ],
)
def test_visibility_contracts_require_their_event_columns(
    visibility: VisibilityRule,
    event_date_column: str | None,
    event_time_column: str | None,
) -> None:
    with pytest.raises(ValidationError, match="event"):
        _contract(
            visibility=visibility,
            event_date_column=event_date_column,
            event_time_column=event_time_column,
        )


def test_unknown_visibility_is_reserved_for_undated_current_snapshots() -> None:
    unknown = FreshnessRule(
        watermark_column="updated_at",
        required_on_open_day=False,
    )
    with pytest.raises(ValidationError, match="UNKNOWN"):
        _contract(
            visibility=VisibilityRule.UNKNOWN,
            event_date_column="trade_date",
            historized=False,
            freshness=unknown,
        )
    with pytest.raises(ValidationError, match="UNKNOWN"):
        _contract(
            visibility=VisibilityRule.UNKNOWN,
            event_date_column=None,
            historized=True,
            freshness=unknown,
        )


def test_multi_source_contract_requires_source_in_physical_primary_key() -> None:
    with pytest.raises(ValidationError, match="source"):
        _contract(sources=("primary", "fallback"))


def test_registry_builder_rejects_duplicate_dataset_ids() -> None:
    contract = DATASET_CONTRACTS[0]
    with pytest.raises(ValueError, match="duplicate dataset_id"):
        build_contract_registry((contract, contract))


def test_registry_validator_rejects_keys_that_disagree_with_models(
    store: DuckDBStore,
) -> None:
    contract = DATASET_CONTRACTS[0]
    with pytest.raises(ValueError, match="registry key"):
        validate_contract_registry(
            (contract,),
            {"not_daily_bar": contract},
            store=store,
        )


def test_contract_columns_and_primary_keys_match_fresh_schema(store: DuckDBStore) -> None:
    validate_contract_registry(DATASET_CONTRACTS, CONTRACTS_BY_ID, store=store)

    for contract in DATASET_CONTRACTS:
        columns = {
            row[0]
            for row in store._conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = ?
                """,
                [contract.table_name],
            ).fetchall()
        }
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
        assert declared_columns <= columns, contract.dataset_id

        primary_key_row = store._conn.execute(
            """
            SELECT constraint_column_names
            FROM duckdb_constraints()
            WHERE schema_name = 'main'
              AND table_name = ?
              AND constraint_type = 'PRIMARY KEY'
            """,
            [contract.table_name],
        ).fetchone()
        assert primary_key_row is not None, contract.dataset_id
        assert tuple(primary_key_row[0]) == contract.physical_primary_key


def test_opted_in_backfill_contracts_map_to_the_same_physical_table() -> None:
    opted_in = {
        contract.dataset_id
        for contract in DATASET_CONTRACTS
        if contract.backfill_dataset_id is not None
    }
    assert opted_in == set(EXPECTED_DATASET_IDS[5:])
    assert opted_in < set(DATASETS)

    for contract in DATASET_CONTRACTS:
        if contract.backfill_dataset_id is None:
            continue
        backfill = DATASETS[contract.backfill_dataset_id]
        assert backfill.name == contract.backfill_dataset_id
        assert backfill.table == contract.table_name


def test_panel_close_visibility_requires_a_prior_civil_trade_date() -> None:
    contract = CONTRACTS_BY_ID["daily_bar"]
    intraday = datetime(2026, 7, 13, 14, 0, tzinfo=SHANGHAI)
    after_close = datetime(2026, 7, 13, 20, 0, tzinfo=SHANGHAI)

    assert not is_visible(contract, as_of_time=intraday, event_date=date(2026, 7, 13))
    assert not is_visible(contract, as_of_time=after_close, event_date=date(2026, 7, 13))
    assert is_visible(contract, as_of_time=intraday, event_date=date(2026, 7, 10))
    assert not is_visible(contract, as_of_time=intraday, event_date=date(2026, 7, 14))


def test_minute_visibility_localizes_naive_exchange_time() -> None:
    contract = CONTRACTS_BY_ID["minute_bar"]
    as_of = datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI)

    assert is_visible(
        contract,
        as_of_time=as_of,
        event_time=datetime(2026, 7, 13, 9, 31),
    )
    assert not is_visible(
        contract,
        as_of_time=as_of,
        event_time=datetime(2026, 7, 13, 9, 32),
    )
    assert is_visible(
        contract,
        as_of_time=datetime(2026, 7, 13, 1, 31, tzinfo=UTC),
        event_time=datetime(2026, 7, 13, 9, 31),
    )


def test_minute_visibility_rejects_missing_or_nonsensical_event_time() -> None:
    contract = CONTRACTS_BY_ID["minute_bar"]
    as_of = datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI)

    with pytest.raises(ValueError, match="event_time"):
        is_visible(contract, as_of_time=as_of)
    with pytest.raises(ValueError, match="event_time"):
        is_visible(
            contract,
            as_of_time=as_of,
            event_time="09:31",  # type: ignore[arg-type]
        )


def test_auction_visibility_uses_0925_asia_shanghai_cutoff() -> None:
    contract = CONTRACTS_BY_ID["auction_bar"]
    event_date = date(2026, 7, 13)

    assert not is_visible(
        contract,
        as_of_time=datetime(2026, 7, 13, 9, 24, 59, tzinfo=SHANGHAI),
        event_date=event_date,
    )
    assert is_visible(
        contract,
        as_of_time=datetime(2026, 7, 13, 9, 25, tzinfo=SHANGHAI),
        event_date=event_date,
    )
    assert is_visible(
        contract,
        as_of_time=datetime(2026, 7, 13, 1, 25, tzinfo=UTC),
        event_date=event_date,
    )
    assert is_visible(
        contract,
        as_of_time=datetime(2026, 7, 13, 9, 0, tzinfo=SHANGHAI),
        event_date=date(2026, 7, 10),
    )


def test_unknown_visibility_fails_closed() -> None:
    contract = CONTRACTS_BY_ID["ths_index"]
    assert not is_visible(
        contract,
        as_of_time=datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI),
    )


@pytest.mark.parametrize("dataset_id", EXPECTED_DATASET_IDS)
def test_visibility_api_requires_timezone_aware_as_of_time(dataset_id: str) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        is_visible(
            CONTRACTS_BY_ID[dataset_id],
            as_of_time=datetime(2026, 7, 13, 10, 0),
        )
