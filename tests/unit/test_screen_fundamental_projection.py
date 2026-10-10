"""A screen may use only the current, verified daily fundamental receipt."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.fundamental_daily import (
    FinancialSource,
    FundamentalDailyQuery,
    FundamentalDailyVersion,
    FundamentalFieldEvidence,
    ValuationSource,
    _version_identity,
)
from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.screen.loader import ScreeningFactError, load_universe
from rquant.screen.replica_source import (
    ScreenReplicaChangedError,
    ScreenReplicaDataError,
    VerifiedReplicaScreenSource,
)
from rquant.screen.rules import gt
from rquant.storage.duckdb import DuckDBStore

_DAY = date(2026, 4, 15)
_CODE = "600001.SH"
_FIELDS = ("pe_ttm", "pb", "dv_ttm", "roe", "or_yoy", "netprofit_yoy")
_COLUMNS = tuple(f"{name.upper()}[0]" for name in _FIELDS)


def _publish(primary: Path, replica: Path) -> None:
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )


def _world(tmp_path: Path) -> tuple[Path, Path, VerifiedReplicaScreenSource]:
    primary = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    with DuckDBStore(primary) as store:
        store._conn.executemany(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, 'fixture', ?)",
            [(day, datetime(2026, 4, 16, tzinfo=UTC)) for day in (_DAY, _DAY - timedelta(days=1))],
        )
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close, pct_chg) VALUES (?, ?, 10, 1)",
            [_CODE, _DAY],
        )
        store._conn.execute(
            "INSERT INTO daily_state (ts_code, trade_date, is_bj, board_type) "
            "VALUES (?, ?, FALSE, 'main')",
            [_CODE, _DAY],
        )
    _publish(primary, replica)
    return primary, replica, VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica)


def _version(
    *,
    revision: int,
    pe: Decimal | float | None = 10.0,
    roe: float = 12.0,
) -> FundamentalDailyVersion:
    decision = datetime(2026, 4, 15, 17, tzinfo=ZoneInfo("Asia/Shanghai"))
    values = (pe, 2.0, 1.5, roe, 8.0, 9.0)
    digest = f"{revision:064x}"
    fields = {
        name: FundamentalFieldEvidence(
            field=name,
            source_api="daily_basic" if index < 3 else "fina_indicator",
            status="selected" if value is not None else "unknown",
            reason="visible" if value is not None else "missing_value",
            value=Decimal(str(value)) if value is not None else None,
            decision_at=decision,
            source_date=_DAY - timedelta(days=1) if index < 3 else date(2026, 3, 31),
            source_version_sha256=digest,
        )
        for index, (name, value) in enumerate(zip(_FIELDS, values, strict=True))
    }
    financial = FinancialSource(
        archive_id=f"fixture-{revision}",
        last_observed_at=None,
        anchor_generation=None,
        anchor_record_sha256=None,
        tip_request_id=None,
        tip_file_sha256=None,
        observation_sha256=None,
    )
    valuation = ValuationSource(
        trade_date=_DAY - timedelta(days=1),
        candidate_generation_id=digest,
        row_sha256=digest,
        row_observed_at=None,
        first_observed_at=None,
        source_generation_id=None,
        source_sequence=None,
        source_batch_id=None,
        revision=None,
        observed_at=None,
        valuation_observed=None,
    )
    query = FundamentalDailyQuery(ts_code=_CODE, trade_date=_DAY)
    return FundamentalDailyVersion(
        version_id=_version_identity(
            query,
            decision,
            date(2026, 3, 31),
            "observed_period",
            financial,
            valuation,
            fields,
        ),
        ts_code=_CODE,
        trade_date=_DAY,
        revision=revision,
        decision_at=decision,
        target_report_period=date(2026, 3, 31),
        target_period_reason="observed_period",
        financial_source=financial,
        valuation_source=valuation,
        fields=fields,
    )


def _insert_version(store: DuckDBStore, version: FundamentalDailyVersion) -> None:
    store._conn.execute(
        "INSERT INTO fundamental_daily_version VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            version.version_id,
            version.ts_code,
            version.trade_date,
            version.revision,
            version.decision_at,
            version.target_report_period,
            version.target_period_reason,
            version.financial_source.model_dump_json(),
            version.valuation_source.model_dump_json(),
            json.dumps(
                {name: item.model_dump(mode="json") for name, item in version.fields.items()},
                sort_keys=True,
                separators=(",", ":"),
            ),
            *(
                float(version.fields[name].value)
                if version.fields[name].value is not None
                else None
                for name in _FIELDS
            ),
        ],
    )


def test_only_current_head_is_projected_once_from_bound_replica(tmp_path: Path) -> None:
    primary, replica, source = _world(tmp_path)
    a = _version(revision=1, pe=8, roe=11)
    b = _version(revision=2, pe=18, roe=13)
    with DuckDBStore(primary) as store:
        _insert_version(store, a)
        _insert_version(store, b)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [_CODE, _DAY, b.version_id, b.revision],
        )
    _publish(primary, replica)

    snapshot = source.load(
        _DAY,
        [gt("PE_TTM[0]", 15)],
        include_columns=_COLUMNS,
        expected_identity=source.generation_identity(),
    )
    assert len(snapshot.frame) == 1
    assert snapshot.frame.loc[0, list(_COLUMNS)].tolist() == [18, 2, 1.5, 13, 8, 9]
    assert snapshot.frame.loc[0, "PE_TTM[0]"] != 8


def test_old_daily_rows_without_head_leave_all_six_values_unknown(tmp_path: Path) -> None:
    _, _, source = _world(tmp_path)
    snapshot = source.load(
        _DAY,
        [gt("PE_TTM[0]", 0)],
        include_columns=_COLUMNS,
        expected_identity=source.generation_identity(),
    )
    assert snapshot.frame.loc[:, list(_COLUMNS)].isna().all().all()
    assert not bool(gt("PE_TTM[0]", 0)(snapshot.frame).iloc[0])


def test_operational_loader_cannot_read_fundamental_from_primary(tmp_path: Path) -> None:
    primary, _, _ = _world(tmp_path)
    with DuckDBStore(primary) as store, pytest.raises(ScreeningFactError):
        load_universe(
            _DAY.isoformat(),
            lookback=0,
            store=store,
            required_columns={"PE_TTM[0]"},
        )


@pytest.mark.parametrize(
    "damage",
    [
        "missing_table",
        "missing_head_table",
        "missing_column",
        "missing_version",
        "wrong_day",
        "wrong_decision",
        "corrupt_value",
        "bad_json",
    ],
)
def test_missing_or_corrupt_fundamental_receipt_fails_closed(
    tmp_path: Path,
    damage: str,
) -> None:
    primary, replica, source = _world(tmp_path)
    version = _version(revision=1)
    with DuckDBStore(primary) as store:
        _insert_version(store, version)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [_CODE, _DAY, version.version_id, version.revision],
        )
        if damage == "missing_table":
            store._conn.execute("DROP TABLE fundamental_daily_version")
        elif damage == "missing_head_table":
            store._conn.execute("DROP TABLE fundamental_daily_head")
        elif damage == "missing_column":
            store._conn.execute("ALTER TABLE fundamental_daily_version DROP COLUMN roe")
        elif damage == "missing_version":
            store._conn.execute("UPDATE fundamental_daily_head SET version_id = ?", ["0" * 64])
        elif damage == "wrong_day":
            store._conn.execute(
                "UPDATE fundamental_daily_version SET trade_date = ?",
                [_DAY - timedelta(days=1)],
            )
        elif damage == "wrong_decision":
            store._conn.execute(
                "UPDATE fundamental_daily_version SET decision_at = ?",
                [datetime(2026, 4, 15, 18, tzinfo=ZoneInfo("Asia/Shanghai"))],
            )
        elif damage == "corrupt_value":
            store._conn.execute("UPDATE fundamental_daily_version SET pe_ttm = 999")
        elif damage == "bad_json":
            store._conn.execute("UPDATE fundamental_daily_version SET fields_json = '[]'")
    _publish(primary, replica)

    with pytest.raises(ScreenReplicaDataError):
        source.load(
            _DAY,
            [gt("PE_TTM[0]", 0)],
            expected_identity=source.generation_identity(),
        )


def test_fundamental_request_requires_exact_t17_and_catalog_identity(tmp_path: Path) -> None:
    primary, replica, source = _world(tmp_path)
    with DuckDBStore(primary) as store:
        version = _version(revision=1)
        _insert_version(store, version)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [_CODE, _DAY, version.version_id, version.revision],
        )
    _publish(primary, replica)

    with pytest.raises(ScreenReplicaChangedError):
        source.load(_DAY, [gt("PE_TTM[0]", 0)])
    with pytest.raises(ScreenReplicaDataError):
        source.load(
            _DAY,
            [gt("PE_TTM[0]", 0)],
            decision_at=datetime(2026, 4, 15, 18, tzinfo=ZoneInfo("Asia/Shanghai")),
            expected_identity=source.generation_identity(),
        )
    before = source.generation_identity()
    with DuckDBStore(primary) as store:
        store._conn.execute("UPDATE daily_bar SET close = 11 WHERE trade_date = ?", [_DAY])
    _publish(primary, replica)
    with pytest.raises(ScreenReplicaChangedError):
        source.load(_DAY, [gt("PE_TTM[0]", 0)], expected_identity=before)


def test_unknown_pe_does_not_become_zero_or_match_gt(tmp_path: Path) -> None:
    primary, replica, source = _world(tmp_path)
    with DuckDBStore(primary) as store:
        version = _version(revision=1, pe=None)
        _insert_version(store, version)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [_CODE, _DAY, version.version_id, version.revision],
        )
    _publish(primary, replica)
    result = source.load(
        _DAY,
        [gt("PE_TTM[0]", -1)],
        expected_identity=source.generation_identity(),
    ).frame
    assert pd.isna(result.loc[0, "PE_TTM[0]"])
    assert not bool(gt("PE_TTM[0]", -1)(result).iloc[0])


def test_unrepresentable_numeric_receipt_is_not_projected(tmp_path: Path) -> None:
    primary, replica, source = _world(tmp_path)
    with DuckDBStore(primary) as store:
        version = _version(revision=1, pe=Decimal("1e10000"))
        _insert_version(store, version)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [_CODE, _DAY, version.version_id, version.revision],
        )
    _publish(primary, replica)
    with pytest.raises(ScreenReplicaDataError):
        source.load(
            _DAY,
            [gt("PE_TTM[0]", 0)],
            expected_identity=source.generation_identity(),
        )


def test_fundamental_history_offset_and_unknown_field_are_rejected(tmp_path: Path) -> None:
    _, _, source = _world(tmp_path)
    with pytest.raises(ValueError, match="offset|dependency"):
        source.load(_DAY, [gt("ROE[1]", 10)])
    with pytest.raises(ValueError, match="dependency"):
        source.load(_DAY, [gt("EPS[0]", 1)])
