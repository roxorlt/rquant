from __future__ import annotations

import base64
import inspect
import os
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

import rquant.reference_slow_source as reference_slow_source_module
from rquant.reference_slow_source import (
    ReferenceAdjustmentSourceFact,
    ReferenceDailySourceFact,
    ReferenceSecuritySourceFact,
    ReferenceSlowSourceError,
    ReferenceSuspensionSourceFact,
    assemble_reference_slow_source_snapshot,
    capture_reference_slow_source_snapshot,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import (
    build_builtin_registry,
    reference_slow_publisher_builder,
    reference_slow_source_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.strict_json import canonical_json_bytes

COMMIT = "a" * 40
TARGET_DATE = date(2026, 7, 31)
PRIOR_DATE = date(2026, 7, 30)
OBSERVED_AT = datetime(2026, 7, 31, 1, 0, tzinfo=UTC)
PUBLICATION_CAPABILITIES = {
    "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "test-reference-v1",
    "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": (b"reference-publication-test-secret-0001".hex()),
}


@pytest.fixture(autouse=True)
def _reference_publication_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "reference-publication-hmac.json"
    path.write_bytes(
        canonical_json_bytes(
            {
                "key_id": "test-reference-v1",
                "secret_hex": b"reference-publication-test-secret-0001".hex(),
            }
        )
    )
    path.chmod(0o600)
    monkeypatch.setenv("RQ_REFERENCE_PUBLICATION_HMAC_FILE", str(path))
    private_key = tmp_path / "reference-source-ed25519"
    subprocess.run(
        ("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)),
        check=True,
    )
    private_key.chmod(0o600)
    monkeypatch.setenv("RQ_REFERENCE_SOURCE_SIGNING_KEY_ID", "source-test-v1")
    monkeypatch.setenv(
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY",
        private_key.read_text(encoding="ascii"),
    )
    monkeypatch.setenv(
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
        private_key.with_suffix(".pub").read_text(encoding="ascii").strip(),
    )


def _runtime_capabilities() -> dict[str, str]:
    return {
        **PUBLICATION_CAPABILITIES,
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": os.environ["RQ_REFERENCE_SOURCE_SIGNING_KEY_ID"],
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": base64.b64encode(
            os.environ["RQ_REFERENCE_SOURCE_PRIVATE_KEY"].encode("ascii")
        ).decode("ascii"),
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY": os.environ["RQ_REFERENCE_SOURCE_PUBLIC_KEY"],
    }


def _calendar(*, producer_commit: str = COMMIT) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=producer_commit,
        coverage_start=date(2026, 7, 29),
        coverage_end=date(2026, 8, 3),
        open_dates=(date(2026, 7, 29), PRIOR_DATE, TARGET_DATE, date(2026, 8, 3)),
        generated_at=datetime(2026, 7, 28, tzinfo=UTC),
    )


def _database(tmp_path: Path) -> Path:
    path = tmp_path / "daily.duckdb"
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, close DOUBLE)")
        connection.execute(
            "CREATE TABLE adj_factor(ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, ?)",
            [
                ("300001.SZ", PRIOR_DATE, 20.0),
                ("600000.SH", PRIOR_DATE, 10.0),
            ],
        )
        connection.executemany(
            "INSERT INTO adj_factor VALUES (?, ?, ?)",
            [
                ("300001.SZ", PRIOR_DATE, 1.0),
                ("600000.SH", PRIOR_DATE, 1.0),
            ],
        )
    finally:
        connection.close()
    os.chmod(path, 0o600)
    return path.resolve()


def test_prior_daily_load_is_bound_to_verified_inode_during_path_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_dir = tmp_path / "trusted"
    malicious_dir = tmp_path / "malicious"
    trusted_dir.mkdir()
    malicious_dir.mkdir()
    trusted_path = _database(trusted_dir)
    malicious_path = _database(malicious_dir)
    with duckdb.connect(str(malicious_path)) as connection:
        connection.execute("UPDATE daily_bar SET close = close + 1000")
    malicious_path.chmod(0o600)
    original_connect = duckdb.connect
    displaced_path = tmp_path / "displaced-trusted.duckdb"
    attack_attempted = False

    def connect_with_path_aba(
        database: str,
        *args: object,
        **kwargs: object,
    ) -> duckdb.DuckDBPyConnection:
        nonlocal attack_attempted
        if Path(database) == trusted_path:
            attack_attempted = True
            os.replace(trusted_path, displaced_path)
            os.replace(malicious_path, trusted_path)
            try:
                return original_connect(database, *args, **kwargs)
            finally:
                os.replace(trusted_path, malicious_path)
                os.replace(displaced_path, trusted_path)
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", connect_with_path_aba)

    rows = reference_slow_source_module._load_prior_daily(
        trusted_path,
        prior_trade_date=PRIOR_DATE,
    )

    assert rows == (("300001.SZ", 20.0, 1.0), ("600000.SH", 10.0, 1.0))
    assert not attack_attempted


def _source_limits(**overrides: object):
    values = {
        "snapshot_max_bytes": 64 * 1024 * 1024,
        "snapshot_min_free_bytes": 16 * 1024 * 1024,
        "snapshot_copy_timeout_seconds": 30.0,
        "query_chunk_rows": 2,
        "max_response_rows": 10_000,
        "max_response_bytes": 8 * 1024 * 1024,
    }
    values.update(overrides)
    return reference_slow_source_module.ReferenceSlowSourceLimits(**values)


def test_verified_snapshot_enforces_size_and_free_space_headroom(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    size = database.stat().st_size

    with (
        pytest.raises(ReferenceSlowSourceError, match="maximum byte budget"),
        reference_slow_source_module._verified_database_snapshot(
            database,
            limits=_source_limits(snapshot_max_bytes=size - 1),
            monotonic_deadline=10.0,
            monotonic_clock=lambda: 0.0,
        ),
    ):
        pass

    monkeypatch.setattr(
        reference_slow_source_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(total=size * 2, used=size, free=size),
    )
    with (
        pytest.raises(ReferenceSlowSourceError, match="free-space headroom"),
        reference_slow_source_module._verified_database_snapshot(
            database,
            limits=_source_limits(snapshot_min_free_bytes=1),
            monotonic_deadline=10.0,
            monotonic_clock=lambda: 0.0,
        ),
    ):
        pass


def test_verified_snapshot_enforces_monotonic_copy_deadline(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    ticks = iter((0.0, 0.5, 1.01))

    with (
        pytest.raises(ReferenceSlowSourceError, match="copy deadline"),
        reference_slow_source_module._verified_database_snapshot(
            database,
            limits=_source_limits(),
            monotonic_deadline=1.0,
            monotonic_clock=lambda: next(ticks),
        ),
    ):
        pass


def test_verified_snapshot_rejects_transient_wal_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    wal_path = Path(f"{database}.wal")
    original_read = reference_slow_source_module.os.read
    injected = False

    def read_with_transient_wal(descriptor: int, size: int) -> bytes:
        nonlocal injected
        chunk = original_read(descriptor, size)
        if chunk and not injected:
            injected = True
            wal_path.write_bytes(b"transient")
            wal_path.unlink()
        return chunk

    monkeypatch.setattr(reference_slow_source_module.os, "read", read_with_transient_wal)

    with (
        pytest.raises(ReferenceSlowSourceError, match="WAL|directory changed"),
        reference_slow_source_module._verified_database_snapshot(
            database,
            limits=_source_limits(),
            monotonic_deadline=100.0,
            monotonic_clock=lambda: 1.0,
        ),
    ):
        pass

    assert injected is True


def test_prior_daily_query_streams_and_fails_closed_at_row_limit(tmp_path: Path) -> None:
    database = _database(tmp_path)

    with pytest.raises(ReferenceSlowSourceError, match="row limit"):
        reference_slow_source_module._load_prior_daily(
            database,
            prior_trade_date=PRIOR_DATE,
            limits=_source_limits(max_response_rows=1),
            monotonic_deadline=10.0,
            monotonic_clock=lambda: 0.0,
        )


def test_nl_universe_capture_has_no_prevalidation_limit() -> None:
    source = inspect.getsource(reference_slow_source_module._load_database_reference_evidence)

    nl_query = source[source.index('"nl_screen_universe"') :]
    assert "LIMIT 8000" not in nl_query


def test_source_frame_fails_closed_before_copy_when_response_exceeds_budget() -> None:
    frame = pd.DataFrame(
        [{"ts_code": "600000.SH", "trade_date": TARGET_DATE, "padding": "x" * 1024}]
    )

    with pytest.raises(ReferenceSlowSourceError, match="byte limit"):
        reference_slow_source_module._required_frame(
            frame,
            label="stock_st",
            columns={"ts_code", "trade_date"},
            limits=_source_limits(max_response_bytes=128),
        )


class _Adapter:
    def __init__(
        self,
        *,
        include_all_adjustments: bool = True,
        include_outside_universe_status: bool = False,
    ) -> None:
        self.include_all_adjustments = include_all_adjustments
        self.include_outside_universe_status = include_outside_universe_status
        self.calls: list[str] = []

    def stock_basic(self, list_status: str = "L") -> pd.DataFrame:
        self.calls.append(f"stock_basic:{list_status}")
        if list_status != "L":
            return pd.DataFrame(columns=["ts_code", "name", "list_date", "delist_date", "market"])
        return pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "name": "成长样本",
                    "list_date": "20200102",
                    "delist_date": None,
                    "market": "创业板",
                },
                {
                    "ts_code": "600000.SH",
                    "name": "普通样本",
                    "list_date": "19991110",
                    "delist_date": None,
                    "market": "主板",
                },
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.calls.append("stock_st")
        rows = [{"ts_code": "600000.SH", "trade_date": trade_date}]
        if self.include_outside_universe_status:
            rows.append({"ts_code": "688888.SH", "trade_date": trade_date})
        return pd.DataFrame(rows)

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
        self.calls.append("suspend_d")
        rows = [
            {
                "ts_code": "600000.SH",
                "trade_date": trade_date,
                "suspend_timing": "全天",
                "suspend_type": "S",
            }
        ]
        if self.include_outside_universe_status:
            rows.append(
                {
                    "ts_code": "688888.SH",
                    "trade_date": trade_date,
                    "suspend_timing": "全天",
                    "suspend_type": "S",
                }
            )
        return pd.DataFrame(rows)

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        self.calls.append("adj_factor")
        rows = [
            {"ts_code": "300001.SZ", "trade_date": trade_date, "adj_factor": 2.0},
            {"ts_code": "600000.SH", "trade_date": trade_date, "adj_factor": 1.0},
        ]
        return pd.DataFrame(rows if self.include_all_adjustments else rows[:1])


def test_captures_exact_pre_market_sources_into_one_sealed_snapshot(tmp_path: Path) -> None:
    snapshot = capture_reference_slow_source_snapshot(
        database_path=_database(tmp_path),
        adapter=_Adapter(),
        calendar=_calendar(),
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
    )

    assert snapshot.target_trade_date == TARGET_DATE
    assert snapshot.captured_at == OBSERVED_AT
    assert snapshot.suspended_codes == ("600000.SH",)
    assert set(snapshot.source_snapshot_ids) == {
        "calendar",
        "daily",
        "security",
        "suspension",
    }
    assert snapshot.daily_facts[0].ts_code == "300001.SZ"
    assert snapshot.daily_facts[0].prior_adj_factor == 1.0
    assert snapshot.daily_facts[0].adj_factor == 2.0
    assert snapshot.security_facts[1].is_st is True


def test_capture_includes_bounded_reference_page_projections_from_same_database_snapshot(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with duckdb.connect(str(database)) as connection:
        connection.execute("ALTER TABLE daily_bar ADD COLUMN open DOUBLE")
        connection.execute("ALTER TABLE daily_bar ADD COLUMN high DOUBLE")
        connection.execute("ALTER TABLE daily_bar ADD COLUMN low DOUBLE")
        connection.execute("ALTER TABLE daily_bar ADD COLUMN pre_close DOUBLE")
        connection.execute("ALTER TABLE daily_bar ADD COLUMN pct_chg DOUBLE")
        connection.execute("ALTER TABLE daily_bar ADD COLUMN vol DOUBLE")
        connection.execute("ALTER TABLE daily_bar ADD COLUMN amount DOUBLE")
        connection.execute(
            """
            UPDATE daily_bar
            SET open = close - 0.1, high = close + 0.2, low = close - 0.2,
                pre_close = close - 0.5, pct_chg = 2.5, vol = 1000, amount = 2000
            """
        )
        connection.execute(
            "CREATE TABLE stock_basic(ts_code VARCHAR, name VARCHAR, industry VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO stock_basic VALUES (?, ?, ?)",
            [
                ("300001.SZ", "成长样本", "软件服务"),
                ("600000.SH", "ST 风险样本", "银行"),
            ],
        )
        connection.execute(
            """
            CREATE TABLE daily_state(
                ts_code VARCHAR, trade_date DATE, is_st BOOLEAN, is_bj BOOLEAN,
                board_type VARCHAR, is_limit_up BOOLEAN, is_limit_down BOOLEAN,
                is_first_limit_up BOOLEAN, is_yiziban BOOLEAN,
                consecutive_limit_ups INTEGER, body_upper DOUBLE, body_lower DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO daily_state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "300001.SZ",
                    PRIOR_DATE,
                    False,
                    False,
                    "gem",
                    False,
                    False,
                    False,
                    False,
                    0,
                    20.1,
                    19.9,
                ),
                (
                    "600000.SH",
                    PRIOR_DATE,
                    True,
                    False,
                    "main",
                    False,
                    False,
                    False,
                    False,
                    0,
                    10.1,
                    9.9,
                ),
            ],
        )
        connection.execute(
            """
            CREATE TABLE daily_indicator(
                ts_code VARCHAR, trade_date DATE, ma5 DOUBLE, ma10 DOUBLE,
                ma20 DOUBLE, ma60 DOUBLE, rsi6 DOUBLE, rsi14 DOUBLE,
                macd DOUBLE, macd_signal DOUBLE, macd_hist DOUBLE,
                kdj_k DOUBLE, kdj_d DOUBLE, kdj_j DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO daily_indicator VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "300001.SZ",
                    PRIOR_DATE,
                    19.0,
                    18.0,
                    17.0,
                    16.0,
                    60.0,
                    55.0,
                    1.0,
                    0.8,
                    0.2,
                    70.0,
                    65.0,
                    80.0,
                ),
                (
                    "600000.SH",
                    PRIOR_DATE,
                    9.5,
                    9.0,
                    8.5,
                    8.0,
                    50.0,
                    48.0,
                    0.5,
                    0.4,
                    0.1,
                    60.0,
                    55.0,
                    70.0,
                ),
            ],
        )
        connection.execute(
            """
            CREATE TABLE daily_basic(
                ts_code VARCHAR, trade_date DATE, circ_mv DOUBLE,
                total_mv DOUBLE, turnover_rate DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO daily_basic VALUES (?, ?, ?, ?, ?)",
            [
                ("300001.SZ", PRIOR_DATE, 1000.0, 1200.0, 3.0),
                ("600000.SH", PRIOR_DATE, 2000.0, 2400.0, 2.0),
            ],
        )
        connection.execute("CREATE TABLE dc_board(ts_code VARCHAR, name VARCHAR, idx_type VARCHAR)")
        connection.execute("INSERT INTO dc_board VALUES ('BK0001.DC', '样本行业', '行业板块')")
        connection.execute("CREATE TABLE dc_board_member(board_code VARCHAR, con_code VARCHAR)")
        connection.execute("INSERT INTO dc_board_member VALUES ('BK0001.DC', '300001.SZ')")
        connection.execute(
            """
            CREATE TABLE risk_blacklist(
                ts_code VARCHAR, list_label VARCHAR,
                expires_at DATE, imported_at DATE
            )
            """
        )
        connection.execute(
            "INSERT INTO risk_blacklist VALUES ('300001.SZ', '风控样本', ?, ?)",
            [TARGET_DATE, PRIOR_DATE],
        )
    database.chmod(0o600)

    snapshot = capture_reference_slow_source_snapshot(
        database_path=database,
        adapter=_Adapter(),
        calendar=_calendar(),
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
    )
    by_table = {projection.table_name: projection for projection in snapshot.projections}

    assert by_table["dc_board"].rows == (
        {"ts_code": "BK0001.DC", "name": "样本行业", "idx_type": "行业板块"},
    )
    assert by_table["dc_board_member"].rows == (
        {"board_code": "BK0001.DC", "con_code": "300001.SZ"},
    )
    assert any(row["list_label"] == "风控样本" for row in by_table["risk_blacklist"].rows)
    assert by_table["stock_basic"].rows[0]["industry"] == "软件服务"
    nl_row = by_table["nl_screen_universe"].rows[0]
    assert nl_row["MA5[0]"] == 19.0
    assert nl_row["RSI14[0]"] == 55.0
    assert nl_row["CIRC_MV[0]"] == 1000.0
    assert nl_row["IS_LIMIT_UP[0]"] is False


class _DelistingAdapter(_Adapter):
    def stock_basic(self, list_status: str = "L") -> pd.DataFrame:
        self.calls.append(f"stock_basic:{list_status}")
        if list_status == "L":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "300001.SZ",
                        "name": "成长样本",
                        "list_date": "20200102",
                        "delist_date": None,
                        "market": "创业板",
                    },
                    {
                        "ts_code": "600000.SH",
                        "name": "普通样本",
                        "list_date": "19991110",
                        "delist_date": None,
                        "market": "主板",
                    },
                ]
            )
        if list_status == "D":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600001.SH",
                        "name": "退市样本",
                        "list_date": "20000101",
                        "delist_date": TARGET_DATE.strftime("%Y%m%d"),
                        "market": "主板",
                    }
                ]
            )
        return pd.DataFrame(columns=["ts_code", "name", "list_date", "delist_date", "market"])


def test_target_universe_excludes_security_delisted_on_target_date_without_batch_failure(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with duckdb.connect(str(database)) as connection:
        connection.execute("INSERT INTO daily_bar VALUES ('600001.SH', ?, 8.0)", [PRIOR_DATE])
        connection.execute("INSERT INTO adj_factor VALUES ('600001.SH', ?, 1.0)", [PRIOR_DATE])
    adapter = _DelistingAdapter()

    snapshot = capture_reference_slow_source_snapshot(
        database_path=database,
        adapter=adapter,
        calendar=_calendar(),
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
    )

    assert tuple(fact.ts_code for fact in snapshot.security_facts) == (
        "300001.SZ",
        "600000.SH",
    )
    assert adapter.calls[:3] == ["stock_st", "stock_basic:L", "stock_basic:D"]


def test_live_capture_reuses_the_same_normalized_fact_assembly(tmp_path: Path) -> None:
    captured = capture_reference_slow_source_snapshot(
        database_path=_database(tmp_path),
        adapter=_Adapter(),
        calendar=_calendar(),
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
    )
    assembled = assemble_reference_slow_source_snapshot(
        calendar=_calendar(),
        observed_at=OBSERVED_AT,
        producer_commit=COMMIT,
        daily_facts=(
            ReferenceDailySourceFact(ts_code="300001.SZ", trade_date=PRIOR_DATE, close_raw=20.0),
            ReferenceDailySourceFact(ts_code="600000.SH", trade_date=PRIOR_DATE, close_raw=10.0),
        ),
        adjustment_facts=(
            ReferenceAdjustmentSourceFact(
                ts_code="300001.SZ", trade_date=PRIOR_DATE, adj_factor=1.0
            ),
            ReferenceAdjustmentSourceFact(
                ts_code="300001.SZ", trade_date=TARGET_DATE, adj_factor=2.0
            ),
            ReferenceAdjustmentSourceFact(
                ts_code="600000.SH", trade_date=PRIOR_DATE, adj_factor=1.0
            ),
            ReferenceAdjustmentSourceFact(
                ts_code="600000.SH", trade_date=TARGET_DATE, adj_factor=1.0
            ),
        ),
        security_facts=(
            ReferenceSecuritySourceFact(
                ts_code="300001.SZ",
                name="成长样本",
                is_st=False,
                list_date=date(2020, 1, 2),
                market="创业板",
            ),
            ReferenceSecuritySourceFact(
                ts_code="600000.SH",
                name="普通样本",
                is_st=True,
                list_date=date(1999, 11, 10),
                market="主板",
            ),
        ),
        suspension_facts=(
            ReferenceSuspensionSourceFact(
                ts_code="600000.SH",
                trade_date=TARGET_DATE,
                suspend_type="S",
                session_scope="full_day",
            ),
        ),
    )

    assert captured == assembled


def test_rejects_missing_target_adjustment_evidence(tmp_path: Path) -> None:
    with pytest.raises(ReferenceSlowSourceError, match="adj_factor"):
        capture_reference_slow_source_snapshot(
            database_path=_database(tmp_path),
            adapter=_Adapter(include_all_adjustments=False),
            calendar=_calendar(),
            target_trade_date=TARGET_DATE,
            captured_at=OBSERVED_AT,
            completion_clock=lambda: OBSERVED_AT,
            producer_commit=COMMIT,
        )


def test_rejects_source_capture_after_strategy_decision_cutoff(tmp_path: Path) -> None:
    with pytest.raises(ReferenceSlowSourceError, match="09:25"):
        capture_reference_slow_source_snapshot(
            database_path=_database(tmp_path),
            adapter=_Adapter(),
            calendar=_calendar(),
            target_trade_date=TARGET_DATE,
            captured_at=datetime(2026, 7, 31, 1, 26, tzinfo=UTC),
            completion_clock=lambda: datetime(2026, 7, 31, 1, 26, tzinfo=UTC),
            producer_commit=COMMIT,
        )


def test_capture_uses_last_response_completion_as_evidence_availability(
    tmp_path: Path,
) -> None:
    completed_at = datetime(2026, 7, 31, 1, 24, 30, tzinfo=UTC)

    snapshot = capture_reference_slow_source_snapshot(
        database_path=_database(tmp_path),
        adapter=_Adapter(),
        calendar=_calendar(),
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: completed_at,
        producer_commit=COMMIT,
    )

    assert snapshot.captured_at == completed_at


def test_capture_accepts_an_independently_versioned_exact_calendar(tmp_path: Path) -> None:
    calendar = _calendar(producer_commit="b" * 40)

    snapshot = capture_reference_slow_source_snapshot(
        database_path=_database(tmp_path),
        adapter=_Adapter(),
        calendar=calendar,
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
    )

    assert snapshot.producer_commit == COMMIT
    assert snapshot.source_snapshot_ids["calendar"] == calendar.content_sha256


def test_capture_rejects_responses_completed_after_decision_cutoff(tmp_path: Path) -> None:
    with pytest.raises(ReferenceSlowSourceError, match="complete by 09:25"):
        capture_reference_slow_source_snapshot(
            database_path=_database(tmp_path),
            adapter=_Adapter(),
            calendar=_calendar(),
            target_trade_date=TARGET_DATE,
            captured_at=OBSERVED_AT,
            completion_clock=lambda: datetime(2026, 7, 31, 1, 25, 1, tzinfo=UTC),
            producer_commit=COMMIT,
        )


def test_full_market_status_rows_outside_tradable_daily_universe_are_ignored(
    tmp_path: Path,
) -> None:
    snapshot = capture_reference_slow_source_snapshot(
        database_path=_database(tmp_path),
        adapter=_Adapter(include_outside_universe_status=True),
        calendar=_calendar(),
        target_trade_date=TARGET_DATE,
        captured_at=OBSERVED_AT,
        completion_clock=lambda: OBSERVED_AT,
        producer_commit=COMMIT,
    )

    assert tuple(fact.ts_code for fact in snapshot.security_facts) == (
        "300001.SZ",
        "600000.SH",
    )
    assert snapshot.suspended_codes == ("600000.SH",)


def _runtime_manifests(
    tmp_path: Path,
) -> tuple[RuntimeServiceManifest, RuntimeServiceManifest]:
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar()
    calendar_path.write_text(calendar.model_dump_json())
    calendar_path.chmod(0o600)
    common = {
        "plane": RuntimeServicePlane.LIVE,
        "interval_seconds": 15,
        "stale_after_seconds": 600,
        "producer_commit": COMMIT,
    }
    spool_root = tmp_path / "reference-slow-spool"
    source = RuntimeServiceManifest(
        service_id="source.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        settings={
            "database_path": str(_database(tmp_path)),
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "quota_path": str(tmp_path / "quota" / "reference.sqlite3"),
            "quota_units_per_window": 500,
            "quota_cost_per_capture": 6,
            "limits": {
                "snapshot_max_bytes": 8 * 1024**3,
                "snapshot_min_free_bytes": 0,
                "snapshot_copy_timeout_seconds": 45.0,
                "query_chunk_rows": 512,
                "max_response_rows": 10_000,
                "max_response_bytes": 8 * 1024**2,
            },
            "producer_version": "reference-slow-v1",
        },
        **common,
    )
    publisher = RuntimeServiceManifest(
        service_id="publisher.reference-slow",
        service_kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        settings={
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "spool_root": str(spool_root),
            "registry_path": str(tmp_path / "reference" / "reference.sqlite3"),
            "cursor_root": str(tmp_path / "publisher-state" / "cursors"),
        },
        **common,
    )
    return source, publisher


def test_runtime_reference_source_and_publisher_are_independent_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _Adapter()
    source_manifest, publisher_manifest = _runtime_manifests(tmp_path)
    observed_at = datetime(2026, 7, 31, 1, 20, tzinfo=UTC)
    monkeypatch.setattr(
        "rquant.reference_slow_runtime._utc_now",
        lambda: observed_at.replace(minute=24, second=40),
    )
    source_step = reference_slow_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: observed_at,
        runtime_capabilities=_runtime_capabilities(),
    )(source_manifest)
    publisher_step = reference_slow_publisher_builder(
        clock=lambda: observed_at.replace(minute=24, second=40),
        runtime_capabilities=_runtime_capabilities(),
    )(publisher_manifest)

    captured = source_step()
    repeated_capture = source_step()
    published = publisher_step()
    repeated_publish = publisher_step()

    assert adapter.calls == [
        "stock_st",
        "stock_basic:L",
        "stock_basic:D",
        "stock_basic:P",
        "adj_factor",
        "suspend_d",
    ]
    assert captured.processed_count == 1
    assert repeated_capture.processed_count == 0
    assert published.processed_count == 1
    assert repeated_publish.processed_count == 0
    assert captured.source_generations["market_calendar"] == _calendar().content_sha256
    assert captured.source_generations["reference_slow"]
    assert published.source_generations["reference_registry"]
    assert (tmp_path / "reference" / "reference.sqlite3").is_file()
    assert (tmp_path / "quota" / "reference.sqlite3").is_file()


def test_runtime_reference_source_is_quiet_after_decision_cutoff(tmp_path: Path) -> None:
    adapter = _Adapter()
    source_manifest, _publisher_manifest = _runtime_manifests(tmp_path)
    step = reference_slow_source_builder(
        adapter_factory=lambda: adapter,
        clock=lambda: datetime(2026, 7, 31, 1, 26, tzinfo=UTC),
        runtime_capabilities=_runtime_capabilities(),
    )(source_manifest)

    result = step()

    assert result.processed_count == 0
    assert result.source_generations == {"market_calendar": _calendar().content_sha256}
    assert adapter.calls == []


def test_runtime_reference_builders_reject_wrong_kind_or_plane(tmp_path: Path) -> None:
    source_manifest, publisher_manifest = _runtime_manifests(tmp_path)
    source_builder = reference_slow_source_builder(
        adapter_factory=_Adapter,
        clock=lambda: OBSERVED_AT,
        runtime_capabilities=_runtime_capabilities(),
    )
    publisher_builder = reference_slow_publisher_builder(
        clock=lambda: OBSERVED_AT,
        runtime_capabilities=_runtime_capabilities(),
    )

    with pytest.raises(ValueError, match="reference_slow_source"):
        source_builder(
            source_manifest.model_copy(update={"service_kind": RuntimeServiceKind.NOTIFIER})
        )
    with pytest.raises(ValueError, match="live plane"):
        publisher_builder(
            publisher_manifest.model_copy(update={"plane": RuntimeServicePlane.RESEARCH})
        )


def test_builtin_registry_registers_reference_source_and_publisher(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _Adapter()
    source_manifest, publisher_manifest = _runtime_manifests(tmp_path)
    observed_at = [datetime(2026, 7, 31, 1, 24, 20, tzinfo=UTC)]
    monkeypatch.setattr("rquant.reference_slow_runtime._utc_now", lambda: observed_at[0])
    registry = build_builtin_registry(
        reference_adapter_factory=lambda: adapter,
        clock=lambda: observed_at[0],
        runtime_capabilities=_runtime_capabilities(),
    )

    captured = registry.build(source_manifest)()
    observed_at[0] += timedelta(seconds=20)
    published = registry.build(publisher_manifest)()

    assert captured.processed_count == 1
    assert published.processed_count == 1
    assert adapter.calls == [
        "stock_st",
        "stock_basic:L",
        "stock_basic:D",
        "stock_basic:P",
        "adj_factor",
        "suspend_d",
    ]
