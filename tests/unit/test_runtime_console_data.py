from __future__ import annotations

import os
import re
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.dashboard.runtime_console_data import (
    ConsoleFreshness,
    ConsoleLimits,
    ConsoleLoadState,
    load_runtime_console,
    query_acquired_serving_frame,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import (
    ServingIntegrityError,
    ServingPublisher,
    ServingReader,
    ServingTableSpec,
)

_NOW = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
_GENERATION = "a" * 64
_COMMIT = "b" * 40


class _FakeResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(self, rows: dict[str, list[tuple[object, ...]]]) -> None:
        self.rows = rows
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.fail_table: str | None = None

    def execute(self, sql: str, parameters: tuple[object, ...]) -> _FakeResult:
        self.queries.append((sql, parameters))
        match = re.search(r'FROM "([a-z_]+)"', sql)
        assert match is not None
        table = match.group(1)
        if table == self.fail_table:
            raise RuntimeError("injected serving query failure")
        return _FakeResult(self.rows.get(table, []))


class _FakeReader:
    connection = _FakeConnection({})
    manifest = SimpleNamespace(
        generation_id=_GENERATION,
        built_at=_NOW - timedelta(minutes=2),
        producer_commit=_COMMIT,
        watermarks=(SimpleNamespace(status=FreshnessStatus.FRESH),),
    )

    def __init__(self, root: object) -> None:
        self.root = root

    def current_manifest(self) -> object:
        return self.manifest

    def open_current_readonly(self) -> nullcontext[_FakeConnection]:
        return nullcontext(self.connection)

    def acquire_generation(self) -> nullcontext[SimpleNamespace]:
        return nullcontext(SimpleNamespace(manifest=self.manifest, connection=self.connection))


@pytest.fixture(autouse=True)
def _reset_reader() -> None:
    _FakeReader.connection = _FakeConnection(
        {
            "runtime_services": [
                (
                    "feature-live",
                    "live",
                    "running",
                    False,
                    _NOW - timedelta(seconds=5),
                    _NOW - timedelta(seconds=5),
                    20,
                    19,
                    1,
                    0,
                    None,
                )
            ],
            "signals": [
                (
                    20,
                    "sig-20",
                    "n-shape",
                    "v1",
                    "600000.SH",
                    "BUY",
                    _NOW - timedelta(minutes=1),
                    _NOW + timedelta(minutes=4),
                    '["volume_progress"]',
                )
            ],
            "deliveries": [
                (
                    "outbox-20",
                    "sig-20",
                    "admin",
                    "pushdeer",
                    "delivered",
                    1,
                    _NOW - timedelta(seconds=30),
                    None,
                )
            ],
            "paper_accounts": [
                ("paper", _NOW - timedelta(seconds=10), 100000, 99000, 1000, 100500, 500, 0)
            ],
            "paper_holdings": [
                (
                    "paper",
                    "600000.SH",
                    100,
                    0,
                    100,
                    10,
                    10.5,
                    1050,
                    50,
                    _NOW - timedelta(seconds=10),
                )
            ],
            "lab_jobs": [
                (
                    "job-1",
                    "n-shape",
                    "backtest",
                    "research",
                    "running",
                    0.5,
                    "replay",
                    5,
                    10,
                    "estimating",
                    _NOW + timedelta(minutes=10),
                    _NOW + timedelta(minutes=12),
                    _NOW + timedelta(minutes=15),
                    _NOW - timedelta(seconds=2),
                )
            ],
            "promotions": [
                (
                    "promotion-1",
                    "paper_candidate",
                    True,
                    "[]",
                    "[]",
                    _NOW - timedelta(minutes=3),
                )
            ],
        }
    )
    _FakeReader.manifest = SimpleNamespace(
        generation_id=_GENERATION,
        built_at=_NOW - timedelta(minutes=2),
        producer_commit=_COMMIT,
        watermarks=(SimpleNamespace(status=FreshnessStatus.FRESH),),
    )


def test_loads_a_verified_generation_into_typed_console_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", _FakeReader)

    snapshot = load_runtime_console("/serving", now=_NOW)

    assert snapshot.state is ConsoleLoadState.READY
    assert snapshot.freshness is ConsoleFreshness.FRESH
    assert snapshot.generation_id == _GENERATION
    assert snapshot.generated_at == _NOW - timedelta(minutes=2)
    assert snapshot.age_seconds == 120
    assert snapshot.services[0].service_id == "feature-live"
    assert snapshot.signals[0].candidate_id == "600000.SH"
    assert snapshot.deliveries[0].status == "delivered"
    assert snapshot.paper_accounts[0].nav == 100500
    assert snapshot.paper_holdings[0].unrealized_pnl == 50
    assert snapshot.lab_jobs[0].eta_finish_center == _NOW + timedelta(minutes=12)
    assert snapshot.promotions[0].approved is True


def test_queries_are_fixed_column_whitelists_with_bounded_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", _FakeReader)
    limits = ConsoleLimits(
        services=7,
        signals=11,
        deliveries=13,
        paper_accounts=17,
        paper_holdings=19,
        lab_jobs=23,
        promotions=29,
    )

    load_runtime_console("/serving", now=_NOW, limits=limits)

    queries = _FakeReader.connection.queries
    assert len(queries) == 7
    assert {re.search(r'FROM "([a-z_]+)"', sql).group(1) for sql, _ in queries} == {
        "runtime_services",
        "signals",
        "deliveries",
        "paper_accounts",
        "paper_holdings",
        "lab_jobs",
        "promotions",
    }
    assert all("SELECT *" not in sql.upper() for sql, _ in queries)
    assert all(" LIMIT ?" in sql for sql, _ in queries)
    assert {parameters[0] for _, parameters in queries} == {7, 11, 13, 17, 19, 23, 29}


def test_limits_are_positive_and_capped() -> None:
    with pytest.raises(ValidationError):
        ConsoleLimits(signals=0)
    with pytest.raises(ValidationError):
        ConsoleLimits(signals=501)


def test_missing_serving_returns_degraded_without_creating_or_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingReader:
        def __init__(self, root: object) -> None:
            raise ServingIntegrityError("serving root is missing")

    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", MissingReader)

    snapshot = load_runtime_console("/missing", now=_NOW)

    assert snapshot.state is ConsoleLoadState.DEGRADED
    assert snapshot.freshness is ConsoleFreshness.UNAVAILABLE
    assert "serving root is missing" in snapshot.detail
    assert snapshot.generation_id is None
    assert snapshot.services == ()


def test_query_failure_keeps_generation_identity_but_degrades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", _FakeReader)
    _FakeReader.connection.fail_table = "deliveries"

    snapshot = load_runtime_console("/serving", now=_NOW)

    assert snapshot.state is ConsoleLoadState.DEGRADED
    assert snapshot.freshness is ConsoleFreshness.DEGRADED
    assert snapshot.generation_id == _GENERATION
    assert snapshot.generated_at == _NOW - timedelta(minutes=2)
    assert "query failure" in snapshot.detail
    assert snapshot.signals == ()


def test_loader_uses_one_atomic_generation_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    class AtomicLease:
        manifest = _FakeReader.manifest
        connection = _FakeReader.connection

        def __enter__(self) -> AtomicLease:
            return self

        def __exit__(self, *_error: object) -> None:
            return None

    class AtomicOnlyReader:
        def __init__(self, root: object) -> None:
            self.root = root

        def current_manifest(self) -> object:
            raise AssertionError("manifest must come from the acquired generation")

        def open_current_readonly(self) -> object:
            raise AssertionError("connection must come from the acquired generation")

        def acquire_generation(self) -> AtomicLease:
            return AtomicLease()

    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", AtomicOnlyReader)

    snapshot = load_runtime_console("/serving", now=_NOW)

    assert snapshot.state is ConsoleLoadState.READY
    assert snapshot.generation_id == _GENERATION


def test_old_or_upstream_stale_generation_is_reported_as_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", _FakeReader)
    _FakeReader.manifest = SimpleNamespace(
        generation_id=_GENERATION,
        built_at=_NOW - timedelta(minutes=30),
        producer_commit=_COMMIT,
        watermarks=(SimpleNamespace(status=FreshnessStatus.STALE),),
    )

    snapshot = load_runtime_console(
        "/serving",
        now=_NOW,
        stale_after=timedelta(minutes=10),
    )

    assert snapshot.state is ConsoleLoadState.READY
    assert snapshot.freshness is ConsoleFreshness.STALE
    assert snapshot.age_seconds == 1800


def test_future_generation_is_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("rquant.dashboard.runtime_console_data.ServingReader", _FakeReader)
    _FakeReader.manifest = SimpleNamespace(
        generation_id=_GENERATION,
        built_at=_NOW + timedelta(seconds=1),
        producer_commit=_COMMIT,
        watermarks=(SimpleNamespace(status=FreshnessStatus.FRESH),),
    )

    snapshot = load_runtime_console("/serving", now=_NOW)

    assert snapshot.state is ConsoleLoadState.DEGRADED
    assert snapshot.freshness is ConsoleFreshness.DEGRADED
    assert "future" in snapshot.detail


def test_real_verified_reader_queries_without_mutating_serving_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    tables = {
        "runtime_services": pd.DataFrame(
            [
                {
                    "service_id": "feature-live",
                    "plane": "live",
                    "status": "running",
                    "stale": False,
                    "observed_at": _NOW - timedelta(seconds=5),
                    "heartbeat_at": _NOW - timedelta(seconds=5),
                    "input_sequence": 20,
                    "output_sequence": 19,
                    "backlog_count": 1,
                    "consecutive_failures": 0,
                    "last_error": None,
                }
            ]
        ),
        "signals": pd.DataFrame(
            columns=[
                "global_sequence",
                "signal_id",
                "strategy_id",
                "strategy_version",
                "candidate_id",
                "action",
                "available_at",
                "expires_at",
                "reason_codes_json",
            ]
        ),
        "deliveries": pd.DataFrame(
            columns=[
                "outbox_id",
                "signal_id",
                "recipient_id",
                "channel",
                "status",
                "attempt_count",
                "updated_at",
                "last_error",
            ]
        ),
        "paper_accounts": pd.DataFrame(
            columns=[
                "account_id",
                "as_of_time",
                "cash",
                "available_cash",
                "frozen_cash",
                "nav",
                "unrealized_pnl",
                "realized_pnl",
            ]
        ),
        "paper_holdings": pd.DataFrame(
            columns=[
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
            ]
        ),
        "lab_jobs": pd.DataFrame(
            columns=[
                "job_id",
                "strategy_name",
                "job_type",
                "resource_class",
                "status",
                "progress_fraction",
                "phase",
                "terminal_shards",
                "total_shards",
                "eta_status",
                "eta_finish_low",
                "eta_finish_center",
                "eta_finish_high",
                "updated_at",
            ]
        ),
        "promotions": pd.DataFrame(
            columns=[
                "decision_id",
                "stage",
                "approved",
                "experiment_ids_json",
                "gate_failures_json",
                "decided_at",
            ]
        ),
    }
    publisher = ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={
            table: ServingTableSpec(sort_keys=(frame.columns[0],))
            for table, frame in tables.items()
        },
    )
    manifest = publisher.publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="runtime-console",
                generation_id="source-1",
                event_time=_NOW - timedelta(minutes=2),
                published_at=_NOW - timedelta(minutes=2),
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"runtime-console": "source-1"},
        built_at=_NOW - timedelta(minutes=2),
    )
    tracked = (
        root,
        root / "current.json",
        root / "generations" / manifest.generation_id / "manifest.json",
        root / "generations" / manifest.generation_id / "serving.duckdb",
    )
    before = {path: os.stat(path, follow_symlinks=False) for path in tracked}

    snapshot = load_runtime_console(root, now=_NOW)

    after = {path: os.stat(path, follow_symlinks=False) for path in tracked}
    assert snapshot.state is ConsoleLoadState.READY
    assert snapshot.services[0].service_id == "feature-live"
    assert {
        path: (item.st_mode, item.st_mtime_ns, item.st_ctime_ns, item.st_size)
        for path, item in after.items()
    } == {
        path: (item.st_mode, item.st_mtime_ns, item.st_ctime_ns, item.st_size)
        for path, item in before.items()
    }


def test_acquired_query_keeps_one_generation_when_current_pointer_advances(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publisher = ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={"page_data": ServingTableSpec(sort_keys=("id",))},
    )

    def publish(value: str, generation: str, built_at: datetime) -> None:
        publisher.publish(
            {"page_data": pd.DataFrame({"id": [1], "value": [value]})},
            watermarks=(
                ServingDatasetWatermark(
                    dataset_id="page-data",
                    generation_id=generation,
                    event_time=built_at,
                    published_at=built_at,
                    sequence=1,
                    status=FreshnessStatus.FRESH,
                ),
            ),
            source_generations={"page-data": generation},
            built_at=built_at,
        )

    publish("old", "source-old", _NOW - timedelta(seconds=2))
    with ServingReader(root).acquire_generation() as acquired:
        first = query_acquired_serving_frame(
            acquired,
            "SELECT value FROM page_data",
            now=_NOW,
        )
        publish("new", "source-new", _NOW - timedelta(seconds=1))
        second = query_acquired_serving_frame(
            acquired,
            "SELECT value FROM page_data",
            now=_NOW,
        )

    assert first.generation_id == second.generation_id
    assert first.rows == (("old",),)
    assert second.rows == (("old",),)
