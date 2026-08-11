from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import duckdb
import pandas as pd
import pytest

from rquant.serving_contracts import (
    FreshnessStatus,
    ServingDatasetWatermark,
)
from rquant.serving_publisher import (
    ServingIntegrityError,
    ServingPublisher,
    ServingReader,
    ServingTableSpec,
    quote_serving_column_identifier,
    quote_serving_table_identifier,
)

_COMMIT = "a" * 40
_BUILT_AT = datetime(2026, 7, 31, 8, 30, tzinfo=UTC)


def _watermark(
    *,
    dataset_id: str = "signals",
    generation_id: str = "source-1",
    built_at: datetime = _BUILT_AT,
) -> ServingDatasetWatermark:
    return ServingDatasetWatermark(
        dataset_id=dataset_id,
        generation_id=generation_id,
        event_time=built_at - timedelta(minutes=1),
        published_at=built_at,
        sequence=1,
        status=FreshnessStatus.FRESH,
    )


def _publisher(root: Path) -> ServingPublisher:
    return ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={
            "signals": ServingTableSpec(sort_keys=("trade_date", "ts_code")),
        },
    )


def _signals(*, price_delta: float = 0.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["600002.SH", "600001.SH"],
            "price": [12.5 + price_delta, 11.0 + price_delta],
            "trade_date": ["2026-07-31", "2026-07-30"],
        }
    )


def _publish(
    publisher: ServingPublisher,
    *,
    frame: pd.DataFrame | None = None,
    built_at: datetime = _BUILT_AT,
    source_generation: str = "source-1",
    failure_hook: object | None = None,
):
    return publisher.publish(
        {"signals": _signals() if frame is None else frame},
        watermarks=(
            _watermark(
                generation_id=source_generation,
                built_at=built_at,
            ),
        ),
        source_generations={"signals": source_generation},
        built_at=built_at,
        failure_hook=failure_hook,
    )


def test_first_publish_builds_verified_private_readonly_generation(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")

    manifest = _publish(publisher)

    pointer = publisher.current_pointer()
    assert pointer.generation_id == manifest.generation_id
    assert pointer.previous_generation_id is None
    assert publisher.current_manifest() == manifest
    assert (tmp_path / "serving" / "generations" / manifest.generation_id).is_dir()
    assert (tmp_path / "serving" / "current.json").is_file()

    with publisher.open_current_readonly() as connection:
        columns = connection.execute("DESCRIBE signals").fetchdf()["column_name"].tolist()
        rows = connection.execute("SELECT * FROM signals").fetchall()
        assert columns == ["price", "trade_date", "ts_code"]
        assert rows == [
            (11.0, "2026-07-30", "600001.SH"),
            (12.5, "2026-07-31", "600002.SH"),
        ]
        with pytest.raises(duckdb.InvalidInputException, match="read-only"):
            connection.execute("CREATE TABLE forbidden(value INTEGER)")


def test_second_publish_switches_current_and_retains_previous_generation(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    first = _publish(publisher)

    second = _publish(
        publisher,
        frame=_signals(price_delta=1.0),
        built_at=_BUILT_AT + timedelta(minutes=5),
        source_generation="source-2",
    )

    pointer = publisher.current_pointer()
    assert second.generation_id != first.generation_id
    assert pointer.generation_id == second.generation_id
    assert pointer.previous_generation_id == first.generation_id
    assert (publisher.generations_root / first.generation_id / "serving.duckdb").is_file()
    with publisher.open_current_readonly() as connection:
        assert connection.execute("SELECT min(price) FROM signals").fetchone() == (12.0,)


def test_publish_canonicalizes_input_column_and_row_order(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    original = _signals()
    shuffled = original.iloc[::-1][["trade_date", "ts_code", "price"]].reset_index(drop=True)

    first = _publish(publisher, frame=original)
    second = _publish(publisher, frame=shuffled)

    assert second.generation_id == first.generation_id
    assert second.content_sha256 == first.content_sha256
    assert len([path for path in publisher.generations_root.iterdir() if path.is_dir()]) == 1


def test_publish_is_idempotent_for_an_existing_current_generation(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    first = _publish(publisher)
    current_bytes = publisher.current_path.read_bytes()

    retried = _publish(publisher)

    assert retried == first
    assert publisher.current_path.read_bytes() == current_bytes
    assert publisher.current_pointer().previous_generation_id is None


def test_failure_before_pointer_switch_leaves_old_current_readable(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    first = _publish(publisher)

    def fail(stage: str) -> None:
        if stage == "before_pointer_switch":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected crash"):
        _publish(
            publisher,
            frame=_signals(price_delta=2.0),
            built_at=_BUILT_AT + timedelta(minutes=5),
            source_generation="source-2",
            failure_hook=fail,
        )

    assert publisher.current_manifest() == first
    with publisher.open_current_readonly() as connection:
        assert connection.execute("SELECT min(price) FROM signals").fetchone() == (11.0,)


def test_failure_after_pointer_switch_cas_rolls_back_to_previous_generation(
    tmp_path: Path,
) -> None:
    publisher = _publisher(tmp_path / "serving")
    first = _publish(publisher)

    def fail(stage: str) -> None:
        if stage == "after_pointer_switch":
            raise RuntimeError("injected post-switch failure")

    with pytest.raises(RuntimeError, match="post-switch failure"):
        _publish(
            publisher,
            frame=_signals(price_delta=2.0),
            built_at=_BUILT_AT + timedelta(minutes=5),
            source_generation="source-2",
            failure_hook=fail,
        )

    assert publisher.current_manifest() == first
    assert not publisher.publication_intent_path.exists()
    with publisher.open_current_readonly() as connection:
        assert connection.execute("SELECT min(price) FROM signals").fetchone() == (11.0,)


def test_restart_recovers_crash_after_pointer_switch_before_receipt(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publisher = _publisher(root)
    first = _publish(publisher)

    class SimulatedProcessCrash(BaseException):
        pass

    def crash(stage: str) -> None:
        if stage == "after_pointer_switch":
            raise SimulatedProcessCrash

    with pytest.raises(SimulatedProcessCrash):
        _publish(
            publisher,
            frame=_signals(price_delta=3.0),
            built_at=_BUILT_AT + timedelta(minutes=5),
            source_generation="source-2",
            failure_hook=crash,
        )

    assert publisher.current_pointer().generation_id != first.generation_id
    restarted = _publisher(root)
    assert restarted.current_manifest() == first
    assert not restarted.publication_intent_path.exists()


def test_reader_acquires_manifest_and_connection_from_one_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publisher = _publisher(root)
    first = _publish(publisher)
    reader = ServingReader(root)

    with reader.acquire_generation() as acquired:
        second = _publish(
            publisher,
            frame=_signals(price_delta=4.0),
            built_at=_BUILT_AT + timedelta(minutes=5),
            source_generation="source-2",
        )
        observed = acquired.connection.execute("SELECT min(price) FROM signals").fetchone()

    assert acquired.closed is True
    assert acquired.manifest.generation_id == first.generation_id
    assert observed == (11.0,)
    assert publisher.current_manifest().generation_id == second.generation_id
    acquired.close()
    with pytest.raises(duckdb.Error, match="closed"):
        acquired.connection.execute("SELECT 1")


def test_concurrent_publishers_serialize_complete_generations(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    first_publisher = _publisher(root)
    _publish(first_publisher)

    def publish_delta(delta: int):
        return _publish(
            _publisher(root),
            frame=_signals(price_delta=float(delta)),
            built_at=_BUILT_AT + timedelta(minutes=delta),
            source_generation=f"source-{delta}",
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        manifests = tuple(executor.map(publish_delta, range(2, 6)))

    reader = ServingReader(root)
    with reader.acquire_generation() as acquired:
        observed = acquired.connection.execute("SELECT min(price) FROM signals").fetchone()

    by_generation = {manifest.generation_id: manifest for manifest in manifests}
    assert acquired.manifest.generation_id in by_generation
    assert observed in {(13.0,), (14.0,), (15.0,), (16.0,)}
    assert not first_publisher.publication_intent_path.exists()


def test_concurrent_publish_and_read_never_crosses_generation_boundary(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    _publish(_publisher(root))
    start = Event()

    def publish_generations() -> None:
        start.wait()
        for delta in range(2, 8):
            _publish(
                _publisher(root),
                frame=_signals(price_delta=float(delta)),
                built_at=_BUILT_AT + timedelta(minutes=delta),
                source_generation=f"source-{delta}",
            )

    def read_generations() -> tuple[tuple[str, float], ...]:
        start.wait()
        observations: list[tuple[str, float]] = []
        reader = ServingReader(root)
        for _ in range(30):
            with reader.acquire_generation() as acquired:
                source = acquired.manifest.source_generations["signals"]
                row = acquired.connection.execute("SELECT min(price) FROM signals").fetchone()
                assert row is not None
                observations.append((source, float(row[0])))
        return tuple(observations)

    with ThreadPoolExecutor(max_workers=5) as executor:
        writer = executor.submit(publish_generations)
        readers = tuple(executor.submit(read_generations) for _ in range(4))
        start.set()
        writer.result()
        observations = tuple(item for reader in readers for item in reader.result())

    expected_prices = {"source-1": 11.0}
    expected_prices.update({f"source-{delta}": 11.0 + delta for delta in range(2, 8)})
    assert observations
    assert all(expected_prices[source] == price for source, price in observations)


def test_failed_publisher_never_overwrites_an_advanced_current_pointer(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    _publish(publisher)
    first_pointer = publisher.current_pointer()
    advanced = _publish(
        publisher,
        frame=_signals(price_delta=9.0),
        built_at=_BUILT_AT + timedelta(minutes=9),
        source_generation="source-9",
    )
    advanced_pointer = publisher.current_pointer()
    publisher._atomic_write_current(first_pointer)

    def fail_after_external_advance(stage: str) -> None:
        if stage == "after_pointer_switch":
            publisher._atomic_write_current(advanced_pointer)
            raise RuntimeError("publisher lost current ownership")

    with pytest.raises(RuntimeError, match="lost current ownership"):
        _publish(
            publisher,
            frame=_signals(price_delta=2.0),
            built_at=_BUILT_AT + timedelta(minutes=2),
            source_generation="source-2",
            failure_hook=fail_after_external_advance,
        )

    assert publisher.current_manifest() == advanced
    recovery_records = tuple(publisher.recovery_root.glob("*.json"))
    assert len(recovery_records) == 1
    payload = json.loads(recovery_records[0].read_text(encoding="utf-8"))
    assert payload["status"] == "current_advanced"
    assert payload["observed_generation_id"] == advanced.generation_id


def test_open_current_detects_database_tamper(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    manifest = _publish(publisher)
    database = publisher.generations_root / manifest.generation_id / "serving.duckdb"
    database.chmod(0o600)
    with database.open("ab") as stream:
        stream.write(b"tamper")

    with pytest.raises(ServingIntegrityError, match="database content hash"):
        publisher.open_current_readonly()


def test_current_manifest_detects_manifest_tamper(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    manifest = _publish(publisher)
    manifest_path = publisher.generations_root / manifest.generation_id / "manifest.json"
    manifest_path.chmod(0o600)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["row_counts"]["signals"] = 999
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ServingIntegrityError, match="manifest"):
        publisher.current_manifest()


def test_current_pointer_tamper_is_detected_before_database_open(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    _publish(publisher)
    payload = json.loads(publisher.current_path.read_text(encoding="utf-8"))
    payload["manifest_sha256"] = "f" * 64
    publisher.current_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ServingIntegrityError, match="manifest hash"):
        publisher.open_current_readonly()


def test_metadata_reads_fail_closed_before_exceeding_byte_budget(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    _publish(publisher)
    publisher.current_path.chmod(0o600)
    publisher.current_path.write_bytes(b"{" + b"x" * (8 * 1024 * 1024) + b"}")

    with pytest.raises(ServingIntegrityError, match="byte budget"):
        publisher.current_pointer()


def test_missing_current_generation_is_detected(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")
    manifest = _publish(publisher)
    generation = publisher.generations_root / manifest.generation_id
    database = generation / "serving.duckdb"
    generation.chmod(0o700)
    database.unlink()

    with pytest.raises(ServingIntegrityError, match="database is missing"):
        publisher.open_current_readonly()


@pytest.mark.parametrize(
    "table_name",
    ["nested.table", "../signals", "signals/child", "signals-name", "9signals", ""],
)
def test_table_names_must_be_flat_safe_identifiers(tmp_path: Path, table_name: str) -> None:
    with pytest.raises(ValueError, match="table name"):
        ServingPublisher(
            tmp_path / "serving",
            producer_commit=_COMMIT,
            table_specs={table_name: ServingTableSpec(sort_keys=("ts_code",))},
        )


def test_publish_rejects_tables_without_exact_specs(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "serving")

    with pytest.raises(ValueError, match="table_specs"):
        publisher.publish(
            {"other": pd.DataFrame({"id": [1]})},
            watermarks=(_watermark(dataset_id="other"),),
            source_generations={"other": "source-1"},
            built_at=_BUILT_AT,
        )


def test_publish_rejects_missing_or_nonunique_sort_keys(tmp_path: Path) -> None:
    missing_key_publisher = ServingPublisher(
        tmp_path / "missing",
        producer_commit=_COMMIT,
        table_specs={"signals": ServingTableSpec(sort_keys=("unknown",))},
    )
    with pytest.raises(ValueError, match="sort key"):
        _publish(missing_key_publisher)

    duplicate_key_publisher = _publisher(tmp_path / "duplicate")
    duplicate = pd.concat([_signals().iloc[[0]], _signals().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="unique"):
        _publish(duplicate_key_publisher, frame=duplicate)


def test_serving_identifier_api_quotes_only_whitelisted_identifiers() -> None:
    assert quote_serving_table_identifier("nl_screen_universe") == '"nl_screen_universe"'
    assert quote_serving_column_identifier("CLOSE[0]") == '"CLOSE[0]"'

    with pytest.raises(ValueError, match="table identifier"):
        quote_serving_table_identifier('signals"; DROP TABLE signals; --')
    with pytest.raises(ValueError, match="column identifier"):
        quote_serving_column_identifier('price"; DROP TABLE signals; --')


def test_publish_rejects_unsafe_dynamic_columns_before_opening_duckdb(tmp_path: Path) -> None:
    publisher = ServingPublisher(
        tmp_path / "serving",
        producer_commit=_COMMIT,
        table_specs={"signals": ServingTableSpec(sort_keys=("trade_date", "ts_code"))},
    )
    frame = _signals().rename(columns={"price": 'price"; DROP TABLE signals; --'})

    with pytest.raises(ValueError, match="column identifier"):
        _publish(publisher, frame=frame)


def test_publish_quotes_bracketed_dynamic_columns(tmp_path: Path) -> None:
    publisher = ServingPublisher(
        tmp_path / "serving",
        producer_commit=_COMMIT,
        table_specs={
            "nl_screen_universe": ServingTableSpec(
                sort_keys=("trade_date", "ts_code"),
            )
        },
    )
    frame = pd.DataFrame(
        {
            "trade_date": ["2026-07-31"],
            "ts_code": ["600000.SH"],
            "CLOSE[0]": [10.6],
        }
    )

    publisher.publish(
        {"nl_screen_universe": frame},
        watermarks=(_watermark(dataset_id="signals"),),
        source_generations={"signals": "source-1"},
        built_at=_BUILT_AT,
    )

    with publisher.open_current_readonly() as connection:
        row = connection.execute('SELECT "CLOSE[0]" FROM "nl_screen_universe"').fetchone()
    assert row == (10.6,)


def test_empty_typed_table_preserves_declared_duckdb_schema(tmp_path: Path) -> None:
    publisher = ServingPublisher(
        tmp_path / "serving",
        producer_commit=_COMMIT,
        table_specs={
            "research_gate_metadata": ServingTableSpec(
                sort_keys=("range_start",),
                column_types=(
                    ("range_start", "DATE"),
                    ("as_of_time", "TIMESTAMPTZ"),
                ),
            )
        },
    )
    frame = pd.DataFrame(columns=("range_start", "as_of_time"))

    publisher.publish(
        {"research_gate_metadata": frame},
        watermarks=(_watermark(dataset_id="lab_jobs"),),
        source_generations={"lab_jobs": "source-1"},
        built_at=_BUILT_AT,
    )

    with publisher.open_current_readonly() as connection:
        types = {
            column: kind
            for column, kind, *_rest in connection.execute(
                "DESCRIBE research_gate_metadata"
            ).fetchall()
        }
        rows = connection.execute(
            """
            SELECT * FROM research_gate_metadata
            WHERE range_start <= DATE '2026-07-31'
              AND as_of_time <= TIMESTAMPTZ '2026-07-31 08:00:00+00'
            """
        ).fetchall()

    assert types == {"as_of_time": "TIMESTAMP WITH TIME ZONE", "range_start": "DATE"}
    assert rows == []


def test_publish_generation_budgets_fail_before_pointer_switch(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publisher = _publisher(root)
    first = _publish(publisher)

    row_limited = ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={"signals": ServingTableSpec(sort_keys=("trade_date", "ts_code"))},
        max_generation_rows=1,
    )
    with pytest.raises(ServingIntegrityError, match="row budget"):
        _publish(
            row_limited,
            frame=_signals(price_delta=2.0),
            built_at=_BUILT_AT + timedelta(minutes=2),
            source_generation="source-2",
        )
    assert publisher.current_manifest() == first

    byte_limited = ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={"signals": ServingTableSpec(sort_keys=("trade_date", "ts_code"))},
        max_database_bytes=128,
    )
    with pytest.raises(ServingIntegrityError, match="database byte budget"):
        _publish(
            byte_limited,
            frame=_signals(price_delta=3.0),
            built_at=_BUILT_AT + timedelta(minutes=3),
            source_generation="source-3",
        )
    assert publisher.current_manifest() == first
