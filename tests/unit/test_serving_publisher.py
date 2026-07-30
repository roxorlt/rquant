from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

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
    ServingTableSpec,
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
