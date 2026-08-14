from __future__ import annotations

import importlib
import os
import sqlite3
from pathlib import Path

import pytest


def _image_module() -> object:
    try:
        return importlib.import_module("rquant._paper_sqlite_image")
    except ModuleNotFoundError as exc:
        pytest.fail(f"private stable SQLite image helper is missing: {exc}")


def _captured_image(tmp_path: Path) -> tuple[object, object]:
    module = _image_module()
    path = tmp_path / "image.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE image_probe(value TEXT NOT NULL)")
        connection.execute("INSERT INTO image_probe VALUES ('bound')")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        image = module._capture_stable_sqlite_image_for_test(descriptor, max_bytes=64 * 1024)
    finally:
        os.close(descriptor)
    return module, image


def test_cpython_memory_deserialize_query_only(tmp_path: Path) -> None:
    module, image = _captured_image(tmp_path)
    connection = module._open_memory_sqlite_image(image)
    try:
        assert connection.execute("SELECT value FROM image_probe").fetchone()[0] == "bound"
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO image_probe VALUES ('forbidden')")
    finally:
        connection.close()


def test_memory_adapter_unavailable_is_closed(tmp_path: Path) -> None:
    module, image = _captured_image(tmp_path)

    class MissingDeserializeConnection:
        closed = False

        def close(self) -> None:
            self.closed = True

    connection = MissingDeserializeConnection()

    class MissingDeserializeAdapter:
        def open_memory(self) -> MissingDeserializeConnection:
            return connection

    with pytest.raises(module._StableSQLiteImageError):
        module._open_memory_sqlite_image(image, adapter=MissingDeserializeAdapter())
    assert connection.closed


def test_memory_adapter_deserialize_raise_is_closed(tmp_path: Path) -> None:
    module, image = _captured_image(tmp_path)

    class RaisingDeserializeConnection:
        closed = False

        def deserialize(self, _data: bytes) -> None:
            raise MemoryError("injected deserialize failure")

        def close(self) -> None:
            self.closed = True

    connection = RaisingDeserializeConnection()

    class RaisingDeserializeAdapter:
        def open_memory(self) -> RaisingDeserializeConnection:
            return connection

    with pytest.raises(module._StableSQLiteImageError):
        module._open_memory_sqlite_image(image, adapter=RaisingDeserializeAdapter())
    assert connection.closed


def test_capture_test_bound_enforces_exact_64_kib_edges(tmp_path: Path) -> None:
    module = _image_module()
    boundary = tmp_path / "boundary.sqlite3"
    boundary.write_bytes(b"x" * (64 * 1024))
    descriptor = os.open(boundary, os.O_RDONLY)
    try:
        image = module._capture_stable_sqlite_image_for_test(
            descriptor,
            max_bytes=64 * 1024,
        )
    finally:
        os.close(descriptor)
    assert len(image.data) == 64 * 1024
    assert image.binding.identity.size == 64 * 1024
    assert module._SERIALIZED_SQLITE_IMAGE_OPERATIONAL_BUDGET_BYTES == 96 * 1024 * 1024

    oversized = tmp_path / "oversized.sqlite3"
    oversized.touch()
    os.truncate(oversized, 64 * 1024 + 1)
    descriptor = os.open(oversized, os.O_RDONLY)
    try:
        with pytest.raises(module._StableSQLiteImageError, match="capacity"):
            module._capture_stable_sqlite_image_for_test(descriptor, max_bytes=64 * 1024)
    finally:
        os.close(descriptor)
    with pytest.raises(ValueError, match="no larger than production"):
        module._capture_stable_sqlite_image_for_test(0, max_bytes=16 * 1024 * 1024 + 1)
