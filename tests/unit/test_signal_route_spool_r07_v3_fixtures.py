"""Read-only Phase-A R07 fixture verifier contracts."""

from __future__ import annotations

import pytest

import rquant.signal_route_spool as spool
from rquant.signal_bus import SignalBusSourceDescriptor
from tests.unit.test_signal_route_spool_r07_v3 import _current_record_bytes


def test_r07_exposes_only_a_pure_fixture_verifier() -> None:
    assert callable(spool.verify_current_signal_route_spool_fixture)


def test_legacy_readonly_spool_rejects_a_v3_record_file(tmp_path) -> None:
    root = tmp_path / "spool"
    records = root / "records"
    records.mkdir(parents=True)
    raw = _current_record_bytes(sequence=1, previous_record_hash=None)
    decoded = spool.decode_current_signal_route_spool_record(raw)
    source = SignalBusSourceDescriptor(generation_id="f" * 64, high_watermark=1)
    (root / "source.json").write_bytes(
        spool._canonical_bytes(source.model_copy(update={"high_watermark": 0}))
    )
    (root / "current.json").write_bytes(
        spool._canonical_bytes(
            spool.SignalRouteSpoolPointer(source=source, last_record_hash=decoded.record_hash)
        )
    )
    (records / "00000000000000000001.json").write_bytes(raw)

    with pytest.raises(spool.SignalRouteSpoolIntegrityError, match="invalid: 1"):
        spool.ReadonlySignalRouteSpool(root).source_descriptor()
