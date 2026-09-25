"""`scripts/route_a_replay_output_diff.py`: commit-bound identities are not differences."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "route_a_replay_output_diff.py"


def _script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("route_a_replay_output_diff", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _sandbox(root: Path, *, commit: str, close: float = 10.0) -> Path:
    runtime = root / "host" / "data" / "runtime"
    minute = runtime / "live" / "market-minute" / "batches" / "market_minute"
    features = runtime / "live" / "features" / "batches"
    for directory in (minute, features):
        directory.mkdir(parents=True)
    (minute / f"{0:020d}.json").write_text(
        json.dumps({"sequence": 0, "batch_id": "raw-0", "producer_commit": commit})
    )
    (minute / f"{0:020d}.payload").write_bytes(b"raw")
    (features / f"{0:020d}.json").write_text(
        json.dumps({"sequence": 0, "batch_id": f"feature-{commit}", "producer_commit": commit})
    )
    (features / f"{0:020d}.payload").write_text(json.dumps({"rows": [{"close": close}]}))
    strategy = runtime / "live" / "strategies" / "svc-a"
    strategy.mkdir(parents=True)
    connection = sqlite3.connect(strategy / "runner.sqlite3")
    connection.executescript(
        """
        CREATE TABLE runner_metadata (strategy_spec_json TEXT);
        CREATE TABLE runner_signal (
            sequence INTEGER, feature_sequence INTEGER, candidate_id TEXT, action TEXT,
            event_time TEXT, available_at TEXT, expires_at TEXT, payload_json TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO runner_metadata VALUES (?)", (json.dumps({"strategy_id": "auction_gap"}),)
    )
    connection.execute(
        "INSERT INTO runner_signal VALUES (1, 0, '600000.SH', 'watch', 't', 't', 't', ?)",
        (
            json.dumps(
                {
                    "reason_codes": ["gap"],
                    "evidence": {"gap": 1.5, "runner_transition": {"feature_batch_id": commit}},
                }
            ),
        ),
    )
    connection.commit()
    connection.close()
    authority = runtime / "authorities" / "paper-execution" / "generations"
    authority.mkdir(parents=True)
    record = {
        "ts_code": "600000.SH",
        "available_at": "t",
        "expires_at": "u",
        "producer_commit": commit,
        "content_hash": commit,
        "source_snapshot_ids": {"market_minute": commit},
        "instrument_context": {"ts_code": "600000.SH", "classification_provenance": commit},
    }
    (authority / f"{commit}.json").write_text(
        json.dumps({"sequence": 0, "content_hash": commit, "records": [record]})
    )
    (authority.parent / "current.json").write_text(
        json.dumps({"sequence": 0, "batch_hash": commit})
    )
    return root


def test_two_sandboxes_that_differ_only_by_commit_are_identical(tmp_path: Path) -> None:
    script = _script()
    report = script.compare(
        _sandbox(tmp_path / "a", commit="a" * 40), _sandbox(tmp_path / "b", commit="b" * 40)
    )
    assert report["identical"] is True


def test_a_different_feature_value_is_reported(tmp_path: Path) -> None:
    script = _script()
    report = script.compare(
        _sandbox(tmp_path / "a", commit="a" * 40),
        _sandbox(tmp_path / "b", commit="b" * 40, close=10.01),
    )
    assert report["identical"] is False
    assert report["features"]["differing"] == [0]
    assert report["signals"]["identical"] is True
