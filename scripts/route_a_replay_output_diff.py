"""Compare what two Route A day replays published, dataset by dataset (#302 equivalence).

Two replay sandboxes of the same day (`scripts/route_a_day_replay.py`) run different code,
so every identity that binds the producer commit differs between them even where the code
computed exactly the same thing: the replay stamps each sandbox with its own commit. This
compares what does not depend on the commit, and says per dataset what "identical" means:

* **market_minute**: every raw batch's payload bytes; its envelope without
  `producer_commit`.
* **features**: every feature batch's payload bytes; its envelope without `batch_id` and
  `producer_commit` (the commit is hashed into `batch_id`); the session close marker's
  batch count and final sequence.
* **signals**: every runner signal of every strategy -- strategy, feature sequence,
  candidate, action, event / available / expires times, reason codes, and the evidence
  without the keys that name an artifact (`*_id`, `*_sha256`, `*_fingerprint`, `*_hash`).
* **paper_fills**: every fill -- code, side, quantity, price, fees, executed_at.
* **paper_constraints**: the sequences of the published generations, and every record of
  the current generation without `producer_commit`, `content_hash` and
  `source_snapshot_ids` (all three bind commit-stamped upstream identities).

Read only: files are opened for reading, SQLite with `immutable=1`. Exit status 0 when
every dataset is identical, 1 otherwise; `--json` prints the full report.

    ./.venv/bin/python scripts/route_a_replay_output_diff.py <sandbox-a> <sandbox-b>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

_RUNTIME = Path("host/data/runtime")


def _json(path: Path) -> Any:
    with path.open("rb") as stream:
        return json.loads(stream.read())


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.sha256(stream.read()).hexdigest()


def _without(document: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key not in keys}


#: evidence keys that name an artifact rather than describe the decision: each one hashes
#: something the sandbox commit is part of (a feature batch id, a candidate generation, an
#: evaluator contract), so it differs between two sandboxes whatever the code decided
_IDENTITY_SUFFIXES = ("_id", "_sha256", "_fingerprint", "_hash")


def _commit_free(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _commit_free(item)
            for key, item in sorted(value.items())
            if not key.endswith(_IDENTITY_SUFFIXES)
        }
    if isinstance(value, list):
        return [_commit_free(item) for item in value]
    return value


def _spool_batches(root: Path, *, drop: tuple[str, ...]) -> dict[int, dict[str, Any]]:
    batches: dict[int, dict[str, Any]] = {}
    for manifest in sorted(root.glob("*.json")):
        if not manifest.stem.isdigit():
            continue
        payload = manifest.with_suffix(".payload")
        batches[int(manifest.stem)] = {
            "envelope": _without(_json(manifest), *drop),
            "payload_sha256": _sha256(payload) if payload.is_file() else None,
        }
    return batches


def market_minute(sandbox: Path) -> dict[int, dict[str, Any]]:
    root = sandbox / _RUNTIME / "live" / "market-minute" / "batches" / "market_minute"
    return _spool_batches(root, drop=("producer_commit",))


def features(sandbox: Path) -> dict[str, Any]:
    root = sandbox / _RUNTIME / "live" / "features"
    markers = {
        path.parent.name: {
            key: value
            for key, value in _json(path).items()
            if key in {"trade_date", "first_sequence", "final_sequence", "batch_count"}
        }
        for path in sorted((root / "sessions").glob("*/close-marker.json"))
    }
    return {
        "batches": _spool_batches(root / "batches", drop=("batch_id", "producer_commit")),
        "close_markers": markers,
    }


def _sqlite_rows(database: Path, query: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{database}?immutable=1", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(query)]
    finally:
        connection.close()


def _strategy_ids(sandbox: Path) -> dict[str, str]:
    """svc-<hash> directory -> strategy id, from the runner's own metadata."""

    found: dict[str, str] = {}
    for database in sorted((sandbox / _RUNTIME / "live" / "strategies").glob("*/runner.sqlite3")):
        rows = _sqlite_rows(database, "SELECT strategy_spec_json FROM runner_metadata")
        spec = json.loads(rows[0]["strategy_spec_json"]) if rows else {}
        found[database.parent.name] = str(spec.get("strategy_id", database.parent.name))
    return found


def signals(sandbox: Path) -> list[tuple[Any, ...]]:
    names = _strategy_ids(sandbox)
    found: list[tuple[Any, ...]] = []
    for database in sorted((sandbox / _RUNTIME / "live" / "strategies").glob("*/runner.sqlite3")):
        for row in _sqlite_rows(
            database,
            "SELECT feature_sequence, candidate_id, action, event_time, available_at, "
            "expires_at, payload_json FROM runner_signal ORDER BY sequence",
        ):
            payload = json.loads(row["payload_json"])
            found.append(
                (
                    names[database.parent.name],
                    row["feature_sequence"],
                    row["candidate_id"],
                    row["action"],
                    row["event_time"],
                    row["available_at"],
                    row["expires_at"],
                    json.dumps(payload.get("reason_codes"), sort_keys=True),
                    json.dumps(_commit_free(payload.get("evidence")), sort_keys=True),
                )
            )
    return sorted(found)


def paper_fills(sandbox: Path) -> list[tuple[Any, ...]]:
    found: list[tuple[Any, ...]] = []
    for database in sorted(
        (sandbox / _RUNTIME / "live" / "paper-brokers").glob("*/broker.sqlite3")
    ):
        for row in _sqlite_rows(
            database,
            "SELECT o.ts_code AS ts_code, o.side AS side, f.quantity AS quantity, "
            "f.price AS price, f.commission AS commission, f.tax AS tax, "
            "f.total_fees AS total_fees, f.executed_at AS executed_at "
            "FROM paper_fill AS f JOIN paper_order AS o ON o.order_id = f.order_id "
            "ORDER BY f.executed_at, o.ts_code",
        ):
            found.append(tuple(row.values()))
    return found


def paper_constraints(sandbox: Path) -> dict[str, Any]:
    root = sandbox / _RUNTIME / "authorities" / "paper-execution"
    generations = sorted(
        (_json(path) for path in (root / "generations").glob("*.json")),
        key=lambda batch: batch["sequence"],
    )
    current = _json(root / "current.json") if (root / "current.json").is_file() else None
    selected = next(
        (
            batch
            for batch in generations
            if current is not None and batch["content_hash"] == current["batch_hash"]
        ),
        None,
    )
    records = (
        []
        if selected is None
        else [
            _without(record, "producer_commit", "content_hash", "source_snapshot_ids")
            | {
                "instrument_context": _without(
                    record.get("instrument_context") or {}, "classification_provenance"
                )
            }
            for record in selected["records"]
        ]
    )
    return {
        "generation_sequences": [batch["sequence"] for batch in generations],
        "current_sequence": None if current is None else current["sequence"],
        "current_records": records,
    }


def _diff_keys(left: dict[Any, Any], right: dict[Any, Any]) -> dict[str, Any]:
    only_left = sorted(set(left) - set(right))
    only_right = sorted(set(right) - set(left))
    differing = sorted(key for key in set(left) & set(right) if left[key] != right[key])
    return {
        "count": [len(left), len(right)],
        "only_a": only_left[:20],
        "only_b": only_right[:20],
        "differing": differing[:20],
        "identical": not (only_left or only_right or differing),
    }


def _diff_lists(left: Iterable[Any], right: Iterable[Any]) -> dict[str, Any]:
    left_list, right_list = list(left), list(right)
    only_left = [item for item in left_list if item not in right_list]
    only_right = [item for item in right_list if item not in left_list]
    return {
        "count": [len(left_list), len(right_list)],
        "only_a": only_left[:20],
        "only_b": only_right[:20],
        "identical": left_list == right_list,
    }


def compare(a: Path, b: Path) -> dict[str, Any]:
    feature_a, feature_b = features(a), features(b)
    constraint_a, constraint_b = paper_constraints(a), paper_constraints(b)
    report = {
        "market_minute": _diff_keys(market_minute(a), market_minute(b)),
        "features": _diff_keys(feature_a["batches"], feature_b["batches"])
        | {"close_markers_identical": feature_a["close_markers"] == feature_b["close_markers"]},
        "signals": _diff_lists(signals(a), signals(b)),
        "paper_fills": _diff_lists(paper_fills(a), paper_fills(b)),
        "paper_constraints": {
            "generation_sequences": _diff_lists(
                constraint_a["generation_sequences"], constraint_b["generation_sequences"]
            ),
            "current_sequence": [
                constraint_a["current_sequence"],
                constraint_b["current_sequence"],
            ],
            "current_records": _diff_lists(
                constraint_a["current_records"], constraint_b["current_records"]
            ),
        },
    }
    report["features"]["identical"] = (
        report["features"]["identical"] and report["features"]["close_markers_identical"]
    )
    constraints = report["paper_constraints"]
    constraints["identical"] = (
        constraints["generation_sequences"]["identical"]
        and constraints["current_records"]["identical"]
        and constraints["current_sequence"][0] == constraints["current_sequence"][1]
    )
    report["identical"] = all(
        report[name]["identical"]
        for name in ("market_minute", "features", "signals", "paper_fills", "paper_constraints")
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sandbox_a", type=Path)
    parser.add_argument("sandbox_b", type=Path)
    parser.add_argument("--json", action="store_true", help="print the whole report")
    arguments = parser.parse_args(argv)
    report = compare(arguments.sandbox_a.resolve(), arguments.sandbox_b.resolve())
    if arguments.json:
        print(json.dumps(report, indent=1, default=str))
    for name in ("market_minute", "features", "signals", "paper_fills", "paper_constraints"):
        section = report[name]
        detail = section.get("count") or section["current_records"]["count"]
        print(f"{name}: {'identical' if section['identical'] else 'DIFFERENT'} {detail}")
    print("IDENTICAL" if report["identical"] else "DIFFERENT")
    return 0 if report["identical"] else 1


if __name__ == "__main__":
    sys.exit(main())
