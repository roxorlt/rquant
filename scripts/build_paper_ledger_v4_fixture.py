#!/usr/bin/env python3
"""Build the frozen paper-ledger v4 fixture from its exact parent commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

PARENT_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
SCHEMA_VERSION = 4
INTERNAL_MIGRATION_VERSION = 2


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _run(command: tuple[str, ...], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
    if completed.returncode:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n{completed.stderr}"
        )
    return completed


def _fixture_measurements(path: Path) -> dict[str, object]:
    uri = f"{path.absolute().as_uri()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        schema = connection.execute(
            "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
        ).fetchone()
        migration = connection.execute(
            "SELECT migration_version FROM paper_ledger_attestation WHERE revision = 1"
        ).fetchone()
        objects = connection.execute(
            "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name"
        ).fetchall()
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ).fetchall()
        columns = {
            str(table[0]): tuple(
                {
                    "cid": int(row[0]),
                    "name": str(row[1]),
                    "type": str(row[2]),
                    "notnull": int(row[3]),
                    "default": row[4],
                    "pk": int(row[5]),
                }
                for row in connection.execute(f'PRAGMA table_info("{str(table[0])}")').fetchall()
            )
            for table in tables
        }
        triggers = tuple(
            {"name": str(row[0]), "sql": " ".join(str(row[1]).split())}
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
            ).fetchall()
        )
        attestation = connection.execute(
            "SELECT schema_fingerprint, attestation_fingerprint FROM paper_ledger_attestation "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        head = connection.execute(
            "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        business_rows = {
            table: tuple(tuple(row) for row in connection.execute(f'SELECT * FROM "{table}"'))
            for table in (
                "broker_account",
                "paper_intent",
                "paper_order",
                "paper_fill",
                "paper_lot",
                "paper_lot_consumption",
                "paper_execution_receipt",
            )
        }
    finally:
        connection.close()
    if schema is None or migration is None or attestation is None or head is None:
        raise RuntimeError("parent fixture trust metadata is incomplete")
    normalized_objects = tuple(
        {
            "type": str(row["type"]),
            "name": str(row["name"]),
            "sql": " ".join(str(row["sql"]).split()),
        }
        for row in objects
    )
    return {
        "schema_version": int(schema[0]),
        "internal_migration_version": int(migration[0]),
        "sqlite_master_fingerprint": _sha256_bytes(_canonical_json(normalized_objects)),
        "column_fingerprints": {
            table: _sha256_bytes(_canonical_json(value)) for table, value in columns.items()
        },
        "trigger_fingerprint": _sha256_bytes(_canonical_json(triggers)),
        "schema_fingerprint": str(attestation[0]),
        "predecessor_attestation_fingerprint": str(attestation[1]),
        "predecessor_head_fingerprint": str(head[0]),
        "seeded_business_rows_fingerprint": _sha256_bytes(_canonical_json(business_rows)),
    }


def _driver(parent_root: Path, output: Path, seed_path: Path) -> None:
    parent_src = parent_root / "src"
    sys.path.insert(0, str(parent_src))
    import rquant.paper_broker as parent_broker
    from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
    from rquant.paper_contracts import PaperOrderIntent, PaperOrderType, PaperSide

    imported = Path(parent_broker.__file__).resolve()
    if parent_src.resolve() not in imported.parents:
        raise RuntimeError(f"current-code import detected: {imported}")
    if parent_broker._LEDGER_MIGRATION_VERSION != INTERNAL_MIGRATION_VERSION:
        raise RuntimeError("parent internal migration version is not 2")
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    frozen_now = datetime.fromisoformat(seed["frozen_now"]).astimezone(UTC)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            return frozen_now if tz is None else frozen_now.astimezone(tz)

    policy = BrokerCostPolicy(**seed["cost_policy"])
    output.parent.mkdir(parents=True, exist_ok=True)
    with (
        patch.object(parent_broker, "datetime", FrozenDateTime),
        patch.object(
            parent_broker.secrets,
            "token_hex",
            return_value=str(seed["ledger_generation"]),
        ),
    ):
        store = PaperBrokerStore(
            output,
            account_id=seed["account_id"],
            initial_cash=Decimal(seed["initial_cash"]),
            cost_policy=policy,
        )
        buy = seed["executions"][0]
        buy_event = datetime.fromisoformat(buy["event_time"])
        buy_intent = PaperOrderIntent(
            signal_id=buy["signal_id"],
            account_id=seed["account_id"],
            ts_code=seed["ts_code"],
            side=PaperSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=buy["quantity"],
            event_time=buy_event,
            available_at=buy_event + timedelta(seconds=1),
            expires_at=buy_event + timedelta(minutes=5),
            earliest_execution_at=buy_event + timedelta(seconds=1),
            price_snapshot_id=seed["price_snapshot_id"],
            producer_commit=seed["producer_commit"],
        )
        buy_order = store.submit_intent(
            buy_intent,
            execution_id=buy["execution_id"],
            decision_time=datetime.fromisoformat(buy["decision_time"]),
            persisted_at=datetime.fromisoformat(buy["persisted_at"]),
            trade_date=date.fromisoformat(buy["trade_date"]),
            quote=BrokerExecutionContext(
                executable_price=Decimal(buy["price"]),
                executable_quantity=buy["executable_quantity"],
                acquisition_available_date=date.fromisoformat(buy["available_date"]),
            ),
        )
        incremental_buy = seed["executions"][1]
        store.apply_execution(
            buy_order.order_id,
            execution_id=incremental_buy["execution_id"],
            executed_at=datetime.fromisoformat(incremental_buy["executed_at"]),
            persisted_at=datetime.fromisoformat(incremental_buy["persisted_at"]),
            trade_date=date.fromisoformat(incremental_buy["trade_date"]),
            quantity=incremental_buy["quantity"],
            price_snapshot_id=seed["price_snapshot_id"],
            quote=BrokerExecutionContext(
                executable_price=Decimal(incremental_buy["price"]),
                executable_quantity=incremental_buy["quantity"],
                acquisition_available_date=date.fromisoformat(incremental_buy["available_date"]),
            ),
        )
        sell = seed["executions"][2]
        sell_decision = datetime.fromisoformat(sell["decision_time"])
        authority = store.sell_quantity_authority(
            exit_signal_id=sell["signal_id"],
            entry_signal_id=buy["signal_id"],
            ts_code=seed["ts_code"],
            action=sell["action"],
            tranche_fraction=Decimal(sell["tranche_fraction"]),
            decision_cutoff=sell_decision,
            trade_date=date.fromisoformat(sell["trade_date"]),
        )
        sell_event = datetime.fromisoformat(sell["event_time"])
        sell_intent = PaperOrderIntent(
            signal_id=sell["signal_id"],
            entry_signal_id=buy["signal_id"],
            sell_quantity_authority=authority,
            account_id=seed["account_id"],
            ts_code=seed["ts_code"],
            side=PaperSide.SELL,
            order_type=PaperOrderType.MARKET,
            quantity=sell["quantity"],
            event_time=sell_event,
            available_at=sell_event + timedelta(seconds=1),
            expires_at=sell_event + timedelta(minutes=5),
            earliest_execution_at=sell_event + timedelta(seconds=1),
            price_snapshot_id=seed["price_snapshot_id"],
            producer_commit=seed["producer_commit"],
        )
        store.submit_intent(
            sell_intent,
            execution_id=sell["execution_id"],
            decision_time=sell_decision,
            persisted_at=datetime.fromisoformat(sell["persisted_at"]),
            trade_date=date.fromisoformat(sell["trade_date"]),
            quote=BrokerExecutionContext(executable_price=Decimal(sell["price"])),
        )
        reconciliation = store.reconcile()
        if not reconciliation.is_consistent:
            raise RuntimeError("parent fixture business ledger did not reconcile")
    with sqlite3.connect(output) as connection:
        connection.execute(
            "UPDATE paper_ledger_schema SET migrated_at = ? WHERE singleton = 1",
            (frozen_now.isoformat(timespec="microseconds").replace("+00:00", "Z"),),
        )
        schema = connection.execute(
            "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
        ).fetchone()
        migration = connection.execute(
            "SELECT migration_version FROM paper_ledger_attestation WHERE revision = 1"
        ).fetchone()
        if schema != (SCHEMA_VERSION,) or migration != (INTERNAL_MIGRATION_VERSION,):
            raise RuntimeError("parent fixture schema identity differs")
        chronological_lot_ids = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT lot_id FROM paper_lot
                ORDER BY available_date, acquisition_trade_date, buy_executed_at,
                         buy_persisted_at, buy_fill_sequence, lot_id
                """
            ).fetchall()
        )
        if len(chronological_lot_ids) != 2 or chronological_lot_ids != tuple(
            sorted(chronological_lot_ids, reverse=True)
        ):
            raise RuntimeError(
                "parent fixture lot ids are not inverse to canonical FIFO chronology"
            )


def _build(repo: Path, output_dir: Path) -> None:
    seed_path = repo / "tests/fixtures/paper_ledger_v4_seed.json"
    output = output_dir / "paper_ledger_v4.sqlite3"
    manifest_path = output_dir / "paper_ledger_v4.manifest.json"
    if output.exists() or manifest_path.exists():
        raise RuntimeError("fixture outputs already exist")
    with tempfile.TemporaryDirectory(prefix="rquant-paper-v4-parent-") as directory:
        parent_root = Path(directory) / "parent"
        _run(("git", "worktree", "add", "--detach", str(parent_root), PARENT_COMMIT), cwd=repo)
        try:
            head = _run(("git", "rev-parse", "HEAD"), cwd=parent_root).stdout.strip()
            dirty = _run(("git", "status", "--porcelain"), cwd=parent_root).stdout
            if head != PARENT_COMMIT or dirty:
                raise RuntimeError("detached parent worktree identity or cleanliness differs")
            _run(
                (
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--driver",
                    str(parent_root),
                    str(output),
                    str(seed_path),
                ),
                cwd=repo,
            )
            with sqlite3.connect(output) as connection:
                checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if checkpoint is None or int(checkpoint[0]) != 0:
                    raise RuntimeError("parent fixture WAL checkpoint did not complete")
                connection.execute("PRAGMA journal_mode = DELETE")
                connection.execute("PRAGMA page_size = 4096")
                connection.execute("VACUUM")
        finally:
            _run(("git", "worktree", "remove", "--force", str(parent_root)), cwd=repo)
    measurements = _fixture_measurements(output)
    if (
        measurements["schema_version"] != SCHEMA_VERSION
        or measurements["internal_migration_version"] != INTERNAL_MIGRATION_VERSION
    ):
        raise RuntimeError("built fixture does not carry exact parent schema identity")
    manifest = {
        "contract": "rquant-paper-ledger-v4-fixture/v1",
        "parent_commit": PARENT_COMMIT,
        "schema_version": SCHEMA_VERSION,
        "internal_migration_version": INTERNAL_MIGRATION_VERSION,
        "fixture_sha256": _sha256_file(output),
        "seed_sha256": _sha256_file(seed_path),
        "python_version": sys.version.split()[0],
        "sqlite_version": sqlite3.sqlite_version,
        **measurements,
    }
    manifest_path.write_bytes(_canonical_json(manifest) + b"\n")
    if (
        _fixture_measurements(output) != measurements
        or _sha256_file(output) != manifest["fixture_sha256"]
    ):
        raise RuntimeError("fixture changed during final read-only verification")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--driver", action="store_true")
    parser.add_argument("driver_args", nargs="*")
    arguments = parser.parse_args()
    if arguments.driver:
        if len(arguments.driver_args) != 3:
            raise RuntimeError("driver requires parent root, output, and seed")
        _driver(*(Path(value) for value in arguments.driver_args))
        return 0
    repo = Path(__file__).resolve().parents[1]
    output_dir = arguments.output_dir or repo / "tests/fixtures"
    _build(repo, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
