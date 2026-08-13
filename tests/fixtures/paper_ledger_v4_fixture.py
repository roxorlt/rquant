"""Read-only loader for the exact-parent paper-ledger v4 binary fixture."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

PARENT_V4_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
EXPECTED_V4_FIXTURE_SHA256 = "9dc51ed7104035a54172f850ecf6bb75f344b565d35db7845722b4acbb2a94f7"
EXPECTED_V4_SCHEMA_FINGERPRINT = "3ae9e0749b1132f8b1e55f15866534e1bcae9a0815fd0d7c6837297fc45dcc70"
EXPECTED_V4_ATTESTATION_FINGERPRINT = (
    "38e5cca0f1bd6a7ce5f619114732afd4efb89aa6f0e12066b2bba8ffb280f3e4"
)
EXPECTED_V4_HEAD_FINGERPRINT = "9601b1ca6135a4142e9701bc7b2d8e591a7a8f2739ab155cc851bd1472653364"
LEGACY_ACCOUNT_ID = "legacy-v4-account"

_ROOT = Path(__file__).resolve().parent
_BINARY = _ROOT / "paper_ledger_v4.sqlite3"
_MANIFEST = _ROOT / "paper_ledger_v4.manifest.json"
_SEED = _ROOT / "paper_ledger_v4_seed.json"
_TABLE_ORDER = {
    "broker_account": "account_id",
    "paper_intent": "intent_id",
    "paper_order": "order_id",
    "paper_fill": "fill_id",
    "paper_lot": "lot_id",
    "paper_lot_consumption": "fill_id, lot_id",
    "paper_execution_receipt": "execution_id",
    "paper_account_authority": "account_id",
    "paper_ledger_schema": "singleton",
    "paper_ledger_attestation": "revision",
    "paper_ledger_head_marker": "revision",
    "paper_ledger_tamper_marker": "tamper_id",
}


@dataclass(frozen=True)
class ParentV4Fixture:
    path: Path
    source_sha256: str
    schema_fingerprint: str
    predecessor_attestation_fingerprint: str
    predecessor_head_fingerprint: str
    historical_rows: dict[str, tuple[tuple[object, ...], ...]]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest() -> dict[str, object]:
    payload = json.loads(_MANIFEST.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("parent-v4 manifest is not an object")
    return payload


def _connection(path: Path) -> sqlite3.Connection:
    uri = f"{path.absolute().as_uri()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _historical_rows(connection: sqlite3.Connection) -> dict[str, tuple[tuple[object, ...], ...]]:
    return {
        table: tuple(
            tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {ordering}')
        )
        for table, ordering in _TABLE_ORDER.items()
    }


def create_parent_v4_fixture(path: Path) -> ParentV4Fixture:
    """Copy the frozen binary after independently checking its hardcoded identity."""

    if path.exists():
        raise ValueError("parent-v4 fixture destination must not exist")
    manifest = _manifest()
    fixture_sha256 = sha256_file(_BINARY)
    if (
        fixture_sha256 != EXPECTED_V4_FIXTURE_SHA256
        or manifest.get("fixture_sha256") != EXPECTED_V4_FIXTURE_SHA256
        or manifest.get("parent_commit") != PARENT_V4_COMMIT
        or manifest.get("schema_version") != 4
        or manifest.get("internal_migration_version") != 2
        or manifest.get("seed_sha256") != sha256_file(_SEED)
    ):
        raise AssertionError("parent-v4 fixture provenance differs from frozen identity")
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_BINARY, path)
    with _connection(path) as connection:
        schema = connection.execute(
            "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
        ).fetchone()
        migration = connection.execute(
            "SELECT migration_version FROM paper_ledger_attestation WHERE revision = 1"
        ).fetchone()
        attestation = connection.execute(
            "SELECT schema_fingerprint, attestation_fingerprint "
            "FROM paper_ledger_attestation ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        head = connection.execute(
            "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if (
            schema is None
            or int(schema[0]) != 4
            or migration is None
            or int(migration[0]) != 2
            or attestation is None
            or str(attestation[0]) != EXPECTED_V4_SCHEMA_FINGERPRINT
            or str(attestation[1]) != EXPECTED_V4_ATTESTATION_FINGERPRINT
            or head is None
            or str(head[0]) != EXPECTED_V4_HEAD_FINGERPRINT
        ):
            raise AssertionError("parent-v4 database trust identity differs")
        historical_rows = _historical_rows(connection)
    return ParentV4Fixture(
        path=path,
        source_sha256=fixture_sha256,
        schema_fingerprint=EXPECTED_V4_SCHEMA_FINGERPRINT,
        predecessor_attestation_fingerprint=EXPECTED_V4_ATTESTATION_FINGERPRINT,
        predecessor_head_fingerprint=EXPECTED_V4_HEAD_FINGERPRINT,
        historical_rows=historical_rows,
    )


def archived_v4_rows(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with _connection(path) as connection:
        return {
            table.removesuffix("_v4_archive"): tuple(
                tuple(row)
                for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {ordering}')
            )
            for table, ordering in (
                ("paper_ledger_schema_v4_archive", "singleton"),
                ("paper_ledger_attestation_v4_archive", "revision"),
                ("paper_ledger_head_marker_v4_archive", "revision"),
                ("paper_ledger_tamper_marker_v4_archive", "tamper_id"),
            )
        }


def canonical_legacy_receipt_json() -> str:
    seed = json.loads(_SEED.read_text(encoding="utf-8"))
    return json.dumps(seed, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
