"""Reconstructed fixed-file reference: independent SQL totals, no row observations."""

from __future__ import annotations

import hashlib
import json
import stat
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

# The original entry uses -I, which excludes the script and working directories.
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from rquant.data_audit_contracts import MAX_AUDIT_DAYS  # noqa: E402
from rquant.data_audit_dataset_evidence import read_catalog_audit_from_connection  # noqa: E402
from rquant.data_audit_datasets import MAX_DATASET_FIELDS  # noqa: E402
from rquant.data_catalog.build import CATALOG_CONTRACTS  # noqa: E402
from rquant.data_contracts import VisibilityRule  # noqa: E402


class ReplicaIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    device: int = Field(strict=True, ge=0)
    inode: int = Field(strict=True, ge=0)
    size: int = Field(strict=True, gt=0)
    mtime_ns: int = Field(strict=True)


class FixedAuditInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    replica_path: Path
    replica_identity: ReplicaIdentity
    audit_start: date
    observed_through: date
    as_of: AwareDatetime
    dataset_ids: tuple[str, ...] = Field(min_length=1, max_length=len(CATALOG_CONTRACTS))

    @model_validator(mode="after")
    def _scope(self) -> Self:
        if not self.replica_path.is_absolute():
            raise ValueError("fixed replica path must be absolute")
        if not 1 <= (self.observed_through - self.audit_start).days + 1 <= MAX_AUDIT_DAYS:
            raise ValueError("invalid bounded audit date range")
        known = {contract.dataset_id for contract in CATALOG_CONTRACTS}
        if len(self.dataset_ids) != len(set(self.dataset_ids)) or set(self.dataset_ids) - known:
            raise ValueError("audit dataset selection is repeated or unknown")
        return self


class AuditSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    source_id: str
    verification_scope: Literal[
        "unchanged_file_identity_not_content_hash_or_completion"
    ] = "unchanged_file_identity_not_content_hash_or_completion"
    independent_row_totals: dict[str, int]
    report_row_totals: dict[str, int]
    totals_match: bool
    identity_before: ReplicaIdentity
    identity_after: ReplicaIdentity
    scratch_empty: bool
    private_directory_removed: bool


def _file_identity(path: Path) -> ReplicaIdentity:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("fixed replica must be a regular file")
    return ReplicaIdentity(
        device=info.st_dev,
        inode=info.st_ino,
        size=info.st_size,
        mtime_ns=info.st_mtime_ns,
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _independent_row_totals(
    connection: duckdb.DuckDBPyConnection, inputs: FixedAuditInput
) -> dict[str, int]:
    totals: dict[str, int] = {}
    for contract in sorted(CATALOG_CONTRACTS, key=lambda item: item.dataset_id):
        if contract.dataset_id not in inputs.dataset_ids:
            continue
        columns = connection.execute(
            "SELECT column_name,data_type FROM information_schema.columns "
            "WHERE table_catalog=current_database() AND table_schema='main' AND table_name=? "
            "ORDER BY ordinal_position LIMIT ?",
            [contract.table_name, MAX_DATASET_FIELDS + 1],
        ).fetchall()
        if not columns:
            totals[contract.dataset_id] = 0
            continue
        if len(columns) > MAX_DATASET_FIELDS:
            raise ValueError("catalog table exceeds column budget")
        types = dict(columns)
        event_column = contract.event_time_column or contract.event_date_column
        parameters: list[date] = []
        where = "true"
        if contract.historized and event_column:
            if contract.visibility == VisibilityRule.MINUTE_AS_OF and contract.event_date_column:
                date_event = _quote_identifier(contract.event_date_column)
            else:
                event = _quote_identifier(event_column)
                if types[event_column] == "TIMESTAMP WITH TIME ZONE":
                    event = f"timezone('Asia/Shanghai', {event})"
                date_event = f"CAST({event} AS DATE)"
            where = f"({date_event} BETWEEN ? AND ? OR {date_event} IS NULL)"
            parameters = [inputs.audit_start, inputs.observed_through]
        row = connection.execute(
            f"SELECT COUNT(*) FROM {_quote_identifier(contract.table_name)} WHERE {where}",
            parameters,
        ).fetchone()
        assert row is not None
        totals[contract.dataset_id] = int(row[0])
    return totals


def run_reference(inputs: FixedAuditInput) -> AuditSummary:
    before = _file_identity(inputs.replica_path)
    if before != inputs.replica_identity:
        raise ValueError("fixed replica identity differs from declared input")
    source_id = "stat-sha256:" + hashlib.sha256(
        json.dumps(before.model_dump(), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with tempfile.TemporaryDirectory(
        prefix="rquant-audit-reference-", dir=Path("/tmp").resolve()
    ) as directory:
        private = Path(directory)
        scratch = private / "scratch"
        scratch.mkdir(mode=0o700)
        with duckdb.connect(
            str(inputs.replica_path),
            read_only=True,
            config={"temp_directory": str(scratch), "threads": "1"},
        ) as connection:
            independent = _independent_row_totals(connection, inputs)
            reports = read_catalog_audit_from_connection(
                connection,
                source_id=source_id,
                audit_start=inputs.audit_start,
                observed_through=inputs.observed_through,
                as_of=inputs.as_of,
                dataset_ids=inputs.dataset_ids,
            )
            observed = {report.dataset_id: report.observed_rows for report in reports}
            if independent != observed:
                raise ValueError("independent totals disagree with catalog audit")
        scratch_empty = not any(scratch.iterdir())
        if not scratch_empty:
            raise ValueError("private scratch remains after connection close")
    after = _file_identity(inputs.replica_path)
    if after != before:
        raise ValueError("fixed replica identity changed during audit")
    return AuditSummary(
        source_id=source_id,
        independent_row_totals=independent,
        report_row_totals=observed,
        totals_match=True,
        identity_before=before,
        identity_after=after,
        scratch_empty=scratch_empty,
        private_directory_removed=not private.exists(),
    )


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: real_source_driver.py INPUT.json")
    inputs = FixedAuditInput.model_validate_json(Path(sys.argv[1]).read_bytes())
    print(run_reference(inputs).model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
