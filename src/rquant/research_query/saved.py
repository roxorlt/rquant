"""Saved SQL authority in the same transaction as its PageControl effect/receipt."""

from __future__ import annotations

import json
from contextlib import closing
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from pydantic import Field

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc

if TYPE_CHECKING:
    from rquant.page_control import PageControlClaim, PageControlOutbox, PageControlReceipt


class SavedResearchQuery(RuntimeContractModel):
    query_id: str
    name: str = Field(max_length=60)
    sql: str
    version: int = Field(ge=1)
    updated_at: AwareUtcDatetime


def complete_saved_query(
    outbox: PageControlOutbox, claim: PageControlClaim, *, now: datetime
) -> PageControlReceipt:
    from rquant.page_control import (
        _COMMAND_ADAPTER,
        PageControlStatus,
        _command_hash,
        _OwnedSaveResearchQuery,
    )

    command = claim.command
    if type(command) is not _OwnedSaveResearchQuery:
        raise TypeError("query completion requires an owned claim")
    observed = normalize_aware_utc(now)
    completed_at = observed.isoformat(timespec="microseconds")
    digest = _command_hash(command)
    with closing(outbox._connect()) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
        ).fetchone()
        if (
            row is None
            or row["command_kind"] != command.kind
            or row["command_hash"] != digest
            or _COMMAND_ADAPTER.validate_json(row["payload_json"]) != command
        ):
            raise ValueError("saved query command content changed")
        if (
            row["status"] != "processing"
            or row["processing_owner"] != claim.owner_id
            or row["claim_token"] != claim.claim_token
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= completed_at
        ):
            raise RuntimeError("stale query claim cannot complete")
        existing = connection.execute(
            "SELECT version FROM research_query_saved WHERE owner_id=? AND query_id=?",
            (command.owner_id, command.query_id),
        ).fetchone()
        code: str | None = None
        if command.requested_at > observed + timedelta(minutes=5):
            code = "future_request"
        elif (None if existing is None else existing["version"]) != command.expected_version:
            code = "version_conflict"
        elif (
            existing is None
            and connection.execute(
                "SELECT count(*) FROM research_query_saved WHERE owner_id=?", (command.owner_id,)
            ).fetchone()[0]
            >= 100
        ):
            code = "capacity_exceeded"
        version = 1 if existing is None else existing["version"] + 1
        if code is None:
            connection.execute(
                "INSERT INTO research_query_saved(owner_id,query_id,version,name,sql,updated_at) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT(owner_id,query_id) DO UPDATE SET "
                "version=excluded.version,name=excluded.name,sql=excluded.sql,"
                "updated_at=excluded.updated_at",
                (
                    command.owner_id,
                    command.query_id,
                    version,
                    command.name,
                    command.sql,
                    completed_at,
                ),
            )
        status = PageControlStatus.SUCCEEDED if code is None else PageControlStatus.FAILED
        result = {
            "query_id": command.query_id,
            "version": version if code is None else None,
            "code": code or "saved",
        }
        result_json = json.dumps(result, ensure_ascii=True)
        connection.execute(
            "INSERT INTO page_control_effect(command_id,command_hash,effect_kind,status,"
            "owner_id,claim_token,started_at,completed_at,result_json,error) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                command.command_id,
                digest,
                command.kind,
                status.value,
                claim.owner_id,
                claim.claim_token,
                completed_at,
                completed_at,
                result_json,
                code,
            ),
        )
        changed = connection.execute(
            "UPDATE page_control_command SET status=?,completed_at=?,result_json=?,error=?,"
            "processing_owner=NULL,lease_expires_at=NULL,claim_token=NULL "
            "WHERE command_id=? AND status='processing' AND processing_owner=? "
            "AND claim_token=? AND lease_expires_at>?",
            (
                status.value,
                completed_at,
                result_json,
                code,
                command.command_id,
                claim.owner_id,
                claim.claim_token,
                completed_at,
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError("query claim changed during completion")
        completed = connection.execute(
            "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
        ).fetchone()
        assert completed is not None
        return outbox._receipt(completed)
