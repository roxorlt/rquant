"""The middle process of the three-layer parent-death case.

pytest is the supervisor, this file is the heartbeat helper's *parent*, and the
production helper is the third layer.  The supervisor cannot be the helper's
parent itself - it has to survive in order to make the assertions - so a layer
that can be killed outright goes in between.

What runs here is the production path and nothing simulated: a real
`SourceBrokerV2Saga`, a real outbox row taken through `_begin_outbox` and
`_acquire_outbox_lease`, and `_invoke_with_heartbeat` with an invocation that
parks forever.  The helper this launches therefore has exactly the three
descriptors production gives it - no liveness pipe, no extra frame - which is
the point: the claim under test is about the helper as shipped.

The report is written from inside the invocation, once the session is
acknowledged and renewals are running, and it is renamed into place so the
supervisor can never read half of it.  It carries the helper's pid and the
inode of the stop pipe, both read here, where the descriptors are owned; the
helper is not asked to report anything about itself.

Single file, run under `-I`, and never collected by pytest
(`python_files = ["test_*.py"]`).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker_v2 import (
    SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
    SourceAuthorityKeyring,
    SourceBrokerV2OutboxPhase,
    SourceBrokerV2Saga,
    _outbox_payload_hash,
)
from rquant.source_quota_authority import SourceQuotaParentAuthority
from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterV2
from rquant.strict_json import canonical_json_bytes


class _Signer:
    key_id = "wp9-parent-death-layer"

    def sign(self, payload: bytes) -> str:
        return canonical_sha256({"payload": payload.hex(), "signer": self.key_id})

    def verify(self, payload: bytes, signature: str) -> bool:
        return signature == self.sign(payload)


class _Transport:
    def __init__(self, keyring: SourceAuthorityKeyring) -> None:
        self.source_authority_keyring = keyring


def _build_saga(args: argparse.Namespace) -> SourceBrokerV2Saga:
    root = Path(args.root)
    keyring = SourceAuthorityKeyring(
        expected_authority_id="wp9-source-authority",
        allowed_public_keys={"wp9-key": Path(args.public_key).read_bytes()},
        expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
        expected_schema_version=2,
    )
    authority = SourceQuotaParentAuthority.for_nonproduction_standalone(
        root / "quota-authority.sqlite3",
        authority_id="wp9-quota-authority",
        signer=_Signer(),
    )
    quota = SourceQuotaBrokerAdapterV2(
        root / "quota-adapter.sqlite3",
        authority=authority,
        adapter_id="wp9-quota-adapter",
    )
    return SourceBrokerV2Saga.for_nonproduction(
        Path(args.db),
        saga_id=args.saga_id,
        current_claim_authority=object(),
        quota_adapter=quota,
        transport=_Transport(keyring),  # type: ignore[arg-type]
        lineage_authority=object(),
        busy_timeout_ms=args.busy_timeout_ms,
        executor_lease_seconds=args.lease_seconds,
        executor_wait_seconds=5.0,
        source_request_deadline_seconds=30.0,
        source_takeover_grace_seconds=5.0,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--public-key", required=True)
    parser.add_argument("--saga-id", required=True)
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--lease-seconds", type=float, required=True)
    parser.add_argument("--busy-timeout-ms", type=int, required=True)
    args = parser.parse_args(argv)

    saga = _build_saga(args)
    phase = SourceBrokerV2OutboxPhase.LINEAGE
    payload = canonical_json_bytes({"wp9": "parent-death"})
    with saga._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT OR IGNORE INTO source_broker_v2_saga "
            "(saga_id, request_json, request_hash, state) VALUES (?, ?, ?, ?)",
            (args.saga_id, "{}", canonical_sha256({}), "claimed"),
        )
        connection.commit()
    saga._begin_outbox(
        phase=phase,
        operation_id=args.operation_id,
        payload=payload,
        payload_hash=canonical_sha256({"wp9": "parent-death"}),
        idempotency_hash=_outbox_payload_hash(payload),
    )
    owner_generation, stored, _ = saga._acquire_outbox_lease(
        phase=phase, operation_id=args.operation_id
    )
    saga._mark_invoke_started(
        phase=phase, operation_id=args.operation_id, owner_generation=owner_generation
    )

    def invoke(_: bytes) -> bytes:
        helper = saga._heartbeat_child
        assert helper is not None
        stop_w = saga._heartbeat_state.stop_w
        assert stop_w is not None
        report = {
            "layer_pid": os.getpid(),
            "helper_pid": helper.pid,
            "stop_inode": os.fstat(stop_w).st_ino,
            "operation_id": args.operation_id,
            "owner_generation": owner_generation,
        }
        temporary = Path(args.report + ".partial")
        temporary.write_text(json.dumps(report), encoding="utf-8")
        temporary.replace(Path(args.report))
        # Parked for good.  The supervisor kills this process from here, which
        # is the event the case is about: nothing in this layer gets a chance
        # to close a descriptor or send a frame on the way out.
        while True:
            time.sleep(3600)

    saga._invoke_with_heartbeat(
        phase=phase,
        operation_id=args.operation_id,
        owner_generation=owner_generation,
        payload=stored,
        invoke=invoke,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
