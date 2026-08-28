#!/usr/bin/env python3
"""Build the sealed, byte-deterministic Phase C producer fixtures.

`authority.md` L1226-1227 requires every reader surface to be entered through its real
production builder, and two of the thirteen — `ReadonlyStrategyRunnerSignalSource.read_batch`
and `route_runner_signals` — cannot be entered at all without producer-side state a bounded
65,536-byte vector cannot carry: an audited runner-state database whose `runner_metadata`,
`runner_source_identity`, and `runner_signal` rows only `StrategyRunnerStore.process_batch`
can legitimately write, plus the frozen routing policy the router's manifest authority names.

This script produces that state by running the real producer surface, and it produces it the
same way every time:

* the authoritative form of the database is its **canonical SQL text dump** (ruling E-2), not
  its page bytes, so no SQLite page layout has to be reproduced and no SQLite parser is ever
  pulled into the root verifier; the harness rebuilds the database inside the child;
* `runner_source_identity.source_generation_id` is `secrets.token_hex(32)` on a first
  initialization, which would make every build differ. The row is therefore seeded first,
  with the production DDL read from `strategy_runner` itself, and `_initialize` then adopts
  it through its own audits rather than minting a new one;
* every timestamp, fingerprint, and identifier the producer consumes is a frozen literal.

Nothing here signs anything, holds any key, or touches any real credential. The output is
checked into `tests/fixtures/signal_family_producer/`, the offline verifier world copies it
into the generation tree it builds, and the full generation manifest covers it there.

Usage::

    python scripts/build-signal-family-producer-fixtures.py            # rebuild in place
    python scripts/build-signal-family-producer-fixtures.py --output D # build into D
    python scripts/build-signal-family-producer-fixtures.py --check    # verify reproducible
"""

from __future__ import annotations

import argparse
import filecmp
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final

REPOSITORY_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT: Final[Path] = REPOSITORY_ROOT / "tests" / "fixtures" / "signal_family_producer"

#: The generation-relative home of the fixture set. The offline world copies the built files
#: here, and the immutable test manifest names exactly these paths.
GENERATION_FIXTURE_PREFIX: Final[str] = "signal-family/fixtures"

STRATEGY_ROUTER_DIRECTORY: Final[str] = "strategy-router"
RUNNER_STATE_DUMP_NAME: Final[str] = "runner-state-v1.sql"
ROUTING_POLICY_NAME: Final[str] = "routing-policy-v1.json"
DESCRIPTOR_NAME: Final[str] = "descriptor.json"

#: Frozen producer inputs. Every one of these is part of the fixture's identity.
PRODUCER_COMMIT: Final[str] = "f" * 40
EVALUATOR_CONTRACT_FINGERPRINT: Final[str] = "2" * 64
STRATEGY_REGISTRATION_FINGERPRINT: Final[str] = "1" * 64
DATASET_SNAPSHOT_ID: Final[str] = "d" * 64
UPSTREAM_SOURCE_GENERATION_ID: Final[str] = "3" * 64
SOURCE_GENERATION_ID: Final[str] = "7" * 64
ROUTE_SOURCE_ID: Final[str] = "n-shape-v1"
STRATEGY_ID: Final[str] = "n-shape"
CANDIDATE_ID: Final[str] = "600000.SH"
SESSION_CLOSE: Final[datetime] = datetime(2026, 8, 24, 7, 0, tzinfo=UTC)
SIGNAL_LIFETIME: Final[timedelta] = timedelta(hours=6)

#: Verbatim from `StrategyRunnerStore._initialize`. Seeding the identity row means creating
#: the table the production initializer would have created, so the two must not diverge; the
#: unit suite pins this string against the module.
RUNNER_SOURCE_IDENTITY_DDL: Final[str] = (
    "CREATE TABLE IF NOT EXISTS runner_source_identity (\n"
    "    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),\n"
    "    source_generation_id TEXT NOT NULL\n"
    ")"
)


def _strategy_spec() -> Any:
    from rquant.feature_contracts import FeatureRequirement, RequirementLevel
    from rquant.signal_contracts import SignalAction
    from rquant.strategy_spec import (
        StateTransition,
        StrategyLifecycleState,
        StrategyRunMode,
        StrategySpec,
    )

    return StrategySpec(
        strategy_id=STRATEGY_ID,
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=1,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=1,
            ),
        ),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={},
        allowed_actions=(SignalAction.WATCH.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=PRODUCER_COMMIT,
    )


def _feature_batch() -> tuple[Any, Any, bytes]:
    import pandas as pd

    from rquant.feature_contracts import (
        FeatureAvailability,
        FeatureBatchEnvelope,
        FeatureFieldStatus,
    )
    from rquant.strategy_runner import canonical_feature_payload

    frame = pd.DataFrame({"ts_code": [CANDIDATE_ID], "rel_same_minute": [2.0]})
    payload = canonical_feature_payload(frame, schema_version=1)
    envelope = FeatureBatchEnvelope(
        schema_version=1,
        batch_id="signal-family-producer-fixture-0",
        contract_id="intraday-pit",
        contract_version=1,
        input_batch_ids=("minute-close",),
        sequence=0,
        event_time=SESSION_CLOSE,
        available_at=SESSION_CLOSE,
        decision_cutoff=SESSION_CLOSE,
        actual_delay_seconds=0.0,
        row_count=1,
        content_hash=hashlib.sha256(payload).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=SESSION_CLOSE,
                available_at=SESSION_CLOSE,
                decision_cutoff=SESSION_CLOSE,
                actual_delay_seconds=0.0,
            ),
        ),
        producer_commit=PRODUCER_COMMIT,
    )
    return envelope, frame, payload


def _evaluator(_spec: Any, state: Any, _features: Any) -> Any:
    from rquant.signal_contracts import SignalAction
    from rquant.strategy_runner import StrategyDecision
    from rquant.strategy_spec import StrategyLifecycleState

    return StrategyDecision(
        event="entry_ready",
        expected_from_state=state.state,
        expected_to_state=StrategyLifecycleState.ARMED,
        expected_action=SignalAction.WATCH,
        action=SignalAction.WATCH,
        reason_codes=("signal_family_producer_fixture",),
        expires_after=SIGNAL_LIFETIME,
    )


def _seed_source_identity(path: Path) -> None:
    """Pin the one value the production initializer would otherwise draw from urandom."""

    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute(RUNNER_SOURCE_IDENTITY_DDL)
        connection.execute(
            "INSERT INTO runner_source_identity(singleton, source_generation_id) VALUES (1, ?)",
            (SOURCE_GENERATION_ID,),
        )
    finally:
        connection.close()


def build_runner_state_dump(scratch: Path) -> tuple[str, dict[str, Any]]:
    """Drive the real producer surface once and return its canonical SQL dump."""

    from rquant.strategy_runner import StrategyRunnerStore, StrategySourceBatchReceipt

    database = scratch / "runner-state.sqlite3"
    _seed_source_identity(database)
    spec = _strategy_spec()
    store = StrategyRunnerStore(
        database,
        spec=spec,
        evaluator_contract_fingerprint=EVALUATOR_CONTRACT_FINGERPRINT,
    )
    if store.source_generation_id != SOURCE_GENERATION_ID:
        raise SystemExit("the seeded runner source generation identity was not adopted")
    envelope, frame, payload = _feature_batch()
    result = store.process_batch(
        envelope,
        frame,
        feature_payload=payload,
        source_receipt=StrategySourceBatchReceipt(
            source_generation_id=UPSTREAM_SOURCE_GENERATION_ID,
            source_sequence=envelope.sequence,
            source_batch_id=envelope.batch_id,
            source_content_hash=envelope.content_hash,
        ),
        dataset_snapshot_id=DATASET_SNAPSHOT_ID,
        observed_at=SESSION_CLOSE,
        evaluator=_evaluator,
    )
    if len(result.signals) != 1:
        raise SystemExit("the producer fixture must emit exactly one runner signal")
    connection = sqlite3.connect(database)
    try:
        dump = "\n".join(connection.iterdump()) + "\n"
    finally:
        connection.close()
    descriptor: dict[str, Any] = {
        "candidate_id": CANDIDATE_ID,
        "evaluator_contract_fingerprint": EVALUATOR_CONTRACT_FINGERPRINT,
        "producer_commit": PRODUCER_COMMIT,
        "signal_high_watermark": result.signals[0].sequence,
        "source_generation_id": SOURCE_GENERATION_ID,
        "source_id": ROUTE_SOURCE_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_registration_fingerprint": STRATEGY_REGISTRATION_FINGERPRINT,
        "strategy_spec_fingerprint": spec.spec_fingerprint,
        "strategy_version": "1",
    }
    return dump, descriptor


def build_routing_policy() -> bytes:
    """The frozen routing policy the router's manifest authority names by digest."""

    return json.dumps(
        {
            "default_no_target_reason": "no_matching_recipient",
            "rules": [
                {
                    "action": "watch",
                    "channel": "pushdeer",
                    "enabled": True,
                    "recipient_id": "admin",
                    "strategy_id": STRATEGY_ID,
                    "strategy_version": "1",
                }
            ],
        },
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def build_fixtures(output: Path) -> tuple[Path, ...]:
    """Write the complete fixture set and return every file it produced, sorted."""

    directory = output / STRATEGY_ROUTER_DIRECTORY
    directory.mkdir(mode=0o755, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="rquant-producer-fixture-") as scratch_name:
        dump, descriptor = build_runner_state_dump(Path(scratch_name).resolve())
    policy = build_routing_policy()
    descriptor["routing_policy_fingerprint"] = hashlib.sha256(policy).hexdigest()
    descriptor["runner_state_dump_sha256"] = hashlib.sha256(
        dump.encode("utf-8")
    ).hexdigest()
    written: list[Path] = []
    for name, payload in (
        (RUNNER_STATE_DUMP_NAME, dump.encode("utf-8")),
        (ROUTING_POLICY_NAME, policy),
        (
            DESCRIPTOR_NAME,
            json.dumps(
                descriptor,
                ensure_ascii=True,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            + b"\n",
        ),
    ):
        target = directory / name
        target.write_bytes(payload)
        target.chmod(0o644)
        written.append(target)
    return tuple(sorted(written))


def _check(output: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="rquant-producer-check-") as scratch_name:
        rebuilt = Path(scratch_name).resolve() / "fixtures"
        build_fixtures(rebuilt)
        differences: list[str] = []
        for candidate in sorted(rebuilt.rglob("*")):
            if not candidate.is_file():
                continue
            relative = candidate.relative_to(rebuilt)
            installed = output / relative
            if not installed.is_file() or not filecmp.cmp(candidate, installed, shallow=False):
                differences.append(str(relative))
    if differences:
        for relative in differences:
            print(f"fixture differs from a fresh build: {relative}", file=sys.stderr)
        return 1
    print(f"producer fixtures reproduce byte for byte under {output}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="rebuild into a scratch directory and compare against --output",
    )
    arguments = parser.parse_args(argv)
    sys.path.insert(0, str(REPOSITORY_ROOT / "src"))
    output = arguments.output.resolve()
    if arguments.check:
        return _check(output)
    if output.exists():
        shutil.rmtree(output)
    written = build_fixtures(output)
    for path in written:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        print(f"{digest}  {path.relative_to(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
