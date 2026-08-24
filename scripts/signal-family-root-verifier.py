#!/usr/bin/env python3
"""The production entry point of the root-owned signal-family verifier.

The four anchors of authority.md L1280-1291 and L1401-1405 are written here as literals
and nowhere else. There is no environment variable, no flag, and no configuration file
that can move the policy, the harness, or the store: the offline suite reaches those
locations only by constructing `VerifierAnchors` itself, which is exactly the injection
ruling O5 allows and exactly the injection this file never performs.

This script installs nothing. Creating or updating
`/etc/rquant/signal-family-verifier-policy-v1.json`, the fixed harness, or
`/var/lib/rquant/signal-family-verification` is a separate root infrastructure
transaction that needs its own explicit user authorization (authority.md L1389-1395,
L1621-1626). Running this verifier against anchors that do not already exist fails closed.
"""

from __future__ import annotations

import json
import pwd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rquant.signal_family_root_verifier import (  # noqa: E402
    PRODUCTION_HARNESS_PATH,
    PRODUCTION_OWNER_GID,
    PRODUCTION_OWNER_UID,
    PRODUCTION_POLICY_PATH,
    PRODUCTION_POLICY_TRUSTED_ROOT,
    PRODUCTION_STORE_ROOT,
    ProductionRuntimeAuthorityGateway,
    RootVerifier,
    SignalFamilyRootVerifierError,
    VerifierAnchors,
)

SUBCOMMANDS: tuple[str, ...] = ("verify", "revoke", "rollback")
UNPRIVILEGED_CHILD_ACCOUNT = "lighthouse"


def production_anchors(*, child_uid: int, child_gid: int) -> VerifierAnchors:
    """The four fixed anchors, hardcoded. Nothing overrides them."""

    return VerifierAnchors(
        policy_trusted_root=PRODUCTION_POLICY_TRUSTED_ROOT,
        policy_path=PRODUCTION_POLICY_PATH,
        harness_path=PRODUCTION_HARNESS_PATH,
        store_root=PRODUCTION_STORE_ROOT,
        expected_owner_uid=PRODUCTION_OWNER_UID,
        expected_owner_gid=PRODUCTION_OWNER_GID,
        child_uid=child_uid,
        child_gid=child_gid,
    )


def build_verifier() -> RootVerifier:
    account = pwd.getpwnam(UNPRIVILEGED_CHILD_ACCOUNT)
    return RootVerifier(
        anchors=production_anchors(child_uid=account.pw_uid, child_gid=account.pw_gid),
        authority_gateway=ProductionRuntimeAuthorityGateway(),
    )


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in SUBCOMMANDS:
        sys.stderr.write(f"usage: signal-family-root-verifier.py {{{'|'.join(SUBCOMMANDS)}}}\n")
        return 2
    command = argv[1]
    verifier = build_verifier()
    try:
        if command == "verify":
            result = verifier.run()
            outcome = {
                "command": command,
                "outcome": result.outcome,
                "state": result.state.value,
                "overlay_content_hash": result.decision.overlay_content_hash,
                "authority_epoch_key": result.decision.authority_epoch_key,
                "decision_hash": result.decision.decision_hash,
                "receipt_fingerprints": list(result.decision.receipt_fingerprints),
            }
        else:
            if len(argv) != 4:
                sys.stderr.write(
                    f"usage: signal-family-root-verifier.py {command} "
                    "<overlay_content_hash> <authority_epoch_key>\n"
                )
                return 2
            transition = verifier.revoke if command == "revoke" else verifier.rollback
            state = transition(
                overlay_content_hash=argv[2],
                authority_epoch_key=argv[3],
            )
            outcome = {
                "command": command,
                "state": state.value,
                "overlay_content_hash": argv[2],
                "authority_epoch_key": argv[3],
            }
    except SignalFamilyRootVerifierError as error:
        sys.stdout.write(
            json.dumps(
                {"command": command, "outcome": "rejected", "reason_code": error.reason_code.value},
                sort_keys=True,
            )
            + "\n"
        )
        return 1
    sys.stdout.write(json.dumps(outcome, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
