"""The verifier's three subcommands, run only after the artifact tree has been verified.

This is the code that used to live in `scripts/signal-family-root-verifier.py`, minus the
`sys.path.insert` that put a mutable checkout ahead of everything else (Codex round-2 P1-4).
The four anchors of `authority.md` L1280-1291 and L1401-1405 are still written here as
literals and nowhere else: no environment variable, flag or configuration file moves the
policy, the harness or the store. The offline suite reaches those locations only by
constructing `VerifierAnchors` itself, which is the injection ruling O5 allows and the one
this module never performs.

Importing this module imports `rquant.signal_family_root_verifier`, so it must not be
imported until the caller has bound the verified tree's site-packages onto `sys.path`.
"""

from __future__ import annotations

import json
import pwd
import sys
from collections.abc import Sequence

SUBCOMMANDS: tuple[str, ...] = ("verify", "revoke", "rollback")
UNPRIVILEGED_CHILD_ACCOUNT = "lighthouse"


def production_anchors(*, child_uid: int, child_gid: int):  # type: ignore[no-untyped-def]
    """The fixed anchors, hardcoded. Nothing overrides them."""

    from rquant.signal_family_root_verifier import (
        PRODUCTION_CHILD_WORKSPACE_ROOT,
        PRODUCTION_HARNESS_PATH,
        PRODUCTION_OWNER_GID,
        PRODUCTION_OWNER_UID,
        PRODUCTION_POLICY_PATH,
        PRODUCTION_POLICY_TRUSTED_ROOT,
        PRODUCTION_PRIVILEGE_LAUNCHER,
        PRODUCTION_STORE_ROOT,
        VerifierAnchors,
    )

    return VerifierAnchors(
        policy_trusted_root=PRODUCTION_POLICY_TRUSTED_ROOT,
        policy_path=PRODUCTION_POLICY_PATH,
        harness_path=PRODUCTION_HARNESS_PATH,
        store_root=PRODUCTION_STORE_ROOT,
        child_workspace_root=PRODUCTION_CHILD_WORKSPACE_ROOT,
        expected_owner_uid=PRODUCTION_OWNER_UID,
        expected_owner_gid=PRODUCTION_OWNER_GID,
        child_uid=child_uid,
        child_gid=child_gid,
        privilege_launcher_path=PRODUCTION_PRIVILEGE_LAUNCHER,
    )


def build_verifier():  # type: ignore[no-untyped-def]
    from rquant.signal_family_root_verifier import (
        ProductionRuntimeAuthorityGateway,
        RootVerifier,
    )

    account = pwd.getpwnam(UNPRIVILEGED_CHILD_ACCOUNT)
    return RootVerifier(
        anchors=production_anchors(child_uid=account.pw_uid, child_gid=account.pw_gid),
        authority_gateway=ProductionRuntimeAuthorityGateway(),
    )


def main(argv: Sequence[str]) -> int:
    from rquant.signal_family_root_verifier import SignalFamilyRootVerifierError

    arguments = list(argv)
    if len(arguments) < 2 or arguments[1] not in SUBCOMMANDS:
        sys.stderr.write(
            f"usage: rquant-signal-family-verifier-v1.pyz {{{'|'.join(SUBCOMMANDS)}}}\n"
        )
        return 2
    command = arguments[1]
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
            if len(arguments) != 4:
                sys.stderr.write(
                    f"usage: rquant-signal-family-verifier-v1.pyz {command} "
                    "<overlay_content_hash> <authority_epoch_key>\n"
                )
                return 2
            transition = verifier.revoke if command == "revoke" else verifier.rollback
            state = transition(
                overlay_content_hash=arguments[2],
                authority_epoch_key=arguments[3],
            )
            outcome = {
                "command": command,
                "state": state.value,
                "overlay_content_hash": arguments[2],
                "authority_epoch_key": arguments[3],
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
