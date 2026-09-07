#!/usr/bin/env python3
"""Produce the two `data/recovery/` documents the recovery oneshots read.

`rquant-runtime-recovery@` and `rquant-runtime-recovery-rehearsal@` read two files whose
paths the deployment profile names and whose contents nothing in this repository has ever
written (`route-a-218-scout.md` §3.3: the only matches for either filename were two argparse
defaults and two test fixtures):

`runtime-recovery-backup.json` (0600)
    `runtime_recovery_backup.load_recovery_backup_config`, then bound to the installed
    profile by `runtime_deployment_profile.validate_runtime_recovery_backup_config`. Every
    field it compares comes from the profile, so this script derives the whole document from
    the profile the host has installed rather than asking the operator to copy hashes.
`runtime-recovery.json` (0600)
    `runtime_recovery_backup.RecoveryBackupAuthenticator.from_file`, the HMAC credential.
    Exactly `{"key_id", "secret_hex"}` in canonical JSON, `secret_hex` at least 32 bytes,
    `key_id` equal to the profile's `signer_key_id` (the generator's default is
    `production-recovery-v1`, and it is what `RQUANT_RECOVERY_SIGNER_KEY_ID` carries).

**Run this on the target host and nowhere else.** It mints a production HMAC secret with
`secrets.token_hex`, writes it 0600 at the path the profile names, and never prints it: the
summary carries the path, the key id and the byte length, never the value. There is no
`--print-secret` and there is no way to supply one, so no secret ever passes through a shell
history, an argv or a terminal. `--only-missing` makes the run idempotent: an existing
credential is verified and kept rather than rotated, because replacing it invalidates every
signature already in the publication root. A document that exists but is not a 0600 regular
file is **refused** under `--only-missing`, with the mode that was observed — replacing it
would be the silent rotation that flag exists to prevent.

Usage (the host layout of 82.156.0.68), with the checkout's own interpreter:

    /home/lighthouse/rquant/.venv/bin/python \\
        scripts/provision_runtime_recovery_credentials.py \\
        --runtime-root /home/lighthouse/rquant/data/runtime \\
        --replay-start-date 2026-07-01 \\
        --replay-end-date 2026-07-31 \\
        --only-missing

The replay window is the only judgement call: it is the date range
`build_runtime_recovery_fixed_replay_expectations` reproduces out of the published
production dataset, so it has to be a range that dataset actually covers. Everything else —
both roots, the target commit, the recovery profile generation, the signer key id, the two
named artifact roles, all twelve artifact role bindings and the deadline — is read out of
`<runtime root>/current/deployment-profile.json`, which is why this has to run after the
bundle is installed.

The run refuses unless the document it produced passes the same
`validate_runtime_recovery_backup_config` the recovery unit will run, so a document that
would fail on the host fails here instead.
"""

from __future__ import annotations

import argparse
import os
import secrets
import stat
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
if str(_REPOSITORY_ROOT / "src") not in sys.path:  # pragma: no cover - import bootstrap
    sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))

from rquant.strict_json import canonical_json_bytes  # noqa: E402

#: `RecoveryBackupAuthenticator.__init__` refuses a shorter secret. 32 bytes is the floor,
#: not the target: HMAC-SHA256's block is 64 bytes, so that is what is minted.
MINIMUM_SECRET_BYTES = 32
SECRET_BYTES = 64

#: Both documents are read by loaders that refuse any group or other bit.
PRIVATE_FILE_MODE = 0o600


class ProvisionError(RuntimeError):
    """The recovery documents cannot be produced from what the host has installed."""


# ---------------------------------------------------------------------------------------
# Filesystem
# ---------------------------------------------------------------------------------------


def write_private_document(path: Path, payload: bytes) -> None:
    """Write `payload` at `path`, 0600, atomically, refusing to follow a symlink.

    The mode is set on the descriptor before the rename, so the file is never briefly
    world-readable — which matters here more than for the other generated authorities,
    because one of these two files is a secret.
    """

    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ProvisionError(f"recovery document path must be absolute and normalized: {path}")
    parent = path.parent
    if not parent.is_dir():
        raise ProvisionError(f"recovery document directory does not exist: {parent}")
    staging = parent / f".{path.name}.staging"
    descriptor = os.open(
        staging,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
        PRIVATE_FILE_MODE,
    )
    try:
        os.fchmod(descriptor, PRIVATE_FILE_MODE)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(staging, path)
    directory = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _classify_existing_document(path: Path) -> tuple[str, int | None]:
    """`("absent", None)`, `("private", mode)` or `("insecure", mode)`.

    `--only-missing` has to tell the last two apart. A single "is it a private regular
    file?" predicate answers no to both a file that is not there and a file that is there
    with the wrong mode, which made `--only-missing` mint a new secret over a credential
    that was already signing (package F review, must-fix M-1).
    """

    try:
        observed = path.lstat()
    except FileNotFoundError:
        return "absent", None
    except OSError as exc:
        raise ProvisionError(f"recovery document is unreadable: {path}") from exc
    mode = stat.S_IMODE(observed.st_mode)
    if not stat.S_ISREG(observed.st_mode) or mode & 0o077:
        return "insecure", mode
    return "private", mode


def _should_write(path: Path, *, only_missing: bool) -> bool:
    """Whether the producer writes this path, refusing rather than replacing in doubt.

    Without `--only-missing` the operator has asked for a fresh document and gets one. With
    it, the only thing that may be replaced is a document that is not there: anything that
    exists but is not a 0600 regular file is refused with the mode that was observed, so the
    operator fixes the mode and runs again rather than discovering afterwards that the key
    every published receipt was signed with is gone.
    """

    if not only_missing:
        return True
    state, mode = _classify_existing_document(path)
    if state == "absent":
        return True
    if state == "private":
        return False
    if mode is None:  # pragma: no cover - `_classify_existing_document` always pairs them
        raise ProvisionError(f"recovery document {path} is unsafe and will not be replaced")
    raise ProvisionError(
        f"recovery document {path} already exists with mode 0o{mode:04o}, not 0o0600, "
        "and --only-missing will not replace it: restore the mode with "
        f"`chmod 0600 {path}` (or remove the file if it is meant to be regenerated) "
        "and run again"
    )


# ---------------------------------------------------------------------------------------
# The credential
# ---------------------------------------------------------------------------------------


def build_recovery_credential_document(key_id: str, *, secret_hex: str | None = None) -> bytes:
    """The canonical `{"key_id", "secret_hex"}` document, with a freshly minted secret.

    `secret_hex` exists for the tests that need a deterministic document; nothing on the
    command line reaches it, so a run on the host always mints its own.
    """

    minted = secret_hex if secret_hex is not None else secrets.token_hex(SECRET_BYTES)
    if len(bytes.fromhex(minted)) < MINIMUM_SECRET_BYTES:
        raise ProvisionError("recovery credential secret must contain at least 32 bytes")
    return canonical_json_bytes({"key_id": key_id, "secret_hex": minted})


def provision_recovery_credential(
    path: Path,
    *,
    key_id: str,
    only_missing: bool,
    secret_hex: str | None = None,
) -> dict[str, object]:
    """Write, or keep and verify, the HMAC credential the backup signer loads."""

    from rquant.runtime_recovery_backup import RecoveryBackupAuthenticator

    created = _should_write(path, only_missing=only_missing)
    if created:
        document = build_recovery_credential_document(key_id, secret_hex=secret_hex)
        write_private_document(path, document)
    authenticator = RecoveryBackupAuthenticator.from_file(path)
    if authenticator.key_id != key_id:
        raise ProvisionError(
            f"recovery credential at {path} carries key id {authenticator.key_id!r}, "
            f"but the installed profile names {key_id!r}"
        )
    return {"path": str(path), "key_id": key_id, "created": created}


# ---------------------------------------------------------------------------------------
# The backup config
# ---------------------------------------------------------------------------------------


def build_recovery_backup_config(
    profile: Any,
    *,
    as_of: datetime,
    replay_start_date: date,
    replay_end_date: date,
) -> Any:
    """Derive the whole producer config from the profile the host has installed.

    `validate_runtime_recovery_backup_config` compares nine scalars and the complete set of
    twelve artifact role bindings against the profile's recovery block, so every one of them
    is taken from that block rather than restated. `verifier_commit` is the profile's
    producer commit because the code that will verify a restore is the code the profile was
    built from. The per-artifact `generation_id` is a label carried through into the
    published records; it is derived from the recovery generation so two runs against the
    same profile produce the same document.
    """

    from rquant.runtime_definition_bootstrap import plan_builtin_definitions
    from rquant.runtime_production_profile import ProductionStrategyBinding
    from rquant.runtime_recovery_artifacts import RealRecoveryArtifactSpec
    from rquant.runtime_recovery_backup import RecoveryBackupConfig

    recovery = profile.recovery
    if recovery is None or recovery.profile_generation is None:
        raise ProvisionError("the installed profile has no recovery production configuration")
    generation = str(recovery.profile_generation)
    artifacts = tuple(
        RealRecoveryArtifactSpec(
            logical_role=role.logical_role,
            kind=role.kind,
            source_path=role.source_path,
            restore_path=role.restore_path,
            generation_id=f"{role.logical_role}@{generation}",
            schema_version=role.schema_version,
            relations=role.relations,
            references=role.references,
        )
        for role in recovery.artifact_roles
    )
    bindings = tuple(
        ProductionStrategyBinding.model_validate(binding.model_dump(mode="python"))
        for binding in plan_builtin_definitions(
            producer_commit=profile.producer_commit
        ).strategies
    )
    return RecoveryBackupConfig(
        source_root=recovery.backup_source_root,
        publication_root=recovery.backup_publication_root,
        target_commit=profile.producer_commit,
        target_profile_generation=generation,
        verifier_commit=profile.producer_commit,
        signer_key_id=recovery.signer_key_id,
        as_of=as_of,
        replay_start_date=replay_start_date,
        replay_end_date=replay_end_date,
        production_artifact_role=recovery.production_artifact_role,
        paper_ledger_artifact_role=recovery.paper_ledger_artifact_role,
        strategy_bindings=bindings,
        artifacts=artifacts,
        deadline_seconds=recovery.recovery_deadline_seconds,
    )


def provision_recovery_backup_config(
    profile: Any,
    *,
    as_of: datetime,
    replay_start_date: date,
    replay_end_date: date,
    only_missing: bool,
) -> dict[str, object]:
    """Write, or keep and verify, the producer config bound to the installed profile."""

    from rquant.runtime_deployment_profile import validate_runtime_recovery_backup_config
    from rquant.runtime_recovery_backup import load_recovery_backup_config

    path = Path(profile.recovery.backup_config_path)
    #: same rule as the credential even though this document is derived rather than secret:
    #: `--only-missing` means "keep what is there", and a run that quietly rewrote it would
    #: also change its `config_id`
    created = _should_write(path, only_missing=only_missing)
    if created:
        config = build_recovery_backup_config(
            profile,
            as_of=as_of,
            replay_start_date=replay_start_date,
            replay_end_date=replay_end_date,
        )
        write_private_document(path, canonical_json_bytes(config.model_dump(mode="json")))
    #: read it back through the loader the unit uses, then bind it the way the unit will
    loaded = load_recovery_backup_config(path)
    validate_runtime_recovery_backup_config(profile, loaded)
    return {
        "path": str(path),
        "config_id": loaded.config_id,
        "target_profile_generation": loaded.target_profile_generation,
        "artifact_roles": len(loaded.artifacts),
        "created": created,
    }


# ---------------------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------------------


def provision(
    runtime_root: Path,
    *,
    as_of: datetime,
    replay_start_date: date,
    replay_end_date: date,
    only_missing: bool = False,
) -> dict[str, object]:
    """Produce both documents from the profile `<runtime root>/current` names."""

    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile

    profile = load_current_runtime_deployment_profile(runtime_root)
    recovery = profile.recovery
    if recovery is None or recovery.profile_generation is None:
        raise ProvisionError("the installed profile has no recovery production configuration")
    backup = provision_recovery_backup_config(
        profile,
        as_of=as_of,
        replay_start_date=replay_start_date,
        replay_end_date=replay_end_date,
        only_missing=only_missing,
    )
    credential = provision_recovery_credential(
        Path(recovery.credential_file),
        key_id=recovery.signer_key_id,
        only_missing=only_missing,
    )
    return {
        "runtime_root": str(runtime_root),
        "recovery_profile_generation": str(recovery.profile_generation),
        "backup_config": backup,
        "credential": credential,
    }


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected an ISO 8601 timestamp, got {value!r}") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--as-of must carry a timezone offset")
    return parsed.astimezone(UTC)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Produce the recovery backup config and HMAC credential on the host",
    )
    parser.add_argument(
        "--runtime-root",
        default="/home/lighthouse/rquant/data/runtime",
        help="the runtime root whose `current` names the installed deployment profile",
    )
    parser.add_argument("--replay-start-date", type=_parse_date, required=True)
    parser.add_argument("--replay-end-date", type=_parse_date, required=True)
    parser.add_argument(
        "--as-of",
        type=_parse_timestamp,
        default=None,
        help="the config's own timestamp; defaults to now in UTC",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help=(
            "keep and verify a document that already exists instead of replacing it; "
            "rotating the credential invalidates every signature already published. "
            "A document that exists but is not a 0600 regular file is refused, not "
            "replaced — fix its mode and run again"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    import json

    arguments = build_argument_parser().parse_args(argv)
    try:
        summary = provision(
            Path(os.path.abspath(arguments.runtime_root)),
            as_of=arguments.as_of or datetime.now(UTC),
            replay_start_date=arguments.replay_start_date,
            replay_end_date=arguments.replay_end_date,
            only_missing=arguments.only_missing,
        )
    except (ProvisionError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main(sys.argv[1:]))
