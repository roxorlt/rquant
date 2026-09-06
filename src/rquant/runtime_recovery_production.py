"""The profile-bound production recovery pass, separated from the CLI module.

`runtime_recovery_service.main()` used to reach this function through `rquant.cli`, and
importing `rquant.cli` pulls in `rquant.logging` and `rquant.storage.duckdb`, which read
the process settings. The wrapper starts a role child with `LANG` / `LC_ALL` / `TZ` and
nothing else, so that import chain made the two recovery roles die on the first statement
of their entry point — before any recovery policy was even read.

Nothing here is new behaviour: the body is the one that lived at
`cli.cmd_runtime_recovery_production`, moved as is, with one import pushed past the profile
load (see the comment at that import). The three helpers it still needs from the CLI module
are imported at call time, after the trusted profile has been loaded, so a child that has
no current deployment fails on the profile rather than on an import. Resolving them through
the module at call time also keeps them monkeypatchable as before.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

#: The setting the profile's own `artifact_retention` manifest carries for the recovery
#: block it was generated beside (`runtime_production_profile`). It is a value in the
#: *recovery* namespace — the same content hash `RuntimeRecoveryProductionConfig` recomputes
#: on every load — and it is the only copy of that hash the deployment bundle holds outside
#: the recovery block itself, which makes it the one thing that can disagree with it.
RECOVERY_GENERATION_SETTING = "recovery_profile_generation"


def bundle_recovery_profile_generation(profile: object) -> str:
    """The recovery profile generation the installed bundle records for this deployment.

    `runtime_production_profile` writes it into the retention owner's manifest settings, and
    `_validate_production_profile` insists there is exactly one retention owner. The
    manifest is hashed into `generation-basis.json`, whose digest names the legacy
    generation directory, so this value is bound to the same bundle as the profile the
    caller loaded — unlike `--expected-generation`, which belongs to the authority chain.
    """

    declared = {
        str(value)
        for manifest in getattr(profile, "manifests", ())
        for value in (dict(getattr(manifest, "settings", {})).get(RECOVERY_GENERATION_SETTING),)
        if value is not None
    }
    if len(declared) != 1:
        raise ValueError("current runtime profile records no single recovery profile generation")
    return declared.pop()


def _require_recovery_generation_binding(
    profile: object,
    *,
    runtime_root: Path,
    manifest_path: Path | None,
    expected_generation: str | None,
    expected_profile_generation: str | None,
) -> None:
    """Bind the recovery pass to this deployment without comparing two id namespaces.

    The unit used to compare `recovery.profile_generation` — the content hash of the
    recovery block, recomputed by `RuntimeRecoveryProductionConfig.validate_identity_and_policy`
    on every load — against the wrapper's `--expected-generation`, which is
    `sha256(<generation>/full-manifest.json)` off the root-owned authority chain. The two
    are different hashes of different documents and never agree by construction, so both
    recovery oneshots failed closed with `recovery unit profile generation is stale` as soon
    as Route A gave the host a real `current` (#218 B). It is the same defect as #187/#207,
    whose fix landed in `runtime_service_main` only.

    Nothing is dropped in exchange. Two checks replace the one that could not pass:

    * the **recovery namespace**, against the retention owner's
      `recovery_profile_generation` — the bundle's own record of the same hash, produced by
      the generator from the same recovery block and hashed into the generation basis. A
      profile whose recovery block was swapped without regenerating the profile fails here;
    * the **authority namespace**, through `resolve_legacy_schema_generation` — the manifest
      has to sit in `<expected_generation>/manifests/`, and that generation's
      `legacy-binding.json` has to name this runtime root and the generation `current`
      currently resolves to. That is what says the profile just loaded belongs to the
      generation the chain slot was staged from.

    The manual `rquant runtime-recovery-production` path has no wrapper argv, so it pins the
    recovery namespace with its own `--expected-profile-generation` instead. A caller that
    supplies neither is refused: there is no "run without a binding" mode.
    """

    recovery = profile.recovery  # type: ignore[attr-defined]
    generation = str(recovery.profile_generation)
    if generation != bundle_recovery_profile_generation(profile):
        raise ValueError("recovery unit profile generation is stale")
    if manifest_path is None and expected_profile_generation is None:
        raise ValueError("recovery unit was given no generation to bind against")
    if expected_profile_generation is not None and generation != str(expected_profile_generation):
        raise ValueError("recovery unit profile generation is stale")
    if manifest_path is None:
        return
    if expected_generation is None:
        raise ValueError("recovery unit manifest was given without its authority generation")
    # Imported here rather than at module scope for the reason the module docstring gives:
    # a role child started from a three-name environment must fail on its profile, not on an
    # import. `runtime_service_main` is the entrypoint every other role already loads, so it
    # reads no settings while importing.
    from rquant.runtime_service_main import (
        legacy_current_generation,
        resolve_legacy_schema_generation,
    )

    legacy_generation = legacy_current_generation(runtime_root)
    if legacy_generation is None:
        raise ValueError("recovery unit requires a current legacy deployment")
    resolve_legacy_schema_generation(
        Path(manifest_path),
        expected_generation=str(expected_generation),
        runtime_root=runtime_root,
        legacy_generation=legacy_generation,
    )


def cmd_runtime_recovery_production(args: argparse.Namespace) -> int:
    """Run recovery using only the current trusted production profile."""

    from rquant.runtime_deployment_profile import (
        load_current_runtime_deployment_profile,
        validate_runtime_recovery_backup_config,
    )

    runtime_root = Path(args.runtime_root)
    profile = load_current_runtime_deployment_profile(runtime_root)
    recovery = profile.recovery
    if recovery is None or recovery.profile_generation is None:
        raise ValueError("current runtime profile has no recovery production configuration")
    manifest_path = getattr(args, "manifest", None)
    _require_recovery_generation_binding(
        profile,
        runtime_root=runtime_root,
        manifest_path=None if manifest_path is None else Path(manifest_path),
        expected_generation=getattr(args, "expected_generation", None),
        expected_profile_generation=getattr(args, "expected_profile_generation", None),
    )
    # Imported after the profile is loaded, not before: `runtime_recovery_backup` reaches
    # `dashboard/strategy_lab_runs.py` through a module-level fingerprint computation in
    # `runtime_recovery_coordinator`, and that module reads the process settings while it is
    # imported. A child with no current deployment therefore fails on the missing profile,
    # which is the failure the operator can act on; the import-time settings read is a
    # separate defect outside this module (PA-1 report, finding F-1).
    from rquant.runtime_recovery_backup import load_recovery_backup_config

    backup_config = load_recovery_backup_config(recovery.backup_config_path)
    validate_runtime_recovery_backup_config(profile, backup_config)
    arguments = dict(recovery.recovery_service_arguments())
    required = {
        "publication_root",
        "state_path",
        "receipt_root",
        "restore_root",
        "credential_file",
        "lease_seconds",
        "max_attempts",
        "retry_delay_seconds",
        "deadline_seconds",
        "rehearsal_interval_seconds",
    }
    if set(arguments) != required:
        raise ValueError("current recovery profile service arguments are incomplete")
    action = str(args.production_recovery_action)
    if action not in {"execute", "rehearse"}:  # pragma: no cover - argparse guards this
        raise ValueError("unknown production recovery action")
    rehearsal_interval = int(arguments["rehearsal_interval_seconds"])
    if action == "rehearse":
        from rquant.cli import _runtime_recovery_rehearsal_due, _utc_now

        due, last_successful, next_due = _runtime_recovery_rehearsal_due(
            state_path=Path(arguments["state_path"]),
            receipt_root=Path(arguments["receipt_root"]),
            interval_seconds=rehearsal_interval,
            now=_utc_now(),
        )
        if not due:
            print(
                json.dumps(
                    {
                        "last_successful_at": (
                            None if last_successful is None else last_successful.isoformat()
                        ),
                        "next_due_at": None if next_due is None else next_due.isoformat(),
                        "profile_generation": recovery.profile_generation,
                        "reason": "rehearsal_not_due",
                        "status": "skipped",
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            return 0
    from rquant.cli import cmd_runtime_recovery

    return cmd_runtime_recovery(
        argparse.Namespace(
            recovery_action="execute",
            publication_root=Path(arguments["publication_root"]),
            state_path=Path(arguments["state_path"]),
            receipt_root=Path(arguments["receipt_root"]),
            restore_root=Path(arguments["restore_root"]),
            credential_file=Path(arguments["credential_file"]),
            lease_seconds=int(arguments["lease_seconds"]),
            max_attempts=int(arguments["max_attempts"]),
            retry_delay_seconds=int(arguments["retry_delay_seconds"]),
            deadline_seconds=int(arguments["deadline_seconds"]),
            schedule_cycle_seconds=(None if action == "execute" else rehearsal_interval),
            worker_id=(f"runtime-recovery-{action}-{recovery.profile_generation[:12]}"),
            accept_current_plan=True,
            plan_id=None,
        )
    )


__all__ = [
    "RECOVERY_GENERATION_SETTING",
    "bundle_recovery_profile_generation",
    "cmd_runtime_recovery_production",
]
