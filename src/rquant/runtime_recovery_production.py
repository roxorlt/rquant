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
    if recovery.profile_generation != str(args.expected_profile_generation):
        raise ValueError("recovery unit profile generation is stale")
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


__all__ = ["cmd_runtime_recovery_production"]
