"""#218 B: what the recovery oneshots compare their profile generation against.

`runtime_recovery_production` used to compare `recovery.profile_generation` — the content
hash of the profile's recovery block, recomputed on every load — with the wrapper's
`--expected-generation`, which is `sha256(<generation>/full-manifest.json)` off the
root-owned authority chain. Two documents, two hashes, never equal by construction, so both
`rquant-runtime-recovery@` and `rquant-runtime-recovery-rehearsal@` failed closed with
`recovery unit profile generation is stale` the moment Route A gave the host a real
`current`. Same defect as #187/#207, whose fix reached `runtime_service_main` only.

These are the unit-level cases. The acceptance over a real installed bundle and a real
staged generation is `tests/integration/test_route_a_recovery_binding_e2e.py`.
"""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.runtime_recovery_production import (
    RECOVERY_GENERATION_SETTING,
    bundle_recovery_profile_generation,
    cmd_runtime_recovery_production,
)

AUTHORITY_GENERATION = "a" * 64
RECOVERY_GENERATION = "b" * 64
OTHER_GENERATION = "c" * 64
COMMIT = "d" * 40
INSTANCE = "svc-" + "e" * 64


def _profile(
    *,
    recovery_generation: str | None = RECOVERY_GENERATION,
    bundle_generation: str | None = RECOVERY_GENERATION,
) -> SimpleNamespace:
    manifests: list[SimpleNamespace] = [SimpleNamespace(settings={"managed_root": "/x"})]
    if bundle_generation is not None:
        manifests.append(
            SimpleNamespace(settings={RECOVERY_GENERATION_SETTING: bundle_generation})
        )
    return SimpleNamespace(
        recovery=SimpleNamespace(
            profile_generation=recovery_generation,
            backup_config_path=Path("/nowhere/backup-config.json"),
            recovery_service_arguments=lambda: {},
        ),
        manifests=tuple(manifests),
    )


def _bind_profile(monkeypatch: pytest.MonkeyPatch, profile: object) -> None:
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda _root: profile,
    )
    monkeypatch.setattr(
        "rquant.cli.cmd_runtime_recovery",
        lambda _args: pytest.fail("the binding must be settled before recovery runs"),
    )


# ---------------------------------------------------------------------------------------
# The entry point no longer forwards the authority id under the recovery namespace's name
# ---------------------------------------------------------------------------------------


def test_the_role_entry_point_forwards_both_halves_under_their_own_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bug in one assertion: `--expected-generation` is the authority id, and it must
    reach the pass as the authority id, not as the recovery block's content hash."""

    import rquant.runtime_recovery_production as production
    import rquant.runtime_recovery_service as recovery_service

    runtime_root = tmp_path / "runtime"
    manifest = tmp_path / "generations" / AUTHORITY_GENERATION / "manifests" / f"{INSTANCE}.json"
    observed: dict[str, object] = {}

    def record(args: Namespace) -> int:
        observed.update(vars(args))
        return 0

    monkeypatch.setattr(production, "cmd_runtime_recovery_production", record)

    code = recovery_service.main(
        [
            "--manifest",
            str(manifest),
            "--control-root",
            str(runtime_root / "control" / "recovery" / INSTANCE),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            AUTHORITY_GENERATION,
            "--mode",
            "rehearse",
        ]
    )

    assert code == 0
    assert observed["runtime_root"] == runtime_root
    assert observed["manifest"] == manifest
    assert observed["expected_generation"] == AUTHORITY_GENERATION
    assert observed["expected_profile_generation"] is None
    assert observed["production_recovery_action"] == "rehearse"


# ---------------------------------------------------------------------------------------
# The recovery namespace half
# ---------------------------------------------------------------------------------------


def test_the_bundle_record_is_read_from_the_retention_owner_manifest() -> None:
    assert bundle_recovery_profile_generation(_profile()) == RECOVERY_GENERATION


def test_a_recovery_block_that_disagrees_with_the_bundle_record_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The check that replaces the impossible one, and it is not vacuous: a recovery block
    swapped without regenerating the profile no longer matches the retention manifest."""

    _bind_profile(
        monkeypatch,
        _profile(recovery_generation=RECOVERY_GENERATION, bundle_generation=OTHER_GENERATION),
    )

    with pytest.raises(ValueError, match="profile generation is stale"):
        cmd_runtime_recovery_production(
            Namespace(
                runtime_root=tmp_path / "runtime",
                manifest=None,
                expected_generation=None,
                expected_profile_generation=RECOVERY_GENERATION,
                production_recovery_action="execute",
            )
        )


def test_a_profile_with_no_bundle_record_is_refused_rather_than_assumed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_profile(monkeypatch, _profile(bundle_generation=None))

    with pytest.raises(ValueError, match="no single recovery profile generation"):
        cmd_runtime_recovery_production(
            Namespace(
                runtime_root=tmp_path / "runtime",
                manifest=None,
                expected_generation=None,
                expected_profile_generation=RECOVERY_GENERATION,
                production_recovery_action="execute",
            )
        )


def test_two_manifests_declaring_different_recovery_generations_are_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    profile.manifests = (
        *profile.manifests,
        SimpleNamespace(settings={RECOVERY_GENERATION_SETTING: OTHER_GENERATION}),
    )
    _bind_profile(monkeypatch, profile)

    with pytest.raises(ValueError, match="no single recovery profile generation"):
        cmd_runtime_recovery_production(
            Namespace(
                runtime_root=tmp_path / "runtime",
                manifest=None,
                expected_generation=None,
                expected_profile_generation=RECOVERY_GENERATION,
                production_recovery_action="execute",
            )
        )


# ---------------------------------------------------------------------------------------
# No "run without a binding" mode
# ---------------------------------------------------------------------------------------


def test_a_caller_that_names_no_generation_at_all_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropping the impossible comparison must not become dropping the check."""

    _bind_profile(monkeypatch, _profile())

    with pytest.raises(ValueError, match="no generation to bind against"):
        cmd_runtime_recovery_production(
            Namespace(
                runtime_root=tmp_path / "runtime",
                manifest=None,
                expected_generation=None,
                expected_profile_generation=None,
                production_recovery_action="execute",
            )
        )


def test_a_manifest_without_its_authority_generation_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_profile(monkeypatch, _profile())

    with pytest.raises(ValueError, match="without its authority generation"):
        cmd_runtime_recovery_production(
            Namespace(
                runtime_root=tmp_path / "runtime",
                manifest=tmp_path / "manifests" / f"{INSTANCE}.json",
                expected_generation=None,
                expected_profile_generation=None,
                production_recovery_action="execute",
            )
        )


def test_a_runtime_root_with_no_current_pointer_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route B has no legacy deployment, and a recovery pass has nothing to bind to there."""

    runtime_root = tmp_path / "runtime"
    manifest = tmp_path / "generations" / AUTHORITY_GENERATION / "manifests" / f"{INSTANCE}.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")
    _bind_profile(monkeypatch, _profile())

    with pytest.raises(ValueError, match="requires a current legacy deployment"):
        cmd_runtime_recovery_production(
            Namespace(
                runtime_root=runtime_root,
                manifest=manifest,
                expected_generation=AUTHORITY_GENERATION,
                expected_profile_generation=None,
                production_recovery_action="execute",
            )
        )
