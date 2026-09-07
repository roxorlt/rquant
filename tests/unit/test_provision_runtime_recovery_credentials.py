"""#218 C: the producer for the two `data/recovery/` documents that had none.

`route-a-218-scout.md` §3.3 found that `runtime-recovery.json` and
`runtime-recovery-backup.json` appear in this repository only as two argparse defaults and
two test fixtures: no script, CLI or runbook step has ever written either, so the recovery
oneshots hit them the moment the generation binding stopped failing first.

Nothing here mints a credential outside `tmp_path`, and the deterministic cases pass their
own `secret_hex` rather than letting `secrets.token_hex` run — the one case that does mint a
real secret keeps it in a temporary directory and checks that it never leaves the file.
"""

from __future__ import annotations

import json
import stat
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from rquant.runtime_deployment_profile import validate_runtime_recovery_backup_config
from rquant.runtime_production_profile import build_production_runtime_profile
from rquant.runtime_recovery_backup import (
    RecoveryBackupAuthenticator,
    load_recovery_backup_config,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_runtime_production_profile import _inputs

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:  # pragma: no cover - import bootstrap
    sys.path.insert(0, str(_SCRIPTS))

from provision_runtime_recovery_credentials import (  # noqa: E402
    MINIMUM_SECRET_BYTES,
    ProvisionError,
    build_recovery_credential_document,
    provision_recovery_backup_config,
    provision_recovery_credential,
)

AS_OF = datetime(2026, 9, 7, 8, 0, tzinfo=UTC)
REPLAY_START = date(2026, 7, 1)
REPLAY_END = date(2026, 7, 31)


@pytest.fixture
def profile(tmp_path: Path):
    """A real production profile, and the two directories its recovery block names."""

    built = build_production_runtime_profile(_inputs(tmp_path))
    recovery = built.recovery
    assert recovery is not None
    Path(recovery.credential_file).parent.mkdir(parents=True, exist_ok=True)
    Path(recovery.backup_config_path).parent.mkdir(parents=True, exist_ok=True)
    return built


# ---------------------------------------------------------------------------------------
# The credential
# ---------------------------------------------------------------------------------------


def test_the_credential_is_the_document_the_authenticator_accepts(profile) -> None:
    """Exactly two fields, canonical bytes, 0600, and a secret the signer can use."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)

    summary = provision_recovery_credential(
        path, key_id=recovery.signer_key_id, only_missing=False
    )

    assert summary["created"] is True
    payload = path.read_bytes()
    decoded = json.loads(payload)
    assert set(decoded) == {"key_id", "secret_hex"}
    assert canonical_json_bytes(decoded) == payload
    assert decoded["key_id"] == recovery.signer_key_id == "production-recovery-v1"
    assert len(bytes.fromhex(decoded["secret_hex"])) >= MINIMUM_SECRET_BYTES
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    authenticator = RecoveryBackupAuthenticator.from_file(path)
    assert authenticator.key_id == recovery.signer_key_id
    assert authenticator.verify(b"payload", authenticator.sign(b"payload"))


def test_the_key_id_is_the_one_the_units_carry_in_their_environment(profile) -> None:
    """`RQUANT_RECOVERY_SIGNER_KEY_ID` and the credential have to name the same key."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)
    provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=False)

    environment = dict(recovery.backup_environment())
    assert json.loads(path.read_bytes())["key_id"] == (
        environment["RQUANT_RECOVERY_SIGNER_KEY_ID"]
    )


def test_the_secret_is_minted_fresh_on_every_unguarded_run(profile) -> None:
    recovery = profile.recovery
    path = Path(recovery.credential_file)

    provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=False)
    first = path.read_bytes()
    provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=False)

    assert path.read_bytes() != first


def test_only_missing_keeps_the_credential_that_is_already_signing(profile) -> None:
    """Rotating it would invalidate every signature already in the publication root."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)
    provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=False)
    first = path.read_bytes()

    summary = provision_recovery_credential(
        path, key_id=recovery.signer_key_id, only_missing=True
    )

    assert summary["created"] is False
    assert path.read_bytes() == first


def test_only_missing_refuses_a_credential_whose_mode_was_widened(profile) -> None:
    """The must-fix (package F review M-1): "not private" is not "not there".

    A credential that is already signing but whose mode someone widened to 0644 used to
    fail the single "is it a private regular file?" predicate, fall into the write branch,
    and be replaced by a freshly minted secret — silently, under the very flag that exists
    to stop that. It has to be refused, with the mode that was observed, so the operator
    restores the mode instead of discovering afterwards that every published signature is
    dead.
    """

    recovery = profile.recovery
    path = Path(recovery.credential_file)
    provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=False)
    before = path.read_bytes()
    path.chmod(0o644)

    with pytest.raises(ProvisionError) as raised:
        provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=True)

    message = str(raised.value)
    assert "0o0644" in message
    assert "0o0600" in message
    assert str(path) in message
    #: the secret is untouched, and the refusal did not print it
    assert path.read_bytes() == before
    assert json.loads(before)["secret_hex"] not in message


def test_only_missing_refuses_a_backup_config_whose_mode_was_widened(profile) -> None:
    """The derived document follows the same rule: `--only-missing` keeps or refuses."""

    path = Path(profile.recovery.backup_config_path)
    provision_recovery_backup_config(
        profile,
        as_of=AS_OF,
        replay_start_date=REPLAY_START,
        replay_end_date=REPLAY_END,
        only_missing=False,
    )
    before = path.read_bytes()
    path.chmod(0o644)

    with pytest.raises(ProvisionError, match="0o0644"):
        provision_recovery_backup_config(
            profile,
            as_of=AS_OF,
            replay_start_date=REPLAY_START,
            replay_end_date=REPLAY_END,
            only_missing=True,
        )

    assert path.read_bytes() == before


def test_only_missing_refuses_a_credential_path_that_is_not_a_regular_file(
    profile,
    tmp_path: Path,
) -> None:
    """A symlink parked at the credential path is not something to overwrite in silence."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)
    elsewhere = tmp_path / "elsewhere.json"
    elsewhere.write_bytes(b"{}")
    path.symlink_to(elsewhere)

    with pytest.raises(ProvisionError, match="will not replace it"):
        provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=True)

    assert path.is_symlink()
    assert elsewhere.read_bytes() == b"{}"


def test_without_only_missing_a_widened_credential_is_replaced_and_reprivatised(
    profile,
) -> None:
    """Refusing is `--only-missing`'s rule, not the producer's: an explicit run still writes."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)
    provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=False)
    before = path.read_bytes()
    path.chmod(0o644)

    summary = provision_recovery_credential(
        path, key_id=recovery.signer_key_id, only_missing=False
    )

    assert summary["created"] is True
    assert path.read_bytes() != before
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_a_kept_credential_naming_another_key_is_refused(profile) -> None:
    """`--only-missing` verifies what it keeps; a leftover from another key id is not it."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)
    provision_recovery_credential(path, key_id="production-recovery-v0", only_missing=False)

    with pytest.raises(ProvisionError, match="carries key id"):
        provision_recovery_credential(path, key_id=recovery.signer_key_id, only_missing=True)


def test_a_secret_below_the_authenticator_floor_is_refused() -> None:
    """`RecoveryBackupAuthenticator` refuses under 32 bytes, so the producer does too."""

    with pytest.raises(ProvisionError, match="at least 32 bytes"):
        build_recovery_credential_document("production-recovery-v1", secret_hex="ab" * 16)

    document = build_recovery_credential_document(
        "production-recovery-v1", secret_hex="ab" * MINIMUM_SECRET_BYTES
    )
    assert json.loads(document)["secret_hex"] == "ab" * MINIMUM_SECRET_BYTES


def test_the_minted_secret_never_reaches_the_summary(profile) -> None:
    """The value stays in the 0600 file: the summary carries the path and the key id."""

    recovery = profile.recovery
    path = Path(recovery.credential_file)

    summary = provision_recovery_credential(
        path, key_id=recovery.signer_key_id, only_missing=False
    )

    secret = json.loads(path.read_bytes())["secret_hex"]
    assert secret not in json.dumps(summary)
    assert set(summary) == {"path", "key_id", "created"}


# ---------------------------------------------------------------------------------------
# The backup config
# ---------------------------------------------------------------------------------------


def test_the_backup_config_binds_to_the_profile_that_produced_it(profile) -> None:
    """The document passes the same validation the recovery unit will run over it."""

    recovery = profile.recovery

    summary = provision_recovery_backup_config(
        profile,
        as_of=AS_OF,
        replay_start_date=REPLAY_START,
        replay_end_date=REPLAY_END,
        only_missing=False,
    )

    path = Path(recovery.backup_config_path)
    assert summary["created"] is True
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert summary["target_profile_generation"] == recovery.profile_generation
    assert summary["artifact_roles"] == len(recovery.artifact_roles)

    loaded = load_recovery_backup_config(path)
    validate_runtime_recovery_backup_config(profile, loaded)
    assert loaded.target_commit == profile.producer_commit
    assert loaded.verifier_commit == profile.producer_commit
    assert loaded.signer_key_id == recovery.signer_key_id
    assert loaded.deadline_seconds == recovery.recovery_deadline_seconds
    assert {binding.strategy_id for binding in loaded.strategy_bindings} == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }


def test_the_backup_config_is_deterministic_for_one_profile_and_timestamp(profile) -> None:
    """Rerunning the producer has to prove the document, not change it."""

    path = Path(profile.recovery.backup_config_path)
    provision_recovery_backup_config(
        profile,
        as_of=AS_OF,
        replay_start_date=REPLAY_START,
        replay_end_date=REPLAY_END,
        only_missing=False,
    )
    first = path.read_bytes()

    provision_recovery_backup_config(
        profile,
        as_of=AS_OF,
        replay_start_date=REPLAY_START,
        replay_end_date=REPLAY_END,
        only_missing=False,
    )

    assert path.read_bytes() == first


def test_only_missing_verifies_the_backup_config_it_keeps(profile) -> None:
    """A leftover document that no longer binds this profile is a refusal, not a skip."""

    recovery = profile.recovery
    path = Path(recovery.backup_config_path)
    provision_recovery_backup_config(
        profile,
        as_of=AS_OF,
        replay_start_date=REPLAY_START,
        replay_end_date=REPLAY_END,
        only_missing=False,
    )
    document = json.loads(path.read_bytes())
    document["target_profile_generation"] = "f" * 64
    document.pop("config_id", None)
    #: written back through the model so it stays a *valid* document — the point is that a
    #: well-formed config that no longer binds this profile is refused, not that a corrupt
    #: file is
    from rquant.runtime_recovery_backup import RecoveryBackupConfig

    tampered = RecoveryBackupConfig.model_validate(document)
    path.write_bytes(canonical_json_bytes(tampered.model_dump(mode="json")))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="differs from the production profile"):
        provision_recovery_backup_config(
            profile,
            as_of=AS_OF,
            replay_start_date=REPLAY_START,
            replay_end_date=REPLAY_END,
            only_missing=True,
        )
