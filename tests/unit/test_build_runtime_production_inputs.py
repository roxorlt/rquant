"""The generator must produce documents every strict consumer already accepts.

Each case loads a generated file through the loader that will read it in production —
`load_production_runtime_profile_inputs`, `load_market_calendar_authority`,
`load_frozen_routing_policy`, `TrustedDescriptorSchemaResolver.from_authority`,
`load_candidate_input`, `_load_sse_open_dates` — rather than re-asserting the shapes the
generator itself chose. A generator checked against its own idea of the format is a
generator that agrees with itself.
"""

from __future__ import annotations

import hashlib
import stat
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

import scripts.build_runtime_production_inputs as generator
from rquant.runtime_builder_candidate import load_candidate_input
from rquant.runtime_builder_retention import TrustedDescriptorSchemaResolver
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_paper_quote import _load_sse_open_dates
from rquant.runtime_production_profile import (
    build_production_runtime_profile,
    load_production_runtime_profile_inputs,
)
from rquant.runtime_routing_policy import load_frozen_routing_policy
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

COMMIT = "a" * 40
SNAPSHOT_ID = "b" * 64
#: `read_sse_calendar` orders by `cal_date`; `updated_at` is what pins `generated_at`, so the
#: fixture gives every row the same instant and the generator's default is reproducible.
CALENDAR_UPDATED_AT = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)


def _write_calendar_database(path: Path, *, coverage_end: date = date(2028, 6, 30)) -> Path:
    """A `trade_calendar` table shaped like the production one: SSE rows, weekdays open."""

    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE trade_calendar (
                exchange      VARCHAR     NOT NULL,
                cal_date      DATE        NOT NULL,
                is_open       BOOLEAN     NOT NULL,
                pretrade_date DATE,
                source        VARCHAR     NOT NULL,
                updated_at    TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (exchange, cal_date)
            )
            """
        )
        current = date(2026, 1, 1)
        rows = []
        while current <= coverage_end:
            rows.append(
                (
                    "SSE",
                    current,
                    current.weekday() < 5,
                    None,
                    "fixture",
                    CALENDAR_UPDATED_AT,
                )
            )
            current += timedelta(days=1)
        connection.executemany(
            "INSERT INTO trade_calendar VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        connection.close()
    return path


def _argv(tmp_path: Path, **overrides: str) -> list[str]:
    data_root = tmp_path / "data"
    arguments = {
        "--checkout": str(generator._REPOSITORY_ROOT),
        "--producer-commit": COMMIT,
        "--calendar-database": str(tmp_path / "calendar.duckdb"),
        "--output-root": str(data_root / "runtime-inputs"),
        "--inputs-output": str(data_root / "runtime-production-inputs.json"),
        "--runtime-root": str(data_root / "runtime"),
        "--operational-database-path": str(data_root / "rquant.duckdb"),
        "--definition-registry-root": str(data_root / "runtime-inputs" / "definitions"),
        "--minutes-snapshot": str(data_root / "runtime-inputs" / "minute-history.parquet"),
        "--minutes-snapshot-sha256": SNAPSHOT_ID,
        "--runtime-mode": "local-test",
        "--recovery-backup-config": str(data_root / "recovery" / "backup.json"),
        "--recovery-credential-file": str(data_root / "recovery" / "credential.json"),
        "--shadow-report-active-key-id": "shadow-report-v1",
        "--shadow-report-active-public-key-pem": "shadow-report-public-test-key",
        "--shadow-completion-active-key-id": "shadow-completion-v1",
        "--shadow-completion-active-public-key-pem": "shadow-completion-public-test-key",
    }
    arguments.update(overrides)
    return [item for pair in arguments.items() for item in pair]


@pytest.fixture
def generated(tmp_path: Path) -> Path:
    _write_calendar_database(tmp_path / "calendar.duckdb")
    assert generator.main(_argv(tmp_path)) == 0
    return tmp_path / "data"


def test_the_inputs_document_loads_through_the_strict_production_loader(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
        expected_runtime_mode="local-test",
    )

    assert inputs.producer_commit == COMMIT
    assert inputs.market_calendar_authority_path == (
        generated / "runtime-inputs" / "market-calendar-authority.json"
    )
    assert inputs.artifact_location_id == generator.DEFAULT_ARTIFACT_LOCATION_ID
    assert inputs.artifact_failure_domain == generator.DEFAULT_ARTIFACT_FAILURE_DOMAIN


def test_the_document_builds_one_profile_whose_id_is_stable(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    first = build_production_runtime_profile(inputs)
    second = build_production_runtime_profile(inputs)

    assert first.profile_id is not None
    assert first.profile_id == second.profile_id
    assert len(first.manifests) == 26


def test_rerunning_the_generator_reproduces_every_byte(tmp_path: Path) -> None:
    """The operator reruns this to prove the host's document is the reviewed one."""

    _write_calendar_database(tmp_path / "calendar.duckdb")
    assert generator.main(_argv(tmp_path)) == 0
    first = {
        path.name: path.read_bytes()
        for path in sorted((tmp_path / "data" / "runtime-inputs").iterdir())
    }
    first["inputs"] = (tmp_path / "data" / "runtime-production-inputs.json").read_bytes()

    assert generator.main(_argv(tmp_path)) == 0
    second = {
        path.name: path.read_bytes()
        for path in sorted((tmp_path / "data" / "runtime-inputs").iterdir())
    }
    second["inputs"] = (tmp_path / "data" / "runtime-production-inputs.json").read_bytes()

    assert first == second


def test_every_generated_document_carries_the_mode_its_loader_demands(generated: Path) -> None:
    """0600 everywhere the loader wants a private file, 0444 where it refuses a write bit."""

    root = generated / "runtime-inputs"
    private = (
        "market-calendar-authority.json",
        "trade-calendar.json",
        "artifact-descriptor-schema.json",
        "n-shape-candidates.json",
        "growth-board-surge-candidates.json",
    )
    for name in private:
        assert stat.S_IMODE((root / name).stat().st_mode) == 0o600, name
    assert stat.S_IMODE((root / "signal-routing-policy.json").stat().st_mode) == 0o444
    assert stat.S_IMODE((generated / "runtime-production-inputs.json").stat().st_mode) == 0o600
    assert stat.S_IMODE(root.stat().st_mode) == 0o700


def test_the_market_calendar_loads_and_binds_its_recorded_content_hash(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    calendar = load_market_calendar_authority(
        inputs.market_calendar_authority_path,
        expected_commit=inputs.market_calendar_producer_commit,
    )

    assert calendar.content_sha256 == inputs.market_calendar_content_sha256
    assert calendar.coverage_end >= generator.DEFAULT_COVERAGE_FLOOR
    assert calendar.generated_at == CALENDAR_UPDATED_AT
    assert date(2026, 1, 2) in calendar.open_dates
    assert date(2026, 1, 3) not in calendar.open_dates


def test_the_pit_trade_calendar_loads_through_the_paper_broker_parser(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    open_dates = _load_sse_open_dates(inputs.trade_calendar_path, inputs.trade_calendar_sha256)

    calendar = load_market_calendar_authority(
        inputs.market_calendar_authority_path,
        expected_commit=COMMIT,
    )
    assert open_dates == calendar.open_dates


def test_the_routing_policy_loads_under_its_recorded_fingerprint(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    resolver = load_frozen_routing_policy(
        inputs.routing_policy_path,
        routing_policy_fingerprint=inputs.routing_policy_fingerprint,
        observed_at=datetime.now(UTC),
    )

    assert resolver.routing_policy_fingerprint == inputs.routing_policy_fingerprint
    assert resolver.default_no_target_reason == generator.DEFAULT_NO_TARGET_REASON
    decoded = strict_canonical_json_loads(inputs.routing_policy_path.read_bytes())
    assert isinstance(decoded, dict)
    assert len(decoded["rules"]) == 15
    assert {rule["recipient_id"] for rule in decoded["rules"]} == {"admin"}
    assert {rule["channel"] for rule in decoded["rules"]} == {"pushdeer"}


def test_the_retention_authority_loads_and_binds_nothing_reachable(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    resolver = TrustedDescriptorSchemaResolver.from_authority(
        root=inputs.artifact_retention_schema_authority_path.parent,
        path=inputs.artifact_retention_schema_authority_path,
        expected_sha256=inputs.artifact_retention_schema_authority_sha256,
    )

    # The single binding names a content hash no artifact can present: a zero-byte artifact
    # hashes to sha256(b""), which is the one pair a `size_bytes: 0` binding could match.
    assert resolver._bindings.keys() == {
        (hashlib.sha256(generator.RETENTION_SENTINEL_CONTENT).hexdigest(), 0)
    }
    assert hashlib.sha256(b"").hexdigest() not in {key[0] for key in resolver._bindings}


def test_both_sealed_candidate_documents_load_empty_for_their_strategy(generated: Path) -> None:
    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    n_shape = load_candidate_input(
        inputs.n_shape_candidate_input_path,
        strategy_id="n_shape",
        expected_commit=COMMIT,
    )
    growth_board = load_candidate_input(
        inputs.growth_board_candidate_input_path,
        strategy_id="growth_board_surge",
        expected_commit=COMMIT,
    )

    assert n_shape.facts == ()
    assert growth_board.facts == ()
    assert n_shape.authority.producer_commit == COMMIT
    # Two seals of the same emptiness still address different upstream authorities, so the
    # lineage `publish_candidate_batch` records cannot confuse one strategy for the other.
    assert n_shape.authority.authority_snapshot_id != (
        growth_board.authority.authority_snapshot_id
    )
    # 2026-01-01 is a Thursday the fixture marks open, and the newest open date at or before
    # the calendar's own `updated_at` (2026-09-01) is what both documents capture.
    assert n_shape.authority.trade_date == date(2026, 9, 1)
    assert n_shape.authority.captured_at.astimezone(generator.SHANGHAI).hour == 15


def test_a_sealed_document_is_refused_for_the_other_strategy(generated: Path) -> None:
    """Each seal is bound to its own strategy, so the two files cannot be swapped."""

    inputs = load_production_runtime_profile_inputs(
        generated / "runtime-production-inputs.json",
        expected_commit=COMMIT,
    )

    with pytest.raises(ValueError, match="batch kind does not match"):
        load_candidate_input(
            inputs.n_shape_candidate_input_path,
            strategy_id="growth_board_surge",
            expected_commit=COMMIT,
        )


def test_a_calendar_that_stops_before_the_coverage_floor_is_refused(tmp_path: Path) -> None:
    """Ruling 8: the calendar must reach at least 2027-12-31, and a short table says so."""

    _write_calendar_database(tmp_path / "calendar.duckdb", coverage_end=date(2027, 6, 30))

    assert generator.main(_argv(tmp_path)) == 2


def test_the_primary_duckdb_is_refused_without_an_explicit_override(tmp_path: Path) -> None:
    """CLAUDE.md's hard rule: readers open the replica, never the write-locked primary."""

    _write_calendar_database(tmp_path / "rquant.duckdb")

    assert (
        generator.main(_argv(tmp_path, **{"--calendar-database": str(tmp_path / "rquant.duckdb")}))
        == 2
    )


# --------------------------------------------------------------------------- S-1 / S-4
# The linux-production path: generated documents, the signed completion keyring the
# credential installers publish, the real strict loader, and the real builder.

import json  # noqa: E402
import subprocess  # noqa: E402

import rquant.runtime_artifact_terminal_lifecycle as terminal_lifecycle_module  # noqa: E402
import rquant.runtime_deployment_profile as deployment_profile_module  # noqa: E402
import rquant.runtime_production_profile as production_profile_module  # noqa: E402
from tests.unit.test_runtime_production_profile import (  # noqa: E402
    _daily_keyring_document,
    _daily_private_key,
    _mark_daily_keyring_root_owned,
)

SCRIPTS = generator._REPOSITORY_ROOT / "scripts"


def _install_credentials(prefix: Path) -> Path:
    """Run the two real installers into a test root and hand back the completion keyring.

    Nothing here is a stand-in: `install-runtime-credential-keys.sh` mints the completion
    key material and `install-runtime-credential-infra.sh` publishes the 0444 signed
    keyring, which is the file the production profile has to read fields 37-38 from.
    """

    prefix.mkdir(mode=0o755)
    for argv in (
        ["init", "--prefix", str(prefix)],
        None,
    ):
        if argv is None:
            result = subprocess.run(
                [
                    "/bin/bash",
                    str(SCRIPTS / "install-runtime-credential-infra.sh"),
                    "--test-root",
                    str(prefix),
                ],
                cwd=str(generator._REPOSITORY_ROOT),
                check=False,
                capture_output=True,
                text=True,
            )
        else:
            result = subprocess.run(
                ["/bin/bash", str(SCRIPTS / "install-runtime-credential-keys.sh"), *argv],
                cwd=str(generator._REPOSITORY_ROOT),
                check=False,
                capture_output=True,
                text=True,
            )
        assert result.returncode == 0, result.stdout + result.stderr
    return prefix / "etc" / "rquant" / "shadow-completion-trusted-keys.json"


def _linux_production_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], Path, Path]:
    """Everything a linux-production generator run needs, on a symlink-free test host.

    Returns the argv, the published completion keyring and the runtime root the frozen
    literal was pointed at.
    """

    # `/home` resolves through an autofs symlink on macOS and the bundle refuses a
    # symlinked ancestor of the runtime root, so the frozen literal points at a
    # symlink-free temporary host layout. The `runtime_mode` branch is what is under test.
    host_root = tmp_path / "host" / "rquant" / "data" / "runtime"
    for module, name in (
        (production_profile_module, "LINUX_PRODUCTION_RUNTIME_ROOT"),
        (deployment_profile_module, "LINUX_PRODUCTION_RUNTIME_ROOT"),
        (terminal_lifecycle_module, "_LINUX_PRODUCTION_RUNTIME_ROOT"),
    ):
        monkeypatch.setattr(module, name, host_root)

    completion_keyring = _install_credentials(tmp_path / "etcroot")
    _mark_daily_keyring_root_owned(monkeypatch, completion_keyring)

    private_key, public_key = _daily_private_key(tmp_path / "daily", key_id="daily-v1")
    daily_keyring = tmp_path / "etc" / "rquant" / "daily-receipt-trusted-keys.json"
    daily_keyring.parent.mkdir(parents=True, mode=0o700)
    daily_keyring.write_bytes(
        canonical_json_bytes(
            _daily_keyring_document(
                private_key,
                active_key_id="daily-v1",
                active_public_key=public_key,
            )
        )
    )
    daily_keyring.chmod(0o444)
    _mark_daily_keyring_root_owned(monkeypatch, daily_keyring)
    monkeypatch.setattr(
        production_profile_module,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        daily_keyring,
    )

    data_root = host_root.parent
    _write_calendar_database(tmp_path / "calendar.duckdb")
    argv = [
        "--producer-commit",
        COMMIT,
        "--calendar-database",
        str(tmp_path / "calendar.duckdb"),
        "--output-root",
        str(tmp_path / "inputs"),
        "--inputs-output",
        str(tmp_path / "runtime-production-inputs.json"),
        "--runtime-root",
        str(host_root),
        "--operational-database-path",
        str(data_root / "rquant.duckdb"),
        "--definition-registry-root",
        str(tmp_path / "inputs" / "definitions"),
        "--minutes-snapshot",
        str(tmp_path / "inputs" / "minute-history.parquet"),
        "--minutes-snapshot-sha256",
        SNAPSHOT_ID,
        "--runtime-mode",
        "linux-production",
        "--recovery-backup-config",
        str(data_root / "recovery" / "backup.json"),
        "--recovery-credential-file",
        str(data_root / "recovery" / "credential.json"),
        "--canvas-active-key-id",
        "canvas-v1",
        "--canvas-active-public-key-pem",
        "canvas-public-test-key",
        "--shadow-report-active-key-id",
        "shadow-report-v1",
        "--shadow-report-active-public-key-pem",
        "shadow-report-public-test-key",
        "--shadow-completion-keyring",
        str(completion_keyring),
    ]

    return argv, completion_keyring, host_root


def test_a_linux_production_document_loads_and_builds_with_the_published_keyring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generate, publish, read: the whole chain the host will walk.

    The completion public key is not passed in as a literal — it comes out of the 0444
    keyring the credential installers just published, read with the same strict rules the
    runtime applies to the Daily receipt keyring. Root ownership is the one host fact a
    test cannot arrange, so both keyrings borrow the production-profile suite's
    `os.stat`/`os.fstat` seam; every other rule runs for real.
    """

    argv, completion_keyring, host_root = _linux_production_argv(tmp_path, monkeypatch)

    assert generator.main(argv) == 0

    inputs = load_production_runtime_profile_inputs(
        tmp_path / "runtime-production-inputs.json",
        expected_commit=COMMIT,
        expected_runtime_mode="linux-production",
    )
    profile = build_production_runtime_profile(inputs)

    published = strict_canonical_json_loads(completion_keyring.read_bytes())
    assert isinstance(published, dict)
    assert inputs.shadow_completion_active_key_id == published["active_key_id"]
    # `RuntimeContractModel` sets `str_strip_whitespace`, so the contract holds the PEM
    # without the trailing newline `openssl pkey -pubout` leaves on it. The bytes the
    # keyring publishes are what the comparison starts from.
    assert inputs.shadow_completion_active_public_key_pem == (
        str(published["active_public_key"]).strip()
    )
    assert str(published["active_public_key"]).startswith("-----BEGIN PUBLIC KEY-----")
    assert inputs.runtime_root == host_root
    assert profile.profile_id is not None
    assert len(profile.manifests) == 26


def test_a_world_readable_completion_keyring_stops_the_generator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The strict rules are the point of reading it this way: 0444 exactly, root owned,
    signature verified. A lenient JSON read parses this same file happily, so the test
    goes through the generator's own wiring rather than calling the strict reader.

    A keyring anyone can rewrite must not get to decide what the production profile
    trusts.
    """

    argv, completion_keyring, _host_root = _linux_production_argv(tmp_path, monkeypatch)
    completion_keyring.chmod(0o644)

    assert generator.main(argv) == 2
    assert not (tmp_path / "runtime-production-inputs.json").exists()


def test_a_tampered_completion_keyring_stops_the_generator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """And the signature is checked, not just the mode: an edited public key is refused
    even though the file is still 0444 and still parses as JSON."""

    argv, completion_keyring, _host_root = _linux_production_argv(tmp_path, monkeypatch)
    document = json.loads(completion_keyring.read_bytes())
    document["active_key_id"] = "completion-tampered"
    completion_keyring.chmod(0o644)
    completion_keyring.write_bytes(canonical_json_bytes(document))
    completion_keyring.chmod(0o444)

    assert generator.main(argv) == 2


def test_the_strict_reader_refuses_a_world_readable_keyring_directly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion_keyring = _install_credentials(tmp_path / "etcroot")
    _mark_daily_keyring_root_owned(monkeypatch, completion_keyring)
    completion_keyring.chmod(0o644)

    with pytest.raises(generator.GeneratorError, match="signed trusted keyring is unusable"):
        generator.read_signed_public_keyring(completion_keyring)


def test_a_missing_completion_keyring_is_refused_rather_than_defaulted(
    tmp_path: Path,
) -> None:
    with pytest.raises(generator.GeneratorError, match="signed trusted keyring is unusable"):
        generator.read_signed_public_keyring(tmp_path / "absent-trusted-keys.json")


def test_linux_production_refuses_a_runtime_root_that_is_not_the_frozen_one(
    tmp_path: Path,
) -> None:
    """S-1: a pure literal comparison the host would make anyway, made before writing."""

    _write_calendar_database(tmp_path / "calendar.duckdb")

    exit_code = generator.main(
        _argv(
            tmp_path,
            **{
                "--runtime-mode": "linux-production",
                "--runtime-root": str(tmp_path / "not-the-production-root"),
            },
        )
    )

    assert exit_code == 2
    assert not (tmp_path / "data" / "runtime-production-inputs.json").exists()
