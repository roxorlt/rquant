"""#192 (first half): every `ReadWritePaths=` entry of a first-gate unit carries `-`.

`systemd.exec(5)`: a path listed in `ReadWritePaths=` / `ReadOnlyPaths=` / `InaccessiblePaths=`
is ignored when it does not exist only if it is written with a leading `-`. Without the
prefix a missing path fails the unit while systemd is still building the mount namespace —
`226/NAMESPACE`, before the wrapper's first instruction — so the runtime roots that
`RuntimeServiceControl._prepare_directories` creates inside the service can never be
created by it on a first install. The same units already write `ReadOnlyPaths=` with the
prefix; this pins the missing half.

Scope of this round, on purpose: the 16 units the first gate starts (`acceptance-pra.md` §5,
mirrored by `test_runtime_authority_publish.FIRST_GATE_UNITS`). The other ten wrapper units
are not started in this window and stay in #192, as does that issue's `[Install]` half.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
RUNTIME_ROOT = "/home/lighthouse/rquant/data/runtime"
CONTROL_ROOT = f"{RUNTIME_ROOT}/control"
RETENTION_INSTANCE = "svc-248ba9b29fdc243fcd4f7d09641fbdedd61871ffeea693ea4eb26f36f264b349"
WRAPPER = "/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz --role"
ARBITER = "/usr/local/libexec/rquant-workload-arbiter research -- "

#: unit -> the paths its `ReadWritePaths=` grants, written without the `-` prefix.
#: Pinned literally so that a path edit is a failing test, not a silent deploy change.
FIRST_GATE_WRITABLE_PATHS: dict[str, tuple[str, ...]] = {
    "rquant-lab-claim-finalizer.service": (
        "/run/rquant-lab-claim-finalizer",
        f"{RUNTIME_ROOT}/research/lab-finalizer-state",
        f"{RUNTIME_ROOT}/research/claims",
        f"{RUNTIME_ROOT}/research/locks",
        f"{RUNTIME_ROOT}/research/readiness",
        f"{RUNTIME_ROOT}/research/lab_jobs.sqlite3",
        f"{RUNTIME_ROOT}/research/lab_jobs.sqlite3-wal",
        f"{RUNTIME_ROOT}/research/lab_jobs.sqlite3-shm",
    ),
    "rquant-runtime-artifact-catalog@.service": (
        f"{CONTROL_ROOT}/artifact-catalogs/%i",
        f"{RUNTIME_ROOT}/research/artifact-catalogs/%i",
        f"{RUNTIME_ROOT}/research/artifact-retention/{RETENTION_INSTANCE}"
        "/catalog-registration-outbox",
    ),
    "rquant-runtime-auction-universe@.service": (
        f"{CONTROL_ROOT}/auction-universe-publishers/%i",
        f"{RUNTIME_ROOT}/authorities/auction-universe",
    ),
    "rquant-runtime-candidate@.service": (
        f"{CONTROL_ROOT}/candidates/%i",
        f"{RUNTIME_ROOT}/live/candidates/%i",
    ),
    "rquant-runtime-daily-orchestrator@.service": (
        f"{CONTROL_ROOT}/daily-orchestrators/%i",
        f"{RUNTIME_ROOT}/research/daily-pipeline",
    ),
    "rquant-runtime-feature@.service": (
        f"{CONTROL_ROOT}/features/%i",
        f"{RUNTIME_ROOT}/live/features",
    ),
    "rquant-runtime-lab-jobs@.service": (
        f"{CONTROL_ROOT}/lab-jobs-publishers/%i",
        f"{RUNTIME_ROOT}/research/serving-authorities/lab-jobs",
    ),
    "rquant-runtime-paper-broker@.service": (
        f"{CONTROL_ROOT}/paper-brokers/%i",
        f"{RUNTIME_ROOT}/live/paper-brokers/%i",
    ),
    "rquant-runtime-paper-constraint@.service": (
        f"{CONTROL_ROOT}/paper-constraints/%i",
        f"{RUNTIME_ROOT}/authorities/paper-execution",
    ),
    "rquant-runtime-promotions@.service": (
        f"{CONTROL_ROOT}/promotions-publishers/%i",
        f"{RUNTIME_ROOT}/research/serving-authorities/promotions",
    ),
    "rquant-runtime-runtime-health@.service": (
        f"{CONTROL_ROOT}/runtime-health-publishers/%i",
        f"{CONTROL_ROOT}/authority-runtime-health",
    ),
    "rquant-runtime-serving@.service": (
        f"{CONTROL_ROOT}/serving-publishers/%i",
        f"{RUNTIME_ROOT}/serving",
    ),
    "rquant-runtime-shadow@.service": (
        f"{CONTROL_ROOT}/shadow-sessions/%i",
        f"{RUNTIME_ROOT}/research/shadow-reports",
    ),
    "rquant-runtime-signal-router@.service": (
        f"{CONTROL_ROOT}/signal-routers/%i",
        f"{RUNTIME_ROOT}/live/signal-bus",
    ),
    "rquant-runtime-strategy@.service": (
        f"{CONTROL_ROOT}/strategies/%i",
        f"{RUNTIME_ROOT}/live/strategies/%i",
    ),
    "rquant-runtime-watchlist-quote@.service": (
        f"{CONTROL_ROOT}/watchlist-quote-sources/%i",
        f"{RUNTIME_ROOT}/live/watchlist-quote",
    ),
}

#: unit -> (`Slice=`, `ExecStart=`). The prefix change must leave both untouched: this is
#: what makes the diff reviewable as "only `-` characters were added".
FIRST_GATE_EXECUTION: dict[str, tuple[str, str]] = {
    "rquant-lab-claim-finalizer.service": (
        "rquant-research.slice",
        f"{ARBITER}{WRAPPER} lab_claim_finalizer",
    ),
    "rquant-runtime-artifact-catalog@.service": (
        "rquant-research.slice",
        f"{ARBITER}{WRAPPER} lab_artifact_catalog --instance %i",
    ),
    "rquant-runtime-auction-universe@.service": (
        "rquant-live.slice",
        f"{WRAPPER} auction_universe_publisher --instance %i",
    ),
    "rquant-runtime-candidate@.service": (
        "rquant-live.slice",
        f"{WRAPPER} candidate_publisher --instance %i",
    ),
    "rquant-runtime-daily-orchestrator@.service": (
        "rquant-research.slice",
        f"{ARBITER}{WRAPPER} daily_pipeline_orchestrator --instance %i",
    ),
    "rquant-runtime-feature@.service": (
        "rquant-live.slice",
        f"{WRAPPER} feature_live --instance %i",
    ),
    "rquant-runtime-lab-jobs@.service": (
        "rquant-research.slice",
        f"{ARBITER}{WRAPPER} lab_jobs_publisher --instance %i",
    ),
    "rquant-runtime-paper-broker@.service": (
        "rquant-live.slice",
        f"{WRAPPER} paper_broker --instance %i",
    ),
    "rquant-runtime-paper-constraint@.service": (
        "rquant-live.slice",
        f"{WRAPPER} paper_constraint_publisher --instance %i",
    ),
    "rquant-runtime-promotions@.service": (
        "rquant-research.slice",
        f"{ARBITER}{WRAPPER} promotions_publisher --instance %i",
    ),
    "rquant-runtime-runtime-health@.service": (
        "rquant-serving.slice",
        f"{WRAPPER} runtime_health_publisher --instance %i",
    ),
    "rquant-runtime-serving@.service": (
        "rquant-serving.slice",
        f"{WRAPPER} serving_publisher --instance %i",
    ),
    "rquant-runtime-shadow@.service": (
        "rquant-research.slice",
        f"{ARBITER}{WRAPPER} shadow_session --instance %i",
    ),
    "rquant-runtime-signal-router@.service": (
        "rquant-live.slice",
        f"{WRAPPER} signal_router --instance %i",
    ),
    "rquant-runtime-strategy@.service": (
        "rquant-live.slice",
        f"{WRAPPER} strategy_live --instance %i",
    ),
    "rquant-runtime-watchlist-quote@.service": (
        "rquant-live.slice",
        f"{WRAPPER} watchlist_quote_source --instance %i",
    ),
}

FIRST_GATE_UNITS = tuple(sorted(FIRST_GATE_WRITABLE_PATHS))


def _directive(unit: str, key: str) -> list[str]:
    """Every physical `key=` line in the unit, values only.

    `configparser` would collapse a repeated key, and a repeat is exactly the shape this
    module has to notice: one prefixed line plus one unprefixed line still fails the unit.
    """

    lines = (SYSTEMD / unit).read_text(encoding="utf-8").splitlines()
    values = [line[len(key) + 1 :].strip() for line in lines if line.startswith(f"{key}=")]
    for value in values:
        assert not value.endswith("\\"), f"{unit}: {key} continues onto the next line"
    return values


def test_the_first_gate_covers_sixteen_wrapper_units() -> None:
    """`acceptance-pra.md` §5: 16 of the 26 protected units are started in the first gate.

    The authoritative list is `test_runtime_authority_publish.FIRST_GATE_UNITS`, which is what
    the A23 deploy-note test reads. Two copies of a list drift; this pins them to each other.
    """

    from tests.unit.test_runtime_authority_publish import FIRST_GATE_UNITS as AUTHORITATIVE

    assert set(FIRST_GATE_UNITS) == set(AUTHORITATIVE)
    assert len(FIRST_GATE_UNITS) == 16
    assert set(FIRST_GATE_EXECUTION) == set(FIRST_GATE_WRITABLE_PATHS)
    for unit in FIRST_GATE_UNITS:
        text = (SYSTEMD / unit).read_text(encoding="utf-8")
        assert (SYSTEMD / unit).is_file(), unit
        assert "/usr/local/libexec/rquant-runtime-exec.pyz" in text, unit


@pytest.mark.parametrize("unit", FIRST_GATE_UNITS)
def test_every_read_write_path_is_ignored_when_missing(unit: str) -> None:
    """The bug: without `-`, a missing path is `226/NAMESPACE` before the wrapper runs."""

    declarations = _directive(unit, "ReadWritePaths")

    assert len(declarations) == 1, f"{unit}: expected one ReadWritePaths= line"
    entries = declarations[0].split()
    assert entries, f"{unit}: ReadWritePaths= is empty"
    for entry in entries:
        assert entry.startswith("-/"), f"{unit}: {entry} is not ignored when missing"


@pytest.mark.parametrize("unit", FIRST_GATE_UNITS)
def test_the_prefix_is_the_only_change_to_the_granted_paths(unit: str) -> None:
    """Same grants as before, character for character once the `-` is taken off."""

    entries = _directive(unit, "ReadWritePaths")[0].split()

    assert tuple(entry.removeprefix("-") for entry in entries) == (
        FIRST_GATE_WRITABLE_PATHS[unit]
    )


@pytest.mark.parametrize("unit", FIRST_GATE_UNITS)
def test_exec_start_and_slice_are_untouched(unit: str) -> None:
    """A `ReadWritePaths=` edit must not carry an execution change in with it."""

    expected_slice, expected_exec_start = FIRST_GATE_EXECUTION[unit]

    assert _directive(unit, "Slice") == [expected_slice]
    assert _directive(unit, "ExecStart") == [expected_exec_start]
    assert _directive(unit, "ExecStartPre") == []
    assert _directive(unit, "User") == ["lighthouse"]
    assert _directive(unit, "Group") == ["lighthouse"]
