"""Owner ruling 2026-09-07: sixteen runtime units may write `control/schema-rollouts`.

#227 left one half of the schema rollout unreachable on the production host. Package G made
admission read the rollout state store read-only, so a role that only needs to read it comes
up; what it could not do is give a *participant* the write it needs. A producer named in a
plan appends its own PREPARE acknowledgement to that plan's hash chain, a consumer appends a
capability receipt, and an append needs a journal file created *beside* the database — a
directory permission, not a file one. Every runtime unit runs `ProtectSystem=strict` plus
`ProtectHome=read-only`, and no unit's `ReadWritePaths=` named the rollout root, so on the
host every participant failed closed.

The owner granted the narrow version of that: the sixteen units that really are participants
get `control/schema-rollouts`, the other seven keep zero access to it. This module is the
boundary. It pins three things a later edit must not move quietly:

* which sixteen — one missing unit and one extra unit are both a failing case here;
* that the grant is the *whole* rollout root and never one plan's directory — a `plan_id` is
  a content hash of the plan, so it changes every generation and a static unit file cannot
  name one (a per-plan grant is only reachable through generated drop-ins, which is not what
  the owner authorised);
* that adding it widened nothing else — each unit's remaining grants are pinned as the exact
  tuple it carried before the ruling.

Which service ids are participants is not a literal decision: it falls out of the plans
`install_runtime_deployment_profile` prepares over the real production profile. That
derivation is asserted against this list by
`tests/integration/test_route_a_rollout_acknowledge_e2e.py::test_the_granted_units_are_exactly_the_participants_of_the_prepared_plans`,
which installs two bundle generations and reads the participants back off the sixteen plans it
prepares.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
RUNTIME_ROOT = "/home/lighthouse/rquant/data/runtime"
CONTROL_ROOT = f"{RUNTIME_ROOT}/control"
ROLLOUT_ROOT = f"{CONTROL_ROOT}/schema-rollouts"
#: The entry itself, `-` prefixed the way #192 requires: the rollout root does not exist on a
#: first install, and an unprefixed missing path fails the unit at `226/NAMESPACE` before the
#: wrapper's first instruction.
GRANT = f"-{ROLLOUT_ROOT}"

#: unit -> the role it names, and why that role is in a plan. `producer` appends a PREPARE and
#: later a CUTOVER acknowledgement; `consumer` appends a capability receipt at CONSUMER_ACK.
PARTICIPANTS: dict[str, tuple[str, str]] = {
    "rquant-runtime-auction-match@.service": ("auction_match_source", "producer"),
    "rquant-runtime-auction-universe@.service": ("auction_universe_publisher", "producer"),
    "rquant-runtime-candidate@.service": ("candidate_publisher", "producer"),
    "rquant-runtime-feature@.service": ("feature_live", "producer"),
    "rquant-runtime-lab-jobs@.service": ("lab_jobs_publisher", "producer"),
    "rquant-runtime-market-minute@.service": ("market_minute_source", "producer"),
    "rquant-runtime-notifier@.service": ("notifier", "producer"),
    "rquant-runtime-paper-broker@.service": ("paper_broker", "producer"),
    "rquant-runtime-paper-constraint@.service": ("paper_constraint_publisher", "producer"),
    "rquant-runtime-promotions@.service": ("promotions_publisher", "producer"),
    "rquant-runtime-reference-slow-publisher@.service": ("reference_slow_publisher", "producer"),
    "rquant-runtime-reference-slow-source@.service": ("reference_slow_source", "producer"),
    "rquant-runtime-runtime-health@.service": ("runtime_health_publisher", "producer"),
    "rquant-runtime-serving@.service": ("serving_publisher", "consumer"),
    "rquant-runtime-signal-router@.service": ("signal_router", "producer"),
    "rquant-runtime-strategy@.service": ("strategy_live", "producer"),
}

#: The runtime units the ruling deliberately leaves out. `watchlist_quote_source` is the one
#: worth naming: it produces a channel nothing consumes, so no plan names it — and it was one
#: of the eight units #227 took down anyway, because admission walks every plan before it
#: knows whether this service is in one. Package G fixed that; it does not need a write.
BYSTANDERS: frozenset[str] = frozenset(
    {
        "rquant-runtime-artifact-catalog@.service",
        "rquant-runtime-daily-close@.service",
        "rquant-runtime-daily-orchestrator@.service",
        "rquant-runtime-recovery-rehearsal@.service",
        "rquant-runtime-recovery@.service",
        "rquant-runtime-shadow@.service",
        "rquant-runtime-watchlist-quote@.service",
    }
)

#: unit -> the `ReadWritePaths=` entries it granted *before* this ruling, verbatim, prefixes
#: included. The ruling is one appended entry and nothing else; this is what says so.
GRANTS_BEFORE_THE_RULING: dict[str, tuple[str, ...]] = {
    "rquant-runtime-auction-match@.service": (
        f"{CONTROL_ROOT}/auction-match-sources/%i",
        f"{RUNTIME_ROOT}/live/auction-match",
    ),
    "rquant-runtime-auction-universe@.service": (
        f"-{CONTROL_ROOT}/auction-universe-publishers/%i",
        f"-{RUNTIME_ROOT}/authorities/auction-universe",
    ),
    "rquant-runtime-candidate@.service": (
        f"-{CONTROL_ROOT}/candidates/%i",
        f"-{RUNTIME_ROOT}/live/candidates/%i",
    ),
    "rquant-runtime-feature@.service": (
        f"-{CONTROL_ROOT}/features/%i",
        f"-{RUNTIME_ROOT}/live/features",
    ),
    "rquant-runtime-lab-jobs@.service": (
        f"-{CONTROL_ROOT}/lab-jobs-publishers/%i",
        f"-{RUNTIME_ROOT}/research/serving-authorities/lab-jobs",
    ),
    "rquant-runtime-market-minute@.service": (
        f"{CONTROL_ROOT}/market-minute-sources/%i",
        f"{RUNTIME_ROOT}/live/market-minute",
    ),
    "rquant-runtime-notifier@.service": (
        f"{CONTROL_ROOT}/notifiers/%i",
        f"{RUNTIME_ROOT}/live/notifications/%i",
    ),
    "rquant-runtime-paper-broker@.service": (
        f"-{CONTROL_ROOT}/paper-brokers/%i",
        f"-{RUNTIME_ROOT}/live/paper-brokers/%i",
    ),
    "rquant-runtime-paper-constraint@.service": (
        f"-{CONTROL_ROOT}/paper-constraints/%i",
        f"-{RUNTIME_ROOT}/authorities/paper-execution",
    ),
    "rquant-runtime-promotions@.service": (
        f"-{CONTROL_ROOT}/promotions-publishers/%i",
        f"-{RUNTIME_ROOT}/research/serving-authorities/promotions",
    ),
    "rquant-runtime-reference-slow-publisher@.service": (
        f"{CONTROL_ROOT}/reference-slow-publishers/%i",
        f"{RUNTIME_ROOT}/authorities/reference-slow",
    ),
    "rquant-runtime-reference-slow-source@.service": (
        f"{CONTROL_ROOT}/reference-slow-sources/%i",
        f"{RUNTIME_ROOT}/live/reference-slow",
    ),
    "rquant-runtime-runtime-health@.service": (
        f"-{CONTROL_ROOT}/runtime-health-publishers/%i",
        f"-{CONTROL_ROOT}/authority-runtime-health",
    ),
    "rquant-runtime-serving@.service": (
        f"-{CONTROL_ROOT}/serving-publishers/%i",
        f"-{RUNTIME_ROOT}/serving",
    ),
    "rquant-runtime-signal-router@.service": (
        f"-{CONTROL_ROOT}/signal-routers/%i",
        f"-{RUNTIME_ROOT}/live/signal-bus",
    ),
    "rquant-runtime-strategy@.service": (
        f"-{CONTROL_ROOT}/strategies/%i",
        f"-{RUNTIME_ROOT}/live/strategies/%i",
    ),
}

PARTICIPANT_UNITS = tuple(sorted(PARTICIPANTS))
BYSTANDER_UNITS = tuple(sorted(BYSTANDERS))


def _text(unit: str) -> str:
    return (SYSTEMD / unit).read_text(encoding="utf-8")


def _directive(unit: str, key: str) -> list[str]:
    """Every physical `key=` line, values only — a repeated key must stay visible here."""

    return [
        line[len(key) + 1 :].strip()
        for line in _text(unit).splitlines()
        if line.startswith(f"{key}=")
    ]


def test_the_runtime_units_are_sixteen_participants_and_seven_bystanders() -> None:
    """A new runtime unit cannot appear without this file being read."""

    on_disk = {path.name for path in SYSTEMD.glob("rquant-runtime-*@.service")}

    assert set(PARTICIPANT_UNITS) | BYSTANDERS == on_disk
    assert not set(PARTICIPANT_UNITS) & BYSTANDERS
    assert len(PARTICIPANT_UNITS) == 16
    assert len(BYSTANDER_UNITS) == 7
    assert set(GRANTS_BEFORE_THE_RULING) == set(PARTICIPANTS)


@pytest.mark.parametrize("unit", PARTICIPANT_UNITS)
def test_a_participant_may_write_the_rollout_root(unit: str) -> None:
    """One `ReadWritePaths=` line, the rollout root on it once, ignored when missing."""

    declarations = _directive(unit, "ReadWritePaths")

    assert len(declarations) == 1, f"{unit}: expected one ReadWritePaths= line"
    entries = declarations[0].split()
    assert entries.count(GRANT) == 1, f"{unit}: {entries}"
    assert ROLLOUT_ROOT not in entries, f"{unit}: the grant must be ignored when missing"


@pytest.mark.parametrize("unit", BYSTANDER_UNITS)
def test_a_bystander_is_given_no_access_to_the_rollout_root_at_all(unit: str) -> None:
    """Not writable, not read-only, not named anywhere in the unit."""

    assert "schema-rollouts" not in _text(unit), unit


def test_no_unit_outside_the_sixteen_names_the_rollout_root() -> None:
    """The whole directory, not just the runtime templates: timers, oneshots, the arbiter."""

    naming = {
        path.name
        for path in sorted(SYSTEMD.iterdir())
        if path.is_file() and "schema-rollouts" in path.read_text(encoding="utf-8")
    }

    assert naming == set(PARTICIPANT_UNITS)


@pytest.mark.parametrize("unit", PARTICIPANT_UNITS)
def test_the_ruling_appended_one_entry_and_widened_nothing_else(unit: str) -> None:
    """Every other grant the unit had, in order, unchanged — the diff is one entry."""

    entries = tuple(_directive(unit, "ReadWritePaths")[0].split())

    assert entries == (*GRANTS_BEFORE_THE_RULING[unit], GRANT)


@pytest.mark.parametrize("unit", PARTICIPANT_UNITS)
def test_the_grant_is_the_whole_rollout_root_and_never_one_plan(unit: str) -> None:
    """`plan_id` is a content hash: it changes every generation, so it cannot be a literal.

    A unit file that named `…/schema-rollouts/<plan_id>` would be correct for exactly one
    generation and then silently grant nothing, which is #227 again with no error message.
    Per-plan granularity is only reachable through installer-generated drop-ins, and that is
    not what the owner authorised.
    """

    for line in _text(unit).splitlines():
        for entry in line.split():
            bare = entry.removeprefix("-")
            assert not bare.startswith(f"{ROLLOUT_ROOT}/"), f"{unit}: {entry} names one plan"
    assert "%i" not in GRANT


@pytest.mark.parametrize("unit", PARTICIPANT_UNITS)
def test_the_rollout_root_is_never_taken_back_by_another_directive(unit: str) -> None:
    """`InaccessiblePaths=` wins over a grant, and a nested `ReadOnlyPaths=` would too.

    `systemd.exec(5)`: a `ReadWritePaths=` nested inside a `ReadOnlyPaths=` directory is the
    documented way to open a writable subdirectory, which is what `rquant-runtime-serving@`
    relies on — it holds all of `control` read-only. What must not happen is the reverse: a
    `ReadOnlyPaths=` or `InaccessiblePaths=` entry at or below the rollout root itself.
    """

    for key in ("ReadOnlyPaths", "InaccessiblePaths"):
        for declaration in _directive(unit, key):
            for entry in declaration.split():
                bare = entry.removeprefix("-")
                assert bare != ROLLOUT_ROOT, f"{unit}: {key} takes the grant back"
                assert not bare.startswith(f"{ROLLOUT_ROOT}/"), f"{unit}: {key} carves it up"


def test_the_serving_unit_is_the_nested_case_and_still_holds_control_read_only() -> None:
    """The one participant whose grant sits inside a read-only parent it must keep."""

    readonly = _directive("rquant-runtime-serving@.service", "ReadOnlyPaths")[0].split()
    entries = _directive("rquant-runtime-serving@.service", "ReadWritePaths")[0].split()

    assert f"-{CONTROL_ROOT}" in readonly
    assert GRANT in entries
    assert ROLLOUT_ROOT.startswith(f"{CONTROL_ROOT}/")


@pytest.mark.parametrize("unit", PARTICIPANT_UNITS)
def test_the_participant_still_runs_the_role_this_file_says_it_does(unit: str) -> None:
    """The grant is bound to a role name, not to a unit file that could be re-pointed."""

    role, kind = PARTICIPANTS[unit]
    exec_start = _directive(unit, "ExecStart")

    assert len(exec_start) == 1, unit
    assert f"--role {role} --instance %i" in exec_start[0], unit
    assert kind in {"producer", "consumer"}
    assert _directive(unit, "User") == ["lighthouse"]


def test_only_one_participant_is_there_for_a_consumer_receipt() -> None:
    """`serving_publisher` produces nothing; it is in the sixteen for its capability receipt."""

    consumers = {unit for unit, (_role, kind) in PARTICIPANTS.items() if kind == "consumer"}

    assert consumers == {"rquant-runtime-serving@.service"}
    assert len(PARTICIPANTS) - len(consumers) == 15
