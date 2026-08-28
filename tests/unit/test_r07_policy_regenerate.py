"""The repeatable R07 policy generator: fixed category rules, idempotence, and --check."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    BASELINE_TREE_SHA,
    HISTORICAL_BASELINE_COMMIT_SHA,
    load_policy,
)
from scripts import r07_policy_regenerate as regenerate

ROOT = Path(__file__).parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"


def _head() -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.mark.parametrize(
    ("path", "category"),
    (
        ("src/rquant/signal_bus.py", "production"),
        ("tests/fixtures/r07_differential_gate/policy-v1.json", "fixture"),
        ("tests/unit/test_signal_bus.py", "test"),
        ("deploy/systemd/rquant-monitor.service", "architecture"),
        ("scripts/r07_ci_evidence.py", "architecture"),
        ("docs/architecture/production-interpreter-authority.md", "architecture"),
        (".github/workflows/ci.yml", "architecture"),
        ("pyproject.toml", "architecture"),
        ("uv.lock", "architecture"),
        (".env.example", "architecture"),
    ),
)
def test_diff_category_rules_are_frozen(path: str, category: str) -> None:
    assert regenerate.diff_category(path) == category


def test_unknown_top_level_paths_require_review_instead_of_a_silent_category() -> None:
    with pytest.raises(ValueError, match="unclassified"):
        regenerate.diff_category("unknown-top-level/thing.txt")
    with pytest.raises(ValueError, match="src/rquant"):
        regenerate.diff_category("src/other/thing.py")


def test_check_mode_passes_on_the_checked_in_policy() -> None:
    assert regenerate.main(["--repo", str(ROOT), "--check"]) == 0


def test_generation_is_idempotent_and_reproduces_the_checked_in_bytes(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    assert regenerate.main(["--repo", str(ROOT), "--output", str(first)]) == 0
    arguments = ["--repo", str(ROOT), "--policy", str(first), "--output", str(second)]
    assert regenerate.main(arguments) == 0

    assert first.read_bytes() == second.read_bytes()
    assert first.read_bytes() == POLICY_PATH.read_bytes()


def test_check_mode_fails_when_the_allowlist_or_digest_drifts(tmp_path: Path) -> None:
    tampered = tmp_path / "tampered.json"
    payload = json.loads(POLICY_PATH.read_bytes())
    payload["allowed_diff"] = payload["allowed_diff"][:-1]
    tampered.write_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())

    with pytest.raises(SystemExit) as failure:
        regenerate.main(["--repo", str(ROOT), "--policy", str(tampered), "--check"])

    assert failure.value.code == 1

    restored = tmp_path / "restored.json"
    assert (
        regenerate.main(["--repo", str(ROOT), "--policy", str(tampered), "--output", str(restored)])
        == 0
    )
    assert restored.read_bytes() == POLICY_PATH.read_bytes()


def test_generated_allowlist_covers_the_complete_merge_base_diff() -> None:
    raw = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "diff",
            "--raw",
            "-z",
            "--no-renames",
            "--abbrev=40",
            BASELINE_COMMIT_SHA,
            _head(),
        ],
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    observed = set()
    for offset in range(0, len(raw) - 1, 2):
        header = raw[offset].decode("ascii")
        old_mode, new_mode, _old, _new, status = header[1:].split(" ")
        observed.add((raw[offset + 1].decode("utf-8"), status[0], old_mode, new_mode))

    policy = load_policy(POLICY_PATH)

    assert policy.baseline_commit_sha == BASELINE_COMMIT_SHA
    assert policy.baseline_tree_sha == BASELINE_TREE_SHA
    assert {entry.policy_key for entry in policy.allowed_diff} == observed
    assert len(policy.allowed_diff) == len(observed)


_DECOY_MODULE = "\n".join(
    (
        "class Decoy:",
        "    def ingest(self, envelope):",
        "        return ('decoy', envelope)",
        "",
        "",
        "class Store:",
        "    def ingest(self, envelope):",
        "        return ('real', envelope)",
        "",
    )
)


def _probe_for(entrypoint: str) -> dict[str, object]:
    return {
        "entrypoint": entrypoint,
        # The stale anchor: line 7 is Store.ingest before the edit above it and
        # Decoy.ingest after it, which is the whole point of not trusting it.
        "source_span": "example.py:7",
        "boundary_ast_sha256": "0" * 64,
        "variant": "dynamic",
    }


def test_boundary_ast_digest_follows_the_entrypoint_name_not_the_stale_line_anchor() -> None:
    """An unrelated edit above a probed method must not re-aim the boundary digest.

    ``source_span`` is a line anchor, and a line anchor points at whatever definition
    occupies that line after the file moves. Here the decoy sits above the real store and
    a four-line edit slides its ``ingest`` onto the anchored line, so a generator that
    trusts the anchor freezes the decoy's AST under the real store's name.
    """

    shifted = "# unrelated edit\n" * 4 + _DECOY_MODULE
    probe = _probe_for("rquant.example.Store.ingest")

    unshifted_result = regenerate._regenerated_boundary_probes(
        [probe], lambda _path: _DECOY_MODULE.encode("utf-8")
    )[0]
    shifted_result = regenerate._regenerated_boundary_probes(
        [probe], lambda _path: shifted.encode("utf-8")
    )[0]
    decoy_result = regenerate._regenerated_boundary_probes(
        [_probe_for("rquant.example.Decoy.ingest")],
        lambda _path: shifted.encode("utf-8"),
    )[0]

    assert unshifted_result["source_span"] == "example.py:7"
    assert shifted_result["source_span"] == "example.py:11"
    assert shifted_result["boundary_ast_sha256"] == unshifted_result["boundary_ast_sha256"]
    # The negative control: resolution really does tell the two definitions apart, so the
    # equality above is the name being honoured and not both sides landing on one node.
    assert decoy_result["source_span"] == "example.py:6"
    assert decoy_result["boundary_ast_sha256"] != shifted_result["boundary_ast_sha256"]

    with pytest.raises(ValueError, match="does not name its own module"):
        regenerate._regenerated_boundary_probes(
            [_probe_for("rquant.other.Store.ingest")],
            lambda _path: _DECOY_MODULE.encode("utf-8"),
        )
    with pytest.raises(ValueError, match="boundary entrypoint is missing"):
        regenerate._regenerated_boundary_probes(
            [_probe_for("rquant.example.Store.absent")],
            lambda _path: _DECOY_MODULE.encode("utf-8"),
        )


_CLEAN_OBSERVATION = {
    "inventory_id": "R07-B01",
    "before_snapshot_digest": "a" * 64,
    "after_snapshot_digest": "a" * 64,
    "reached_count": 1,
    "mutation_guard_counts": {"writes": 0},
}


@pytest.mark.parametrize(
    ("override", "message"),
    (
        ({"after_snapshot_digest": "b" * 64}, "mutated observable state"),
        ({"reached_count": 2}, "did not reach its sentinel once"),
        ({"mutation_guard_counts": {"writes": 1}}, "tripped a mutation guard"),
    ),
)
def test_boundary_snapshot_refresh_only_accepts_a_clean_no_mutation_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    override: dict[str, object],
    message: str,
) -> None:
    """A refreshed digest is an R07 authority value, so the run it came from has to have
    shown the boundary rejecting before it touched anything."""

    import tests.r07_differential_probe_runner as runner

    def observe(**_kwargs: object) -> dict[str, object]:
        # The refresh exists for the run whose snapshot digest is stale, and that run
        # fails, so the payload arrives as the child's result JSON on an AssertionError.
        raise AssertionError(json.dumps({**_CLEAN_OBSERVATION, **override}))

    monkeypatch.setattr(runner, "run_boundary_probe_subprocess", observe)
    probes = [{"inventory_id": "R07-B01", "variant": "dynamic"}]
    with pytest.raises(ValueError, match=message):
        regenerate._refreshed_boundary_snapshots(ROOT, probes, policy_path=tmp_path / "p.json")


def test_boundary_snapshot_refresh_writes_back_only_what_the_probe_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tests.r07_differential_probe_runner as runner

    calls: list[str] = []

    def observe(*, inventory_id: str, **_kwargs: object) -> dict[str, object]:
        calls.append(inventory_id)
        raise AssertionError(json.dumps(_CLEAN_OBSERVATION))

    monkeypatch.setattr(runner, "run_boundary_probe_subprocess", observe)
    probes = [
        {"inventory_id": "R07-B01", "variant": "dynamic", "before_snapshot_digest": "c" * 64},
        {"inventory_id": "R07-S01", "variant": "static_only", "before_snapshot_digest": "d" * 64},
    ]

    refreshed = regenerate._refreshed_boundary_snapshots(
        ROOT, probes, policy_path=tmp_path / "p.json"
    )

    assert refreshed[0]["before_snapshot_digest"] == "a" * 64
    assert refreshed[0]["after_snapshot_digest"] == "a" * 64
    # `static_only` rows observe no store, so the refresh must not spawn a runner for them
    # or invent digests they never had.
    assert refreshed[1] == probes[1]
    assert calls == ["R07-B01"]


def test_boundary_snapshot_refresh_propagates_a_failure_that_is_not_its_own_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tests.r07_differential_probe_runner as runner

    def observe(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("boundary probe rejected for some other reason")

    monkeypatch.setattr(runner, "run_boundary_probe_subprocess", observe)
    probes = [{"inventory_id": "R07-B01", "variant": "dynamic"}]
    with pytest.raises(AssertionError, match="some other reason"):
        regenerate._refreshed_boundary_snapshots(ROOT, probes, policy_path=tmp_path / "p.json")

    def observe_other_probe(**_kwargs: object) -> dict[str, object]:
        raise AssertionError(json.dumps({**_CLEAN_OBSERVATION, "inventory_id": "R07-B02"}))

    monkeypatch.setattr(runner, "run_boundary_probe_subprocess", observe_other_probe)
    with pytest.raises(AssertionError, match="R07-B02"):
        regenerate._refreshed_boundary_snapshots(ROOT, probes, policy_path=tmp_path / "p.json")


def test_the_generator_refuses_an_interpreter_whose_ast_dump_is_not_the_frozen_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """3.13 is the dangerous case because it is self-consistent, not because it errors.

    It regenerates cleanly and its own ``--check`` passes against its own output; the drift
    only surfaces in the 3.11 and 3.12 CI jobs, after the bytes are committed. Refusing to
    start is the only place that catches it while there is still a person watching.
    """

    regenerate.require_supported_interpreter((3, 11))
    regenerate.require_supported_interpreter((3, 12))

    for version in ((3, 10), (3, 13), (4, 0)):
        with pytest.raises(SystemExit) as refusal:
            regenerate.require_supported_interpreter(version)
        assert refusal.value.code == 2
    captured = capsys.readouterr()
    assert "3.11 or 3.12" in captured.err
    assert "ast.dump" in captured.err


def test_the_generator_has_no_moving_ref_to_read_a_baseline_from(tmp_path: Path) -> None:
    """The old ``--baseline-ref origin/main`` is gone, not merely defaulted away.

    A base has to be an explicit commit SHA, because the whole failure this replaces was a
    ref moving under a frozen constant. Leaving the option in place with a better default
    would have kept the failure one flag away.
    """

    with pytest.raises(SystemExit) as removed:
        regenerate.main(["--repo", str(ROOT), "--baseline-ref", "origin/main", "--check"])
    assert removed.value.code == 2

    with pytest.raises(ValueError, match="lowercase 40-hex"):
        regenerate.main(
            [
                "--repo",
                str(ROOT),
                "--event",
                "pull_request",
                "--base-sha",
                "origin/main",
                "--output",
                str(tmp_path / "never.json"),
            ]
        )
    assert not (tmp_path / "never.json").exists()


def test_the_generator_takes_its_baseline_from_the_stated_context_and_refuses_a_wrong_base(
    tmp_path: Path,
) -> None:
    """Deriving the context from HEAD and stating it explicitly must produce one answer.

    Those are the two ways the generator is invoked - a developer running it locally and a
    workflow passing the pull request endpoints - and a policy whose bytes depended on which
    one was used would be unreviewable.
    """

    derived = tmp_path / "derived.json"
    stated = tmp_path / "stated.json"

    assert regenerate.main(["--repo", str(ROOT), "--output", str(derived)]) == 0
    assert (
        regenerate.main(
            [
                "--repo",
                str(ROOT),
                "--event",
                "pull_request",
                "--base-sha",
                BASELINE_COMMIT_SHA,
                "--candidate-sha",
                _head(),
                "--output",
                str(stated),
            ]
        )
        == 0
    )
    assert derived.read_bytes() == stated.read_bytes()

    # A real commit that is a real merge base, just not the frozen one.
    with pytest.raises(ValueError, match="not the merge base of this run's stated endpoints"):
        regenerate.main(
            [
                "--repo",
                str(ROOT),
                "--event",
                "pull_request",
                "--base-sha",
                HISTORICAL_BASELINE_COMMIT_SHA,
                "--candidate-sha",
                _head(),
                "--output",
                str(tmp_path / "never.json"),
            ]
        )
    assert not (tmp_path / "never.json").exists()


def test_the_generator_reports_which_source_decided_the_baseline(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The four sources produce identical policy bytes and are not equally strong.

    ``frozen_baseline_fallback`` degenerates the merge-base equality into ancestry, so a run
    that took it proved less than a run that did not - and nothing in the output said which
    had happened. A label nobody can read is a label the next refactor deletes as unused.
    """

    def summary_lines() -> list[str]:
        return [
            line
            for line in capsys.readouterr().err.splitlines()
            if line.startswith("R07 baseline: ")
        ]

    # What CI passes: both endpoints stated on the command line.
    assert (
        regenerate.main(
            [
                "--repo",
                str(ROOT),
                "--event",
                "pull_request",
                "--base-sha",
                BASELINE_COMMIT_SHA,
                "--candidate-sha",
                _head(),
                "--check",
            ]
        )
        == 0
    )
    stated = summary_lines()
    assert len(stated) == 1
    assert f"event=pull_request base={BASELINE_COMMIT_SHA} candidate={_head()}" in stated[0]
    assert "base_source=explicit_cli" in stated[0]
    assert "frozen_baseline_fallback" not in stated[0]

    # The other CI shape: nothing on the command line, the endpoints read off the event.
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps(
            {
                "pull_request": {
                    "base": {"sha": BASELINE_COMMIT_SHA},
                    "head": {"sha": _head()},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))

    assert regenerate.main(["--repo", str(ROOT), "--check"]) == 0
    from_event = summary_lines()
    assert len(from_event) == 1
    assert "base_source=github_event_payload" in from_event[0]
    assert "frozen_baseline_fallback" not in from_event[0]

    # The two CI shapes have to be distinguishable from each other, and neither may be the
    # degraded fallback. Nothing further is claimed about this checkout: whether a bare local
    # run takes the fallback depends on whether HEAD has one parent or two, and every CI
    # checkout has two, so asserting the fallback here would pass on a laptop and fail in CI.
    # Both shapes are pinned exactly against fixture repositories in
    # test_signal_family_differential_gate.py::
    # test_the_resolution_summary_names_the_source_each_checkout_shape_produces.
    observed = {line.rsplit("base_source=", 1)[1] for line in (stated[0], from_event[0])}
    assert observed == {"explicit_cli", "github_event_payload"}
