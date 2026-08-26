"""The repeatable R07 policy generator: fixed category rules, idempotence, and --check."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    BASELINE_TREE_SHA,
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
