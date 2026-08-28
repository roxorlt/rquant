"""One miniature Git repository the R07 baseline-context tests share.

The generator, the CI producer, and the differential gate all resolve their baseline through
``rquant.signal_family_differential_gate.resolve_baseline_context``. Their tests therefore
need the same four shapes to point it at — a pull request pair, a real merge, a squash, and
an unrelated history — and building those twice is how the two diff implementations this
repository already regrets came about.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

_IDENTITY = (
    "-c",
    "user.email=r07-fixture@example.invalid",
    "-c",
    "user.name=r07-fixture",
)
_STABLE_DATE = "2026-01-01T00:00:00+00:00"


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def commit_all(repo: Path, message: str) -> str:
    """Commit the whole worktree under a fixed identity and date, then return the SHA."""

    _git(repo, "add", "-A")
    subprocess.run(
        ["git", "-C", str(repo), *_IDENTITY, "commit", "--quiet", "-m", message],
        check=True,
        capture_output=True,
        env={
            **_environment(),
            "GIT_AUTHOR_DATE": _STABLE_DATE,
            "GIT_COMMITTER_DATE": _STABLE_DATE,
        },
    )
    return _git(repo, "rev-parse", "HEAD")


def _environment() -> dict[str, str]:
    return dict(os.environ)


def head(repo: Path) -> str:
    return _git(repo, "rev-parse", "HEAD")


def merge_fixture_repo(root: Path) -> dict[str, str]:
    """A miniature main branch plus a feature branch, a real merge, and a squash.

    ``main_tip`` stands in for the frozen baseline: it is the real two-sided merge base of
    ``feature`` and ``main_tip`` and the first parent of ``merge``, which is exactly the two
    relationships a pull request context and a push context each have to prove. ``base`` is an
    older ancestor and ``squash`` a single-parent rewrite, so a wrong base and a wrong commit
    shape both have a concrete object to fail against. ``orphan`` shares no history at all,
    which is the only way to reach an uncomputable merge base.

    ``stale_feature`` forks from ``base`` rather than ``main_tip``, and ``stale_merge`` is the
    object GitHub synthesizes for ``refs/pull/N/merge`` in that situation. Together they are
    the ``github.sha`` trap: the real merge base of ``main_tip`` and ``stale_feature`` is
    ``base``, but ``main_tip`` is a parent of ``stale_merge``, so proving the merge base
    against the merge ref instead of the pull request head asserts nothing at all.
    """

    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(root)],
        check=True,
        capture_output=True,
    )
    (root / "base.txt").write_text("base\n", encoding="utf-8")
    base = commit_all(root, "base")
    (root / "main-only.txt").write_text("main\n", encoding="utf-8")
    main_tip = commit_all(root, "main tip")
    subprocess.run(
        ["git", "-C", str(root), "checkout", "--quiet", "-b", "feature", main_tip],
        check=True,
    )
    (root / "feature.txt").write_text("feature\n", encoding="utf-8")
    feature = commit_all(root, "feature")
    subprocess.run(["git", "-C", str(root), "checkout", "--quiet", "main"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            *_IDENTITY,
            "merge",
            "--no-ff",
            "--quiet",
            "-m",
            "merge feature",
            feature,
        ],
        check=True,
        env={
            **_environment(),
            "GIT_AUTHOR_DATE": _STABLE_DATE,
            "GIT_COMMITTER_DATE": _STABLE_DATE,
        },
    )
    merge = head(root)
    subprocess.run(
        ["git", "-C", str(root), "checkout", "--quiet", "-B", "squash", main_tip],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(root), "merge", "--squash", feature],
        check=True,
        capture_output=True,
    )
    squash = commit_all(root, "squashed feature")
    subprocess.run(
        ["git", "-C", str(root), "checkout", "--quiet", "-b", "stale-feature", base],
        check=True,
        capture_output=True,
    )
    (root / "stale-feature.txt").write_text("stale feature\n", encoding="utf-8")
    stale_feature = commit_all(root, "stale feature")
    subprocess.run(["git", "-C", str(root), "checkout", "--quiet", main_tip], check=True)
    stale_merge = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "commit-tree",
            _git(root, "rev-parse", "--verify", f"{stale_feature}^{{tree}}"),
            "-p",
            main_tip,
            "-p",
            stale_feature,
            "-m",
            "Merge stale-feature into main",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={
            **_environment(),
            "GIT_AUTHOR_NAME": "r07-fixture",
            "GIT_AUTHOR_EMAIL": "r07-fixture@example.invalid",
            "GIT_COMMITTER_NAME": "r07-fixture",
            "GIT_COMMITTER_EMAIL": "r07-fixture@example.invalid",
            "GIT_AUTHOR_DATE": _STABLE_DATE,
            "GIT_COMMITTER_DATE": _STABLE_DATE,
        },
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", str(root), "checkout", "--quiet", "--orphan", "unrelated"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(root), "rm", "-rqf", "."],
        check=True,
        capture_output=True,
    )
    (root / "orphan.txt").write_text("orphan\n", encoding="utf-8")
    orphan = commit_all(root, "unrelated history")
    subprocess.run(["git", "-C", str(root), "checkout", "--quiet", "main"], check=True)
    return {
        "base": base,
        "main_tip": main_tip,
        "feature": feature,
        "merge": merge,
        "squash": squash,
        "stale_feature": stale_feature,
        "stale_merge": stale_merge,
        "orphan": orphan,
    }


def write_github_event(path: Path, payload: object) -> Path:
    """Write a GitHub event payload exactly where ``GITHUB_EVENT_PATH`` would point."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
