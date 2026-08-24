#!/usr/bin/env python3
"""Resolve one pre-checkout R07 deployment decision in an isolated child process.

The production deployer itself runs under ``python -I -S`` and therefore cannot import the
release virtual environment, while the R07 policy models and the private verifier require it.
This entrypoint is launched by the deployer with the release interpreter, resolves the installed
and target policies from Git objects, runs the Release A/B table plus any enforced evidence
verification, and prints the decision as canonical JSON. Running it out of process also keeps the
boundary-probe harness out of the live deployment interpreter.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from rquant.ops.r07_deploy_evidence import (  # noqa: E402
    R07DeployEvidenceGate,
    R07EvidenceError,
)
from rquant.ops.r07_deploy_evidence import (  # noqa: E402
    UrllibEvidenceTransport as _UrllibEvidenceTransport,
)
from rquant.strict_json import canonical_json_bytes  # noqa: E402


class _TrustedGitRunner:
    """Execute only the trusted Git binary supplied by the deployer."""

    def __init__(self, *, repo: Path, git_path: Path) -> None:
        self._repo = repo
        self._git_path = git_path

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        if not args or Path(args[0]) != self._git_path:
            raise R07EvidenceError("the R07 deployment gate runs only the trusted Git binary")
        return subprocess.run(
            args,
            cwd=self._repo,
            check=check,
            capture_output=True,
            text=True,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve one R07 deployment gate decision")
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--trusted-git-path", type=Path, required=True)
    parser.add_argument("--evidence-cache-dir", type=Path, required=True)
    parser.add_argument("--installed-commit", required=True)
    parser.add_argument("--target-commit", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo = args.repo.resolve()
    gate = R07DeployEvidenceGate(
        cache_dir=args.evidence_cache_dir,
        transport=_UrllibEvidenceTransport(),
    )
    decision = gate.evaluate(
        repo=repo,
        runner=_TrustedGitRunner(repo=repo, git_path=args.trusted_git_path),
        git_path=args.trusted_git_path,
        installed_commit_sha=args.installed_commit,
        target_commit_sha=args.target_commit,
    )
    sys.stdout.buffer.write(canonical_json_bytes(decision.model_dump(mode="json")))
    sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
