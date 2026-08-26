"""Regenerate the frozen R07 policy from the exact merge base and candidate.

Amended per Codex round-2 order 2026-08-25, item P1-1. The reviewed allowlist is the
complete ``merge_base(origin/main, candidate)..candidate`` raw Git diff, which is far too
large to maintain by hand, and every derived digest has to move with it. This generator is
the one repeatable way to produce those bytes: it is idempotent, its category rules are
frozen in code, and ``--check`` re-derives the policy and refuses to differ, so CI and
review can prove the checked-in fixture is exactly what the rules produce.

Every later work package that adds, removes, or edits a repository file must run it again.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    EXPECTED_FORBIDDEN_SOURCE_FILES,
    POLICY_RELATIVE_PATH,
    R07PolicyV1,
    boundary_manifest_digest,
    fixture_manifest_digest,
    normalized_ast_sha256,
    source_file_snapshot_from_source,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

DEFAULT_BASELINE_REF = "origin/main"
_ARCHITECTURE_DIRECTORIES = ("deploy/", "scripts/", "docs/", ".github/")
_ARCHITECTURE_ROOT_FILES = (
    ".env.example",
    ".gitignore",
    "AGENTS.md",
    "CHANGELOG.md",
    "DEPLOY.md",
    "README.md",
    "pyproject.toml",
    "uv.lock",
)


def diff_category(path: str) -> str:
    """The frozen category rules for one repository path.

    ``src/rquant`` is the declaration-scanned production surface, ``tests/fixtures`` is
    fixture data, the rest of ``tests`` is test code, and the reviewed deployment, tooling,
    documentation, workflow, and root configuration surface is architecture. Anything else
    is unclassified on purpose: a new top-level entry must be categorized by a reviewer, not
    by a silent default.
    """

    if path.startswith("src/"):
        if not path.startswith("src/rquant/"):
            raise ValueError(f"only src/rquant is a declared production surface: {path}")
        return "production"
    if path.startswith("tests/fixtures/"):
        return "fixture"
    if path.startswith("tests/"):
        return "test"
    if path.startswith(_ARCHITECTURE_DIRECTORIES) or path in _ARCHITECTURE_ROOT_FILES:
        return "architecture"
    raise ValueError(f"unclassified repository path needs a reviewed category rule: {path}")


def _git(repo: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
    ).stdout


def _resolve(repo: Path, revision: str) -> str:
    return _git(repo, "rev-parse", "--verify", f"{revision}^{{commit}}").decode().strip()


def _tree_of(repo: Path, commit: str) -> str:
    return _git(repo, "rev-parse", "--verify", f"{commit}^{{tree}}").decode().strip()


def _raw_diff(repo: Path, baseline: str, candidate: str | None) -> tuple[dict[str, Any], ...]:
    arguments = ["diff", "--raw", "-z", "--no-renames", "--abbrev=40"]
    if candidate is None:
        arguments.append("--cached")
        arguments.append(baseline)
    else:
        arguments.extend((baseline, candidate))
    parts = _git(repo, *arguments).split(b"\0")
    if parts[-1] != b"":
        raise ValueError("raw Git diff is not NUL terminated")
    entries: list[dict[str, Any]] = []
    for offset in range(0, len(parts) - 1, 2):
        header = parts[offset].decode("ascii")
        path = parts[offset + 1].decode("utf-8")
        if not header.startswith(":"):
            raise ValueError("invalid raw Git diff header")
        old_mode, new_mode, _old, _new, status_value = header[1:].split(" ")
        if len(status_value) != 1 or status_value not in {"A", "M", "D", "T"}:
            raise ValueError(f"unsupported raw Git diff status: {status_value}")
        entries.append(
            {
                "path": path,
                "status": status_value,
                "old_mode": old_mode,
                "new_mode": new_mode,
                "category": diff_category(path),
            }
        )
    ordered = sorted(
        entries,
        key=lambda entry: (
            entry["path"],
            entry["status"],
            entry["old_mode"],
            entry["new_mode"],
        ),
    )
    key_fields = ("path", "status", "old_mode", "new_mode")
    keys = [tuple(entry[field] for field in key_fields) for entry in ordered]
    if len(set(keys)) != len(keys):
        raise ValueError("raw Git diff produced duplicate allowlist keys")
    return tuple(ordered)


def _source_reader(repo: Path, candidate: str | None) -> Callable[[str], bytes]:
    def read(path: str) -> bytes:
        locator = f":{path}" if candidate is None else f"{candidate}:{path}"
        return _git(repo, "cat-file", "blob", locator)

    return read


def _validated(model: type[Any], value: object) -> Any:
    """Validate one policy section through JSON so tuple fields keep their exact shape."""

    return model.model_validate_json(canonical_json_bytes(value))


def _function_node(tree: ast.Module, qualname: str) -> ast.AST:
    name = qualname.rsplit(".", 1)[-1]
    node = next(
        (
            item
            for item in tree.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == name
        ),
        None,
    )
    if node is None:
        raise ValueError(f"declared root function is missing: {qualname}")
    return node


def _regenerated_root_snapshots(
    payload: list[dict[str, Any]],
    read: Callable[[str], bytes],
) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for snapshot in payload:
        source = read(snapshot["module_path"]).decode("utf-8")
        tree = ast.parse(source, filename=snapshot["module_path"])
        node = _function_node(tree, snapshot["qualname"])
        segment = ast.get_source_segment(source, node)
        if segment is None:
            raise ValueError(f"declared root source is missing: {snapshot['qualname']}")
        snapshots.append(
            {
                **snapshot,
                "signature": differential_gate._signature_text(node),
                "source_sha256": differential_gate.hashlib.sha256(segment.encode()).hexdigest(),
                "ast_sha256": normalized_ast_sha256(node),
            }
        )
    return snapshots


def _regenerated_production_declarations(
    payload: list[dict[str, Any]],
    read: Callable[[str], bytes],
) -> list[dict[str, Any]]:
    declarations: list[dict[str, Any]] = []
    for declaration in payload:
        source = read(declaration["module_path"]).decode("utf-8")
        tree = ast.parse(source, filename=declaration["module_path"])
        node = next(
            (
                item
                for item in tree.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                and item.name == declaration["symbol"]
            ),
            None,
        )
        if node is None:
            raise ValueError(f"declared production symbol is missing: {declaration['symbol']}")
        declarations.append(
            {
                **declaration,
                "source_span": f"{node.lineno}:{node.end_lineno}",
                "normalized_ast_sha256": normalized_ast_sha256(node),
            }
        )
    return declarations


def _regenerated_boundary_probes(
    payload: list[dict[str, Any]],
    read: Callable[[str], bytes],
) -> list[dict[str, Any]]:
    probes: list[dict[str, Any]] = []
    for probe in payload:
        filename, line_text = probe["source_span"].rsplit(":", 1)
        module_path = f"src/rquant/{filename}"
        source = read(module_path).decode("utf-8")
        tree = ast.parse(source, filename=module_path)
        line = int(line_text)
        candidates = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            and node.lineno <= line <= (node.end_lineno or node.lineno)
        ]
        if not candidates:
            raise ValueError(f"boundary source anchor is missing: {probe['inventory_id']}")
        node = min(candidates, key=lambda item: (item.end_lineno or item.lineno) - item.lineno)
        probes.append({**probe, "boundary_ast_sha256": normalized_ast_sha256(node)})
    return probes


def regenerate_policy_bytes(
    repo: Path,
    policy_bytes: bytes,
    *,
    baseline_ref: str = DEFAULT_BASELINE_REF,
    candidate: str | None = "HEAD",
) -> bytes:
    """Re-derive every baseline-, diff-, and source-derived policy field, then re-digest."""

    payload = strict_canonical_json_loads(policy_bytes)
    if not isinstance(payload, dict):
        raise ValueError("R07 policy must be a JSON object")
    head = _resolve(repo, "HEAD" if candidate is None else candidate)
    baseline = _git(repo, "merge-base", _resolve(repo, baseline_ref), head).decode().strip()
    if baseline != BASELINE_COMMIT_SHA:
        raise ValueError(
            "the frozen baseline constant is no longer merge_base(origin/main, candidate): "
            f"{baseline}"
        )
    read = _source_reader(repo, candidate)
    payload["baseline_commit_sha"] = baseline
    payload["baseline_tree_sha"] = _tree_of(repo, baseline)
    payload["allowed_diff"] = [dict(entry) for entry in _raw_diff(repo, baseline, candidate)]
    payload["source_file_snapshots"] = [
        source_file_snapshot_from_source(module_path, read(module_path)).model_dump(mode="json")
        for module_path in EXPECTED_FORBIDDEN_SOURCE_FILES
    ]
    payload["root_snapshots"] = _regenerated_root_snapshots(payload["root_snapshots"], read)
    payload["production_declarations"] = _regenerated_production_declarations(
        payload["production_declarations"],
        read,
    )
    payload["boundary_probes"] = _regenerated_boundary_probes(payload["boundary_probes"], read)
    payload["fixtures_digest"] = fixture_manifest_digest(
        tuple(_validated(differential_gate.FixtureValueV1, value) for value in payload["fixtures"]),
        tuple(
            _validated(differential_gate.CurrentFixtureV1, value)
            for value in payload["current_fixtures"]
        ),
    )
    payload["boundary_manifest_digest"] = boundary_manifest_digest(
        tuple(
            _validated(differential_gate.ProbeSetupV1, value) for value in payload["probe_setups"]
        ),
        tuple(
            _validated(differential_gate.BoundaryProbeV1, value)
            for value in payload["boundary_probes"]
        ),
    )
    payload["policy_digest"] = "0" * 64
    without_digest = {key: value for key, value in payload.items() if key != "policy_digest"}
    payload["policy_digest"] = differential_gate.hashlib.sha256(
        canonical_json_bytes(without_digest)
    ).hexdigest()
    regenerated = canonical_json_bytes(payload)
    policy = R07PolicyV1.model_validate_json(regenerated)
    completeness = differential_gate.verify_policy_completeness(policy)
    if not completeness.passed:
        raise ValueError("regenerated R07 policy is incomplete: " + "; ".join(completeness.reasons))
    if policy.canonical_bytes != regenerated:
        raise ValueError("regenerated R07 policy is not canonical")
    return regenerated


def _parse_args(arguments: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--policy", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--baseline-ref", default=DEFAULT_BASELINE_REF)
    parser.add_argument("--candidate", default="HEAD")
    parser.add_argument(
        "--staged",
        action="store_true",
        help="derive the policy from the Git index so one commit stays self-consistent",
    )
    parser.add_argument("--check", action="store_true")
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = _parse_args(arguments)
    repo = Path(args.repo).resolve(strict=True)
    policy_path = Path(args.policy) if args.policy is not None else repo / POLICY_RELATIVE_PATH
    output_path = Path(args.output) if args.output is not None else policy_path
    regenerated = regenerate_policy_bytes(
        repo,
        policy_path.read_bytes(),
        baseline_ref=args.baseline_ref,
        candidate=None if args.staged else args.candidate,
    )
    if args.check:
        current = policy_path.read_bytes()
        if current != regenerated:
            print(
                f"R07 policy {policy_path} is not what the frozen rules regenerate "
                f"({len(current)} bytes on disk, {len(regenerated)} bytes regenerated)",
                file=sys.stderr,
            )
            raise SystemExit(1)
        return 0
    output_path.write_bytes(regenerated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
