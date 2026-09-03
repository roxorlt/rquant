"""Regenerate the frozen R07 policy from the exact merge base and candidate.

The reviewed allowlist is the complete ``baseline..candidate`` raw Git diff, which is far too
large to maintain by hand, and every derived digest has to move with it. This generator is
the one repeatable way to produce those bytes: it is idempotent, its category rules are
frozen in code, and ``--check`` re-derives the policy and refuses to differ, so CI and
review can prove the checked-in fixture is exactly what the rules produce.

Amended for Release B: the baseline is no longer rediscovered here by asking a branch where
it is. It comes from ``resolve_baseline_context``, which reads explicit endpoints, the GitHub
event, or HEAD's own parents, and never a remote-tracking ref. Rediscovering it from
``origin/main`` inside the checkout was self-defeating, because the merge that freezes a
baseline is also what makes ``origin/main`` equal to the candidate.

Every later work package that adds, removes, or edits a repository file must run it again,
and it must be run on Python 3.11 or 3.12: ``ast.dump`` drops default-valued fields from 3.13
on, so a 3.13 run produces a self-consistent policy that CI rejects.
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    EXPECTED_FORBIDDEN_SOURCE_FILES,
    POLICY_RELATIVE_PATH,
    R07PolicyV1,
    baseline_resolution_summary,
    boundary_manifest_digest,
    fixture_manifest_digest,
    normalized_ast_sha256,
    resolve_baseline_context,
    source_file_snapshot_from_source,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

SUPPORTED_GENERATOR_PYTHON = ((3, 11), (3, 12))
_ARCHITECTURE_DIRECTORIES = ("deploy/", "scripts/", "docs/", ".github/")
_ARCHITECTURE_ROOT_FILES = (
    ".env.example",
    ".gitignore",
    "AGENTS.md",
    "CHANGELOG.md",
    "CLAUDE.md",
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


def _entrypoint_node(tree: ast.Module, entrypoint: str, *, module: str) -> ast.AST:
    """Resolve one boundary probe's declared entrypoint to its exact definition node.

    ``source_span`` is only a line anchor, and a line anchor silently re-aims at whatever
    definition happens to occupy that line after an unrelated edit above it. ``entrypoint``
    is the reviewed name, so the span is derived from it rather than trusted as input.
    """

    prefix = f"rquant.{module}."
    if not entrypoint.startswith(prefix):
        raise ValueError(f"boundary entrypoint does not name its own module: {entrypoint}")
    body: list[ast.stmt] = list(tree.body)
    node: ast.AST | None = None
    for part in entrypoint[len(prefix) :].split("."):
        node = next(
            (
                item
                for item in body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                and item.name == part
            ),
            None,
        )
        if node is None:
            raise ValueError(f"boundary entrypoint is missing: {entrypoint}")
        body = list(node.body)  # type: ignore[attr-defined]
    if node is None:
        raise ValueError(f"boundary entrypoint is empty: {entrypoint}")
    return node


def _regenerated_boundary_probes(
    payload: list[dict[str, Any]],
    read: Callable[[str], bytes],
) -> list[dict[str, Any]]:
    probes: list[dict[str, Any]] = []
    for probe in payload:
        filename = probe["source_span"].rsplit(":", 1)[0]
        module_path = f"src/rquant/{filename}"
        source = read(module_path).decode("utf-8")
        tree = ast.parse(source, filename=module_path)
        node = _entrypoint_node(tree, probe["entrypoint"], module=filename.removesuffix(".py"))
        probes.append(
            {
                **probe,
                "source_span": f"{filename}:{node.lineno}",  # type: ignore[attr-defined]
                "boundary_ast_sha256": normalized_ast_sha256(node),
            }
        )
    return probes


def _refreshed_boundary_snapshots(
    repo: Path,
    payload: list[dict[str, Any]],
    *,
    policy_path: Path,
) -> list[dict[str, Any]]:
    """Re-observe every boundary probe's no-mutation snapshot digest by running the probe.

    These two digests are a property of the production store the probe touches, so any
    schema change to a probed database moves them. They cannot be derived statically: the
    only honest source is an actual probe run, and the run must still show the boundary
    rejecting before it mutates anything, which is exactly what is asserted here before a
    refreshed digest is accepted.

    Off by default because it spawns one subprocess per probe. Pass
    ``--refresh-boundary-snapshots`` after changing a probed store's schema.
    """

    sys.path.insert(0, str(repo))
    from tests.r07_differential_probe_runner import run_boundary_probe_subprocess

    refreshed: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="rquant-r07-refresh-") as directory:
        root = Path(directory)
        for index, probe in enumerate(payload):
            inventory_id = probe["inventory_id"]
            if probe["variant"] == "static_only":
                # `static_only` rows have no dynamic runner: their evidence is the static
                # snapshot universe, and their digests do not observe any store.
                refreshed.append(dict(probe))
                continue
            try:
                observed = run_boundary_probe_subprocess(
                    policy_path=policy_path,
                    candidate_root=repo,
                    inventory_id=inventory_id,
                    tmp_path=root / f"probe-{index:02d}",
                )
            except AssertionError as exc:
                # The child exits nonzero whenever `passed` is false, and a stale snapshot
                # digest is exactly that case, so the run this refresh exists for always
                # lands here. Its own result JSON is still the payload; anything that is
                # not that JSON is a real failure and propagates.
                try:
                    observed = json.loads(str(exc))
                except ValueError:
                    raise exc from None
                if not isinstance(observed, dict) or observed.get("inventory_id") != inventory_id:
                    raise exc from None
            before = observed["before_snapshot_digest"]
            after = observed["after_snapshot_digest"]
            if before != after:
                raise ValueError(f"boundary probe mutated observable state: {inventory_id}")
            if observed["reached_count"] != 1:
                raise ValueError(f"boundary probe did not reach its sentinel once: {inventory_id}")
            guards = observed["mutation_guard_counts"]
            if not isinstance(guards, dict) or any(guards.values()):
                raise ValueError(f"boundary probe tripped a mutation guard: {inventory_id}")
            refreshed.append(
                {
                    **probe,
                    "before_snapshot_digest": before,
                    "after_snapshot_digest": after,
                }
            )
    return refreshed


def regenerate_policy_bytes(
    repo: Path,
    policy_bytes: bytes,
    *,
    baseline_commit: str,
    candidate: str | None = "HEAD",
    refresh_boundary_snapshots: bool = False,
    policy_path: Path | None = None,
) -> bytes:
    """Re-derive every baseline-, diff-, and source-derived policy field, then re-digest.

    ``baseline_commit`` is decided by ``resolve_baseline_context`` and handed in already
    proven. Re-checking it against the module constant here is defence in depth: it is the
    difference between a policy nobody can validate and one that refuses to be written.
    """

    payload = strict_canonical_json_loads(policy_bytes)
    if not isinstance(payload, dict):
        raise ValueError("R07 policy must be a JSON object")
    baseline = _resolve(repo, baseline_commit)
    if baseline != BASELINE_COMMIT_SHA:
        raise ValueError(
            "the R07 policy can only be generated against the frozen baseline constant; "
            f"this run resolved {baseline}"
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
    if refresh_boundary_snapshots:
        if policy_path is None:
            raise ValueError("refreshing boundary snapshots needs the on-disk policy path")
        payload["boundary_probes"] = _refreshed_boundary_snapshots(
            repo,
            payload["boundary_probes"],
            policy_path=policy_path,
        )
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
    parser.add_argument(
        "--event",
        choices=("pull_request", "push"),
        default=None,
        help="state the R07 context explicitly instead of deriving it from HEAD's parents",
    )
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--candidate-sha", default=None)
    parser.add_argument("--event-before-sha", default=None)
    parser.add_argument(
        "--staged",
        action="store_true",
        help="derive the policy from the Git index so one commit stays self-consistent",
    )
    parser.add_argument(
        "--refresh-boundary-snapshots",
        action="store_true",
        help="re-observe each boundary probe's no-mutation snapshot digest by running it",
    )
    parser.add_argument("--check", action="store_true")
    return parser.parse_args(arguments)


def require_supported_interpreter(version: tuple[int, int]) -> None:
    """Refuse to generate policy bytes on an interpreter whose AST dump is not the frozen one.

    ``ast.dump`` omits fields equal to their default from Python 3.13 on, so every normalized
    AST digest in the policy shifts. A 3.13 run is the dangerous case precisely because it is
    self-consistent: it regenerates cleanly, ``--check`` passes against its own output, and
    only the 3.11 and 3.12 jobs in CI ever see the drift.
    """

    if version not in SUPPORTED_GENERATOR_PYTHON:
        print(
            "R07 policy generation requires Python 3.11 or 3.12; this interpreter is "
            f"{version[0]}.{version[1]}. ast.dump changed which fields it emits, so every "
            "normalized AST digest in the policy would drift and CI would reject the bytes.",
            file=sys.stderr,
        )
        raise SystemExit(2)


def main(arguments: list[str] | None = None) -> int:
    require_supported_interpreter(sys.version_info[:2])
    args = _parse_args(arguments)
    repo = Path(args.repo).resolve(strict=True)
    policy_path = Path(args.policy) if args.policy is not None else repo / POLICY_RELATIVE_PATH
    output_path = Path(args.output) if args.output is not None else policy_path
    resolution = resolve_baseline_context(
        repo,
        event=args.event,
        base_sha=args.base_sha,
        candidate_sha=args.candidate_sha,
        event_before_sha=args.event_before_sha,
    )
    # Both the generation and the --check path go through here, so a log always records which
    # of the four sources answered. They produce identical bytes, and one of them is weaker.
    print(baseline_resolution_summary(resolution), file=sys.stderr)
    regenerated = regenerate_policy_bytes(
        repo,
        policy_path.read_bytes(),
        baseline_commit=resolution.baseline_commit_sha,
        candidate=None if args.staged else resolution.context.candidate_sha,
        refresh_boundary_snapshots=args.refresh_boundary_snapshots,
        policy_path=policy_path,
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
