#!/usr/bin/env python3
"""Recompute and backfill every derived value the Phase C fixture channel moves.

Ruling E-7 asked for one command instead of four manual steps, because the values below
depend on each other and a hand-synchronized subset is the failure mode: change a producer
fixture and the generation identity moves, which moves the immutable test manifest, which
moves `vector_set_hash` and `expected_result_set_hash`, which moves every release entry the
external verifier policy authorizes.

What it recomputes, in dependency order:

1. **The producer fixtures.** `scripts/build-signal-family-producer-fixtures.py` is re-run
   into a scratch directory and compared byte for byte with what is checked in; `--write`
   installs the rebuild. A fixture that is not exactly what the producer surface emits is
   not producer evidence.
2. **The Phase C expectation set.** The offline world derives `vector_set_hash` and
   `expected_result_set_hash` at policy-authoring time rather than storing them, so there is
   no literal to backfill — but there is a property to enforce, and it is the one that
   matters: the derivation must be stable. Both values are derived twice, in two separate
   private workspaces, and must agree.
3. **The R07 differential-gate policy.** `allowed_diff` is regenerated from the real
   `baseline..HEAD` raw diff with the frozen category rule, sorted into its canonical
   `(path, status, old_mode, new_mode)` order, and `policy_digest` is recomputed over the
   result. Nothing else in the policy is touched.
4. **The full-suite shard manifest**, through `scripts/full_suite_shards.py generate`.

Running it twice is a no-op: the second run reports every item as already current.

Usage::

    python scripts/signal_family_recompute_expectations.py            # report drift only
    python scripts/signal_family_recompute_expectations.py --write    # backfill
    python scripts/signal_family_recompute_expectations.py --write --skip-manifest
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Final

REPOSITORY_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
POLICY_PATH: Final[Path] = (
    REPOSITORY_ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"
)
MANIFEST_DIRECTORY: Final[Path] = REPOSITORY_ROOT / "tests" / "manifests" / "full-suite-v1"
SHARD_CONTRACT_TEST: Final[Path] = (
    REPOSITORY_ROOT / "tests" / "unit" / "test_assert_full_suite_shards.py"
)
FIXTURE_ROOT: Final[Path] = (
    REPOSITORY_ROOT / "tests" / "fixtures" / "signal_family_producer"
)

#: Ruling B-3 fixed `deploy/` at `architecture`; the rest of the rule is read straight off
#: the categories the existing entries already carry, so a regeneration reproduces them.
_FIXTURE_PREFIXES: Final[tuple[str, ...]] = ("tests/fixtures/", "tests/manifests/")


@dataclass(frozen=True)
class Outcome:
    """One recomputed item: what it is, whether it moved, and what it moved to."""

    name: str
    changed: bool
    detail: str


def _load_script(name: str) -> ModuleType:
    """Import a sibling script by path, registered so `@dataclass` inside it resolves.

    `dataclasses` looks its owner up in `sys.modules`, so a module executed straight out of
    `module_from_spec` without being registered raises on the first decorated class.
    """

    module_name = f"_recompute_{name.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(
        module_name,
        REPOSITORY_ROOT / "scripts" / f"{name}.py",
    )
    if spec is None or spec.loader is None:  # pragma: no cover - the scripts exist
        raise SystemExit(f"cannot load scripts/{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _private_scratch(prefix: str) -> Path:
    """A `0700` scratch root under `$HOME`, for the same reason the Phase C suites use one.

    Both Phase C ancestry walks refuse a group- or world-writable ancestor, and `TMPDIR`
    defaults to a sticky `/tmp` on Linux, so a derivation run under `TMPDIR` would fail with
    a message about the child workspace rather than about the temp directory.
    """

    root = Path(tempfile.mkdtemp(prefix=prefix, dir=Path.home()))
    root.chmod(0o700)
    return root


# ---------------------------------------------------------------------------------------
# 1. the producer fixtures
# ---------------------------------------------------------------------------------------


def recompute_producer_fixtures(*, write: bool) -> Outcome:
    builder = _load_script("build-signal-family-producer-fixtures")
    scratch = _private_scratch("rquant-recompute-fixtures-")
    try:
        rebuilt = scratch / "fixtures"
        builder.build_fixtures(rebuilt)
        differences: list[str] = []
        for candidate in sorted(rebuilt.rglob("*")):
            if not candidate.is_file():
                continue
            relative = candidate.relative_to(rebuilt)
            installed = FIXTURE_ROOT / relative
            if (
                not installed.is_file()
                or installed.read_bytes() != candidate.read_bytes()
            ):
                differences.append(str(relative))
        if differences and write:
            if FIXTURE_ROOT.exists():
                shutil.rmtree(FIXTURE_ROOT)
            shutil.copytree(rebuilt, FIXTURE_ROOT)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    if not differences:
        return Outcome("producer fixtures", False, "byte-identical to a fresh build")
    return Outcome(
        "producer fixtures",
        True,
        ("rewritten: " if write else "stale: ") + ", ".join(sorted(differences)),
    )


# ---------------------------------------------------------------------------------------
# 2. the Phase C expectation set
# ---------------------------------------------------------------------------------------


def recompute_expectation_set() -> Outcome:
    """Derive the two policy-bound hashes twice and require the derivation to be stable."""

    sys.path.insert(0, str(REPOSITORY_ROOT))
    sys.path.insert(0, str(REPOSITORY_ROOT / "src"))
    from rquant.signal_family_verification import (
        SignalFamilyExpectedResultV1,
        expected_result_set_hash,
        vector_set_hash,
    )
    from tests.support.signal_family_harness_vectors import (
        expected_results_for,
        harness_vectors,
    )

    vectors = harness_vectors()
    derived: list[tuple[str, str]] = []
    for attempt in range(2):
        scratch = _private_scratch(f"rquant-recompute-expected-{attempt}-")
        try:
            results = expected_results_for(vectors, scratch)
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        expected = tuple(
            SignalFamilyExpectedResultV1(
                vector_id=vector.vector_id,
                canonical_result_sha256=hashlib.sha256(
                    results[vector.vector_id].encode("utf-8")
                ).hexdigest(),
            )
            for vector in vectors
        )
        derived.append((vector_set_hash(vectors), expected_result_set_hash(expected)))
    if derived[0] != derived[1]:
        raise SystemExit(
            "the Phase C expectation set is not reproducible: "
            f"{derived[0]} then {derived[1]}"
        )
    vectors_hash, results_hash = derived[0]
    return Outcome(
        "phase C expectation set",
        False,
        (
            f"{len(vectors)} vectors; vector_set_hash={vectors_hash}; "
            f"expected_result_set_hash={results_hash}"
        ),
    )


# ---------------------------------------------------------------------------------------
# 3. the R07 differential-gate policy
# ---------------------------------------------------------------------------------------


def _category(path: str) -> str:
    if path.startswith("src/"):
        return "production"
    if path.startswith(_FIXTURE_PREFIXES):
        return "fixture"
    if path.startswith("tests/"):
        return "test"
    return "architecture"


def _raw_diff(baseline: str) -> tuple[dict[str, Any], ...]:
    completed = subprocess.run(  # noqa: S603 - fixed argv, repository-local
        ("git", "-C", str(REPOSITORY_ROOT), "diff", "--raw", "-z", baseline, "HEAD"),
        check=True,
        capture_output=True,
    )
    fields = completed.stdout.decode("utf-8").split("\0")
    entries: list[dict[str, Any]] = []
    index = 0
    while index < len(fields):
        record = fields[index]
        if not record.startswith(":"):
            index += 1
            continue
        parts = record[1:].split(" ")
        status = parts[4]
        index += 1
        path = fields[index]
        index += 1
        if status.startswith(("R", "C")):
            # A rename is two paths in the raw stream. The policy records the destination as
            # added and the source as deleted, which is what the tree diff itself shows.
            destination = fields[index]
            index += 1
            entries.append(
                {
                    "category": _category(path),
                    "new_mode": "000000",
                    "old_mode": parts[0],
                    "path": path,
                    "status": "D",
                }
            )
            entries.append(
                {
                    "category": _category(destination),
                    "new_mode": parts[1],
                    "old_mode": "000000",
                    "path": destination,
                    "status": "A",
                }
            )
            continue
        entries.append(
            {
                "category": _category(path),
                "new_mode": parts[1],
                "old_mode": parts[0],
                "path": path,
                "status": status[0],
            }
        )
    return tuple(
        sorted(
            entries,
            key=lambda entry: (
                entry["path"],
                entry["status"],
                entry["old_mode"],
                entry["new_mode"],
            ),
        )
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def recompute_policy(*, write: bool) -> Outcome:
    sys.path.insert(0, str(REPOSITORY_ROOT / "src"))
    from rquant.signal_family_differential_gate import BASELINE_COMMIT_SHA

    decoded = json.loads(POLICY_PATH.read_bytes())
    regenerated = _raw_diff(BASELINE_COMMIT_SHA)
    previous = tuple(decoded["allowed_diff"])
    previous_digest = decoded["policy_digest"]
    decoded["allowed_diff"] = [dict(entry) for entry in regenerated]
    body = {key: value for key, value in decoded.items() if key != "policy_digest"}
    digest = hashlib.sha256(_canonical_bytes(body)).hexdigest()
    decoded["policy_digest"] = digest
    if tuple(previous) == regenerated and previous_digest == digest:
        return Outcome(
            "r07 policy",
            False,
            f"{len(regenerated)} allowed_diff entries; policy_digest={digest}",
        )
    if write:
        POLICY_PATH.write_bytes(_canonical_bytes(decoded))
    added = {entry["path"] for entry in regenerated} - {
        entry["path"] for entry in previous
    }
    removed = {entry["path"] for entry in previous} - {
        entry["path"] for entry in regenerated
    }
    detail = (
        f"{len(previous)} -> {len(regenerated)} entries; "
        f"+{len(added)} -{len(removed)} paths; "
        f"policy_digest {previous_digest[:8]}… -> {digest[:8]}…"
    )
    return Outcome("r07 policy", True, ("rewritten: " if write else "stale: ") + detail)


# ---------------------------------------------------------------------------------------
# 4. the full-suite shard manifest
# ---------------------------------------------------------------------------------------


def _backfill_shard_baseline(full_suite: dict[str, Any], *, write: bool) -> bool:
    """Keep the contract case's frozen baseline in step with the manifest it guards.

    The literal exists so a silently shrinking suite is caught, which means it has to be
    updated deliberately whenever the manifest is regenerated. Doing it here rather than by
    hand is the difference between "the count moved because the suite grew" and "the count
    moved because someone edited a number until the test passed".
    """

    source = SHARD_CONTRACT_TEST.read_text(encoding="utf-8")
    updated = source
    for field in ("cases", "skips"):
        updated = re.sub(
            rf'assert full_suite\["{field}"\] == \d+',
            f'assert full_suite["{field}"] == {full_suite[field]}',
            updated,
        )
    if updated == source:
        return False
    if write:
        SHARD_CONTRACT_TEST.write_text(updated, encoding="utf-8")
    return True


def recompute_shard_manifest(*, write: bool, expected_skips: int) -> Outcome:
    index_path = MANIFEST_DIRECTORY / "index.json"
    before = json.loads(index_path.read_bytes())["full_suite"]
    if not write:
        drifted = _backfill_shard_baseline(before, write=False)
        return Outcome(
            "full-suite manifest",
            drifted,
            (
                f"not regenerated (--write not given); {before['cases']} cases"
                + ("; contract baseline is stale" if drifted else "")
            ),
        )
    shards = _load_script("full_suite_shards")
    code = shards.main(
        [
            "generate",
            "--manifest-dir",
            str(MANIFEST_DIRECTORY),
            "--expected-skips",
            str(expected_skips),
        ]
    )
    if code != 0:
        raise SystemExit(f"full_suite_shards generate exited {code}")
    after = json.loads(index_path.read_bytes())["full_suite"]
    baseline_moved = _backfill_shard_baseline(after, write=True)
    changed = before != after or baseline_moved
    return Outcome(
        "full-suite manifest",
        changed,
        (
            f"{before['cases']} -> {after['cases']} cases, {after['skips']} skips"
            + ("; contract baseline backfilled" if baseline_moved else "")
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="backfill the recomputed values instead of only reporting drift",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="leave the full-suite shard manifest alone (it needs a full collect)",
    )
    parser.add_argument("--expected-skips", type=int, default=48)
    arguments = parser.parse_args(argv)

    outcomes = [
        recompute_producer_fixtures(write=arguments.write),
        recompute_expectation_set(),
        recompute_policy(write=arguments.write),
    ]
    if not arguments.skip_manifest:
        outcomes.append(
            recompute_shard_manifest(
                write=arguments.write,
                expected_skips=arguments.expected_skips,
            )
        )
    for outcome in outcomes:
        marker = "CHANGED" if outcome.changed else "current"
        print(f"[{marker:>7}] {outcome.name}: {outcome.detail}")
    if any(outcome.changed for outcome in outcomes) and not arguments.write:
        print("re-run with --write to backfill", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
