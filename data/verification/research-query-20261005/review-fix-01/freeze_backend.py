"""Bind the four changed query backend files and the synthetic Linux proof."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def main() -> None:
    worktree = Path(__file__).resolve().parents[4]
    directory = Path(__file__).resolve().parents[1]
    previous = json.loads((directory / "linux-backend-freeze-r06.json").read_text())
    current = []
    changed = []
    for entry in previous["src_files"]:
        content = (worktree / entry["path"]).read_bytes()
        actual = {
            "path": entry["path"],
            "byte_count": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        current.append(actual)
        if actual != entry:
            changed.append(entry["path"])
    assert set(changed) == {
        "src/rquant/research_query/child.py",
        "src/rquant/research_query/contracts.py",
        "src/rquant/research_query/snapshot.py",
        "src/rquant/web/routes/research_query.py",
    }, changed
    binding = hashlib.sha256(
        json.dumps(current, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    proof = directory / "linux-review-boundaries-r07.py"
    manifest = {
        "kind": "research_query_linux_backend_freeze_r07",
        "base_commit": previous["base_commit"],
        "spec_commit": previous["spec_commit"],
        "review_start_commit": "90b61007d47bd906bb85a3e4f67521b12cdf2cbb",
        "backend_sources_frozen": True,
        "product_candidate_frozen": False,
        "production_write": False,
        "provider_http": 0,
        "bundle_binding_sha256": binding,
        "previous_bundle_binding_sha256": previous["bundle_binding_sha256"],
        "changes_from_r06": changed,
        "scope": (
            "RQ-R02/R04/R05 actual restricted-child boundaries plus r06 "
            "zero-spill/capacity/source/cleanup; RQ-R01 HTTP and RQ-R03 React verified locally"
        ),
        "proof": {
            "path": str(proof.relative_to(worktree)),
            "bytes": proof.stat().st_size,
            "sha256": hashlib.sha256(proof.read_bytes()).hexdigest(),
        },
        "src_files": current,
        "linux_execution": "pending root execution; syntax check is not Linux proof",
    }
    target = directory / "linux-backend-freeze-r07.json"
    with target.open("x") as handle:
        json.dump(manifest, handle, sort_keys=True, indent=2)
        handle.write("\n")
    print(
        json.dumps(
            {
                "manifest": str(target.relative_to(worktree)),
                "manifest_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                "source_files": len(current),
                "source_binding_sha256": binding,
                "changed_paths": changed,
                "proof": manifest["proof"],
            }
        )
    )


if __name__ == "__main__":
    main()
