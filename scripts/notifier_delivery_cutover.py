#!/usr/bin/env python3
"""The three operator checks of the notifier's shadow -> live switch (#281, #284, #190).

Standard library only, and importing nothing from this repository, so an operator can run
it from any checkout -- including one fetched with `git show <tag>:scripts/...` next to a
bootstrap worktree that has to stay on the installed tag, because the switch must not
change the producer commit (see DEPLOY.md, "2026-09-28 · 待执行 · notifier 切正式推送").

`set-mode`
    Rewrite the one key `notifier_delivery_mode` of the production inputs document and
    nothing else. The document is read by `load_production_runtime_profile_inputs`, which
    refuses anything that is not canonical JSON (`strict_canonical_json_loads`: sorted
    keys, `(",", ":")` separators, no trailing newline), so the rewrite is canonical too --
    the DEPLOY.md recipe it replaces wrote `indent=2` plus a newline, which that loader
    refuses with "persistent JSON is not canonical". Refuses unless the current value is
    the one `--from` names, the file is a private regular file owned by this user, and the
    input was itself canonical. Setting it back reproduces the original bytes exactly, so
    the sha256 printed on the way back is the one the file had before the switch.

`diff-profiles OLD NEW`
    Compare two runtime-production profiles (`data/runtime-profiles/<id>.json`). Exit 0
    only when the documents differ in `profile_id` and in the notifier manifest's
    delivery switches (`settings.suppress_delivery`, `settings.paused`) and nowhere else.

`diff-generations OLD NEW`
    The same judgement over two installed deployment-bundle generations
    (`data/runtime/generations/<hash>/`): every `manifests/*.json` must be byte-identical
    except the notifier's, which may differ only in the delivery switches; and
    `schema-contracts.json` may differ only in the notifier's entry of
    `manifest_fingerprints` and in its own `content_hash` -- a channel shape, a producer or
    a consumer that moved would be a real schema change, and a reason to stop.

Both diffs print a JSON summary. Exit status: 0 = only the expected difference, 1 = any
other difference (the summary lists it), 2 = usage error or unreadable input.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

DELIVERY_MODES = ("paused", "shadow", "live")
MODE_KEY = "notifier_delivery_mode"
#: the service id of the one notifier the production profile builds; it keeps the word
#: `shadow` in every mode, because renaming it would change its instance label and with it
#: the authority profile id (#190)
NOTIFIER_SERVICE_ID = "notifier.admin.shadow.v1"
#: what one delivery mode differs from another by, inside the notifier manifest
DELIVERY_SETTINGS = frozenset({"settings.suppress_delivery", "settings.paused"})
#: what the schema contract bundle of a generation differs by when only the notifier's
#: manifest did: its fingerprint of that manifest, and the bundle's hash of itself
SCHEMA_CONTRACT_PATHS = frozenset(
    {f"manifest_fingerprints.{NOTIFIER_SERVICE_ID}", "content_hash"}
)
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024


class CutoverError(RuntimeError):
    """A refusal the operator reads; the process exits 2."""


# ---------------------------------------------------------------------------------------
# Canonical JSON, restated (the script must not import the package)
# ---------------------------------------------------------------------------------------


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CutoverError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise CutoverError(f"non-finite JSON number: {value}")


def strict_loads(payload: bytes) -> Any:
    try:
        return json.loads(
            payload,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CutoverError(f"not JSON: {exc}") from exc


def canonical_bytes(value: Any) -> bytes:
    """`rquant.strict_json.canonical_json_bytes`, byte for byte (the suite pins the two)."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------------------
# set-mode
# ---------------------------------------------------------------------------------------


def _read_private_document(path: Path) -> bytes:
    if not path.is_absolute():
        raise CutoverError(f"--inputs must be an absolute path: {path}")
    try:
        observed = os.lstat(path)
    except OSError as exc:
        raise CutoverError(f"inputs document is unreadable: {exc}") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise CutoverError("inputs document must be a regular file, not a link")
    if observed.st_uid != os.geteuid():
        raise CutoverError("inputs document must be owned by the user running this")
    if observed.st_nlink != 1:
        raise CutoverError("inputs document must have exactly one link")
    if stat.S_IMODE(observed.st_mode) != 0o600:
        raise CutoverError(
            f"inputs document mode is {oct(stat.S_IMODE(observed.st_mode))}, expected 0o600"
        )
    if observed.st_size <= 0 or observed.st_size > MAX_DOCUMENT_BYTES:
        raise CutoverError("inputs document size is unsafe")
    return path.read_bytes()


def _write_private_document(path: Path, payload: bytes) -> None:
    """Sibling temporary file, 0600 before the first byte, fsync, rename, fsync the parent."""

    staging = path.with_name(f".{path.name}.cutover-staging")
    descriptor = os.open(
        staging,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    except BaseException:
        os.close(descriptor)
        staging.unlink(missing_ok=True)
        raise
    os.close(descriptor)
    os.replace(staging, path)
    parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def set_mode(path: Path, *, expected: str, target: str, dry_run: bool) -> dict[str, Any]:
    original = _read_private_document(path)
    document = strict_loads(original)
    if not isinstance(document, dict):
        raise CutoverError("inputs document is not a JSON object")
    if canonical_bytes(document) != original:
        raise CutoverError(
            "inputs document is not canonical JSON; it was not written by "
            "build_runtime_production_inputs.py (or was edited by hand) -- regenerate it"
        )
    if MODE_KEY not in document:
        raise CutoverError(
            f"inputs document has no {MODE_KEY}: it predates #281 and this code cannot switch it"
        )
    current = document[MODE_KEY]
    if current != expected:
        raise CutoverError(f"{MODE_KEY} is {current!r}, not the expected {expected!r}")
    updated = dict(document)
    updated[MODE_KEY] = target
    payload = canonical_bytes(updated)
    changed = sorted(
        key for key in set(document) | set(updated) if document.get(key) != updated.get(key)
    )
    summary: dict[str, Any] = {
        "inputs": str(path),
        "from": current,
        "to": target,
        "sha256_before": _sha256(original),
        "sha256_after": _sha256(payload),
        "changed_keys": changed,
        "written": False,
    }
    if payload == original or dry_run:
        return summary
    _write_private_document(path, payload)
    if path.read_bytes() != payload:  # pragma: no cover - a filesystem that lies
        raise CutoverError("inputs document does not read back as written")
    summary["written"] = True
    return summary


# ---------------------------------------------------------------------------------------
# diff-profiles / diff-generations
# ---------------------------------------------------------------------------------------


def _paths(old: Any, new: Any, prefix: str = "") -> Iterator[str]:
    """Every leaf path at which two JSON values differ."""

    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(set(old) | set(new)):
            child = f"{prefix}.{key}" if prefix else str(key)
            if key not in old or key not in new:
                yield child
            else:
                yield from _paths(old[key], new[key], child)
        return
    if isinstance(old, list) and isinstance(new, list) and len(old) == len(new):
        for index, (left, right) in enumerate(zip(old, new, strict=True)):
            yield from _paths(left, right, f"{prefix}[{index}]")
        return
    if old != new:
        yield prefix or "<root>"


def _manifests_by_service(document: Any, *, label: str) -> dict[str, Any]:
    manifests = document.get("manifests") if isinstance(document, dict) else None
    if not isinstance(manifests, list):
        raise CutoverError(f"{label} has no manifests list")
    by_service: dict[str, Any] = {}
    for manifest in manifests:
        service_id = manifest.get("service_id") if isinstance(manifest, dict) else None
        if not isinstance(service_id, str) or service_id in by_service:
            raise CutoverError(f"{label} carries a manifest without a unique service_id")
        by_service[service_id] = manifest
    return by_service


def _judge_manifests(old: dict[str, Any], new: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    unexpected: list[str] = []
    notifier: list[str] = []
    for service_id in sorted(set(old) - set(new)):
        unexpected.append(f"manifest removed: {service_id}")
    for service_id in sorted(set(new) - set(old)):
        unexpected.append(f"manifest added: {service_id}")
    for service_id in sorted(set(old) & set(new)):
        differing = list(_paths(old[service_id], new[service_id]))
        if not differing:
            continue
        if service_id != NOTIFIER_SERVICE_ID:
            unexpected.extend(f"{service_id}: {path}" for path in differing)
            continue
        notifier = differing
        unexpected.extend(
            f"{service_id}: {path}" for path in differing if path not in DELIVERY_SETTINGS
        )
    return {
        "manifests_compared": len(set(old) & set(new)),
        "notifier_differences": notifier,
        "notifier_before": _delivery_switches(old.get(NOTIFIER_SERVICE_ID)),
        "notifier_after": _delivery_switches(new.get(NOTIFIER_SERVICE_ID)),
    }, unexpected


def _delivery_switches(manifest: Any) -> dict[str, Any] | None:
    if not isinstance(manifest, dict) or not isinstance(manifest.get("settings"), dict):
        return None
    settings = manifest["settings"]
    return {key: settings.get(key) for key in ("paused", "suppress_delivery")}


def _read_json_file(path: Path, *, label: str) -> tuple[bytes, Any]:
    try:
        observed = os.lstat(path)
    except OSError as exc:
        raise CutoverError(f"{label} is unreadable: {exc}") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise CutoverError(f"{label} must be a regular file, not a link: {path}")
    if observed.st_size > MAX_DOCUMENT_BYTES:
        raise CutoverError(f"{label} is too large: {path}")
    payload = path.read_bytes()
    return payload, strict_loads(payload)


def diff_profiles(old_path: Path, new_path: Path) -> tuple[dict[str, Any], list[str]]:
    _, old = _read_json_file(old_path, label="old profile")
    _, new = _read_json_file(new_path, label="new profile")
    if not isinstance(old, dict) or not isinstance(new, dict):
        raise CutoverError("a profile is not a JSON object")
    unexpected: list[str] = []
    for key in sorted(set(old) | set(new)):
        if key in {"manifests", "profile_id"}:
            continue
        unexpected.extend(_paths({key: old.get(key)}, {key: new.get(key)}))
    manifests, manifest_problems = _judge_manifests(
        _manifests_by_service(old, label="old profile"),
        _manifests_by_service(new, label="new profile"),
    )
    unexpected.extend(manifest_problems)
    summary = {
        "old": str(old_path),
        "new": str(new_path),
        "profile_id_before": old.get("profile_id"),
        "profile_id_after": new.get("profile_id"),
        "producer_commit": new.get("producer_commit"),
        **manifests,
    }
    return summary, unexpected


def _generation_manifests(root: Path, *, label: str) -> tuple[dict[str, Any], dict[str, bytes]]:
    directory = root / "manifests"
    if not directory.is_dir() or directory.is_symlink():
        raise CutoverError(f"{label} has no manifests directory: {directory}")
    manifests: dict[str, Any] = {}
    payloads: dict[str, bytes] = {}
    for path in sorted(directory.iterdir()):
        if path.suffix != ".json":
            raise CutoverError(f"{label} manifests directory holds a stranger: {path}")
        payload, document = _read_json_file(path, label=f"{label} manifest")
        service_id = document.get("service_id") if isinstance(document, dict) else None
        if not isinstance(service_id, str) or service_id in manifests:
            raise CutoverError(f"{label} manifest without a unique service_id: {path}")
        manifests[service_id] = document
        payloads[service_id] = payload
    return manifests, payloads


def diff_generations(old_root: Path, new_root: Path) -> tuple[dict[str, Any], list[str]]:
    old, old_payloads = _generation_manifests(old_root, label="old generation")
    new, new_payloads = _generation_manifests(new_root, label="new generation")
    manifests, unexpected = _judge_manifests(old, new)
    byte_identical = sorted(
        service_id
        for service_id in set(old_payloads) & set(new_payloads)
        if old_payloads[service_id] == new_payloads[service_id]
    )
    old_contracts, new_contracts = (
        _read_json_file(root / "schema-contracts.json", label=f"{label} schema contracts")[1]
        for root, label in ((old_root, "old generation"), (new_root, "new generation"))
    )
    contract_differences = list(_paths(old_contracts, new_contracts))
    unexpected.extend(
        f"schema-contracts.json: {path}"
        for path in contract_differences
        if path not in SCHEMA_CONTRACT_PATHS
    )
    summary = {
        "old": str(old_root),
        "new": str(new_root),
        "manifests_byte_identical": len(byte_identical),
        "schema_contract_differences": contract_differences,
        **manifests,
    }
    return summary, unexpected


# ---------------------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="notifier_delivery_cutover.py",
        description="Operator checks of the notifier delivery-mode switch.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    set_parser = commands.add_parser("set-mode", help="rewrite notifier_delivery_mode only")
    set_parser.add_argument("--inputs", type=Path, required=True)
    set_parser.add_argument("--from", dest="expected", choices=DELIVERY_MODES, required=True)
    set_parser.add_argument("--to", dest="target", choices=DELIVERY_MODES, required=True)
    set_parser.add_argument("--dry-run", action="store_true")
    profiles = commands.add_parser("diff-profiles", help="compare two runtime-production profiles")
    profiles.add_argument("old", type=Path)
    profiles.add_argument("new", type=Path)
    generations = commands.add_parser(
        "diff-generations", help="compare two installed deployment-bundle generations"
    )
    generations.add_argument("old", type=Path)
    generations.add_argument("new", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "set-mode":
            summary = set_mode(
                arguments.inputs,
                expected=arguments.expected,
                target=arguments.target,
                dry_run=arguments.dry_run,
            )
            print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if arguments.command == "diff-profiles":
            summary, unexpected = diff_profiles(arguments.old, arguments.new)
        else:
            summary, unexpected = diff_generations(arguments.old, arguments.new)
    except CutoverError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2
    summary["unexpected_differences"] = unexpected
    summary["ok"] = not unexpected
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not unexpected else 1


if __name__ == "__main__":
    raise SystemExit(main())
