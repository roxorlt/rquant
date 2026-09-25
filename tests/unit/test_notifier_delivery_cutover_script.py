"""`scripts/notifier_delivery_cutover.py`: the operator's three checks of the live switch.

Each case runs the script the way the operator does -- a subprocess, the standard library
only -- over documents written by the real producers: the inputs document by
`scripts/build_runtime_production_inputs.py`, the profiles by
`build_production_runtime_profile`. What the script decides is then checked against the
loader that reads the result in production, `load_production_runtime_profile_inputs`,
rather than against the script's own idea of the format.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import scripts.build_runtime_production_inputs as generator
from rquant.delivery_contracts import DeliveryChannel, OutboxStatus
from rquant.runtime_builder_signal import notifier_builder
from rquant.runtime_production_profile import (
    build_production_runtime_profile,
    load_production_runtime_profile_inputs,
)
from rquant.strict_json import StrictJsonError, canonical_json_bytes
from tests.unit.test_build_runtime_production_inputs import (
    COMMIT,
    _argv,
    _write_calendar_database,
)
from tests.unit.test_runtime_builder_signal import NOW, _notifier_manifest, _Provider, _seed_outbox

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "notifier_delivery_cutover.py"
NOTIFIER = "notifier.admin.shadow.v1"


def run(*arguments: str) -> tuple[int, dict[str, Any] | None, str]:
    completed = subprocess.run(
        [sys.executable, "-I", str(SCRIPT), *arguments],
        capture_output=True,
        text=True,
        check=False,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C"},
    )
    summary = json.loads(completed.stdout) if completed.stdout.strip() else None
    return completed.returncode, summary, completed.stderr


def generate(root: Path, **overrides: str) -> Path:
    """One real run of the generator; returns the inputs document it wrote."""

    root.mkdir(parents=True, exist_ok=True)
    database = root / "calendar.duckdb"
    if not database.exists():
        _write_calendar_database(database)
    assert generator.main(_argv(root, **overrides)) == 0
    return root / "data" / "runtime-production-inputs.json"


def notifier_switches(document: Path) -> dict[str, Any]:
    inputs = load_production_runtime_profile_inputs(
        document, expected_commit=COMMIT, expected_runtime_mode="local-test"
    )
    manifest = next(
        item
        for item in build_production_runtime_profile(inputs).manifests
        if item.service_id == NOTIFIER
    )
    return {
        "mode": inputs.notifier_delivery_mode,
        "paused": manifest.settings["paused"],
        "suppress_delivery": manifest.settings["suppress_delivery"],
    }


def test_the_restated_canonical_encoding_is_the_packages_byte_for_byte() -> None:
    script = _load_script()
    value = {
        "z": [1, 2.5, None, True],
        "a": {"中文": "值", "nested": {"b": 1, "a": -0.0}},
        "notifier_delivery_mode": "shadow",
    }
    assert script.canonical_bytes(value) == canonical_json_bytes(value)


def _load_script() -> Any:
    import importlib.util

    spec = importlib.util.spec_from_file_location("notifier_delivery_cutover", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_set_mode_switches_one_key_the_production_loader_accepts_and_back_exactly(
    tmp_path: Path,
) -> None:
    document = generate(tmp_path / "host")
    original = document.read_bytes()
    assert notifier_switches(document) == {
        "mode": "shadow",
        "paused": False,
        "suppress_delivery": True,
    }

    code, summary, stderr = run(
        "set-mode", "--inputs", str(document), "--from", "shadow", "--to", "live"
    )

    assert code == 0, stderr
    assert summary is not None
    assert summary["changed_keys"] == ["notifier_delivery_mode"]
    assert summary["written"] is True
    assert (document.stat().st_mode & 0o777) == 0o600
    assert document.stat().st_nlink == 1
    assert notifier_switches(document) == {
        "mode": "live",
        "paused": False,
        "suppress_delivery": False,
    }
    live_sha = summary["sha256_after"]

    code, back, stderr = run(
        "set-mode", "--inputs", str(document), "--from", "live", "--to", "shadow"
    )

    assert code == 0, stderr
    assert back is not None
    assert back["sha256_before"] == live_sha
    #: the way back reproduces the document the generator wrote, byte for byte
    assert back["sha256_after"] == summary["sha256_before"]
    assert document.read_bytes() == original
    assert not list(document.parent.glob(".*cutover-staging"))


def test_set_mode_writes_exactly_what_the_generator_writes_for_live(tmp_path: Path) -> None:
    """The one-key rewrite and a full rerun with `--notifier-delivery-mode live` agree.

    Same `--generated-at`, so every other generated file is identical as well: the
    rewrite is not a second way of producing the document, it is the same document.
    """

    shadow = generate(tmp_path / "shadow")
    live = generate(tmp_path / "live", **{"--notifier-delivery-mode": "live"})
    code, _summary, stderr = run(
        "set-mode", "--inputs", str(shadow), "--from", "shadow", "--to", "live"
    )
    assert code == 0, stderr

    def normalized(path: Path, root: Path) -> bytes:
        return path.read_bytes().replace(str(root).encode(), b"<root>")

    assert normalized(shadow, tmp_path / "shadow") == normalized(live, tmp_path / "live")


def test_the_recipe_it_replaces_writes_a_document_the_loader_refuses(tmp_path: Path) -> None:
    """DEPLOY.md's 2026-09-21 "改法 A": `json.dumps(indent=2, sort_keys=True) + "\\n"`.

    `runtime-production-profile` reads the document with `strict_canonical_json_loads`,
    so the pretty-printed rewrite fails with "persistent JSON is not canonical" before any
    profile is built -- the recipe could never have switched the host. The script refuses
    such a document too, rather than canonicalizing whatever it finds.
    """

    document = generate(tmp_path / "host")
    payload = json.loads(document.read_bytes())
    payload["notifier_delivery_mode"] = "live"
    document.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(StrictJsonError, match="not canonical"):
        load_production_runtime_profile_inputs(document, expected_commit=COMMIT)

    code, summary, stderr = run(
        "set-mode", "--inputs", str(document), "--from", "live", "--to", "shadow"
    )
    assert code == 2
    assert summary is None
    assert "not canonical" in stderr


def test_set_mode_refuses_everything_it_was_not_asked_to_trust(tmp_path: Path) -> None:
    document = generate(tmp_path / "host")
    original = document.read_bytes()

    def refused(*arguments: str) -> str:
        code, summary, stderr = run("set-mode", *arguments)
        assert code == 2, (arguments, summary)
        return stderr

    #: the current value is not the one the operator said it was
    assert "not the expected 'live'" in refused(
        "--inputs", str(document), "--from", "live", "--to", "shadow"
    )
    #: a relative path
    assert "absolute" in refused(
        "--inputs", "runtime-production-inputs.json", "--from", "shadow", "--to", "live"
    )
    #: a link, or a second name for the same file
    link = document.with_name("inputs-link.json")
    link.symlink_to(document)
    assert "not a link" in refused("--inputs", str(link), "--from", "shadow", "--to", "live")
    link.unlink()
    hard = document.with_name("inputs-hard.json")
    os.link(document, hard)
    assert "exactly one link" in refused(
        "--inputs", str(document), "--from", "shadow", "--to", "live"
    )
    hard.unlink()
    #: readable by anybody else
    document.chmod(0o644)
    assert "expected 0o600" in refused(
        "--inputs", str(document), "--from", "shadow", "--to", "live"
    )
    document.chmod(0o600)
    #: a document from before #281, with no mode in it at all
    older = document.with_name("older.json")
    payload = json.loads(original)
    payload.pop("notifier_delivery_mode")
    older.write_bytes(canonical_json_bytes(payload))
    older.chmod(0o600)
    assert "predates #281" in refused("--inputs", str(older), "--from", "shadow", "--to", "live")

    #: and a dry run writes nothing
    code, summary, _stderr = run(
        "set-mode", "--inputs", str(document), "--from", "shadow", "--to", "live", "--dry-run"
    )
    assert code == 0 and summary is not None and summary["written"] is False
    assert document.read_bytes() == original


def _profile_file(path: Path, profile: Any) -> Path:
    path.write_text(json.dumps(profile.model_dump(mode="json"), sort_keys=True), encoding="utf-8")
    return path


def test_diff_profiles_passes_the_switch_and_nothing_else(tmp_path: Path) -> None:
    document = generate(tmp_path / "host")

    def profile() -> Any:
        return build_production_runtime_profile(
            load_production_runtime_profile_inputs(document, expected_commit=COMMIT)
        )

    shadow = _profile_file(tmp_path / "shadow.json", profile())
    assert run("set-mode", "--inputs", str(document), "--from", "shadow", "--to", "live")[0] == 0
    live = _profile_file(tmp_path / "live.json", profile())

    code, summary, stderr = run("diff-profiles", str(shadow), str(live))
    assert code == 0, (summary, stderr)
    assert summary is not None
    assert summary["ok"] is True
    assert summary["manifests_compared"] == 26
    assert summary["notifier_differences"] == ["settings.suppress_delivery"]
    assert summary["notifier_before"] == {"paused": False, "suppress_delivery": True}
    assert summary["notifier_after"] == {"paused": False, "suppress_delivery": False}
    assert summary["profile_id_before"] != summary["profile_id_after"]

    #: the same profile twice is "no difference" -- the check the rollback uses
    code, same, _stderr = run("diff-profiles", str(shadow), str(shadow))
    assert code == 0 and same is not None and same["notifier_differences"] == []

    #: any other change is a stop: another role's setting, a top-level field, a commit
    document_value = json.loads(live.read_text(encoding="utf-8"))
    router = next(
        item for item in document_value["manifests"] if item["service_id"].startswith("signal-")
    )
    router["settings"]["batch_limit"] = 1
    document_value["producer_commit"] = "b" * 40
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(document_value), encoding="utf-8")
    code, summary, _stderr = run("diff-profiles", str(shadow), str(changed))
    assert code == 1
    assert summary is not None and summary["ok"] is False
    assert f"{router['service_id']}: settings.batch_limit" in summary["unexpected_differences"]
    assert "producer_commit" in summary["unexpected_differences"]


def _generation(root: Path, manifests: dict[str, dict[str, Any]], contracts: Any) -> Path:
    (root / "manifests").mkdir(parents=True)
    for index, (service_id, manifest) in enumerate(sorted(manifests.items())):
        (root / "manifests" / f"svc-{index:064d}.json").write_text(
            json.dumps({"service_id": service_id, **manifest}, sort_keys=True),
            encoding="utf-8",
        )
    (root / "schema-contracts.json").write_text(json.dumps(contracts), encoding="utf-8")
    return root


def test_diff_generations_allows_the_notifier_fingerprint_and_refuses_a_channel(
    tmp_path: Path,
) -> None:
    manifests = {
        NOTIFIER: {"settings": {"paused": False, "suppress_delivery": True}},
        "signal-router.all-strategies.v1": {"settings": {"batch_limit": 256}},
    }
    contracts = {
        "content_hash": "1" * 64,
        "manifest_fingerprints": {NOTIFIER: "2" * 64, "signal-router.all-strategies.v1": "3" * 64},
        "channels": [{"channel_id": "runtime.serving.signals", "shape": "4" * 64}],
    }
    old = _generation(tmp_path / "old", manifests, contracts)
    live_manifests = json.loads(json.dumps(manifests))
    live_manifests[NOTIFIER]["settings"]["suppress_delivery"] = False
    live_contracts = json.loads(json.dumps(contracts))
    live_contracts["content_hash"] = "5" * 64
    live_contracts["manifest_fingerprints"][NOTIFIER] = "6" * 64
    new = _generation(tmp_path / "new", live_manifests, live_contracts)

    code, summary, stderr = run("diff-generations", str(old), str(new))
    assert code == 0, (summary, stderr)
    assert summary is not None
    assert summary["manifests_byte_identical"] == 1
    assert sorted(summary["schema_contract_differences"]) == [
        "content_hash",
        f"manifest_fingerprints.{NOTIFIER}",
    ]

    moved = json.loads(json.dumps(live_contracts))
    moved["channels"][0]["shape"] = "7" * 64
    shutil.rmtree(new)
    new = _generation(tmp_path / "new", live_manifests, moved)
    code, summary, _stderr = run("diff-generations", str(old), str(new))
    assert code == 1
    assert summary is not None
    assert summary["unexpected_differences"] == ["schema-contracts.json: channels[0].shape"]


def test_the_deploy_entry_outbox_check_runs_against_a_real_notifier_store(tmp_path: Path) -> None:
    """The read-only outbox check of the 2026-09-28 DEPLOY entry, lifted out of the file.

    The step-0 gate reads the notifier's own store with a Python heredoc; this case runs
    that heredoc, byte for byte as DEPLOY.md has it, against a real `NotificationStateStore`
    holding one shadow delivery, so a renamed table or column makes this red instead of
    making the operator's gate fail on Monday.
    """

    deploy = (REPO_ROOT / "DEPLOY.md").read_text(encoding="utf-8")
    (snippet,) = re.findall(
        r"<<'EOF'\n(import sqlite3, sys\n.*?non-shadow receipts.*?)\nEOF", deploy, re.S
    )
    assert "mode=ro" in snippet

    state = _seed_outbox(tmp_path)
    notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path, suppress_delivery=True))()
    (record,) = state.outbox_records()
    assert record.status is OutboxStatus.SUCCEEDED

    completed = subprocess.run(
        [sys.executable, "-", str(tmp_path / "notification-state.sqlite3")],
        input=snippet,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == [
        "outbox {'succeeded': 1}",
        "non-shadow receipts 0",
        "unknown 0",
    ]
