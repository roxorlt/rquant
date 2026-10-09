"""Original persisted publication grant: writer and physical readonly consumers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from rquant.experiment_platform import ExperimentChildAdmission, ExperimentPlatformStore
from rquant.experiment_registry import (
    ExperimentRegistry,
    ExperimentRegistryError,
    ExperimentRegistryReadonlyReader,
)
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
from rquant.lab_jobs import LabJobReader

JOB = UUID("be75c2f4-ac29-54fa-b021-d425a0b1a779")
MATERIAL = (
    Path(__file__).resolve().parents[1]
    / "fixtures/react_platform_publication/original-publication-job.json"
)


def original_copy(root: Path) -> tuple[Path, LabCommandEnvelope]:
    root.mkdir(mode=0o700)
    raw = MATERIAL.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == (
        "33b3ce3a64ec809eb597c230c7959739659b43f344066d2907c602da1f20c312"
    )
    material = json.loads(raw)
    path = root / "experiments.sqlite"
    with closing(sqlite3.connect(path)) as destination:
        destination.execute(f"PRAGMA application_id={material['application_id']}")
        destination.execute(f"PRAGMA user_version={material['user_version']}")
        destination.execute("PRAGMA foreign_keys=ON")
        for item in material["schema"]:
            destination.execute(item["sql"])
        for table in material["tables"]:
            columns = ",".join('"' + column + '"' for column in table["columns"])
            placeholders = ",".join("?" for _ in table["columns"])
            destination.executemany(
                f'INSERT INTO "{table["name"]}" ({columns}) VALUES ({placeholders})',
                table["rows"],
            )
        destination.commit()
    path.chmod(0o600)
    original = ExperimentRegistryReadonlyReader(path, managed_trust_root=root)
    intent = original.get_submission_intent_for_job(JOB)
    assert intent is not None
    return path, LabCommandEnvelope.model_validate_json(intent.envelope_json)


def facade(root: Path, path: Path) -> LabCommandSubmissionFacade:
    return LabCommandSubmissionFacade(
        reader=LabJobReader(root / "unused-jobs.sqlite"),
        spool=LabCommandSpool(root / "commands"),
        experiment_registry=ExperimentRegistryReadonlyReader(
            path, managed_trust_root=root
        ),
        clock=lambda: datetime.now(UTC),
    )


def patch_child(path: Path, changes: dict[str, object]) -> None:
    with sqlite3.connect(path) as connection:
        raw = json.loads(connection.execute(
            "SELECT payload_json FROM experiment_child_admission WHERE job_id=?",
            (str(JOB),),
        ).fetchone()[0])
        raw.update(changes)
        connection.execute(
            "UPDATE experiment_child_admission SET payload_json=? WHERE job_id=?",
            (json.dumps(raw), str(JOB)),
        )


def test_readonly_accepts_exact_original_grant_without_store_or_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "readonly"
    path, envelope = original_copy(root)
    reader = facade(root, path)
    before = (path.stat().st_ino, hashlib.sha256(path.read_bytes()).hexdigest())

    def refuse_store(*args: object, **kwargs: object) -> None:
        pytest.fail("readonly publication must not construct ExperimentPlatformStore")

    monkeypatch.setattr(ExperimentPlatformStore, "__init__", refuse_store)
    reader._validate_private_publication(envelope)
    assert before == (path.stat().st_ino, hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.mark.parametrize("changes", [
    {"publish_grant_seq": None},
    {"cancel_state": "before_publication"},
    {"request_id": str(UUID(int=11))},
    {"experiment_id": "1" * 64},
    {"command_content_hash": "2" * 64},
])
def test_writer_and_readonly_keep_original_grant_rejections(
    tmp_path: Path, changes: dict[str, object],
) -> None:
    root = tmp_path / "reject"
    path, envelope = original_copy(root)
    patch_child(path, changes)
    store = ExperimentPlatformStore(ExperimentRegistry(path, managed_trust_root=root))
    intent = store.registry.get_submission_intent_for_job(JOB)
    assert intent is not None
    with pytest.raises(PermissionError, match="persisted grant"):
        store.validate_publication(intent)
    with pytest.raises(PermissionError):
        facade(root, path)._validate_private_publication(envelope)


def test_writer_and_readonly_keep_original_pending_cancel_after_grant(tmp_path: Path) -> None:
    root = tmp_path / "pending"
    path, envelope = original_copy(root)
    patch_child(path, {"cancel_state": "pending"})
    store = ExperimentPlatformStore(ExperimentRegistry(path, managed_trust_root=root))
    intent = store.registry.get_submission_intent_for_job(JOB)
    assert intent is not None
    store.validate_publication(intent)
    facade(root, path)._validate_private_publication(envelope)


@pytest.mark.parametrize("column,value", [
    ("owner", "foreign"),
    ("hypothesis_family", "experiment-search:foreign"),
    ("experiment_id", "3" * 64),
])
def test_readonly_rejects_scalar_payload_identity_mismatch(
    tmp_path: Path, column: str, value: str,
) -> None:
    root = tmp_path / "scalar"
    path, envelope = original_copy(root)
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE experiment_child_admission SET {column}=? WHERE job_id=?",
            (value, str(JOB)),
        )
    with pytest.raises((PermissionError, ValueError)):
        facade(root, path)._validate_private_publication(envelope)


@pytest.mark.parametrize("mode", ["missing", "oversized", "duplicate", "wrong-job"])
def test_readonly_rejects_missing_or_ambiguous_full_child(tmp_path: Path, mode: str) -> None:
    root = tmp_path / mode
    path, envelope = original_copy(root)
    with sqlite3.connect(path) as connection:
        raw = connection.execute(
            "SELECT payload_json FROM experiment_child_admission WHERE job_id=?", (str(JOB),)
        ).fetchone()[0]
        if mode == "missing":
            connection.execute("DELETE FROM experiment_child_admission WHERE job_id=?", (str(JOB),))
        else:
            if mode == "oversized":
                raw += " " * (1024 * 1024)
            elif mode == "duplicate":
                raw = '{"owner":"foreign",' + raw.lstrip()[1:]
            else:
                child = ExperimentChildAdmission.model_validate_json(raw)
                raw = child.model_copy(update={"job_id": UUID(int=12)}).model_dump_json()
            connection.execute(
                "UPDATE experiment_child_admission SET payload_json=? WHERE job_id=?", (raw, str(JOB))
            )
    with pytest.raises((PermissionError, ValueError, ExperimentRegistryError)) as error:
        facade(root, path)._validate_private_publication(envelope)
    if mode == "duplicate":
        assert "duplicate JSON key" in str(error.value.__cause__)
