from __future__ import annotations

from pathlib import Path
from uuid import UUID

from rquant.lab_job_protocol import PauseJobCommand
from rquant.lab_page_control import LabPageControlWriter
from rquant.page_control import DiscardLabArtifactZip


class _Commands:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def submit_pause(self, job_id: UUID, **kwargs: object):
        self.calls.append(("pause", (job_id, kwargs)))
        return _Result({"result": "submitted"})


class _Exports:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.discarded: list[object] = []

    def export(self, job_id: UUID):
        return _Result(
            {
                "request_id": str(UUID(int=2)),
                "job_id": str(job_id),
                "path": str(self.path),
                "byte_size": 3,
                "sha256": "a" * 64,
            }
        )

    def discard(self, receipt: object) -> None:
        self.discarded.append(receipt)


class _Result:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def model_dump(self, *, mode: str):
        assert mode == "json"
        return self.payload


def test_lab_page_control_writer_dispatches_mutations_behind_control_boundary(
    tmp_path: Path,
) -> None:
    commands = _Commands()
    exports = _Exports(tmp_path / "result.zip")
    writer = LabPageControlWriter(commands=commands, zip_exports=exports)
    job_id = UUID(int=1)

    submitted = writer.submit_command(
        PauseJobCommand(job_id=job_id, expected_version=4, reason="page"),
        interaction_key="pause:1:4",
    )
    exported = writer.export_zip(job_id)
    discarded = writer.discard_zip(
        DiscardLabArtifactZip(
            command_id="discard",
            requested_at="2026-08-03T01:00:00Z",
            request_id=UUID(int=2),
            job_id=job_id,
            path=tmp_path / "result.zip",
            byte_size=3,
            sha256="a" * 64,
        )
    )

    assert submitted == {"result": "submitted"}
    assert exported["path"] == str(tmp_path / "result.zip")
    assert discarded == {"discarded": True}
    assert commands.calls == [
        (
            "pause",
            (
                job_id,
                {
                    "expected_version": 4,
                    "reason": "page",
                    "interaction_key": "pause:1:4",
                },
            ),
        )
    ]
    assert len(exports.discarded) == 1
