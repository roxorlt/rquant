from __future__ import annotations

import os
import plistlib
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LAUNCHD_DIR = ROOT / "deploy" / "launchd"
EXECUTABLE = "/Users/roxor/brain/30-projects/rQuant/.venv/bin/rquant"
WORKING_DIRECTORY = "/Users/roxor/brain/30-projects/rQuant"
EXPECTED_ROOT_ARGUMENTS = ["--expected-checkout-root", WORKING_DIRECTORY]


@pytest.mark.parametrize(
    ("name", "label", "command"),
    [
        (
            "com.roxor.rquant-lab-scheduler.plist",
            "com.roxor.rquant-lab-scheduler",
            "lab-scheduler",
        ),
        (
            "com.roxor.rquant-lab-worker.plist",
            "com.roxor.rquant-lab-worker",
            "lab-worker",
        ),
        (
            "com.roxor.rquant-lab-finalizer.plist",
            "com.roxor.rquant-lab-finalizer",
            "lab-finalizer",
        ),
    ],
)
def test_lab_launchd_plists_are_private_bounded_daemons(
    name: str,
    label: str,
    command: str,
) -> None:
    path = LAUNCHD_DIR / name
    with path.open("rb") as stream:
        document = plistlib.load(stream)

    assert document["Label"] == label
    assert document["ProgramArguments"][:4] == [
        EXECUTABLE,
        command,
        *EXPECTED_ROOT_ARGUMENTS,
    ]
    assert document["WorkingDirectory"] == WORKING_DIRECTORY
    assert document["RunAtLoad"] is True
    assert document["KeepAlive"] == {"SuccessfulExit": False}
    assert document["ThrottleInterval"] >= 10
    assert document["ExitTimeOut"] >= 30
    assert document["ProcessType"] == "Background"
    assert document["Umask"] == 0o077
    assert document["StandardOutPath"].startswith(f"{WORKING_DIRECTORY}/logs/")
    assert document["StandardErrorPath"].startswith(f"{WORKING_DIRECTORY}/logs/")
    assert set(document.get("EnvironmentVariables", {})) == {"PATH"}
    serialized = path.read_text(encoding="utf-8")
    assert "SECRET" not in serialized
    assert "KEY=" not in serialized


def test_lab_worker_launchd_uses_configured_stable_identity() -> None:
    path = LAUNCHD_DIR / "com.roxor.rquant-lab-worker.plist"
    with path.open("rb") as stream:
        document = plistlib.load(stream)

    assert document["ProgramArguments"] == [
        EXECUTABLE,
        "lab-worker",
        *EXPECTED_ROOT_ARGUMENTS,
        "--worker-id",
        "rquant-mac-primary",
    ]


@pytest.mark.skipif(
    not Path(EXECUTABLE).is_file() or not Path(WORKING_DIRECTORY).is_dir(),
    reason="owner Mac launchd runtime is unavailable",
)
def test_lab_launchd_exact_runtime_rejects_editable_import_from_other_worktree(
    tmp_path: Path,
) -> None:
    path = LAUNCHD_DIR / "com.roxor.rquant-lab-worker.plist"
    with path.open("rb") as stream:
        document = plistlib.load(stream)
    editable_site = tmp_path / "editable-site"
    editable_site.mkdir()
    (editable_site / "rquant-editable.pth").write_text(
        f"{ROOT / 'src'}\n",
        encoding="utf-8",
    )
    bootstrap_site = tmp_path / "bootstrap-site"
    bootstrap_site.mkdir()
    (bootstrap_site / "sitecustomize.py").write_text(
        f"import site\nsite.addsitedir({str(editable_site)!r})\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(bootstrap_site)
    environment["LAB_SCHEDULER_WORKER_IDS"] = "rquant-mac-primary"
    environment["__PYVENV_LAUNCHER__"] = f"{WORKING_DIRECTORY}/.venv/bin/python"

    result = subprocess.run(
        [*document["ProgramArguments"], "--once"],
        cwd=document["WorkingDirectory"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode != 0
    assert "package root" in result.stdout + result.stderr


def test_lab_launchd_plists_pass_plutil_lint() -> None:
    paths = sorted(LAUNCHD_DIR.glob("com.roxor.rquant-lab-*.plist"))
    assert len(paths) == 3
    result = subprocess.run(
        ["plutil", "-lint", *(str(path) for path in paths)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
