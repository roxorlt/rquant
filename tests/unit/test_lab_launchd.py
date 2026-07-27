from __future__ import annotations

import os
import plistlib
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LAUNCHD_DIR = ROOT / "deploy" / "launchd"
WORKING_DIRECTORY = "/Users/roxor/brain/30-projects/rQuant"
PYTHON = f"{WORKING_DIRECTORY}/.venv/bin/python"
WRAPPER = f"{WORKING_DIRECTORY}/scripts/run-lab-daemon.py"
EXECUTABLE = f"{WORKING_DIRECTORY}/.venv/bin/rquant"
TRUSTED_GIT = "/usr/bin/git"
EXPECTED_ROOT_ARGUMENTS = ["--expected-checkout-root", WORKING_DIRECTORY]
TRUSTED_GIT_ARGUMENTS = ["--trusted-git-path", TRUSTED_GIT]
WRAPPER_ARGUMENTS = [
    PYTHON,
    "-I",
    "-S",
    WRAPPER,
    *EXPECTED_ROOT_ARGUMENTS,
    *TRUSTED_GIT_ARGUMENTS,
    "--",
]


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
    expected_prefix = [
        *WRAPPER_ARGUMENTS,
        EXECUTABLE,
        command,
        *EXPECTED_ROOT_ARGUMENTS,
        *TRUSTED_GIT_ARGUMENTS,
    ]
    assert document["ProgramArguments"][: len(expected_prefix)] == expected_prefix
    assert document["WorkingDirectory"] == WORKING_DIRECTORY
    assert document["RunAtLoad"] is True
    assert document["KeepAlive"] == {"SuccessfulExit": False}
    assert document["ThrottleInterval"] >= 10
    assert document["ExitTimeOut"] >= 30
    assert document["ProcessType"] == "Background"
    assert document["Umask"] == 0o077
    assert document["StandardOutPath"].startswith(f"{WORKING_DIRECTORY}/logs/")
    assert document["StandardErrorPath"].startswith(f"{WORKING_DIRECTORY}/logs/")
    assert document.get("EnvironmentVariables", {}) == {
        "PATH": f"{WORKING_DIRECTORY}/.venv/bin:/usr/local/bin:/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "RQUANT_TRUSTED_GIT_PATH": TRUSTED_GIT,
    }
    serialized = path.read_text(encoding="utf-8")
    assert "SECRET" not in serialized
    assert "KEY=" not in serialized


def test_lab_worker_launchd_uses_configured_stable_identity() -> None:
    path = LAUNCHD_DIR / "com.roxor.rquant-lab-worker.plist"
    with path.open("rb") as stream:
        document = plistlib.load(stream)

    assert document["ProgramArguments"] == [
        *WRAPPER_ARGUMENTS,
        EXECUTABLE,
        "lab-worker",
        *EXPECTED_ROOT_ARGUMENTS,
        *TRUSTED_GIT_ARGUMENTS,
        "--worker-id",
        "rquant-mac-primary",
    ]


@pytest.mark.skipif(
    not Path(EXECUTABLE).is_file()
    or not Path(WRAPPER).is_file()
    or not Path(WORKING_DIRECTORY).is_dir(),
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
    assert "environment injection" in result.stdout + result.stderr


@pytest.mark.skipif(
    not (ROOT / ".venv" / "bin" / "rquant").is_file(),
    reason="linked worktree runtime is unavailable",
)
def test_real_worktree_launcher_rejects_symlinked_venv_before_config() -> None:
    executable = ROOT / ".venv" / "bin" / "rquant"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment["DATA_DIR"] = "relative-data-must-not-be-read"

    result = subprocess.run(
        [
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(ROOT),
            "--trusted-git-path",
            TRUSTED_GIT,
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "physical virtualenv" in output
    assert "relative-data-must-not-be-read" not in output


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
