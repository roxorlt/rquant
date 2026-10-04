from __future__ import annotations

import os
import subprocess
import sys
import time
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


def test_query_cli_has_explicit_separate_build_execute_and_control_roles(capsys) -> None:
    assert find_spec("rquant.research_query.cli") is not None, (
        "configuration-free query CLI is not implemented"
    )
    cli = import_module("rquant.research_query.cli")
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    text = capsys.readouterr().out
    assert "build" in text and "serve" in text and "save-serve" in text


def test_cli_sigterm_closes_private_socket_and_exits_cleanly() -> None:
    with TemporaryDirectory(
        prefix="rq-stop-", dir="/private/tmp" if sys.platform == "darwin" else "/tmp"
    ) as directory:
        root = Path(directory)
        endpoint = root / "uds"
        endpoint.mkdir(mode=0o710)
        os.chown(endpoint, os.geteuid(), os.getegid())
        endpoint.chmod(0o710)
        socket_path = endpoint / "query.sock"
        source = Path(__file__).resolve().parents[2] / "src"
        bootstrap = (
            "import sys; sys.path.insert(0,sys.argv.pop(1)); "
            "from rquant.research_query.cli import main; raise SystemExit(main(sys.argv[1:]))"
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-B",
                "-c",
                bootstrap,
                str(source),
                "save-serve",
                "--socket",
                str(socket_path),
                "--web-uid",
                str(os.geteuid() + 1),
                "--shared-gid",
                str(os.getegid()),
                "--users",
                "alice",
                "--outbox",
                str(root / "state.sqlite"),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={"RQUANT_DISABLE_DOTENV": "1", "PATH": os.defpath},
            close_fds=True,
        )
        try:
            deadline = time.monotonic() + 5
            while (
                not socket_path.exists() and process.poll() is None and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            assert socket_path.exists(), "private synthetic service did not bind"
            process.terminate()
            assert process.wait(timeout=5) == 0
            assert not socket_path.exists()
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
