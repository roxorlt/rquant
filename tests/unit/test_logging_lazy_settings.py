"""#189: importing `rquant.logging` must not construct `Settings`.

The console script's import chain is `rquant.cli` -> `rquant.logging` -> `rquant.config`.
While `rquant/logging.py` bound `settings` at module level, that chain built a fully
validated `Settings` during the import itself, so `rquant runtime-authority-stage` — the
one command that has to run inside a bootstrap worktree with no `.env` (acceptance A22) —
died in the import, before `main()` could reach its early dispatch.

The fix is the shape TP9 already applied to `storage/duckdb.py` and
`page_control_service.py`: a `_settings()` read at call time plus a PEP 562 module hook, so
the object is built by the first reader instead of by the importer.

`rquant --help` deliberately keeps failing closed without configuration — that is T9-9
(`test_tp9_role_child_runtime.py::test_t9_9_cli_help_still_fails_closed_without_configuration`),
and this module pins the import, not the entry point's fail-fast.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

# Five required fields plus the dotenv switch: this is exactly the environment the Release A
# bootstrap worktree has, and nothing in it can build a `Settings`.
CONFIGURATION_FREE_ENVIRONMENT = {
    "PATH": os.environ.get("PATH", os.defpath),
    "LANG": "C",
    "RQUANT_DISABLE_DOTENV": "1",
}


def _run(argv: tuple[str, ...], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=str(cwd),
        env=dict(CONFIGURATION_FREE_ENVIRONMENT),
        capture_output=True,
        text=True,
        check=False,
    )


def test_importing_rquant_logging_never_touches_the_configuration_module(tmp_path: Path) -> None:
    """No `.env`, no required variables: the import has to succeed and stay config-free."""

    program = (
        "import sys\n"
        "import rquant.logging\n"
        "assert 'rquant.config' not in sys.modules, 'rquant.config was imported'\n"
        "sys.stdout.write('IMPORT-CONFIG-FREE\\n')\n"
    )
    result = _run((sys.executable, "-c", program), cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    assert "IMPORT-CONFIG-FREE" in result.stdout


def test_importing_rquant_cli_succeeds_without_configuration(tmp_path: Path) -> None:
    """The console script's own import chain, which is what #189 broke."""

    program = "import rquant.cli\nprint('CLI-IMPORTED')\n"
    result = _run((sys.executable, "-c", program), cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    assert "CLI-IMPORTED" in result.stdout
    assert "ValidationError" not in result.stderr


def test_console_script_stages_without_configuration(tmp_path: Path) -> None:
    """`rquant runtime-authority-stage` reaches its own parser with no settings in reach.

    The runbook had to spell this command as `python -m rquant.runtime_authority_stage`
    because the console script died in the import; with the import lazified, the entry
    point's early dispatch (`cli.py`, ahead of the fail-fast `get_settings()`) is real.
    """

    console_script = Path(sys.executable).with_name("rquant")
    argv: tuple[str, ...]
    if console_script.exists():
        argv = (str(console_script), "runtime-authority-stage", "--help")
    else:  # a venv without the entry point installed: drive `main()` the same way it would
        argv = (
            sys.executable,
            "-c",
            "import sys; sys.argv = ['rquant', 'runtime-authority-stage', '--help'];"
            " from rquant.cli import main; raise SystemExit(main())",
        )

    result = _run(argv, cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    assert "--bootstrap-from-checkout" in result.stdout
    assert "ValidationError" not in result.stderr


def test_setup_logging_still_reads_the_configured_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deferring the read must not change what is read: both sinks come from `Settings`."""

    from rquant import logging as logging_module
    from rquant.config import get_settings

    configured: list[tuple[object, dict[str, object]]] = []
    monkeypatch.setattr(logging_module, "_initialized", False)
    monkeypatch.setattr(logging_module.logger, "remove", lambda: None)
    monkeypatch.setattr(
        logging_module.logger,
        "add",
        lambda sink, **kwargs: configured.append((sink, kwargs)),
    )

    logging_module.setup_logging(enqueue=False)

    settings = get_settings()
    assert [options["level"] for _sink, options in configured] == [
        settings.log_level,
        settings.log_level,
    ]
    stream_sink, _stream_options = configured[0]
    file_sink, _file_options = configured[1]
    assert stream_sink is sys.stderr
    assert Path(str(file_sink)).parent == settings.log_dir


def test_module_attribute_still_yields_the_shared_settings_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`rquant.logging.settings` keeps resolving, and a test-bound object still wins."""

    from rquant import logging as logging_module
    from rquant.config import get_settings

    assert "settings" not in vars(logging_module)
    assert logging_module.settings is get_settings()
    assert logging_module._settings() is get_settings()

    sentinel = object()
    monkeypatch.setattr(logging_module, "settings", sentinel)
    assert logging_module._settings() is sentinel
    monkeypatch.undo()
    assert logging_module._settings() is get_settings()

    with pytest.raises(AttributeError, match="no attribute 'not_a_setting'"):
        logging_module.not_a_setting  # noqa: B018 - the access is the assertion
