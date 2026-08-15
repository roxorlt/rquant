from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path, PurePosixPath
from xml.etree import ElementTree

import pytest

_ALLOWED_TOP_LEVEL_IMPORTS = frozenset({"collections", "os"})
_DYNAMIC_IMPORTERS = frozenset(
    {
        "__import__",
        "builtins.__import__",
        "import_module",
        "importlib.import_module",
    }
)
_EXACT_TEST_NAMES = (
    "test_checkout_b_executes_real_generation_a_and_publishes_bound_artifacts",
    "test_real_generation_business_gate_rejects_unknown_audit_and_snapshot",
)


def _top_level_imports(module: ast.Module) -> set[str]:
    imported_modules = {
        alias.name.split(".")[0]
        for node in ast.walk(module)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_modules.update(
        node.module.split(".")[0]
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    return imported_modules


def _import_aliases(module: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                aliases[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            for alias in node.names:
                aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return aliases


def _literal_dynamic_imports(module: ast.Module) -> set[str]:
    aliases = _import_aliases(module)
    imported: set[str] = set()
    for node in ast.walk(module):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        argument = node.args[0]
        if not isinstance(argument, ast.Constant) or not isinstance(argument.value, str):
            continue
        importer: str | None = None
        if isinstance(node.func, ast.Name):
            importer = aliases.get(node.func.id, node.func.id)
        elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            base = aliases.get(node.func.value.id, node.func.value.id)
            importer = f"{base}.{node.func.attr}"
        if importer in _DYNAMIC_IMPORTERS:
            imported.add(argument.value)
    return imported


def _assert_fd_exec_import_graph(source: str) -> None:
    module = ast.parse(source)
    violations: list[str] = []
    unexpected_imports = _top_level_imports(module) - _ALLOWED_TOP_LEVEL_IMPORTS - {"__future__"}
    if unexpected_imports:
        violations.append(f"disallowed top-level imports: {sorted(unexpected_imports)}")
    dynamic_imports = _literal_dynamic_imports(module)
    if dynamic_imports:
        violations.append(f"dynamic literal imports: {sorted(dynamic_imports)}")
    assert not violations, "; ".join(violations)


def test_fd_exec_import_graph_is_stdlib_only() -> None:
    module_path = Path(__file__).parents[2] / "src" / "rquant" / "fd_exec.py"
    _assert_fd_exec_import_graph(module_path.read_text(encoding="utf-8"))

    source_root = module_path.parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-S",
            "-c",
            (
                "import sys\n"
                "assert 'site' not in sys.modules\n"
                "try:\n"
                "    import pydantic\n"
                "except ModuleNotFoundError:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('pydantic must not be importable')\n"
                f"sys.path.insert(0, {str(source_root)!r})\n"
                "import rquant.fd_exec\n"
                "forbidden = (\n"
                "    'pydantic', 'pydantic_core', 'pydantic_settings',\n"
                "    'rquant.config', 'rquant.runtime_code_attestation',\n"
                "    'rquant.runtime_code_generation',\n"
                ")\n"
                "assert not any(\n"
                "    name == blocked or name.startswith(f'{blocked}.')\n"
                "    for blocked in forbidden for name in sys.modules\n"
                ")\n"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_fd_exec_capability_reports_only_supported_descriptor_execution() -> None:
    from rquant.fd_exec import descriptor_execution_supported

    assert descriptor_execution_supported() is (os.execve in os.supports_fd)


@pytest.mark.parametrize(
    "source",
    [
        "__import__('pydantic')",
        "import importlib\nimportlib.import_module('pydantic')",
        "import importlib as loader\nloader.import_module('pydantic')",
        "from importlib import import_module as load\nload('pydantic')",
        "import builtins as loader\nloader.__import__('pydantic')",
    ],
)
def test_fd_exec_import_graph_rejects_literal_dynamic_imports(source: str) -> None:
    with pytest.raises(AssertionError, match="dynamic literal import"):
        _assert_fd_exec_import_graph(source)


def test_runtime_fd_exec_ci_job_pins_official_actions() -> None:
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    job = workflow.split("  runtime-fd-exec-linux:\n", maxsplit=1)[1]

    assert "name: Linux FD-exec exact (${{ matrix.python-version }})" in job
    assert (
        "uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # actions/checkout@v4"
    ) in job
    assert (
        "uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065"
        " # actions/setup-python@v5"
    ) in job
    assert (
        "uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
        " # actions/upload-artifact@v4"
    ) in job
    assert "uv sync --frozen" in job
    assert "--junitxml=" in job
    assert "if-no-files-found: error" in job


def test_paper_publication_ci_job_machine_rejects_skipped_exact_node() -> None:
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    job = workflow.split("  paper-publication-primitives:\n", maxsplit=1)[1].split(
        "\n  formal-smoke-real-generation-linux:", maxsplit=1
    )[0]
    node = (
        "tests/unit/test_paper_migration_publication.py"
        "::test_actual_platform_publication_capabilities"
    )
    report = "test-results/paper-publication-${{ matrix.os }}.xml"

    assert "os: [macos-14, ubuntu-24.04]" in job
    assert 'root="${RUNNER_TEMP}/rquant-paper-publication"' in job
    assert "printf 'DATA_DIR=%s/data\\n' \"${root}\"" in job
    assert '>> "${GITHUB_ENV}"' in job
    assert job.count(node) == 1
    assert "pytest -o addopts='' -rA" in job
    assert f'--junitxml="{report}"' in job
    assert f"python - \"{report}\" <<'PY'" in job
    assert "ElementTree.parse(report).getroot()" in job
    assert "assert len(suites) == 1" in job
    assert 'assert int(suite.attrib.get("tests", "0")) == 1' in job
    assert 'assert int(suite.attrib.get("failures", "0")) == 0' in job
    assert 'assert int(suite.attrib.get("errors", "0")) == 0' in job
    assert 'assert int(suite.attrib.get("skipped", "0")) == 0' in job
    assert "assert len(cases) == 1" in job
    assert 'assert not cases[0].findall("skipped")' in job
    assert "name: paper-publication-junit-${{ matrix.os }}" in job
    assert f"path: {report}" in job
    assert "if-no-files-found: error" in job


def _write_exact_junit(
    path: Path,
    *,
    tests: int = 2,
    skipped: int = 0,
    names: tuple[str, ...] = _EXACT_TEST_NAMES,
) -> None:
    suite = ElementTree.Element(
        "testsuite",
        tests=str(tests),
        failures="0",
        errors="0",
        skipped=str(skipped),
    )
    for index in range(tests):
        name = names[index] if index < len(names) else f"extra-{index}"
        case = ElementTree.SubElement(suite, "testcase", name=name)
        if index < skipped:
            ElementTree.SubElement(case, "skipped")
    ElementTree.ElementTree(suite).write(path, encoding="unicode")


def test_real_generation_junit_parser_rejects_changed_case_count(tmp_path: Path) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / "changed-count.xml"
    _write_exact_junit(report, tests=1)

    with pytest.raises(ValueError, match="exactly 2"):
        verify_exact_junit(report)


def test_real_generation_junit_parser_rejects_skipped_case(tmp_path: Path) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / "skipped.xml"
    _write_exact_junit(report, skipped=1)

    with pytest.raises(ValueError, match="skipped"):
        verify_exact_junit(report)


def test_real_generation_product_import_roots_use_real_contract_order() -> None:
    from pydantic import ValidationError

    from rquant.runtime_code_attestation import RuntimeCodeExecutionSpec
    from tests.formal_smoke_real_generation_support import PRODUCT_IMPORT_ROOTS

    fields = {
        "launcher_path": "release/bin/rquant",
        "working_directory": "release",
        "interpreter_path": "release/bin/python",
        "interpreter_sha256": "a" * 64,
        "python_abi": "cpython-312",
    }
    expected = ("release/runtime-site-packages", "release/src")

    assert expected == PRODUCT_IMPORT_ROOTS
    assert RuntimeCodeExecutionSpec(import_roots=expected, **fields).import_roots == expected
    with pytest.raises(ValidationError, match="ordered by canonical path"):
        RuntimeCodeExecutionSpec(import_roots=tuple(reversed(expected)), **fields)


@pytest.mark.parametrize(
    "provider",
    [
        "release/runtime-site-packages/rquant.py",
        "release/runtime-site-packages/rquant/__init__.py",
        "release/runtime-site-packages/rquant/nested/module.py",
        "release/runtime-site-packages/RQuAnT/__init__.py",
        "release/runtime-site-packages/RQUANT.PY",
    ],
)
def test_real_generation_rejects_higher_priority_rquant_provider(provider: str) -> None:
    from rquant.runtime_code_attestation import RuntimeCodeBundleEntry
    from tests.formal_smoke_real_generation_support import (
        PRODUCT_IMPORT_ROOTS,
        require_no_higher_priority_rquant_provider,
    )

    entries = (RuntimeCodeBundleEntry(path=provider, mode=0o444, content=b"VALUE = 1\n"),)

    with pytest.raises(RuntimeError, match="higher-priority rQuant provider"):
        require_no_higher_priority_rquant_provider(
            entries,
            import_roots=PRODUCT_IMPORT_ROOTS,
        )


@pytest.mark.parametrize(
    "unrelated",
    [
        "release/runtime-site-packages/not_rquant.py",
        "release/runtime-site-packages/vendor/rquant/__init__.py",
        "release/runtime-site-packages/rquant_tools/__init__.py",
    ],
)
def test_real_generation_allows_unrelated_dependency_provider(unrelated: str) -> None:
    from rquant.runtime_code_attestation import RuntimeCodeBundleEntry
    from tests.formal_smoke_real_generation_support import (
        PRODUCT_IMPORT_ROOTS,
        require_no_higher_priority_rquant_provider,
    )

    entries = (RuntimeCodeBundleEntry(path=unrelated, mode=0o444, content=b"VALUE = 1\n"),)

    require_no_higher_priority_rquant_provider(entries, import_roots=PRODUCT_IMPORT_ROOTS)


@pytest.mark.parametrize("divergence", ["untracked", "modified"])
def test_real_generation_rejects_source_set_or_head_byte_divergence(
    tmp_path: Path,
    divergence: str,
) -> None:
    from tests.formal_smoke_real_generation_support import verified_head_rquant_sources

    source = tmp_path / "source"
    package = source / "src" / "rquant"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(("git", "init", "-q"), cwd=source, check=True)
    subprocess.run(("git", "add", "src/rquant/__init__.py"), cwd=source, check=True)
    subprocess.run(
        (
            "git",
            "-c",
            "user.name=E2 Test",
            "-c",
            "user.email=e2@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ),
        cwd=source,
        check=True,
    )
    assert verified_head_rquant_sources(source) == {PurePosixPath("__init__.py"): b"VALUE = 1\n"}

    target = package / ("injected.py" if divergence == "untracked" else "__init__.py")
    target.write_text("VALUE = 2\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="differs from HEAD"):
        verified_head_rquant_sources(source)


def _canonical_facts(**updates: object) -> bytes:
    payload: dict[str, object] = {
        "artifact_digests": {"json": "d" * 64, "markdown": "e" * 64},
        "content_root_sha256": "b" * 64,
        "generation_id": "a" * 64,
        "python_version": "3.12.9",
        "receipt_digest": "c" * 64,
    }
    payload.update(updates)
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def test_real_generation_facts_accept_exact_artifact_digest_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from rquant.strict_json import strict_model_validate_canonical_json
    from tests.formal_smoke_real_generation_support import (
        RedactedExactFacts,
        verify_ci_evidence,
        write_redacted_exact_facts_if_requested,
    )

    facts_path = tmp_path / "facts.json"
    monkeypatch.setenv("RQUANT_FORMAL_SMOKE_EXACT_FACTS_PATH", str(facts_path))
    generation = SimpleNamespace(
        code_trust_evidence=SimpleNamespace(
            generation_id="a" * 64,
            content_root_sha256="b" * 64,
        )
    )
    receipt = SimpleNamespace(
        artifacts=(
            SimpleNamespace(kind="json", sha256="d" * 64),
            SimpleNamespace(kind="markdown", sha256="e" * 64),
        )
    )
    write_redacted_exact_facts_if_requested(
        generation=generation,
        receipt=receipt,
        receipt_digest="c" * 64,
    )

    report = tmp_path / "junit.xml"
    _write_exact_junit(report)
    verify_ci_evidence(
        junit=report,
        facts_path=facts_path,
        expected_python=f"{sys.version_info.major}.{sys.version_info.minor}",
    )

    payload = json.loads(facts_path.read_text(encoding="utf-8"))
    assert set(payload["artifact_digests"]) == {"json", "markdown"}
    facts = strict_model_validate_canonical_json(RedactedExactFacts, facts_path.read_bytes())

    assert facts.artifact_digests.json_sha256 == "d" * 64
    assert facts.artifact_digests.markdown_sha256 == "e" * 64


@pytest.mark.parametrize(
    "updates",
    [
        {"generation_id": "A" * 64},
        {"generation_id": "/checkout-b/generation"},
        {"content_root_sha256": "B" * 64},
        {"receipt_digest": "DUMMY_TOKEN_VALUE"},
        {"artifact_digests": ("/checkout-b/private.pem", "DUMMY_TOKEN_VALUE")},
        {"artifact_digests": {"json": "/checkout-b/private.pem", "markdown": "e" * 64}},
        {"artifact_digests": {"json": "d" * 64}},
        {
            "artifact_digests": {
                "json": "d" * 64,
                "markdown": "e" * 64,
                "csv": "f" * 64,
            }
        },
        {"source_path": "/checkout-b/src/rquant"},
    ],
)
def test_real_generation_facts_reject_nonredacted_or_noncontract_value(
    updates: dict[str, object],
) -> None:
    from rquant.strict_json import strict_model_validate_canonical_json
    from tests.formal_smoke_real_generation_support import RedactedExactFacts

    with pytest.raises(ValueError):
        strict_model_validate_canonical_json(RedactedExactFacts, _canonical_facts(**updates))


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_real_generation_junit_rejects_child_outcome_hidden_by_zero_summary(
    tmp_path: Path,
    outcome: str,
) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / f"hidden-{outcome}.xml"
    _write_exact_junit(report)
    root = ElementTree.parse(report).getroot()
    ElementTree.SubElement(root.findall("testcase")[0], outcome)
    ElementTree.ElementTree(root).write(report, encoding="unicode")

    with pytest.raises(ValueError, match=outcome):
        verify_exact_junit(report)


@pytest.mark.parametrize(
    "names",
    [
        (_EXACT_TEST_NAMES[0], _EXACT_TEST_NAMES[0]),
        (_EXACT_TEST_NAMES[0], "test_unapproved_extra_node"),
    ],
)
def test_real_generation_junit_rejects_duplicate_missing_or_extra_case(
    tmp_path: Path,
    names: tuple[str, str],
) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / "wrong-names.xml"
    _write_exact_junit(report, names=names)

    with pytest.raises(ValueError, match="testcase set"):
        verify_exact_junit(report)


def test_real_generation_junit_rejects_nested_suite_summary_mismatch(tmp_path: Path) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / "nested-mismatch.xml"
    root = ElementTree.Element(
        "testsuites", tests="1", failures="0", errors="0", skipped="0", deselected="0"
    )
    suite = ElementTree.SubElement(
        root,
        "testsuite",
        tests="2",
        failures="0",
        errors="0",
        skipped="0",
        deselected="0",
    )
    for name in _EXACT_TEST_NAMES:
        ElementTree.SubElement(suite, "testcase", name=name)
    ElementTree.ElementTree(root).write(report, encoding="unicode")

    with pytest.raises(ValueError, match="summary"):
        verify_exact_junit(report)


@pytest.mark.parametrize("field", ["failures", "errors", "skipped", "deselected"])
def test_real_generation_junit_rejects_nonzero_suite_summary_without_case_outcome(
    tmp_path: Path,
    field: str,
) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / f"false-{field}-summary.xml"
    _write_exact_junit(report)
    root = ElementTree.parse(report).getroot()
    root.set(field, "1")
    ElementTree.ElementTree(root).write(report, encoding="unicode")

    with pytest.raises(ValueError, match="summary mismatch"):
        verify_exact_junit(report)


def test_real_generation_junit_accepts_exact_named_zero_outcome_cases(tmp_path: Path) -> None:
    from tests.formal_smoke_real_generation_support import verify_exact_junit

    report = tmp_path / "exact.xml"
    _write_exact_junit(report)

    verify_exact_junit(report)


def test_real_generation_timeout_plugin_is_executable(tmp_path: Path) -> None:
    test_file = tmp_path / "test_timeout.py"
    test_file.write_text(
        "import time\n"
        "import pytest\n\n"
        "@pytest.mark.exact_timeout(0.05)\n"
        "def test_bounded():\n"
        "    time.sleep(5)\n",
        encoding="utf-8",
    )
    started = time.monotonic()
    result = subprocess.run(
        (
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests.formal_smoke_real_generation_support",
            "-o",
            "addopts=",
            "--strict-markers",
            "-q",
            str(test_file),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert time.monotonic() - started < 2
    assert "exact node exceeded" in result.stdout + result.stderr


def test_real_generation_exact_nodes_freeze_probe_and_180_second_bound() -> None:
    module_path = (
        Path(__file__).parents[1] / "integration" / "test_formal_smoke_real_generation_linux_e2e.py"
    )
    module = ast.parse(module_path.read_text(encoding="utf-8"))
    exact_tests = {
        node.name: node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in _EXACT_TEST_NAMES
    }

    assert set(exact_tests) == set(_EXACT_TEST_NAMES)
    for node in exact_tests.values():
        assert any(
            ast.unparse(decorator) == "pytest.mark.exact_timeout(180)"
            for decorator in node.decorator_list
        )
    success = ast.unparse(exact_tests[_EXACT_TEST_NAMES[0]])
    assert "('release/runtime-site-packages', 'release/src')" in success
    assert "generation.provenance_probe" in success


def test_real_generation_outer_cli_isolated_from_authority_threads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests import formal_smoke_real_generation_support as support

    captured: dict[str, object] = {}

    def run_outer(
        arguments: tuple[str, ...],
        *,
        cwd: Path,
        environment: dict[str, str],
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        captured["arguments"] = arguments
        captured["cwd"] = cwd
        captured["env"] = environment
        captured["timeout"] = timeout_seconds
        return subprocess.CompletedProcess(arguments, 0, '{"status":"ok"}\n', "")

    monkeypatch.setattr(support, "_run_isolated_checkout_command", run_outer)
    monkeypatch.setattr(
        "rquant.cli.main",
        lambda: pytest.fail("formal smoke outer CLI ran in the authority thread process"),
    )
    formal_input = support.FormalSmokeInput(
        strategy="n_shape",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
    )

    invocation = support.invoke_outer_formal_smoke_cli_from_checkout_b(
        bootstrap_config=tmp_path / "bootstrap.json",
        trusted_base=tmp_path,
        output=tmp_path / "output",
        formal_input=formal_input,
        child_environment={"RQUANT_DISABLE_DOTENV": "1"},
        timeout_seconds=90,
    )

    arguments = captured["arguments"]
    assert isinstance(arguments, tuple)
    assert arguments[:3] == (sys.executable, "-I", "-c")
    assert "from rquant import cli" in arguments[3]
    assert arguments[4] == os.fspath(Path(__file__).parents[2] / "src")
    assert captured["timeout"] <= 120
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert environment["RQUANT_DISABLE_DOTENV"] == "1"
    assert not any(name.startswith(("GIT_", "PYTHON", "DYLD_", "LD_")) for name in environment)
    assert invocation.exit_code == 0
    assert invocation.stdout == '{"status":"ok"}'


def test_outer_checkout_timeout_reaps_ignoring_descendant_process(
    tmp_path: Path,
) -> None:
    from tests import formal_smoke_real_generation_support as support

    descendant_pid_path = tmp_path / "descendant.pid"
    launcher = tmp_path / "hold-process-group.py"
    launcher.write_text(
        "import signal, subprocess, sys, time\n"
        "from pathlib import Path\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "child = subprocess.Popen((\n"
        "    sys.executable, '-c',\n"
        "    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)',\n"
        "))\n"
        "Path(sys.argv[1]).write_text(str(child.pid), encoding='ascii')\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )

    completed = support._run_isolated_checkout_command(
        (sys.executable, os.fspath(launcher), os.fspath(descendant_pid_path)),
        cwd=tmp_path,
        environment={},
        timeout_seconds=0.75,
    )

    assert completed.returncode == 124
    assert descendant_pid_path.exists()
    descendant_pid = int(descendant_pid_path.read_text(encoding="ascii"))
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            os.kill(descendant_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"timed-out checkout descendant {descendant_pid} is still alive")


def test_formal_smoke_synchronous_logging_preserves_configured_file_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import logging as logging_module

    configured: list[tuple[object, dict[str, object]]] = []
    monkeypatch.setattr(logging_module, "_initialized", False)
    monkeypatch.setattr(logging_module.logger, "remove", lambda: None)
    monkeypatch.setattr(
        logging_module.logger,
        "add",
        lambda sink, **kwargs: configured.append((sink, kwargs)),
    )

    logging_module.setup_logging(enqueue=False)

    assert len(configured) == 2
    assert configured[0][0] is sys.stderr
    file_sink, file_options = configured[1]
    assert str(file_sink).endswith("rquant_{time:YYYY-MM-DD}.log")
    assert file_options["level"] == logging_module.settings.log_level
    assert file_options["rotation"] == "00:00"
    assert file_options["retention"] == "30 days"
    assert file_options["compression"] == "zip"
    assert file_options["enqueue"] is False


def test_formal_smoke_synchronous_logging_writes_without_background_thread(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    log_root = tmp_path / "logs"
    environment = {
        "RQUANT_DISABLE_DOTENV": "1",
        "TUSHARE_TOKEN_MAIN": "0" * 32,
        "DATA_DIR": os.fspath(data_root),
        "DUCKDB_PATH": os.fspath(data_root / "rquant.duckdb"),
        "PARQUET_DIR": os.fspath(tmp_path / "parquet"),
        "LOG_DIR": os.fspath(log_root),
        "PATH": os.environ.get("PATH", os.defpath),
    }
    script = (
        "import json, threading\n"
        "from loguru import logger\n"
        "from rquant.logging import setup_logging\n"
        "before = {thread.ident for thread in threading.enumerate()}\n"
        "setup_logging(enqueue=False)\n"
        "logger.info('formal-sync-audit')\n"
        "logger.complete()\n"
        "created = [\n"
        "    thread.name for thread in threading.enumerate() if thread.ident not in before\n"
        "]\n"
        "print(json.dumps(created))\n"
    )

    completed = subprocess.run(
        (sys.executable, "-c", script),
        cwd=Path(__file__).parents[2],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == []
    log_files = tuple(log_root.glob("rquant_*.log"))
    assert len(log_files) == 1
    assert "formal-sync-audit" in log_files[0].read_text(encoding="utf-8")


def test_real_generation_exact_ci_job_is_no_skip_and_redacted() -> None:
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    job = workflow.split("  formal-smoke-real-generation-linux:\n", maxsplit=1)[1]

    assert "name: Linux real generation smoke (${{ matrix.python-version }})" in job
    assert "timeout-minutes: 6" in job
    assert 'python-version: ["3.11", "3.12"]' in job
    job_declaration = job.split("\n    steps:\n", maxsplit=1)[0]
    assert "${{ runner.temp }}" not in job_declaration
    assert 'exact_root="${RUNNER_TEMP}/rquant-formal-exact-py${{ matrix.python-version }}"' in job
    assert 'exact_root="${RQUANT_FORMAL_EXACT_ROOT}"' in job
    assert "printf 'RQUANT_FORMAL_EXACT_ROOT=%s\\n' \"${exact_root}\"" in job
    assert '>> "${GITHUB_ENV}"' in job
    assert "uv sync --frozen" in job
    assert "openssl genpkey -algorithm ED25519" in job
    assert "chmod 700" in job
    assert "-m linux_exact" in job
    assert "uv run python -m pytest -p tests.formal_smoke_real_generation_support" in job
    assert "uv run pytest -p tests.formal_smoke_real_generation_support" not in job
    assert "--junitxml=" in job
    assert "verify-ci-evidence" in job
    assert "formal-smoke-real-generation-facts" in job
    assert (
        "uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # actions/checkout@v4"
    ) in job
    assert (
        "uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065"
        " # actions/setup-python@v5"
    ) in job
    assert (
        "uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
        " # actions/upload-artifact@v4"
    ) in job
    assert (
        "path: ${{ runner.temp }}/rquant-formal-exact-py${{ matrix.python-version }}/test-results/"
    ) in job
    assert "path: ${{ env.RQUANT_FORMAL_EXACT_ROOT }}/test-results/" not in job
    assert "if-no-files-found: error" in job
