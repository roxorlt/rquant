from __future__ import annotations

import ast
import json
import subprocess
import sys
import time
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
    assert 'printf \'DATA_DIR=%s/data\\n\' "${root}"' in job
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


def test_real_generation_facts_accept_exact_artifact_digest_contract() -> None:
    from rquant.strict_json import strict_model_validate_canonical_json
    from tests.formal_smoke_real_generation_support import RedactedExactFacts

    facts = strict_model_validate_canonical_json(RedactedExactFacts, _canonical_facts())

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
    assert 'printf \'RQUANT_FORMAL_EXACT_ROOT=%s\\n\' "${exact_root}"' in job
    assert '>> "${GITHUB_ENV}"' in job
    assert "uv sync --frozen" in job
    assert "openssl genpkey -algorithm ED25519" in job
    assert "chmod 700" in job
    assert "-m linux_exact" in job
    assert "-p tests.formal_smoke_real_generation_support" in job
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
        "path: ${{ runner.temp }}/rquant-formal-exact-py"
        "${{ matrix.python-version }}/test-results/"
    ) in job
    assert "path: ${{ env.RQUANT_FORMAL_EXACT_ROOT }}/test-results/" not in job
    assert "if-no-files-found: error" in job
