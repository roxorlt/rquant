"""Stdlib-only parent facade for isolated R07 boundary probe subprocesses."""


def _capture_parent_state() -> tuple[
    tuple[tuple[str, str], ...], tuple[str, ...], tuple[tuple[str, object], ...]
]:
    os_module = __import__("os")
    sys_module = __import__("sys")
    return (
        tuple(os_module.environ.items()),
        tuple(sys_module.path),
        tuple(sys_module.modules.items()),
    )


def _parent_snapshot() -> tuple[
    tuple[tuple[str, str], ...], tuple[str, ...], tuple[tuple[str, int], ...]
]:
    environment, path, modules = _capture_parent_state()
    return environment, path, tuple((name, id(module)) for name, module in modules)


def _restore_parent_state(
    state: tuple[tuple[tuple[str, str], ...], tuple[str, ...], tuple[tuple[str, object], ...]],
) -> None:
    os_module = __import__("os")
    sys_module = __import__("sys")
    environment, path, modules = state
    if tuple(os_module.environ.items()) != environment:
        os_module.environ.clear()
        os_module.environ.update(environment)
    if tuple(sys_module.path) != path:
        sys_module.path[:] = path

    expected_names = {name for name, _module in modules}
    for name in tuple(sys_module.modules):
        if name not in expected_names:
            del sys_module.modules[name]
    for name, module in modules:
        if sys_module.modules.get(name) is not module:
            sys_module.modules[name] = module
    if tuple(sys_module.modules) != tuple(name for name, _module in modules):
        sys_module.modules.clear()
        sys_module.modules.update(modules)

    expected = (
        environment,
        path,
        tuple((name, id(module)) for name, module in modules),
    )
    if _parent_snapshot() != expected:
        raise AssertionError("parent probe baseline could not be restored")


def _child_environment(
    environment_root: object,
    *,
    candidate_root: object | None = None,
) -> dict[str, str]:
    before = _capture_parent_state()
    try:
        os_module = __import__("os")
        path_type = __import__("pathlib").Path
        environment_root = path_type(environment_root).resolve()
        if candidate_root is None:
            candidate_root = path_type(__file__).parents[1]
        candidate_root = path_type(candidate_root).resolve(strict=True)
        candidate_src = (candidate_root / "src").resolve(strict=True)
        roots = {
            "HOME": environment_root / "home",
            "TMPDIR": environment_root / "tmp",
            "TMP": environment_root / "tmp",
            "TEMP": environment_root / "tmp",
            "DATA_DIR": environment_root / "data",
            "PARQUET_DIR": environment_root / "parquet",
            "LOG_DIR": environment_root / "logs",
        }
        for path in set(roots.values()):
            path.mkdir(parents=True, exist_ok=True)
        return {
            "LANG": "C",
            "LC_ALL": "C",
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONPATH": os_module.pathsep.join((str(candidate_src), str(candidate_root))),
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            **{name: str(path) for name, path in roots.items()},
            "DUCKDB_PATH": str(environment_root / "data" / "probe.duckdb"),
            "DUCKDB_READONLY_PATH": str(environment_root / "data" / "probe-ro.duckdb"),
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "0" * 32,
            "NOTIFY_ENABLED": "false",
        }
    finally:
        _restore_parent_state(before)


def run_boundary_probe_subprocess(
    *,
    policy_path: object,
    candidate_root: object | None = None,
    inventory_id: str,
    tmp_path: object,
    fail_on_dotenv_read: bool = False,
) -> dict[str, object]:
    before = _capture_parent_state()
    try:
        json_module = __import__("json")
        subprocess_module = __import__("subprocess")
        path_type = __import__("pathlib").Path
        temporary_directory_type = __import__("tempfile").TemporaryDirectory
        sys_module = __import__("sys")

        policy_path = path_type(policy_path)
        tmp_path = path_type(tmp_path).resolve()
        tmp_path.mkdir(parents=True, exist_ok=True)
        if candidate_root is None:
            candidate_root = path_type(__file__).parents[1]
        candidate_root = path_type(candidate_root)
        executable = path_type(sys_module.executable).resolve(strict=True)
        site_packages = (
            path_type(sys_module.prefix)
            / "lib"
            / f"python{sys_module.version_info.major}.{sys_module.version_info.minor}"
            / "site-packages"
        )
        bootstrap = (
            "import runpy, sys; "
            f"site_packages = {str(site_packages)!r}; "
            "sys.path.append(site_packages) if site_packages not in sys.path else None; "
            'entrypoint = "tests.r07_differential_probe_child"; '
            "runpy.run_module(entrypoint, run_name='__main__')"
        )
        with temporary_directory_type(
            prefix="rquant-r07-probe-env-", dir=tmp_path.parent
        ) as directory:
            environment_root = path_type(directory).resolve(strict=True)
            environment = _child_environment(
                environment_root,
                candidate_root=candidate_root,
            )
            if fail_on_dotenv_read:
                environment["RQUANT_R07_FAIL_DOTENV_READ"] = "1"
            completed = subprocess_module.run(
                [
                    str(executable),
                    "-c",
                    bootstrap,
                    "--inventory-id",
                    inventory_id,
                    "--tmp-path",
                    str(tmp_path),
                    "--policy-path",
                    str(policy_path.resolve(strict=True)),
                ],
                cwd=environment_root,
                env=environment,
                capture_output=True,
                check=False,
                text=True,
            )
        if completed.returncode != 0:
            raise AssertionError(completed.stdout + completed.stderr)
        payload = json_module.loads(completed.stdout)
        if type(payload) is not dict:
            raise AssertionError("probe child result must be a JSON object")
        return payload
    finally:
        _restore_parent_state(before)
