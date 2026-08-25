"""The fixed root-owned runtime wrapper: what it validates before it execs anything.

Codex round-2 P1-3. Every protected runtime unit now runs

    /usr/local/libexec/rquant-runtime-exec.pyz --role <literal>

instead of `.venv/bin/python -m rquant.runtime_service_main` out of the checkout, with the
expected commit and generation taken from a file the application writes and the service
manifest taken from a `%i`-interpolated path. The wrapper takes the role literal and derives
everything else from two root-owned documents, checking the code it is about to run file by
file against the generation's full manifest first.

The world below is the real on-disk shape — the same profile, record and manifest schemas
`rquant.runtime_authority` writes — rooted in `tmp_path` with the expected owner injected,
which is the only concession to running without root.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from rquant.runtime_exec_wrapper import __main__ as wrapper_main
from rquant.runtime_exec_wrapper import _verify

ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-runtime-exec-pyz.py"
ROLE = "strategy_live"
MODULE = "rquant.runtime_service_main"


def _canonical(value: Any, *, newline: bool = False) -> bytes:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return payload + b"\n" if newline else payload


class World:
    """A complete root-owned runtime world, rooted at `tmp_path`."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.trusted_root = root
        self.profile_path = root / "etc" / "rquant" / "production-runtime-profile.json"
        self.authority_path = root / "var" / "lib" / "rquant" / "runtime-authority" / "current.json"
        self.generation_root = root / "var" / "lib" / "rquant" / "runtime-authority" / "generations"
        self.owner_uid = os.getuid()

    # -- construction -------------------------------------------------------------

    def build(self) -> None:
        for directory in (
            self.profile_path.parent,
            self.authority_path.parent,
            self.generation_root,
        ):
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o755)
        self.root.chmod(0o755)
        for parent in (self.root / "etc", self.root / "var", self.root / "var" / "lib"):
            parent.chmod(0o755)
        (self.root / "var" / "lib" / "rquant").chmod(0o755)
        self._write_generation()
        self._write_profile()
        self._write_record()

    def _write_generation(self) -> None:
        staging = self.generation_root / ".staging"
        venv = staging / "venv"
        (venv / "bin").mkdir(parents=True)
        (venv / "lib" / "site-packages").mkdir(parents=True)
        (staging / "app").mkdir()
        (staging / "cwd").mkdir()
        (venv / "bin" / "python").write_text("#!/bin/sh\nexec /usr/bin/true\n", encoding="utf-8")
        (venv / "lib" / "site-packages" / "marker.py").write_text("VALUE = 1\n", encoding="utf-8")
        (staging / "app" / "main.py").write_text("VALUE = 2\n", encoding="utf-8")
        (staging / "pyvenv.cfg").write_text(
            "home = /usr/bin\ninclude-system-site-packages = false\nversion = 3.11.15\n",
            encoding="utf-8",
        )
        for path in sorted(staging.rglob("*"), reverse=True):
            if path.is_dir():
                path.chmod(0o555)
            else:
                path.chmod(0o555 if path.name == "python" else 0o444)
        staging.chmod(0o555)

        entries = []
        walked = sorted(staging.rglob("*"), key=lambda item: item.relative_to(staging).as_posix())
        for path in walked:
            relative = path.relative_to(staging).as_posix()
            info = path.lstat()
            if path.is_dir():
                entries.append(
                    {
                        "path": relative,
                        "type": "directory",
                        "owner_uid": info.st_uid,
                        "mode": stat.S_IMODE(info.st_mode),
                        "nlink": info.st_nlink,
                        "size": 0,
                        "sha256": None,
                    }
                )
                continue
            payload = path.read_bytes()
            entries.append(
                {
                    "path": relative,
                    "type": "file",
                    "owner_uid": info.st_uid,
                    "mode": stat.S_IMODE(info.st_mode),
                    "nlink": info.st_nlink,
                    "size": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
        self._staging = staging
        self._entries = entries

    def _role_spec(self, generation: Path) -> dict[str, Any]:
        return {
            "python_path": str(generation / "venv" / "bin" / "python"),
            "module": MODULE,
            "working_directory": str(generation / "cwd"),
            "app_source": str(generation / "app"),
            "site_packages": [str(generation / "venv" / "lib" / "site-packages")],
        }

    def _profile_body(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "platform": "linux-x86_64",
            "ancestors": [],
            "system_python": {
                "path": "/usr/bin/python3.11",
                "sha256": "0" * 64,
                "owner_uid": 0,
                "mode": 0o555,
            },
            "elf_loader": {
                "path": "/lib64/ld-linux-x86-64.so.2",
                "sha256": "1" * 64,
                "owner_uid": 0,
                "mode": 0o555,
            },
            "stdlib": [],
            "shared_libraries": [],
            "deploy_pyz": {
                "path": "/usr/local/libexec/rquant-production-deploy.pyz",
                "sha256": "2" * 64,
                "owner_uid": 0,
                "mode": 0o555,
            },
            "runtime_pyz": {
                "path": _verify.RUNTIME_PYZ_PATH,
                "sha256": "3" * 64,
                "owner_uid": 0,
                "mode": 0o555,
            },
            "inbox_root": "/var/lib/rquant/runtime-authority/inbox",
            "quarantine_root": "/var/lib/rquant/runtime-authority/quarantine",
            "generation_root": str(self.generation_root),
            "allowed_operations": ["publish", "rollback"],
            "roles": {ROLE: {"module": MODULE, "environment_allowlist": ["LANG", "LC_ALL", "TZ"]}},
            "manifest_schema": {"schema_id": "rquant-full-manifest/v1"},
        }

    def _write_profile(self) -> None:
        body = self._profile_body()
        document = dict(body)
        document["profile_id"] = hashlib.sha256(_canonical(body)).hexdigest()
        self.profile_id = document["profile_id"]
        self._write_root_file(self.profile_path, _canonical(document), 0o444)

    def _write_record(self) -> None:
        manifest_document = {
            "schema_id": "rquant-full-manifest/v1",
            "profile_id": self.profile_id,
            "roles": {ROLE: {"module": MODULE}},
            "entries": self._entries,
        }
        manifest_bytes = _canonical(manifest_document, newline=True)
        generation_id = hashlib.sha256(manifest_bytes).hexdigest()
        generation = self.generation_root / generation_id
        self._staging.chmod(0o700)
        self._staging.replace(generation)
        self.generation_path = generation
        self._write_root_file(generation / "full-manifest.json", manifest_bytes, 0o444)

        # The manifest is written into the generation after it is hashed, so it is not one
        # of its own entries — exactly how `runtime_authority` publishes it.
        manifest_document["entries"] = self._entries
        generation.chmod(0o555)

        slot = {
            "lifecycle": "active",
            "generation_id": generation_id,
            "generation_path": str(generation),
            "commit": "0" * 40,
            "full_manifest_hash": generation_id,
            "profile_id": self.profile_id,
            "roles": {ROLE: self._role_spec(generation)},
        }
        record: dict[str, Any] = {
            "schema_version": 1,
            "operation_id": "a" * 32,
            "sequence": 1,
            "state": "active",
        }
        for field in _verify._SLOT_FIELDS:
            record[f"current_{field}"] = slot[field]
            record[f"prior_{field}"] = None
        self._write_root_file(self.authority_path, _canonical(record), 0o444)

    @staticmethod
    def _write_root_file(path: Path, payload: bytes, mode: int) -> None:
        writable = path.parent.stat().st_mode
        path.parent.chmod(0o755)
        path.write_bytes(payload)
        path.chmod(mode)
        path.parent.chmod(stat.S_IMODE(writable))

    # -- driving ------------------------------------------------------------------

    def resolve(self, role: str = ROLE, **overrides: Any) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "profile_path": str(self.profile_path),
            "authority_path": str(self.authority_path),
            "generation_root": str(self.generation_root),
            "trusted_root": str(self.trusted_root),
            "expected_owner_uid": self.owner_uid,
            "source_environment": {"LANG": "C", "TZ": "UTC", "SECRET": "leak"},
        }
        arguments.update(overrides)
        return _verify.resolve_launch(role, **arguments)

    def rewrite(self, path: Path, payload: bytes, mode: int = 0o444) -> None:
        parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
        path.parent.chmod(0o755)
        path.chmod(0o644)
        path.write_bytes(payload)
        path.chmod(mode)
        path.parent.chmod(parent_mode)


@pytest.fixture(autouse=True)
def _restore_readonly(tmp_path: Path):  # type: ignore[no-untyped-def]
    yield
    for path in sorted(tmp_path.rglob("*"), reverse=True):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o755)


@pytest.fixture()
def world(tmp_path: Path) -> World:
    built = World(tmp_path / "root")
    (tmp_path / "root").mkdir()
    built.build()
    return built


# ---------------------------------------------------------------------------------------
# The unit contract: one role literal, nothing else
# ---------------------------------------------------------------------------------------


class TestArgumentTransport:
    def test_the_wrapper_accepts_exactly_one_role_flag(self) -> None:
        assert wrapper_main.parse_role(["pyz", "--role", ROLE]) == ROLE

    @pytest.mark.parametrize(
        "argv",
        [
            ["pyz"],
            ["pyz", ROLE],
            ["pyz", "--role"],
            ["pyz", "--role", ROLE, "--manifest", "/x.json"],
            ["pyz", "--expected-kind", ROLE],
        ],
    )
    def test_the_wrapper_refuses_any_other_argument_shape(self, argv: list[str]) -> None:
        with pytest.raises(_verify.RuntimeExecError):
            wrapper_main.parse_role(argv)

    def test_the_wrapper_refuses_a_role_outside_the_frozen_allowlist(self) -> None:
        for role in ("", "STRATEGY_LIVE", "strategy-live", "../daily", "unknown_role"):
            with pytest.raises(_verify.RuntimeExecError, match="allowlisted"):
                wrapper_main.parse_role(["pyz", "--role", role])

    def test_the_role_allowlist_is_sorted_and_covers_every_protected_unit(self) -> None:
        roles = _verify.PROTECTED_ROLES

        assert list(roles) == sorted(roles)
        assert len(set(roles)) == len(roles)
        assert "daily" in roles
        assert "strategy_live" in roles
        assert "page_control" in roles


# ---------------------------------------------------------------------------------------
# What the wrapper refuses to read
# ---------------------------------------------------------------------------------------


class TestUntrustedInputsAreNeverRead:
    def test_the_wrapper_never_names_the_mutable_runtime_environment_file(self) -> None:
        source = "\n".join(
            (Path(_verify.__file__).read_text(encoding="utf-8")),
        )

        assert "runtime.env" not in source
        assert "data/runtime/current" not in source
        assert ".venv/bin/python" not in source

    def test_the_wrapper_reads_no_environment_variable_for_authority(self) -> None:
        source = Path(_verify.__file__).read_text(encoding="utf-8")

        assert "os.environ.get" not in source
        assert "os.getenv" not in source

    def test_the_fixed_anchors_are_the_frozen_absolute_paths(self) -> None:
        assert _verify.PROFILE_PATH == "/etc/rquant/production-runtime-profile.json"
        assert _verify.AUTHORITY_PATH == "/var/lib/rquant/runtime-authority/current.json"
        assert _verify.GENERATION_ROOT == "/var/lib/rquant/runtime-authority/generations"
        assert _verify.RUNTIME_PYZ_PATH == "/usr/local/libexec/rquant-runtime-exec.pyz"


# ---------------------------------------------------------------------------------------
# The happy path, and every way it is refused
# ---------------------------------------------------------------------------------------


class TestResolveLaunch:
    def test_a_valid_world_resolves_to_the_generation_local_interpreter(
        self,
        world: World,
    ) -> None:
        launch = world.resolve()

        assert launch["role"] == ROLE
        assert launch["module"] == MODULE
        assert launch["generation_path"] == str(world.generation_path)
        assert launch["python_path"] == str(
            world.generation_path / "venv" / "bin" / "python"
        )
        assert launch["working_directory"] == str(world.generation_path / "cwd")
        assert Path(launch["python_path"]).is_relative_to(world.generation_path)

    def test_the_child_environment_is_the_profile_allowlist_and_nothing_else(
        self,
        world: World,
    ) -> None:
        launch = world.resolve()

        assert set(launch["environment"]) == {"LANG", "TZ", "PWD"}
        assert "SECRET" not in launch["environment"]
        assert "PATH" not in launch["environment"]
        assert launch["environment"]["PWD"] == launch["working_directory"]

    def test_the_child_argv_is_the_frozen_isolated_bootstrap_form(self, world: World) -> None:
        launch = world.resolve()

        argv = _verify.child_argv(launch, "BOOTSTRAP")

        assert argv == (launch["python_path"], "-I", "-S", "-c", "BOOTSTRAP", ROLE)

    def test_an_unknown_role_refuses(self, world: World) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="allowlisted"):
            world.resolve("unknown_role")

    def test_a_role_the_generation_does_not_declare_refuses(self, world: World) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="does not declare"):
            world.resolve("notifier")

    def test_a_tampered_profile_refuses(self, world: World) -> None:
        document = json.loads(world.profile_path.read_bytes())
        document["platform"] = "linux-aarch64"
        world.rewrite(world.profile_path, _canonical(document))

        with pytest.raises(_verify.RuntimeExecError, match="profile id"):
            world.resolve()

    def test_a_world_writable_profile_refuses(self, world: World) -> None:
        world.rewrite(world.profile_path, world.profile_path.read_bytes(), mode=0o446)

        with pytest.raises(_verify.RuntimeExecError, match="mode is not"):
            world.resolve()

    def test_a_non_canonical_record_refuses(self, world: World) -> None:
        world.rewrite(world.authority_path, b" " + world.authority_path.read_bytes())

        with pytest.raises(_verify.RuntimeExecError, match="canonical"):
            world.resolve()

    def test_a_record_whose_generation_hash_does_not_match_refuses(self, world: World) -> None:
        """Repoint the slot at a directory that exists but is not the manifested one."""

        decoy = world.generation_root / ("b" * 64)
        parent = stat.S_IMODE(world.generation_root.stat().st_mode)
        world.generation_root.chmod(0o755)
        decoy.mkdir()
        (decoy / "full-manifest.json").write_bytes(
            (world.generation_path / "full-manifest.json").read_bytes()
        )
        (decoy / "full-manifest.json").chmod(0o444)
        decoy.chmod(0o555)
        world.generation_root.chmod(parent)
        record = json.loads(world.authority_path.read_bytes())
        record["current_full_manifest_hash"] = "b" * 64
        record["current_generation_id"] = "b" * 64
        record["current_generation_path"] = str(decoy)
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(_verify.RuntimeExecError, match="generation manifest hash"):
            world.resolve()

    def test_a_generation_path_outside_the_fixed_root_refuses(self, world: World) -> None:
        record = json.loads(world.authority_path.read_bytes())
        record["current_generation_path"] = str(world.root / "elsewhere")
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(_verify.RuntimeExecError, match="outside the fixed generation root"):
            world.resolve()

    def test_one_flipped_generation_byte_refuses(self, world: World) -> None:
        target = world.generation_path / "app" / "main.py"
        world.rewrite(target, b"VALUE = 3\n")

        with pytest.raises(_verify.RuntimeExecError, match="node changed"):
            world.resolve()

    def test_a_relaxed_generation_mode_refuses(self, world: World) -> None:
        target = world.generation_path / "app" / "main.py"
        parent = stat.S_IMODE(target.parent.stat().st_mode)
        target.parent.chmod(0o755)
        target.chmod(0o644)
        target.parent.chmod(parent)

        with pytest.raises(_verify.RuntimeExecError, match="mode changed"):
            world.resolve()

    def test_a_missing_generation_node_refuses(self, world: World) -> None:
        target = world.generation_path / "app" / "main.py"
        parent = stat.S_IMODE(target.parent.stat().st_mode)
        target.parent.chmod(0o755)
        target.unlink()
        target.parent.chmod(parent)

        with pytest.raises(_verify.RuntimeExecError, match="missing"):
            world.resolve()

    def test_a_pyvenv_that_enables_system_site_packages_refuses(self, world: World) -> None:
        target = world.generation_path / "pyvenv.cfg"
        payload = b"home = /usr/bin\ninclude-system-site-packages = true\nversion = 3.11.15\n"
        world.rewrite(target, payload)
        # Keep the manifest honest about the new bytes so the pyvenv rule is what fires.
        entries = json.loads((world.generation_path / "full-manifest.json").read_bytes())
        for entry in entries["entries"]:
            if entry["path"] == "pyvenv.cfg":
                entry["sha256"] = hashlib.sha256(payload).hexdigest()
                entry["size"] = len(payload)
        record = json.loads(world.authority_path.read_bytes())
        manifest_bytes = _canonical(entries, newline=True)
        world.rewrite(world.generation_path / "full-manifest.json", manifest_bytes)
        record["current_full_manifest_hash"] = hashlib.sha256(manifest_bytes).hexdigest()
        record["current_generation_id"] = record["current_full_manifest_hash"]
        record["current_generation_path"] = str(
            world.generation_root / record["current_generation_id"]
        )
        parent = stat.S_IMODE(world.generation_root.stat().st_mode)
        world.generation_root.chmod(0o755)
        world.generation_path.chmod(0o700)
        world.generation_path.replace(
            world.generation_root / record["current_generation_id"]
        )
        (world.generation_root / record["current_generation_id"]).chmod(0o555)
        world.generation_root.chmod(parent)
        world.generation_path = world.generation_root / record["current_generation_id"]
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(_verify.RuntimeExecError, match="system site-packages"):
            world.resolve()

    def test_a_rolled_back_but_inactive_slot_refuses(self, world: World) -> None:
        record = json.loads(world.authority_path.read_bytes())
        record["current_lifecycle"] = "failed"
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(_verify.RuntimeExecError, match="not active"):
            world.resolve()

    def test_a_group_writable_ancestor_refuses(self, world: World) -> None:
        (world.root / "etc").chmod(0o775)

        with pytest.raises(_verify.RuntimeExecError, match="writable"):
            world.resolve()

    def test_a_symlinked_profile_refuses(self, world: World) -> None:
        real = world.profile_path.with_name("real.json")
        parent = stat.S_IMODE(world.profile_path.parent.stat().st_mode)
        world.profile_path.parent.chmod(0o755)
        world.profile_path.replace(real)
        world.profile_path.symlink_to(real)
        world.profile_path.parent.chmod(parent)

        with pytest.raises(_verify.RuntimeExecError, match="openable regular file"):
            world.resolve()


# ---------------------------------------------------------------------------------------
# The frozen child bootstrap
# ---------------------------------------------------------------------------------------


class TestFrozenBootstrap:
    def test_the_bootstrap_is_this_module_plus_a_fixed_trailer(self) -> None:
        bootstrap = _verify.frozen_bootstrap()

        assert bootstrap.endswith(_verify.CHILD_TRAILER)
        assert "def resolve_launch(" in bootstrap
        assert "def child_main(" in bootstrap

    def test_the_bootstrap_interpolates_nothing(self) -> None:
        assert "{" not in _verify.CHILD_TRAILER
        assert "%" not in _verify.CHILD_TRAILER
        assert "_sys.argv[1]" in _verify.CHILD_TRAILER

    def test_the_bootstrap_compiles(self) -> None:
        compile(_verify.frozen_bootstrap(), "<frozen-bootstrap>", "exec")

    def test_the_bootstrap_repeats_the_validation_in_the_child(self, world: World) -> None:
        """Run the real frozen bootstrap and let it refuse a world it cannot verify."""

        program = _verify.frozen_bootstrap().replace(
            "raise SystemExit(child_main(_sys.argv[1]))",
            "raise SystemExit(_probe(_sys.argv[1]))",
        )
        probe = (
            "\ndef _probe(role):\n"
            "    import json as _json, sys as _s\n"
            "    try:\n"
            "        launch = resolve_launch(\n"
            "            role,\n"
            "            profile_path=_s.argv[2],\n"
            "            authority_path=_s.argv[3],\n"
            "            generation_root=_s.argv[4],\n"
            "            trusted_root=_s.argv[5],\n"
            "            expected_owner_uid=int(_s.argv[6]),\n"
            "        )\n"
            "    except RuntimeExecError as error:\n"
            "        print(_json.dumps({'outcome': 'rejected', 'detail': str(error)}))\n"
            "        return 0\n"
            "    print(_json.dumps({'outcome': 'resolved', 'module': launch['module']}))\n"
            "    return 0\n"
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                program.replace(
                    "\nimport sys as _sys\n", probe + "\nimport sys as _sys\n"
                ),
                ROLE,
                str(world.profile_path),
                str(world.authority_path),
                str(world.generation_root),
                str(world.trusted_root),
                str(world.owner_uid),
            ],
            capture_output=True,
            text=True,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )
        assert completed.returncode == 0, completed.stderr
        observed = json.loads(completed.stdout)

        assert observed == {"outcome": "resolved", "module": MODULE}


# ---------------------------------------------------------------------------------------
# The artifact
# ---------------------------------------------------------------------------------------


class TestDeterministicBuild:
    def _build(self, output: Path) -> str:
        completed = subprocess.run(
            [
                sys.executable,
                str(BUILD_SCRIPT),
                "--repository-root",
                str(ROOT),
                "--output",
                str(output),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout.strip()

    def test_two_builds_are_byte_identical(self, tmp_path: Path) -> None:
        first = self._build(tmp_path / "one.pyz")
        second = self._build(tmp_path / "two.pyz")

        assert first == second
        assert (tmp_path / "one.pyz").read_bytes() == (tmp_path / "two.pyz").read_bytes()

    def test_the_artifact_is_read_only_and_executable(self, tmp_path: Path) -> None:
        self._build(tmp_path / "one.pyz")

        assert stat.S_IMODE((tmp_path / "one.pyz").stat().st_mode) == 0o555

    def test_the_archive_relocates_the_package_and_carries_the_verifier(
        self,
        tmp_path: Path,
    ) -> None:
        import zipfile

        self._build(tmp_path / "one.pyz")
        with zipfile.ZipFile(tmp_path / "one.pyz") as archive:
            names = set(archive.namelist())

        assert "__main__.py" in names
        assert "rquant_runtime_exec_wrapper/_verify.py" in names
        assert "rquant_runtime_exec_wrapper/__main__.py" in names
        assert not any(name.startswith("rquant/") for name in names)

    def test_the_built_archive_refuses_an_unknown_role(self, tmp_path: Path) -> None:
        self._build(tmp_path / "one.pyz")

        completed = subprocess.run(
            [sys.executable, "-I", "-S", str(tmp_path / "one.pyz"), "--role", "nope"],
            capture_output=True,
            text=True,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )

        assert completed.returncode == wrapper_main.REFUSAL_EXIT_CODE
        assert "allowlisted" in completed.stderr

    def test_the_built_archive_refuses_without_the_fixed_root_owned_anchors(
        self,
        tmp_path: Path,
    ) -> None:
        self._build(tmp_path / "one.pyz")

        completed = subprocess.run(
            [sys.executable, "-I", "-S", str(tmp_path / "one.pyz"), "--role", ROLE],
            capture_output=True,
            text=True,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )

        assert completed.returncode == wrapper_main.REFUSAL_EXIT_CODE
        # On a host where `/etc` is not a canonical root-owned directory the anchored walk
        # refuses even earlier than the profile read; both are the same fail-closed outcome.
        assert "runtime profile" in completed.stderr or "ancestor" in completed.stderr
