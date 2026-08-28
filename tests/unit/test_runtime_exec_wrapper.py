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
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from rquant.runtime_exec_wrapper import __main__ as wrapper_main
from rquant.runtime_exec_wrapper import _verify
from tests.support import signal_family_private_root as _private_root

# These modules walk real ancestor chains and refuse a group- or world-writable directory.
# pytest's `tmp_path` is rooted at `TMPDIR`, which on Linux defaults to a sticky `1777`
# `/tmp`, so a bare `pytest` run would fail here with messages that never mention the
# temporary directory. Rebinding both fixture names roots every temporary directory of this
# module in a verified-private `$HOME` root.
signal_family_private_root = _private_root.signal_family_private_root
tmp_path = _private_root.tmp_path

ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-runtime-exec-pyz.py"
SYSTEMD = ROOT / "deploy" / "systemd"
#: The one role no unit names and nothing else invokes: the `daily` HYBRID adapter of
#: `authority.md` L200, whose caller argv count is 0.
UNIT_LESS_ROLES = frozenset({"daily"})
#: Roles `deploy/libexec/rquant-workload-arbiter` invokes itself, before it execs the unit's
#: own child. They are in the wrapper allowlist without ever appearing in an `ExecStart`, so
#: the allowlist is no longer the unit set alone — it is the unit set plus these, exactly.
ARBITER_INVOKED_ROLES = frozenset({"workload_admission"})
class ParserlessProbe(NamedTuple):
    """How one module with no argparse parser answers "is this argv mine?".

    `attribute` is the function it does its real work in, `stub` builds a replacement for it
    so the answer is about the argv rather than the verdict, and `refusal` is what the module
    returns for an argv that is not its own. Both are per module: `rquant.workload_isolation`
    prints usage and answers 2, while `rquant.lab_formal_runtime_entry` reports a failed
    wrapper binding and answers 1.
    """

    attribute: str
    stub: Any
    refusal: int


#: Policy modules that answer "is this argv mine?" without argparse. Held closed by
#: `test_every_parserless_policy_module_is_registered_with_its_probe`: a new module without a
#: parser has to add a line here, and one that grows a parser has to remove its line.
PARSERLESS_MODULE_PROBES = {
    "rquant.workload_isolation": ParserlessProbe(
        "check_research_workload_admission",
        lambda module: (
            lambda: module.WorkloadCheck(
                name="research-admission", status="ok", summary="stubbed"
            )
        ),
        2,
    ),
    "rquant.lab_formal_runtime_entry": ParserlessProbe(
        "run_formal_finalizer_session",
        lambda _module: (lambda _binding: 0),
        1,
    ),
}
ROLE = "strategy_live"
MODULE = "rquant.runtime_service_main"
INSTANCES = ("svc-" + "a" * 64, "svc-" + "b" * 64)
CONTROL_ROOT = "/home/lighthouse/rquant/data/runtime/control/strategies"

#: What the generation's own module prints when the frozen bootstrap actually reaches it.
#: `textwrap` is a pure-stdlib module the wrapper itself never imports, so it is only
#: importable here if the child kept the interpreter's own baseline on `sys.path`.
CHILD_PROBE = """\
import json
import os
import sys
import textwrap

import marker
import rquant


def main(argv=None):
    print(json.dumps({
        "outcome": "ran",
        "textwrap": textwrap.dedent("  x").strip(),
        "rquant_file": rquant.__file__,
        "rquant_origin": rquant.ORIGIN,
        "marker_file": marker.__file__,
        "argv": sys.argv,
        "main_argv": list(argv or []),
        "cwd": os.getcwd(),
        "sys_path": sys.path,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
"""


def _canonical(value: Any, *, newline: bool = False) -> bytes:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return payload + b"\n" if newline else payload


class World:
    """A complete root-owned runtime world, rooted at `tmp_path`."""

    def __init__(
        self,
        root: Path,
        *,
        profile_role_overrides: dict[str, Any] | None = None,
        whole_policy: bool = False,
        omit_manifest_for: frozenset[str] = frozenset(),
        module_source: str = CHILD_PROBE,
    ) -> None:
        self.module_source = module_source
        self.omit_manifest_for = omit_manifest_for
        self.whole_policy = whole_policy
        self.profile_role_overrides = dict(profile_role_overrides or {})
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
        # A real, importable `rquant.runtime_service_main` inside the generation. The child
        # must reach this one and the standard library, and never the checkout's copy.
        package = staging / "app" / "rquant"
        package.mkdir()
        (package / "__init__.py").write_text('ORIGIN = "generation"\n', encoding="utf-8")
        for module in sorted(self._policy_modules()):
            (package / f"{module}.py").write_text(self.module_source, encoding="utf-8")
        (staging / "app" / "main.py").write_text("VALUE = 2\n", encoding="utf-8")
        # Per-instance service manifests live inside the generation, so they are covered by
        # its full manifest and hash-verified with the rest of the code.
        manifests = staging / "manifests"
        manifests.mkdir()
        for label in sorted(self._all_instances() - self.omit_manifest_for):
            (manifests / f"{label}.json").write_text(
                f'{{"service_id": "{label}"}}\n', encoding="utf-8"
            )
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
            "roles": self._profile_roles(),
            "manifest_schema": {"schema_id": "rquant-full-manifest/v1"},
        }

    def _policy_modules(self) -> set[str]:
        return {
            role["module"].rsplit(".", 1)[-1] for role in self._profile_roles().values()
        }

    def _all_instances(self) -> set[str]:
        labels: set[str] = set()
        for role in self._profile_roles().values():
            labels.update(role["instances"])
        return labels

    def _profile_roles(self) -> dict[str, Any]:
        if not self.whole_policy:
            return {
                ROLE: {
                    "module": MODULE,
                    "environment_allowlist": ["LANG", "LC_ALL", "TZ"],
                    "instances": list(INSTANCES),
                    "service_kind": ROLE,
                    "control_root": CONTROL_ROOT,
                    "once": False,
                    "module_arguments": [],
                    **self.profile_role_overrides,
                }
            }
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        return {
            entry.name: {
                "module": entry.module,
                "environment_allowlist": list(entry.environment_allowlist),
                "instances": (
                    sorted(
                        "svc-" + hashlib.sha256(f"{entry.name}-{index}".encode()).hexdigest()
                        for index in range(2)
                    )
                    if entry.instanced
                    else []
                ),
                "service_kind": entry.service_kind,
                "control_root": entry.control_root,
                "once": entry.once,
                "module_arguments": list(entry.module_arguments),
            }
            for entry in PRODUCTION_ROLE_POLICY
        }

    def _slot_roles(self, generation: Path) -> dict[str, Any]:
        if not self.whole_policy:
            return {ROLE: self._role_spec(generation)}
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        return {
            entry.name: {**self._role_spec(generation), "module": entry.module}
            for entry in PRODUCTION_ROLE_POLICY
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
            "roles": {
                name: {"module": spec["module"]}
                for name, spec in self._slot_roles(
                    self.generation_root / "placeholder"
                ).items()
            },
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
            "commit": "a1b2c3d4" * 5,
            "full_manifest_hash": generation_id,
            "profile_id": self.profile_id,
            "roles": self._slot_roles(generation),
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
            "instance": (
                self._profile_roles()[role]["instances"][0]
                if self._profile_roles().get(role, {}).get("instances")
                else None
            ),
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


def _build_world(tmp_path: Path, **overrides: Any) -> World:
    """A fresh, internally consistent world. Profile mutations rebuild everything.

    Changing a profile role changes `profile_id`, which the record and the generation
    manifest both carry, so a mutation cannot be patched in afterwards — the world is built
    from the mutated policy instead, exactly as a real profile version would be.
    """

    root = tmp_path / f"root-{len(list(tmp_path.iterdir()))}"
    root.mkdir()
    built = World(root, **overrides)
    built.build()
    return built


@pytest.fixture()
def world(tmp_path: Path) -> World:
    return _build_world(tmp_path)


# ---------------------------------------------------------------------------------------
# The unit contract: one role literal, nothing else
# ---------------------------------------------------------------------------------------


class TestArgumentTransport:
    def test_the_wrapper_accepts_a_role_literal_with_an_optional_instance_label(
        self,
    ) -> None:
        assert wrapper_main.parse_role(["pyz", "--role", ROLE]) == (ROLE, None)
        assert wrapper_main.parse_role(
            ["pyz", "--role", ROLE, "--instance", INSTANCES[0]]
        ) == (ROLE, INSTANCES[0])

    @pytest.mark.parametrize(
        "argv",
        [
            ["pyz"],
            ["pyz", ROLE],
            ["pyz", "--role"],
            ["pyz", "--role", ROLE, "--manifest", "/x.json"],
            ["pyz", "--expected-kind", ROLE],
            ["pyz", "--role", ROLE, "--instance"],
            ["pyz", "--role", ROLE, "--control-root", "/x"],
            ["pyz", "--role", ROLE, "--instance", "../escape"],
            ["pyz", "--role", ROLE, "--instance", "UPPER"],
            ["pyz", "--role", ROLE, "--instance", ""],
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
        """Both directions, with the roles no unit names spelled out rather than excused.

        Every role a unit passes has to be allowlisted, or the unit cannot start. The other
        direction used to hold implicitly — the allowlist was the unit set plus `daily` —
        and R3-A adds the first role that is reached without a unit at all:
        `workload_admission` is what the arbiter execs before the unit's own child. Relaxing
        the equality to "every unit role is allowlisted" would let any future name sit in the
        allowlist unnoticed, so the roles reached some other way are an explicit set that
        takes part in the equality instead.
        """

        roles = _verify.PROTECTED_ROLES
        unit_roles = {
            match.group(1)
            for path in sorted(SYSTEMD.glob("*.service"))
            for match in re.finditer(
                r"--role ([a-z][a-z0-9_]*)", path.read_text(encoding="utf-8")
            )
        }

        assert list(roles) == sorted(roles)
        assert len(set(roles)) == len(roles)
        assert unit_roles, "no unit named a role — the ExecStart shape must have changed"
        assert unit_roles <= set(roles), sorted(unit_roles - set(roles))
        assert set(roles) == unit_roles | UNIT_LESS_ROLES | ARBITER_INVOKED_ROLES
        assert not unit_roles & (UNIT_LESS_ROLES | ARBITER_INVOKED_ROLES)


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

        assert argv == (
            launch["python_path"],
            "-I",
            "-S",
            "-c",
            "BOOTSTRAP",
            ROLE,
            INSTANCES[0],
        )

    def test_an_unknown_role_refuses(self, world: World) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="allowlisted"):
            world.resolve("unknown_role")

    def test_a_role_the_generation_does_not_declare_refuses(self, world: World) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="does not declare"):
            world.resolve("notifier")

    def test_a_role_the_profile_does_not_declare_refuses(self, world: World) -> None:
        """Mutation M5: the slot may declare a role the root-owned profile has not."""

        record = json.loads(world.authority_path.read_bytes())
        record["current_roles"]["notifier"] = record["current_roles"][ROLE]
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(
            _verify.RuntimeExecError,
            match="the runtime profile does not declare",
        ):
            world.resolve("notifier")

    def test_a_module_that_differs_between_the_record_and_the_profile_refuses(
        self,
        tmp_path: Path,
    ) -> None:
        """Mutation M6: the two root-owned documents must agree on what will be executed."""

        diverged = _build_world(
            tmp_path,
            profile_role_overrides={"module": "rquant.other_service_main"},
        )

        with pytest.raises(_verify.RuntimeExecError, match="module differs"):
            diverged.resolve()

    def test_an_instance_outside_the_root_owned_allowlist_refuses(
        self,
        world: World,
    ) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="not in the root-owned allowlist"):
            world.resolve(instance="svc-" + "c" * 64)

    def test_an_instanced_role_without_a_label_refuses(self, world: World) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="requires an instance label"):
            world.resolve(instance=None)

    def test_a_non_canonical_instance_allowlist_refuses(self, tmp_path: Path) -> None:
        unsorted = _build_world(
            tmp_path,
            profile_role_overrides={"instances": [INSTANCES[1], INSTANCES[0]]},
        )

        with pytest.raises(_verify.RuntimeExecError, match="allowlist is not canonical"):
            unsorted.resolve()

    def test_each_authorised_instance_resolves_to_its_own_launch(self, world: World) -> None:
        """Two instances of one role differ exactly in their authorised label, nowhere else."""

        first = world.resolve(instance=INSTANCES[0])
        second = world.resolve(instance=INSTANCES[1])

        assert first["instance"] == INSTANCES[0]
        assert second["instance"] == INSTANCES[1]
        assert first["module_argv"][:2] == ("--manifest", first["service_manifest"])
        assert INSTANCES[0] in first["service_manifest"]
        assert INSTANCES[1] in second["service_manifest"]
        assert first["module_argv"] != second["module_argv"]
        assert first != second
        for shared in ("role", "module", "python_path", "generation_id", "working_directory"):
            assert first[shared] == second[shared]

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
        assert "_sys.argv[1:3]" in _verify.CHILD_TRAILER

    def test_the_bootstrap_compiles(self) -> None:
        compile(_verify.frozen_bootstrap(), "<frozen-bootstrap>", "exec")

    @staticmethod
    def _run_bootstrap(world: World, call: str, *arguments: str) -> str:
        """Execute the real frozen bootstrap in a `-I -S` child, exactly as the wrapper does."""

        # Swap only the trailer, never the module body: `CHILD_TRAILER`'s own text also
        # appears inside `_verify.py` as a string literal, so a blind replace corrupts it.
        source = _verify.frozen_bootstrap()
        body = source[: -len(_verify.CHILD_TRAILER)]
        program = f"{body}\n\nimport sys as _sys\n\n{call}"
        completed = subprocess.run(
            [sys.executable, "-I", "-S", "-c", program, *arguments],
            capture_output=True,
            text=True,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout

    def test_the_bootstrap_repeats_the_validation_in_the_child(self, world: World) -> None:
        """Run the real frozen bootstrap and let it revalidate a world it was not told about."""

        call = (
            "import json as _json\n"
            "try:\n"
            "    _launch = resolve_launch(\n"
            "        _sys.argv[1],\n"
            "        instance=_sys.argv[2],\n"
            "        profile_path=_sys.argv[3],\n"
            "        authority_path=_sys.argv[4],\n"
            "        generation_root=_sys.argv[5],\n"
            "        trusted_root=_sys.argv[6],\n"
            "        expected_owner_uid=int(_sys.argv[7]),\n"
            "    )\n"
            "except RuntimeExecError as _error:\n"
            "    print(_json.dumps({'outcome': 'rejected', 'detail': str(_error)}))\n"
            "    raise SystemExit(0)\n"
            "print(_json.dumps({'outcome': 'resolved', 'module': _launch['module']}))\n"
        )
        observed = json.loads(
            self._run_bootstrap(
                world,
                call,
                ROLE,
                INSTANCES[0],
                str(world.profile_path),
                str(world.authority_path),
                str(world.generation_root),
                str(world.trusted_root),
                str(world.owner_uid),
            )
        )

        assert observed == {"outcome": "resolved", "module": MODULE}

    def test_the_child_reaches_the_standard_library_and_the_generation_module(
        self,
        world: World,
    ) -> None:
        """R2D-SPEC-01: `child_main` must insert its paths, not replace `sys.path`.

        The generation is a venv, and a venv holds no standard library. Replacing
        `sys.path` with `[app_source, *site_packages]` left the child unable to import
        `textwrap` — or `_sqlite3`, or `zlib` — and it died on the first uncached stdlib
        import. This runs the real frozen bootstrap all the way into the generation's own
        module and reads back what it could actually reach.
        """

        call = (
            "raise SystemExit(child_main(\n"
            "    _sys.argv[1],\n"
            "    _sys.argv[2],\n"
            "    profile_path=_sys.argv[3],\n"
            "    authority_path=_sys.argv[4],\n"
            "    generation_root=_sys.argv[5],\n"
            "    trusted_root=_sys.argv[6],\n"
            "    expected_owner_uid=int(_sys.argv[7]),\n"
            "))\n"
        )
        observed = json.loads(
            self._run_bootstrap(
                world,
                call,
                ROLE,
                INSTANCES[0],
                str(world.profile_path),
                str(world.authority_path),
                str(world.generation_root),
                str(world.trusted_root),
                str(world.owner_uid),
            )
        )

        assert observed["outcome"] == "ran"
        assert observed["textwrap"] == "x"
        assert observed["marker_file"].startswith(str(world.generation_path) + "/")
        assert observed["cwd"] == str(world.generation_path / "cwd")
        # `runpy(alter_sys=True)` rewrites argv[0] to the module's own file, which is
        # itself proof the module came out of the generation.
        assert observed["argv"][0].startswith(str(world.generation_path / "app") + "/")
        assert observed["argv"][1] == "--manifest"
        assert observed["argv"][2].endswith(f"/manifests/{INSTANCES[0]}.json")
        assert "--control-root" in observed["argv"]

    def test_the_child_imports_rquant_from_the_generation_not_the_checkout(
        self,
        world: World,
    ) -> None:
        call = (
            "raise SystemExit(child_main(\n"
            "    _sys.argv[1], _sys.argv[2],\n"
            "    profile_path=_sys.argv[3], authority_path=_sys.argv[4],\n"
            "    generation_root=_sys.argv[5], trusted_root=_sys.argv[6],\n"
            "    expected_owner_uid=int(_sys.argv[7]),\n"
            "))\n"
        )
        observed = json.loads(
            self._run_bootstrap(
                world,
                call,
                ROLE,
                INSTANCES[0],
                str(world.profile_path),
                str(world.authority_path),
                str(world.generation_root),
                str(world.trusted_root),
                str(world.owner_uid),
            )
        )

        assert observed["rquant_origin"] == "generation"
        assert observed["rquant_file"].startswith(str(world.generation_path / "app") + "/")
        assert not observed["rquant_file"].startswith(str(ROOT / "src"))
        assert str(ROOT / "src") not in observed["sys_path"]
        for entry in observed["sys_path"]:
            assert entry, "the child must not inherit the working directory"

    def test_the_child_import_paths_keep_the_interpreter_baseline(self, world: World) -> None:
        launch = world.resolve()
        baseline = ("/usr/lib/python311.zip", "/usr/lib/python3.11")

        paths = _verify.child_import_paths(
            launch,
            baseline=baseline,
            interpreter_roots=("/usr", "/usr"),
        )

        assert paths == (launch["app_source"], *launch["site_packages"], *baseline)

    def test_the_child_import_paths_refuse_a_foreign_entry(self, world: World) -> None:
        launch = world.resolve()

        for hostile in ("", "/home/lighthouse/rquant/src", "/tmp"):
            with pytest.raises(_verify.RuntimeExecError):
                _verify.child_import_paths(
                    launch,
                    baseline=("/usr/lib/python3.11", hostile),
                    interpreter_roots=("/usr",),
                )

    def test_the_child_refuses_an_interpreter_that_is_not_isolated(self) -> None:
        class Flags:
            isolated = 0
            no_site = 1

        with pytest.raises(_verify.RuntimeExecError, match=r"isolated"):
            _verify.assert_isolated_startup(Flags())

        Flags.isolated, Flags.no_site = 1, 0
        with pytest.raises(_verify.RuntimeExecError, match="site processing"):
            _verify.assert_isolated_startup(Flags())


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


# ---------------------------------------------------------------------------------------
# Every protected unit, every instance: one distinct, correct launch
# ---------------------------------------------------------------------------------------


class TestEveryProtectedUnitResolves:
    """Codex round-2 P1-2 acceptance: 25 units must be startable, not 0.

    Before the role policy was expanded, `PRODUCTION_ROLE_POLICY` froze the single `daily`
    role, so every one of the 25 protected units died in `select_role` with "the runtime
    profile does not declare the requested role" — a fail-closed refusal, but a production
    entry point that could never start. This builds one world declaring the whole policy and
    resolves a launch for each unit's role and each of its authorised instances.
    """

    @staticmethod
    def _instances(world: World, role: str) -> tuple[str, ...]:
        return tuple(world._profile_roles()[role]["instances"])

    def test_every_protected_role_resolves_for_every_authorised_instance(
        self,
        tmp_path: Path,
    ) -> None:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        world = _build_world(tmp_path, whole_policy=True)
        resolved: dict[tuple[str, str | None], dict[str, Any]] = {}

        for entry in PRODUCTION_ROLE_POLICY:
            role = entry.name
            labels: tuple[str | None, ...] = self._instances(world, role) or (None,)
            for label in labels:
                launch = world.resolve(role, instance=label)
                assert launch["role"] == role
                assert launch["module"] == entry.module
                assert launch["instance"] == label
                assert Path(launch["python_path"]).is_relative_to(world.generation_path)
                assert Path(launch["app_source"]).is_relative_to(world.generation_path)
                resolved[(role, label)] = launch

        instanced = sum(1 for entry in PRODUCTION_ROLE_POLICY if entry.instanced)
        assert len(resolved) == 2 * instanced + (len(PRODUCTION_ROLE_POLICY) - instanced)
        signatures = {
            (launch["role"], launch["instance"]) for launch in resolved.values()
        }
        assert len(signatures) == len(resolved), "each unit instance must be distinguishable"

    def test_the_whole_policy_world_is_built_from_the_role_table_alone(
        self,
        tmp_path: Path,
    ) -> None:
        """Adding a role must be one line in the policy, not a line here as well.

        R3-A and R3-B each add a role to the same table, and this file is where all three
        packages meet. Everything the world needs — which roles the profile and the slot
        declare, which module sources the generation carries, which instance labels are
        authorised and which service manifests answer to them — is derived from the policy,
        and this holds it that way rather than trusting that nobody hand-lists a role again.
        """

        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        world = _build_world(tmp_path, whole_policy=True)
        package = world.generation_path / "app" / "rquant"
        names = {entry.name for entry in PRODUCTION_ROLE_POLICY}

        assert set(world._profile_roles()) == names
        assert set(world._slot_roles(world.generation_path)) == names
        for entry in PRODUCTION_ROLE_POLICY:
            source = package / f"{entry.module.rsplit('.', 1)[-1]}.py"
            assert source.is_file(), entry.module
            labels = world._profile_roles()[entry.name]["instances"]
            assert bool(labels) is entry.instanced, entry.name
            for label in labels:
                assert (world.generation_path / "manifests" / f"{label}.json").is_file()

    def test_every_unit_role_is_declared_by_the_expanded_policy(self) -> None:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        declared = {entry.name for entry in PRODUCTION_ROLE_POLICY}

        assert set(_verify.PROTECTED_ROLES) == declared
        assert len(declared) == 28

    def test_the_arbiter_invoked_admission_role_is_declared_with_its_frozen_argv(
        self,
    ) -> None:
        """R3-A's half of the shared prerequisite: the role the arbiter will exec.

        `deploy/libexec/rquant-workload-arbiter` runs the research admission probe through a
        checkout interpreter today. Registering the probe as a role of its own is what lets
        that call become the same root-owned wrapper the unit's real child already goes
        through; the entry it wants is named by the profile's frozen `module_arguments`,
        because the arbiter passes nothing but the role literal.
        """

        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        entry = next(
            item for item in PRODUCTION_ROLE_POLICY if item.name == "workload_admission"
        )

        assert entry.module == "rquant.workload_isolation"
        assert entry.environment_allowlist == ("LANG", "LC_ALL", "TZ")
        assert entry.instanced is False
        assert entry.control_root == ""
        assert entry.service_kind == ""
        assert entry.once is False
        assert entry.module_arguments == ("research-admission",)

    def test_an_unauthorised_instance_is_refused_for_every_instanced_role(
        self,
        tmp_path: Path,
    ) -> None:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        world = _build_world(tmp_path, whole_policy=True)

        for entry in PRODUCTION_ROLE_POLICY:
            if not entry.instanced:
                continue
            with pytest.raises(
                _verify.RuntimeExecError,
                match="not in the root-owned allowlist",
            ):
                world.resolve(entry.name, instance="svc-" + "f" * 64)


# ---------------------------------------------------------------------------------------
# The derived argv is the one the existing module actually accepts
# ---------------------------------------------------------------------------------------


class TestDerivedModuleArgv:
    """R2D-SPEC-02(b): the wrapper execs, and the module has to accept what it is handed.

    The units used to carry `--manifest`, `--control-root`, `--expected-commit` and
    `--expected-generation` themselves — the first through a `%i`-interpolated path under
    the `current` symlink, the last two out of `runtime.env`, a file the application it
    configures writes. All four are now derived from the two root-owned documents, and the
    instance label only picks *which* authorised manifest, never where it lives.
    """

    @staticmethod
    def _real_parser(module: str) -> Any:
        """Each role's own module parser — not one fixed parser for all of them.

        Five different modules are behind the 28 roles, and three of them did not have an
        entry point at all before this round. Validating everything against
        `runtime_service_main`'s parser hid exactly that.
        """

        import importlib

        imported = importlib.import_module(module)
        builder = getattr(imported, "build_parser", None)
        assert callable(builder), f"{module} exposes no build_parser"
        return builder()

    @staticmethod
    def _assert_parserless_module_accepts(
        module: str,
        argv: tuple[str, ...],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The same question for a module that answers it without argparse.

        `rquant.workload_isolation` has no parser: `main` compares the argv tuple literally
        and answers 2 to anything else, so "the module accepts what the wrapper hands it" is
        checked against that answer, with the probe itself stubbed out — the point here is
        the argv contract, not the verdict. Adding the wrong literals to the role policy
        would leave the arbiter's admission call exiting 2 forever, which the arbiter reads
        as a rejected research plane.
        """

        import importlib

        probe = PARSERLESS_MODULE_PROBES[module]
        imported = importlib.import_module(module)
        assert getattr(imported, "build_parser", None) is None, module
        monkeypatch.setattr(imported, probe.attribute, probe.stub(imported))

        assert imported.main(list(argv)) == 0
        assert imported.main([*argv, "unexpected"]) == probe.refusal
        assert imported.main([]) == probe.refusal

    def test_every_role_argv_is_accepted_by_its_own_module_parser(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        world = _build_world(tmp_path, whole_policy=True)
        accepted: dict[str, int] = {}
        skipped: list[str] = []

        for entry in PRODUCTION_ROLE_POLICY:
            labels: tuple[str | None, ...] = (
                tuple(world._profile_roles()[entry.name]["instances"]) or (None,)
            )
            for label in labels:
                argv = world.resolve(entry.name, instance=label)["module_argv"]
                if not argv:
                    # `daily` is the HYBRID adapter of authority.md L200: "caller argv
                    # count 0". It is the one role with no unit in this package.
                    skipped.append(entry.name)
                    continue
                if not entry.control_root:
                    # An arbiter-invoked role carries only its own frozen literals: there is
                    # no instance, so none of the derived arguments below exist.
                    self._assert_parserless_module_accepts(entry.module, argv, monkeypatch)
                    accepted[entry.name] = accepted.get(entry.name, 0) + 1
                    continue
                parsed = self._real_parser(entry.module).parse_args(list(argv))
                assert str(parsed.manifest).endswith(f"/manifests/{label}.json")
                assert str(parsed.control_root).endswith(f"/{label}")
                assert parsed.expected_generation == world.generation_path.name
                if entry.module_arguments:
                    assert parsed.mode == entry.module_arguments[1]
                if entry.service_kind:
                    assert [kind.value for kind in parsed.expected_kind] == [
                        entry.service_kind
                    ]
                accepted[entry.name] = accepted.get(entry.name, 0) + 1

        assert skipped == ["daily"]
        assert len(accepted) == len(PRODUCTION_ROLE_POLICY) - 1
        assert sum(accepted.values()) == sum(
            2 if entry.instanced else 1
            for entry in PRODUCTION_ROLE_POLICY
            if entry.control_root or entry.module_arguments
        )

    def test_every_parserless_policy_module_is_registered_with_its_probe(self) -> None:
        """The acceptance table above has to keep covering the policy, or it goes quiet.

        A module with no `build_parser` that is missing from the table would raise `KeyError`
        rather than assert, and one that grows a parser later would silently stop being
        checked the parser-free way. Both are spelled out here so a new role is one line of
        data in each table and nothing else.
        """

        import importlib

        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        parserless = {
            entry.module
            for entry in PRODUCTION_ROLE_POLICY
            if getattr(importlib.import_module(entry.module), "build_parser", None) is None
        }

        assert set(PARSERLESS_MODULE_PROBES) == parserless
        for module, probe in PARSERLESS_MODULE_PROBES.items():
            assert callable(getattr(importlib.import_module(module), probe.attribute)), module

    def test_each_policy_module_really_exposes_the_entry_the_wrapper_assumes(self) -> None:
        """The five modules behind the 28 roles each have a real, argv-reading entry.

        The last assertion runs the wrapper's own static gate over the module's *production*
        source. Everywhere else in this file the generation carries `CHILD_PROBE`, so
        `assert_module_entry_contract` never sees what will actually be published; a module
        that grew a keyword-only `main` or lost its `__main__` block would pass every other
        test here and then be refused on the host, at exit 78, at start time.
        """

        import importlib
        import inspect

        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        for module in sorted({entry.module for entry in PRODUCTION_ROLE_POLICY}):
            imported = importlib.import_module(module)
            entry = getattr(imported, "main", None)
            assert callable(entry), module
            parameters = list(inspect.signature(entry).parameters.values())
            assert parameters, f"{module}.main takes no positional argv"
            assert parameters[0].kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ), f"{module}.main ignores argv"
            source = inspect.getsource(imported)
            assert '__name__ == "__main__"' in source, module
            _verify.assert_module_entry_contract(source, expects_argv=True)

    def test_the_derived_paths_are_verbatim_from_the_authority_records(
        self,
        world: World,
    ) -> None:
        launch = world.resolve(instance=INSTANCES[1])
        argv = list(launch["module_argv"])

        assert argv[argv.index("--manifest") + 1] == str(
            world.generation_path / "manifests" / f"{INSTANCES[1]}.json"
        )
        assert argv[argv.index("--control-root") + 1] == f"{CONTROL_ROOT}/{INSTANCES[1]}"
        assert argv[argv.index("--expected-generation") + 1] == world.generation_path.name
        assert argv[argv.index("--expected-commit") + 1] == "a1b2c3d4" * 5

    def test_no_derived_argument_carries_a_template_or_environment_expansion(
        self,
        tmp_path: Path,
    ) -> None:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        world = _build_world(tmp_path, whole_policy=True)

        for entry in PRODUCTION_ROLE_POLICY:
            for label in world._profile_roles()[entry.name]["instances"] or [None]:
                for argument in world.resolve(entry.name, instance=label)["module_argv"]:
                    assert "%i" not in argument
                    assert "${" not in argument
                    assert "$" not in argument

    def test_the_manifest_lives_under_the_root_owned_generation(self, world: World) -> None:
        launch = world.resolve()

        assert Path(launch["service_manifest"]).is_relative_to(world.generation_path)
        assert Path(launch["service_manifest"]).is_file()

    def test_a_manifest_outside_the_generation_full_manifest_refuses(
        self,
        tmp_path: Path,
    ) -> None:
        """The label is authorised, but no manifested file answers to it."""

        stray = "svc-" + "e" * 64
        starved = _build_world(
            tmp_path,
            profile_role_overrides={"instances": sorted([*INSTANCES, stray])},
            omit_manifest_for=frozenset({stray}),
        )

        with pytest.raises(
            _verify.RuntimeExecError,
            match="not covered by the generation full manifest",
        ):
            starved.resolve(instance=stray)

    def test_a_symlinked_service_manifest_refuses(self, world: World) -> None:
        target = world.generation_path / "manifests" / f"{INSTANCES[0]}.json"
        elsewhere = world.root / "escaped.json"
        elsewhere.write_text('{"service_id": "escaped"}\n', encoding="utf-8")
        parent = stat.S_IMODE(target.parent.stat().st_mode)
        target.parent.chmod(0o755)
        target.unlink()
        target.symlink_to(elsewhere)
        target.parent.chmod(parent)

        with pytest.raises(_verify.RuntimeExecError, match="symlink"):
            world.resolve(instance=INSTANCES[0])

    def test_a_record_commit_that_is_not_a_sha_refuses(self, world: World) -> None:
        record = json.loads(world.authority_path.read_bytes())
        record["current_commit"] = "untrusted-audit-prose"
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(_verify.RuntimeExecError, match="forwardable commit sha"):
            world.resolve()

    def test_a_role_without_a_control_root_receives_its_own_module_arguments(
        self,
        tmp_path: Path,
    ) -> None:
        """R3-0: an arbiter-invoked role has no control root but still needs its literals.

        `workload_admission` is reached as `--role workload_admission` and nothing else, so
        the one thing that tells `rquant.workload_isolation` which of its entries to run is
        the frozen `module_arguments` of the root-owned profile. Dropping them here left
        that role no way to say what it is, which is why the arbiter had to keep calling a
        checkout interpreter instead.
        """

        world = _build_world(
            tmp_path,
            profile_role_overrides={
                "control_root": "",
                "instances": [],
                "module_arguments": ["research-admission"],
            },
        )

        assert world.resolve(ROLE, instance=None)["module_argv"] == ("research-admission",)

    def test_a_role_with_neither_a_control_root_nor_arguments_receives_no_argv(
        self,
        tmp_path: Path,
    ) -> None:
        """`daily` is still the HYBRID adapter of authority.md L200: caller argv count 0."""

        world = _build_world(tmp_path, whole_policy=True)

        assert world.resolve("daily", instance=None)["module_argv"] == ()

    @pytest.mark.parametrize(
        "arguments",
        [["research-admission", ""], ["research-admission", 7], "research-admission"],
    )
    def test_a_role_without_a_control_root_still_validates_its_arguments(
        self,
        tmp_path: Path,
        arguments: Any,
    ) -> None:
        """The no-control-root path must not be a hole in the argument type check."""

        world = _build_world(
            tmp_path,
            profile_role_overrides={
                "control_root": "",
                "instances": [],
                "module_arguments": arguments,
            },
        )

        with pytest.raises(_verify.RuntimeExecError, match="module arguments are invalid"):
            world.resolve(ROLE, instance=None)

    def test_the_child_execs_into_the_module_with_the_derived_argv(
        self,
        world: World,
    ) -> None:
        """A real exec through the frozen bootstrap into a stub module's entry point."""

        program = _verify.frozen_bootstrap()
        body = program[: -len(_verify.CHILD_TRAILER)]
        call = (
            "raise SystemExit(child_main(\n"
            "    _sys.argv[1], _sys.argv[2],\n"
            "    profile_path=_sys.argv[3], authority_path=_sys.argv[4],\n"
            "    generation_root=_sys.argv[5], trusted_root=_sys.argv[6],\n"
            "    expected_owner_uid=int(_sys.argv[7]),\n"
            "))\n"
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                f"{body}\n\nimport sys as _sys\n\n{call}",
                ROLE,
                INSTANCES[0],
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

        assert observed["outcome"] == "ran"
        argv = observed["argv"]
        assert argv[argv.index("--manifest") + 1].endswith(f"/manifests/{INSTANCES[0]}.json")
        assert argv[argv.index("--control-root") + 1] == f"{CONTROL_ROOT}/{INSTANCES[0]}"
        assert argv[argv.index("--expected-kind") + 1] == ROLE


# ---------------------------------------------------------------------------------------
# R2D-SPEC-08: a module with no entry, or one that ignores argv, must be refused
# ---------------------------------------------------------------------------------------


class TestModuleEntryContract:
    """`runpy.run_module` imports and returns, so a module with no `__main__` block
    *succeeds silently*. For a oneshot unit that is worse than exit 78: systemd records a
    clean run of a recovery pass that never happened. A `main` that takes only keyword
    arguments is the same hazard one step later — the derived argv is built, handed over
    and quietly dropped. Both are refused before any generation code is executed.
    """

    WITH_ENTRY = (
        "import sys\n\n\ndef main(argv=None):\n    return 0\n\n\n"
        'if __name__ == "__main__":\n    raise SystemExit(main(sys.argv[1:]))\n'
    )
    NO_ENTRY = "VALUE = 1\n\n\ndef helper():\n    return 0\n"
    KEYWORD_ONLY = (
        "def main(*, runtime_root=None):\n    return 0\n\n\n"
        'if __name__ == "__main__":\n    main()\n'
    )
    NO_GUARD = "def main(argv=None):\n    return 0\n"

    def test_a_conforming_module_is_accepted(self) -> None:
        _verify.assert_module_entry_contract(self.WITH_ENTRY, expects_argv=True)

    def test_a_module_without_a_main_entry_refuses(self) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="defines no main entry point"):
            _verify.assert_module_entry_contract(self.NO_ENTRY, expects_argv=True)

    def test_a_main_that_ignores_argv_refuses(self) -> None:
        with pytest.raises(
            _verify.RuntimeExecError,
            match="accepts no positional arguments",
        ):
            _verify.assert_module_entry_contract(self.KEYWORD_ONLY, expects_argv=True)

    def test_a_module_without_a_dunder_main_block_refuses(self) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="no __main__ entry"):
            _verify.assert_module_entry_contract(self.NO_GUARD, expects_argv=True)

    def test_a_module_that_does_not_parse_refuses(self) -> None:
        with pytest.raises(_verify.RuntimeExecError, match="does not parse"):
            _verify.assert_module_entry_contract("def main(:\n", expects_argv=True)

    def test_the_source_path_is_derived_from_the_manifest_covered_import_root(self) -> None:
        assert (
            _verify.module_source_relative_path(
                "rquant.runtime_service_main",
                app_source="/gen/abc/app",
                generation_path="/gen/abc",
            )
            == "app/rquant/runtime_service_main.py"
        )

    def test_a_module_name_that_is_not_an_identifier_path_refuses(self) -> None:
        for hostile in ("", ".rquant", "rquant..x", "rquant./etc/passwd", "rquant.x-y"):
            with pytest.raises(_verify.RuntimeExecError, match="module name is invalid"):
                _verify.module_source_relative_path(
                    hostile,
                    app_source="/gen/abc/app",
                    generation_path="/gen/abc",
                )

    def test_a_role_whose_module_has_no_entry_is_refused_end_to_end(
        self,
        tmp_path: Path,
    ) -> None:
        """The wrapper refuses rather than exec-ing into a module that would no-op."""

        starved = _build_world(tmp_path, module_source=self.NO_ENTRY)

        with pytest.raises(_verify.RuntimeExecError, match="defines no main entry point"):
            starved.resolve()

    def test_a_role_whose_entry_ignores_argv_is_refused_end_to_end(
        self,
        tmp_path: Path,
    ) -> None:
        deaf = _build_world(tmp_path, module_source=self.KEYWORD_ONLY)

        with pytest.raises(
            _verify.RuntimeExecError,
            match="accepts no positional arguments",
        ):
            deaf.resolve()

    def test_an_unmanifested_module_source_is_refused(self, world: World) -> None:
        """The module the role names has to be one of the hash-verified files."""

        record = json.loads(world.authority_path.read_bytes())
        record["current_roles"][ROLE]["module"] = "rquant.absent_module"
        world.rewrite(world.authority_path, _canonical(record))
        document = json.loads(world.profile_path.read_bytes())
        body = {key: value for key, value in document.items() if key != "profile_id"}
        body["roles"][ROLE]["module"] = "rquant.absent_module"
        document = {"profile_id": hashlib.sha256(_canonical(body)).hexdigest(), **body}
        world.rewrite(world.profile_path, _canonical(document))
        record["current_profile_id"] = document["profile_id"]
        world.rewrite(world.authority_path, _canonical(record))

        with pytest.raises(_verify.RuntimeExecError):
            world.resolve()


# ---------------------------------------------------------------------------------------
# Codex round-3 verdict 2026-08-28, RQ-WI-R2-P1-02: the finalizer reaches its own entry
# ---------------------------------------------------------------------------------------


FINALIZER_ROLE = "lab_claim_finalizer"


class TestTheFinalizerRoleReachesItsGenerationEntry:
    """`rquant-lab-claim-finalizer.service` stops being an exception to the trust chain.

    It used to run `.venv/bin/rquant runtime-code dry-run` and then
    `.venv/bin/python .../run-lab-daemon.py formal` — two checkout commands, the first of
    which decided whether the deployment was trustworthy using the tree in question. The
    unit now carries `--role lab_claim_finalizer` and nothing else, so what has to hold is
    that the role resolves, that it resolves to the generation's own copy of
    `rquant.lab_formal_runtime_entry`, and that the environment the child is handed is the
    role's allowlist rather than whatever the unit's `EnvironmentFile` happened to contain.
    """

    @staticmethod
    def _entry() -> Any:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        return next(item for item in PRODUCTION_ROLE_POLICY if item.name == FINALIZER_ROLE)

    def test_the_finalizer_role_resolves_and_execs_the_generation_entry(
        self,
        tmp_path: Path,
    ) -> None:
        """A real exec through the frozen bootstrap into the role's own module."""

        world = _build_world(tmp_path, whole_policy=True)
        entry = self._entry()
        launch = world.resolve(FINALIZER_ROLE, instance=None)

        assert launch["module"] == "rquant.lab_formal_runtime_entry"
        assert launch["instance"] is None
        assert launch["service_manifest"] is None
        assert launch["module_argv"] == entry.module_arguments
        assert launch["module_source"] == "app/rquant/lab_formal_runtime_entry.py"

        body = _verify.frozen_bootstrap()[: -len(_verify.CHILD_TRAILER)]
        call = (
            "raise SystemExit(child_main(\n"
            "    _sys.argv[1], None,\n"
            "    profile_path=_sys.argv[2], authority_path=_sys.argv[3],\n"
            "    generation_root=_sys.argv[4], trusted_root=_sys.argv[5],\n"
            "    expected_owner_uid=int(_sys.argv[6]),\n"
            "))\n"
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                f"{body}\n\nimport sys as _sys\n\n{call}",
                FINALIZER_ROLE,
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

        assert observed["outcome"] == "ran"
        assert observed["argv"][0] == str(
            world.generation_path / "app" / "rquant" / "lab_formal_runtime_entry.py"
        )
        assert observed["argv"][1:] == list(entry.module_arguments)
        assert observed["main_argv"] == list(entry.module_arguments)
        assert str(ROOT / "src") not in observed["sys_path"]

    def test_the_finalizer_child_environment_is_exactly_the_role_allowlist(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The silent-drop hazard of moving the unit behind the wrapper, pinned by name.

        `build_child_environment` starts from an empty dictionary, so a name the role does
        not list never reaches the daemon and nothing says so. `APP_ENV` and
        `RQUANT_DISABLE_DOTENV` are listed and must arrive; `PYTHONDONTWRITEBYTECODE` is one
        the wrapper refuses outright and `formal_runtime.py` refuses again a chain deeper, so
        it must not; and whatever else the `EnvironmentFile` contributes must not either.

        The second half follows the same dictionary all the way to `execve`, because a
        correct `build_child_environment` that the wrapper then ignored would look identical
        from the first half alone.
        """

        world = _build_world(tmp_path, whole_policy=True)
        ambient = {
            "APP_ENV": "prod",
            "RQUANT_DISABLE_DOTENV": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "LANG": "C",
            "LC_ALL": "C",
            "TZ": "UTC",
            "LAB_FINALIZER_SECRET": "leak",
            "PATH": "/usr/bin:/bin",
        }
        launch = world.resolve(
            FINALIZER_ROLE,
            instance=None,
            source_environment=dict(ambient),
        )

        assert launch["environment"] == {
            "APP_ENV": "prod",
            "LANG": "C",
            "LC_ALL": "C",
            "RQUANT_DISABLE_DOTENV": "1",
            "TZ": "UTC",
            "PWD": launch["working_directory"],
        }
        assert set(self._entry().environment_allowlist) == {
            "APP_ENV",
            "LANG",
            "LC_ALL",
            "RQUANT_DISABLE_DOTENV",
            "TZ",
        }

        executed: dict[str, Any] = {}

        def capture(path: str, argv: list[str], environment: dict[str, str]) -> None:
            executed["path"] = path
            executed["argv"] = argv
            executed["environment"] = environment
            raise OSError("captured")

        monkeypatch.setattr(wrapper_main.os, "execve", capture)
        monkeypatch.setattr(wrapper_main.os, "chdir", lambda _path: None)
        monkeypatch.setattr(wrapper_main.os, "environ", dict(ambient))
        monkeypatch.setattr(
            wrapper_main,
            "resolve_launch",
            lambda role, *, instance, source_environment: world.resolve(
                role, instance=instance, source_environment=source_environment
            ),
        )

        assert wrapper_main.main(["pyz", "--role", FINALIZER_ROLE]) == (
            wrapper_main.REFUSAL_EXIT_CODE
        )
        assert executed["environment"] == launch["environment"]
        assert "PYTHONDONTWRITEBYTECODE" not in executed["environment"]
        assert "LAB_FINALIZER_SECRET" not in executed["environment"]
        assert executed["argv"][1:3] == ["-I", "-S"]
        assert executed["argv"][-1] == FINALIZER_ROLE
