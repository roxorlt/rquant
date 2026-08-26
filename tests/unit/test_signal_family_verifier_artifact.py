"""The root-owned, content-addressed verifier artifact and its install-time identity.

Codex round-2 P1-4: the root verifier must not import business code from a mutable
checkout. It runs from an installed tree at
`/usr/local/lib/rquant-signal-family-verifier/<content-id>/`, entered through a fixed pyz
at `/usr/local/libexec/rquant-signal-family-verifier-v1.pyz` that carries the tree manifest
frozen inside it. The content id is the SHA-256 of that manifest's canonical bytes, so one
flipped byte anywhere under the tree is a different artifact and refuses to start.

Nothing here installs anything. Every test builds its own tree under `tmp_path` and injects
the owner it expects, which is exactly the seam production never uses: the pyz bootstrap
supplies `0`.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from rquant.signal_family_verifier_entry import _artifact as artifact

ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-signal-family-verifier-artifact.py"
LEGACY_ENTRY_SCRIPT = ROOT / "scripts" / "signal-family-root-verifier.py"


@pytest.fixture(autouse=True)
def _restore_readonly_trees(tmp_path: Path):  # type: ignore[no-untyped-def]
    """Every tree here is `0555`; hand pytest a removable directory back."""

    yield
    for path in sorted(tmp_path.rglob("*"), reverse=True):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o755)


def _tree(root: Path) -> Path:
    """A miniature installed tree with the shape the real one has."""

    site = root / "lib" / "python3.11" / "site-packages" / "rquant"
    site.mkdir(parents=True)
    (root / "pyvenv.cfg").write_text(
        "home = /usr/bin\ninclude-system-site-packages = false\nversion = 3.11.15\n",
        encoding="utf-8",
    )
    (site / "__init__.py").write_text('"""x"""\n', encoding="utf-8")
    (site / "strict_json.py").write_text("VALUE = 1\n", encoding="utf-8")
    (site.parent / "typing_extensions.py").write_text("VALUE = 2\n", encoding="utf-8")
    artifact.freeze_tree_modes(root)
    return root


def _installed(tmp_path: Path) -> tuple[Path, tuple[artifact.TreeEntry, ...], str]:
    root = _tree(tmp_path / "artifact")
    entries = artifact.build_tree_manifest(root)
    return root, entries, artifact.content_id(entries)


def _verify(root: Path, entries: tuple[artifact.TreeEntry, ...], identifier: str) -> None:
    artifact.verify_installed_tree(
        root,
        manifest=entries,
        expected_content_id=identifier,
        expected_owner_uid=os.getuid(),
        expected_owner_gid=root.stat().st_gid,
    )


class TestInstallLocations:
    def test_the_install_locations_are_the_frozen_constants(self) -> None:
        assert Path("/usr/local/lib/rquant-signal-family-verifier") == (
            artifact.ARTIFACT_INSTALL_ROOT
        )
        assert Path("/usr/local/libexec/rquant-signal-family-verifier-v1.pyz") == (
            artifact.ARTIFACT_ENTRY_PATH
        )

    def test_the_install_plan_binds_path_owner_and_mode_for_every_target(self) -> None:
        plan = artifact.install_plan(content_id="a" * 64)

        assert plan.tree_root == Path(
            "/usr/local/lib/rquant-signal-family-verifier/" + "a" * 64
        )
        assert plan.entry_path == artifact.ARTIFACT_ENTRY_PATH
        assert plan.owner_uid == 0
        assert plan.owner_gid == 0
        assert plan.directory_mode == 0o555
        assert plan.file_mode == 0o444
        assert plan.executable_mode == 0o555
        assert plan.entry_mode == 0o555

    def test_the_install_plan_refuses_a_content_id_that_is_not_a_digest(self) -> None:
        for bad in ("", "zz", "A" * 64, "a" * 63, "../escape"):
            with pytest.raises(artifact.VerifierArtifactError, match="content id"):
                artifact.install_plan(content_id=bad)


class TestContentAddressing:
    def test_the_manifest_is_sorted_canonical_and_covers_every_node(
        self,
        tmp_path: Path,
    ) -> None:
        root, entries, _ = _installed(tmp_path)
        paths = [entry.relative_path for entry in entries]

        assert paths == sorted(paths)
        assert len(set(paths)) == len(paths)
        assert "pyvenv.cfg" in paths
        assert "lib/python3.11/site-packages/rquant/strict_json.py" in paths
        assert "lib" in paths and "lib/python3.11" in paths
        assert all(entry.relative_path != "." for entry in entries)
        for entry in entries:
            node = root / entry.relative_path
            if entry.entry_type == "file":
                assert entry.sha256 == hashlib.sha256(node.read_bytes()).hexdigest()
                assert entry.size == node.stat().st_size
            else:
                assert entry.sha256 is None and entry.size is None

    def test_the_content_id_is_the_digest_of_the_canonical_manifest_bytes(
        self,
        tmp_path: Path,
    ) -> None:
        _, entries, identifier = _installed(tmp_path)
        payload = artifact.canonical_manifest_bytes(entries)

        assert identifier == hashlib.sha256(payload).hexdigest()
        assert payload == json.dumps(
            json.loads(payload), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        assert json.loads(payload)["schema_id"] == artifact.MANIFEST_SCHEMA_ID

    def test_the_manifest_round_trips_through_its_canonical_bytes(
        self,
        tmp_path: Path,
    ) -> None:
        _, entries, identifier = _installed(tmp_path)

        parsed = artifact.parse_manifest(artifact.canonical_manifest_bytes(entries))

        assert parsed == entries
        assert artifact.content_id(parsed) == identifier

    def test_a_non_canonical_manifest_payload_rejects(self, tmp_path: Path) -> None:
        _, entries, _ = _installed(tmp_path)
        payload = artifact.canonical_manifest_bytes(entries)

        with pytest.raises(artifact.VerifierArtifactError, match="canonical"):
            artifact.parse_manifest(b" " + payload)

    def test_one_flipped_source_byte_changes_the_content_id(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        target = root / "lib" / "python3.11" / "site-packages" / "rquant" / "strict_json.py"
        target.chmod(0o644)
        target.write_text("VALUE = 2\n", encoding="utf-8")
        target.chmod(0o444)

        assert artifact.content_id(artifact.build_tree_manifest(root)) != identifier


class TestInstalledTreeVerification:
    def test_a_correct_tree_verifies(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)

        _verify(root, entries, identifier)

    def test_a_flipped_byte_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        target = root / "lib" / "python3.11" / "site-packages" / "rquant" / "strict_json.py"
        target.chmod(0o644)
        target.write_bytes(target.read_bytes().replace(b"1", b"2"))
        target.chmod(0o444)

        with pytest.raises(artifact.VerifierArtifactError, match="hash"):
            _verify(root, entries, identifier)

    def test_an_extra_file_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        site = root / "lib" / "python3.11" / "site-packages"
        site.chmod(0o755)
        (site / "sitecustomize.py").write_text("import os\n", encoding="utf-8")
        site.chmod(0o555)

        with pytest.raises(artifact.VerifierArtifactError, match="unmanifested"):
            _verify(root, entries, identifier)

    def test_a_missing_file_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        target = root / "lib" / "python3.11" / "site-packages" / "typing_extensions.py"
        target.parent.chmod(0o755)
        target.unlink()
        target.parent.chmod(0o555)

        with pytest.raises(artifact.VerifierArtifactError, match="missing"):
            _verify(root, entries, identifier)

    def test_a_relaxed_mode_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        target = root / "lib" / "python3.11" / "site-packages" / "typing_extensions.py"
        target.chmod(0o666)

        with pytest.raises(artifact.VerifierArtifactError, match="mode"):
            _verify(root, entries, identifier)

    def test_a_foreign_owner_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)

        with pytest.raises(artifact.VerifierArtifactError, match="owner"):
            artifact.verify_installed_tree(
                root,
                manifest=entries,
                expected_content_id=identifier,
                expected_owner_uid=os.getuid() + 4242,
                expected_owner_gid=root.stat().st_gid,
            )

    def test_a_symlinked_member_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        target = root / "lib" / "python3.11" / "site-packages" / "typing_extensions.py"
        target.parent.chmod(0o755)
        target.unlink()
        target.symlink_to(tmp_path / "elsewhere.py")
        (tmp_path / "elsewhere.py").write_text("VALUE = 2\n", encoding="utf-8")
        target.parent.chmod(0o555)

        with pytest.raises(artifact.VerifierArtifactError, match="regular file"):
            _verify(root, entries, identifier)

    def test_a_hardlinked_member_refuses_to_start(self, tmp_path: Path) -> None:
        """The second link lives outside the tree, which is where an attacker would put it."""

        root, entries, identifier = _installed(tmp_path)
        target = root / "lib" / "python3.11" / "site-packages" / "typing_extensions.py"
        os.link(target, tmp_path / "alias.py")

        with pytest.raises(artifact.VerifierArtifactError, match="single link"):
            _verify(root, entries, identifier)

    def test_a_group_writable_tree_root_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        root.chmod(0o575)

        with pytest.raises(artifact.VerifierArtifactError, match="writable"):
            _verify(root, entries, identifier)

    def test_a_group_writable_ancestor_refuses_to_start(self, tmp_path: Path) -> None:
        root, entries, identifier = _installed(tmp_path)
        tmp_path.chmod(0o775)
        try:
            with pytest.raises(artifact.VerifierArtifactError, match="writable"):
                _verify(root, entries, identifier)
        finally:
            tmp_path.chmod(0o700)

    def test_a_content_id_that_does_not_match_the_manifest_refuses(
        self,
        tmp_path: Path,
    ) -> None:
        root, entries, _ = _installed(tmp_path)

        with pytest.raises(artifact.VerifierArtifactError, match="content id"):
            _verify(root, entries, "0" * 64)


class TestRollback:
    def test_two_content_addressed_trees_coexist_and_verify_independently(
        self,
        tmp_path: Path,
    ) -> None:
        first = _tree(tmp_path / "a")
        first_entries = artifact.build_tree_manifest(first)
        first_id = artifact.content_id(first_entries)

        second = _tree(tmp_path / "b")
        target = second / "lib" / "python3.11" / "site-packages" / "rquant" / "strict_json.py"
        target.chmod(0o644)
        target.write_text("VALUE = 99\n", encoding="utf-8")
        artifact.freeze_tree_modes(second)
        second_entries = artifact.build_tree_manifest(second)
        second_id = artifact.content_id(second_entries)

        assert first_id != second_id
        _verify(first, first_entries, first_id)
        _verify(second, second_entries, second_id)

    def test_rolling_back_to_the_previous_tree_does_not_touch_the_newer_one(
        self,
        tmp_path: Path,
    ) -> None:
        first = _tree(tmp_path / "a")
        first_entries = artifact.build_tree_manifest(first)
        first_id = artifact.content_id(first_entries)
        second = _tree(tmp_path / "b")
        second_entries = artifact.build_tree_manifest(second)

        _verify(first, first_entries, first_id)

        assert artifact.build_tree_manifest(second) == second_entries


class TestCheckoutRefusal:
    def test_the_import_root_is_the_only_path_the_entry_may_add(
        self,
        tmp_path: Path,
    ) -> None:
        root, _, _ = _installed(tmp_path)

        assert artifact.import_root(root) == root / "lib" / "python3.11" / "site-packages"

    def test_a_checkout_path_on_sys_path_refuses(self, tmp_path: Path) -> None:
        root, _, _ = _installed(tmp_path)
        site = artifact.import_root(root)

        with pytest.raises(artifact.VerifierArtifactError, match="outside the verified"):
            artifact.assert_import_paths_are_confined(
                [str(site), str(ROOT / "src")],
                tree_root=root,
                entry_path=tmp_path / "entry.pyz",
            )

    def test_the_entry_archive_and_the_import_root_are_accepted(
        self,
        tmp_path: Path,
    ) -> None:
        root, _, _ = _installed(tmp_path)
        entry = tmp_path / "entry.pyz"
        entry.write_bytes(b"PK")

        artifact.assert_import_paths_are_confined(
            [str(entry), str(artifact.import_root(root))],
            tree_root=root,
            entry_path=entry,
        )

    def test_an_imported_module_outside_the_tree_refuses(self, tmp_path: Path) -> None:
        root, _, _ = _installed(tmp_path)

        with pytest.raises(artifact.VerifierArtifactError, match="outside the verified"):
            artifact.assert_module_is_from_tree(
                module_file=ROOT / "src" / "rquant" / "strict_json.py",
                tree_root=root,
            )

    def test_an_imported_module_inside_the_tree_is_accepted(self, tmp_path: Path) -> None:
        root, _, _ = _installed(tmp_path)

        artifact.assert_module_is_from_tree(
            module_file=artifact.import_root(root) / "rquant" / "strict_json.py",
            tree_root=root,
        )


class TestLegacyCheckoutEntryPoint:
    def test_the_checkout_entry_script_refuses_to_run(self) -> None:
        """Running the root verifier from the checkout is the exact P1-4 finding."""

        completed = subprocess.run(
            [sys.executable, str(LEGACY_ENTRY_SCRIPT), "verify"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode != 0
        assert "mutable checkout" in completed.stderr
        assert str(artifact.ARTIFACT_ENTRY_PATH) in completed.stderr

    def test_the_checkout_entry_script_no_longer_extends_sys_path(self) -> None:
        source = LEGACY_ENTRY_SCRIPT.read_text(encoding="utf-8")

        assert "sys.path.insert" not in source
        assert "from rquant." not in source


class TestDeterministicBuild:
    @staticmethod
    def _source_venv(root: Path) -> Path:
        site = root / "lib" / "python3.11" / "site-packages"
        (site / "fakedep").mkdir(parents=True)
        (site / "fakedep" / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
        (site / "fakemod.py").write_text("VALUE = 2\n", encoding="utf-8")
        (root / "bin").mkdir()
        (root / "pyvenv.cfg").write_text(
            "home = /usr/bin\ninclude-system-site-packages = true\nversion = 3.11.15\n",
            encoding="utf-8",
        )
        return root

    def _build(self, tmp_path: Path, name: str) -> dict[str, object]:
        source = self._source_venv(tmp_path / f"src-{name}")
        completed = subprocess.run(
            [
                sys.executable,
                str(BUILD_SCRIPT),
                "--repository-root",
                str(ROOT),
                "--source-venv",
                str(source),
                "--output-root",
                str(tmp_path / f"out-{name}"),
                "--third-party",
                "fakedep",
                "--third-party",
                "fakemod",
                "--interpreter-home",
                "/usr/bin",
                "--python-version",
                "3.11.15",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(completed.stdout)

    def test_two_builds_of_one_tree_are_byte_identical(self, tmp_path: Path) -> None:
        first = self._build(tmp_path, "one")
        second = self._build(tmp_path, "two")

        assert first["content_id"] == second["content_id"]
        assert first["entry_sha256"] == second["entry_sha256"]
        assert first["manifest_sha256"] == second["manifest_sha256"]

    def test_the_build_ships_only_the_verifier_import_closure(
        self,
        tmp_path: Path,
    ) -> None:
        built = self._build(tmp_path, "closure")
        tree = Path(str(built["tree_root"]))
        shipped = sorted(
            path.stem
            for path in (tree / "lib" / "python3.11" / "site-packages" / "rquant").glob("*.py")
        )

        assert "signal_family_root_verifier" in shipped
        assert "privilege_launcher" in shipped
        assert "config" not in shipped
        assert "storage" not in shipped

    def test_the_built_tree_declares_no_system_site_packages(self, tmp_path: Path) -> None:
        built = self._build(tmp_path, "pyvenv")
        config = (Path(str(built["tree_root"])) / "pyvenv.cfg").read_text(encoding="utf-8")

        assert "include-system-site-packages = false" in config
        assert "include-system-site-packages = true" not in config

    def test_the_built_tree_carries_no_pth_or_customize_escape(
        self,
        tmp_path: Path,
    ) -> None:
        built = self._build(tmp_path, "escape")
        tree = Path(str(built["tree_root"]))
        names = {path.name for path in tree.rglob("*")}

        assert not any(name.endswith(".pth") for name in names)
        assert "sitecustomize.py" not in names
        assert "usercustomize.py" not in names
        assert "__pycache__" not in names

    def test_the_built_tree_verifies_against_its_own_frozen_manifest(
        self,
        tmp_path: Path,
    ) -> None:
        built = self._build(tmp_path, "verify")
        tree = Path(str(built["tree_root"]))
        entries = artifact.build_tree_manifest(tree)

        assert artifact.content_id(entries) == built["content_id"]
        _verify(tree, entries, str(built["content_id"]))

    def test_the_entry_archive_carries_the_frozen_manifest_and_content_id(
        self,
        tmp_path: Path,
    ) -> None:
        import zipfile

        built = self._build(tmp_path, "frozen")
        entry = Path(str(built["entry_path"]))
        with zipfile.ZipFile(entry) as archive:
            names = set(archive.namelist())
            frozen = archive.read(
                "rquant_signal_family_verifier_entry/_frozen_manifest.py"
            ).decode("utf-8")

        assert "__main__.py" in names
        assert "rquant_signal_family_verifier_entry/_artifact.py" in names
        assert str(built["content_id"]) in frozen
        assert "rquant_signal_family_verifier_entry/_cli.py" in names

    def test_the_unbuilt_frozen_manifest_refuses_to_bootstrap(self) -> None:
        from rquant.signal_family_verifier_entry import _frozen_manifest

        assert _frozen_manifest.CONTENT_ID == ""
        with pytest.raises(artifact.VerifierArtifactError, match="not been built"):
            _frozen_manifest.require_frozen_manifest()


class TestBuiltEntryEndToEnd:
    """Run the real built archive against a real tree, without installing anything.

    The archive is executed with `-I -S` exactly as the production unit would, but its
    install root is redirected onto a tree this test owns through the same keyword seam
    `VerifierAnchors` uses. Production `main` passes nothing, so the frozen literals are
    the only values a production run can see.
    """

    PROBE = (
        "import json, sys\n"
        "from pathlib import Path\n"
        # A zipapp puts its own archive at `sys.path[0]`; `-c` has to do it by hand.
        "sys.path.insert(0, sys.argv[2])\n"
        "BASELINE = list(sys.path)\n"
        "from rquant_signal_family_verifier_entry import __main__ as boot\n"
        "from rquant_signal_family_verifier_entry._artifact import VerifierArtifactError\n"
        "try:\n"
        "    cli = boot.bind_verified_tree(\n"
        "        install_root=Path(sys.argv[1]),\n"
        "        entry_path=Path(sys.argv[2]),\n"
        "        expected_owner_uid=int(sys.argv[3]),\n"
        "        expected_owner_gid=int(sys.argv[4]),\n"
        "    )\n"
        "except VerifierArtifactError as error:\n"
        "    print(json.dumps({'outcome': 'rejected', 'detail': str(error)}))\n"
        "    raise SystemExit(0)\n"
        "import rquant.signal_family_root_verifier as verifier\n"
        "print(json.dumps({\n"
        "    'outcome': 'bound',\n"
        "    'verifier_file': verifier.__file__,\n"
        "    'cli': cli.__name__,\n"
        "    'sys_path': list(sys.path),\n"
        "    'baseline': BASELINE,\n"
        "}))\n"
    )

    @staticmethod
    def _build(tmp_path: Path) -> dict[str, object]:
        completed = subprocess.run(
            [
                sys.executable,
                str(BUILD_SCRIPT),
                "--repository-root",
                str(ROOT),
                # The tree ships this interpreter's own dependencies: a `pydantic_core`
                # built for another ABI would not import in the subprocess below.
                "--source-venv",
                str(Path(sys.prefix)),
                "--output-root",
                str(tmp_path / "staging"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(completed.stdout)

    def _run(self, built: dict[str, object], install_root: Path) -> dict[str, object]:
        entry = Path(str(built["entry_path"]))
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                self.PROBE,
                str(install_root),
                str(entry),
                str(os.getuid()),
                str(install_root.stat().st_gid),
            ],
            capture_output=True,
            text=True,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(completed.stdout)

    @pytest.fixture()
    def built(self, tmp_path: Path) -> tuple[dict[str, object], Path]:
        report = self._build(tmp_path)
        install_root = tmp_path / "install"
        install_root.mkdir(mode=0o755)
        artifact.relocate_frozen_tree(
            Path(str(report["tree_root"])),
            install_root / str(report["content_id"]),
        )
        return report, install_root

    def test_the_built_entry_binds_only_the_verified_tree(
        self,
        built: tuple[dict[str, object], Path],
    ) -> None:
        report, install_root = built
        tree = install_root / str(report["content_id"])

        observed = self._run(report, install_root)

        assert observed["outcome"] == "bound", observed
        assert Path(str(observed["verifier_file"])).is_relative_to(tree)
        assert str(ROOT / "src") not in observed["sys_path"]  # type: ignore[operator]
        baseline = set(observed["baseline"])  # type: ignore[arg-type]
        for entry in observed["sys_path"]:  # type: ignore[union-attr]
            assert str(entry) in baseline or Path(str(entry)).is_relative_to(tree), entry
        assert str(tree / "lib" / "python3.11" / "site-packages") in observed["sys_path"]  # type: ignore[operator]

    def test_one_flipped_byte_in_the_installed_tree_refuses_to_start(
        self,
        built: tuple[dict[str, object], Path],
    ) -> None:
        report, install_root = built
        tree = install_root / str(report["content_id"])
        target = tree / "lib" / "python3.11" / "site-packages" / "rquant" / "strict_json.py"
        target.parent.chmod(0o755)
        target.chmod(0o644)
        target.write_bytes(target.read_bytes() + b"\n")
        target.chmod(0o444)
        target.parent.chmod(0o555)

        observed = self._run(report, install_root)

        assert observed["outcome"] == "rejected"
        assert "hash changed" in str(observed["detail"])

    def test_a_checkout_module_smuggled_into_the_tree_refuses_to_start(
        self,
        built: tuple[dict[str, object], Path],
    ) -> None:
        report, install_root = built
        tree = install_root / str(report["content_id"])
        site = tree / "lib" / "python3.11" / "site-packages"
        site.chmod(0o755)
        (site / "sitecustomize.py").write_text("import os\n", encoding="utf-8")
        site.chmod(0o555)

        observed = self._run(report, install_root)

        assert observed["outcome"] == "rejected"
        assert "unmanifested" in str(observed["detail"])
