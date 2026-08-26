"""The production bootstrap of the installed verifier entry archive.

Invoked as:

    /usr/bin/python3.11 -I -S /usr/local/libexec/rquant-signal-family-verifier-v1.pyz verify

`-I -S` already removes `PYTHONPATH`, the user site directory and the current working
directory from `sys.path`. What this bootstrap adds is the positive half: it verifies the
content-addressed tree named by the manifest frozen inside this very archive, binds exactly
that tree's site-packages, refuses any other import path, and proves after the fact that the
verifier module it imported came out of the tree rather than from anywhere else.

Nothing here can be redirected. The tree root is `install root / frozen content id`, both
literals; there is no flag, environment variable or record field that moves either.
"""

from __future__ import annotations

import sys
from pathlib import Path

from ._artifact import (
    ARTIFACT_ENTRY_PATH,
    ARTIFACT_INSTALL_ROOT,
    VerifierArtifactError,
    assert_import_paths_are_confined,
    assert_isolated_startup,
    assert_module_is_from_tree,
    import_root,
    verify_installed_tree,
)
from ._frozen_manifest import require_frozen_manifest

#: Production is root-owned. The offline suite never runs this module; it drives
#: `_artifact` directly with a tree it owns.
EXPECTED_OWNER_UID = 0
EXPECTED_OWNER_GID = 0


def bind_verified_tree(
    *,
    install_root: Path = ARTIFACT_INSTALL_ROOT,
    entry_path: Path = ARTIFACT_ENTRY_PATH,
    expected_owner_uid: int = EXPECTED_OWNER_UID,
    expected_owner_gid: int = EXPECTED_OWNER_GID,
) -> object:
    """Verify the installed tree, bind its imports, and return the verified CLI module.

    The four keyword arguments are the same injection seam `VerifierAnchors` uses (ruling
    O5): the offline suite drives this function against a tree it built and owns, and
    `main` — the only production caller — passes nothing, so the fixed literals are the
    only values a production run can ever see. No environment variable, flag or record
    field reaches them.
    """

    assert_isolated_startup(sys.flags)
    interpreter_baseline = tuple(sys.path)
    content_id, manifest = require_frozen_manifest()
    tree_root = install_root / content_id
    verify_installed_tree(
        tree_root,
        manifest=manifest,
        expected_content_id=content_id,
        expected_owner_uid=expected_owner_uid,
        expected_owner_gid=expected_owner_gid,
    )
    # The `-I -S` baseline is the interpreter's own standard library plus this archive:
    # `PYTHONPATH`, the user site directory, the working directory and every `.pth` hook
    # were removed before this module ran. Nothing is added to it but the verified tree's
    # site-packages, and the result is checked rather than trusted.
    site_packages = str(import_root(tree_root))
    sys.path = [path for path in interpreter_baseline if path]
    sys.path.insert(0, site_packages)
    assert_import_paths_are_confined(
        list(sys.path),
        tree_root=tree_root,
        entry_path=entry_path,
        interpreter_baseline=interpreter_baseline,
    )
    import rquant.signal_family_root_verifier as verifier

    assert_module_is_from_tree(module_file=verifier.__file__, tree_root=tree_root)
    from . import _cli

    return _cli


def main(argv: list[str]) -> int:
    try:
        cli = bind_verified_tree()
    except VerifierArtifactError as error:
        sys.stderr.write(f"{error}\n")
        return 78
    return int(cli.main(argv))  # type: ignore[attr-defined]


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
