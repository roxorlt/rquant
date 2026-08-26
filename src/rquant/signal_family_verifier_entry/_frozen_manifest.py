"""The tree manifest the build freezes into the entry archive.

In the checkout this module is a placeholder: there is no tree yet, so there is nothing to
freeze and every bootstrap attempt refuses. `scripts/build-signal-family-verifier-artifact.py`
replaces it inside the archive with the real content id and canonical manifest bytes, which
is what makes the installed pair self-checking — the entry knows which bytes it expects
before it opens a single one of them.
"""

from __future__ import annotations

from typing import Final

from ._artifact import TreeEntry, VerifierArtifactError, parse_manifest

#: Empty in the checkout; a 64 character digest inside a built archive.
CONTENT_ID: Final[str] = ""
#: Empty in the checkout; the canonical manifest bytes inside a built archive.
MANIFEST_JSON: Final[bytes] = b""


def require_frozen_manifest() -> tuple[str, tuple[TreeEntry, ...]]:
    """The frozen pair, or a refusal. An unbuilt entry can never bootstrap."""

    if not CONTENT_ID or not MANIFEST_JSON:
        raise VerifierArtifactError(
            "this verifier entry has not been built: run "
            "scripts/build-signal-family-verifier-artifact.py to produce the installed pair"
        )
    return CONTENT_ID, parse_manifest(MANIFEST_JSON)
