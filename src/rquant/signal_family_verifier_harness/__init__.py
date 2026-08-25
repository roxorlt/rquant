"""The fixed root-owned harness that runs Phase C vectors inside the child.

`authority.md` L1424-L1446 splits the Phase C verification in two. The privileged half
(`rquant.signal_family_root_verifier`) never imports generation code; this package is the
other half: the unprivileged child that the root launches as
`<generation interpreter> -I <policy-hashed harness>.pyz`, hands one canonical request on a
pipe, and reads exactly one bounded canonical response from.

Three properties hold everywhere in this package:

* **Only stdlib at import time.** Generation code is imported inside functions, never at
  module scope, so a harness that is loaded but never asked to exercise a surface still
  touches nothing of the generation.
* **The child never learns an expected result.** The request carries vector identity and
  input bytes only. Nothing here reads, derives, or compares an expected result; the root
  does that after the child has exited.
* **The vector tuple is untouchable.** The child recomputes every `vector_id` from the
  vector's own canonical content, requires the sorted duplicate-free order the policy
  hashed, and emits exactly one result per vector. It cannot add, omit, reorder, or alter
  one.

The harness writes only inside its own empty private cwd, which the root creates, chowns to
the unprivileged child, and removes after the run.

Every intra-package import is relative on purpose. The build script relocates this package
into the zipapp under its own top-level name, so an absolute `rquant.…` import would rebind
the harness to whatever copy the *generation* ships, and the policy hash would then cover
bytes the child does not run.
"""

from __future__ import annotations

from ._canonical import (
    canonical_json_bytes,
    canonical_sha256,
    sha256_hex,
    strict_canonical_loads,
)
from ._request import (
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    ChildRequest,
    ChildRequestError,
    RequestVector,
    build_child_response,
    parse_child_request,
)
from ._resolve import (
    SurfaceResolutionError,
    bound_surface,
    resolve_surface,
)
from ._surfaces import (
    BLOCKED_SURFACE_REASONS,
    IMPLEMENTED_SURFACE_IDS,
    SurfaceExerciseError,
    exercise_vector,
)
from ._workspace import (
    VOLATILE_SUFFIXES,
    VectorWorkspace,
    WorkspaceError,
    tree_digest,
)

__all__ = [
    "BLOCKED_SURFACE_REASONS",
    "IMPLEMENTED_SURFACE_IDS",
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "ChildRequest",
    "ChildRequestError",
    "RequestVector",
    "SurfaceExerciseError",
    "SurfaceResolutionError",
    "VOLATILE_SUFFIXES",
    "VectorWorkspace",
    "WorkspaceError",
    "bound_surface",
    "build_child_response",
    "canonical_json_bytes",
    "canonical_sha256",
    "exercise_vector",
    "parse_child_request",
    "resolve_surface",
    "sha256_hex",
    "strict_canonical_loads",
    "tree_digest",
]
