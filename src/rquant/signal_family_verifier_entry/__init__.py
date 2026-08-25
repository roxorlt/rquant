"""The installed, root-owned entry point of the signal-family root verifier.

Codex round-2 P1-4 forbids the root verifier from importing business code out of a mutable
checkout. This package is what gets built into the fixed pyz at
`/usr/local/libexec/rquant-signal-family-verifier-v1.pyz`: it verifies the content-addressed
tree at `/usr/local/lib/rquant-signal-family-verifier/<content-id>/` against a manifest
frozen inside the archive, binds exactly that tree's site-packages onto `sys.path`, and only
then imports `rquant.signal_family_root_verifier`.

Nothing here installs anything. Creating the tree, the entry pyz, the policy, the harness or
the append store is a separate root infrastructure transaction with its own explicit user
authorization (`authority.md` L1389-1395).
"""

from __future__ import annotations
