"""The fixed root-owned runtime wrapper every protected production unit executes.

`authority.md` L1707-1712 fixes the production unit command at

    /usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz --role <literal>

and Codex round-2 P1-3 makes it mandatory for the runtime units this branch adds. Before
this package existed, `/usr/local/libexec/rquant-runtime-exec.pyz` was a path constant with
no artifact behind it, and the units ran `.venv/bin/python -m rquant.runtime_service_main`
straight out of a `lighthouse`-writable checkout, taking the expected commit and generation
from `data/runtime/current/runtime.env` — a file the application itself writes — and the
service manifest from a path interpolated through a systemd instance name.

The wrapper takes one thing from the unit: a role literal. Everything else — which
generation, which interpreter, which module, which cwd, which environment names — comes from
the two root-owned documents, and the code it is about to execute is checked file by file
against the generation's full manifest first.
"""

from __future__ import annotations
