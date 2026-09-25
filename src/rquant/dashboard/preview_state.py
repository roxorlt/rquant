"""Shared marker for "this page is running mounted inside preview_app.py".

``preview_app.py`` sets ``st.session_state[PREVIEW_MOUNTED_SESSION_KEY] = True``
before calling ``st.navigation(...).run()``. A page checks the same key to tell
whether it is embedded in that multipage session (browser tab shared with sibling
pages) or running standalone in its own Streamlit process (e.g. production's
``rquant-dashboard.service`` runs ``app.py`` directly on port 8501). Detection goes
through ``st.session_state`` rather than the request URL because ``st.session_state``
is the one thing ``st.navigation`` reliably carries across page switches within a
session, and it cannot be spoofed by directly deep-linking a sub-page's path.

Pure constant module: importing it must never touch Streamlit, serving, or any
other side effect, so every page can import it unconditionally at module load
(including from tests that only check import safety).
"""

from __future__ import annotations

PREVIEW_MOUNTED_SESSION_KEY = "rquant_preview_mounted"

__all__ = ["PREVIEW_MOUNTED_SESSION_KEY"]
