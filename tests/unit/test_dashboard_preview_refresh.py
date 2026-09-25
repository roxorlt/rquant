"""The health dashboard's meta refresh must not bleed across preview_app.py's
``st.navigation`` page switches.

2026-09-25 preview bug report: ``app.py``'s 30s ``<meta http-equiv="refresh">``
survives client-side navigation to a sibling page (``st.navigation`` never does a
real document-level navigation, so the browser's pending refresh timer keeps
running) and bounces the tab back to the health dashboard even while the owner is
looking at the runtime console. Standalone (production's ``rquant-dashboard.service``
runs ``app.py`` directly on port 8501) must keep the exact same 30s meta refresh.

Pattern copied from ``test_serving_page_isolation.py`` / ``test_preview_app.py``:
run the Streamlit AppTest harness in a subprocess and inspect the rendered element
tree (``app.markdown`` carries the raw HTML passed to ``st.markdown``).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_APP_PATH = _PROJECT_ROOT / "src/rquant/dashboard/app.py"
_PREVIEW_PATH = _PROJECT_ROOT / "src/rquant/dashboard/preview_app.py"


def _base_environment(tmp_path: Path, *, missing_serving_root: Path) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "TUSHARE_TOKEN_MAIN": "0" * 40,
            "DATA_DIR": str(tmp_path / "data"),
            "DUCKDB_PATH": str(tmp_path / "data" / "rquant.duckdb"),
            "PARQUET_DIR": str(tmp_path / "data" / "parquet"),
            "LOG_DIR": str(tmp_path / "data" / "logs"),
            "RQUANT_DISABLE_DOTENV": "1",
            "PYTHONPATH": str(_PROJECT_ROOT / "src"),
            # A serving root that does not exist is enough here: app.py already
            # renders a degraded-but-not-crashing page against a missing root
            # (see test_serving_page_isolation.py), and this test only cares
            # about the refresh meta tag / button, not the data sections.
            "RQUANT_SERVING_ROOT": str(missing_serving_root),
        }
    )
    return environment


def _run_harness(harness: str, *, environment: dict[str, str]) -> dict:
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=_PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    [result_line] = [
        line for line in completed.stdout.splitlines() if line.startswith("RESULT_JSON=")
    ]
    return json.loads(result_line.removeprefix("RESULT_JSON="))


def test_standalone_app_still_injects_the_30s_meta_refresh(tmp_path: Path) -> None:
    harness = textwrap.dedent(
        f"""
        import json
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_file({str(_APP_PATH)!r}).run(timeout=30)
        result = {{
            "exceptions": [str(item.value) for item in app.exception],
            "has_refresh_meta": any(
                'http-equiv="refresh"' in md.value for md in app.markdown
            ),
            "has_manual_refresh_button": any(
                "刷新" in button.label for button in app.button
            ),
        }}
        print("RESULT_JSON=" + json.dumps(result, ensure_ascii=False))
        """
    )
    environment = _base_environment(tmp_path, missing_serving_root=tmp_path / "no-serving-root")
    result = _run_harness(harness, environment=environment)

    assert result["exceptions"] == [], result["exceptions"]
    assert result["has_refresh_meta"] is True
    assert result["has_manual_refresh_button"] is False


def test_app_mounted_via_preview_does_not_inject_meta_refresh(tmp_path: Path) -> None:
    harness = textwrap.dedent(
        f"""
        import json
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_file({str(_PREVIEW_PATH)!r}).run(timeout=30)
        health_result = {{
            "exceptions": [str(item.value) for item in app.exception],
            "has_refresh_meta": any(
                'http-equiv="refresh"' in md.value for md in app.markdown
            ),
            "has_manual_refresh_button": any(
                "刷新" in button.label for button in app.button
            ),
        }}

        app.switch_page("runtime_console.py")
        app.run(timeout=30)
        console_result = {{
            "exceptions": [str(item.value) for item in app.exception],
            "has_refresh_meta": any(
                'http-equiv="refresh"' in md.value for md in app.markdown
            ),
            "has_manual_refresh_button": any(
                "刷新" in button.label for button in app.button
            ),
        }}

        print(
            "RESULT_JSON="
            + json.dumps(
                {{"health": health_result, "console": console_result}}, ensure_ascii=False
            )
        )
        """
    )
    environment = _base_environment(tmp_path, missing_serving_root=tmp_path / "no-serving-root")
    result = _run_harness(harness, environment=environment)

    health = result["health"]
    console = result["console"]

    assert health["exceptions"] == [], health["exceptions"]
    assert health["has_refresh_meta"] is False
    assert health["has_manual_refresh_button"] is True

    assert console["exceptions"] == [], console["exceptions"]
    assert console["has_refresh_meta"] is False
    assert console["has_manual_refresh_button"] is True
