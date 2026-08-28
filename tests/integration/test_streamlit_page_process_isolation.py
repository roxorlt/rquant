from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_five_streamlit_pages_render_while_main_and_replica_are_write_locked(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    main = data_dir / "rquant.duckdb"
    replica = data_dir / "rquant_ro.duckdb"
    writers = (duckdb.connect(str(main)), duckdb.connect(str(replica)))
    for writer in writers:
        writer.execute("CREATE TABLE operational_only(value INTEGER)")
        writer.execute("BEGIN TRANSACTION")
        writer.execute("INSERT INTO operational_only VALUES (1)")

    harness = """
from pathlib import Path
from streamlit.testing.v1 import AppTest

root = Path.cwd()
pages = (
    'src/rquant/dashboard/app.py',
    'src/rquant/dashboard/nl_screen.py',
    'src/rquant/dashboard/nl_canvas.py',
    'src/rquant/dashboard/lab/app.py',
    'src/rquant/dashboard/market_panorama.py',
)
failures = []
for relative in pages:
    app = AppTest.from_file(str(root / relative)).run(timeout=20)
    errors = [str(item.value) for item in app.exception]
    if errors:
        failures.append((relative, errors))
if failures:
    raise SystemExit(repr(failures))
print('STREAMLIT_PAGES_OK=5')
"""
    environment = dict(os.environ)
    environment.update(
        {
            "DATA_DIR": str(data_dir),
            "DUCKDB_PATH": str(main),
            "DUCKDB_READONLY_PATH": str(replica),
            "DEEPSEEK_API_KEY": "test-only-key",
            "PYTHONPATH": str(_PROJECT_ROOT / "src"),
            "RQUANT_SERVING_ROOT": str(tmp_path / "missing-serving"),
        }
    )
    try:
        completed = subprocess.run(
            [sys.executable, "-c", harness],
            cwd=_PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    finally:
        for writer in writers:
            writer.rollback()
            writer.close()

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "STREAMLIT_PAGES_OK=5" in completed.stdout
