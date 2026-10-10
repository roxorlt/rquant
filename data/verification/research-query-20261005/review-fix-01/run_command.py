"""Record one authorized local verification command and its original outputs."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


def main() -> int:
    label, *arguments = sys.argv[1:]
    if re.fullmatch(r"[a-z0-9-]+", label) is None or not arguments:
        raise ValueError("explicit verification label and arguments required")
    directory = Path(__file__).resolve().parent
    environment = dict(os.environ)
    environment.update(
        DATA_DIR="/private/tmp/rquant-query-tests",
        DUCKDB_PATH="/private/tmp/rquant-query-tests/main.duckdb",
        PARQUET_DIR="/private/tmp/rquant-query-tests/parquet",
        LOG_DIR="/private/tmp/rquant-query-tests/logs",
        RQUANT_DISABLE_DOTENV="1",
        TUSHARE_TOKEN_MAIN="00000000000000000000000000000000",
        PUSHDEER_KEYS="",
        PUSHPLUS_TOKENS="",
    )
    stdout_path, stderr_path = directory / f"{label}.stdout", directory / f"{label}.stderr"
    started = datetime.now(UTC).isoformat()
    began = time.monotonic()
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        completed = subprocess.run(
            arguments, env=environment, stdout=stdout, stderr=stderr, close_fds=True
        )
    record = {
        "argv": arguments,
        "cwd": str(Path.cwd()),
        "started_utc": started,
        "elapsed_seconds": time.monotonic() - began,
        "exit_code": completed.returncode,
        "stdout_sha256": hashlib.sha256(stdout_path.read_bytes()).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr_path.read_bytes()).hexdigest(),
        "starting_candidate": "90b61007d47bd906bb85a3e4f67521b12cdf2cbb",
    }
    (directory / f"{label}.command.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record))
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
