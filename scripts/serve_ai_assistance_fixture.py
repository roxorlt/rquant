"""Serve the original AI owners and a real sealed seed over private synthetic inputs."""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import sys
from pathlib import Path
from types import FrameType

ROOT = Path(__file__).resolve().parents[1]
for directory in (ROOT, ROOT / "src"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bind", required=True)
    args = parser.parse_args(argv)
    host, port = args.bind.rsplit(":", 1)
    if host != "127.0.0.1":
        parser.error("the synthetic AI fixture requires IPv4 loopback")

    import uvicorn

    from tests.support.ai_assistance_fixture import build_ai_integration_fixture

    root = args.root.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.environ.update(
        {
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "synthetic-test-token-0000000000000000",
            "DATA_DIR": str(root),
            "DUCKDB_PATH": str(root / "unused-primary.duckdb"),
            "PARQUET_DIR": str(root / "parquet"),
            "LOG_DIR": str(root / "logs"),
        }
    )
    fixture = None
    try:
        fixture = build_ai_integration_fixture(root)
        fixture.seal_seed_with_original_worker()
        fixture.install_original_nightly(enabled=True).trigger()
        fixture.publish()
        print("Original AI seed sealed and fully read.", flush=True)

        def stop(_signal: int, _frame: FrameType | None) -> None:
            raise SystemExit(0)

        # Uvicorn replays SIGTERM after lifespan.close; the owner finally must run too.
        previous = signal.signal(signal.SIGTERM, stop)
        try:
            uvicorn.run(
                fixture.app,
                host=host,
                port=int(port),
                workers=1,
                proxy_headers=False,
                server_header=False,
                access_log=False,
                log_level="warning",
            )
        finally:
            signal.signal(signal.SIGTERM, previous)
    finally:
        try:
            if fixture is not None:
                fixture.close()
        finally:
            # Serving generations are read-only; remove only this newly created root.
            for current, _, _ in os.walk(root, followlinks=False):
                os.chmod(current, 0o700)
            shutil.rmtree(root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
