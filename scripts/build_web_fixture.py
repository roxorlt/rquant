"""Publish a synthetic Serving generation for the web API (CI browser tests, local runs).

    uv run python scripts/build_web_fixture.py --out <dir> --scenario panorama
    uv run python scripts/build_web_fixture.py --out <dir> --scenario panorama --publish-next

The data is invented (see ``tests/support/web_serving_fixture.py``). ``--out`` must be a
new or empty directory, or one this script wrote before; ``--replace`` wipes the latter
first. ``--publish-next`` adds the next generation of the same scenario on top, which is
how the generation-switch test makes the page see a new generation.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tests.support.web_serving_fixture import SCENARIOS, build_web_fixture  # noqa: E402

_MARKER = ".rquant-web-fixture"


def _make_writable(path: Path) -> None:
    for current, directories, files in os.walk(path):
        os.chmod(current, stat.S_IRWXU)
        for name in files:
            os.chmod(Path(current) / name, stat.S_IRUSR | stat.S_IWUSR)
        for name in directories:
            os.chmod(Path(current) / name, stat.S_IRWXU)


def _generation_count(root: Path) -> int:
    generations = root / "generations"
    return len(list(generations.iterdir())) if generations.is_dir() else 0


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, default="panorama")
    parser.add_argument("--publish-next", action="store_true")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args(arguments)

    root: Path = args.out
    marker = root / _MARKER
    if root.exists() and any(root.iterdir()) and not args.publish_next:
        if not marker.exists():
            parser.error(f"{root} is not empty and was not written by this script")
        if not args.replace:
            parser.error(f"{root} already holds a fixture; pass --replace or --publish-next")
        _make_writable(root)
        shutil.rmtree(root)
    if args.publish_next:
        if not marker.exists():
            parser.error(f"{root} holds no fixture to publish onto")
        recorded = json.loads(marker.read_text(encoding="utf-8"))
        if recorded["scenario"] != args.scenario:
            parser.error(f"{root} holds scenario {recorded['scenario']}, not {args.scenario}")
        sequence = _generation_count(root)
    else:
        sequence = 0

    manifest = build_web_fixture(root, args.scenario, sequence=sequence)
    marker.write_text(json.dumps({"scenario": args.scenario}) + "\n", encoding="utf-8")
    report = {
        "serving_root": str(root),
        "scenario": args.scenario,
        "sequence": sequence,
        "generation_id": manifest.generation_id,
        "built_at": manifest.built_at.isoformat(),
    }
    sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
