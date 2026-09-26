"""``rquant web-serve`` and ``rquant web-openapi``.

``rquant.cli.main`` hands these two commands over before it constructs ``Settings``:
the web process runs without ``.env`` (its unit hides the file), like
``runtime-authority-stage``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

WEB_COMMANDS = frozenset({"web-serve", "web-openapi"})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rquant")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("web-serve", help="启动只读网页 API（/app/api/ 背后的进程）")
    serve.add_argument("--bind", help="host:port，只允许回环地址（默认 127.0.0.1:8768）")
    serve.add_argument(
        "--self-check",
        action="store_true",
        help="只导入并读一次 serving 数据代，然后退出（发布脚本用）",
    )
    commands.add_parser("web-openapi", help="把 OpenAPI 文档输出到标准输出")
    return parser


def _self_check(settings: object) -> int:
    from rquant.web.serving import GenerationTracker
    from rquant.web.settings import WebSettings

    assert isinstance(settings, WebSettings)
    tracker = GenerationTracker(settings.serving_root)
    try:
        tracker.refresh()
        with tracker.borrow() as borrowed:
            report = {
                "ok": borrowed is not None,
                "serving_root": str(settings.serving_root),
                "generation_id": None if borrowed is None else borrowed.manifest.generation_id,
                "detail": tracker.failure,
            }
    finally:
        tracker.close()
    sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
    return 0 if report["ok"] else 1


def main(argv: Sequence[str]) -> int:
    args = _parser().parse_args(list(argv))
    from rquant.web.app import create_app, openapi_document
    from rquant.web.settings import WebSettings

    if args.command == "web-openapi":
        # Building the app opens nothing, so any serving root will do here.
        app = create_app(WebSettings(serving_root=Path("data/runtime/serving")), background=False)
        sys.stdout.write(openapi_document(app))
        return 0

    settings = WebSettings.from_env(bind=args.bind)
    if args.self_check:
        return _self_check(settings)

    import uvicorn

    uvicorn.run(
        create_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        workers=1,
        proxy_headers=False,
        server_header=False,
        access_log=False,
        log_level="info",
    )
    return 0
