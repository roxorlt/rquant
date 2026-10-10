from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m rquant.web")
    sub = parser.add_subparsers(dest="cmd", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8768)
    serve.add_argument("--fixture", action="store_true", help="invented demo data")
    sub.add_parser("openapi")
    args = parser.parse_args(argv)

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource

    if args.cmd == "openapi":
        app = create_app(FixtureSource(), dist=None)
        json.dump(app.openapi(), sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return
    import uvicorn

    app = create_app(FixtureSource() if args.fixture else None)
    if args.fixture:
        # Demo mode: pretend page control accepted every command; nothing is written.
        app.state.page_control_transport = lambda payload: {
            "command_id": payload["command_id"], "status": "accepted", "detail": "fixture"}
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
