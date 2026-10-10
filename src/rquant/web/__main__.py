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

    fixture = FixtureSource() if args.fixture else None
    research = None
    if fixture is not None:
        import tempfile
        from pathlib import Path

        from rquant.web.source import write_demo_research

        research = Path(tempfile.mkdtemp(prefix="rquant-research-"))
        write_demo_research(research)
    app = create_app(fixture, research_root=research)
    if fixture is not None:
        # Demo mode: acks land in the in-memory fixture (as if page control + Serving
        # republished); other commands are accepted and dropped. Nothing touches disk.
        def transport(payload: dict) -> dict:
            if payload["kind"] == "ack_alert":
                return fixture.record_ack(payload)
            if payload["kind"] == "save_alert_rule":
                return fixture.record_rule(payload)
            return {"command_id": payload["command_id"], "status": "succeeded"}

        app.state.page_control_transport = transport
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
