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
    if fixture is not None and research is not None:
        from rquant.web.commands_service import handle as handle_command

        cmds = research / "commands"
        app.state.commands_dir = cmds

        def transport(payload: dict) -> dict:
            handle_command(cmds, payload)
            if payload["kind"] == "save_alert_rule":
                return fixture.record_rule(payload)
            return {"command_id": payload["command_id"], "status": "succeeded"}

        app.state.page_control_transport = transport
        app.state.preview_writes = True

        def demo_llm(messages: list[dict]) -> str:  # no network in demo mode
            sys_msg = messages[0]["content"]
            user_msg = messages[-1]["content"]
            if "白盒编译器" in sys_msg:
                from rquant.web.screener_engine import parse_nl_locally

                local_blocks = parse_nl_locally(user_msg)
                if local_blocks:
                    return json.dumps(
                        {"blocks": [b.model_dump() for b in local_blocks]},
                        ensure_ascii=False,
                    )
                return json.dumps(
                    {"blocks": [{"type": "tdx", "tdx": "C>MA(C,20) AND V>MA(V,5)*2"}]},
                    ensure_ascii=False,
                )
            if "通达信" in sys_msg:
                return "C>MA(C,20) AND V>MA(V,5)*2"
            return "1. 演示结果。"

        app.state.llm = (demo_llm, "demo")
        from rquant.web.tasks import demo_runner

        app.state.task_runner = demo_runner
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
