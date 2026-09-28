"""The Web acknowledgment ingress uses an OS permission boundary."""

from __future__ import annotations

import asyncio
import grp
import os
import socket
import stat
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
import uvicorn
from fastapi import FastAPI

from rquant.web import cli as web_cli
from rquant.web.ingress import private_web_ingress_socket

SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"


def test_ack_web_cli_uses_only_private_unix_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    with TemporaryDirectory(prefix="rqi-", dir=SHORT_TMP) as directory:
        root = Path(directory)
        os.chmod(root, 0o755)
        parent = root / "ingress"
        parent.mkdir(mode=0o710)
        os.chown(parent, -1, os.getegid())
        os.chmod(parent, 0o710)
        path = parent / "web.sock"
        monkeypatch.setenv("RQUANT_SERVING_ROOT", str(root / "serving"))
        monkeypatch.setenv("RQUANT_WEB_INGRESS_SOCKET", str(path))
        monkeypatch.setenv("RQUANT_WEB_ACK_ADMISSION_SOCKET", str(root / "ack" / "ack.sock"))
        monkeypatch.delenv("RQUANT_WEB_BIND", raising=False)
        def nginx_group(name: str) -> SimpleNamespace:
            assert name == "www"
            return SimpleNamespace(gr_gid=os.getegid())

        monkeypatch.setattr(grp, "getgrnam", nginx_group)
        observed: dict[str, object] = {}

        def fake_run(_app: object, **options: object) -> None:
            observed.update(options)
            assert "host" not in options and "port" not in options
            fd = options.get("fd")
            assert isinstance(fd, int)
            assert stat.S_ISSOCK(os.fstat(fd).st_mode)
            info = path.lstat()
            assert stat.S_ISSOCK(info.st_mode)
            assert (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (
                os.geteuid(),
                os.getegid(),
                0o660,
            )
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(path))

        monkeypatch.setattr(uvicorn, "run", fake_run)
        assert web_cli.main(["web-serve"]) == 0
        assert "fd" in observed
        assert not path.exists()


def test_private_ingress_rejects_a_directory_other_users_can_traverse() -> None:
    with TemporaryDirectory(prefix="rqi-", dir=SHORT_TMP) as directory:
        parent = Path(directory) / "ingress"
        parent.mkdir(mode=0o755)
        os.chown(parent, -1, os.getegid())
        os.chmod(parent, 0o755)
        path = parent / "web.sock"
        with pytest.raises(ValueError, match="mode 0710"), private_web_ingress_socket(
            path, nginx_group_gid=os.getegid()
        ):
            pytest.fail("unsafe directory accepted")
        assert not path.exists()


def test_private_ingress_refuses_to_replace_an_existing_path() -> None:
    with TemporaryDirectory(prefix="rqi-", dir=SHORT_TMP) as directory:
        parent = Path(directory) / "ingress"
        parent.mkdir(mode=0o710)
        os.chown(parent, -1, os.getegid())
        os.chmod(parent, 0o710)
        path = parent / "web.sock"
        path.write_text("occupied")
        with pytest.raises(ValueError, match="already exists"), private_web_ingress_socket(
            path, nginx_group_gid=os.getegid()
        ):
            pytest.fail("existing path was replaced")
        assert path.read_text() == "occupied"


def test_uvicorn_fd_serves_only_the_prebound_unix_socket() -> None:
    with TemporaryDirectory(prefix="rqi-", dir=SHORT_TMP) as directory:
        parent = Path(directory) / "ingress"
        parent.mkdir(mode=0o710)
        os.chown(parent, -1, os.getegid())
        os.chmod(parent, 0o710)
        path = parent / "web.sock"
        app = FastAPI()

        @app.get("/")
        def health() -> dict[str, bool]:
            return {"ok": True}

        with private_web_ingress_socket(path, nginx_group_gid=os.getegid()) as listener:
            config = uvicorn.Config(
                app,
                fd=listener.fileno(),
                workers=1,
                lifespan="off",
                log_level="critical",
                proxy_headers=False,
            )
            server = uvicorn.Server(config)

            async def exercise() -> None:
                task = asyncio.create_task(server.serve())
                try:
                    for _ in range(100):
                        if server.started:
                            break
                        await asyncio.sleep(0.01)
                    assert server.started
                    assert len(server.servers) == 1
                    assert server.servers[0].sockets[0].family == socket.AF_UNIX
                    reader, writer = await asyncio.open_unix_connection(str(path))
                    writer.write(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
                    await writer.drain()
                    response = await reader.read()
                    writer.close()
                    await writer.wait_closed()
                    assert b"HTTP/1.1 200 OK" in response
                    assert stat.S_IMODE(path.lstat().st_mode) == 0o660
                finally:
                    server.should_exit = True
                    await asyncio.wait_for(task, timeout=5)

            asyncio.run(exercise())
