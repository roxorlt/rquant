"""全景页登录网关单测（U1-U6，全离线，标准库）。"""

from __future__ import annotations

import http.client
import threading
import time
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from rquant.cli import build_parser
from rquant.panorama_auth import (
    COOKIE_NAME,
    SESSION_MAX_AGE,
    UserStore,
    _build_set_cookie,
    _cookie_value,
    derive_gate_token,
    hash_password,
    make_server,
    serve_auth,
    sign_token,
    verify_password,
    verify_token,
)

_SECRET = "0123456789abcdef0123456789abcdef"
# map 方案：登录成功下发的固定网关令牌（从 _SECRET 确定性派生，测试全程用它比对 cookie）。
_GATE = derive_gate_token(_SECRET)


# ===== U1 签名令牌 =====

class TestSignToken:
    def test_roundtrip(self) -> None:
        exp = int(time.time()) + 3600
        token = sign_token("alice", exp, _SECRET)
        assert verify_token(token, _SECRET) == "alice"

    def test_tampered_user_fails(self) -> None:
        # 拿别人的 payload（不同 user）拼上原 token 的 mac，验签必须失败
        exp = int(time.time()) + 3600
        token = sign_token("alice", exp, _SECRET)
        other = sign_token("bob", exp, _SECRET)
        frankenstein = other.rpartition(".")[0] + "." + token.rpartition(".")[2]
        assert verify_token(frankenstein, _SECRET) is None

    def test_tampered_exp_fails(self) -> None:
        exp = int(time.time()) + 3600
        token = sign_token("alice", exp, _SECRET)
        forged = sign_token("alice", exp + 999, _SECRET)
        frankenstein = forged.rpartition(".")[0] + "." + token.rpartition(".")[2]
        assert verify_token(frankenstein, _SECRET) is None

    def test_tampered_mac_fails(self) -> None:
        exp = int(time.time()) + 3600
        token = sign_token("alice", exp, _SECRET)
        head, _, mac = token.rpartition(".")
        flipped = mac[:-1] + ("0" if mac[-1] != "0" else "1")
        assert verify_token(head + "." + flipped, _SECRET) is None

    def test_wrong_secret_fails(self) -> None:
        exp = int(time.time()) + 3600
        token = sign_token("alice", exp, _SECRET)
        assert verify_token(token, "another-secret-entirely") is None

    def test_expired_rejected(self) -> None:
        now = 1_000_000
        token = sign_token("alice", now + 10, _SECRET)
        assert verify_token(token, _SECRET, now=now + 100) is None
        assert verify_token(token, _SECRET, now=now) == "alice"

    def test_garbage_token(self) -> None:
        assert verify_token("", _SECRET) is None
        assert verify_token("no-dot-here", _SECRET) is None
        assert verify_token("@@@.deadbeef", _SECRET) is None


# ===== U2 pbkdf2 =====

class TestPassword:
    def test_correct_and_wrong(self) -> None:
        stored = hash_password("s3cr3t")
        assert verify_password("s3cr3t", stored) is True
        assert verify_password("wrong", stored) is False

    def test_iterations_meets_floor(self) -> None:
        stored = hash_password("pw")
        _, iter_str, _, _ = stored.split("$")
        assert int(iter_str) >= 200000

    def test_same_password_different_salt(self) -> None:
        a = hash_password("same")
        b = hash_password("same")
        assert a != b  # salt 不同 → 哈希不同
        assert verify_password("same", a)
        assert verify_password("same", b)

    def test_malformed_stored(self) -> None:
        assert verify_password("x", "not-a-valid-record") is False
        assert verify_password("x", "md5$1$aa$bb") is False


# ===== U3 用户库 CRUD =====

class TestUserStore:
    def test_add_list_remove(self, tmp_path: Path) -> None:
        store = UserStore(tmp_path / "users.txt")
        assert store.list_users() == []  # 文件不存在 → 空
        store.add("alice", "pw1")
        store.add("bob", "pw2")
        assert store.list_users() == ["alice", "bob"]
        assert store.verify("alice", "pw1")
        assert not store.verify("alice", "pw2")
        assert store.remove("alice") is True
        assert store.list_users() == ["bob"]

    def test_remove_absent_is_noop(self, tmp_path: Path) -> None:
        store = UserStore(tmp_path / "users.txt")
        assert store.remove("ghost") is False
        store.add("alice", "pw")
        assert store.remove("ghost") is False
        assert store.list_users() == ["alice"]

    def test_overwrite_same_name(self, tmp_path: Path) -> None:
        store = UserStore(tmp_path / "users.txt")
        store.add("alice", "old")
        store.add("alice", "new")
        assert store.list_users() == ["alice"]
        assert not store.verify("alice", "old")
        assert store.verify("alice", "new")

    def test_verify_missing_file(self, tmp_path: Path) -> None:
        store = UserStore(tmp_path / "nope.txt")
        assert store.verify("alice", "pw") is False

    def test_file_permissions_0600(self, tmp_path: Path) -> None:
        path = tmp_path / "users.txt"
        UserStore(path).add("alice", "pw")
        assert (path.stat().st_mode & 0o777) == 0o600

    def test_rejects_bad_username(self, tmp_path: Path) -> None:
        store = UserStore(tmp_path / "users.txt")
        for bad in ["a:b", "a|b", "with space", "", "x" * 65]:
            with pytest.raises(ValueError):
                store.add(bad, "pw")


# ===== U4 / E1 HTTP 端点 =====

@contextmanager
def _running_server(tmp_path: Path) -> Iterator[tuple[str, int]]:
    store = UserStore(tmp_path / "users.txt")
    store.add("alice", "pw")
    srv = make_server(_GATE, tmp_path / "users.txt", host="127.0.0.1", port=0)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = srv.server_address[0], srv.server_address[1]
        yield host, port
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join(timeout=2)


def _login(host: str, port: int, user: str, password: str) -> http.client.HTTPResponse:
    conn = http.client.HTTPConnection(host, port, timeout=5)
    body = urllib.parse.urlencode({"username": user, "password": password})
    conn.request(
        "POST", "/login", body=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    return conn.getresponse()


class TestHttpEndpoints:
    def test_get_login_returns_form(self, tmp_path: Path) -> None:
        with _running_server(tmp_path) as (host, port):
            conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request("GET", "/login")
            resp = conn.getresponse()
            html = resp.read().decode("utf-8")
        assert resp.status == 200
        assert "<form" in html
        assert 'name="username"' in html
        assert 'name="password"' in html

    def test_post_correct_sets_cookie_and_redirects(self, tmp_path: Path) -> None:
        with _running_server(tmp_path) as (host, port):
            resp = _login(host, port, "alice", "pw")
            resp.read()
            set_cookie = resp.getheader("Set-Cookie")
            location = resp.getheader("Location")
        assert resp.status == 302
        assert location == "/"
        assert set_cookie is not None
        assert set_cookie.startswith(f"{COOKIE_NAME}=")
        # map 方案：cookie 值是固定网关令牌（不再 per-user 签名令牌），且等于 GATE_TOKEN。
        token = set_cookie.split(";")[0].split("=", 1)[1]
        assert token == _GATE

    def test_post_wrong_returns_login_no_cookie(self, tmp_path: Path) -> None:
        with _running_server(tmp_path) as (host, port):
            resp = _login(host, port, "alice", "WRONG")
            html = resp.read().decode("utf-8")
            set_cookie = resp.getheader("Set-Cookie")
        assert resp.status == 200
        assert set_cookie is None
        assert "账号或密码错误" in html
        assert "<form" in html

    def test_verify_no_cookie_401(self, tmp_path: Path) -> None:
        with _running_server(tmp_path) as (host, port):
            conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request("GET", "/verify")
            resp = conn.getresponse()
            resp.read()
        assert resp.status == 401

    def test_verify_valid_cookie_200(self, tmp_path: Path) -> None:
        # map 方案：/verify 用 compare_digest 比对 cookie 是否等于固定网关令牌。
        with _running_server(tmp_path) as (host, port):
            conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request("GET", "/verify", headers={"Cookie": f"{COOKIE_NAME}={_GATE}"})
            resp = conn.getresponse()
            resp.read()
        assert resp.status == 200

    def test_verify_tampered_cookie_401(self, tmp_path: Path) -> None:
        with _running_server(tmp_path) as (host, port):
            tampered = _GATE[:-1] + ("0" if _GATE[-1] != "0" else "1")
            conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request(
                "GET", "/verify", headers={"Cookie": f"{COOKIE_NAME}={tampered}"}
            )
            resp = conn.getresponse()
            resp.read()
        assert resp.status == 401


# ===== U5 CLI 解析 =====

class TestCliParsing:
    def test_auth_serve_defaults(self) -> None:
        args = build_parser().parse_args(["panorama-auth-serve"])
        assert args.command == "panorama-auth-serve"
        assert args.host == "127.0.0.1"
        assert args.port == 8507

    def test_auth_serve_custom_port(self) -> None:
        args = build_parser().parse_args(["panorama-auth-serve", "--port", "9000"])
        assert args.port == 9000

    def test_user_add_parses_name(self) -> None:
        args = build_parser().parse_args(["panorama-user-add", "carol"])
        assert args.command == "panorama-user-add"
        assert args.name == "carol"

    def test_user_remove_parses_name(self) -> None:
        args = build_parser().parse_args(["panorama-user-remove", "carol"])
        assert args.name == "carol"

    def test_user_list_parses(self) -> None:
        args = build_parser().parse_args(["panorama-user-list"])
        assert args.command == "panorama-user-list"

    def test_gate_token_parses(self) -> None:
        args = build_parser().parse_args(["panorama-gate-token"])
        assert args.command == "panorama-gate-token"

    def test_user_add_via_getpass_mock(self, tmp_path: Path, monkeypatch) -> None:
        import getpass as getpass_mod

        from rquant import cli
        from rquant.config import settings

        users_path = tmp_path / "users.txt"
        monkeypatch.setattr(settings, "panorama_users_path", users_path)
        monkeypatch.setattr(getpass_mod, "getpass", lambda prompt="": "hunter2")

        args = build_parser().parse_args(["panorama-user-add", "dave"])
        assert cli.cmd_panorama_user_add(args) == 0
        assert UserStore(users_path).verify("dave", "hunter2")

    def test_user_add_mismatch_returns_1(self, tmp_path: Path, monkeypatch) -> None:
        import getpass as getpass_mod

        from rquant import cli
        from rquant.config import settings

        users_path = tmp_path / "users.txt"
        monkeypatch.setattr(settings, "panorama_users_path", users_path)
        seq = iter(["first", "second"])
        monkeypatch.setattr(getpass_mod, "getpass", lambda prompt="": next(seq))

        args = build_parser().parse_args(["panorama-user-add", "eve"])
        assert cli.cmd_panorama_user_add(args) == 1
        assert not users_path.exists()


# ===== U6 安全 =====

class TestSecurity:
    def test_cookie_flags(self) -> None:
        cookie = _build_set_cookie("abc.def")
        assert "HttpOnly" in cookie
        assert "SameSite=Lax" in cookie
        assert f"Max-Age={SESSION_MAX_AGE}" in cookie
        assert "Path=/" in cookie
        assert "Secure" not in cookie  # 28080 走 http，刻意不加

    def test_cookie_value_parsing(self) -> None:
        assert _cookie_value(None) is None
        assert _cookie_value("other=1") is None
        assert _cookie_value(f"{COOKIE_NAME}=xyz; other=1") == "xyz"

    def test_gate_token_missing_raises_systemexit(self, tmp_path: Path) -> None:
        # 网关令牌为空（GATE_TOKEN 与 COOKIE_SECRET 均未配置）→ 登录服务拒绝启动。
        with pytest.raises(SystemExit):
            serve_auth("", tmp_path / "users.txt")


# ===== U7 map 网关令牌派生 =====

class TestGateTokenDerivation:
    def test_deterministic(self) -> None:
        # 同 secret 两次派生结果相同 → 重启后网关令牌稳定不变。
        assert derive_gate_token(_SECRET) == derive_gate_token(_SECRET)

    def test_differs_by_secret(self) -> None:
        assert derive_gate_token(_SECRET) != derive_gate_token("another-secret-value")

    def test_empty_secret_is_empty(self) -> None:
        assert derive_gate_token("") == ""

    def test_length_32_hex(self) -> None:
        token = derive_gate_token(_SECRET)
        assert len(token) == 32
        assert all(c in "0123456789abcdef" for c in token)


# ===== U8 panorama-gate-token CLI =====

class TestGateTokenCli:
    def test_prints_derived_token(self, capsys, monkeypatch) -> None:
        # 未显式配 GATE_TOKEN → 从 cookie_secret 派生并打印到 stdout。
        from rquant import cli
        from rquant.config import settings

        monkeypatch.setattr(settings, "panorama_gate_token", "")
        monkeypatch.setattr(settings, "panorama_cookie_secret", _SECRET)
        args = build_parser().parse_args(["panorama-gate-token"])
        assert cli.cmd_panorama_gate_token(args) == 0
        assert capsys.readouterr().out.strip() == derive_gate_token(_SECRET)

    def test_explicit_token_overrides_secret(self, capsys, monkeypatch) -> None:
        from rquant import cli
        from rquant.config import settings

        monkeypatch.setattr(settings, "panorama_gate_token", "explicit-token-abc123")
        monkeypatch.setattr(settings, "panorama_cookie_secret", _SECRET)
        args = build_parser().parse_args(["panorama-gate-token"])
        assert cli.cmd_panorama_gate_token(args) == 0
        assert capsys.readouterr().out.strip() == "explicit-token-abc123"

    def test_unconfigured_returns_1_no_stdout(self, capsys, monkeypatch) -> None:
        # GATE_TOKEN 与 COOKIE_SECRET 均空 → 返回 1，stdout 不打印空令牌（错误走 stderr）。
        from rquant import cli
        from rquant.config import settings

        monkeypatch.setattr(settings, "panorama_gate_token", "")
        monkeypatch.setattr(settings, "panorama_cookie_secret", "")
        args = build_parser().parse_args(["panorama-gate-token"])
        assert cli.cmd_panorama_gate_token(args) == 1
        assert capsys.readouterr().out.strip() == ""
