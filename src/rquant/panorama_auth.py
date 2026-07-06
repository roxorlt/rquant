"""全景页登录网关：微信友好的 cookie 登录服务（纯标准库，零第三方依赖）。

背景：微信内置浏览器不支持 HTTP basic auth（不弹框、直接 401），无法登录。
改成网页登录页 + 签名 cookie——微信浏览器原生支持 cookie 与表单 POST，朋友在微信里
点链接即可登录。

本模块只用标准库（http.server / hmac / hashlib / base64 / secrets / http.cookies /
urllib.parse），刻意不 import loguru / pydantic 等第三方，保证登录网关可脱离主项目
依赖独立运行、独立测试。凭据（SECRET / 用户库路径）由调用方（cli.py 读 settings）注入。

三个端点（监听 127.0.0.1:8507，只给 nginx 反代，不直接对外）：
- GET  /login   移动端友好登录表单（内联 CSS）；
- POST /login   校验账号密码（pbkdf2），通过则 Set-Cookie 签名令牌 + 302 回 /；
- GET  /verify  nginx auth_request 子请求调用，验签 + 验过期，200 / 401（空 body）。

签名令牌（无会话存储，自验证）：
    payload = f"{user}|{exp_epoch}"
    token   = base64url(payload) + "." + hmac_sha256(payload, SECRET).hexdigest()
验签 = 重算 hmac 用 compare_digest 比对（防时序）+ exp > now。改 SECRET 即全体失效。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.cookies
import logging
import re
import secrets
import tempfile
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

_logger = logging.getLogger("rquant.panorama_auth")

COOKIE_NAME = "rq_panorama"
SESSION_MAX_AGE = 30 * 24 * 3600  # 30 天（秒）
DEFAULT_PORT = 8507
PBKDF2_ITERATIONS = 200000

# 用户名合法字符：字母/数字/点/下划线/短横，1-64 长度。
# 刻意排除 ":"（用户库行分隔符）、"|"（令牌 payload 分隔符）、空白/换行，杜绝注入。
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


# ===== base64url 编解码（去 padding，令牌更干净且仍在 cookie 合法字符集内）=====

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


# ===== 签名令牌 =====

def sign_token(user: str, exp_epoch: int, secret: str) -> str:
    """生成签名令牌 `base64url(user|exp).hmac_sha256(user|exp, SECRET)`。"""
    payload = f"{user}|{exp_epoch}"
    mac = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return _b64url_encode(payload.encode("utf-8")) + "." + mac


def verify_token(token: str, secret: str, now: int | None = None) -> str | None:
    """验签 + 验过期，成功返回用户名，失败返回 None。

    重算 hmac 用 hmac.compare_digest 比对（防时序侧信道）。任一环节异常均判失败。
    """
    if not token or "." not in token:
        return None
    payload_b64, _, mac = token.rpartition(".")
    try:
        payload = _b64url_decode(payload_b64).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None
    expected = hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, mac):
        return None
    user, sep, exp_str = payload.rpartition("|")
    if not sep or not user:
        return None
    try:
        exp = int(exp_str)
    except ValueError:
        return None
    current = int(time.time()) if now is None else now
    if exp <= current:
        return None
    return user


# ===== 密码哈希（pbkdf2_hmac，标准库）=====

def hash_password(password: str, iterations: int = PBKDF2_ITERATIONS) -> str:
    """返回 `pbkdf2_sha256$iter$salt_hex$hash_hex`（salt 每次随机，同密码哈希各异）。"""
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """比对明文密码与存储哈希，格式非法或不匹配均返回 False。"""
    try:
        algo, iter_str, salt_hex, hash_hex = stored.split("$")
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    try:
        iterations = int(iter_str)
        salt = bytes.fromhex(salt_hex)
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(dk.hex(), hash_hex)


def _validate_username(user: str) -> None:
    if not _USERNAME_RE.match(user):
        raise ValueError(
            f"用户名只允许字母/数字/点/下划线/短横（1-64 字符），收到 {user!r}"
        )


# ===== 用户库（纯文本，行格式 user:pbkdf2_sha256$...，0600 权限）=====

def _atomic_write_0600(path: Path, lines: list[str]) -> None:
    """原子写用户库：临时文件（mkstemp 默认 0600）→ os.replace，避免半写与提权窗口。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".panorama-users-")
    tmp_path = Path(tmp)
    try:
        tmp_path.chmod(0o600)
        content = "".join(f"{line}\n" for line in lines)
        with open(fd, "w", encoding="utf-8") as f:
            f.write(content)
        tmp_path.replace(path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


class UserStore:
    """全景页登录用户库读写（单一 helper）。文件不存在时 load/verify 安全降级为空/失败。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _read(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        users: dict[str, str] = {}
        for raw in self.path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            user, _, hashed = line.partition(":")
            user = user.strip()
            hashed = hashed.strip()
            if user and hashed:
                users[user] = hashed
        return users

    def list_users(self) -> list[str]:
        return sorted(self._read())

    def add(self, user: str, password: str, iterations: int = PBKDF2_ITERATIONS) -> None:
        """新增或覆盖同名用户（幂等）。"""
        _validate_username(user)
        users = self._read()
        users[user] = hash_password(password, iterations)
        self._write(users)

    def remove(self, user: str) -> bool:
        """删除用户，存在返回 True，不存在 no-op 返回 False。"""
        users = self._read()
        if user not in users:
            return False
        del users[user]
        self._write(users)
        return True

    def verify(self, user: str, password: str) -> bool:
        stored = self._read().get(user)
        if stored is None:
            return False
        return verify_password(password, stored)

    def _write(self, users: dict[str, str]) -> None:
        _atomic_write_0600(self.path, [f"{u}:{users[u]}" for u in sorted(users)])


# ===== HTTP 服务 =====

def _login_html(error: str = "") -> str:
    """移动端友好登录页（内联 CSS，viewport + 大输入框大按钮 + 错误提示区）。

    错误文案为固定静态串（不回显用户输入），无 XSS 面。
    """
    error_block = (
        f'<div class="err">{error}</div>' if error else ""
    )
    return f"""<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>rQuant 全景页登录</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; min-height: 100vh; display: flex; align-items: center;
    justify-content: center; padding: 24px;
    font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", sans-serif;
    background: #0f172a; color: #e2e8f0;
  }}
  .card {{
    width: 100%; max-width: 380px; background: #1e293b; border-radius: 16px;
    padding: 32px 24px; box-shadow: 0 10px 40px rgba(0,0,0,.4);
  }}
  h1 {{ margin: 0 0 4px; font-size: 22px; text-align: center; }}
  .sub {{ margin: 0 0 24px; font-size: 14px; text-align: center; color: #94a3b8; }}
  .inp {{
    width: 100%; height: 52px; margin-bottom: 14px; padding: 0 16px;
    font-size: 17px; border: 1px solid #334155; border-radius: 10px;
    background: #0f172a; color: #f1f5f9; -webkit-appearance: none;
  }}
  .inp:focus {{ outline: none; border-color: #38bdf8; }}
  .btn {{
    width: 100%; height: 52px; margin-top: 6px; font-size: 18px; font-weight: 600;
    border: none; border-radius: 10px; background: #38bdf8; color: #06283d;
    -webkit-appearance: none;
  }}
  .btn:active {{ background: #0ea5e9; }}
  .err {{
    margin-bottom: 16px; padding: 12px 14px; font-size: 14px; border-radius: 10px;
    background: #7f1d1d; color: #fecaca; text-align: center;
  }}
</style>
</head>
<body>
  <div class="card">
    <h1>rQuant 全景页</h1>
    <p class="sub">请输入账号密码登录</p>
    {error_block}
    <form method="post" action="/login">
      <input class="inp" type="text" name="username" placeholder="账号"
             autocomplete="username" autocapitalize="none" autocorrect="off" required>
      <input class="inp" type="password" name="password" placeholder="密码"
             autocomplete="current-password" required>
      <button class="btn" type="submit">登 录</button>
    </form>
  </div>
</body>
</html>"""


def _build_set_cookie(token: str) -> str:
    """构造 Set-Cookie 头。不加 Secure（28080 走 http），加 HttpOnly + SameSite=Lax。"""
    return (
        f"{COOKIE_NAME}={token}; Max-Age={SESSION_MAX_AGE}; "
        "HttpOnly; Path=/; SameSite=Lax"
    )


def _cookie_value(cookie_header: str | None) -> str | None:
    """从 Cookie 请求头解析 rq_panorama 的值，缺失/非法返回 None。"""
    if not cookie_header:
        return None
    jar: http.cookies.SimpleCookie = http.cookies.SimpleCookie()
    try:
        jar.load(cookie_header)
    except http.cookies.CookieError:
        return None
    morsel = jar.get(COOKIE_NAME)
    return morsel.value if morsel is not None else None


def build_handler_class(
    secret: str, store: UserStore
) -> type[BaseHTTPRequestHandler]:
    """把 SECRET 与用户库绑进 handler 类（便于测试注入不同凭据）。"""

    class _AuthHandler(BaseHTTPRequestHandler):
        server_version = "rquant-panorama-auth/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: object) -> None:
            _logger.info("%s %s", self.address_string(), fmt % args)

        def do_GET(self) -> None:  # noqa: N802  (http.server 约定大写)
            path = urllib.parse.urlparse(self.path).path
            if path == "/verify":
                self._handle_verify()
            elif path == "/login":
                self._send_html(200, _login_html())
            else:
                self._send_status(404)

        def do_POST(self) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path == "/login":
                self._handle_login_post()
            else:
                self._send_status(404)

        def _handle_verify(self) -> None:
            token = _cookie_value(self.headers.get("Cookie"))
            user = verify_token(token, secret) if token else None
            self._send_status(200 if user is not None else 401)

        def _handle_login_post(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length > 0 else b""
            form = urllib.parse.parse_qs(body.decode("utf-8", "replace"))
            username = (form.get("username") or [""])[0].strip()
            password = (form.get("password") or [""])[0]
            if username and store.verify(username, password):
                exp = int(time.time()) + SESSION_MAX_AGE
                token = sign_token(username, exp, secret)
                data = b""
                self.send_response(302)
                self.send_header("Location", "/")
                self.send_header("Set-Cookie", _build_set_cookie(token))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
            else:
                self._send_html(200, _login_html(error="账号或密码错误，请重试"))

        def _send_html(self, status: int, html_text: str) -> None:
            data = html_text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_status(self, status: int) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

    return _AuthHandler


def make_server(
    secret: str,
    users_path: Path,
    host: str = "127.0.0.1",
    port: int = DEFAULT_PORT,
) -> ThreadingHTTPServer:
    """构造登录服务（不阻塞，供测试在线程内 serve_forever）。"""
    store = UserStore(Path(users_path))
    handler_cls = build_handler_class(secret, store)
    return ThreadingHTTPServer((host, port), handler_cls)


def _configure_logging() -> None:
    if not _logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] panorama-auth: %(message)s")
        )
        _logger.addHandler(handler)
        _logger.setLevel(logging.INFO)
        _logger.propagate = False


def serve_auth(
    secret: str,
    users_path: Path,
    host: str = "127.0.0.1",
    port: int = DEFAULT_PORT,
) -> int:
    """启动登录服务（阻塞）。SECRET 为空则明确报错退出，绝不使用空密钥静默降级。"""
    if not secret:
        raise SystemExit(
            "RQUANT_PANORAMA_COOKIE_SECRET 未配置，登录服务拒绝启动"
            "（`openssl rand -hex 32` 生成后写 .env，不使用空密钥）"
        )
    _configure_logging()
    httpd = make_server(secret, users_path, host, port)
    _logger.info("登录服务启动 http://%s:%s（用户库 %s）", host, port, users_path)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        _logger.info("收到中断信号，关闭登录服务")
    finally:
        httpd.server_close()
    return 0
