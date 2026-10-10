"""The /app/ install kit: nginx block, rquant-web.service, the sudoers drop-in.

Static checks only; what needs the host (systemd-analyze verify, nginx -t, ACLs) is in
the DEPLOY.md 待安装 entry.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NGINX = ROOT / "deploy" / "nginx" / "rquant-backup.conf"
UNIT = ROOT / "deploy" / "systemd" / "rquant-web.service"
SUDOERS = ROOT / "deploy" / "sudoers" / "rquant-web"
RELEASE = ROOT / "scripts" / "web-release.sh"
HTPASSWD = "auth_basic_user_file /www/server/nginx/conf/.rquant-backup.htpasswd;"


def _locations(text: str) -> dict[str, str]:
    blocks: dict[str, str] = {}
    for match in re.finditer(r"location\s+(=\s+)?(\S+)\s*\{(.*?)\n    \}", text, flags=re.S):
        blocks[f"{(match.group(1) or '').strip()}{match.group(2)}"] = match.group(3)
    return blocks


def test_the_app_block_sits_after_preview_and_before_upload() -> None:
    text = NGINX.read_text(encoding="utf-8")
    order = [
        text.index(marker)
        for marker in ("location /preview/", "location = /app", "location /upload/")
    ]
    assert order == sorted(order)


def test_the_api_location_proxies_to_the_loopback_api_with_the_port_and_the_user() -> None:
    api = _locations(NGINX.read_text(encoding="utf-8"))["/app/api/"]

    assert "proxy_pass http://127.0.0.1:8768/api/;" in api
    # The write guard compares Origin's host:port with Host (src/rquant/web/security.py).
    assert "proxy_set_header Host $host:$server_port;" in api
    # basic auth fills $remote_user, overwriting any header the browser sent.
    assert "proxy_set_header X-Rquant-User $remote_user;" in api
    assert "client_max_body_size 1m;" in api


def test_every_app_location_uses_the_same_login_as_the_other_pages() -> None:
    blocks = _locations(NGINX.read_text(encoding="utf-8"))

    assert HTPASSWD in blocks["/preview/"]
    for name in ("/app/api/", "/app/assets/", "/app/"):
        assert HTPASSWD in blocks[name], name
    assert "return 301 /app/;" in blocks["=/app"]


def test_static_files_come_from_the_release_link_with_the_csp() -> None:
    blocks = _locations(NGINX.read_text(encoding="utf-8"))

    assert "alias /home/lighthouse/rquant-web/app/;" in blocks["/app/"]
    assert "alias /home/lighthouse/rquant-web/app/assets/;" in blocks["/app/assets/"]
    csp = next(line for line in blocks["/app/"].splitlines() if "Content-Security-Policy" in line)
    assert "script-src 'self';" in csp
    assert "unsafe-eval" not in csp
    assert 'Cache-Control "no-cache"' in blocks["/app/"]
    assert "immutable" in blocks["/app/assets/"]


def _directives(path: Path) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith(("#", "[")) or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values.setdefault(key, []).append(value)
    return values


def test_the_unit_is_bounded_loopback_read_only_and_has_no_env_file() -> None:
    unit = _directives(UNIT)

    assert unit["Slice"] == ["rquant-serving.slice"]
    assert unit["MemoryHigh"] == ["384M"]
    assert unit["MemoryMax"] == ["640M"]
    assert unit["Restart"] == ["on-failure"]
    assert unit["User"] == ["lighthouse"]
    assert unit["WorkingDirectory"] == ["/home/lighthouse/rquant-web/current"]
    assert unit["ExecStart"] == [
        "/home/lighthouse/rquant-web/current/.venv/bin/rquant web-serve --bind 127.0.0.1:8768"
    ]
    assert "EnvironmentFile" not in unit
    assert "RQUANT_DISABLE_DOTENV=1" in " ".join(unit["Environment"])
    assert unit["ProtectSystem"] == ["strict"]
    assert unit["ProtectHome"] == ["read-only"]
    assert unit["ReadOnlyPaths"] == ["/home/lighthouse/rquant/data/runtime/serving"]
    hidden = unit["InaccessiblePaths"][0].split()
    assert "/home/lighthouse/rquant/.env" in hidden
    assert "-/home/lighthouse/rquant/data/rquant.duckdb" in hidden
    assert "-/home/lighthouse/rquant/data/rquant_ro.duckdb" in hidden
    assert unit["IPAddressDeny"] == ["any"]
    assert "ReadWritePaths" not in unit


def test_the_sudoers_drop_in_allows_exactly_the_restart_the_release_script_runs() -> None:
    rules = [
        line
        for line in SUDOERS.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]

    assert rules == [
        "lighthouse ALL=(root) NOPASSWD: /usr/bin/systemctl restart rquant-web.service"
    ]
    assert 'RESTART=(sudo -n /usr/bin/systemctl restart "${UNIT}")' in RELEASE.read_text()
    assert 'UNIT="rquant-web.service"' in RELEASE.read_text()
