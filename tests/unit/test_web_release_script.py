"""``scripts/web-release.sh`` against a local origin, with sudo, curl, uv and ACLs faked.

git, python3 and the filesystem are real: releases are real worktrees of real tags, the
links are really swapped. What is faked is what only the host has — systemd behind sudo,
the API answering on 127.0.0.1:8768, uv building a venv, and setfacl / getfacl.
"""

from __future__ import annotations

import getpass
import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "web-release.sh"

_FAKES = {
    "sudo": """#!/bin/sh
echo "$*" >> "$FAKE_LOG/sudo.log"
""",
    # The API is "up" unless the release `current` points at is listed in FAKE_BAD.
    "curl": """#!/bin/sh
release=$(basename "$(readlink "$RQUANT_WEB_HOME/current" 2>/dev/null)")
for bad in $FAKE_BAD; do
  [ "$release" = "$bad" ] && exit 7
done
[ -f "$FAKE_LOG/api-down" ] && exit 7
if [ "$release" = "v0.34.1" ]; then
  case "$FAKE_META_MODE" in
    unavailable)
      echo '{"data":{"generation":null},"serving":{"generation_id":null,"state":"unavailable"}}'
      exit 0
      ;;
    missing_generation)
      echo '{"data":{"generation":null},"serving":{"generation_id":null,"state":"ready"}}'
      exit 0
      ;;
  esac
fi
printf '{"data":{},"serving":{"generation_id":"fake-generation","state":"%s"}}\n' \
  "${FAKE_SERVING_STATE:-ready}"
""",
    # The self-check runs under `env -i`, so a failing one is baked in at build time.
    "uv": """#!/bin/sh
echo "uv $*" >> "$FAKE_LOG/uv.log"
mkdir -p .venv/bin
if [ -n "$FAKE_SELF_CHECK_FAIL" ]; then
  printf '#!/bin/sh\necho "{\\"ok\\": false}"\nexit 1\n' > .venv/bin/rquant
else
  printf '#!/bin/sh\n[ -n "$RQUANT_SERVING_ROOT" ] || exit 3\necho "{\\"ok\\": true}"\n' \
    > .venv/bin/rquant
fi
chmod +x .venv/bin/rquant
""",
    "setfacl": """#!/bin/sh
[ -n "$FAKE_NO_ACL" ] && exit 1
echo "$*" >> "$FAKE_LOG/setfacl.log"
""",
    "getfacl": """#!/bin/sh
echo "user:$RQUANT_WEB_NGINX_USER:r--"
""",
}


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args), cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(repo: Path, text: str) -> None:
    (repo / "web" / "dist").mkdir(parents=True, exist_ok=True)
    (repo / "web" / "dist" / "index.html").write_text(f"<!doctype html>{text}\n")
    (repo / "pyproject.toml").write_text('[project]\nname = "fake"\n')
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", text)


@pytest.fixture
def host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    monkeypatch.setenv("GIT_AUTHOR_NAME", "t")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "t@example.invalid")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "t")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "t@example.invalid")
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-q", "-b", "main")
    _commit(origin, "v0.34.0")
    _git(origin, "tag", "-a", "v0.34.0", "-m", "v0.34.0")
    _commit(origin, "v0.34.1")
    _git(origin, "tag", "-a", "v0.34.1", "-m", "v0.34.1")
    _commit(origin, "v0.34.2")
    _git(origin, "tag", "-a", "v0.34.2", "-m", "v0.34.2")
    _git(origin, "checkout", "-q", "-b", "side")
    _commit(origin, "side")
    _git(origin, "tag", "v0.34.9")
    _git(origin, "checkout", "-q", "main")

    fakes = tmp_path / "bin"
    fakes.mkdir()
    for name, body in _FAKES.items():
        path = fakes / name
        path.write_text(body)
        path.chmod(0o755)
    log = tmp_path / "log"
    log.mkdir()
    # Put the web home in a parent the test owns, like /home/lighthouse.
    home = tmp_path / "lighthouse" / "rquant-web"
    home.parent.mkdir()
    env = {
        "PATH": f"{fakes}:{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "RQUANT_WEB_HOME": str(home),
        "RQUANT_WEB_REPO_URL": str(origin),
        "RQUANT_WEB_SERVING_ROOT": str(tmp_path / "serving"),
        "RQUANT_WEB_NGINX_USER": getpass.getuser(),
        "RQUANT_WEB_HEALTH_SECONDS": "1",
        "FAKE_LOG": str(log),
        "FAKE_BAD": "",
    }
    return {"env": env, "home": home, "log": log, "origin": origin}


def _run(host: dict[str, object], *args: str, **extra: str) -> subprocess.CompletedProcess[str]:
    env = {**host["env"], **extra}  # type: ignore[dict-item]
    return subprocess.run(
        ("bash", str(SCRIPT), *args), env=env, capture_output=True, text=True, check=False
    )


def _link(home: Path, name: str) -> str:
    return os.readlink(home / name)


def _log(host: dict[str, object], name: str) -> list[str]:
    path = Path(host["log"]) / name  # type: ignore[arg-type]
    return path.read_text().splitlines() if path.exists() else []


def _releases(home: Path) -> list[dict[str, str]]:
    return [json.loads(line) for line in (home / "releases.jsonl").read_text().splitlines()]


def test_dry_run_prints_the_plan_and_creates_nothing(host: dict[str, object]) -> None:
    result = _run(host, "--target", "v0.34.0", "--dry-run")

    assert result.returncode == 0, result.stderr
    assert "[dry-run] tag v0.34.0 exists" in result.stdout
    assert "sudo -n /usr/bin/systemctl restart rquant-web.service" in result.stdout
    assert not Path(host["home"]).exists()  # type: ignore[arg-type]
    assert _log(host, "sudo.log") == []


def test_prepare_builds_release_without_changing_active_links_or_api(
    host: dict[str, object],
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    origin = Path(host["origin"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0
    before_current = _link(home, "current")
    before_app = _link(home, "app")
    before_sudo = _log(host, "sudo.log")
    before_records = _releases(home)

    prepared = _run(host, "--target", "v0.34.1", "--prepare")

    assert prepared.returncode == 0, prepared.stderr
    assert _link(home, "current") == before_current
    assert _link(home, "app") == before_app
    assert not (home / "previous").exists()
    assert _log(host, "sudo.log") == before_sudo
    assert _releases(home) == before_records
    marker = home / "releases" / "v0.34.1" / ".rquant-web-release"
    assert f"commit={_git(origin, 'rev-parse', 'v0.34.1^{commit}')}" in marker.read_text()
    assert len(_log(host, "uv.log")) == 2
    assert len(_log(host, "setfacl.log")) == 4


def test_activation_reuses_prepared_release_without_installing_again(
    host: dict[str, object],
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0
    assert _run(host, "--target", "v0.34.1", "--prepare").returncode == 0
    installs_after_prepare = _log(host, "uv.log")

    activated = _run(host, "--target", "v0.34.1")

    assert activated.returncode == 0, activated.stderr
    assert _link(home, "current") == "releases/v0.34.1"
    assert _link(home, "app") == "releases/v0.34.1/web/dist"
    assert _link(home, "previous") == "releases/v0.34.0"
    assert _log(host, "uv.log") == installs_after_prepare
    assert len(_log(host, "sudo.log")) == 2
    assert [record["target"] for record in _releases(home)] == ["v0.34.0", "v0.34.1"]


def test_prepare_before_first_release_creates_no_active_links(host: dict[str, object]) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]

    prepared = _run(host, "--prepare", "--target", "v0.34.0")

    assert prepared.returncode == 0, prepared.stderr
    assert (home / "releases" / "v0.34.0" / ".rquant-web-release").is_file()
    assert not (home / "current").is_symlink()
    assert not (home / "app").is_symlink()
    assert not (home / "previous").is_symlink()
    assert not (home / "releases.jsonl").exists()
    assert _log(host, "sudo.log") == []


def test_prepare_dry_run_only_plans_preparation(host: dict[str, object]) -> None:
    result = _run(host, "--target", "v0.34.0", "--prepare", "--dry-run")

    assert result.returncode == 0, result.stderr
    assert "[dry-run] tag v0.34.0 exists" in result.stdout
    assert "grant" in result.stdout
    assert "switch current" not in result.stdout
    assert "switch app" not in result.stdout
    assert "restart rquant-web.service" not in result.stdout
    assert not Path(host["home"]).exists()  # type: ignore[arg-type]


def test_prepare_dry_run_for_current_release_does_not_plan_api_work(
    host: dict[str, object],
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0

    result = _run(host, "--target", "v0.34.0", "--prepare", "--dry-run")

    assert result.returncode == 0, result.stderr
    assert "already current" in result.stdout
    assert "nginx access" in result.stdout
    assert "API" not in result.stdout
    assert "restart" not in result.stdout
    assert _link(home, "current") == "releases/v0.34.0"
    assert _log(host, "sudo.log") == ["-n /usr/bin/systemctl restart rquant-web.service"]


@pytest.mark.parametrize(
    "args",
    (
        ("--prepare",),
        ("--rollback", "--prepare"),
        ("--status", "--prepare"),
        ("--target", "v0.34.0", "--prepare", "--no-restart"),
        ("--rollback", "--target", "v0.34.0", "--prepare"),
    ),
)
def test_prepare_rejects_incompatible_options(
    host: dict[str, object], args: tuple[str, ...]
) -> None:
    result = _run(host, *args)

    assert result.returncode == 2
    assert not Path(host["home"]).exists()  # type: ignore[arg-type]


def test_a_release_switches_both_links_restarts_the_api_and_records_it(
    host: dict[str, object],
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]

    result = _run(host, "--target", "v0.34.0")

    assert result.returncode == 0, result.stderr
    assert _link(home, "current") == "releases/v0.34.0"
    assert _link(home, "app") == "releases/v0.34.0/web/dist"
    assert (home / "app" / "index.html").read_text().startswith("<!doctype html>v0.34.0")
    assert not (home / "previous").exists()
    assert _log(host, "sudo.log") == ["-n /usr/bin/systemctl restart rquant-web.service"]
    assert _log(host, "uv.log") == ["uv sync --quiet --frozen --python 3.11 --no-dev"]
    [record] = _releases(home)
    assert (record["action"], record["target"], record["result"], record["access"]) == (
        "release",
        "v0.34.0",
        "ok",
        "acl",
    )
    # nginx gets traverse on the parents (the owned parent too) and read on web/dist only.
    acl = _log(host, "setfacl.log")
    user = host["env"]["RQUANT_WEB_NGINX_USER"]  # type: ignore[index]
    assert acl[0].startswith(f"-m u:{user}:--x {home.parent} {home} {home / 'releases'}")
    assert acl[1] == f"-R -m u:{user}:rX {home / 'releases' / 'v0.34.0' / 'web' / 'dist'}"
    # Everything the script created is lighthouse-only; nginx relies on the ACL alone.
    assert stat.S_IMODE((home / "releases").stat().st_mode) == 0o700


def test_re_running_the_current_target_changes_nothing(host: dict[str, object]) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0

    again = _run(host, "--target", "v0.34.0")

    assert again.returncode == 0, again.stderr
    assert "already current" in again.stdout
    assert len(_log(host, "sudo.log")) == 1
    assert len(_log(host, "uv.log")) == 1
    assert len(_releases(home)) == 1


def test_a_second_release_keeps_the_first_as_previous_and_rollback_returns_to_it(
    host: dict[str, object],
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0
    assert _run(host, "--target", "v0.34.1").returncode == 0
    assert _link(home, "current") == "releases/v0.34.1"
    assert _link(home, "previous") == "releases/v0.34.0"

    dry = _run(host, "--rollback", "--dry-run")
    assert dry.returncode == 0 and "to v0.34.0" in dry.stdout
    assert _link(home, "current") == "releases/v0.34.1"

    back = _run(host, "--rollback")

    assert back.returncode == 0, back.stderr
    assert _link(home, "current") == "releases/v0.34.0"
    assert _link(home, "app") == "releases/v0.34.0/web/dist"
    assert _link(home, "previous") == "releases/v0.34.1"
    assert _releases(home)[-1]["action"] == "rollback"


def test_a_release_whose_api_does_not_answer_is_rolled_back(host: dict[str, object]) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0

    bad = _run(host, "--target", "v0.34.1", FAKE_BAD="v0.34.1")

    assert bad.returncode == 1
    assert "rolled back to v0.34.0" in bad.stderr
    assert _link(home, "current") == "releases/v0.34.0"
    # The static files were never switched to the failed release.
    assert _link(home, "app") == "releases/v0.34.0/web/dist"
    assert _releases(home)[-1]["result"] == "rolled_back"
    assert len(_log(host, "sudo.log")) == 3


@pytest.mark.parametrize("meta_mode", ("unavailable", "missing_generation"))
def test_an_http_200_without_usable_serving_rolls_back_before_switching_app(
    host: dict[str, object], meta_mode: str
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    assert _run(host, "--target", "v0.34.0").returncode == 0

    bad = _run(host, "--target", "v0.34.1", FAKE_META_MODE=meta_mode)

    assert bad.returncode == 1
    assert "rolled back to v0.34.0" in bad.stderr
    assert _link(home, "current") == "releases/v0.34.0"
    assert _link(home, "app") == "releases/v0.34.0/web/dist"
    assert _releases(home)[-1]["result"] == "rolled_back"
    assert len(_log(host, "sudo.log")) == 3


@pytest.mark.parametrize("serving_state", ("ready", "stale", "degraded"))
def test_a_release_with_a_generation_and_usable_serving_state_can_activate(
    host: dict[str, object], serving_state: str
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]

    result = _run(host, "--target", "v0.34.0", FAKE_SERVING_STATE=serving_state)

    assert result.returncode == 0, result.stderr
    assert _link(home, "app") == "releases/v0.34.0/web/dist"


def test_a_failed_self_check_publishes_nothing(host: dict[str, object]) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]

    result = _run(host, "--target", "v0.34.0", FAKE_SELF_CHECK_FAIL="1")

    assert result.returncode == 1
    assert "self-check failed" in result.stderr
    assert not (home / "current").exists()
    assert _log(host, "sudo.log") == []
    # The next run rebuilds the incomplete release instead of trusting it.
    retry = _run(host, "--target", "v0.34.0")
    assert retry.returncode == 0, retry.stderr
    assert "removing the incomplete release" in retry.stdout


def test_without_acl_support_it_falls_back_to_world_readable_dist(
    host: dict[str, object],
) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]

    result = _run(host, "--target", "v0.34.0", FAKE_NO_ACL="1")

    assert result.returncode == 0, result.stderr
    dist = home / "releases" / "v0.34.0" / "web" / "dist"
    assert (dist / "index.html").stat().st_mode & stat.S_IROTH
    for directory in (home.parent, home, home / "releases", dist.parent.parent, dist.parent):
        assert directory.stat().st_mode & stat.S_IXOTH, directory
    assert _releases(home)[-1]["access"] == "chmod"


def test_no_restart_leaves_the_unit_alone(host: dict[str, object]) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]

    result = _run(host, "--target", "v0.34.0", "--no-restart")

    assert result.returncode == 0, result.stderr
    assert _link(home, "current") == "releases/v0.34.0"
    assert _log(host, "sudo.log") == []


@pytest.mark.parametrize(
    ("target", "message"),
    (
        ("v0.34.9", "is not on main"),
        ("v0.99.0", "does not exist"),
        ("main", "must be an exact tag"),
        ("v0.34", "must be an exact tag"),
    ),
)
def test_only_exact_tags_on_main_are_published(
    host: dict[str, object], target: str, message: str
) -> None:
    result = _run(host, "--target", target)

    assert result.returncode == 1
    assert message in result.stderr
    assert not (Path(host["home"]) / "current").exists()  # type: ignore[arg-type]


def test_only_the_three_newest_releases_are_kept(host: dict[str, object]) -> None:
    home = Path(host["home"])  # type: ignore[arg-type]
    origin = Path(host["origin"])  # type: ignore[arg-type]
    for index in range(3, 6):
        _commit(origin, f"v0.34.{index}")
        _git(origin, "tag", "-a", f"v0.34.{index}", "-m", "t")
    for index in range(6):
        result = _run(host, "--target", f"v0.34.{index}")
        assert result.returncode == 0, result.stderr

    kept = sorted(path.name for path in (home / "releases").iterdir())

    assert kept == ["v0.34.3", "v0.34.4", "v0.34.5"]
    assert _link(home, "previous") == "releases/v0.34.4"
