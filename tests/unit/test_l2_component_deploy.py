"""Offline deployment gates and exact component lifecycle, using synthetic inputs."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
TARGET, TREE, MAIN = "a" * 40, "b" * 40, "c" * 40
JOBS = {"L2 raw feed (Python 3.11)", "L2 raw feed (Python 3.12)", "web"}


def deployer():
    path = ROOT / "scripts/deploy-l2-component.py"
    if not path.exists():
        pytest.fail("L2 component deployment is not implemented")
    spec = importlib.util.spec_from_file_location("l2_component_deploy", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CacheResponse(io.BytesIO):
    def __init__(self, payload: bytes):
        super().__init__(payload)
        self.read_limits: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_limits.append(size)
        return super().read(size)


@pytest.mark.parametrize("response_bytes", [17_100_000, 25_232_061])
def test_health_batches_reads_complete_large_json_envelope(monkeypatch, response_bytes: int) -> None:
    d = deployer()
    prefix, suffix = b'{"padding":"', b'","tail":"complete"}'
    padding_size = response_bytes - len(prefix) - len(suffix)
    response = CacheResponse(prefix + b"x" * padding_size + suffix)
    calls = []

    def open_response(url, *, timeout):
        calls.append((url, timeout))
        return response

    monkeypatch.setattr(d, "urlopen", open_response)
    result = d.health_batches(7)
    assert len(result["padding"]) == padding_size and result["tail"] == "complete"
    assert calls == [("http://127.0.0.1:8771/api/v1/l2/batches?after_sequence=7&limit=1", 5)]
    assert response.read_limits == [(32 << 20) + 1] and response.closed


def test_health_status_keeps_original_read_and_timeout_limits(monkeypatch) -> None:
    d = deployer(); response = CacheResponse(b'{"state":"connected","last_sequence":1}')
    calls = []

    def open_response(url, *, timeout):
        calls.append((url, timeout))
        return response

    monkeypatch.setattr(d, "urlopen", open_response)
    assert d.health_status() == {"state": "connected", "last_sequence": 1}
    assert calls == [("http://127.0.0.1:8771/api/v1/l2/status", 2)]
    assert response.read_limits == [(2 << 20) + 1] and response.closed


@pytest.mark.parametrize("failure", ["oversize", "invalid_json", "duplicate_key", "non_object", "read_error", "read_timeout"])
def test_health_batches_closes_failed_response(monkeypatch, failure: str) -> None:
    d = deployer()
    payload = {"invalid_json": b"{", "duplicate_key": b'{"a":1,"a":2}', "non_object": b"[]"}.get(failure, b"{}")
    if failure == "oversize": payload = b" " * ((32 << 20) + 2)
    response = CacheResponse(payload)
    monkeypatch.setattr(d, "urlopen", lambda *args, **kwargs: response)
    if failure == "oversize":
        monkeypatch.setattr(d, "json_object", lambda _: pytest.fail("oversized response parsed"))
    elif failure in {"read_error", "read_timeout"}:
        def fail_read(size):
            response.read_limits.append(size)
            raise (OSError("synthetic read failure") if failure == "read_error" else TimeoutError("synthetic timeout"))
        monkeypatch.setattr(response, "read", fail_read)
    with pytest.raises(d.DeployError, match="^api_health_failed$"):
        d.health_batches(0)
    assert response.read_limits == [(32 << 20) + 1] and response.closed


def test_health_batches_open_timeout_keeps_fixed_error(monkeypatch) -> None:
    d = deployer(); calls = []

    def fail_open(url, *, timeout):
        calls.append((url, timeout))
        raise TimeoutError("synthetic open timeout")

    monkeypatch.setattr(d, "urlopen", fail_open)
    with pytest.raises(d.DeployError, match="^api_health_failed$"):
        d.health_batches(0)
    assert calls == [("http://127.0.0.1:8771/api/v1/l2/batches?after_sequence=0&limit=1", 5)]


SYMBOLS = tuple(f"600{index:03d}" for index in range(100))


def proof(count: int = 20, *, attempted: int | None = None) -> dict:
    origin = datetime(2026, 10, 19, 1, 30, tzinfo=timezone.utc)
    attempted = count if attempted is None else attempted
    streams, issued = [], []
    last_seed = (3 * count - 1) * 1.02 + 0.02
    for slot, code in enumerate(SYMBOLS[:attempted]):
        for offset, (kind, side) in enumerate((("vendor4013", None), ("vendor4027", "buy"), ("vendor4027", "sell"))):
            index = len(issued)
            binding = {"code": code, "market": 17, "kind": kind, "side": side, "page_id": 10117 + 10 * slot + offset}
            issued.append({"binding": binding, "request_id": 3 + index,
                "sent_started_elapsed_seconds": index * 1.02, "sent_completed_elapsed_seconds": index * 1.02 + .01})
            if slot >= count: continue
            streams.append({"binding": binding, "epoch_id": "synthetic", "seed_ids": [100], "seed_sequence": index + 1,
                "seed_received_at": (origin + timedelta(seconds=index * 1.02 + .02)).isoformat(),
                "forward_batches": [{"sequence": 3 * attempted + 1 + step * 3 * count + index,
                    "received_at": (origin + timedelta(seconds=last_seed + second)).isoformat(), "ids": [101 + step]}
                    for step, second in enumerate((5, 35, 65, 85))]})
    batch_count = 3 * attempted + 12 * count
    value = {"schema": "rquant-l2-acceptance-v3", "source_commit": TARGET, "source_tree": TREE,
        "duration_seconds": 1200, "run_elapsed_seconds": 1200, "collector_started_at": origin.isoformat(), "connections": 1,
        "max_symbols": count, "highest_verified_tier": count, "verified_symbols": list(SYMBOLS[:count]),
        "attempted_symbols": list(SYMBOLS[:attempted]), "issued_requests": issued,
        "upstream_sends": 1 + len(issued), "send_attempts": 1 + len(issued), "reconnect_attempts": 0,
        "minimum_query_spacing_seconds": 1.01,
        "certification_window": {"started_at": (origin + timedelta(seconds=last_seed)).isoformat(),
            "ended_at": (origin + timedelta(seconds=last_seed + 90)).isoformat()},
        "termination": {"classification": "completed" if count == attempted else "insufficient_events",
                        "blocking_error": None, "partial_send": False},
        "cache": {"first_sequence": 1, "last_sequence": batch_count, "batch_count": batch_count, "seed_batches": 3 * attempted,
                  "export_sha256": "d" * 64, "integrity_proof_sha256": "e" * 64},
        "http": {"successful_requests": 9, "failed_requests": 0, "extra_upstream_sends": 0,
                 "windows": {"early": 3, "mid": 3, "late": 3}, "proof_sha256": "f" * 64,
                 "consumer_sequences": [{"first_sequence": 1, "last_sequence": batch_count,
                     "batch_count": batch_count, "sha256": "d" * 64} for _ in range(2)]},
        "websocket": {"connections": 2, "failed_connections": 0, "successful_messages": 6,
            "failed_messages": 0, "extra_upstream_sends": 0, "proof_sha256": "5" * 64,
            "max_consumer_lag_seconds": 1, "all_clients_closed": True, "consumer_workers_alive": [False, False],
            "consumer_sequences": [{"first_sequence": 1, "last_sequence": batch_count,
                "batch_count": batch_count, "sha256": "d" * 64,
                "windows": {"early": 1, "mid": 1, "late": 1}} for _ in range(2)]},
        "cleanup": {"worker_alive": False, "http_worker_alive": False,
                    "all_upstream_sockets_closed": True, "proof_sha256": "1" * 64}, "streams": streams,
        "resources": {"sample_interval_seconds": 1, "startup_settle_seconds": 10, "cpu_window_seconds": 30,
            "http_p95_seconds": .1, "http_p99_seconds": .2, "max_http_seconds": .5, "max_consumer_lag_seconds": 1,
            "max_combined_rss_bytes": 100_000_000, "min_host_available_fraction": .5, "max_cpu_core_fraction": .2,
            "max_cache_bytes": 100_000_000, "min_disk_free_bytes": 10 * 1024**3, "proof_sha256": "4" * 64}}
    return value


def artifact(tmp_path: Path, target: str = TARGET) -> tuple[Path, dict[str, bytes], list[dict]]:
    files = {"uv.lock": b"synthetic lock", "src/rquant/l2/api.py": b"# synthetic API\n",
             "src/rquant/l2/__main__.py": b"# synthetic collector\n",
             "src/rquant/l2/symbols.py": b"# synthetic symbol routing\n",
             "src/rquant/l2/subscriptions.py": b"# synthetic durable control\n"}
    for unit in ("api", "collector"):
        files[f"deploy/systemd/rquant-l2-{unit}.service"] = (ROOT / f"deploy/systemd/rquant-l2-{unit}.service").read_bytes()
    tree = [{"path": name, "type": "blob", "mode": "100644",
             "sha": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()}
            for name, data in files.items()]
    contents = {**files, "RELEASE.json": json.dumps({"commit": target, "short": target[:8]}).encode(),
                "web/dist/index.html": b"synthetic frontend"}
    path = tmp_path / "release.tar.gz"
    with tarfile.open(path, "w:gz") as archive:
        for name, data in contents.items():
            entry = tarfile.TarInfo(name); entry.size = len(data); entry.mode = 0o644
            archive.addfile(entry, io.BytesIO(data))
    return path, contents, tree


def github(archive: Path, tree: list[dict], target: str = TARGET):
    responses = {
        f"/commits/{TARGET}": {"sha": TARGET, "commit": {"tree": {"sha": TREE}}},
        "/git/ref/heads/main": {"object": {"sha": MAIN}},
        f"/compare/{TARGET}...{MAIN}": {"merge_base_commit": {"sha": TARGET}, "status": "ahead"},
        f"/actions/workflows/web-mvp.yml/runs?event=push&head_sha={TARGET}&per_page=100": {
            "workflow_runs": [{"id": 123, "event": "push", "head_sha": TARGET, "head_branch": "main",
                               "status": "completed", "conclusion": "success"}]},
        "/actions/runs/123/jobs?filter=latest&per_page=100": {"jobs": [
            {"name": name, "status": "completed", "conclusion": "success"} for name in sorted(JOBS)]},
        f"/releases/tags/web-{TARGET[:8]}": {"id": 456, "draft": False, "assets": [
            {"id": 789, "name": f"rquant-web-{TARGET[:8]}.tar.gz", "state": "uploaded",
             "digest": "sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest(), "size": archive.stat().st_size}]},
        f"/git/trees/{TREE}?recursive=1": {"sha": TREE, "truncated": False, "tree": tree},
    }
    if target != TARGET:
        responses = {key.replace(TARGET, target).replace(TARGET[:8], target[:8]): value for key, value in responses.items()}
        responses[f"/commits/{target}"]["sha"] = target
        responses[f"/compare/{target}...{MAIN}"]["merge_base_commit"]["sha"] = target
        responses[f"/actions/workflows/web-mvp.yml/runs?event=push&head_sha={target}&per_page=100"]["workflow_runs"][0]["head_sha"] = target
        responses[f"/releases/tags/web-{target[:8]}"]["assets"][0]["name"] = f"rquant-web-{target[:8]}.tar.gz"
        responses[f"/compare/{TARGET}...{target}"] = {"merge_base_commit": {"sha": TARGET}, "status": "ahead"}
    return responses


def test_acceptance_recomputes_six_streams_and_dynamic_late_window() -> None:
    d = deployer()
    result = d.validate_acceptance(proof(), TREE)
    assert result["streams"] == 60 and result["max_symbols"] == 20
    assert result["late_window_seconds"][1] - result["late_window_seconds"][0] == 60
    assert d.validate_acceptance(proof(20, attempted=40), TREE)["max_symbols"] == 20


@pytest.mark.parametrize("digests", [("0" * 64, "d" * 64), ("0" * 64, "0" * 64)])
def test_acceptance_requires_each_consumer_to_match_cache_chain(digests: tuple[str, str]) -> None:
    d = deployer(); value = proof()
    for consumer, digest in zip(value["http"]["consumer_sequences"], digests):
        consumer["sha256"] = digest
    with pytest.raises(d.DeployError, match="acceptance_consumers"):
        d.validate_acceptance(value, TREE)


@pytest.mark.parametrize('bad', ['missing', 'v2', 'connections', 'failed_connection', 'messages', 'failed_message',
    'extra', 'lag', 'consumer_count', 'head', 'sha', 'both_sha', 'early', 'missing_late', 'client_open', 'worker_alive'])
def test_v3_requires_actual_websocket_proof(bad: str) -> None:
    d = deployer(); value = proof(); ws = value['websocket']
    if bad == 'missing': value.pop('websocket')
    elif bad == 'v2': value['schema'] = 'rquant-l2-acceptance-v2'
    elif bad == 'connections': ws['connections'] = 1
    elif bad == 'failed_connection': ws['failed_connections'] = 1
    elif bad == 'messages': ws['successful_messages'] = 5
    elif bad == 'failed_message': ws['failed_messages'] = 1
    elif bad == 'extra': ws['extra_upstream_sends'] = 1
    elif bad == 'lag': ws['max_consumer_lag_seconds'] = 5.01
    elif bad == 'consumer_count': ws['consumer_sequences'].pop()
    elif bad == 'head': ws['consumer_sequences'][0]['last_sequence'] -= 1
    elif bad == 'sha': ws['consumer_sequences'][0]['sha256'] = '0'*64
    elif bad == 'both_sha':
        for consumer in ws['consumer_sequences']: consumer['sha256'] = '0'*64
    elif bad == 'early': ws['consumer_sequences'][0]['windows']['early'] = 0
    elif bad == 'missing_late': ws['consumer_sequences'][1]['windows'].pop('late')
    elif bad == 'client_open': ws['all_clients_closed'] = False
    else: ws['consumer_workers_alive'][1] = True
    with pytest.raises(d.DeployError):
        d.validate_acceptance(value, TREE)


@pytest.mark.parametrize("bad", ["legacy", "missing_stream", "capacity", "wrong_symbol", "missing_send", "short_window",
                                  "protocol", "partial_unknown", "resource", "consumer_gap"])
def test_batch_v2_capacity_and_ledger_gates(tmp_path: Path, bad: str) -> None:
    d = deployer(); value = proof()
    if bad == "legacy":
        value.update(schema="rquant-l2-acceptance-v1", upstream_sends=7)
        value["cache"].update(last_sequence=30, batch_count=30, seed_batches=6)
        value["streams"] = value["streams"][:6]
        for index, stream in enumerate(value["streams"]):
            slot, offset = divmod(index, 3)
            stream["binding"].update(code=("603258", "300006")[slot], market=(17, 33)[slot], page_id=10117 + 10*slot + offset)
            for step, batch in enumerate(stream["forward_batches"]):
                batch.update(sequence=7+step*6+index, received_at=(datetime(2026, 10, 19, 1, 30, tzinfo=timezone.utc)
                    + timedelta(seconds=(1010, 1080, 1150, 1190)[step])).isoformat())
    elif bad == "missing_stream": value["streams"].pop()
    elif bad == "capacity": value["max_symbols"] = 40
    elif bad == "wrong_symbol": value["streams"][0]["binding"] = {**value["streams"][0]["binding"], "code": "000001"}
    elif bad == "missing_send": value["issued_requests"].pop(1)
    elif bad == "short_window": value["certification_window"]["ended_at"] = value["certification_window"]["started_at"]
    elif bad == "protocol": value["termination"]["blocking_error"] = "protocol_error"
    elif bad == "partial_unknown": value["termination"].update(classification="unknown", partial_send=True)
    elif bad == "resource": value["resources"]["http_p99_seconds"] = 3
    else: value["http"]["consumer_sequences"][1]["last_sequence"] -= 1
    with pytest.raises(d.DeployError): d.validate_acceptance(value, TREE)


@pytest.mark.parametrize("bad", ["five", "duplicate", "seed_replay", "same_late_batch", "short_span", "two_windows", "old_late", "wrong_tree", "cache_hole", "http_extra"])
def test_acceptance_rejects_false_sustained_proof(bad: str) -> None:
    d = deployer(); value = proof()
    if bad == "five": value["streams"].pop()
    elif bad == "duplicate": value["streams"][1]["binding"] = value["streams"][0]["binding"]
    elif bad == "seed_replay": value["streams"][0]["forward_batches"][0]["ids"] = [100]
    elif bad == "same_late_batch": value["streams"][0]["forward_batches"][-1]["sequence"] = value["streams"][0]["forward_batches"][-2]["sequence"]
    elif bad == "short_span": value["streams"][0]["forward_batches"] = value["streams"][0]["forward_batches"][-2:]
    elif bad == "two_windows":
        for batch in value["streams"][0]["forward_batches"]:
            batch["received_at"] = "2026-10-19T01:34:10+00:00"
    elif bad == "old_late": value["certification_window"] = {"started_at": "2026-10-19T01:40:00+00:00", "ended_at": "2026-10-19T01:42:00+00:00"}
    elif bad == "wrong_tree": value["source_tree"] = "9" * 40
    elif bad == "cache_hole": value["cache"]["last_sequence"] += 1
    elif bad == "http_extra": value["http"]["extra_upstream_sends"] = 1
    with pytest.raises(d.DeployError): d.validate_acceptance(value, TREE)


@pytest.mark.parametrize("bad", ["off_main", "failed_ci", "pr_ci", "missing_job", "missing_digest", "bad_digest"])
def test_origin_requires_live_main_ci_and_asset_digest(tmp_path: Path, bad: str) -> None:
    d = deployer(); archive, _, tree = artifact(tmp_path); api = github(archive, tree)
    if bad == "off_main": api[f"/compare/{TARGET}...{MAIN}"]["merge_base_commit"]["sha"] = MAIN
    elif bad in {"failed_ci", "pr_ci"}:
        run = api[f"/actions/workflows/web-mvp.yml/runs?event=push&head_sha={TARGET}&per_page=100"]["workflow_runs"][0]
        run["conclusion" if bad == "failed_ci" else "event"] = "cancelled" if bad == "failed_ci" else "pull_request"
    elif bad == "missing_job": api["/actions/runs/123/jobs?filter=latest&per_page=100"]["jobs"].pop()
    else: api[f"/releases/tags/web-{TARGET[:8]}"]["assets"][0]["digest"] = None if bad == "missing_digest" else "sha256:" + "0" * 64
    with pytest.raises(d.DeployError): d.verify_origin(TARGET, archive, api.__getitem__)


def test_exact_archive_tree_and_unsafe_entry_rejection(tmp_path: Path) -> None:
    d = deployer(); archive, _, tree = artifact(tmp_path)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    assert d.inspect_archive(archive, TARGET, tree, digest)["unit_sha256"]
    for name, kind in (("../outside", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)):
        bad = tmp_path / "unsafe.tar.gz"
        with tarfile.open(bad, "w:gz") as tar:
            item = tarfile.TarInfo(name); item.type = kind; item.linkname = "outside"
            tar.addfile(item)
        with pytest.raises(d.DeployError):
            d.inspect_archive(bad, TARGET, tree, hashlib.sha256(bad.read_bytes()).hexdigest())


def test_entry_and_units_keep_component_boundary() -> None:
    entry = (ROOT / "scripts/deploy-production.sh").read_text()
    assert "deploy-l2-component.py" in entry and '"${1:-}" == "--component"' in entry
    subprocess.run(["/bin/bash", "-n", str(ROOT / "scripts/deploy-production.sh")], check=True)
    for kind in ("api", "collector"):
        text = (ROOT / f"deploy/systemd/rquant-l2-{kind}.service").read_text()
        assert "User=lighthouse" in text and "UMask=0077" in text and "NoNewPrivileges=yes" in text
        assert "ExecStart=/usr/bin/env -i " in text and "/bin/sh" not in text
        assert "RQUANT_DISABLE_DOTENV=1" in text
        if kind == "api":
            assert "rquant.l2.api --cache-root" in text and "rquant-l2-private" in text
            assert "bootstrap.bin" not in text and "Restart=on-failure" in text
        else:
            assert "--resident" in text and "--max-reconnects 0" in text and "Restart=no" in text
            assert "TimeoutStopSec=30" in text


class FakeHost:
    def __init__(self, module, root: Path) -> None:
        self.d = module
        for name in ("releases", "state", "inputs", "units"):
            (root / name).mkdir(mode=0o700)
        self.paths = module.Paths(root / "releases", root / "state", root / "inputs", root / "units", os.getuid())
        for name in ("bootstrap.bin", "order-template.bin", "trade-template.bin"):
            path = self.paths.inputs / name; path.write_bytes(b"SYNTHETIC_PRIVATE_DO_NOT_READ"); path.chmod(0o600)
        self.active = {name: False for name in module.UNITS}
        self.commands: list[list[str]] = []
        self.dropins = ""
        self.bad_fragment = False
        self.collector_error = False
        self.fail_uv = False
        self.at = datetime(2026, 10, 19, 8, tzinfo=timezone.utc)
        self.epoch = 0
        self.foreign_port = False
        self.foreign_upstream = False
        self.on_stop = None
        self.fail_install_after_copy = False
        self.api_never_active = False
        self.committed = []
        self.batch_cursors = []
        self.on_api_stop = None
        self.on_api_start = None

    def command(self, argv: list[str], cwd: Path | None = None) -> str:
        self.commands.append(list(argv))
        args = argv[2:] if argv[:2] == ["sudo", "-n"] else argv
        if args[0].endswith("/uv"):
            if self.fail_uv: raise self.d.DeployError("synthetic_uv_failure")
            assert cwd is not None
            path = cwd / ".venv/bin/python"; path.parent.mkdir(parents=True)
            path.write_bytes(b"synthetic executable"); path.chmod(0o700)
        elif args[0] == "/usr/bin/install":
            source, dest = Path(args[-2]), Path(args[-1]); dest.write_bytes(source.read_bytes()); dest.chmod(0o644)
            if self.fail_install_after_copy: raise self.d.DeployError("synthetic_partial_install")
        elif args[0] == "/usr/bin/rm":
            Path(args[-1]).unlink()
        elif args[0] == "/usr/sbin/ss":
            if "-ltnp" in args:
                if self.foreign_port: return 'LISTEN 0 128 127.0.0.1:8771 0.0.0.0:* users:(("other",pid=999,fd=5))\n'
                return 'LISTEN 0 128 127.0.0.1:8771 0.0.0.0:* users:(("python",pid=101,fd=5))\n' if self.active[self.d.UNITS[0]] else ""
            if self.foreign_upstream: return 'ESTAB 0 0 198.18.0.1:12345 122.9.43.156:9528 users:(("other",pid=999,fd=5))\n'
            return 'ESTAB 0 0 198.18.0.1:12345 122.9.43.156:9528 users:(("python",pid=102,fd=5))\n' if self.active[self.d.UNITS[1]] else ""
        elif args[0] == "/usr/bin/systemctl":
            action = args[1]
            if action == "show":
                unit = args[2]; exists = (self.paths.units / unit).exists()
                fragment = str(self.paths.units / unit) if exists else ""
                if exists and self.bad_fragment: fragment = "/usr/lib/systemd/system/foreign.service"
                return "\n".join([f"LoadState={'loaded' if exists else 'not-found'}", f"FragmentPath={fragment}",
                    f"DropInPaths={self.dropins if exists else ''}",
                    f"MainPID={101 if self.active[unit] and unit==self.d.UNITS[0] else 102 if self.active[unit] else 0}",
                    f"ActiveState={'active' if self.active[unit] else 'inactive'}"])
            if action in {"start", "restart", "enable"}:
                if self.api_never_active and args[-1] == self.d.UNITS[0]: return ""
                self.active[args[-1]] = True
                if args[-1] == self.d.UNITS[1]:
                    self.epoch += 1
                    desired = json.loads((self.paths.state / "control/desired.json").read_text())
                    for binding in self.d.symbol_bindings(desired["symbols"]):
                        self.committed.append({"local_batch_sequence": len(self.committed) + 1,
                            "epoch_id": f"synthetic-{self.epoch}", "delivery": "seed",
                            "binding": binding})
                elif self.on_api_start: self.on_api_start()
            elif action in {"stop", "disable"}:
                self.active[args[-1]] = False
                if action == "stop" and args[-1] == self.d.UNITS[1] and self.on_stop: self.on_stop()
                if action == "stop" and args[-1] == self.d.UNITS[0] and self.on_api_stop: self.on_api_stop()
        return ""

    def health(self) -> dict:
        running = self.active[self.d.UNITS[1]]
        return {"state": "blocked" if running and self.collector_error else "listening_candidate" if running else "unconfigured",
                "error_code": "protocol_error" if running and self.collector_error else None,
                "epoch_id": f"synthetic-{self.epoch}" if self.epoch else None,
                "seed_queries": sum(row["epoch_id"] == f"synthetic-{self.epoch}" for row in self.committed) if running else 0,
                "market_batch_count": len(self.committed),
                "last_sequence": len(self.committed)}

    def batches(self, after: int) -> dict:
        self.batch_cursors.append(after)
        rows = [row for row in self.committed if row["local_batch_sequence"] > after][:8]
        return {"batches": rows, "next_sequence": rows[-1]["local_batch_sequence"] if rows else after,
                "status": self.health()}

    def mutations(self) -> list[list[str]]:
        return [a for a in self.commands if a[:2] == ["sudo", "-n"]]


def deployment(tmp_path: Path):
    d = deployer(); host = FakeHost(d, tmp_path)
    archive, _, tree = artifact(tmp_path)
    receipt = tmp_path / "proof.json"; receipt.write_text(json.dumps(proof())); receipt.chmod(0o600);tmp_path.chmod(0o700)
    args = d.parse_cli(["--target", TARGET, "--artifact", str(archive), "--acceptance-proof", str(receipt),
                        "--acceptance-sha256", hashlib.sha256(receipt.read_bytes()).hexdigest()])
    api = github(archive, tree)
    return d, host, args, api


def run_deploy(d, host: FakeHost, args, api: dict):
    return d.deploy(args, paths=host.paths, api=api.__getitem__, command=host.command,
        health=host.health, batches=host.batches, now=lambda: host.at, sleep=lambda _: None)


def test_first_install_and_bad_receipt_before_mutation(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path)
    original = args.acceptance_sha256; args.acceptance_sha256 = "0" * 64
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert host.commands == []
    args.acceptance_sha256 = original
    assert run_deploy(d, host, args, api)["status"] == "deployed"
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert all(host.active.values())
    assert host.paths.cache.exists()
    assert len([c for c in host.commands if "enable" in c and c[-1] == d.UNITS[1]]) == 1


def test_first_failure_removes_only_new_units_link_and_preserves_data(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.collector_error = True
    inputs = {p.name: p.read_bytes() for p in host.paths.inputs.iterdir()}
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert not host.paths.current.exists() and not any(host.active.values())
    assert not any(host.paths.units.iterdir()) and host.paths.cache.is_dir()
    assert inputs == {p.name: p.read_bytes() for p in host.paths.inputs.iterdir()}
    assert len([c for c in host.commands if "enable" in c and c[-1] == d.UNITS[1]]) == 1


@pytest.mark.parametrize("foreign", ["unit", "dropin", "fragment", "writer", "deploy_lock", "port", "upstream"])
def test_foreign_authority_and_second_writer_rejected(tmp_path: Path, foreign: str) -> None:
    import fcntl
    d, host, args, api = deployment(tmp_path); held = None
    if foreign in {"dropin", "fragment"}:
        run_deploy(d, host, args, api); host.commands.clear()
        if foreign == "dropin": host.dropins = "/etc/systemd/system/rquant-l2-api.service.d/foreign.conf"
        else: host.bad_fragment = True
    elif foreign == "unit":
        (host.paths.units / d.UNITS[0]).write_text("foreign")
    elif foreign in {"port", "upstream"}:
        host.foreign_port = foreign == "port"; host.foreign_upstream = foreign == "upstream"
    else:
        path = host.paths.state / "deploy.lock" if foreign == "deploy_lock" else host.paths.cache / ".writer.lock"
        path.parent.mkdir(mode=0o700, exist_ok=True); held = path.open("w"); path.chmod(0o600)
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
        assert host.mutations() == []
    finally:
        if held: held.close()


def test_upgrade_requires_fast_forward_and_rollback_does_not_reauthenticate(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); run_deploy(d, host, args, api)
    host.commands.clear(); next_sha = "d" * 40
    sub = tmp_path / "next"; sub.mkdir()
    archive, _, tree = artifact(sub, next_sha); args.target = next_sha; args.artifact = archive
    api = github(archive, tree, next_sha)
    api[f"/compare/{TARGET}...{next_sha}"]["merge_base_commit"]["sha"] = next_sha
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert host.mutations() == []
    api[f"/compare/{TARGET}...{next_sha}"]["merge_base_commit"]["sha"] = TARGET
    host.at += timedelta(seconds=61)
    host.collector_error = True
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert host.active[d.UNITS[0]] and not host.active[d.UNITS[1]]
    assert len([c for c in host.commands if "start" in c and c[-1] == d.UNITS[1]]) == 1


def next_release(tmp_path: Path, d, host: FakeHost, args) -> dict:
    host.at += timedelta(seconds=61)
    sub = tmp_path / "next"; sub.mkdir()
    archive, _, tree = artifact(sub, "d" * 40); args.target = "d" * 40; args.artifact = archive
    return github(archive, tree, args.target)


def test_recorded_previous_rollback_restores_api_without_collector_start(tmp_path: Path) -> None:
    d, host, args, old_api = deployment(tmp_path); run_deploy(d, host, args, old_api)
    api = next_release(tmp_path, d, host, args); run_deploy(d, host, args, api)
    assert host.batch_cursors[0] == 0 and 60 in host.batch_cursors
    args = d.parse_cli(["--rollback", "--target", TARGET]); host.commands.clear()
    assert run_deploy(d, host, args, old_api)["status"] == "rolled_back"
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert host.active[d.UNITS[0]] and not host.active[d.UNITS[1]]
    assert not [c for c in host.commands if any(a in c for a in ("enable", "start", "restart")) and c[-1] == d.UNITS[1]]
    args.target = "e" * 40; host.commands.clear()
    with pytest.raises(d.DeployError, match="rollback_not_recorded_previous"):
        run_deploy(d, host, args, old_api)
    assert host.mutations() == []


@pytest.mark.parametrize("reason", ["window", "cooldown", "incomplete", "uv_failure"])
def test_upgrade_refusal_leaves_running_current_untouched(tmp_path: Path, reason: str) -> None:
    d, host, args, api = deployment(tmp_path); run_deploy(d, host, args, api)
    api = next_release(tmp_path, d, host, args); host.commands.clear()
    if reason == "window": host.at = datetime(2026, 10, 20, 1, 30, tzinfo=timezone.utc)
    elif reason == "cooldown": host.at -= timedelta(seconds=61)
    elif reason == "incomplete":
        path = host.paths.state / "deploy-intent.json"
        data = json.loads(path.read_text()); data["phase"] = "activating"; path.write_text(json.dumps(data))
    else: host.fail_uv = True
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert host.paths.current.resolve() == host.paths.releases / TARGET
    assert all(host.active.values()) and host.mutations() == []


def test_second_preflight_refuses_new_writer_and_does_not_kill_it(tmp_path: Path) -> None:
    import fcntl
    d, host, args, api = deployment(tmp_path); run_deploy(d, host, args, api)
    api = next_release(tmp_path, d, host, args); host.commands.clear(); held = []
    def lock_after_stop() -> None:
        if held: return
        file = (host.paths.cache / ".writer.lock").open("r+")
        fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB); held.append(file)
    host.on_stop = lock_after_stop
    try:
        with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
        assert host.paths.current.resolve() == host.paths.releases / TARGET
        assert not host.active[d.UNITS[1]] and host.active[d.UNITS[0]]
        assert [(c[3], c[-1]) for c in host.mutations()] == [
            ("stop", d.UNITS[0]), ("stop", d.UNITS[1]), ("start", d.UNITS[0])]
        assert held and not held[0].closed
    finally:
        for file in held: file.close()


def test_initial_second_preflight_refuses_writer_started_during_build(tmp_path: Path) -> None:
    import fcntl
    d, host, args, api = deployment(tmp_path); original = host.command; held = []
    def command(argv: list[str], cwd: Path | None = None) -> str:
        result = original(argv, cwd)
        if argv[0].endswith("/uv"):
            file = (host.paths.cache / ".writer.lock").open("r+")
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB); held.append(file)
        return result
    try:
        with pytest.raises(d.DeployError):
            d.deploy(args, paths=host.paths, api=api.__getitem__, command=command, health=host.health, batches=host.batches,
                     now=lambda: host.at, sleep=lambda _: None)
        assert host.mutations() == [] and not host.paths.current.exists()
    finally:
        for file in held: file.close()


def test_input_contents_are_not_read_by_installer(tmp_path: Path, monkeypatch) -> None:
    d, host, args, api = deployment(tmp_path); original = Path.open
    def deny_input(self: Path, *argv, **kwargs):
        if self.parent == host.paths.inputs: pytest.fail("Installer read a credential input")
        return original(self, *argv, **kwargs)
    monkeypatch.setattr(Path, "open", deny_input)
    assert run_deploy(d, host, args, api)["status"] == "deployed"


def test_partial_first_unit_install_failure_is_cleaned(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.fail_install_after_copy = True
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert not any(host.paths.units.iterdir()) and not host.paths.current.exists()
    assert not any(host.active.values())


def test_api_health_cannot_substitute_for_active_managed_service(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); host.api_never_active = True
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert not host.paths.current.exists() and not any(host.active.values())


@pytest.mark.parametrize("seed_case", ["missing_with_push", "last_pending", "four_orders", "old_epoch", "complete", "paginated"])
def test_persist_f1_requires_six_committed_seed_bindings(monkeypatch, seed_case: str) -> None:
    d = deployer(); bindings = d.symbol_bindings(["600000", "000001"])
    expected = {d.identity(binding) for binding in bindings}
    rows = [{"local_batch_sequence": i + 1, "epoch_id": "new", "delivery": "seed", "binding": binding}
            for i, binding in enumerate(bindings)]
    if seed_case == "missing_with_push": rows[-1]["delivery"] = "candidate_push"
    elif seed_case == "last_pending": rows[-1]["binding"] = rows[0]["binding"]; rows[-1]["delivery"] = "candidate_push"
    elif seed_case == "four_orders":
        for row in rows:
            if row["binding"]["kind"] == "vendor4013": row["delivery"] = "candidate_push"
    elif seed_case == "old_epoch":
        for row in rows: row["epoch_id"] = "old"
    elif seed_case == "paginated":
        rows = [{**row, "epoch_id": "old"} for row in rows] + [
            {**row, "local_batch_sequence": row["local_batch_sequence"] + 6} for row in rows]
    status = {"state": "listening_candidate", "epoch_id": "new", "error_code": None,
              "seed_queries": 6, "market_batch_count": len(rows), "last_sequence": len(rows)}
    cursors = []
    def batches(after: int) -> dict:
        cursors.append(after); selected = [row for row in rows if row["local_batch_sequence"] > after][:8]
        return {"batches": selected, "next_sequence": selected[-1]["local_batch_sequence"] if selected else after,
                "status": status}
    monkeypatch.setattr(d, "health_batches", batches, raising=False)
    if seed_case in {"complete", "paginated"}:
        d.wait_health(lambda: status, lambda _: None, collector=True, old_epoch="old", expected=expected)
        assert cursors == ([0] if seed_case == "complete" else [0, 8])
    else:
        with pytest.raises(d.DeployError, match="collector_health_failed"):
            d.wait_health(lambda: status, lambda _: None, collector=True, old_epoch="old", expected=expected)


def test_health_limit1_walks_every_batch_to_all_sixty_current_seed_bindings(monkeypatch) -> None:
    d = deployer(); bindings = d.symbol_bindings(list(SYMBOLS[:20]))
    expected = {d.identity(binding) for binding in bindings}
    rows = [{"local_batch_sequence": index + 1, "epoch_id": epoch, "delivery": "seed", "binding": binding}
            for index, (epoch, binding) in enumerate(
                (epoch, binding) for epoch in ("old", "new") for binding in bindings)]
    status = {"state": "listening_candidate", "epoch_id": "new", "error_code": None,
              "seed_queries": 60, "last_sequence": len(rows)}
    calls, responses = [], []

    def open_response(url, *, timeout):
        query = parse_qs(urlsplit(url).query)
        after, limit = int(query["after_sequence"][0]), int(query["limit"][0])
        selected = rows[after:after + limit]
        calls.append((after, limit, timeout))
        response = CacheResponse(json.dumps({"batches": selected,
            "next_sequence": selected[-1]["local_batch_sequence"] if selected else after,
            "status": status}).encode())
        responses.append(response)
        return response

    monkeypatch.setattr(d, "urlopen", open_response)
    d.wait_health(lambda: status, lambda _: pytest.fail("complete seeds should not retry"),
        collector=True, old_epoch="old", expected=expected)
    assert calls == [(cursor, 1, 5) for cursor in range(120)]
    assert all(response.closed and response.read_limits == [(32 << 20) + 1] for response in responses)


@pytest.mark.parametrize("peer,pid,rejected", [
    ("[::ffff:122.9.43.156]:9528", 999, True),
    ("122.9.43.156:9528", 102, False),
    ("[::ffff:122.9.43.156]:9528", 102, False),
    ("[::ffff:198.18.0.2]:9528", 999, False),
])
def test_persist_f2_normalizes_upstream_peer(tmp_path: Path, peer: str, pid: int, rejected: bool) -> None:
    d, host, _, _ = deployment(tmp_path); host.paths.cache.mkdir(mode=0o700)
    views = {unit: {"MainPID": str(102 if unit == d.UNITS[1] else 0), "ActiveState": "active"} for unit in d.UNITS}
    def command(argv: list[str], cwd: Path | None = None) -> str:
        if "-ltnp" in argv: return ""
        return f'ESTAB 0 0 [::ffff:198.18.0.1]:12345 {peer} users:(("python",pid={pid},fd=5))\n'
    if rejected:
        with pytest.raises(d.DeployError, match="foreign_socket"):
            d.socket_writer_preflight(host.paths, views, command)
    else:
        d.socket_writer_preflight(host.paths, views, command)


def use_proof(args, value: dict) -> None:
    args.acceptance_proof.write_text(json.dumps(value))
    args.acceptance_sha256 = hashlib.sha256(args.acceptance_proof.read_bytes()).hexdigest()


def test_production_policy_initializes_twenty_verified_targets(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); use_proof(args, proof(40))
    assert run_deploy(d, host, args, api)["status"] == "deployed"
    assert json.loads((host.paths.state / "capacity.json").read_text()) == {
        "max_symbols": 40, "source_tree": TREE, "acceptance_sha256": args.acceptance_sha256}
    desired = json.loads((host.paths.state / "control/desired.json").read_text())
    assert desired == {"revision": 1, "symbols": list(SYMBOLS[:20])}
    assert host.health()["seed_queries"] == 60


def test_upgrade_capacity_rechecks_last_accepted_union_after_api_stop(tmp_path: Path) -> None:
    from rquant.l2.subscriptions import SubscriptionStore
    d, host, args, api = deployment(tmp_path); use_proof(args, proof(40)); run_deploy(d, host, args, api)
    api = next_release(tmp_path, d, host, args); use_proof(args, proof(20)); host.commands.clear()
    host.on_api_stop = lambda: SubscriptionStore(host.paths.state / "control", max_symbols=40).add([SYMBOLS[20]])
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert len(json.loads((host.paths.state / "control/desired.json").read_text())["symbols"]) == 21
    assert json.loads((host.paths.state / "capacity.json").read_text())["max_symbols"] == 40
    assert all(host.active.values())
    assert not [c for c in host.commands if "stop" in c and c[-1] == d.UNITS[1]]


@pytest.mark.parametrize("old_capacity,new_capacity,blocked", [(20, 40, True), (40, 80, False)])
def test_failed_upgrade_never_loses_accepted_union_or_expands_old_capacity(tmp_path: Path,
        old_capacity: int, new_capacity: int, blocked: bool) -> None:
    from rquant.l2.subscriptions import SubscriptionStore
    d, host, args, api = deployment(tmp_path); use_proof(args, proof(old_capacity)); run_deploy(d, host, args, api)
    api = next_release(tmp_path, d, host, args); use_proof(args, proof(new_capacity)); host.commands.clear()
    def accepted() -> None:
        SubscriptionStore(host.paths.state / "control", max_symbols=new_capacity).add([SYMBOLS[20]])
    host.on_api_start = accepted; host.collector_error = True
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    desired = json.loads((host.paths.state / "control/desired.json").read_text())
    assert desired == {"revision": 2, "symbols": list(SYMBOLS[:21])}
    assert not host.active[d.UNITS[1]]
    assert host.active[d.UNITS[0]] is (not blocked)
    policy = json.loads((host.paths.state / "capacity.json").read_text())
    assert policy["max_symbols"] == (new_capacity if blocked else old_capacity)
    if blocked:
        assert json.loads((host.paths.state / "deploy-intent.json").read_text())["phase"] == "blocked"


@pytest.mark.parametrize("ready_at,accepted", [(310, True), (700, False)])
def test_dynamic_health_budget_grows_with_union_without_resetting_origin(monkeypatch, ready_at: int, accepted: bool) -> None:
    d = deployer(); ticks = [0.0]; rows = []
    for index, binding in enumerate(d.symbol_bindings(list(SYMBOLS))):
        rows.append({"local_batch_sequence": index + 1, "epoch_id": "new", "delivery": "seed", "binding": binding})
    monkeypatch.setattr(d.time, "monotonic", lambda: ticks[0])
    def status() -> dict:
        return {"state": "listening_candidate", "epoch_id": "new", "error_code": None,
                "seed_queries": 300 if ticks[0] >= ready_at else 0, "last_sequence": 300 if ticks[0] >= ready_at else 0}
    def batches(after: int) -> dict:
        selected = rows[after:after+128]
        return {"batches": selected, "next_sequence": selected[-1]["local_batch_sequence"]}
    def run() -> None:
        d.wait_health(status, lambda second: ticks.__setitem__(0, ticks[0] + second), collector=True,
            old_epoch="old", batches=batches, symbols=lambda: list(SYMBOLS[:20 if ticks[0] < 100 else 100]))
    if accepted: run()
    else:
        with pytest.raises(d.DeployError): run()
        assert ticks[0] <= 630


@pytest.mark.parametrize("case", ["old_blocked_then_new", "old_blocked_only", "new_blocked", "new_none_blocked",
    "new_then_old", "new_then_none", "different_blocked_epoch", "empty_old_blocked", "none_old_blocked", "old_then_new_blocked"])
def test_start_health_rejects_only_current_epoch_failure(monkeypatch: pytest.MonkeyPatch, case: str) -> None:
    d = deployer()
    bindings = d.symbol_bindings(["600000"])
    expected = {d.identity(binding) for binding in bindings}
    baseline = 45130
    old = {"state": "blocked", "epoch_id": "old", "error_code": "decode_error",
        "seed_queries": 3, "last_sequence": baseline}
    starting = {**old, "state": "starting", "epoch_id": None, "error_code": None, "seed_queries": 0}
    fresh_start = {**starting, "epoch_id": "new"}
    ready = {**old, "state": "listening_candidate", "epoch_id": "new", "error_code": None, "last_sequence": baseline + 3}
    new_blocked = {**ready, "state": "blocked", "error_code": "decode_error"}
    sequence = {
        "old_blocked_then_new": [old, starting, ready],
        "old_blocked_only": [old],
        "new_blocked": [new_blocked],
        "new_none_blocked": [{**old, "epoch_id": None, "error_code": "input_error"}],
        "new_then_old": [fresh_start, {**ready, "epoch_id": "old"}, ready],
        "new_then_none": [fresh_start, starting, ready],
        "different_blocked_epoch": [{**old, "epoch_id": "other"}],
        "empty_old_blocked": [{**old, "epoch_id": ""}],
        "none_old_blocked": [{**old, "epoch_id": None, "error_code": "input_error"}],
        "old_then_new_blocked": [old, new_blocked],
    }[case]
    old_epoch = "" if case == "empty_old_blocked" else None if case == "none_old_blocked" else "old"
    ticks, polls, cursors = [0.0], [], []
    rows = [{"local_batch_sequence": baseline + index + 1, "epoch_id": "new", "delivery": "seed", "binding": binding}
            for index, binding in enumerate(bindings)]
    monkeypatch.setattr(d.time, "monotonic", lambda: ticks[0])

    def health() -> dict:
        value = sequence[min(len(polls), len(sequence) - 1)]
        polls.append(value)
        return value

    def batches(after: int) -> dict:
        cursors.append(after)
        selected = [row for row in rows if row["local_batch_sequence"] > after]
        return {"batches": selected, "next_sequence": selected[-1]["local_batch_sequence"] if selected else after}

    def wait() -> None:
        d.wait_health(health, lambda second: ticks.__setitem__(0, ticks[0] + second), collector=True,
            old_epoch=old_epoch, after_sequence=baseline, expected=expected, batches=batches)

    if case == "old_blocked_then_new":
        wait()
        assert len(polls) == 3 and ticks[0] == 2 and cursors == [baseline]
    else:
        with pytest.raises(d.DeployError, match="^collector_health_failed$"):
            wait()
        if case == "old_blocked_only":
            assert ticks[0] == 2 * len(expected) + 30 and len(polls) == ticks[0]
        else:
            assert len(polls) == (2 if case in {"new_then_old", "new_then_none", "old_then_new_blocked"} else 1)
        assert cursors == []


def test_previous_without_dynamic_control_contract_is_rejected_before_stop(tmp_path: Path) -> None:
    d, host, args, api = deployment(tmp_path); run_deploy(d, host, args, api)
    api = next_release(tmp_path, d, host, args); run_deploy(d, host, args, api)
    installed_file = host.paths.state / "installation.json"; installed = json.loads(installed_file.read_text())
    manifest = host.paths.releases / TARGET / ".l2-deployment.json"
    value = json.loads(manifest.read_text()); value.pop("control_schema"); manifest.write_text(json.dumps(value))
    installed["previous"]["manifest_sha256"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
    installed_file.write_text(json.dumps(installed)); host.commands.clear()
    args = d.parse_cli(["--rollback", "--target", TARGET])
    with pytest.raises(d.DeployError): run_deploy(d, host, args, api)
    assert host.mutations() == []
