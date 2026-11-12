"""Daily post-close archive of L1 snapshot day dirs to Cloudflare R2 (S3 API, stdlib only).

For each ``$RQUANT_RESEARCH_ROOT/snapshots/YYYY-MM-DD`` that is finished (compacted, not today
while the collector is still running) and not yet archived:
  tar.gz -> PUT ``s3://$R2_BUCKET/snapshots/YYYY-MM-DD/YYYY-MM-DD.tar.gz`` (signed payload
  sha256, so R2 rejects corrupt bodies) -> HEAD to verify size + sha256 metadata ->
  write ``.r2_archived.json`` locally.
Then local day dirs older than ``--keep-days`` (default 14) are deleted ONLY if a fresh HEAD
confirms the archived object matches the local marker.

Credentials: R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT / R2_BUCKET
(``/etc/rquant-r2.env`` on the server, never in the repo).

Fetch a day back:  python -m rquant.research.snapshot_archive fetch 2026-10-12 --dest /tmp/x
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import http.client
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

log = logging.getLogger("rquant.snapshot_archive")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MARKER = ".r2_archived.json"
PREFIX = "snapshots"
#: how long a run waits for today's collector to finish compacting (unit timeout is 2h)
WAIT_S = float(os.environ.get("SNAPSHOT_ARCHIVE_WAIT_S", 3600))
COLLECTOR_UNIT = "rquant-snapshot-collector.service"
#: one R2 request timeout (seconds); override with R2_TIMEOUT_S for slow links
DEFAULT_TIMEOUT_S = 600.0
#: total attempts per operation (first try + retries) and the waits between them
DEFAULT_ATTEMPTS = 5
DEFAULT_BACKOFF_S: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0)
T = TypeVar("T")


def _retryable(error: BaseException) -> bool:
    """Transport failures and 5xx are transient; a 4xx is a definite answer."""
    if isinstance(error, urllib.error.HTTPError):
        return 500 <= error.code < 600
    return isinstance(
        error,
        (TimeoutError, ConnectionError, urllib.error.URLError, http.client.HTTPException),
    )


def _uncertain(error: BaseException) -> bool:
    """A transport failure leaves a write's outcome unknown; the server may have committed."""
    return _retryable(error) and not isinstance(error, urllib.error.HTTPError)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"invalid {name}") from None


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class R2:
    """Minimal SigV4 S3 client (PUT/HEAD/GET/DELETE single objects).

    Operations retry transient failures (timeouts, connection errors, 5xx) with
    exponential backoff: 5 attempts and 1/2/4/8s waits by default.  A 4xx is a
    definite answer and is never retried.  Each attempt includes a fresh
    signature and, for uploads, rewinds the body, so retries stay byte-identical.
    """

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        key_id: str,
        secret: str,
        *,
        timeout_s: float | None = None,
        attempts: int = DEFAULT_ATTEMPTS,
        backoff_s: tuple[float, ...] = DEFAULT_BACKOFF_S,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.host = urllib.parse.urlparse(self.endpoint).netloc
        self.bucket, self.key_id, self.secret = bucket, key_id, secret
        if timeout_s is None:
            timeout_s = _env_float("R2_TIMEOUT_S", DEFAULT_TIMEOUT_S)
        if not timeout_s > 0 or attempts < 1 or any(delay < 0 for delay in backoff_s):
            raise ValueError("invalid R2 retry configuration")
        self.timeout_s = float(timeout_s)
        self.attempts = attempts
        self.backoff_s = tuple(float(delay) for delay in backoff_s)

    @classmethod
    def from_env(cls) -> R2:
        e = os.environ
        missing = [k for k in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")
                   if not e.get(k)]
        if missing:
            raise SystemExit(f"missing env: {missing} (see /etc/rquant-r2.env)")
        return cls(e["R2_ENDPOINT"], e.get("R2_BUCKET", "rquant-snapshots"),
                   e["R2_ACCESS_KEY_ID"], e["R2_SECRET_ACCESS_KEY"])

    def _delay(self, attempt: int) -> float:
        if not self.backoff_s:
            return 0.0
        return self.backoff_s[min(attempt - 1, len(self.backoff_s) - 1)]

    def _retry(self, operation: Callable[[], T], key: str, *, conditional: bool = False) -> T:
        """Run one storage operation, retrying only transient failures."""
        uncertain: BaseException | None = None
        for attempt in range(1, self.attempts + 1):
            try:
                return operation()
            except Exception as error:  # noqa: BLE001 - re-raised below unless retryable
                if isinstance(error, urllib.error.HTTPError):
                    error.close()
                    if conditional and uncertain is not None and error.code in (409, 412):
                        # An earlier transport failure may have committed this
                        # conditional write, so the precondition failure is
                        # ambiguous. Surface the original error and let the
                        # caller verify the exact remote bytes.
                        assert uncertain is not None
                        raise uncertain
                elif _uncertain(error):
                    uncertain = error
                if attempt == self.attempts or not _retryable(error):
                    raise
                delay = self._delay(attempt)
                log.warning(
                    "R2 %s attempt %d/%d failed (%s: %s); retry in %.1fs",
                    key, attempt, self.attempts, type(error).__name__, error, delay,
                )
                if delay > 0:
                    time.sleep(delay)
        raise AssertionError("unreachable")

    def _request(self, method: str, key: str, *, body: Any = None, payload_sha: str | None = None,
                 headers: dict[str, str] | None = None, now: dt.datetime | None = None):
        now = now or dt.datetime.now(dt.UTC)
        amz = now.strftime("%Y%m%dT%H%M%SZ")
        day = amz[:8]
        path = "/" + self.bucket + "/" + urllib.parse.quote(key, safe="/-_.~")
        h = {k.lower(): v for k, v in (headers or {}).items()}
        h.update({"host": self.host, "x-amz-date": amz,
                  "x-amz-content-sha256": payload_sha or hashlib.sha256(b"").hexdigest()})
        names = sorted(h)
        canon = "\n".join([method, path, "", "".join(f"{k}:{h[k].strip()}\n" for k in names),
                           ";".join(names), h["x-amz-content-sha256"]])
        scope = f"{day}/auto/s3/aws4_request"
        creq = hashlib.sha256(canon.encode()).hexdigest()
        sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, creq])
        k = ("AWS4" + self.secret).encode()
        for part in (day, "auto", "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        h["authorization"] = (f"AWS4-HMAC-SHA256 Credential={self.key_id}/{scope}, "
                              f"SignedHeaders={';'.join(names)}, Signature={sig}")
        del h["host"]
        req = urllib.request.Request(self.endpoint + path, data=body, method=method, headers=h)
        return urllib.request.urlopen(req, timeout=self.timeout_s)

    def put_file(self, key: str, src: Path, sha: str, *, if_match: str | None = None,
                 if_none_match: str | None = None) -> None:
        if (if_match is not None and if_none_match is not None
                or if_none_match is not None and if_none_match != "*"
                or if_match is not None and (not isinstance(if_match, str) or not if_match
                                             or any(ord(c) < 33 or ord(c) > 126
                                                    for c in if_match))):
            raise ValueError("invalid conditional write")
        size = src.stat().st_size
        headers = {"content-length": str(size), "x-amz-meta-sha256": sha,
                   "content-type": "application/octet-stream"}
        if if_match is not None:
            headers["if-match"] = if_match
        if if_none_match is not None:
            headers["if-none-match"] = if_none_match
        with src.open("rb") as stream:
            def send() -> None:
                stream.seek(0)
                self._request("PUT", key, body=stream, payload_sha=sha, headers=headers).close()

            self._retry(
                send, key, conditional=if_match is not None or if_none_match is not None
            )

    def head(self, key: str, *, include_etag: bool = False) -> dict[str, Any] | None:
        try:
            with self._retry(lambda: self._request("HEAD", key), key) as r:
                result = {"size": int(r.headers["Content-Length"]),
                          "sha256": r.headers.get("x-amz-meta-sha256")}
                if include_etag:
                    result["etag"] = r.headers.get("ETag")
                return result
        except urllib.error.HTTPError as e:
            e.close()
            if e.code == 404:
                return None
            raise

    def get_file(self, key: str, dest: Path, *, maximum_bytes: int | None = None) -> None:
        if maximum_bytes is not None and (type(maximum_bytes) is not int or maximum_bytes < 0):
            raise ValueError("invalid download bound")

        def download() -> None:
            try:
                with self._request("GET", key) as r, dest.open("wb") as f:
                    if maximum_bytes is None:
                        shutil.copyfileobj(r, f, 1 << 20)
                        return
                    remaining = maximum_bytes
                    while chunk := r.read(min(1 << 20, remaining + 1)):
                        if len(chunk) > remaining:
                            raise ValueError("download exceeds bound")
                        f.write(chunk)
                        remaining -= len(chunk)
            except BaseException:
                dest.unlink(missing_ok=True)
                raise

        self._retry(download, key)

    def delete(self, key: str) -> None:
        self._retry(lambda: self._request("DELETE", key).close(), key)


def object_key(day: str) -> str:
    return f"{PREFIX}/{day}/{day}.tar.gz"


def collector_active() -> bool:
    try:
        return subprocess.run(["systemctl", "is-active", "--quiet", COLLECTOR_UNIT],
                              timeout=10).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _busy(d: Path) -> bool:
    """Collector still running or mid-compaction (quotes.tmp present)."""
    return (d / "quotes.tmp").exists() or collector_active()


def wait_for_collector(d: Path, wait_s: float, poll_s: float = 60.0) -> bool:
    """Block until today's dir is no longer being written (True) or wait_s elapses."""
    deadline = time.monotonic() + wait_s
    while _busy(d):
        if time.monotonic() >= deadline:
            return False
        log.info("collector still busy on %s; waiting", d.name)
        time.sleep(poll_s)
    return True


def _ready(d: Path, today: str) -> bool:
    if (d / MARKER).exists() or (d / "quotes.tmp").exists():
        return False
    if any((d / "quotes").glob("part-*.parquet")) if (d / "quotes").is_dir() else False:
        return d.name != today  # uncompacted leftovers from a past day: archive as-is
    if d.name == today and collector_active():
        return False
    return any(p.is_file() for p in d.rglob("*"))


def archive_day(client: R2, d: Path) -> dict[str, Any]:
    key = object_key(d.name)
    with tempfile.TemporaryDirectory(prefix="r2arc-") as td:
        tgz = Path(td) / f"{d.name}.tar.gz"
        with tarfile.open(tgz, "w:gz", compresslevel=6) as tar:
            for p in sorted(d.rglob("*")):
                # stream/ = transient 3s delta rounds (already inside the 30s shards)
                if p.name != MARKER and p.relative_to(d).parts[0] != "stream":
                    tar.add(p, arcname=str(Path(d.name) / p.relative_to(d)), recursive=False)
        sha, size = _sha256_file(tgz), tgz.stat().st_size
        client.put_file(key, tgz, sha)
    got = client.head(key)
    if not got or got["size"] != size or got["sha256"] != sha:
        raise RuntimeError(f"verify failed for {key}: local size={size} remote={got}")
    info = {"key": key, "bucket": client.bucket, "size": size, "sha256": sha,
            "archived_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds")}
    (d / MARKER).write_text(json.dumps(info))
    log.info("archived %s -> %s (%d bytes)", d, key, size)
    return info


def prune(client: R2, root: Path, keep_days: int, today: dt.date) -> list[str]:
    cutoff = today - dt.timedelta(days=keep_days)
    removed = []
    for d in sorted(root.iterdir()):
        if not (d.is_dir() and DAY_RE.match(d.name)):
            continue
        if dt.date.fromisoformat(d.name) >= cutoff:
            continue
        m = d / MARKER
        if not m.exists():
            log.warning("keep %s: not archived", d)
            continue
        info = json.loads(m.read_text())
        got = client.head(info["key"])
        if not got or got["size"] != info["size"] or got["sha256"] != info["sha256"]:
            log.error("keep %s: R2 object missing/mismatch (%s)", d, got)
            continue
        shutil.rmtree(d)
        removed.append(d.name)
        log.info("pruned %s (verified in R2)", d)
    return removed


def run(root: Path, keep_days: int = 14, today: dt.date | None = None,
        client: R2 | None = None, wait_s: float | None = None) -> int:
    today = today or dt.date.today()
    wait_s = WAIT_S if wait_s is None else wait_s
    if not root.is_dir():
        log.info("no snapshot root %s", root)
        return 0
    client = client or R2.from_env()
    rc = 0
    td = root / today.isoformat()
    if td.is_dir() and not (td / MARKER).exists() and not wait_for_collector(td, wait_s):
        log.warning("collector still busy after %ss; today left for the next run", wait_s)
    for d in sorted(root.iterdir()):
        if d.is_dir() and DAY_RE.match(d.name) and _ready(d, today.isoformat()):
            try:
                archive_day(client, d)
            except Exception:  # noqa: BLE001 - keep going, never prune unverified
                log.exception("archive failed for %s", d)
                rc = 1
    prune(client, root, keep_days, today)
    return rc


def fetch(day: str, dest: Path, client: R2 | None = None) -> Path:
    client = client or R2.from_env()
    dest.mkdir(parents=True, exist_ok=True)
    tgz = dest / f"{day}.tar.gz"
    client.get_file(object_key(day), tgz)
    head = client.head(object_key(day))
    if head and head["sha256"] and _sha256_file(tgz) != head["sha256"]:
        raise RuntimeError("checksum mismatch on download")
    with tarfile.open(tgz) as tar:
        tar.extractall(dest, filter="data")
    tgz.unlink()
    return dest / day


def selftest(client: R2 | None = None) -> int:
    client = client or R2.from_env()
    key = f"{PREFIX}/_selftest/{os.getpid()}.bin"
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.bin"
        p.write_bytes(os.urandom(4096))
        sha = _sha256_file(p)
        client.put_file(key, p, sha)
        ok = client.head(key) == {"size": 4096, "sha256": sha}
        back = Path(td) / "y.bin"
        client.get_file(key, back)
        ok = ok and _sha256_file(back) == sha
    client.delete(key)
    ok = ok and client.head(key) is None
    print(json.dumps({"selftest": "ok" if ok else "FAILED", "key": key}))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="rquant.research.snapshot_archive")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--root", type=Path, default=None)
    r.add_argument("--keep-days", type=int, default=14)
    f = sub.add_parser("fetch")
    f.add_argument("day")
    f.add_argument("--dest", type=Path, default=Path("."))
    sub.add_parser("selftest")
    a = p.parse_args(argv)
    if a.cmd == "selftest":
        return selftest()
    if a.cmd == "fetch":
        print(fetch(a.day, a.dest))
        return 0
    root = a.root or Path(os.environ.get("RQUANT_RESEARCH_ROOT") or "data/research") / "snapshots"
    return run(root, a.keep_days)


if __name__ == "__main__":
    raise SystemExit(main())
