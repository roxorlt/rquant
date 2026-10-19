"""Offline archive integrity tests. No remote or member-session access."""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import importlib
import json
import re
import shutil
import socket
import urllib.error
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from rquant.l2.cache import CacheError, RawL2Cache
from rquant.l2.models import FeedStatus
from tests.unit.test_l2_cache import observation

PREFIX = "l2/v1/_acceptance/" + "a" * 32
NOW = dt.datetime(2026, 10, 16, 9, 20, tzinfo=dt.UTC)


def archive() -> ModuleType:
    try:
        return importlib.import_module("rquant.l2.archive")
    except ModuleNotFoundError:
        pytest.fail("archive implementation is missing")


class FakeR2:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[tuple[str, str | None, str | None]] = []
        self.generation: dict[str, int] = {}
        self.on_put = None
        self.after_get = None

    def head(self, key: str, *, include_etag: bool = False) -> dict | None:
        if key not in self.objects:
            return None
        result = {
            "size": len(self.objects[key]),
            "sha256": hashlib.sha256(self.objects[key]).hexdigest(),
        }
        if include_etag:
            result["etag"] = f'"generation-{self.generation[key]}"'
        return result

    def put_file(
        self,
        key: str,
        src: Path,
        sha: str,
        *,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> None:
        assert if_match is None or re.fullmatch(r'"[^"\r\n]+"', if_match), "invalid HTTP entity-tag"
        self.puts.append((key, if_match, if_none_match))
        if self.on_put:
            self.on_put(key)
        head = self.head(key, include_etag=True)
        if (
            if_none_match == "*"
            and head is not None
            or if_match is not None
            and (head is None or head["etag"] != if_match)
        ):
            raise urllib.error.HTTPError("synthetic", 412, "precondition", {}, None)
        data = src.read_bytes()
        assert hashlib.sha256(data).hexdigest() == sha
        self.objects[key] = data
        self.generation[key] = self.generation.get(key, 0) + 1

    def get_file(self, key: str, dest: Path, *, maximum_bytes: int | None = None) -> None:
        data = self.objects[key]
        assert maximum_bytes is not None
        if len(data) > maximum_bytes:
            raise ValueError("size limit")
        dest.write_bytes(data)
        if self.after_get:
            self.after_get(key)


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def deny(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("real socket forbidden")

    monkeypatch.setattr(socket, "create_connection", deny)
    monkeypatch.setattr(socket.socket, "connect", deny)


def source(tmp_path: Path, count: int = 4) -> RawL2Cache:
    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        for index in range(count):
            received = dt.datetime(2026, 10, 14 + index // 2, 7, tzinfo=dt.UTC)
            cache.append(
                observation().model_copy(
                    update={"received_at": received, "epoch_id": f"epoch_{index // 2}"}
                )
            )
        cache.write_status(FeedStatus(state="stopped", gap=True, epoch_id="epoch_1"))
    return cache


def setup_archive(tmp_path: Path, count: int = 4) -> tuple[ModuleType, FakeR2, RawL2Cache, Path]:
    a, client, cache = archive(), FakeR2(), source(tmp_path, count)
    state = tmp_path / "archive-state"
    a.initialize(cache, state, client=client, prefix=PREFIX)
    return a, client, cache, state


def test_roundtrip_multidate_without_local_progress_and_zero_put_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    head = cache.root / "batches" / ".head"
    assert head.stat().st_ino == (cache.root / "batches" / f"{4:020d}" / "batch.json").stat().st_ino

    def deny_cache_mutation_or_scan(*args: Any, **kwargs: Any) -> None:
        pytest.fail("published-head archive must read only its fixed batch range")

    monkeypatch.setattr(cache, "writer", deny_cache_mutation_or_scan)
    monkeypatch.setattr(cache, "_directories", deny_cache_mutation_or_scan)
    receipt = a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert receipt.status == "archived" and receipt.last_sequence == 4
    assert a.check_archive(client=client, prefix=PREFIX).last_sequence == 4
    listing = a.list_archive(client=client, prefix=PREFIX)
    assert listing.days == ("2026-10-14", "2026-10-15")
    restored = []
    for day in listing.days:
        dest = tmp_path / day
        fetched = a.fetch_archive(day, dest, client=client, prefix=PREFIX)
        assert fetched.status == "fetched" and fetched.records == 2
        restored.extend((dest / "batches.jsonl").read_bytes().splitlines(keepends=True))
        assert (dest / "receipt.json").is_file()
    expected = [b.model_dump_json().encode() + b"\n" for b in cache.read_batches(limit=4).batches]
    assert restored == expected
    catalog = json.loads(client.objects[PREFIX + "/catalog.json"])
    assert catalog["source_observation"]["gap"] is True
    before = len(client.puts)
    assert a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW).status == "unchanged"
    assert a.initialize(cache, state, client=client, prefix=PREFIX).status == "already_initialized"
    assert len(client.puts) == before
    assert all(none == "*" for key, match, none in client.puts if key != PREFIX + "/catalog.json")
    assert client.puts[0][2] == "*" and client.puts[-1][1] is not None


def test_actual_763_row_trade_push_archive_restore_preserves_old_batch_and_full_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.l2._codec import decode_trade_response
    from rquant.l2.models import Binding, MarketObservation, RawL2Batch

    body = (Path(__file__).parents[1] / "fixtures" / "l2_trade_push_763.bin").read_bytes()
    body_sha = "f7c0734e1de817820745374ca311dab9ca936a52706179b89a9dd85d5f78024b"
    table_sha = "43be6a8dc34261bafa8e546c09b76ec590b021aff34687c2d6092869b59d4ffb"
    row_sha = "5b967e2cdd8a92581e0be872c9971b28955bab09dbc86bd5b8f6dd785b7f59c6"
    assert hashlib.sha256(body).hexdigest() == body_sha
    decoded = decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10237,
                                    code="601899", market=17, source_fd_ordinal=4953)
    observed = MarketObservation(
        epoch_id="epoch_0",
        binding=Binding(page_id=10237, kind="vendor4013", code="601899", market=17),
        received_at=dt.datetime(2026, 10, 14, 7, 0, 1, tzinfo=dt.UTC),
        request_id=0xFFFFFFFF, source_fd_ordinal=4953, delivery="candidate_push",
        response_body_hex=body.hex(), raw_table_hex=decoded["raw_table"].hex(),
        table=decoded["table"], response_sha256=decoded["response_sha256"],
    )
    a, client, cache = archive(), FakeR2(), source(tmp_path, 1)
    with cache.writer():
        cache.append(observed)
        cache.write_status(FeedStatus(state="stopped", gap=True, epoch_id="epoch_0"))
    expected = b"".join(b.model_dump_json().encode() + b"\n"
                        for b in cache.read_batches(limit=2).batches)
    before = {p.relative_to(cache.root): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in cache.root.rglob("*") if p.is_file()}

    def deny_writer(*args: Any, **kwargs: Any) -> None:
        pytest.fail("archive attempted to acquire the source cache writer")

    monkeypatch.setattr(cache, "writer", deny_writer)
    state = tmp_path / "archive-state"
    a.initialize(cache, state, client=client, prefix=PREFIX)
    archived = a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert archived.status == "archived" and archived.last_sequence == 2
    destination = tmp_path / "restored"
    fetched = a.fetch_archive("2026-10-14", destination, client=client, prefix=PREFIX)
    assert fetched.status == "fetched" and fetched.records == 2
    restored = (destination / "batches.jsonl").read_bytes()
    assert hashlib.sha256(restored).hexdigest() == hashlib.sha256(expected).hexdigest()
    batches = [RawL2Batch.model_validate_json(line) for line in restored.splitlines()]
    assert [b.local_batch_sequence for b in batches] == [1, 2]
    assert batches[0].table.row_count == 1
    assert batches[1].table.row_count == len(batches[1].table.rows) == 763
    assert batches[1].response_sha256 == body_sha
    assert batches[1].table.source_sha256 == table_sha
    assert batches[1].table.row_data_sha256 == row_sha
    puts = len(client.puts)
    assert a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW).status == "unchanged"
    assert len(client.puts) == puts
    assert before == {p.relative_to(cache.root): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in cache.root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("fault", ["missing", "sha", "type"])
def test_published_head_new_interval_fault_never_advances_catalog(
    tmp_path: Path, fault: str
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    catalog_key = PREFIX + "/catalog.json"
    before = client.objects[catalog_key]
    directory = cache.root / "batches" / f"{2:020d}"
    if fault == "missing":
        shutil.rmtree(directory)
    elif fault == "sha":
        (directory / "sha256").write_text("0" * 64 + "\n")
    else:
        (directory / "batch.json").unlink()
        (directory / "batch.json").mkdir(mode=0o700)
    # H is valid and does not inspect the unselected middle batch. The archive
    # must still validate every newly included record before publishing progress.
    assert cache.read_status().last_sequence == 4
    before_puts = len(client.puts)
    with pytest.raises(CacheError):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert client.objects[catalog_key] == before
    assert all(key != catalog_key for key, _, _ in client.puts[before_puts:])


@pytest.mark.parametrize("fault", ["symlink", "copied_inode", "sha"])
def test_invalid_published_head_fails_before_archive_upload(tmp_path: Path, fault: str) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    head = cache.root / "batches" / ".head"
    last = cache.root / "batches" / f"{4:020d}"
    if fault == "sha":
        (last / "sha256").write_text("0" * 64 + "\n")
    else:
        payload = head.read_bytes()
        head.unlink()
        if fault == "symlink":
            head.symlink_to(last / "batch.json")
        else:
            head.write_bytes(payload)
            head.chmod(0o600)
    before_objects, before_puts = dict(client.objects), len(client.puts)
    with pytest.raises(CacheError):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert client.objects == before_objects and len(client.puts) == before_puts


def test_legacy_headless_archive_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, client, cache = archive(), FakeR2(), source(tmp_path)
    head = cache.root / "batches" / ".head"
    head.unlink()
    before = {p.relative_to(cache.root): p.read_bytes() for p in cache.root.rglob("*") if p.is_file()}

    def deny_writer(*args: Any, **kwargs: Any) -> None:
        pytest.fail("legacy archive must not acquire a cache writer")

    monkeypatch.setattr(RawL2Cache, "writer", deny_writer)
    state = tmp_path / "archive-state"
    a.initialize(cache, state, client=client, prefix=PREFIX)
    assert a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW).last_sequence == 4
    fetched = a.fetch_archive("2026-10-14", tmp_path / "restored", client=client, prefix=PREFIX)
    assert fetched.records == 2
    assert not head.exists()
    assert before == {p.relative_to(cache.root): p.read_bytes() for p in cache.root.rglob("*") if p.is_file()}


def test_fixed_highwater_does_not_decode_later_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    original = cache.read_status

    def append_after_snapshot(**kwargs: Any) -> FeedStatus:
        snapshot = original(**kwargs)
        writer = RawL2Cache(cache.root)
        with writer.writer():
            writer.append(observation())
        # An invalid moving tail must not be touched when archiving the fixed prefix.
        (cache.root / "batches" / f"{5:020d}" / "batch.json").write_bytes(b"bad")
        return snapshot

    monkeypatch.setattr(cache, "read_status", append_after_snapshot)
    assert a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW).last_sequence == 4


@pytest.mark.parametrize("hour,minute", [(7, 0), (8, 59), (16, 30)])
def test_market_catchup_defers_before_cache_or_remote(
    tmp_path: Path, hour: int, minute: int
) -> None:
    a = archive()

    class Forbidden:
        def __getattr__(self, key: str) -> Any:
            pytest.fail("deferred job touched cache/remote")

    now = dt.datetime(2026, 10, 16, hour, minute, tzinfo=a.SHANGHAI)
    receipt = a.run_archive(
        Forbidden(), tmp_path / "state", client=Forbidden(), prefix=PREFIX, now=now
    )
    assert receipt.status == "deferred_market_hours" and not (tmp_path / "state").exists()


@pytest.mark.parametrize("fault", ["truncate", "first", "last"])
def test_changed_source_refuses_to_advance_or_reinitialize(tmp_path: Path, fault: str) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    path = cache.root / "batches" / f"{1 if fault == 'first' else 4:020d}"
    if fault == "truncate":
        import shutil

        shutil.rmtree(path)
    else:
        data = json.loads((path / "batch.json").read_bytes())
        data["epoch_id"] = "changed"
        payload = json.dumps(data).encode()
        (path / "batch.json").write_bytes(payload)
        (path / "sha256").write_text(hashlib.sha256(payload).hexdigest() + "\n")
    before = len(client.puts)
    for operation in (a.run_archive, a.initialize):
        with pytest.raises((a.ArchiveError, ValueError, RuntimeError)):
            operation(
                cache,
                state,
                client=client,
                prefix=PREFIX,
                **({"now": NOW} if operation == a.run_archive else {}),
            )
    assert len(client.puts) == before


def test_missing_remote_reference_blocks_check_and_run(tmp_path: Path) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    shard = next(k for k in client.objects if k.endswith(".jsonl.gz"))
    del client.objects[shard]
    before = len(client.puts)
    for call in (
        lambda: a.check_archive(client=client, prefix=PREFIX),
        lambda: a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW),
    ):
        with pytest.raises(a.ArchiveError):
            call()
    assert len(client.puts) == before


def test_late_catalog_commit_causes_cas_failure_never_overwrite(tmp_path: Path) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    key = PREFIX + "/catalog.json"
    old = client.objects[key]
    fired = False

    def late_commit(target: str) -> None:
        nonlocal fired
        if target == key and not fired:
            fired = True
            client.objects[key] = old
            client.generation[key] += 1

    client.on_put = late_commit
    with pytest.raises(a.ArchiveError, match="precondition_failed"):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert client.objects[key] == old
    assert client.puts[-1][1] == '"generation-1"'


def test_receipt_failure_after_catalog_commit_resumes_with_zero_put(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    original = a._write_receipt

    def failed(*args: Any, **kwargs: Any) -> None:
        raise OSError("synthetic local receipt failure")

    monkeypatch.setattr(a, "_write_receipt", failed)
    with pytest.raises(OSError):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert a.check_archive(client=client, prefix=PREFIX).last_sequence == 4
    before = len(client.puts)
    monkeypatch.setattr(a, "_write_receipt", original)
    assert (
        a.run_archive(cache, tmp_path / "fresh-state", client=client, prefix=PREFIX, now=NOW).status
        == "unchanged"
    )
    assert len(client.puts) == before


def test_restore_existing_destination_budget_and_corruption_leave_no_partial(
    tmp_path: Path,
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    dest = tmp_path / "restored"
    for maximum in (1, 100):
        with pytest.raises(a.ArchiveError):
            a.fetch_archive(
                "2026-10-14", dest, client=client, prefix=PREFIX, maximum_output_bytes=maximum
            )
        assert not dest.exists()
    dest.mkdir()
    with pytest.raises(a.ArchiveError):
        a.fetch_archive("2026-10-14", dest, client=client, prefix=PREFIX)
    dest.rmdir()
    shard = next(k for k in client.objects if k.endswith(".jsonl.gz"))
    client.objects[shard] = gzip.compress(b"not valid typed records\n", mtime=0)
    with pytest.raises(a.ArchiveError):
        a.fetch_archive("2026-10-14", dest, client=client, prefix=PREFIX)
    assert not dest.exists()


def test_missing_catalog_never_auto_initializes_and_bad_prefix_rejected(tmp_path: Path) -> None:
    a, client, cache = archive(), FakeR2(), source(tmp_path)
    with pytest.raises(a.ArchiveError, match="missing_catalog"):
        a.run_archive(cache, tmp_path / "state", client=client, prefix=PREFIX, now=NOW)
    for prefix in ("", "snapshots", "l2/v1/_acceptance/../bad", "l2/v1/_acceptance/x"):
        with pytest.raises((a.ArchiveError, ValueError)):
            a.initialize(cache, tmp_path / "state", client=client, prefix=prefix)
    assert not client.puts


def test_status_only_update_and_separate_archive_lock(tmp_path: Path) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    with cache.writer():
        cache.write_status(FeedStatus(state="disconnected", gap=True, epoch_id="new_epoch"))
    before = len(client.puts)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert len(client.puts) == before + 1 and client.puts[-1][0].endswith("catalog.json")
    with a._job_lock(state), pytest.raises(a.ArchiveError, match="archive_busy"):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)


def test_catalog_source_observation_rejects_coerced_types(tmp_path: Path) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    key = PREFIX + "/catalog.json"
    value = json.loads(client.objects[key])
    value["source_observation"]["gap"] = "false"
    client.objects[key] = json.dumps(value).encode()
    with pytest.raises(ValueError):
        a.check_archive(client=client, prefix=PREFIX)


def test_delayed_timed_out_catalog_put_cannot_overwrite_newer_progress(tmp_path: Path) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    key = PREFIX + "/catalog.json"
    original = client.put_file
    delayed = []

    def timeout(key_arg: str, path: Path, sha: str, **kwargs: Any) -> None:
        if key_arg == key:
            delayed.append(path.read_bytes())
            raise TimeoutError("synthetic unknown PUT outcome")
        return original(key_arg, path, sha, **kwargs)

    client.put_file = timeout
    with pytest.raises(a.ArchiveError, match="publication_uncertain"):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert json.loads(client.objects[key])["last_sequence"] == 0
    client.put_file = original

    def finish_delayed(target: str) -> None:
        if target == key:
            client.objects[key] = delayed[0]
            client.generation[key] += 1
            client.on_put = None

    client.on_put = finish_delayed
    with pytest.raises(a.ArchiveError, match="precondition_failed"):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert json.loads(client.objects[key])["last_sequence"] == 4
    before = len(client.puts)
    assert a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW).status == "unchanged"
    assert len(client.puts) == before


@pytest.mark.parametrize(
    "fault",
    ["sequence", "date", "epoch", "raw", "duplicate", "truncated", "gzip", "line", "expanded"],
)
def test_restore_validates_streamed_rows_not_only_object_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    a, client, cache = archive(), FakeR2(), source(tmp_path, 1)
    record = cache.read_batches().batches[0]
    line = record.model_dump_json().encode() + b"\n"
    data = json.loads(line)
    if fault in ("sequence", "date", "epoch", "raw"):
        field, value = {
            "sequence": ("local_batch_sequence", 2),
            "date": ("received_at", "2026-10-13T07:00:00Z"),
            "epoch": ("epoch_id", "other"),
            "raw": ("response_sha256", "0" * 64),
        }[fault]
        data[field] = value
        line = json.dumps(data).encode() + b"\n"
    elif fault == "duplicate":
        line = b'{"epoch_id":"duplicate",' + line[1:]
    compressed = gzip.compress(line, mtime=0)
    if fault == "truncated":
        compressed = compressed[:-3]
    if fault == "gzip":
        compressed = b"not gzip"
    key = PREFIX + "/synthetic"
    client.objects[key], client.generation[key] = compressed, 1
    chunk = a.Chunk(
        key=key,
        received_date="2026-10-14",
        first_sequence=1,
        last_sequence=1,
        records=1,
        compressed_bytes=len(compressed),
        expanded_bytes=len(line),
        sha256=hashlib.sha256(compressed).hexdigest(),
        content_sha256=hashlib.sha256(line).hexdigest(),
        epochs=("epoch_0",),
    )
    if fault == "line":
        monkeypatch.setattr(a, "MAX_LINE", len(line) - 1)
    if fault == "expanded":
        monkeypatch.setattr(a, "MAX_SHARD", len(line) - 1)
    with pytest.raises((a.ArchiveError, ValueError)):
        list(a._chunk_lines(client, chunk, tmp_path))


def test_nonmonotonic_received_dates_keep_original_ranges(tmp_path: Path) -> None:
    a, client, cache, state = setup_archive(tmp_path, 0)
    writer = RawL2Cache(cache.root)
    with writer.writer():
        for day in (15, 14, 15):
            writer.append(
                observation().model_copy(
                    update={"received_at": dt.datetime(2026, 10, day, 7, tzinfo=dt.UTC)}
                )
            )
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    receipt = a.fetch_archive("2026-10-15", tmp_path / "out", client=client, prefix=PREFIX)
    rows = [
        json.loads(line)["local_batch_sequence"]
        for line in (tmp_path / "out" / "batches.jsonl").read_bytes().splitlines()
    ]
    assert rows == [1, 3] and receipt.records == 2
    assert receipt.sequence_ranges == ((1, 1), (3, 3))


def test_archive_state_must_not_write_inside_source_cache(tmp_path: Path) -> None:
    a, client, cache = archive(), FakeR2(), source(tmp_path)
    before = sorted(str(p.relative_to(cache.root)) for p in cache.root.rglob("*"))
    for state in (cache.root, cache.root / "archive-state"):
        with pytest.raises(a.ArchiveError, match="unsafe_state"):
            a.initialize(cache, state, client=client, prefix=PREFIX)
    assert sorted(str(p.relative_to(cache.root)) for p in cache.root.rglob("*")) == before
    assert not client.puts


def test_standalone_probe_no_dev_dependency_and_synthetic_cannot_claim_real_r2(
    tmp_path: Path,
) -> None:
    import runpy

    root = Path(__file__).resolve().parents[2]
    script = root / "scripts/validate-l2-archive.py"
    assert script.is_file(), "standalone no-dev R2 validator is missing"
    probe = runpy.run_path(str(script))
    result = probe["run_probe"](root, "f" * 40, tmp_path / "probe", PREFIX, client=FakeR2())
    assert result["schema"] == "rquant-l2-archive-acceptance-v1"
    assert result["real_r2"] is False and result["synthetic_only"] is True
    assert result["roundtrip"]["source_count"] == result["roundtrip"]["restored_count"] == 4
    assert result["roundtrip"]["source_sha256"] == result["roundtrip"]["restored_sha256"]
    assert result["idempotence_puts"] == 0 and result["closed"] is True
    assert all(result["cases"].values()) and result["requests"]["precondition_failed"] == 1
    assert (tmp_path / "probe/execution-source.json").is_file()
    assert "pytest" not in script.read_text()


def test_byte_envelope_standalone_probe_streams_all_four_large_records(tmp_path: Path, monkeypatch) -> None:
    import importlib.util
    entry = Path(__file__).resolve().parents[2] / "scripts/validate-l2-archive.py"
    spec = importlib.util.spec_from_file_location("archive_large_probe", entry)
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    original = probe._observation

    def factory(index):
        obs = original(index)
        return obs.model_copy(update={"table": obs.table.model_copy(update={
            "interpretation_limits": "x" * (13 << 20)})}) if index % 2 == 0 else obs

    monkeypatch.setattr(probe, "_observation", factory)
    client = FakeR2()
    result = probe.run_probe(entry.parents[1], "a" * 40, tmp_path / "large-probe", PREFIX, client=client)
    assert result["roundtrip"]["source_count"] == result["roundtrip"]["restored_count"] == 4
    assert result["roundtrip"]["source_sha256"] == result["roundtrip"]["restored_sha256"]
    assert result["idempotence_puts"] == 0 and result["requests"]["precondition_failed"] == 1
    assert result["closed"] is True and all(result["cases"].values())
    # Real byte-limited cache reads return fewer than four records, which the
    # standalone proof must never confuse with the complete fixed H=4 source.
    cache = RawL2Cache(tmp_path / "large-probe/synthetic-cache")
    assert cache.read_batches(limit=4).next_sequence < 4


def test_byte_envelope_archive_retains_whole_large_lines_and_fixed_sequence(tmp_path: Path):
    a, client = archive(), FakeR2()
    cache = RawL2Cache(tmp_path / "large-source")
    obs = observation()
    obs = obs.model_copy(update={"table": obs.table.model_copy(update={
        "interpretation_limits": "x" * (13 << 20)})})
    with cache.writer():
        for _ in range(3): cache.append(obs)
        cache.write_status(FeedStatus(state="stopped", gap=True, epoch_id=obs.epoch_id))
    state = tmp_path / "large-state"
    a.initialize(cache, state, client=client, prefix=PREFIX)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    catalog = a.Catalog.model_validate_json(client.objects[PREFIX + "/catalog.json"])
    day = next(iter(catalog.days))
    manifest = a.DayManifest.model_validate_json(client.objects[catalog.days[day].key])
    assert len(manifest.chunks) == 2
    assert [chunk.records for chunk in manifest.chunks] == [2, 1]
    assert all(chunk.expanded_bytes <= 32 << 20 for chunk in manifest.chunks)
    destination = tmp_path / "large-restored"
    receipt = a.fetch_archive(day, destination, client=client, prefix=PREFIX)
    assert receipt.records == receipt.last_sequence == 3 and catalog.source_observation.gap is True
    with (destination / "batches.jsonl").open("rb") as stream:
        for sequence in range(1, 4):
            line = stream.readline(a.MAX_LINE + 1)
            assert line == a._bytes(a._batch(cache, sequence))
        assert stream.read(1) == b""


@pytest.mark.parametrize("target", ["catalog", "day"])
@pytest.mark.parametrize("variant", ["missing", "internal_alias", "unknown"])
def test_remote_wire_schema_is_required_before_restore(
    tmp_path: Path, target: str, variant: str
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    catalog_key = PREFIX + "/catalog.json"
    catalog = json.loads(client.objects[catalog_key])
    value = catalog if target == "catalog" else json.loads(
        client.objects[catalog["days"]["2026-10-14"]["key"]]
    )
    version = value.pop("schema")
    if variant == "internal_alias":
        value["schema_"] = version
    elif variant == "unknown":
        value["schema"] = version + "-unknown"
    if target == "day":
        payload = json.dumps(value).encode()
        digest = hashlib.sha256(payload).hexdigest()
        key = f"{PREFIX}/days/2026-10-14/manifests/{digest}.json"
        client.objects[key], client.generation[key] = payload, 1
        catalog["days"]["2026-10-14"].update(key=key, sha256=digest, size=len(payload))
    client.objects[catalog_key] = json.dumps(catalog).encode()
    before = len(client.puts)
    with pytest.raises((a.ArchiveError, ValueError)):
        a.check_archive(client=client, prefix=PREFIX)
    destination = tmp_path / "invalid-wire-schema"
    with pytest.raises((a.ArchiveError, ValueError)):
        a.fetch_archive("2026-10-14", destination, client=client, prefix=PREFIX)
    assert not destination.exists() and len(client.puts) == before


@pytest.mark.parametrize("token", ["*", "unquoted", 'W/"weak"', '"one","two"',
                                   '"one", "two"', ' "spaced"', '"contains space"'])
def test_catalog_requires_single_strong_etag_before_conditional_put(
    tmp_path: Path, token: str
) -> None:
    a, client, cache, state = setup_archive(tmp_path)
    original = client.head

    def bad_etag(key: str, *, include_etag: bool = False) -> dict | None:
        head = original(key, include_etag=include_etag)
        if head is not None and include_etag and key == PREFIX + "/catalog.json":
            head["etag"] = token
        return head

    client.head = bad_etag
    before = len(client.puts)
    with pytest.raises(a.ArchiveError, match="etag"):
        a.run_archive(cache, state, client=client, prefix=PREFIX, now=NOW)
    assert len(client.puts) == before


@pytest.mark.parametrize("token", ['"opaque-generation-4"', '""', '"a,b"', '"back\\slash"'])
def test_single_strong_etag_is_preserved_without_hash_interpretation(token: str) -> None:
    assert archive()._etag({"etag": token}) == token
