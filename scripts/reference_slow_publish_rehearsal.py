"""Rehearse the reference-slow publication (#297) on a copy of a real sealed batch.

Copies one sealed reference-slow batch out of a runtime root, re-seals it into a fresh spool
with the current source rule (the envelope's `available_at`), publishes it with the current
`publish_reference_slow_batches` into a fresh registry, and checks what
`auction_gap_candidate_input` would say at 09:29 (its reference lookups for every auction
code through the one-read `as_of_snapshot` path of #299, cross-checked against the old
single-key `as_of` for `--single-key-sample` codes, and with `--full-auction-assembly` one
whole production round over the copied auction-match batch) -- everything under one private
rehearsal root. Nothing outside that root is written: the production spool, registry and
DuckDB files are only read (the batch, the calendar generation, optionally the auction-match
spool and, with `--production-registry-copy`, the live registry are copied first), and the
only DuckDB this opens is a synthetic one it creates in the root.

    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<checkout>/src <venv>/bin/python \\
        <checkout>/scripts/reference_slow_publish_rehearsal.py \\
        --runtime-root /home/lighthouse/rquant/data/runtime \\
        --rehearsal-root /home/lighthouse/rquant/var/rehearsal

The publisher clock starts at `--publisher-start` (default 09:21:26 on the batch's trade date,
the 2026-09-24 first attempt) and then advances with real wall time, so commit latency is
real. `--slow-commit-seconds N` sleeps N real seconds right after the registry's stage
`COMMIT`, i.e. makes the commit N seconds slower: N=10 must still publish (the five-second
guard is gone), N large enough to cross 09:25 must be refused and rolled back.

`--production-registry-copy` also copies the live reference registry (SQLite backup API, read
from a `mode=ro` connection) and times what the auction_gap publisher pays on it every round:
opening it (the reader's integrity pass reads every record the registry has ever held) and
one snapshot over all codes of its current generation. That step only reports.

Exit status: 0 when the batch was published visible at 09:25 and the 09:29 reference checks
accept it; 1 on any refusal (including the one `--slow-commit-seconds` is meant to provoke --
the last lines say which and why); 2 on a usage error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CONSUMER_ID = "reference-slow-publisher"
_SSH_KEYGEN = "/usr/bin/ssh-keygen"
#: the v0.33.20 source promised `prepared_at + 5 s`; its batches say when they were prepared
_OLD_SOURCE_GUARD = timedelta(seconds=5)


class RehearsalRefusedError(RuntimeError):
    """A step refused; the rehearsal exits 1 with this message."""


@dataclass
class _Timings:
    marks: dict[str, float] = field(default_factory=dict)

    def timed(self, name: str, function: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            started = time.monotonic()
            try:
                return function(*args, **kwargs)
            finally:
                self.marks[name] = round(self.marks.get(name, 0.0) + time.monotonic() - started, 3)

        return wrapper


class _MappedClock:
    """A wall clock pinned to `start` at construction and advancing with real time."""

    def __init__(self, start: datetime) -> None:
        self._start = start
        self._origin = time.monotonic()

    def __call__(self) -> datetime:
        return self._start + timedelta(seconds=time.monotonic() - self._origin)


def _local(day: date, value: str) -> datetime:
    parsed = clock_time.fromisoformat(value)
    return datetime.combine(day, parsed, tzinfo=_SHANGHAI).astimezone(UTC)


def _show(out: Callable[[str], None], label: str, value: object) -> None:
    out(f"{label}: {json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)}")


def _private_directory(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    path.chmod(0o700)
    return path


def _copy_private(source: Path, target: Path) -> Path:
    """Read `source` and write a 0600 copy -- the source is never opened for writing."""

    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with source.open("rb") as reader, target.open("xb") as writer:
        shutil.copyfileobj(reader, writer)
    target.chmod(0o600)
    return target


def _copy_tree_read_only(source: Path, target: Path) -> None:
    """Copy a spool directory, skipping the quota ledger and any lock files."""

    for directory, subdirectories, files in os.walk(source, followlinks=False):
        relative = Path(directory).relative_to(source)
        subdirectories[:] = [name for name in subdirectories if name != "locks"]
        (target / relative).mkdir(mode=0o700, parents=True, exist_ok=True)
        (target / relative).chmod(0o700)
        for name in files:
            if name.startswith("quota.sqlite3") or name.endswith(".lock"):
                continue
            path = Path(directory) / name
            if path.is_symlink() or not path.is_file():
                continue
            _copy_private(path, target / relative / name)


def _read_sqlite(path: Path, query: str) -> list[tuple[Any, ...]]:
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as connection:
        return connection.execute(query).fetchall()


def _time_production_registry(
    *,
    runtime_root: Path,
    root: Path,
    codes: tuple[str, ...],
    datasets: tuple[Any, ...],
    out: Callable[[str], None],
) -> None:
    """What the live registry costs the auction_gap publisher per round; reports only.

    The rehearsal registry holds one generation, the live one every session since the first
    publication, and the reader's integrity pass at open reads all of it. The copy is taken
    with SQLite's backup API from a `mode=ro` connection, so the live file is only read.
    """

    from rquant.reference_data_registry import (
        ReadonlyReferenceRegistry,
        ReferenceDataUnavailableError,
    )

    source = runtime_root / "authorities" / "reference-slow" / "reference.sqlite3"
    target = root / "production-registry" / "reference.sqlite3"
    try:
        target.parent.mkdir(mode=0o700)
        with (
            closing(sqlite3.connect(f"file:{source}?mode=ro", uri=True)) as reader,
            closing(sqlite3.connect(target)) as writer,
        ):
            reader.backup(writer)
        target.chmod(0o600)
        (records, generations) = _read_sqlite(
            target,
            "SELECT (SELECT COUNT(*) FROM reference_record), "
            "(SELECT COUNT(*) FROM reference_generation)",
        )[0]
        started = time.monotonic()
        copy = ReadonlyReferenceRegistry(target)
        open_seconds = time.monotonic() - started
        pointer = copy.current_pointer()
        started = time.monotonic()
        snapshot = copy.as_of_snapshot(
            dataset_ids=datasets,
            keys=codes,
            generation_id=pointer.generation_id,
        )
        read_seconds = time.monotonic() - started
        unavailable = 0
        for code in codes:
            for dataset in datasets:
                try:
                    snapshot.as_of(
                        dataset_id=dataset,
                        key=code,
                        event_time=pointer.switched_at,
                        decision_time=pointer.switched_at,
                    )
                except ReferenceDataUnavailableError:
                    unavailable += 1
        lookups_seconds = time.monotonic() - started
    except Exception as error:  # noqa: BLE001 - this step only reports
        out(f"production registry copy: unusable ({type(error).__name__}: {error})")
        return
    _show(
        out,
        "production registry copy",
        {
            "records": records,
            "generations": generations,
            "current_generation_id": pointer.generation_id,
            "current_switched_at": pointer.switched_at,
            #: paid by every auction_gap round (load_live_auction_candidate_input)
            "open_seconds": round(open_seconds, 3),
            "codes": len(codes),
            "snapshot_read_seconds": round(read_seconds, 3),
            "snapshot_and_lookups_seconds": round(lookups_seconds, 3),
            "unavailable_at_switched_at": unavailable,
        },
    )


def run_rehearsal(
    *,
    runtime_root: Path,
    rehearsal_root: Path,
    sequence: int,
    publisher_start: str,
    slow_commit_seconds: float,
    round_interval_seconds: float,
    auction: bool,
    auction_observed: str,
    auction_sample: int = 0,
    single_key_sample: int = 50,
    full_auction_assembly: bool = False,
    production_registry_copy: bool = False,
    out: Callable[[str], None] = print,
) -> int:
    from rquant.live_contracts import BatchEnvelope, LiveChannel
    from rquant.live_spool import (
        LiveBatchSpool,
        ReferenceSourceBatchSigner,
        ReferenceSourceBatchVerifier,
    )
    from rquant.reference_data_registry import (
        ReadonlyReferenceRegistry,
        ReferenceDataset,
        ReferenceDataUnavailableError,
        ReferencePublicationAuthenticator,
        ReferenceRegistry,
    )
    from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
    from rquant.reference_slow_runtime import (
        capture_reference_slow_batch,
        publish_reference_slow_batches,
    )
    from rquant.runtime_market_session import load_market_calendar_authority
    from rquant.runtime_serving_authority import (
        ServingSourceAuthorityReader,
        ServingSourceAuthorityUnavailableError,
    )
    from rquant.runtime_serving_snapshot import REFERENCE_SLOW_AUTHORITY_DATASET_ID
    from rquant.strict_json import strict_model_validate_canonical_json

    runtime_root = runtime_root.resolve()
    stamp = datetime.now(_SHANGHAI).strftime("%Y%m%dT%H%M%S")
    root = rehearsal_root.resolve() / f"{stamp}-{secrets.token_hex(3)}"
    if root.is_relative_to(runtime_root) or runtime_root.is_relative_to(root):
        raise RehearsalRefusedError("the rehearsal root must not overlap the runtime root")
    rehearsal_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    _private_directory(root)
    tmp = _private_directory(root / "tmp")
    os.environ["TMPDIR"] = str(tmp)
    import tempfile

    tempfile.tempdir = str(tmp)
    out(f"rehearsal root: {root}")

    # 1. copy the sealed batch and the calendar generation it names -------------------------
    source_batches = runtime_root / "live" / "reference-slow" / "batches" / "reference_slow"
    name = f"{sequence:020d}"
    manifest_copy = _copy_private(source_batches / f"{name}.json", root / "input" / f"{name}.json")
    payload_copy = _copy_private(
        source_batches / f"{name}.payload",
        root / "input" / f"{name}.payload",
    )
    original = BatchEnvelope.model_validate_json(manifest_copy.read_bytes())
    payload = payload_copy.read_bytes()
    if hashlib.sha256(payload).hexdigest() != original.content_sha256:
        raise RehearsalRefusedError("copied payload does not match its envelope content_sha256")
    snapshot = strict_model_validate_canonical_json(ReferenceSlowSourceSnapshot, payload)
    if snapshot.content_sha256 != original.batch_id:
        raise RehearsalRefusedError("copied payload does not match its envelope batch_id")
    calendar_sha = snapshot.source_snapshot_ids["calendar"]
    calendar_copy = _copy_private(
        runtime_root / "authorities" / "market-calendar" / "generations" / f"{calendar_sha}.json",
        root / "input" / "calendar.json",
    )
    calendar_commit = json.loads(calendar_copy.read_bytes())["producer_commit"]
    calendar = load_market_calendar_authority(calendar_copy, expected_commit=calendar_commit)
    if calendar.content_sha256 != calendar_sha:
        raise RehearsalRefusedError("calendar generation does not match the batch's calendar id")
    trade_date = snapshot.target_trade_date
    _show(
        out,
        "copied batch",
        {
            "sequence": original.sequence,
            "target_trade_date": trade_date,
            "row_count": original.row_count,
            "captured_at": snapshot.captured_at,
            "original_available_at": original.available_at,
            "producer_commit": original.producer_commit,
        },
    )

    # 2. re-seal it with the current source rule, under a throwaway signing key ------------
    key = root / "keys" / "source-ed25519"
    key.parent.mkdir(mode=0o700)
    subprocess.run(
        (_SSH_KEYGEN, "-q", "-t", "ed25519", "-N", "", "-f", str(key)),
        check=True,
    )
    key.chmod(0o600)
    signer = ReferenceSourceBatchSigner(key_id="rehearsal-v1", private_key=key.read_text("ascii"))
    verifier = ReferenceSourceBatchVerifier(
        key_id="rehearsal-v1",
        public_key=key.with_suffix(".pub").read_text("ascii").strip(),
    )
    spool_root = root / "live" / "reference-slow"
    source_spool = LiveBatchSpool(spool_root, source_signer=signer, source_verifier=verifier)
    source_clock = _MappedClock(original.available_at - _OLD_SOURCE_GUARD)
    timings = _Timings()
    source_spool.publish = timings.timed("source_spool_write_s", source_spool.publish)  # type: ignore[method-assign]
    resealed = capture_reference_slow_batch(
        spool=source_spool,
        calendar=calendar,
        observed_at=snapshot.captured_at,
        producer_commit=original.producer_commit,
        producer_version=original.producer_version,
        snapshot_loader=lambda: snapshot,
        completion_clock=source_clock,
    )
    (record,) = source_spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)
    _show(
        out,
        "re-sealed batch",
        {
            "sequence": resealed.output_sequence,
            "prepared_at_clock_start": original.available_at - _OLD_SOURCE_GUARD,
            "available_at": record.envelope.available_at,
            "spool_write_seconds": timings.marks.get("source_spool_write_s"),
        },
    )

    # 3. publish it with the current publisher ---------------------------------------------
    authenticator = ReferencePublicationAuthenticator(
        key_id="rehearsal-hmac-v1",
        secret=secrets.token_bytes(32),
    )
    consumer = LiveBatchSpool(
        spool_root,
        cursor_root=root / "control" / "reference-slow-publishers" / "cursors",
        source_read_only=True,
        publication_authenticator=authenticator,
        source_verifier=verifier,
    )
    registry_path = root / "authorities" / "reference-slow" / "reference.sqlite3"
    registry = ReferenceRegistry(registry_path, publication_authenticator=authenticator)
    original_append = registry.append_many_and_publish_before

    def append_with_latency(records: Any, *, completion_clock: Any, **kwargs: Any) -> Any:
        readings = 0

        def inside() -> datetime:
            nonlocal readings
            readings += 1
            if readings == 2 and slow_commit_seconds > 0:
                #: the reading right after the stage COMMIT: this is where 09-24 ran long
                time.sleep(slow_commit_seconds)
            return completion_clock()

        return original_append(records, completion_clock=inside, **kwargs)

    registry.append_many_and_publish_before = timings.timed(  # type: ignore[method-assign]
        "registry_stage_commit_s", append_with_latency
    )
    registry.commit_publication_stage = timings.timed(  # type: ignore[method-assign]
        "registry_commit_stage_s", registry.commit_publication_stage
    )
    registry.finalize_publication = timings.timed(  # type: ignore[method-assign]
        "registry_finalize_s", registry.finalize_publication
    )
    publisher_clock = _MappedClock(_local(trade_date, publisher_start))
    decision = _local(trade_date, "09:25:00")
    published = None
    rounds: list[dict[str, object]] = []
    while published is None:
        started = publisher_clock()
        round_started = time.monotonic()
        try:
            result = publish_reference_slow_batches(
                spool=consumer,
                registry=registry,
                calendar=calendar,
                consumer_id=_CONSUMER_ID,
                observed_at=started,
                producer_commit=original.producer_commit,
                completion_clock=publisher_clock,
            )
        except ReferenceDataUnavailableError as error:
            #: nothing visible yet and no generation to reconcile: production records this
            #: round as a failure and retries; so does the rehearsal
            rounds.append({"started": started, "outcome": f"not yet: {error}"})
        except Exception as error:  # noqa: BLE001 - every refusal is reported the same way
            rounds.append({"started": started, "outcome": f"REFUSED: {error}"})
            _show(out, "publisher rounds", rounds)
            _show(out, "timings", timings.marks)
            try:
                registry.current_pointer()
                rolled_back = False
            except ReferenceDataUnavailableError:
                rolled_back = True
            _show(out, "registry rolled back (no current generation)", rolled_back)
            raise RehearsalRefusedError(f"publisher refused: {error}") from error
        else:
            rounds.append(
                {
                    "started": started,
                    "outcome": "published" if result.processed_count else "nothing visible",
                    "round_seconds": round(time.monotonic() - round_started, 3),
                }
            )
            if result.processed_count:
                published = result
                break
        time.sleep(round_interval_seconds)
    _show(out, "publisher rounds", rounds)
    _show(
        out,
        "publication result",
        {
            "processed_count": published.processed_count,
            "input_sequence": published.input_sequence,
            "output_sequence": published.output_sequence,
            "degraded_reasons": published.degraded_reasons,
            "source_generations": dict(published.source_generations),
        },
    )
    _show(out, "measured seconds", timings.marks)

    # 4. what became visible, and when -------------------------------------------------------
    pointer = registry.current_pointer()
    manifest = registry.current_manifest()
    (records_written, first_min, first_max) = _read_sqlite(
        registry_path,
        "SELECT COUNT(*), MIN(first_available_at), MAX(first_available_at) FROM reference_record",
    )[0]
    receipts = _read_sqlite(
        registry_path,
        "SELECT completed_at, visible_at FROM reference_publication_receipt",
    )
    authority = ServingSourceAuthorityReader(
        root=spool_root / "serving-authority",
        expected_producer_commit=original.producer_commit,
        expected_dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        expected_payload_kind="reference_slow",
    )(decision)
    try:
        ServingSourceAuthorityReader(
            root=spool_root / "serving-authority",
            expected_producer_commit=original.producer_commit,
            expected_dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            expected_payload_kind="reference_slow",
        )(decision - timedelta(microseconds=1))
        authority_before_decision = "VISIBLE (unexpected)"
    except ServingSourceAuthorityUnavailableError as error:
        authority_before_decision = f"not visible ({error})"
    _show(
        out,
        "registry",
        {
            "generation_id": manifest.generation_id,
            "records_written": records_written,
            "first_available_at": sorted({first_min, first_max}),
            "pointer_switched_at": pointer.switched_at,
            "manifest_published_at": manifest.published_at,
            "receipt_completed_at_visible_at": receipts,
        },
    )
    _show(
        out,
        "serving authority",
        {
            "published_at": authority.published_at,
            "sequence": authority.sequence,
            "reference_generation_id": authority.payload.reference_generation_id,
            "at_09:24:59.999999": authority_before_decision,
        },
    )
    visible_at = {pointer.switched_at, manifest.published_at}
    if visible_at != {decision} or first_min != first_max:
        raise RehearsalRefusedError(f"generation is not visible exactly at 09:25: {visible_at}")
    if authority_before_decision.startswith("VISIBLE"):
        raise RehearsalRefusedError("serving authority is visible before 09:25")

    # 5. auction_gap_candidate_input's reference checks at 09:29 ------------------------------
    readonly = ReadonlyReferenceRegistry(registry_path)
    codes = tuple(fact.ts_code for fact in snapshot.security_facts)
    auction_copy = root / "live" / "auction-match"
    auction_envelope = None
    auction_spool = None
    if auction:
        source_auction = runtime_root / "live" / "auction-match"
        if source_auction.is_dir():
            _copy_tree_read_only(source_auction, auction_copy)
            try:
                auction_spool = LiveBatchSpool(auction_copy)
                current = auction_spool.current(LiveChannel.AUCTION_MATCH)
                if current is not None:
                    (auction_record,) = auction_spool.list_after(
                        LiveChannel.AUCTION_MATCH,
                        sequence=current.sequence - 1,
                    )
                    auction_envelope = auction_record.envelope
            except Exception as error:  # noqa: BLE001 - reported, then the reference-only check runs
                out(f"auction-match copy unusable ({error}); checking snapshot codes instead")
    observed = _local(trade_date, auction_observed)
    event_time = observed
    if auction_envelope is not None and auction_spool is not None:
        from rquant.auction_match_gateway import AuctionMatchGateway

        frame = AuctionMatchGateway.decode_payload(auction_spool.read_payload(auction_record))
        codes = tuple(sorted(str(value) for value in frame["ts_code"]))
        event_time = auction_envelope.event_time_end
        observed = max(observed, auction_envelope.available_at)
        _show(
            out,
            "copied auction-match batch",
            {
                "sequence": auction_envelope.sequence,
                "quality_status": auction_envelope.quality_status,
                "trade_date": auction_envelope.event_time_end.astimezone(_SHANGHAI).date(),
                "available_at": auction_envelope.available_at,
                "rows": len(codes),
            },
        )
        if auction_envelope.event_time_end.astimezone(_SHANGHAI).date() != trade_date:
            out("auction-match batch belongs to another session; checking its codes only")
            auction_envelope = None

    datasets = (
        ReferenceDataset.ST_STATUS,
        ReferenceDataset.SUSPENSION_STATUS,
        ReferenceDataset.LISTING_STATUS,
        ReferenceDataset.PRICE_LIMIT_REGIME,
    )

    def reference_checks(
        at: datetime,
        checked: tuple[str, ...],
        *,
        single_key_codes: int = 0,
    ) -> dict[str, object]:
        """The reference half of `assemble_auction_gap_candidate_batch`, as it reads now.

        One `as_of_snapshot` for the four datasets and every checked code, then four
        lookups per code from it (#299). The first `single_key_codes` codes are also read
        the pre-#299 way -- one `as_of` round trip per lookup -- and must answer the same.
        """

        pointer = readonly.current_pointer()
        manifest = readonly.current_manifest()
        if pointer.generation_id != manifest.generation_id:
            return {"observed_at": at, "refused": "reference pointer and manifest disagree"}
        if pointer.switched_at > at or manifest.published_at > at:
            return {"observed_at": at, "refused": "reference generation is future evidence"}
        missing: dict[str, int] = {}
        answers: dict[tuple[str, str], str] = {}
        started = time.monotonic()
        snapshot = readonly.as_of_snapshot(
            dataset_ids=datasets,
            keys=checked,
            generation_id=manifest.generation_id,
        )
        read_seconds = time.monotonic() - started
        for code in checked:
            for dataset in datasets:
                try:
                    answers[(code, dataset.value)] = snapshot.as_of(
                        dataset_id=dataset,
                        key=code,
                        event_time=event_time,
                        decision_time=at,
                    ).record.record_id
                except ReferenceDataUnavailableError as error:
                    missing[dataset.value] = missing.get(dataset.value, 0) + 1
                    answers[(code, dataset.value)] = f"unavailable: {error}"
        elapsed = time.monotonic() - started
        result: dict[str, object] = {
            "observed_at": at,
            "codes_checked": len(checked),
            "lookups": 4 * len(checked),
            "unavailable": missing,
            "snapshot_read_seconds": round(read_seconds, 3),
            #: what one `assemble_auction_gap_candidate_batch` round now spends on them
            "seconds": round(elapsed, 3),
        }
        if single_key_codes > 0:
            sample = checked[:single_key_codes]
            mismatches: list[str] = []
            started = time.monotonic()
            for code in sample:
                for dataset in datasets:
                    try:
                        single = readonly.as_of(
                            dataset_id=dataset,
                            key=code,
                            event_time=event_time,
                            decision_time=at,
                            generation_id=manifest.generation_id,
                        ).record.record_id
                    except ReferenceDataUnavailableError as error:
                        single = f"unavailable: {error}"
                    if single != answers[(code, dataset.value)]:
                        mismatches.append(f"{code}/{dataset.value}")
            single_elapsed = time.monotonic() - started
            per_lookup = single_elapsed / max(1, 4 * len(sample))
            result["single_key_as_of"] = {
                "codes": len(sample),
                "seconds": round(single_elapsed, 3),
                "ms_per_lookup": round(per_lookup * 1000, 2),
                #: what the pre-#299 per-row reads would have spent on every checked code
                "projected_seconds_for_all_codes": round(per_lookup * 4 * len(checked), 1),
                "mismatches_with_bulk": mismatches,
            }
        return result

    sample = codes if auction_sample <= 0 else codes[:auction_sample]
    early = reference_checks(decision - timedelta(seconds=1), sample)
    late = reference_checks(observed, sample, single_key_codes=single_key_sample)
    _show(out, "auction_gap reference checks at 09:24:59", early)
    _show(out, "auction_gap reference checks", late)
    if "refused" not in early:
        raise RehearsalRefusedError("today's generation was usable before 09:25")
    if "refused" in late or late["unavailable"]:
        raise RehearsalRefusedError(f"auction_gap reference checks refuse the generation: {late}")
    single_key = late.get("single_key_as_of")
    if isinstance(single_key, dict) and single_key["mismatches_with_bulk"]:
        raise RehearsalRefusedError(
            f"bulk and single-key reference reads disagree: {single_key['mismatches_with_bulk']}"
        )

    if full_auction_assembly and auction_envelope is not None and auction_spool is not None:
        import duckdb

        from rquant.auction_gap_candidate_input import AuctionGapCandidateInputError
        from rquant.runtime_builder_candidate import load_live_auction_candidate_input

        prior = sorted(item for item in calendar.open_dates if item < trade_date)[-5:]
        synthetic = root / "synthetic-daily.duckdb"
        with duckdb.connect(str(synthetic)) as connection:
            connection.execute(
                "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)"
            )
            connection.executemany(
                "INSERT INTO daily_bar VALUES (?, ?, ?)",
                [(code, day, 1_000.0) for code in codes for day in prior],
            )
        synthetic.chmod(0o600)
        replica_time = _local(trade_date, "09:00:00").timestamp()
        os.utime(synthetic, (replica_time, replica_time))
        #: the candidate publisher calls load_live_auction_candidate_input every round: it
        #: opens the registry afresh (the reader's integrity pass) and assembles. The open
        #: is timed on its own first; `registry_connections` counts the round's, open
        #: included (pre-#299 that was 4 per auction row more)
        started = time.monotonic()
        ReadonlyReferenceRegistry(registry_path)
        open_seconds = time.monotonic() - started
        connections = [0]
        original_connect = ReadonlyReferenceRegistry._connect

        def counted_connect(self: Any) -> Any:
            connections[0] += 1
            return original_connect(self)

        ReadonlyReferenceRegistry._connect = counted_connect  # type: ignore[method-assign]
        started = time.monotonic()
        try:
            batch = load_live_auction_candidate_input(
                auction_spool_root=auction_copy,
                daily_database_path=synthetic,
                reference_registry_path=registry_path,
                calendar_path=calendar_copy,
                calendar_expected_commit=calendar_commit,
                calendar_content_sha256=calendar.content_sha256,
                trade_date=trade_date,
                observed_at=observed,
                producer_commit=auction_envelope.producer_commit,
            )
        except AuctionGapCandidateInputError as error:
            raise RehearsalRefusedError(f"auction_gap_candidate_input refused: {error}") from error
        finally:
            ReadonlyReferenceRegistry._connect = original_connect  # type: ignore[method-assign]
        round_seconds = time.monotonic() - started
        _show(
            out,
            "auction_gap_candidate_input (one production round, synthetic prior-5 volumes)",
            {
                "observed_at": observed,
                "facts": len(batch.facts),
                "listed": sum(fact.is_listed for fact in batch.facts),
                "st": sum(fact.is_st for fact in batch.facts),
                "suspended": sum(fact.is_suspended for fact in batch.facts),
                "captured_at": batch.authority.captured_at,
                "registry_open_seconds": round(open_seconds, 3),
                "registry_connections": connections[0],
                #: load_live_auction_candidate_input: calendar + registry open + assembly
                "seconds": round(round_seconds, 3),
            },
        )

    if production_registry_copy:
        _time_production_registry(
            runtime_root=runtime_root,
            root=root,
            codes=codes,
            datasets=datasets,
            out=out,
        )
    out("REHEARSAL OK")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--rehearsal-root", type=Path, required=True)
    parser.add_argument("--sequence", type=int, default=0, help="reference-slow batch sequence")
    parser.add_argument(
        "--publisher-start",
        default="09:21:26",
        help="local wall time the publisher clock starts at (default 09:21:26)",
    )
    parser.add_argument("--slow-commit-seconds", type=float, default=0.0)
    parser.add_argument("--round-interval-seconds", type=float, default=5.0)
    parser.add_argument(
        "--auction-observed",
        default="09:29:00",
        help="local time of the auction_gap checks (later if the auction batch is)",
    )
    parser.add_argument("--no-auction", action="store_true", help="skip the auction-match copy")
    parser.add_argument(
        "--auction-sample",
        type=int,
        default=0,
        help="codes to run the reference lookups for (default 0 = every auction row, "
        "through one as_of_snapshot read)",
    )
    parser.add_argument(
        "--single-key-sample",
        type=int,
        default=50,
        help="of those, how many codes to also read the pre-#299 way (one as_of round trip "
        "per lookup) to time it and compare its answers (0 = skip)",
    )
    parser.add_argument(
        "--full-auction-assembly",
        action="store_true",
        help="also run one production round of the auction_gap input "
        "(load_live_auction_candidate_input: registry open + assembly) over the whole "
        "copied batch with synthetic prior-5 volumes, and time it",
    )
    parser.add_argument(
        "--production-registry-copy",
        action="store_true",
        help="also copy the live reference registry (read-only backup) and time its open "
        "and one snapshot over all codes -- the per-round cost at its real size",
    )
    arguments = parser.parse_args(argv)
    if arguments.slow_commit_seconds < 0 or arguments.round_interval_seconds <= 0:
        parser.error("seconds must be positive")
    if arguments.single_key_sample < 0:
        parser.error("--single-key-sample must not be negative")

    import rquant

    print(f"rquant imported from {Path(rquant.__file__).resolve().parent}")
    try:
        return run_rehearsal(
            runtime_root=arguments.runtime_root,
            rehearsal_root=arguments.rehearsal_root,
            sequence=arguments.sequence,
            publisher_start=arguments.publisher_start,
            slow_commit_seconds=arguments.slow_commit_seconds,
            round_interval_seconds=arguments.round_interval_seconds,
            auction=not arguments.no_auction,
            auction_observed=arguments.auction_observed,
            auction_sample=arguments.auction_sample,
            single_key_sample=arguments.single_key_sample,
            full_auction_assembly=arguments.full_auction_assembly,
            production_registry_copy=arguments.production_registry_copy,
        )
    except RehearsalRefusedError as error:
        print(f"REHEARSAL REFUSED: {error}")
        return 1
    except Exception as error:  # noqa: BLE001 - any refusal ends the rehearsal the same way
        print(f"REHEARSAL REFUSED: {type(error).__name__}: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
