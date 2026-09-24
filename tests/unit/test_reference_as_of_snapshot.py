"""#299: `ReferenceRegistry.as_of_snapshot` answers exactly as N single `as_of` calls do."""

from __future__ import annotations

import json
import math
import random
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.reference_data_registry as registry_module
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceAsOfSnapshot,
    ReferenceDataIntegrityError,
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferenceLookup,
    ReferencePublicationAuthenticator,
    ReferenceRecord,
    ReferenceRegistry,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)
DAY = timedelta(days=1)
HOUR = timedelta(hours=1)
KEYS = tuple(f"{index:06d}.SZ" for index in range(1, 13))
MISSING_KEY = "999999.SZ"
#: two populated datasets, one that exists but holds nothing, one no registry has heard of
POPULATED = (ReferenceDataset.ST_STATUS, ReferenceDataset.PRICE_LIMIT_REGIME)
QUERIED_DATASETS = (*POPULATED, ReferenceDataset.BOARD_MEMBERSHIP, "no_such_dataset")
EVENT_TIMES = (
    BASE - HOUR,
    BASE,
    BASE + 12 * HOUR,
    BASE + DAY,
    BASE + DAY + 12 * HOUR,
    BASE + 2 * DAY,
    BASE + 2 * DAY + 12 * HOUR,
    BASE + 3 * DAY + 12 * HOUR,
    BASE + 30 * DAY,
)
DECISION_TIMES = (
    BASE + 30 * timedelta(minutes=1),
    BASE + HOUR,
    BASE + DAY + 30 * timedelta(minutes=1),
    BASE + DAY + HOUR,
    BASE + 2 * DAY + HOUR,
    BASE + 3 * DAY + HOUR,
    BASE + 30 * DAY,
)


def _record(
    *,
    dataset_id: str,
    key: str,
    effective_from: datetime,
    effective_to: datetime | None,
    revision: int,
    first_available_at: datetime,
    tag: str,
) -> ReferenceRecord:
    return ReferenceRecord(
        dataset_id=dataset_id,
        key=key,
        effective_from=effective_from,
        effective_to=effective_to,
        revision=revision,
        source="test.bulk",
        first_available_at=first_available_at,
        replacement_reason=None if revision == 1 else f"correction {revision}",
        payload={"tag": tag},
    )


class _History:
    """Lineages appended so far, so every revision the generator writes is the next one."""

    def __init__(self) -> None:
        self.lineages: dict[tuple[str, str, datetime], tuple[int, datetime | None]] = {}

    def start(
        self,
        *,
        dataset_id: str,
        key: str,
        effective_from: datetime,
        effective_to: datetime | None,
        available: datetime,
        tag: str,
    ) -> ReferenceRecord:
        self.lineages[(dataset_id, key, effective_from)] = (1, effective_to)
        return _record(
            dataset_id=dataset_id,
            key=key,
            effective_from=effective_from,
            effective_to=effective_to,
            revision=1,
            first_available_at=available,
            tag=tag,
        )

    def correct(
        self,
        lineage: tuple[str, str, datetime],
        *,
        available: datetime,
        tag: str,
    ) -> ReferenceRecord:
        revision, effective_to = self.lineages[lineage]
        self.lineages[lineage] = (revision + 1, effective_to)
        dataset_id, key, effective_from = lineage
        return _record(
            dataset_id=dataset_id,
            key=key,
            effective_from=effective_from,
            effective_to=effective_to,
            revision=revision + 1,
            first_available_at=available,
            tag=tag,
        )

    def of_day(self, effective_from: datetime) -> list[tuple[str, str, datetime]]:
        return sorted(lineage for lineage in self.lineages if lineage[2] == effective_from)


def _generated_registry(path: Path, rng: random.Random) -> tuple[ReferenceRegistry, list[str]]:
    """Four generations: G1 -> G2 -> G3, then a rollback to G2 and G4 on top of it.

    Revisions correct earlier lineages in later generations, new lineages start in later
    generations, some keys never get a record in some datasets, one key appears only from
    G2, G3 is a sibling branch of G4 (its records are re-added to G4 as G4's own members),
    and the last lineage of each key is open-ended.
    """

    registry = ReferenceRegistry(path)
    history = _History()
    generations: list[str] = []

    first = [
        history.start(
            dataset_id=dataset,
            key=key,
            effective_from=BASE,
            effective_to=BASE + DAY,
            available=BASE + HOUR,
            tag=f"g1/{dataset}/{key}",
        )
        for key in KEYS[:-1]
        for dataset in POPULATED
        if rng.random() < 0.85
    ]
    generations.append(
        registry.append_many_and_publish(tuple(first), published_at=BASE + 2 * HOUR)[
            1
        ].generation_id
    )

    second = [
        history.correct(lineage, available=BASE + DAY + HOUR, tag=f"g2-fix/{lineage[1]}")
        for lineage in history.of_day(BASE)
        if rng.random() < 0.4
    ]
    second += [
        history.start(
            dataset_id=dataset,
            key=key,
            effective_from=BASE + DAY,
            effective_to=BASE + 2 * DAY,
            available=BASE + DAY + HOUR,
            tag=f"g2/{dataset}/{key}",
        )
        for key in KEYS
        for dataset in POPULATED
        if rng.random() < 0.8
    ]
    generations.append(
        registry.append_many_and_publish(tuple(second), published_at=BASE + DAY + 2 * HOUR)[
            1
        ].generation_id
    )

    third = [
        history.correct(lineage, available=BASE + 2 * DAY + HOUR, tag=f"g3-fix/{lineage[1]}")
        for lineage in history.of_day(BASE + DAY)
        if rng.random() < 0.3
    ]
    third += [
        history.start(
            dataset_id=dataset,
            key=key,
            effective_from=BASE + 2 * DAY,
            effective_to=None,
            available=BASE + 2 * DAY + HOUR,
            tag=f"g3/{dataset}/{key}",
        )
        for key in KEYS
        for dataset in POPULATED
        if rng.random() < 0.7
    ]
    generations.append(
        registry.append_many_and_publish(tuple(third), published_at=BASE + 2 * DAY + 2 * HOUR)[
            1
        ].generation_id
    )

    registry.rollback(generations[1], switched_at=BASE + 2 * DAY + 3 * HOUR)
    fourth = [
        history.correct(lineage, available=BASE + 3 * DAY + HOUR, tag=f"g4-fix/{lineage[1]}")
        for day in (BASE, BASE + DAY)
        for lineage in history.of_day(day)
        if rng.random() < 0.3
    ]
    generations.append(
        registry.append_many_and_publish(tuple(fourth), published_at=BASE + 3 * DAY + 2 * HOUR)[
            1
        ].generation_id
    )
    return registry, generations


@contextmanager
def _raw(path: Path) -> Iterator[sqlite3.Connection]:
    with closing(sqlite3.connect(path)) as connection:
        yield connection
        connection.commit()


def _tamper_per_key_integrity(path: Path, generation_id: str) -> tuple[str, str]:
    """One undecodable record and one overlapping pair, both inside G1 (every ancestry).

    Opening a registry validates both away, so this runs after the registries are open --
    exactly the state in which `as_of` itself raises per key.
    """

    with _raw(path) as connection:
        victim = connection.execute(
            "SELECT record_id, business_key FROM reference_record "
            "WHERE dataset_id = ? ORDER BY business_key LIMIT 1",
            (ReferenceDataset.PRICE_LIMIT_REGIME.value,),
        ).fetchone()
        connection.execute(
            "UPDATE reference_record SET payload_json = ? WHERE record_id = ?",
            (json.dumps({"tag": "tampered"}), victim[0]),
        )
        overlapped = connection.execute(
            "SELECT business_key FROM reference_record WHERE dataset_id = ? "
            "AND effective_from = ? ORDER BY business_key DESC LIMIT 1",
            (ReferenceDataset.ST_STATUS.value, registry_module._encode_time(BASE)),
        ).fetchone()[0]
        overlap = _record(
            dataset_id=ReferenceDataset.ST_STATUS,
            key=overlapped,
            effective_from=BASE + 6 * HOUR,
            effective_to=BASE + DAY + 6 * HOUR,
            revision=1,
            first_available_at=BASE + HOUR,
            tag="overlap",
        )
        registry_module.ReferenceRegistry._insert_records_in_connection(connection, (overlap,))
        connection.execute(
            "INSERT INTO reference_generation_member(generation_id, record_id) VALUES (?, ?)",
            (generation_id, overlap.record_id),
        )
    return str(victim[1]), overlapped


Outcome = tuple[str, object]


def _outcome(call: Callable[..., ReferenceLookup], **arguments: object) -> Outcome:
    try:
        return ("ok", call(**arguments))
    except (ReferenceDataUnavailableError, ReferenceDataIntegrityError) as exc:
        return (type(exc).__name__, str(exc))


def _compare_all(
    registry: ReferenceRegistry,
    generations: list[str | None],
    rng: random.Random,
    *,
    pairs_per_key: int = 6,
) -> dict[str, int]:
    tally: dict[str, int] = {}
    keys = (*KEYS, MISSING_KEY)
    for generation_id in generations:
        snapshot = registry.as_of_snapshot(
            dataset_ids=QUERIED_DATASETS,
            keys=keys,
            generation_id=generation_id,
        )
        for dataset_id in QUERIED_DATASETS:
            for key in keys:
                for _ in range(pairs_per_key):
                    event = rng.choice(EVENT_TIMES)
                    decision = rng.choice(DECISION_TIMES)
                    single = _outcome(
                        registry.as_of,
                        dataset_id=dataset_id,
                        key=key,
                        event_time=event,
                        decision_time=decision,
                        generation_id=generation_id,
                    )
                    bulk = _outcome(
                        snapshot.as_of,
                        dataset_id=dataset_id,
                        key=key,
                        event_time=event,
                        decision_time=decision,
                    )
                    assert bulk == single, (generation_id, dataset_id, key, event, decision)
                    label = single[0] if single[0] == "ok" else str(single[1])
                    tally[label] = tally.get(label, 0) + 1
                    if isinstance(single[1], ReferenceLookup) and single[1].record.revision > 1:
                        tally["ok_corrected"] = tally.get("ok_corrected", 0) + 1
    return tally


@pytest.mark.parametrize("seed", [299, 2026, 928])
def test_bulk_lookups_equal_single_lookups_across_revisions_lineage_and_branches(
    tmp_path: Path,
    seed: int,
) -> None:
    rng = random.Random(seed)
    writer, generations = _generated_registry(tmp_path / "reference.sqlite3", rng)
    readonly = ReadonlyReferenceRegistry(writer.path)
    undecodable, overlapping = _tamper_per_key_integrity(writer.path, generations[0])

    for registry in (writer, readonly):
        tally = _compare_all(registry, [*generations, None], rng)

        #: every branch of `as_of` was actually taken, so equality is not vacuous
        assert tally["ok"] > 100
        assert tally.get("ok_corrected", 0) > 0, tally
        for refusal in (
            "reference key is not present in generation",
            "reference value is not available at decision_time",
            "reference value is not effective at event_time",
        ):
            assert tally.get(refusal, 0) > 0, (refusal, tally)
    #: and the tampered keys refuse per key, not per snapshot
    for key, dataset_id, event, message in (
        (undecodable, ReferenceDataset.PRICE_LIMIT_REGIME, BASE + HOUR, "stored reference record"),
        (overlapping, ReferenceDataset.ST_STATUS, BASE + 12 * HOUR, "overlapping effective"),
    ):
        snapshot = readonly.as_of_snapshot(dataset_ids=POPULATED, keys=KEYS)
        with pytest.raises(ReferenceDataIntegrityError, match=message):
            snapshot.as_of(
                dataset_id=dataset_id,
                key=key,
                event_time=event,
                decision_time=BASE + 30 * DAY,
            )


def test_a_sibling_branch_and_a_later_generation_stay_out_of_an_older_snapshot(
    tmp_path: Path,
) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    history = _History()
    first = history.start(
        dataset_id=ReferenceDataset.ST_STATUS,
        key=KEYS[0],
        effective_from=BASE,
        effective_to=None,
        available=BASE + HOUR,
        tag="first",
    )
    g1 = registry.append_many_and_publish((first,), published_at=BASE + 2 * HOUR)[1]
    later_key = history.start(
        dataset_id=ReferenceDataset.ST_STATUS,
        key=KEYS[1],
        effective_from=BASE,
        effective_to=None,
        available=BASE + 3 * HOUR,
        tag="later",
    )
    g2 = registry.append_many_and_publish((later_key,), published_at=BASE + 4 * HOUR)[1]

    older = registry.as_of_snapshot(
        dataset_ids=(ReferenceDataset.ST_STATUS,),
        keys=KEYS[:2],
        generation_id=g1.generation_id,
    )
    with pytest.raises(ReferenceDataUnavailableError, match="not present"):
        older.as_of(
            dataset_id=ReferenceDataset.ST_STATUS,
            key=KEYS[1],
            event_time=BASE + DAY,
            decision_time=BASE + DAY,
        )
    current = registry.as_of_snapshot(dataset_ids=(ReferenceDataset.ST_STATUS,), keys=KEYS[:2])
    assert current.generation_id == g2.generation_id
    assert (
        current.as_of(
            dataset_id=ReferenceDataset.ST_STATUS,
            key=KEYS[1],
            event_time=BASE + DAY,
            decision_time=BASE + DAY,
        ).record
        == later_key
    )


def _authenticated_registry(path: Path) -> ReferenceRegistry:
    return ReferenceRegistry(
        path,
        publication_authenticator=ReferencePublicationAuthenticator(
            key_id="test-reference-v1",
            secret=b"reference-publication-test-secret-0001",
        ),
    )


def test_a_pending_publication_fails_the_snapshot_closed_like_every_single_lookup(
    tmp_path: Path,
) -> None:
    registry = _authenticated_registry(tmp_path / "reference.sqlite3")
    baseline = _History().start(
        dataset_id=ReferenceDataset.ST_STATUS,
        key=KEYS[0],
        effective_from=BASE,
        effective_to=None,
        available=BASE + HOUR,
        tag="baseline",
    )
    registry.append_many_and_publish((baseline,), published_at=BASE + 2 * HOUR)
    readonly = ReadonlyReferenceRegistry(registry.path)
    staged = _record(
        dataset_id=ReferenceDataset.ST_STATUS,
        key=KEYS[1],
        effective_from=BASE,
        effective_to=None,
        revision=1,
        first_available_at=BASE + 3 * HOUR,
        tag="staged",
    )
    _results, manifest, rollback = registry.append_many_and_publish_before(
        (staged,),
        published_at=BASE + 3 * HOUR,
        completion_clock=lambda: BASE + 3 * HOUR,
        not_after=BASE + 3 * HOUR + timedelta(seconds=1),
        retain_intent=True,
        publication_id="b" * 64,
        completion_receipt_path=tmp_path / "completion" / f"{'b' * 64}.json",
    )

    for reader in (registry, readonly):
        for generation_id in (None, manifest.generation_id):
            with pytest.raises(ReferenceDataUnavailableError, match="pending") as single:
                reader.as_of(
                    dataset_id=ReferenceDataset.ST_STATUS,
                    key=KEYS[0],
                    event_time=BASE + DAY,
                    decision_time=BASE + DAY,
                    generation_id=generation_id,
                )
            with pytest.raises(ReferenceDataUnavailableError, match="pending") as bulk:
                reader.as_of_snapshot(
                    dataset_ids=(ReferenceDataset.ST_STATUS,),
                    keys=KEYS[:2],
                    generation_id=generation_id,
                )
            assert str(bulk.value) == str(single.value)

    registry.compensate_publication(rollback)
    snapshot = readonly.as_of_snapshot(dataset_ids=(ReferenceDataset.ST_STATUS,), keys=KEYS[:2])
    assert (
        snapshot.as_of(
            dataset_id=ReferenceDataset.ST_STATUS,
            key=KEYS[0],
            event_time=BASE + DAY,
            decision_time=BASE + DAY,
        ).record
        == baseline
    )


def test_generation_level_refusals_match_the_single_lookup(tmp_path: Path) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    record = _History().start(
        dataset_id=ReferenceDataset.ST_STATUS,
        key=KEYS[0],
        effective_from=BASE,
        effective_to=None,
        available=BASE + HOUR,
        tag="only",
    )
    registry.append(record)

    def both(generation_id: str | None) -> tuple[Outcome, Outcome]:
        single = _outcome(
            registry.as_of,
            dataset_id=ReferenceDataset.ST_STATUS,
            key=KEYS[0],
            event_time=BASE + DAY,
            decision_time=BASE + DAY,
            generation_id=generation_id,
        )
        try:
            registry.as_of_snapshot(
                dataset_ids=(ReferenceDataset.ST_STATUS,),
                keys=KEYS[:1],
                generation_id=generation_id,
            )
            bulk: Outcome = ("ok", None)
        except (ReferenceDataUnavailableError, ReferenceDataIntegrityError) as exc:
            bulk = (type(exc).__name__, str(exc))
        return single, bulk

    #: nothing published yet: no current generation
    single, bulk = both(None)
    assert bulk == single
    assert single == ("ReferenceDataUnavailableError", "current reference generation is missing")
    #: a generation that does not exist
    single, bulk = both("f" * 64)
    assert bulk == single
    assert single[0] == "ReferenceDataUnavailableError" and "does not exist" in str(single[1])
    #: a manifest whose stored columns no longer match it
    published = registry.publish(published_at=BASE + 2 * HOUR)
    with _raw(registry.path) as connection:
        connection.execute(
            "UPDATE reference_generation SET row_count = row_count + 1 WHERE generation_id = ?",
            (published.generation_id,),
        )
    for generation_id in (None, published.generation_id):
        single, bulk = both(generation_id)
        assert (
            bulk
            == single
            == (
                "ReferenceDataIntegrityError",
                "generation manifest hash or columns mismatch",
            )
        )


def test_a_lookup_the_snapshot_was_not_read_for_is_a_caller_error(tmp_path: Path) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    registry.append(
        _History().start(
            dataset_id=ReferenceDataset.ST_STATUS,
            key=KEYS[0],
            effective_from=BASE,
            effective_to=None,
            available=BASE + HOUR,
            tag="only",
        )
    )
    registry.publish(published_at=BASE + 2 * HOUR)
    snapshot = registry.as_of_snapshot(dataset_ids=(ReferenceDataset.ST_STATUS,), keys=KEYS[:1])

    for dataset_id, key in (
        (ReferenceDataset.SUSPENSION_STATUS, KEYS[0]),
        (ReferenceDataset.ST_STATUS, KEYS[1]),
    ):
        with pytest.raises(LookupError, match="was not read"):
            snapshot.as_of(
                dataset_id=dataset_id,
                key=key,
                event_time=BASE + DAY,
                decision_time=BASE + DAY,
            )


class RegistryIo:
    """Connections a registry opened, and the statements run on them after connect."""

    def __init__(self) -> None:
        self.connections = 0
        self.statements: list[str] = []


def count_registry_io(
    monkeypatch: pytest.MonkeyPatch,
    registry_class: type[ReferenceRegistry],
) -> RegistryIo:
    """A CI-robust stand-in for wall time (#299): the old path opened one connection per
    lookup, so the count is what regresses, whatever the machine's speed."""

    io = RegistryIo()
    original = registry_class._connect

    @contextmanager
    def counted(self: ReferenceRegistry) -> Iterator[sqlite3.Connection]:
        with original(self) as connection:
            io.connections += 1
            connection.set_trace_callback(io.statements.append)
            try:
                yield connection
            finally:
                connection.set_trace_callback(None)

    monkeypatch.setattr(registry_class, "_connect", counted)
    return io


def test_one_snapshot_is_one_connection_and_chunked_key_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = tuple(f"{index:06d}.SH" for index in range(600_000, 601_203))
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    history = _History()
    records = tuple(
        history.start(
            dataset_id=dataset,
            key=key,
            effective_from=BASE,
            effective_to=None,
            available=BASE + HOUR,
            tag=f"{dataset}/{key}",
        )
        for key in keys
        for dataset in POPULATED
    )
    registry.append_many_and_publish(records, published_at=BASE + 2 * HOUR)
    readonly = ReadonlyReferenceRegistry(registry.path)
    io = count_registry_io(monkeypatch, ReadonlyReferenceRegistry)

    snapshot = readonly.as_of_snapshot(dataset_ids=POPULATED, keys=keys)

    assert isinstance(snapshot, ReferenceAsOfSnapshot)
    assert io.connections == 1
    chunk_queries = [statement for statement in io.statements if "business_key IN" in statement]
    assert len(chunk_queries) == len(POPULATED) * math.ceil(
        len(keys) / registry_module._AS_OF_SNAPSHOT_KEYS_PER_QUERY
    )
    #: every key -- including the last one of each chunk -- is served
    for key in keys:
        for dataset in POPULATED:
            assert (
                snapshot.as_of(
                    dataset_id=dataset,
                    key=key,
                    event_time=BASE + DAY,
                    decision_time=BASE + DAY,
                ).record.payload["tag"]
                == f"{dataset}/{key}"
            )
    assert io.connections == 1
