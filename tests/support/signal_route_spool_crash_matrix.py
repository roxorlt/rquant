"""Synthetic in-memory durability model for the frozen route-spool crash matrix.

The model exists only so the frozen crash, orphan, retry and conflict contract in
``docs/architecture/production-interpreter-authority.md`` (L558-594) can be asserted as
state transitions. It never opens a file, never imports or exposes a publication
primitive, and is not reachable from ``rquant.runtime_service_main.build_builtin_registry``,
a builtin factory, a production builder or any runtime capability: it is a plain
test-support object built from record bytes the caller already verified.

Two dialects are expressed side by side, because the frozen v2 path and the frozen
later-writer contract disagree at two boundaries:

``frozen-v2-observed``
    what ``rquant.signal_route_spool._immutable_write_at`` actually does today. A retry
    always mints a fresh temporary; an existing byte-identical target is accepted
    silently and the records directory is *not* fsynced again; a byte conflict raises
    without recording any audit evidence. This dialect is observed and pinned, not
    endorsed as spec-compliant.

``v3-spec``
    what L582-588 freezes for the separately authorized later writer tranche. A retry
    may reuse a temporary only for byte-identical content; observing an existing
    byte-identical target withdraws durability evidence until the records directory is
    fsynced again; a byte conflict appends conflict audit evidence and rejects before
    any pointer temporary or visible pointer is created, replaced or fsynced.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass

FROZEN_LEGACY_DIALECT = "frozen-v2-observed"
CURRENT_SPEC_DIALECT = "v3-spec"
SPOOL_DIALECTS: tuple[str, ...] = (FROZEN_LEGACY_DIALECT, CURRENT_SPEC_DIALECT)

CRASH_POINTS: tuple[str, ...] = (
    "record-temporary-write",
    "record-temporary-fsync",
    "immutable-record-link",
    "records-directory-fsync",
    "pointer-temporary-write",
    "pointer-temporary-fsync",
    "pointer-replace",
    "root-directory-fsync",
)


class SyntheticSpoolRecoveryError(Exception):
    """A synthetic durability, publication or recovery rule rejected the transition."""


def record_name(sequence: int) -> str:
    """Mirror ``_SignalRouteSpoolPaths.record_name`` without importing the spool."""

    if type(sequence) is not int or sequence < 1:
        raise ValueError("record sequence must be a positive native integer")
    return f"{sequence:020d}.json"


@dataclass(frozen=True)
class SpoolRecordImage:
    """One durable record candidate: exact bytes plus its verified chain identity."""

    sequence: int
    payload: bytes
    previous_record_hash: str | None
    record_hash: str

    @property
    def name(self) -> str:
        return record_name(self.sequence)

    @property
    def content_digest(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()


@dataclass(frozen=True)
class SpoolPointerImage:
    """The family-neutral pointer content a publication makes visible."""

    first_sequence: int
    high_watermark: int
    last_record_hash: str | None


@dataclass(frozen=True)
class SpoolConflictAuditEntry:
    """Conflict audit evidence appended when one sequence sees differing bytes."""

    sequence: int
    existing_hash: str
    attempted_hash: str


@dataclass(frozen=True)
class SpoolRecoveryObservation:
    """One pointer recovery may legitimately observe, with what it may expose."""

    pointer: SpoolPointerImage | None
    verified_prefix: tuple[int, ...]
    orphan_sequences: tuple[int, ...]


@dataclass(frozen=True)
class SpoolRecoveryOutcome:
    observations: tuple[SpoolRecoveryObservation, ...]
    ignored_temporary_names: tuple[str, ...]
    durable_sequences: tuple[int, ...]
    conflict_audit: tuple[SpoolConflictAuditEntry, ...]

    @property
    def admissible_pointers(self) -> tuple[SpoolPointerImage | None, ...]:
        return tuple(observation.pointer for observation in self.observations)


class SyntheticSpoolDurabilityModel:
    """A persistence state machine for the eight frozen durability boundaries."""

    def __init__(self, *, dialect: str) -> None:
        if dialect not in SPOOL_DIALECTS:
            raise ValueError(f"unknown spool durability dialect: {dialect}")
        self.dialect = dialect
        self.temporaries: dict[str, bytes] = {}
        self.linked_names: dict[str, SpoolRecordImage] = {}
        self.records_dir_fsynced: set[str] = set()
        self.pointer_temp: SpoolPointerImage | None = None
        self.pointer_visible: SpoolPointerImage | None = None
        self.root_fsynced: bool = True
        self.conflict_audit: list[SpoolConflictAuditEntry] = []
        self._staged: dict[int, tuple[str, SpoolRecordImage]] = {}
        self._durable_temporaries: set[str] = set()
        self._replaced_pointer: SpoolPointerImage | None = None
        self._temporary_ordinal = 0

    def stage_record_temporary(self, image: SpoolRecordImage) -> str:
        """Crash point 1: a temporary record exists and is never authority."""

        temporary = self._next_temporary_name(image.name)
        self.temporaries[temporary] = image.payload
        self._staged[image.sequence] = (temporary, image)
        return temporary

    def fsync_record_temporary(self, sequence: int) -> None:
        """Crash point 2: the temporary content is durable and still not authority."""

        temporary, _ = self._require_staged(sequence)
        self._durable_temporaries.add(temporary)

    def retry_record_temporary(self, image: SpoolRecordImage) -> str:
        """Row 2 retry: only byte-identical content may be reused."""

        staged = self._staged.get(image.sequence)
        if staged is None:
            return self.stage_record_temporary(image)
        temporary, existing = staged
        if self.dialect == FROZEN_LEGACY_DIALECT:
            return self.stage_record_temporary(image)
        if existing.payload != image.payload:
            raise SyntheticSpoolRecoveryError(
                f"record temporary retry must reuse byte-identical content: {image.sequence}"
            )
        return temporary

    def link_record(self, sequence: int) -> None:
        """Crash point 3: one immutable sequence name wins; a byte conflict rejects."""

        temporary, image = self._require_staged(sequence)
        existing = self.linked_names.get(image.name)
        if existing is None:
            self.linked_names[image.name] = image
            self._consume_temporary(temporary)
            return
        if existing.payload != image.payload:
            entry = SpoolConflictAuditEntry(
                sequence=sequence,
                existing_hash=existing.content_digest,
                attempted_hash=image.content_digest,
            )
            if self.dialect == CURRENT_SPEC_DIALECT:
                self.conflict_audit.append(entry)
            self._consume_temporary(temporary)
            raise SyntheticSpoolRecoveryError(
                f"immutable routed-signal record changed: {image.name}"
            )
        if self.dialect == CURRENT_SPEC_DIALECT:
            self.records_dir_fsynced.discard(image.name)
        self._consume_temporary(temporary)

    def fsync_records_directory(self) -> None:
        """Crash point 4: only a fsynced immutable name may enter a complete prefix."""

        self.records_dir_fsynced.update(self.linked_names)

    def stage_pointer_temporary(self, pointer: SpoolPointerImage) -> None:
        """Crash point 5: a temporary pointer is never authority."""

        self._require_publishable_prefix(pointer)
        self.pointer_temp = pointer

    def fsync_pointer_temporary(self) -> None:
        """Crash point 6: the old pointer stays authoritative until replacement."""

        if self.pointer_temp is None:
            raise SyntheticSpoolRecoveryError("no pointer temporary exists to fsync")

    def replace_pointer(self) -> None:
        """Crash point 7: the visible pointer must name a complete verified prefix."""

        pointer = self.pointer_temp
        if pointer is None:
            raise SyntheticSpoolRecoveryError("no pointer temporary exists to replace")
        self._require_publishable_prefix(pointer)
        self._replaced_pointer = self.pointer_visible
        self.pointer_visible = pointer
        self.pointer_temp = None
        self.root_fsynced = False

    def fsync_root_directory(self) -> None:
        """Crash point 8: the replacement becomes the only admissible pointer."""

        self.root_fsynced = True
        self._replaced_pointer = None

    def recover(self) -> SpoolRecoveryOutcome:
        """Apply the frozen recovery rules to whatever state the crash left behind."""

        candidates: list[SpoolPointerImage | None] = []
        if not self.root_fsynced:
            candidates.append(self._replaced_pointer)
        candidates.append(self.pointer_visible)
        observations: list[SpoolRecoveryObservation] = []
        for pointer in candidates:
            if observations and observations[-1].pointer == pointer:
                continue
            prefix = self._verify_prefix(pointer)
            watermark = 0 if pointer is None else pointer.high_watermark
            orphans = tuple(
                sorted(
                    image.sequence
                    for image in self.linked_names.values()
                    if image.sequence > watermark
                )
            )
            observations.append(
                SpoolRecoveryObservation(
                    pointer=pointer,
                    verified_prefix=prefix,
                    orphan_sequences=orphans,
                )
            )
        durable = tuple(
            sorted(
                image.sequence
                for name, image in self.linked_names.items()
                if name in self.records_dir_fsynced
            )
        )
        return SpoolRecoveryOutcome(
            observations=tuple(observations),
            ignored_temporary_names=tuple(sorted(self.temporaries)),
            durable_sequences=durable,
            conflict_audit=tuple(self.conflict_audit),
        )

    def _next_temporary_name(self, name: str) -> str:
        self._temporary_ordinal += 1
        return f".{name}.{self._temporary_ordinal:032x}"

    def _require_staged(self, sequence: int) -> tuple[str, SpoolRecordImage]:
        staged = self._staged.get(sequence)
        if staged is None:
            raise SyntheticSpoolRecoveryError(f"no record temporary is staged: {sequence}")
        return staged

    def _consume_temporary(self, temporary: str) -> None:
        self.temporaries.pop(temporary, None)
        self._durable_temporaries.discard(temporary)

    def _require_publishable_prefix(self, pointer: SpoolPointerImage) -> None:
        sequences = self._verify_prefix(pointer)
        if self.dialect != CURRENT_SPEC_DIALECT:
            return
        missing = tuple(
            sequence
            for sequence in sequences
            if record_name(sequence) not in self.records_dir_fsynced
        )
        if missing:
            listed = ",".join(str(sequence) for sequence in missing)
            raise SyntheticSpoolRecoveryError(
                f"records directory must be fsynced before pointer publication: {listed}"
            )

    def _verify_prefix(self, pointer: SpoolPointerImage | None) -> tuple[int, ...]:
        if pointer is None:
            return ()
        if pointer.high_watermark < pointer.first_sequence:
            if pointer.last_record_hash is not None:
                raise SyntheticSpoolRecoveryError("an empty pointer cannot name a head hash")
            return ()
        sequences: list[int] = []
        previous_hash: str | None = None
        for sequence in range(pointer.first_sequence, pointer.high_watermark + 1):
            image = self.linked_names.get(record_name(sequence))
            if image is None:
                raise SyntheticSpoolRecoveryError(
                    f"routed-signal sequence is missing: {sequence}"
                )
            if image.sequence != sequence:
                raise SyntheticSpoolRecoveryError(f"routed-signal sequence gap at {sequence}")
            if image.previous_record_hash != previous_hash:
                raise SyntheticSpoolRecoveryError(
                    f"routed-signal hash chain mismatch at {sequence}"
                )
            previous_hash = image.record_hash
            sequences.append(sequence)
        if pointer.last_record_hash != previous_hash:
            raise SyntheticSpoolRecoveryError("route spool pointer head hash mismatch")
        return tuple(sequences)


def drive_publication(
    model: SyntheticSpoolDurabilityModel,
    *,
    images: Sequence[SpoolRecordImage],
    pointer: SpoolPointerImage,
    crash_point: str | None = None,
) -> None:
    """Run the frozen publication order and stop right after ``crash_point``."""

    if crash_point is not None and crash_point not in CRASH_POINTS:
        raise ValueError(f"unknown crash point: {crash_point}")
    stop = len(CRASH_POINTS) if crash_point is None else CRASH_POINTS.index(crash_point)
    last = len(images) - 1
    for index, image in enumerate(images):
        limit = stop if index == last else len(CRASH_POINTS)
        model.stage_record_temporary(image)
        if limit == 0:
            return
        model.fsync_record_temporary(image.sequence)
        if limit == 1:
            return
        model.link_record(image.sequence)
        if limit == 2:
            return
        model.fsync_records_directory()
        if limit == 3:
            return
    if stop < 4:
        return
    model.stage_pointer_temporary(pointer)
    if stop == 4:
        return
    model.fsync_pointer_temporary()
    if stop == 5:
        return
    model.replace_pointer()
    if stop == 6:
        return
    model.fsync_root_directory()
