"""Dynamic immutable authorities for default serving source readers."""

from __future__ import annotations

import fcntl
import hashlib
import os
import secrets
import stat
import threading
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, StrictStr, StringConstraints, ValidationError, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.runtime_serving_snapshot import SourceReadResult

Sha256 = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Clock = Callable[[], datetime]


class ServingSourceAuthorityUnavailableError(RuntimeError):
    """The current source generation is not available at the requested time."""


class ServingSourceAuthorityIntegrityError(RuntimeError):
    """The authority path, pointer, or immutable content is untrustworthy."""


class _StrictAuthorityModel(RuntimeContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )


class ServingSourceAuthorityDocument(_StrictAuthorityModel):
    """One immutable owner result bound to code and canonical content."""

    schema_version: Literal[1]
    producer_commit: CommitSha
    result: SourceReadResult
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_content_identity(self) -> Self:
        expected_generation = _result_generation_id(self.result)
        if self.result.generation_id != expected_generation:
            raise ValueError("result generation_id does not match canonical content")
        expected_content = canonical_sha256(
            self.model_dump(mode="python", exclude={"content_hash"})
        )
        if self.content_hash != expected_content:
            raise ValueError("content_hash does not match canonical document content")
        return self


class ServingSourceAuthorityPointer(_StrictAuthorityModel):
    """Atomic mutable pointer to one immutable authority generation."""

    schema_version: Literal[1]
    generation_id: Sha256
    file_sha256: Sha256
    published_at: datetime
    producer_commit: CommitSha
    dataset_id: StrictStr
    payload_kind: StrictStr
    publication_id: Sha256 | None = None
    previous_publication_id: Sha256 | None = None
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        try:
            normalized = normalize_aware_utc(self.published_at)
        except ValueError as exc:
            raise ValueError("published_at must be timezone-aware") from exc
        object.__setattr__(self, "published_at", normalized)
        if not self.dataset_id:
            raise ValueError("dataset_id must be non-empty")
        if not self.payload_kind:
            raise ValueError("payload_kind must be non-empty")
        if self.previous_publication_id is not None and self.publication_id is None:
            raise ValueError("previous publication requires a publication identity")
        if self.publication_id is not None:
            expected_publication = _pointer_publication_id(self)
            if self.publication_id != expected_publication:
                raise ValueError("publication_id does not match canonical pointer identity")
        excluded = {"content_hash"}
        if self.publication_id is None:
            excluded.update({"publication_id", "previous_publication_id"})
        expected_content = canonical_sha256(self.model_dump(mode="python", exclude=excluded))
        if self.content_hash != expected_content:
            raise ValueError("pointer content_hash does not match canonical content")
        return self


class ServingSourceAuthorityPublisher:
    """Single-owner atomic publisher for current plus retained generations."""

    def __init__(
        self,
        *,
        root: Path,
        producer_commit: str,
        dataset_id: str,
        payload_kind: str,
        clock: Clock | None = None,
        max_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.root = _validated_root(root)
        _require_digest(producer_commit, length=40, name="producer_commit")
        self.producer_commit = producer_commit
        self.dataset_id = _require_label(dataset_id, name="dataset_id")
        self.payload_kind = _require_label(payload_kind, name="payload_kind")
        self.clock = clock or (lambda: datetime.now(UTC))
        if not callable(self.clock):
            raise TypeError("clock must be callable")
        self.max_bytes = _require_max_bytes(max_bytes)

    def publish(self, result: SourceReadResult) -> ServingSourceAuthorityPointer:
        if not isinstance(result, SourceReadResult):
            raise TypeError("result must be SourceReadResult")
        result = SourceReadResult.model_validate(result)
        _validate_result_owner(
            result,
            expected_dataset_id=self.dataset_id,
            expected_payload_kind=self.payload_kind,
        )
        if result.generation_id != _result_generation_id(result):
            raise ServingSourceAuthorityIntegrityError(
                "result generation_id does not match canonical content"
            )

        document = _build_document(result, producer_commit=self.producer_commit)
        document_bytes = _canonical_json(document)
        if len(document_bytes) > self.max_bytes:
            raise ServingSourceAuthorityIntegrityError(
                "immutable generation exceeds configured size limit"
            )
        file_sha256 = hashlib.sha256(document_bytes).hexdigest()

        chain = _open_or_create_root(self.root)
        root_fd = chain[-1][0]
        generations_fd = -1
        publications_fd = -1
        lock_fd = -1
        generations_entry: DirectoryEntry | None = None
        publications_entry: DirectoryEntry | None = None
        try:
            generations_fd = _open_or_create_child_directory(root_fd, "generations")
            generations_entry = _directory_entry(root_fd, generations_fd, "generations")
            publications_fd = _open_or_create_child_directory(root_fd, "publications")
            publications_entry = _directory_entry(root_fd, publications_fd, "publications")
            lock_fd = _open_publish_lock(root_fd)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            _verify_publisher_directories(
                chain,
                root_fd=root_fd,
                generations_entry=generations_entry,
                publications_entry=publications_entry,
            )

            current = _load_current_for_publisher(
                root_fd=root_fd,
                generations_fd=generations_fd,
                dataset_id=self.dataset_id,
                payload_kind=self.payload_kind,
                max_bytes=self.max_bytes,
            )
            if current is not None:
                current_pointer, current_document, current_pointer_bytes = current
                if current_pointer.generation_id == result.generation_id:
                    if current_document == document:
                        if current_pointer_bytes != _canonical_json(current_pointer):
                            raise ServingSourceAuthorityIntegrityError(
                                "idempotent current pointer bytes conflict"
                            )
                        _archive_publication(
                            publications_fd,
                            pointer=current_pointer,
                            payload=current_pointer_bytes,
                            max_bytes=self.max_bytes,
                        )
                        _verify_existing_current_pointer(
                            chain,
                            root_fd=root_fd,
                            generations_entry=generations_entry,
                            publications_entry=publications_entry,
                            expected_payload=current_pointer_bytes,
                            max_bytes=self.max_bytes,
                        )
                        return current_pointer
                    if current_document.result != result:
                        raise ServingSourceAuthorityIntegrityError(
                            "idempotent authority publication conflicts with current content"
                        )
                    if current_document.producer_commit == self.producer_commit:
                        raise ServingSourceAuthorityIntegrityError(
                            "idempotent authority publication conflicts within one producer commit"
                        )

            _publish_immutable_generation(
                generations_fd,
                generation_id=result.generation_id,
                producer_commit=self.producer_commit,
                commit_bound=(
                    current is not None
                    and current[0].generation_id == result.generation_id
                    and current[1].producer_commit != self.producer_commit
                ),
                payload=document_bytes,
                max_bytes=self.max_bytes,
            )
            published_at = _read_clock(self.clock)
            if result.event_time > published_at or result.published_at > published_at:
                raise ServingSourceAuthorityIntegrityError(
                    "authority result contains future evidence"
                )
            pointer = _build_pointer(
                document=document,
                file_sha256=file_sha256,
                dataset_id=self.dataset_id,
                payload_kind=self.payload_kind,
                published_at=published_at,
                previous_publication_id=(
                    None
                    if current is None
                    else _archive_publication(
                        publications_fd,
                        pointer=current[0],
                        payload=current[2],
                        max_bytes=self.max_bytes,
                    )
                ),
            )
            pointer_bytes = _canonical_json(pointer)
            if len(pointer_bytes) > self.max_bytes:
                raise ServingSourceAuthorityIntegrityError(
                    "current pointer exceeds configured size limit"
                )
            if current is not None:
                _reject_publish_rollback(
                    current_pointer=current[0],
                    current_result=current[1].result,
                    next_pointer=pointer,
                    next_result=result,
                )
            assert pointer.publication_id is not None
            _publish_immutable_payload(
                publications_fd,
                identity=pointer.publication_id,
                payload=pointer_bytes,
                max_bytes=self.max_bytes,
                label="immutable publication",
            )
            _replace_current_pointer(
                root_fd,
                pointer_bytes,
                directory_chain=chain,
                generations_entry=generations_entry,
                publications_entry=publications_entry,
                max_bytes=self.max_bytes,
            )
            return pointer
        finally:
            if lock_fd >= 0:
                with suppress(OSError):
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                with suppress(OSError):
                    os.close(lock_fd)
            with suppress(OSError):
                if generations_fd >= 0:
                    os.close(generations_fd)
            with suppress(OSError):
                if publications_fd >= 0:
                    os.close(publications_fd)
            _close_directory_chain(chain)


class ServingSourceAuthorityReader:
    """Resolve the latest verified immutable generation at an explicit PIT cutoff."""

    def __init__(
        self,
        *,
        root: Path,
        expected_producer_commit: str,
        expected_dataset_id: str,
        expected_payload_kind: str,
        max_bytes: int = 8 * 1024 * 1024,
        history_scan_limit: int = 1_024,
        trusted_historical_producer_commits: tuple[str, ...] = (),
    ) -> None:
        self.root = _validated_root(root)
        _require_digest(
            expected_producer_commit,
            length=40,
            name="expected_producer_commit",
        )
        self.expected_producer_commit = expected_producer_commit
        self.expected_dataset_id = _require_label(
            expected_dataset_id,
            name="expected_dataset_id",
        )
        self.expected_payload_kind = _require_label(
            expected_payload_kind,
            name="expected_payload_kind",
        )
        self.max_bytes = _require_max_bytes(max_bytes)
        if (
            not isinstance(history_scan_limit, int)
            or isinstance(history_scan_limit, bool)
            or not 1 <= history_scan_limit <= 10_000
        ):
            raise ValueError("history_scan_limit must be an integer between 1 and 10000")
        self.history_scan_limit = history_scan_limit
        trusted: set[str] = {expected_producer_commit}
        for commit in trusted_historical_producer_commits:
            _require_digest(commit, length=40, name="trusted_historical_producer_commit")
            trusted.add(commit)
        self.trusted_historical_producer_commits = frozenset(trusted)
        self._watermark_lock = threading.Lock()
        self._last_observation: tuple[datetime, datetime, int, str] | None = None

    def __call__(self, as_of: datetime, /) -> SourceReadResult:
        observed_at = _normalize_as_of(as_of)
        try:
            chain = _open_existing_directory_chain(self.root)
        except FileNotFoundError as exc:
            raise ServingSourceAuthorityUnavailableError(
                "current authority is unavailable"
            ) from exc
        except OSError as exc:
            raise ServingSourceAuthorityIntegrityError(
                "authority root is unsafe or contains a symlink"
            ) from exc
        root_fd = chain[-1][0]
        publications_fd = -1
        publications_entry: DirectoryEntry | None = None
        try:
            pointer_bytes = _read_regular_file_at(
                root_fd,
                "current.json",
                max_bytes=self.max_bytes,
                label="current pointer",
                missing_unavailable=True,
            )
            assert pointer_bytes is not None
            pointer = _parse_pointer(pointer_bytes)
            _validate_pointer_owner(
                pointer,
                expected_producer_commit=self.expected_producer_commit,
                expected_dataset_id=self.expected_dataset_id,
                expected_payload_kind=self.expected_payload_kind,
            )
            try:
                generations_fd = _open_existing_child_directory(root_fd, "generations")
            except FileNotFoundError as exc:
                raise ServingSourceAuthorityIntegrityError(
                    "current pointer has a generation gap"
                ) from exc
            chain.append(_directory_entry(root_fd, generations_fd, "generations"))
            result = _read_pointer_result(
                generations_fd=generations_fd,
                pointer=pointer,
                expected_producer_commit=self.expected_producer_commit,
                expected_dataset_id=self.expected_dataset_id,
                expected_payload_kind=self.expected_payload_kind,
                max_bytes=self.max_bytes,
            )
            selected_pointer = pointer
            selected_result = result
            if not _pointer_visible_at(pointer, result, observed_at):
                previous_publication_id = pointer.previous_publication_id
                if previous_publication_id is None:
                    raise ServingSourceAuthorityUnavailableError(
                        "current authority is not yet available at as_of"
                    )
                try:
                    publications_fd = _open_existing_child_directory(root_fd, "publications")
                except FileNotFoundError as exc:
                    raise ServingSourceAuthorityIntegrityError(
                        "current pointer has a publication history gap"
                    ) from exc
                publications_entry = _directory_entry(
                    root_fd,
                    publications_fd,
                    "publications",
                )
                selected_pointer, selected_result = _read_historical_result(
                    publications_fd=publications_fd,
                    generations_fd=generations_fd,
                    starting_publication_id=previous_publication_id,
                    newer_pointer=pointer,
                    newer_result=result,
                    observed_at=observed_at,
                    trusted_producer_commits=self.trusted_historical_producer_commits,
                    expected_dataset_id=self.expected_dataset_id,
                    expected_payload_kind=self.expected_payload_kind,
                    max_bytes=self.max_bytes,
                    scan_limit=self.history_scan_limit,
                )

            current_after = _read_regular_file_at(
                root_fd,
                "current.json",
                max_bytes=self.max_bytes,
                label="current pointer",
                missing_unavailable=True,
            )
            if current_after != pointer_bytes:
                raise ServingSourceAuthorityIntegrityError(
                    "current pointer changed while serving generation"
                )
            _verify_directory_chain(chain)
            if publications_entry is not None:
                _verify_child_directory(root_fd, publications_entry)
            self._accept_monotonic(observed_at, selected_pointer, selected_result)
            return selected_result
        except ServingSourceAuthorityUnavailableError:
            raise
        except ServingSourceAuthorityIntegrityError:
            raise
        except FileNotFoundError as exc:
            raise ServingSourceAuthorityUnavailableError(
                "current authority is unavailable"
            ) from exc
        except OSError as exc:
            raise ServingSourceAuthorityIntegrityError(
                "authority path is unsafe or contains a symlink"
            ) from exc
        finally:
            with suppress(OSError):
                if publications_fd >= 0:
                    os.close(publications_fd)
            _close_directory_chain(chain)

    def _accept_monotonic(
        self,
        as_of: datetime,
        pointer: ServingSourceAuthorityPointer,
        result: SourceReadResult,
    ) -> None:
        observation = (as_of, pointer.published_at, result.sequence, pointer.generation_id)
        with self._watermark_lock:
            previous = self._last_observation
            if previous is not None:
                previous_as_of, previous_published_at, previous_sequence, previous_generation = (
                    previous
                )
                if as_of >= previous_as_of:
                    if pointer.published_at < previous_published_at:
                        raise ServingSourceAuthorityIntegrityError(
                            "current authority pointer rollback detected"
                        )
                    if result.sequence < previous_sequence:
                        raise ServingSourceAuthorityIntegrityError(
                            "current authority sequence rollback detected"
                        )
                    if (
                        result.sequence == previous_sequence
                        and pointer.generation_id != previous_generation
                    ):
                        raise ServingSourceAuthorityIntegrityError(
                            "current authority generation rollback detected"
                        )
                else:
                    return
            self._last_observation = observation


def _pointer_visible_at(
    pointer: ServingSourceAuthorityPointer,
    result: SourceReadResult,
    observed_at: datetime,
) -> bool:
    return (
        pointer.published_at <= observed_at
        and result.event_time <= observed_at
        and result.published_at <= observed_at
    )


def _read_pointer_result(
    *,
    generations_fd: int,
    pointer: ServingSourceAuthorityPointer,
    expected_producer_commit: str,
    expected_dataset_id: str,
    expected_payload_kind: str,
    max_bytes: int,
) -> SourceReadResult:
    document_bytes = _read_generation_bytes(
        generations_fd=generations_fd,
        pointer=pointer,
        max_bytes=max_bytes,
        gap_message="authority pointer has a generation gap",
        conflict_message="immutable generation file sha256 does not match authority pointer",
    )
    document = _parse_document(document_bytes)
    _validate_document_binding(
        pointer,
        document,
        expected_producer_commit=expected_producer_commit,
        expected_dataset_id=expected_dataset_id,
        expected_payload_kind=expected_payload_kind,
    )
    return document.result


def _read_historical_result(
    *,
    publications_fd: int,
    generations_fd: int,
    starting_publication_id: str,
    newer_pointer: ServingSourceAuthorityPointer,
    newer_result: SourceReadResult,
    observed_at: datetime,
    trusted_producer_commits: frozenset[str],
    expected_dataset_id: str,
    expected_payload_kind: str,
    max_bytes: int,
    scan_limit: int,
) -> tuple[ServingSourceAuthorityPointer, SourceReadResult]:
    publication_id: str | None = starting_publication_id
    visited: set[str] = set()
    previous_pointer = newer_pointer
    previous_result = newer_result
    best: tuple[ServingSourceAuthorityPointer, SourceReadResult] | None = None
    scanned = 0
    while publication_id is not None and scanned < scan_limit:
        if publication_id in visited:
            raise ServingSourceAuthorityIntegrityError(
                "immutable publication history contains a cycle"
            )
        visited.add(publication_id)
        try:
            pointer_bytes = _read_regular_file_at(
                publications_fd,
                f"{publication_id}.json",
                max_bytes=max_bytes,
                label="immutable publication",
                missing_unavailable=False,
            )
        except FileNotFoundError as exc:
            raise ServingSourceAuthorityIntegrityError(
                "immutable publication history has a gap"
            ) from exc
        assert pointer_bytes is not None
        try:
            pointer = _parse_pointer(pointer_bytes)
        except ServingSourceAuthorityIntegrityError as exc:
            raise ServingSourceAuthorityIntegrityError(
                "immutable publication pointer is invalid"
            ) from exc
        if _publication_archive_id(pointer) != publication_id:
            raise ServingSourceAuthorityIntegrityError(
                "immutable publication identity does not match content"
            )
        if pointer.producer_commit not in trusted_producer_commits:
            raise ServingSourceAuthorityIntegrityError(
                "historical publication producer_commit is not trusted"
            )
        _validate_pointer_owner(
            pointer,
            expected_producer_commit=pointer.producer_commit,
            expected_dataset_id=expected_dataset_id,
            expected_payload_kind=expected_payload_kind,
        )
        result = _read_pointer_result(
            generations_fd=generations_fd,
            pointer=pointer,
            expected_producer_commit=pointer.producer_commit,
            expected_dataset_id=expected_dataset_id,
            expected_payload_kind=expected_payload_kind,
            max_bytes=max_bytes,
        )
        if pointer.published_at > previous_pointer.published_at:
            raise ServingSourceAuthorityIntegrityError(
                "immutable publication time order is invalid"
            )
        if result.sequence > previous_result.sequence:
            raise ServingSourceAuthorityIntegrityError(
                "immutable publication sequence order is invalid"
            )
        if (
            result.sequence == previous_result.sequence
            and pointer.generation_id != previous_pointer.generation_id
        ):
            raise ServingSourceAuthorityIntegrityError(
                "ambiguous immutable publication generation at one sequence"
            )

        if _pointer_visible_at(pointer, result, observed_at):
            if best is None:
                best = (pointer, result)
            else:
                best_pointer, best_result = best
                candidate_key = (result.sequence, pointer.published_at)
                best_key = (best_result.sequence, best_pointer.published_at)
                if (
                    candidate_key == best_key
                    and pointer.generation_id != best_pointer.generation_id
                ):
                    raise ServingSourceAuthorityIntegrityError(
                        "ambiguous immutable publication selection"
                    )
                if candidate_key > best_key:
                    best = (pointer, result)
            if result.sequence < best[1].sequence:
                break

        previous_pointer = pointer
        previous_result = result
        publication_id = pointer.previous_publication_id
        scanned += 1

    if (
        publication_id is not None
        and scanned >= scan_limit
        and (best is None or previous_result.sequence >= best[1].sequence)
    ):
        raise ServingSourceAuthorityUnavailableError(
            "visible authority history exceeds configured scan limit"
        )
    if best is None:
        raise ServingSourceAuthorityUnavailableError(
            "current authority is not yet available at as_of"
        )
    return best


def _result_generation_id(result: SourceReadResult) -> str:
    return canonical_sha256(result.model_dump(mode="python", exclude={"generation_id"}))


def _build_document(
    result: SourceReadResult,
    *,
    producer_commit: str,
) -> ServingSourceAuthorityDocument:
    values: dict[str, object] = {
        "schema_version": 1,
        "producer_commit": producer_commit,
        "result": result,
    }
    values["content_hash"] = canonical_sha256(values)
    return ServingSourceAuthorityDocument.model_validate(values)


def _build_pointer(
    *,
    document: ServingSourceAuthorityDocument,
    file_sha256: str,
    dataset_id: str,
    payload_kind: str,
    published_at: datetime,
    previous_publication_id: str | None,
) -> ServingSourceAuthorityPointer:
    values: dict[str, object] = {
        "schema_version": 1,
        "generation_id": document.result.generation_id,
        "file_sha256": file_sha256,
        "published_at": published_at,
        "producer_commit": document.producer_commit,
        "dataset_id": dataset_id,
        "payload_kind": payload_kind,
        "previous_publication_id": previous_publication_id,
    }
    values["publication_id"] = canonical_sha256(
        {
            "contract": "serving-source-authority-publication/v1",
            **values,
        }
    )
    values["content_hash"] = canonical_sha256(values)
    return ServingSourceAuthorityPointer.model_validate(values)


def _pointer_publication_id(pointer: ServingSourceAuthorityPointer) -> str:
    values = pointer.model_dump(
        mode="python",
        exclude={"content_hash", "publication_id"},
    )
    return canonical_sha256(
        {
            "contract": "serving-source-authority-publication/v1",
            **values,
        }
    )


def _publication_archive_id(pointer: ServingSourceAuthorityPointer) -> str:
    if pointer.publication_id is not None:
        return pointer.publication_id
    return canonical_sha256(
        {
            "contract": "serving-source-authority-legacy-publication/v1",
            "pointer": pointer,
        }
    )


def _parse_pointer(payload: bytes) -> ServingSourceAuthorityPointer:
    try:
        return ServingSourceAuthorityPointer.model_validate_json(payload)
    except (ValidationError, ValueError) as exc:
        raise ServingSourceAuthorityIntegrityError("current pointer is invalid") from exc


def _parse_document(payload: bytes) -> ServingSourceAuthorityDocument:
    try:
        return ServingSourceAuthorityDocument.model_validate_json(payload)
    except (ValidationError, ValueError) as exc:
        raise ServingSourceAuthorityIntegrityError(
            "immutable generation document is invalid"
        ) from exc


def _canonical_json(model: RuntimeContractModel) -> bytes:
    import json

    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_result_owner(
    result: SourceReadResult,
    *,
    expected_dataset_id: str,
    expected_payload_kind: str,
) -> None:
    if result.dataset_id != expected_dataset_id:
        raise ServingSourceAuthorityIntegrityError(
            "result dataset_id does not match publisher owner"
        )
    if result.payload.payload_kind != expected_payload_kind:
        raise ServingSourceAuthorityIntegrityError(
            "result payload_kind does not match publisher owner"
        )


def _validate_pointer_owner(
    pointer: ServingSourceAuthorityPointer,
    *,
    expected_producer_commit: str,
    expected_dataset_id: str,
    expected_payload_kind: str,
) -> None:
    if pointer.producer_commit != expected_producer_commit:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer producer_commit does not match expected commit"
        )
    if pointer.dataset_id != expected_dataset_id:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer dataset_id does not match expected dataset"
        )
    if pointer.payload_kind != expected_payload_kind:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer payload_kind does not match expected payload"
        )


def _validate_document_binding(
    pointer: ServingSourceAuthorityPointer,
    document: ServingSourceAuthorityDocument,
    *,
    expected_producer_commit: str,
    expected_dataset_id: str,
    expected_payload_kind: str,
) -> None:
    if document.producer_commit != expected_producer_commit:
        raise ServingSourceAuthorityIntegrityError(
            "immutable generation producer_commit does not match expected commit"
        )
    result = document.result
    _validate_result_owner(
        result,
        expected_dataset_id=expected_dataset_id,
        expected_payload_kind=expected_payload_kind,
    )
    if pointer.generation_id != result.generation_id:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer generation_id does not match immutable document"
        )
    if pointer.producer_commit != document.producer_commit:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer producer_commit does not match immutable document"
        )
    if pointer.published_at < result.published_at:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer published_at precedes immutable result"
        )


def _reject_publish_rollback(
    *,
    current_pointer: ServingSourceAuthorityPointer,
    current_result: SourceReadResult,
    next_pointer: ServingSourceAuthorityPointer,
    next_result: SourceReadResult,
) -> None:
    if next_result.sequence < current_result.sequence:
        raise ServingSourceAuthorityIntegrityError("generation sequence rollback rejected")
    if next_result.sequence == current_result.sequence and (
        next_pointer.generation_id != current_pointer.generation_id or next_result != current_result
    ):
        raise ServingSourceAuthorityIntegrityError(
            "different generation at the current sequence is a rollback"
        )
    if next_pointer.published_at < current_pointer.published_at:
        raise ServingSourceAuthorityIntegrityError("generation publication rollback rejected")


def _load_current_for_publisher(
    *,
    root_fd: int,
    generations_fd: int,
    dataset_id: str,
    payload_kind: str,
    max_bytes: int,
) -> (
    tuple[
        ServingSourceAuthorityPointer,
        ServingSourceAuthorityDocument,
        bytes,
    ]
    | None
):
    pointer_bytes = _read_regular_file_at(
        root_fd,
        "current.json",
        max_bytes=max_bytes,
        label="current pointer",
        missing_unavailable=False,
        optional=True,
    )
    if pointer_bytes is None:
        return None
    pointer = _parse_pointer(pointer_bytes)
    _validate_pointer_owner(
        pointer,
        expected_producer_commit=pointer.producer_commit,
        expected_dataset_id=dataset_id,
        expected_payload_kind=payload_kind,
    )
    document_bytes = _read_generation_bytes(
        generations_fd=generations_fd,
        pointer=pointer,
        max_bytes=max_bytes,
        gap_message="current pointer has a generation gap",
        conflict_message="immutable generation file sha256 conflicts with current pointer",
    )
    document = _parse_document(document_bytes)
    _validate_document_binding(
        pointer,
        document,
        expected_producer_commit=pointer.producer_commit,
        expected_dataset_id=dataset_id,
        expected_payload_kind=payload_kind,
    )
    return pointer, document, pointer_bytes


def _read_clock(clock: Clock) -> datetime:
    try:
        return normalize_aware_utc(clock())
    except (TypeError, ValueError) as exc:
        raise ServingSourceAuthorityIntegrityError(
            "publisher clock must return an aware datetime"
        ) from exc


def _publish_immutable_generation(
    directory_fd: int,
    *,
    generation_id: str,
    producer_commit: str,
    commit_bound: bool,
    payload: bytes,
    max_bytes: int,
) -> None:
    _publish_immutable_payload(
        directory_fd,
        identity=(f"{producer_commit}-{generation_id}" if commit_bound else generation_id),
        payload=payload,
        max_bytes=max_bytes,
        label="immutable generation",
    )


def _read_generation_bytes(
    *,
    generations_fd: int,
    pointer: ServingSourceAuthorityPointer,
    max_bytes: int,
    gap_message: str,
    conflict_message: str,
) -> bytes:
    candidates = (
        f"{pointer.producer_commit}-{pointer.generation_id}.json",
        f"{pointer.generation_id}.json",
    )
    found = False
    for name in candidates:
        document_bytes = _read_regular_file_at(
            generations_fd,
            name,
            max_bytes=max_bytes,
            label="immutable generation",
            missing_unavailable=False,
            optional=True,
        )
        if document_bytes is None:
            continue
        found = True
        if hashlib.sha256(document_bytes).hexdigest() == pointer.file_sha256:
            return document_bytes
    if not found:
        raise ServingSourceAuthorityIntegrityError(gap_message)
    raise ServingSourceAuthorityIntegrityError(conflict_message)


def _archive_publication(
    directory_fd: int,
    *,
    pointer: ServingSourceAuthorityPointer,
    payload: bytes,
    max_bytes: int,
) -> str:
    publication_id = _publication_archive_id(pointer)
    _publish_immutable_payload(
        directory_fd,
        identity=publication_id,
        payload=payload,
        max_bytes=max_bytes,
        label="immutable publication",
    )
    return publication_id


def _publish_immutable_payload(
    directory_fd: int,
    *,
    identity: str,
    payload: bytes,
    max_bytes: int,
    label: str,
) -> None:
    name = f"{identity}.json"
    existing = _read_regular_file_at(
        directory_fd,
        name,
        max_bytes=max_bytes,
        label=label,
        missing_unavailable=False,
        optional=True,
    )
    if existing is not None:
        if existing != payload:
            raise ServingSourceAuthorityIntegrityError(
                f"{label} already exists with conflicting content"
            )
        return

    temporary = f".{identity}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=directory_fd,
        )
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(
                temporary,
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            existing = _read_regular_file_at(
                directory_fd,
                name,
                max_bytes=max_bytes,
                label=label,
                missing_unavailable=False,
            )
            if existing != payload:
                raise ServingSourceAuthorityIntegrityError(
                    f"{label} publication conflicted with existing content"
                ) from exc
        os.fsync(directory_fd)
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)
        with suppress(OSError):
            os.unlink(temporary, dir_fd=directory_fd)


def _replace_current_pointer(
    root_fd: int,
    payload: bytes,
    *,
    directory_chain: list[DirectoryEntry],
    generations_entry: DirectoryEntry,
    publications_entry: DirectoryEntry,
    max_bytes: int,
) -> None:
    temporary = f".current.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=root_fd,
        )
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        staged = os.fstat(descriptor)
        staged_at_path = os.stat(temporary, dir_fd=root_fd, follow_symlinks=False)
        if not _same_regular_file(staged, staged_at_path):
            raise ServingSourceAuthorityIntegrityError("staged current pointer identity is unsafe")
        _verify_publisher_directories(
            directory_chain,
            root_fd=root_fd,
            generations_entry=generations_entry,
            publications_entry=publications_entry,
        )
        os.rename(
            temporary,
            "current.json",
            src_dir_fd=root_fd,
            dst_dir_fd=root_fd,
        )
        os.fsync(root_fd)
        _verify_committed_current_pointer(
            root_fd,
            expected_identity=staged,
            expected_payload=payload,
            max_bytes=max_bytes,
        )
        _verify_publisher_directories(
            directory_chain,
            root_fd=root_fd,
            generations_entry=generations_entry,
            publications_entry=publications_entry,
        )
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)
        with suppress(OSError):
            os.unlink(temporary, dir_fd=root_fd)


def _verify_existing_current_pointer(
    directory_chain: list[DirectoryEntry],
    *,
    root_fd: int,
    generations_entry: DirectoryEntry,
    publications_entry: DirectoryEntry,
    expected_payload: bytes,
    max_bytes: int,
) -> None:
    _verify_publisher_directories(
        directory_chain,
        root_fd=root_fd,
        generations_entry=generations_entry,
        publications_entry=publications_entry,
    )
    observed = _read_regular_file_at(
        root_fd,
        "current.json",
        max_bytes=max_bytes,
        label="current pointer",
        missing_unavailable=False,
    )
    if observed != expected_payload:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer changed before publisher success"
        )
    assert observed is not None
    _parse_pointer(observed)
    _verify_publisher_directories(
        directory_chain,
        root_fd=root_fd,
        generations_entry=generations_entry,
        publications_entry=publications_entry,
    )


def _verify_committed_current_pointer(
    root_fd: int,
    *,
    expected_identity: os.stat_result,
    expected_payload: bytes,
    max_bytes: int,
) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            "current.json",
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=root_fd,
        )
        before = os.fstat(descriptor)
        at_path_before = os.stat("current.json", dir_fd=root_fd, follow_symlinks=False)
        if not _same_published_file_identity(expected_identity, before):
            raise ServingSourceAuthorityIntegrityError("current pointer commit identity changed")
        if not _same_regular_file(before, at_path_before):
            raise ServingSourceAuthorityIntegrityError("current pointer identity is unsafe")
        if before.st_size > max_bytes:
            raise ServingSourceAuthorityIntegrityError(
                "current pointer exceeds configured size limit"
            )
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        observed = b"".join(chunks)
        after = os.fstat(descriptor)
        at_path_after = os.stat("current.json", dir_fd=root_fd, follow_symlinks=False)
        if not _same_observation(before, after) or not _same_regular_file(after, at_path_after):
            raise ServingSourceAuthorityIntegrityError(
                "current pointer changed after atomic publish"
            )
        if observed != expected_payload:
            raise ServingSourceAuthorityIntegrityError(
                "current pointer content changed after atomic publish"
            )
        _parse_pointer(observed)
    except ServingSourceAuthorityIntegrityError:
        raise
    except OSError as exc:
        raise ServingSourceAuthorityIntegrityError(
            "current pointer path is unsafe after atomic publish"
        ) from exc
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)


DirectoryEntry = tuple[int, str | None, os.stat_result]


def _open_or_create_root(root: Path) -> list[DirectoryEntry]:
    parent_chain = _open_existing_directory_chain(root.parent)
    parent_fd = parent_chain[-1][0]
    try:
        root_fd = _open_existing_child_directory(parent_fd, root.name)
    except FileNotFoundError:
        os.mkdir(root.name, mode=0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
        root_fd = _open_existing_child_directory(parent_fd, root.name)
    parent_chain.append(_directory_entry(parent_fd, root_fd, root.name))
    return parent_chain


def _open_existing_directory_chain(path: Path) -> list[DirectoryEntry]:
    flags = _directory_open_flags()
    chain: list[DirectoryEntry] = []
    try:
        root_fd = os.open("/", flags)
        root_stat = os.fstat(root_fd)
        if not _is_trusted_directory(root_stat):
            raise ServingSourceAuthorityIntegrityError("filesystem root is unsafe")
        chain.append((root_fd, None, root_stat))
        for component in path.parts[1:]:
            parent_fd = chain[-1][0]
            child_fd = os.open(component, flags, dir_fd=parent_fd)
            chain.append(_directory_entry(parent_fd, child_fd, component))
        return chain
    except Exception:
        _close_directory_chain(chain)
        raise


def _open_existing_child_directory(parent_fd: int, name: str) -> int:
    return os.open(name, _directory_open_flags(), dir_fd=parent_fd)


def _open_or_create_child_directory(parent_fd: int, name: str) -> int:
    try:
        return _open_existing_child_directory(parent_fd, name)
    except FileNotFoundError:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
        return _open_existing_child_directory(parent_fd, name)


def _directory_entry(parent_fd: int, child_fd: int, name: str) -> DirectoryEntry:
    child = os.fstat(child_fd)
    at_path = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not _same_directory(child, at_path):
        os.close(child_fd)
        raise ServingSourceAuthorityIntegrityError("authority directory identity is unsafe")
    return child_fd, name, child


def _open_publish_lock(root_fd: int) -> int:
    descriptor = os.open(
        ".publish.lock",
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        0o600,
        dir_fd=root_fd,
    )
    opened = os.fstat(descriptor)
    at_path = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
    if not _same_regular_file(opened, at_path):
        os.close(descriptor)
        raise ServingSourceAuthorityIntegrityError("publisher lock identity is unsafe")
    return descriptor


def _read_regular_file_at(
    directory_fd: int,
    name: str,
    *,
    max_bytes: int,
    label: str,
    missing_unavailable: bool,
    optional: bool = False,
) -> bytes | None:
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
        before = os.fstat(descriptor)
        at_path_before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not _same_regular_file(before, at_path_before):
            raise ServingSourceAuthorityIntegrityError(f"{label} identity is unsafe")
        if before.st_size > max_bytes:
            raise ServingSourceAuthorityIntegrityError(f"{label} exceeds configured size limit")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise ServingSourceAuthorityIntegrityError(f"{label} exceeds configured size limit")
        after = os.fstat(descriptor)
        at_path_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not _same_observation(before, after) or not _same_regular_file(after, at_path_after):
            raise ServingSourceAuthorityIntegrityError(f"{label} changed while being read")
        return payload
    except FileNotFoundError as exc:
        if optional:
            return None
        if missing_unavailable:
            raise ServingSourceAuthorityUnavailableError(f"{label} is unavailable") from exc
        raise
    except ServingSourceAuthorityUnavailableError:
        raise
    except ServingSourceAuthorityIntegrityError:
        raise
    except OSError as exc:
        raise ServingSourceAuthorityIntegrityError(
            f"{label} path is unsafe or contains a symlink"
        ) from exc
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)


def _verify_directory_chain(chain: list[DirectoryEntry]) -> None:
    for index, (directory_fd, name, initial) in enumerate(chain):
        current = os.fstat(directory_fd)
        if not _same_directory_identity(initial, current):
            raise ServingSourceAuthorityIntegrityError(
                "authority directory changed while being read"
            )
        if index == 0:
            continue
        parent_fd = chain[index - 1][0]
        assert name is not None
        at_path = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not _same_directory(current, at_path):
            raise ServingSourceAuthorityIntegrityError(
                "authority directory changed while being read"
            )


def _verify_child_directory(parent_fd: int, entry: DirectoryEntry) -> None:
    directory_fd, name, initial = entry
    assert name is not None
    current = os.fstat(directory_fd)
    if not _same_directory_identity(initial, current):
        raise ServingSourceAuthorityIntegrityError("authority directory changed while being read")
    at_path = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not _same_directory(current, at_path):
        raise ServingSourceAuthorityIntegrityError("authority directory changed while being read")


def _verify_publisher_directories(
    directory_chain: list[DirectoryEntry],
    *,
    root_fd: int,
    generations_entry: DirectoryEntry,
    publications_entry: DirectoryEntry,
) -> None:
    _verify_directory_chain(directory_chain)
    _verify_child_directory(root_fd, generations_entry)
    _verify_child_directory(root_fd, publications_entry)


def _close_directory_chain(chain: list[DirectoryEntry]) -> None:
    seen: set[int] = set()
    for descriptor, _name, _initial in reversed(chain):
        if descriptor in seen:
            continue
        seen.add(descriptor)
        with suppress(OSError):
            os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("short write while publishing serving authority")
        offset += written


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )


def _validated_root(root: Path) -> Path:
    value = Path(root)
    if not value.is_absolute() or len(value.parts) < 2:
        raise ValueError("authority root must be an absolute non-root path")
    if any(component in {"", ".", ".."} for component in value.parts[1:]):
        raise ValueError("authority root contains an unsafe component")
    return value


def _normalize_as_of(as_of: datetime) -> datetime:
    if not isinstance(as_of, datetime):
        raise ServingSourceAuthorityUnavailableError("as_of must be a timezone-aware datetime")
    try:
        return normalize_aware_utc(as_of)
    except ValueError as exc:
        raise ServingSourceAuthorityUnavailableError("as_of must be timezone-aware") from exc


def _require_digest(value: str, *, length: int, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a lowercase hexadecimal digest")


def _require_label(value: str, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _require_max_bytes(value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("max_bytes must be a positive integer")
    return value


def _same_directory(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        _is_trusted_directory(first)
        and _is_trusted_directory(second)
        and _same_directory_identity(first, second)
    )


def _same_regular_file(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        _is_trusted_regular_file(first)
        and _is_trusted_regular_file(second)
        and first.st_nlink == 1
        and second.st_nlink == 1
        and _same_observation(first, second)
    )


def _same_published_file_identity(
    first: os.stat_result,
    second: os.stat_result,
) -> bool:
    return (
        _is_trusted_regular_file(first)
        and _is_trusted_regular_file(second)
        and (
            first.st_dev,
            first.st_ino,
            first.st_mode,
            first.st_uid,
            first.st_gid,
            first.st_nlink,
            first.st_size,
        )
        == (
            second.st_dev,
            second.st_ino,
            second.st_mode,
            second.st_uid,
            second.st_gid,
            second.st_nlink,
            second.st_size,
        )
        and first.st_nlink == 1
    )


def _same_directory_identity(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        _is_trusted_directory(first)
        and _is_trusted_directory(second)
        and (
            first.st_dev,
            first.st_ino,
            first.st_uid,
            first.st_gid,
            first.st_mode,
        )
        == (
            second.st_dev,
            second.st_ino,
            second.st_uid,
            second.st_gid,
            second.st_mode,
        )
    )


def _is_trusted_directory(observation: os.stat_result) -> bool:
    return stat.S_ISDIR(observation.st_mode) and _is_trusted_path_node(observation)


def _is_trusted_regular_file(observation: os.stat_result) -> bool:
    return stat.S_ISREG(observation.st_mode) and _is_trusted_path_node(observation)


def _is_trusted_path_node(observation: os.stat_result) -> bool:
    return observation.st_uid in {0, os.geteuid()} and not observation.st_mode & (
        stat.S_IWGRP | stat.S_IWOTH
    )


def _same_observation(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        first.st_dev,
        first.st_ino,
        first.st_mode,
        first.st_uid,
        first.st_gid,
        first.st_size,
        first.st_mtime_ns,
        first.st_ctime_ns,
    ) == (
        second.st_dev,
        second.st_ino,
        second.st_mode,
        second.st_uid,
        second.st_gid,
        second.st_size,
        second.st_mtime_ns,
        second.st_ctime_ns,
    )


__all__ = [
    "ServingSourceAuthorityDocument",
    "ServingSourceAuthorityIntegrityError",
    "ServingSourceAuthorityPointer",
    "ServingSourceAuthorityPublisher",
    "ServingSourceAuthorityReader",
    "ServingSourceAuthorityUnavailableError",
]
