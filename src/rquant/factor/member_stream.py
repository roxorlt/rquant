"""One-pass member files with completion tied to the existing research layers."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.member_archive import (
    MAX_FACTOR_MEMBER_DAY_BYTES,
    FactorMemberArchiveManifest,
    FactorMemberArchiveReference,
    _bytes,
    _check_identities,
    _day_reference,
    _load_manifest,
    _parse_day,
    _read_file,
    _sha,
    _universe,
)
from rquant.factor.result_artifact import _open_private_root, _root_identity, _root_path
from rquant.factor.stream_adapter import FactorStreamAdapterRequest
from rquant.factor.stream_runner import (
    FactorStreamResearchWithDecayResult,
    run_factor_stream_research_with_decay,
)
from rquant.factor.time_series import MAX_TRADE_DAYS
from rquant.factor.universe import FactorUniverseRequest, Sha256, select_factor_universe
from rquant.research_snapshot import SnapshotMetadataStore
from rquant.runtime_contracts import canonical_sha256

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


def _history_sha(
    reference: FactorMemberArchiveReference,
    manifest: FactorMemberArchiveManifest,
    hashes: tuple[str, ...],
) -> str:
    history = canonical_sha256((reference.sha256, manifest.request_sha256))
    for day, universe_sha in zip(manifest.days, hashes, strict=True):
        history = canonical_sha256((history, day.sha256, universe_sha))
    return history


class FactorMemberStreamCompletion(BaseModel):
    model_config = _IMMUTABLE

    member_archive: FactorMemberArchiveReference
    manifest: FactorMemberArchiveManifest
    processed_days: int = Field(ge=1, le=MAX_TRADE_DAYS)
    universe_request_sha256s: tuple[Sha256, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    history_sha256: Sha256
    sha256: Sha256

    @model_validator(mode="after")
    def _complete_binding(self) -> FactorMemberStreamCompletion:
        data = _bytes(self.manifest)
        if _sha(data) != self.member_archive.sha256 or len(data) != self.member_archive.byte_count:
            raise ValueError("member completion manifest reference differs")
        if self.processed_days != len(self.manifest.days) or self.processed_days != len(
            self.universe_request_sha256s
        ):
            raise ValueError("member completion did not consume the exact schedule")
        if self.history_sha256 != _history_sha(
            self.member_archive, self.manifest, self.universe_request_sha256s
        ):
            raise ValueError("member completion history digest differs")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("member completion digest differs")
        return self


class FactorMemberStream(Iterator[FactorUniverseRequest]):
    """Retain one day's claims and at most 1,024 small file identities/hashes."""

    def __init__(self, root: Path, reference: FactorMemberArchiveReference) -> None:
        self.root = _root_path(root)
        self.reference = FactorMemberArchiveReference.model_validate(reference)
        self._root_fd: int | None = _open_private_root(self.root)
        try:
            self._root_identity = _root_identity(os.fstat(self._root_fd))
            self.manifest, identity = _load_manifest(self._root_fd, self.reference)
            self._identities = {self.reference.filename: identity}
            _check_identities(self.root, self._root_fd, self._identities)
        except BaseException:
            os.close(self._root_fd)
            self._root_fd = None
            raise
        self._completion: FactorMemberStreamCompletion | None = None
        self._closed = False
        self._iterator = self._iterate()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def completion(self) -> FactorMemberStreamCompletion | None:
        return self._completion

    def __iter__(self) -> FactorMemberStream:
        return self

    def __next__(self) -> FactorUniverseRequest:
        if self._closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self._completion = None
            self.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._iterator.close()
            finally:
                if self._root_fd is not None:
                    os.close(self._root_fd)
                    self._root_fd = None

    def require_completion(self) -> FactorMemberStreamCompletion:
        """Recheck the physical files after downstream completion, even after close."""
        if self._completion is None:
            raise ValueError("member stream did not naturally complete")
        descriptor = _open_private_root(self.root)
        try:
            if _root_identity(os.fstat(descriptor)) != self._root_identity:
                raise ValueError("member archive root changed after consumption")
            _check_identities(self.root, descriptor, self._identities)
            return FactorMemberStreamCompletion.model_validate(self._completion)
        except BaseException:
            self._completion = None
            raise
        finally:
            os.close(descriptor)

    def _iterate(self) -> Iterator[FactorUniverseRequest]:
        hashes: list[str] = []
        assert self._root_fd is not None
        for expected in self.manifest.days:
            data, identity = _read_file(
                self._root_fd, expected.filename, MAX_FACTOR_MEMBER_DAY_BYTES, expected.sha256
            )
            payload = _parse_day(data, stored=True)
            if _day_reference(payload, self.manifest.request) != expected:
                raise ValueError("member daily payload differs from manifest")
            universe = _universe(payload, self.manifest.request, self.manifest.sources)
            hashes.append(select_factor_universe(universe).input_sha256)
            self._identities[expected.filename] = identity
            del payload, data
            yield universe
            del universe
        _check_identities(self.root, self._root_fd, self._identities)
        fields = {
            "member_archive": self.reference,
            "manifest": self.manifest,
            "processed_days": len(hashes),
            "universe_request_sha256s": tuple(hashes),
            "history_sha256": _history_sha(self.reference, self.manifest, tuple(hashes)),
        }
        self._completion = FactorMemberStreamCompletion(**fields, sha256=canonical_sha256(fields))


@contextmanager
def open_factor_member_stream(
    root: Path, reference: FactorMemberArchiveReference
) -> Iterator[FactorMemberStream]:
    stream = FactorMemberStream(root, reference)
    try:
        yield stream
    finally:
        stream.close()


def _matching_request(
    manifest: FactorMemberArchiveManifest, request: FactorStreamAdapterRequest
) -> None:
    member, formula = manifest.request, request.formula
    if (
        member.selection != formula.selection
        or member.as_of != formula.as_of
        or member.trading_days != formula.trading_days
        or member.computation_stock_codes != formula.computation_stock_codes
        or member.computation_stock_codes != request.source.scope.stock_codes
        or manifest.sources.security_source_id != formula.sources.security_source_id
        or manifest.sources.security_source_sha256 != formula.sources.security_source_sha256
        or manifest.sources.index_source_id != formula.sources.index_source_id
        or manifest.sources.index_source_sha256 != formula.sources.index_source_sha256
    ):
        raise ValueError("member archive and research request bindings differ")


class FactorMemberResearchResult(BaseModel):
    model_config = _IMMUTABLE

    member_archive: FactorMemberArchiveReference
    member_completion: FactorMemberStreamCompletion
    research: FactorStreamResearchWithDecayResult
    sha256: Sha256

    @model_validator(mode="after")
    def _completed_bindings(self) -> FactorMemberResearchResult:
        if self.member_archive != self.member_completion.member_archive:
            raise ValueError("member research archive reference differs")
        _matching_request(self.member_completion.manifest, self.research.research.request)
        raw = self.research.research.adapter_completion
        if raw.processed_days != self.member_completion.processed_days:
            raise ValueError("member and raw completed schedules differ")
        counts = {
            day.trade_date: day.selected_count for day in self.member_completion.manifest.days
        }
        if any(day.selected_count != counts[day.window.decision_date] for day in raw.return_days):
            raise ValueError("member and evaluated selection counts differ")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("member research result digest differs")
        return self


def run_factor_stream_research_from_members(
    request: FactorStreamAdapterRequest,
    *,
    member_root: Path,
    member_archive: FactorMemberArchiveReference,
    metadata_store: SnapshotMetadataStore,
    lake_root: Path,
) -> FactorMemberResearchResult:
    """The existing single execution plus actual member-file exhaustion and receipt."""
    request = FactorStreamAdapterRequest.model_validate(request)
    with open_factor_member_stream(member_root, member_archive) as stream:
        _matching_request(stream.manifest, request)
        result = run_factor_stream_research_with_decay(
            request, metadata_store=metadata_store, lake_root=lake_root, universe_requests=stream
        )
        fields = {
            "member_archive": stream.reference,
            "member_completion": stream.require_completion(),
            "research": result,
        }
        return FactorMemberResearchResult(**fields, sha256=canonical_sha256(fields))
