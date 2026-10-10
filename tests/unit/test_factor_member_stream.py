"""Only actual complete member files can bind the existing research pipeline."""

from __future__ import annotations

import gc
import os
import weakref
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from rquant.factor.universe import FactorUniverseRequest, select_factor_universe
from tests.unit.test_factor_member_archive import (
    _CODES,
    _FIRST,
    _OBSERVED,
    _payload,
    _private,
    _request,
    _write,
)
from tests.unit.test_factor_stream_adapter import _pools, _prepared

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rquant.factor.member_archive import FactorMemberArchiveReference
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest


def _archive(
    tmp_path: Path, request: FactorStreamAdapterRequest, rows: Iterable[FactorUniverseRequest]
) -> tuple[Path, FactorMemberArchiveReference, FactorStreamAdapterRequest]:
    from rquant.factor.member_archive import (
        FactorMemberArchiveRequest,
        load_factor_member_archive,
        publish_factor_member_archive,
    )

    inputs, root = _private(tmp_path / "member-input"), _private(tmp_path / "members")
    names = []
    for i, row in enumerate(rows):
        name = f"day{i}.json"
        names.append(name)
        _write(
            inputs,
            name,
            {
                "schema_version": 1,
                "trade_date": row.trade_date.isoformat(),
                "securities": row.securities.model_dump(mode="json"),
                "membership": None
                if row.membership is None
                else row.membership.model_dump(mode="json"),
            },
        )
    ref = publish_factor_member_archive(
        FactorMemberArchiveRequest(
            selection=request.formula.selection,
            trading_days=request.formula.trading_days,
            as_of=request.formula.as_of,
            computation_stock_codes=request.formula.computation_stock_codes,
        ),
        input_root=inputs,
        daily_filenames=names,
        root=root,
    )
    manifest = load_factor_member_archive(root, ref)
    sources = request.formula.sources.model_copy(
        update=manifest.sources.model_dump(exclude={"source_mode"})
    )
    bound = type(request).model_validate(
        request.model_copy(
            update={
                "formula": request.formula.model_copy(update={"sources": sources}),
            }
        )
    )
    return root, ref, bound


def test_reader_releases_daily_models_and_completes_only_after_natural_tail(tmp_path: Path) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive
    from rquant.factor.member_stream import open_factor_member_stream

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    days = tuple(_FIRST + timedelta(days=i) for i in range(12))
    for i, day in enumerate(days):
        payload = _payload(day)
        if i == 2:
            for fact in payload["securities"]["facts"]:
                fact["is_st"] = True
        _write(inputs, f"day{i}.json", payload)
    reference = publish_factor_member_archive(
        _request(days),
        input_root=inputs,
        daily_filenames=(f"day{i}.json" for i in range(12)),
        root=root,
    )
    refs = []
    selected = []
    with open_factor_member_stream(root, reference) as stream:
        for row in stream:
            assert stream.completion is None
            selected.append(select_factor_universe(row).stock_codes)
            refs.extend((weakref.ref(row), weakref.ref(row.securities)))
        del row
        gc.collect()
        assert selected[2] == () and selected[0] == _CODES
        assert all(ref() is None for ref in refs)
        assert stream.completion.processed_days == 12
        stream.require_completion()
    assert stream.closed


@pytest.mark.parametrize("selection", ["all", "gem", "hs300", "zz1000"])
def test_files_drive_runner_and_decay_with_legacy_entry_golden_result(
    tmp_path: Path, selection: str
) -> None:
    from rquant.factor.member_stream import (
        open_factor_member_stream,
        run_factor_stream_research_from_members,
    )
    from rquant.factor.stream_runner import run_factor_stream_research_with_decay

    with _prepared(tmp_path, selection=selection) as (metadata, lake, request):
        codes, days = request.formula.computation_stock_codes, request.formula.trading_days
        members = {day: codes for day in days}
        if selection != "gem":
            members[days[1]], members[days[-1]] = codes[:10], ()
        root, reference, request = _archive(tmp_path, request, _pools(request, members))
        with open_factor_member_stream(root, reference) as stream:
            golden = run_factor_stream_research_with_decay(
                request, metadata_store=metadata, lake_root=lake, universe_requests=stream
            )
            assert stream.completion is not None
        result = run_factor_stream_research_from_members(
            request,
            member_root=root,
            member_archive=reference,
            metadata_store=metadata,
            lake_root=lake,
        )
        assert result.research == golden
        assert result.member_archive == reference
        assert result.member_completion.processed_days == len(days)
        assert result.member_completion.manifest.request.trading_days == days
        assert not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize("tail", ["missing", "corrupt", "cancel"])
def test_tail_failure_after_evaluations_cannot_publish_a_member_research_result(
    tmp_path: Path, tail: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import member_stream
    from rquant.factor.member_archive import load_factor_member_archive

    captured, descriptors = [], []
    original_init = member_stream.FactorMemberStream.__init__

    def initialized(stream: member_stream.FactorMemberStream, *args: object) -> None:
        original_init(stream, *args)
        captured.append(stream)
        descriptors.append(stream._root_fd)

    monkeypatch.setattr(member_stream.FactorMemberStream, "__init__", initialized)
    with _prepared(tmp_path) as (metadata, lake, request):
        root, reference, request = _archive(tmp_path, request, _pools(request))
        manifest = load_factor_member_archive(root, reference)
        last = manifest.days[-1].filename
        if tail == "missing":
            (root / last).unlink()
        elif tail == "corrupt":
            (root / last).write_bytes(b"{}")
        else:
            original_read = member_stream._read_file

            def cancelled(root_fd: int, name: str, *args: object) -> object:
                if name == last:
                    raise KeyboardInterrupt("synthetic member tail cancellation")
                return original_read(root_fd, name, *args)

            monkeypatch.setattr(member_stream, "_read_file", cancelled)
        exception = (
            KeyboardInterrupt
            if tail == "cancel"
            else (FileNotFoundError if tail == "missing" else ValueError)
        )
        with pytest.raises(exception):
            member_stream.run_factor_stream_research_from_members(
                request,
                member_root=root,
                member_archive=reference,
                metadata_store=metadata,
                lake_root=lake,
            )
        assert len(captured) == 1 and captured[0].closed and captured[0].completion is None
        for descriptor in descriptors:
            with pytest.raises(OSError):
                os.fstat(descriptor)
        assert not list((lake / ".execution_sessions").iterdir())
        print(
            f"MEMBER_TAIL_RESOURCES: tail={tail} completed=false "
            "fd_closed=true execution_copies_clean=true"
        )


def test_early_close_and_same_content_replacement_do_not_complete(tmp_path: Path) -> None:
    from rquant.factor.member_archive import (
        load_factor_member_archive,
        publish_factor_member_archive,
    )
    from rquant.factor.member_stream import open_factor_member_stream

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    days = (_FIRST, _FIRST + timedelta(days=1))
    for i, day in enumerate(days):
        _write(inputs, f"day{i}.json", _payload(day))
    reference = publish_factor_member_archive(
        _request(days), input_root=inputs, daily_filenames=("day0.json", "day1.json"), root=root
    )
    manifest = load_factor_member_archive(root, reference)
    with open_factor_member_stream(root, reference) as stream:
        next(stream)
    assert stream.completion is None
    with pytest.raises(ValueError, match="naturally complete"):
        stream.require_completion()
    with open_factor_member_stream(root, reference) as stream:
        next(stream)
        first = root / manifest.days[0].filename
        (root / "replacement.json").write_bytes(first.read_bytes())
        (root / "replacement.json").chmod(0o600)
        os.replace(root / "replacement.json", first)
        next(stream)
        with pytest.raises(ValueError, match="changed after read"):
            next(stream)
    assert stream.closed and stream.completion is None


def test_wrapper_rejects_unbound_real_archive_before_opening_raw_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import stream_snapshot
    from rquant.factor.member_stream import run_factor_stream_research_from_members

    with _prepared(tmp_path) as (metadata, lake, unbound):
        root, reference, _ = _archive(tmp_path, unbound, _pools(unbound))

        def forbidden(**kwargs: object) -> object:
            raise AssertionError("must reject member mismatch before creating raw session")

        monkeypatch.setattr(stream_snapshot, "ResearchExecutionSession", forbidden)
        with pytest.raises(ValueError, match="request bindings differ"):
            run_factor_stream_research_from_members(
                unbound,
                member_root=root,
                member_archive=reference,
                metadata_store=metadata,
                lake_root=lake,
            )


def test_result_roundtrips_strictly_and_rejects_an_incomplete_member_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pydantic import ValidationError

    from rquant.factor.member_stream import (
        FactorMemberResearchResult,
        FactorMemberStream,
        run_factor_stream_research_from_members,
    )
    from rquant.runtime_contracts import canonical_sha256

    with _prepared(tmp_path) as (metadata, lake, request):
        root, reference, request = _archive(tmp_path, request, _pools(request))
        iterations = []
        original_iter = FactorMemberStream.__iter__

        def iterated(stream: FactorMemberStream) -> FactorMemberStream:
            iterations.append(stream)
            return original_iter(stream)

        monkeypatch.setattr(FactorMemberStream, "__iter__", iterated)
        result = run_factor_stream_research_from_members(
            request,
            member_root=root,
            member_archive=reference,
            metadata_store=metadata,
            lake_root=lake,
        )
        assert FactorMemberResearchResult.model_validate_json(result.model_dump_json()) == result
        assert len(iterations) == 1 and iterations[0].closed
        with pytest.raises(ValidationError, match="frozen_instance"):
            result.sha256 = "0" * 64
        completion = result.member_completion.model_copy(update={"processed_days": 1})
        fields = result.model_dump(exclude={"sha256"})
        fields["member_completion"] = completion
        with pytest.raises(ValidationError, match="exact schedule"):
            FactorMemberResearchResult(**fields, sha256=canonical_sha256(fields))


def test_completed_reader_preserves_real_observation_and_rechecks_file_generation(
    tmp_path: Path,
) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive
    from rquant.factor.member_stream import open_factor_member_stream

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    _write(inputs, "day.json", _payload(_FIRST))
    reference = publish_factor_member_archive(
        _request((_FIRST,)), input_root=inputs, daily_filenames=("day.json",), root=root
    )
    with open_factor_member_stream(root, reference) as stream:
        descriptor = stream._root_fd
        row = next(stream)
        assert row.securities.observed_at == _OBSERVED
        assert row.securities.observed_at.date() > row.trade_date
        with pytest.raises(StopIteration):
            next(stream)
        assert stream.completion is not None
    with pytest.raises(OSError):
        os.fstat(descriptor)
    original = root / reference.filename
    (root / "replacement.json").write_bytes(original.read_bytes())
    (root / "replacement.json").chmod(0o600)
    os.replace(root / "replacement.json", original)
    with pytest.raises(ValueError, match="changed after read"):
        stream.require_completion()
    assert stream.completion is None
    print(
        "MEMBER_READER_RESOURCES: real_observed_at_preserved=true "
        "fd_closed=true post_read_replacement_refused=true"
    )
