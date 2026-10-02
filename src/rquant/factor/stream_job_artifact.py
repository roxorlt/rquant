"""Bounded journal and compact artifacts for independently checked v2 statistics.

Verification recomputes statistics from journal facts and actual archived member
files. It does not independently prove raw data, formula evaluation, or provider
coverage. No lease capability or validation wall clock enters artifact content.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from math import fsum
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from rquant.factor.daily_stream import (
    FactorDailyStreamBatch,
    FactorDailyStreamCoverage,
    FactorDailyStreamGrouping,
    evaluate_factor_daily_stream,
)
from rquant.factor.decay_stream import FactorICDecayStream, FactorICDecayStreamRequest
from rquant.factor.definition import FactorDefinition
from rquant.factor.display_artifact import FactorDisplayDecayPeriod, FactorDisplayICPoint
from rquant.factor.extended_statistics import FactorExtendedStatistics
from rquant.factor.member_archive import _bytes, _check_identities, _publish_bytes, _read_file, _sha
from rquant.factor.member_stream import (
    FactorMemberResearchResult,
    _matching_request,
    open_factor_member_stream,
)
from rquant.factor.neutralization_context import (
    FactorNeutralizationSources,
    require_factor_neutralization_binding,
)
from rquant.factor.result import ResearchPortfolioStatus, ResearchSummaryStatus
from rquant.factor.result_artifact import _file_identity, _open_private_root, _root_path
from rquant.factor.run_request import NeutralizationMode
from rquant.factor.stream_adapter import factor_stream_statistics_request
from rquant.factor.stream_job_spec import FactorStreamJobSpec
from rquant.factor.summary import FactorICSummary
from rquant.factor.universe import Sha256, UniverseSelection, select_factor_universe
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import strict_canonical_json_loads

MAX_STREAM_JOURNAL_DAY_BYTES = 16 * 1024 * 1024
MAX_STREAM_JOURNAL_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_STREAM_FULL_BYTES = 16 * 1024 * 1024
MAX_STREAM_DISPLAY_BYTES = 4 * 1024 * 1024
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_Identity = tuple[int, ...]


class FactorStreamArtifactReference(BaseModel):
    model_config = _IMMUTABLE
    kind: Literal["journal-day", "journal", "full", "display"]
    sha256: Sha256
    filename: str
    byte_count: int = Field(gt=0, strict=True)

    @model_validator(mode="after")
    def _name_and_budget(self) -> FactorStreamArtifactReference:
        limits = {
            "journal-day": MAX_STREAM_JOURNAL_DAY_BYTES,
            "journal": MAX_STREAM_JOURNAL_MANIFEST_BYTES,
            "full": MAX_STREAM_FULL_BYTES,
            "display": MAX_STREAM_DISPLAY_BYTES,
        }
        if (
            self.filename != f"factor-stream-{self.kind}-v2-{self.sha256}.json"
            or self.byte_count > limits[self.kind]
        ):
            raise ValueError("stream artifact name or byte budget differs")
        return self


class FactorStreamJournalDay(BaseModel):
    model_config = _IMMUTABLE
    trade_date: date
    artifact: FactorStreamArtifactReference
    batch_sha256: Sha256

    @model_validator(mode="after")
    def _kind(self) -> FactorStreamJournalDay:
        if self.artifact.kind != "journal-day":
            raise ValueError("journal day requires its own artifact kind")
        return self


class FactorStreamJournalManifest(BaseModel):
    model_config = _IMMUTABLE
    schema_version: Literal[2] = 2
    spec_sha256: Sha256
    result_sha256: Sha256
    days: tuple[FactorStreamJournalDay, ...] = Field(min_length=1, max_length=1024)
    content_sha256: Sha256

    @model_validator(mode="after")
    def _digest(self) -> FactorStreamJournalManifest:
        if self.content_sha256 != _sha(_bytes_payload(self)):
            raise ValueError("journal manifest digest differs")
        return self

    @property
    def processed_days(self) -> int:
        return len(self.days)


class FactorStreamFullArtifact(BaseModel):
    model_config = _IMMUTABLE
    schema_version: Literal[2] = 2
    spec: FactorStreamJobSpec
    result: FactorMemberResearchResult
    journal: FactorStreamJournalManifest
    journal_reference: FactorStreamArtifactReference
    content_sha256: Sha256

    @model_validator(mode="after")
    def _binding(self) -> FactorStreamFullArtifact:
        stats = self.result.research.research.statistics
        if (
            self.result.member_archive != self.spec.member_archive
            or self.result.research.research.request != self.spec.adapter_request
            or self.journal.spec_sha256 != self.spec.spec_sha256
            or self.journal.result_sha256 != self.result.sha256
            or tuple(day.trade_date for day in self.journal.days)
            != self.spec.adapter_request.evaluation_days
            or tuple(day.batch_sha256 for day in self.journal.days) != stats.batch_sha256s
            or self.journal_reference != _reference("journal", _bytes(self.journal))
        ):
            raise ValueError("full artifact completion or journal bindings differ")
        if self.content_sha256 != _sha(_bytes_payload(self)):
            raise ValueError("full artifact digest differs")
        return self


class FactorStreamDisplayPortfolioDay(BaseModel):
    model_config = _IMMUTABLE
    decision_date: date
    groupings: tuple[FactorDailyStreamGrouping, ...] = Field(min_length=3, max_length=3)


class FactorStreamDisplayCoverageDay(BaseModel):
    model_config = _IMMUTABLE
    decision_date: date
    status: Literal["complete", "partial", "no_samples"]
    coverage: FactorDailyStreamCoverage


class FactorStreamDisplayArtifact(BaseModel):
    """Scalar diagnostics only; groups contain no synthetic dense holdings."""

    model_config = _IMMUTABLE
    schema_version: Literal[2] = 2
    full_artifact_sha256: Sha256
    result_sha256: Sha256
    input_sha256: Sha256
    definition: FactorDefinition
    definition_content_sha256: Sha256
    factor_id: str
    factor_version: int
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_sha256: Sha256
    snapshot_id: Sha256
    binding_hash: Sha256
    snapshot_as_of_time: AwareDatetime
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"
    visibility_basis: Literal["retrospective_adapter_assumption"] = (
        "retrospective_adapter_assumption"
    )
    return_price_basis: Literal["forward_adjusted"] = "forward_adjusted"
    holding_sessions: Literal[1, 5, 10, 20]
    as_of: AwareDatetime
    selection: UniverseSelection
    neutralization: NeutralizationMode = Field(default="none", exclude_if=lambda v: v == "none")
    context: FactorNeutralizationSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    mad_multiple: float | None = Field(
        default=None, strict=True, gt=0, allow_inf_nan=False, exclude_if=lambda v: v is None
    )
    extended_statistics: FactorExtendedStatistics | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    pool_label: Literal["全市场（沪深非 ST）", "创业板与科创板", "沪深300", "中证1000"]
    summary_status: ResearchSummaryStatus
    ic_summary: FactorICSummary
    ic_cumulative_kind: Literal["sum_of_valid_daily_ic"] = "sum_of_valid_daily_ic"
    ic_points: tuple[FactorDisplayICPoint, ...] = Field(min_length=1, max_length=1024)
    decay_periods: tuple[FactorDisplayDecayPeriod, ...] = Field(min_length=10, max_length=10)
    portfolio_status: ResearchPortfolioStatus
    portfolio_days: tuple[FactorStreamDisplayPortfolioDay, ...] = Field(
        min_length=1, max_length=1024
    )
    coverage_days: tuple[FactorStreamDisplayCoverageDay, ...] = Field(min_length=1, max_length=1024)
    content_sha256: Sha256

    @model_validator(mode="after")
    def _digest(self) -> FactorStreamDisplayArtifact:
        require_factor_neutralization_binding(
            self.context,
            mode=self.neutralization,
            snapshot_id=self.snapshot_id,
            binding_hash=self.binding_hash,
            as_of=self.snapshot_as_of_time,
        )
        if self.extended_statistics is not None and (
            tuple(p.trade_date for p in self.extended_statistics.autocorrelation_points)
            != tuple(day.decision_date for day in self.coverage_days)
            or (self.extended_statistics.industry_status == "available")
            != (self.context is not None and self.context.industry is not None)
        ):
            raise ValueError("display diagnostics dates or industry binding differ")
        if self.content_sha256 != _sha(_bytes_payload(self)):
            raise ValueError("stream display digest differs")
        return self


def _bytes_payload(model: BaseModel) -> bytes:
    from rquant.strict_json import canonical_json_bytes

    return canonical_json_bytes(
        model.model_dump(mode="json", round_trip=True, exclude={"content_sha256"})
    )


def _reference(kind: str, data: bytes) -> FactorStreamArtifactReference:
    digest = _sha(data)
    return FactorStreamArtifactReference(
        kind=kind,
        sha256=digest,
        filename=f"factor-stream-{kind}-v2-{digest}.json",
        byte_count=len(data),
    )


def _with_digest(model: type[BaseModel], fields: dict[str, object]) -> BaseModel:
    from rquant.strict_json import canonical_json_bytes

    temporary = model.model_construct(**fields)
    digest = _sha(
        canonical_json_bytes(
            temporary.model_dump(mode="json", round_trip=True, exclude={"content_sha256"})
        )
    )
    return model(**fields, content_sha256=digest)


def publish_stream_artifact(
    root: Path, kind: str, model: BaseModel
) -> FactorStreamArtifactReference:
    data = _bytes(model)
    reference = _reference(kind, data)
    descriptor = _open_private_root(root)
    try:
        _publish_bytes(root, descriptor, reference.filename, data, reference.byte_count)
        return reference
    finally:
        os.close(descriptor)


class FactorStreamJournalWriter:
    """Observe processed batches; only a completed member research publishes a manifest."""

    def __init__(self, root: Path, spec: FactorStreamJobSpec) -> None:
        self.root, self.spec = _root_path(root), spec
        self._fd: int | None = _open_private_root(self.root)
        self._identities: dict[str, _Identity] = {}
        self._days: list[FactorStreamJournalDay] = []

    def consume(self, batch: FactorDailyStreamBatch) -> None:
        if self._fd is None or len(self._days) >= len(self.spec.adapter_request.evaluation_days):
            raise ValueError("journal observer is closed or received an extra batch")
        expected = self.spec.adapter_request.evaluation_days[len(self._days)]
        if batch.universe.trade_date != expected:
            raise ValueError("journal batch differs from evaluation schedule")
        data = _bytes(batch)
        reference = _reference("journal-day", data)
        identity = _publish_bytes(
            self.root, self._fd, reference.filename, data, MAX_STREAM_JOURNAL_DAY_BYTES
        )
        self._identities[reference.filename] = identity
        self._days.append(
            FactorStreamJournalDay(
                trade_date=expected, artifact=reference, batch_sha256=canonical_sha256(batch)
            )
        )

    def finish(
        self, result: FactorMemberResearchResult
    ) -> tuple[FactorStreamJournalManifest, FactorStreamArtifactReference]:
        if self._fd is None or len(self._days) != len(self.spec.adapter_request.evaluation_days):
            raise ValueError("journal did not complete")
        if (
            tuple(day.batch_sha256 for day in self._days)
            != result.research.research.statistics.batch_sha256s
        ):
            raise ValueError("journal differs from processed statistics")
        _check_identities(self.root, self._fd, self._identities)
        manifest = _with_digest(
            FactorStreamJournalManifest,
            {
                "spec_sha256": self.spec.spec_sha256,
                "result_sha256": result.sha256,
                "days": tuple(self._days),
            },
        )
        return manifest, publish_stream_artifact(self.root, "journal", manifest)

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        self._days.clear()
        self._identities.clear()


def project_factor_stream_display(full: FactorStreamFullArtifact) -> FactorStreamDisplayArtifact:
    request, result = full.spec.adapter_request, full.result
    stats, decay = result.research.research.statistics, result.research.decay
    normal, rank, points = [], [], []
    for day in stats.days:
        n, r = day.evaluation.normal_ic, day.evaluation.rank_ic
        if n.value is not None:
            normal.append(n.value)
        if r.value is not None:
            rank.append(r.value)
        points.append(
            FactorDisplayICPoint(
                decision_date=day.trade_date,
                normal_ic=n,
                rank_ic=r,
                normal_ic_cumulative_sum=fsum(normal) if n.value is not None else None,
                rank_ic_cumulative_sum=fsum(rank) if r.value is not None else None,
            )
        )
    status = "evaluated" if any(day.coverage.valid_count for day in stats.days) else "no_samples"
    groups = [
        group.status == "ok" and group.cumulative_status == "available"
        for day in stats.days
        for group in day.portfolio_groupings
    ]
    any_group = any(group.status == "ok" for day in stats.days for group in day.portfolio_groupings)
    portfolio = (
        "available" if all(groups) else ("available_partial" if any_group else "insufficient_data")
    )
    definition = request.formula.definition
    labels = {
        "all": "全市场（沪深非 ST）",
        "gem": "创业板与科创板",
        "hs300": "沪深300",
        "zz1000": "中证1000",
    }
    fields = dict(
        full_artifact_sha256=_sha(_bytes(full)),
        result_sha256=result.sha256,
        input_sha256=stats.input_sha256,
        definition=definition,
        definition_content_sha256=full.spec.definition_content_sha256,
        factor_id=definition.factor_id,
        factor_version=definition.version,
        code_revision=full.spec.code_revision,
        source_sha256=result.research.research.adapter_completion.sha256,
        snapshot_id=request.source.snapshot_id,
        binding_hash=request.source.binding_hash,
        snapshot_as_of_time=request.source.scope.as_of_time,
        holding_sessions=request.holding_sessions,
        as_of=request.formula.as_of,
        selection=request.formula.selection,
        neutralization=request.formula.neutralization,
        context=request.formula.sources.context,
        mad_multiple=request.formula.mad_multiple,
        extended_statistics=stats.extended_statistics,
        pool_label=labels[request.formula.selection],
        summary_status=status,
        ic_summary=stats.ic_summary,
        ic_points=tuple(points),
        decay_periods=tuple(
            FactorDisplayDecayPeriod(
                lag=p.lag,
                status=p.status,
                source_day_count=p.source_day_count,
                valid_pair_count=p.valid_pair_count,
                ic_summary=p.ic_summary,
            )
            for p in decay.periods
        ),
        portfolio_status=portfolio,
        portfolio_days=tuple(
            FactorStreamDisplayPortfolioDay(
                decision_date=day.trade_date, groupings=day.portfolio_groupings
            )
            for day in stats.days
        ),
        coverage_days=tuple(
            FactorStreamDisplayCoverageDay(
                decision_date=day.trade_date, status=day.status, coverage=day.coverage
            )
            for day in stats.days
        ),
    )
    return _with_digest(FactorStreamDisplayArtifact, fields)


@dataclass(frozen=True, slots=True)
class StreamArtifactWitness:
    root: Path
    root_identity: _Identity
    files: tuple[tuple[str, _Identity], ...]

    def recheck(self) -> None:
        descriptor = _open_private_root(self.root)
        try:
            if _file_identity(os.fstat(descriptor)) != self.root_identity:
                raise ValueError("prepared artifact root identity changed")
            _check_identities(self.root, descriptor, dict(self.files))
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class VerifiedFactorStreamArtifacts:
    full: FactorStreamFullArtifact
    display: FactorStreamDisplayArtifact
    witnesses: tuple[StreamArtifactWitness, ...]


def _load(
    root_fd: int, reference: FactorStreamArtifactReference, model: type[BaseModel], limit: int
) -> tuple[BaseModel, _Identity]:
    data, identity = _read_file(root_fd, reference.filename, limit, reference.sha256)
    if len(data) != reference.byte_count:
        raise ValueError("artifact receipt size differs")
    strict_canonical_json_loads(data)
    parsed = model.model_validate_json(data)
    if _bytes(parsed) != data:
        raise ValueError("artifact model bytes are not canonical")
    return parsed, identity


def load_factor_stream_display(root: Path, digest: str) -> FactorStreamDisplayArtifact:
    return _load_factor_stream_display_with_identity(root, digest)[0]


def _load_factor_stream_display_with_identity(
    root: Path, digest: str
) -> tuple[FactorStreamDisplayArtifact, tuple[int, ...], tuple[int, ...]]:
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("stream display requires a SHA-256 filename identity")
    descriptor = _open_private_root(root)
    try:
        from rquant.factor.result_artifact import _root_identity

        root_identity = _root_identity(os.fstat(descriptor))
        data, identity = _read_file(
            descriptor, f"factor-stream-display-v2-{digest}.json", MAX_STREAM_DISPLAY_BYTES, digest
        )
        strict_canonical_json_loads(data)
        display = FactorStreamDisplayArtifact.model_validate_json(data)
        if _bytes(display) != data:
            raise ValueError("display bytes are not canonical")
        _check_identities(root, descriptor, {f"factor-stream-display-v2-{digest}.json": identity})
        return display, root_identity, identity
    finally:
        os.close(descriptor)


def verify_factor_stream_artifacts(
    spec: FactorStreamJobSpec, completion: BaseModel, artifact_root: Path, member_root: Path
) -> VerifiedFactorStreamArtifacts:
    """Outside SQLite transactions: independent daily/decay replay from actual files."""
    from rquant.factor.stream_job_runner import FactorStreamCompletion

    checked = FactorStreamCompletion.model_validate(completion.model_dump(mode="python"))
    descriptor = _open_private_root(artifact_root)
    identities = {}
    decay = None
    batches = None
    try:
        root_identity = _file_identity(os.fstat(descriptor))
        full_ref, display_ref = checked.full_reference, checked.display_reference
        full, identities[full_ref.filename] = _load(
            descriptor, full_ref, FactorStreamFullArtifact, MAX_STREAM_FULL_BYTES
        )
        display, identities[display_ref.filename] = _load(
            descriptor, display_ref, FactorStreamDisplayArtifact, MAX_STREAM_DISPLAY_BYTES
        )
        journal, identities[full.journal_reference.filename] = _load(
            descriptor,
            full.journal_reference,
            FactorStreamJournalManifest,
            MAX_STREAM_JOURNAL_MANIFEST_BYTES,
        )
        if (
            full.spec != spec
            or journal != full.journal
            or display != project_factor_stream_display(full)
        ):
            raise ValueError("stream artifacts differ from exact spec or projection")
        if checked != checked_from_full(full, full_ref, display_ref, checked.completed_at):
            raise ValueError("stream completion differs from exact artifacts")
        request = factor_stream_statistics_request(spec.adapter_request)
        features = {
            day.trade_date: day
            for day in full.result.research.research.adapter_completion.feature_days
        }
        decay = FactorICDecayStream(
            FactorICDecayStreamRequest(
                statistics_request=request,
                computation_stock_codes=spec.adapter_request.source.scope.stock_codes,
            )
        )
        with open_factor_member_stream(member_root, spec.member_archive) as members:
            _matching_request(members.manifest, spec.adapter_request)
            journal_days = iter(journal.days)
            evaluated = frozenset(request.evaluation_days)

            def replay() -> Iterator[FactorDailyStreamBatch]:
                for universe in members:
                    selected = select_factor_universe(universe)
                    if universe.trade_date in evaluated:
                        day = next(journal_days)
                        batch, identity = _load(
                            descriptor,
                            day.artifact,
                            FactorDailyStreamBatch,
                            MAX_STREAM_JOURNAL_DAY_BYTES,
                        )
                        identities[day.artifact.filename] = identity
                        if (
                            day.trade_date != universe.trade_date
                            or batch.universe != selected
                            or canonical_sha256(batch) != day.batch_sha256
                        ):
                            raise ValueError("journal selection differs from actual member archive")
                        if batch.context is not None and (
                            batch.context.panel_date != features[day.trade_date].panel_date
                            or batch.context.sha256 != features[day.trade_date].context_input_sha256
                            or batch.context.stock_codes
                            != spec.adapter_request.source.scope.stock_codes
                        ):
                            raise ValueError(
                                "journal context differs from its completed daily source"
                            )
                        yield batch
                        decay.consume(batch)
                        del batch
                    del universe, selected
                if next(journal_days, None) is not None:
                    raise ValueError("extra journal batch")

            batches = replay()
            statistics = evaluate_factor_daily_stream(request, batches)
            completed_members = members.require_completion()
            if (
                statistics != full.result.research.research.statistics
                or decay.finish(statistics) != full.result.research.decay
                or completed_members != full.result.member_completion
            ):
                raise ValueError("independent journal statistics or member completion differs")
            member_fd = _open_private_root(member_root)
            try:
                member_witness = StreamArtifactWitness(
                    _root_path(member_root),
                    _file_identity(os.fstat(member_fd)),
                    tuple(members._identities.items()),
                )
            finally:
                os.close(member_fd)
        artifact_witness = StreamArtifactWitness(
            _root_path(artifact_root), root_identity, tuple(identities.items())
        )
        for witness in (artifact_witness, member_witness):
            witness.recheck()
        return VerifiedFactorStreamArtifacts(full, display, (artifact_witness, member_witness))
    finally:
        if batches is not None:
            batches.close()
        if decay is not None:
            decay.close()
        os.close(descriptor)


def checked_from_full(
    full: FactorStreamFullArtifact,
    full_ref: FactorStreamArtifactReference,
    display_ref: FactorStreamArtifactReference,
    completed_at: object,
) -> BaseModel:
    from rquant.factor.stream_job_runner import FactorStreamCompletion

    raw = full.result.research.research.adapter_completion
    source = full.spec.adapter_request.source
    return FactorStreamCompletion(
        schema_version=2,
        spec_sha256=full.spec.spec_sha256,
        artifact_sha256=full_ref.sha256,
        artifact_filename=full_ref.filename,
        artifact_byte_count=full_ref.byte_count,
        display_artifact_sha256=display_ref.sha256,
        display_artifact_filename=display_ref.filename,
        display_artifact_byte_count=display_ref.byte_count,
        result_sha256=full.result.sha256,
        source_sha256=raw.sha256,
        snapshot_id=source.snapshot_id,
        binding_hash=source.binding_hash,
        snapshot_as_of_time=source.scope.as_of_time,
        code_revision=full.spec.code_revision,
        completed_at=completed_at,
        neutralization=full.spec.adapter_request.formula.neutralization,
        context=full.spec.adapter_request.formula.sources.context,
    )
