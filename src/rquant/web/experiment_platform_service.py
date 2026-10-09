"""One-generation private views; complete numbers come from the original sealed reader."""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from datetime import datetime
from typing import TYPE_CHECKING, Literal
from uuid import UUID, uuid5

from rquant.experiment_platform import ExperimentSourceProfile, HoldoutPolicy, stable_experiment_job, NativeMinuteExperimentRequest, ExperimentConfiguration
from rquant.experiment_platform_commands import (
    ExperimentCommand,
    ExperimentCommandResult,
    RegisterExperimentFamily,
    UnsealExperimentOuterTest,
)
from rquant.experiment_platform_projection import (
    PRIVATE_TABLES,
    ExperimentAttemptFact,
    ExperimentFamilyFact,
    ExperimentPlannedSlotFact,
    ExperimentPrivateResultAuthority,
)
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from rquant.web import readers
from rquant.web.experiment_platform_models import (
    ExperimentAttemptRow,
    ExperimentCapabilities,
    ExperimentComparisonData,
    ExperimentFamilyData,
    ExperimentHeatmapData,
    ExperimentMineData,
    ExperimentPreparationFamily,
    ExperimentPreparationRow,
    ExperimentResultData,
    ExperimentSourceOption,
    ExperimentStatisticsData,
)
from rquant.web.lab_control_gateway import LabControlGateway
from rquant.web.collaboration_gateway import CollaborationGateway
from rquant.web.models.backtests import PortfolioEditableConfig
from rquant.web.serving import BorrowedGeneration
from rquant.strategy_promotion_contracts import NativeMinuteConfiguration

if TYPE_CHECKING:
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader


def _editable_configuration(configuration: ExperimentConfiguration) -> PortfolioEditableConfig | NativeMinuteConfiguration:
    return configuration if isinstance(configuration, NativeMinuteConfiguration) else PortfolioEditableConfig.from_domain(configuration)


class _Cursor(RuntimeContractModel):
    contract: Literal["private-experiment-page/v1"] = "private-experiment-page/v1"
    owner: str
    generation_id: str
    registered_at: datetime
    experiment_id: str
    page_size: int


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode().rstrip("=")


def _cursor_encode(cursor: _Cursor, key: bytes) -> str:
    raw = cursor.model_dump_json().encode()
    return _segment(raw) + "." + _segment(hmac.new(key, raw, hashlib.sha256).digest())


def _cursor_decode(token: str, key: bytes) -> _Cursor:
    payload, signature = token.split(".")
    raw = b64decode(payload + "=" * (-len(payload) % 4), altchars=b"-_", validate=True)
    digest = b64decode(signature + "=" * (-len(signature) % 4), altchars=b"-_", validate=True)
    if (
        _segment(raw) != payload
        or _segment(digest) != signature
        or not hmac.compare_digest(hmac.new(key, raw, hashlib.sha256).digest(), digest)
    ):
        raise ValueError("private cursor signature differs")
    cursor = _Cursor.model_validate_json(raw)
    if cursor.model_dump_json().encode() != raw:
        raise ValueError("private cursor content is not canonical")
    return cursor


class ExperimentWebService:
    def __init__(
        self,
        *,
        results: PortfolioResultReader | None = None,
        template_results: StrategyTemplateSealedResultReader | None = None,
        private_authority: ExperimentPrivateResultAuthority | None = None,
        gateway: LabControlGateway | None = None,
        profiles: tuple[ExperimentSourceProfile, ...] = (),
        default_config: PortfolioBacktestConfig | None = None,
        owners: frozenset[str] = frozenset(),
        administrators: frozenset[str] = frozenset(),
        enabled: bool = False,
        template_available: bool = False,
        native_results: MinuteSealedReplayReader | None = None,
    ) -> None:
        if not administrators <= owners:
            raise ValueError("experiment administrators need owner permission")
        if len(profiles) > 100 or len({(p.source_key, p.source_version) for p in profiles}) != len(
            profiles
        ):
            raise ValueError("experiment source profiles repeat or exceed capacity")
        if default_config is not None and (
            default_config.source_key,
            default_config.source_version,
        ) not in {(p.source_key, p.source_version) for p in profiles}:
            raise ValueError("default experiment config has no trusted source")
        self.results, self.private_authority = results, private_authority
        self.template_results = template_results
        self.native_results = native_results
        self.gateway = gateway or LabControlGateway()
        self.profiles, self.default_config = profiles, default_config
        self.owners, self.administrators, self.enabled = owners, administrators, enabled
        self.template_available = template_available
        self.collaboration: CollaborationGateway | None = None

    def can_submit(self, owner: str, *, policy: bool = False) -> bool:
        # The writer performs original-request lookup before applying its current switch.
        original = owner in (self.administrators if policy else self.owners)
        if self.collaboration is None:
            return original
        me = self.collaboration.me(owner)
        return original and (me.can_manage_users if policy else me.can_research)

    @staticmethod
    def _published(borrowed: BorrowedGeneration | None) -> bool:
        if borrowed is None:
            return False
        states = readers.table_states(borrowed.cursor)
        present = tuple(states.get(name) for name in PRIVATE_TABLES)
        flags = tuple(s is not None and s.available for s in present)
        if not any(flags):
            return False
        if not all(flags) or len({s.available_at for s in present}) != 1:
            raise ValueError("private experiment projection is partial")
        status = borrowed.cursor.execute(
            "SELECT table_name,owner_dataset_id,owner_generation_id,available_at "
            "FROM projection_status WHERE table_name IN (?,?,?) ORDER BY table_name",
            PRIVATE_TABLES,
        ).fetchall()
        mark = next((w for w in borrowed.manifest.watermarks if w.dataset_id == "promotions"), None)
        if (
            mark is None
            or len(status) != 3
            or any(row[1] != "promotions" or row[2] != mark.generation_id for row in status)
        ):
            raise ValueError("private experiment projection owner generation differs")
        if any(s.row_count != borrowed.manifest.row_counts.get(s.table_name) for s in present):
            raise ValueError("private experiment projection counts differ")
        return True

    def capabilities(
        self, borrowed: BorrowedGeneration | None, owner: str
    ) -> ExperimentCapabilities:
        published = self._published(borrowed)
        policy = None
        if published:
            row = borrowed.cursor.execute(
                "SELECT policy_json FROM experiment_private_window ORDER BY owner LIMIT 1"
            ).fetchone()
            if row is not None:
                policy = HoldoutPolicy.model_validate_json(row[0])
        ready = published and self.enabled and self.can_submit(owner)
        sources = tuple(
            ExperimentSourceOption(
                key=p.source_key,
                version=p.source_version,
                label=p.label,
                start_date=p.coverage.start_date,
                end_date=p.latest_complete,
                trading_dates=p.calendar.dates,
                available=p.phase_slice_available,
                message=None if p.phase_slice_available else "这份来源尚不能隔离样本外日期。",
            )
            for p in self.profiles
        )
        return ExperimentCapabilities(
            available=published,
            can_search=ready and any(p.phase_slice_available for p in self.profiles),
            can_unseal=ready and self.results is not None and self.private_authority is not None,
            can_edit_policy=ready and self.can_submit(owner, policy=True),
            message=None if ready and sources else "正式实验尚未启用，请先准备受限来源。",
            sources=sources,
            default_config=None
            if self.default_config is None
            else PortfolioEditableConfig.from_domain(self.default_config),
            policy=policy,
            can_search_templates=ready
            and self.template_available
            and self.template_results is not None,
        )

    def _family(
        self, borrowed: BorrowedGeneration, owner: str, family_id: str
    ) -> ExperimentFamilyFact:
        if not self._published(borrowed):
            raise LookupError("private experiments are unavailable")
        rows = borrowed.cursor.execute(
            (
                "SELECT payload_json FROM experiment_private_family WHERE owner=? AND "
                "family_id=? LIMIT "
                "2"
            ),
            (owner, family_id),
        ).fetchall()
        if len(rows) != 1:
            raise LookupError("private family is unavailable")
        family = ExperimentFamilyFact.model_validate_json(rows[0][0])
        if (family.owner, family.family_id) != (owner, family_id):
            raise ValueError("private family indexed owner differs")
        return family

    def _fact(
        self, borrowed: BorrowedGeneration, owner: str, experiment_id: str
    ) -> ExperimentAttemptFact:
        if not self._published(borrowed):
            raise LookupError("private experiments are unavailable")
        rows = borrowed.cursor.execute(
            (
                "SELECT payload_json FROM experiment_private_attempt WHERE owner=? AND "
                "experiment_id=? LIMIT "
                "2"
            ),
            (owner, experiment_id),
        ).fetchall()
        if len(rows) != 1:
            raise LookupError("private result is unavailable")
        fact = ExperimentAttemptFact.model_validate_json(rows[0][0])
        if (
            fact.owner != owner
            or fact.attempt.spec.experiment_id != experiment_id
            or fact.child.owner != owner
            or fact.child.family_id != fact.family_id
            or fact.child.experiment_id != experiment_id
        ):
            raise ValueError("private result indexed owner differs")
        return fact

    def _facts(
        self, borrowed: BorrowedGeneration, owner: str, family: ExperimentFamilyFact
    ) -> tuple[ExperimentAttemptFact, ...]:
        rows = borrowed.cursor.execute(
            (
                "SELECT experiment_id FROM experiment_private_attempt WHERE owner=? AND "
                "family_id=? ORDER BY experiment_id LIMIT "
                "65"
            ),
            (owner, family.family_id),
        ).fetchall()
        facts = tuple(self._fact(borrowed, owner, row[0]) for row in rows)
        if family.preparation_state != "ready":
            if facts or len(family.preparations) != family.planned_count:
                raise ValueError("preparing family has unexpected jobs or missing planned slots")
            return ()
        if len(facts) != family.planned_count or sorted(f.index for f in facts) != list(
            range(family.planned_count)
        ):
            raise ValueError("private full family rows differ")
        return tuple(sorted(facts, key=lambda f: f.index))

    @staticmethod
    def _strategy(
        family: ExperimentFamilyFact, configuration: ExperimentConfiguration
    ) -> tuple[str, int, StrategyTemplate | None]:
        if isinstance(configuration, NativeMinuteConfiguration):
            target = configuration.selection.target
            return target.name, target.head.version, None
        if family.request.template is None:
            return "组合回测", 1, None
        if family.template_name is None or family.template_rules is None:
            raise ValueError("private row lost its registered template baseline")
        rules = StrategyTemplate.model_validate(
            family.template_rules.model_dump(mode="python")
            | {
                "weight_rule": configuration.weight_rule,
                "rebalance_rule": configuration.rebalance_rule,
            }
        )
        return family.template_name, family.request.template.head.version, rules

    def _preparation(
        self, slot: ExperimentPlannedSlotFact, family: ExperimentFamilyFact
    ) -> ExperimentPreparationRow:
        from rquant.experiment_platform_evidence import unavailable_experiment_metrics

        strategy_name, strategy_version, rules = self._strategy(family, slot.configuration)
        return ExperimentPreparationRow(
            index=slot.index,
            configuration=_editable_configuration(slot.configuration),
            definition_state=slot.definition_state,
            input_prepared=slot.input_prepared,
            failure=slot.failure,
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            rules=rules,
            metrics=unavailable_experiment_metrics(),
        )

    def _row(
        self, fact: ExperimentAttemptFact, family: ExperimentFamilyFact
    ) -> ExperimentAttemptRow:
        from rquant.experiment_platform_evidence import (
            read_experiment_result,
            unavailable_experiment_metrics,
        )

        if (fact.owner, fact.family_id) != (family.owner, family.family_id):
            raise PermissionError("private row belongs to another owner or family")
        status = fact.attempt.status.value
        labels = {
            "registered": "等待运行",
            "running": "正在运行",
            "executed": "已完成",
            "succeeded": "已完成",
            "failed": "运行失败",
            "cancelled": "已取消",
        }
        message = (
            "结果尚未保存，请稍后重试。"
            if status in ("executed", "succeeded") and fact.result_hash is None
            else None
        )
        if status == "failed":
            message = "本次未完成，原尝试仍计入搜索总数。"
        elif status == "cancelled":
            message = "已取消，原尝试仍计入搜索总数。"
        metrics = unavailable_experiment_metrics()
        strategy_name, strategy_version, rules = self._strategy(family, fact.configuration)
        if status in ("executed", "succeeded") and fact.result_hash is not None:
            if self.private_authority is None or (
                self.native_results is None if isinstance(fact.configuration, NativeMinuteConfiguration)
                else self.results is None
            ):
                message = "完整指标暂时无法读取，请稍后重试。"
            else:
                result = read_experiment_result(
                    fact,
                    family,
                    results=self.results,
                    authority=self.private_authority,
                    template_results=self.template_results,
                    native_results=self.native_results,
                )
                if rules is not None and (
                    result.template is None or result.template.rules != rules
                ):
                    raise ValueError("private row rules differ from its sealed result")
                metrics = result.metrics
                if any(metric.value is None for metric in metrics):
                    message = "部分指标暂不可计算。"
        return ExperimentAttemptRow(
            experiment_id=fact.attempt.spec.experiment_id,
            family_id=fact.family_id,
            family_name=family.name,
            phase=family.phase,
            registered_at=fact.attempt.registered_at,
            status=status,
            label=labels[status],
            index=fact.index,
            configuration=_editable_configuration(fact.configuration),
            job_id=fact.child.job_id,
            result_hash=fact.result_hash,
            message=message,
            cancellation_pending=fact.child.cancel_state == "pending",
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            rules=rules,
            metrics=metrics,
        )

    def mine(
        self,
        borrowed: BorrowedGeneration | None,
        owner: str,
        *,
        limit: int,
        cursor: str | None,
        cursor_key: bytes,
    ) -> ExperimentMineData:
        if not self._published(borrowed):
            return ExperimentMineData(available=False, retained_count=0, truncated=False)
        preparation_rows = borrowed.cursor.execute(
            "SELECT payload_json FROM experiment_private_family WHERE owner=? AND "
            "json_extract_string(payload_json,'$.preparation_state')!='ready' "
            "ORDER BY json_extract_string(payload_json,'$.registered_at') "
            "DESC,family_id DESC LIMIT 501",
            (owner,),
        ).fetchall()
        preparing = tuple(ExperimentFamilyFact.model_validate_json(r[0]) for r in preparation_rows)
        if any(
            f.owner != owner
            or f.preparation_state == "ready"
            or len(f.preparations) != f.planned_count
            for f in preparing
        ):
            raise ValueError("private planned owner window conflicts")
        preparing_rows = tuple(
            ExperimentPreparationFamily(
                family_id=f.family_id,
                name=f.name,
                registered_at=f.registered_at,
                state=f.preparation_state,
                planned_count=f.planned_count,
                definition_saved_count=sum(p.definition_state == "saved" for p in f.preparations),
                input_prepared_count=sum(p.input_prepared for p in f.preparations),
                failed_count=sum(p.definition_state == "failed" for p in f.preparations),
                cancelled_count=sum(p.definition_state == "cancelled" for p in f.preparations),
            )
            for f in preparing
        )
        window = borrowed.cursor.execute(
            (
                "SELECT retained_count,truncated,oldest_registered_at FROM "
                "experiment_private_window WHERE owner=? LIMIT "
                "2"
            ),
            (owner,),
        ).fetchall()
        rows = borrowed.cursor.execute(
            (
                "SELECT experiment_id,registered_at FROM experiment_private_attempt "
                "WHERE owner=? ORDER BY registered_at DESC,experiment_id DESC LIMIT "
                "501"
            ),
            (owner,),
        ).fetchall()
        if not window and not rows:
            return ExperimentMineData(
                available=True,
                retained_count=0,
                truncated=False,
                preparing_families=preparing_rows,
                preparing_window_truncated=any(f.preparation_window_truncated for f in preparing),
            )
        if (
            len(window) != 1
            or window[0][0] != min(len(rows), 500)
            or window[0][2] != (rows[min(len(rows), 500) - 1][1] if rows else None)
            or (window[0][1] and window[0][0] != 500)
        ):
            raise ValueError("private owner window differs from visible rows")
        retained = rows[:500]
        boundary = _cursor_decode(cursor, cursor_key) if cursor is not None else None
        if boundary is not None:
            if (boundary.owner, boundary.generation_id, boundary.page_size) != (
                owner,
                borrowed.manifest.generation_id,
                limit,
            ):
                raise ValueError("private cursor belongs to another owner or generation")
            retained = tuple(
                row
                for row in retained
                if (row[1], row[0]) < (boundary.registered_at, boundary.experiment_id)
            )
        selected = retained[:limit]
        items = tuple(
            self._row(f, self._family(borrowed, owner, f.family_id))
            for f in (self._fact(borrowed, owner, row[0]) for row in selected)
        )
        next_cursor = None
        if len(retained) > limit:
            last = items[-1]
            next_cursor = _cursor_encode(
                _Cursor(
                    owner=owner,
                    generation_id=borrowed.manifest.generation_id,
                    registered_at=last.registered_at,
                    experiment_id=last.experiment_id,
                    page_size=limit,
                ),
                cursor_key,
            )
        return ExperimentMineData(
            available=True,
            items=items,
            retained_count=window[0][0],
            truncated=window[0][1],
            oldest_registered_at=window[0][2],
            next_cursor=next_cursor,
            preparing_families=preparing_rows,
            preparing_window_truncated=any(f.preparation_window_truncated for f in preparing),
        )

    def family(
        self, borrowed: BorrowedGeneration, owner: str, family_id: str
    ) -> ExperimentFamilyData:
        family = self._family(borrowed, owner, family_id)
        facts = self._facts(borrowed, owner, family)
        return ExperimentFamilyData(
            family_id=family_id,
            name=family.name,
            phase=family.phase,
            parent_family_id=family.parent_family_id,
            registered_at=family.registered_at,
            protocol=family.request.protocol,
            planned_count=family.planned_count,
            potential_count=family.potential_count,
            search_count=family.search_count,
            parameters=tuple(d.parameter for d in family.request.dimensions),
            failed_count=sum(f.attempt.status.value == "failed" for f in facts)
            + sum(p.definition_state == "failed" for p in family.preparations),
            cancelled_count=sum(f.attempt.status.value == "cancelled" for f in facts)
            + sum(p.definition_state == "cancelled" for p in family.preparations),
            items=tuple(self._row(f, family) for f in facts),
            note="" if family.note is None else family.note.text,
            note_version=0 if family.note is None else family.note.version,
            outer_admitted=family.outer_admitted,
            preparation_state=family.preparation_state,
            preparations=tuple(self._preparation(p, family) for p in family.preparations),
        )

    def result(
        self, borrowed: BorrowedGeneration, owner: str, experiment_id: str, *, result_hash: str
    ) -> ExperimentResultData:
        from rquant.experiment_platform_evidence import read_experiment_result

        fact = self._fact(borrowed, owner, experiment_id)
        family = self._family(borrowed, owner, fact.family_id)
        if (
            (self.native_results is None if isinstance(fact.configuration, NativeMinuteConfiguration) else self.results is None)
            or self.private_authority is None
            or fact.result_hash != result_hash
        ):
            raise ValueError("private exact sealed result is unavailable or changed")
        return read_experiment_result(
            fact,
            family,
            results=self.results,
            authority=self.private_authority,
            template_results=self.template_results,
            native_results=self.native_results,
        )

    def compare(
        self, borrowed: BorrowedGeneration, owner: str, a: str, b: str
    ) -> ExperimentComparisonData:
        from rquant.experiment_platform_evidence import compare_experiment_results

        if a == b:
            raise ValueError("comparison requires two distinct experiments")
        facts = tuple(self._fact(borrowed, owner, i) for i in (a, b))
        results = tuple(
            self.result(borrowed, owner, f.attempt.spec.experiment_id, result_hash=f.result_hash)
            for f in facts
        )
        return compare_experiment_results(*results)

    def heatmap(
        self,
        borrowed: BorrowedGeneration,
        owner: str,
        family_id: str,
        *,
        selected: str,
        x: str,
        y: str,
        metric: str,
        phase: str,
    ) -> ExperimentHeatmapData:
        from rquant.experiment_platform_evidence import experiment_heatmap

        family = self._family(borrowed, owner, family_id)
        facts = self._facts(borrowed, owner, family)

        def read(fact: ExperimentAttemptFact) -> ExperimentResultData:
            return self.result(
                borrowed, owner, fact.attempt.spec.experiment_id, result_hash=fact.result_hash
            )

        return experiment_heatmap(
            family, facts, read=read, selected=selected, x=x, y=y, metric=metric, phase=phase
        )

    def statistics(
        self, borrowed: BorrowedGeneration, owner: str, experiment_id: str
    ) -> ExperimentStatisticsData:
        selected = self._fact(borrowed, owner, experiment_id)
        family = self._family(borrowed, owner, selected.family_id)
        target = experiment_id
        if family.parent_family_id is not None:
            target = family.selected_search_experiment_id
            family = self._family(borrowed, owner, family.parent_family_id)
        facts = self._facts(borrowed, owner, family)
        if family.evidence_id is None:
            return ExperimentStatisticsData(
                family_id=family.family_id,
                experiment_id=target,
                search_count=family.search_count,
                failed_count=sum(f.attempt.status.value == "failed" for f in facts)
                + sum(p.definition_state == "failed" for p in family.preparations),
                cancelled_count=sum(f.attempt.status.value == "cancelled" for f in facts)
                + sum(p.definition_state == "cancelled" for p in family.preparations),
                evidence_id=canonical_sha256(facts),
                reasons=("完整搜索证据尚未保存，请稍后重试。",),
            )
        if self.private_authority is None:
            raise ValueError("private evidence authority is unavailable")
        evidence = self.private_authority.evidence(owner, family.family_id, family.evidence_id)
        if (
            evidence.search_count != family.search_count
            or evidence.result_hashes
            != tuple(
                (f.attempt.spec.experiment_id, f.result_hash)
                for f in sorted(facts, key=lambda f: f.attempt.spec.experiment_id)
            )
            or evidence.attempt_digest
            != canonical_sha256(
                tuple(f.attempt for f in sorted(facts, key=lambda f: f.attempt.spec.experiment_id))
            )
        ):
            raise ValueError("sealed statistics differ from the complete current ledger")
        for fact in facts:
            if fact.result_hash is not None:
                self.result(
                    borrowed, owner, fact.attempt.spec.experiment_id, result_hash=fact.result_hash
                )
        statistic = next((s for s in evidence.statistics if s.experiment_id == target), None)
        if statistic is None:
            raise ValueError("selected statistics are not in the complete ledger")
        return statistic.model_copy(update={"pbo": evidence.pbo})

    @staticmethod
    def verify_receipt(command: ExperimentCommand, result: ExperimentCommandResult) -> None:
        if isinstance(command, RegisterExperimentFamily):
            from rquant.experiment_platform import enumerate_search

            count = len(enumerate_search(command.request))
            if (
                result.status != "registered"
                or result.planned_count != count
                or result.job_ids
                != tuple(
                    (uuid5(UUID(command.command_id), f"strategy-fixed-wf:{i + 1}")
                     if isinstance(command.request, NativeMinuteExperimentRequest) and command.request.walk_forward_command_id is not None
                     else stable_experiment_job(command.actor_id, result.command_id, i))
                    for i in range(count)
                )
            ):
                raise ValueError("registered receipt has another actual enumeration")
        elif isinstance(command, UnsealExperimentOuterTest):
            if (
                result.status != "outer_admitted"
                or result.planned_count != 1
                or result.job_ids
                != (stable_experiment_job(command.actor_id, result.command_id, 0),)
            ):
                raise ValueError("outer receipt has another job")
        else:
            actions = {
                "cancel_experiment_family": {
                    "cancellation_pending",
                    "cancelled",
                    "already_completed",
                    "already_finished",
                },
                "set_experiment_note": {"note_saved"},
                "set_experiment_holdout_policy": {"policy_saved"},
            }
            if result.status not in actions[command.kind] or result.job_ids:
                raise ValueError("experiment receipt action differs")
            if hasattr(command, "family_id") and result.family_id != command.family_id:
                raise ValueError("experiment receipt family differs")
