"""One bounded screening application over either the legacy page or a verified replica."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from pydantic import ValidationError

from rquant.llm.compile import compile_screen_plan
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage
from rquant.screen.core import _collect_aggregates, rule_state
from rquant.screen.dynamic_rsi import (
    DynamicRsiProjectionUnavailableError,
    VerifiedDynamicRsiProjection,
)
from rquant.screen.formula_history_projection import (
    FormulaProjectionBudgetError,
    FormulaProjectionChangedError,
    FormulaProjectionDateError,
    FormulaProjectionUnavailableError,
    VerifiedFormulaHistoryProjection,
)
from rquant.screen.loader import FUNDAMENTAL_COLS_MAP
from rquant.screen.query_contracts import ScreenQueryDefinition
from rquant.screen.ranking import RETURN_20D_COLUMN, RankingCondition, rank_screen_results
from rquant.screen.replica_source import (
    ScreenReplicaBudgetError,
    ScreenReplicaChangedError,
    ScreenReplicaDataError,
    ScreenReplicaUnavailableError,
    VerifiedReplicaScreenSource,
)
from rquant.screen.rules import Rule, required_rule_columns
from rquant.screen.tdx import parse_formula
from rquant.screen.tdx.evaluate import (
    EvaluationRejectedError,
    FormulaEvaluationInput,
    evaluate_formula,
)
from rquant.serving_read_models import (
    PAGE_PROJECTION_CONTRACTS,
    NlScreenPage,
    NlScreenPageError,
    NlScreenProjectionFeatureError,
    nl_screen_query_digest,
    paginate_nl_screen_projection,
    paginate_ranked_nl_screen_projection,
    screen_nl_projection,
)
from rquant.web import readers
from rquant.web.models.screen import (
    ScreenCatalogData,
    ScreenCondition,
    ScreenRow,
    ScreenRunData,
    ScreenRunRequest,
    ScreenSourceInfo,
    ScreenStep,
    TdxPreviewData,
    TdxPreviewRequest,
    TdxPreviewSourceData,
)
from rquant.web.screen_catalog import (
    RANKING_METRIC_LABELS,
    available_ranking_metrics,
    screen_blocks,
    validate_screen_choices,
)
from rquant.web.serving import BorrowedGeneration

_REPLICA_RANK_COLUMNS = frozenset(
    {"TURNOVER_RATE[0]", "CIRC_MV[0]", "PCT_CHG[0]", RETURN_20D_COLUMN}
)
_FUNDAMENTAL_COLUMNS = frozenset(f"{name}[0]" for name in FUNDAMENTAL_COLS_MAP.values())
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PREVIEW_UNKNOWN = {
    "missing_date": "这只股票在所选日期缺少日线，暂无法判断。",
    "missing_listing": "缺少上市日期，暂无法核对完整历史。",
    "missing_calendar": "交易日历不完整，暂无法判断。",
    "missing_history": "历史日线不完整，暂无法判断。",
    "invalid_value": "行情字段无效，暂无法判断。",
    "insufficient_history": "历史天数不足，暂无法判断。",
    "incomplete_history": "历史覆盖不足，暂无法判断。",
    "missing_value": "行情字段缺失，暂无法判断。",
    "division_by_zero": "公式遇到除零，暂无法判断。",
    "non_finite": "公式结果超出数值范围，暂无法判断。",
    "numeric_underflow": "公式结果过小，暂无法判断。",
    "never_true": "此前没有满足条件的记录，暂无法判断。",
}


@dataclass(frozen=True, slots=True)
class ScreenApplicationError(Exception):
    status_code: int
    detail: str


def _empty_run(trade_date: date, status: str) -> ScreenRunData:
    return ScreenRunData(
        trade_date=trade_date,
        status=status,
        base_count=None,
        total=None,
        steps=[],
        rows=[],
        next_cursor=None,
        source=None,
    )


def _number(value: object) -> float | None:
    return None if value is None or bool(pd.isna(value)) else float(value)


_replica_rule_state = rule_state


class ScreenApplicationService:
    def __init__(
        self,
        *,
        cursor_key: bytes,
        replica: VerifiedReplicaScreenSource | None = None,
        history: VerifiedFormulaHistoryProjection | None = None,
        rsi: VerifiedDynamicRsiProjection | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.cursor_key = cursor_key
        self.replica = replica
        self.history = history
        self.rsi = rsi
        self.clock = clock or (lambda: datetime.now(UTC))

    def _rsi_ready(self, source_identity: str, dates: list[date]) -> bool:
        if self.rsi is None:
            return False
        try:
            projected_dates = self.rsi.catalog(source_identity).dates
            return bool(dates) and set(dates).issubset(projected_dates)
        except DynamicRsiProjectionUnavailableError:
            return False

    def preview_source(self) -> TdxPreviewSourceData:
        if self.history is None:
            return TdxPreviewSourceData(available=False, dates=[], source=None)
        try:
            catalog = self.history.catalog()
        except FormulaProjectionUnavailableError:
            return TdxPreviewSourceData(available=False, dates=[], source=None)
        return TdxPreviewSourceData(
            available=bool(catalog.dates),
            dates=catalog.dates,
            source=ScreenSourceInfo(
                identity=catalog.identity,
                updated_at=catalog.updated_at,
            ),
        )

    def preview(
        self,
        body: TdxPreviewRequest,
        *,
        decision_at: datetime,
    ) -> TdxPreviewData:
        if self.history is None:
            raise ScreenApplicationError(503, "公式预览数据暂不可用，请稍后重试。")
        parsed = parse_formula(body.source)
        if parsed.status != "parsed" or parsed.translation is None:
            raise ScreenApplicationError(422, "公式尚未通过检查，请修改后重试。")
        if decision_at.astimezone(_SHANGHAI) < datetime.combine(
            body.trade_date,
            time(17),
            _SHANGHAI,
        ):
            raise ScreenApplicationError(422, "这一天的日线尚未收盘，请换日期。")
        try:
            snapshot = self.history.formula_history(
                body.trade_date,
                body.stock_code,
                expected_identity=body.source_identity,
                lookback=parsed.translation.window_lookback_bars,
                full_history=parsed.translation.requires_full_history,
            )
        except FormulaProjectionChangedError as error:
            raise ScreenApplicationError(409, "公式预览数据已更新，请刷新后重试。") from error
        except FormulaProjectionDateError as error:
            raise ScreenApplicationError(422, "请选择已开市的交易日。") from error
        except FormulaProjectionUnavailableError as error:
            raise ScreenApplicationError(503, "公式预览数据暂不可用，请稍后重试。") from error
        except FormulaProjectionBudgetError as error:
            raise ScreenApplicationError(
                422,
                "这只股票的历史超出单次预览范围，请换股票。",
            ) from error

        status = "unknown"
        reason = _PREVIEW_UNKNOWN[snapshot.unknown_reason] if snapshot.unknown_reason else None
        if snapshot.stock is not None:
            try:
                evaluated = evaluate_formula(
                    FormulaEvaluationInput(
                        formula=body.source,
                        decision_date=body.trade_date,
                        decision_at=decision_at,
                        stocks=(snapshot.stock,),
                    )
                )
            except EvaluationRejectedError as error:
                raise ScreenApplicationError(
                    422,
                    "公式或历史超出单次预览范围，请缩短后重试。",
                ) from error
            decision = evaluated.decisions[0]
            status = decision.status
            reason = _PREVIEW_UNKNOWN[decision.reason] if decision.reason else None
        return TdxPreviewData(
            stock_code=body.stock_code,
            trade_date=body.trade_date,
            status=status,
            reason=reason,
            source_updated_at=snapshot.updated_at,
        )

    def catalog(
        self, borrowed: BorrowedGeneration | None, *, mode: Literal["daily", "intraday"] = "daily"
    ) -> ScreenCatalogData:
        if mode == "intraday":
            from rquant.web.screen_intraday import intraday_screen_context

            try:
                context = intraday_screen_context(
                    borrowed, now=self.clock(), replica=self.replica, rsi=self.rsi
                )
            except (ValueError, RuntimeError, duckdb.Error):
                return ScreenCatalogData(
                    source_kind="intraday",
                    blocks=screen_blocks(daily_anchor=True),
                    dates=[],
                    available=False,
                    ranking_metrics=[],
                    source=None,
                )
            return ScreenCatalogData(
                source_kind="intraday",
                blocks=screen_blocks(
                    dynamic_ma=context.dynamic_ma,
                    dynamic_rsi=context.dynamic_rsi,
                    fundamental_fields=context.fundamental_fields,
                    extra_fields=context.extra_fields,
                    daily_anchor=True,
                ),
                dates=[context.snapshot.source.trade_date],
                available=bool(context.extra_fields),
                ranking_metrics=available_ranking_metrics(
                    tuple(name for name, _ in context.extra_fields)
                    + (tuple(_REPLICA_RANK_COLUMNS) if context.daily_source_identity else ())
                ),
                source=context.source,
            )
        if self.replica is not None:
            try:
                snapshot = self.replica.available_dates()
            except (ScreenReplicaUnavailableError, ScreenReplicaDataError):
                return ScreenCatalogData(
                    source_kind="replica",
                    blocks=screen_blocks(),
                    dates=[],
                    available=False,
                    ranking_metrics=[],
                    source=None,
                )
            rsi_ready = self._rsi_ready(snapshot.identity, snapshot.dates)
            try:
                fundamental_fields = self.replica.available_fundamental_fields(
                    expected_identity=snapshot.identity,
                    dates=snapshot.dates,
                )
            except (
                ScreenReplicaUnavailableError,
                ScreenReplicaDataError,
                ScreenReplicaChangedError,
            ):
                fundamental_fields = frozenset()
            try:
                current = self.replica.available_dates()
            except (ScreenReplicaUnavailableError, ScreenReplicaDataError):
                current = None
            if current is None or current.identity != snapshot.identity:
                rsi_ready = False
                fundamental_fields = frozenset()
            return ScreenCatalogData(
                source_kind="replica",
                blocks=screen_blocks(
                    dynamic_ma=True,
                    dynamic_rsi=rsi_ready,
                    fundamental_fields=fundamental_fields,
                ),
                dates=snapshot.dates,
                available=bool(snapshot.dates),
                ranking_metrics=available_ranking_metrics(_REPLICA_RANK_COLUMNS),
                source=ScreenSourceInfo(
                    identity=snapshot.identity,
                    updated_at=snapshot.updated_at,
                ),
            )

        available = False
        dates: list[date] = []
        ranking_metrics = []
        source = None
        if borrowed is not None:
            state = readers.table_states(borrowed.cursor).get("nl_screen_universe")
            available = state is not None and state.available
            if available:
                columns = {
                    item[0]
                    for item in borrowed.cursor.execute(
                        "SELECT * FROM nl_screen_universe LIMIT 0"
                    ).description
                }
                ranking_metrics = available_ranking_metrics(columns)
                dates = [
                    row[0]
                    for row in borrowed.cursor.execute(
                        "SELECT DISTINCT trade_date FROM nl_screen_universe "
                        "ORDER BY trade_date DESC LIMIT 30"
                    ).fetchall()
                ]
                source = ScreenSourceInfo(
                    identity=borrowed.manifest.generation_id,
                    updated_at=borrowed.manifest.built_at,
                )
        return ScreenCatalogData(
            source_kind="serving",
            blocks=screen_blocks(),
            dates=dates,
            available=available,
            ranking_metrics=ranking_metrics,
            source=source,
        )

    def run_complete(
        self,
        definition: ScreenQueryDefinition,
        *,
        borrowed: BorrowedGeneration | None,
        serving_unavailable: bool,
    ) -> ScreenRunData:
        if definition.mode == "intraday":
            # The source descriptor is read again; the request never supplies source facts.
            from rquant.web.screen_intraday import intraday_screen_context

            try:
                context = intraday_screen_context(
                    borrowed, now=self.clock(), replica=self.replica, rsi=self.rsi
                )
            except (ValueError, RuntimeError, duckdb.Error) as error:
                raise ScreenApplicationError(503, "盘中数据暂不可用，请稍后重试。") from error
            body = ScreenRunRequest(
                trade_date=definition.trade_date,
                conditions=[
                    ScreenCondition(key=c.name, args=c.args) for c in definition.conditions
                ],
                source_identity=definition.source_identity,
                ranking=definition.ranking,
                mode="intraday",
                decision_cutoff=definition.cutoff,
                intraday_source_identity=context.snapshot.source.source_identity,
            )
            return self._evaluate(
                body, borrowed=borrowed, serving_unavailable=serving_unavailable, complete=True
            )
        actual_kind = "replica" if self.replica is not None else "serving"
        if definition.source_kind != actual_kind:
            raise ScreenApplicationError(409, "选股数据已更新，请重新运行。")
        body = ScreenRunRequest(
            trade_date=definition.trade_date,
            conditions=[ScreenCondition(key=c.name, args=c.args) for c in definition.conditions],
            source_identity=definition.source_identity,
            ranking=definition.ranking,
        )
        return self._evaluate(
            body, borrowed=borrowed, serving_unavailable=serving_unavailable, complete=True
        )

    def run(
        self,
        body: ScreenRunRequest,
        *,
        borrowed: BorrowedGeneration | None,
        serving_unavailable: bool,
    ) -> ScreenRunData:
        return self._evaluate(
            body, borrowed=borrowed, serving_unavailable=serving_unavailable, complete=False
        )

    def _evaluate(
        self,
        body: ScreenRunRequest,
        *,
        borrowed: BorrowedGeneration | None,
        serving_unavailable: bool,
        complete: bool,
    ) -> ScreenRunData:
        if body.mode == "intraday":
            return self._evaluate_intraday(
                body, borrowed=borrowed, serving_unavailable=serving_unavailable, complete=complete
            )
        if (
            self.replica is None
            and body.source_identity is not None
            and (borrowed is None or body.source_identity != borrowed.manifest.generation_id)
        ):
            raise ScreenApplicationError(409, "选股数据已更新，请刷新条件后重试。")
        requested_fundamental = {
            value
            for condition in body.conditions
            for value in condition.args.values()
            if type(value) is str and value in _FUNDAMENTAL_COLUMNS
        }
        fundamental_fields: frozenset[str] = frozenset()
        if requested_fundamental:
            if self.replica is None:
                raise ScreenApplicationError(422, "请从条件目录选择数据项或板块。")
            if body.source_identity is None:
                raise ScreenApplicationError(422, "请刷新条件目录后重新筛选。")
            try:
                snapshot = self.replica.available_dates()
                fundamental_fields = self.replica.available_fundamental_fields(
                    expected_identity=body.source_identity,
                    dates=snapshot.dates,
                )
            except ScreenReplicaChangedError as error:
                raise ScreenApplicationError(409, "选股数据已更新，请重新筛选。") from error
            except (ScreenReplicaUnavailableError, ScreenReplicaDataError) as error:
                raise ScreenApplicationError(503, "基本面数据暂不可用，请稍后重试。") from error
        rsi_ready = False
        if self.replica is not None and self.rsi is not None:
            with suppress(ScreenReplicaUnavailableError, ScreenReplicaDataError):
                rsi_ready = self._rsi_ready(self.replica.generation_identity(), [body.trade_date])
        try:
            normalized_args = validate_screen_choices(
                body.conditions,
                dynamic_ma=self.replica is not None,
                dynamic_rsi=rsi_ready,
                fundamental_fields=fundamental_fields,
            )
        except ValueError as error:
            if "RSI period" in str(error):
                detail = "RSI 周期请填 2 到 60 日。"
            elif "indicator period" in str(error):
                detail = (
                    "均线周期请填 2 到 250 日；RSI 请从目录选择。"
                    if self.replica is not None
                    else "当前仅支持已列出的均线和 RSI 周期，请调整条件。"
                )
            elif "custom indicator field" in str(error):
                detail = "所选指标暂不可用，请调整周期或刷新数据。"
            else:
                detail = "请从条件目录选择数据项或板块。"
            raise ScreenApplicationError(422, detail) from error
        if body.ranking is not None and any(
            condition.metric not in RANKING_METRIC_LABELS for condition in body.ranking.conditions
        ):
            raise ScreenApplicationError(422, "请从排名指标目录选择。")

        labels = {block.key: block.label for block in screen_blocks()}
        try:
            plan = ScreenPlan(
                trade_date=body.trade_date.isoformat(),
                stages=[
                    Stage(
                        label="条件",
                        rules=[
                            RuleCall(name=condition.key, args=args)
                            for condition, args in zip(
                                body.conditions, normalized_args, strict=True
                            )
                        ],
                    )
                ],
            )
            compiled = compile_screen_plan(plan)
            rule_labels = [labels[condition.key] for condition in body.conditions]
        except KeyError as error:
            raise ScreenApplicationError(422, "没有找到这条条件，请重新选择。") from error
        except ValidationError as error:
            raise ScreenApplicationError(422, "条件填写有误，请检查后重试。") from error
        except ValueError as error:
            raise ScreenApplicationError(422, "没有找到这条条件，请重新选择。") from error

        ranking = body.ranking
        rank_columns = [condition.metric for condition in ranking.conditions] if ranking else []
        page_rules = compiled.rules
        unknown_counts = [0] * len(compiled.rules)
        if self.replica is not None:
            unsupported_metric = next(
                (metric for metric in rank_columns if metric not in _REPLICA_RANK_COLUMNS),
                None,
            )
            if unsupported_metric is not None:
                label = RANKING_METRIC_LABELS[unsupported_metric]
                raise ScreenApplicationError(
                    422,
                    f"当前数据还没有「{label}」，请换一个排名指标。",
                )
            try:
                snapshot = self.replica.load(
                    body.trade_date,
                    compiled.rules,
                    include_columns=rank_columns,
                    rsi_projection=self.rsi if rsi_ready else None,
                    expected_identity=body.source_identity,
                )
            except ScreenReplicaChangedError as error:
                raise ScreenApplicationError(409, "选股数据已更新，请重新筛选。") from error
            except ScreenReplicaUnavailableError as error:
                raise ScreenApplicationError(
                    409 if body.cursor else 503,
                    "选股数据已更新，请重新筛选。"
                    if body.cursor
                    else "选股数据暂不可用，请稍后重试。",
                ) from error
            except ScreenReplicaDataError as error:
                raise ScreenApplicationError(
                    409 if body.cursor else 503,
                    "选股数据已更新，请重新筛选。"
                    if body.cursor
                    else "所选日期的数据不完整，请换日期或稍后重试。",
                ) from error
            except ScreenReplicaBudgetError as error:
                raise ScreenApplicationError(
                    422,
                    "条件组合超出单次筛选范围，请减少条件或回看天数。",
                ) from error
            except ValueError as error:
                raise ScreenApplicationError(
                    422,
                    "当前数据还不支持这个条件，请换一条或稍后重试。",
                ) from error
            universe = snapshot.frame
            # An entirely unknown input would read as zero hits for a valid rule.
            required_columns = required_rule_columns(compiled.rules) | {
                request.name for request in _collect_aggregates(compiled.rules)
            }
            if any(
                column not in universe.columns
                or (column not in _FUNDAMENTAL_COLUMNS and universe[column].isna().all())
                for column in required_columns
            ):
                raise ScreenApplicationError(
                    503,
                    "所选日期的数据不完整，请换日期或稍后重试。",
                )
            page_rules, unknown_counts = _replica_rule_state(universe, compiled.rules)
            source = ScreenSourceInfo(
                identity=snapshot.identity,
                updated_at=snapshot.updated_at,
            )
        else:
            if borrowed is None or serving_unavailable:
                if body.cursor is not None:
                    raise ScreenApplicationError(409, "数据已更新，请重新筛选。")
                return _empty_run(body.trade_date, "unavailable")
            state = readers.table_states(borrowed.cursor).get("nl_screen_universe")
            if state is None or not state.available:
                if body.cursor is not None:
                    raise ScreenApplicationError(409, "数据已更新，请重新筛选。")
                return _empty_run(body.trade_date, "unavailable")
            max_rows = PAGE_PROJECTION_CONTRACTS["nl_screen_universe"].max_rows
            universe = borrowed.cursor.execute(
                "SELECT * FROM nl_screen_universe WHERE trade_date = ? "
                "ORDER BY trade_date, ts_code LIMIT ?",
                (body.trade_date, max_rows + 1),
            ).fetchdf()
            if len(universe) > max_rows:
                raise ScreenApplicationError(503, "可筛选股票暂时过多，请稍后重试。")
            if universe.empty:
                if body.cursor is not None:
                    raise ScreenApplicationError(409, "数据已更新，请重新筛选。")
                return _empty_run(body.trade_date, "no_date")
            source = ScreenSourceInfo(
                identity=borrowed.manifest.generation_id,
                updated_at=borrowed.manifest.built_at,
            )

        return self._page_result(
            body,
            universe=universe,
            page_rules=page_rules,
            unknown_counts=unknown_counts,
            source=source,
            rule_labels=rule_labels,
            normalized_plan=compiled.normalized_plan,
            complete=complete,
        )

    def _evaluate_intraday(
        self,
        body: ScreenRunRequest,
        *,
        borrowed: BorrowedGeneration | None,
        serving_unavailable: bool,
        complete: bool,
    ) -> ScreenRunData:
        from rquant.web.screen_intraday import (
            intraday_screen_context,
            prepare_intraday_screen_frame,
        )

        if serving_unavailable or borrowed is None:
            raise ScreenApplicationError(503, "盘中数据暂不可用，请稍后重试。")
        try:
            context = intraday_screen_context(
                borrowed, now=self.clock(), replica=self.replica, rsi=self.rsi
            )
        except (ValueError, RuntimeError, duckdb.Error) as error:
            raise ScreenApplicationError(503, "盘中数据暂不可用，请稍后重试。") from error
        actual = context.snapshot.source
        if (
            body.trade_date != actual.trade_date
            or body.source_identity != context.source.identity
            or body.decision_cutoff != actual.cutoff
            or body.intraday_source_identity != actual.source_identity
        ):
            raise ScreenApplicationError(409, "盘中数据已更新，请保留条件重新运行。")
        if not context.extra_fields:
            raise ScreenApplicationError(503, "盘中来源已过期，请稍后重试。")
        try:
            args = validate_screen_choices(
                body.conditions,
                dynamic_ma=context.dynamic_ma,
                dynamic_rsi=context.dynamic_rsi,
                fundamental_fields=context.fundamental_fields,
                extra_fields=context.extra_fields,
            )
            plan = ScreenPlan(
                trade_date=body.trade_date.isoformat(),
                stages=[
                    Stage(
                        label="条件",
                        rules=[
                            RuleCall(name=c.key, args=a)
                            for c, a in zip(body.conditions, args, strict=True)
                        ],
                    )
                ],
            )
            compiled = compile_screen_plan(plan)
            labels = {block.key: block.label for block in screen_blocks()}
            rule_labels = [labels[c.key] for c in body.conditions]
            rank_columns = [c.metric for c in body.ranking.conditions] if body.ranking else []
            offered = {
                option.value for option in self.catalog(borrowed, mode="intraday").ranking_metrics
            }
            if set(rank_columns) - offered:
                raise ValueError("intraday ranking metric is unavailable")
            inputs = prepare_intraday_screen_frame(
                context,
                borrowed=borrowed,
                rules=compiled.rules,
                rank_columns=rank_columns,
                replica=self.replica,
                rsi=self.rsi,
            )
            universe = inputs.frame
            page_rules, unknown_counts = rule_state(universe, compiled.rules)
        except ScreenReplicaChangedError as error:
            raise ScreenApplicationError(409, "日线数据已更新，请保留条件重新运行。") from error
        except (
            ScreenReplicaUnavailableError,
            ScreenReplicaDataError,
            ScreenReplicaBudgetError,
        ) as error:
            raise ScreenApplicationError(503, "上个交易日的数据暂不可用，请稍后重试。") from error
        except duckdb.Error as error:
            raise ScreenApplicationError(503, "盘中数据暂不可用，请稍后重试。") from error
        except (ValueError, KeyError) as error:
            raise ScreenApplicationError(422, "所选盘中条件暂不可用，请刷新目录。") from error
        normalized = {
            **compiled.normalized_plan,
            "mode": "intraday",
            "cutoff": actual.cutoff.isoformat(),
            "source_identity": actual.source_identity,
            "daily_anchor_date": actual.daily_anchor_date.isoformat(),
        }
        return self._page_result(
            body,
            universe=universe,
            page_rules=page_rules,
            unknown_counts=unknown_counts,
            source=context.source,
            rule_labels=rule_labels,
            normalized_plan=normalized,
            complete=complete,
        )

    def _page_result(
        self,
        body: ScreenRunRequest,
        *,
        universe: pd.DataFrame,
        page_rules: list[Rule],
        unknown_counts: list[int],
        source: ScreenSourceInfo,
        rule_labels: list[str],
        normalized_plan: Mapping[str, object],
        complete: bool,
    ) -> ScreenRunData:
        ranking = body.ranking
        rank_columns = [condition.metric for condition in ranking.conditions] if ranking else []
        missing_metrics = [metric for metric in rank_columns if metric not in universe.columns]
        if missing_metrics:
            label = RANKING_METRIC_LABELS[missing_metrics[0]]
            raise ScreenApplicationError(
                422,
                f"当前数据还没有「{label}」，请换一个排名指标。",
            )
        if self.replica is not None and any(
            universe[metric].isna().all() for metric in rank_columns
        ):
            raise ScreenApplicationError(503, "当前排名数据不完整，请换一个指标或稍后重试。")

        ranking_unknown_count = 0
        try:
            ranked_input = None
            if ranking is not None:
                ranked_input, _ = screen_nl_projection(
                    universe,
                    trade_date=body.trade_date.isoformat(),
                    rules=page_rules,
                    rule_labels=rule_labels,
                    include_columns=rank_columns,
                )
                positive_metrics = [
                    condition.metric for condition in ranking.conditions if condition.weight > 0
                ]
                ranking_unknown_count = int(
                    ranked_input[positive_metrics]
                    .apply(lambda values: values.map(_number))
                    .isna()
                    .any(axis=1)
                    .sum()
                )
            page_args = dict(
                generation_id=source.identity,
                trade_date=body.trade_date.isoformat(),
                rules=page_rules,
                rule_labels=rule_labels,
                normalized_plan=normalized_plan,
                page_size=body.page_size,
                signing_key=self.cursor_key,
                cursor=body.cursor,
            )
            if complete:
                if len(universe) > 8000:
                    raise ScreenApplicationError(503, "本次结果超出保存范围，请收窄条件。")
                rank_args = (
                    [
                        RankingCondition(column=c.metric, ascending=c.ascending, weight=c.weight)
                        for c in ranking.conditions
                    ]
                    if ranking
                    else []
                )
                selected, diagnostics = screen_nl_projection(
                    universe,
                    trade_date=body.trade_date.isoformat(),
                    rules=page_rules,
                    rule_labels=rule_labels,
                    include_columns=rank_columns,
                )
                if ranking is not None:
                    selected = rank_screen_results(selected, rank_args, top_n=ranking.top_n)
                    selected["rank_position"] = range(1, len(selected) + 1)
                page = NlScreenPage(
                    rows=selected,
                    diagnostics=diagnostics,
                    start_cursor="",
                    next_cursor=None,
                    generation_id=source.identity,
                    query_digest=nl_screen_query_digest(normalized_plan),
                )
            elif ranking is None:
                page = paginate_nl_screen_projection(universe, **page_args)
            else:
                page = paginate_ranked_nl_screen_projection(
                    universe,
                    ranking=[
                        RankingCondition(
                            column=condition.metric,
                            ascending=condition.ascending,
                            weight=condition.weight,
                        )
                        for condition in ranking.conditions
                    ],
                    top_n=ranking.top_n,
                    **page_args,
                )
        except NlScreenPageError as error:
            raise ScreenApplicationError(
                409,
                "选股数据已更新，请重新筛选。" if self.replica else "数据已更新，请重新筛选。",
            ) from error
        except NlScreenProjectionFeatureError as error:
            raise ScreenApplicationError(
                422,
                "当前数据还不支持这个条件，请换一条或稍后重试。",
            ) from error
        except ValueError as error:
            raise ScreenApplicationError(
                422,
                "当前数据还不支持这个条件，请换一条或稍后重试。"
                if ranking is None
                else "当前数据还不支持这项排名，请换一个指标。",
            ) from error

        display = universe.set_index("ts_code")
        rows = [
            ScreenRow(
                ts_code=str(row["ts_code"]),
                name=str(row["name"]) if pd.notna(row["name"]) else None,
                close=_number(display.loc[row["ts_code"], "INTRADAY_PRICE[0]"])
                if body.mode == "intraday"
                else _number(row["CLOSE[0]"]),
                pct_chg=_number(display.loc[row["ts_code"], "INTRADAY_PCT_CHG[0]"])
                if body.mode == "intraday"
                else _number(row["PCT_CHG[0]"]),
                ranking_score=_number(row.get("ranking_score")),
                rank_position=int(row["rank_position"]) if "rank_position" in row else None,
            )
            for row in page.rows.to_dict(orient="records")
        ]
        steps = [
            ScreenStep(label=label, count=count, unknown_count=unknown)
            for (label, count), unknown in zip(page.diagnostics, unknown_counts, strict=True)
        ]
        total = steps[-1].count if steps else len(universe)
        return ScreenRunData(
            trade_date=body.trade_date,
            status="ready",
            base_count=len(universe),
            total=total,
            unknown_count=(steps[-1].unknown_count if steps else 0) + ranking_unknown_count,
            ranked_count=min(total, ranking.top_n) if ranking is not None else None,
            steps=steps,
            rows=rows,
            next_cursor=page.next_cursor,
            source=source,
        )
