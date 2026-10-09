import { type FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import type { Schemas } from "@/api/client";
import {
  type MinuteParameters,
  type MinuteStudyCreate,
  type MinuteStudyResult,
  type MinuteStudySource,
  type MinuteStudyTrial,
  minuteParameterCreateRecipe,
  restoreMinuteStudyRequest,
  submitMinuteStudy,
  useMinuteStudies,
  useMinuteStudyCapabilities,
  useMinuteStudyHeatmap,
  useMinuteStudyResult,
} from "@/api/minuteBacktests";
import { useCurrentMeta } from "@/api/useMeta";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import type { ChartColors } from "@/charts/tokens";
import { formatCount, formatNumber, formatPercent, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, EmptyState, PageSkeleton, Panel, SideDrawer, Tip } from "@/ui";
import {
  MinuteParameterControls,
  type MinuteParameterStudyField,
  minuteParameterStudyFields,
} from "./MinuteParameterControls";
import {
  MinuteStudyControls,
  type MinuteStudyDraft,
  type MinuteStudyMode,
} from "./MinuteStudyControls";
import { MinuteStudyViews, type StudyAxisOption, type StudyHeatmapView } from "./MinuteStudyViews";

const modes: readonly { key: MinuteStudyMode; label: string; detail: string }[] = [
  {
    key: "grid",
    label: "参数网格",
    detail: "Python 枚举有限取值。仅使用当时可见的训练资料评分与选择。",
  },
  {
    key: "random",
    label: "随机搜索",
    detail: "按原随机种子抽取有限参数组合，不用验证或最终测试收益选优。",
  },
  { key: "ablation", label: "五组消融", detail: "沿原成长板五组固定对照，只改规定的过滤项。" },
  {
    key: "walk_forward",
    label: "滚动分窗",
    detail: "每折独立冻结训练、验证与最终测试区间。缺交易日时不补齐窗口。",
  },
];
const nature: Record<Schemas["MinuteParameterFactSourceOption"]["source_nature"], string> = {
  real_retained: "真实留存",
  historical_reconstruction: "历史重建",
  synthetic_validation: "合成验证",
};
const ablations = [
  { key: "full", label: "完整策略", detail: "累计放量、同刻放量或五分加速、均价强度。" },
  { key: "no_vwap", label: "去掉均价强度", detail: "不要求信号价强于当日成交均价。" },
  { key: "no_same_minute", label: "去掉同刻放量", detail: "只保留累计放量与五分加速。" },
  { key: "no_accel_5m", label: "去掉五分加速", detail: "只保留累计放量与同刻放量。" },
  { key: "cum_only", label: "只看累计放量", detail: "同刻放量、五分加速、均价强度不参与过滤。" },
] as const;
const bounds = {
  topN: { min: 1, max: 500 },
  minTrades: { min: 1, max: Number.MAX_SAFE_INTEGER },
  randomTrials: { min: 1, max: 20_000 },
  seed: { min: 0, max: Number.MAX_SAFE_INTEGER },
  folds: { min: 1, max: 1_830 },
  minTrainingDates: { min: 1, max: 1_830 },
  validationDates: { min: 1, max: 1_830 },
  maxSearchFields: 64,
};
const sourceKey = (item: Schemas["MinuteParameterFactSourceOption"]) =>
  JSON.stringify([item.source_key, item.source_version, item.full_input_hash]);
const storageKey = (viewer: string) => `rquant.minute.study:${viewer}`;

function saved(viewer: string): { request: MinuteStudyCreate | null; error: string | null } {
  try {
    const raw = sessionStorage.getItem(storageKey(viewer));
    const request = restoreMinuteStudyRequest(raw);
    return {
      request,
      error: raw !== null && request === null ? "原研究请求无法读取，请核对保存内容。" : null,
    };
  } catch {
    return { request: null, error: "原研究请求无法读取，请恢复浏览器存储后重试。" };
  }
}
function sameJson(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (Array.isArray(a) || Array.isArray(b))
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((value, index) => sameJson(value, b[index]))
    );
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return false;
  const left = Object.entries(a);
  const right = new Map(Object.entries(b));
  return (
    left.length === right.size &&
    left.every(([key, value]) => right.has(key) && sameJson(value, right.get(key)))
  );
}
function sameRequest(a: MinuteStudyCreate, b: MinuteStudyCreate) {
  return sameJson(
    { ...a, search: a.search ?? null, walk_forward: a.walk_forward ?? null },
    { ...b, search: b.search ?? null, walk_forward: b.walk_forward ?? null },
  );
}
function studyStatus(status: Schemas["MinuteStudyResultData"]["status"]) {
  return {
    pending: "等待准备",
    processing: "正在准备",
    unknown: "待确认",
    submitted: "已提交",
    complete: "已完成",
    unavailable: "资料不足",
    failed: "处理失败",
    conflict: "内容冲突",
  }[status];
}
function trialStatus(state: MinuteStudyTrial["state"]) {
  return {
    not_prepared: "未准备",
    awaiting_submission_receipt: "待确认",
    pending: "待确认",
    queued: "等待运行",
    running: "正在运行",
    checkpointed: "正在运行",
    pending_seal: "正在保存",
    sealed: "已完成",
    failed: "运行失败",
    cancelled: "已取消",
    rejected: "未通过",
    unknown: "待确认",
  }[state];
}
function axisOption(field: MinuteParameterStudyField): StudyAxisOption {
  return {
    key: field.key,
    label: field.label,
    format:
      field.kind === "boolean"
        ? "boolean"
        : field.percent
          ? "ratio-percent"
          : field.integer
            ? "count"
            : "number",
  };
}
function valueText(
  value: Schemas["MinuteParameterSearchAxis"]["values"][number],
  field?: MinuteParameterStudyField,
) {
  if (value === null) return "null";
  if (Array.isArray(value)) return JSON.stringify(value);
  if (typeof value === "boolean") return String(value);
  return String(field?.percent ? value * 100 : value);
}
function restoreDraft(
  body: MinuteStudyCreate,
  fields: readonly MinuteParameterStudyField[],
): MinuteStudyDraft {
  const setting = body.settings[0];
  return {
    mode: body.mode === "single" ? "grid" : body.mode,
    scoreProfile: setting?.score_profile ?? "",
    topN: String(setting?.top_n ?? ""),
    minTrades: String(setting?.min_trades ?? ""),
    seed: String(body.random_seed),
    randomTrials: String(body.search?.requested_trials ?? ""),
    axes:
      body.search?.axes.map((axis) => {
        const field = fields.find((item) => item.key === axis.path);
        return {
          fieldKey: axis.path,
          inputMode: "values",
          valuesText: axis.values
            .map((value) => valueText(value, field))
            .join(field?.kind === "integer-list" ? ";" : ","),
          selectedValues: axis.values.map((value) => String(value)),
          minimum: "",
          maximum: "",
          step: "",
        };
      }) ?? [],
    windows: {
      folds: String(body.walk_forward?.fold_count ?? "3"),
      minTrainingDates: String(body.walk_forward?.min_training_dates ?? "2"),
      validationDates: String(body.walk_forward?.validation_date_count ?? "1"),
    },
  };
}

function searchValues(
  axis: MinuteStudyDraft["axes"][number],
  field: MinuteParameterStudyField,
): Schemas["MinuteParameterSearchAxis"]["values"] | null {
  if (axis.inputMode !== "values") return null;
  if (field.kind === "boolean") {
    return axis.selectedValues.length > 0 &&
      axis.selectedValues.every((value) => value === "true" || value === "false")
      ? axis.selectedValues.map((value) => value === "true")
      : null;
  }
  if (axis.valuesText.trim() === "") return null;
  const values: Schemas["MinuteParameterSearchAxis"]["values"] = [];
  for (const text of axis.valuesText.split(field.kind === "integer-list" ? ";" : /[,，]/)) {
    const trimmed = text.trim();
    if (trimmed === "null" && field.nullable) {
      values.push(null);
      continue;
    }
    if (trimmed === "") return null;
    if (field.kind === "integer-list") {
      try {
        const list: unknown = JSON.parse(trimmed);
        if (
          !Array.isArray(list) ||
          list.length === 0 ||
          !list.every(
            (value) => typeof value === "number" && Number.isSafeInteger(value) && value > 0,
          )
        )
          return null;
        values.push(list);
      } catch {
        return null;
      }
    } else {
      const value = Number(trimmed);
      if (!Number.isFinite(value) || (field.integer && !Number.isSafeInteger(value))) return null;
      values.push(field.percent ? value / 100 : value);
    }
  }
  return values;
}

function StudyNav({ label, daily }: { label: string; daily: Schemas["MinutePerformanceDay"][] }) {
  const build = useCallback(
    (colors: ChartColors): EChartOption => ({
      animation: false,
      tooltip: { trigger: "axis" },
      grid: { top: 16, right: 24, bottom: 32, left: 65 },
      xAxis: {
        type: "category",
        data: daily.map((point) => point.trade_date),
        boundaryGap: false,
        axisLabel: { color: colors.muted },
        axisLine: { lineStyle: { color: colors.rule } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { color: colors.muted },
        splitLine: { lineStyle: { color: colors.grid } },
      },
      series: [
        {
          type: "line",
          showSymbol: false,
          connectNulls: false,
          data: daily.map((point) => (point.nav === null ? null : Number(point.nav))),
          lineStyle: { color: colors.accent },
          itemStyle: { color: colors.accent },
        },
      ],
    }),
    [daily],
  );
  return <EChart label={label} build={build} />;
}

function WindowResult({
  label,
  data,
}: {
  label: string;
  data: Schemas["MinuteStudyWindowData"] | null | undefined;
}) {
  return (
    <Panel label={`${label}结果`} title={`${label}结果`}>
      {data == null ? (
        <EmptyState title="尚无窗口结果" />
      ) : (
        <>
          <p className="bt-runtime-note">
            {data.window.start_date} 至 {data.window.end_date}
            {data.status === "unavailable" ? " · 资料不足" : ""}
          </p>
          <div className="bt-parameter-grid">
            <span>
              闭环交易 <output aria-label="闭环交易">{formatCount(data.summary?.trades)}</output>
            </span>
            <span>
              平均交易收益{" "}
              <output aria-label="平均交易收益">{formatPercent(data.summary?.mean_ret_pct)}</output>
            </span>
            <span>
              胜率 <output aria-label="胜率">{formatPercent(data.summary?.win_rate_pct)}</output>
            </span>
            <span>
              最差交易{" "}
              <output aria-label="最差交易">{formatPercent(data.summary?.worst_ret_pct)}</output>
            </span>
            <span>
              跨窗交易{" "}
              <output aria-label="跨窗交易">{formatCount(data.cross_window_trades)}</output>
            </span>
          </div>
          {data.unavailable_reasons.length > 0 ? (
            <Tip content={data.unavailable_reasons.join("；")}>
              <span className="bt-context-tip">查看缺口</span>
            </Tip>
          ) : null}
          <StudyNav label={`${label}逐日净值`} daily={data.daily} />
          <details className="bt-parameter-section">
            <summary>逐日原值</summary>
            {data.daily.map((day) => (
              <p key={day.trade_date}>
                {day.trade_date} ·{" "}
                <output className="num" aria-label={`逐日净值 ${day.trade_date}`}>
                  {day.nav === null ? "—" : formatPrice(Number(day.nav))}
                </output>
                {day.status === "unavailable" ? " · 资料不足" : ""}
              </p>
            ))}
          </details>
        </>
      )}
    </Panel>
  );
}

function StudyResults({
  result,
  source,
  scopeKey,
  comparisonRequested,
  comparisonLoading,
  comparisonError,
  onCompare,
}: {
  result: MinuteStudyResult;
  source: MinuteStudySource | undefined;
  scopeKey: string;
  comparisonRequested: boolean;
  comparisonLoading: boolean;
  comparisonError: string | null;
  onCompare: () => void;
}) {
  const [selected, setSelected] = useState<number | null>(null);
  const [inspection, setInspection] = useState(false);
  const trial = result.trials.find((item) => item.index === selected) ?? result.trials[0] ?? null;
  const allFields = minuteParameterStudyFields(trial?.parameters ?? result.request.parameters);
  const fields = allFields.filter(
    (field) => field.kind !== "integer-list" && source?.heatmap_parameter_names.includes(field.key),
  );
  const [axes, setAxes] = useState<{ x: string; y: string } | null>(null);
  const x =
    axes?.x ??
    result.request.search?.axes.find((axis) => fields.some((field) => field.key === axis.path))
      ?.path ??
    fields[0]?.key ??
    null;
  const y =
    axes?.y ??
    result.request.search?.axes.find(
      (axis) => axis.path !== x && fields.some((field) => field.key === axis.path),
    )?.path ??
    fields.find((field) => field.key !== x)?.key ??
    null;
  const sealed = trial?.state === "sealed";
  const map = useMinuteStudyHeatmap(
    sealed ? result.command_id : null,
    result.plan_id ?? null,
    sealed ? trial.index : null,
    sealed ? (trial.study_id ?? null) : null,
    x,
    y,
    JSON.stringify([
      trial?.result_hash,
      result.trials.filter((item) => item.state === "sealed").length,
      result.missing_trial_indices.length,
    ]),
  );
  const mapData = map.error === null ? map.data : undefined;
  const heatmap: StudyHeatmapView | null =
    mapData == null
      ? null
      : {
          resultKey: JSON.stringify([
            mapData.command_id,
            mapData.plan_id,
            mapData.heatmap.trial_set_hash,
          ]),
          axes: fields.map(axisOption),
          xAxis: {
            key: mapData.heatmap.x_axis.parameter_name,
            values: mapData.heatmap.x_axis.values,
          },
          yAxis: {
            key: mapData.heatmap.y_axis.parameter_name,
            values: mapData.heatmap.y_axis.values,
          },
          cells: mapData.heatmap.cells.map((cell) => ({
            xIndex: cell.x_index,
            yIndex: cell.y_index,
            studyId: cell.study_id,
            isCurrent: cell.is_current,
            status: cell.status,
            score: cell.training_score,
            trades: cell.observation?.summary.trades ?? null,
            neighborhoodMinimum: cell.neighborhood.minimum_score,
            neighborhoodReason: {
              complete: "原邻域完整",
              center_unavailable: "当前格不可用",
              missing_neighbors: "邻域有缺口",
              no_neighbors: "没有合法相邻格",
            }[cell.neighborhood.reason],
            detail:
              cell.protocol === null
                ? null
                : `选择时点：${formatShanghaiDateTime(mapData.heatmap.selection_cutoff)}；参数依据：${JSON.stringify(cell.protocol.parameters)}`,
          })),
        };
  const columns = useMemo<readonly DataColumn<MinuteStudyTrial>[]>(
    () => [
      {
        id: "label",
        header: "方案",
        value: (row) => row.label,
        wrap: true,
        cell: (row) => (
          <Button
            size="sm"
            onClick={() => {
              setSelected(row.index);
              setAxes(null);
              setInspection(false);
            }}
          >
            查看{row.label}
          </Button>
        ),
      },
      { id: "status", header: "状态", value: (row) => trialStatus(row.state) },
      {
        id: "score",
        header: "训练评分",
        value: (row) => row.training_rank?.training_score ?? null,
        numeric: true,
        cell: (row) => formatNumber(row.training_rank?.training_score, 4),
      },
      {
        id: "trades",
        header: "训练交易",
        value: (row) => row.training?.summary?.trades ?? null,
        numeric: true,
        secondary: true,
        cell: (row) => formatCount(row.training?.summary?.trades),
      },
    ],
    [],
  );
  const hasSealedResults = result.trials.some((item) => item.state === "sealed");
  const insufficientTraining =
    hasSealedResults && result.unavailable_reasons.includes("insufficient_training_trades");
  const unavailableDetail = result.unavailable_reasons
    .map((reason) =>
      reason === "insufficient_training_trades" ? "训练交易不足，暂不能排名" : reason,
    )
    .join("；");
  const viewState =
    result.status === "failed" || result.status === "conflict"
      ? "failed"
      : hasSealedResults
        ? "completed"
        : result.status === "unavailable"
          ? "unavailable"
          : "pending";
  return (
    <div className="bt-runtime">
      <Panel title="研究结果" sub={studyStatus(result.status)}>
        {result.trial_count != null ? (
          <p>
            {formatCount(result.trials.length)} / {formatCount(result.trial_count)} 组结果
          </p>
        ) : null}
        {result.missing_trial_indices.length > 0 ? <p>部分方案尚无完整结果。</p> : null}
        {insufficientTraining ? <p role="status">训练交易不足，暂不能排名</p> : null}
        {result.message || result.unavailable_reasons.length > 0 ? (
          <Tip content={[result.message, unavailableDetail].filter(Boolean).join("；")}>
            <span className="bt-context-tip">查看详情</span>
          </Tip>
        ) : null}
        <DataTable
          label="研究方案"
          rows={result.trials}
          columns={columns}
          rowKey={(row) => String(row.index)}
          emptyText="研究正在准备"
        />
      </Panel>
      {hasSealedResults && !comparisonRequested ? (
        <Button onClick={onCompare}>查看参数对照</Button>
      ) : null}
      {comparisonLoading ? <PageSkeleton label="正在读取参数对照" /> : null}
      {comparisonError === null ? null : (
        <>
          <EmptyState
            title="参数对照暂不可用"
            hint={
              <Tip content={comparisonError}>
                <span>查看原因</span>
              </Tip>
            }
          />
          <Button onClick={onCompare}>重试参数对照</Button>
        </>
      )}
      <MinuteStudyViews
        state={viewState}
        scopeKey={scopeKey}
        heatmap={heatmap}
        ablationReturnLabel="训练均收益"
        foldReturnLabel="测试均收益"
        ablations={
          result.request.mode === "ablation"
            ? result.trials.map((item) => ({
                key: String(item.index),
                label: item.label,
                status: item.state === "sealed" ? "available" : "unavailable",
                score: item.training_rank?.training_score ?? null,
                returnPercent: item.training?.summary?.mean_ret_pct ?? null,
                trades: item.training?.summary?.trades ?? null,
                detail: `训练平均交易收益：${formatPercent(item.training?.summary?.mean_ret_pct)}；${item.unavailable_reasons.join("；")}`,
              }))
            : []
        }
        folds={
          result.request.mode === "walk_forward"
            ? result.trials.map((item) => ({
                key: String(item.index),
                label: item.label,
                status: item.state === "sealed" ? "available" : "unavailable",
                trainStart: item.protocol.train_range.start_date,
                trainEnd: item.protocol.train_range.end_date,
                validationStart: item.protocol.validation_range.start_date,
                validationEnd: item.protocol.validation_range.end_date,
                testStart: item.protocol.frozen_outer_test_range.start_date,
                testEnd: item.protocol.frozen_outer_test_range.end_date,
                trainingScore: item.training_rank?.training_score ?? null,
                validationScore: null,
                testScore: null,
                testReturnPercent: item.out_of_sample?.summary?.mean_ret_pct ?? null,
                testTrades: item.out_of_sample?.summary?.trades ?? null,
                detail: `最终测试平均交易收益：${formatPercent(item.out_of_sample?.summary?.mean_ret_pct)}；${item.unavailable_reasons.join("；")}`,
              }))
            : []
        }
        detail={unavailableDetail}
        onAxesChange={(nextX, nextY) => setAxes({ x: nextX, y: nextY })}
      />
      {map.isFetching ? <p role="status">正在请求原研究图…</p> : null}
      {map.error ? (
        <p role="status">
          参数对照暂不可用。
          <Tip content={map.error.message}>
            <span className="bt-context-tip">查看原因</span>
          </Tip>
        </p>
      ) : null}
      {trial !== null ? (
        <Panel title="所选方案" sub={trial.label}>
          <Button onClick={() => setInspection(true)}>查看参数与依据</Button>
        </Panel>
      ) : null}
      <SideDrawer
        open={inspection && trial !== null}
        onClose={() => setInspection(false)}
        title="研究依据"
        wide
      >
        {inspection && trial !== null ? (
          <>
            <MinuteParameterControls
              value={trial.parameters}
              frequency={trial.parameters.parameters.freq}
              supported={[]}
              disabled
              onChange={() => undefined}
              onValidityChange={() => undefined}
            />
            <pre className="bt-runtime-proof">{JSON.stringify(trial, null, 2)}</pre>
          </>
        ) : null}
      </SideDrawer>
      {sealed ? (
        <>
          <WindowResult label="训练" data={trial.training} />
          <WindowResult label="验证" data={trial.validation} />
          <WindowResult label="最终测试" data={trial.out_of_sample} />
        </>
      ) : null}
    </div>
  );
}

function Workspace({ viewer, identity }: { viewer: string; identity: string }) {
  const [recovery] = useState(() => saved(viewer));
  const [pending, setPending] = useState(recovery.request);
  const [configurationOpen, setConfigurationOpen] = useState(
    recovery.request !== null || recovery.error !== null,
  );
  const [configurationVisited, setConfigurationVisited] = useState(configurationOpen);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(recovery.error);
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [cursor, setCursor] = useState<string | null>(null);
  const listing = useMinuteStudies(cursor, refresh);
  const [command, setCommand] = useState<string | null>(pending?.command_id ?? null);
  const [poll, setPoll] = useState(true);
  const result = useMinuteStudyResult(command, poll);
  const [comparisonCommand, setComparisonCommand] = useState<string | null>(null);
  const comparisonRequested =
    result.data !== undefined &&
    result.data.trials.some((trial) => trial.state === "sealed") &&
    (result.data.request.mode === "grid" ||
      result.data.request.mode === "random" ||
      comparisonCommand === result.data.command_id);
  const capabilitiesNeeded = configurationOpen || pending !== null || comparisonRequested;
  const capabilities = useMinuteStudyCapabilities(capabilitiesNeeded);
  const [choice, setChoice] = useState<string | null>(null);
  const sources = capabilities.data?.sources ?? [];
  const source =
    pending === null
      ? (sources.find((item) => sourceKey(item.source) === choice) ?? sources[0])
      : sources.find(
          (item) =>
            item.source.source_key === pending.source_key &&
            item.source.source_version === pending.source_version &&
            item.source.full_input_hash === pending.full_input_hash,
        );
  const [family, setFamily] = useState<MinuteParameters["parameters"]["family"] | null>(null);
  const selectedFamily =
    source?.source.capabilities.find(
      (item) => item.family === (pending?.parameters.parameters.family ?? family),
    ) ?? source?.source.capabilities[0];
  const baseIdentity = JSON.stringify([
    identity,
    source === undefined ? null : sourceKey(source.source),
    selectedFamily?.family,
  ]);
  const [edited, setEdited] = useState<{ identity: string; value: MinuteParameters } | null>(null);
  const parameters =
    pending?.parameters ??
    (edited?.identity === baseIdentity ? edited.value : selectedFamily?.default_parameters);
  const [parametersValid, setParametersValid] = useState(true);
  const [dates, setDates] = useState({
    trainStart: "",
    trainEnd: "",
    validationStart: "",
    validationEnd: "",
    testStart: "",
    testEnd: "",
  });
  const displayedDates =
    pending === null
      ? dates
      : {
          trainStart: pending.protocol.train_range.start_date,
          trainEnd: pending.protocol.train_range.end_date,
          validationStart: pending.protocol.validation_range.start_date,
          validationEnd: pending.protocol.validation_range.end_date,
          testStart: pending.protocol.frozen_outer_test_range.start_date,
          testEnd: pending.protocol.frozen_outer_test_range.end_date,
        };
  const fields =
    parameters === undefined
      ? []
      : minuteParameterStudyFields(parameters).filter(
          (field) =>
            pending?.search?.axes.some((axis) => axis.path === field.key) ||
            (source?.searchable_parameter_names.includes(field.key) &&
              selectedFamily?.supported_parameter_names.includes(field.key)),
        );
  const draftIdentity = JSON.stringify([identity, pending?.command_id ?? baseIdentity]);
  const [draft, setDraft] = useState<{
    identity: string;
    value: MinuteStudyDraft;
    complete: boolean;
  } | null>(null);
  const defaultDraft: MinuteStudyDraft = {
    mode:
      modes.find(
        (item) =>
          source?.modes.includes(item.key) &&
          (item.key !== "ablation" || parameters?.parameters.family === "growth_board_surge"),
      )?.key ?? "grid",
    scoreProfile: source?.score_profiles.find((profile) => profile.available)?.name ?? "",
    topN: "10",
    minTrades: "30",
    randomTrials: "20",
    seed: "0",
    axes:
      fields[0] === undefined
        ? []
        : [
            {
              fieldKey: fields[0].key,
              inputMode: "values",
              valuesText: "",
              selectedValues: [],
              minimum: "",
              maximum: "",
              step: "",
            },
          ],
    windows: { folds: "3", minTrainingDates: "2", validationDates: "1" },
  };
  const currentDraft = draft?.identity === draftIdentity ? draft : null;
  const selectedResultSource =
    result.data === undefined
      ? undefined
      : sources.find(
          (item) =>
            item.source.source_key === result.data?.request.source_key &&
            item.source.source_version === result.data.request.source_version &&
            item.source.full_input_hash === result.data.request.full_input_hash,
        );
  const canRun =
    capabilities.error === null &&
    capabilities.data?.available === true &&
    capabilities.data.can_run &&
    source !== undefined &&
    selectedFamily !== undefined &&
    source.unavailable_reasons.length === 0 &&
    source.source.unavailable_reasons.length === 0 &&
    selectedFamily.unavailable_reasons.length === 0;
  const locked = pending !== null || busy || (error === recovery.error && recovery.error !== null);
  const datesValid =
    source !== undefined &&
    Object.values(displayedDates).every(
      (date) => date >= source.source.start_date && date <= source.source.end_date,
    ) &&
    displayedDates.trainStart <= displayedDates.trainEnd &&
    displayedDates.trainEnd < displayedDates.validationStart &&
    displayedDates.validationStart <= displayedDates.validationEnd &&
    displayedDates.validationEnd < displayedDates.testStart &&
    displayedDates.testStart <= displayedDates.testEnd;
  const resultMatchesPending =
    pending === null ||
    result.data === undefined ||
    result.data.command_id !== pending.command_id ||
    sameRequest(pending, result.data.request);

  useEffect(() => {
    if (
      result.data !== undefined &&
      ["complete", "unavailable", "failed", "conflict"].includes(result.data.status)
    )
      setPoll(false);
  }, [result.data]);
  useEffect(() => {
    if (
      pending === null ||
      result.data?.command_id !== pending.command_id ||
      !sameRequest(pending, result.data.request) ||
      !["submitted", "complete", "unavailable", "failed", "conflict"].includes(result.data.status)
    )
      return;
    try {
      sessionStorage.removeItem(storageKey(viewer));
      setPending(null);
    } catch {
      setError("原研究请求无法清理，请恢复浏览器存储后重试。");
    }
  }, [pending, result.data, viewer]);

  async function send(body: MinuteStudyCreate) {
    if (!capabilities.data?.can_run || capabilities.error !== null || busy) return;
    setBusy(true);
    setError(null);
    try {
      sessionStorage.setItem(storageKey(viewer), JSON.stringify(body));
    } catch {
      setPending(body);
      setError("原请求未保存，未发送研究。请恢复浏览器存储后重试。");
      setBusy(false);
      return;
    }
    setPending(body);
    setCommand(body.command_id);
    setPoll(true);
    try {
      const receipt = await submitMinuteStudy(body);
      setMessage(studyStatus(receipt.status));
      if (["submitted", "unavailable", "failed", "conflict"].includes(receipt.status)) {
        try {
          sessionStorage.removeItem(storageKey(viewer));
          setPending(null);
        } catch {
          setError("原研究请求无法清理，请恢复浏览器存储后重试。");
        }
      }
      setRefresh((previous) => previous + 1);
      result.refetch();
    } catch (failure) {
      setMessage(failure instanceof Error ? failure.message : "提交状态待确认，请重试原请求。");
    } finally {
      setBusy(false);
    }
  }
  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (
      !canRun ||
      locked ||
      !datesValid ||
      !parametersValid ||
      parameters === undefined ||
      currentDraft?.complete !== true ||
      source === undefined
    )
      return;
    const value = currentDraft.value;
    const axes: Schemas["MinuteParameterSearchAxis"][] = [];
    if (value.mode === "grid" || value.mode === "random") {
      for (const axis of value.axes) {
        const field = fields.find((item) => item.key === axis.fieldKey);
        const values = field === undefined ? null : searchValues(axis, field);
        if (values === null) {
          setError("参数取值不完整，请检查原单位与有限取值。");
          return;
        }
        axes.push({ path: axis.fieldKey, values });
      }
    }
    const at = new Date();
    const createParameters = minuteParameterCreateRecipe(parameters);
    const body: MinuteStudyCreate = {
      command_id: crypto.randomUUID(),
      requested_at: at.toISOString(),
      source_key: source.source.source_key,
      source_version: source.source.source_version,
      full_input_hash: source.source.full_input_hash,
      parameters: createParameters,
      protocol: {
        train_range: { start_date: dates.trainStart, end_date: dates.trainEnd },
        validation_range: { start_date: dates.validationStart, end_date: dates.validationEnd },
        frozen_outer_test_range: { start_date: dates.testStart, end_date: dates.testEnd },
      },
      settings: [
        {
          score_profile: value.scoreProfile,
          top_n: Number(value.topN),
          min_trades: Number(value.minTrades),
        },
      ],
      random_seed: Number(value.seed),
      deadline: new Date(at.getTime() + 24 * 60 * 60 * 1000).toISOString(),
      mode: value.mode,
      search:
        value.mode === "grid" || value.mode === "random"
          ? {
              base: createParameters,
              axes,
              mode: value.mode,
              seed: Number(value.seed),
              requested_trials: value.mode === "random" ? Number(value.randomTrials) : null,
            }
          : null,
      walk_forward:
        value.mode === "walk_forward"
          ? {
              fold_count: Number(value.windows.folds),
              min_training_dates: Number(value.windows.minTrainingDates),
              validation_date_count: Number(value.windows.validationDates),
            }
          : null,
    };
    if (restoreMinuteStudyRequest(JSON.stringify(body)) === null) {
      setError("原请求不完整或超过容量，请减少取值后重试。");
      return;
    }
    void send(body);
  }
  const listColumns = useMemo<readonly DataColumn<Schemas["MinuteStudyListItem"]>[]>(
    () => [
      {
        id: "name",
        header: "研究",
        value: (row) => row.display_name,
        wrap: true,
        cell: (row) => (
          <Button
            size="sm"
            onClick={() => {
              setCommand(row.command_id);
              setPoll(true);
            }}
          >
            查看{row.display_name}
          </Button>
        ),
      },
      { id: "status", header: "状态", value: (row) => studyStatus(row.status) },
      {
        id: "count",
        header: "方案",
        value: (row) => row.trial_count ?? null,
        numeric: true,
        secondary: true,
        cell: (row) => formatCount(row.trial_count),
      },
      {
        id: "time",
        header: "申请时间",
        value: (row) => row.requested_at,
        secondary: true,
        cell: (row) => formatShanghaiDateTime(row.requested_at),
      },
    ],
    [],
  );
  return (
    <section aria-label="分钟参数研究" className="bt-runtime">
      <Panel
        title="参数研究"
        actions={
          <Button
            disabled={busy || pending !== null}
            aria-expanded={configurationOpen}
            aria-controls="minute-study-configuration"
            onClick={() => {
              setConfigurationVisited(true);
              setConfigurationOpen((previous) => !previous);
            }}
          >
            {configurationOpen ? "收起配置" : "新建研究"}
          </Button>
        }
      >
        {configurationVisited ? (
          <div id="minute-study-configuration" hidden={!configurationOpen}>
            <form aria-label="研究配置" onSubmit={submit}>
              <fieldset disabled={locked || capabilities.isLoading} className="bt-runtime-fields">
                <legend className="sr-only">研究来源与区间</legend>
                <label className="bt-runtime-source">
                  研究资料
                  <select
                    className="inp"
                    aria-label="研究资料"
                    value={source === undefined ? "" : sourceKey(source.source)}
                    onChange={(event) => {
                      setChoice(event.target.value);
                      setFamily(null);
                      setEdited(null);
                      setDates({
                        trainStart: "",
                        trainEnd: "",
                        validationStart: "",
                        validationEnd: "",
                        testStart: "",
                        testEnd: "",
                      });
                      setError(null);
                    }}
                  >
                    {source === undefined ? (
                      <option value="">
                        {pending === null ? "暂无可用研究资料" : "原研究资料"}
                      </option>
                    ) : null}
                    {sources.map((item) => (
                      <option key={sourceKey(item.source)} value={sourceKey(item.source)}>
                        {item.source.display_name} · {nature[item.source.source_nature]} ·{" "}
                        {item.source.frequency.replace("min", " 分钟")}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="bt-runtime-source">
                  研究策略族
                  <select
                    className="inp"
                    aria-label="研究策略族"
                    value={parameters?.parameters.family ?? ""}
                    onChange={(event) => {
                      const next = source?.source.capabilities.find(
                        (item) => item.family === event.target.value,
                      );
                      if (next) {
                        setFamily(next.family);
                        setEdited(null);
                        setParametersValid(true);
                        setError(null);
                      }
                    }}
                  >
                    {source === undefined && pending !== null ? (
                      <option value={pending.parameters.parameters.family}>原研究配置</option>
                    ) : null}
                    {source?.source.capabilities.map((item) => (
                      <option key={item.family} value={item.family}>
                        {item.display_name}
                      </option>
                    ))}
                  </select>
                </label>
                {(
                  [
                    ["trainStart", "研究训练开始"],
                    ["trainEnd", "研究训练结束"],
                    ["validationStart", "研究验证开始"],
                    ["validationEnd", "研究验证结束"],
                    ["testStart", "研究最终测试开始"],
                    ["testEnd", "研究最终测试结束"],
                  ] as const
                ).map(([key, label]) => (
                  <label className="bt-parameter-field" key={key}>
                    {label}
                    <input
                      className="inp"
                      aria-label={label}
                      type="date"
                      value={displayedDates[key]}
                      min={source?.source.start_date}
                      max={source?.source.end_date}
                      onChange={(event) =>
                        setDates((previous) => ({ ...previous, [key]: event.target.value }))
                      }
                    />
                  </label>
                ))}
              </fieldset>
              {source === undefined && pending !== null ? (
                <p className="bt-runtime-note">
                  原研究资料不在当前可用来源中。
                  <Tip
                    content={`原来源：${pending.source_key} · ${pending.source_version} · ${pending.full_input_hash}`}
                  >
                    <span className="bt-context-tip">原来源依据</span>
                  </Tip>
                </p>
              ) : source === undefined ? null : (
                <p className="bt-runtime-note">
                  {nature[source.source.source_nature]} · {source.source.start_date} 至{" "}
                  {source.source.end_date}。
                  <Tip
                    content={
                      <>
                        <div>{source.source.provenance.visibility_limitations}</div>
                        <div>
                          来源依据：{source.source.source_key} · {source.source.full_input_hash}
                        </div>
                      </>
                    }
                  >
                    <span className="bt-context-tip">来源说明</span>
                  </Tip>
                </p>
              )}
              {parameters === undefined ? null : (
                <MinuteParameterControls
                  key={baseIdentity}
                  value={parameters}
                  frequency={
                    pending?.parameters.parameters.freq ??
                    source?.source.frequency ??
                    parameters.parameters.freq
                  }
                  supported={selectedFamily?.supported_parameter_names ?? []}
                  disabled={locked || !canRun}
                  onChange={(value) => setEdited({ identity: baseIdentity, value })}
                  onValidityChange={setParametersValid}
                />
              )}
              <MinuteStudyControls
                scopeKey={draftIdentity}
                draftKey={pending?.command_id}
                defaultDraft={defaultDraft}
                initialDraft={pending === null ? null : restoreDraft(pending, fields)}
                status={
                  busy
                    ? "submitting"
                    : pending !== null
                      ? "pending"
                      : capabilities.isLoading
                        ? "loading"
                        : !canRun
                          ? "unavailable"
                          : error !== null
                            ? "failed"
                            : "ready"
                }
                message={!canRun ? "研究暂不可用" : null}
                detail={[
                  capabilities.error?.message,
                  capabilities.data?.message,
                  ...(source?.unavailable_reasons ?? []),
                  ...(selectedFamily?.unavailable_reasons ?? []),
                ]
                  .filter(Boolean)
                  .join("；")}
                modes={modes.map((item) => ({
                  ...item,
                  available:
                    source?.modes.includes(item.key) === true &&
                    (item.key !== "ablation" ||
                      parameters?.parameters.family === "growth_board_surge"),
                }))}
                scores={
                  source?.score_profiles.map((profile) => ({
                    key: profile.name,
                    label: profile.label,
                    detail: profile.missing_features.join("；") || null,
                    available: profile.available,
                  })) ??
                  pending?.settings.map((setting) => ({
                    key: setting.score_profile,
                    label: "原评分",
                    detail: setting.score_profile,
                  })) ??
                  []
                }
                fields={fields.map((field) => ({
                  key: field.key,
                  label: field.label,
                  kind: field.kind === "boolean" ? "choice" : "number",
                  available: true,
                  inputModes: ["values"],
                  choices:
                    field.kind === "boolean"
                      ? [
                          { value: "false", label: "关闭" },
                          { value: "true", label: "开启" },
                        ]
                      : undefined,
                  detail:
                    field.kind === "integer-list"
                      ? "每组回看天数用方括号填写，多组用分号分隔。如 [1,3];[5,10]。"
                      : field.percent
                        ? "按标注百分比输入；发送时保留原比例单位。"
                        : "用逗号分隔有限取值，由原研究程序检查和枚举。",
                }))}
                ablations={parameters?.parameters.family === "growth_board_surge" ? ablations : []}
                bounds={bounds}
                onDraftChange={(value, complete) =>
                  setDraft({ identity: draftIdentity, value, complete })
                }
              />
              <div className="bt-runtime-actions">
                <Button
                  type="submit"
                  disabled={
                    !canRun ||
                    locked ||
                    !datesValid ||
                    !parametersValid ||
                    currentDraft?.complete !== true
                  }
                >
                  开始研究
                </Button>
                {pending === null ? null : (
                  <Button
                    disabled={
                      busy || capabilities.data?.can_run !== true || capabilities.error !== null
                    }
                    onClick={() => void send(pending)}
                  >
                    重试原研究
                  </Button>
                )}
              </div>
              {message === null ? null : <p role="status">{message}</p>}
              {error === null ? null : <p role="alert">{error}</p>}
            </form>
          </div>
        ) : null}
      </Panel>
      <Panel title="研究记录" flush>
        {listing.error ? (
          <EmptyState title="研究记录暂不可用" />
        ) : listing.isLoading ? (
          <PageSkeleton label="正在读取研究记录" />
        ) : (
          <DataTable
            label="研究记录"
            rows={listing.data?.studies ?? []}
            columns={listColumns}
            rowKey={(row) => row.command_id}
            emptyText="暂无研究请求"
          />
        )}
        <div className="bt-runtime-actions">
          <Button
            disabled={busy}
            onClick={() => {
              if (capabilitiesNeeded) capabilities.refetch();
              listing.refetch();
              if (command !== null) result.refetch();
            }}
          >
            刷新研究
          </Button>
          {cursor !== null ? (
            <Button size="sm" onClick={() => setCursor(null)}>
              返回第一页
            </Button>
          ) : null}
          <Button
            size="sm"
            disabled={listing.data?.next_cursor == null || listing.isFetching}
            onClick={() => setCursor(listing.data?.next_cursor ?? null)}
          >
            下一页研究
          </Button>
        </div>
      </Panel>
      {command === null ? null : !resultMatchesPending ? (
        <EmptyState title="原研究内容不一致" hint="请核对已保存的原请求。" />
      ) : result.error ? (
        <EmptyState
          title="原研究暂不可用"
          hint={
            <Tip content={result.error.message}>
              <span>查看原因</span>
            </Tip>
          }
        />
      ) : result.isLoading ? (
        <PageSkeleton label="正在加载原研究" />
      ) : result.data === undefined ? null : (
        <StudyResults
          key={JSON.stringify([identity, result.data.command_id, result.data.plan_id])}
          result={result.data}
          source={comparisonRequested ? selectedResultSource : undefined}
          scopeKey={JSON.stringify([identity, result.data.command_id, result.data.plan_id])}
          comparisonRequested={comparisonRequested}
          comparisonLoading={comparisonRequested && capabilities.isLoading}
          comparisonError={
            !comparisonRequested
              ? null
              : (capabilities.error?.message ??
                (capabilities.data !== undefined &&
                (!capabilities.data.available || selectedResultSource === undefined)
                  ? (capabilities.data.message ?? "原研究资料不在当前可用来源中。")
                  : null))
          }
          onCompare={() => {
            if (comparisonRequested) capabilities.refetch();
            else setComparisonCommand(result.data?.command_id ?? null);
          }}
        />
      )}
    </section>
  );
}

export function MinuteStudyWorkspace() {
  const meta = useCurrentMeta();
  if (meta.isLoading) return <PageSkeleton label="正在读取账号" />;
  const viewer = meta.error === null ? (meta.data?.data.viewer ?? null) : null;
  const generation = meta.data?.data.generation?.generation_id ?? null;
  if (viewer === null || generation === null)
    return <EmptyState title="账号暂不可用" hint="恢复账号后可查看原研究。" />;
  const identity = JSON.stringify([viewer, generation]);
  return <Workspace key={identity} viewer={viewer} identity={identity} />;
}
