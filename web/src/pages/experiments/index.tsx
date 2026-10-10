import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type ExperimentCapabilities,
  type ExperimentComparison,
  type ExperimentFamily,
  type ExperimentItem,
  type ExperimentMetric,
  type ExperimentResult,
  type ExperimentSearch,
  type ExperimentWrite,
  type FormalExperiment,
  isNativeExperimentConfiguration,
  type NativeExperimentConfiguration,
  submitExperiment,
  useExperimentCapabilities,
  useExperimentComparison,
  useExperimentFamily,
  useExperimentHeatmap,
  useExperimentResult,
  useExperimentStatistics,
  useExperiments,
  useMyExperiments,
} from "@/api/experiments";
import { useCurrentMeta } from "@/api/useMeta";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import type { ChartColors } from "@/charts/tokens";
import { formatCount, formatNumber, formatPercent } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { ACTION_COPY, TemplateRulesSummary } from "@/pages/strategies/TemplateRules";
import type { TemplateDetail } from "@/pages/strategies/templateApi";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ChangeText,
  ConfirmDialog,
  EmptyState,
  PageHeader,
  PageSkeleton,
  Panel,
  Pill,
  RelativeTime,
  SideDrawer,
  Tip,
} from "@/ui";
import "./experiments.css";
import "./nativeExperiment.css";
import { ExperimentTemplatePicker } from "./TemplatePicker";

const statusLabel: Record<
  ExperimentItem["status"],
  { label: string; kind: "ok" | "warn" | "crit" | "idle" | "acc" }
> = {
  registered: { label: "已登记", kind: "idle" },
  running: { label: "运行中", kind: "acc" },
  executed: { label: "结果待确认", kind: "warn" },
  succeeded: { label: "已完成", kind: "ok" },
  failed: { label: "未完成", kind: "crit" },
  cancelled: { label: "已取消", kind: "idle" },
};

function coverageStartText(at: string | null): string {
  if (!at || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(at)) {
    return "覆盖起点暂不可用";
  }
  const date = new Date(at);
  if (!Number.isFinite(date.getTime())) return "覆盖起点暂不可用";
  return `最早登记于 ${formatShanghaiDateTime(date).slice(0, 16)}（北京时间）`;
}

const parameters = {
  "weight_rule.max_positions": "最多持仓",
  "weight_rule.max_stock_weight": "单股上限",
  "weight_rule.cash_reserve": "现金保留",
  "weight_rule.min_target_amount": "最小买入金额",
  "rebalance_rule.every_n_days": "调仓间隔",
} as const;
type Parameter = keyof typeof parameters;
const phaseNames = { training: "训练", validation: "验证", outer: "样本外" } as const;

const comparisonParameters: Record<string, string> = {
  ...parameters,
  "template.entry.kind": "入场方式",
  "template.entry.pool_key": "股票池",
  "template.entry.strategy_id": "策略信号",
  "template.entry.version": "来源版本",
  "template.entry.body_hash": "股票池来源",
  "template.entry.source_hash": "信号来源",
  "template.entry.action": "信号动作",
  "template.exit.stop_loss": "止损",
  "template.exit.take_profit": "止盈",
  "template.exit.trailing_profit": "移动止盈",
  "template.exit.max_holding_days": "持有上限",
  "template.exit.exit_time": "定时退出",
  "template.index_filter.benchmark_code": "过滤指数",
  "template.index_filter.ma_days": "指数均线",
  "template.index_filter.direction": "指数条件",
};

function comparisonParameter(path: string): string {
  const known = comparisonParameters[path];
  if (known) return known;
  const condition = /^template\.entry\.conditions\[(\d+)\]\.(key|args\..+)$/.exec(path);
  if (condition)
    return `入场条件 ${Number(condition[1]) + 1}${condition[2] === "key" ? "" : " · 参数"}`;
  return "其他设置";
}

function comparisonValue(path: string, value: string | null): string | null {
  if (!path.startsWith("template.")) return value;
  if (value === null) return "未启用";
  if (/^template\.exit\.(stop_loss|take_profit|trailing_profit)$/.test(path))
    return formatPercent(Number(value) * 100);
  if (path === "template.exit.max_holding_days") return `${value} 个交易日`;
  if (path === "template.index_filter.ma_days") return `${value} 日`;
  if (path === "template.exit.exit_time" || path === "template.index_filter.benchmark_code")
    return value;
  if (path === "template.entry.kind")
    return (
      ({ pool: "股票池", conditions: "条件筛选", signal: "策略信号" } as Record<string, string>)[
        value
      ] ?? "查看设置"
    );
  if (path === "template.index_filter.direction")
    return value === "above" ? "高于均线" : "低于均线";
  if (path === "template.entry.action")
    return ACTION_COPY[value as keyof typeof ACTION_COPY] ?? "查看设置";
  if (/(_hash|pool_key|strategy_id)$/.test(path)) return "已绑定";
  if (/^template\.entry\.conditions\[\d+\]\.key$/.test(path))
    return value === "not_st" ? "非 ST" : "原筛选条件";
  return Number.isFinite(Number(value)) ? value : "查看设置";
}

function metricText(metric: ExperimentMetric): string {
  if (metric.value === null) return "—";
  if (metric.unit === "percent") return formatPercent(metric.value * 100);
  if (metric.unit === "count" || metric.unit === "days") return formatCount(metric.value);
  return metric.value.toLocaleString("zh-CN", { maximumFractionDigits: 4 });
}

function parameterSummary(config: FormalExperiment["configuration"]): string {
  if (isNativeExperimentConfiguration(config))
    return `分钟策略 · 第 ${config.selection.target.head.version} 版`;
  return `${config.weight_rule.max_positions} 只 · 现金 ${formatPercent(Number(config.weight_rule.cash_reserve) * 100)}`;
}

function Metrics({ values }: { values: ExperimentMetric[] }) {
  return (
    <dl className="exp-metrics">
      {values.map((value) => (
        <div key={value.key}>
          <dt>{value.label}</dt>
          <dd className="num">{metricText(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function RowParameters({
  item,
  interactiveReady = true,
}: {
  item: Pick<FormalExperiment, "configuration" | "rules">;
  interactiveReady?: boolean;
}) {
  return (
    <div>
      <span>{parameterSummary(item.configuration)}</span>
      <details>
        <summary>全部参数</summary>
        <Configuration value={item.configuration} interactiveReady={interactiveReady} />
        {item.rules ? <TemplateRulesSummary rules={item.rules} sources={undefined} /> : null}
      </details>
    </div>
  );
}

type MetricRow = Pick<FormalExperiment, "metrics">;

function rowMetric(item: MetricRow, key: string): ExperimentMetric | undefined {
  return item.metrics?.find((metric) => metric.key === key);
}

function metricsForRows(rows: MetricRow[]): ExperimentMetric[] {
  return Array.from(
    new Map(
      rows.flatMap((item) => item.metrics ?? []).map((metric) => [metric.key, metric]),
    ).values(),
  );
}

function metricColumn<T extends MetricRow>(
  metric: ExperimentMetric,
  secondary: boolean,
): DataColumn<T> {
  return {
    id: `metric-${metric.key}`,
    header: metric.label,
    value: (item) => rowMetric(item, metric.key)?.value ?? null,
    cell: (item) => {
      const value = rowMetric(item, metric.key);
      return value ? metricText(value) : "—";
    },
    numeric: true,
    secondary,
  };
}

function NativeConfiguration({
  value,
  native,
  interactiveReady,
}: {
  value: NativeExperimentConfiguration;
  native?: ExperimentResult["native"];
  interactiveReady: boolean;
}) {
  const { target, source_key, source_version, profile_hash } = value.selection;
  const costs = native?.execution_costs;
  const profile = native?.execution_profile;
  const policy = profile?.paper_policy;
  const initialCash = profile ? formatNumber(Number(profile.initial_cash)) : "—";
  const lagSeconds = policy?.execution_lag.match(/^PT(\d+(?:\.\d+)?)S$/)?.[1];
  const parameterRows = native?.parameters ?? [];
  return (
    <section aria-label="分钟策略配置">
      <fieldset className="exp-native-controls" disabled={!interactiveReady}>
        <dl className="exp-metrics exp-native-config">
          <div>
            <dt>策略</dt>
            <dd>
              {target.name} · 第 {target.head.version} 版
            </dd>
          </div>
          <div>
            <dt>区间</dt>
            <dd className="num">
              {value.start_date} — {value.end_date}
            </dd>
          </div>
          <div>
            <dt>来源</dt>
            <dd>
              <Tip interactive content={`来源 ${source_key}；第 ${source_version} 版`}>
                <button type="button" className="exp-native-tip" aria-label="查看分钟数据来源">
                  第 {source_version} 版
                </button>
              </Tip>
            </dd>
          </div>
          <div>
            <dt>策略类型</dt>
            <dd>{target.source_kind === "builtin" ? "内置策略" : "策略模板"}</dd>
          </div>
          <div>
            <dt>参数明细</dt>
            <dd>
              {parameterRows.length > 0 ? (
                <dl className="exp-native-parameters" aria-label="原生策略参数">
                  {parameterRows.map((parameter) => (
                    <div key={parameter.name}>
                      <dt>{parameter.label || "参数"}</dt>
                      <dd>
                        <span className="num">{parameter.display_value || "—"}</span>
                        <Tip
                          interactive
                          content={`参数 ${parameter.name}；原值 ${JSON.stringify(parameter.value)}；参数摘要 ${target.parameter_fingerprint}`}
                        >
                          <button
                            type="button"
                            className="exp-native-tip"
                            aria-label={`查看${parameter.label || "参数"}来源`}
                          >
                            详情
                          </button>
                        </Tip>
                      </dd>
                    </div>
                  ))}
                </dl>
              ) : (
                <Tip
                  interactive
                  content={`本结果未提供参数明细；参数摘要 ${target.parameter_fingerprint}`}
                >
                  <button type="button" className="exp-native-tip" aria-label="查看分钟参数来源">
                    —
                  </button>
                </Tip>
              )}
            </dd>
          </div>
          <div>
            <dt>执行配置</dt>
            <dd>
              <Tip
                interactive
                content={`配置 ${profile_hash}；定义 ${target.head.spec_fingerprint}${profile ? `；原执行配置 ${JSON.stringify(profile)}` : ""}`}
              >
                <button type="button" className="exp-native-tip" aria-label="查看分钟执行配置">
                  已绑定
                </button>
              </Tip>
            </dd>
          </div>
          <div>
            <dt>初始资金</dt>
            <dd className="num">{initialCash === "—" ? "—" : `${initialCash} 元`}</dd>
          </div>
          <div>
            <dt>模拟数量</dt>
            <dd>
              {policy && Object.keys(policy.action_quantities).length > 0 ? (
                <ul className="exp-native-quantities">
                  {Object.entries(policy.action_quantities).map(([action, quantity]) => (
                    <li key={action}>
                      <Tip interactive content={`动作 ${action}；模拟账户 ${policy.account_id}`}>
                        <button type="button" className="exp-native-tip">
                          {Object.entries(ACTION_COPY).find(([key]) => key === action)?.[1] ||
                            "其他动作"}{" "}
                          · {formatCount(quantity)} 股
                        </button>
                      </Tip>
                    </li>
                  ))}
                </ul>
              ) : (
                "—"
              )}
            </dd>
          </div>
          <div>
            <dt>执行延迟</dt>
            <dd>
              {policy ? (
                <Tip interactive content={`原执行延迟 ${policy.execution_lag}`}>
                  <button type="button" className="exp-native-tip" aria-label="查看分钟执行延迟">
                    {lagSeconds === undefined ? "查看延迟" : `${lagSeconds} 秒`}
                  </button>
                </Tip>
              ) : (
                "—"
              )}
            </dd>
          </div>
          <div>
            <dt>采集方式</dt>
            <dd>{native ? (native.source_kind === "captured" ? "原始记录" : "历史重建") : "—"}</dd>
          </div>
          <div>
            <dt>费用</dt>
            <dd>
              {costs ? (
                <Tip
                  interactive
                  content={`费用摘要 ${target.cost_fingerprint}；原费用 ${JSON.stringify(costs)}`}
                >
                  <button type="button" className="exp-native-tip" aria-label="查看分钟策略费用">
                    查看费用
                  </button>
                </Tip>
              ) : (
                "—"
              )}
            </dd>
          </div>
        </dl>
      </fieldset>
    </section>
  );
}

function Configuration({
  value,
  native,
  interactiveReady = true,
}: {
  value: ExperimentResult["configuration"];
  native?: ExperimentResult["native"];
  interactiveReady?: boolean;
}) {
  if (isNativeExperimentConfiguration(value))
    return (
      <NativeConfiguration value={value} native={native} interactiveReady={interactiveReady} />
    );
  const cost = value.execution_cost_spec;
  return (
    <dl className="exp-metrics">
      <div>
        <dt>区间</dt>
        <dd className="num">
          {value.start_date} — {value.end_date}
        </dd>
      </div>
      <div>
        <dt>初始资金</dt>
        <dd className="num">{formatCount(Number(value.initial_cash))}</dd>
      </div>
      <div>
        <dt>最多持仓</dt>
        <dd>{value.weight_rule.max_positions} 只</dd>
      </div>
      <div>
        <dt>单股上限</dt>
        <dd>{formatPercent(Number(value.weight_rule.max_stock_weight) * 100)}</dd>
      </div>
      <div>
        <dt>行业上限</dt>
        <dd>
          {value.weight_rule.max_industry_weight == null
            ? "—"
            : formatPercent(Number(value.weight_rule.max_industry_weight) * 100)}
        </dd>
      </div>
      <div>
        <dt>现金保留</dt>
        <dd>{formatPercent(Number(value.weight_rule.cash_reserve) * 100)}</dd>
      </div>
      <div>
        <dt>最小买入</dt>
        <dd>{formatCount(Number(value.weight_rule.min_target_amount))}</dd>
      </div>
      <div>
        <dt>分配方式</dt>
        <dd>
          {(
            { equal: "等权", rank_score: "排名加权", inverse_volatility: "波动倒数" } as Record<
              string,
              string
            >
          )[value.weight_rule.method] ?? "暂不可用"}
        </dd>
      </div>
      <div>
        <dt>调仓</dt>
        <dd>
          {value.rebalance_rule.kind === "every_n"
            ? `每 ${value.rebalance_rule.every_n_days} 日`
            : ({ daily: "每日", weekly: "每周", monthly: "每月" } as Record<string, string>)[
                value.rebalance_rule.kind
              ]}
        </dd>
      </div>
      <div>
        <dt>基准</dt>
        <dd className="num">{value.benchmark_code}</dd>
      </div>
      <div>
        <dt>费用</dt>
        <dd>
          <Tip
            content={
              cost.commission_rules
                .map((rule) => `佣金 ${rule.rate_bps} 基点，最低 ${rule.minimum_amount} 元`)
                .join("；") +
              "；" +
              cost.stamp_duty_rules.map((rule) => `印花税 ${rule.rate_bps} 基点`).join("；")
            }
          >
            <span>查看费用</span>
          </Tip>
        </dd>
      </div>
      <div>
        <dt>风控</dt>
        <dd>
          {value.drawdown_rule == null ? (
            "未设回撤暂停"
          ) : (
            <Tip content={JSON.stringify(value.drawdown_rule)}>
              <span>查看回撤规则</span>
            </Tip>
          )}
        </dd>
      </div>
    </dl>
  );
}

function ResultRules({ result }: { result: ExperimentResult }) {
  if (!result.template) return null;
  return (
    <div className="exp-template-rules">
      <Tip
        content={`策略 ${result.template.strategy_id}；定义 ${result.template.head.registration_fingerprint}；正文 ${result.template.content_hash}`}
      >
        <span className="exp-help">策略 · 第 {result.template.head.version} 版</span>
      </Tip>
      <TemplateRulesSummary rules={result.template.rules} sources={undefined} />
    </div>
  );
}

function Curves({ results }: { results: ExperimentResult[] }) {
  const build = useCallback(
    (colors: ChartColors): EChartOption => {
      const dates = Array.from(
        new Set(results.flatMap((result) => result.curves.map((point) => point.trade_date))),
      ).sort();
      return {
        animation: false,
        tooltip: { trigger: "axis" },
        legend: { textStyle: { color: colors.muted } },
        grid: { left: 50, right: 15, top: 35, bottom: 32 },
        xAxis: { type: "category", data: dates, axisLabel: { color: colors.muted } },
        yAxis: {
          type: "value",
          name: "归一净值",
          scale: true,
          axisLabel: { color: colors.muted },
          splitLine: { lineStyle: { color: colors.grid } },
        },
        series: results.flatMap((result, index) => {
          const points = new Map(result.curves.map((point) => [point.trade_date, point]));
          return [
            {
              type: "line" as const,
              name: results.length === 1 ? "组合" : index === 0 ? "实验 A" : "实验 B",
              data: dates.map((date) => points.get(date)?.nav ?? null),
              connectNulls: false,
              showSymbol: false,
              lineStyle: { color: index === 0 ? colors.accent : colors.series[1], width: 2 },
            },
            ...(results.length === 1
              ? [
                  {
                    type: "line" as const,
                    name: "基准",
                    data: dates.map((date) => points.get(date)?.benchmark_nav ?? null),
                    connectNulls: false,
                    showSymbol: false,
                    lineStyle: { type: "dashed" as const, color: colors.series[1] },
                  },
                ]
              : []),
          ];
        }),
      };
    },
    [results],
  );
  return (
    <>
      <EChart label={results.length === 1 ? "实验与基准净值" : "两份实验净值"} build={build} />
      <details className="exp-table-details">
        <summary>逐日净值</summary>
        {results.map((result, index) => (
          <DataTable
            key={result.experiment_id}
            label={`实验${index + 1}逐日净值`}
            rows={result.curves}
            rowKey={(point) => point.trade_date}
            columns={[
              { id: "date", header: "日期", value: (point) => point.trade_date },
              {
                id: "nav",
                header: "净值",
                value: (point) => point.nav,
                numeric: true,
                cell: (point) => (
                  <Tip content={`完整净值：${point.nav}`}>
                    <span>{formatNumber(point.nav, 4)}</span>
                  </Tip>
                ),
              },
              {
                id: "return",
                header: "日收益",
                value: (point) => point.daily_return,
                numeric: true,
                cell: (point) => formatPercent(point.daily_return * 100),
              },
              {
                id: "benchmark",
                header: "基准",
                value: (point) => point.benchmark_nav ?? null,
                cell: (point) =>
                  point.benchmark_nav == null ? (
                    "—"
                  ) : (
                    <Tip content={`完整基准净值：${point.benchmark_nav}`}>
                      <span>{formatNumber(point.benchmark_nav, 4)}</span>
                    </Tip>
                  ),
                numeric: true,
                secondary: true,
              },
            ]}
          />
        ))}
      </details>
    </>
  );
}

const journalKey = (owner: string) => `rquant:experiment-request:v1:${encodeURIComponent(owner)}`;
function originalRequest(owner: string): ExperimentWrite | null {
  try {
    const raw = sessionStorage.getItem(journalKey(owner));
    if (!raw || raw.length > 40 * 1024) return null;
    const saved: unknown = JSON.parse(raw);
    if (
      typeof saved !== "object" ||
      saved === null ||
      !("owner" in saved) ||
      saved.owner !== owner ||
      !("body" in saved)
    )
      return null;
    const body = saved.body;
    if (
      typeof body !== "object" ||
      body === null ||
      !("command_id" in body) ||
      typeof body.command_id !== "string" ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(body.command_id) ||
      !("kind" in body) ||
      ![
        "register_experiment_family",
        "cancel_experiment_family",
        "set_experiment_note",
        "unseal_experiment_outer_test",
        "set_experiment_holdout_policy",
      ].includes(String(body.kind))
    )
      return null;
    // The strict server DTO validates saved browser input; it never supplies authority.
    return body as ExperimentWrite;
  } catch {
    return null;
  }
}

function SearchForm({
  owner,
  generation,
  capabilities,
  busy,
  blocked,
  onSubmit,
}: {
  owner: string;
  generation: string;
  capabilities: ExperimentCapabilities;
  busy: boolean;
  blocked: boolean;
  onSubmit: (request: ExperimentSearch) => void;
}) {
  const defaults = capabilities.default_config;
  const [execution, setExecution] = useState<"portfolio" | "template">("portfolio");
  const [template, setTemplate] = useState<TemplateDetail | null>(null);
  const base =
    execution === "template" && template && defaults
      ? {
          ...defaults,
          weight_rule: template.rules.weight_rule,
          rebalance_rule: template.rules.rebalance_rule,
        }
      : defaults;
  const available = capabilities.sources.filter((source) => source.available);
  const [sourceId, setSourceId] = useState(`${defaults?.source_key}@${defaults?.source_version}`);
  const source =
    available.find((source) => `${source.key}@${source.version}` === sourceId) ?? available[0];
  const dates =
    source?.trading_dates.filter((date) => date >= source.start_date && date <= source.end_date) ??
    [];
  const split = Math.max(1, Math.floor(dates.length / 3));
  const defaultDates = [
    dates[0] ?? "",
    dates[split - 1] ?? "",
    dates[split] ?? "",
    dates[2 * split - 1] ?? "",
    dates[2 * split] ?? "",
    dates.at(-1) ?? "",
  ];
  const [name, setName] = useState("");
  const [ranges, setRanges] = useState(defaultDates);
  const [method, setMethod] = useState<"grid" | "random">("grid");
  const [count, setCount] = useState(4);
  const [seed, setSeed] = useState(42);
  const [confidence, setConfidence] = useState("0.95");
  const [target, setTarget] = useState("0");
  const [slices, setSlices] = useState<4 | 6 | 8 | 10>(4);
  const [values, setValues] = useState<Record<Parameter, string>>({
    "weight_rule.max_positions": "1, 2",
    "weight_rule.cash_reserve": "0.25, 0.50",
    "weight_rule.max_stock_weight": "",
    "weight_rule.min_target_amount": "",
    "rebalance_rule.every_n_days": "",
  });
  const [error, setError] = useState("");
  const dimensions = Object.entries(values)
    .filter(
      ([key, value]) =>
        value.trim() !== "" &&
        (key !== "rebalance_rule.every_n_days" || base?.rebalance_rule.kind === "every_n"),
    )
    .map(([parameter, value]) => ({
      parameter,
      values: value.split(/[,，]/).map((v) => v.trim()),
    }));
  const potential = dimensions.reduce((total, dimension) => total * dimension.values.length, 1);
  const validDimension = (dimension: { parameter: string; values: string[] }) => {
    const numbers = dimension.values.map(Number);
    if (
      dimension.values.some((value) => value.trim() === "" || !Number.isFinite(Number(value))) ||
      numbers.some((value, index) => index > 0 && value <= (numbers[index - 1] ?? value))
    )
      return false;
    return numbers.every((value, index) => {
      if (dimension.parameter === "weight_rule.max_positions")
        return Number.isInteger(value) && value >= 1 && value <= 500;
      if (dimension.parameter === "rebalance_rule.every_n_days")
        return Number.isInteger(value) && value >= 1 && value <= 252;
      if (dimension.parameter === "weight_rule.min_target_amount")
        return (
          value >= 0 && value <= 1e12 && /^\d+(?:\.\d{1,2})?$/.test(dimension.values[index] ?? "")
        );
      return (
        value >= 0 &&
        value <= 1 &&
        (dimension.parameter !== "weight_rule.max_stock_weight" || value > 0)
      );
    });
  };
  const submit = () => {
    if (execution === "template" && (!capabilities.can_search_templates || !template)) {
      setError("请先核对策略及版本。");
      return;
    }
    if (
      !base ||
      !source ||
      !name.trim() ||
      ranges.length !== 6 ||
      ranges.some((date) => !dates.includes(date)) ||
      ranges.some((date, index) => index > 0 && date < (ranges[index - 1] ?? "")) ||
      ranges[1] === ranges[2] ||
      ranges[3] === ranges[4]
    ) {
      setError("请填写名称，并按顺序选三个完整区间。");
      return;
    }
    if (
      !dimensions.length ||
      potential > (method === "grid" ? 64 : 4096) ||
      count < 1 ||
      count > 64 ||
      (method === "random" && count > potential) ||
      dimensions.some((d) => d.values.length > 16 || !validDimension(d)) ||
      !Number.isInteger(count) ||
      !Number.isInteger(seed) ||
      seed < 0 ||
      seed > 2 ** 32 - 1 ||
      !target.trim() ||
      !Number.isFinite(Number(target)) ||
      !(Number(confidence) > 0.5 && Number(confidence) < 1)
    ) {
      setError("请检查参数范围、搜索数量和统计设置。");
      return;
    }
    onSubmit({
      name: name.trim(),
      base_config: { ...base, source_key: source.key, source_version: source.version },
      ...(execution === "template" && template
        ? {
            template: { strategy_id: template.strategy_id, head: template.head },
          }
        : {}),
      protocol: {
        train_range: { start_date: ranges[0] ?? "", end_date: ranges[1] ?? "" },
        validation_range: { start_date: ranges[2] ?? "", end_date: ranges[3] ?? "" },
        frozen_outer_test_range: { start_date: ranges[4] ?? "", end_date: ranges[5] ?? "" },
      },
      dimensions: dimensions as ExperimentSearch["dimensions"],
      method,
      random_count: count,
      seed,
      confidence,
      target_period_sharpe: target,
      pbo_slices: slices,
    });
  };
  return (
    <form
      className="exp-form"
      onSubmit={(event) => {
        event.preventDefault();
        submit();
      }}
    >
      <label className="field">
        实验名称
        <input
          className="inp"
          aria-label="实验名称"
          value={name}
          maxLength={60}
          onChange={(event) => setName(event.target.value)}
        />
      </label>
      {capabilities.can_search_templates ? (
        <>
          <label className="field">
            策略类型
            <select
              className="inp"
              aria-label="实验执行方式"
              value={execution}
              onChange={(event) => {
                setTemplate(null);
                setExecution(event.target.value as "portfolio" | "template");
              }}
            >
              <option value="portfolio">组合策略</option>
              <option value="template">已保存策略</option>
            </select>
          </label>
          {execution === "template" ? (
            <ExperimentTemplatePicker
              key={`${owner}:${generation}`}
              owner={owner}
              generation={generation}
              onChange={setTemplate}
            />
          ) : null}
        </>
      ) : null}
      <label className="field">
        来源
        <select
          className="inp"
          aria-label="实验来源"
          value={sourceId}
          onChange={(event) => {
            setSourceId(event.target.value);
            setRanges([]);
          }}
        >
          {available.map((option) => (
            <option
              key={`${option.key}@${option.version}`}
              value={`${option.key}@${option.version}`}
            >
              {option.label} · 第 {option.version} 版
            </option>
          ))}
        </select>
      </label>
      <Tip
        content={
          execution === "template"
            ? "沿用所选版本的完整规则、撮合和费用。搜索只改变下列参数；入场与退出规则保持原值。"
            : "组合策略沿用已登记版本、撮合和费用。搜索只改变下列参数。验证承接训练末账户，样本外另开账户。"
        }
      >
        <span className="exp-help">
          {execution === "template" ? "参数搜索说明" : "组合策略 · 第 1 版"}
        </span>
      </Tip>
      <div className="exp-range-grid">
        {["训练开始", "训练结束", "验证开始", "验证结束", "样本外开始", "样本外结束"].map(
          (label, index) => (
            <label className="field" key={label}>
              {label}
              <select
                className="inp num"
                aria-label={label}
                value={ranges[index] ?? ""}
                onChange={(event) =>
                  setRanges((current) => {
                    const next = [...current];
                    next[index] = event.target.value;
                    return next;
                  })
                }
              >
                <option value="">选择日期</option>
                {dates.map((date) => (
                  <option key={date}>{date}</option>
                ))}
              </select>
            </label>
          ),
        )}
      </div>
      <Tip content="样本外封存区间在搜索时不读取。本人相交区间只能解封一次；这不证明你从未通过其他工具观察数据。">
        <span className="exp-help">样本外区间说明</span>
      </Tip>
      <label className="field">
        搜索方式
        <select
          className="inp"
          aria-label="搜索方式"
          value={method}
          onChange={(event) => setMethod(event.target.value as "grid" | "random")}
        >
          <option value="grid">网格搜索</option>
          <option value="random">随机搜索</option>
        </select>
      </label>
      <fieldset>
        <legend>参数范围</legend>
        {(Object.keys(parameters) as Parameter[])
          .filter(
            (key) =>
              key !== "rebalance_rule.every_n_days" || base?.rebalance_rule.kind === "every_n",
          )
          .map((key) => (
            <label className="field" key={key}>
              {parameters[key]}
              <input
                className="inp num"
                aria-label={`${parameters[key]}范围`}
                value={values[key]}
                placeholder="多个值用逗号分隔"
                onChange={(event) => setValues({ ...values, [key]: event.target.value })}
              />
            </label>
          ))}
      </fieldset>
      {method === "random" ? (
        <div className="exp-range-grid">
          <label className="field">
            抽取数量
            <input
              className="inp num"
              aria-label="抽取数量"
              type="number"
              min={1}
              max={64}
              value={count}
              onChange={(event) => setCount(Number(event.target.value))}
            />
          </label>
          <label className="field">
            随机种子
            <input
              className="inp num"
              aria-label="随机种子"
              type="number"
              min={0}
              value={seed}
              onChange={(event) => setSeed(Number(event.target.value))}
            />
          </label>
        </div>
      ) : null}
      <details>
        <summary>统计设置</summary>
        <div className="exp-range-grid">
          <label className="field">
            置信度
            <input
              className="inp num"
              aria-label="置信度"
              value={confidence}
              onChange={(event) => setConfidence(event.target.value)}
            />
          </label>
          <label className="field">
            目标单期夏普
            <input
              className="inp num"
              aria-label="目标单期夏普"
              value={target}
              onChange={(event) => setTarget(event.target.value)}
            />
          </label>
          <label className="field">
            切片数量
            <select
              className="inp"
              aria-label="切片数量"
              value={slices}
              onChange={(event) => setSlices(Number(event.target.value) as 4 | 6 | 8 | 10)}
            >
              {[4, 6, 8, 10].map((n) => (
                <option key={n}>{n}</option>
              ))}
            </select>
          </label>
        </div>
      </details>
      <div className="exp-search-total">
        计划 {method === "grid" ? potential : count} 次{" "}
        <Tip
          content={`可选组合 ${potential}。失败和取消的尝试也计入搜索总数。网格最多 64 次；随机最多 64 次，候选空间最多 4096。`}
        >
          <span>搜索总数说明</span>
        </Tip>
      </div>
      {error ? <p role="alert">{error}</p> : null}
      <Button
        type="submit"
        disabled={
          busy || blocked || !capabilities.can_search || (execution === "template" && !template)
        }
      >
        开始搜索
      </Button>
    </form>
  );
}

export default function ExperimentsPage() {
  const meta = useCurrentMeta();
  const owner = meta.isError ? null : (meta.data?.data.viewer ?? null);
  const generation = meta.isError ? null : (meta.data?.data.generation?.generation_id ?? null);
  const caps = useExperimentCapabilities(owner, generation);
  const [legacyView, setLegacyView] = useState<{ owner: string; generation: string } | null>(null);
  const legacy = legacyView?.owner === owner && legacyView?.generation === generation;
  if (owner && generation && caps.isLoading)
    return (
      <>
        <PageHeader eyebrow="策略与验证" title="实验记录" />
        <PageSkeleton label="正在加载实验记录" />
      </>
    );
  if (owner && generation && caps.error)
    return (
      <>
        <PageHeader eyebrow="策略与验证" title="我的实验" />
        <Panel>
          <p role="alert">
            {caps.error instanceof ApiError && caps.error.status === 409
              ? "数据已更新，请重新查看实验。"
              : caps.error instanceof ApiError && [401, 403].includes(caps.error.status)
                ? "当前账号无法查看本人实验，请核对登录与权限。"
                : "实验权限暂时无法核对，请稍后重试。"}
          </p>
        </Panel>
      </>
    );
  if (owner && generation && caps.data?.available && !legacy)
    return (
      <FormalExperimentsPage
        key={`${owner}:${generation}`}
        owner={owner}
        generation={generation}
        caps={caps.data}
        onLegacy={() => setLegacyView({ owner, generation })}
      />
    );
  return (
    <>
      {owner && generation && caps.data?.available ? (
        <div className="exp-toolbar">
          <Button variant="ghost" onClick={() => setLegacyView(null)}>
            我的实验
          </Button>
        </div>
      ) : null}
      <LegacyExperimentsPage />
    </>
  );
}

function FormalExperimentsPage({
  owner,
  generation,
  caps,
  onLegacy,
}: {
  owner: string;
  generation: string;
  caps: ExperimentCapabilities;
  onLegacy: () => void;
}) {
  const [refresh, setRefresh] = useState(0);
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [chosen, setChosen] = useState<FormalExperiment[]>([]);
  const [compareOpen, setCompareOpen] = useState(false);
  const [selected, setSelected] = useState<FormalExperiment | null>(null);
  const [familyId, setFamilyId] = useState<string | null>(null);
  const [familyReady, setFamilyReady] = useState(false);
  const [newOpen, setNewOpen] = useState(false);
  const [policyOpen, setPolicyOpen] = useState(false);
  const [months, setMonths] = useState(caps.policy?.months ?? 0);
  const [pending, setPending] = useState<ExperimentWrite | null>(() => originalRequest(owner));
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [accessFailure, setAccessFailure] = useState<ApiError | null>(null);
  const [unseal, setUnseal] = useState<{
    family: ExperimentFamily;
    item: FormalExperiment;
    expires: Date;
  } | null>(null);
  const [note, setNote] = useState<{ family: string; version: number; text: string } | null>(null);
  const trigger = useRef<HTMLElement | null>(null);
  const unsealTrigger = useRef<HTMLElement | null>(null);
  const focusReturn = useRef<{
    owner: string;
    generation: string;
    element: HTMLElement | null;
    commandId: string | null;
    drawer: Element | null;
  } | null>(null);
  const [drawerClosed, setDrawerClosed] = useState(false);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const mine = useMyExperiments(owner, generation, cursors.at(-1) ?? null, refresh);
  const family = useExperimentFamily(owner, generation, familyId, refresh);
  const result = useExperimentResult(owner, generation, selected);
  const stats = useExperimentStatistics(owner, generation, selected);
  const comparison = useExperimentComparison(
    owner,
    generation,
    compareOpen ? chosen.map((item) => item.experiment_id) : null,
  );
  const [x, setX] = useState("weight_rule.max_positions");
  const [y, setY] = useState("weight_rule.cash_reserve");
  const [phase, setPhase] = useState("validation");
  const [metric, setMetric] = useState("total_return");
  const axes = family.data?.parameters ?? [];
  const activeX = axes.find((parameter) => parameter === x) ?? axes[0] ?? "";
  const activeY =
    axes.find((parameter) => parameter === y && parameter !== activeX) ??
    axes.find((parameter) => parameter !== activeX) ??
    "";
  const heat = useExperimentHeatmap(
    owner,
    generation,
    family.data?.phase === "search" && result.data ? familyId : null,
    selected?.experiment_id ?? null,
    activeX,
    activeY,
    phase,
    metric,
  );
  const boundaryError = [mine, family, result, stats, comparison, heat].find(
    (query) => query.error instanceof ApiError && [401, 403, 409].includes(query.error.status),
  )?.error;
  const accessError = accessFailure ?? (boundaryError instanceof ApiError ? boundaryError : null);
  useEffect(() => {
    if (!(boundaryError instanceof ApiError)) return;
    setAccessFailure(boundaryError);
    setChosen([]);
    setCompareOpen(false);
    setFamilyId(null);
    setFamilyReady(false);
    setSelected(null);
    setUnseal(null);
    setNewOpen(false);
    setPolicyOpen(false);
    focusReturn.current = null;
  }, [boundaryError]);
  const close = () => {
    focusReturn.current = {
      owner,
      generation,
      element: trigger.current,
      commandId: pending?.command_id ?? null,
      drawer:
        document.activeElement?.closest('[role="dialog"]') ??
        document.querySelector('.rq-drawer [role="dialog"], .rq-drawer[role="dialog"]'),
    };
    setDrawerClosed(false);
    setNewOpen(false);
    setPolicyOpen(false);
    setFamilyId(null);
    setFamilyReady(false);
    setSelected(null);
    setUnseal(null);
  };
  const afterDrawerOpenChange = (open: boolean) => {
    if (open || !focusReturn.current) return;
    setDrawerClosed(true);
  };
  const afterFamilyOpenChange = (open: boolean) => {
    setFamilyReady(open);
    afterDrawerOpenChange(open);
  };
  useEffect(() => {
    const request = focusReturn.current;
    if (!request || drawerClosed || newOpen || policyOpen || familyId !== null) return;
    const panel = request.drawer;
    const hidden = () => {
      if (!panel?.isConnected || panel.getClientRects().length === 0) return true;
      const style = window.getComputedStyle(panel);
      return style.visibility === "hidden" || style.display === "none";
    };
    const check = () => {
      if (focusReturn.current === request && hidden()) setDrawerClosed(true);
    };
    if (hidden()) {
      setDrawerClosed(true);
      return;
    }
    // Closing during the opening motion can skip AntDrawer's callback.
    // Observe the captured panel's actual removal or hiding in that path.
    const observer = new MutationObserver(check);
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["class", "style", "aria-hidden"],
    });
    panel?.addEventListener("transitionend", check);
    return () => {
      observer.disconnect();
      panel?.removeEventListener("transitionend", check);
    };
  }, [drawerClosed, newOpen, policyOpen, familyId]);
  useEffect(() => {
    const request = focusReturn.current;
    if (!drawerClosed || !request || busy || newOpen || policyOpen || familyId !== null) return;
    if (
      request.owner !== owner ||
      request.generation !== generation ||
      (pending && pending.command_id !== request.commandId)
    ) {
      focusReturn.current = null;
      return;
    }
    const current = request.element?.id
      ? document.getElementById(request.element.id)
      : request.element;
    const target =
      current?.isConnected && !current.matches(":disabled")
        ? current
        : pending?.command_id === request.commandId
          ? document.getElementById("experiment-request-retry")
          : null;
    if (!target || target.matches(":disabled")) return;
    const active = document.activeElement;
    focusReturn.current = null;
    // Do not take focus back after the user has selected another page control.
    if (
      active &&
      active !== document.body &&
      active !== target &&
      !(active instanceof HTMLElement && active.closest(".rq-drawer"))
    )
      return;
    target.focus();
  }, [busy, newOpen, policyOpen, familyId, pending, owner, generation, drawerClosed]);
  const open = (element: HTMLElement, operation: () => void) => {
    focusReturn.current = null;
    trigger.current = element;
    operation();
  };
  const write = async (body: ExperimentWrite) => {
    setBusy(true);
    setPending(body);
    setMessage("");
    try {
      sessionStorage.setItem(journalKey(owner), JSON.stringify({ owner, body }));
    } catch {
      setMessage("无法保存原请求，请释放浏览器存储后重试。");
      setBusy(false);
      return;
    }
    try {
      const receipt = await submitExperiment(body);
      if (!mounted.current) return;
      setMessage(receipt.message);
      if (!["pending", "processing", "unknown"].includes(receipt.status)) {
        sessionStorage.removeItem(journalKey(owner));
        setPending(null);
        setRefresh((value) => value + 1);
        if (receipt.status === "registered" || receipt.status === "outer_admitted") {
          setNewOpen(false);
          setUnseal(null);
        }
        if (receipt.status === "policy_saved") setPolicyOpen(false);
        // A late note receipt acknowledges the submitted draft without replacing newer edits.
        if (receipt.status === "note_saved" && body.kind === "set_experiment_note")
          setNote((current) => {
            if (current?.family !== body.family_id) return current;
            if (current.text === body.text) return null;
            return current.version === body.expected_version && receipt.version != null
              ? { ...current, version: receipt.version }
              : current;
          });
      }
    } catch (error) {
      if (mounted.current)
        setMessage(error instanceof Error ? error.message : "回执待核对，请重试原请求。");
    } finally {
      if (mounted.current) setBusy(false);
    }
  };
  const command = () => ({
    command_id: crypto.randomUUID(),
    requested_at: new Date().toISOString(),
  });
  const columns: DataColumn<FormalExperiment>[] = [
    {
      id: "select",
      header: "选择",
      value: () => null,
      cell: (item) => {
        const checked = chosen.some((entry) => entry.experiment_id === item.experiment_id);
        return (
          <input
            id={`experiment-select-${item.experiment_id}`}
            type="checkbox"
            aria-label={`选择${item.family_name}第${item.index + 1}项`}
            checked={checked}
            disabled={!item.result_hash || (!checked && chosen.length >= 2)}
            onChange={(event) => {
              const hadFocus = document.activeElement === event.currentTarget;
              setChosen(
                checked
                  ? chosen.filter((entry) => entry.experiment_id !== item.experiment_id)
                  : chosen.length < 2
                    ? [...chosen, item]
                    : chosen,
              );
              setCompareOpen(false);
              if (hadFocus)
                window.setTimeout(
                  () => document.getElementById(`experiment-select-${item.experiment_id}`)?.focus(),
                  0,
                );
            }}
          />
        );
      },
    },
    {
      id: "name",
      header: "实验",
      value: (item) => item.family_name,
      wrap: true,
      cell: (item) => (
        <Button
          id={`experiment-open-${item.experiment_id}`}
          variant="ghost"
          size="sm"
          onClick={(event) =>
            open(event.currentTarget, () => {
              setFamilyId(item.family_id);
              setSelected(item);
              setPhase(item.phase === "outer" ? "outer" : "validation");
            })
          }
        >
          {item.family_name} · {item.index + 1}
        </Button>
      ),
    },
    {
      id: "strategy",
      header: "策略 / 版本",
      value: (item) => item.strategy_name ?? "组合回测",
      cell: (item) => `${item.strategy_name ?? "组合回测"} · 第 ${item.strategy_version ?? 1} 版`,
      wrap: true,
    },
    {
      id: "parameters",
      header: "参数",
      value: (item) => parameterSummary(item.configuration),
      cell: (item) => <RowParameters item={item} />,
      wrap: true,
    },
    {
      id: "status",
      header: "状态",
      value: (item) => item.label,
      cell: (item) => (
        <Tip
          content={item.message ?? (item.cancellation_pending ? "正在核对取消结果。" : item.label)}
        >
          <Pill kind={statusLabel[item.status].kind}>
            {item.cancellation_pending ? "取消待确认" : item.label}
          </Pill>
        </Tip>
      ),
    },
    {
      id: "phase",
      header: "区间",
      value: (item) => (item.phase === "outer" ? "样本外" : "训练与验证"),
      secondary: true,
    },
    ...metricsForRows(mine.data?.items ?? [])
      .filter((metric) => ["total_return", "max_drawdown", "sharpe"].includes(metric.key))
      .map((metric) => metricColumn<FormalExperiment>(metric, metric.key !== "total_return")),
    {
      id: "time",
      header: "登记",
      value: (item) => item.registered_at,
      cell: (item) => <RelativeTime at={item.registered_at} />,
      secondary: true,
    },
  ];
  if (accessError)
    return (
      <>
        <PageHeader eyebrow="策略与验证" title="我的实验" />
        <Panel>
          <div role="alert">
            {accessError.status === 409
              ? "数据已更新，请重新查看实验。"
              : "当前账号无法查看本人实验，请核对登录与权限。"}
          </div>
          <Button onClick={() => window.location.reload()}>重新加载</Button>
        </Panel>
      </>
    );
  const currentFamily = family.data;
  const currentNote = note?.family === familyId ? note.text : (currentFamily?.note ?? "");
  const phaseMetrics = result.data?.phases.find((stage) => stage.phase === phase)?.metrics ?? [];
  return (
    <>
      <PageHeader eyebrow="策略与验证" title="我的实验" note="搜索参数，比较结果，验证样本外表现" />
      <div className="exp-toolbar">
        <Button variant="ghost" onClick={onLegacy}>
          旧共享记录
        </Button>
        <Button
          disabled={!caps.can_search || !!pending}
          onClick={(event) => open(event.currentTarget, () => setNewOpen(true))}
        >
          新建实验
        </Button>
        <Button
          size="sm"
          onClick={() => {
            setRefresh((value) => value + 1);
            setCursors([null]);
          }}
        >
          刷新
        </Button>
        {caps.can_edit_policy ? (
          <Button
            variant="ghost"
            size="sm"
            disabled={!!pending}
            onClick={(event) => open(event.currentTarget, () => setPolicyOpen(true))}
          >
            样本外设置
          </Button>
        ) : null}
        {caps.message ? (
          <Tip content={caps.message}>
            <span>操作暂不可用</span>
          </Tip>
        ) : null}
      </div>
      {pending || message ? (
        <Panel>
          <div className="exp-request" role="status">
            <span>{message || "有一笔提交等待核对。"}</span>
            {pending ? (
              <Button
                id="experiment-request-retry"
                size="sm"
                disabled={busy}
                onClick={() => void write(pending)}
              >
                核对原请求
              </Button>
            ) : null}
          </div>
        </Panel>
      ) : null}
      {!mine.error && (mine.data?.preparing_families?.length ?? 0) > 0 ? (
        <Panel title="准备记录" flush>
          {mine.data?.preparing_window_truncated ? (
            <Tip content="仅展示最近的准备记录；每份实验保留全部计划项。">
              <span className="exp-window">显示最近的准备记录</span>
            </Tip>
          ) : null}
          <DataTable
            label="准备中的实验"
            rows={mine.data?.preparing_families ?? []}
            rowKey={(record) => record.family_id}
            columns={[
              {
                id: "name",
                header: "实验",
                value: (record) => record.name,
                wrap: true,
                cell: (record) => (
                  <Button
                    size="sm"
                    variant="ghost"
                    id={`experiment-preparation-${record.family_id}`}
                    onClick={(event) =>
                      open(event.currentTarget, () => {
                        setFamilyId(record.family_id);
                        setSelected(null);
                      })
                    }
                  >
                    {record.name}
                  </Button>
                ),
              },
              {
                id: "total",
                header: "计划",
                value: (record) => record.planned_count,
                cell: (record) => `计划 ${record.planned_count} 次`,
                numeric: true,
              },
              {
                id: "progress",
                header: "进度",
                value: (record) =>
                  `已保存 ${record.definition_saved_count} · 已准备 ${record.input_prepared_count}`,
                wrap: true,
              },
              {
                id: "state",
                header: "状态",
                value: (record) =>
                  record.state === "cancelled"
                    ? "已取消"
                    : record.failed_count
                      ? `未完成 ${record.failed_count} 项`
                      : "准备中",
              },
            ]}
          />
        </Panel>
      ) : null}
      <Panel title="全部尝试" flush>
        {mine.error ? (
          <div className="exp-message" role="alert">
            {mine.error.message}
          </div>
        ) : mine.isLoading ? (
          <PageSkeleton label="正在加载我的实验" />
        ) : (
          <>
            {mine.data?.truncated ? (
              <p className="exp-window">
                最近 {formatCount(mine.data.retained_count)} 条 ·{" "}
                {coverageStartText(mine.data.oldest_registered_at ?? null)}
              </p>
            ) : null}
            <div className="exp-select-bar">
              <span>已选 {chosen.length} / 2</span>
              <Button size="sm" disabled={chosen.length !== 2} onClick={() => setCompareOpen(true)}>
                对比所选
              </Button>
              {chosen.length ? (
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => {
                    setChosen([]);
                    setCompareOpen(false);
                  }}
                >
                  清空选择
                </Button>
              ) : null}
            </div>
            <DataTable
              label="我的实验"
              rows={mine.data?.items ?? []}
              columns={columns}
              rowKey={(item) => item.experiment_id}
              emptyText="还没有实验，点击新建实验开始。"
            />
            <div className="exp-pages">
              <span className="exp-count">
                第 {cursors.length} 页 · {formatCount(mine.data?.retained_count)} 条
              </span>
              <Button
                size="sm"
                disabled={cursors.length === 1}
                onClick={() => setCursors((value) => value.slice(0, -1))}
              >
                上一页
              </Button>
              <Button
                size="sm"
                disabled={!mine.data?.next_cursor}
                onClick={() => setCursors((value) => [...value, mine.data?.next_cursor ?? null])}
              >
                下一页
              </Button>
            </div>
          </>
        )}
      </Panel>
      {compareOpen ? (
        <Panel
          title="两份实验对比"
          actions={
            <Button size="sm" onClick={() => setCompareOpen(false)}>
              收起对比
            </Button>
          }
        >
          {comparison.isLoading ? (
            <PageSkeleton label="正在核对两份结果" />
          ) : comparison.error ? (
            <p role="alert">{comparison.error.message}</p>
          ) : comparison.data ? (
            <>
              <Curves results={[comparison.data.a, comparison.data.b]} />
              <div className="exp-compare-grid">
                {[comparison.data.a, comparison.data.b].map((entry, index) => (
                  <article key={entry.experiment_id}>
                    <h3>实验 {index === 0 ? "A" : "B"}</h3>
                    <Metrics values={entry.metrics} />
                    <Configuration value={entry.configuration} native={entry.native} />
                    <ResultRules result={entry} />
                  </article>
                ))}
              </div>
              {comparison.data.comparable ? (
                <>
                  <h3>差值 · B − A</h3>
                  <Metrics values={comparison.data.metric_differences} />
                </>
              ) : (
                <Tip content="日期、区间、费用、代码或来源口径不同，不能把差值作为同口径结果。">
                  <span>{comparison.data.message}</span>
                </Tip>
              )}
              <DataTable
                label="参数差异"
                rows={comparison.data.differences}
                rowKey={(row) => row.path}
                columns={[
                  {
                    id: "name",
                    header: "参数",
                    value: (row) => comparisonParameter(row.path),
                    cell: (row) => (
                      <Tip content={row.path}>
                        <span>{comparisonParameter(row.path)}</span>
                      </Tip>
                    ),
                  },
                  ...(["a", "b"] as const).map((side) => ({
                    id: side,
                    header: side.toUpperCase(),
                    value: (row: ExperimentComparison["differences"][number]) =>
                      comparisonValue(row.path, row[side]),
                    cell: (row: ExperimentComparison["differences"][number]) =>
                      row.path.startsWith("template.") ? (
                        <Tip content={row[side] ?? undefined}>
                          <span>{comparisonValue(row.path, row[side])}</span>
                        </Tip>
                      ) : (
                        row[side]
                      ),
                  })),
                ]}
              />
            </>
          ) : null}
        </Panel>
      ) : null}
      <SideDrawer
        open={newOpen}
        onClose={close}
        afterOpenChange={afterDrawerOpenChange}
        wide
        title="新建实验"
      >
        <SearchForm
          owner={owner}
          generation={generation}
          capabilities={caps}
          busy={busy}
          blocked={!!pending}
          onSubmit={(request) =>
            void write({ ...command(), kind: "register_experiment_family", request })
          }
        />
      </SideDrawer>
      <SideDrawer
        open={policyOpen}
        onClose={close}
        afterOpenChange={afterDrawerOpenChange}
        title="样本外设置"
      >
        <label className="field">
          封存最近月数
          <input
            className="inp num"
            aria-label="封存最近月数"
            type="number"
            min={0}
            max={36}
            value={months}
            onChange={(event) => setMonths(Number(event.target.value))}
          />
        </label>
        <Tip content="正式实验只能使用当前政策允许的完整交易日。0 表示不保留最近月份，仍不能使用未收盘日期；已准入请求按原政策恢复。">
          <span className="exp-help">封存范围说明</span>
        </Tip>
        <Button
          disabled={busy || !!pending || !Number.isInteger(months) || months < 0 || months > 36}
          onClick={() =>
            void write({
              ...command(),
              kind: "set_experiment_holdout_policy",
              months,
              expected_version: caps.policy?.version ?? 0,
            })
          }
        >
          保存设置
        </Button>
      </SideDrawer>
      <SideDrawer
        open={familyId !== null}
        onClose={close}
        afterOpenChange={afterFamilyOpenChange}
        wide
        title={currentFamily?.name ?? "实验详情"}
      >
        {family.error ? (
          <p role="alert">{family.error.message}</p>
        ) : family.isLoading ? (
          <PageSkeleton label="正在加载完整实验" />
        ) : currentFamily ? (
          <>
            <div className="exp-facts">
              <span>计划 {currentFamily.planned_count} 次</span>
              <span>搜索总数 {currentFamily.search_count}</span>
              <span>失败 {currentFamily.failed_count}</span>
              <span>取消 {currentFamily.cancelled_count}</span>
            </div>
            <Tip
              content={`可选组合 ${currentFamily.potential_count}。训练 ${currentFamily.protocol.train_range.start_date}—${currentFamily.protocol.train_range.end_date}；验证 ${currentFamily.protocol.validation_range.start_date}—${currentFamily.protocol.validation_range.end_date}；样本外 ${currentFamily.protocol.frozen_outer_test_range.start_date}—${currentFamily.protocol.frozen_outer_test_range.end_date}。`}
            >
              <span className="exp-help">查看区间与搜索范围</span>
            </Tip>
            <div className="exp-toolbar">
              <Button
                size="sm"
                disabled={
                  busy ||
                  !!pending ||
                  (currentFamily.preparation_state !== "preparing" &&
                    currentFamily.items.every(
                      (item) => !["registered", "running"].includes(item.status),
                    ))
                }
                onClick={() =>
                  void write({
                    ...command(),
                    kind: "cancel_experiment_family",
                    family_id: currentFamily.family_id,
                  })
                }
              >
                取消未完成项
              </Button>
              <Button
                size="sm"
                disabled={
                  !!pending ||
                  !caps.can_unseal ||
                  currentFamily.phase !== "search" ||
                  currentFamily.outer_admitted ||
                  !selected?.result_hash ||
                  currentFamily.items.some((item) =>
                    ["registered", "running"].includes(item.status),
                  )
                }
                onClick={(event) => {
                  if (selected?.result_hash) {
                    unsealTrigger.current = event.currentTarget;
                    setUnseal({
                      family: currentFamily,
                      item: selected,
                      expires: new Date(Date.now() + 120_000),
                    });
                  }
                }}
              >
                {currentFamily.outer_admitted ? "已解封" : "解封样本外"}
              </Button>
            </div>
            {(currentFamily.preparations?.length ?? 0) > 0 ? (
              <>
                <h3>
                  {currentFamily.preparation_state === "cancelled" ? "准备已取消" : "准备进度"}
                </h3>
                <Tip content="每个计划项均保留。规则保存或输入准备失败的项不会计作已运行，也不会生成回测统计。">
                  <span className="exp-help">准备进度说明</span>
                </Tip>
                <DataTable
                  label="完整准备清单"
                  rows={currentFamily.preparations ?? []}
                  rowKey={(slot) => String(slot.index)}
                  columns={[
                    {
                      id: "index",
                      header: "计划项",
                      value: (slot) => slot.index + 1,
                      numeric: true,
                    },
                    {
                      id: "strategy",
                      header: "策略 / 版本",
                      value: (slot) => slot.strategy_name ?? "组合回测",
                      cell: (slot) =>
                        `${slot.strategy_name ?? "组合回测"} · 第 ${slot.strategy_version ?? 1} 版`,
                      wrap: true,
                    },
                    {
                      id: "config",
                      header: "参数",
                      value: (slot) => parameterSummary(slot.configuration),
                      wrap: true,
                      cell: (slot) => (
                        <>
                          <RowParameters item={slot} interactiveReady={familyReady} />
                          <details>
                            <summary>全部指标</summary>
                            <Metrics values={slot.metrics ?? []} />
                          </details>
                        </>
                      ),
                    },
                    {
                      id: "state",
                      header: "状态",
                      value: (slot) =>
                        slot.definition_state === "cancelled"
                          ? "已取消"
                          : slot.definition_state === "failed"
                            ? "未完成"
                            : slot.input_prepared
                              ? "已准备"
                              : slot.definition_state === "saved"
                                ? "规则已保存"
                                : "待准备",
                    },
                    {
                      id: "reason",
                      header: "原因",
                      value: (slot) =>
                        slot.failure === "capacity"
                          ? "容量不足"
                          : slot.failure === "source_changed"
                            ? "来源已更新"
                            : slot.failure === "invalid_definition"
                              ? "规则无法保存"
                              : "—",
                      wrap: true,
                    },
                    ...metricsForRows(currentFamily.preparations ?? []).map((metric) =>
                      metricColumn<NonNullable<ExperimentFamily["preparations"]>[number]>(
                        metric,
                        metric.key !== "total_return",
                      ),
                    ),
                  ]}
                />
              </>
            ) : null}
            {currentFamily.items.length > 0 ? (
              <DataTable
                label="完整参数与指标"
                rows={currentFamily.items}
                columns={[
                  {
                    id: "configuration",
                    header: "组合",
                    value: (item) => parameterSummary(item.configuration),
                    cell: (item) => (
                      <>
                        <Button variant="ghost" size="sm" onClick={() => setSelected(item)}>
                          第 {item.index + 1} 项
                        </Button>
                        <RowParameters item={item} interactiveReady={familyReady} />
                        <details>
                          <summary>全部指标</summary>
                          <Metrics values={item.metrics ?? []} />
                        </details>
                      </>
                    ),
                    wrap: true,
                  },
                  {
                    id: "strategy",
                    header: "策略 / 版本",
                    value: (item) => item.strategy_name ?? "组合回测",
                    cell: (item) =>
                      `${item.strategy_name ?? "组合回测"} · 第 ${item.strategy_version ?? 1} 版`,
                    wrap: true,
                  },
                  {
                    id: "state",
                    header: "状态",
                    value: (item) => (item.cancellation_pending ? "取消待确认" : item.label),
                  },
                  {
                    id: "reason",
                    header: "原因",
                    value: (item) => item.message ?? null,
                    wrap: true,
                  },
                  ...metricsForRows(currentFamily.items).map((metric) =>
                    metricColumn<FormalExperiment>(metric, metric.key !== "total_return"),
                  ),
                ]}
                rowKey={(item) => item.experiment_id}
              />
            ) : null}
            <label className="field">
              研究备注
              <textarea
                className="inp"
                aria-label="研究备注"
                rows={3}
                maxLength={1024}
                value={currentNote}
                onChange={(event) =>
                  setNote({
                    family: currentFamily.family_id,
                    version:
                      note?.family === currentFamily.family_id
                        ? note.version
                        : currentFamily.note_version,
                    text: event.target.value,
                  })
                }
              />
            </label>
            <Button
              size="sm"
              disabled={busy || !!pending || currentNote === currentFamily.note}
              onClick={() =>
                void write({
                  ...command(),
                  kind: "set_experiment_note",
                  family_id: currentFamily.family_id,
                  expected_version: note?.version ?? currentFamily.note_version,
                  text: currentNote,
                })
              }
            >
              保存备注
            </Button>
            {result.error ? (
              <p role="alert">{result.error.message}</p>
            ) : result.isLoading ? (
              <PageSkeleton label="正在核对完整结果" />
            ) : result.data ? (
              <>
                <h3>第 {(selected?.index ?? 0) + 1} 项 · 完整结果</h3>
                <Tip
                  content={`结果 ${result.data.result_hash}；输入 ${result.data.input_hash}；定义 ${result.data.spec_hash}；产物 ${result.data.manifest_hash}`}
                >
                  <span className="exp-help">结果来源</span>
                </Tip>
                <Curves results={[result.data]} />
                <Configuration
                  value={result.data.configuration}
                  native={result.data.native}
                  interactiveReady={familyReady}
                />
                <ResultRules result={result.data} />
                <h3>全区间指标</h3>
                <Metrics values={result.data.metrics} />
                <label className="field">
                  分段指标
                  <select
                    className="inp"
                    aria-label="分段指标"
                    value={phase}
                    onChange={(event) => setPhase(event.target.value)}
                  >
                    {result.data.phases.map((stage) => (
                      <option key={stage.phase} value={stage.phase}>
                        {phaseNames[stage.phase]}
                      </option>
                    ))}
                  </select>
                </label>
                <Metrics values={phaseMetrics} />
                <Tip content="验证沿用训练末账户；阶段闭合交易只计入该阶段内完成的交易。样本外独立起始资金。">
                  <span className="exp-help">分段口径</span>
                </Tip>
                <h3>过拟合检查</h3>
                {stats.error ? (
                  <p role="alert">{stats.error.message}</p>
                ) : stats.data ? (
                  <>
                    <dl className="exp-metrics">
                      <div>
                        <dt>夏普显著性</dt>
                        <dd>
                          {formatPercent(
                            stats.data.psr?.probability == null
                              ? null
                              : stats.data.psr.probability * 100,
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>修正夏普</dt>
                        <dd>
                          {formatPercent(
                            stats.data.dsr?.probability == null
                              ? null
                              : stats.data.dsr.probability * 100,
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>所需样本</dt>
                        <dd>
                          {stats.data.mintrl?.status === "unreachable"
                            ? "达不到目标"
                            : formatCount(stats.data.mintrl?.minimum_observations)}
                        </dd>
                      </div>
                      <div>
                        <dt>过拟合概率</dt>
                        <dd>
                          {formatPercent(
                            stats.data.pbo?.probability_of_backtest_overfitting == null
                              ? null
                              : stats.data.pbo.probability_of_backtest_overfitting * 100,
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>多重检验</dt>
                        <dd>
                          {stats.data.bh_adjusted_p == null
                            ? "—"
                            : stats.data.bh_adjusted_p.toFixed(4)}
                        </dd>
                      </div>
                    </dl>
                    {stats.data.reasons.map((reason) => (
                      <Tip key={reason} content={reason}>
                        <span className="exp-help">暂不可计算 · 查看原因</span>
                      </Tip>
                    ))}
                  </>
                ) : null}
                {currentFamily.phase === "search" && axes.length < 2 ? (
                  <EmptyState title="本次只搜索一个参数" hint="完整结果可在上方逐项查看。" />
                ) : currentFamily.phase === "search" ? (
                  <>
                    <h3>参数热力图</h3>
                    <div className="exp-range-grid">
                      <label className="field">
                        横轴
                        <select
                          className="inp"
                          aria-label="热图横轴"
                          value={activeX}
                          onChange={(event) => setX(event.target.value)}
                        >
                          {axes.map((key) => (
                            <option key={key} value={key}>
                              {parameters[key]}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="field">
                        纵轴
                        <select
                          className="inp"
                          aria-label="热图纵轴"
                          value={activeY}
                          onChange={(event) => setY(event.target.value)}
                        >
                          {axes
                            .filter((key) => key !== activeX)
                            .map((key) => (
                              <option key={key} value={key}>
                                {parameters[key]}
                              </option>
                            ))}
                        </select>
                      </label>
                      <label className="field">
                        指标
                        <select
                          className="inp"
                          aria-label="热图指标"
                          value={metric}
                          onChange={(event) => setMetric(event.target.value)}
                        >
                          {phaseMetrics.map((value) => (
                            <option key={value.key} value={value.key}>
                              {value.label}
                            </option>
                          ))}
                        </select>
                      </label>
                    </div>
                    {heat.error ? (
                      <p role="alert">{heat.error.message}</p>
                    ) : heat.data ? (
                      <>
                        <div className="exp-heat-scroll">
                          <table className="exp-heat" aria-label="参数热力图">
                            <thead>
                              <tr>
                                <th>{parameters[activeY as Parameter]}</th>
                                {heat.data.x_values.map((value) => (
                                  <th key={value} className="num">
                                    {value}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {heat.data.y_values.map((yi) => (
                                <tr key={yi}>
                                  <th className="num">{yi}</th>
                                  {heat.data?.x_values.map((xi) => {
                                    const cell = heat.data?.cells.find(
                                      (entry) => entry.x === xi && entry.y === yi,
                                    );
                                    return (
                                      <td key={xi}>
                                        <button
                                          type="button"
                                          className={`exp-heat-cell ${cell?.value == null ? "" : "measured"}`}
                                          aria-label={`${parameters[activeX as Parameter]}${xi}，${parameters[activeY as Parameter]}${yi}，${cell?.value ?? "未完成"}`}
                                          onClick={() => {
                                            const item = currentFamily.items.find(
                                              (entry) =>
                                                entry.experiment_id === cell?.experiment_id,
                                            );
                                            if (item) setSelected(item);
                                          }}
                                          onKeyDown={(event) => {
                                            const buttons = Array.from(
                                              event.currentTarget
                                                .closest("tbody")
                                                ?.querySelectorAll("button") ?? [],
                                            );
                                            const index = buttons.indexOf(event.currentTarget);
                                            const width = heat.data?.x_values.length ?? 1;
                                            const offset = (
                                              {
                                                ArrowRight: 1,
                                                ArrowLeft: -1,
                                                ArrowDown: width,
                                                ArrowUp: -width,
                                              } as Record<string, number>
                                            )[event.key];
                                            if (offset !== undefined) {
                                              event.preventDefault();
                                              buttons[index + offset]?.focus();
                                            }
                                          }}
                                        >
                                          {cell?.value == null
                                            ? "—"
                                            : metricText({
                                                key: metric,
                                                label: metric,
                                                unit:
                                                  phaseMetrics.find((value) => value.key === metric)
                                                    ?.unit ?? "number",
                                                value: cell.value,
                                              })}
                                        </button>
                                      </td>
                                    );
                                  })}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                        <div className="exp-facts">
                          <span>
                            邻域 {heat.data.available_neighbors} / {heat.data.neighbor_count} 格
                          </span>
                          <span>
                            最低{" "}
                            {heat.data.neighbor_minimum == null
                              ? "—"
                              : heat.data.neighbor_minimum.toFixed(4)}
                          </span>
                        </div>
                        <Tip
                          content={
                            heat.data.complete_neighborhood
                              ? "相邻已运行组合均有完整值；只显示实际邻域，不自动判定稳定。"
                              : "有邻格未完成，不能确认完整邻域。"
                          }
                        >
                          <span className="exp-help">邻域说明</span>
                        </Tip>
                        {heat.data.fixed_parameters.length ? (
                          <dl className="exp-metrics">
                            {heat.data.fixed_parameters.map((value) => (
                              <div key={value.path}>
                                <dt>{parameters[value.path as Parameter]}</dt>
                                <dd>{value.a}</dd>
                              </div>
                            ))}
                          </dl>
                        ) : null}
                      </>
                    ) : null}
                  </>
                ) : null}
              </>
            ) : (
              <EmptyState title="尚无完整结果" hint="任务完成并封存后，可以查看曲线与指标。" />
            )}
          </>
        ) : null}
      </SideDrawer>
      <ConfirmDialog
        open={unseal !== null}
        level="high"
        title="解封样本外"
        confirmName={unseal?.family.name}
        expiresAt={unseal?.expires}
        description={
          unseal ? (
            <>
              <p>
                {unseal.family.protocol.frozen_outer_test_range.start_date} —{" "}
                {unseal.family.protocol.frozen_outer_test_range.end_date}
              </p>
              <Configuration value={unseal.item.configuration} />
              <p>确认后即使用这一次解封机会。运行失败或取消也不会恢复次数。</p>
            </>
          ) : null
        }
        busy={busy}
        disabled={!!pending || !caps.can_unseal}
        confirmLabel="确认解封"
        onCancel={() => {
          setUnseal(null);
          const element = unsealTrigger.current;
          window.setTimeout(() => element?.focus(), 0);
        }}
        onConfirm={() => {
          if (unseal?.item.result_hash)
            void write({
              ...command(),
              kind: "unseal_experiment_outer_test",
              family_id: unseal.family.family_id,
              experiment_id: unseal.item.experiment_id,
              result_hash: unseal.item.result_hash,
              confirmed: true,
            });
        }}
      />
    </>
  );
}

const resultColumns: DataColumn<ExperimentItem>[] = [
  {
    id: "family",
    header: "研究假设",
    value: (item) => item.hypothesis_family,
    wrap: true,
    cell: (item) => (
      <Tip content={`实验标识 ${item.experiment_id}`}>
        <strong className="exp-family">{item.hypothesis_family}</strong>
      </Tip>
    ),
  },
  {
    id: "status",
    header: "状态",
    value: (item) => statusLabel[item.status].label,
    cell: (item) => {
      const state = statusLabel[item.status];
      return <Pill kind={state.kind}>{state.label}</Pill>;
    },
  },
  {
    id: "registered",
    header: "登记时间",
    value: (item) => item.registered_at,
    cell: (item) => <RelativeTime at={item.registered_at} />,
  },
  {
    id: "return",
    header: "净收益",
    value: (item) => item.net_return_pct,
    numeric: true,
    cell: (item) => <ChangeText value={item.net_return_pct} />,
  },
  {
    id: "drawdown",
    header: "最大回撤",
    value: (item) => item.max_drawdown_pct,
    numeric: true,
    secondary: true,
    cell: (item) => formatPercent(item.max_drawdown_pct),
  },
  {
    id: "winrate",
    header: "胜率",
    value: (item) => item.win_rate_pct,
    numeric: true,
    secondary: true,
    cell: (item) => formatPercent(item.win_rate_pct),
  },
  {
    id: "trades",
    header: "交易数",
    value: (item) => item.trade_count,
    numeric: true,
    cell: (item) => formatCount(item.trade_count),
  },
];

function LegacyExperimentsPage() {
  const meta = useCurrentMeta();
  const generationId = meta.isError ? null : (meta.data?.data.generation?.generation_id ?? null);
  const [pageState, setPageState] = useState<{
    generationId: string | null;
    cursors: (string | null)[];
  }>({ generationId: null, cursors: [null] });
  const [selectionState, setSelectionState] = useState<{
    generationId: string | null;
    items: ExperimentItem[];
  }>({ generationId: null, items: [] });
  const [comparison, setComparison] = useState<{
    generationId: string | null;
    ids: string[];
  } | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const cursors = pageState.generationId === generationId ? pageState.cursors : [null];
  const selected = selectionState.generationId === generationId ? selectionState.items : [];
  const compareOpen =
    selected.length === 2 &&
    comparison?.generationId === generationId &&
    comparison.ids.every((id, index) => id === selected[index]?.experiment_id);
  const page = cursors.length;
  const query = useExperiments(cursors[page - 1] ?? null, generationId, refreshKey);

  const toggleSelected = (item: ExperimentItem) => {
    const hasItem = selected.some((entry) => entry.experiment_id === item.experiment_id);
    const items = hasItem
      ? selected.filter((entry) => entry.experiment_id !== item.experiment_id)
      : selected.length < 2
        ? [...selected, item]
        : selected;
    setSelectionState({ generationId, items });
    setComparison(null);
  };

  const columns: DataColumn<ExperimentItem>[] = [
    {
      id: "select",
      header: "选择",
      value: () => null,
      cell: (item) => {
        const checked = selected.some((entry) => entry.experiment_id === item.experiment_id);
        return (
          <input
            type="checkbox"
            aria-label={`选择${item.hypothesis_family}`}
            checked={checked}
            disabled={!checked && selected.length >= 2}
            onChange={() => toggleSelected(item)}
          />
        );
      },
    },
    ...resultColumns,
  ];

  const previous = () => setPageState({ generationId, cursors: cursors.slice(0, -1) });

  const reload = () => {
    setPageState({ generationId, cursors: [null] });
    setRefreshKey((value) => value + 1);
    void meta.refetch();
  };

  return (
    <>
      <PageHeader eyebrow="策略与验证" title="实验记录" note="查看已登记实验和已有结果" />
      {meta.isError ? (
        <Panel>
          <div className="exp-message" role="alert">
            <p>实验记录暂时无法核对，请稍后重试。</p>
            <Button size="sm" onClick={() => void meta.refetch()}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : meta.data === undefined || (generationId !== null && query.isLoading) ? (
        <PageSkeleton label="正在加载实验记录" />
      ) : generationId === null ? (
        <Panel>
          <EmptyState title="实验记录暂时不可用" hint="数据恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : query.error ? (
        <Panel>
          <div className="exp-message" role="alert">
            <p>{query.error.message}</p>
            <Button size="sm" onClick={reload}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : !query.data?.available ? (
        <Panel>
          <EmptyState title="实验记录暂时读不到" hint="数据发布后会显示在这里。" />
          <div className="exp-actions">
            <Button size="sm" onClick={reload}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : query.data.items.length === 0 ? (
        <Panel>
          <EmptyState
            title={page === 1 ? "还没有登记的实验" : "这一页没有更多实验"}
            hint={page === 1 ? "完成登记后会显示在这里。" : "可返回上一页继续查看。"}
          />
          {page > 1 ? (
            <div className="exp-actions">
              <Button size="sm" onClick={previous}>
                上一页
              </Button>
            </div>
          ) : null}
        </Panel>
      ) : (
        <>
          <Panel title="实验记录" sub="仅展示已发布的结果" flush>
            {query.data.truncated ? (
              <p className="exp-window" role="status">
                仅显示最近 {formatCount(query.data.retained_count)} 条实验 ·{" "}
                {coverageStartText(query.data.oldest_registered_at)}
              </p>
            ) : null}
            <div className="exp-select-bar">
              <div className="exp-selected" aria-live="polite">
                {selected.length === 0 ? (
                  <span>选择两条实验进行对比</span>
                ) : (
                  selected.map((item) => (
                    <Button
                      key={item.experiment_id}
                      variant="ghost"
                      size="sm"
                      aria-label={`移除${item.hypothesis_family}`}
                      onClick={() => toggleSelected(item)}
                    >
                      {item.hypothesis_family} <span aria-hidden="true">×</span>
                    </Button>
                  ))
                )}
              </div>
              <Button
                size="sm"
                disabled={selected.length !== 2}
                disabledReason={selected.length !== 2 ? "请选择两条实验" : undefined}
                onClick={() =>
                  setComparison({ generationId, ids: selected.map((item) => item.experiment_id) })
                }
              >
                对比所选
              </Button>
            </div>
            <DataTable
              rows={query.data.items}
              columns={columns}
              rowKey={(item) => item.experiment_id}
              label="实验记录"
              emptyText="这一页没有更多实验"
            />
            <div className="exp-pages">
              <span className="exp-count">
                第 {formatCount(page)} 页 · 最近 {formatCount(query.data.retained_count)} 条
              </span>
              <Button size="sm" disabled={page === 1} onClick={previous}>
                上一页
              </Button>
              <Button
                size="sm"
                disabled={query.data.next_cursor === null}
                onClick={() => {
                  const next = query.data?.next_cursor;
                  if (next) setPageState({ generationId, cursors: [...cursors, next] });
                }}
              >
                下一页
              </Button>
            </div>
          </Panel>
          {compareOpen ? (
            <Panel title="实验对比" label="实验对比" sub="并排查看已发布结果">
              <div className="exp-compare-grid">
                {selected.map((item) => {
                  const state = statusLabel[item.status];
                  return (
                    <article className="exp-compare-card" key={item.experiment_id}>
                      <div className="exp-compare-heading">
                        <h3>{item.hypothesis_family}</h3>
                        <Pill kind={state.kind}>{state.label}</Pill>
                      </div>
                      <dl>
                        <div>
                          <dt>净收益</dt>
                          <dd>
                            <ChangeText value={item.net_return_pct} />
                          </dd>
                        </div>
                        <div>
                          <dt>最大回撤</dt>
                          <dd>{formatPercent(item.max_drawdown_pct)}</dd>
                        </div>
                        <div>
                          <dt>胜率</dt>
                          <dd>{formatPercent(item.win_rate_pct)}</dd>
                        </div>
                        <div>
                          <dt>交易数</dt>
                          <dd>{formatCount(item.trade_count)}</dd>
                        </div>
                      </dl>
                    </article>
                  );
                })}
              </div>
              <p className="exp-compare-note">样本与成本口径尚未发布，暂不计算差值。</p>
            </Panel>
          ) : null}
        </>
      )}
    </>
  );
}
