import { useState } from "react";
import { ApiError } from "@/api/client";
import {
  type FactorDefinitionItem,
  type FactorResearchDisplay,
  type FactorResultItem,
  useFactorResultDetail,
  useFactorResults,
} from "@/api/factors";
import { EChart } from "@/charts/EChart";
import { toneClass, toneOf } from "@/format/color";
import {
  EMPTY,
  formatCount,
  formatNumber,
  formatPercent,
  formatSignedNumber,
  formatSignedPercent,
} from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, EmptyState, Panel, Pill, RelativeTime, Segmented, Tip } from "@/ui";
import {
  decayOption,
  decaySeries,
  groupCounts,
  groupOption,
  groupSeries,
  type IcMethod,
  icOption,
  icSeries,
  turnoverOption,
} from "./factorCharts";

type SelectedRun = { generationId: string; factorId: string; factorVersion: number; jobId: string };

function missing(value: number | null, reason: string, digits = 4) {
  return value === null ? (
    <Tip content={reason}>
      <span className="muted num">{EMPTY}</span>
    </Tip>
  ) : (
    <span className={`num ${toneClass(toneOf(value))}`}>{formatSignedNumber(value, digits)} </span>
  );
}

function rate(value: number | null, reason: string) {
  return value === null ? (
    <Tip content={reason}>
      <span className="muted num">{EMPTY}</span>
    </Tip>
  ) : (
    <span className="num">{formatPercent(value * 100, 1)}</span>
  );
}

function unsigned(value: number | null, reason: string, digits = 4) {
  return value === null ? (
    missing(null, reason)
  ) : (
    <span className="num">{formatNumber(value, digits)} </span>
  );
}

function unavailableReason(status: string): string {
  switch (status) {
    case "no_target_period":
      return "后续收益尚未成熟";
    case "zero_variance":
      return "样本数值没有变化";
    case "precision_limit":
      return "当前样本精度不足";
    case "no_valid_days":
      return "没有可计算的日期";
    default:
      return "有效样本不足";
  }
}

function Disclosure({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <details className="factor-disclosure">
      <summary>{label}</summary>
      {children}
    </details>
  );
}

function ResultState({
  title,
  hint,
  onRefresh,
}: {
  title: string;
  hint: string;
  onRefresh?: () => void;
}) {
  return (
    <div className="factor-result-state">
      <EmptyState title={title} hint={hint} />
      {onRefresh ? (
        <Button size="sm" onClick={onRefresh}>
          重新加载结果
        </Button>
      ) : null}
    </div>
  );
}

const runColumns: DataColumn<FactorResultItem>[] = [
  {
    id: "time",
    header: "检验时间",
    value: (item) => item.updated_at,
    cell: (item) => <RelativeTime at={item.updated_at} />,
  },
  {
    id: "state",
    header: "状态",
    value: (item) => item.status_label,
    cell: (item) => item.status_label,
  },
];

function Research({ research }: { research: FactorResearchDisplay }) {
  const [method, setMethod] = useState<IcMethod>("normal_ic");
  const [requestedGroupCount, setRequestedGroupCount] = useState<number | null>(null);
  const counts = groupCounts(research);
  const count =
    requestedGroupCount !== null && counts.includes(requestedGroupCount)
      ? requestedGroupCount
      : (counts[0] ?? null);
  const summary = research.ic_summary?.[method] ?? null;
  const points = icSeries(research, method);
  const decay = decaySeries(research, method);
  const grouping = count === null ? [] : groupSeries(research, count);
  const coverage = research.coverage_days;
  const covered = coverage.reduce((sum, day) => sum + day.coverage.valid_count, 0);
  const expected = coverage.reduce((sum, day) => sum + day.coverage.expected_count, 0);
  const partial =
    coverage.some(
      (day) =>
        day.status !== ("schema_version" in research ? "complete" : "evaluated") ||
        day.coverage.valid_count < day.coverage.expected_count,
    ) || research.portfolio_status === "available_partial";
  const dates = coverage.map((day) => day.decision_date).sort();
  const period = dates.length === 0 ? EMPTY : `${dates[0]} 至 ${dates[dates.length - 1]}`;
  const stats = [
    {
      label: "IC 均值",
      value: missing(summary?.mean ?? null, unavailableReason(summary?.status ?? "")),
    },
    { label: "标准差", value: unsigned(summary?.sample_std ?? null, "有效日期不足") },
    {
      label: "IR",
      value: missing(summary?.ir ?? null, unavailableReason(summary?.status ?? ""), 3),
    },
    { label: "IC > 0 占比", value: rate(summary?.positive_rate ?? null, "没有可计算的日期") },
    {
      label: "|IC| > 0.02 占比",
      value: rate(summary?.strong_signal_rate ?? null, "没有可计算的日期"),
    },
    { label: "t 值", value: missing(summary?.t_value ?? null, "有效日期不足", 3) },
    {
      label: "p 值",
      value:
        summary?.p_value === null || summary?.p_value === undefined ? (
          missing(null, unavailableReason(summary?.status ?? ""))
        ) : (
          <span className="num">{formatNumber(summary.p_value, 3)}</span>
        ),
    },
    { label: "偏度", value: missing(summary?.skewness ?? null, "有效日期不足", 3) },
    { label: "超额峰度", value: missing(summary?.excess_kurtosis ?? null, "有效日期不足", 3) },
    {
      label: "有效日期",
      value: (
        <span className="num">
          {formatCount(summary?.valid_day_count ?? 0)} /{" "}
          {formatCount(summary?.source_day_count ?? 0)}
        </span>
      ),
    },
  ];
  const icColumns: DataColumn<(typeof points)[number]>[] = [
    { id: "date", header: "日期", value: (point) => point.date },
    {
      id: "ic",
      header: method === "normal_ic" ? "NormalIC" : "RankIC",
      value: (point) => point.value,
      numeric: true,
      cell: (point) => missing(point.value, unavailableReason(point.reason)),
    },
    {
      id: "cumulative",
      header: "累计 IC",
      value: (point) => point.cumulative,
      numeric: true,
      cell: (point) => missing(point.cumulative, "该日没有可计算的 IC"),
    },
  ];
  const decayColumns: DataColumn<(typeof decay)[number]>[] = [
    {
      id: "lag",
      header: "滞后期",
      value: (point) => point.lag,
      cell: (point) => `第 ${point.lag} 期`,
    },
    {
      id: "mean",
      header: "IC 均值",
      value: (point) => point.value,
      numeric: true,
      cell: (point) => missing(point.value, unavailableReason(point.reason)),
    },
  ];
  const groupColumns = (
    measure: "cumulative_return" | "target_weight_turnover",
  ): DataColumn<(typeof grouping)[number]>[] => [
    { id: "date", header: "日期", value: (point) => point.date },
    ...Array.from(
      { length: count ?? 0 },
      (_, index): DataColumn<(typeof grouping)[number]> => ({
        id: String(index + 1),
        header: `第 ${index + 1} 组`,
        numeric: true,
        value: (point) =>
          point.groups.find((group) => group.group_number === index + 1)?.[measure] ?? null,
        cell: (point) => {
          const value =
            point.groups.find((group) => group.group_number === index + 1)?.[measure] ?? null;
          return value === null ? (
            <Tip
              content={
                measure === "target_weight_turnover"
                  ? "该期未产生可比较的目标权重"
                  : "收益尚未成熟或样本不足"
              }
            >
              <span className="muted num">{EMPTY}</span>
            </Tip>
          ) : (
            <span
              className={`num ${measure === "cumulative_return" ? toneClass(toneOf(value)) : ""}`}
            >
              {measure === "cumulative_return"
                ? formatSignedPercent(value * 100)
                : formatPercent(value * 100)}
            </span>
          );
        },
      }),
    ),
  ];

  return (
    <div className="factor-research">
      <div className="factor-research-meta">
        <Pill kind="acc">历史回溯研究</Pill>
        <span className="num">{period}</span>
        <span>{research.pool_label}</span>
        <span>调仓 {research.holding_sessions} 个交易日</span>
        <Tip content={`${research.basis_label}；未计成交和费用。`}>收益口径</Tip>
        <span className="num">
          样本 {formatCount(covered)} / {formatCount(expected)}
        </span>
        {partial ? <span className="factor-partial">部分日期可计算</span> : null}
      </div>
      <div className="factor-research-top">
        <section className="factor-result-section" aria-label="IC 统计">
          <div className="factor-section-head">
            <h3>IC 统计</h3>
            <Segmented
              label="IC 算法"
              value={method}
              onChange={setMethod}
              options={[
                { value: "normal_ic", label: "NormalIC" },
                { value: "rank_ic", label: "RankIC" },
              ]}
            />
          </div>
          <dl className="factor-stats">
            {stats.map((stat) => (
              <div key={stat.label}>
                <dt>{stat.label}</dt>
                <dd>{stat.value}</dd>
              </div>
            ))}
          </dl>
        </section>
        <section className="factor-result-section" aria-label="IC 衰减">
          <div className="factor-section-head">
            <h3>IC 衰减</h3>
            <span className="hint">滞后 1–10 期</span>
          </div>
          <EChart
            label="IC 衰减"
            className="chart sm"
            build={(colors) => decayOption(research, method, colors)}
          />
          <Disclosure label="查看衰减明细">
            <DataTable
              rows={decay}
              columns={decayColumns}
              rowKey={(point) => String(point.lag)}
              label="衰减明细"
            />
          </Disclosure>
        </section>
      </div>
      <section className="factor-result-section" aria-label="IC 时序">
        <div className="factor-section-head">
          <h3>IC 时序</h3>
          <Tip content="累计 IC 是有效日 IC 之和，不是收益。">累计 IC</Tip>
        </div>
        <EChart
          label="IC 时序与累计 IC"
          className="chart lg"
          build={(colors) => icOption(research, method, colors)}
        />
        <Disclosure label="查看 IC 明细">
          <DataTable
            rows={points}
            columns={icColumns}
            rowKey={(point) => point.date}
            label="IC 明细"
          />
        </Disclosure>
      </section>
      <div className="factor-research-bottom">
        <section className="factor-result-section" aria-label="分组累计收益">
          <div className="factor-section-head">
            <h3>分组累计收益</h3>
            {counts.length > 0 ? (
              <Segmented
                label="分组数"
                value={String(count)}
                onChange={(value) => setRequestedGroupCount(Number(value))}
                options={counts.map((value) => ({ value: String(value), label: `${value} 组` }))}
              />
            ) : null}
          </div>
          {count === null ? (
            <EmptyState title="暂无分组收益" hint="收益窗口成熟后会显示。" />
          ) : (
            <>
              <EChart
                label="分组累计收益"
                build={(colors) => groupOption(research, count, colors)}
              />
              <Disclosure label="查看分组收益明细">
                <DataTable
                  rows={grouping}
                  columns={groupColumns("cumulative_return")}
                  rowKey={(point) => point.date}
                  label="分组收益明细"
                />
              </Disclosure>
            </>
          )}
        </section>
        <section className="factor-result-section" aria-label="目标权重换手">
          <div className="factor-section-head">
            <h3>目标权重换手</h3>
            <Tip content="每期目标持仓权重的变化；不代表实际成交。">说明</Tip>
          </div>
          {count === null ? (
            <EmptyState title="暂无换手数据" hint="分组结果发布后会显示。" />
          ) : (
            <>
              <EChart
                label="目标权重换手"
                build={(colors) => turnoverOption(research, count, colors)}
              />
              <Disclosure label="查看换手明细">
                <DataTable
                  rows={grouping}
                  columns={groupColumns("target_weight_turnover")}
                  rowKey={(point) => point.date}
                  label="换手明细"
                />
              </Disclosure>
            </>
          )}
        </section>
      </div>
    </div>
  );
}

export function FactorResults({
  factor,
  generationId,
  onRefresh,
}: {
  factor: FactorDefinitionItem;
  generationId: string;
  onRefresh: () => void;
}) {
  const results = useFactorResults(generationId);
  const [selection, setSelection] = useState<SelectedRun | null>(null);
  const sameGeneration = results.serving?.generation_id === generationId;
  const runs = sameGeneration
    ? (results.data?.results ?? [])
        .filter(
          (item) =>
            item.factor_id === factor.factor_id &&
            item.factor_version === factor.version &&
            item.definition_status === "current",
        )
        .sort((a, b) => b.updated_at.localeCompare(a.updated_at))
    : [];
  const currentSelection =
    selection?.generationId === generationId &&
    selection.factorId === factor.factor_id &&
    selection.factorVersion === factor.version
      ? (runs.find((item) => item.job_id === selection.jobId) ?? null)
      : null;
  const picked =
    currentSelection ?? runs.find((item) => item.status === "succeeded") ?? runs[0] ?? null;
  const detail = useFactorResultDetail(
    generationId,
    picked?.status === "succeeded" && picked.display_status === "available" ? picked.job_id : null,
  );
  const detailMatches =
    picked !== null &&
    detail.serving?.generation_id === generationId &&
    detail.data?.availability === "ready" &&
    detail.data.result?.job_id === picked.job_id &&
    detail.data.result.factor_id === factor.factor_id &&
    detail.data.result.factor_version === factor.version &&
    detail.data.result.definition_status === "current" &&
    detail.data.result.status === "succeeded" &&
    detail.data.result.display_status === "available" &&
    detail.data.result.updated_at === picked.updated_at &&
    detail.data.result.as_of_time === picked.as_of_time;
  const research = detailMatches ? detail.data?.research : null;
  const latest = runs[0] ?? null;

  let body: React.ReactNode;
  if (results.error) {
    const changed = results.error instanceof ApiError && results.error.status === 409;
    body = (
      <ResultState
        title={results.error.message}
        hint={changed ? "重新加载后查看当前检验。" : "请稍后重试。"}
        onRefresh={changed ? onRefresh : results.refetch}
      />
    );
  } else if (results.data !== undefined && !sameGeneration) {
    body = <ResultState title="数据已更新" hint="重新加载后查看当前检验。" onRefresh={onRefresh} />;
  } else if (results.data === undefined) {
    body = (
      <div className="factor-result-loading" role="status">
        正在加载检验结果…
      </div>
    );
  } else if (results.data.availability === "unavailable") {
    body = <ResultState title="检验结果暂不可用" hint="请稍后刷新。" onRefresh={results.refetch} />;
  } else if (runs.length === 0) {
    const historical = results.data.results.some((item) => item.factor_id === factor.factor_id);
    body = (
      <ResultState
        title={historical ? "当前版本还没有检验记录" : "还没有检验记录"}
        hint="检验任务完成后会显示在这里。"
      />
    );
  } else if (picked?.status === "queued" || picked?.status === "running") {
    body = (
      <ResultState title="检验进行中" hint="完成后刷新查看结果。" onRefresh={results.refetch} />
    );
  } else if (picked?.status === "failed") {
    body = (
      <ResultState
        title="本次检验未完成"
        hint={picked.failure_message ?? "请查看任务状态后重试。"}
        onRefresh={results.refetch}
      />
    );
  } else if (picked?.display_status === "display_unavailable") {
    body = <ResultState title="这次检验没有可展示的图表" hint="历史记录仍在；可查看其他检验。" />;
  } else if (picked?.display_status === "not_published") {
    body = (
      <ResultState
        title="这次结果尚未收录图表"
        hint="可查看其他检验，或稍后再试。"
        onRefresh={results.refetch}
      />
    );
  } else if (picked?.display_status !== "available") {
    body = <ResultState title="图表尚未就绪" hint="稍后刷新查看。" onRefresh={results.refetch} />;
  } else if (detail.error) {
    const changed = detail.error instanceof ApiError && detail.error.status === 409;
    body = (
      <ResultState
        title={detail.error.message}
        hint={changed ? "重新加载后查看当前检验。" : "请稍后重试。"}
        onRefresh={changed ? onRefresh : detail.refetch}
      />
    );
  } else if (detail.data !== undefined && !detailMatches) {
    body = (
      <ResultState title="检验详情暂不可用" hint="重新加载后查看当前检验。" onRefresh={onRefresh} />
    );
  } else if (detail.data === undefined) {
    body = (
      <div className="factor-result-loading" role="status">
        正在加载检验详情…
      </div>
    );
  } else if (research === null || research === undefined) {
    body = <ResultState title="检验详情暂不可用" hint="请稍后刷新。" onRefresh={results.refetch} />;
  } else {
    body = <Research key={`${generationId}:${picked.job_id}`} research={research} />;
  }

  return (
    <Panel
      label="检验结果"
      title="检验结果"
      sub={
        latest ? (
          <span>
            最近更新 <RelativeTime at={latest.updated_at} />
          </span>
        ) : undefined
      }
    >
      {runs.length > 0 ? (
        <div className="factor-run-list">
          <div className="factor-section-head">
            <h3>最近检验</h3>
            <span className="hint">选择记录查看</span>
          </div>
          <DataTable
            rows={runs.slice(0, 10)}
            columns={runColumns}
            rowKey={(item) => item.job_id}
            selectedKey={picked?.job_id}
            onSelect={(item) =>
              setSelection({
                generationId,
                factorId: factor.factor_id,
                factorVersion: factor.version,
                jobId: item.job_id,
              })
            }
            label="最近检验"
            height={200}
          />
        </div>
      ) : null}
      {body}
    </Panel>
  );
}
