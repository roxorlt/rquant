import { useState } from "react";
import { Link } from "react-router";
import {
  useExplainBacktest,
  usePortfolioBacktest,
  usePortfolioBacktestMany,
  usePortfolioBacktests,
  useStrategies,
} from "@/api/endpoints";
import type { components } from "@/api/schema";
import { useUrlState } from "@/app/useUrlState";
import { TimeChart } from "@/charts/Chart";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, ChangeText, KpiStrip, Panel, Pill } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import { BrinsonPanel } from "./Brinson";
import { PerfPanel } from "./Perf";

type Perf = components["schemas"]["BacktestPerf"];
const pct = (v: number | null | undefined) => (v == null ? "—" : `${(v * 100).toFixed(2)}%`);
const num = (v: number | null | undefined, d = 2) => (v == null ? "—" : v.toFixed(d));

const COMPARE_COLORS = ["#2448D0", "#C47B12", "#6B4C9A", "#0E7490", "#9A3412"];
const compareColor = new Map<string, string>();

function colorForRun(id: string): string {
  const existing = compareColor.get(id);
  if (existing) return existing;
  const used = new Set(compareColor.values());
  const free =
    COMPARE_COLORS.find((color) => !used.has(color)) ??
    COMPARE_COLORS[compareColor.size % COMPARE_COLORS.length] ??
    "#2448D0";
  compareColor.set(id, free);
  return free;
}

function releaseRunColor(id: string): void {
  compareColor.delete(id);
}

/** Up to five runs side by side: NAV overlay plus a metric table, best value highlighted. */
function Compare({ ids, onRemove }: { ids: string[]; onRemove: (id: string) => void }) {
  const results = usePortfolioBacktestMany(ids);
  if (results.some((r) => r.isLoading)) return <Panel title="对比">读取中…</Panel>;
  const runs = results.flatMap((r) => (r.data ? [r.data.data] : []));
  if (runs.length < 2) return null;
  const rows: [
    string,
    (p: Perf | null | undefined) => number | null | undefined,
    (v: number | null | undefined) => string,
    boolean,
  ][] = [
    ["总收益", (p) => p?.total_return, pct, true],
    ["年化", (p) => p?.annualized_return, pct, true],
    ["年化波动", (p) => p?.annualized_volatility, pct, false],
    ["夏普", (p) => p?.sharpe, num, true],
    ["卡玛", (p) => p?.calmar, num, true],
    ["最大回撤", (p) => p?.max_drawdown, pct, true],
    ["日胜率", (p) => p?.win_rate, pct, true],
    ["超额", (p) => p?.benchmark?.excess_return, pct, true],
  ];
  // Union of dates so runs over different windows still line up.
  const dates = [...new Set(runs.flatMap((r) => r.perf?.nav.map((p) => p.date) ?? []))].sort();
  return (
    <Panel
      title={`对比 · ${runs.length} 条`}
      sub="净值叠在一起。加粗并标「最优」的是这一行更好的数；回撤越小越好。"
    >
      <div>
        {runs.map((r) => (
          <span key={r.run.run_id} className="cmp-chip">
            <i className="cmp-swatch" style={{ background: colorForRun(r.run.run_id) }} />
            {r.run.title}
            <button
              type="button"
              className="x"
              aria-label={`移出对比：${r.run.title}`}
              onClick={() => {
                releaseRunColor(r.run.run_id);
                onRemove(r.run.run_id);
              }}
            >
              ×
            </button>
          </span>
        ))}
      </div>
      <TimeChart
        label="对比净值"
        dates={dates}
        baseline={1}
        format={(v) => v.toFixed(3)}
        series={runs.map((r) => {
          const byDate = new Map(r.perf?.nav.map((p) => [p.date, p.nav]) ?? []);
          return {
            name: r.run.title,
            values: dates.map((d) => byDate.get(d) ?? null),
            color: colorForRun(r.run.run_id),
          };
        })}
      />
      <table className="tbl compare-table" aria-label="回测对比">
        <thead>
          <tr>
            <th>指标</th>
            {runs.map((r) => (
              <th key={r.run.run_id}>
                <i className="cmp-swatch" style={{ background: colorForRun(r.run.run_id) }} />{" "}
                {r.run.title}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>参数</td>
            {runs.map((r) => (
              <td key={r.run.run_id} className="num">
                {`持股 ${r.run.max_positions} 只 · 每 ${r.run.rebalance_every} 个交易日调仓`}
              </td>
            ))}
          </tr>
          {rows.map(([label, get, fmt, higher]) => {
            const vals = runs.map((r) => get(r.perf));
            const real = vals.filter((v): v is number => v != null);
            const best = real.length ? (higher ? Math.max(...real) : Math.min(...real)) : null;
            return (
              <tr key={label}>
                <td>{label}</td>
                {vals.map((v, k) => (
                  <td
                    key={runs[k]?.run.run_id}
                    className={v != null && v === best && real.length > 1 ? "num best" : "num"}
                  >
                    {fmt(v)}
                    {v != null && v === best && real.length > 1 ? " 最优" : ""}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </Panel>
  );
}

function Explain({ runId }: { runId: string }) {
  const explain = useExplainBacktest();
  const result = explain.data;
  return (
    <Panel
      title="AI 解读"
      sub="只用上面的指标；回答里的每个数字都会和指标核对"
      actions={
        <Button size="sm" variant="ghost" onClick={() => explain.mutate(runId)}>
          {explain.isPending ? "解读中…" : "生成解读"}
        </Button>
      }
    >
      {explain.error ? (
        <div className="error-block" role="alert">
          <b>解读没生成</b>
          <p>解读服务暂时没有响应，回测数字不受影响。</p>
          <Button size="sm" onClick={() => explain.mutate(runId)}>
            重试
          </Button>
        </div>
      ) : null}
      {result ? (
        <>
          {result.unverified.length ? (
            <Pill kind="warn">{`有 ${result.unverified.length} 个数字无法核对：${result.unverified.join("、")}`}</Pill>
          ) : (
            <Pill kind="ok">数字已全部核对</Pill>
          )}
          <p aria-live="polite" style={{ whiteSpace: "pre-wrap" }}>
            {result.text}
          </p>
          <span className="sub">{`模型：${result.model}`}</span>
        </>
      ) : null}
    </Panel>
  );
}

function Detail({ runId, onCompare }: { runId: string; onCompare: () => void }) {
  const query = usePortfolioBacktest(runId);
  return (
    <QueryView query={query}>
      {(data) => (
        <>
          {data.perf ? <PerfPanel perf={data.perf} /> : null}
          <Panel
            title="过拟合检查"
            sub={`同一预设已存 ${data.overfit?.trials ?? 0} 次回测，按试验数计 DSR（下限估计）`}
            actions={
              <Button size="sm" variant="ghost" onClick={onCompare}>
                加入对比
              </Button>
            }
          >
            <KpiStrip
              label="过拟合指标"
              compact
              items={[
                { key: "psr", label: "PSR(>0)", value: pct(data.overfit?.psr) },
                { key: "dsr", label: "DSR", value: pct(data.overfit?.dsr) },
                {
                  key: "trl",
                  label: "最短样本(95%)",
                  value: data.overfit?.min_track_record_days ?? "—",
                  unit: "天",
                },
                { key: "obs", label: "样本天数", value: data.overfit?.observations ?? "—" },
              ]}
            />
          </Panel>
          <Explain runId={runId} />
          <BrinsonPanel runId={runId} />
          {(data.exposure ?? []).length ? (
            <Panel title="行业暴露" sub="期末权重 vs 最后一次候选池等权" flush>
              <table className="tbl" aria-label="行业暴露">
                <thead>
                  <tr>
                    <th>行业</th>
                    <th className="num">期末</th>
                    <th className="num">平均</th>
                    <th className="num">候选池</th>
                    <th className="num">偏离</th>
                  </tr>
                </thead>
                <tbody>
                  {(data.exposure ?? []).map((row) => (
                    <tr key={row.industry}>
                      <td>{row.industry}</td>
                      <td className="num">{pct(row.weight)}</td>
                      <td className="num">{pct(row.avg_weight)}</td>
                      <td className="num">{pct(row.pool_weight)}</td>
                      <td className="num">{pct(row.deviation)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Panel>
          ) : null}
          <Panel title={`委托 · ${data.orders?.length ?? 0}`} sub="含拒单原因" flush>
            <DataTable
              label="组合回测委托"
              rows={data.orders ?? []}
              rowKey={(row) => `${row.trade_date}:${row.code}:${row.side}`}
              height={360}
              columns={[
                { id: "date", header: "日期", value: (row) => row.trade_date },
                {
                  id: "code",
                  header: "代码",
                  value: (row) => row.code,
                  cell: (row) => <StockLink code={row.code} />,
                },
                {
                  id: "side",
                  header: "方向",
                  value: (row) => row.side,
                  cell: (row) => (row.side === "buy" ? "买入" : "卖出"),
                },
                { id: "qty", header: "数量", numeric: true, value: (row) => row.quantity },
                {
                  id: "price",
                  header: "价格",
                  numeric: true,
                  value: (row) => row.price ?? null,
                  cell: (row) => formatPrice(row.price),
                },
                { id: "fee", header: "费用", numeric: true, value: (row) => row.fee },
                {
                  id: "status",
                  header: "状态",
                  value: (row) => row.status,
                  cell: (row) =>
                    row.status === "filled" ? (
                      <Pill kind="ok">成交</Pill>
                    ) : (
                      <Pill kind="warn">拒单 · {row.reason}</Pill>
                    ),
                },
              ]}
            />
          </Panel>
        </>
      )}
    </QueryView>
  );
}

export function PortfolioRuns() {
  const query = usePortfolioBacktests();
  const [runId, setRunId] = useUrlState("prun");
  const [picked, setPicked] = useState<string[]>([]);
  const addCompare = (id: string) =>
    setPicked((list) => (list.includes(id) ? list : [...list, id].slice(-5)));
  const [preset, setPreset] = useUrlState("preset");
  return (
    <QueryView query={query}>
      {(all) => {
        const data = {
          ...all,
          runs: preset ? all.runs.filter((r) => r.preset === preset) : all.runs,
        };
        return (
          <>
            <Panel
              title={`组合回测 · ${data.runs.length}`}
              sub={preset ? `只看入场池 ${preset}` : "点一行看净值和委托"}
              actions={
                <>
                  <span className="hint">已选 {picked.length} / 5</span>
                  {picked.length >= 2 ? (
                    <Button
                      size="sm"
                      onClick={() =>
                        document.getElementById("compare-panel")?.scrollIntoView({ block: "start" })
                      }
                    >
                      看对比
                    </Button>
                  ) : null}
                  {preset ? (
                    <Button size="sm" variant="ghost" onClick={() => setPreset(null)}>
                      显示全部
                    </Button>
                  ) : null}
                </>
              }
              flush
            >
              <DataTable
                label="组合回测列表"
                tools="组合回测"
                rows={data.runs}
                rowKey={(row) => row.run_id}
                onSelect={(row) => setRunId(row.run_id)}
                selectedKey={runId}
                emptyText="还没有组合回测结果。"
                columns={[
                  {
                    id: "cmp",
                    header: "对比",
                    value: () => null,
                    cell: (row) => {
                      const on = picked.includes(row.run_id);
                      const full = !on && picked.length >= 5;
                      return (
                        <Button
                          size="sm"
                          variant="ghost"
                          disabled={full}
                          aria-pressed={on}
                          onClick={(event) => {
                            event.stopPropagation();
                            if (on) {
                              releaseRunColor(row.run_id);
                              setPicked((list) => list.filter((id) => id !== row.run_id));
                            } else addCompare(row.run_id);
                          }}
                        >
                          {on ? "已在对比" : "加入对比"}
                        </Button>
                      );
                    },
                  },
                  { id: "title", header: "名称", value: (row) => row.title },
                  { id: "strategy", header: "策略版本", value: (row) => row.strategy ?? "—" },
                  {
                    id: "range",
                    header: "区间",
                    value: (row) => row.start,
                    cell: (row) => `${row.start} ~ ${row.end}`,
                  },
                  { id: "n", header: "持股数", numeric: true, value: (row) => row.max_positions },
                  {
                    id: "every",
                    header: "调仓间隔（交易日）",
                    numeric: true,
                    value: (row) => row.rebalance_every,
                  },
                  {
                    id: "nav",
                    header: "期末净值",
                    numeric: true,
                    value: (row) => row.final_nav ?? null,
                    cell: (row) => (row.final_nav == null ? "—" : row.final_nav.toFixed(4)),
                  },
                  {
                    id: "ret",
                    header: "收益",
                    numeric: true,
                    value: (row) => (row.final_nav == null ? null : row.final_nav - 1),
                    cell: (row) => (
                      <ChangeText
                        value={row.final_nav == null ? null : (row.final_nav - 1) * 100}
                      />
                    ),
                  },
                  { id: "filled", header: "成交", numeric: true, value: (row) => row.filled },
                  { id: "rejected", header: "拒单", numeric: true, value: (row) => row.rejected },
                ]}
              />
            </Panel>
            <div id="compare-panel">
              {picked.length >= 2 ? (
                <Compare
                  ids={picked}
                  onRemove={(id) => setPicked((list) => list.filter((item) => item !== id))}
                />
              ) : picked.length === 1 ? (
                <Panel title="对比">已选 1 条，再选一条就能对比，最多 5 条。</Panel>
              ) : null}
            </div>
            {runId ? <Detail runId={runId} onCompare={() => addCompare(runId)} /> : null}
          </>
        );
      }}
    </QueryView>
  );
}

export function StrategyList() {
  const query = useStrategies();
  return (
    <QueryView query={query}>
      {(data) => (
        <Panel title="策略版本" sub="保存过的入场池和持仓规则" flush>
          <DataTable
            label="策略版本"
            rows={data.strategies}
            rowKey={(row) => row.slug}
            emptyText="还没有保存的策略。"
            columns={[
              { id: "title", header: "策略", value: (row) => row.title },
              {
                id: "ver",
                header: "当前版本",
                value: (row) => row.version,
                cell: (row) => `${row.slug}@${row.version}（共 ${row.versions} 版）`,
              },
              { id: "preset", header: "入场池", value: (row) => row.preset },
              { id: "n", header: "持股数", numeric: true, value: (row) => row.max_positions },
              {
                id: "method",
                header: "权重",
                value: (row) => row.method,
                cell: (row) => (row.method === "equal" ? "等权" : "按分"),
              },
              {
                id: "every",
                header: "调仓间隔",
                numeric: true,
                value: (row) => row.rebalance_every,
              },
              {
                id: "go",
                header: "",
                sortable: false,
                value: () => null,
                cell: () => <Link to="/backtest">看回测 →</Link>,
              },
            ]}
          />
        </Panel>
      )}
    </QueryView>
  );
}
