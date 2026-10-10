import { useState } from "react";
import { usePortfolioBacktest, usePortfolioBacktests, usePortfolioCompare } from "@/api/endpoints";
import type { components } from "@/api/schema";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, KpiStrip, Panel, Pill } from "@/ui";
import { QueryView } from "../shared";
import { PerfPanel } from "./Perf";

type Perf = components["schemas"]["BacktestPerf"];
const pct = (v: number | null | undefined) => (v == null ? "—" : `${(v * 100).toFixed(2)}%`);
const num = (v: number | null | undefined, d = 2) => (v == null ? "—" : v.toFixed(d));

function Compare({ a, b }: { a: string; b: string }) {
  const query = usePortfolioCompare(a, b);
  const rows: [string, (p: Perf | null | undefined) => string][] = [
    ["总收益", (p) => pct(p?.total_return)],
    ["年化", (p) => pct(p?.annualized_return)],
    ["夏普", (p) => num(p?.sharpe)],
    ["最大回撤", (p) => pct(p?.max_drawdown)],
    ["超额", (p) => pct(p?.benchmark?.excess_return)],
  ];
  return (
    <QueryView query={query}>
      {(data) => (
        <Panel title="对比" sub={`${data.a.run.title} vs ${data.b.run.title}`}>
          <table className="tbl" aria-label="回测对比">
            <thead>
              <tr>
                <th>指标</th>
                <th>{data.a.run.title}</th>
                <th>{data.b.run.title}</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>参数</td>
                <td>{`${data.a.run.max_positions} 只 / 每 ${data.a.run.rebalance_every} 次调仓`}</td>
                <td>{`${data.b.run.max_positions} 只 / 每 ${data.b.run.rebalance_every} 次调仓`}</td>
              </tr>
              {rows.map(([label, f]) => (
                <tr key={label}>
                  <td>{label}</td>
                  <td className="num">{f(data.a.perf)}</td>
                  <td className="num">{f(data.b.perf)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      )}
    </QueryView>
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
          {data.exposure.length ? (
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
                  {data.exposure.map((row) => (
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
          <Panel title={`委托 · ${data.orders.length}`} sub="含拒单原因" flush>
            <DataTable
              label="组合回测委托"
              rows={data.orders}
              rowKey={(row) => `${row.trade_date}:${row.code}:${row.side}`}
              height={360}
              columns={[
                { id: "date", header: "日期", value: (row) => row.trade_date },
                { id: "code", header: "代码", value: (row) => row.code },
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
  const [runId, setRunId] = useState<string | null>(null);
  const [picked, setPicked] = useState<string[]>([]);
  const addCompare = (id: string) =>
    setPicked((list) => (list.includes(id) ? list : [...list, id].slice(-2)));
  return (
    <QueryView query={query}>
      {(data) => (
        <>
          <Panel title="组合回测" sub="python -m rquant.backtest 产出，点一行看净值与委托" flush>
            <DataTable
              label="组合回测列表"
              rows={data.runs}
              rowKey={(row) => row.run_id}
              onSelect={(row) => setRunId(row.run_id)}
              selectedKey={runId}
              emptyText="还没有组合回测结果"
              columns={[
                { id: "title", header: "名称", value: (row) => row.title },
                {
                  id: "range",
                  header: "区间",
                  value: (row) => row.start,
                  cell: (row) => `${row.start} ~ ${row.end}`,
                },
                { id: "n", header: "持股数", numeric: true, value: (row) => row.max_positions },
                {
                  id: "every",
                  header: "调仓间隔",
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
                { id: "filled", header: "成交", numeric: true, value: (row) => row.filled },
                { id: "rejected", header: "拒单", numeric: true, value: (row) => row.rejected },
              ]}
            />
          </Panel>
          {runId ? <Detail runId={runId} onCompare={() => addCompare(runId)} /> : null}
          {picked.length === 2 && picked[0] && picked[1] ? (
            <Compare a={picked[0]} b={picked[1]} />
          ) : picked.length === 1 ? (
            <Panel title="对比">再选一条回测，点「加入对比」</Panel>
          ) : null}
        </>
      )}
    </QueryView>
  );
}
