import type { components } from "@/api/schema";
import { TimeChart } from "@/charts/Chart";
import { formatPercent } from "@/format/number";
import { KpiStrip, Panel } from "@/ui";

type BacktestPerf = components["schemas"]["BacktestPerf"];

const pct = (v: number | null | undefined, d = 2) =>
  v === null || v === undefined ? "—" : formatPercent(v * 100, d);
const num = (v: number | null | undefined) => (v === null || v === undefined ? "—" : v.toFixed(2));

/** NAV vs benchmark, then drawdown underneath, sharing the date axis. */
function NavChart({ nav }: { nav: BacktestPerf["nav"] }) {
  if (nav.length < 2) return null;
  const dates = nav.map((p) => p.date);
  const hasBench = nav.some((p) => p.benchmark_nav != null);
  return (
    <>
      <TimeChart
        label="净值"
        dates={dates}
        baseline={1}
        format={(v) => v.toFixed(3)}
        series={[
          { name: "策略净值", values: nav.map((p) => p.nav), color: "var(--accent)" },
          ...(hasBench
            ? [
                {
                  name: "基准",
                  values: nav.map((p) => p.benchmark_nav ?? null),
                  color: "var(--faint)",
                  dashed: true,
                },
              ]
            : []),
        ]}
      />
      <TimeChart
        label="回撤"
        dates={dates}
        height={110}
        baseline={0}
        format={(v) => `${(v * 100).toFixed(1)}%`}
        series={[
          { name: "回撤", values: nav.map((p) => p.drawdown), color: "var(--down)", area: true },
        ]}
      />
    </>
  );
}

function Monthly({ monthly }: { monthly: BacktestPerf["monthly"] }) {
  const years = [...new Set(monthly.map((m) => m.year))].sort();
  const cell = new Map(monthly.map((m) => [`${m.year}-${m.month}`, m.ret]));
  const months = Array.from({ length: 12 }, (_, i) => i + 1);
  return (
    <table className="tbl" aria-label="月度收益">
      <thead>
        <tr>
          <th>年</th>
          {months.map((m) => (
            <th key={m}>{m}月</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {years.map((year) => (
          <tr key={year}>
            <td>{year}</td>
            {months.map((m) => {
              const v = cell.get(`${year}-${m}`);
              const alpha = v === undefined ? 0 : Math.min(0.6, Math.abs(v) * 4);
              const bg =
                v === undefined
                  ? undefined
                  : `color-mix(in srgb, ${v >= 0 ? "var(--up)" : "var(--down, #2a9d5c)"} ${Math.round(alpha * 100)}%, transparent)`;
              return (
                <td key={m} className="num" style={{ background: bg }}>
                  {v === undefined ? "" : pct(v, 1)}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function PerfPanel({ perf }: { perf: BacktestPerf }) {
  return (
    <Panel title="绩效" sub={perf.method}>
      <KpiStrip
        label="绩效指标"
        compact
        items={[
          { key: "total", label: "总收益", value: pct(perf.total_return) },
          { key: "annual", label: "年化", value: pct(perf.annualized_return) },
          { key: "vol", label: "年化波动", value: pct(perf.annualized_volatility) },
          { key: "sharpe", label: "夏普", value: num(perf.sharpe) },
          { key: "sortino", label: "索提诺", value: num(perf.sortino) },
          { key: "calmar", label: "卡玛", value: num(perf.calmar) },
          {
            key: "mdd",
            label: "最大回撤",
            value: pct(perf.max_drawdown),
            sub: perf.max_drawdown_days === null ? undefined : `${perf.max_drawdown_days} 天`,
          },
          { key: "win", label: "日胜率", value: pct(perf.win_rate, 1) },
          { key: "payoff", label: "盈亏比", value: num(perf.payoff_ratio) },
          ...(perf.benchmark
            ? [
                {
                  key: "excess",
                  label: `超额 vs ${perf.benchmark.code}`,
                  value: pct(perf.benchmark.excess_return),
                  sub: `基准 ${pct(perf.benchmark.total_return)}`,
                },
                { key: "beta", label: "Beta", value: num(perf.benchmark.beta) },
                { key: "ir", label: "信息比率", value: num(perf.benchmark.information_ratio) },
              ]
            : []),
        ]}
      />
      <NavChart nav={perf.nav} />
      <Monthly monthly={perf.monthly} />
    </Panel>
  );
}
