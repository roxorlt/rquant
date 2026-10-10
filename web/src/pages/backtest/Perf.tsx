import type { components } from "@/api/schema";
import { formatPercent } from "@/format/number";
import { KpiStrip, Panel } from "@/ui";

type BacktestPerf = components["schemas"]["BacktestPerf"];

const pct = (v: number | null | undefined, d = 2) =>
  v === null || v === undefined ? "—" : formatPercent(v * 100, d);
const num = (v: number | null | undefined) => (v === null || v === undefined ? "—" : v.toFixed(2));

/** NAV line with the drawdown band under it; plain SVG, no chart library. */
function NavChart({ nav }: { nav: BacktestPerf["nav"] }) {
  const w = 720;
  const h = 200;
  const dd = 60;
  if (nav.length < 2) return null;
  const values = nav.flatMap((p) => (p.benchmark_nav == null ? [p.nav] : [p.nav, p.benchmark_nav]));
  const lo = Math.min(1, ...values);
  const hi = Math.max(1, ...values);
  const minDd = Math.min(-0.0001, ...nav.map((p) => p.drawdown));
  const x = (i: number) => (i / (nav.length - 1)) * w;
  const y = (v: number) => h - ((v - lo) / (hi - lo || 1)) * (h - 8) - 4;
  const line = nav.map((p, i) => `${x(i).toFixed(1)},${y(p.nav).toFixed(1)}`).join(" ");
  const benchLine = nav
    .filter((p) => p.benchmark_nav != null)
    .map((p) => `${x(nav.indexOf(p)).toFixed(1)},${y(p.benchmark_nav ?? 1).toFixed(1)}`)
    .join(" ");
  const band = nav
    .map((p, i) => `${x(i).toFixed(1)},${(h + (p.drawdown / minDd) * dd).toFixed(1)}`)
    .join(" ");
  return (
    <svg
      viewBox={`0 0 ${w} ${h + dd}`}
      width="100%"
      role="img"
      aria-label="净值与回撤"
      style={{ display: "block" }}
    >
      <line x1={0} x2={w} y1={y(1)} y2={y(1)} stroke="var(--line, #ccc)" strokeDasharray="3 3" />
      {benchLine ? (
        <polyline
          points={benchLine}
          fill="none"
          stroke="var(--text-3, #999)"
          strokeWidth={1}
          strokeDasharray="4 2"
        />
      ) : null}
      <polyline points={line} fill="none" stroke="var(--accent)" strokeWidth={1.5} />
      <polygon points={`0,${h} ${band} ${w},${h}`} fill="var(--down, #2a9d5c)" opacity={0.25} />
    </svg>
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
