import type { components } from "@/api/schema";
import { TimeChart } from "@/charts/Chart";
import { kpiTone } from "@/format/color";
import { formatPercent } from "@/format/number";
import { rollingSharpe, topDrawdowns, yearlyReturns } from "@/format/perf";
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
        bands={topDrawdowns(
          nav.map((p) => p.nav),
          3,
        ).map((d) => ({
          from: d.start,
          to: d.end ?? nav.length - 1,
        }))}
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

/** Deepest drawdowns, rolling Sharpe and calendar-year returns (QuantConnect / 聚宽 style). */
function RiskBlock({ nav }: { nav: BacktestPerf["nav"] }) {
  if (nav.length < 3) return null;
  const values = nav.map((p) => p.nav);
  const dates = nav.map((p) => p.date);
  const dds = topDrawdowns(values, 5);
  const window = nav.length >= 180 ? 60 : 20;
  const sharpe = rollingSharpe(values, window);
  const years = yearlyReturns(nav);
  return (
    <div className="risk-block">
      <div>
        <h3 className="sub">前 {dds.length} 大回撤（净值图阴影为前 3）</h3>
        <table className="tbl" aria-label="前五大回撤">
          <thead>
            <tr>
              <th>开始</th>
              <th>谷底</th>
              <th>恢复</th>
              <th className="num">深度</th>
              <th className="num">持续(日)</th>
            </tr>
          </thead>
          <tbody>
            {dds.map((d) => (
              <tr key={d.start}>
                <td className="mono">{dates[d.start]}</td>
                <td className="mono">{dates[d.trough]}</td>
                <td className="mono">{d.end === null ? "未恢复" : dates[d.end]}</td>
                <td className="num down">{pct(d.depth)}</td>
                <td className="num">{(d.end ?? nav.length - 1) - d.start}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <h3 className="sub">年度收益</h3>
        <table className="tbl" aria-label="年度收益">
          <tbody>
            {years.map((y) => (
              <tr key={y.year}>
                <td>{y.year}</td>
                <td className={`num ${y.ret > 0 ? "up" : y.ret < 0 ? "down" : ""}`}>
                  {pct(y.ret)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div>
        <h3 className="sub">滚动夏普（{window} 日）</h3>
        {sharpe.some((v) => v !== null) ? (
          <TimeChart
            label="滚动夏普"
            dates={dates}
            height={150}
            baseline={0}
            format={(v) => v.toFixed(1)}
            series={[{ name: "夏普", values: sharpe, color: "var(--accent)" }]}
          />
        ) : (
          <p className="hint">区间不足 {window + 1} 个交易日，暂不计算滚动夏普。</p>
        )}
      </div>
    </div>
  );
}

function Monthly({ monthly }: { monthly: BacktestPerf["monthly"] | null | undefined }) {
  if (!monthly?.length) return null;
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
          {
            key: "total",
            label: "总收益",
            value: pct(perf.total_return),
            tone: kpiTone(perf.total_return),
          },
          {
            key: "annual",
            label: "年化",
            value: pct(perf.annualized_return),
            tone: kpiTone(perf.annualized_return),
          },
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
                  tone: kpiTone(perf.benchmark.excess_return),
                  sub: `基准 ${pct(perf.benchmark.total_return)}`,
                },
                { key: "beta", label: "Beta", value: num(perf.benchmark.beta) },
                { key: "ir", label: "信息比率", value: num(perf.benchmark.information_ratio) },
              ]
            : []),
        ]}
      />
      <NavChart nav={perf.nav} />
      <RiskBlock nav={perf.nav} />
      <Monthly monthly={perf.monthly} />
    </Panel>
  );
}
