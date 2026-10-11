import { useBrinson } from "@/api/endpoints";
import { kpiTone, toneClass, toneOf } from "@/format/color";
import { KpiStrip, Panel } from "@/ui";

const pct = (v: number) => `${v > 0 ? "+" : ""}${(v * 100).toFixed(2)}%`;
const w = (v: number) => `${(v * 100).toFixed(1)}%`;

/** Industry Brinson-Fachler attribution vs the market-cap-weighted A-share market. */
export function BrinsonPanel({ runId }: { runId: string }) {
  const query = useBrinson(runId);
  const b = query.data;
  if (query.isLoading) return <Panel title="Brinson 归因">计算中…</Panel>;
  if (!b)
    return (
      <Panel title="Brinson 归因">
        <p className="hint">研究库里没有覆盖这段区间的行情/市值数据，暂时无法归因。</p>
      </Panel>
    );
  return (
    <Panel
      title="Brinson 归因（行业）"
      sub={`基准：${b.benchmark}；${b.days} 个交易日逐日归因后相加（算术）`}
      flush
    >
      <KpiStrip
        label="归因汇总"
        compact
        items={[
          {
            key: "p",
            label: "组合",
            value: pct(b.portfolio_return),
            tone: kpiTone(b.portfolio_return),
          },
          {
            key: "b",
            label: "基准",
            value: pct(b.benchmark_return),
            tone: kpiTone(b.benchmark_return),
          },
          {
            key: "x",
            label: "超额",
            value: pct(b.portfolio_return - b.benchmark_return),
            tone: kpiTone(b.portfolio_return - b.benchmark_return),
          },
          { key: "a", label: "配置", value: pct(b.allocation), tone: kpiTone(b.allocation) },
          { key: "s", label: "选股", value: pct(b.selection), tone: kpiTone(b.selection) },
          { key: "i", label: "交互", value: pct(b.interaction), tone: kpiTone(b.interaction) },
        ]}
      />
      <table className="tbl" aria-label="Brinson 归因">
        <thead>
          <tr>
            <th>行业</th>
            <th className="num">组合权重</th>
            <th className="num">基准权重</th>
            <th className="num">配置</th>
            <th className="num">选股</th>
            <th className="num">交互</th>
            <th className="num">合计</th>
          </tr>
        </thead>
        <tbody>
          {b.rows.slice(0, 15).map((r) => {
            const total = r.allocation + r.selection + r.interaction;
            return (
              <tr key={r.industry}>
                <td>{r.industry}</td>
                <td className="num">{w(r.weight_portfolio)}</td>
                <td className="num">{w(r.weight_benchmark)}</td>
                <td className={`num ${toneClass(toneOf(r.allocation))}`}>{pct(r.allocation)}</td>
                <td className={`num ${toneClass(toneOf(r.selection))}`}>{pct(r.selection)}</td>
                <td className={`num ${toneClass(toneOf(r.interaction))}`}>{pct(r.interaction)}</td>
                <td className={`num ${toneClass(toneOf(total))}`}>{pct(total)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </Panel>
  );
}
