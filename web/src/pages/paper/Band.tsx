import { useState } from "react";
import { usePaper, usePortfolioBacktests, usePortfolioBand } from "@/api/endpoints";
import { Panel, Pill } from "@/ui";

const pct = (v: number) => `${(v * 100).toFixed(2)}%`;

/** Weekdays from start (exclusive) to today — close enough for an A-share day count. */
function tradingDaysSince(start: string): number {
  const from = new Date(`${start}T00:00:00`);
  const today = new Date();
  let n = 0;
  for (const d = new Date(from); d < today; d.setDate(d.getDate() + 1)) {
    const wd = d.getDay();
    if (d > from && wd !== 0 && wd !== 6) n += 1;
  }
  return n;
}

export function PaperBand() {
  const runs = usePortfolioBacktests();
  const paper = usePaper();
  const [runId, setRunId] = useState<string>("");
  const [start, setStart] = useState("");
  const [capital, setCapital] = useState(1_000_000);
  const days = start ? Math.min(500, tradingDaysSince(start)) : 0;
  const band = usePortfolioBand(runId || null, days);
  const last = band.data?.points.at(-1);
  return (
    <Panel
      title="与回测区间对照"
      sub="用回测日收益自助抽样（假设独立同分布）得到 N 日后 5–95% 区间，看模拟盘是否落在区间内"
    >
      <div className="rule-form">
        <select aria-label="对照回测" value={runId} onChange={(e) => setRunId(e.target.value)}>
          <option value="">选择组合回测</option>
          {(runs.data?.runs ?? []).map((r) => (
            <option key={r.run_id} value={r.run_id}>
              {r.title}
            </option>
          ))}
        </select>
        <label>
          模拟起始日
          <input
            aria-label="模拟起始日"
            type="date"
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
        </label>
        <label>
          初始资金
          <input
            aria-label="初始资金"
            type="number"
            value={capital}
            onChange={(e) => setCapital(Number(e.target.value))}
          />
        </label>
      </div>
      {last ? (
        <table className="tbl" aria-label="区间对照">
          <thead>
            <tr>
              <th>账户</th>
              <th className="num">模拟收益</th>
              <th className="num">{`${days} 日区间 5% / 50% / 95%`}</th>
              <th>位置</th>
            </tr>
          </thead>
          <tbody>
            {(paper.data?.accounts ?? []).map((a) => {
              const ret = a.nav / capital - 1;
              const where = ret < last.p5 ? "低于区间" : ret > last.p95 ? "高于区间" : "区间内";
              return (
                <tr key={a.account_id}>
                  <td>{a.account_id}</td>
                  <td className="num">{pct(ret)}</td>
                  <td className="num">{`${pct(last.p5)} / ${pct(last.p50)} / ${pct(last.p95)}`}</td>
                  <td>
                    <Pill kind={where === "区间内" ? "ok" : "warn"}>{where}</Pill>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      ) : (
        <p className="sub">选好回测和起始日后显示区间（回测至少需要 5 个交易日）。</p>
      )}
    </Panel>
  );
}
