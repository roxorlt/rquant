import { useEventStudies } from "@/api/endpoints";
import { Panel } from "@/ui";
import { QueryView } from "../shared";

const pct = (v: number | undefined) => (v === undefined ? "—" : `${(v * 100).toFixed(2)}%`);

export function EventStudies() {
  const query = useEventStudies();
  return (
    <QueryView query={query}>
      {(data) => (
        <>
          {data.studies.map((s) => (
            <Panel
              key={s.slug}
              title={`事件研究 · ${s.title}`}
              sub={`${s.window.join("~")} · 信号 ${s.signals} · 次日开盘买入`}
            >
              <p className="sub">
                每年信号：
                {Object.entries(s.per_year ?? {})
                  .map(([y, n]) => `${y}: ${n}`)
                  .join("，")}
              </p>
              <table aria-label={`事件研究 ${s.slug}`} className="tbl">
                <thead>
                  <tr>
                    <th>持有</th>
                    <th>样本</th>
                    <th>平均</th>
                    <th>中位</th>
                    <th>胜率</th>
                    <th>超额(沪深300)</th>
                    <th>超额(中证1000)</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(s.horizons ?? {}).map(([n, h]) => (
                    <tr key={n}>
                      <td>{n} 日</td>
                      <td>{h.n}</td>
                      <td>{pct(h.mean)}</td>
                      <td>{pct(h.median)}</td>
                      <td>{pct(h.win_rate)}</td>
                      <td>{pct(h.excess_mean?.["沪深300"])}</td>
                      <td>{pct(h.excess_mean?.["中证1000"])}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="sub">注意：{(s.caveats ?? []).join("；")}</p>
            </Panel>
          ))}
        </>
      )}
    </QueryView>
  );
}
