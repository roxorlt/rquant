import type { ReactNode } from "react";
import { Link, useParams } from "react-router";
import { useStock } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import { poolRefLabel, triggerLabel } from "@/copy/publicText";
import { formatFixed } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { ChangeText, PageHeader, Panel, Pill, Segmented } from "@/ui";
import { QueryView } from "../shared";
import { FlowPanel } from "./Flow";
import { IntradayPanel } from "./Intraday";
import { KlinePanel } from "./Kline";
import { QuoteGrid } from "./QuoteGrid";
import { TpoPanel } from "./Tpo";

const fix = (v: number | null | undefined, d = 2) => (v == null ? "—" : formatFixed(v, d));

function ChartTabs({
  kline,
  intraday,
}: {
  kline: (period: ReactNode) => ReactNode;
  intraday: (span: 1 | 5, period: ReactNode) => ReactNode;
}) {
  const [tab, setTab] = useUrlState("chart");
  const cur = tab === "intraday" || tab === "5d" ? tab : "kline";
  const period = (
    <Segmented
      label="K线周期"
      value={cur}
      onChange={(next) => setTab(next === "kline" ? null : next)}
      options={[
        { value: "kline", label: "日K" },
        { value: "intraday", label: "分时" },
        { value: "5d", label: "5日" },
      ]}
    />
  );
  return cur === "kline" ? kline(period) : intraday(cur === "5d" ? 5 : 1, period);
}

/** 个股档案 (F9-lite): quote, K-line, valuation, returns, fundamentals, pools, alerts. */
export default function StockPage() {
  const { code } = useParams();
  const query = useStock(code);
  return (
    <QueryView query={query}>
      {(s) => {
        const last = s.bars[s.bars.length - 1];
        return (
          <>
            <PageHeader
              eyebrow={[s.industry, s.market, s.list_date ? `上市 ${s.list_date}` : null]
                .filter(Boolean)
                .join(" · ")}
              title={
                <span className="stock-head">
                  <span>{s.name ?? s.code}</span>
                  <span className="mono hint">{s.code}</span>
                  {last ? <span className="px">{formatFixed(last.close, 2)}</span> : null}
                  {last ? <ChangeText value={last.pct_chg ?? null} /> : null}
                </span>
              }
              note={last ? null : "没有行情"}
            />
            <QuoteGrid s={s} />
            <ChartTabs
              kline={(period) => (
                <KlinePanel
                  key={s.code}
                  code={s.code}
                  bars={s.bars}
                  flow={s.flow}
                  period={period}
                />
              )}
              intraday={(span, period) => (
                <IntradayPanel key={s.code} code={s.code} span={span} period={period} />
              )}
            />
            <TpoPanel code={s.code} />
            <FlowPanel s={s} />
            <div className="risk-block">
              <Panel
                title="财务指标"
                sub={s.fundamentals_period ? `报告期 ${s.fundamentals_period}` : "暂无"}
                flush
              >
                {Object.keys(s.fundamentals).length ? (
                  <table className="tbl" aria-label="个股财务">
                    <tbody>
                      {Object.entries(s.fundamentals).map(([k, v]) => (
                        <tr key={k}>
                          <td>{s.fundamentals_fields[k] ?? k}</td>
                          <td
                            className={`num ${k.endsWith("yoy") && v != null ? (v > 0 ? "up" : v < 0 ? "down" : "") : ""}`}
                          >
                            {fix(v)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <p className="hint pad">没有这只股票的财务指标（数据中心按股票池抓取）。</p>
                )}
              </Panel>
              <Panel title="关联" sub="所在池子与近期告警">
                <p>
                  {s.pools.length ? (
                    s.pools.map((p) => (
                      <Link key={p} to={`/pools?pool=${encodeURIComponent(p)}`}>
                        <Pill kind="ok">{poolRefLabel(p)}</Pill>
                      </Link>
                    ))
                  ) : (
                    <span className="hint">不在任何池子里</span>
                  )}
                </p>
                {s.alerts.length ? (
                  <table className="tbl" aria-label="个股告警">
                    <tbody>
                      {s.alerts.map((a) => (
                        <tr key={`${String(a.at)}-${String(a.trigger_type)}-${String(a.level)}`}>
                          <td className="mono">
                            {a.at ? formatShanghaiDateTime(String(a.at)) : "—"}
                          </td>
                          <td>{String(a.level ?? "")}</td>
                          <td>
                            {triggerLabel(a.trigger_type == null ? null : String(a.trigger_type))}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <p className="hint">近期没有告警。</p>
                )}
              </Panel>
            </div>
          </>
        );
      }}
    </QueryView>
  );
}
