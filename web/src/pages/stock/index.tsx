import { Link, useParams } from "react-router";
import { useStock } from "@/api/endpoints";
import { Candles } from "@/charts/Candles";
import { kpiTone } from "@/format/color";
import { formatShanghaiDateTime } from "@/format/time";
import { ChangeText, KpiStrip, PageHeader, Panel, Pill } from "@/ui";
import { QueryView } from "../shared";

const pct = (v: number | null | undefined) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(2)}%`;
const fix = (v: number | null | undefined, d = 2) => (v == null ? "—" : v.toFixed(d));
/** Tushare total_mv is in 万元. */
const yi = (wan: number | null | undefined) => (wan == null ? "—" : `${(wan / 1e4).toFixed(0)} 亿`);

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
                  {last ? <span className="px">{last.close.toFixed(2)}</span> : null}
                  {last ? <ChangeText value={last.pct_chg ?? null} /> : null}
                </span>
              }
              note={last ? `截至 ${last.date}` : "没有行情"}
            />
            <KpiStrip
              label="估值与区间收益"
              compact
              items={[
                { key: "pe", label: "PE(TTM)", value: fix(s.valuation?.pe_ttm, 1) },
                { key: "pb", label: "PB", value: fix(s.valuation?.pb) },
                { key: "mv", label: "总市值", value: yi(s.valuation?.total_mv) },
                { key: "cmv", label: "流通市值", value: yi(s.valuation?.circ_mv) },
                { key: "to", label: "换手率", value: fix(s.valuation?.turnover_rate), unit: "%" },
                ...Object.entries(s.returns).map(([k, v]) => ({
                  key: k,
                  label: k,
                  value: pct(v),
                  tone: kpiTone(v),
                })),
              ]}
            />
            <Panel title="日K线" sub={`近 ${s.bars.length} 个交易日，未复权`}>
              <Candles bars={s.bars} />
            </Panel>
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
                        <Pill kind="ok">{p}</Pill>
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
                          <td>{String(a.trigger_type ?? "")}</td>
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
