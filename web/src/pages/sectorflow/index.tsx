import { useEffect, useMemo, useState } from "react";
import {
  inTradingHours,
  type SectorFlowData,
  useSectorFlow,
  useSectorLeadersAt,
} from "@/api/endpoints";
import { HelpMark, PageHeader, Segmented } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { PHONE_QUERY, useMedia } from "../panorama/useMedia";
import { FlowChart } from "./FlowChart";
import {
  colorMap,
  DEFAULT_TOP_N,
  defaultSelection,
  fmtAmountYi,
  fmtPct,
  fmtRatioPct,
  fmtSectorNet,
  fmtTurnoverChg,
  fmtValue,
  fmtYi,
  type Mode,
  type Sector,
  tone,
} from "./model";
import "./sectorflow.css";

type Leader = SectorFlowData["leaders"][number];
type Stock = Leader["stocks"][number];

const MODES: { value: Mode; label: string }[] = [
  { value: "net", label: "净额" },
  { value: "ratio", label: "占成交额%" },
  { value: "pct", label: "板块涨幅" },
];

export default function SectorFlowPage() {
  const phone = useMedia(PHONE_QUERY);
  const [day, setDay] = useState<string | null>(null);
  const [replayAt, setReplayAt] = useState<string | null>(null);
  const q = useSectorFlow(day);
  const data = q.data;
  return (
    <div className={`sf ${phone ? "sf-phone" : ""}`}>
      <PageHeader eyebrow="跟踪与告警" title="板块资金流" />
      {q.error ? <p className="hint pad">暂时读不到数据</p> : null}
      {!data && !q.error ? <p className="hint pad">加载中…</p> : null}
      {data?.warming ? <p className="hint pad">正在汇总当日快照（首次约 10–60 秒）…</p> : null}
      {data && !data.warming && !data.sectors.length ? (
        <p className="hint pad">今天还没有快照（09:25 集合竞价后出现）。</p>
      ) : null}
      {data && !data.warming && data.sectors.length ? (
        <Body
          key={data.date}
          day={day}
          onDay={setDay}
          replayAt={replayAt}
          data={data}
          phone={phone}
          live={!day && inTradingHours()}
          onReplay={setReplayAt}
        />
      ) : null}
    </div>
  );
}

function Body({
  day,
  onDay,
  replayAt,
  data,
  phone,
  live,
  onReplay,
}: {
  day: string | null;
  onDay: (day: string | null) => void;
  replayAt: string | null;
  data: SectorFlowData;
  phone: boolean;
  live: boolean;
  onReplay: (time: string | null) => void;
}) {
  const [mode, setMode] = useState<Mode>("net");
  const [picked, setPicked] = useState<string[] | null>(null);
  const [focus, setFocus] = useState<string | null>(null);
  const [cursor, setCursor] = useState<number | null>(null); // null = follow live
  const [allBars, setAllBars] = useState(false);
  const last = data.t.length - 1;
  const upto = cursor == null ? last : Math.min(cursor, last);
  const asOf = useSectorLeadersAt(day ?? data.date ?? null, cursor == null ? null : upto);
  const leaders = cursor == null ? data.leaders : (asOf.data ?? data.leaders);

  const byName = useMemo(() => new Map(data.sectors.map((s) => [s.name, s])), [data.sectors]);
  const sel = picked ?? defaultSelection(data.sectors, DEFAULT_TOP_N);
  const shown = sel.map((n) => byName.get(n)).filter((s): s is Sector => !!s);
  const colors = useMemo(() => colorMap(shown), [shown]);
  const toggle = (name: string) => {
    const cur = picked ?? sel;
    setPicked(cur.includes(name) ? cur.filter((n) => n !== name) : [...cur, name]);
  };
  const clock = data.t[upto] ?? data.time ?? "";
  const replay = cursor != null;
  useEffect(() => {
    onReplay(replay ? clock : null);
  }, [replay, clock, onReplay]);
  const at = (s: Sector) => s.series[upto] ?? s.series[s.series.length - 1] ?? 0;
  const ranked = [...data.sectors].sort((a, b) => at(b) - at(a));
  const sm = data.summary;

  return (
    <>
      {sm ? (
        <div className="sf-stats">
          <TurnoverCard sm={sm} />
          <div className="sf-stat">
            <span className="k">上涨 / 下跌</span>
            <span className="v">
              <span className="up">{sm.up}</span>
              <span className="sl"> / </span>
              <span className="down">{sm.down}</span>
            </span>
            <span className="sf-ud">
              <i className="up-bg" style={{ flex: sm.up }} />
              <i className="down-bg" style={{ flex: sm.down }} />
            </span>
          </div>
          <div className="sf-stat">
            <span className="k">涨停 / 跌停</span>
            <span className="v">
              <span className="up">{sm.limit_up}</span>
              <span className="sl"> / </span>
              <span className="down">{sm.limit_down}</span>
            </span>
          </div>
        </div>
      ) : null}
      <div className="sf-grid">
        <div className="sf-main">
          <section className="sf-card">
            <div className="sf-ch">
              <h3>板块累计{mode === "pct" ? "涨幅" : "净流入"}</h3>
              <div className="sf-meta">
                <span className="sf-sub">09:25 起，估算</span>
                {replay ? <span className="sf-pill replay">回放 {clock}</span> : null}
                <span className={`sf-live ${live && !replay ? "on" : ""}`}>
                  {live && !replay ? <i /> : null}
                  {replay ? clock : (data.time ?? "")}
                </span>
              </div>
              <div className="tools">
                {data.days?.length ? (
                  <select
                    className="select sf-day"
                    aria-label="数据时间"
                    value={day ?? ""}
                    onChange={(e) => onDay(e.target.value || null)}
                  >
                    <option value="">{replayAt ? `今天 · 回放 ${replayAt}` : "今天 / 最新"}</option>
                    {data.days.slice(1).map((d) => (
                      <option key={d} value={d}>
                        {d}
                      </option>
                    ))}
                  </select>
                ) : null}
                <Segmented label="口径" value={mode} onChange={setMode} options={MODES} />
              </div>
            </div>
            <div className="sf-chips">
              {shown.map((s) => (
                <span key={s.name} className={`sf-chip ${focus === s.name ? "hl" : ""}`}>
                  <button
                    type="button"
                    className="lab"
                    onClick={() => setFocus(focus === s.name ? null : s.name)}
                  >
                    <i style={{ background: colors.get(s.name) }} />
                    {s.name}
                  </button>
                  <button
                    type="button"
                    className="x"
                    aria-label={`移除 ${s.name}`}
                    onClick={() => toggle(s.name)}
                  >
                    ×
                  </button>
                </span>
              ))}
              {picked ? (
                <button type="button" className="sf-chip more" onClick={() => setPicked(null)}>
                  恢复默认
                </button>
              ) : null}
            </div>
            <FlowChart
              t={data.t}
              shown={shown}
              colors={colors}
              mode={mode}
              upto={upto}
              phone={phone}
              focus={focus}
              onFocus={setFocus}
            />
            <div className="sf-scrub">
              <span>回放</span>
              <input
                type="range"
                min={0}
                max={last}
                value={upto}
                aria-label="回放时间"
                onChange={(e) => {
                  const v = Number(e.target.value);
                  setCursor(v >= last ? null : v);
                }}
              />
              <span className="sf-scrub-t">
                {data.t[upto]}
                {cursor == null ? (live ? " · 实时" : "") : " · 回放中"}
              </span>
              {cursor != null ? (
                <button type="button" className="sf-link" onClick={() => setCursor(null)}>
                  回到最新
                </button>
              ) : null}
            </div>
            <p className="sf-note sf-note-row">
              估算值，看相对强弱，和同花顺、东方财富的数会不一样
              <HelpMark label="估算口径">
                每只股票 3 秒快照间的成交额增量按“价 ≥ 前卖一记主动买、≤ 前买一记主动卖，否则按涨跌
                tick”归类；09:25 竞价按竞价价对昨收定方向。
              </HelpMark>
            </p>
          </section>
          {!phone ? (
            <Bars
              ranked={ranked}
              at={at}
              sel={sel}
              colors={colors}
              toggle={toggle}
              upto={upto}
              t={data.t}
              all
            />
          ) : null}
        </div>
        <Leaders
          leaders={leaders}
          asOf={cursor == null ? null : (data.t[upto] ?? null)}
          ranked={ranked}
          at={at}
          phone={phone}
        />
        {phone ? (
          <Bars
            ranked={ranked}
            at={at}
            sel={sel}
            colors={colors}
            toggle={toggle}
            upto={upto}
            t={data.t}
            all={allBars}
            onAll={() => setAllBars(true)}
          />
        ) : null}
      </div>
      <p className="sf-foot">
        {data.date} · 数据 {replay ? clock : data.time}
        {mode !== "net" ? ` · 当前口径：${MODES.find((o) => o.value === mode)?.label}` : ""}
        {" · "}
        {data.t.length} 个时点
      </p>
    </>
  );
}

/** 两市成交额 (沪深京全 A, 含北交所) + change vs the previous open day; replaces the old
 *  "全市场估算净流入" card (the summed tick-rule estimate is not a market net inflow). */
function TurnoverCard({ sm }: { sm: NonNullable<SectorFlowData["summary"]> }) {
  const yi = sm.turnover ?? sm.amount / 1e8;
  const chg = fmtTurnoverChg(sm.turnover_chg, sm.turnover_chg_pct);
  const snap = sm.turnover_source === "snapshot_last";
  const src = snap ? "快照末笔累计（日线尚未入库）" : "日线合计";
  const prev =
    sm.prev_turnover != null
      ? `；上一交易日 ${sm.prev_date ?? ""} ${sm.prev_turnover.toFixed(1)}亿`
      : "";
  return (
    <div
      className="sf-stat sf-turn"
      title={`沪深京全 A 成交额（含北交所）${yi.toFixed(1)}亿，来源：${src}${prev}`}
    >
      <span className="k">两市成交额{snap ? " · 快照" : ""}</span>
      <span className="v">{fmtAmountYi(yi * 1e8)}</span>
      {chg ? <span className="sf-chg">{chg}</span> : null}
    </div>
  );
}

function Bars({
  ranked,
  at,
  sel,
  colors,
  toggle,
  upto,
  t,
  all,
  lim: limit = 8,
  onAll,
}: {
  ranked: Sector[];
  at: (s: Sector) => number;
  sel: string[];
  colors: Map<string, string>;
  toggle: (n: string) => void;
  upto: number;
  t: string[];
  all: boolean;
  lim?: number;
  onAll?: () => void;
}) {
  const ins = ranked.filter((s) => at(s) > 0);
  const outs = ranked.filter((s) => at(s) <= 0).reverse();
  const lim = all ? 999 : limit;
  const max = Math.max(1e-9, ...ranked.map((s) => Math.abs(at(s))));
  const row = (s: Sector) => {
    const on = sel.includes(s.name);
    const v = at(s);
    return (
      <label key={s.name} className="sf-bar">
        <input type="checkbox" checked={on} onChange={() => toggle(s.name)} />
        <span
          className="cb"
          style={
            on ? { background: colors.get(s.name), borderColor: colors.get(s.name) } : undefined
          }
        />
        <span className="n">{s.name}</span>
        <span className="bt-wrap">
          <span
            className={`bt ${v >= 0 ? "up-bg" : "down-bg"}`}
            style={{ width: `${(Math.abs(v) / max) * 100}%`, opacity: on ? 1 : 0.5 }}
          />
        </span>
        <span className={`bv ${tone(v)}`}>{fmtYi(v)}</span>
      </label>
    );
  };
  return (
    <section className="sf-card">
      <div className="sf-ch">
        <h3>全部板块 · 净流入</h3>
        <span className="sf-hint">勾选即加入上图</span>
        <span className="sf-sp" />
        <span className="sf-sub">
          {t[upto]} · 流入 {ins.length} / 流出 {outs.length}
        </span>
      </div>
      <div className="sf-bars">
        <div>
          <div className="sf-sub">流入</div>
          {ins.slice(0, lim).map(row)}
        </div>
        <div>
          <div className="sf-sub">流出</div>
          {outs.slice(0, lim).map(row)}
        </div>
      </div>
      {!all && onAll && (ins.length > lim || outs.length > lim) ? (
        <button type="button" className="sf-more" onClick={onAll}>
          展开全部 {ranked.length} 个板块
        </button>
      ) : null}
    </section>
  );
}

function Spark({ s, w = 64 }: { s: Stock; w?: number }) {
  const h = 20;
  const pts = s.spark
    .map((v, i) => [i, v] as const)
    .filter((p): p is readonly [number, number] => p[1] != null);
  if (pts.length < 2 || !s.pre_close) return <svg width={w} height={h} />;
  const pre = s.pre_close;
  const ys = [pre, ...pts.map((p) => p[1])];
  const lo = Math.min(...ys);
  const hi = Math.max(...ys);
  const n = s.spark.length - 1 || 1;
  const sx = (i: number) => 1 + (i / n) * (w - 2);
  const sy = (v: number) => h - 2 - ((v - lo) / (hi - lo || 1)) * (h - 4);
  const d = pts
    .map((p, i) => `${i ? "L" : "M"}${sx(p[0]).toFixed(1)} ${sy(p[1]).toFixed(1)}`)
    .join("");
  const up = (s.pct ?? 0) >= 0;
  return (
    <svg width={w} height={h} className="sf-spark" aria-hidden="true">
      <line x1={0} x2={w} y1={sy(pre)} y2={sy(pre)} className="sf-spark-base" />
      <path d={d} fill="none" className={up ? "up-stroke" : "down-stroke"} strokeWidth={1.5} />
    </svg>
  );
}

function Leaders({
  leaders,
  asOf,
  ranked,
  at,
  phone,
}: {
  leaders: Leader[];
  asOf: string | null;
  ranked: Sector[];
  at: (sector: Sector) => number;
  phone: boolean;
}) {
  const netOf = (name: string, fallback: number) => {
    const sector = ranked.find((item) => item.name === name);
    return sector ? at(sector) : fallback;
  };
  const shown = [...leaders]
    .map((leader) => ({ ...leader, net: netOf(leader.name, leader.net) }))
    .sort((a, b) => b.net - a.net)
    .slice(0, 3);
  const outs = [...ranked]
    .filter((sector) => at(sector) < 0)
    .sort((a, b) => at(a) - at(b))
    .slice(0, 3);
  return (
    <aside className="sf-card sf-rank">
      <div className="sf-ch">
        <h3>流入前三 · 资金领先股</h3>
        <span className={asOf ? "sf-pill replay" : "sf-sub"}>
          {asOf ? `回放 ${asOf}` : "净流入 / 流通市值"}
        </span>
      </div>
      {shown.length === 0 ? <p className="sf-sub">暂无净流入板块。</p> : null}
      {shown.map((l, k) => (
        <div key={l.name} className="sf-sec">
          <div className="sf-sh">
            <span className="rank">{k + 1}</span>
            <span className="sname">
              {l.name}
              <em>({fmtSectorNet(l.net)})</em>
            </span>
            <span className="smeta">
              <span className={tone(l.pct)}>{fmtPct(l.pct)}</span>
              {l.limit_up ? <span className="tag">涨停 {l.limit_up}</span> : null}
            </span>
          </div>
          {l.stocks.length === 0 ? (
            <p className="sf-sub">没有满足流通市值 ≥30亿、成交 ≥1亿 的成分股。</p>
          ) : null}
          {l.stocks.map((s) => (
            <div key={s.code} className="sf-stk">
              <div className="info">
                <div className="n">
                  {s.name ?? s.code}
                  <span className="c">
                    <StockLink code={s.code} />
                  </span>
                  {s.sealed ? <span className="tag">涨停</span> : null}
                </div>
                <div className="r">
                  占流通 <b>{fmtRatioPct(s.net_ratio == null ? null : s.net_ratio * 100)}</b> ·
                  净流入 {fmtYi(s.net / 1e8)}
                </div>
              </div>
              <div className={`p ${tone(s.pct)}`}>{fmtPct(s.pct)}</div>
              {phone ? null : <Spark s={s} />}
            </div>
          ))}
        </div>
      ))}
      <div className="sf-out">
        <div className="sf-sub">流出前三</div>
        {outs.map((s) => (
          <div key={s.name} className="row">
            <span>
              {s.name} <span className={`small ${tone(s.pct)}`}>{fmtPct(s.pct)}</span>
            </span>
            <b className="down">{fmtValue(at(s), "net")}</b>
          </div>
        ))}
      </div>
      <p className="sf-note">
        资金领先股门槛：流通市值 ≥30 亿且成交额 ≥1 亿；按自 09:25 净流入 ÷
        流通市值排序；回放时按该分钟重新计算。
      </p>
    </aside>
  );
}
