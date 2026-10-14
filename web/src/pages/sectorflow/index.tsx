import { useMemo, useState } from "react";
import {
  inTradingHours,
  type SectorFlowData,
  useSectorFlow,
  useSectorLeadersAt,
} from "@/api/endpoints";
import { PageHeader } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { PHONE_QUERY, useMedia } from "../panorama/useMedia";
import { FlowChart } from "./FlowChart";
import {
  colorMap,
  DEFAULT_TOP_N,
  defaultSelection,
  fmtAmountYi,
  fmtPct,
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
  const q = useSectorFlow(day);
  const data = q.data;
  return (
    <div className={`sf ${phone ? "sf-phone" : ""}`}>
      <PageHeader
        eyebrow="跟踪与告警"
        title="板块资金流"
        note="行业（Tushare 分类，与市场全景一致）· 自 09:25 累计主动买卖估算"
        actions={
          data?.days?.length ? (
            <select
              className="sf-day"
              aria-label="交易日"
              value={day ?? ""}
              onChange={(e) => setDay(e.target.value || null)}
            >
              <option value="">今天 / 最新</option>
              {data.days.slice(1).map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          ) : undefined
        }
      />
      {q.error ? <p className="hint pad">加载失败：{String(q.error.message ?? q.error)}</p> : null}
      {!data && !q.error ? <p className="hint pad">加载中…</p> : null}
      {data?.warming ? <p className="hint pad">正在汇总当日快照（首次约 10–60 秒）…</p> : null}
      {data && !data.warming && !data.sectors.length ? (
        <p className="hint pad">今天还没有快照（09:25 集合竞价后出现）。</p>
      ) : null}
      {data && !data.warming && data.sectors.length ? (
        <Body key={data.date} day={day} data={data} phone={phone} live={!day && inTradingHours()} />
      ) : null}
    </div>
  );
}

function Body({
  day,
  data,
  phone,
  live,
}: {
  day: string | null;
  data: SectorFlowData;
  phone: boolean;
  live: boolean;
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
  const at = (s: Sector) => s.series[upto] ?? s.series[s.series.length - 1] ?? 0;
  const ranked = [...data.sectors].sort((a, b) => at(b) - at(a));
  const sm = data.summary;

  return (
    <>
      {sm ? (
        <div className="sf-stats">
          <div className="sf-stat">
            <span className="k">两市成交额</span>
            <span className="v">{fmtAmountYi(sm.amount)}</span>
          </div>
          <div className="sf-stat">
            <span className="k">全市场估算净流入</span>
            <span className={`v ${tone(sm.net)}`}>{fmtYi(sm.net / 1e8)}</span>
          </div>
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
              <span className="sf-pill">09:25 起 · 估算</span>
              <span className={`sf-live ${live ? "on" : ""}`}>
                {live ? <i /> : null}
                {data.time ?? ""}
              </span>
              <span className="sf-sp" />
              <div className="sf-seg" role="tablist" aria-label="口径">
                {MODES.map((o) => (
                  <button
                    key={o.value}
                    type="button"
                    role="tab"
                    aria-selected={mode === o.value}
                    className={mode === o.value ? "on" : ""}
                    onClick={() => setMode(o.value)}
                  >
                    {o.label}
                  </button>
                ))}
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
            <p className="sf-note">
              估算，看相对强弱：每只股票 3 秒快照间的成交额增量按“价 ≥ 前卖一记主动买、≤
              前买一记主动卖，否则按涨跌 tick”归类；09:25
              竞价按竞价价对昨收定方向。口径与同花顺/东财不同。
              {live ? " 每 3 秒拉取，快照约 30 秒落盘一次。" : ""}
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
          ranked={data.sectors}
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
        {data.date} · 数据 {data.time}
        {mode !== "net" ? ` · 当前口径：${MODES.find((o) => o.value === mode)?.label}` : ""}
        {" · "}
        {data.t.length} 个时点
      </p>
    </>
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
        <span className="sf-pill">勾选即加入上图</span>
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

function Spark({ s, w = 88 }: { s: Stock; w?: number }) {
  const h = 30;
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
  const first = pts[0] ?? [0, pre];
  const lastP = pts[pts.length - 1] ?? first;
  return (
    <svg width={w} height={h} className="sf-spark" aria-hidden="true">
      <line x1={0} x2={w} y1={sy(pre)} y2={sy(pre)} className="sf-spark-base" />
      <path
        d={`${d}L${sx(lastP[0])} ${h}L${sx(first[0])} ${h}Z`}
        className={up ? "up-fill" : "down-fill"}
      />
      <path d={d} fill="none" className={up ? "up-stroke" : "down-stroke"} strokeWidth={1.4} />
    </svg>
  );
}

function Leaders({
  leaders,
  asOf,
  ranked,
  phone,
}: {
  leaders: Leader[];
  asOf: string | null;
  ranked: Sector[];
  phone: boolean;
}) {
  const outs = ranked
    .filter((s) => s.net < 0)
    .slice(-3)
    .reverse();
  return (
    <aside className="sf-card sf-rank">
      <div className="sf-ch">
        <h3>流入前三 · 资金领先股</h3>
        <span className={`sf-pill ${asOf ? "replay" : ""}`}>
          {asOf ? `回放 ${asOf}` : "净流入/流通市值"}
        </span>
      </div>
      {leaders.length === 0 ? <p className="sf-sub">暂无净流入板块。</p> : null}
      {leaders.map((l, k) => (
        <div key={l.name} className="sf-sec">
          <div className="sf-sh">
            <span className="rank">{k + 1}</span>
            <span className="sname">
              {l.name}
              <em>({fmtYi(l.net / 1e8)})</em>
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
                  占流通 <b>{s.net_ratio == null ? "—" : `${(s.net_ratio * 100).toFixed(2)}%`}</b> ·
                  净流入 {fmtYi(s.net / 1e8)}
                </div>
              </div>
              <div className={`p ${tone(s.pct)}`}>{fmtPct(s.pct)}</div>
              <Spark s={s} w={phone ? 76 : 88} />
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
            <b className="down">{fmtValue(s.net / 1e8, "net")}</b>
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
