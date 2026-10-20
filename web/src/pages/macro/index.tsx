import { useMemo, useState } from "react";
import { type MacroData, useMacro } from "@/api/endpoints";
import { type LineSeries, TimeChart } from "@/charts/Chart";
import { DataTable } from "@/table/DataTable";
import { EmptyState, KpiStrip, PageHeader, Panel, Segmented } from "@/ui";
import { QueryView } from "../shared";
import { MacroOverlay } from "./Overlay";

type Cols = NonNullable<MacroData["daily"]>;
type Range = "1y" | "3y" | "5y" | "all";

const RANGES = [
  { value: "1y", label: "1年" },
  { value: "3y", label: "3年" },
  { value: "5y", label: "5年" },
  { value: "all", label: "全部" },
] as const;

/** 2024-09-24 政策反转、2026-05 回落起点：图上浅色标出。 */
const EVENTS: readonly [string, string][] = [
  ["20240920", "20241010"],
  ["20260501", "20260531"],
];

const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
const yi = (v: number) => `${(v / 1e8).toFixed(0)}亿`;

function cut(c: Cols, range: Range): Cols {
  if (range === "all" || c.dates.length === 0) return c;
  const years = { "1y": 1, "3y": 3, "5y": 5 }[range];
  const last = c.dates[c.dates.length - 1] ?? "";
  const from = `${Number(last.slice(0, 4)) - years}${last.slice(4)}`;
  const i = Math.max(
    0,
    c.dates.findIndex((d) => d >= from),
  );
  const cols: Cols["cols"] = {};
  for (const [k, v] of Object.entries(c.cols)) cols[k] = v.slice(i);
  return { dates: c.dates.slice(i), cols };
}

function eventBands(dates: readonly string[]) {
  return EVENTS.flatMap(([a, b]) => {
    const from = dates.findIndex((d) => d >= a);
    let to = dates.findIndex((d) => d > b);
    if (to < 0) to = dates.length - 1;
    return from >= 0 && from <= to ? [{ from, to: Math.max(from, to) }] : [];
  });
}

function col(c: Cols, k: string): (number | null)[] {
  return c.cols[k] ?? [];
}

function flat(n: number, v: number, name: string, color: string): LineSeries {
  return { name, values: Array(n).fill(v), color, dashed: true };
}

function IndexChart({ c, k = "csi300", name = "沪深300" }: { c: Cols; k?: string; name?: string }) {
  return (
    <TimeChart
      label={name}
      dates={c.dates}
      height={96}
      bands={eventBands(c.dates)}
      format={(v) => v.toFixed(0)}
      series={[{ name, values: col(c, k), color: "var(--muted)" }]}
    />
  );
}

function Holders({ data }: { data: MacroData }) {
  const [entity, setEntity] = useState("中央汇金");
  const [basis, setBasis] = useState<"report" | "announce">("announce");
  const rows = data.holders.filter((h) => h.entity === entity && h.basis === basis);
  const dates = rows.map((r) => r.quarter);
  // quarter-end CSI300 for alignment
  const im = data.index_m;
  const qIdx = rows.map((r) => {
    if (!im) return null;
    const y = r.quarter.slice(0, 4);
    const m = String(Number(r.quarter.slice(5)) * 3).padStart(2, "0");
    const i = im.dates.indexOf(`${y}${m}`) >= 0 ? im.dates.indexOf(`${y}${m}`) : -1;
    const j = i >= 0 ? i : im.dates.findIndex((d) => d.slice(0, 6) === `${y}${m}`);
    return j >= 0 ? (im.cols.csi300?.[j] ?? null) : null;
  });
  const evq = ["2024Q3", "2024Q4", "2026Q2"];
  const bands = evq
    .map((q) => dates.indexOf(q))
    .filter((i) => i >= 0)
    .map((i) => ({ from: i, to: i }));
  return (
    <Panel
      title="社保基金 / 中央汇金 持仓变动"
      sub="来源：前十大流通股东。按公告季度归集=信息真正可见的时间；按报告期=实际调仓时间。流入=Σ股数变动×期末价"
      actions={
        <>
          <Segmented
            label="机构"
            value={entity}
            onChange={setEntity}
            options={[
              { value: "中央汇金", label: "中央汇金" },
              { value: "社保基金", label: "社保基金" },
            ]}
          />
          <Segmented
            label="口径"
            value={basis}
            onChange={setBasis}
            options={[
              { value: "announce", label: "按公告季" },
              { value: "report", label: "按报告期" },
            ]}
          />
        </>
      }
    >
      <h3 className="sub">季末沪深300</h3>
      <TimeChart
        label="季末沪深300"
        dates={dates}
        height={90}
        bands={bands}
        format={(v) => v.toFixed(0)}
        series={[{ name: "沪深300", values: qIdx, color: "var(--muted)" }]}
      />
      <h3 className="sub">净增持金额（亿元）</h3>
      <TimeChart
        label="净增持金额"
        dates={dates}
        bars
        baseline={0}
        height={120}
        bands={bands}
        format={(v) => `${v.toFixed(0)}亿`}
        series={[
          { name: "净增持", values: rows.map((r) => (r.flow ?? 0) / 1e8), color: "var(--accent)" },
        ]}
      />
      <h3 className="sub">家数：新进 / 增持 / 减持 / 退出</h3>
      <TimeChart
        label="家数"
        dates={dates}
        height={120}
        bands={bands}
        format={(v) => v.toFixed(0)}
        series={[
          { name: "新进", values: rows.map((r) => r.n_new), color: "var(--up)" },
          { name: "增持", values: rows.map((r) => r.n_add), color: "var(--accent)" },
          { name: "减持", values: rows.map((r) => r.n_cut), color: "var(--down)" },
          { name: "退出", values: rows.map((r) => r.n_exit), color: "var(--warn)", dashed: true },
          {
            name: "持仓家数",
            values: rows.map((r) => r.n_hold),
            color: "var(--muted)",
            dashed: true,
          },
        ]}
      />
      <DataTable
        label="季度明细"
        rows={[...rows].reverse()}
        rowKey={(r) => r.quarter}
        columns={[
          { id: "q", header: "季度", value: (r) => r.quarter },
          { id: "n", header: "持仓家数", value: (r) => r.n_hold },
          { id: "new", header: "新进", value: (r) => r.n_new ?? "—" },
          { id: "exit", header: "退出", value: (r) => r.n_exit ?? "—" },
          { id: "add", header: "增持", value: (r) => r.n_add ?? "—" },
          { id: "cut", header: "减持", value: (r) => r.n_cut ?? "—" },
          {
            id: "flow",
            header: "净增持(亿)",
            value: (r) => (r.flow == null ? "—" : (r.flow / 1e8).toFixed(1)),
          },
          {
            id: "mv",
            header: "持仓市值(亿)",
            value: (r) => (r.mv == null ? "—" : (r.mv / 1e8).toFixed(0)),
          },
        ]}
      />
    </Panel>
  );
}

function Banded({
  title,
  sub,
  c,
  k,
  name,
  bands,
  format,
}: {
  title: string;
  sub: string;
  c: Cols;
  k: string;
  name: string;
  bands: MacroData["buffett_bands"];
  format: (v: number) => string;
}) {
  const n = c.dates.length;
  const series: LineSeries[] = [{ name, values: col(c, k), color: "var(--accent)" }];
  if (bands) {
    series.push(
      flat(n, bands.p80, "80%分位", "var(--up)"),
      flat(n, bands.mean, "均值", "var(--muted)"),
      flat(n, bands.p20, "20%分位", "var(--down)"),
    );
  }
  return (
    <Panel title={title} sub={sub}>
      <IndexChart
        c={c}
        k={c.cols.all_a ? "all_a" : "csi300"}
        name={c.cols.all_a ? "中证全指" : "沪深300"}
      />
      <TimeChart
        label={name}
        dates={c.dates}
        height={130}
        bands={eventBands(c.dates)}
        format={format}
        series={series}
      />
    </Panel>
  );
}

function Options({ data }: { data: MacroData }) {
  const names = Object.keys(data.options);
  const [und, setUnd] = useState(names.includes("沪300ETF") ? "沪300ETF" : (names[0] ?? ""));
  const c = data.options[und];
  return (
    <Panel
      title="ETF 期权情绪（日频）"
      sub="PCR=认沽/认购；放量=当日成交量÷前20日均量；ATM IV=近月（≥7天）平值认购认沽 IV 均值"
      actions={
        <Segmented
          label="标的"
          value={und}
          onChange={setUnd}
          options={names.map((n) => ({ value: n, label: n }))}
        />
      }
    >
      <DataTable
        label="最新"
        rows={data.options_latest}
        rowKey={(r) => r.und}
        columns={[
          { id: "u", header: "标的", value: (r) => r.und },
          { id: "d", header: "日期", value: (r) => r.trade_date },
          { id: "pv", header: "PCR(量)", value: (r) => r.pcr_vol?.toFixed(2) ?? "—" },
          { id: "po", header: "PCR(持仓)", value: (r) => r.pcr_oi?.toFixed(2) ?? "—" },
          { id: "vs", header: "放量倍数", value: (r) => r.vol_surge?.toFixed(2) ?? "—" },
          { id: "os", header: "持仓变化倍数", value: (r) => r.oi_surge?.toFixed(2) ?? "—" },
          { id: "iv", header: "ATM IV", value: (r) => (r.atm_iv == null ? "—" : pct(r.atm_iv)) },
          {
            id: "ivp",
            header: "IV分位",
            value: (r) => (r.atm_iv_pct == null ? "—" : pct(r.atm_iv_pct)),
          },
        ]}
      />
      {c ? (
        <>
          <IndexChart c={c} k="spot" name={`${und} 价格`} />
          <TimeChart
            label="PCR"
            dates={c.dates}
            height={110}
            baseline={1}
            bands={eventBands(c.dates)}
            format={(v) => v.toFixed(2)}
            series={[
              { name: "PCR(量)", values: col(c, "pcr_vol"), color: "var(--accent)" },
              { name: "上轨", values: col(c, "pcr_vol_up"), color: "var(--up)", dashed: true },
              { name: "下轨", values: col(c, "pcr_vol_lo"), color: "var(--down)", dashed: true },
            ]}
          />
          <TimeChart
            label="PCR(持仓) 布林"
            dates={c.dates}
            height={96}
            bands={eventBands(c.dates)}
            format={(v) => v.toFixed(2)}
            series={[
              { name: "PCR(持仓)", values: col(c, "pcr_oi"), color: "var(--warn)" },
              { name: "上轨", values: col(c, "pcr_oi_up"), color: "var(--up)", dashed: true },
              { name: "下轨", values: col(c, "pcr_oi_lo"), color: "var(--down)", dashed: true },
            ]}
          />
          <TimeChart
            label="ATM IV"
            dates={c.dates}
            height={96}
            bands={eventBands(c.dates)}
            format={pct}
            series={[{ name: "ATM IV", values: col(c, "atm_iv"), color: "var(--up)" }]}
          />
          <TimeChart
            label="放量倍数"
            dates={c.dates}
            height={90}
            bars
            baseline={1}
            format={(v) => v.toFixed(1)}
            series={[
              {
                name: "成交放量",
                values: col(c, "vol_surge"),
                color: "var(--accent)",
                neutral: true,
              },
            ]}
          />
        </>
      ) : null}
    </Panel>
  );
}

function MacroBody({ data }: { data: MacroData }) {
  const [range, setRange] = useState<Range>("5y");
  const daily = useMemo(() => (data.daily ? cut(data.daily, range) : null), [data.daily, range]);
  const buffett = useMemo(() => {
    if (!data.buffett || !data.index_m) return null;
    const m = new Map(data.index_m.dates.map((d, i) => [d.slice(0, 6), i]));
    const idx = (k: string) =>
      data.buffett?.dates.map((d) => {
        const i = m.get(d.slice(0, 6));
        return i === undefined ? null : (data.index_m?.cols[k]?.[i] ?? null);
      }) ?? [];
    return {
      dates: data.buffett.dates,
      cols: { ...data.buffett.cols, csi300: idx("csi300"), all_a: idx("all_a") },
    };
  }, [data.buffett, data.index_m]);
  const bb = data.buffett_bands;
  const eb = data.erp_bands;
  const last = (k: string) => {
    const v = data.daily?.cols[k] ?? [];
    for (let i = v.length - 1; i >= 0; i--) if (v[i] != null) return v[i] as number;
    return null;
  };
  const etf = data.etf_share;
  const etfAll = etf
    ? etf.dates.map((_, i) => Object.values(etf.cols).reduce((s, v) => s + (v[i] ?? 0), 0) / 1e4)
    : [];
  const etf300 = etf
    ? etf.dates.map(
        (_, i) =>
          ["510300.SH", "159919.SZ", "510310.SH", "510330.SH"].reduce(
            (s, k) => s + (etf.cols[k]?.[i] ?? 0),
            0,
          ) / 1e4,
      )
    : [];
  return (
    <>
      <MacroOverlay data={data} />
      <KpiStrip
        label="当前读数"
        items={[
          {
            key: "buffett",
            label: "巴菲特指标",
            value: bb ? pct(bb.current) : "—",
            sub: bb ? `历史分位 ${pct(bb.current_pct)} · ${bb.current_date}` : undefined,
            tone: bb
              ? bb.current > bb.p80
                ? "crit"
                : bb.current < bb.p20
                  ? "ok"
                  : undefined
              : undefined,
            tip: "A股总市值 ÷ 最近已公布的滚动4季名义GDP；分位为全样本，仅描述性",
          },
          {
            key: "erp",
            label: "股债利差",
            value: eb ? `${eb.current.toFixed(2)}%` : "—",
            sub: eb ? `历史分位 ${pct(eb.current_pct)}` : undefined,
            tip: "沪深300 1/PE(TTM) − 10年国债收益率；越高股票相对越便宜",
          },
          {
            key: "rz",
            label: "两融余额",
            value: last("rzrqye") == null ? "—" : yi(last("rzrqye") as number),
          },
          {
            key: "fin",
            label: "融资买入占成交",
            value: last("fin_buy_share") == null ? "—" : pct(last("fin_buy_share") as number),
          },
        ]}
      />
      <div className="card-grid">
        {buffett ? (
          <Banded
            title="巴菲特指标（A股总市值 / GDP）"
            sub="总市值=daily_basic 全A total_mv 月末合计；GDP=cn_gdp 滚动4季，按季末+20天视为已公布。虚线=全样本20/80分位与均值"
            c={buffett}
            k="ratio"
            name="总市值/GDP"
            bands={bb}
            format={pct}
          />
        ) : null}
        <div className="span-all">
          <Segmented label="区间" value={range} onChange={setRange} options={RANGES} />
        </div>
        {daily ? (
          <>
            <Banded
              title="股债利差（沪深300）"
              sub="1/PE_TTM − 中国10年国债收益率。虚线=全样本20/80分位"
              c={daily}
              k="erp"
              name="股债利差%"
              bands={eb}
              format={(v) => `${v.toFixed(1)}%`}
            />
            <Panel title="两融" sub="沪深两市合计；融资买入占比=融资买入额 ÷ 中证全指成交额">
              <IndexChart c={daily} k="all_a" name="中证全指" />
              <TimeChart
                label="两融余额"
                dates={daily.dates}
                height={104}
                bands={eventBands(daily.dates)}
                format={yi}
                series={[
                  { name: "两融余额", values: col(daily, "rzrqye"), color: "var(--accent)" },
                ]}
              />
              <TimeChart
                label="融资买入占成交"
                dates={daily.dates}
                height={90}
                bands={eventBands(daily.dates)}
                format={pct}
                series={[
                  {
                    name: "融资买入占比",
                    values: col(daily, "fin_buy_share"),
                    color: "var(--warn)",
                  },
                ]}
              />
            </Panel>
            <Panel
              title="北向资金"
              sub={`${data.north_regime_change} 起交易所停止披露北向每日净买入，此后 Tushare north_money 不再是净流入，改为展示北向成交额，两段不可比`}
            >
              <IndexChart c={daily} />
              <TimeChart
                label="北向累计净买入"
                dates={daily.dates}
                height={104}
                bands={eventBands(daily.dates)}
                format={(v) => `${(v / 100).toFixed(0)}亿`}
                series={[
                  {
                    name: "累计净买入",
                    values: col(daily, "north_net_cum"),
                    color: "var(--accent)",
                  },
                ]}
              />
              <TimeChart
                label="北向成交额（新口径）"
                dates={daily.dates}
                height={90}
                bands={eventBands(daily.dates)}
                format={(v) => `${(v / 100).toFixed(0)}亿`}
                series={[
                  {
                    name: "北向成交额",
                    values: col(daily, "north_turnover"),
                    color: "var(--warn)",
                  },
                ]}
              />
            </Panel>
          </>
        ) : null}
        <Holders data={data} />
        {etf && etf.dates.length > 1 ? (
          <Panel
            title="宽基 ETF 份额与申赎"
            sub="510300/159919/510310/510330 份额合计（亿份）。汇金托底常以买入宽基ETF进行，份额陡增可作日频线索"
          >
            <TimeChart
              label="300ETF份额"
              dates={etf.dates}
              height={120}
              bands={eventBands(etf.dates)}
              format={(v) => `${v.toFixed(0)}亿份`}
              series={[{ name: "300ETF合计份额", values: etf300, color: "var(--accent)" }]}
            />
            <TimeChart
              label="宽基ETF净申赎"
              dates={etf.dates}
              height={104}
              bars
              baseline={0}
              bands={eventBands(etf.dates)}
              format={(v) => `${v.toFixed(0)}亿份`}
              series={[
                {
                  name: "宽基ETF份额日变动",
                  values: etfAll.map((v, i) =>
                    i && v != null && etfAll[i - 1] != null ? v - (etfAll[i - 1] as number) : null,
                  ),
                  color: "var(--accent)",
                },
              ]}
            />
          </Panel>
        ) : null}
        <Options data={data} />
        <Futures data={data} />
        <Breadth data={data} />
        <HkSystem data={data} />
        <div className="span-all">
          <Studies data={data} />
        </div>
      </div>
    </>
  );
}

export default function MacroPage() {
  const query = useMacro();
  return (
    <>
      <PageHeader
        eyebrow="宏观"
        title="宏观与资金面"
        note="低频指标：盘后更新；阴影=2024-09-24 反转 / 2026-05 回落"
      />
      <QueryView query={query}>
        {(data) =>
          data.available ? (
            <MacroBody data={data} />
          ) : (
            <EmptyState
              title="宏观数据尚未生成"
              hint="刷新后会显示最新数据。"
            />
          )
        }
      </QueryView>
    </>
  );
}

function Futures({ data }: { data: MacroData }) {
  const syms = Object.keys(data.futures);
  const [sym, setSym] = useState(syms.includes("IF") ? "IF" : (syms[0] ?? ""));
  const c = data.futures[sym];
  if (!c) return null;
  return (
    <Panel
      title="股指期货：基差与前20会员净持仓"
      sub="基差=主力合约收盘/现货指数−1（负=贴水）；净持仓=中金所前20会员多单−空单，汇总所有月份"
      actions={
        <Segmented
          label="品种"
          value={sym}
          onChange={setSym}
          options={syms.map((s) => ({ value: s, label: s }))}
        />
      }
    >
      <IndexChart c={c} k="spot" name="现货指数" />
      <TimeChart
        label="基差"
        dates={c.dates}
        height={104}
        baseline={0}
        bands={eventBands(c.dates)}
        format={pct}
        series={[{ name: "基差", values: col(c, "basis_pct"), color: "var(--accent)" }]}
      />
      <TimeChart
        label="前20净多"
        dates={c.dates}
        height={104}
        baseline={0}
        bands={eventBands(c.dates)}
        format={(v) => v.toFixed(0)}
        series={[{ name: "前20净多(手)", values: col(c, "net"), color: "var(--warn)" }]}
      />
    </Panel>
  );
}

function Breadth({ data }: { data: MacroData }) {
  const c = data.breadth_sel;
  if (!c || !data.breadth_picks.length) return null;
  return (
    <Panel
      title="市场宽度（每类只保留一个）"
      sub="4 类共 28 个原始/平滑变体，按训练期（2024-07 前）与中证全指未来20/60日收益的秩相关挑选，测试期同号才保留；IC 全为负 = 宽度在A股更像反向（均值回归）指标。其余变体只在事件研究表里"
    >
      <DataTable
        label="宽度入选"
        rows={data.breadth_picks}
        rowKey={(r) => r.group}
        columns={[
          { id: "g", header: "类别", value: (r) => r.group },
          { id: "v", header: "入选变体", value: (r) => r.variant },
          { id: "tr", header: "训练IC", value: (r) => r.train_ic.toFixed(3) },
          { id: "te", header: "测试IC", value: (r) => r.test_ic.toFixed(3) },
          { id: "w", header: "理由", value: (r) => r.reason },
        ]}
      />
      <IndexChart c={c} k="index" name="中证全指" />
      {data.breadth_picks.map((p) => (
        <TimeChart
          key={p.variant}
          label={p.variant}
          dates={c.dates}
          height={90}
          bands={eventBands(c.dates)}
          format={(v) => (Math.abs(v) < 5 ? v.toFixed(2) : v.toFixed(0))}
          series={[
            { name: `${p.group}：${p.variant}`, values: col(c, p.variant), color: "var(--accent)" },
          ]}
        />
      ))}
    </Panel>
  );
}

function HkSystem({ data }: { data: MacroData }) {
  const c = data.hk;
  if (!c) return null;
  const sig = col(c, "signal").map((v, i) => (v ? (col(c, "csi300")[i] ?? null) : null));
  return (
    <Panel
      title="定价权系统C"
      sub="恒生科技指数 × 恒生科技ETF成交量 构造 SMARTMONEY；信号=SM>MA20 且 上证50/沪深300/创业板 同日收涨"
    >
      <TimeChart
        label="沪深300与信号"
        dates={c.dates}
        height={110}
        bands={eventBands(c.dates)}
        format={(v) => v.toFixed(0)}
        series={[
          { name: "沪深300", values: col(c, "csi300"), color: "var(--muted)" },
          { name: "信号日", values: sig, color: "var(--up)" },
        ]}
      />
      <TimeChart
        label="SMARTMONEY"
        dates={c.dates}
        height={104}
        bands={eventBands(c.dates)}
        format={(v) => v.toFixed(2)}
        series={[
          { name: "SMARTMONEY", values: col(c, "smart"), color: "var(--accent)" },
          { name: "MA20", values: col(c, "smart_ma20"), color: "var(--warn)", dashed: true },
        ]}
      />
      {data.leadlag ? (
        <TimeChart
          label="领先滞后相关"
          dates={data.leadlag.dates}
          height={96}
          baseline={0}
          bars
          format={(v) => v.toFixed(3)}
          series={[
            {
              name: "corr(恒科 t, 沪深300 t+k)",
              values: col(data.leadlag, "corr"),
              color: "var(--accent)",
            },
          ]}
        />
      ) : null}
      {data.hk_rollcorr ? (
        <TimeChart
          label="滚动相关"
          dates={data.hk_rollcorr.dates}
          height={84}
          baseline={0}
          format={(v) => v.toFixed(2)}
          series={[
            {
              name: "250日滚动 corr(恒科 t, 沪深300 t+1)",
              values: col(data.hk_rollcorr, "corr_lag1"),
              color: "var(--warn)",
            },
          ]}
        />
      ) : null}
    </Panel>
  );
}

function Studies({ data }: { data: MacroData }) {
  if (!data.studies.length) return null;
  const f = (v: number | null | undefined) => (v == null ? "—" : pct(v));
  return (
    <Panel
      title="信号事件研究（前瞻收益）"
      sub="事件日去重（10个交易日内只算首日）；基准=同样本所有交易日。样本少时（n<10）结论不可靠"
      flush
    >
      <DataTable
        label="事件研究"
        rows={data.studies}
        rowKey={(r) => `${r.study}-${r.horizon}`}
        columns={[
          { id: "s", header: "信号", value: (r) => r.study },
          {
            id: "sp",
            header: "样本",
            value: (r) => (r.split === "train" ? "训练" : r.split === "test" ? "测试" : "全样本"),
          },
          { id: "h", header: "持有天数", value: (r) => r.horizon },
          { id: "n", header: "次数", value: (r) => r.n },
          { id: "m", header: "平均收益", value: (r) => f(r.mean) },
          { id: "w", header: "胜率", value: (r) => f(r.win) },
          { id: "bm", header: "基准平均", value: (r) => f(r.base_mean) },
          { id: "bw", header: "基准胜率", value: (r) => f(r.base_win) },
        ]}
      />
    </Panel>
  );
}
