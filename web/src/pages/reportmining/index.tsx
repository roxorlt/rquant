import { useReportFactors } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import { TimeChart } from "@/charts/Chart";
import { DataTable } from "@/table/DataTable";
import { KpiStrip, PageHeader, Panel, Pill } from "@/ui";
import { QueryView } from "../shared";

/* Shapes written by rquant.research.report_factors (served opaque as results). */
interface Stat {
  mean: number | null;
  ir: number | null;
  win: number | null;
  n: number;
}
interface Block {
  rank_ic: Stat;
  ls_ann_gross: number | null;
  ls_ann_net: number | null;
  long_excess_ann_gross: number | null;
  long_excess_ann_net: number | null;
  turnover_top: number | null;
  quintile_ann: (number | null)[];
}
interface FactorResult {
  name: string;
  data: string;
  catalog: string;
  sign: number;
  split: string;
  train: Block;
  test: Block;
  size_neutral_rank_ic_test: number;
  horizon_rank_ic: Record<string, number>;
  decay: boolean;
  validated: boolean;
  latest_date: string;
  series: {
    dates: string[];
    rank_ic: number[];
    q_nav: number[][];
    ls_nav_net: number[];
    long_excess_nav_net: number[];
  };
}
interface CatalogItem {
  id: string;
  title: string;
  broker: string;
  date: string;
  url: string;
  formula: string;
  data: string;
  replicability: string;
  note?: string;
}
interface Results {
  generated_at: string;
  hold_days: number;
  cost_per_side: number;
  factors: Record<string, FactorResult>;
  catalog: CatalogItem[];
}
type Row = FactorResult & { key: string };
interface ChangeRow {
  id: string;
  name: string;
  test_rank_ic: number | null;
  prev_test_rank_ic: number | null;
}
interface NewReport {
  id: string;
  title: string;
  broker: string;
  date: string;
  url: string;
  replicability: string;
  status: string;
}
export interface WeeklySummary {
  generated_at: string;
  week_of: string;
  headline: string;
  new_reports: NewReport[];
  new_passing: ChangeRow[];
  lost_passing: ChangeRow[];
  decayed: ChangeRow[];
  validated_now: string[];
}

const STATUS: Record<string, string> = {
  linked: "同族已复现",
  pending_impl: "待实现",
  registered: "已登记",
};

function Recent({ sum }: { sum: WeeklySummary | null }) {
  if (!sum) {
    return (
      <Panel title="最近更新">
        <p className="hint">每周六 06:30 自动检索新研报、复评衰减；首份周报生成后显示在这里。</p>
      </Panel>
    );
  }
  const names = (rows: ChangeRow[]) => (rows.length ? rows.map((r) => r.name).join("、") : "无");
  return (
    <Panel title={`最近更新 · ${sum.week_of}`} sub={sum.headline}>
      <KpiStrip
        compact
        label="周度变化"
        items={[
          { key: "new", label: "新收录研报", value: sum.new_reports.length },
          {
            key: "pass",
            label: "新通过",
            value: sum.new_passing.length,
            sub: names(sum.new_passing),
          },
          {
            key: "lost",
            label: "失效",
            value: sum.lost_passing.length,
            sub: names(sum.lost_passing),
          },
          { key: "dec", label: "新衰减", value: sum.decayed.length, sub: names(sum.decayed) },
        ]}
      />
      {sum.new_reports.length ? (
        <ul className="weight-bars">
          {sum.new_reports.slice(0, 8).map((n) => (
            <li key={n.id}>
              {n.date} {n.broker}{" "}
              <a href={n.url} target="_blank" rel="noreferrer">
                {n.title}
              </a>{" "}
              <Pill kind={n.status === "registered" ? "idle" : "acc"}>
                {STATUS[n.status] ?? n.status}
              </Pill>
            </li>
          ))}
        </ul>
      ) : null}
    </Panel>
  );
}

const pct = (v: number | null | undefined, d = 1) => (v == null ? "—" : `${(v * 100).toFixed(d)}%`);
const num = (v: number | null | undefined, d = 2) => (v == null ? "—" : v.toFixed(d));

const REPL: Record<string, { label: string; kind: "ok" | "acc" | "warn" | "crit" }> = {
  daily: { label: "日线可做", kind: "ok" },
  "5min": { label: "5分钟可做", kind: "ok" },
  "3s_snapshot_approx": { label: "3s快照·积累中", kind: "warn" },
  needs_tick_infeasible: { label: "需逐笔·不可行", kind: "crit" },
};
const Q_COLORS = ["#4caf50", "#8bc34a", "#9e9e9e", "#ff9800", "#f44336"];

function status(r: FactorResult) {
  if (r.validated) return <Pill kind="ok">通过</Pill>;
  if (r.decay) return <Pill kind="crit">衰减</Pill>;
  return <Pill kind="idle">未通过</Pill>;
}

function Detail({ r, hold }: { r: FactorResult; hold: number }) {
  const s = r.series;
  return (
    <Panel
      title={`${r.name} · ${r.data}`}
      sub={`方向 ${r.sign > 0 ? "越大越好" : "越小越好"} · 训练/测试切分 ${r.split} · 每 ${hold} 日调仓 · 最新 ${r.latest_date}`}
    >
      <KpiStrip
        compact
        label="测试期指标"
        items={[
          { key: "ic", label: "测试 RankIC", value: num(r.test.rank_ic.mean, 3) },
          { key: "ir", label: "ICIR(年化)", value: num(r.test.rank_ic.ir) },
          { key: "neu", label: "市值中性 RankIC", value: num(r.size_neutral_rank_ic_test, 3) },
          { key: "ls", label: "多空年化(净)", value: pct(r.test.ls_ann_net) },
          { key: "lx", label: "多头超额(净)", value: pct(r.test.long_excess_ann_net) },
          { key: "to", label: "Top组换手", value: pct(r.test.turnover_top, 0) },
        ]}
      />
      <div className="risk-block">
        <div>
          <h3 className="sub">净值（扣费）</h3>
          <TimeChart
            label="净值"
            height={150}
            dates={s.dates}
            baseline={1}
            format={(v) => v.toFixed(3)}
            bands={[]}
            series={[
              { name: "多空(扣费)", values: s.ls_nav_net, color: "var(--accent)" },
              { name: "多头超额(扣费)", values: s.long_excess_nav_net, color: "#f44336" },
            ]}
          />
        </div>
        <div>
          <h3 className="sub">五分组超额</h3>
          <TimeChart
            label="五分组"
            dates={s.dates}
            baseline={1}
            height={150}
            format={(v) => v.toFixed(3)}
            series={s.q_nav.map((v, k) => ({
              name: `Q${k + 1}`,
              values: v,
              color: Q_COLORS[k] ?? "#999",
            }))}
          />
        </div>
      </div>
      <div className="risk-block">
        <div>
          <h3 className="sub">RankIC（每期，已按方向调整）</h3>
          <TimeChart
            label="RankIC"
            dates={s.dates}
            bars
            baseline={0}
            height={96}
            format={(v) => v.toFixed(3)}
            series={[{ name: "RankIC", values: s.rank_ic, color: "var(--accent)" }]}
          />
        </div>
        <div>
          <h3 className="sub">IC 衰减（持有期 → RankIC）</h3>
          <TimeChart
            label="IC 衰减"
            dates={Object.keys(r.horizon_rank_ic).map((h) => `${h}日`)}
            bars
            baseline={0}
            height={96}
            format={(v) => v.toFixed(3)}
            series={[
              { name: "RankIC", values: Object.values(r.horizon_rank_ic), color: "var(--accent)" },
            ]}
          />
        </div>
      </div>
    </Panel>
  );
}

function Body({ res, sum }: { res: Results; sum: WeeklySummary | null }) {
  const [sel, setSel] = useUrlState("f");
  const rows: Row[] = Object.entries(res.factors).map(([key, r]) => ({ ...r, key }));
  const current = rows.find((r) => r.key === sel) ?? rows[0];
  const feasible = res.catalog.filter(
    (c) => c.replicability === "daily" || c.replicability === "5min",
  );
  return (
    <>
      <KpiStrip
        label="研报因子概况"
        items={[
          { key: "cat", label: "收录研报", value: res.catalog.length },
          { key: "feas", label: "我们数据可做", value: feasible.length },
          { key: "done", label: "已复现", value: rows.length },
          { key: "ok", label: "通过验证→选股器", value: rows.filter((r) => r.validated).length },
        ]}
      />
      <div className="card-grid">
        <Panel
          title="测试期 RankIC 一览"
          sub={`已按训练期方向调整；>0 即样本外仍有效。成本 ${(res.cost_per_side * 100).toFixed(1)}%/单边`}
        >
          <TimeChart
            label="测试 RankIC"
            dates={rows.map((r) => r.name)}
            bars
            baseline={0}
            height={110}
            format={(v) => v.toFixed(3)}
            series={[
              {
                name: "测试RankIC",
                values: rows.map((r) => r.test.rank_ic.mean),
                color: "var(--accent)",
              },
            ]}
          />
        </Panel>
        <Recent sum={sum} />
        <div className="span-all">
          <Panel
            title="复现结果"
            sub="点一行看图；通过 = 测试 RankIC≥0.02、ICIR≥1、扣费多头超额>0"
            flush
          >
            <DataTable<Row>
              label="复现结果"
              rows={rows}
              rowKey={(r) => r.key}
              selectedKey={current?.key ?? null}
              onSelect={(r) => setSel(r.key)}
              columns={[
                { id: "name", header: "因子", value: (r) => r.name },
                { id: "data", header: "数据", value: (r) => r.data },
                {
                  id: "tr",
                  header: "训练RankIC",
                  numeric: true,
                  value: (r) => r.train.rank_ic.mean,
                  cell: (r) => num(r.train.rank_ic.mean, 3),
                },
                {
                  id: "te",
                  header: "测试RankIC",
                  numeric: true,
                  value: (r) => r.test.rank_ic.mean,
                  cell: (r) => num(r.test.rank_ic.mean, 3),
                },
                {
                  id: "ir",
                  header: "ICIR",
                  numeric: true,
                  value: (r) => r.test.rank_ic.ir,
                  cell: (r) => num(r.test.rank_ic.ir),
                },
                {
                  id: "ls",
                  header: "多空(净)",
                  numeric: true,
                  value: (r) => r.test.ls_ann_net,
                  cell: (r) => pct(r.test.ls_ann_net),
                },
                {
                  id: "lx",
                  header: "多头超额(净)",
                  numeric: true,
                  value: (r) => r.test.long_excess_ann_net,
                  cell: (r) => pct(r.test.long_excess_ann_net),
                },
                {
                  id: "to",
                  header: "换手",
                  numeric: true,
                  value: (r) => r.test.turnover_top,
                  cell: (r) => pct(r.test.turnover_top, 0),
                },
                {
                  id: "st",
                  header: "状态",
                  value: (r) => (r.validated ? 2 : r.decay ? 0 : 1),
                  cell: status,
                },
              ]}
            />
          </Panel>
        </div>
        <div className="span-all">
          {current ? <Detail r={current} hold={res.hold_days} /> : null}
        </div>
        <div className="span-all">
          <Panel title="研报目录" sub="近 12 个月 + 经典；链接为公开摘要页" flush>
            <DataTable<CatalogItem>
              label="研报目录"
              rows={res.catalog}
              rowKey={(c) => c.id}
              columns={[
                { id: "broker", header: "券商", sortable: true, value: (c) => c.broker },
                { id: "date", header: "日期", sortable: true, value: (c) => c.date },
                {
                  id: "title",
                  header: "报告",
                  wrap: true,
                  value: (c) => c.title,
                  cell: (c) => (
                    <a href={c.url} target="_blank" rel="noreferrer">
                      {c.title}
                    </a>
                  ),
                },
                { id: "formula", header: "定义", wrap: true, value: (c) => c.formula },
                {
                  id: "repl",
                  header: "可复现",
                  sortable: true,
                  value: (c) => c.replicability,
                  cell: (c) => {
                    const m = REPL[c.replicability] ?? {
                      label: c.replicability,
                      kind: "acc" as const,
                    };
                    return <Pill kind={m.kind}>{m.label}</Pill>;
                  },
                },
              ]}
            />
          </Panel>
        </div>
      </div>
    </>
  );
}

export default function ReportMiningPage() {
  const query = useReportFactors();
  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="研报挖掘"
        note="复现券商金工因子：5 分钟 / 日线数据可做的已回测；3s 快照类登记待数据积累"
      />
      <QueryView query={query}>
        {(data) =>
          data.results ? (
            <Body
              res={data.results as unknown as Results}
              sum={(data.summary ?? null) as unknown as WeeklySummary | null}
            />
          ) : (
            <Panel>
              <p className="hint">
                还没有研报因子结果。
              </p>
            </Panel>
          )
        }
      </QueryView>
    </>
  );
}
