import { useFactor, useFactors } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import { TimeChart } from "@/charts/Chart";
import { DataTable } from "@/table/DataTable";
import { KpiStrip, PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";

const num = (v: number | null | undefined, d = 3) => (v == null ? "—" : v.toFixed(d));
const pct = (v: number | null | undefined) => (v == null ? "—" : `${(v * 100).toFixed(2)}%`);

function IcBars({
  points,
  label = "每日 IC",
}: {
  points: { date: string; ic: number }[];
  label?: string;
}) {
  return (
    <TimeChart
      label={label}
      dates={points.map((p) => p.date)}
      bars
      baseline={0}
      height={140}
      format={(v) => v.toFixed(3)}
      series={[{ name: "IC", values: points.map((p) => p.ic), color: "var(--accent)" }]}
    />
  );
}

function Detail({ id }: { id: string }) {
  const query = useFactor(id);
  return (
    <QueryView query={query}>
      {({ factor, result, tracking }) => (
        <>
          {tracking ? (
            <Panel
              title="近期跟踪"
              sub={`${tracking.window_start} ~ ${tracking.latest_date ?? "—"}，python -m rquant.factor track 更新`}
            >
              <KpiStrip
                label="跟踪指标"
                compact
                items={[
                  { key: "recent", label: "近 20 日 IC", value: num(tracking.recent_ic) },
                  { key: "research", label: "检验期 IC", value: num(tracking.research_ic) },
                  {
                    key: "state",
                    label: "状态",
                    value:
                      tracking.recent_ic == null || tracking.research_ic == null
                        ? "—"
                        : Math.sign(tracking.recent_ic) !== Math.sign(tracking.research_ic)
                          ? "方向反转"
                          : Math.abs(tracking.recent_ic) < Math.abs(tracking.research_ic) / 2
                            ? "衰减"
                            : "正常",
                  },
                ]}
              />
              <IcBars points={tracking.points} label="跟踪 IC" />
            </Panel>
          ) : null}
          <Panel title={factor.name} sub={factor.expression}>
            <KpiStrip
              label="因子检验指标"
              compact
              items={[
                { key: "ic", label: `RankIC(${result.horizon}日)`, value: num(result.mean_ic) },
                { key: "ir", label: "ICIR", value: num(result.ic_ir, 2) },
                { key: "t", label: "t 值", value: num(result.t_stat, 2) },
                { key: "pos", label: "IC>0 占比", value: pct(result.positive_ratio) },
                { key: "ls", label: "多空(5-1)", value: pct(result.long_short) },
                { key: "cov", label: "覆盖率", value: pct(result.coverage) },
                { key: "to", label: "Q5 日换手", value: pct(result.top_turnover) },
              ]}
            />
            <IcBars points={result.ic_series} />
          </Panel>
          <QuantilePanel result={result} />
          <Panel title="分组与衰减" sub="收盘到收盘，未计成本，仅作筛选" flush>
            <table className="tbl" aria-label="分组收益">
              <thead>
                <tr>
                  {result.quantiles.map((q) => (
                    <th key={q.quantile} className="num">{`Q${q.quantile}`}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                <tr>
                  {result.quantiles.map((q) => (
                    <td key={q.quantile} className="num">
                      {pct(q.mean_return)}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
            <table className="tbl" aria-label="IC 衰减">
              <thead>
                <tr>
                  {result.decay.map((d) => (
                    <th key={d.horizon} className="num">{`${d.horizon}日`}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                <tr>
                  {result.decay.map((d) => (
                    <td key={d.horizon} className="num">
                      {num(d.mean_ic)}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          </Panel>
        </>
      )}
    </QueryView>
  );
}

type TestResult = NonNullable<ReturnType<typeof useFactor>["data"]>["result"];

const Q_COLORS = ["var(--down)", "var(--faint)", "var(--muted)", "var(--warn)", "var(--up)"];

/** Cumulative quantile returns + long-short, IC decay and IC distribution (qlib-style). */
function QuantilePanel({ result }: { result: TestResult }) {
  const days = result.quantile_series ?? [];
  const n = result.quantiles.length;
  const cum = (pick: (r: (number | null)[]) => number | null) => {
    let acc = 1;
    return days.map((d) => {
      const v = pick(d.returns);
      if (v != null) acc *= 1 + v;
      return acc;
    });
  };
  const series = Array.from({ length: n }, (_, k) => ({
    name: `Q${k + 1}`,
    values: cum((r) => r[k] ?? null),
    color:
      Q_COLORS[Math.round((k * (Q_COLORS.length - 1)) / Math.max(1, n - 1))] ?? "var(--accent)",
  }));
  const longShort = {
    name: `多空 Q${n}-Q1`,
    values: cum((r) => {
      const hi = r[n - 1];
      const lo = r[0];
      return hi == null || lo == null ? null : hi - lo;
    }),
    color: "var(--accent)",
  };
  // IC histogram: 15 equal bins over the observed range.
  const ics = result.ic_series.map((p) => p.ic);
  const lo = Math.min(...ics, 0);
  const hi = Math.max(...ics, 0);
  const bins = 15;
  const width = (hi - lo) / bins || 1;
  const counts = Array.from({ length: bins }, () => 0);
  for (const v of ics) {
    const k = Math.min(bins - 1, Math.floor((v - lo) / width));
    counts[k] = (counts[k] ?? 0) + 1;
  }
  return (
    <Panel title="分组累计收益" sub={`按日均摊 ${result.horizon} 日前瞻收益后连乘，未计成本`}>
      {days.length > 1 ? (
        <TimeChart
          label="分组累计收益"
          dates={days.map((d) => d.date)}
          baseline={1}
          format={(v) => v.toFixed(3)}
          series={[...series, longShort]}
        />
      ) : (
        <p className="hint">
          这条检验结果产出较早，没有逐日分组数据；重跑 python -m rquant.factor 后显示。
        </p>
      )}
      <div className="risk-block">
        <div>
          <h3 className="sub">IC 衰减（持有期 → 平均 RankIC）</h3>
          <TimeChart
            label="IC 衰减"
            dates={result.decay.map((d) => `${d.horizon}日`)}
            bars
            baseline={0}
            height={130}
            format={(v) => v.toFixed(3)}
            series={[
              {
                name: "IC",
                values: result.decay.map((d) => d.mean_ic ?? null),
                color: "var(--accent)",
              },
            ]}
          />
        </div>
        <div>
          <h3 className="sub">IC 分布（{ics.length} 天）</h3>
          {ics.length > 1 ? (
            <TimeChart
              label="IC 分布"
              dates={counts.map((_, k) => (lo + (k + 0.5) * width).toFixed(2))}
              bars
              baseline={0}
              height={130}
              format={(v) => v.toFixed(0)}
              series={[{ name: "天数", values: counts, color: "var(--accent)", neutral: true }]}
            />
          ) : null}
        </div>
      </div>
    </Panel>
  );
}

export default function FactorPage() {
  const query = useFactors();
  const [id, setId] = useUrlState("factor");
  return (
    <>
      <PageHeader eyebrow="研究" title="因子研究" />
      <QueryView query={query}>
        {(data) => (
          <Panel
            title={`因子 · ${data.factors.length}`}
            sub="python -m rquant.factor --name … --expr … 产出"
            flush
          >
            <DataTable
              label="因子列表"
              tools="因子"
              rows={data.factors}
              rowKey={(row) => row.factor_id}
              onSelect={(row) => setId(row.factor_id)}
              selectedKey={id}
              emptyText="还没有因子检验结果：运行 python -m rquant.factor 产出 IC / 分组检验后显示。"
              columns={[
                { id: "name", header: "名称", value: (row) => row.name },
                { id: "expr", header: "表达式", value: (row) => row.expression },
                {
                  id: "range",
                  header: "区间",
                  value: (row) => row.start,
                  cell: (row) => `${row.start} ~ ${row.end}`,
                },
                {
                  id: "ic",
                  header: "IC",
                  numeric: true,
                  value: (row) => row.mean_ic ?? null,
                  cell: (row) => num(row.mean_ic),
                },
                {
                  id: "recent",
                  header: "近期 IC",
                  numeric: true,
                  value: (row) => row.recent_ic ?? null,
                  cell: (row) => num(row.recent_ic),
                },
                {
                  id: "ir",
                  header: "ICIR",
                  numeric: true,
                  value: (row) => row.ic_ir ?? null,
                  cell: (row) => num(row.ic_ir, 2),
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      {id ? <Detail id={id} /> : null}
    </>
  );
}
