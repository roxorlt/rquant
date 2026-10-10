import { useState } from "react";
import { useFactor, useFactors } from "@/api/endpoints";
import { DataTable } from "@/table/DataTable";
import { KpiStrip, PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";

const num = (v: number | null | undefined, d = 3) => (v == null ? "—" : v.toFixed(d));
const pct = (v: number | null | undefined) => (v == null ? "—" : `${(v * 100).toFixed(2)}%`);

function IcBars({ points }: { points: { date: string; ic: number }[] }) {
  if (points.length < 2) return null;
  const w = 640;
  const h = 120;
  const max = Math.max(0.05, ...points.map((p) => Math.abs(p.ic)));
  const bw = w / points.length;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} role="img" aria-label="每日 IC">
      <line x1={0} x2={w} y1={h / 2} y2={h / 2} stroke="var(--line, #ddd)" />
      {points.map((p, i) => {
        const bar = (Math.abs(p.ic) / max) * (h / 2 - 4);
        return (
          <rect
            key={p.date}
            x={i * bw}
            width={Math.max(1, bw - 1)}
            y={p.ic >= 0 ? h / 2 - bar : h / 2}
            height={bar}
            fill={p.ic >= 0 ? "var(--up, #d33)" : "var(--down, #2a2)"}
          />
        );
      })}
    </svg>
  );
}

function Detail({ id }: { id: string }) {
  const query = useFactor(id);
  return (
    <QueryView query={query}>
      {({ factor, result }) => (
        <>
          <Panel title={factor.name} sub={factor.expression}>
            <KpiStrip
              label="因子检验指标"
              compact
              items={[
                { key: "ic", label: `IC(${result.horizon}日)`, value: num(result.mean_ic) },
                { key: "ir", label: "ICIR", value: num(result.ic_ir, 2) },
                { key: "t", label: "t 值", value: num(result.t_stat, 2) },
                { key: "pos", label: "IC>0 占比", value: pct(result.positive_ratio) },
                { key: "ls", label: "多空(5-1)", value: pct(result.long_short) },
                { key: "cov", label: "覆盖率", value: pct(result.coverage) },
              ]}
            />
            <IcBars points={result.ic_series} />
          </Panel>
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

export default function FactorPage() {
  const query = useFactors();
  const [id, setId] = useState<string | null>(null);
  return (
    <>
      <PageHeader eyebrow="研究" title="因子检验" />
      <QueryView query={query}>
        {(data) => (
          <Panel title="因子" sub="python -m rquant.factor --name … --expr … 产出" flush>
            <DataTable
              label="因子列表"
              rows={data.factors}
              rowKey={(row) => row.factor_id}
              onSelect={(row) => setId(row.factor_id)}
              selectedKey={id}
              emptyText="还没有因子检验结果"
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
