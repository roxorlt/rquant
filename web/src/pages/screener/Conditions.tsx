import { useState } from "react";
import { useConditions, useTranslateTdx } from "@/api/endpoints";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, Panel } from "@/ui";
import { QueryView } from "../shared";

function TdxImport() {
  const [tdx, setTdx] = useState("C>MA(C,5) AND V>REF(V,1)*2");
  const translate = useTranslateTdx();
  const result = translate.data;
  return (
    <Panel title="通达信公式导入" sub="只翻译与校验；执行用 python -m rquant.factor screen --tdx">
      <textarea
        aria-label="通达信公式"
        rows={3}
        style={{ width: "100%" }}
        value={tdx}
        onChange={(e) => setTdx(e.target.value)}
      />
      <Button size="sm" onClick={() => translate.mutate(tdx)}>
        翻译
      </Button>
      {result ? (
        <output aria-label="翻译结果" className="sub">
          {result.expression ? `表达式：${result.expression}` : `不支持：${result.error}`}
        </output>
      ) : null}
    </Panel>
  );
}

export function Conditions() {
  const query = useConditions();
  const [picked, setPicked] = useState(0);
  return (
    <>
      <TdxImport />
      <QueryView query={query}>
        {(data) => {
          const run = data.runs[picked];
          return (
            <Panel
              title="条件选股"
              sub={
                run
                  ? `${run.name} · ${run.trade_date ?? "—"} · 命中 ${run.hits.length}/${run.universe}`
                  : "还没有条件选股结果"
              }
              flush
            >
              {data.runs.length > 1 ? (
                <select
                  aria-label="选择条件"
                  value={picked}
                  onChange={(e) => setPicked(Number(e.target.value))}
                >
                  {data.runs.map((r, i) => (
                    <option key={r.run_id} value={i}>
                      {r.name}
                    </option>
                  ))}
                </select>
              ) : null}
              {run ? (
                <DataTable
                  label="条件命中"
                  rows={run.hits}
                  rowKey={(row) => row.code}
                  height={320}
                  columns={[
                    { id: "code", header: "代码", value: (row) => row.code },
                    {
                      id: "close",
                      header: "收盘",
                      numeric: true,
                      value: (row) => row.close ?? null,
                      cell: (row) => formatPrice(row.close),
                    },
                    {
                      id: "pct",
                      header: "涨跌%",
                      numeric: true,
                      value: (row) => row.pct_chg ?? null,
                      cell: (row) => (row.pct_chg == null ? "—" : row.pct_chg.toFixed(2)),
                    },
                  ]}
                />
              ) : null}
            </Panel>
          );
        }}
      </QueryView>
    </>
  );
}
