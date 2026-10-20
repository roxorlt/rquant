import { useState } from "react";
import { useConditions, useTextToCondition, useTranslateTdx } from "@/api/endpoints";
import { toneClass, toneOf } from "@/format/color";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, failureCopy, Panel } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";

/** How many tokens the translator refused. One message is one miss. */
function unrecognizedCount(error: string | null): number {
  if (!error) return 0;
  const hits = error.match(/不支持：|无法识别：/g);
  return hits?.length ?? 1;
}

function TdxImport() {
  const [tdx, setTdx] = useState("C>MA(C,5) AND V>REF(V,1)*2");
  const translate = useTranslateTdx();
  const result = translate.data;
  const [text, setText] = useState("");
  const ai = useTextToCondition();
  const passed = Boolean(result?.expression);
  const misses = unrecognizedCount(result?.error ?? null);
  return (
    <Panel title="通达信公式导入" sub="只翻译和校验。">
      <div className="formula-ai">
        <input
          aria-label="一句话条件"
          placeholder="一句话描述条件，例如：站上20日线且放量一倍"
          value={text}
          onChange={(e) => setText(e.target.value)}
        />
        <Button
          variant="quiet"
          disabled={ai.isPending || text.trim().length < 2}
          onClick={() =>
            ai.mutate(text, {
              onSuccess: (d) => {
                if (d.tdx) setTdx(d.tdx);
                translate.reset();
              },
            })
          }
        >
          {ai.isPending ? "生成中…" : "一句话生成"}
        </Button>
      </div>
      {ai.error ? <p className="row-err">{failureCopy("条件", "生成", ai.error).text}</p> : null}
      <textarea
        aria-label="通达信公式"
        rows={3}
        style={{ width: "100%" }}
        value={tdx}
        onChange={(e) => setTdx(e.target.value)}
      />
      <div className="formula-actions">
        <Button onClick={() => translate.mutate(tdx)}>翻译</Button>
      </div>
      {translate.error ? (
        <p className="row-err">{failureCopy("公式", "翻译", translate.error).text}</p>
      ) : null}
      {result ? (
        <div className="formula-result">
          <div className="formula-result-h">
            <span className="formula-result-label">翻译结果</span>
            <span className={passed ? "st ok" : "st warn"}>
              {passed ? "校验通过" : `有 ${misses} 处不认识`}
            </span>
          </div>
          <div className="formula-result-expr">{result.expression ?? result.error}</div>
        </div>
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
          const runs = data.runs ?? [];
          const run = runs[picked];
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
              {runs.length > 1 ? (
                <select
                  aria-label="选择条件"
                  value={picked}
                  onChange={(e) => setPicked(Number(e.target.value))}
                >
                  {runs.map((r, i) => (
                    <option key={r.run_id} value={i}>
                      {r.name}
                    </option>
                  ))}
                </select>
              ) : null}
              {run ? (
                <DataTable
                  label="条件命中"
                  tools="条件选股"
                  rows={run.hits}
                  rowKey={(row) => row.code}
                  height={320}
                  columns={[
                    {
                      id: "code",
                      header: "代码",
                      sortable: true,
                      value: (row) => row.code,
                      cell: (row) => <StockLink code={row.code} />,
                    },
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
                      cell: (row) =>
                        row.pct_chg == null ? (
                          "—"
                        ) : (
                          <span className={toneClass(toneOf(row.pct_chg))}>
                            {row.pct_chg > 0 ? "+" : ""}
                            {row.pct_chg.toFixed(2)}
                          </span>
                        ),
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
