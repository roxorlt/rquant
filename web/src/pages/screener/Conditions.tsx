import { useState } from "react";
import { useConditions, useTranslateTdx } from "@/api/endpoints";
import { toneClass, toneOf } from "@/format/color";
import { formatFixed, formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, FailureNotice, failureCopy, Panel } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";

/** How many tokens the translator refused. One message is one miss. */
function unrecognizedCount(error: string | null): number {
  if (!error) return 0;
  const hits = error.match(/不支持：|无法识别：/g);
  return hits?.length ?? 1;
}

function TdxImport({ onAddTdx }: { onAddTdx?: (tdx: string) => void }) {
  const [tdx, setTdx] = useState("C>MA(C,5) AND V>REF(V,1)*2");
  const translate = useTranslateTdx();
  const result = translate.data;
  const passed = Boolean(result?.expression);
  const misses = unrecognizedCount(result?.error ?? null);
  return (
    <Panel title="通达信公式沙盒" sub="校验语法后可一键加入上方实时选股漏斗">
      <textarea
        aria-label="通达信公式"
        rows={3}
        style={{ width: "100%" }}
        value={tdx}
        onChange={(e) => setTdx(e.target.value)}
      />
      <div className="formula-actions">
        <Button variant="quiet" onClick={() => translate.mutate(tdx)}>
          翻译
        </Button>
        {onAddTdx ? (
          <Button variant="primary" disabled={!tdx.trim()} onClick={() => onAddTdx(tdx.trim())}>
            加入选股漏斗
          </Button>
        ) : null}
      </div>
      {translate.error ? (
        <FailureNotice
          copy={failureCopy("公式", "翻译", translate.error)}
          onRetry={() => translate.mutate(tdx)}
        />
      ) : null}
      {result ? (
        <output className="formula-result" aria-label="翻译结果">
          <div className="formula-result-h">
            <span className="formula-result-label">翻译结果</span>
            <span className={passed ? "st ok" : "st warn"}>
              {passed ? "校验通过" : `有 ${misses} 处不认识`}
            </span>
          </div>
          <div className="formula-result-expr">{result.expression ?? result.error}</div>
        </output>
      ) : null}
    </Panel>
  );
}

export function Conditions({ onAddTdx }: { onAddTdx?: (tdx: string) => void }) {
  const query = useConditions();
  const [picked, setPicked] = useState(0);
  return (
    <>
      <TdxImport onAddTdx={onAddTdx} />
      <QueryView query={query}>
        {(data) => {
          const runs = data.runs ?? [];
          const run = runs[picked];
          return (
            <Panel
              title="历史离线条件选股"
              sub={
                run
                  ? `${run.name} · ${run.trade_date ?? "—"} · 命中 ${run.hits.length}/${run.universe}`
                  : "还没有离线条件选股结果"
              }
              actions={
                <span className="tools-group">
                  {runs.length > 1 ? (
                    <select
                      className="select"
                      aria-label="演示条件"
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
                  {run?.source_formula && onAddTdx ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => onAddTdx(run.source_formula ?? "")}
                    >
                      载入公式到漏斗
                    </Button>
                  ) : null}
                </span>
              }
              flush
            >
              {run ? (
                <DataTable
                  label="条件命中"
                  tools="条件选股"
                  rows={run.hits}
                  rowKey={(row) => row.code}
                  height={300}
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
                            {formatFixed(row.pct_chg, 2)}
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
