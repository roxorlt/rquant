import { useState } from "react";
import { useQueryTables, useRunQuery } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { DataTable } from "@/table/DataTable";
import { Button, PageHeader, Panel } from "@/ui";

export default function QueryPage() {
  const readOnly = useReadOnlyReason();
  const tables = useQueryTables();
  const run = useRunQuery();
  const [sql, setSql] = useState("SELECT * FROM stock_basic LIMIT 50");
  const result = run.data;
  return (
    <>
      <PageHeader
        eyebrow="数据"
        title="数据查询"
        note="只读 SQL（SELECT / WITH），最多 1000 行，读 Serving 当前代"
      />
      <Panel
        title="SQL"
        sub={tables.data ? `可查询的表：${tables.data.join("、")}` : undefined}
        actions={
          <Button variant="primary" onClick={() => run.mutate(sql)} disabledReason={readOnly}>
            运行
          </Button>
        }
      >
        <textarea
          aria-label="SQL"
          className="sql-input"
          rows={5}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
        />
        {run.error ? (
          <p className="hint" role="alert">
            {run.error.message}
          </p>
        ) : null}
      </Panel>
      {result ? (
        <Panel
          title={`结果 · ${result.rows.length} 行${result.truncated ? "（已截断）" : ""}`}
          flush
        >
          <DataTable
            label="查询结果"
            rows={result.rows.map((r, i) => ({ i, r }))}
            rowKey={(x) => String(x.i)}
            height={480}
            columns={result.columns.map((c, j) => ({
              id: c,
              header: c,
              value: (x: { r: unknown[] }) => {
                const v = x.r[j];
                return v === null || v === undefined ? null : typeof v === "number" ? v : String(v);
              },
            }))}
          />
        </Panel>
      ) : null}
    </>
  );
}
