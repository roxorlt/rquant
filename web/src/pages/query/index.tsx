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
        sub="Ctrl+Enter 运行；点表名快速预览"
        actions={
          <Button variant="primary" onClick={() => run.mutate(sql)} disabledReason={readOnly}>
            运行
          </Button>
        }
      >
        {tables.data ? (
          <nav className="sql-tables" aria-label="可查询的表">
            {tables.data.map((t) => (
              <button
                key={t}
                type="button"
                className="sql-table"
                onClick={() => {
                  const next = `SELECT * FROM ${t} LIMIT 50`;
                  setSql(next);
                  if (!readOnly) run.mutate(next);
                }}
              >
                {t}
              </button>
            ))}
          </nav>
        ) : null}
        <textarea
          aria-label="SQL"
          onKeyDown={(e) => {
            if (e.key === "Enter" && (e.ctrlKey || e.metaKey) && !readOnly) {
              e.preventDefault();
              run.mutate(sql);
            }
          }}
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
            tools="查询结果"
            emptyText="查询成功但 0 行：表为空，或 WHERE 条件没有匹配。"
            rows={result.rows.map((r, i) => ({ i, r }))}
            rowKey={(x) => String(x.i)}
            height={480}
            columns={result.columns.map((c, j) => ({
              id: c,
              header: c,
              sortable: true,
              // Right-align columns whose values are all numbers.
              numeric:
                result.rows.length > 0 &&
                result.rows.every((r) => r[j] == null || typeof r[j] === "number"),
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
