import { useMemo, useRef, useState } from "react";
import { useQuerySchema, useRunQuery } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { TimeChart } from "@/charts/Chart";
import { DataTable } from "@/table/DataTable";
import { readPreference, writePreference } from "@/theme/storage";
import { Button, PageHeader, Panel } from "@/ui";
import { candidates, wordAt } from "./complete";

interface Saved {
  sql: string;
  starred?: boolean;
}

const HKEY = "query.history";

function loadHistory(): Saved[] {
  try {
    const v = JSON.parse(readPreference(HKEY) ?? "[]");
    return Array.isArray(v) ? (v as Saved[]) : [];
  } catch {
    return [];
  }
}

function storeHistory(list: Saved[]) {
  writePreference(HKEY, JSON.stringify(list));
}

/** Starred entries stay; the rest keep the 20 most recent. */
function remember(list: Saved[], sql: string): Saved[] {
  const old = list.find((h) => h.sql === sql);
  const rest = list.filter((h) => h.sql !== sql);
  const next = [{ sql, starred: old?.starred }, ...rest];
  const starred = next.filter((h) => h.starred);
  const plain = next.filter((h) => !h.starred).slice(0, 20);
  return [...starred, ...plain];
}

function QuickChart({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  const numeric = columns.filter(
    (_, j) => rows.length > 0 && rows.every((r) => r[j] == null || typeof r[j] === "number"),
  );
  const [x, setX] = useState(columns[0] ?? "");
  const [y, setY] = useState(numeric.find((c) => c !== columns[0]) ?? numeric[0] ?? "");
  const [bars, setBars] = useState(false);
  if (!numeric.length || rows.length < 2) return null;
  const xi = columns.indexOf(x);
  const yi = columns.indexOf(y);
  const data = [...rows].sort((a, b) =>
    String(a[xi]).localeCompare(String(b[xi]), "zh", { numeric: true }),
  );
  return (
    <Panel
      title="快速图表"
      sub="按 X 排序后画 Y；最多 1000 行"
      actions={
        <span className="qc-ctl">
          <label>
            X{" "}
            <select aria-label="X 轴" value={x} onChange={(e) => setX(e.target.value)}>
              {columns.map((c) => (
                <option key={c}>{c}</option>
              ))}
            </select>
          </label>
          <label>
            Y{" "}
            <select aria-label="Y 轴" value={y} onChange={(e) => setY(e.target.value)}>
              {numeric.map((c) => (
                <option key={c}>{c}</option>
              ))}
            </select>
          </label>
          <label>
            <input type="checkbox" checked={bars} onChange={(e) => setBars(e.target.checked)} />{" "}
            柱状
          </label>
        </span>
      }
    >
      <TimeChart
        label="查询结果图表"
        dates={data.map((r) => String(r[xi] ?? ""))}
        bars={bars}
        format={(v) => (Math.abs(v) >= 1e4 ? v.toExponential(2) : v.toFixed(2))}
        series={[
          {
            name: y,
            values: data.map((r) => (typeof r[yi] === "number" ? (r[yi] as number) : null)),
            color: "var(--accent)",
            neutral: true,
          },
        ]}
      />
    </Panel>
  );
}

export default function QueryPage() {
  const readOnly = useReadOnlyReason();
  const schema = useQuerySchema();
  const run = useRunQuery();
  const [sql, setSql] = useState("SELECT * FROM stock_basic LIMIT 50");
  const [cursor, setCursor] = useState(0);
  const [open, setOpen] = useState<string | null>(null);
  const [history, setHistory] = useState<Saved[]>(loadHistory);
  const ref = useRef<HTMLTextAreaElement>(null);
  const result = run.data;
  const tables = schema.data ?? {};
  const { start, word } = wordAt(sql, cursor);
  const sugg = useMemo(() => candidates(sql, word, tables), [sql, word, tables]);

  const exec = (text: string) => {
    if (readOnly) return;
    run.mutate(text);
    const next = remember(history, text.trim());
    setHistory(next);
    storeHistory(next);
  };
  const insert = (text: string, replaceWord = false) => {
    const from = replaceWord ? start : cursor;
    const next = sql.slice(0, from) + text + sql.slice(cursor);
    setSql(next);
    const pos = from + text.length;
    setCursor(pos);
    requestAnimationFrame(() => {
      ref.current?.focus();
      ref.current?.setSelectionRange(pos, pos);
    });
  };
  const toggleStar = (s: string) => {
    const next = history.map((h) => (h.sql === s ? { ...h, starred: !h.starred } : h));
    setHistory(next);
    storeHistory(next);
  };

  return (
    <>
      <PageHeader
        eyebrow="数据"
        title="数据查询"
        note="只读 SQL（SELECT / WITH），最多 1000 行，读 Serving 当前代"
      />
      <div className="query-grid">
        <Panel title="表与字段" sub="点表名预览，点字段插入" flush>
          <nav className="schema-tree" aria-label="可查询的表">
            {Object.entries(tables).map(([t, cols]) => (
              <div key={t}>
                <div className="schema-t">
                  <button
                    type="button"
                    aria-expanded={open === t}
                    className="schema-toggle"
                    onClick={() => setOpen(open === t ? null : t)}
                  >
                    {open === t ? "▾" : "▸"}
                  </button>
                  <button
                    type="button"
                    className="sql-table"
                    onClick={() => {
                      const next = `SELECT * FROM ${t} LIMIT 50`;
                      setSql(next);
                      exec(next);
                    }}
                  >
                    {t}
                  </button>
                  <span className="hint">{cols.length}</span>
                </div>
                {open === t ? (
                  <ul>
                    {cols.map(([c, type]) => (
                      <li key={c}>
                        <button type="button" onClick={() => insert(c ?? "")}>
                          {c}
                        </button>
                        <span className="hint mono">{type?.toLowerCase()}</span>
                      </li>
                    ))}
                  </ul>
                ) : null}
              </div>
            ))}
          </nav>
        </Panel>
        <div>
          <Panel
            title="SQL"
            sub="Ctrl+Enter 运行；Tab 补全"
            actions={
              <Button variant="primary" onClick={() => exec(sql)} disabledReason={readOnly}>
                运行
              </Button>
            }
          >
            <textarea
              ref={ref}
              aria-label="SQL"
              spellCheck={false}
              onKeyDown={(e) => {
                if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
                  e.preventDefault();
                  exec(sql);
                } else if (e.key === "Tab" && sugg[0]) {
                  e.preventDefault();
                  insert(sugg[0], true);
                }
              }}
              onSelect={(e) => setCursor(e.currentTarget.selectionStart)}
              className="sql-input"
              rows={6}
              value={sql}
              onChange={(e) => {
                setSql(e.target.value);
                setCursor(e.target.selectionStart);
              }}
            />
            <div className="sql-sugg">
              {sugg.map((c) => (
                <button key={c} type="button" onClick={() => insert(c, true)}>
                  {c}
                </button>
              ))}
            </div>
            {run.error ? (
              <p className="hint" role="alert">
                {run.error.message}
              </p>
            ) : null}
          </Panel>
          {history.length ? (
            <Panel title="历史与收藏" sub="本浏览器保存；★ 收藏不会被挤掉" flush>
              <ul className="sql-history">
                {history.map((h) => (
                  <li key={h.sql}>
                    <button
                      type="button"
                      className={h.starred ? "star on" : "star"}
                      aria-label={h.starred ? "取消收藏" : "收藏"}
                      onClick={() => toggleStar(h.sql)}
                    >
                      ★
                    </button>
                    <button
                      type="button"
                      className="mono"
                      onClick={() => setSql(h.sql)}
                      title="载入"
                    >
                      {h.sql}
                    </button>
                  </li>
                ))}
              </ul>
            </Panel>
          ) : null}
        </div>
      </div>
      {result ? (
        <>
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
                numeric:
                  result.rows.length > 0 &&
                  result.rows.every((r) => r[j] == null || typeof r[j] === "number"),
                value: (x: { r: unknown[] }) => {
                  const v = x.r[j];
                  return v === null || v === undefined
                    ? null
                    : typeof v === "number"
                      ? v
                      : String(v);
                },
              }))}
            />
          </Panel>
          <QuickChart key={result.columns.join(",")} columns={result.columns} rows={result.rows} />
        </>
      ) : null}
    </>
  );
}
