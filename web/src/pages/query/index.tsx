import { useEffect, useMemo, useRef, useState } from "react";
import {
  downloadQueryCsv,
  executeQuery,
  type QueryResult,
  type QueryValue,
  queryJournalKey,
  queryValueText,
  readQueryJournal,
  type SavedResearchQuery,
  type SaveResearchQuery,
  saveQuery,
  useQueryCatalog,
  useSavedQueries,
} from "@/api/researchQuery";
import { useCurrentMeta } from "@/api/useMeta";
import { formatCount, formatNumber } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, EmptyState, PageHeader, Panel, RelativeTime, SkeletonRows, Tabs, Tip } from "@/ui";
import "./query.css";

const DEFAULT_SQL =
  "SELECT ts_code, trade_date, close\nFROM daily_bar\nORDER BY trade_date DESC\nLIMIT 100;";
type ResultRow = { index: number; cells: QueryValue[] };

export default function QueryPage() {
  const meta = useCurrentMeta();
  const viewer = meta.data?.data.viewer;
  const catalog = useQueryCatalog(viewer);
  const saved = useSavedQueries(viewer);
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [name, setName] = useState("");
  const [loaded, setLoaded] = useState<SavedResearchQuery | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [plan, setPlan] = useState<QueryResult | null>(null);
  const [mode, setMode] = useState("query");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [saveMessage, setSaveMessage] = useState("");
  const [saveBusy, setSaveBusy] = useState(false);
  const [pending, setPending] = useState<SaveResearchQuery | null>(null);
  const [journalReady, setJournalReady] = useState(false);
  const sequence = useRef(0);
  const loadSequence = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const currentViewer = useRef(viewer);
  currentViewer.current = viewer;

  useEffect(() => {
    sequence.current += 1;
    loadSequence.current += 1;
    controller.current?.abort();
    setBusy(false);
    setResult(null);
    setPlan(null);
    setSql(DEFAULT_SQL);
    setMessage("");
    setLoaded(null);
    setSaveMessage("");
    setPending(null);
    setName("");
    setSaveBusy(false);
    setJournalReady(false);
    if (typeof viewer === "string") {
      try {
        const original = readQueryJournal(viewer);
        sessionStorage.setItem(`${queryJournalKey(viewer)}.probe`, "1");
        sessionStorage.removeItem(`${queryJournalKey(viewer)}.probe`);
        setPending(original);
        setJournalReady(true);
        if (original) setSaveMessage("保存结果尚未确认，请恢复原命令。");
      } catch {
        setSaveMessage("浏览器无法保存恢复记录，请允许会话存储后重试。");
      }
    }
    return () => {
      sequence.current += 1;
      controller.current?.abort();
    };
  }, [viewer]);

  function changeSql(next: string) {
    sequence.current += 1;
    controller.current?.abort();
    setSql(next);
    setBusy(false);
    setResult(null);
    setPlan(null);
    setMessage("");
  }

  async function run(nextMode: "query" | "explain") {
    if (!catalog.data?.available || !sql.trim() || busy) return;
    const request = ++sequence.current;
    const actor = viewer;
    controller.current?.abort();
    const abort = new AbortController();
    controller.current = abort;
    setBusy(true);
    setMessage("");
    setMode(nextMode);
    if (nextMode === "query") setResult(null);
    else setPlan(null);
    try {
      const data = await executeQuery(sql, nextMode, abort.signal);
      if (request !== sequence.current || currentViewer.current !== actor) return;
      if (nextMode === "query") setResult(data);
      else setPlan(data);
    } catch (error) {
      if (request === sequence.current && currentViewer.current === actor)
        setMessage(error instanceof Error ? error.message : "查询未能执行，请重试。");
    } finally {
      if (request === sequence.current && currentViewer.current === actor) setBusy(false);
    }
  }

  async function persist(action: "save" | "resume" | "retry") {
    if (typeof viewer !== "string" || saveBusy || !journalReady) return;
    const actor = viewer;
    const body =
      action !== "save"
        ? pending
        : {
            kind: "save_research_query" as const,
            command_id: crypto.randomUUID(),
            requested_at: new Date().toISOString(),
            query_id: loaded?.query_id ?? crypto.randomUUID(),
            expected_version: loaded?.version ?? null,
            name: name.trim(),
            sql,
          };
    if (body === null) return;
    const targetSequence = loadSequence.current;
    const sameTarget =
      loaded === null
        ? body.expected_version == null
        : loaded.query_id === body.query_id && loaded.version === body.expected_version;
    try {
      sessionStorage.setItem(queryJournalKey(actor), JSON.stringify(body));
    } catch {
      setJournalReady(false);
      setSaveMessage("浏览器无法保存恢复记录，请允许会话存储后重试。");
      return;
    }
    setPending(body);
    setSaveBusy(true);
    setSaveMessage("");
    try {
      const data = await saveQuery(body, action === "resume");
      if (currentViewer.current !== actor) return;
      const receipt = data.receipt;
      if (
        receipt?.command_id !== body.command_id ||
        receipt == null ||
        !["succeeded", "failed"].includes(receipt.status)
      ) {
        setSaveMessage(
          receipt == null
            ? "原命令尚未找到，可再次提交原命令。"
            : "保存正在处理，请稍后恢复原命令。",
        );
        return;
      }
      const receiptValue =
        receipt.result !== null &&
        typeof receipt.result === "object" &&
        !Array.isArray(receipt.result)
          ? (receipt.result as Record<string, unknown>)
          : null;
      if (
        receipt.status === "succeeded" &&
        (receiptValue?.query_id !== body.query_id ||
          receiptValue.code !== "saved" ||
          receiptValue.version !== (body.expected_version ?? 0) + 1)
      ) {
        setSaveMessage("保存结果尚未确认，请恢复原命令。");
        return;
      }
      sessionStorage.removeItem(queryJournalKey(actor));
      setPending(null);
      if (receipt.status === "succeeded") {
        const stillLoaded = sameTarget && targetSequence === loadSequence.current;
        if (stillLoaded && typeof receiptValue?.version === "number") {
          setLoaded({
            query_id: body.query_id,
            name: body.name,
            sql: body.sql,
            version: receiptValue.version,
            updated_at: receipt.completed_at ?? body.requested_at,
          });
        }
        setSaveMessage(stillLoaded ? "已保存" : "此前查询已保存");
        saved.refetch();
      } else {
        setSaveMessage(
          receipt.error === "version_conflict"
            ? "查询已有新版本，请重新载入后保存。"
            : receipt.error === "capacity_exceeded"
              ? "已保存 100 条查询，请修改已有查询。"
              : "未能保存，请检查内容后重试。",
        );
      }
    } catch (error) {
      if (currentViewer.current === actor)
        setSaveMessage(error instanceof Error ? error.message : "保存结果尚未确认，请恢复原命令。");
    } finally {
      if (currentViewer.current === actor) setSaveBusy(false);
    }
  }

  function load(item: SavedResearchQuery) {
    loadSequence.current += 1;
    changeSql(item.sql);
    setName(item.name);
    setLoaded(item);
    setSaveMessage("");
  }

  const shown = mode === "query" ? result : plan;
  const rows = useMemo<ResultRow[]>(
    () => (shown?.rows ?? []).map((cells, index) => ({ index, cells })),
    [shown],
  );
  const columns = useMemo<DataColumn<ResultRow>[]>(
    () =>
      (shown?.columns ?? []).map((column, index) => ({
        id: `column-${index}`,
        header: column.name,
        value: (row) => queryValueText(row.cells[index]),
        wrap: true,
        cell: (row) => <span className="mono query-cell">{queryValueText(row.cells[index])}</span>,
      })),
    [shown],
  );
  const sqlTooLong = new TextEncoder().encode(sql).length > 32 * 1024;
  const unavailable = catalog.error?.message ?? catalog.data?.message ?? "";
  const runReason = busy
    ? "正在查询。"
    : !catalog.data?.available
      ? "查询数据暂不可用。"
      : sqlTooLong
        ? "SQL 超过 32 KiB，请删减。"
        : !sql.trim()
          ? "请填写 SQL。"
          : undefined;
  const saveReason = !catalog.data?.save_enabled
    ? "查询保存尚未启用。"
    : !journalReady
      ? "恢复记录暂不可用。"
      : pending
        ? "先恢复上次保存。"
        : saveBusy
          ? "正在保存。"
          : !name.trim()
            ? "请填写查询名称。"
            : sqlTooLong
              ? "SQL 超过 32 KiB，请删减。"
              : !sql.trim()
                ? "请填写 SQL。"
                : undefined;

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="查询"
        actions={
          <Button
            onClick={() => {
              catalog.refetch();
              saved.refetch();
            }}
            disabled={catalog.isFetching}
          >
            刷新
          </Button>
        }
      />
      <div className="query-layout">
        <aside className="query-reference">
          <Panel
            title="可用数据"
            actions={
              catalog.data?.source_at ? (
                <Tip content="原行情数据的核验时刻">
                  <span className="sub">
                    数据 <RelativeTime at={catalog.data.source_at} />
                  </span>
                </Tip>
              ) : null
            }
          >
            {catalog.isLoading ? (
              <SkeletonRows rows={4} />
            ) : unavailable ? (
              <p role="status">{unavailable}</p>
            ) : null}
            {(catalog.data?.tables ?? []).map((table) => (
              <details key={table.name} className="query-table-info">
                <summary>
                  {table.label} <span className="mono">{table.name}</span>
                </summary>
                <p className="sub">
                  {formatCount(table.row_count)} 行
                  {table.earliest_date ? ` · ${table.earliest_date} 至 ${table.latest_date}` : ""}
                </p>
                <ul>
                  {table.columns.map((column) => (
                    <li key={column.name}>
                      <Tip content={`${column.description} · ${column.data_type}`}>
                        <code>{column.name}</code>
                      </Tip>
                    </li>
                  ))}
                </ul>
                <Button
                  size="sm"
                  onClick={() => {
                    loadSequence.current += 1;
                    setLoaded(null);
                    changeSql(`SELECT * FROM ${table.name}\nLIMIT 100;`);
                  }}
                >
                  使用此表
                </Button>
              </details>
            ))}
          </Panel>
          <Panel title="我的查询">
            {saved.isLoading ? (
              <SkeletonRows rows={3} />
            ) : saved.error ? (
              <p role="status">{saved.error.message}</p>
            ) : !saved.data?.available ? (
              <p className="sub">{saved.data?.message ?? "保存尚未启用。"}</p>
            ) : saved.data.items.length === 0 ? (
              <EmptyState title="还没有保存查询" hint="写好 SQL 后命名保存。" />
            ) : (
              <ul className="query-saved-list">
                {saved.data.items.map((item) => (
                  <li key={item.query_id}>
                    <Button variant="ghost" onClick={() => load(item)}>
                      {item.name}
                    </Button>
                    <RelativeTime at={item.updated_at} />
                  </li>
                ))}
              </ul>
            )}
          </Panel>
        </aside>
        <div className="query-workspace">
          <Panel
            title="SQL"
            actions={
              <Tip
                interactive
                content="只接受一条 SELECT 或 WITH。最多 30 秒、10,000 行、16 MiB。大查询请先筛选日期或股票。Ctrl / ⌘ + Enter 运行。"
              >
                <Button variant="ghost" size="sm" className="query-help">
                  只读查询
                </Button>
              </Tip>
            }
          >
            <label className="field">
              <span className="sr-only">SQL 查询</span>
              <textarea
                className="query-editor mono"
                aria-label="SQL 查询"
                value={sql}
                spellCheck={false}
                onChange={(event) => changeSql(event.target.value)}
                onKeyDown={(event) => {
                  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
                    event.preventDefault();
                    if (!runReason) void run("query");
                  }
                }}
              />
            </label>
            <div className="query-controls">
              <Button
                variant="primary"
                disabledReason={runReason}
                onClick={() => void run("query")}
              >
                运行
              </Button>
              <Button disabledReason={runReason} onClick={() => void run("explain")}>
                查看计划
              </Button>
              <span className="sub" role="status">
                {busy ? "正在查询…" : message}
              </span>
            </div>
            <div className="query-save">
              <label className="field">
                <span className="lbl">查询名称</span>
                <input
                  className="inp"
                  aria-label="查询名称"
                  maxLength={60}
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  placeholder="如：最新收盘价"
                />
              </label>
              <Button disabledReason={saveReason} onClick={() => void persist("save")}>
                保存
              </Button>
              {loaded ? (
                <Button
                  variant="ghost"
                  onClick={() => {
                    loadSequence.current += 1;
                    setLoaded(null);
                    setName("");
                  }}
                >
                  另存为
                </Button>
              ) : null}
            </div>
            <div className="query-save-feedback" role="status">
              {saveMessage}
              {pending ? (
                <Button size="sm" disabled={saveBusy} onClick={() => void persist("resume")}>
                  恢复保存
                </Button>
              ) : null}
              {pending && saveMessage.includes("尚未找到") ? (
                <Button size="sm" disabled={saveBusy} onClick={() => void persist("retry")}>
                  重试原保存
                </Button>
              ) : null}
            </div>
          </Panel>
          <Panel
            title="查询结果"
            flush
            actions={
              <Button
                size="sm"
                disabledReason={
                  !result || !["ready", "partial"].includes(result.status) || busy
                    ? "运行成功后可导出当前结果。"
                    : undefined
                }
                onClick={() => {
                  if (result) downloadQueryCsv(result);
                }}
              >
                导出 CSV
              </Button>
            }
          >
            <Tabs
              activeKey={mode}
              onChange={setMode}
              items={[
                { key: "query", label: "结果" },
                { key: "explain", label: "执行计划" },
              ]}
            />
            {busy ? (
              <div className="query-result-state">
                <SkeletonRows rows={5} />
              </div>
            ) : !shown ? (
              <EmptyState
                title={mode === "query" ? "尚无查询结果" : "尚无执行计划"}
                hint={mode === "query" ? "填写 SQL 后运行。" : "点击「查看计划」。"}
              />
            ) : !["ready", "partial"].includes(shown.status) ? (
              <EmptyState
                title={shown.status === "timeout" ? "查询已停止" : "查询未完成"}
                hint={shown.message}
              />
            ) : (
              <>
                <div className="query-result-meta" role="status">
                  <span>
                    {formatCount(shown.rows.length)} 行 · {formatNumber(shown.elapsed_ms / 1000, 2)}{" "}
                    秒
                  </span>
                  {shown.source_at ? (
                    <span>
                      数据 <RelativeTime at={shown.source_at} />
                    </span>
                  ) : null}
                  {shown.status === "partial" ? <span>{shown.message}</span> : null}
                </div>
                {mode === "explain" ? (
                  <pre className="query-plan mono">
                    {shown.rows.map((row) => row.map(queryValueText).join("\n")).join("\n")}
                  </pre>
                ) : (
                  <DataTable
                    label="查询结果"
                    rows={rows}
                    columns={columns}
                    rowKey={(row) => String(row.index)}
                    height={400}
                    emptyText="没有符合条件的数据，请调整 SQL。"
                  />
                )}
              </>
            )}
          </Panel>
        </div>
      </div>
    </>
  );
}
