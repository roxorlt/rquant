import { useNavigate } from "react-router";
import { useDataCenter, useHealth, useTaskLog, useTasks } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import {
  datasetLabel,
  layerTitle,
  publicLines,
  taskDescription,
  taskState,
  unitLabel,
} from "@/copy/publicText";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { AppDrawer, KpiStrip, PageHeader, Panel, Pill, StatusBadge } from "@/ui";
import { QueryView } from "../shared";

/** Large counts get thousands separators; dates and text pass through. */
function formatFresh(value: string | number): string {
  const n = typeof value === "number" ? value : /^\d+$/.test(value) ? Number(value) : null;
  return n === null ? String(value) : n.toLocaleString("zh-CN");
}

function Log({ unit }: { unit: string }) {
  const log = useTaskLog(unit);
  return (
    <QueryView query={log}>
      {(data) =>
        data.error ? (
          <p className="hint">{data.error}</p>
        ) : (
          <pre className="logview">{data.lines.join("\n") || "没有日志"}</pre>
        )
      }
    </QueryView>
  );
}

/** "Mon 2026-10-12 17:30:00 CST" → "10-12 周一 17:30" (already Shanghai time). */
const WEEK: Record<string, string> = {
  Mon: "周一",
  Tue: "周二",
  Wed: "周三",
  Thu: "周四",
  Fri: "周五",
  Sat: "周六",
  Sun: "周日",
};
function shortRun(value: string | null | undefined): string {
  if (!value) return "—";
  const m = /^(\w{3})\s+\d{4}-(\d{2}-\d{2})\s+(\d{2}:\d{2})/.exec(value);
  return m ? `${m[2]} ${WEEK[m[1] ?? ""] ?? ""} ${m[3]}` : value;
}

function TasksSection() {
  const query = useTasks();
  const [unit, setUnit] = useUrlState("unit");
  const navigate = useNavigate();
  return (
    <>
      <QueryView query={query}>
        {(rows) => (
          <Panel title={`定时任务与服务 · ${rows.length}`} sub="点一行看日志" flush>
            <DataTable
              label="任务列表"
              emptyText="没有读到定时任务。这些任务还没装上，或当前环境未启用调度服务。"
              rows={rows}
              rowKey={(r) => r.unit}
              onSelect={(r) => setUnit(r.unit)}
              selectedKey={unit}
              columns={[
                {
                  id: "unit",
                  header: "单元",
                  value: (r) => r.unit,
                  cell: (r) => unitLabel(r.unit),
                },
                {
                  id: "kind",
                  header: "类型",
                  value: (r) => (r.kind === "timer" ? "定时器" : "服务"),
                },
                {
                  id: "state",
                  header: "状态",
                  value: (r) => r.sub,
                  cell: (r) => (
                    <Pill
                      kind={r.active === "active" ? "ok" : r.active === "failed" ? "crit" : "idle"}
                    >
                      {taskState(r.active, r.sub)}
                    </Pill>
                  ),
                },
                {
                  id: "next",
                  header: "下次运行",
                  sortable: true,
                  value: (r) => r.next_run ?? null,
                  cell: (r) => <span className="mono">{shortRun(r.next_run)}</span>,
                },
                {
                  id: "last",
                  header: "上次运行",
                  sortable: true,
                  value: (r) => r.last_run ?? null,
                  cell: (r) => <span className="mono">{shortRun(r.last_run)}</span>,
                },
                {
                  id: "desc",
                  header: "说明",
                  value: (r) => taskDescription(r.description),
                  secondary: true,
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      <AppDrawer
        open={unit !== null}
        onClose={() => navigate(-1)}
        title={unit ? unitLabel(unit) : ""}
        label={unit ? `${unit} 日志` : "日志"}
      >
        {unit ? <Log unit={unit} /> : null}
      </AppDrawer>
    </>
  );
}

function DataCoverageSection() {
  const query = useDataCenter();
  const [picked, setPicked] = useUrlState("dataset");
  const navigate = useNavigate();
  return (
    <QueryView query={query}>
      {(data) => {
        const coverage = new Map((data.audit?.datasets ?? []).map((c) => [c.dataset_id, c]));
        const dataset = data.datasets.find((d) => d.dataset_id === picked);
        return (
          <>
            <Panel
              title={`数据集 · ${data.datasets.length}`}
              sub={
                data.audit
                  ? `覆盖审计 ${formatShanghaiDateTime(data.audit.generated_at)}，窗口 ${data.audit.window_start} ~ ${data.audit.window_end}（${data.audit.open_days} 个交易日）`
                  : "还没有覆盖审计。"
              }
              flush
            >
              <DataTable
                label="数据集"
                rows={data.datasets}
                rowKey={(row) => row.dataset_id}
                onSelect={(row) => setPicked(row.dataset_id)}
                selectedKey={picked}
                columns={[
                  { id: "name", header: "名称", value: (row) => row.name },
                  { id: "table", header: "表", value: (row) => datasetLabel(row.table_name) },
                  { id: "cat", header: "类别", value: (row) => row.category },
                  { id: "src", header: "来源", value: (row) => row.sources.join(" / ") },
                  { id: "upd", header: "更新", value: (row) => row.update_note },
                  {
                    id: "latest",
                    header: "最新日期",
                    value: (row) => coverage.get(row.dataset_id)?.latest_date ?? null,
                  },
                  {
                    id: "missing",
                    header: "缺失交易日",
                    value: (row) => coverage.get(row.dataset_id)?.missing_open_days.length ?? null,
                    cell: (row) => {
                      const c = coverage.get(row.dataset_id);
                      if (!c) return "—";
                      if (c.error) return <Pill kind="warn">读取失败</Pill>;
                      const n = c.missing_open_days.length;
                      return n ? (
                        <Pill kind="warn">{`缺 ${n} 天`}</Pill>
                      ) : (
                        <Pill kind="ok">齐</Pill>
                      );
                    },
                  },
                ]}
              />
            </Panel>
            <AppDrawer
              open={dataset !== undefined}
              onClose={() => navigate(-1)}
              title={dataset?.name ?? ""}
              label={dataset ? `${dataset.name} 字段` : "字段"}
              extra={<span>{dataset ? datasetLabel(dataset.table_name) : ""}</span>}
            >
              {dataset ? (
                <>
                  <p className="hint">{dataset.purpose}</p>
                  <DataTable
                    label="字段"
                    rows={dataset.fields}
                    rowKey={(row) => row.key}
                    columns={[
                      { id: "key", header: "字段", value: (row) => row.key },
                      { id: "name", header: "名称", value: (row) => row.name },
                      { id: "type", header: "类型", value: (row) => row.data_type },
                      { id: "unit", header: "单位", value: (row) => row.unit ?? "" },
                      { id: "desc", header: "说明", value: (row) => row.description },
                    ]}
                  />
                </>
              ) : null}
            </AppDrawer>
            <Panel title="研究库覆盖" sub="行情库各表最近 30 天" flush>
              <DataTable
                label="研究库覆盖"
                rows={(data.audit?.datasets ?? []).filter((c) => !c.error)}
                rowKey={(c) => c.dataset_id}
                columns={[
                  { id: "table", header: "表", value: (c) => datasetLabel(c.table_name) },
                  { id: "latest", header: "最新日期", value: (c) => c.latest_date ?? null },
                  {
                    id: "rows",
                    header: "窗口行数",
                    numeric: true,
                    value: (c) => c.rows_in_window,
                  },
                  {
                    id: "missing",
                    header: "缺失交易日",
                    numeric: true,
                    value: (c) => c.missing_open_days.length,
                  },
                ]}
              />
            </Panel>
          </>
        );
      }}
    </QueryView>
  );
}

export default function HealthPage() {
  const query = useHealth();
  return (
    <>
      <PageHeader eyebrow="运维" title="系统状态" />
      <QueryView query={query}>
        {(data) => (
          <>
            {data.layers?.length ? (
              <Panel title="分层健康" sub="每层取其输入中最差的状态">
                <ul className="health-layers" aria-label="分层健康">
                  {data.layers.map((layer) => (
                    <li key={layer.key}>
                      <StatusBadge
                        state={
                          layer.state === "unknown"
                            ? "warn"
                            : (layer.state as "ok" | "warn" | "crit")
                        }
                        label={`${layerTitle(layer.title)}${layer.state === "unknown" ? "（未知）" : ""}`}
                        reason={publicLines(layer.detail).join("；")}
                      />
                      <span className="sub">{publicLines(layer.detail)[0] ?? "—"}</span>
                    </li>
                  ))}
                </ul>
              </Panel>
            ) : null}
            <Panel title="数据新鲜度" sub="各数据面最新日期与行数">
              <KpiStrip
                compact
                label="数据新鲜度"
                items={data.freshness.map((item) => ({
                  key: item.key,
                  label: item.label,
                  value: item.value == null ? "—" : formatFresh(item.value),
                }))}
              />
            </Panel>
          </>
        )}
      </QueryView>
      <TasksSection />
      <DataCoverageSection />
    </>
  );
}
