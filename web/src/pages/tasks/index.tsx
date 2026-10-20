import { useNavigate } from "react-router";
import { useTaskLog, useTasks } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import { DataTable } from "@/table/DataTable";
import { AppDrawer, PageHeader, Panel, Pill } from "@/ui";
import { QueryView } from "../shared";

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

/** systemd's "Mon 2026-10-12 17:30:00 CST" → "10-12 周一 17:30" (already Shanghai time). */
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

export default function TasksPage() {
  const query = useTasks();
  const [unit, setUnit] = useUrlState("unit");
  const navigate = useNavigate();
  return (
    <>
      <PageHeader eyebrow="运维" title="任务与调度" />
      <QueryView query={query}>
        {(rows) => (
          <Panel title={`任务 · ${rows.length}`} sub="点一行看日志" flush>
            <DataTable
              label="任务列表"
              emptyText="没有读到 rquant* 服务或定时器：本机未安装这些 systemd 单元，或网页进程无权读取 systemctl。"
              rows={rows}
              rowKey={(r) => r.unit}
              onSelect={(r) => setUnit(r.unit)}
              selectedKey={unit}
              columns={[
                {
                  id: "unit",
                  header: "单元",
                  value: (r) => r.unit,
                  cell: (r) => <span className="mono">{r.unit}</span>,
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
                      {r.active} · {r.sub}
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
                { id: "desc", header: "说明", value: (r) => r.description, secondary: true },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      <AppDrawer
        open={unit !== null}
        onClose={() => navigate(-1)}
        title={unit?.replace(/\.(service|timer)$/, "") ?? ""}
        label={unit ? `${unit} 日志` : "日志"}
      >
        {unit ? <Log unit={unit} /> : null}
      </AppDrawer>
    </>
  );
}
