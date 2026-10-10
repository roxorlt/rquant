import { useState } from "react";
import { useTaskLog, useTasks } from "@/api/endpoints";
import { DataTable } from "@/table/DataTable";
import { PageHeader, Panel, Pill } from "@/ui";
import { QueryView } from "../shared";

function Log({ unit }: { unit: string }) {
  const log = useTaskLog(unit);
  return (
    <Panel title={`日志 · ${unit}`} sub="journalctl 最近 200 行">
      <QueryView query={log}>
        {(data) =>
          data.error ? (
            <p className="hint">{data.error}</p>
          ) : (
            <pre className="log-view">{data.lines.join("\n") || "（没有日志）"}</pre>
          )
        }
      </QueryView>
    </Panel>
  );
}

export default function TasksPage() {
  const query = useTasks();
  const [unit, setUnit] = useState<string | null>(null);
  return (
    <>
      <PageHeader
        eyebrow="运维"
        title="任务与调度"
        note="只读：systemd 中 rquant* 服务与定时器；立即运行需要服务器授权，暂不提供"
      />
      <QueryView query={query}>
        {(rows) => (
          <Panel title={`任务 · ${rows.length}`} sub="点一行看日志" flush>
            <DataTable
              label="任务列表"
              rows={rows}
              rowKey={(r) => r.unit}
              onSelect={(r) => setUnit(r.unit)}
              selectedKey={unit}
              columns={[
                { id: "unit", header: "单元", value: (r) => r.unit },
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
                { id: "next", header: "下次运行", value: (r) => r.next_run ?? null },
                { id: "last", header: "上次运行", value: (r) => r.last_run ?? null },
                { id: "desc", header: "说明", value: (r) => r.description, secondary: true },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      {unit ? <Log unit={unit} /> : null}
    </>
  );
}
