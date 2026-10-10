import { useHealth } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { PageHeader, Panel, StatusBadge } from "@/ui";
import { QueryView } from "../shared";

export default function HealthPage() {
  const query = useHealth();
  return (
    <>
      <PageHeader eyebrow="运维" title="系统健康" />
      <QueryView query={query}>
        {(data) => (
          <>
            <Panel title="服务" flush>
              <DataTable
                label="服务"
                rows={data.services}
                rowKey={(row) => row.service_id}
                columns={[
                  { id: "service", header: "服务", value: (row) => row.service_id },
                  { id: "plane", header: "平面", value: (row) => row.plane },
                  {
                    id: "status",
                    header: "状态",
                    value: (row) => row.status,
                    cell: (row) => {
                      const bad = row.stale || ["failed", "error"].includes(row.status);
                      return (
                        <StatusBadge
                          state={bad ? "crit" : "ok"}
                          label={row.stale ? `${row.status}（过期）` : row.status}
                          reason={row.last_error}
                        />
                      );
                    },
                  },
                  {
                    id: "heartbeat",
                    header: "心跳",
                    value: (row) => row.heartbeat_at ?? null,
                    cell: (row) =>
                      row.heartbeat_at ? formatShanghaiDateTime(row.heartbeat_at) : "—",
                  },
                  {
                    id: "backlog",
                    header: "积压",
                    numeric: true,
                    value: (row) => row.backlog_count,
                  },
                  {
                    id: "failures",
                    header: "连续失败",
                    numeric: true,
                    value: (row) => row.consecutive_failures,
                  },
                ]}
              />
            </Panel>
            <Panel title="数据新鲜度">
              <dl className="kv">
                {data.freshness.map((item) => (
                  <div key={item.key} style={{ display: "contents" }}>
                    <dt>{item.label}</dt>
                    <dd className="num">{item.value ?? "—"}</dd>
                  </div>
                ))}
              </dl>
            </Panel>
          </>
        )}
      </QueryView>
    </>
  );
}
