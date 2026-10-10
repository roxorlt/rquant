import { useOperations } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";

const KIND: Record<string, string> = {
  ack_alert: "确认告警",
  add_watchlist_item: "加自选",
  save_alert_rule: "告警规则",
};

export default function OpsPage() {
  const query = useOperations();
  return (
    <>
      <PageHeader eyebrow="运维" title="操作记录" />
      <QueryView query={query}>
        {(data) => (
          <Panel
            title={`${data.items.length} 条`}
            sub="页面写操作经 page control 落盘后的回读"
            flush
          >
            <DataTable
              label="操作记录"
              rows={data.items}
              rowKey={(row) => `${row.kind}:${row.command_id}`}
              emptyText="还没有网页操作"
              height={600}
              columns={[
                {
                  id: "at",
                  header: "时间",
                  value: (row) => row.at ?? null,
                  cell: (row) => (row.at ? formatShanghaiDateTime(row.at) : "—"),
                },
                {
                  id: "kind",
                  header: "类型",
                  value: (row) => row.kind,
                  cell: (row) => KIND[row.kind] ?? row.kind,
                },
                { id: "summary", header: "内容", value: (row) => row.summary },
                { id: "actor", header: "操作人", value: (row) => row.actor ?? "—" },
                { id: "cmd", header: "命令 ID", value: (row) => row.command_id },
              ]}
            />
          </Panel>
        )}
      </QueryView>
    </>
  );
}
