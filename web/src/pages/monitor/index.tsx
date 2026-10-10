import { useState } from "react";
import { type AlertItem, useAckAlert, useAlerts } from "@/api/endpoints";
import { formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { Button, PageHeader, Panel, Pill, useToast } from "@/ui";
import { QueryView } from "../shared";

export default function MonitorPage() {
  const query = useAlerts();
  const ack = useAckAlert();
  const toast = useToast();
  // Acks are applied by the page-control service; until Serving republishes we remember
  // which ones this tab already sent.
  const [sent, setSent] = useState<ReadonlySet<string>>(new Set());

  const acknowledge = (row: AlertItem) =>
    ack.mutate(row.alert_id, {
      onSuccess: (r) => {
        setSent((prev) => new Set(prev).add(row.alert_id));
        toast(`已提交确认：${r.status}`);
      },
      onError: (e) => toast(`确认失败：${e.message}`),
    });

  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="告警时间线" />
      <QueryView query={query}>
        {(data) => (
          <Panel title={`${data.items.length} 条`} flush>
            <DataTable
              label="告警时间线"
              rows={data.items}
              rowKey={(row) => row.alert_id}
              height={600}
              emptyText="没有告警"
              columns={[
                {
                  id: "at",
                  header: "时间",
                  value: (row) => row.at ?? null,
                  cell: (row) => (row.at ? formatShanghaiDateTime(row.at) : row.trade_date),
                },
                { id: "code", header: "代码", value: (row) => row.code },
                { id: "name", header: "名称", value: (row) => row.name ?? null },
                { id: "level", header: "级别", value: (row) => row.level },
                { id: "type", header: "类型", value: (row) => row.trigger_type ?? null },
                {
                  id: "price",
                  header: "触发价",
                  numeric: true,
                  value: (row) => row.trigger_price ?? null,
                  cell: (row) => formatPrice(row.trigger_price),
                },
                { id: "pool", header: "池", value: (row) => row.pool ?? null, secondary: true },
                {
                  id: "ack",
                  header: "",
                  sortable: false,
                  value: () => null,
                  cell: (row) =>
                    sent.has(row.alert_id) ? (
                      <Pill kind="ok">已提交</Pill>
                    ) : (
                      <Button size="sm" variant="ghost" onClick={() => acknowledge(row)}>
                        确认
                      </Button>
                    ),
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
    </>
  );
}
