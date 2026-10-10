import { type AlertItem, useAckAlert, useAlerts } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { Button, PageHeader, Panel, Pill, useToast } from "@/ui";
import { QueryView } from "../shared";
import { AlertRules } from "./Rules";

export default function MonitorPage() {
  const query = useAlerts();
  const ack = useAckAlert();
  const toast = useToast();
  const readOnly = useReadOnlyReason();
  // Ack state comes back from Serving (alert_ack projection); after a successful
  // submit the alerts query is refetched, and the row flips once the new generation lands.
  const acknowledge = (row: AlertItem) =>
    ack.mutate(row.alert_id, {
      onSuccess: (r) => toast(`已提交确认（${r.status}），数据刷新后显示`),
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
                    row.acked_at ? (
                      <Pill kind="ok">已确认{row.acked_by ? ` · ${row.acked_by}` : ""}</Pill>
                    ) : (
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => acknowledge(row)}
                        disabledReason={
                          readOnly ??
                          (ack.isPending && ack.variables === row.alert_id ? "提交中" : undefined)
                        }
                      >
                        确认
                      </Button>
                    ),
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      <AlertRules />
    </>
  );
}
