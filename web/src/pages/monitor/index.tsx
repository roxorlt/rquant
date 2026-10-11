import { useState } from "react";
import { type AlertItem, useAckAlert, useAlerts } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { Button, PageHeader, Panel, Pill, Segmented, useToast } from "@/ui";
import { QueryView } from "../shared";
import { AlertRules } from "./Rules";

export default function MonitorPage() {
  const query = useAlerts();
  const ack = useAckAlert();
  const toast = useToast();
  const readOnly = useReadOnlyReason();
  const [show, setShow] = useState<"all" | "open">("all");
  // Ack state comes back from Serving (alert_ack projection); after a successful
  // submit the alerts query is refetched, and the row flips once the new generation lands.
  const acknowledge = (row: AlertItem) =>
    ack.mutate(row.alert_id, {
      onSuccess: (r) => toast(`已提交确认（${r.status}），数据刷新后显示`),
      onError: (e) => toast(`确认失败：${e.message}`),
    });

  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="盯盘与告警" />
      <QueryView query={query}>
        {(data) => {
          const open = data.items.filter((r) => !r.acked_at).length;
          const rows = show === "open" ? data.items.filter((r) => !r.acked_at) : data.items;
          return (
            <Panel
              title={`告警时间线 · 未确认 ${open} / 共 ${data.items.length}`}
              sub="最新在上；确认后数据刷新才会变为已确认"
              actions={
                <Segmented
                  label="显示"
                  value={show}
                  onChange={(v) => setShow(v as "all" | "open")}
                  options={[
                    { value: "all", label: "全部" },
                    { value: "open", label: `未确认 ${open}` },
                  ]}
                />
              }
              flush
            >
              <DataTable
                label="告警时间线"
                rows={rows}
                initialSort={{ id: "at", desc: true }}
                rowKey={(row) => row.alert_id}
                height={600}
                emptyText={
                  show === "open"
                    ? "告警都已确认。"
                    : "今天还没有告警：盯盘服务未触发任何规则。规则在下方「告警规则」调整。"
                }
                columns={[
                  {
                    id: "at",
                    header: "时间",
                    sortable: true,
                    value: (row) => row.at ?? null,
                    cell: (row) => (row.at ? formatShanghaiDateTime(row.at) : row.trade_date),
                  },
                  { id: "code", header: "代码", sortable: true, value: (row) => row.code },
                  { id: "name", header: "名称", value: (row) => row.name ?? null },
                  {
                    id: "level",
                    header: "级别",
                    sortable: true,
                    value: (row) => row.level,
                    cell: (row) => <Pill kind={row.acked_at ? "idle" : "warn"}>{row.level}</Pill>,
                  },
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
          );
        }}
      </QueryView>
      <AlertRules />
    </>
  );
}
