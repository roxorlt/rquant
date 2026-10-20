import { useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { type AlertItem, useAckAlert, useAlerts, useUnackAlert } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { Button, FailureNotice, PageHeader, Panel, Pill, ReadOnlyBanner, useToast } from "@/ui";
import { failureCopy, type FailureCopy } from "@/ui/failure";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import { AlertRules } from "./Rules";
import { SurgePanel } from "./Surge";

interface RowFailure {
  id: string;
  copy: FailureCopy;
  attempt: number;
}

export default function MonitorPage() {
  const query = useAlerts();
  const ack = useAckAlert();
  const unack = useUnackAlert();
  const toast = useToast();
  const readOnly = useReadOnlyReason();
  const client = useQueryClient();
  const inflight = useRef(new Set<string>());
  const [busyId, setBusyId] = useState<string | null>(null);
  const [show, setShow] = useState<"all" | "open">("all");
  const [fail, setFail] = useState<RowFailure | null>(null);

  const patchAck = (alertId: string, acked: boolean) => {
    client.setQueryData(
      ["alerts"],
      (old: { data?: { items?: AlertItem[] } } | undefined) => {
        const items = old?.data?.items;
        if (!items) return old;
        return {
          ...old,
          data: {
            ...old.data,
            items: items.map((item) =>
              item.alert_id === alertId
                ? {
                    ...item,
                    acked_at: acked ? new Date().toISOString() : null,
                    acked_by: acked ? "我" : null,
                  }
                : item,
            ),
          },
        };
      },
    );
  };

  const acknowledge = async (row: AlertItem) => {
    if (inflight.current.has(row.alert_id) || row.acked_at) return;
    inflight.current.add(row.alert_id);
    setBusyId(row.alert_id);
    const previous = client.getQueryData(["alerts"]);
    patchAck(row.alert_id, true);
    try {
      await ack.mutateAsync(row.alert_id);
      await client.invalidateQueries({ queryKey: ["alerts"] });
      setFail(null);
      toast({
        text: "已确认 1 条告警",
        duration: 6000,
        action: { label: "撤销", onClick: () => void undo(row) },
      });
    } catch (error) {
      client.setQueryData(["alerts"], previous);
      const attempt = fail?.id === row.alert_id ? fail.attempt + 1 : 1;
      const who = `${row.code} ${row.name ?? ""}`.trim();
      setFail({
        id: row.alert_id,
        attempt,
        copy: failureCopy(`告警『${who}』`, "确认", error, attempt),
      });
    } finally {
      inflight.current.delete(row.alert_id);
      setBusyId(null);
    }
  };

  const undo = async (row: AlertItem) => {
    if (inflight.current.has(row.alert_id)) return;
    inflight.current.add(row.alert_id);
    setBusyId(row.alert_id);
    try {
      await unack.mutateAsync(row.alert_id);
      await client.invalidateQueries({ queryKey: ["alerts"] });
      patchAck(row.alert_id, false);
      toast({ text: "已撤销确认", duration: 4000 });
    } catch (error) {
      const who = `${row.code} ${row.name ?? ""}`.trim();
      setFail({ id: row.alert_id, attempt: 1, copy: failureCopy(`告警『${who}』`, "撤销", error) });
    } finally {
      inflight.current.delete(row.alert_id);
      setBusyId(null);
    }
  };

  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="盯盘与告警" />
      <ReadOnlyBanner />
      <SurgePanel />
      <QueryView query={query}>
        {(data) => {
          const open = data.items.filter((r) => !r.acked_at).length;
          const rows = show === "open" ? data.items.filter((r) => !r.acked_at) : data.items;
          const failed = rows.find((row) => row.alert_id === fail?.id);
          return (
            <Panel
              title={`告警时间线 · 未确认 ${open} / 共 ${data.items.length}`}
              sub="最新在上"
              actions={
                <div className="seg" role="group" aria-label="显示">
                  <button type="button" aria-pressed={show === "all"} onClick={() => setShow("all")}>
                    全部
                  </button>
                  <button type="button" aria-pressed={show === "open"} onClick={() => setShow("open")}>
                    未确认 {open}
                  </button>
                </div>
              }
              flush
            >
              {fail ? (
                <FailureNotice
                  copy={fail.copy}
                  busy={busyId === fail.id}
                  onRetry={failed ? () => void acknowledge(failed) : undefined}
                />
              ) : null}
              <DataTable
                label="告警时间线"
                tools="告警"
                rows={rows}
                initialSort={{ id: "at", desc: true }}
                rowKey={(row) => row.alert_id}
                height={600}
                emptyText={
                  show === "open" ? "告警都已确认。" : "今天还没有告警。规则在下方调整。"
                }
                columns={[
                  {
                    id: "at",
                    header: "时间",
                    sortable: true,
                    value: (row) => row.at ?? null,
                    cell: (row) => (row.at ? formatShanghaiDateTime(row.at) : row.trade_date),
                  },
                  {
                    id: "code",
                    header: "代码",
                    sortable: true,
                    value: (row) => row.code,
                    cell: (row) => <StockLink code={row.code} />,
                  },
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
                    header: "状态",
                    sortable: false,
                    value: () => null,
                    cell: (row) =>
                      row.acked_at ? (
                        <span className="st" tabIndex={-1}>
                          已确认{row.acked_by ? ` · ${row.acked_by}` : ""}
                        </span>
                      ) : (
                        <span className="ack-cell">
                          {fail?.id === row.alert_id ? <span className="row-err">没确认上</span> : null}
                          <Button
                            size="sm"
                            variant="ghost"
                            aria-busy={busyId === row.alert_id || undefined}
                            aria-describedby={readOnly ? "readonly-notice" : undefined}
                            disabled={!!readOnly || busyId === row.alert_id}
                            onClick={() => void acknowledge(row)}
                          >
                            {fail?.id === row.alert_id ? "重试" : "确认"}
                          </Button>
                        </span>
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
