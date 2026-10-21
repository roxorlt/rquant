import { useRef, useState } from "react";
import {
  type AlertItem,
  useAckAlert,
  useAlerts,
  useAlertsCache,
  useUnackAlert,
} from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { AlertPoolTip } from "@/copy/AlertPoolTip";
import { ALERT_POOL_TIP_WIDTH, alertPoolLabel, triggerLabel } from "@/copy/publicText";
import { alertUnackEnabled } from "@/features";
import { formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import {
  Button,
  FailureNotice,
  PageHeader,
  Panel,
  Pill,
  ReadOnlyBanner,
  Segmented,
  useToast,
} from "@/ui";
import { type FailureCopy, failureCopy } from "@/ui/failure";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import { AlertRules } from "./Rules";
import { SurgePanel } from "./Surge";

interface RowFailure {
  ids: string[];
  copy: FailureCopy;
  attempt: number;
}

export default function MonitorPage() {
  const query = useAlerts();
  const ack = useAckAlert();
  const unack = useUnackAlert();
  const toast = useToast();
  const readOnly = useReadOnlyReason();
  const alertsCache = useAlertsCache();
  const inflight = useRef(new Set<string>());
  const [busyId, setBusyId] = useState<string | null>(null);
  const [show, setShow] = useState<"all" | "open">("all");
  const [fail, setFail] = useState<RowFailure | null>(null);

  const acknowledge = async (row: AlertItem) => {
    if (readOnly || inflight.current.has(row.alert_id) || row.acked_at) return;
    inflight.current.add(row.alert_id);
    setBusyId(row.alert_id);
    const previous = alertsCache.snapshot();
    alertsCache.patchAck(row.alert_id, true);
    try {
      await ack.mutateAsync(row.alert_id);
      await alertsCache.refresh();
      setFail((prev) => {
        if (!prev) return null;
        const ids = prev.ids.filter((id) => id !== row.alert_id);
        return ids.length ? { ...prev, ids } : null;
      });
      toast({
        text: "已确认 1 条告警",
        duration: 6000,
        ...(alertUnackEnabled()
          ? {
              action: { label: "撤销", onClick: () => void undo(row) },
              returnFocus: () =>
                document.querySelector<HTMLElement>(`[data-alert-focus="${row.alert_id}"]`),
            }
          : {}),
      });
    } catch (error) {
      alertsCache.restore(previous);
      const attempt = fail?.ids.includes(row.alert_id) ? fail.attempt + 1 : 1;
      const who = `${row.name ?? row.code}（${row.code}）`;
      const one = failureCopy(`告警『${who}』`, "确认", error, attempt);
      setFail((prev) => {
        const ids = [...new Set([...(prev?.ids ?? []), row.alert_id])];
        if (ids.length < 2) return { ids, attempt, copy: one };
        const rest = one.text.split("：").slice(1).join("：");
        return {
          ids,
          attempt,
          copy: {
            text: `${ids.length} 条告警没确认上：${rest}`,
            retry: one.retry,
            health: one.health,
          },
        };
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
      await alertsCache.refresh();
      alertsCache.patchAck(row.alert_id, false);
      toast({ text: "已撤销确认", duration: 4000 });
    } catch (error) {
      const who = `${row.name ?? row.code}（${row.code}）`;
      setFail({
        ids: [row.alert_id],
        attempt: 1,
        copy: failureCopy(`告警『${who}』`, "撤销", error),
      });
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
          const failed = rows.find((row) => fail?.ids.includes(row.alert_id));
          return (
            <Panel
              title={`告警时间线 · 未确认 ${open} / 共 ${data.items.length}`}
              sub="最新在上"
              actions={
                <Segmented
                  label="显示"
                  value={show}
                  onChange={setShow}
                  options={[
                    { value: "all", label: "全部" },
                    { value: "open", label: `未确认 ${open}` },
                  ]}
                />
              }
              flush
            >
              {fail ? (
                <FailureNotice
                  pin
                  copy={fail.copy}
                  busy={busyId != null && fail.ids.includes(busyId)}
                  onRetry={
                    failed
                      ? () => {
                          for (const id of fail?.ids ?? []) {
                            const row = rows.find((item) => item.alert_id === id);
                            if (row) void acknowledge(row);
                          }
                        }
                      : undefined
                  }
                />
              ) : null}
              <DataTable
                label="告警时间线"
                tools="告警"
                rows={rows}
                initialSort={{ id: "at", desc: true }}
                rowKey={(row) => row.alert_id}
                height={600}
                emptyText={show === "open" ? "告警都已确认。" : "今天还没有告警。规则在下方调整。"}
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
                  {
                    id: "type",
                    header: "类型",
                    phoneLine: 2,
                    value: (row) => row.trigger_type ?? null,
                    cell: (row) => triggerLabel(row.trigger_type),
                  },
                  {
                    id: "price",
                    header: "触发价",
                    numeric: true,
                    value: (row) => row.trigger_price ?? null,
                    cell: (row) => formatPrice(row.trigger_price),
                  },
                  {
                    id: "pool",
                    header: "范围",
                    headerTip: <AlertPoolTip />,
                    headerTipWidth: ALERT_POOL_TIP_WIDTH,
                    phoneLine: 2,
                    value: (row) => row.pool ?? null,
                    cell: (row) => alertPoolLabel(row.pool),
                  },
                  {
                    id: "ack",
                    header: "状态",
                    sortable: false,
                    value: () => null,
                    cell: (row) =>
                      row.acked_at ? (
                        <span className="st" tabIndex={-1} data-alert-focus={row.alert_id}>
                          已确认
                        </span>
                      ) : (
                        <span className="ack-cell">
                          <span className="st warn" data-alert-focus={row.alert_id} tabIndex={-1}>
                            未确认
                          </span>
                          {fail?.ids.includes(row.alert_id) ? (
                            <span className="row-err">没确认上</span>
                          ) : null}
                          <Button
                            size="sm"
                            variant="ghost"
                            aria-busy={busyId === row.alert_id || undefined}
                            aria-describedby={readOnly ? "readonly-notice" : undefined}
                            ariaDisabled={!!readOnly}
                            disabled={busyId === row.alert_id}
                            onClick={() => void acknowledge(row)}
                          >
                            {fail?.ids.includes(row.alert_id) ? "重试" : "确认"}
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
