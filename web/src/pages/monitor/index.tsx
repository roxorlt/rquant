import { useEffect } from "react";
import { type AlertItem, useAlerts } from "@/api/endpoints";
import { AlertPoolTip } from "@/copy/AlertPoolTip";
import { ALERT_POOL_TIP_WIDTH, alertPoolLabel, triggerLabel } from "@/copy/publicText";
import { formatPrice } from "@/format/number";
import { formatListClock, formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { SortMenu } from "@/table/SortMenu";
import { readPreference, usePreference, writePreference } from "@/theme/storage";
import { PageHeader, Panel, Pill } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { PHONE_QUERY, useMedia } from "../panorama/useMedia";
import { QueryView } from "../shared";
import { AlertRules } from "./Rules";
import { SurgePanel } from "./Surge";

export default function MonitorPage() {
  const query = useAlerts();
  const [sort, setSort] = usePreference(
    "monitor.sort",
    "at:desc",
    (v: unknown): v is string => typeof v === "string",
  );
  const phone = useMedia(PHONE_QUERY);

  useEffect(() => {
    const items = query.data?.items ?? [];
    let latest = "";
    for (const item of items) {
      if (item.at && item.at > latest) latest = item.at;
    }
    if (!latest) return;
    const prev = readPreference("alerts.seenAt") ?? "";
    if (latest > prev) writePreference("alerts.seenAt", latest);
  }, [query.data]);

  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="盯盘与告警" />
      <QueryView query={query}>
        {(data) => {
          const rows = data.items;
          return (
            <Panel
              title={phone ? "告警时间线" : `告警时间线 · 共 ${rows.length}`}
              sub="最新在上"
              actions={
                phone ? (
                  <SortMenu
                    value={sort}
                    onChange={setSort}
                    options={[
                      { value: "at:desc", label: "时间降序" },
                      { value: "at:asc", label: "时间升序" },
                      { value: "code:desc", label: "代码降序" },
                      { value: "code:asc", label: "代码升序" },
                      { value: "level:desc", label: "级别降序" },
                      { value: "level:asc", label: "级别升序" },
                    ]}
                  />
                ) : undefined
              }
              flush
            >
              {phone ? (
                <AlertList
                  rows={sortedAlerts(rows, sort)}
                  emptyText="今天还没有告警。规则在下方调整。"
                />
              ) : (
                <DataTable
                  label="告警时间线"
                  tools="告警"
                  rows={rows}
                  initialSort={{ id: "at", desc: true }}
                  rowKey={(row) => row.alert_id}
                  height={600}
                  emptyText="今天还没有告警。规则在下方调整。"
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
                      cell: (row) => <Pill kind="warn">{row.level}</Pill>,
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
                  ]}
                />
              )}
            </Panel>
          );
        }}
      </QueryView>
      <SurgePanel />
      <AlertRules />
    </>
  );
}

function sortedAlerts(rows: readonly AlertItem[], sort: string): AlertItem[] {
  const [id, dir] = sort.split(":");
  const sign = dir === "asc" ? 1 : -1;
  const value = (row: AlertItem) =>
    id === "code" ? row.code : id === "level" ? row.level : (row.at ?? "");
  return [...rows].sort((a, b) => value(a).localeCompare(value(b), "zh-CN") * sign);
}

function AlertMeta({ row }: { row: AlertItem }) {
  const clock = row.at ? formatListClock(row.at) : row.trade_date.slice(5);
  // The separator sits at the end of the fixed part. The space after it is a flex gap,
  // because a trailing space is dropped under nowrap + ellipsis.
  const prefix = `${clock} · ${formatPrice(row.trigger_price)} · ${row.level} · ${triggerLabel(row.trigger_type)} ·`;
  return (
    <span className="meta alert-meta">
      <span className="alert-meta-keep">{prefix}</span>
      <span className="alert-meta-pool">{alertPoolLabel(row.pool)}</span>
    </span>
  );
}

function AlertList({ rows, emptyText }: { rows: readonly AlertItem[]; emptyText: string }) {
  if (!rows.length) {
    return (
      <p className="hint" style={{ padding: "12px 16px" }}>
        {emptyText}
      </p>
    );
  }
  return (
    <ul className="li2-list" aria-label="告警时间线">
      {rows.map((row) => (
        <li key={row.alert_id} className="li2 alert-row">
          <span className="t">
            <span className="name">{row.name ?? "—"}</span>
            <span className="code">
              <StockLink code={row.code} />
            </span>
          </span>
          <AlertMeta row={row} />
        </li>
      ))}
    </ul>
  );
}
