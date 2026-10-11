import { type SurgeEvent, useSurge } from "@/api/endpoints";
import { formatAmount, formatNumber, formatSignedPercent } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Panel, Pill } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";

/** 量能异动: events from the snapshot collector's surge scanner (today, newest first). */
export function SurgePanel() {
  const query = useSurge();
  return (
    <QueryView query={query}>
      {(data) => (
        <Panel
          title={`量能异动 · ${data.date ?? "暂无"} · ${data.items.length} 条`}
          sub="量比 = 今日累计量 / 昨日同一时刻累计量；≥3 且成交额 ≥3000万，或 5 分钟量加速 ≥5；剔除 ST 与封板；同股 15 分钟冷却。每 15 秒刷新"
          flush
        >
          <DataTable<SurgeEvent>
            label="量能异动"
            tools="异动"
            rows={data.items}
            initialSort={{ id: "time", desc: true }}
            rowKey={(row) => `${row.code}-${row.kind}-${row.time}`}
            height={420}
            emptyText="今天还没有异动事件（盘中 09:35 起由快照采集器每 ~3 秒扫描）。"
            columns={[
              { id: "time", header: "时间", sortable: true, value: (row) => row.time },
              {
                id: "code",
                header: "代码",
                sortable: true,
                value: (row) => row.code,
                cell: (row) => <StockLink code={row.code} />,
              },
              { id: "name", header: "名称", value: (row) => row.name ?? null },
              {
                id: "kind",
                header: "类型",
                value: (row) => row.kind,
                cell: (row) => (
                  <Pill kind={row.kind === "ratio" ? "warn" : "idle"}>
                    {row.kind === "ratio" ? "量比" : "加速"}
                  </Pill>
                ),
              },
              {
                id: "ratio",
                header: "量比",
                numeric: true,
                sortable: true,
                value: (row) => row.ratio ?? null,
                cell: (row) => formatNumber(row.ratio, 1),
              },
              {
                id: "acc5",
                header: "5分加速",
                numeric: true,
                sortable: true,
                value: (row) => row.acc5 ?? null,
                cell: (row) => formatNumber(row.acc5, 1),
              },
              {
                id: "pct",
                header: "涨跌幅",
                numeric: true,
                sortable: true,
                value: (row) => row.pct ?? null,
                cell: (row) => formatSignedPercent(row.pct == null ? null : row.pct * 100),
              },
              {
                id: "amount",
                header: "成交额",
                numeric: true,
                sortable: true,
                value: (row) => row.amount ?? null,
                cell: (row) => formatAmount(row.amount),
              },
            ]}
          />
        </Panel>
      )}
    </QueryView>
  );
}
