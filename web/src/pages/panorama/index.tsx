import { useState } from "react";
import { usePanorama } from "@/api/endpoints";
import { formatAmount, formatCount } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { ChangeText, KpiStrip, PageHeader, Panel, Segmented } from "@/ui";
import { QueryView } from "../shared";
import { Heatmap } from "./Heatmap";

export default function PanoramaPage() {
  const query = usePanorama();
  const asOf = query.data?.as_of;
  const [system, setSystem] = useState<string | null>(null);
  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="市场全景"
        note={asOf ? `快照 ${formatShanghaiDateTime(asOf)}` : undefined}
      />
      <QueryView query={query}>
        {(data) => {
          const systems = [...new Set(data.boards.map((b) => b.system))];
          return (
            <>
              <KpiStrip
                label="市场脉搏"
                items={[
                  { key: "up", label: "上涨", value: formatCount(data.pulse.up) },
                  { key: "down", label: "下跌", value: formatCount(data.pulse.down) },
                  { key: "flat", label: "平盘", value: formatCount(data.pulse.flat) },
                  { key: "lu", label: "涨停", value: formatCount(data.pulse.limit_up) },
                  { key: "ld", label: "跌停", value: formatCount(data.pulse.limit_down) },
                ]}
              />
              <Panel
                title="板块热力图"
                sub="面积 = 成交额，颜色 = 成分股涨跌幅中位数（±3% 封顶）；取成交额前 80"
                actions={
                  systems.length > 1 ? (
                    <Segmented
                      label="板块体系"
                      value={system ?? systems[0] ?? ""}
                      onChange={setSystem}
                      options={systems.map((s) => ({ value: s, label: s }))}
                    />
                  ) : undefined
                }
                flush
              >
                <Heatmap boards={data.boards.filter((b) => b.system === (system ?? systems[0]))} />
              </Panel>
              <Panel title={`板块 · ${data.boards.length}`} flush>
                <DataTable
                  label="板块"
                  rows={data.boards}
                  rowKey={(row) => `${row.system}:${row.board_code}`}
                  initialSort={{ id: "amount", desc: true }}
                  emptyText="没有板块快照：盘中快照尚未生成（非交易时段或采集未运行），开盘后自动出现。"
                  columns={[
                    { id: "name", header: "板块", value: (row) => row.board_name },
                    { id: "system", header: "体系", value: (row) => row.system, secondary: true },
                    {
                      id: "pct",
                      header: "涨幅中位",
                      numeric: true,
                      value: (row) => row.pct_chg_median ?? null,
                      cell: (row) => <ChangeText value={row.pct_chg_median ?? null} />,
                    },
                    {
                      id: "amount",
                      header: "成交额",
                      numeric: true,
                      value: (row) => row.amount ?? null,
                      cell: (row) => formatAmount(row.amount),
                    },
                    {
                      id: "net",
                      header: "主力净额",
                      numeric: true,
                      value: (row) => row.main_net_amount ?? null,
                      cell: (row) => formatAmount(row.main_net_amount),
                    },
                    {
                      id: "lu",
                      header: "涨停",
                      numeric: true,
                      value: (row) => row.limit_up_count ?? null,
                    },
                    { id: "lead", header: "领涨", value: (row) => row.leading_stock ?? null },
                  ]}
                />
              </Panel>
            </>
          );
        }}
      </QueryView>
    </>
  );
}
