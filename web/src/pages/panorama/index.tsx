import { useState } from "react";
import { usePanorama } from "@/api/endpoints";
import { formatAmount } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { ChangeText, PageHeader, Panel, Segmented } from "@/ui";
import { QueryView } from "../shared";
import { Heatmap } from "./Heatmap";
import { LivePanorama } from "./Live";

export default function PanoramaPage() {
  const query = usePanorama();
  const asOf = query.data?.as_of;
  const [system, setSystem] = useState<string | null>(null);
  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="市场全景" />
      <LivePanorama />
      <QueryView query={query}>
        {(data) => {
          if (!data.boards.length) return null;
          const systems = [...new Set(data.boards.map((b) => b.system))];
          return (
            <>
              <Panel
                title={`东财板块热力图${asOf ? ` · ${formatShanghaiDateTime(asOf)}` : ""}`}
                sub="面积 = 成交额，颜色 = 成分股涨跌幅中位数（±3% 封顶）；取成交额前 80"
                actions={
                  systems.length > 1 ? (
                    <Segmented
                      label="板块体系"
                      value={system ?? systems[0] ?? ""}
                      onChange={setSystem}
                      options={systems.map((s) => ({
                        value: s,
                        label: s === "dc_industry" ? "行业" : s,
                      }))}
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
