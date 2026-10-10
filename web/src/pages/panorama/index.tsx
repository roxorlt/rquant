import { usePanorama } from "@/api/endpoints";
import { formatAmount, formatCount } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { ChangeText, KpiStrip, PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";

export default function PanoramaPage() {
  const query = usePanorama();
  const asOf = query.data?.as_of;
  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="市场全景"
        note={asOf ? `快照 ${formatShanghaiDateTime(asOf)}` : undefined}
      />
      <QueryView query={query}>
        {(data) => (
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
            <Panel title="板块" flush>
              <DataTable
                label="板块"
                rows={data.boards}
                rowKey={(row) => `${row.system}:${row.board_code}`}
                initialSort={{ id: "amount", desc: true }}
                emptyText="没有板块快照"
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
        )}
      </QueryView>
    </>
  );
}
