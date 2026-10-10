import { usePaper } from "@/api/endpoints";
import { formatAmount, formatCount, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { EmptyState, KpiStrip, PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";
import { PaperBand } from "./Band";

export default function PaperPage() {
  const query = usePaper();
  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="模拟盘" note="只读" />
      <QueryView query={query}>
        {(data) =>
          data.accounts.length === 0 ? (
            <EmptyState title="没有模拟账户" />
          ) : (
            data.accounts.map((account) => (
              <Panel
                key={account.account_id}
                title={account.account_id}
                sub={account.as_of ? `截至 ${formatShanghaiDateTime(account.as_of)}` : undefined}
              >
                <KpiStrip
                  compact
                  label="账户"
                  items={[
                    { key: "nav", label: "净值", value: formatAmount(account.nav) },
                    { key: "cash", label: "现金", value: formatAmount(account.cash) },
                    { key: "upnl", label: "浮动盈亏", value: formatAmount(account.unrealized_pnl) },
                    { key: "rpnl", label: "已实现", value: formatAmount(account.realized_pnl) },
                  ]}
                />
                <DataTable
                  label="持仓"
                  rows={account.holdings}
                  rowKey={(row) => row.code}
                  emptyText="空仓"
                  columns={[
                    { id: "code", header: "代码", value: (row) => row.code },
                    { id: "name", header: "名称", value: (row) => row.name ?? null },
                    {
                      id: "qty",
                      header: "数量",
                      numeric: true,
                      value: (row) => row.quantity,
                      cell: (row) => formatCount(row.quantity),
                    },
                    {
                      id: "cost",
                      header: "成本",
                      numeric: true,
                      value: (row) => row.average_cost,
                      cell: (row) => formatPrice(row.average_cost),
                    },
                    {
                      id: "price",
                      header: "现价",
                      numeric: true,
                      value: (row) => row.market_price,
                      cell: (row) => formatPrice(row.market_price),
                    },
                    {
                      id: "value",
                      header: "市值",
                      numeric: true,
                      value: (row) => row.market_value,
                      cell: (row) => formatAmount(row.market_value),
                    },
                    {
                      id: "pnl",
                      header: "浮盈",
                      numeric: true,
                      value: (row) => row.unrealized_pnl,
                      cell: (row) => formatAmount(row.unrealized_pnl),
                    },
                  ]}
                />
              </Panel>
            ))
          )
        }
      </QueryView>
      <PaperBand />
    </>
  );
}
