import { useState } from "react";
import { usePortfolioBacktest, usePortfolioBacktests } from "@/api/endpoints";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Panel, Pill } from "@/ui";
import { QueryView } from "../shared";
import { PerfPanel } from "./Perf";

function Detail({ runId }: { runId: string }) {
  const query = usePortfolioBacktest(runId);
  return (
    <QueryView query={query}>
      {(data) => (
        <>
          {data.perf ? <PerfPanel perf={data.perf} /> : null}
          <Panel title={`委托 · ${data.orders.length}`} sub="含拒单原因" flush>
            <DataTable
              label="组合回测委托"
              rows={data.orders}
              rowKey={(row) => `${row.trade_date}:${row.code}:${row.side}`}
              height={360}
              columns={[
                { id: "date", header: "日期", value: (row) => row.trade_date },
                { id: "code", header: "代码", value: (row) => row.code },
                {
                  id: "side",
                  header: "方向",
                  value: (row) => row.side,
                  cell: (row) => (row.side === "buy" ? "买入" : "卖出"),
                },
                { id: "qty", header: "数量", numeric: true, value: (row) => row.quantity },
                {
                  id: "price",
                  header: "价格",
                  numeric: true,
                  value: (row) => row.price ?? null,
                  cell: (row) => formatPrice(row.price),
                },
                { id: "fee", header: "费用", numeric: true, value: (row) => row.fee },
                {
                  id: "status",
                  header: "状态",
                  value: (row) => row.status,
                  cell: (row) =>
                    row.status === "filled" ? (
                      <Pill kind="ok">成交</Pill>
                    ) : (
                      <Pill kind="warn">拒单 · {row.reason}</Pill>
                    ),
                },
              ]}
            />
          </Panel>
        </>
      )}
    </QueryView>
  );
}

export function PortfolioRuns() {
  const query = usePortfolioBacktests();
  const [runId, setRunId] = useState<string | null>(null);
  return (
    <QueryView query={query}>
      {(data) => (
        <>
          <Panel title="组合回测" sub="python -m rquant.backtest 产出，点一行看净值与委托" flush>
            <DataTable
              label="组合回测列表"
              rows={data.runs}
              rowKey={(row) => row.run_id}
              onSelect={(row) => setRunId(row.run_id)}
              selectedKey={runId}
              emptyText="还没有组合回测结果"
              columns={[
                { id: "title", header: "名称", value: (row) => row.title },
                {
                  id: "range",
                  header: "区间",
                  value: (row) => row.start,
                  cell: (row) => `${row.start} ~ ${row.end}`,
                },
                { id: "n", header: "持股数", numeric: true, value: (row) => row.max_positions },
                {
                  id: "every",
                  header: "调仓间隔",
                  numeric: true,
                  value: (row) => row.rebalance_every,
                },
                {
                  id: "nav",
                  header: "期末净值",
                  numeric: true,
                  value: (row) => row.final_nav ?? null,
                  cell: (row) => (row.final_nav == null ? "—" : row.final_nav.toFixed(4)),
                },
                { id: "filled", header: "成交", numeric: true, value: (row) => row.filled },
                { id: "rejected", header: "拒单", numeric: true, value: (row) => row.rejected },
              ]}
            />
          </Panel>
          {runId ? <Detail runId={runId} /> : null}
        </>
      )}
    </QueryView>
  );
}
