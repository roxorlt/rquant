import { useState } from "react";
import { useBacktestDetail, useBacktests } from "@/api/endpoints";
import { formatPercent, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { ChangeText, PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";
import { PerfPanel } from "./Perf";
import { PortfolioRuns } from "./Portfolio";

function Trades({ runId }: { runId: string }) {
  const query = useBacktestDetail(runId);
  return (
    <QueryView query={query}>
      {(data) => (
        <>
          {data.perf ? <PerfPanel perf={data.perf} /> : null}
          <Panel title={`逐笔交易 · ${data.trades.length}`} flush>
            <DataTable
              label="逐笔交易"
              rows={data.trades}
              rowKey={(row) => row.trade_id}
              height={420}
              columns={[
                { id: "date", header: "信号日", value: (row) => row.signal_date ?? null },
                { id: "code", header: "代码", value: (row) => row.code },
                { id: "name", header: "名称", value: (row) => row.name ?? null },
                {
                  id: "entry",
                  header: "买入",
                  value: (row) => row.entry_time ?? null,
                  cell: (row) =>
                    `${row.entry_time ? formatShanghaiDateTime(row.entry_time) : "—"} @ ${formatPrice(row.entry_price)}`,
                },
                {
                  id: "exit",
                  header: "卖出",
                  value: (row) => row.exit_time ?? null,
                  cell: (row) =>
                    `${row.exit_time ? formatShanghaiDateTime(row.exit_time) : "—"} @ ${formatPrice(row.exit_price)}`,
                },
                { id: "reason", header: "原因", value: (row) => row.exit_reason ?? null },
                {
                  id: "ret",
                  header: "收益",
                  numeric: true,
                  value: (row) => row.ret_pct ?? null,
                  cell: (row) => <ChangeText value={row.ret_pct ?? null} />,
                },
              ]}
            />
          </Panel>
        </>
      )}
    </QueryView>
  );
}

export default function BacktestPage() {
  const query = useBacktests();
  const [runId, setRunId] = useState<string | null>(null);
  return (
    <>
      <PageHeader eyebrow="策略与验证" title="回测结果" />
      <QueryView query={query}>
        {(data) => (
          <Panel title="已发布回测" sub="点一行看逐笔交易" flush>
            <DataTable
              label="回测列表"
              rows={data.runs}
              rowKey={(row) => `${row.run_id}:${row.entry_mode}:${row.profile_variant}`}
              onSelect={(row) => setRunId(row.run_id)}
              selectedKey={null}
              emptyText="还没有已发布的回测"
              columns={[
                { id: "run", header: "运行", value: (row) => row.run_id },
                { id: "mode", header: "入场", value: (row) => row.entry_mode },
                { id: "variant", header: "变体", value: (row) => row.profile_variant },
                {
                  id: "range",
                  header: "区间",
                  value: (row) => row.start_date ?? null,
                  cell: (row) => `${row.start_date ?? "—"} ~ ${row.end_date ?? "—"}`,
                },
                { id: "trades", header: "笔数", numeric: true, value: (row) => row.trades ?? null },
                {
                  id: "win",
                  header: "胜率",
                  numeric: true,
                  value: (row) => row.win_rate_pct ?? null,
                  cell: (row) => formatPercent(row.win_rate_pct, 1),
                },
                {
                  id: "mean",
                  header: "平均收益",
                  numeric: true,
                  value: (row) => row.mean_ret_pct ?? null,
                  cell: (row) => <ChangeText value={row.mean_ret_pct ?? null} />,
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      {runId ? <Trades runId={runId} /> : null}
      <PortfolioRuns />
    </>
  );
}
