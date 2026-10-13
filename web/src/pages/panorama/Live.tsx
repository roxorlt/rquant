import { useState } from "react";
import { inTradingHours, useMarketLive } from "@/api/endpoints";
import { TimeChart } from "@/charts/Chart";
import { formatAmount, formatCount } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { ChangeText, KpiStrip, Panel } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import { Heatmap } from "./Heatmap";

const pct = (v: number) => `${(v * 100).toFixed(0)}%`;

/** Live 市场全景 from the research 3-second snapshots (rebuild of the old Streamlit page). */
export function LivePanorama() {
  const [board, setBoard] = useState<string | null>(null);
  const query = useMarketLive(board);
  return (
    <QueryView query={query}>
      {(data) => {
        const p = data.pulse;
        if (!p || !data.curve)
          return <p className="hint pad">今天还没有快照（采集器 09:10 启动，开盘后出现）。</p>;
        const c = data.curve;
        const live = inTradingHours();
        const boards = data.boards;
        const selected = data.board ?? board;
        return (
          <>
            <KpiStrip
              label="市场脉搏"
              compact
              items={[
                {
                  key: "ratio",
                  label: "上涨占比",
                  value: p.up_ratio == null ? "—" : pct(p.up_ratio),
                  tone: (p.up_ratio ?? 0) >= 0.5 ? "up" : "down",
                  tip: "上涨家数 ÷ 有成交的全部 A 股",
                },
                {
                  key: "ud",
                  label: "涨 / 跌",
                  value: `${formatCount(p.up)} / ${formatCount(p.down)}`,
                  sub: `平 ${formatCount(p.flat)}`,
                },
                {
                  key: "med",
                  label: "涨跌中位数",
                  value: <ChangeText value={p.median_pct ?? null} />,
                  tip: "全部个股涨跌幅的中位数，比指数更能代表“大多数股票”",
                },
                { key: "lu", label: "涨停", value: formatCount(p.limit_up), tone: "up" },
                {
                  key: "br",
                  label: "炸板",
                  value: formatCount(p.broken),
                  tip: "今天摸到过涨停、现在没封住的股票",
                  sub:
                    p.limit_up + p.broken
                      ? `封板率 ${pct(p.limit_up / (p.limit_up + p.broken))}`
                      : undefined,
                },
                { key: "ld", label: "跌停", value: formatCount(p.limit_down), tone: "down" },
                {
                  key: "amt",
                  label: "成交额",
                  value: formatAmount(p.amount),
                  sub: data.time ?? "",
                },
              ]}
            />
            <div className="card-grid">
              <Panel
                title="赚钱效应（逐分钟）"
                sub={`上涨家数占比；50% 以上说明多数股票在涨${live ? " · 每 15 秒刷新" : ""}`}
              >
                <TimeChart
                  label="上涨占比"
                  dates={c.t}
                  height={150}
                  baseline={0.5}
                  format={pct}
                  series={[
                    { name: "上涨占比", values: c.up_ratio, color: "var(--accent)", area: true },
                  ]}
                />
                <TimeChart
                  label="涨停与炸板家数"
                  dates={c.t}
                  height={110}
                  format={(v) => v.toFixed(0)}
                  series={[
                    { name: "涨停", values: c.limit_up, color: "var(--up)" },
                    { name: "炸板", values: c.broken, color: "var(--muted)", dashed: true },
                  ]}
                />
              </Panel>
              <Panel
                title="行业热力图"
                sub="面积 = 成交额，颜色 = 行业内个股涨跌中位数（±3% 封顶）"
                flush
              >
                <Heatmap
                  boards={boards.map((b) => ({
                    system: "行业",
                    board_code: b.name,
                    board_name: b.name,
                    amount: b.amount,
                    main_net_amount: null,
                    pct_chg_median: b.pct_median ?? null,
                    limit_up_count: b.limit_up,
                    stock_count: b.n,
                    leading_stock: b.leader ?? null,
                  }))}
                />
              </Panel>
              <Panel title={`行业 · ${boards.length}`} sub="点一行看右侧成分股" flush>
                <DataTable
                  label="行业"
                  rows={boards}
                  rowKey={(r) => r.name}
                  height={420}
                  onSelect={(r) => setBoard(r.name)}
                  selectedKey={selected}
                  initialSort={{ id: "amount", desc: true }}
                  columns={[
                    { id: "name", header: "行业", value: (r) => r.name },
                    {
                      id: "pct",
                      header: "涨跌中位",
                      numeric: true,
                      value: (r) => r.pct_median ?? null,
                      cell: (r) => <ChangeText value={r.pct_median ?? null} />,
                    },
                    {
                      id: "up",
                      header: "上涨占比",
                      numeric: true,
                      value: (r) => r.up_ratio ?? null,
                      cell: (r) => (r.up_ratio == null ? "—" : pct(r.up_ratio)),
                    },
                    {
                      id: "amount",
                      header: "成交额",
                      numeric: true,
                      value: (r) => r.amount,
                      cell: (r) => formatAmount(r.amount),
                    },
                    { id: "lu", header: "涨停", numeric: true, value: (r) => r.limit_up },
                    {
                      id: "lead",
                      header: "领涨",
                      secondary: true,
                      value: (r) => r.leader_pct ?? null,
                      cell: (r) => (
                        <span>
                          {r.leader ?? "—"} <ChangeText value={r.leader_pct ?? null} />
                        </span>
                      ),
                    },
                  ]}
                />
              </Panel>
              <Panel
                title={`${selected ?? ""} 成分 · ${data.members.length}`}
                sub="按涨跌幅排序"
                flush
              >
                <DataTable
                  label="成分股"
                  rows={data.members}
                  rowKey={(r) => r.code}
                  height={420}
                  emptyText="在左侧选一个行业"
                  initialSort={{ id: "pct", desc: true }}
                  columns={[
                    {
                      id: "code",
                      header: "代码",
                      value: (r) => r.code,
                      cell: (r) => <StockLink code={r.code} />,
                    },
                    { id: "name", header: "名称", value: (r) => r.name ?? null },
                    {
                      id: "pct",
                      header: "涨跌",
                      numeric: true,
                      value: (r) => r.pct ?? null,
                      cell: (r) => <ChangeText value={r.pct ?? null} />,
                    },
                    {
                      id: "amount",
                      header: "成交额",
                      numeric: true,
                      value: (r) => r.amount ?? null,
                      cell: (r) => formatAmount(r.amount),
                    },
                    {
                      id: "state",
                      header: "状态",
                      value: (r) => r.state ?? null,
                      cell: (r) =>
                        r.state ? (
                          <span
                            className={
                              r.state === "跌停" ? "down" : r.state === "涨停" ? "up" : "muted"
                            }
                          >
                            {r.state}
                          </span>
                        ) : (
                          ""
                        ),
                    },
                  ]}
                />
              </Panel>
            </div>
          </>
        );
      }}
    </QueryView>
  );
}
