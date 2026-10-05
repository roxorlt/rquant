import type { PortfolioRows, PortfolioView } from "@/api/backtests";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { ChangeText, SideDrawer, Tip } from "@/ui";
import { portfolioMoney, portfolioPercent, portfolioPrice } from "./portfolioFormat";

export const portfolioViews: { key: PortfolioView; label: string }[] = [
  { key: "trades", label: "成交" },
  { key: "holdings", label: "持仓" },
  { key: "daily", label: "每日" },
  { key: "monthly", label: "月度" },
  { key: "log", label: "日志" },
];
export type PortfolioDetail = { title: string; fields: { label: string; value: string }[] };
type Trade = PortfolioRows["trades"][number];
type Holding = PortfolioRows["holdings"][number];
type Day = PortfolioRows["daily"][number];
type Month = PortfolioRows["monthly"][number];
type Log = PortfolioRows["log"][number];
const tradeColumns: DataColumn<Trade>[] = [
  { id: "date", header: "日期", value: (row) => row.trade_date },
  {
    id: "code",
    header: "股票",
    value: (row) => row.ts_code,
    cell: (row) => <span className="mono"> {row.ts_code} </span>,
  },
  {
    id: "side",
    header: "方向",
    value: (row) => row.side,
    cell: (row) => (row.side === "BUY" ? "买入" : "卖出"),
  },
  {
    id: "quantity",
    header: "数量",
    value: (row) => row.quantity,
    cell: (row) => row.quantity.toLocaleString("zh-CN"),
    numeric: true,
  },
  {
    id: "price",
    header: "价格",
    value: (row) => row.price,
    cell: (row) => (
      <Tip content={row.price === null ? null : `完整成交价：${row.price}`}>
        {portfolioPrice(row.price)}
      </Tip>
    ),
    numeric: true,
    secondary: true,
  },
  {
    id: "amount",
    header: "金额",
    value: (row) => row.amount,
    cell: (row) => portfolioMoney(row.amount),
    numeric: true,
    secondary: true,
  },
  {
    id: "fees",
    header: "费用",
    value: (row) => row.fees,
    cell: (row) => portfolioMoney(row.fees),
    numeric: true,
    secondary: true,
  },
  { id: "status", header: "状态", value: (row) => row.status },
  { id: "reason", header: "原因", value: (row) => row.reason, secondary: true, wrap: true },
];
const holdingColumns: DataColumn<Holding>[] = [
  { id: "date", header: "日期", value: (row) => row.trade_date },
  {
    id: "code",
    header: "股票",
    value: (row) => row.code,
    cell: (row) => <span className="mono"> {row.code} </span>,
  },
  {
    id: "quantity",
    header: "持有",
    value: (row) => row.quantity,
    cell: (row) => row.quantity.toLocaleString("zh-CN"),
    numeric: true,
  },
  {
    id: "available",
    header: "可卖",
    value: (row) => row.available_quantity,
    cell: (row) => row.available_quantity.toLocaleString("zh-CN"),
    numeric: true,
  },
  {
    id: "frozen",
    header: "待交收",
    value: (row) => row.frozen_quantity,
    cell: (row) => row.frozen_quantity.toLocaleString("zh-CN"),
    numeric: true,
    secondary: true,
  },
  {
    id: "cost",
    header: "成本价",
    value: (row) => row.average_cost,
    cell: (row) => (
      <Tip content={`完整成本价：${row.average_cost}`}>{portfolioPrice(row.average_cost)}</Tip>
    ),
    numeric: true,
    secondary: true,
  },
  {
    id: "price",
    header: "市价",
    value: (row) => row.market_price,
    cell: (row) => (
      <Tip content={`完整市价：${row.market_price ?? "—"}`}>{portfolioPrice(row.market_price)}</Tip>
    ),
    numeric: true,
    secondary: true,
  },
];
const dailyColumns: DataColumn<Day>[] = [
  { id: "date", header: "日期", value: (row) => row.trade_date },
  {
    id: "nav",
    header: "净资产",
    value: (row) => row.nav,
    cell: (row) => portfolioMoney(row.nav),
    numeric: true,
  },
  {
    id: "return",
    header: "当日收益",
    value: (row) => row.daily_return,
    cell: (row) => (
      <ChangeText value={row.daily_return === null ? null : Number(row.daily_return) * 100} />
    ),
    numeric: true,
  },
  {
    id: "cash",
    header: "现金",
    value: (row) => row.cash,
    cell: (row) => portfolioMoney(row.cash),
    numeric: true,
    secondary: true,
  },
  {
    id: "market",
    header: "持仓市值",
    value: (row) => row.market_value,
    cell: (row) => portfolioMoney(row.market_value),
    numeric: true,
    secondary: true,
  },
  {
    id: "fees",
    header: "费用",
    value: (row) => row.fees,
    cell: (row) => portfolioMoney(row.fees),
    numeric: true,
    secondary: true,
  },
  {
    id: "benchmark",
    header: "基准收益",
    value: (row) => row.benchmark_return,
    cell: (row) => (
      <ChangeText value={row.benchmark_return === null ? null : row.benchmark_return * 100} />
    ),
    numeric: true,
    secondary: true,
  },
  {
    id: "drawdown",
    header: "回撤",
    value: (row) => row.drawdown,
    cell: (row) => portfolioPercent(row.drawdown),
    numeric: true,
    secondary: true,
  },
];
const monthlyColumns: DataColumn<Month>[] = [
  {
    id: "month",
    header: "月份",
    value: (row) => `${row.year}-${String(row.month).padStart(2, "0")}`,
  },
  {
    id: "return",
    header: "月收益",
    value: (row) => row.return_rate,
    cell: (row) => <ChangeText value={row.return_rate === null ? null : row.return_rate * 100} />,
    numeric: true,
  },
];
const logColumns: DataColumn<Log>[] = [
  { id: "date", header: "日期", value: (row) => row.trade_date },
  {
    id: "code",
    header: "股票",
    value: (row) => row.ts_code,
    cell: (row) => <span className="mono"> {row.ts_code ?? "—"} </span>,
    secondary: true,
  },
  {
    id: "level",
    header: "状态",
    value: (row) => row.level,
    cell: (row) => ({ normal: "正常", note: "注意", error: "异常" })[row.level],
  },
  { id: "message", header: "说明", value: (row) => row.message, wrap: true },
];

export function PortfolioTable({
  rows,
  onDetail,
}: {
  rows: PortfolioRows;
  onDetail: (value: PortfolioDetail) => void;
}) {
  if (rows.view === "trades")
    return (
      <DataTable
        label="成交"
        rows={rows.trades}
        columns={tradeColumns}
        rowKey={(row) => `${row.trade_date}:${row.ts_code}:${row.side}`}
        onSelect={(row) =>
          onDetail({
            title: `${row.ts_code} · 成交详情`,
            fields: [
              { label: "日期", value: row.trade_date },
              { label: "方向", value: row.side === "BUY" ? "买入" : "卖出" },
              { label: "数量", value: row.quantity.toLocaleString("zh-CN") },
              { label: "价格", value: portfolioMoney(row.price) },
              { label: "金额", value: portfolioMoney(row.amount) },
              { label: "费用", value: portfolioMoney(row.fees) },
              { label: "状态", value: row.status },
              { label: "原因", value: row.reason ?? "—" },
            ],
          })
        }
      />
    );
  if (rows.view === "holdings")
    return (
      <DataTable
        label="持仓"
        rows={rows.holdings}
        columns={holdingColumns}
        rowKey={(row) => `${row.trade_date}:${row.code}`}
        onSelect={(row) =>
          onDetail({
            title: `${row.code} · 持仓详情`,
            fields: [
              { label: "日期", value: row.trade_date },
              { label: "持有数量", value: row.quantity.toLocaleString("zh-CN") },
              { label: "可卖数量", value: row.available_quantity.toLocaleString("zh-CN") },
              { label: "待交收数量", value: row.frozen_quantity.toLocaleString("zh-CN") },
              { label: "成本价", value: portfolioMoney(row.average_cost) },
              { label: "市价", value: portfolioMoney(row.market_price) },
            ],
          })
        }
      />
    );
  if (rows.view === "daily")
    return (
      <DataTable
        label="每日"
        rows={rows.daily}
        columns={dailyColumns}
        rowKey={(row) => row.trade_date}
        onSelect={(row) =>
          onDetail({
            title: `${row.trade_date} · 每日账户`,
            fields: [
              { label: "净资产", value: portfolioMoney(row.nav) },
              { label: "累计净值", value: row.normalized_nav ?? "—" },
              { label: "当日收益", value: portfolioPercent(row.daily_return) },
              { label: "现金", value: portfolioMoney(row.cash) },
              { label: "持仓市值", value: portfolioMoney(row.market_value) },
              { label: "费用", value: portfolioMoney(row.fees) },
              { label: "基准净值", value: row.benchmark_nav?.toFixed(4) ?? "—" },
              { label: "基准当日收益", value: portfolioPercent(row.benchmark_return) },
              { label: "回撤", value: portfolioPercent(row.drawdown) },
              { label: "是否调仓", value: row.rebalanced ? "是" : "否" },
              {
                label: "完整性",
                value: row.incomplete_reason === null ? "完整" : "缺少持仓收盘价",
              },
            ],
          })
        }
      />
    );
  if (rows.view === "monthly")
    return (
      <DataTable
        label="月度"
        rows={rows.monthly}
        columns={monthlyColumns}
        rowKey={(row) => `${row.year}-${row.month}`}
      />
    );
  return (
    <DataTable
      label="日志"
      rows={rows.log}
      columns={logColumns}
      rowKey={(row) => `${row.trade_date}:${row.ts_code}:${row.level}:${row.message}`}
    />
  );
}

export function PortfolioDetailDrawer({
  detail,
  onClose,
}: {
  detail: PortfolioDetail | null;
  onClose: () => void;
}) {
  return (
    <SideDrawer open={detail !== null} title={detail?.title ?? "结果详情"} onClose={onClose}>
      <dl className="pb-detail">
        {detail?.fields.map((field) => (
          <div key={field.label}>
            <dt>{field.label}</dt>
            <dd className="num">{field.value}</dd>
          </div>
        ))}
      </dl>
    </SideDrawer>
  );
}
