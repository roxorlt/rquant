import type { Schemas } from "@/api/client";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Panel, SideDrawer, Tip } from "@/ui";
import { portfolioPercent, portfolioRatio } from "./portfolioFormat";

type Performance = Schemas["PortfolioPerformanceData"];
type Metric = { label: string; value: string };
type Group = Schemas["RoundTripStats"] & { label: string };
const metricsColumns: DataColumn<Metric>[] = [
  { id: "label", header: "指标", value: (row) => row.label },
  { id: "value", header: "数值", value: (row) => row.value, numeric: true },
];
const groupsColumns: DataColumn<Group>[] = [
  { id: "label", header: "分组", value: (row) => row.label },
  { id: "count", header: "交易", value: (row) => row.count, numeric: true },
  {
    id: "win",
    header: "胜率",
    value: (row) => row.win_rate,
    cell: (row) => portfolioPercent(row.win_rate),
    numeric: true,
  },
  {
    id: "pnl",
    header: "净收益（元）",
    value: (row) => row.net_pnl,
    cell: (row) =>
      row.net_pnl.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    numeric: true,
    secondary: true,
  },
  {
    id: "ratio",
    header: "盈亏比",
    value: (row) => row.payoff_ratio,
    cell: (row) => portfolioRatio(row.payoff_ratio),
    numeric: true,
    secondary: true,
  },
  {
    id: "holding",
    header: "平均持有日",
    value: (row) => row.average_holding_days,
    cell: (row) => portfolioRatio(row.average_holding_days),
    numeric: true,
    secondary: true,
  },
];

function summaryMetrics(summary: Schemas["PerformanceSummary"]): Metric[] {
  return [
    { label: "完整交易日", value: String(summary.observations) },
    { label: "累计收益", value: portfolioPercent(summary.total_return) },
    { label: "年化收益", value: portfolioPercent(summary.annualized_return) },
    { label: "年化波动", value: portfolioPercent(summary.annualized_volatility) },
    { label: "最大回撤", value: portfolioPercent(summary.max_drawdown) },
    {
      label: "最大回撤持续日",
      value: summary.max_drawdown_duration?.toLocaleString("zh-CN") ?? "—",
    },
    { label: "夏普比率", value: portfolioRatio(summary.sharpe) },
    { label: "索提诺比率", value: portfolioRatio(summary.sortino) },
    { label: "卡玛比率", value: portfolioRatio(summary.calmar) },
    { label: "盈利日占比", value: portfolioPercent(summary.win_rate) },
    { label: "日收益盈亏比", value: portfolioRatio(summary.payoff_ratio) },
  ];
}

export function PortfolioMetrics({
  performance,
  open,
  onClose,
}: {
  performance: Performance | null;
  open: boolean;
  onClose: () => void;
}) {
  const relative = performance?.relative;
  const trades = performance?.round_trip_analysis;
  const groupRows = (
    values: Record<string, Schemas["RoundTripStats"]> | undefined,
    kind: "symbol" | "industry" | "holding",
  ) =>
    Object.entries(values ?? {}).map(([key, value]) => ({
      ...value,
      label: kind === "holding" ? `${key} 个交易日` : key === "unknown" ? "未提供行业" : key,
    }));
  const distribution = performance?.distribution;
  return (
    <SideDrawer wide title="完整绩效" open={open} onClose={onClose}>
      {performance ? (
        <div className="pb-metrics">
          <Panel title="组合绩效" flush>
            <DataTable
              label="组合绩效"
              rows={summaryMetrics(performance.summary)}
              columns={metricsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="基准与超额" flush>
            <DataTable
              label="基准与超额"
              rows={[
                ...(performance.benchmark_summary
                  ? summaryMetrics(performance.benchmark_summary).map((row) => ({
                      ...row,
                      label: `基准 · ${row.label}`,
                    }))
                  : [{ label: "基准", value: "缺少完整行情" }]),
                { label: "超额累计收益", value: portfolioPercent(relative?.excess_total_return) },
                {
                  label: "超额年化收益",
                  value: portfolioPercent(relative?.excess_annualized_return),
                },
                { label: "阿尔法", value: portfolioPercent(relative?.alpha) },
                { label: "贝塔", value: portfolioRatio(relative?.beta) },
                { label: "信息比率", value: portfolioRatio(relative?.information_ratio) },
                { label: "跟踪误差", value: portfolioPercent(relative?.tracking_error) },
                {
                  label: "共同交易日",
                  value: relative?.aligned_observations.toLocaleString("zh-CN") ?? "—",
                },
                { label: "年化换手率", value: portfolioPercent(performance.annualized_turnover) },
              ]}
              columns={metricsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="已完成交易" flush>
            <DataTable
              label="全部交易绩效"
              rows={trades ? [{ ...trades.overall, label: "全部" }] : []}
              columns={groupsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="按股票" flush>
            <DataTable
              label="按股票绩效"
              rows={groupRows(trades?.by_symbol, "symbol")}
              columns={groupsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="按行业" flush>
            <DataTable
              label="按行业绩效"
              rows={groupRows(trades?.by_industry, "industry")}
              columns={groupsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="按持有日" flush>
            <DataTable
              label="按持有日绩效"
              rows={groupRows(trades?.by_holding_days, "holding")}
              columns={groupsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="收益分布与连续盈亏" flush>
            <DataTable
              label="收益分布与连续盈亏"
              rows={[
                { label: "收益样本数", value: distribution?.count.toLocaleString("zh-CN") ?? "—" },
                { label: "平均收益", value: portfolioPercent(distribution?.mean) },
                { label: "收益中位数", value: portfolioPercent(distribution?.median) },
                { label: "收益第 5 百分位", value: portfolioPercent(distribution?.p05) },
                { label: "收益第 95 百分位", value: portfolioPercent(distribution?.p95) },
                { label: "最长连续盈利日", value: String(performance.streaks.longest_win) },
                { label: "最长连续亏损日", value: String(performance.streaks.longest_loss) },
                { label: "当前连续盈利日", value: String(performance.streaks.current_win) },
                { label: "当前连续亏损日", value: String(performance.streaks.current_loss) },
              ]}
              columns={metricsColumns}
              rowKey={(row) => row.label}
            />
          </Panel>
          <Panel title="收益分布区间" flush>
            <DataTable
              label="收益分布区间"
              rows={performance.distribution.bins}
              columns={[
                {
                  id: "lower",
                  header: "下限",
                  value: (row) => row.lower,
                  cell: (row) => portfolioPercent(row.lower),
                  numeric: true,
                },
                {
                  id: "upper",
                  header: "上限",
                  value: (row) => row.upper,
                  cell: (row) => portfolioPercent(row.upper),
                  numeric: true,
                },
                { id: "count", header: "样本", value: (row) => row.count, numeric: true },
              ]}
              rowKey={(row) => `${row.lower}:${row.upper}`}
            />
          </Panel>
          <Panel title="滚动指标" flush>
            <DataTable
              label="滚动指标"
              rows={performance.rolling}
              columns={[
                { id: "date", header: "日期", value: (row) => row.trade_date },
                {
                  id: "sharpe",
                  header: "夏普比率",
                  value: (row) => row.sharpe,
                  cell: (row) => portfolioRatio(row.sharpe),
                  numeric: true,
                },
                {
                  id: "volatility",
                  header: "年化波动",
                  value: (row) => row.volatility,
                  cell: (row) => portfolioPercent(row.volatility),
                  numeric: true,
                },
              ]}
              rowKey={(row) => row.trade_date}
            />
          </Panel>
          <Tip content="本次尚未执行独立样本外与过拟合检验。登记了研究计划，不代表已经通过这些检验。">
            <span className="pb-notice">过拟合检验 · 尚未评估</span>
          </Tip>
        </div>
      ) : null}
    </SideDrawer>
  );
}
