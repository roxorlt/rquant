import { type PaperData, usePaper } from "@/api/endpoints";
import { TimeChart } from "@/charts/Chart";
import { kpiTone, toneClass, toneOf } from "@/format/color";
import { formatAmount, formatCount, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { EmptyState, KpiStrip, PageHeader, Panel } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import { PaperBand } from "./Band";

type Account = PaperData["accounts"][number];

/** Total equity: cash + holdings (nav may be a fraction for simulated accounts). */
function equity(a: Account): number {
  return a.cash + a.holdings.reduce((sum, h) => sum + h.market_value, 0) || 1;
}

/** NAV curve (simulated accounts carry a daily series) and industry weights. */
function PaperCharts({ account }: { account: Account }) {
  const nav = account.nav_series ?? [];
  const total = equity(account);
  const byIndustry = new Map<string, number>();
  for (const h of account.holdings) {
    const k = h.industry ?? "未分类";
    byIndustry.set(k, (byIndustry.get(k) ?? 0) + h.market_value / total);
  }
  const cash = account.cash / total;
  const weights = [...byIndustry.entries()].sort((a, b) => b[1] - a[1]);
  if (nav.length < 2 && weights.length === 0) return null;
  return (
    <div className="risk-block">
      <div>
        <h3 className="sub">净值</h3>
        {nav.length > 1 ? (
          <TimeChart
            label="模拟盘净值"
            dates={nav.map((p) => p.date)}
            baseline={1}
            height={160}
            format={(v) => v.toFixed(3)}
            series={[{ name: "净值", values: nav.map((p) => p.nav), color: "var(--accent)" }]}
          />
        ) : (
          <p className="hint">这个账户没有逐日净值（实盘镜像账户只有当前快照）。</p>
        )}
      </div>
      <div>
        <h3 className="sub">行业权重</h3>
        <ul className="weight-bars" aria-label="行业权重">
          {[...weights, ["现金", cash] as [string, number]].map(([k, w]) => (
            <li key={k}>
              <span className="wb-label">{k}</span>
              <span className="wb-bar">
                <span style={{ width: `${Math.max(0, Math.min(1, w)) * 100}%` }} />
              </span>
              <span className="num">{(w * 100).toFixed(1)}%</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default function PaperPage() {
  const query = usePaper();
  return (
    <>
      <PageHeader eyebrow="跟踪与告警" title="模拟盘" note="只读" />
      <QueryView query={query}>
        {(data) =>
          data.accounts.length === 0 ? (
            <EmptyState
              title="没有模拟账户"
              hint="模拟账户由模拟盘服务创建。创建并完成一次调仓后，这里显示净值和持仓。"
            />
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
                    {
                      key: "upnl",
                      label: "浮动盈亏",
                      value: formatAmount(account.unrealized_pnl),
                      tone: kpiTone(account.unrealized_pnl),
                    },
                    {
                      key: "rpnl",
                      label: "已实现",
                      value: formatAmount(account.realized_pnl),
                      tone: kpiTone(account.realized_pnl),
                    },
                  ]}
                />
                <PaperCharts account={account} />
                <DataTable
                  label="持仓"
                  rows={account.holdings}
                  rowKey={(row) => row.code}
                  emptyText="空仓：模拟盘当前没有持仓，下一次调仓信号后会建仓。"
                  columns={[
                    {
                      id: "code",
                      header: "代码",
                      value: (row) => row.code,
                      cell: (row) => <StockLink code={row.code} />,
                    },
                    { id: "name", header: "名称", value: (row) => row.name ?? null },
                    {
                      id: "industry",
                      header: "行业",
                      value: (row) => row.industry ?? null,
                      secondary: true,
                    },
                    {
                      id: "weight",
                      header: "权重",
                      numeric: true,
                      value: (row) => (account.nav > 0 ? row.market_value / equity(account) : null),
                      cell: (row) => `${((row.market_value / equity(account)) * 100).toFixed(1)}%`,
                    },
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
                      cell: (row) => (
                        <span className={toneClass(toneOf(row.unrealized_pnl))}>
                          {formatAmount(row.unrealized_pnl)}
                        </span>
                      ),
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
