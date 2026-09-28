import { useState } from "react";
import {
  type PaperAccountItem,
  type PaperAccountsData,
  type PaperHoldingItem,
  usePaperAccounts,
} from "@/api/endpoints";
import { useMeta } from "@/api/useMeta";
import { toneClass, toneOf } from "@/format/color";
import {
  formatCount,
  formatNumber,
  formatPrice,
  formatSignedNumber,
  formatSignedPercent,
} from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  EmptyState,
  type Kpi,
  KpiStrip,
  PageHeader,
  PageSkeleton,
  Panel,
  RelativeTime,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import { PaperHistory } from "./PaperHistory";
import "./paper.css";

function accountMetrics(account: PaperAccountItem, valuationNote: string | null): Kpi[] {
  return [
    {
      key: "nav",
      label: "总资产",
      value: formatNumber(account.nav),
      tip: "现金与持仓市值合计，来自已发布的账户快照",
    },
    { key: "cash", label: "现金", value: formatNumber(account.cash) },
    {
      key: "market",
      label: "持仓市值",
      value: formatNumber(account.market_value),
      tip: valuationNote ?? undefined,
    },
    {
      key: "pnl",
      label: "浮动盈亏",
      value: (
        <span className={`num ${toneClass(toneOf(account.unrealized_pnl))}`}>
          {formatSignedNumber(account.unrealized_pnl)}
        </span>
      ),
      tip: "持仓估值相对持仓成本的变化，不含已实现盈亏",
    },
  ];
}

const HOLDING_COLUMNS: DataColumn<PaperHoldingItem>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.code,
    cell: (row) => (
      <div className="paper-stock">
        <StockCell code={row.code} name={row.name} />
        <span className={`paper-mobile-pnl ${toneClass(toneOf(row.unrealized_pnl))}`}>
          浮盈 <span className="num">{formatSignedNumber(row.unrealized_pnl)}</span>
          {row.unrealized_pct === null ? null : (
            <span className="num"> · {formatSignedPercent(row.unrealized_pct)}</span>
          )}
        </span>
      </div>
    ),
    wrap: true,
    sortable: true,
  },
  {
    id: "quantity",
    header: "持仓 / 可卖",
    value: (row) => row.quantity,
    cell: (row) => (
      <span className="paper-quantity">
        <strong className="num">{formatCount(row.quantity)}</strong>
        <span className="muted num">可卖 {formatCount(row.available_quantity)}</span>
      </span>
    ),
    numeric: true,
    sortable: true,
  },
  {
    id: "cost",
    header: "成本价",
    value: (row) => row.average_cost,
    cell: (row) => formatPrice(row.average_cost),
    numeric: true,
    secondary: true,
  },
  {
    id: "price",
    header: "估值价",
    value: (row) => row.market_price,
    cell: (row) => formatPrice(row.market_price),
    numeric: true,
    secondary: true,
  },
  {
    id: "value",
    header: "市值",
    value: (row) => row.market_value,
    cell: (row) => formatNumber(row.market_value),
    numeric: true,
    secondary: true,
  },
  {
    id: "pnl",
    header: "浮动盈亏",
    value: (row) => row.unrealized_pnl,
    cell: (row) => (
      <span className={`paper-pnl ${toneClass(toneOf(row.unrealized_pnl))}`}>
        <strong className="num">{formatSignedNumber(row.unrealized_pnl)}</strong>
        <span className="num">{formatSignedPercent(row.unrealized_pct)}</span>
      </span>
    ),
    numeric: true,
    sortable: true,
    secondary: true,
  },
];

function emptyMessage(state: PaperAccountsData["source_state"]) {
  if (state === "empty") {
    return { title: "还没有模拟账户", hint: "账户发布后会显示，请稍后刷新。" };
  }
  if (state === "not_published") {
    return { title: "模拟账户尚未发布", hint: "账户来源发布后会显示，请稍后刷新。" };
  }
  return { title: "暂时读不到页面数据", hint: "请稍后刷新，或查看系统健康。" };
}

export default function PaperPage() {
  const result = usePaperAccounts();
  const meta = useMeta();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const data = result.data;
  const generationChanged =
    meta.data !== undefined &&
    result.serving !== undefined &&
    meta.data.data.generation?.generation_id !== result.serving.generation_id;
  const account =
    data?.accounts.find((item) => item.account_id === selectedId) ?? data?.accounts[0];

  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="模拟盘"
        note="账户、持仓与指令"
        actions={
          <Button size="sm" variant="ghost" onClick={result.refetch} disabled={result.isFetching}>
            刷新
          </Button>
        }
      />
      {result.error ? (
        <Panel title="模拟账户">
          <EmptyState
            title="模拟账户暂时无法加载"
            hint={
              <Button size="sm" onClick={result.refetch}>
                重试
              </Button>
            }
          />
        </Panel>
      ) : generationChanged ? (
        <Panel title="模拟账户">
          <EmptyState title="账户数据更新中" hint="请稍后刷新，查看最新账户。" />
        </Panel>
      ) : result.isLoading ? (
        <PageSkeleton label="模拟账户加载中" />
      ) : data && account ? (
        <div className="paper-content">
          <div className="paper-toolbar">
            <div className="paper-choice">
              <span>当前账户</span>
              <fieldset className="paper-accounts" aria-label="选择模拟账户">
                {data.accounts.map((item, index) => (
                  <Tip key={item.account_id} content={`账户 ${item.account_id}`} interactive>
                    <button
                      type="button"
                      aria-pressed={account.account_id === item.account_id}
                      onClick={() => setSelectedId(item.account_id)}
                    >
                      模拟账户 {index + 1}
                    </button>
                  </Tip>
                ))}
              </fieldset>
            </div>
            <div className="paper-times">
              <span>
                账户 <RelativeTime at={account.as_of} suffix="更新" />
              </span>
              {data.source_updated_at ? (
                <span>
                  数据 <RelativeTime at={data.source_updated_at} suffix="更新" />
                </span>
              ) : null}
            </div>
          </div>
          {data.source_note ? (
            <p className="paper-notice" role="status">
              {data.source_note}
            </p>
          ) : null}
          <KpiStrip items={accountMetrics(account, data.valuation_note)} label="账户资产" />
          <PaperHistory
            key={`${account.account_id}:${result.serving?.generation_id ?? ""}`}
            accountId={account.account_id}
            history={data.history}
          />
          <Panel title="持仓明细" sub={`${formatCount(account.holdings.length)} 只`}>
            {account.holdings.length ? (
              <DataTable
                rows={account.holdings}
                columns={HOLDING_COLUMNS}
                rowKey={(row) => row.code}
                label="模拟账户持仓"
              />
            ) : (
              <EmptyState title="当前账户没有持仓" hint="新持仓发布后会显示。" />
            )}
          </Panel>
        </div>
      ) : data ? (
        <Panel title="模拟账户">
          <EmptyState {...emptyMessage(data.source_state)} />
        </Panel>
      ) : null}
    </>
  );
}
