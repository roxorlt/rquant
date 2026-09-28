import { useState } from "react";
import type { PaperAccountsData } from "@/api/endpoints";
import { EMPTY, formatCount } from "@/format/number";
import { formatShanghaiDateTime, formatTradeDate, shanghaiDateOf } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Panel, RelativeTime, Segmented, SideDrawer, Tip } from "@/ui";
import { StockCell } from "../shared/StockCell";

type PaperHistoryData = PaperAccountsData["history"];
type PaperOrder = PaperHistoryData["orders"][number];
type PaperFill = PaperOrder["fills"][number];
type HistoryView = "day" | "recent";

function exactDecimal(value: string | null): string {
  if (value === null) return EMPTY;
  const [whole = "0", fraction = ""] = value.split(".");
  return `${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}.${fraction.padEnd(2, "0")}`;
}

function orderStateClass(order: PaperOrder): string {
  if (order.status === "FILLED") return "complete";
  if (order.status === "REJECTED" || order.status === "CANCELLED" || order.status === "EXPIRED") {
    return "stopped";
  }
  return "active";
}

const ORDER_COLUMNS: DataColumn<PaperOrder>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.code,
    cell: (row) => <StockCell code={row.code} name={row.name} />,
  },
  {
    id: "side",
    header: "方向",
    value: (row) => row.side_label,
    cell: (row) => <span className="paper-order-side">{row.side_label}</span>,
  },
  {
    id: "status",
    header: "状态",
    value: (row) => row.status_label,
    cell: (row) => (
      <span className="paper-order-status">
        <span className={`paper-order-state ${orderStateClass(row)}`}>{row.status_label}</span>
        {row.reject_message ? (
          <span className="paper-order-reason">{row.reject_message}</span>
        ) : null}
      </span>
    ),
  },
  {
    id: "quantity",
    header: "成交 / 指令",
    value: (row) => row.quantity,
    cell: (row) => (
      <span className="num paper-order-quantity">
        {formatCount(row.filled_quantity)} <span className="muted">/</span>{" "}
        {formatCount(row.quantity)}
      </span>
    ),
    numeric: true,
  },
  {
    id: "price",
    header: "成交均价",
    value: (row) => row.average_fill_price,
    cell: (row) => <span className="num">{exactDecimal(row.average_fill_price)}</span>,
    numeric: true,
    secondary: true,
  },
  {
    id: "updated",
    header: "最近更新",
    value: (row) => row.updated_at,
    cell: (row) => <RelativeTime at={row.updated_at} />,
    secondary: true,
    sortable: true,
  },
];

function fillDetail(fill: PaperFill) {
  return (
    <li key={fill.fill_id} className="paper-fill">
      <div className="paper-fill-head">
        <strong>第 {formatCount(fill.sequence)} 笔</strong>
        <span className="mono">{formatShanghaiDateTime(fill.executed_at).slice(5, 16)}</span>
      </div>
      <dl className="paper-detail-grid">
        <div>
          <dt>成交股数</dt>
          <dd className="num">{formatCount(fill.quantity)}</dd>
        </div>
        <div>
          <dt>成交价</dt>
          <dd className="num">{exactDecimal(fill.price)}</dd>
        </div>
        <div>
          <dt>费用合计</dt>
          <dd className="num">{exactDecimal(fill.total_fees)}</dd>
        </div>
        <div>
          <dt>佣金 / 过户费 / 印花税</dt>
          <dd className="num">
            {exactDecimal(fill.commission)} / {exactDecimal(fill.transfer_fee)} /{" "}
            {exactDecimal(fill.tax)}
          </dd>
        </div>
      </dl>
    </li>
  );
}

function OrderDetail({ order, onClose }: { order: PaperOrder | null; onClose: () => void }) {
  return (
    <SideDrawer
      open={order !== null}
      onClose={onClose}
      title={`${order?.name ?? order?.code ?? "个股"} · 指令详情`}
    >
      {order ? (
        <div className="paper-order-detail">
          <div className="paper-order-summary">
            <StockCell code={order.code} name={order.name} />
            <span>{order.side_label}</span>
            <strong className={`paper-order-state ${orderStateClass(order)}`}>
              {order.status_label}
            </strong>
          </div>
          {order.reject_message ? (
            <p className="paper-order-rejection">
              {order.reject_message}
              {order.reject_reason ? <Tip content={order.reject_reason}> · 查看原因</Tip> : null}
            </p>
          ) : null}
          <dl className="paper-detail-grid">
            <div>
              <dt>成交进度</dt>
              <dd className="num">
                已成交 {formatCount(order.filled_quantity)} / {formatCount(order.quantity)}
              </dd>
            </div>
            <div>
              <dt>成交均价</dt>
              <dd className="num">{exactDecimal(order.average_fill_price)}</dd>
            </div>
            <div>
              <dt>指令方式</dt>
              <dd>{order.order_type === "LIMIT" ? "限价" : "市价"}</dd>
            </div>
            <div>
              <dt>发出时间</dt>
              <dd className="mono">{formatShanghaiDateTime(order.created_at)}</dd>
            </div>
            <div>
              <dt>最近更新</dt>
              <dd className="mono">{formatShanghaiDateTime(order.updated_at)}</dd>
            </div>
          </dl>
          <section className="paper-fills" aria-label="成交明细">
            <h3>成交明细</h3>
            {order.fills.length ? (
              <ol>{order.fills.map(fillDetail)}</ol>
            ) : (
              <EmptyState title="暂无成交" hint="有成交记录后会在这里显示。" />
            )}
          </section>
        </div>
      ) : null}
    </SideDrawer>
  );
}

function historyUnavailable(history: PaperHistoryData, accountId: string) {
  if (history.source_state === "not_published") {
    return { title: "指令记录尚未发布", hint: "账户和持仓仍可查看，记录发布后会显示。" };
  }
  if (history.source_state === "unavailable") {
    return { title: "指令记录暂时不可用", hint: "请稍后刷新，或查看系统健康。" };
  }
  if (history.account_id !== accountId) {
    return { title: "当前账户的指令记录尚未发布", hint: "切回有记录的账户，或等待本账户发布。" };
  }
  if (history.source_state === "empty") {
    return { title: "还没有指令记录", hint: "新记录发布后会显示。" };
  }
  if (!history.source_updated_at) {
    return { title: "指令记录暂时不可用", hint: "请稍后刷新，或查看系统健康。" };
  }
  return null;
}

export function PaperHistory({
  history,
  accountId,
}: {
  history: PaperHistoryData;
  accountId: string;
}) {
  const [view, setView] = useState<HistoryView>("day");
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const unavailable = historyUnavailable(history, accountId);
  const sourceAt = history.source_updated_at;
  if (unavailable || sourceAt === null) {
    return (
      <Panel title="指令记录">
        <EmptyState
          {...(unavailable ?? {
            title: "指令记录暂时不可用",
            hint: "请稍后刷新，或查看系统健康。",
          })}
        />
      </Panel>
    );
  }

  const sourceDay = shanghaiDateOf(sourceAt);
  const rows =
    view === "day"
      ? history.orders.filter((order) => shanghaiDateOf(order.created_at) === sourceDay)
      : history.orders;
  const selectedOrder = history.orders.find((order) => order.order_id === selectedOrderId) ?? null;
  const dayLabel = `当日指令 · ${formatTradeDate(sourceDay).slice(0, 5)}`;
  const windowCount = `${formatCount(history.orders.length)} / ${formatCount(history.total_orders)}`;

  return (
    <>
      <Panel
        title="指令记录"
        sub={
          <span>
            记录 <RelativeTime at={history.source_updated_at} suffix="更新" />
          </span>
        }
      >
        <div className="paper-history-controls">
          <Segmented
            label="筛选模拟指令"
            options={[
              { value: "day", label: dayLabel },
              { value: "recent", label: "最近指令" },
            ]}
            value={view}
            onChange={setView}
          />
          <Tip content={`记录截至 ${formatShanghaiDateTime(sourceAt)}`}>
            <span className="paper-history-count num">最近 {windowCount} 条</span>
          </Tip>
        </div>
        {history.oldest_updated_at && history.newest_updated_at ? (
          <p className="paper-history-range num">
            覆盖 {formatShanghaiDateTime(history.oldest_updated_at).slice(5, 16)} 至{" "}
            {formatShanghaiDateTime(history.newest_updated_at).slice(5, 16)}
          </p>
        ) : null}
        {history.source_note ? <p className="paper-history-note">{history.source_note}</p> : null}
        {history.has_more ? (
          <p className="paper-history-window" role="status">
            更早的指令未包含，当日列表可能不完整。
          </p>
        ) : null}
        {rows.length ? (
          <div className="paper-orders">
            <DataTable
              rows={rows}
              columns={ORDER_COLUMNS}
              rowKey={(row) => row.order_id}
              label={view === "day" ? "当日模拟指令" : "最近模拟指令"}
              onSelect={(order) => setSelectedOrderId(order.order_id)}
              selectedKey={selectedOrderId}
              height={390}
              virtualizeFrom={80}
            />
          </div>
        ) : (
          <EmptyState
            title="该日没有可见指令"
            hint={
              history.has_more
                ? "更早的指令未包含，暂无法确认这一天的完整记录。"
                : "该日确实没有指令记录。"
            }
          />
        )}
      </Panel>
      <OrderDetail order={selectedOrder} onClose={() => setSelectedOrderId(null)} />
    </>
  );
}
