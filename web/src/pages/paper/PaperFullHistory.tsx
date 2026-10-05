import { useRef, useState } from "react";
import { ApiError, type Schemas } from "@/api/client";
import { formatCount, formatNumber, formatPrice } from "@/format/number";
import { formatShanghaiDateTime, shanghaiDateOf } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  EmptyState,
  Panel,
  RelativeTime,
  Segmented,
  SideDrawer,
  SkeletonRows,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import { usePaperFullHistory } from "./paperPortfolioApi";

type Record = Schemas["PaperPortfolioHistoryRecordView"];
const columns: DataColumn<Record>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.order.ts_code,
    cell: (row) => <StockCell code={row.order.ts_code} name={null} />,
  },
  { id: "side", header: "方向", value: (row) => row.side_label, cell: (row) => row.side_label },
  {
    id: "state",
    header: "状态",
    value: (row) => row.status_label,
    cell: (row) => (
      <span className="paper-order-status">
        <span>{row.status_label}</span>
        {row.reject_message ? (
          <span className="paper-order-reason">{row.reject_message}</span>
        ) : null}
      </span>
    ),
    wrap: true,
  },
  {
    id: "quantity",
    header: "成交 / 指令",
    value: (row) => row.order.quantity,
    cell: (row) => (
      <span className="num">
        {formatCount(row.order.filled_quantity)} / {formatCount(row.order.quantity)}
      </span>
    ),
    numeric: true,
  },
  {
    id: "updated",
    header: "更新",
    value: (row) => row.order.updated_at,
    cell: (row) => <RelativeTime at={row.order.updated_at} />,
    secondary: true,
  },
];

export function PaperFullHistory({
  viewer,
  generation,
  account,
  asOf,
  available,
}: {
  viewer: string;
  generation: string;
  account: string;
  asOf: string;
  available: boolean;
}) {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [page, setPage] = useState(0);
  const [mode, setMode] = useState<"day" | "all">("day");
  const [selected, setSelected] = useState<Record | null>(null);
  const trigger = useRef<HTMLElement | null>(null);
  const result = usePaperFullHistory(viewer, generation, account, cursors[page] ?? null, available);
  const changed =
    (result.error instanceof ApiError && result.error.status === 409) ||
    (result.serving != null && result.serving.generation_id !== generation);
  const data = changed || result.error ? null : result.data;
  const day = shanghaiDateOf(asOf);
  const records =
    data?.records.filter((row) => mode === "all" || shanghaiDateOf(row.order.created_at) === day) ??
    [];
  function close(): void {
    setSelected(null);
    requestAnimationFrame(() => trigger.current?.focus());
  }
  return (
    <Panel
      title="指令历史"
      sub={data ? `共 ${formatCount(data.total_orders)} 条` : undefined}
      actions={
        available ? (
          <Button size="sm" variant="ghost" disabled={result.isFetching} onClick={result.refetch}>
            刷新历史
          </Button>
        ) : undefined
      }
    >
      {!available ? (
        <EmptyState title="完整指令历史尚未发布" hint="记录发布后会显示。" />
      ) : result.isLoading ? (
        <SkeletonRows rows={4} />
      ) : changed ? (
        <EmptyState
          title="历史已更新"
          hint={
            <Button
              onClick={() => {
                setPage(0);
                setCursors([null]);
                result.refetch();
              }}
            >
              从第一页查看
            </Button>
          }
        />
      ) : result.error ? (
        <EmptyState
          title="指令历史暂时无法加载"
          hint={<Button onClick={result.refetch}>重试</Button>}
        />
      ) : data ? (
        <>
          <div className="paper-history-toolbar">
            <Segmented
              label="筛选完整模拟指令"
              value={mode}
              onChange={(value) => {
                setMode(value);
                setSelected(null);
              }}
              options={[
                { value: "day", label: `当日指令 · ${day.slice(5)}` },
                { value: "all", label: "完整历史" },
              ]}
            />
            <span className="num muted">
              第 {page + 1} 页 · 本页 {records.length} 条
            </span>
          </div>
          {records.length ? (
            <DataTable
              rows={records}
              columns={columns}
              rowKey={(row) => String(row.sequence)}
              label={mode === "day" ? "当日模拟指令" : "完整模拟指令"}
              onSelect={(row) => {
                trigger.current =
                  document.activeElement instanceof HTMLElement ? document.activeElement : null;
                setSelected(row);
              }}
            />
          ) : (
            <EmptyState
              title={mode === "day" ? "本页没有当日指令" : "还没有指令记录"}
              hint={data.next_cursor ? "可继续查看下一页。" : undefined}
            />
          )}
          <div className="paper-history-pager">
            <Button
              size="sm"
              disabled={page === 0 || result.isFetching}
              onClick={() => {
                setSelected(null);
                setPage((value) => value - 1);
              }}
            >
              上一页
            </Button>
            <Button
              size="sm"
              disabled={data.next_cursor == null || result.isFetching}
              onClick={() => {
                if (data.next_cursor == null) return;
                setCursors((old) => [...old.slice(0, page + 1), data.next_cursor ?? null]);
                setSelected(null);
                setPage((value) => value + 1);
              }}
            >
              下一页
            </Button>
          </div>
        </>
      ) : null}
      <SideDrawer
        open={selected !== null && !changed && !result.error}
        onClose={close}
        title="模拟指令详情"
      >
        {selected ? (
          <div className="paper-detail">
            <StockCell code={selected.order.ts_code} name={null} />
            <p>
              {selected.side_label} · {selected.status_label}
            </p>
            {selected.reject_message ? <p role="status">{selected.reject_message}</p> : null}
            <dl className="paper-detail-grid">
              <div>
                <dt>成交 / 指令</dt>
                <dd className="num">
                  {formatCount(selected.order.filled_quantity)} /{" "}
                  {formatCount(selected.order.quantity)}
                </dd>
              </div>
              <div>
                <dt>成交均价</dt>
                <dd className="num">
                  {formatPrice(
                    selected.order.average_fill_price == null
                      ? null
                      : Number(selected.order.average_fill_price),
                  )}
                </dd>
              </div>
              <div>
                <dt>更新</dt>
                <dd className="num">{formatShanghaiDateTime(selected.order.updated_at)}</dd>
              </div>
            </dl>
            {selected.fills.length ? (
              <ul className="paper-fills">
                {selected.fills.map((fill) => (
                  <li key={fill.fill_id} className="paper-fill">
                    <strong>第 {fill.sequence} 笔</strong>
                    <dl className="paper-detail-grid">
                      <div>
                        <dt>成交股数</dt>
                        <dd className="num">{formatCount(fill.quantity)}</dd>
                      </div>
                      <div>
                        <dt>成交价</dt>
                        <dd className="num">{formatPrice(Number(fill.price))}</dd>
                      </div>
                      <div>
                        <dt>费用合计</dt>
                        <dd className="num">
                          {formatNumber(fill.total_fees == null ? null : Number(fill.total_fees))}
                        </dd>
                      </div>
                      <div>
                        <dt>成交时间</dt>
                        <dd className="num">{formatShanghaiDateTime(fill.executed_at)}</dd>
                      </div>
                    </dl>
                    <Tip
                      content={`原始成交价 ${fill.price}，费用 ${fill.total_fees ?? "缺数据"}。`}
                    >
                      <span className="muted">查看精度</span>
                    </Tip>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="muted">没有成交记录</p>
            )}
          </div>
        ) : null}
      </SideDrawer>
    </Panel>
  );
}
