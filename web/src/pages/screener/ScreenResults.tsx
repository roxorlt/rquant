import type { ScreenRow, ScreenRunData } from "@/api/screen";
import { formatCount, formatPrice } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, ChangeText, EmptyState, Panel, SkeletonRows } from "@/ui";
import { StockCell } from "../shared/StockCell";

const PAGE_SIZE = 20;
const COLUMNS: DataColumn<ScreenRow>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.ts_code,
    cell: (row) => <StockCell code={row.ts_code} name={row.name} />,
  },
  {
    id: "close",
    header: "收盘价",
    value: (row) => row.close,
    cell: (row) => <span className="num">{formatPrice(row.close)}</span>,
    numeric: true,
  },
  {
    id: "change",
    header: "涨跌幅",
    value: (row) => row.pct_chg,
    cell: (row) => <ChangeText value={row.pct_chg} />,
    numeric: true,
  },
];

function ResultBody({
  data,
  running,
  onStock,
}: {
  data: ScreenRunData | null;
  running: boolean;
  onStock: (code: string) => void;
}) {
  if (data === null) {
    return running ? (
      <SkeletonRows rows={5} />
    ) : (
      <EmptyState title="还没有运行筛选" hint="调整条件后，点「运行筛选」查看结果。" />
    );
  }
  if (data.status === "unavailable") {
    return <EmptyState title="选股数据暂不可用" hint="数据发布后就能运行条件。" />;
  }
  if (data.status === "no_date") {
    return <EmptyState title="所选日期没有选股数据" hint="换一个数据日期后重试。" />;
  }
  if (data.total === 0) {
    return <EmptyState title="没有命中股票" hint="查看逐条命中，放宽让数量变为 0 的条件。" />;
  }
  return (
    <DataTable
      rows={data.rows}
      columns={COLUMNS}
      rowKey={(row) => row.ts_code}
      label="选股结果"
      onSelect={(row) => onStock(row.ts_code)}
    />
  );
}

export function ScreenResults({
  data,
  stale,
  staleText,
  error,
  running,
  pageIndex,
  onStock,
  onPrevious,
  onNext,
}: {
  data: ScreenRunData | null;
  stale: boolean;
  staleText: string;
  error: string | null;
  running: boolean;
  pageIndex: number;
  onStock: (code: string) => void;
  onPrevious: () => void;
  onNext: () => void;
}) {
  return (
    <>
      <section className="screen-funnel" aria-label="逐条命中">
        <h2>逐条命中</h2>
        {data?.status === "ready" && data.base_count !== null ? (
          <ol>
            {[{ label: "全部股票", count: data.base_count }, ...data.steps].map((step, index) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: the ordered funnel has no stateful children.
              <li key={`${index}-${step.label}`}>
                <span>{step.label}</span>
                <div className="screen-funnel-track" aria-hidden="true">
                  <span
                    style={{
                      width: `${Math.max(2, (step.count / Math.max(data.base_count ?? 1, 1)) * 100)}%`,
                    }}
                  />
                </div>
                <strong className="num">{formatCount(step.count)}</strong>
              </li>
            ))}
          </ol>
        ) : (
          <p className="hint">运行后查看每条条件留下多少只。</p>
        )}
      </section>
      <Panel
        title={
          data?.status === "ready" ? (
            <>
              结果 · <span>命中 {formatCount(data.total)} 只</span>
            </>
          ) : (
            "结果"
          )
        }
        sub={data?.status === "ready" ? `${data.trade_date} 收盘` : undefined}
        flush
      >
        {stale ? (
          <p className="screen-notice" role="status">
            {staleText}
          </p>
        ) : null}
        {error ? (
          <p className="screen-notice error" role="alert">
            {error}
          </p>
        ) : null}
        <ResultBody data={data} running={running} onStock={onStock} />
        {data?.status === "ready" && data.total !== null && data.total > 0 ? (
          <div className="screen-pages">
            <span className="hint">
              第 {pageIndex + 1} 页 · 每页最多 {PAGE_SIZE} 只
            </span>
            <Button size="sm" onClick={onPrevious} disabled={running || stale || pageIndex === 0}>
              上一页
            </Button>
            <Button
              size="sm"
              onClick={onNext}
              disabled={running || stale || data.next_cursor === null}
            >
              下一页
            </Button>
          </div>
        ) : null}
      </Panel>
    </>
  );
}
