import { useState } from "react";
import { ApiError } from "@/api/client";
import { type MonitorTimelineItem, useMonitorTimeline } from "@/api/endpoints";
import { StockDrawer } from "@/app/StockDrawer";
import { formatCount, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import {
  Button,
  ChangeText,
  EmptyState,
  type Kpi,
  KpiStrip,
  PageHeader,
  PageSkeleton,
  Panel,
  Pill,
  RelativeTime,
  StatusBadge,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import "./monitor.css";

const DELIVERY_TONE = {
  delivered: "ok",
  recorded: "idle",
  unconfirmed: "idle",
  sending: "waiting",
  failed: "crit",
  expired: "idle",
  none: "idle",
} as const;

const RECEIPT_KPI_VALUE = {
  not_published: "未就绪",
  no_receipts: "暂无",
  has_receipts: "有回执",
  truncated: "仅部分",
} as const;

function TimelineEntry({
  row,
  onStock,
}: {
  row: MonitorTimelineItem;
  onStock: (code: string) => void;
}) {
  if (row.kind === "notification") {
    return (
      <li className="monitor-event monitor-notification">
        <span className="monitor-event-time">
          <RelativeTime at={row.at} />
        </span>
        <div className="monitor-event-body">
          <div className="monitor-event-head">
            <strong className="monitor-notification-title">通知记录</strong>
            <span className="monitor-strategy">{row.scene_label}</span>
            <span className="monitor-strategy">{row.channel_label}</span>
            <StatusBadge
              state={row.submitted ? "ok" : "crit"}
              label={row.submission_label}
              reason="仅表示通知接口提交结果，无法确认手机是否收到"
            />
          </div>
        </div>
      </li>
    );
  }
  return (
    <li className="monitor-event">
      <span className="monitor-event-time">
        <RelativeTime at={row.at} />
      </span>
      <div className="monitor-event-body">
        <div className="monitor-event-head">
          <button
            className="monitor-stock"
            type="button"
            aria-label={`查看${row.name ?? row.code}详情`}
            onClick={() => onStock(row.code)}
          >
            <StockCell code={row.code} name={row.name} />
          </button>
          <span className="monitor-strategy">
            {row.kind === "signal" ? row.strategy_name : row.event_label}
          </span>
          {row.kind === "signal" ? (
            <Pill kind={row.action === "b_intent" ? "acc" : "idle"}>{row.action_label}</Pill>
          ) : (
            <Pill kind={row.kind === "monitor" ? "acc" : "idle"}>{row.status_label}</Pill>
          )}
        </div>
        {row.kind === "signal" ? (
          <>
            {row.reasons.length ? (
              <p className="monitor-reasons">{row.reasons.join(" · ")}</p>
            ) : null}
            <div className="monitor-event-foot">
              <StatusBadge
                state={DELIVERY_TONE[row.delivery]}
                label={row.delivery_label}
                reason={row.delivery_note}
              />
              {row.receipts.length ? (
                <ul className="monitor-receipts" aria-label="通知回执">
                  {row.receipts.map((receipt) => (
                    <li key={receipt.outbox_id}>
                      <Tip
                        content={`回执 ${receipt.outbox_id} · ${formatShanghaiDateTime(receipt.updated_at)}`}
                      >
                        <span>
                          {receipt.channel_label} · {receipt.status_label}
                        </span>
                      </Tip>
                    </li>
                  ))}
                </ul>
              ) : null}
            </div>
          </>
        ) : row.kind === "monitor" ? (
          <p className="monitor-reasons">
            触发价 <span className="num">{formatPrice(row.price)}</span>
            <span className="monitor-detail-separator">·</span>参考价{" "}
            <span className="num">{formatPrice(row.level_price)}</span>
          </p>
        ) : (
          <p className="monitor-reasons">
            价格 <span className="num">{formatPrice(row.price)}</span>
            <span className="monitor-detail-separator">·</span>涨幅{" "}
            <ChangeText value={row.pct_chg} />
          </p>
        )}
      </div>
    </li>
  );
}

export default function MonitorPage() {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const pageIndex = cursors.length - 1;
  const result = useMonitorTimeline(cursors[pageIndex] ?? null, refreshKey);
  const data = result.data;
  const changed = result.error instanceof ApiError && result.error.status === 409;

  function refresh() {
    setCursors([null]);
    setRefreshKey((value) => value + 1);
  }

  const metrics: Kpi[] = data
    ? [
        {
          key: "events",
          label: "时间线记录",
          value: formatCount(data.total),
          unit: data.total === null ? undefined : "条",
          sub: data.source_state === "ready" && data.next_cursor ? "可向前翻看历史" : undefined,
        },
        {
          key: "mode",
          label: "新信号通知",
          value: data.mode_label,
          tip: data.mode_note ?? "仅反映当前状态，历史回执不据此判定送达",
          sub: data.mode === "shadow" ? "新信号仅记录" : undefined,
        },
        {
          key: "receipts",
          label: "信号回执",
          value: RECEIPT_KPI_VALUE[data.receipt_state],
          tip: `${data.receipt_label}；旧通知记录另列，提交结果不代表手机送达`,
        },
      ]
    : [];

  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="盯盘与告警"
        note={data?.market_note ?? "查看盯盘触发、爆量和通知回执"}
        actions={
          <Button size="sm" variant="ghost" onClick={refresh} disabled={result.isFetching}>
            刷新
          </Button>
        }
      />
      {result.isLoading ? (
        <PageSkeleton label="告警时间线加载中" />
      ) : result.error ? (
        <Panel title="告警时间线">
          <EmptyState
            title={changed ? "数据已更新，请从第一页重新查看。" : "告警时间线暂时无法加载"}
            hint={
              <Button size="sm" onClick={changed ? refresh : result.refetch}>
                {changed ? "返回最新" : "重试"}
              </Button>
            }
          />
        </Panel>
      ) : data ? (
        <div className="monitor-content">
          <KpiStrip items={metrics} label="信号与通知概况" compact />
          <Panel title="告警时间线" sub={`最近 30 天 · 按时间倒序 · 每页最多 ${data.page_size} 条`}>
            {data.source_note ? (
              <p className="monitor-notice" role="status">
                {data.source_note}
              </p>
            ) : null}
            {data.source_state !== "ready" ? (
              <EmptyState
                title={data.source_label}
                hint={
                  data.source_state === "empty" && !data.source_note
                    ? "盘中出现新记录后会显示，也可稍后刷新。"
                    : "请稍后刷新，或查看系统健康。"
                }
              />
            ) : (
              <>
                {data.items.some((item) => item.kind === "signal") &&
                (data.receipt_state === "no_receipts" || data.receipt_state === "truncated") ? (
                  <p className="monitor-notice" role="status">
                    {data.receipt_label}
                  </p>
                ) : null}
                <ul className="monitor-timeline" aria-label="告警时间线">
                  {data.items.map((row) => (
                    <TimelineEntry key={row.event_key} row={row} onStock={setSelectedStock} />
                  ))}
                </ul>
                <nav className="monitor-pages" aria-label="时间线翻页">
                  <span className="hint">第 {pageIndex + 1} 页</span>
                  <Button
                    size="sm"
                    disabled={pageIndex === 0 || result.isFetching}
                    onClick={() => setCursors((current) => current.slice(0, -1))}
                  >
                    上一页
                  </Button>
                  <Button
                    size="sm"
                    disabled={data.next_cursor === null || result.isFetching}
                    onClick={() =>
                      setCursors((current) =>
                        data.next_cursor ? [...current, data.next_cursor] : current,
                      )
                    }
                  >
                    下一页
                  </Button>
                </nav>
              </>
            )}
          </Panel>
        </div>
      ) : null}
      <StockDrawer tsCode={selectedStock} onClose={() => setSelectedStock(null)} />
    </>
  );
}
