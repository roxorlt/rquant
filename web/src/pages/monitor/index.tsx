import { useState } from "react";
import { ApiError } from "@/api/client";
import { type MonitorSignal, useMonitorSignals } from "@/api/endpoints";
import { StockDrawer } from "@/app/StockDrawer";
import { formatCount } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import {
  Button,
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

function SignalEntry({ row, onStock }: { row: MonitorSignal; onStock: (code: string) => void }) {
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
          <span className="monitor-strategy">{row.strategy_name}</span>
          <Pill kind={row.action === "b_intent" ? "acc" : "idle"}>{row.action_label}</Pill>
        </div>
        {row.reasons.length ? <p className="monitor-reasons">{row.reasons.join(" · ")}</p> : null}
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
      </div>
    </li>
  );
}

export default function MonitorPage() {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const pageIndex = cursors.length - 1;
  const result = useMonitorSignals(cursors[pageIndex] ?? null, refreshKey);
  const data = result.data;
  const changed = result.error instanceof ApiError && result.error.status === 409;

  function refresh() {
    setCursors([null]);
    setRefreshKey((value) => value + 1);
  }

  const metrics: Kpi[] = data
    ? [
        {
          key: "signals",
          label: "已发布信号",
          value: formatCount(data.total),
          unit: data.total === null ? undefined : "条",
          sub: "可向前翻看历史",
        },
        {
          key: "mode",
          label: "当前通知方式",
          value: data.mode_label,
          tip: data.mode_note ?? "仅反映当前状态，历史回执不据此判定送达",
          sub: data.mode === "shadow" ? "正式推送尚未开通" : undefined,
        },
        {
          key: "receipts",
          label: "本页回执",
          value: data.receipt_label,
        },
      ]
    : [];

  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="盯盘与告警"
        note={data?.market_note ?? "查看最近发布的信号与通知回执"}
        actions={
          <Button size="sm" variant="ghost" onClick={refresh} disabled={result.isFetching}>
            刷新
          </Button>
        }
      />
      {result.isLoading ? (
        <PageSkeleton label="最近信号加载中" />
      ) : result.error ? (
        <Panel title="最近信号">
          <EmptyState
            title={changed ? "数据已更新，请从第一页重新查看。" : "最近信号暂时无法加载"}
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
          <Panel title="最近信号" sub={`按时间倒序 · 每页最多 ${data.page_size} 条`}>
            {data.source_state !== "ready" ? (
              <EmptyState
                title={data.source_label}
                hint={
                  data.source_state === "empty"
                    ? "盘中出现新信号后会自动显示，也可稍后刷新。"
                    : "请稍后刷新，或查看系统健康。"
                }
              />
            ) : (
              <>
                {data.receipt_state === "no_receipts" || data.receipt_state === "truncated" ? (
                  <p className="monitor-notice" role="status">
                    {data.receipt_label}
                  </p>
                ) : null}
                <ul className="monitor-timeline" aria-label="最近信号">
                  {data.items.map((row) => (
                    <SignalEntry key={row.signal_id} row={row} onStock={setSelectedStock} />
                  ))}
                </ul>
                <nav className="monitor-pages" aria-label="信号翻页">
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
