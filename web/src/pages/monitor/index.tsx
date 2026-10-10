import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { submitAlertAckCommand } from "@/api/alertAckCommand";
import { ApiError } from "@/api/client";
import {
  type MonitorTimelineItem,
  useMonitorChannels,
  useMonitorRuntime,
  useMonitorTimeline,
} from "@/api/endpoints";
import { type ManualWatchlistItem, useManualWatchlist } from "@/api/manualWatchlist";
import {
  publishedSuperseded,
  useManualWatchlistCommand,
  useUnresolvedManualWatchlistCodes,
} from "@/api/manualWatchlistCommand";
import { useCurrentMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import { formatCount, formatNumber, formatPercent, formatPrice } from "@/format/number";
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
import { AlertAcknowledgment, unacknowledgedKpi } from "../shared/AlertAcknowledgment";
import { StockCell } from "../shared/StockCell";
import { type AckCommandSnapshot, AlertAckCommandSession } from "./alertAckCommandSession";
import { ChannelStatus } from "./ChannelStatus";
import { ConditionAlertRules } from "./ConditionAlertRules";
import { MonitorControls } from "./MonitorControls";
import { PriceAlertRules } from "./PriceAlertRules";
import "./monitor.css";

const ALERT_ID = /^[0-9a-f]{64}$/;

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

const WATCHLIST_SOURCE = {
  detail: "来自个股详情",
  screen_result: "来自选股结果",
  pool_member: "来自池子结果",
} as const satisfies Record<ManualWatchlistItem["source"], string>;

function ManualWatchlistRow({
  item,
  viewer,
  generationId,
  onStock,
  refresh,
}: {
  item: ManualWatchlistItem;
  viewer: string | null;
  generationId: string | null;
  onStock: (code: string) => void;
  refresh: () => void;
}) {
  const command = useManualWatchlistCommand(viewer, item.ts_code, generationId);
  const confirmedAdd =
    command.record?.body.action === "add" &&
    command.record.status === "published" &&
    generationId !== null &&
    generationId !== command.record.body.generation_id &&
    item.version === command.record.version;
  const superseded =
    command.record !== null &&
    publishedSuperseded(command.record, { generationId, version: item.version });
  const record = confirmedAdd || superseded ? null : command.record;
  const unresolved =
    record !== null &&
    ["pending", "processing", "unknown", "uncertain", "saved_syncing", "published"].includes(
      record.status,
    );
  const failure = record !== null && ["conflict", "capacity", "failed"].includes(record.status);
  const label =
    record?.status === "saved_syncing" || record?.status === "published"
      ? "已保存，正在同步"
      : record?.status === "unknown" || record?.status === "uncertain"
        ? "状态待核对"
        : record?.status === "pending" || record?.status === "processing"
          ? "正在处理"
          : failure
            ? "未移出"
            : "已加入";
  return (
    <li>
      <button
        className="manual-watchlist-stock"
        type="button"
        onClick={() => onStock(item.ts_code)}
        aria-label={`查看 ${item.ts_code} 详情`}
      >
        {item.ts_code}
      </button>
      <span className="hint">{WATCHLIST_SOURCE[item.source]}</span>
      <Pill kind={unresolved ? "acc" : failure ? "warn" : "ok"}>{label}</Pill>
      {unresolved ? (
        <Button
          size="sm"
          variant="ghost"
          disabled={command.busy}
          onClick={() => {
            if (record?.status === "published") refresh();
            else void command.session.advance();
          }}
        >
          继续核对
        </Button>
      ) : (
        <Button
          size="sm"
          variant="ghost"
          disabled={
            !command.storageAvailable ||
            command.busy ||
            generationId === null ||
            (failure && record.body.generation_id === generationId)
          }
          aria-label={`移出 ${item.ts_code}`}
          onClick={() => {
            if (generationId)
              void command.session.start({
                action: "remove",
                generationId,
                expectedVersion: item.version,
                observedStatus: "active",
              });
          }}
        >
          移出
        </Button>
      )}
      {failure ? (
        <Button size="sm" variant="ghost" onClick={refresh}>
          刷新名单
        </Button>
      ) : null}
    </li>
  );
}

function ManualWatchlistRecoveryRow({
  tsCode,
  viewer,
  generationId,
  onStock,
}: {
  tsCode: string;
  viewer: string;
  generationId: string | null;
  onStock: (code: string) => void;
}) {
  const command = useManualWatchlistCommand(viewer, tsCode, generationId);
  const record = command.record;
  if (
    record === null ||
    !["pending", "processing", "saved_syncing", "uncertain", "unknown"].includes(record.status)
  )
    return null;
  const label =
    record.status === "saved_syncing"
      ? "已保存，正在同步"
      : record.status === "pending" || record.status === "processing"
        ? "正在处理"
        : "状态待核对";
  return (
    <li>
      <button
        className="manual-watchlist-stock"
        type="button"
        onClick={() => onStock(tsCode)}
        aria-label={`查看 ${tsCode} 详情`}
      >
        {tsCode}
      </button>
      <span className="hint">{record.body.action === "add" ? "加入" : "移出"}</span>
      <Pill kind="acc">{label}</Pill>
      <Button
        size="sm"
        variant="ghost"
        aria-label={`继续核对 ${tsCode}`}
        disabled={command.busy || !command.storageAvailable}
        onClick={() => void command.session.advance()}
      >
        继续核对
      </Button>
    </li>
  );
}

function ManualWatchlistPanel({ onStock }: { onStock: (code: string) => void }) {
  const watchlist = useManualWatchlist();
  const viewer = watchlist.viewer;
  const unresolved = useUnresolvedManualWatchlistCodes(viewer).filter(
    (code) => !watchlist.items.some((item) => item.ts_code === code),
  );
  return (
    <Panel title="手动盯盘" label="手动盯盘" sub="手动加入 · 与自动策略分开">
      {watchlist.state === "loading" ? (
        <p className="hint" role="status">
          正在读取名单
        </p>
      ) : watchlist.state === "unavailable" ? (
        <div className="manual-watchlist-unavailable">
          <p>名单暂不可用，请稍后重试</p>
          <Button size="sm" onClick={watchlist.retry}>
            重试
          </Button>
        </div>
      ) : watchlist.items.length === 0 ? (
        <EmptyState title="暂无手动盯盘股票" hint="从个股详情加入后会显示在这里。" />
      ) : (
        <ul className="manual-watchlist" aria-label="手动盯盘名单">
          {watchlist.items.map((item) => (
            <ManualWatchlistRow
              key={item.ts_code}
              item={item}
              viewer={watchlist.viewer}
              generationId={watchlist.generationId}
              onStock={onStock}
              refresh={watchlist.refreshMeta}
            />
          ))}
        </ul>
      )}
      {viewer !== null && unresolved.length > 0 ? (
        <div className="manual-watchlist-recovery">
          <p className="hint">待核对操作</p>
          <ul className="manual-watchlist" aria-label="待核对操作">
            {unresolved.map((code) => (
              <ManualWatchlistRecoveryRow
                key={code}
                tsCode={code}
                viewer={viewer}
                generationId={watchlist.generationId}
                onStock={onStock}
              />
            ))}
          </ul>
        </div>
      ) : null}
    </Panel>
  );
}

function TimelineEntry({
  row,
  onStock,
  commandSession,
  command,
  canConfirm,
  canResume,
  generationId,
}: {
  row: MonitorTimelineItem;
  onStock: (code: string) => void;
  commandSession: AlertAckCommandSession;
  command: AckCommandSnapshot;
  canConfirm: boolean;
  canResume: boolean;
  generationId: string | null | undefined;
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
  if (row.kind === "channel_attempt") {
    return (
      <li className="monitor-event monitor-notification">
        <span className="monitor-event-time">
          <RelativeTime at={row.at} />
        </span>
        <div className="monitor-event-body">
          <div className="monitor-event-head">
            <strong className="monitor-notification-title">{row.channel_label}</strong>
            <Pill kind={row.state === "rejected" ? "warn" : "idle"}>{row.state_label}</Pill>
            <Tip content={row.source_note}>
              <span className="monitor-strategy">
                {row.mode === "shadow" ? "仅记录" : "正式推送"}
              </span>
            </Tip>
          </div>
          <p className="monitor-reasons">
            逻辑通知 <span className="num">{formatCount(row.logical_count)}</span>
            {row.attempt_no != null ? (
              <>
                <span className="monitor-detail-separator">·</span>成员尝试{" "}
                <span className="num">{row.attempt_no}</span>
              </>
            ) : null}
          </p>
        </div>
      </li>
    );
  }
  if (row.kind === "builtin" || row.kind === "condition") {
    const market = row.kind === "builtin" && row.subject === "market";
    return (
      <li className="monitor-event">
        <span className="monitor-event-time">
          <RelativeTime at={row.at} />
        </span>
        <div className="monitor-event-body">
          <div className="monitor-event-head">
            {!market && row.code !== null ? (
              <button
                className="monitor-stock"
                type="button"
                aria-label={`查看${row.name ?? row.code}详情`}
                onClick={() => {
                  if (row.code !== null) onStock(row.code);
                }}
              >
                <StockCell code={row.code} name={row.name} />
              </button>
            ) : (
              <strong>{market ? "全市场" : "个股未确认"}</strong>
            )}
            <Tip content={row.source_note}>
              <span className="monitor-strategy">{row.event_label}</span>
            </Tip>
            <AckAction
              acknowledgment={row.acknowledgment}
              commandSession={commandSession}
              command={command}
              canConfirm={canConfirm}
              canResume={canResume}
              generationId={generationId}
            />
          </div>
          {row.kind === "builtin" && market ? (
            <p className="monitor-reasons">
              <span className="num">
                {row.comparison_unit === "percent"
                  ? formatPercent(row.before)
                  : row.comparison_unit === "count"
                    ? formatCount(row.before)
                    : formatNumber(row.before)}
                {" → "}
                {row.comparison_unit === "percent"
                  ? formatPercent(row.after)
                  : row.comparison_unit === "count"
                    ? formatCount(row.after)
                    : formatNumber(row.after)}
                {row.comparison_unit === "count" ? " 家" : null}
              </span>
            </p>
          ) : row.kind === "builtin" ? (
            <p className="monitor-reasons">
              触发价 <span className="num">{formatPrice(row.price)}</span>
              {row.threshold != null ? (
                <>
                  <span className="monitor-detail-separator">·</span>
                  {row.threshold_unit === "CNY"
                    ? "参考价"
                    : row.threshold_unit === "multiple"
                      ? "相对量"
                      : "阈值"}{" "}
                  <span className="num">
                    {formatNumber(row.threshold)}
                    {row.threshold_unit === "multiple" ? "倍" : null}
                  </span>
                </>
              ) : null}
            </p>
          ) : (
            <Tip content={row.source_note}>
              <span className="monitor-strategy">{row.status_label}</span>
            </Tip>
          )}
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
          <AckAction
            acknowledgment={row.acknowledgment}
            commandSession={commandSession}
            command={command}
            canConfirm={canConfirm}
            canResume={canResume}
            generationId={generationId}
          />
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

type Acknowledgment = Exclude<
  MonitorTimelineItem,
  { kind: "notification" | "channel_attempt" }
>["acknowledgment"];

function AckAction({
  acknowledgment,
  commandSession,
  command,
  canConfirm,
  canResume,
  generationId,
}: {
  acknowledgment: Acknowledgment;
  commandSession: AlertAckCommandSession;
  command: AckCommandSnapshot;
  canConfirm: boolean;
  canResume: boolean;
  generationId: string | null | undefined;
}) {
  const alertId = acknowledgment?.alert_id;
  const validId = typeof alertId === "string" && ALERT_ID.test(alertId);
  const entry = validId ? command.entries[alertId] : undefined;
  const busy = validId && command.busyAlerts.includes(alertId);
  const verifiedConfirmation =
    validId &&
    acknowledgment?.state === "confirmed" &&
    acknowledgment.eligible === false &&
    typeof acknowledgment.confirmation_id === "string" &&
    acknowledgment.confirmation_id.length > 0;
  const confirmed =
    verifiedConfirmation &&
    (entry?.status !== "succeeded" ||
      (generationId !== entry.body.generation_id &&
        acknowledgment?.confirmation_id === entry.confirmationId));
  const eligible =
    canConfirm &&
    validId &&
    acknowledgment?.state === "unconfirmed" &&
    acknowledgment.eligible === true;

  if (confirmed) return <AlertAcknowledgment acknowledgment={acknowledgment} />;
  if (entry?.status === "succeeded") {
    return (
      <StatusBadge
        state="waiting"
        label="已受理，正在同步"
        reason="确认已保存。页面数据更新后将显示最终状态。"
      />
    );
  }
  if (entry && entry.status !== "failed") {
    return (
      <span className="monitor-ack">
        <StatusBadge
          state="waiting"
          label={busy ? "正在核对" : "状态待核对"}
          reason="本次请求已有记录，请用原请求继续核对。"
        />
        {command.storageAvailable ? (
          <Button
            size="sm"
            disabled={busy || !canResume}
            disabledReason={!canResume ? "请登录后继续核对本次请求。" : undefined}
            onClick={() => {
              if (alertId) void commandSession.advance(alertId);
            }}
          >
            继续核对
          </Button>
        ) : null}
      </span>
    );
  }
  if (entry?.status === "failed") {
    const stale = entry.failureKind === "stale_generation";
    return (
      <span className="monitor-ack">
        <StatusBadge
          state="warn"
          label={stale ? "数据已更新" : "确认未完成"}
          reason={
            stale ? "旧数据上的请求未受理，刷新后可重新确认。" : "上次请求已结束，告警仍需确认。"
          }
        />
        {eligible && (!stale || generationId !== entry.body.generation_id) ? (
          <Button
            size="sm"
            onClick={() => {
              if (generationId && alertId) void commandSession.start(generationId, alertId);
            }}
          >
            重新确认
          </Button>
        ) : null}
      </span>
    );
  }
  return (
    <span className="monitor-ack">
      <AlertAcknowledgment
        acknowledgment={
          verifiedConfirmation
            ? acknowledgment
            : acknowledgment?.state === "confirmed"
              ? undefined
              : acknowledgment
        }
      />
      {eligible && command.storageAvailable ? (
        <Button
          size="sm"
          onClick={() => {
            if (generationId && alertId) void commandSession.start(generationId, alertId);
          }}
        >
          确认
        </Button>
      ) : null}
    </span>
  );
}

export default function MonitorPage() {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const [cursorScope, setCursorScope] = useState<string | null>(null);
  const [selectedScope, setSelectedScope] = useState<string | null>(null);
  const meta = useCurrentMeta();
  useEffect(() => {
    void meta.refetch();
  }, [meta.refetch]);
  const currentGeneration = meta.data?.data.generation?.generation_id;
  const identityKnown = meta.isFetchedAfterMount && !meta.isError && meta.data !== undefined;
  const viewer = identityKnown ? (meta.data?.data.viewer ?? null) : null;
  const scope = `${viewer}:${currentGeneration}`;
  const liveCursors = cursorScope === scope ? cursors : [null];
  const pageIndex = liveCursors.length - 1;
  const result = useMonitorTimeline(liveCursors[pageIndex] ?? null, refreshKey);
  const channelResult = useMonitorChannels(refreshKey);
  const runtimeResult = useMonitorRuntime(refreshKey);
  useEffect(() => {
    setCursors([null]);
    setCursorScope(scope);
    setSelectedStock(null);
    setSelectedScope(null);
  }, [scope]);
  const commandSession = useMemo(
    () =>
      new AlertAckCommandSession(
        (() => {
          try {
            return window.localStorage;
          } catch {
            return null;
          }
        })(),
        viewer,
        submitAlertAckCommand,
        () =>
          `web-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (item) => item.toString(16).padStart(2, "0")).join("")}`,
        () => new Date().toISOString(),
      ),
    [viewer],
  );
  const command = useSyncExternalStore(
    commandSession.subscribe,
    commandSession.snapshot,
    commandSession.snapshot,
  );
  useEffect(() => {
    if (viewer && !meta.isError) void commandSession.resumePending();
  }, [commandSession, viewer, meta.isError]);
  const oldGeneration =
    currentGeneration !== undefined && result.serving?.generation_id !== currentGeneration;
  const oldChannelGeneration =
    currentGeneration !== undefined && channelResult.serving?.generation_id !== currentGeneration;
  const channelLoading = channelResult.isLoading || (!meta.isFetchedAfterMount && !meta.isError);
  const channelData =
    !identityKnown ||
    meta.isError ||
    oldChannelGeneration ||
    channelResult.error ||
    channelResult.serving?.state === "unavailable"
      ? undefined
      : channelResult.data;
  const runtimeData =
    !identityKnown ||
    !viewer ||
    meta.isError ||
    runtimeResult.error ||
    runtimeResult.serving?.state !== "ready" ||
    runtimeResult.serving.generation_id !== currentGeneration
      ? undefined
      : runtimeResult.data;
  const data = !identityKnown || oldGeneration || result.error ? undefined : result.data;
  const changed = result.error instanceof ApiError && result.error.status === 409;
  const pageFresh =
    meta.isFetchedAfterMount &&
    !meta.isError &&
    meta.data?.serving.state === "ready" &&
    result.serving?.state === "ready" &&
    currentGeneration !== undefined &&
    currentGeneration !== null &&
    currentGeneration === result.serving.generation_id;
  const canConfirm =
    pageFresh &&
    !!viewer &&
    command.storageAvailable &&
    data?.source_state === "ready" &&
    data.unacknowledged?.state === "ready" &&
    data.unacknowledged.count !== null &&
    data.unacknowledged.count_as_of !== null;
  const canResume = !!viewer && !meta.isError && command.storageAvailable;
  const canConfirmBuiltin =
    pageFresh &&
    !!viewer &&
    command.storageAvailable &&
    data?.builtin_unacknowledged?.state === "ready" &&
    data.builtin_unacknowledged.count != null &&
    data.builtin_unacknowledged.count_as_of != null;

  function refresh() {
    setCursors([null]);
    setCursorScope(scope);
    setRefreshKey((value) => value + 1);
    void meta.refetch();
  }

  function openStock(code: string) {
    setSelectedStock(code);
    setSelectedScope(scope);
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
        unacknowledgedKpi(pageFresh ? data.unacknowledged : undefined),
        ...(data.builtin_unacknowledged
          ? [
              {
                key: "builtin-unacknowledged",
                label: "内置待确认",
                value: formatCount(
                  pageFresh && data.builtin_unacknowledged.state === "ready"
                    ? data.builtin_unacknowledged.count
                    : null,
                ),
                tip: data.builtin_unacknowledged.note ?? "只统计当前账号已核对的完整内置告警窗口。",
              },
            ]
          : []),
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
      <ChannelStatus
        data={channelData}
        loading={channelLoading}
        retry={channelResult.refetch}
        runtime={runtimeData}
        runtimeLoading={runtimeResult.isLoading}
        retryRuntime={runtimeResult.refetch}
      />
      <MonitorControls
        viewer={viewer}
        identityKnown={identityKnown}
        generationId={currentGeneration ?? null}
        refreshKey={refreshKey}
        data={runtimeData}
        loading={runtimeResult.isLoading}
        onRefresh={refresh}
      />
      <ManualWatchlistPanel onStock={openStock} />
      <PriceAlertRules />
      <ConditionAlertRules />
      {result.isLoading || (oldGeneration && !result.error) ? (
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
          <Panel
            title="告警时间线"
            sub={`自动策略与池子 · 最近 30 天 · 每页最多 ${data.page_size} 条`}
          >
            {data.source_note ? (
              <p className="monitor-notice" role="status">
                {data.source_note}
              </p>
            ) : null}
            {meta.data &&
            !viewer &&
            data.items.some(
              (item) =>
                item.kind !== "notification" &&
                item.kind !== "channel_attempt" &&
                item.acknowledgment?.eligible,
            ) ? (
              <p className="monitor-notice" role="status">
                请先登录，才能确认告警。
              </p>
            ) : null}
            {command.message ? (
              <p className="monitor-notice" role="status">
                {command.message}
              </p>
            ) : null}
            {!command.storageAvailable && viewer ? (
              <p className="monitor-notice" role="status">
                浏览器记录不可用，暂时无法安全确认。
              </p>
            ) : null}
            {data.source_state !== "ready" && data.items.length === 0 ? (
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
                    <TimelineEntry
                      key={row.event_key}
                      row={row}
                      onStock={openStock}
                      commandSession={commandSession}
                      command={command}
                      canConfirm={row.kind === "builtin" ? canConfirmBuiltin : canConfirm}
                      canResume={canResume}
                      generationId={result.serving?.generation_id}
                    />
                  ))}
                </ul>
                <nav className="monitor-pages" aria-label="时间线翻页">
                  <span className="hint">第 {pageIndex + 1} 页</span>
                  <Button
                    size="sm"
                    disabled={pageIndex === 0 || result.isFetching}
                    onClick={() => {
                      setCursorScope(scope);
                      setCursors(liveCursors.slice(0, -1));
                    }}
                  >
                    上一页
                  </Button>
                  <Button
                    size="sm"
                    disabled={data.next_cursor === null || result.isFetching}
                    onClick={() => {
                      setCursorScope(scope);
                      setCursors(
                        data.next_cursor ? [...liveCursors, data.next_cursor] : liveCursors,
                      );
                    }}
                  >
                    下一页
                  </Button>
                </nav>
              </>
            )}
          </Panel>
        </div>
      ) : null}
      <StockDrawer
        tsCode={selectedScope === scope ? selectedStock : null}
        onClose={() => setSelectedStock(null)}
      />
    </>
  );
}
