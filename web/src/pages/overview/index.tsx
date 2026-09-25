import { useMemo, useState } from "react";
import {
  type CandidateItem,
  type HoldingItem,
  type OverviewData,
  type SignalItem,
  useOverview,
} from "@/api/endpoints";
import { toneClass, toneOf } from "@/format/color";
import { EMPTY, formatCount, formatNumber, formatPrice, formatSignedNumber } from "@/format/number";
import { formatShanghaiTime, formatTradeDate } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
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
  type PillKind,
  RelativeTime,
  Segmented,
  StatusBadge,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import { Attention } from "./Attention";
import { Pipeline } from "./Pipeline";

const ACTION_KIND: Record<string, PillKind> = {
  b_intent: "acc",
  s_intent: "warn",
  reduce: "warn",
  watch: "idle",
  cancel: "idle",
};

const DELIVERY_STATE = {
  delivered: "ok",
  recorded: "idle",
  unconfirmed: "idle",
  sending: "waiting",
  failed: "crit",
  expired: "idle",
  none: "idle",
} as const;

function sessionNote(data: OverviewData) {
  const { session } = data;
  if (session.trade_date === null) {
    return "交易日历暂时读不到";
  }
  const day = formatTradeDate(session.trade_date);
  if (session.is_today) {
    return `今天 ${day}`;
  }
  const why =
    session.phase === "pre_open"
      ? "还没开盘，显示上一交易日"
      : `今天休市，显示最近一个交易日${
          session.next_trading_day
            ? `；下一交易日 ${formatTradeDate(session.next_trading_day)}`
            : ""
        }`;
  return (
    <Tip content={why}>
      <span>{day} · 最近交易日</span>
    </Tip>
  );
}

function kpis(data: OverviewData): Kpi[] {
  const { candidates, signals, deliveries, paper, services, freshness } = data;
  const screenDate = candidates.groups.find((group) => group.source === "screen")?.as_of;
  const notRunning = services.idle + services.waiting;
  return [
    {
      key: "candidates",
      label: "候选",
      value: formatCount(candidates.total),
      unit: "只",
      sub: candidates.groups.length
        ? candidates.groups.map((group) => `${group.name} ${group.count}`).join(" · ")
        : "还没有候选",
      tip: screenDate
        ? `N 字来自 ${formatTradeDate(screenDate)} 收盘后的选股；其余是盘中策略给出的候选`
        : "盘中策略给出的候选",
    },
    {
      key: "signals",
      label: "信号",
      value: formatCount(signals.total),
      unit: "条",
      sub: signals.by_action.length
        ? signals.by_action.map((item) => `${item.label} ${item.count}`).join(" · ")
        : "还没有信号",
    },
    {
      key: "deliveries",
      label: "推送",
      value: formatCount(deliveries.total),
      unit: deliveries.mode === "shadow" ? "条 · 仅记录" : "条",
      tone: deliveries.failed > 0 ? "crit" : undefined,
      tip: deliveries.mode_note ?? undefined,
      sub: (
        <>
          {deliveries.mode === "live"
            ? `送达 ${deliveries.delivered}`
            : deliveries.mode === "shadow"
              ? "正式推送未开通"
              : `未确认 ${deliveries.delivered}`}
          {deliveries.sending ? ` · 发送中 ${deliveries.sending}` : ""}
          {deliveries.failed ? <span className="t-crit"> · 失败 {deliveries.failed}</span> : ""}
        </>
      ),
    },
    {
      key: "paper",
      label: "模拟盘净值",
      value: paper ? (
        <Tip content={[paper.note, `账户 ${paper.account_id}`].filter(Boolean).join("；")}>
          <span>{formatNumber(paper.nav, 2)}</span>
        </Tip>
      ) : (
        EMPTY
      ),
      sub: paper ? (
        <>
          浮动盈亏{" "}
          <span className={`num ${toneClass(toneOf(paper.unrealized_pnl))}`}>
            {formatSignedNumber(paper.unrealized_pnl)}
          </span>{" "}
          · 持仓 {paper.holdings.length} 只
        </>
      ) : (
        "暂无账户"
      ),
    },
    {
      key: "services",
      label: "服务",
      value: formatCount(services.ok),
      unit: `/ ${services.total} 正常`,
      tip: notRunning ? `另有 ${notRunning} 个服务未运行或等待开盘` : undefined,
      sub:
        services.warn || services.crit ? (
          <>
            {services.crit ? <span className="t-crit">异常 {services.crit}</span> : null}
            {services.crit && services.warn ? " · " : null}
            {services.warn ? <span className="t-warn">注意 {services.warn}</span> : null}
          </>
        ) : (
          "全部正常"
        ),
    },
    {
      key: "freshness",
      label: "数据按时",
      value: formatCount(freshness.on_time),
      unit: `/ ${freshness.checked}`,
      tip: freshness.no_source ? `另有 ${freshness.no_source} 项暂时没有数据来源` : undefined,
      sub: freshness.late.length ? (
        <span className="t-warn">
          {freshness.late.length > 2
            ? `${freshness.late.slice(0, 2).join("、")}等 ${freshness.late.length} 项没按时`
            : `${freshness.late.join("、")}没按时`}
        </span>
      ) : (
        "全部按时"
      ),
    },
  ];
}

function signalColumns(modeNote: string | null): DataColumn<SignalItem>[] {
  return SIGNAL_COLUMNS.map((column) =>
    column.id === "delivery"
      ? {
          ...column,
          cell: (row: SignalItem) => (
            <StatusBadge
              state={DELIVERY_STATE[row.delivery]}
              label={row.delivery_label}
              reason={
                row.delivery === "recorded" || row.delivery === "unconfirmed" ? modeNote : null
              }
            />
          ),
        }
      : column,
  );
}

const SIGNAL_COLUMNS: DataColumn<SignalItem>[] = [
  {
    id: "at",
    header: "时间",
    value: (row) => row.at,
    cell: (row) => (
      <Tip content={row.reasons.length ? row.reasons.join("、") : undefined}>
        <span className="num">{formatShanghaiTime(row.at)}</span>
      </Tip>
    ),
    sortable: true,
  },
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.code,
    cell: (row) => <StockCell code={row.code} name={row.name} />,
  },
  { id: "strategy", header: "策略", value: (row) => row.strategy_name, secondary: true },
  {
    id: "action",
    header: "动作",
    value: (row) => row.action_label,
    cell: (row) => <Pill kind={ACTION_KIND[row.action] ?? "idle"}>{row.action_label}</Pill>,
  },
  {
    id: "delivery",
    header: "推送",
    value: (row) => row.delivery_label,
    cell: (row) => <StatusBadge state={DELIVERY_STATE[row.delivery]} label={row.delivery_label} />,
  },
];

const CANDIDATE_COLUMNS: DataColumn<CandidateItem>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.code,
    cell: (row) => <StockCell code={row.code} name={row.name} />,
  },
  { id: "group", header: "来源", value: (row) => row.group_name },
  {
    id: "close",
    header: "收盘价",
    value: (row) => row.close,
    cell: (row) => formatPrice(row.close),
    numeric: true,
    sortable: true,
    secondary: true,
  },
  {
    id: "pct",
    header: "涨跌幅",
    value: (row) => row.pct_chg,
    cell: (row) => <ChangeText value={row.pct_chg} />,
    numeric: true,
    sortable: true,
  },
  {
    id: "seen",
    header: "首次出现",
    value: (row) => row.first_seen_at,
    cell: (row) => (row.first_seen_at ? formatShanghaiTime(row.first_seen_at) : EMPTY),
    numeric: true,
    secondary: true,
  },
];

const HOLDING_COLUMNS: DataColumn<HoldingItem>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.code,
    cell: (row) => <StockCell code={row.code} name={row.name} />,
  },
  {
    id: "qty",
    header: "数量",
    value: (row) => row.quantity,
    cell: (row) => (
      <Tip content={`可卖 ${formatCount(row.available_quantity)}（T+1）`}>
        <span className="num">{formatCount(row.quantity)}</span>
      </Tip>
    ),
    numeric: true,
  },
  {
    id: "cost",
    header: "成本",
    value: (row) => row.average_cost,
    cell: (row) => formatPrice(row.average_cost),
    numeric: true,
    secondary: true,
  },
  {
    id: "price",
    header: "现价",
    value: (row) => row.market_price,
    cell: (row) => formatPrice(row.market_price),
    numeric: true,
  },
  {
    id: "value",
    header: "市值",
    value: (row) => row.market_value,
    cell: (row) => formatNumber(row.market_value, 2),
    numeric: true,
    secondary: true,
    sortable: true,
  },
  {
    id: "pnl",
    header: "浮动盈亏",
    value: (row) => row.unrealized_pnl,
    cell: (row) => (
      <div className="cell2">
        <span className={`num ${toneClass(toneOf(row.unrealized_pnl))}`}>
          {formatSignedNumber(row.unrealized_pnl)}
        </span>
        <span className="s">
          <ChangeText value={row.unrealized_pct} />
        </span>
      </div>
    ),
    numeric: true,
    sortable: true,
  },
];

function signalEmpty(data: OverviewData) {
  if (data.session.is_today && ["pre_open", "call_auction"].includes(data.session.phase)) {
    return <EmptyState title="今天还没有信号" hint="09:30 开盘后出现" />;
  }
  return <EmptyState title="这一天没有信号" />;
}

function Candidates({ data }: { data: OverviewData }) {
  const [group, setGroup] = useState("all");
  const options = useMemo(
    () => [
      { value: "all", label: "全部" },
      ...data.candidates.groups.map((item) => ({ value: item.key, label: item.name })),
    ],
    [data.candidates.groups],
  );
  const rows =
    group === "all"
      ? data.candidates.items
      : data.candidates.items.filter((item) => item.group === group);
  return (
    <Panel
      title="候选"
      sub={`${formatCount(data.candidates.total)} 只`}
      actions={
        options.length > 2 ? (
          <Segmented label="候选来源" options={options} value={group} onChange={setGroup} />
        ) : undefined
      }
      flush
    >
      <DataTable
        label="候选"
        rows={rows}
        columns={CANDIDATE_COLUMNS}
        rowKey={(row) => `${row.group}-${row.code}`}
        height={440}
        emptyText={<EmptyState title="还没有候选" hint="收盘后选股、盘中策略出现候选后显示" />}
      />
    </Panel>
  );
}

function Holdings({ data }: { data: OverviewData }) {
  const paper = data.paper;
  return (
    <Panel
      title="模拟盘持仓"
      sub={paper ? <RelativeTime at={paper.as_of} suffix="估值" /> : undefined}
      flush
    >
      {paper ? (
        <DataTable
          label="模拟盘持仓"
          rows={paper.holdings}
          columns={HOLDING_COLUMNS}
          rowKey={(row) => row.code}
          emptyText={<EmptyState title="当前没有持仓" hint="出现买入信号并成交后显示" />}
        />
      ) : (
        <EmptyState title="还没有模拟账户数据" hint="模拟撮合服务运行后显示" />
      )}
    </Panel>
  );
}

export default function OverviewPage() {
  const { data, isLoading, isFetching, error, refetch } = useOverview();
  const refresh = (
    <Button size="sm" variant="ghost" onClick={refetch} disabled={isFetching}>
      {isFetching ? "刷新中" : "刷新"}
    </Button>
  );
  if (isLoading) {
    return <PageSkeleton label="总览加载中" />;
  }
  if (data === undefined) {
    return (
      <>
        <PageHeader eyebrow="概览" title="总览" actions={refresh} />
        <Panel>
          <EmptyState
            title="暂时读不到总览数据"
            hint={error ? `${error.message}，稍后点「刷新」再试` : "稍后点「刷新」再试"}
          />
        </Panel>
      </>
    );
  }
  return (
    <>
      <PageHeader eyebrow="概览" title="总览" note={sessionNote(data)} actions={refresh} />
      {data.pipeline.length ? <Pipeline stages={data.pipeline} /> : null}
      <KpiStrip label="今日关键数字" items={kpis(data)} />
      <div className="g2">
        <Panel title="最新信号" sub={`${formatCount(data.signals.total)} 条`} flush>
          <DataTable
            label="最新信号"
            rows={data.signals.items}
            columns={signalColumns(data.deliveries.mode_note)}
            rowKey={(row) => row.signal_id}
            emptyText={signalEmpty(data)}
          />
        </Panel>
        <Panel
          title="需要关注"
          sub={data.attention.length ? `${data.attention.length} 项` : undefined}
          flush
        >
          <Attention items={data.attention} />
        </Panel>
      </div>
      <div className="g2e">
        <Candidates data={data} />
        <Holdings data={data} />
      </div>
    </>
  );
}
