import { useMemo, useState } from "react";
import { type FreshnessItem, type HealthData, type ServiceItem, useHealth } from "@/api/endpoints";
import { EMPTY, formatCount } from "@/format/number";
import { formatAge, formatShanghaiDateTime, formatTradeDate } from "@/format/time";
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
  Segmented,
  SideDrawer,
  StatusBadge,
  Tip,
} from "@/ui";
import { NamedKey } from "../shared/StockCell";

type StateFilter = "all" | "attention";
type PlaneFilter = "all" | "live" | "serving" | "research";

const STATE_OPTIONS = [
  { value: "all", label: "全部" },
  { value: "attention", label: "只看异常" },
] as const;

function planeOptions(services: readonly ServiceItem[]) {
  const labels = new Map(services.map((item) => [item.plane, item.plane_label]));
  const order: PlaneFilter[] = ["live", "serving", "research"];
  return [
    { value: "all" as PlaneFilter, label: "全部分组" },
    ...order
      .filter((plane) => labels.has(plane))
      .map((plane) => ({ value: plane, label: labels.get(plane) ?? plane })),
  ];
}

function kpis(data: HealthData): Kpi[] {
  const { counts, page_data: page } = data;
  const notRunning = counts.idle + counts.waiting;
  return [
    { key: "total", label: "服务", value: formatCount(counts.total), unit: "个" },
    {
      key: "ok",
      label: "正常",
      value: formatCount(counts.ok),
      tone: counts.ok ? "ok" : undefined,
    },
    {
      key: "warn",
      label: "注意",
      value: formatCount(counts.warn),
      tone: counts.warn ? "warn" : undefined,
      tip: "还在运行，但有功能打了折扣",
    },
    {
      key: "crit",
      label: "异常",
      value: formatCount(counts.crit),
      tone: counts.crit ? "crit" : undefined,
      tip: "需要处理：心跳中断或连续失败",
    },
    {
      key: "idle",
      label: "未运行",
      value: formatCount(notRunning),
      sub: counts.waiting ? `其中 ${counts.waiting} 个是盘中服务，不在交易时段` : undefined,
    },
    {
      key: "page",
      label: "页面数据",
      value: page.age_seconds === null ? EMPTY : formatAge(page.age_seconds),
      tone: page.status.state === "ok" ? undefined : "crit",
      sub: page.status.label === "正常" ? "约每分钟更新" : page.status.reason,
      tip: "页面上所有数字都来自这批数据",
    },
  ];
}

const SERVICE_COLUMNS: DataColumn<ServiceItem>[] = [
  {
    id: "name",
    header: "服务",
    value: (row) => row.name,
    cell: (row) => <NamedKey name={row.name} techKey={row.service_id} />,
    sortable: true,
  },
  { id: "plane", header: "分组", value: (row) => row.plane_label, secondary: true },
  {
    id: "status",
    header: "状态",
    value: (row) => row.status.label,
    cell: (row) => (
      <StatusBadge state={row.status.state} label={row.status.label} reason={row.status.reason} />
    ),
  },
  {
    id: "heartbeat",
    header: "心跳",
    value: (row) => row.heartbeat_at,
    cell: (row) => <RelativeTime at={row.heartbeat_at} />,
    sortable: true,
  },
];

function latestCell(row: FreshnessItem) {
  if (row.kind === "dataset") {
    return <RelativeTime at={row.latest_at} />;
  }
  if (row.latest_date === null) {
    return <span className="muted">{EMPTY}</span>;
  }
  const text = formatTradeDate(row.latest_date);
  return row.latest_at ? (
    <Tip content={formatShanghaiDateTime(row.latest_at)}>
      <span className="num">{text}</span>
    </Tip>
  ) : (
    <span className="num">{text}</span>
  );
}

const FRESHNESS_COLUMNS: DataColumn<FreshnessItem>[] = [
  {
    id: "name",
    header: "数据",
    value: (row) => row.name,
    cell: (row) => <NamedKey name={row.name} techKey={row.key} />,
  },
  {
    id: "latest",
    header: "最新",
    value: (row) => row.latest_at ?? row.latest_date,
    cell: latestCell,
  },
  {
    id: "status",
    header: "状态",
    value: (row) => row.status.label,
    cell: (row) => (
      <StatusBadge state={row.status.state} label={row.status.label} reason={row.status.reason} />
    ),
  },
];

function ServiceDetail({ item, onClose }: { item: ServiceItem | null; onClose: () => void }) {
  return (
    <SideDrawer open={item !== null} onClose={onClose} title={item?.name ?? ""}>
      {item ? (
        <div className="drawer-body">
          <p className="row">
            <StatusBadge state={item.status.state} label={item.status.label} />
            <span className="muted">{item.status.reason}</span>
          </p>
          <dl className="kv">
            <dt>技术名称</dt>
            <dd className="mono">{item.service_id}</dd>
            <dt>分组</dt>
            <dd>{item.plane_label}</dd>
            <dt>原始状态</dt>
            <dd className="mono">
              {item.raw_status}
              {item.stale ? "（心跳超时）" : ""}
            </dd>
            <dt>最近心跳</dt>
            <dd className="num">
              {item.heartbeat_at ? formatShanghaiDateTime(item.heartbeat_at) : EMPTY}
            </dd>
            <dt>记录时间</dt>
            <dd className="num">{formatShanghaiDateTime(item.observed_at)}</dd>
            <dt>输入 / 输出</dt>
            <dd className="num">
              {item.input_sequence < 0 ? EMPTY : formatCount(item.input_sequence)} /{" "}
              {item.output_sequence < 0 ? EMPTY : formatCount(item.output_sequence)}
            </dd>
            <dt>积压</dt>
            <dd className="num">{formatCount(item.backlog_count)}</dd>
            <dt>连续失败</dt>
            <dd className="num">{formatCount(item.consecutive_failures)}</dd>
            <dt>最近错误</dt>
            <dd className="mono small">{item.last_error ?? EMPTY}</dd>
          </dl>
        </div>
      ) : null}
    </SideDrawer>
  );
}

function PageData({ data }: { data: HealthData }) {
  const page = data.page_data;
  const version = page.generation_id ? `数据版本 ${page.generation_id.slice(0, 12)}` : null;
  return (
    <Panel title="页面数据">
      <dl className="kv">
        <dt>状态</dt>
        <dd>
          <StatusBadge
            state={page.status.state}
            label={page.status.label}
            reason={[page.status.reason, version].filter(Boolean).join("；")}
          />
        </dd>
        <dt>最近更新</dt>
        <dd>
          <RelativeTime at={page.built_at} />
        </dd>
        <dt>暂无数据的模块</dt>
        <dd>
          {page.unpublished.length ? (
            <Tip content={page.unpublished.map((item) => item.name).join("、")}>
              <span className="num">{page.unpublished.length} 个</span>
            </Tip>
          ) : (
            "没有"
          )}
        </dd>
      </dl>
    </Panel>
  );
}

function Errors({ data }: { data: HealthData }) {
  return (
    <Panel title="最近错误" sub={data.errors.length ? `${data.errors.length} 条` : undefined} flush>
      {data.errors.length ? (
        <ul className="errors">
          {data.errors.map((item) => (
            <li key={item.service_id}>
              <span className="nm">{item.name}</span>
              <span className="when">
                <RelativeTime at={item.at} />
              </span>
              <Tip content={<span className="tip-detail">{item.message}</span>}>
                <span className="what">{item.summary}</span>
              </Tip>
            </li>
          ))}
        </ul>
      ) : (
        <EmptyState title="没有服务报错" />
      )}
    </Panel>
  );
}

export default function HealthPage() {
  const { data, isLoading, isFetching, error, refetch } = useHealth();
  const [stateFilter, setStateFilter] = useState<StateFilter>("all");
  const [plane, setPlane] = useState<PlaneFilter>("all");
  const [selected, setSelected] = useState<ServiceItem | null>(null);
  const services = data?.services ?? [];
  const planes = useMemo(() => planeOptions(services), [services]);
  const rows = services.filter(
    (item) =>
      (stateFilter === "all" || item.status.state === "warn" || item.status.state === "crit") &&
      (plane === "all" || item.plane === plane),
  );
  const refresh = (
    <Button size="sm" variant="ghost" onClick={refetch} disabled={isFetching}>
      {isFetching ? "刷新中" : "刷新"}
    </Button>
  );
  if (isLoading) {
    return <PageSkeleton label="系统健康加载中" />;
  }
  if (data === undefined) {
    return (
      <>
        <PageHeader eyebrow="运维" title="系统健康" actions={refresh} />
        <Panel>
          <EmptyState
            title="暂时读不到健康数据"
            hint={error ? `${error.message}，稍后点「刷新」再试` : "稍后点「刷新」再试"}
          />
        </Panel>
      </>
    );
  }
  return (
    <>
      <PageHeader eyebrow="运维" title="系统健康" actions={refresh} />
      <KpiStrip label="服务与数据" items={kpis(data)} />
      <Panel
        title="运行服务"
        sub={`${rows.length} 个`}
        actions={
          <div className="panel-tools">
            <Segmented
              label="按状态筛选"
              options={STATE_OPTIONS}
              value={stateFilter}
              onChange={setStateFilter}
            />
            {planes.length > 2 ? (
              <Segmented label="按分组筛选" options={planes} value={plane} onChange={setPlane} />
            ) : null}
          </div>
        }
        flush
      >
        <DataTable
          label="运行服务"
          rows={rows}
          columns={SERVICE_COLUMNS}
          rowKey={(row) => row.service_id}
          onSelect={setSelected}
          selectedKey={selected?.service_id ?? null}
          emptyText={
            stateFilter === "attention" ? (
              <EmptyState title="没有异常的服务" />
            ) : (
              <EmptyState title="还没有服务心跳" hint="新运行时启动后显示" />
            )
          }
        />
      </Panel>
      <div className="g2e">
        <Panel title="数据新鲜度" flush>
          <DataTable
            label="数据新鲜度"
            rows={data.freshness}
            columns={FRESHNESS_COLUMNS}
            rowKey={(row) => row.key}
            emptyText={<EmptyState title="暂时没有数据" />}
          />
        </Panel>
        <div className="stack">
          <PageData data={data} />
          <Errors data={data} />
        </div>
      </div>
      <ServiceDetail item={selected} onClose={() => setSelected(null)} />
    </>
  );
}
