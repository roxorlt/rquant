import { useState } from "react";
import { type ExperimentItem, useExperiments } from "@/api/experiments";
import { useCurrentMeta } from "@/api/useMeta";
import { formatCount, formatPercent } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ChangeText,
  EmptyState,
  PageHeader,
  PageSkeleton,
  Panel,
  Pill,
  RelativeTime,
  Tip,
} from "@/ui";
import "./experiments.css";

const statusLabel: Record<
  ExperimentItem["status"],
  { label: string; kind: "ok" | "warn" | "crit" | "idle" | "acc" }
> = {
  registered: { label: "已登记", kind: "idle" },
  running: { label: "运行中", kind: "acc" },
  executed: { label: "结果待确认", kind: "warn" },
  succeeded: { label: "已完成", kind: "ok" },
  failed: { label: "未完成", kind: "crit" },
  cancelled: { label: "已取消", kind: "idle" },
};

const columns: DataColumn<ExperimentItem>[] = [
  {
    id: "family",
    header: "研究假设",
    value: (item) => item.hypothesis_family,
    wrap: true,
    cell: (item) => (
      <Tip content={`实验标识 ${item.experiment_id}`}>
        <strong className="exp-family">{item.hypothesis_family}</strong>
      </Tip>
    ),
  },
  {
    id: "status",
    header: "状态",
    value: (item) => statusLabel[item.status].label,
    cell: (item) => {
      const state = statusLabel[item.status];
      return <Pill kind={state.kind}>{state.label}</Pill>;
    },
  },
  {
    id: "registered",
    header: "登记时间",
    value: (item) => item.registered_at,
    cell: (item) => <RelativeTime at={item.registered_at} />,
  },
  {
    id: "return",
    header: "净收益",
    value: (item) => item.net_return_pct,
    numeric: true,
    cell: (item) => <ChangeText value={item.net_return_pct} />,
  },
  {
    id: "drawdown",
    header: "最大回撤",
    value: (item) => item.max_drawdown_pct,
    numeric: true,
    secondary: true,
    cell: (item) => formatPercent(item.max_drawdown_pct),
  },
  {
    id: "winrate",
    header: "胜率",
    value: (item) => item.win_rate_pct,
    numeric: true,
    secondary: true,
    cell: (item) => formatPercent(item.win_rate_pct),
  },
  {
    id: "trades",
    header: "交易数",
    value: (item) => item.trade_count,
    numeric: true,
    cell: (item) => formatCount(item.trade_count),
  },
];

export default function ExperimentsPage() {
  const meta = useCurrentMeta();
  const generationId = meta.data?.data.generation?.generation_id ?? null;
  const [pageState, setPageState] = useState<{
    generationId: string | null;
    cursors: (string | null)[];
  }>({ generationId: null, cursors: [null] });
  const [refreshKey, setRefreshKey] = useState(0);
  const cursors = pageState.generationId === generationId ? pageState.cursors : [null];
  const page = cursors.length;
  const query = useExperiments(cursors[page - 1] ?? null, generationId, refreshKey);

  const previous = () => setPageState({ generationId, cursors: cursors.slice(0, -1) });

  const reload = () => {
    setPageState({ generationId, cursors: [null] });
    setRefreshKey((value) => value + 1);
    void meta.refetch();
  };

  return (
    <>
      <PageHeader eyebrow="策略与验证" title="实验记录" note="查看已登记实验和已有结果" />
      {query.isLoading ? (
        <PageSkeleton label="正在加载实验记录" />
      ) : query.error ? (
        <Panel>
          <div className="exp-message" role="alert">
            <p>{query.error.message}</p>
            <Button size="sm" onClick={reload}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : !query.data?.available ? (
        <Panel>
          <EmptyState title="实验记录暂时读不到" hint="数据发布后会显示在这里。" />
          <div className="exp-actions">
            <Button size="sm" onClick={reload}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : query.data.items.length === 0 ? (
        <Panel>
          <EmptyState
            title={page === 1 ? "还没有登记的实验" : "这一页没有更多实验"}
            hint={page === 1 ? "完成登记后会显示在这里。" : "可返回上一页继续查看。"}
          />
          {page > 1 ? (
            <div className="exp-actions">
              <Button size="sm" onClick={previous}>
                上一页
              </Button>
            </div>
          ) : null}
        </Panel>
      ) : (
        <Panel title="实验记录" sub="参数、夏普和年化尚未发布" flush>
          {query.data.truncated ? (
            <p className="exp-window" role="status">
              仅显示最近 {formatCount(query.data.retained_count)} 条实验
              <Tip content="较早记录暂未纳入本页">
                <span> · 按登记时间排序</span>
              </Tip>
            </p>
          ) : null}
          <DataTable
            rows={query.data.items}
            columns={columns}
            rowKey={(item) => item.experiment_id}
            label="实验记录"
            emptyText="这一页没有更多实验"
          />
          <div className="exp-pages">
            <span className="exp-count">
              第 {formatCount(page)} 页 · 最近 {formatCount(query.data.retained_count)} 条
            </span>
            <Button size="sm" disabled={page === 1} onClick={previous}>
              上一页
            </Button>
            <Button
              size="sm"
              disabled={query.data.next_cursor === null}
              onClick={() => {
                const next = query.data?.next_cursor;
                if (next) setPageState({ generationId, cursors: [...cursors, next] });
              }}
            >
              下一页
            </Button>
          </div>
        </Panel>
      )}
    </>
  );
}
