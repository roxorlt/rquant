import { useState } from "react";
import {
  type StrategyCatalogItem,
  type StrategyParameter,
  useStrategyCatalog,
} from "@/api/strategies";
import { useCurrentMeta } from "@/api/useMeta";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, EmptyState, PageHeader, PageSkeleton, Panel, RelativeTime, Tip } from "@/ui";
import "./strategies.css";

const strategyColumns: DataColumn<StrategyCatalogItem>[] = [
  {
    id: "strategy",
    header: "策略",
    value: (row) => row.name,
    cell: (row) => <Tip content={`策略标识：${row.strategy_id}`}>{row.name}</Tip>,
  },
  {
    id: "version",
    header: "版本",
    value: (row) => row.version,
    cell: (row) => `第 ${row.version} 版`,
  },
  {
    id: "registered",
    header: "登记时间",
    value: (row) => row.registered_at,
    secondary: true,
    cell: (row) => <RelativeTime at={row.registered_at} />,
  },
];

const parameterColumns: DataColumn<StrategyParameter>[] = [
  {
    id: "parameter",
    header: "参数",
    value: (row) => row.label,
    cell: (row) => <Tip content={`参数标识：${row.key}`}>{row.label}</Tip>,
  },
  {
    id: "value",
    header: "当前值",
    value: (row) => row.display_value,
    cell: (row) => <span className="num">{row.display_value}</span>,
  },
];

export default function StrategiesPage() {
  const meta = useCurrentMeta();
  const currentGeneration =
    meta.data === undefined ? undefined : (meta.data.data.generation?.generation_id ?? null);
  const catalog = useStrategyCatalog(currentGeneration);
  const [selection, setSelection] = useState<{ generationId: string; strategyId: string } | null>(
    null,
  );
  const selectedId =
    selection !== null && selection.generationId === currentGeneration
      ? selection.strategyId
      : null;
  const rows = catalog.data?.strategies ?? [];
  const selected = rows.find((row) => row.strategy_id === selectedId) ?? rows[0] ?? null;
  const changed =
    currentGeneration !== undefined &&
    catalog.serving !== undefined &&
    currentGeneration !== catalog.serving.generation_id;

  return (
    <>
      <PageHeader eyebrow="策略与验证" title="策略" note="已核验的策略定义与当前参数" />
      {meta.isError ? (
        <Panel>
          <div className="strategy-state" role="alert">
            <p>策略目录暂时无法核对，请稍后重试。</p>
            <Button size="sm" onClick={() => void meta.refetch()}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : meta.data === undefined || (currentGeneration !== null && catalog.isLoading) ? (
        <PageSkeleton label="正在加载策略目录" />
      ) : currentGeneration === null ? (
        <Panel>
          <EmptyState title="策略目录暂时不可用" hint="数据恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : catalog.error || changed ? (
        <Panel>
          <div className="strategy-state" role="alert">
            <p>{changed ? "数据已更新，请重新查看策略。" : catalog.error?.message}</p>
            <Button size="sm" onClick={catalog.refetch}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : !catalog.data?.available ? (
        <Panel>
          <EmptyState title="策略目录暂时不可用" hint="已发布的定义恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : (
        <div className="strategy-stack">
          <Panel title="策略列表" sub="选择一项查看当前参数" flush>
            <DataTable
              rows={rows}
              columns={strategyColumns}
              rowKey={(row) => row.strategy_id}
              label="策略列表"
              selectedKey={selected?.strategy_id}
              onSelect={
                typeof currentGeneration === "string"
                  ? (row) =>
                      setSelection({ generationId: currentGeneration, strategyId: row.strategy_id })
                  : undefined
              }
              emptyText="当前没有可显示的策略定义"
            />
          </Panel>
          {selected ? (
            <Panel
              title={selected.name}
              sub={
                <span className="strategy-detail-meta">
                  <span>第 {selected.version} 版</span>
                  <span>
                    登记时间 <RelativeTime at={selected.registered_at} />
                  </span>
                </span>
              }
              flush
            >
              <DataTable
                rows={selected.parameters}
                columns={parameterColumns}
                rowKey={(row) => row.key}
                label="当前参数"
                emptyText="当前定义没有参数"
              />
            </Panel>
          ) : null}
        </div>
      )}
    </>
  );
}
