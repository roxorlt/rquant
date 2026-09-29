import { useState } from "react";
import { type FactorDefinitionItem, useFactorCatalog } from "@/api/factors";
import { useCurrentMeta } from "@/api/useMeta";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, EmptyState, PageHeader, PageSkeleton, Panel, RelativeTime, Tip } from "@/ui";
import "./factors.css";

const columns: DataColumn<FactorDefinitionItem>[] = [
  {
    id: "name",
    header: "因子",
    value: (row) => row.name_zh,
    cell: (row) => (
      <span className="factor-name-cell">
        <span>{row.name_zh}</span>
        {row.archived ? <span className="factor-archive">已归档</span> : null}
      </span>
    ),
    sortable: true,
  },
  { id: "category", header: "分类", value: (row) => row.category_label, secondary: true },
  {
    id: "version",
    header: "版本",
    value: (row) => row.version,
    cell: (row) => `第 ${row.version} 版`,
    secondary: true,
  },
];

export default function FactorsPage() {
  const meta = useCurrentMeta();
  const currentGeneration =
    meta.data === undefined ? undefined : (meta.data.data.generation?.generation_id ?? null);
  const catalog = useFactorCatalog(currentGeneration);
  const [selection, setSelection] = useState<{ generationId: string; factorId: string } | null>(
    null,
  );
  const selectedId =
    selection !== null && selection.generationId === currentGeneration ? selection.factorId : null;
  const changed =
    currentGeneration !== undefined &&
    catalog.serving !== undefined &&
    currentGeneration !== catalog.serving.generation_id;
  const rows = changed ? [] : (catalog.data?.definitions ?? []);
  const selected = rows.find((row) => row.factor_id === selectedId) ?? rows[0] ?? null;
  const refreshDefinitions = async () => {
    const refreshed = await meta.refetch();
    if (refreshed.isError || refreshed.data === undefined) return;
    const nextGeneration = refreshed.data.data.generation?.generation_id ?? null;
    if (nextGeneration !== currentGeneration) {
      setSelection(null);
    } else if (typeof nextGeneration === "string") {
      catalog.refetch();
    }
  };

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="因子研究"
        note="查看已发布因子的当前定义"
        actions={
          <Button
            size="sm"
            onClick={() => void refreshDefinitions()}
            disabled={
              currentGeneration === null || currentGeneration === undefined || catalog.isFetching
            }
          >
            刷新
          </Button>
        }
      />
      {meta.isError ? (
        <Panel>
          <div className="factor-state" role="alert">
            <p>因子库暂时无法核对，请稍后重试。</p>
            <Button size="sm" onClick={() => void meta.refetch()}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : meta.data === undefined || (currentGeneration !== null && catalog.isLoading) ? (
        <PageSkeleton label="正在加载因子库" />
      ) : currentGeneration === null ? (
        <Panel>
          <EmptyState title="因子库暂时无法查看" hint="数据恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : catalog.error || changed ? (
        <Panel>
          <div className="factor-state" role="alert">
            <p>{changed ? "数据已更新，请重新查看因子。" : catalog.error?.message}</p>
            <Button size="sm" onClick={() => void refreshDefinitions()}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : catalog.data?.availability === "unavailable" ? (
        <Panel>
          <EmptyState title="因子库暂时无法查看" hint="数据恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : catalog.data?.availability === "empty" ? (
        <Panel>
          <EmptyState title="还没有因子" hint="保存因子后会显示在这里。" />
        </Panel>
      ) : (
        <div className="factor-layout">
          <Panel
            title="因子列表"
            sub={
              catalog.data?.available_at ? (
                <span>
                  {rows.length} 个因子 · <RelativeTime at={catalog.data.available_at} />
                  更新
                </span>
              ) : undefined
            }
            flush
          >
            <DataTable
              rows={rows}
              columns={columns}
              rowKey={(row) => row.factor_id}
              label="因子列表"
              selectedKey={selected?.factor_id}
              onSelect={
                typeof currentGeneration === "string"
                  ? (row) =>
                      setSelection({ generationId: currentGeneration, factorId: row.factor_id })
                  : undefined
              }
              emptyText="还没有因子"
            />
          </Panel>
          {selected ? (
            <Panel
              title={selected.name_zh}
              label="因子详情"
              sub={
                <span>
                  第 {selected.version} 版{selected.archived ? " · 已归档" : ""}
                </span>
              }
            >
              <div className="factor-detail">
                <div className="factor-detail-lead">
                  <span className="factor-detail-category">{selected.category_label}</span>
                  <span>{selected.direction_label}</span>
                </div>
                <dl className="factor-facts">
                  <div>
                    <dt>最早可用</dt>
                    <dd>{selected.earliest_available_date}</dd>
                  </div>
                  <div>
                    <dt>历史窗口</dt>
                    <dd>{selected.max_history_window} 个交易日</dd>
                  </div>
                  <div>
                    <dt>使用字段</dt>
                    <dd>{selected.dependency_columns.join("、") || "无"}</dd>
                  </div>
                </dl>
                <div className="factor-expression">
                  <div className="factor-expression-head">
                    <h3>计算表达式</h3>
                    <Tip content={`因子标识：${selected.factor_id}`}>查看标识</Tip>
                  </div>
                  <code>{selected.expression}</code>
                </div>
              </div>
            </Panel>
          ) : null}
        </div>
      )}
    </>
  );
}
