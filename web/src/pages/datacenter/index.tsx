import { useMemo, useState } from "react";
import {
  type CatalogDataset,
  type CatalogField,
  type CatalogSummary,
  useCatalog,
  useCatalogDataset,
} from "@/api/endpoints";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  EmptyState,
  PageHeader,
  PageSkeleton,
  Panel,
  SearchInput,
  Segmented,
  Tip,
} from "@/ui";
import "./datacenter.css";

const FIELD_COLUMNS: DataColumn<CatalogField>[] = [
  {
    id: "name",
    header: "字段",
    value: (field) => field.name,
    cell: (field) => (
      <Tip content={field.description}>
        <span className="dc-field-name">
          <span>{field.name}</span>
          <span className="mono dc-field-key">{field.key}</span>
        </span>
      </Tip>
    ),
    sortable: true,
  },
  {
    id: "type",
    header: "类型",
    value: (field) => field.data_type,
    cell: (field) => <span className="mono dc-field-type">{field.data_type}</span>,
  },
  { id: "unit", header: "单位", value: (field) => field.unit, cell: (field) => field.unit ?? "—" },
  {
    id: "description",
    header: "说明",
    value: (field) => field.description,
    wrap: true,
    secondary: true,
  },
];

function DatasetList({
  datasets,
  activeId,
  onSelect,
}: {
  datasets: readonly CatalogSummary[];
  activeId: string | null;
  onSelect: (id: string) => void;
}) {
  return (
    <ul className="dc-list" aria-label="数据集">
      {datasets.map((dataset) => (
        <li key={dataset.dataset_id}>
          <button
            className="dc-dataset"
            type="button"
            aria-current={activeId === dataset.dataset_id ? "true" : undefined}
            onClick={() => onSelect(dataset.dataset_id)}
          >
            <span className="dc-dataset-top">
              <strong>{dataset.name}</strong>
              <span className="dc-dataset-category">{dataset.category}</span>
            </span>
            <span className="dc-dataset-purpose">{dataset.purpose}</span>
          </button>
        </li>
      ))}
    </ul>
  );
}

function DatasetDetail({ dataset }: { dataset: CatalogDataset }) {
  const [query, setQuery] = useState("");
  const needle = query.trim().toLocaleLowerCase();
  const fields = useMemo(
    () =>
      dataset.fields.filter((field) =>
        [field.name, field.key, field.description].some((value) =>
          value.toLocaleLowerCase().includes(needle),
        ),
      ),
    [dataset.fields, needle],
  );

  return (
    <div className="dc-detail-stack">
      <Panel title={dataset.name} sub={dataset.category}>
        <p className="dc-purpose">{dataset.purpose}</p>
        <dl className="dc-facts">
          <div>
            <dt>来源</dt>
            <dd>{dataset.sources.join("、")}</dd>
          </div>
          <div>
            <dt>更新要求</dt>
            <dd>{dataset.update_note}</dd>
          </div>
          <div>
            <dt>可见</dt>
            <dd>{dataset.visibility_note}</dd>
          </div>
          <div>
            <dt>主键</dt>
            <dd className="mono dc-key-list">{dataset.primary_key.join(" · ")}</dd>
          </div>
        </dl>
      </Panel>
      <Panel
        title="字段字典"
        sub={dataset.schema_available ? `${dataset.fields.length} 个字段` : undefined}
        actions={
          dataset.schema_available ? (
            <SearchInput
              value={query}
              onSearch={setQuery}
              label="搜索字段"
              placeholder="字段名或说明"
            />
          ) : undefined
        }
        flush
      >
        {dataset.schema_available ? (
          <DataTable
            label="字段字典"
            rows={fields}
            columns={FIELD_COLUMNS}
            rowKey={(field) => field.key}
            emptyText={<EmptyState title="没有匹配的字段" hint="换个关键词试试" />}
          />
        ) : (
          <EmptyState title="字段结构待发布" hint="这份数据尚无可核对的字段结构" />
        )}
      </Panel>
      <Panel title="样例数据">
        <EmptyState title="样例数据尚未发布" />
      </Panel>
    </div>
  );
}

export default function DataCenterPage() {
  const catalog = useCatalog();
  const [category, setCategory] = useState("全部");
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [mobileDetail, setMobileDetail] = useState(false);
  const datasets = catalog.data?.datasets ?? [];
  const categories = useMemo(
    () => ["全部", ...new Set(datasets.map((item) => item.category))],
    [datasets],
  );
  const filtered = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    return datasets.filter(
      (item) =>
        (category === "全部" || item.category === category) &&
        [item.name, item.purpose, ...item.sources].some((text) =>
          text.toLocaleLowerCase().includes(needle),
        ),
    );
  }, [datasets, category, query]);
  const activeId = filtered.some((item) => item.dataset_id === selectedId)
    ? selectedId
    : (filtered[0]?.dataset_id ?? null);
  const detail = useCatalogDataset(activeId);

  if (catalog.isLoading) {
    return <PageSkeleton label="数据目录加载中" />;
  }

  if (!catalog.data) {
    return (
      <div className="data-center">
        <PageHeader eyebrow="数据" title="数据中心" />
        <Panel>
          <EmptyState title="暂时读不到数据目录" hint="稍后刷新页面再试" />
        </Panel>
      </div>
    );
  }

  return (
    <div className={`data-center${mobileDetail ? " dc-show-detail" : ""}`}>
      <PageHeader eyebrow="数据" title="数据中心" note={`${datasets.length} 份数据`} />
      <div className="dc-layout">
        <section className="dc-index" aria-label="数据目录">
          <div className="dc-index-head">
            <SearchInput
              value={query}
              onSearch={setQuery}
              label="搜索数据集"
              placeholder="搜索数据集"
            />
            <div className="dc-category-scroll">
              <Segmented
                label="按数据分类"
                options={categories.map((item) => ({ value: item, label: item }))}
                value={category}
                onChange={setCategory}
              />
            </div>
          </div>
          {filtered.length ? (
            <DatasetList
              datasets={filtered}
              activeId={activeId}
              onSelect={(id) => {
                setSelectedId(id);
                setMobileDetail(true);
              }}
            />
          ) : (
            <div className="dc-list-empty">
              <EmptyState
                title={datasets.length ? "没有匹配的数据集" : "还没有数据集说明"}
                hint={datasets.length ? "换个分类或关键词试试" : "目录发布后会在这里显示"}
              />
            </div>
          )}
        </section>
        <section className="dc-detail" aria-label="数据集详情">
          <Button size="sm" variant="ghost" onClick={() => setMobileDetail(false)}>
            ← 返回目录
          </Button>
          {activeId === null ? (
            <Panel>
              <EmptyState title="选一份数据查看字段" />
            </Panel>
          ) : detail.isLoading ? (
            <PageSkeleton label="字段说明加载中" />
          ) : detail.data ? (
            <DatasetDetail key={detail.data.dataset_id} dataset={detail.data} />
          ) : (
            <Panel>
              <EmptyState title="暂时读不到字段说明" hint="返回目录后重试" />
            </Panel>
          )}
        </section>
      </div>
    </div>
  );
}
