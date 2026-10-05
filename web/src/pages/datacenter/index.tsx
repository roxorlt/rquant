import { useMemo, useState, useSyncExternalStore } from "react";
import { submitBackfillPlanCommand } from "@/api/backfillPlanCommand";
import {
  type CatalogDataset,
  type CatalogField,
  type CatalogSummary,
  useCatalog,
  useCatalogDataset,
} from "@/api/endpoints";
import { useMeta } from "@/api/useMeta";
import { formatNumber, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
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
import { AuditPanel } from "./AuditPanel";
import { BackfillPlanPanel } from "./BackfillPlanPanel";
import { BackfillPlanCommandSession } from "./backfillPlanCommandSession";
import { DailyReportPanel } from "./DailyReportPanel";
import { FinancialPanel } from "./FinancialPanel";
import "./datacenter.css";

const FIELD_COLUMNS: DataColumn<CatalogField>[] = [
  {
    id: "name",
    header: "字段",
    value: (field) => field.name,
    cell: (field) => (
      <Tip content={`${field.description} · ${field.key}`}>
        <span className="dc-field-name">
          <span>{field.name}</span>
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

type SampleRow = CatalogDataset["sample"]["rows"][number];

function sampleText(field: CatalogField, value: SampleRow[string] | undefined): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") {
    if (field.unit === "%") return `${formatNumber(value, 2)}%`;
    if (field.key === "close" || field.key === "price") return formatPrice(value);
    return value.toLocaleString("zh-CN", { maximumFractionDigits: 6 });
  }
  if (field.data_type.toUpperCase().startsWith("TIMESTAMP WITH TIME ZONE")) {
    return formatShanghaiDateTime(value);
  }
  if (field.data_type.toUpperCase().startsWith("TIMESTAMP")) return value.replace("T", " ");
  return value;
}

function sampleColumns(fields: CatalogField[]): DataColumn<SampleRow>[] {
  return fields.map((field) => ({
    id: field.key,
    header: field.name,
    value: (row) => {
      const value = row[field.key];
      return typeof value === "boolean" ? (value ? "是" : "否") : (value ?? null);
    },
    numeric: ["DOUBLE", "FLOAT", "INTEGER", "BIGINT", "SMALLINT"].includes(field.data_type),
    cell: (row) => {
      const display = sampleText(field, row[field.key]);
      return display.length > 22 ? (
        <Tip content={display}>
          <span className="dc-sample-truncated">{display.slice(0, 22)}…</span>
        </Tip>
      ) : (
        display
      );
    },
  }));
}

const SAMPLE_EMPTY_COPY: Record<Exclude<CatalogDataset["sample"]["state"], "available">, string> = {
  unpublished: "样例数据尚未发布",
  empty: "这份数据暂时没有记录",
  missing: "这份数据尚未接入样例",
  unsupported: "这份数据暂时没有可展示的字段",
  stale: "样例数据需要更新",
  error: "暂时读不到样例数据",
};

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
  const [showHistoricalAudit, setShowHistoricalAudit] = useState(false);
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
            <dd>
              <Tip content={dataset.primary_key.join(" · ")}>
                <span>
                  {dataset.primary_key
                    .map((key) => dataset.fields.find((field) => field.key === key)?.name ?? "字段")
                    .join(" · ")}
                </span>
              </Tip>
            </dd>
          </div>
        </dl>
      </Panel>
      <DailyReportPanel datasetId={dataset.dataset_id} />
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
      <Panel
        title="样例数据"
        sub={
          dataset.sample.state === "available" ? `最近 ${dataset.sample.rows.length} 条` : undefined
        }
        flush={dataset.sample.state === "available"}
      >
        {dataset.sample.state === "available" && dataset.sample_fields.length ? (
          <div className="dc-sample-table">
            <DataTable
              label="样例数据"
              rows={dataset.sample.rows.slice(0, 20)}
              columns={sampleColumns(dataset.sample_fields)}
              rowKey={(row) => String(dataset.sample.rows.indexOf(row))}
            />
          </div>
        ) : (
          <EmptyState
            title={
              SAMPLE_EMPTY_COPY[
                dataset.sample.state === "available" ? "error" : dataset.sample.state
              ]
            }
          />
        )}
      </Panel>
      {dataset.dataset_id === "daily_bar" ? (
        <div className="dc-history">
          <Button
            size="sm"
            variant="ghost"
            aria-expanded={showHistoricalAudit}
            onClick={() => setShowHistoricalAudit((value) => !value)}
          >
            {showHistoricalAudit ? "收起历史审计记录" : "查看历史审计记录"}
          </Button>
          {showHistoricalAudit ? <AuditPanel datasetId={dataset.dataset_id} /> : null}
        </div>
      ) : (
        <AuditPanel datasetId={dataset.dataset_id} />
      )}
    </div>
  );
}

function CatalogView() {
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
      <Panel>
        <EmptyState title="暂时读不到数据目录" hint="稍后刷新页面再试" />
      </Panel>
    );
  }

  return (
    <div className={mobileDetail ? "dc-show-detail" : ""}>
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

export default function DataCenterPage() {
  const meta = useMeta();
  const [commandSession] = useState(
    () =>
      new BackfillPlanCommandSession(
        (() => {
          try {
            return window.sessionStorage;
          } catch {
            return {
              getItem: () => {
                throw new Error("storage unavailable");
              },
            } as unknown as Storage;
          }
        })(),
        submitBackfillPlanCommand,
        () =>
          `web-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (item) => item.toString(16).padStart(2, "0")).join("")}`,
        () => new Date().toISOString(),
      ),
  );
  const command = useSyncExternalStore(
    commandSession.subscribe,
    commandSession.snapshot,
    commandSession.snapshot,
  );
  const [view, setView] = useState<"catalog" | "financial" | "plans">(() =>
    commandSession.snapshot().journal ? "plans" : "catalog",
  );
  const [requestOpen, setRequestOpen] = useState(false);
  const pending =
    command.journal !== null && !["queued", "failed"].includes(command.journal.status);
  const disabledReason = !meta.data
    ? "正在加载用户信息。"
    : !meta.data.data.viewer
      ? "请先登录，才能生成回补计划。"
      : !command.storageAvailable
        ? "浏览器存储不可用，无法安全提交。"
        : pending
          ? "请先核对上一次请求。"
          : undefined;
  return (
    <div className="data-center">
      <PageHeader
        eyebrow="数据"
        title="数据中心"
        actions={
          <Button
            variant="primary"
            disabledReason={disabledReason}
            onClick={() => {
              setView("plans");
              setRequestOpen(true);
            }}
          >
            生成回补计划
          </Button>
        }
      />
      <div className="dc-view-switch">
        <Segmented
          label="数据中心内容"
          options={[
            { value: "catalog", label: "数据目录" },
            { value: "financial", label: "财务" },
            { value: "plans", label: "回补计划" },
          ]}
          value={view}
          onChange={setView}
        />
      </div>
      {view === "catalog" ? (
        <CatalogView />
      ) : view === "financial" ? (
        <FinancialPanel />
      ) : (
        <BackfillPlanPanel
          commandSession={commandSession}
          command={command}
          canSubmit={!!meta.data?.data.viewer}
          requestOpen={requestOpen}
          onCloseRequest={() => setRequestOpen(false)}
        />
      )}
    </div>
  );
}
