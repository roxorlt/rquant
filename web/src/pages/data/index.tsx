import { useNavigate } from "react-router";
import { useDataCenter, useFundamentals } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import { datasetLabel } from "@/copy/publicText";
import { toneClass, toneOf } from "@/format/color";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { AppDrawer, PageHeader, Panel, Pill } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";

function Fundamentals() {
  const query = useFundamentals();
  const doc = query.data;
  if (!doc) {
    return (
      <Panel title="财务指标" sub="还没有财务指标。">
        <p className="hint">每只股票显示最近一个报告期。</p>
      </Panel>
    );
  }
  const keys = Object.keys(doc.fields);
  return (
    <Panel
      title={`财务指标 · ${doc.rows.length}`}
      sub={`财务指标，更新于 ${formatShanghaiDateTime(doc.fetched_at)}`}
      flush
    >
      <DataTable
        label="财务指标"
        tools="财务指标"
        rows={doc.rows}
        rowKey={(r) => r.code}
        columns={[
          {
            id: "code",
            header: "代码",
            value: (r) => r.code,
            cell: (r) => <StockLink code={r.code} />,
          },
          { id: "end", header: "报告期", value: (r) => r.end_date },
          ...keys.map((k) => ({
            id: k,
            header: doc.fields[k] ?? k,
            numeric: true,
            value: (r: (typeof doc.rows)[number]) => r.values[k] ?? null,
            cell: (r: (typeof doc.rows)[number]) => {
              const v = r.values[k];
              if (v === null || v === undefined) return "—";
              // Growth rates (同比) carry direction: red up / green down.
              return /同比|增长/.test(doc.fields[k] ?? "") ? (
                <span className={toneClass(toneOf(v))}>{v.toFixed(2)}</span>
              ) : (
                v.toFixed(2)
              );
            },
          })),
        ]}
      />
    </Panel>
  );
}

export default function DataCenterPage() {
  const query = useDataCenter();
  const [picked, setPicked] = useUrlState("dataset");
  const navigate = useNavigate();
  return (
    <>
      <PageHeader eyebrow="数据" title="数据中心" />
      <QueryView query={query}>
        {(data) => {
          const coverage = new Map((data.audit?.datasets ?? []).map((c) => [c.dataset_id, c]));
          const dataset = data.datasets.find((d) => d.dataset_id === picked);
          return (
            <>
              <Panel
                title={`数据集 · ${data.datasets.length}`}
                sub={
                  data.audit
                    ? `覆盖审计 ${formatShanghaiDateTime(data.audit.generated_at)}，窗口 ${data.audit.window_start} ~ ${data.audit.window_end}（${data.audit.open_days} 个交易日）`
                    : "还没有覆盖审计。"
                }
                flush
              >
                <DataTable
                  label="数据集"
                  rows={data.datasets}
                  rowKey={(row) => row.dataset_id}
                  onSelect={(row) => setPicked(row.dataset_id)}
                  selectedKey={picked}
                  columns={[
                    { id: "name", header: "名称", value: (row) => row.name },
                    { id: "table", header: "表", value: (row) => row.table_name },
                    { id: "cat", header: "类别", value: (row) => row.category },
                    { id: "src", header: "来源", value: (row) => row.sources.join(" / ") },
                    { id: "upd", header: "更新", value: (row) => row.update_note },
                    {
                      id: "latest",
                      header: "最新日期",
                      value: (row) => coverage.get(row.dataset_id)?.latest_date ?? null,
                    },
                    {
                      id: "missing",
                      header: "缺失交易日",
                      value: (row) =>
                        coverage.get(row.dataset_id)?.missing_open_days.length ?? null,
                      cell: (row) => {
                        const c = coverage.get(row.dataset_id);
                        if (!c) return "—";
                        if (c.error) return <Pill kind="warn">读取失败</Pill>;
                        const n = c.missing_open_days.length;
                        return n ? (
                          <Pill kind="warn">{`缺 ${n} 天`}</Pill>
                        ) : (
                          <Pill kind="ok">齐</Pill>
                        );
                      },
                    },
                  ]}
                />
              </Panel>
              <AppDrawer
                open={dataset !== undefined}
                onClose={() => navigate(-1)}
                title={dataset?.name ?? ""}
                label={dataset ? `${dataset.name} 字段` : "字段"}
                extra={<span>{dataset ? datasetLabel(dataset.table_name) : ""}</span>}
              >
                {dataset ? (
                  <>
                    <p className="hint">{dataset.purpose}</p>
                    <DataTable
                      label="字段"
                      rows={dataset.fields}
                      rowKey={(row) => row.key}
                      columns={[
                        { id: "key", header: "字段", value: (row) => row.key },
                        { id: "name", header: "名称", value: (row) => row.name },
                        { id: "type", header: "类型", value: (row) => row.data_type },
                        { id: "unit", header: "单位", value: (row) => row.unit ?? "" },
                        { id: "desc", header: "说明", value: (row) => row.description },
                      ]}
                    />
                  </>
                ) : null}
              </AppDrawer>
              <Panel title="研究库覆盖" sub="行情库各表最近 30 天" flush>
                <DataTable
                  label="研究库覆盖"
                  rows={(data.audit?.datasets ?? []).filter((c) => !c.error)}
                  rowKey={(c) => c.dataset_id}
                  columns={[
                    { id: "table", header: "表", value: (c) => datasetLabel(c.table_name) },
                    { id: "latest", header: "最新日期", value: (c) => c.latest_date ?? null },
                    {
                      id: "rows",
                      header: "窗口行数",
                      numeric: true,
                      value: (c) => c.rows_in_window,
                    },
                    {
                      id: "missing",
                      header: "缺失交易日",
                      numeric: true,
                      value: (c) => c.missing_open_days.length,
                    },
                  ]}
                />
              </Panel>
            </>
          );
        }}
      </QueryView>
      <Fundamentals />
    </>
  );
}
