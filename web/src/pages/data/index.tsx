import { useState } from "react";
import { useDataCenter } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { PageHeader, Panel, Pill } from "@/ui";
import { QueryView } from "../shared";

export default function DataCenterPage() {
  const query = useDataCenter();
  const [picked, setPicked] = useState<string | null>(null);
  return (
    <>
      <PageHeader eyebrow="运维" title="数据中心" />
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
                    : "尚无覆盖审计：python -m rquant.data_catalog.audit"
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
              {dataset ? (
                <Panel title={dataset.name} sub={dataset.purpose} flush>
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
                  {coverage.get(dataset.dataset_id)?.missing_open_days.length ? (
                    <p className="sub">
                      缺失：{coverage.get(dataset.dataset_id)?.missing_open_days.join("、")}
                    </p>
                  ) : null}
                </Panel>
              ) : null}
            </>
          );
        }}
      </QueryView>
    </>
  );
}
