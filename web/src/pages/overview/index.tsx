import { useOverview } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { KpiStrip, PageHeader, Panel } from "@/ui";
import { QueryView } from "../shared";

export default function OverviewPage() {
  const query = useOverview();
  return (
    <>
      <PageHeader eyebrow="概览" title="总览" note={query.data?.trade_date ?? undefined} />
      <QueryView query={query}>
        {(data) => (
          <>
            <KpiStrip
              label="关键数字"
              items={data.kpis.map((kpi) => ({ ...kpi, tone: kpi.tone ?? undefined }))}
            />
            <Panel title="最新信号" flush>
              <DataTable
                label="最新信号"
                rows={data.signals}
                rowKey={(row) => String(row.sequence)}
                emptyText="今天还没有信号"
                columns={[
                  {
                    id: "at",
                    header: "时间",
                    value: (row) => row.at ?? null,
                    cell: (row) => (row.at ? formatShanghaiDateTime(row.at) : "—"),
                  },
                  { id: "code", header: "代码", value: (row) => row.code },
                  { id: "name", header: "名称", value: (row) => row.name ?? null },
                  { id: "strategy", header: "策略", value: (row) => row.strategy_id },
                  { id: "action", header: "动作", value: (row) => row.action },
                ]}
              />
            </Panel>
          </>
        )}
      </QueryView>
    </>
  );
}
