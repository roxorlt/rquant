import { useState } from "react";
import { usePools } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { EmptyState, PageHeader, Panel, Segmented } from "@/ui";
import { QueryView } from "../shared";

export default function PoolsPage() {
  const query = usePools();
  const [selected, setSelected] = useState<string | null>(null);
  return (
    <>
      <PageHeader eyebrow="研究" title="池子" note={query.data?.trade_date ?? undefined} />
      <QueryView query={query}>
        {(data) => {
          if (!data.pools.length) return <EmptyState title="还没有池子" hint="在选股页保存一个" />;
          const pool = data.pools.find((p) => p.name === selected) ?? data.pools[0];
          if (!pool) return null;
          return (
            <>
              <Segmented
                label="池子"
                value={pool.name}
                onChange={setSelected}
                options={data.pools.map((p) => ({ value: p.name, label: p.name }))}
              />
              <Panel
                title={`${pool.name} · ${pool.members.length} 只`}
                sub={[
                  pool.description,
                  pool.pool_refs.join(" + "),
                  pool.updated_at ? `更新于 ${formatShanghaiDateTime(pool.updated_at)}` : "",
                ]
                  .filter(Boolean)
                  .join(" · ")}
                flush
              >
                <DataTable
                  label="池子成员"
                  rows={pool.members}
                  rowKey={(row) => `${row.preset}:${row.code}`}
                  emptyText="今天没有成员"
                  columns={[
                    { id: "code", header: "代码", value: (row) => row.code },
                    { id: "name", header: "名称", value: (row) => row.name ?? null },
                    { id: "preset", header: "来源", value: (row) => row.preset },
                  ]}
                />
              </Panel>
            </>
          );
        }}
      </QueryView>
    </>
  );
}
