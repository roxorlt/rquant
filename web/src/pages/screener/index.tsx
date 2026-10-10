import { useState } from "react";
import { type ScreenRow, useAddWatch, useSavePool, useScreen } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, ChangeText, PageHeader, Panel, Segmented, useToast } from "@/ui";
import { QueryView } from "../shared";
import { Conditions } from "./Conditions";

const ALL = "__all__";

export default function ScreenerPage() {
  const readOnly = useReadOnlyReason();
  const [preset, setPreset] = useState<string>(ALL);
  const query = useScreen(preset === ALL ? null : preset);
  const savePool = useSavePool();
  const addWatch = useAddWatch();
  const toast = useToast();
  const presets = query.data?.presets ?? [];

  const save = () => {
    const name = window.prompt("池子名称", preset === ALL ? "选股结果" : preset);
    if (!name) return;
    savePool.mutate(
      { name, description: "", pool_refs: preset === ALL ? presets : [preset] },
      {
        onSuccess: (r) => toast(`已提交保存：${r.status}`),
        onError: (e) => toast(`保存失败：${e.message}`),
      },
    );
  };
  const watch = (row: ScreenRow) =>
    addWatch.mutate(row.code, {
      onSuccess: (r) => toast(`${row.code} 加入自选：${r.status}`),
      onError: (e) => toast(`加入自选失败：${e.message}`),
    });

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="选股与排序"
        note={query.data?.trade_date ?? undefined}
        actions={
          <Button
            onClick={save}
            disabledReason={readOnly ?? (presets.length ? undefined : "没有可保存的结果")}
          >
            保存为池子
          </Button>
        }
      />
      <Segmented
        label="选股方案"
        value={preset}
        onChange={setPreset}
        options={[{ value: ALL, label: "全部" }, ...presets.map((p) => ({ value: p, label: p }))]}
      />
      <QueryView query={query}>
        {(data) => (
          <Panel title={`${data.rows.length} 只`} flush>
            <DataTable
              label="选股结果"
              rows={data.rows}
              rowKey={(row) => `${row.preset}:${row.code}`}
              initialSort={{ id: "pct", desc: true }}
              height={560}
              emptyText="当天没有选股结果"
              columns={[
                { id: "code", header: "代码", value: (row) => row.code },
                { id: "name", header: "名称", value: (row) => row.name ?? null },
                { id: "preset", header: "方案", value: (row) => row.preset, secondary: true },
                {
                  id: "close",
                  header: "收盘",
                  numeric: true,
                  value: (row) => row.close ?? null,
                  cell: (row) => formatPrice(row.close),
                },
                {
                  id: "pct",
                  header: "涨跌幅",
                  numeric: true,
                  value: (row) => row.pct_chg ?? null,
                  cell: (row) => <ChangeText value={row.pct_chg ?? null} />,
                },
                {
                  id: "watch",
                  header: "",
                  sortable: false,
                  value: () => null,
                  cell: (row) => (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => watch(row)}
                      disabledReason={readOnly}
                    >
                      加自选
                    </Button>
                  ),
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      <Conditions />
    </>
  );
}
