import { useMemo, useState } from "react";
import { useSearchParams } from "react-router";
import { type ScreenRow, useAddWatch, useSavePool, useScreen } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { Button, ChangeText, FlowSteps, PageHeader, Panel, Segmented, useToast } from "@/ui";
import { QueryView } from "../shared";
import { applyFilters, FIELDS, type Field, type Filter, type Rank, score } from "./Builder";
import { Conditions } from "./Conditions";

function stepLabel(filters: Filter[], i: number): string {
  const f = filters[i - 1];
  return f ? `${FIELDS[f.field]} ${f.op} ${f.value}` : "股票池";
}

function FilterRows({ filters, onChange }: { filters: Filter[]; onChange: (f: Filter[]) => void }) {
  const set = (i: number, patch: Partial<Filter>) =>
    onChange(filters.map((f, j) => (j === i ? { ...f, ...patch } : f)));
  return (
    <div>
      {filters.map((f, i) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: rows are positional.
        <div className="cond-row" key={i}>
          <span className="mono">{i + 1}</span>
          <select
            aria-label="字段"
            value={f.field}
            onChange={(e) => set(i, { field: e.target.value as Field })}
          >
            {Object.entries(FIELDS).map(([k, v]) => (
              <option key={k} value={k}>
                {v}
              </option>
            ))}
          </select>
          <select
            aria-label="比较"
            value={f.op}
            onChange={(e) => set(i, { op: e.target.value as Filter["op"] })}
          >
            <option value=">=">≥</option>
            <option value="<=">≤</option>
          </select>
          <input
            aria-label="阈值"
            type="number"
            value={f.value}
            onChange={(e) => set(i, { value: Number(e.target.value) })}
          />
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onChange(filters.filter((_, j) => j !== i))}
          >
            ×
          </Button>
        </div>
      ))}
      <Button
        size="sm"
        variant="ghost"
        onClick={() => onChange([...filters, { field: "pct_chg", op: ">=", value: 0 }])}
      >
        + 添加条件
      </Button>
    </div>
  );
}

function RankRows({ ranks, onChange }: { ranks: Rank[]; onChange: (r: Rank[]) => void }) {
  const set = (i: number, patch: Partial<Rank>) =>
    onChange(ranks.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  return (
    <div>
      {ranks.map((r, i) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: rows are positional.
        <div className="cond-row" key={i}>
          <span className="mono">{i + 1}</span>
          <select
            aria-label="排名字段"
            value={r.field}
            onChange={(e) => set(i, { field: e.target.value as Field })}
          >
            {Object.entries(FIELDS).map(([k, v]) => (
              <option key={k} value={k}>
                {v}
              </option>
            ))}
          </select>
          <select
            aria-label="方向"
            value={r.desc ? "d" : "a"}
            onChange={(e) => set(i, { desc: e.target.value === "d" })}
          >
            <option value="d">降序</option>
            <option value="a">升序</option>
          </select>
          <input
            aria-label="权重"
            type="number"
            value={r.weight}
            onChange={(e) => set(i, { weight: Number(e.target.value) })}
          />
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onChange(ranks.filter((_, j) => j !== i))}
          >
            ×
          </Button>
        </div>
      ))}
      <Button
        size="sm"
        variant="ghost"
        onClick={() => onChange([...ranks, { field: "close", desc: false, weight: 50 }])}
      >
        + 添加排名
      </Button>
    </div>
  );
}

const ALL = "__all__";

export default function ScreenerPage() {
  const readOnly = useReadOnlyReason();
  const [preset, setPreset] = useState<string>(ALL);
  const query = useScreen(preset === ALL ? null : preset);
  const savePool = useSavePool();
  const addWatch = useAddWatch();
  const toast = useToast();
  const presets = query.data?.presets ?? [];
  const [params] = useSearchParams();
  const q = (params.get("q") ?? "").trim();
  const [filters, setFilters] = useState<Filter[]>([]);
  const [ranks, setRanks] = useState<Rank[]>([{ field: "pct_chg", desc: true, weight: 100 }]);
  const base = useMemo(
    () =>
      (query.data?.rows ?? []).filter(
        (r) => !q || r.code.includes(q) || (r.name ?? "").includes(q),
      ),
    [query.data, q],
  );
  const filtered = useMemo(() => applyFilters(base, filters), [base, filters]);
  const scores = useMemo(() => score(filtered.rows, ranks), [filtered, ranks]);

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
        title="选股器"
        note={query.data?.trade_date ?? undefined}
        actions={
          <>
            <FlowSteps />
            <Button
              onClick={save}
              disabledReason={readOnly ?? (presets.length ? undefined : "没有可保存的结果")}
            >
              保存为池子
            </Button>
          </>
        }
      />
      <Segmented
        label="选股方案"
        value={preset}
        onChange={setPreset}
        options={[{ value: ALL, label: "全部" }, ...presets.map((p) => ({ value: p, label: p }))]}
      />
      <Panel title="条件" sub={q ? `搜索：${q}` : "门槛全部满足才保留；排名按加权分位打分"}>
        <div className="cond-grid">
          <div>
            <h3 className="sub">筛选（门槛）</h3>
            {filters.length === 0 ? <p className="cond-none">无门槛，全部股票进入排名</p> : null}
            <FilterRows filters={filters} onChange={setFilters} />
          </div>
          <div>
            <h3 className="sub">排名（权重）</h3>
            {ranks.length === 0 ? <p className="cond-none">未设排名，按原顺序输出</p> : null}
            <RankRows ranks={ranks} onChange={setRanks} />
          </div>
        </div>
        <ol className="funnel-inline" aria-label="逐条命中">
          {filtered.steps.map((n, i) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: steps are positional.
            <li key={i}>
              <span className="fl">{stepLabel(filters, i)}</span>
              <span className="fv num">{n}</span>
            </li>
          ))}
        </ol>
      </Panel>
      <QueryView query={query}>
        {() => (
          <Panel title={`结果 · 命中 ${filtered.rows.length} 只`} flush>
            <DataTable
              label="选股结果"
              rows={filtered.rows}
              rowKey={(row) => `${row.preset}:${row.code}`}
              initialSort={{ id: "score", desc: true }}
              height={560}
              emptyText="当天没有选股结果：盘后选股尚未产出，或该方案今天无命中。可切换方案，或用下方条件选股自定义。"
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
                  id: "score",
                  header: "排名分",
                  numeric: true,
                  value: (row) => scores.get(row) ?? null,
                  cell: (row) => (scores.get(row) ?? 0).toFixed(1),
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
