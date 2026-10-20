import { useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { type ScreenRow, useAddWatch, usePools, useSavePool, useScreen } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import {
  AppDrawer,
  Button,
  ChangeText,
  FailureNotice,
  FlowSteps,
  PageHeader,
  Panel,
  ReadOnlyBanner,
  Segmented,
  useToast,
} from "@/ui";
import { type FailureCopy, failureCopy } from "@/ui/failure";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import {
  applyFilters,
  BASE_FIELDS,
  type Field,
  type Filter,
  fieldsFor,
  type Rank,
  score,
} from "./Builder";
import { Conditions } from "./Conditions";
import { FieldPicker } from "./FieldPicker";
import { Schemes } from "./Schemes";

function stepLabel(filters: Filter[], i: number): string {
  const f = filters[i - 1];
  return f
    ? `${BASE_FIELDS[f.field] ?? f.field.replace(/^f:/, "因子·")} ${f.op} ${f.value}`
    : "股票池";
}

function FilterRows({
  filters,
  onChange,
  fields,
}: {
  filters: Filter[];
  onChange: (f: Filter[]) => void;
  fields: Record<string, string>;
}) {
  const set = (i: number, patch: Partial<Filter>) =>
    onChange(filters.map((f, j) => (j === i ? { ...f, ...patch } : f)));
  return (
    <div>
      {filters.map((f, i) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: rows are positional.
        <div className="cond-row" key={i}>
          <span className="mono">{i + 1}</span>
          <FieldPicker
            label="字段"
            value={f.field}
            fields={fields}
            onChange={(v) => set(i, { field: v as Field })}
          />
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

function RankRows({
  ranks,
  onChange,
  fields,
}: {
  ranks: Rank[];
  onChange: (r: Rank[]) => void;
  fields: Record<string, string>;
}) {
  const set = (i: number, patch: Partial<Rank>) =>
    onChange(ranks.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  return (
    <div>
      {ranks.map((r, i) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: rows are positional.
        <div className="cond-row" key={i}>
          <span className="mono">{i + 1}</span>
          <FieldPicker
            label="排名字段"
            value={r.field}
            fields={fields}
            onChange={(v) => set(i, { field: v as Field })}
          />
          <select
            aria-label="方向"
            className="select"
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
  const navigate = useNavigate();
  const [saveOpen, setSaveOpen] = useState(false);
  const [poolMode, setPoolMode] = useState<"new" | "existing">("new");
  const [poolName, setPoolName] = useState("");
  const pools = usePools();
  const [saveFail, setSaveFail] = useState<FailureCopy | null>(null);
  const [watchFail, setWatchFail] = useState<{ code: string; copy: FailureCopy } | null>(null);
  const [inPool, setInPool] = useState<Set<string>>(new Set());
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
  const fields = useMemo(() => fieldsFor(query.data?.rows ?? []), [query.data]);
  const factorNames = Object.keys(fields)
    .filter((k) => k.startsWith("f:"))
    .map((k) => k.slice(2));
  const scores = useMemo(() => score(filtered.rows, ranks), [filtered, ranks]);

  const openSave = () => {
    const day = query.data?.trade_date ?? "";
    const [, month, date] = day.split("-");
    setPoolMode("new");
    setPoolName(month && date ? `选股 ${month}-${date}` : "选股结果");
    setSaveFail(null);
    setSaveOpen(true);
  };
  const save = () => {
    if (readOnly) return;
    const name = poolName.trim();
    if (!name) return;
    savePool.mutate(
      { name, description: "", pool_refs: preset === ALL ? presets : [preset] },
      {
        onSuccess: () => {
          setSaveOpen(false);
          setSaveFail(null);
          toast({
            text: `已保存为池子『${name}』，共 ${filtered.rows.length} 只`,
            action: { label: "去看看", onClick: () => navigate("/pools") },
          });
        },
        onError: (error) => setSaveFail(failureCopy(`池子『${name}』`, "保存", error)),
      },
    );
  };
  const watch = (row: ScreenRow) => {
    if (readOnly) return;
    addWatch.mutate(row.code, {
      onSuccess: () => {
        setInPool((prev) => new Set(prev).add(row.code));
        setWatchFail(null);
        toast({ text: "已加入自选", duration: 4000 });
      },
      onError: (error) => {
        const who = `${row.name ?? row.code}（${row.code}）`;
        setWatchFail({ code: row.code, copy: failureCopy(who, "加进自选", error) });
      },
    });
  };

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
              onClick={openSave}
              aria-describedby={readOnly ? "readonly-notice" : undefined}
              ariaDisabled={!!readOnly}
              disabled={!readOnly && presets.length === 0}
            >
              保存为池子
            </Button>
          </>
        }
      />
      <ReadOnlyBanner />
      <Segmented
        label="选股方案"
        value={preset}
        onChange={setPreset}
        options={[{ value: ALL, label: "全部" }, ...presets.map((p) => ({ value: p, label: p }))]}
      />
      <Panel title="条件" sub={q ? `搜索：${q}` : "门槛全部满足才保留；排名按加权分位打分"}>
        <div className="cond-grid">
          <div>
            <h3 className="sub">筛选</h3>
            {filters.length === 0 ? <p className="cond-none">无门槛，全部股票进入排名</p> : null}
            <FilterRows filters={filters} onChange={setFilters} fields={fields} />
          </div>
          <div>
            <h3 className="sub">排名</h3>
            {ranks.length === 0 ? <p className="cond-none">未设排名，按原顺序输出</p> : null}
            <RankRows ranks={ranks} onChange={setRanks} fields={fields} />
          </div>
        </div>
        <Schemes
          filters={filters}
          ranks={ranks}
          onLoad={(sc) => {
            setFilters(sc.filters);
            setRanks(sc.ranks);
          }}
        />
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
            {watchFail ? (
              <FailureNotice
                copy={watchFail.copy}
                onRetry={() => {
                  const row = filtered.rows.find((item) => item.code === watchFail.code);
                  if (row) watch(row);
                }}
              />
            ) : null}
            <DataTable
              label="选股结果"
              tools="选股结果"
              pinEnds
              rows={filtered.rows}
              rowKey={(row) => `${row.preset}:${row.code}`}
              initialSort={{ id: "score", desc: true }}
              height={560}
              emptyText="当天没有选股结果：盘后选股尚未产出，或该方案今天无命中。可切换方案，或用下方条件选股自定义。"
              columns={[
                {
                  id: "code",
                  header: "代码",
                  value: (row) => row.code,
                  cell: (row) => <StockLink code={row.code} />,
                },
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
                  id: "industry",
                  header: "行业",
                  value: (row) => row.industry ?? null,
                  secondary: true,
                },
                {
                  id: "pe",
                  header: "PE",
                  numeric: true,
                  value: (row) => row.pe_ttm ?? null,
                  cell: (row) => (row.pe_ttm == null ? "—" : row.pe_ttm.toFixed(1)),
                },
                {
                  id: "pb",
                  header: "PB",
                  numeric: true,
                  value: (row) => row.pb ?? null,
                  cell: (row) => (row.pb == null ? "—" : row.pb.toFixed(2)),
                },
                {
                  id: "mv",
                  header: "市值(亿)",
                  numeric: true,
                  value: (row) => row.total_mv ?? null,
                  cell: (row) => (row.total_mv == null ? "—" : row.total_mv.toFixed(0)),
                },
                {
                  id: "roe",
                  header: "ROE%",
                  numeric: true,
                  value: (row) => row.roe ?? null,
                  cell: (row) => (row.roe == null ? "—" : row.roe.toFixed(1)),
                },
                {
                  id: "npyoy",
                  header: "净利同比",
                  numeric: true,
                  value: (row) => row.netprofit_yoy ?? null,
                  cell: (row) => <ChangeText value={row.netprofit_yoy ?? null} />,
                },
                ...factorNames.map((name) => ({
                  id: `f:${name}`,
                  header: `${name}分位`,
                  numeric: true,
                  hiddenByDefault: true,
                  value: (row: ScreenRow) => row.factors?.[name] ?? null,
                  cell: (row: ScreenRow) => row.factors?.[name]?.toFixed(0) ?? "—",
                })),
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
                  cell: (row) =>
                    inPool.has(row.code) ? (
                      <Button size="sm" variant="quiet" className="in-watch" ariaDisabled>
                        ✓ 已在自选
                      </Button>
                    ) : (
                      <span className="ack-cell">
                        <Button
                          size="sm"
                          variant="ghost"
                          aria-describedby={readOnly ? "readonly-notice" : undefined}
                          ariaDisabled={!!readOnly}
                          disabled={addWatch.isPending}
                          onClick={() => watch(row)}
                        >
                          {watchFail?.code === row.code ? "重试" : "加自选"}
                        </Button>
                      </span>
                    ),
                },
              ]}
            />
          </Panel>
        )}
      </QueryView>
      <AppDrawer
        open={saveOpen}
        onClose={() => setSaveOpen(false)}
        title="保存为池子"
        label="保存为池子"
        footer={
          <Button
            variant="primary"
            aria-busy={savePool.isPending || undefined}
            ariaDisabled={!!readOnly}
            onClick={save}
          >
            保存
          </Button>
        }
      >
        <Segmented
          label="保存方式"
          value={poolMode}
          onChange={(mode) => {
            setPoolMode(mode);
            if (mode === "existing") {
              const first = pools.data?.pools[0]?.name;
              if (first) setPoolName(first);
            }
          }}
          options={[
            { value: "new", label: "新建" },
            { value: "existing", label: "加入已有" },
          ]}
        />
        <p className="hint">共 {filtered.rows.length} 只</p>
        {poolMode === "existing" ? (
          <label className="fld">
            已有池子
            <select
              className="select"
              aria-label="已有池子"
              value={poolName}
              onChange={(event) => setPoolName(event.target.value)}
            >
              {(pools.data?.pools ?? []).map((pool) => (
                <option key={pool.name} value={pool.name}>
                  {pool.name}
                </option>
              ))}
            </select>
          </label>
        ) : (
          <label className="fld">
            池子名称
            <input
              aria-label="池子名称"
              value={poolName}
              onChange={(event) => setPoolName(event.target.value)}
            />
          </label>
        )}
        {saveFail ? (
          <FailureNotice copy={saveFail} busy={savePool.isPending} onRetry={save} />
        ) : null}
      </AppDrawer>
      <Conditions />
    </>
  );
}
