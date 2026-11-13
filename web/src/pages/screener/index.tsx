import { previewWatchlistCodes } from "virtual:static-preview";
import { useCallback, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import {
  type CustomBlockItem,
  type ScreenerBlock,
  type ScreenerRunRequest,
  type ScreenerUpstream,
  type ScreenRow,
  useAddWatch,
  usePools,
  useSavePool,
  useScreen,
  useScreenerCatalog,
  useScreenerRun,
  useWatchlist,
} from "@/api/endpoints";
import type { ServingQueryResult } from "@/api/useServingQuery";
import { presetLabel } from "@/copy/publicText";
import { formatFixed, formatPrice } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import {
  AppDrawer,
  Button,
  ChangeText,
  FailureNotice,
  PageHeader,
  Panel,
  Segmented,
  useToast,
} from "@/ui";
import { type FailureCopy, failureCopy } from "@/ui/failure";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";
import { BlockListWorkbench, FunnelStrip, NlWhiteBoxBar, UpstreamScopeBar } from "./BlockWorkbench";
import {
  applyClientBlocks,
  DEFAULT_UPSTREAM,
  dedupScreenRows,
  FALLBACK_RULES,
  fieldsFor,
  filtersToBlocks,
  groupsFor,
  newBlockId,
  type Rank,
  score,
} from "./Builder";
import { Conditions } from "./Conditions";
import { PoolCanvasWorkbench } from "./PoolCanvas";
import { Schemes } from "./Schemes";

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
  const groups = groupsFor(fields);
  return (
    <div>
      {ranks.map((r, i) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: rows are positional.
        <div className="tools" key={i}>
          <span className="mono">{i + 1}</span>
          <span className="tools-group">
            <select
              className="select"
              aria-label="排序指标"
              value={r.field}
              onChange={(e) => set(i, { field: e.target.value })}
            >
              {groups.map(([name, opts]) =>
                opts.length ? (
                  <optgroup key={name} label={name}>
                    {opts.map(([value, label]) => (
                      <option key={value} value={value}>
                        {label}
                      </option>
                    ))}
                  </optgroup>
                ) : null,
              )}
            </select>
            <select
              className="select"
              aria-label="排序方向"
              value={r.desc ? "d" : "a"}
              onChange={(e) => set(i, { desc: e.target.value === "d" })}
            >
              <option value="d">降序</option>
              <option value="a">升序</option>
            </select>
          </span>
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

const rememberedWatch = new Set<string>();

/** Test hook: the in-memory watchlist dies with a full reload, not with navigation. */
export function resetRememberedWatch(): void {
  rememberedWatch.clear();
}

function currentWatch(): Set<string> {
  return new Set([...rememberedWatch, ...previewWatchlistCodes()]);
}

interface WatchFail {
  code: string;
  name: string;
  copy: FailureCopy;
}

function watchBanner(fails: WatchFail[]): FailureCopy {
  const latest = fails[fails.length - 1];
  if (fails.length < 2 && latest) return latest.copy;
  const reason = latest?.copy.text.split("：")[1]?.split("。")[0] ?? "这项操作暂时用不了";
  return {
    text: `${fails.length} 只股票没加进自选：${reason}。可以在每一行点「重试」。`,
    retry: true,
    health: latest?.copy.health ?? true,
  };
}

export default function ScreenerPage() {
  const [upstream, setUpstreamState] = useState<ScreenerUpstream>(DEFAULT_UPSTREAM);
  const [blocks, setBlocksState] = useState<ScreenerBlock[]>([]);
  const [ranks, setRanks] = useState<Rank[]>([{ field: "pct_chg", desc: true, weight: 100 }]);
  const [previewStep, setPreviewStep] = useState<number | null>(null);
  const [extraCustomBlocks, setExtraCustomBlocks] = useState<CustomBlockItem[]>([]);

  const handleCustomBlockSaved = useCallback((item: CustomBlockItem) => {
    setExtraCustomBlocks((prev) => [...prev.filter((b) => b.block_id !== item.block_id), item]);
  }, []);

  const setUpstream = (next: ScreenerUpstream) => {
    setUpstreamState(next);
    setPreviewStep(null);
  };
  const setBlocks = (next: ScreenerBlock[]) => {
    setBlocksState(next);
    setPreviewStep(null);
  };

  const screenPreset = upstream.base === "preset" && upstream.preset ? upstream.preset : null;
  const screenQuery = useScreen(screenPreset);
  const catalogQuery = useScreenerCatalog();
  const runBody = useMemo<ScreenerRunRequest>(
    () => ({
      upstream,
      blocks,
      ranks,
      preview_step: previewStep,
    }),
    [upstream, blocks, ranks, previewStep],
  );
  const runQuery = useScreenerRun(runBody);

  const savePool = useSavePool();
  const addWatch = useAddWatch();
  const toast = useToast();
  const navigate = useNavigate();
  const [saveOpen, setSaveOpen] = useState(false);
  const [poolMode, setPoolMode] = useState<"new" | "existing">("new");
  const [poolName, setPoolName] = useState("");
  const pools = usePools();
  const watchlist = useWatchlist();
  const [saveFail, setSaveFail] = useState<FailureCopy | null>(null);
  const [watchFails, setWatchFails] = useState<WatchFail[]>([]);
  const [inPool, setInPool] = useState<Set<string>>(currentWatch);

  const watched = useMemo(() => {
    const items = watchlist.data?.items;
    const serverCodes = Array.isArray(items) ? items.map((item) => item.code) : [];
    return new Set([...inPool, ...serverCodes]);
  }, [inPool, watchlist.data]);

  const [params, setParams] = useSearchParams();
  const q = (params.get("q") ?? "").trim();
  const viewMode: "list" | "canvas" = params.get("view") === "canvas" ? "canvas" : "list";
  const setViewMode = (mode: "list" | "canvas") => {
    const next = new URLSearchParams(params);
    if (mode === "canvas") next.set("view", "canvas");
    else next.delete("view");
    setParams(next, { replace: true });
  };

  const serverActive = Array.isArray(runQuery.data?.rows);
  const rules = useMemo(
    () =>
      Array.isArray(catalogQuery.data?.rules) && catalogQuery.data.rules.length > 0
        ? catalogQuery.data.rules
        : FALLBACK_RULES,
    [catalogQuery.data],
  );
  const customBlocks = useMemo(() => {
    const byId = new Map<string, CustomBlockItem>();
    if (Array.isArray(catalogQuery.data?.custom_blocks)) {
      for (const b of catalogQuery.data.custom_blocks) byId.set(b.block_id, b);
    }
    for (const b of extraCustomBlocks) byId.set(b.block_id, b);
    return [...byId.values()];
  }, [catalogQuery.data, extraCustomBlocks]);
  const ruleMap = useMemo(() => new Map(rules.map((r) => [r.name, r])), [rules]);
  const customMap = useMemo(
    () => new Map(customBlocks.map((b) => [b.block_id, b])),
    [customBlocks],
  );

  const dedupedFallbackRows = useMemo(
    () => dedupScreenRows(screenQuery.data?.rows ?? []),
    [screenQuery.data],
  );

  const rawRowsForFields = useMemo(
    () => (serverActive ? dedupScreenRows(runQuery.data?.rows ?? []) : dedupedFallbackRows),
    [serverActive, runQuery.data, dedupedFallbackRows],
  );
  const fields = useMemo(
    () => fieldsFor(rawRowsForFields, catalogQuery.data?.field_groups),
    [rawRowsForFields, catalogQuery.data],
  );
  const factorNames = useMemo(
    () =>
      Object.keys(fields)
        .filter((k) => k.startsWith("f:"))
        .map((k) => k.slice(2)),
    [fields],
  );

  const fallbackResult = useMemo(() => {
    if (serverActive) return null;
    const baseRows = dedupedFallbackRows.filter(
      (r) => !q || r.code.includes(q) || (r.name ?? "").includes(q),
    );
    return applyClientBlocks(baseRows, blocks, fields, customMap, ruleMap, previewStep);
  }, [serverActive, dedupedFallbackRows, q, blocks, fields, customMap, ruleMap, previewStep]);

  const effectiveRows = useMemo(() => {
    if (serverActive) {
      const serverRows = dedupScreenRows(runQuery.data?.rows ?? []);
      return q
        ? serverRows.filter((r) => r.code.includes(q) || (r.name ?? "").includes(q))
        : serverRows;
    }
    return fallbackResult?.rows ?? [];
  }, [serverActive, runQuery.data, q, fallbackResult]);

  const effectiveSteps = serverActive
    ? (runQuery.data?.steps ?? [])
    : (fallbackResult?.steps ?? []);
  const activeStep = serverActive
    ? (runQuery.data?.active_step ?? Math.max(0, effectiveSteps.length - 1))
    : (fallbackResult?.activeStep ?? 0);

  const presets = useMemo(() => {
    if (Array.isArray(runQuery.data?.presets) && runQuery.data.presets.length > 0) {
      return runQuery.data.presets;
    }
    if (Array.isArray(catalogQuery.data?.presets) && catalogQuery.data.presets.length > 0) {
      return catalogQuery.data.presets;
    }
    return screenQuery.data?.presets ?? [];
  }, [runQuery.data, catalogQuery.data, screenQuery.data]);

  const tradeDate =
    runQuery.data?.trade_date ??
    catalogQuery.data?.trade_date ??
    screenQuery.data?.trade_date ??
    "";

  const poolRefs = useMemo(() => {
    if (Array.isArray(runQuery.data?.pool_refs) && runQuery.data.pool_refs.length > 0) {
      return runQuery.data.pool_refs;
    }
    if (upstream.base === "preset" && upstream.preset) return [upstream.preset];
    if (upstream.base === "all_market") return ["all_market"];
    if (upstream.base === "pool" && upstream.pool) return [upstream.pool];
    if (upstream.base === "watchlist") return ["manual"];
    return presets;
  }, [runQuery.data, upstream, presets]);

  const clientScores = useMemo(() => score(effectiveRows, ranks), [effectiveRows, ranks]);

  const openSave = () => {
    const [, month, date] = tradeDate.split("-");
    setPoolMode("new");
    setPoolName(month && date ? `选股 ${month}-${date}` : "选股结果");
    setSaveFail(null);
    setSaveOpen(true);
  };
  const save = () => {
    const name = poolName.trim();
    if (!name) return;
    savePool.mutate(
      {
        name,
        description: "",
        pool_refs: poolRefs,
        codes: effectiveRows.map((r) => r.code),
        mode: poolMode,
      },
      {
        onSuccess: () => {
          setSaveOpen(false);
          setSaveFail(null);
          toast({
            text: `已保存为池子『${name}』，共 ${effectiveRows.length} 只`,
            action: { label: "去看看", onClick: () => navigate("/pools") },
          });
        },
        onError: (error) => setSaveFail(failureCopy(`池子『${name}』`, "保存", error)),
      },
    );
  };
  const watch = (row: ScreenRow) => {
    addWatch.mutate(row.code, {
      onSuccess: () => {
        rememberedWatch.add(row.code);
        setInPool(currentWatch());
        setWatchFails((prev) => prev.filter((item) => item.code !== row.code));
        toast({ text: "已加入自选", duration: 4000 });
      },
      onError: (error) => {
        const name = row.name ?? row.code;
        const who = `${name}（${row.code}）`;
        const copy = failureCopy(who, "加进自选", error);
        setWatchFails((prev) => [
          ...prev.filter((item) => item.code !== row.code),
          { code: row.code, name, copy },
        ]);
      },
    });
  };
  const failedCodes = new Set(watchFails.map((item) => item.code));
  const primaryQuery: ServingQueryResult<unknown> = serverActive ? runQuery : screenQuery;

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="选股器"
        note={tradeDate || undefined}
        actions={
          <div className="wb-head-actions">
            <Segmented
              label="选股器视图"
              value={viewMode}
              onChange={setViewMode}
              options={[
                { value: "list", label: "列表工作台" },
                { value: "canvas", label: "节点画布" },
              ]}
            />
            {viewMode === "list" ? (
              <Button
                onClick={openSave}
                disabled={effectiveRows.length === 0 && presets.length === 0}
              >
                保存为池子
              </Button>
            ) : null}
          </div>
        }
      />

      {viewMode === "canvas" ? (
        <PoolCanvasWorkbench
          tradeDate={tradeDate}
          presets={presets}
          pools={pools.data?.pools ?? []}
          allMarketCount={catalogQuery.data?.all_market_count ?? 0}
          fields={fields}
          rules={rules}
          customBlocks={customBlocks}
          fallbackRows={dedupedFallbackRows}
          watchedCodes={watched}
          onWatch={watch}
          onCustomBlockSaved={handleCustomBlockSaved}
        />
      ) : (
        <>
          <UpstreamScopeBar
            upstream={upstream}
            onChange={setUpstream}
            presets={presets}
            pools={pools.data?.pools ?? []}
            upstreamOptions={catalogQuery.data?.upstream_options ?? []}
            allMarketCount={catalogQuery.data?.all_market_count ?? 0}
          />

          <Panel
            title="条件工作台"
            sub={
              q
                ? `搜索：${q}`
                : "支持 26 个内置积木、财务与已验证因子、可执行 TDX 公式与自定义积木；点击漏斗任意层可预览中间结果"
            }
          >
            <NlWhiteBoxBar
              onAppendBlocks={(added) => setBlocks([...blocks, ...added])}
              onCustomBlockSaved={handleCustomBlockSaved}
            />

            <div className="cond-grid">
              <div>
                <h3 className="sub">筛选积木</h3>
                <BlockListWorkbench
                  blocks={blocks}
                  onChange={setBlocks}
                  fields={fields}
                  rules={rules}
                  customBlocks={customBlocks}
                  onCustomBlockSaved={handleCustomBlockSaved}
                />
              </div>
              <div>
                <h3 className="sub">加权排名</h3>
                {ranks.length === 0 ? <p className="cond-none">未设排名，按涨跌幅输出</p> : null}
                <RankRows ranks={ranks} onChange={setRanks} fields={fields} />
              </div>
            </div>

            <Schemes
              blocks={blocks}
              upstream={upstream}
              ranks={ranks}
              onLoad={(sc) => {
                if (sc.upstream) setUpstreamState(sc.upstream);
                setBlocksState(
                  sc.blocks && sc.blocks.length > 0 ? sc.blocks : filtersToBlocks(sc.filters),
                );
                setRanks(sc.ranks);
                setPreviewStep(null);
              }}
            />

            <FunnelStrip
              steps={effectiveSteps}
              activeStep={activeStep}
              previewStep={previewStep}
              onSelectStep={setPreviewStep}
            />
          </Panel>

          <QueryView query={primaryQuery}>
            {() => (
              <Panel
                title={
                  previewStep !== null && previewStep < effectiveSteps.length - 1
                    ? `中间层预览（第 ${activeStep} 层）· 共 ${effectiveRows.length} 只`
                    : `结果 · 命中 ${effectiveRows.length} 只`
                }
                flush
              >
                {watchFails.length ? (
                  <FailureNotice
                    pin
                    copy={watchBanner(watchFails)}
                    busy={addWatch.isPending}
                    onRetry={() => {
                      for (const item of watchFails) {
                        const row = effectiveRows.find((candidate) => candidate.code === item.code);
                        if (row) watch(row);
                      }
                    }}
                  />
                ) : null}
                <DataTable
                  label="选股结果"
                  tools="选股结果"
                  phoneSort
                  pinEnds
                  rows={effectiveRows}
                  rowKey={(row) => row.code}
                  initialSort={{ id: "score", desc: true }}
                  height={560}
                  emptyText="当前输入范围与条件组合下暂无命中股票。可点击上方漏斗诊断条查看是在哪一层被过滤，或放宽条件阈值。"
                  columns={[
                    {
                      id: "code",
                      header: "代码",
                      value: (row) => row.code,
                      cell: (row) => <StockLink code={row.code} />,
                    },
                    { id: "name", header: "名称", value: (row) => row.name ?? null },
                    {
                      id: "preset",
                      header: "来源",
                      value: (row) => presetLabel(row.preset),
                      secondary: true,
                    },
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
                      cell: (row) => (row.pe_ttm == null ? "—" : formatFixed(row.pe_ttm, 1)),
                    },
                    {
                      id: "pb",
                      header: "PB",
                      numeric: true,
                      value: (row) => row.pb ?? null,
                      cell: (row) => (row.pb == null ? "—" : formatFixed(row.pb, 2)),
                    },
                    {
                      id: "mv",
                      header: "市值(亿)",
                      numeric: true,
                      value: (row) => row.total_mv ?? null,
                      cell: (row) => (row.total_mv == null ? "—" : formatFixed(row.total_mv, 0)),
                    },
                    {
                      id: "roe",
                      header: "ROE%",
                      numeric: true,
                      value: (row) => row.roe ?? null,
                      cell: (row) => (row.roe == null ? "—" : formatFixed(row.roe, 1)),
                    },
                    {
                      id: "gpm",
                      header: "毛利率%",
                      numeric: true,
                      hiddenByDefault: true,
                      value: (row) => row.grossprofit_margin ?? null,
                      cell: (row) =>
                        row.grossprofit_margin == null
                          ? "—"
                          : formatFixed(row.grossprofit_margin, 1),
                    },
                    {
                      id: "npyoy",
                      header: "净利同比",
                      numeric: true,
                      value: (row) => row.netprofit_yoy ?? null,
                      cell: (row) => <ChangeText value={row.netprofit_yoy ?? null} />,
                    },
                    {
                      id: "debt",
                      header: "负债率%",
                      numeric: true,
                      hiddenByDefault: true,
                      value: (row) => row.debt_to_assets ?? null,
                      cell: (row) =>
                        row.debt_to_assets == null ? "—" : formatFixed(row.debt_to_assets, 1),
                    },
                    ...factorNames.map((name) => ({
                      id: `f:${name}`,
                      header: `${name}分位`,
                      numeric: true,
                      hiddenByDefault: true,
                      value: (row: ScreenRow) => row.factors?.[name] ?? null,
                      cell: (row: ScreenRow) => formatFixed(row.factors?.[name], 0),
                    })),
                    {
                      id: "score",
                      header: "排名分",
                      numeric: true,
                      value: (row) => row.score ?? clientScores.get(row) ?? null,
                      cell: (row) => formatFixed(row.score ?? clientScores.get(row) ?? 0, 1),
                    },
                    {
                      id: "watch",
                      header: "",
                      sortable: false,
                      value: () => null,
                      cell: (row) =>
                        watched.has(row.code) ? (
                          <Button size="sm" variant="quiet" className="in-watch" ariaDisabled>
                            ✓ 已在自选
                          </Button>
                        ) : (
                          <span className="ack-cell">
                            {failedCodes.has(row.code) ? (
                              <span className="row-err">没加进自选</span>
                            ) : null}
                            <Button
                              size="sm"
                              variant="ghost"
                              disabled={addWatch.isPending}
                              onClick={() => watch(row)}
                            >
                              {failedCodes.has(row.code) ? "重试" : "加自选"}
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
              <Button variant="primary" aria-busy={savePool.isPending || undefined} onClick={save}>
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
            <p className="hint">共 {effectiveRows.length} 只</p>
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

          <Conditions
            onAddTdx={(tdx) => {
              setBlocks([
                ...blocks,
                { id: newBlockId(), type: "tdx", enabled: true, tdx, op: ">=", value: 0 },
              ]);
              toast("已将通达信公式加入实时选股漏斗");
            }}
          />
        </>
      )}
    </>
  );
}
