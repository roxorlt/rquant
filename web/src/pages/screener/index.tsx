import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  fetchScreenRun,
  type ScreenBlock,
  type ScreenRunData,
  type ScreenRunRequest,
  useScreenCatalog,
} from "@/api/screen";
import { StockDrawer } from "@/app/StockDrawer";
import { Button, EmptyState, PageHeader, PageSkeleton, Panel, Tip } from "@/ui";
import { ParamControl, type ParameterValue } from "./ParamControl";
import { type RankingDraft, RankingEditor } from "./RankingEditor";
import { ScreenResults } from "./ScreenResults";
import "./screener.css";

type Draft = { id: number; key: string; args: Record<string, ParameterValue> };
const PAGE_SIZE = 20;

function makeDraft(block: ScreenBlock, id: number): Draft {
  return {
    id,
    key: block.key,
    args: Object.fromEntries(
      block.parameters.map((parameter) => [parameter.key, parameter.initial]),
    ),
  };
}

export default function ScreenerPage() {
  const catalog = useScreenCatalog();
  const [draft, setDraft] = useState<Draft[]>([]);
  const [tradeDate, setTradeDate] = useState<string | null>(null);
  const [addKey, setAddKey] = useState("");
  const [initialized, setInitialized] = useState(false);
  const nextId = useRef(1);
  const nextRankId = useRef(1);
  const [rankDraft, setRankDraft] = useState<RankingDraft[]>([]);
  const [topN, setTopN] = useState("20");
  const [result, setResult] = useState<ScreenRunData | null>(null);
  const [resultGeneration, setResultGeneration] = useState<string | null>(null);
  const [applied, setApplied] = useState<ScreenRunRequest | null>(null);
  const [appliedKey, setAppliedKey] = useState<string | null>(null);
  const [forceStale, setForceStale] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [pageIndex, setPageIndex] = useState(0);
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const blocks = catalog.data?.blocks ?? [];
  const dates = catalog.data?.dates ?? [];
  const rankMetrics = catalog.data?.ranking_metrics ?? [];

  useEffect(() => {
    if (initialized || blocks.length === 0) return;
    const first = blocks.find((block) => block.key === "not_st") ?? blocks[0];
    if (!first) return;
    setDraft([makeDraft(first, nextId.current++)]);
    setAddKey(first.key);
    setTradeDate(dates[0] ?? null);
    setInitialized(true);
  }, [blocks, dates, initialized]);

  useEffect(() => {
    if (initialized && dates.length > 0 && !dates.includes(tradeDate ?? "")) {
      setTradeDate(dates[0] ?? null);
    }
  }, [dates, initialized, tradeDate]);

  const byKey = new Map(blocks.map((block) => [block.key, block]));
  const rankWeights = rankDraft.map((row) =>
    row.weight.trim() === "" ? Number.NaN : Number(row.weight),
  );
  const totalWeight = rankWeights.reduce((sum, weight) => sum + weight, 0);
  const availableRankMetrics = new Set(rankMetrics.map((metric) => metric.value));
  const rankingError =
    rankDraft.length === 0
      ? null
      : rankDraft.some((row) => !availableRankMetrics.has(row.metric))
        ? "所选排名指标暂不可用，请重新选择。"
        : new Set(rankDraft.map((row) => row.metric)).size !== rankDraft.length
          ? "同一排名指标只能添加一次。"
          : rankWeights.some((weight) => !Number.isFinite(weight) || weight < 0 || weight > 100)
            ? "权重请填 0 到 100。"
            : totalWeight <= 0
              ? "至少一项权重大于 0。"
              : !Number.isInteger(Number(topN)) || Number(topN) < 1 || Number(topN) > 100
                ? "前 N 只请填 1 到 100。"
                : null;
  const snapshotKey = JSON.stringify({
    tradeDate,
    draft: draft.map(({ key, args }) => ({ key, args })),
    ranking: rankDraft.map(({ metric, ascending, weight }) => ({ metric, ascending, weight })),
    topN: rankDraft.length > 0 ? topN : null,
  });
  const stale =
    result !== null &&
    (forceStale ||
      snapshotKey !== appliedKey ||
      resultGeneration !== catalog.serving?.generation_id);
  const staleText =
    forceStale || (result !== null && resultGeneration !== catalog.serving?.generation_id)
      ? "数据已更新，请重新筛选。旧结果仅供参考。"
      : "条件已改，请重新运行。旧结果仅供参考。";
  const canRun =
    catalog.data?.available === true &&
    dates.length > 0 &&
    tradeDate !== null &&
    draft.length > 0 &&
    rankingError === null;

  function updateArg(id: number, key: string, value: ParameterValue) {
    setDraft((current) =>
      current.map((condition) =>
        condition.id === id
          ? { ...condition, args: { ...condition.args, [key]: value } }
          : condition,
      ),
    );
  }

  function addCondition() {
    const block = byKey.get(addKey);
    if (!block || draft.length >= 26) return;
    setDraft((current) => [...current, makeDraft(block, nextId.current++)]);
  }

  function addRanking() {
    const firstUnused = rankMetrics.find(
      (metric) => !rankDraft.some((row) => row.metric === metric.value),
    );
    if (!firstUnused) return;
    setRankDraft((current) => [
      ...current,
      {
        id: nextRankId.current++,
        metric: firstUnused.value,
        ascending: firstUnused.value === "CIRC_MV[0]",
        weight: current.length === 0 ? "100" : "0",
      },
    ]);
  }

  function updateRanking(id: number, change: Partial<RankingDraft>) {
    setRankDraft((current) => current.map((row) => (row.id === id ? { ...row, ...change } : row)));
  }

  async function run(body: ScreenRunRequest, key: string, nextIndex: number) {
    if (running) return;
    setRunning(true);
    setError(null);
    try {
      const envelope = await fetchScreenRun(body);
      setResult(envelope.data);
      setResultGeneration(envelope.serving.generation_id);
      setForceStale(false);
      if (nextIndex === 0) {
        setApplied({ ...body, cursor: null });
        setAppliedKey(key);
        setCursors([null]);
      } else {
        setCursors((current) => {
          const copy = current.slice(0, nextIndex);
          copy[nextIndex] = body.cursor ?? null;
          return copy;
        });
      }
      setPageIndex(nextIndex);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "筛选暂时无法完成，请稍后重试。");
      if (caught instanceof ApiError && caught.status === 409) setForceStale(true);
    } finally {
      setRunning(false);
    }
  }

  function runDraft() {
    if (!canRun || tradeDate === null) return;
    const body: ScreenRunRequest = {
      trade_date: tradeDate,
      conditions: draft.map(({ key, args }) => ({ key, args })),
      page_size: PAGE_SIZE,
      cursor: null,
      ranking:
        rankDraft.length > 0
          ? {
              conditions: rankDraft.map(({ metric, ascending, weight }) => ({
                metric,
                ascending,
                weight: Number(weight),
              })),
              top_n: Number(topN),
            }
          : null,
    };
    void run(body, snapshotKey, 0);
  }

  function runPage(index: number, cursor: string | null) {
    if (!applied || stale) return;
    void run({ ...applied, cursor }, appliedKey ?? snapshotKey, index);
  }

  if (catalog.isLoading) return <PageSkeleton />;

  return (
    <>
      <PageHeader eyebrow="研究" title="选股器" />
      {catalog.error ? (
        <Panel>
          <div className="screen-error" role="alert">
            <EmptyState title="条件目录暂时无法加载" />
            <Button size="sm" onClick={catalog.refetch}>
              重试
            </Button>
          </div>
        </Panel>
      ) : (
        <>
          <Panel
            title="条件"
            sub="全部满足才保留"
            actions={
              <label className="field screen-date">
                <span className="lbl">数据日期</span>
                <select
                  className="inp"
                  value={tradeDate ?? ""}
                  onChange={(event) => setTradeDate(event.target.value)}
                  disabled={dates.length === 0}
                >
                  {dates.length === 0 ? <option value="">暂无日期</option> : null}
                  {dates.map((day) => (
                    <option key={day} value={day}>
                      {day}
                    </option>
                  ))}
                </select>
              </label>
            }
          >
            {!catalog.data?.available || dates.length === 0 ? (
              <EmptyState title="选股数据还没有发布" hint="数据发布后就能运行条件。" />
            ) : null}
            <div className="screen-conditions">
              {draft.map((condition, index) => {
                const block = byKey.get(condition.key);
                if (!block) return null;
                return (
                  <div key={condition.id} className="screen-condition">
                    <div className="screen-condition-head">
                      <span className="screen-index num">{index + 1}</span>
                      <Tip content={block.hint}>
                        <strong>{block.label}</strong>
                      </Tip>
                      <Button
                        size="sm"
                        variant="ghost"
                        aria-label={`删除第 ${index + 1} 条条件`}
                        onClick={() =>
                          setDraft((current) => current.filter((item) => item.id !== condition.id))
                        }
                      >
                        删除
                      </Button>
                    </div>
                    {block.parameters.length > 0 ? (
                      <div className="screen-params">
                        {block.parameters.map((parameter) => (
                          <ParamControl
                            key={parameter.key}
                            parameter={parameter}
                            value={condition.args[parameter.key] ?? null}
                            onChange={(value) => updateArg(condition.id, parameter.key, value)}
                          />
                        ))}
                      </div>
                    ) : null}
                  </div>
                );
              })}
              {draft.length === 0 ? (
                <EmptyState title="还没有条件" hint="从目录添加一条条件。" />
              ) : null}
            </div>
            <div className="screen-actions">
              <select
                className="inp screen-add-select"
                aria-label="条件目录"
                value={addKey}
                onChange={(event) => setAddKey(event.target.value)}
              >
                {Array.from(new Set(blocks.map((block) => block.category))).map((category) => (
                  <optgroup
                    key={category}
                    label={
                      blocks.find((block) => block.category === category)?.category_label ?? "其他"
                    }
                  >
                    {blocks
                      .filter((block) => block.category === category)
                      .map((block) => (
                        <option key={block.key} value={block.key}>
                          {block.label}
                        </option>
                      ))}
                  </optgroup>
                ))}
              </select>
              <Button onClick={addCondition} disabled={draft.length >= 26}>
                添加条件
              </Button>
              <Button
                variant="primary"
                onClick={runDraft}
                disabled={running}
                disabledReason={!canRun ? "先选好数据日期并添加条件" : undefined}
              >
                {running ? "正在筛选…" : "运行筛选"}
              </Button>
            </div>
          </Panel>
          <RankingEditor
            metrics={rankMetrics}
            rows={rankDraft}
            topN={topN}
            totalWeight={totalWeight}
            error={rankingError}
            onAdd={addRanking}
            onUpdate={updateRanking}
            onRemove={(id) => setRankDraft((current) => current.filter((row) => row.id !== id))}
            onTopN={setTopN}
          />
          <ScreenResults
            data={result}
            stale={stale}
            staleText={staleText}
            error={error}
            running={running}
            pageIndex={pageIndex}
            onStock={setSelectedStock}
            onPrevious={() => runPage(pageIndex - 1, cursors[pageIndex - 1] ?? null)}
            onNext={() => runPage(pageIndex + 1, result?.next_cursor ?? null)}
          />
        </>
      )}
      <StockDrawer tsCode={selectedStock} onClose={() => setSelectedStock(null)} />
    </>
  );
}
