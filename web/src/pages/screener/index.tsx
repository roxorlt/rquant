import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  fetchScreenRun,
  isFundamentalScreenField,
  type ScreenBlock,
  type ScreenRunData,
  type ScreenRunRequest,
  useScreenCatalog,
} from "@/api/screen";
import { StockDrawer } from "@/app/StockDrawer";
import {
  Button,
  EmptyState,
  PageHeader,
  PageSkeleton,
  Panel,
  ParamControl,
  type ParameterValue,
  RelativeTime,
  Tip,
} from "@/ui";
import { CustomMaParamControl } from "./CustomMaParamControl";
import { FormulaPreviewDialog } from "./FormulaPreviewDialog";
import { type RankingDraft, RankingEditor } from "./RankingEditor";
import { type EditableScreenCondition, ScreenNaturalLanguage } from "./ScreenNaturalLanguage";
import { ScreenResults } from "./ScreenResults";
import "./screener.css";

type Draft = { id: number; key: string; args: Record<string, ParameterValue> };
const PAGE_SIZE = 20;
function fieldUnit(block: ScreenBlock, value: ParameterValue): string | null {
  if (!isFundamentalScreenField(value)) return null;
  const option = block.parameters
    .flatMap((parameter) => parameter.options ?? [])
    .find((item) => item.value === value);
  return /（(%|倍)）$/.exec(option?.label ?? "")?.[1] ?? null;
}

function conditionNumberUnit(block: ScreenBlock, condition: Draft, key: string): string | null {
  if (condition.key === "between" && (key === "low" || key === "high")) {
    return fieldUnit(block, condition.args.field ?? null);
  }
  if (["gt", "lt", "gte", "lte"].includes(condition.key)) {
    if (key === "left") return fieldUnit(block, condition.args.right ?? null);
    if (key === "right") return fieldUnit(block, condition.args.left ?? null);
  }
  return null;
}

function usesFundamental(conditions: ScreenRunRequest["conditions"]): boolean {
  return conditions.some((condition) =>
    Object.values(condition.args ?? {}).some((value) => isFundamentalScreenField(value)),
  );
}

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
  const undoDraft = useRef<Draft[] | null>(null);
  const [conditionRevision, setConditionRevision] = useState(0);
  const nextRankId = useRef(1);
  const lastSourceIdentity = useRef<string | null | undefined>(undefined);
  const sourceEpoch = useRef(0);
  const draftEpoch = useRef(0);
  const [rankDraft, setRankDraft] = useState<RankingDraft[]>([]);
  const [topN, setTopN] = useState("20");
  const [result, setResult] = useState<ScreenRunData | null>(null);
  const [applied, setApplied] = useState<ScreenRunRequest | null>(null);
  const [appliedKey, setAppliedKey] = useState<string | null>(null);
  const [successfulRunRevision, setSuccessfulRunRevision] = useState(0);
  const [forcedStaleReason, setForcedStaleReason] = useState<"source" | "condition" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [pageIndex, setPageIndex] = useState(0);
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const [formulaOpen, setFormulaOpen] = useState(false);
  const blocks = catalog.data?.blocks ?? [];
  const dates = catalog.data?.dates ?? [];
  const rankMetrics = catalog.data?.ranking_metrics ?? [];
  const customRsiReady = blocks.some(
    (block) =>
      (block.key === "rsi_oversold" || block.key === "rsi_overbought") &&
      block.parameters.some(
        (parameter) => parameter.key === "period" && parameter.input === "integer",
      ),
  );

  useEffect(() => {
    const activeCatalog = catalog.data;
    if (!activeCatalog) return;
    const identity = activeCatalog.source?.identity ?? null;
    if (lastSourceIdentity.current === undefined) {
      lastSourceIdentity.current = identity;
      return;
    }
    if (lastSourceIdentity.current === identity) return;
    lastSourceIdentity.current = identity;
    sourceEpoch.current += 1;
    setResult(null);
    setApplied(null);
    setAppliedKey(null);
    setCursors([null]);
    setPageIndex(0);
    setForcedStaleReason("source");
    undoDraft.current = null;
    setError(null);
    setDraft((current) =>
      current.filter((condition) => {
        const block = activeCatalog.blocks.find((item) => item.key === condition.key);
        if (!block) return false;
        return !Object.entries(condition.args).some(
          ([key, value]) =>
            isFundamentalScreenField(value) &&
            !block.parameters
              .find((parameter) => parameter.key === key)
              ?.options?.some((option) => option.value === value),
        );
      }),
    );
  }, [catalog.data]);

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

  useEffect(() => {
    if (!initialized || customRsiReady || blocks.length === 0) return;
    setDraft((current) => {
      let changed = false;
      const next = current.map((condition) => {
        const block = blocks.find((item) => item.key === condition.key);
        if (!block) return condition;
        let args = condition.args;
        for (const parameter of block.parameters) {
          const value = args[parameter.key];
          const listed = parameter.options?.some((option) => option.value === String(value));
          const unavailablePeriod =
            (condition.key === "rsi_oversold" || condition.key === "rsi_overbought") &&
            parameter.key === "period" &&
            parameter.input === "choice" &&
            !listed;
          const unavailableField =
            parameter.custom_ma && typeof value === "string" && value.startsWith("RSI") && !listed;
          if (unavailablePeriod || unavailableField) {
            args = { ...args, [parameter.key]: parameter.initial };
            changed = true;
          }
        }
        return args === condition.args ? condition : { ...condition, args };
      });
      return changed ? next : current;
    });
  }, [blocks, customRsiReady, initialized]);

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
  const currentSnapshotKey = useRef(snapshotKey);
  useEffect(() => {
    currentSnapshotKey.current = snapshotKey;
  }, [snapshotKey]);
  const stale =
    forcedStaleReason === "source" ||
    (result !== null &&
      (forcedStaleReason === "condition" ||
        snapshotKey !== appliedKey ||
        (result.source?.identity ?? null) !== (catalog.data?.source?.identity ?? null)));
  const staleText =
    result === null
      ? "选股数据已更新，请重新筛选。"
      : forcedStaleReason === "source" || result.source?.identity !== catalog.data?.source?.identity
        ? "选股数据已更新，请重新筛选。旧结果仅供参考。"
        : "条件已改，请重新运行。旧结果仅供参考。";
  const canRun =
    catalog.data?.available === true &&
    dates.length > 0 &&
    tradeDate !== null &&
    draft.length > 0 &&
    catalog.data?.source?.identity != null &&
    rankingError === null;

  function markManualConditionEdit() {
    draftEpoch.current += 1;
    undoDraft.current = null;
    setConditionRevision((current) => current + 1);
  }

  function updateArg(id: number, key: string, value: ParameterValue) {
    markManualConditionEdit();
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
    markManualConditionEdit();
    setDraft((current) => [...current, makeDraft(block, nextId.current++)]);
  }

  function removeCondition(id: number) {
    markManualConditionEdit();
    setDraft((current) => current.filter((item) => item.id !== id));
  }

  function applySuggestion(conditions: EditableScreenCondition[]) {
    draftEpoch.current += 1;
    undoDraft.current = draft;
    setDraft(
      conditions.map((condition) => ({
        id: nextId.current++,
        key: condition.key,
        args: condition.args,
      })),
    );
    setForcedStaleReason("condition");
  }

  function undoSuggestion() {
    if (undoDraft.current === null) return;
    draftEpoch.current += 1;
    setDraft(undoDraft.current);
    undoDraft.current = null;
    setForcedStaleReason(null);
  }

  function invalidateSource() {
    sourceEpoch.current += 1;
    setResult(null);
    setApplied(null);
    setAppliedKey(null);
    setCursors([null]);
    setPageIndex(0);
    setForcedStaleReason("source");
    setError(null);
    catalog.refetch();
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
    const startedAtEpoch = sourceEpoch.current;
    const startedAtDraftEpoch = draftEpoch.current;
    try {
      const envelope = await fetchScreenRun(body);
      if (
        startedAtEpoch !== sourceEpoch.current ||
        envelope.data.source?.identity !== body.source_identity ||
        envelope.data.trade_date !== body.trade_date
      ) {
        throw new ApiError(409, "选股数据已更新，请重新筛选。");
      }
      setResult(envelope.data);
      setForcedStaleReason(startedAtDraftEpoch === draftEpoch.current ? null : "condition");
      if (
        nextIndex === 0 &&
        startedAtDraftEpoch === draftEpoch.current &&
        key === currentSnapshotKey.current
      ) {
        undoDraft.current = null;
        setSuccessfulRunRevision((current) => current + 1);
      }
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
      setError(
        caught instanceof ApiError && caught.status === 409
          ? null
          : caught instanceof Error
            ? caught.message
            : "筛选暂时无法完成，请稍后重试。",
      );
      if (caught instanceof ApiError && (caught.status === 409 || caught.status === 503)) {
        setResult(null);
        setApplied(null);
        setAppliedKey(null);
        setCursors([null]);
        setPageIndex(0);
        setForcedStaleReason(caught.status === 409 ? "source" : null);
        if (caught.status === 409) catalog.refetch();
      }
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
      source_identity: catalog.data?.source?.identity ?? null,
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
      <div className="screen-source">
        {catalog.data?.source_kind === "replica" ? (
          <span>{customRsiReady ? "RSI 可填 2–60 日" : "自定义 RSI 暂不可用"}</span>
        ) : null}
        {catalog.data?.source ? (
          <span>
            选股数据 <RelativeTime at={catalog.data.source.updated_at} suffix="更新" />
          </span>
        ) : null}
        <Button size="sm" variant="ghost" aria-label="刷新选股数据" onClick={catalog.refetch}>
          刷新
        </Button>
      </div>
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
          <ScreenNaturalLanguage
            available={catalog.data?.nl_generate_available === true}
            sourceKind={catalog.data?.source_kind ?? null}
            sourceIdentity={catalog.data?.source?.identity ?? null}
            tradeDate={tradeDate}
            conditionRevision={conditionRevision}
            successfulRunRevision={successfulRunRevision}
            blocks={blocks}
            onApply={applySuggestion}
            onUndo={undoSuggestion}
            onConflict={invalidateSource}
          />
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
              <EmptyState title="选股数据暂不可用" hint="稍后点「刷新」重试。" />
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
                        onClick={() => removeCondition(condition.id)}
                      >
                        删除
                      </Button>
                    </div>
                    {block.parameters.length > 0 ? (
                      <div className="screen-params">
                        {block.parameters.map((parameter) =>
                          parameter.custom_ma ? (
                            <CustomMaParamControl
                              key={parameter.key}
                              parameter={parameter}
                              value={condition.args[parameter.key] ?? null}
                              onChange={(value) => updateArg(condition.id, parameter.key, value)}
                              allowRsi={customRsiReady}
                              numberUnit={conditionNumberUnit(block, condition, parameter.key)}
                            />
                          ) : (
                            <ParamControl
                              key={parameter.key}
                              parameter={parameter}
                              value={condition.args[parameter.key] ?? null}
                              onChange={(value) => updateArg(condition.id, parameter.key, value)}
                              numberUnit={conditionNumberUnit(block, condition, parameter.key)}
                            />
                          ),
                        )}
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
              <Button onClick={() => setFormulaOpen(true)}>导入公式</Button>
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
            usesFundamental={applied !== null && usesFundamental(applied.conditions)}
          />
        </>
      )}
      <StockDrawer tsCode={selectedStock} onClose={() => setSelectedStock(null)} />
      {formulaOpen ? <FormulaPreviewDialog onClose={() => setFormulaOpen(false)} /> : null}
    </>
  );
}
