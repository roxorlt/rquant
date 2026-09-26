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
  const snapshotKey = JSON.stringify({
    tradeDate,
    draft: draft.map(({ key, args }) => ({ key, args })),
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
    catalog.data?.available === true && dates.length > 0 && tradeDate !== null && draft.length > 0;

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
