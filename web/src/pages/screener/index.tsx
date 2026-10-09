import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { useNavigate } from "react-router";
import { ApiError, type Schemas } from "@/api/client";
import {
  fetchScreenExecutionResults,
  isFundamentalScreenField,
  type ScreenBlock,
  type ScreenExecutionView,
  type ScreenPresetSaveRequest,
  type ScreenQueryDefinition,
  type ScreenRunData,
  type ScreenRunRequest,
  screenQueryTransport,
  useScreenCatalog,
  useScreenQueryHistory,
} from "@/api/screen";
import { useCurrentMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import {
  Button,
  EmptyState,
  PageHeader,
  PageSkeleton,
  Panel,
  type ParameterValue,
  RelativeTime,
} from "@/ui";
import { type ScreenConditionDraft, ScreenConditionEditor } from "../shared/ScreenConditionEditor";
import { FormulaPreviewDialog } from "./FormulaPreviewDialog";
import { type RankingDraft, RankingEditor } from "./RankingEditor";
import { ScreenAlertImport } from "./ScreenAlertImport";
import { ScreenIntraday } from "./ScreenIntraday";
import { type EditableScreenCondition, ScreenNaturalLanguage } from "./ScreenNaturalLanguage";
import { ScreenPoolSave } from "./ScreenPoolSave";
import { ScreenQueryHistory } from "./ScreenQueryHistory";
import { ScreenQueryPresets } from "./ScreenQueryPresets";
import { ScreenResults } from "./ScreenResults";
import { screenPoolSaveStorage } from "./screenPoolSaveSession";
import {
  ScreenQueryCommandSession,
  type ScreenQueryCommandSnapshot,
} from "./screenQueryCommandSession";
import "./screener.css";
import { AiScreenBacktest } from "@/app/AiAssistantDrawer";

type Draft = ScreenConditionDraft;
const PAGE_SIZE = 20;
const EMPTY_COMMAND: ScreenQueryCommandSnapshot = {
  original: null,
  status: "idle",
  data: null,
  busy: false,
  storageAvailable: true,
  message: null,
};
function requestBody(
  definition: ScreenQueryDefinition,
  source: ScreenRunData["source"] = null,
): ScreenRunRequest {
  return {
    trade_date: definition.trade_date,
    source_identity: definition.source_identity,
    ranking: definition.ranking,
    conditions: definition.conditions.map((call) => ({ key: call.name, args: call.args })),
    page_size: PAGE_SIZE,
    cursor: null,
    mode: definition.mode,
    decision_cutoff: definition.cutoff,
    intraday_source_identity: source?.intraday_source_identity ?? null,
  };
}
function snapshotOf(body: ScreenRunRequest): string {
  return JSON.stringify({
    mode: body.mode ?? "daily",
    tradeDate: body.trade_date,
    draft: body.conditions.map(({ key, args }) => ({ key, args })),
    ranking: (body.ranking?.conditions ?? []).map(({ metric, ascending, weight }) => ({
      metric,
      ascending,
      weight: String(weight),
    })),
    topN: body.ranking ? String(body.ranking.top_n) : null,
  });
}
function editableArgs(args: Record<string, unknown>): Record<string, ParameterValue> {
  const result: Record<string, ParameterValue> = {};
  for (const [key, value] of Object.entries(args)) {
    if (
      value === null ||
      typeof value === "string" ||
      (typeof value === "number" && Number.isFinite(value)) ||
      (Array.isArray(value) && value.every((item) => typeof item === "string"))
    )
      result[key] = value;
    else throw new Error("原条件暂不能编辑，请查看原请求。");
  }
  return result;
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
  const navigate = useNavigate();
  const [mode, setMode] = useState<ScreenQueryDefinition["mode"]>("daily");
  const catalog = useScreenCatalog(mode);
  const meta = useCurrentMeta();
  const viewer = meta.data?.data.viewer ?? null;
  const privateHistory = useScreenQueryHistory(viewer);
  const scope = privateHistory.data?.owner_scope_tag ?? null;
  const commandSession = useMemo(
    () =>
      viewer !== null && scope !== null
        ? new ScreenQueryCommandSession(scope, screenPoolSaveStorage(), screenQueryTransport)
        : null,
    [viewer, scope],
  );
  const commandState = useSyncExternalStore(
    commandSession?.subscribe ?? (() => () => undefined),
    commandSession?.snapshot ?? (() => EMPTY_COMMAND),
  );
  const previousViewer = useRef(viewer);
  const previousSession = useRef(commandSession);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [presetsOpen, setPresetsOpen] = useState(false);
  const queryActions = useRef<HTMLDivElement>(null);
  const drawerFocus = useRef<{
    viewer: string | null;
    scope: string | null;
    drawer: "history" | "presets";
  } | null>(null);
  const drawerOwner = useRef({ viewer, scope, historyOpen, presetsOpen });
  drawerOwner.current = { viewer, scope, historyOpen, presetsOpen };
  const pageActive = useRef(true);
  useEffect(() => {
    pageActive.current = true;
    return () => {
      pageActive.current = false;
      drawerFocus.current = null;
    };
  }, []);
  const restoreDrawerFocus = useCallback(() => {
    const target = drawerFocus.current;
    if (!target) return true;
    const owner = drawerOwner.current;
    if (!pageActive.current || target.viewer !== owner.viewer || target.scope !== owner.scope) {
      drawerFocus.current = null;
      return true;
    }
    if (owner.historyOpen || owner.presetsOpen) return false;
    const title = target.drawer === "history" ? "选股历史" : "常用条件";
    const visible = [...document.querySelectorAll('[role="dialog"]')].some((dialog) =>
      dialog
        .getAttribute("aria-labelledby")
        ?.split(/\s+/)
        .some((id) => document.getElementById(id)?.textContent?.trim() === title),
    );
    if (visible) return false;
    const entry = queryActions.current?.querySelector<HTMLButtonElement>(
      `button[data-screen-query-entry="${target.drawer}"]`,
    );
    if (!entry?.isConnected) return false;
    drawerFocus.current = null;
    if (!entry.disabled) entry.focus();
    return true;
  }, []);
  useEffect(() => {
    const target = drawerFocus.current;
    if (historyOpen || presetsOpen || !target) return;
    if (target.viewer !== viewer || target.scope !== scope) {
      drawerFocus.current = null;
      return;
    }
    // Interrupted opening can remove the dialog without a false motion callback.
    const observer = new MutationObserver(() => {
      if (restoreDrawerFocus()) observer.disconnect();
    });
    observer.observe(document.body, { childList: true, subtree: true });
    if (restoreDrawerFocus()) observer.disconnect();
    return () => observer.disconnect();
  }, [historyOpen, presetsOpen, viewer, scope, restoreDrawerFocus]);
  function afterDrawerChange(open: boolean, drawer: "history" | "presets") {
    if (!open && drawerFocus.current?.drawer === drawer) queueMicrotask(() => restoreDrawerFocus());
  }
  const [description, setDescription] = useState("");
  const [activeExecution, setActiveExecution] = useState<ScreenExecutionView | null>(null);
  const [draft, setDraft] = useState<Draft[]>([]);
  const [tradeDate, setTradeDate] = useState<string | null>(null);
  const [addKey, setAddKey] = useState("");
  const [initialized, setInitialized] = useState(false);
  const nextId = useRef(1);
  const undoDraft = useRef<Draft[] | null>(null);
  const undoRanking = useRef<{ rows: RankingDraft[]; topN: string } | null>(null);
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
    const prior = previousViewer.current;
    if (prior !== viewer && prior !== null) {
      previousSession.current?.dispose(true);
      try {
        for (let index = window.sessionStorage.length - 1; index >= 0; index--) {
          const key = window.sessionStorage.key(index);
          if (
            key?.startsWith("rquant.screen-command.v1:") ||
            key?.startsWith("rquant.screen.recent-descriptions.v1:") ||
            key?.startsWith("rquant.screen-alert-draft.v1:")
          )
            window.sessionStorage.removeItem(key);
        }
      } catch {
        /* New writes still require durable storage. */
      }
      sourceEpoch.current += 1;
      draftEpoch.current += 1;
      setDraft([]);
      setRankDraft([]);
      setDescription("");
      setInitialized(false);
      setMode("daily");
      setResult(null);
      setApplied(null);
      setAppliedKey(null);
      setActiveExecution(null);
      setHistoryOpen(false);
      setPresetsOpen(false);
      setRunning(false);
      setError(null);
    }
    previousViewer.current = viewer;
    previousSession.current = commandSession;
  }, [viewer, commandSession]);
  useEffect(() => () => commandSession?.dispose(), [commandSession]);

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
    undoRanking.current = null;
    setError(null);
    setActiveExecution(null);
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
    if (tradeDate === null && dates.length > 0) setTradeDate(dates[0] ?? null);
  }, [dates, tradeDate]);

  const byKey = new Map(blocks.map((block) => [block.key, block]));
  const unavailableCondition = draft.some(
    (condition) =>
      !byKey.has(condition.key) ||
      Object.entries(condition.args).some(([key, value]) => {
        const parameter = byKey.get(condition.key)?.parameters.find((item) => item.key === key);
        return (
          !parameter ||
          ((isFundamentalScreenField(value) ||
            (typeof value === "string" && value.startsWith("INTRADAY_"))) &&
            !parameter.options?.some((option) => option.value === value)) ||
          (parameter.input === "choice" &&
            !parameter.options?.some((option) => option.value === String(value)))
        );
      }),
  );
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
    mode,
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
    dates.includes(tradeDate) &&
    draft.length > 0 &&
    catalog.data?.source?.identity != null &&
    rankingError === null &&
    !unavailableCondition &&
    commandSession !== null &&
    !commandState.original &&
    commandState.storageAvailable;
  const saveCandidate =
    mode === "daily" &&
    result?.status === "ready" &&
    !stale &&
    !running &&
    rankingError === null &&
    applied !== null &&
    appliedKey === snapshotKey &&
    applied.source_identity !== null &&
    result.source?.identity === applied.source_identity &&
    result.trade_date === applied.trade_date
      ? applied
      : null;
  const saveBlockedReason =
    result?.status !== "ready"
      ? null
      : running
        ? "筛选完成后再保存。"
        : mode === "intraday"
          ? "盘中条件暂不能作为日终池子保存。"
          : forcedStaleReason === "source" ||
              result.source?.identity !== catalog.data?.source?.identity
            ? "选股数据已更新，请重新运行筛选。"
            : saveCandidate === null
              ? "条件已改，请重新运行筛选。"
              : null;

  function markManualConditionEdit() {
    draftEpoch.current += 1;
    undoDraft.current = null;
    undoRanking.current = null;
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

  function applySuggestion(
    conditions: EditableScreenCondition[],
    ranking?: Schemas["ScreenRankingPlan"] | null,
  ) {
    draftEpoch.current += 1;
    undoDraft.current = draft;
    if (ranking !== undefined) {
      undoRanking.current = { rows: rankDraft, topN };
      setRankDraft(
        (ranking?.conditions ?? []).map((row) => ({
          id: nextRankId.current++,
          metric: row.metric,
          ascending: row.ascending,
          weight: String(row.weight),
        })),
      );
      setTopN(String(ranking?.top_n ?? 20));
    }
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
    if (undoRanking.current) {
      setRankDraft(undoRanking.current.rows);
      setTopN(undoRanking.current.topN);
      undoRanking.current = null;
    }
    undoDraft.current = null;
    undoRanking.current = null;
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

  function refill(definition: ScreenQueryDefinition) {
    try {
      const restored = definition.conditions.map((call) => ({
        id: nextId.current++,
        key: call.name,
        args: editableArgs(call.args ?? {}),
      }));
      draftEpoch.current += 1;
      undoDraft.current = null;
      undoRanking.current = null;
      setDraft(restored);
      setTradeDate(definition.trade_date);
      setDescription(definition.description);
      setMode(definition.mode);
      setRankDraft(
        (definition.ranking?.conditions ?? []).map((row) => ({
          id: nextRankId.current++,
          metric: row.metric,
          ascending: row.ascending,
          weight: String(row.weight),
        })),
      );
      setTopN(String(definition.ranking?.top_n ?? 20));
      setInitialized(true);
      setConditionRevision((current) => current + 1);
      setForcedStaleReason(
        definition.source_identity === catalog.data?.source?.identity ? "condition" : "source",
      );
      setHistoryOpen(false);
      setPresetsOpen(false);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "原条件暂不能回填。");
    }
  }

  const candidateDefinition: ScreenQueryDefinition | null =
    canRun && tradeDate !== null && catalog.data?.source
      ? {
          schema_version: 1,
          description,
          mode,
          trade_date: tradeDate,
          source_kind: catalog.data.source_kind,
          source_identity: catalog.data.source.identity,
          cutoff: mode === "intraday" ? (catalog.data.source.cutoff ?? null) : null,
          conditions: draft.map(({ key, args }) => ({ name: key, args })),
          ranking: rankDraft.length
            ? {
                top_n: Number(topN),
                conditions: rankDraft.map(({ metric, ascending, weight }) => ({
                  metric,
                  ascending,
                  weight: Number(weight),
                })),
              }
            : null,
        }
      : null;

  function showExecution(
    execution: ScreenExecutionView,
    rows: ScreenRunData["rows"],
    nextCursor: string | null,
    body: ScreenRunRequest,
    key: string,
  ) {
    if (
      execution.status !== "succeeded" ||
      execution.base_count == null ||
      execution.total == null ||
      execution.unknown_count == null ||
      !execution.source ||
      execution.source.identity !== body.source_identity ||
      execution.definition.trade_date !== body.trade_date
    )
      throw new ApiError(409, "选股数据已更新，请重新筛选。");
    setResult({
      status: "ready",
      trade_date: execution.definition.trade_date,
      base_count: execution.base_count,
      total: execution.total,
      unknown_count: execution.unknown_count,
      ranked_count: execution.ranked_count ?? null,
      steps: execution.steps ?? [],
      rows,
      source: execution.source,
      next_cursor: nextCursor,
    });
    setActiveExecution(execution);
    setApplied(body);
    setAppliedKey(key);
    setCursors([null]);
    setPageIndex(0);
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
    if (running || commandState.busy || !commandSession) return;
    setRunning(true);
    setError(null);
    const startedAtEpoch = sourceEpoch.current;
    const startedAtDraftEpoch = draftEpoch.current;
    try {
      const definition = candidateDefinition;
      if (nextIndex !== 0 || definition === null) return;
      await commandSession.submit({
        action: "execute",
        command: {
          kind: "execute_screen_query",
          command_id: `screen-${crypto.randomUUID()}`,
          requested_at: new Date().toISOString(),
          definition,
          page_size: PAGE_SIZE,
        },
      });
      const reply = commandSession.snapshot().data;
      if (!reply?.execution || !reply.results) return;
      if (
        startedAtEpoch !== sourceEpoch.current ||
        reply.execution.source?.identity !== body.source_identity ||
        reply.execution.definition.trade_date !== body.trade_date
      ) {
        throw new ApiError(409, "选股数据已更新，请重新筛选。");
      }
      showExecution(reply.execution, reply.results.rows, reply.results.next_cursor, body, key);
      setForcedStaleReason(startedAtDraftEpoch === draftEpoch.current ? null : "condition");
      if (
        nextIndex === 0 &&
        startedAtDraftEpoch === draftEpoch.current &&
        key === currentSnapshotKey.current
      ) {
        undoDraft.current = null;
        undoRanking.current = null;
        setSuccessfulRunRevision((current) => current + 1);
      }
      void privateHistory.refetch();
    } catch (caught) {
      if (viewer !== previousViewer.current) return;
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
      if (viewer === previousViewer.current) setRunning(false);
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
      mode,
      decision_cutoff: mode === "intraday" ? (catalog.data?.source?.cutoff ?? null) : null,
      intraday_source_identity:
        mode === "intraday" ? (catalog.data?.source?.intraday_source_identity ?? null) : null,
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
    if (!applied || !activeExecution || running) return;
    const captured = activeExecution;
    const epoch = sourceEpoch.current;
    setRunning(true);
    setError(null);
    void fetchScreenExecutionResults(captured.execution_id, cursor)
      .then((data) => {
        const results = data.results;
        if (
          !results ||
          epoch !== sourceEpoch.current ||
          data.owner_scope_tag !== scope ||
          results.artifact_sha256 !== captured.artifact_sha256 ||
          results.execution_id !== captured.execution_id
        )
          throw new ApiError(409, "原结果已变化，请重试。");
        setResult((current) =>
          current === null
            ? null
            : {
                ...current,
                rows: results.rows,
                next_cursor: results.next_cursor,
              },
        );
        setCursors((current) => {
          const next = current.slice(0, index);
          next[index] = cursor;
          return next;
        });
        setPageIndex(index);
      })
      .catch((caught: unknown) => {
        if (epoch === sourceEpoch.current)
          setError(caught instanceof Error ? caught.message : "原结果暂不可用，请重试。");
      })
      .finally(() => {
        if (viewer === previousViewer.current) setRunning(false);
      });
  }

  async function inspect(execution: ScreenExecutionView) {
    const epoch = sourceEpoch.current;
    try {
      const data = await fetchScreenExecutionResults(execution.execution_id);
      if (epoch !== sourceEpoch.current) return;
      if (
        data.owner_scope_tag !== scope ||
        !data.results ||
        data.results.artifact_sha256 !== execution.artifact_sha256
      )
        throw new Error("原结果暂不可用，请重试。");
      refill(execution.definition);
      const body = requestBody(execution.definition, execution.source);
      showExecution(execution, data.results.rows, data.results.next_cursor, body, snapshotOf(body));
      setForcedStaleReason(
        execution.definition.source_identity === catalog.data?.source?.identity ? null : "source",
      );
    } catch (caught) {
      if (epoch === sourceEpoch.current)
        setError(caught instanceof Error ? caught.message : "原结果暂不可用，请重试。");
    }
  }

  async function recover(operation: "lookup" | "resume") {
    if (!commandSession || commandState.busy) return;
    const epoch = sourceEpoch.current;
    await commandSession.recover(operation);
    const data = commandSession.snapshot().data;
    if (epoch !== sourceEpoch.current || !data?.execution || !data.results) return;
    refill(data.execution.definition);
    const body = requestBody(data.execution.definition, data.execution.source);
    showExecution(
      data.execution,
      data.results.rows,
      data.results.next_cursor,
      body,
      snapshotOf(body),
    );
    setForcedStaleReason(
      data.execution.definition.source_identity === catalog.data?.source?.identity
        ? null
        : "source",
    );
    void privateHistory.refetch();
  }

  async function savePreset(request: ScreenPresetSaveRequest) {
    if (!commandSession || commandState.busy || commandState.original) return null;
    const captured = commandSession;
    await commandSession.submit({ action: "presets_save", request });
    if (captured !== previousSession.current) return null;
    return commandSession.snapshot().data;
  }

  const header = (
    <>
      <PageHeader eyebrow="研究" title="选股器" />
      <ScreenIntraday
        mode={mode}
        source={catalog.data?.source ?? null}
        onChange={(next) => {
          if (next === mode) return;
          sourceEpoch.current += 1;
          setMode(next);
          setTradeDate(null);
          setResult(null);
          setApplied(null);
          setAppliedKey(null);
          setActiveExecution(null);
          setCursors([null]);
          setPageIndex(0);
          setForcedStaleReason("source");
          setError(null);
        }}
      />
    </>
  );
  if (catalog.isLoading || (viewer !== null && privateHistory.isLoading))
    return (
      <>
        {header}
        <PageSkeleton />
      </>
    );

  return (
    <>
      {header}
      <div className="screen-source">
        <div className="screen-query-actions" ref={queryActions}>
          <Button
            size="sm"
            data-screen-query-entry="history"
            onClick={() => {
              drawerFocus.current = { viewer, scope, drawer: "history" };
              setHistoryOpen(true);
            }}
            disabledReason={viewer === null ? "请先登录。" : undefined}
          >
            历史
          </Button>
          <Button
            size="sm"
            data-screen-query-entry="presets"
            onClick={() => {
              drawerFocus.current = { viewer, scope, drawer: "presets" };
              setPresetsOpen(true);
            }}
            disabledReason={viewer === null ? "请先登录。" : undefined}
          >
            常用条件
          </Button>
        </div>
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
      {commandState.message ? (
        <div className="screen-query-recovery">
          <p role="status">{commandState.message}</p>
          {commandState.original ? (
            <div className="screen-query-actions">
              <Button
                size="sm"
                disabled={commandState.busy}
                onClick={() => {
                  void recover("lookup");
                }}
              >
                查询原请求
              </Button>
              <Button
                size="sm"
                disabled={commandState.busy}
                onClick={() => {
                  void recover("resume");
                }}
              >
                恢复原请求
              </Button>
            </div>
          ) : null}
        </div>
      ) : null}
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
            key={viewer ?? "anonymous"}
            ownerScope={scope}
            description={description}
            onDescriptionChange={setDescription}
            available={catalog.data?.nl_generate_available === true}
            sourceKind={
              catalog.data?.source_kind === "intraday" ? null : (catalog.data?.source_kind ?? null)
            }
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
                  {tradeDate && !dates.includes(tradeDate) ? (
                    <option value={tradeDate}>{tradeDate}（暂不可用）</option>
                  ) : null}
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
            <ScreenConditionEditor
              conditions={draft}
              blocks={blocks}
              allowRsi={customRsiReady}
              onUpdate={updateArg}
              onRemove={removeCondition}
            />
            {draft.length === 0 ? (
              <EmptyState title="还没有条件" hint="从目录添加一条条件。" />
            ) : null}
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
                disabledReason={
                  !commandSession
                    ? "选股记录暂不可用，请稍后重试。"
                    : commandState.original
                      ? "先核对原请求。"
                      : !commandState.storageAvailable
                        ? "浏览器存储不可用，请先核对历史。"
                        : unavailableCondition
                          ? "原条件暂不可用，请核对后再运行。"
                          : !canRun
                            ? "先选好数据日期并添加条件"
                            : undefined
                }
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
            batchRevision={JSON.stringify([
              snapshotKey,
              successfulRunRevision,
              conditionRevision,
              sourceEpoch.current,
              draftEpoch.current,
            ])}
            onStock={setSelectedStock}
            onPrevious={() => runPage(pageIndex - 1, cursors[pageIndex - 1] ?? null)}
            onNext={() => runPage(pageIndex + 1, result?.next_cursor ?? null)}
            usesFundamental={applied !== null && usesFundamental(applied.conditions)}
          />
          <AiScreenBacktest
            viewer={viewer ?? null}
            execution={activeExecution}
            blocked={running || (result !== null && stale)}
          />
          <ScreenAlertImport
            key={`alert-${scope ?? "anonymous"}`}
            ownerScope={scope}
            execution={activeExecution}
            blockedReason={
              running
                ? "筛选完成后再设置。"
                : result !== null && stale
                  ? "条件或数据已改，请重新筛选。"
                  : null
            }
            onOpen={(draftId) => {
              void navigate(`/monitor?conditionDraft=${draftId}`);
            }}
          />
          <ScreenPoolSave
            candidate={saveCandidate}
            blockedReason={saveBlockedReason}
            blocks={blocks}
            rankingMetrics={rankMetrics}
            dailyWriterCapability={privateHistory.data?.daily_writer_capability ?? null}
            dailyRunEvidence={privateHistory.data?.daily_run_evidence ?? []}
            onCheckEvidence={() => {
              void privateHistory.refetch();
            }}
          />
        </>
      )}
      <StockDrawer tsCode={selectedStock} onClose={() => setSelectedStock(null)} />
      <ScreenQueryHistory
        key={`history-${viewer ?? "anonymous"}`}
        viewer={viewer}
        open={historyOpen}
        onClose={() => setHistoryOpen(false)}
        afterOpenChange={(open) => afterDrawerChange(open, "history")}
        onRestore={refill}
        onInspect={(execution) => {
          void inspect(execution);
        }}
        onRecover={(original) => {
          if (!commandSession || commandState.original) {
            setError("先核对当前原请求。");
            return;
          }
          const captured = commandSession;
          const epoch = sourceEpoch.current;
          setHistoryOpen(false);
          void commandSession.submit(original, "resume").then(() => {
            if (epoch !== sourceEpoch.current || captured !== previousSession.current) return;
            const data = captured.snapshot().data;
            if (data?.owner_scope_tag === scope && data.execution) void inspect(data.execution);
          });
        }}
      />
      <ScreenQueryPresets
        key={`presets-${viewer ?? "anonymous"}`}
        viewer={viewer}
        open={presetsOpen}
        definition={candidateDefinition}
        busy={commandState.busy || commandState.original !== null}
        onClose={() => setPresetsOpen(false)}
        afterOpenChange={(open) => afterDrawerChange(open, "presets")}
        onRestore={refill}
        onSave={savePreset}
      />
      {formulaOpen ? <FormulaPreviewDialog onClose={() => setFormulaOpen(false)} /> : null}
    </>
  );
}
