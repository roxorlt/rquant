import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router";
import { apiClient } from "@/api/client";
import {
  type CustomBlockItem,
  type PoolItem,
  type RuleCatalogItem,
  type ScreenerBlock,
  type ScreenerFunnelStep,
  type ScreenerRunData,
  type ScreenerUpstream,
  type ScreenRow,
  useAddWatch,
  useSavePool,
} from "@/api/endpoints";
import { presetLabel } from "@/copy/publicText";
import { formatPrice } from "@/format/number";
import { usePreference } from "@/theme/storage";
import { AppDrawer, Button, ChangeText, FailureNotice, useToast } from "@/ui";
import { failureCopy } from "@/ui/failure";
import { StockLink } from "@/ui/StockLink";
import { BlockListWorkbench, FunnelStrip, NlWhiteBoxBar } from "./BlockWorkbench";
import { applyClientBlocks, dedupScreenRows, type Rank } from "./Builder";

export type CanvasEdgeOp = "inherit" | "union" | "intersect" | "exclude";

export interface CanvasParentLink {
  nodeId: string;
  op: CanvasEdgeOp;
  lookback: string;
}

export interface CanvasNode {
  id: string;
  name: string;
  subtitle: string;
  kind: "root" | "daily" | "intraday";
  x: number;
  y: number;
  trigger: string;
  actions: string;
  rootUpstream: ScreenerUpstream;
  parents: CanvasParentLink[];
  blocks: ScreenerBlock[];
  ranks: Rank[];
}

export interface CanvasGraph {
  version: 1;
  nodes: CanvasNode[];
}

interface NodeEvalResult {
  status: "loading" | "ready" | "error";
  inputCount: number;
  finalCount: number;
  steps: ScreenerFunnelStep[];
  rows: ScreenRow[];
  error?: string;
}

const NODE_WIDTH = 248;
const NODE_HEIGHT = 118;
const MIN_ZOOM = 0.45;
const MAX_ZOOM = 1.85;

const EDGE_OP_LABEL: Record<CanvasEdgeOp, string> = {
  inherit: "继承",
  union: "+ 并集",
  intersect: "∩ 交集",
  exclude: "− 排除",
};

const LOOKBACK_OPTIONS = ["当日截面 T0", "昨日池 T-1", "回溯 T-3~T-1", "回溯 T-5~T-1"] as const;
const TRIGGER_OPTIONS = [
  "盘后 17:05 日频",
  "盘后 17:10 精选",
  "09:25 竞价定池",
  "盘中 3s 快照盯盘",
  "手动实时触发",
] as const;
const ACTION_OPTIONS = [
  "仅入池留档",
  "入池 + 盘后汇总推送",
  "入池 + PushDeer 告警",
  "PushDeer + 五虎刘海",
] as const;

export function buildDefaultCanvasGraph(): CanvasGraph {
  return {
    version: 1,
    nodes: [
      {
        id: "root_market",
        name: "全市场 A 股源",
        subtitle: "全 A 可交易截面底座",
        kind: "root",
        x: 32,
        y: 96,
        trigger: "每日收盘入库",
        actions: "供下游节点引用",
        rootUpstream: {
          base: "all_market",
          preset: null,
          pool: null,
          codes: [],
          base_label: "全市场 A 股",
          include_pools: [],
          intersect_pools: [],
          exclude_pools: [],
        },
        parents: [],
        blocks: [],
        ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
      },
      {
        id: "root_presets",
        name: "今日候选池源",
        subtitle: "当日系统预设去重合集",
        kind: "root",
        x: 32,
        y: 286,
        trigger: "盘后 17:00",
        actions: "供下游节点引用",
        rootUpstream: {
          base: "presets",
          preset: null,
          pool: null,
          codes: [],
          base_label: "今日候选池",
          include_pools: [],
          intersect_pools: [],
          exclude_pools: [],
        },
        parents: [],
        blocks: [],
        ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
      },
      {
        id: "pool_1",
        name: "Pool 1 · 基础初筛池",
        subtitle: "剔除 ST 与北交所基础滤网",
        kind: "daily",
        x: 336,
        y: 88,
        trigger: "盘后 17:05 日频",
        actions: "仅入池留档",
        rootUpstream: {
          base: "all_market",
          preset: null,
          pool: null,
          codes: [],
          base_label: null,
          include_pools: [],
          intersect_pools: [],
          exclude_pools: [],
        },
        parents: [{ nodeId: "root_market", op: "inherit", lookback: "当日截面 T0" }],
        blocks: [
          {
            id: "c-p1-st",
            type: "rule",
            enabled: true,
            rule_name: "not_st",
            args: {},
            op: ">=",
            value: 0,
          },
          {
            id: "c-p1-bj",
            type: "rule",
            enabled: true,
            rule_name: "not_bj",
            args: {},
            op: ">=",
            value: 0,
          },
        ],
        ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
      },
      {
        id: "pool_2_1",
        name: "Pool 2-1 · 趋势精选池",
        subtitle: "承接初筛池做均线趋势过滤",
        kind: "daily",
        x: 652,
        y: 52,
        trigger: "盘后 17:10 精选",
        actions: "入池 + 盘后汇总推送",
        rootUpstream: {
          base: "presets",
          preset: null,
          pool: null,
          codes: [],
          base_label: null,
          include_pools: [],
          intersect_pools: [],
          exclude_pools: [],
        },
        parents: [{ nodeId: "pool_1", op: "inherit", lookback: "回溯 T-3~T-1" }],
        blocks: [
          {
            id: "c-p21-ma",
            type: "rule",
            enabled: true,
            rule_name: "above_ma",
            args: { period: 20, offset: 0 },
            op: ">=",
            value: 0,
          },
        ],
        ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
      },
      {
        id: "pool_2_2",
        name: "Pool 2-2 · 多源盯盘池",
        subtitle: "候选源与趋势池并集实时监控",
        kind: "intraday",
        x: 652,
        y: 268,
        trigger: "盘中 3s 快照盯盘",
        actions: "PushDeer + 五虎刘海",
        rootUpstream: {
          base: "presets",
          preset: null,
          pool: null,
          codes: [],
          base_label: null,
          include_pools: [],
          intersect_pools: [],
          exclude_pools: [],
        },
        parents: [
          { nodeId: "root_presets", op: "inherit", lookback: "当日截面 T0" },
          { nodeId: "pool_2_1", op: "union", lookback: "昨日池 T-1" },
        ],
        blocks: [
          {
            id: "c-p22-st",
            type: "rule",
            enabled: true,
            rule_name: "not_st",
            args: {},
            op: ">=",
            value: 0,
          },
        ],
        ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
      },
    ],
  };
}

function isValidGraph(val: unknown): val is CanvasGraph {
  if (!val || typeof val !== "object") return false;
  const g = val as { version?: unknown; nodes?: unknown };
  return g.version === 1 && Array.isArray(g.nodes) && g.nodes.length > 0;
}

/** Topological sort with cycle immunity. */
function topoSortNodes(nodes: readonly CanvasNode[]): CanvasNode[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const ordered: CanvasNode[] = [];

  const visit = (node: CanvasNode) => {
    if (visited.has(node.id) || visiting.has(node.id)) return;
    visiting.add(node.id);
    for (const p of node.parents) {
      const parent = byId.get(p.nodeId);
      if (parent) visit(parent);
    }
    visiting.delete(node.id);
    visited.add(node.id);
    ordered.push(node);
  };

  for (const n of nodes) visit(n);
  return ordered;
}

/** Check if candidateParentId is a descendant of targetNodeId (prevents DAG cycles). */
function wouldCreateCycle(
  nodes: readonly CanvasNode[],
  targetNodeId: string,
  candidateParentId: string,
): boolean {
  if (targetNodeId === candidateParentId) return true;
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const seen = new Set<string>();
  const stack = [candidateParentId];
  while (stack.length > 0) {
    const cur = stack.pop();
    if (!cur) continue;
    if (cur === targetNodeId) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    const node = byId.get(cur);
    if (node) {
      for (const p of node.parents) stack.push(p.nodeId);
    }
  }
  return false;
}

function combineParentRows(
  parents: readonly CanvasParentLink[],
  results: ReadonlyMap<string, NodeEvalResult>,
): ScreenRow[] {
  const rowMap = new Map<string, ScreenRow>();
  let initialized = false;

  for (const link of parents) {
    const parentRows = results.get(link.nodeId)?.rows ?? [];
    if (link.op === "inherit" || link.op === "union") {
      for (const r of parentRows) {
        if (!rowMap.has(r.code)) rowMap.set(r.code, r);
      }
      initialized = true;
    } else if (link.op === "intersect") {
      if (!initialized) {
        for (const r of parentRows) rowMap.set(r.code, r);
        initialized = true;
      } else {
        const keep = new Set(parentRows.map((r) => r.code));
        for (const code of [...rowMap.keys()]) {
          if (!keep.has(code)) rowMap.delete(code);
        }
      }
    } else if (link.op === "exclude") {
      for (const r of parentRows) rowMap.delete(r.code);
    }
  }
  return [...rowMap.values()];
}

function rootScopeLabel(up: ScreenerUpstream, presets: readonly string[]): string {
  if (up.base === "all_market") return "全市场 A 股";
  if (up.base === "watchlist") return "自选股";
  if (up.base === "preset") return `预设 · ${presetLabel(up.preset ?? presets[0] ?? "")}`;
  if (up.base === "pool") return `池子 · ${up.pool ?? ""}`;
  return "今日候选池";
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

/** Semantic signature of node rules & upstream (ignores x/y position so dragging never refetches!). */
function semanticGraphSignature(nodes: readonly CanvasNode[]): string {
  return JSON.stringify(
    nodes.map((n) => ({
      id: n.id,
      rootUpstream: n.rootUpstream,
      parents: n.parents,
      blocks: n.blocks,
      ranks: n.ranks,
    })),
  );
}

export function PoolCanvasWorkbench({
  tradeDate,
  presets,
  pools,
  allMarketCount,
  fields,
  rules,
  customBlocks,
  fallbackRows,
  watchedCodes,
  onWatch,
  onCustomBlockSaved,
}: {
  tradeDate: string;
  presets: readonly string[];
  pools: readonly PoolItem[];
  allMarketCount: number;
  fields: Record<string, string>;
  rules: readonly RuleCatalogItem[];
  customBlocks: readonly CustomBlockItem[];
  fallbackRows: readonly ScreenRow[];
  watchedCodes: ReadonlySet<string>;
  onWatch: (row: ScreenRow) => void;
  onCustomBlockSaved: (item: CustomBlockItem) => void;
}) {
  const [graph, setGraph] = usePreference<CanvasGraph>(
    "screener.canvas.v1",
    buildDefaultCanvasGraph(),
    isValidGraph,
  );
  const [selectedId, setSelectedId] = useState<string>(
    () => graph.nodes[2]?.id ?? graph.nodes[0]?.id ?? "",
  );
  const [pan, setPan] = useState<{ x: number; y: number }>({ x: 16, y: 16 });
  const [zoom, setZoom] = useState<number>(1);
  const [evalMap, setEvalMap] = useState<Map<string, NodeEvalResult>>(() => new Map());
  const [previewStep, setPreviewStep] = useState<number | null>(null);

  // Node creation modal state ("root" = initial node, "downstream" = connected child node)
  const [createMode, setCreateMode] = useState<"root" | "downstream" | null>(null);
  const [draftName, setDraftName] = useState("");
  const [draftKind, setDraftKind] = useState<"root" | "daily" | "intraday">("daily");
  const [draftTrigger, setDraftTrigger] = useState<string>(TRIGGER_OPTIONS[0]);
  const [draftActions, setDraftActions] = useState<string>(ACTION_OPTIONS[0]);
  const [draftRootBase, setDraftRootBase] = useState<ScreenerUpstream["base"]>("all_market");
  const [draftRootSub, setDraftRootSub] = useState<string>("");
  const [draftParents, setDraftParents] = useState<CanvasParentLink[]>([]);
  const [draftBlocks, setDraftBlocks] = useState<ScreenerBlock[]>([]);

  // Save current node output as persistent Pool modal
  const [savePoolOpen, setSavePoolOpen] = useState(false);
  const [savePoolName, setSavePoolName] = useState("");
  const [savePoolMode, setSavePoolMode] = useState<"new" | "existing">("new");
  const [saveFail, setSaveFail] = useState<ReturnType<typeof failureCopy> | null>(null);

  const stageRef = useRef<HTMLDivElement>(null);
  const zoomRef = useRef(zoom);
  const panRef = useRef(pan);
  zoomRef.current = zoom;
  panRef.current = pan;

  const savePool = useSavePool();
  const addWatch = useAddWatch();
  const toast = useToast();
  const navigate = useNavigate();

  const customMap = useMemo(
    () => new Map(customBlocks.map((b) => [b.block_id, b])),
    [customBlocks],
  );
  const ruleMap = useMemo(() => new Map(rules.map((r) => [r.name, r])), [rules]);
  const userPools = useMemo(() => pools.filter((p) => p.name !== "自选股"), [pools]);

  const selectedNode = useMemo(
    () => graph.nodes.find((n) => n.id === selectedId) ?? graph.nodes[0],
    [graph.nodes, selectedId],
  );

  useEffect(() => {
    if (selectedNode && selectedNode.id !== selectedId) {
      setSelectedId(selectedNode.id);
    }
  }, [selectedNode, selectedId]);

  const updateNode = useCallback(
    (nodeId: string, patch: Partial<CanvasNode> | ((prev: CanvasNode) => CanvasNode)) => {
      setGraph({
        ...graph,
        nodes: graph.nodes.map((n) => {
          if (n.id !== nodeId) return n;
          return typeof patch === "function" ? patch(n) : { ...n, ...patch };
        }),
      });
    },
    [graph, setGraph],
  );

  // MacBook Trackpad two-finger pan & pinch-to-zoom (ctrlKey wheel + Safari GestureEvent)
  useEffect(() => {
    const el = stageRef.current;
    if (!el) return;

    const applyZoomAt = (nextZoomRaw: number, clientX: number, clientY: number) => {
      const rect = el.getBoundingClientRect();
      const cx = clientX - rect.left;
      const cy = clientY - rect.top;
      const prevZ = zoomRef.current;
      const nextZ = clamp(nextZoomRaw, MIN_ZOOM, MAX_ZOOM);
      const prevPan = panRef.current;
      const nextPan = {
        x: cx - ((cx - prevPan.x) / prevZ) * nextZ,
        y: cy - ((cy - prevPan.y) / prevZ) * nextZ,
      };
      zoomRef.current = nextZ;
      panRef.current = nextPan;
      setZoom(nextZ);
      setPan(nextPan);
    };

    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      if (e.ctrlKey || e.metaKey) {
        // MacBook trackpad two-finger pinch gesture (or Ctrl/Cmd + wheel)
        const factor = Math.exp(-e.deltaY * 0.01);
        applyZoomAt(zoomRef.current * factor, e.clientX, e.clientY);
      } else {
        // MacBook trackpad two-finger swipe pan
        const nextPan = {
          x: panRef.current.x - e.deltaX,
          y: panRef.current.y - e.deltaY,
        };
        panRef.current = nextPan;
        setPan(nextPan);
      }
    };

    let gestureBaseZoom = 1;
    const onGestureStart = (ev: Event) => {
      ev.preventDefault();
      gestureBaseZoom = zoomRef.current;
    };
    const onGestureChange = (ev: Event) => {
      ev.preventDefault();
      const ge = ev as Event & { scale?: number; clientX?: number; clientY?: number };
      if (typeof ge.scale === "number" && ge.scale > 0) {
        const rect = el.getBoundingClientRect();
        applyZoomAt(
          gestureBaseZoom * ge.scale,
          ge.clientX ?? rect.left + rect.width / 2,
          ge.clientY ?? rect.top + rect.height / 2,
        );
      }
    };

    el.addEventListener("wheel", onWheel, { passive: false });
    el.addEventListener("gesturestart", onGestureStart, { passive: false });
    el.addEventListener("gesturechange", onGestureChange, { passive: false });
    return () => {
      el.removeEventListener("wheel", onWheel);
      el.removeEventListener("gesturestart", onGestureStart);
      el.removeEventListener("gesturechange", onGestureChange);
    };
  }, []);

  //Topological live evaluation via POST /api/v1/screener/run (with client fallback)
  const sig = useMemo(() => semanticGraphSignature(graph.nodes), [graph.nodes]);
  const graphNodesRef = useRef(graph.nodes);
  graphNodesRef.current = graph.nodes;

  useEffect(() => {
    if (!sig) return;
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      const ordered = topoSortNodes(graphNodesRef.current);
      const localResults = new Map<string, NodeEvalResult>();

      setEvalMap((prev) => {
        const next = new Map(prev);
        for (const n of ordered) {
          const existing = next.get(n.id);
          next.set(
            n.id,
            existing
              ? { ...existing, status: "loading" }
              : { status: "loading", inputCount: 0, finalCount: 0, steps: [], rows: [] },
          );
        }
        return next;
      });

      const cleanFallback = dedupScreenRows(fallbackRows);

      for (const node of ordered) {
        if (cancelled) return;
        const isDownstream = node.parents.length > 0;
        const parentRows = isDownstream ? combineParentRows(node.parents, localResults) : [];
        const parentNames = node.parents
          .map(
            (p) =>
              ordered
                .find((x) => x.id === p.nodeId)
                ?.name.split("·")[0]
                ?.trim() ?? p.nodeId,
          )
          .join(" / ");
        const upstreamReq: ScreenerUpstream = isDownstream
          ? {
              base: "codes",
              codes: parentRows.map((r) => r.code),
              base_label: `上游 ${parentNames || "节点"}`,
              preset: null,
              pool: null,
              include_pools: [],
              intersect_pools: [],
              exclude_pools: [],
            }
          : node.rootUpstream;

        if (isDownstream && parentRows.length === 0) {
          const emptyRes: NodeEvalResult = {
            status: "ready",
            inputCount: 0,
            finalCount: 0,
            steps: [
              {
                step: 0,
                block_id: null,
                label: `上游 ${parentNames || "节点"}`,
                remaining: 0,
                dropped: 0,
                dropped_pct: 0,
              },
            ],
            rows: [],
          };
          localResults.set(node.id, emptyRes);
          if (!cancelled) setEvalMap(new Map(localResults));
          continue;
        }

        try {
          const { data, response } = await apiClient().POST("/api/v1/screener/run", {
            body: {
              upstream: upstreamReq,
              blocks: node.blocks,
              ranks: node.ranks,
            },
          });
          const payload = (data as { data?: ScreenerRunData } | undefined)?.data;
          if (response.ok && payload && Array.isArray(payload.rows)) {
            const deduped = dedupScreenRows(payload.rows);
            const steps = payload.steps ?? [];
            const inputN = steps[0]?.remaining ?? deduped.length;
            const nodeRes: NodeEvalResult = {
              status: "ready",
              inputCount: inputN,
              finalCount: deduped.length,
              steps,
              rows: deduped,
            };
            localResults.set(node.id, nodeRes);
            if (!cancelled) setEvalMap(new Map(localResults));
            continue;
          }
        } catch {
          // Fall through to client-side block evaluation
        }

        const targetPreset = node.rootUpstream.base === "preset" ? node.rootUpstream.preset : null;
        const seedRows = isDownstream
          ? parentRows
          : targetPreset
            ? cleanFallback.filter((r) => (r.presets ?? r.preset.split(",")).includes(targetPreset))
            : cleanFallback;
        const clientEval = applyClientBlocks(
          seedRows,
          node.blocks,
          fields,
          customMap,
          ruleMap,
          null,
        );
        const fallbackNodeRes: NodeEvalResult = {
          status: "ready",
          inputCount: seedRows.length,
          finalCount: clientEval.rows.length,
          steps: clientEval.steps,
          rows: clientEval.rows,
        };
        localResults.set(node.id, fallbackNodeRes);
        if (!cancelled) setEvalMap(new Map(localResults));
      }
    }, 140);

    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [sig, fallbackRows, fields, customMap, ruleMap]);

  const fitCanvas = () => {
    const el = stageRef.current;
    if (!el || graph.nodes.length === 0) {
      setZoom(1);
      setPan({ x: 16, y: 16 });
      return;
    }
    const minX = Math.min(...graph.nodes.map((n) => n.x));
    const minY = Math.min(...graph.nodes.map((n) => n.y));
    const maxX = Math.max(...graph.nodes.map((n) => n.x + NODE_WIDTH));
    const maxY = Math.max(...graph.nodes.map((n) => n.y + NODE_HEIGHT));
    const w = Math.max(320, maxX - minX + 64);
    const h = Math.max(240, maxY - minY + 64);
    const rect = el.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) return;
    const nextZ = clamp(Math.min(rect.width / w, rect.height / h, 1.1), MIN_ZOOM, 1.15);
    setZoom(nextZ);
    setPan({
      x: Math.round((rect.width - (maxX - minX) * nextZ) / 2 - minX * nextZ),
      y: Math.round((rect.height - (maxY - minY) * nextZ) / 2 - minY * nextZ),
    });
  };

  const autoLayout = () => {
    const ordered = topoSortNodes(graph.nodes);
    const depthMap = new Map<string, number>();
    for (const n of ordered) {
      if (n.parents.length === 0) {
        depthMap.set(n.id, 0);
      } else {
        const maxParent = Math.max(...n.parents.map((p) => depthMap.get(p.nodeId) ?? 0));
        depthMap.set(n.id, maxParent + 1);
      }
    }
    const columns = new Map<number, CanvasNode[]>();
    for (const n of ordered) {
      const d = depthMap.get(n.id) ?? 0;
      const list = columns.get(d) ?? [];
      list.push(n);
      columns.set(d, list);
    }
    const posById = new Map<string, { x: number; y: number }>();
    for (const [col, list] of columns) {
      list.forEach((n, idx) => {
        posById.set(n.id, { x: 32 + col * 308, y: 56 + idx * 176 });
      });
    }
    setGraph({
      ...graph,
      nodes: graph.nodes.map((n) => {
        const pos = posById.get(n.id);
        return pos ? { ...n, x: pos.x, y: pos.y } : n;
      }),
    });
    setZoom(1);
    setPan({ x: 16, y: 16 });
  };

  // Dragging blank canvas background to pan
  const onStagePointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    if ((e.target as HTMLElement).closest("[data-canvas-node]")) return;
    if ((e.target as HTMLElement).closest("[data-canvas-hud]")) return;
    const startX = e.clientX;
    const startY = e.clientY;
    const origPan = { ...panRef.current };
    const target = e.currentTarget;
    target.setPointerCapture(e.pointerId);

    const onMove = (ev: PointerEvent) => {
      const next = {
        x: origPan.x + (ev.clientX - startX),
        y: origPan.y + (ev.clientY - startY),
      };
      panRef.current = next;
      setPan(next);
    };
    const onUp = () => {
      target.removeEventListener("pointermove", onMove);
      target.removeEventListener("pointerup", onUp);
      target.removeEventListener("pointercancel", onUp);
    };
    target.addEventListener("pointermove", onMove);
    target.addEventListener("pointerup", onUp);
    target.addEventListener("pointercancel", onUp);
  };

  // Smoothly drag individual node card on canvas
  const onNodePointerDown = (e: React.PointerEvent<HTMLDivElement>, node: CanvasNode) => {
    if ((e.target as HTMLElement).closest("button")) return;
    e.stopPropagation();
    const startX = e.clientX;
    const startY = e.clientY;
    const origX = node.x;
    const origY = node.y;
    let moved = false;
    let latestX = origX;
    let latestY = origY;
    const el = e.currentTarget;
    try {
      el.setPointerCapture(e.pointerId);
    } catch {
      // Synthetic pointer events in unit tests may not register an active pointerId
    }

    const onMove = (ev: PointerEvent) => {
      const rawDx = ev.clientX - startX;
      const rawDy = ev.clientY - startY;
      if (Math.hypot(rawDx, rawDy) > 3) moved = true;
      if (moved) {
        const z = zoomRef.current || 1;
        latestX = Math.round(origX + rawDx / z);
        latestY = Math.round(origY + rawDy / z);
        setGraph({
          ...graph,
          nodes: graph.nodes.map((n) => (n.id === node.id ? { ...n, x: latestX, y: latestY } : n)),
        });
      }
    };
    const onUp = () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("pointercancel", onUp);
      if (!moved) {
        setSelectedId(node.id);
        setPreviewStep(null);
      }
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    window.addEventListener("pointercancel", onUp);
  };

  const openCreateRoot = () => {
    setCreateMode("root");
    setDraftName(`初始源 · ${graph.nodes.filter((n) => n.parents.length === 0).length + 1}`);
    setDraftKind("root");
    setDraftTrigger("盘后 17:05 日频");
    setDraftActions("仅入池留档");
    setDraftRootBase("all_market");
    setDraftRootSub(presets[0] ?? userPools[0]?.name ?? "");
    setDraftParents([]);
    setDraftBlocks([]);
  };

  const openCreateDownstream = (parentNode?: CanvasNode) => {
    const parent = parentNode ?? selectedNode ?? graph.nodes[0];
    setCreateMode("downstream");
    setDraftName(`下游池 · ${graph.nodes.filter((n) => n.parents.length > 0).length + 1}`);
    setDraftKind("daily");
    setDraftTrigger(TRIGGER_OPTIONS[0]);
    setDraftActions(ACTION_OPTIONS[0]);
    setDraftParents(
      parent ? [{ nodeId: parent.id, op: "inherit", lookback: LOOKBACK_OPTIONS[0] }] : [],
    );
    setDraftBlocks([]);
  };

  const confirmCreateNode = () => {
    const cleanName = draftName.trim();
    if (!cleanName) return;
    const id = `node_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 5)}`;
    const primaryParent =
      createMode === "downstream" && draftParents[0]
        ? graph.nodes.find((n) => n.id === draftParents[0]?.nodeId)
        : undefined;
    const x = primaryParent ? primaryParent.x + 308 : 36;
    const y = primaryParent
      ? primaryParent.y +
        (graph.nodes.filter((n) => n.x === primaryParent.x + 308).length > 0 ? 156 : 0)
      : 64 + graph.nodes.filter((n) => n.parents.length === 0).length * 168;

    const rootUpstream: ScreenerUpstream = {
      base: draftRootBase,
      preset: draftRootBase === "preset" ? draftRootSub || presets[0] || null : null,
      pool: draftRootBase === "pool" ? draftRootSub || userPools[0]?.name || null : null,
      codes: [],
      base_label: null,
      include_pools: [],
      intersect_pools: [],
      exclude_pools: [],
    };

    const created: CanvasNode = {
      id,
      name: cleanName,
      subtitle:
        createMode === "root"
          ? `初始范围：${rootScopeLabel(rootUpstream, presets)}`
          : `承接 ${draftParents.length} 个上游节点`,
      kind: createMode === "root" ? "root" : draftKind,
      x,
      y,
      trigger: draftTrigger,
      actions: draftActions,
      rootUpstream,
      parents: createMode === "downstream" ? draftParents : [],
      blocks: draftBlocks,
      ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
    };

    setGraph({ ...graph, nodes: [...graph.nodes, created] });
    setSelectedId(id);
    setPreviewStep(null);
    setCreateMode(null);
    toast(`已在画布新建节点『${cleanName}』`);
  };

  const deleteNode = (nodeId: string) => {
    if (graph.nodes.length <= 1) return;
    const remaining = graph.nodes
      .filter((n) => n.id !== nodeId)
      .map((n) => ({
        ...n,
        parents: n.parents.filter((p) => p.nodeId !== nodeId),
      }));
    setGraph({ ...graph, nodes: remaining });
    if (selectedId === nodeId && remaining[0]) {
      setSelectedId(remaining[0].id);
    }
  };

  const selectedEval = selectedNode ? evalMap.get(selectedNode.id) : undefined;
  const selectedRows = selectedEval?.rows ?? [];
  const selectedSteps = selectedEval?.steps ?? [];
  const [stepPreviewRows, setStepPreviewRows] = useState<ScreenRow[] | null>(null);

  useEffect(() => {
    if (previewStep === null || !selectedNode || previewStep >= selectedSteps.length - 1) {
      setStepPreviewRows(null);
      return;
    }
    let cancelled = false;
    const isDownstream = selectedNode.parents.length > 0;
    const parentRows = isDownstream ? combineParentRows(selectedNode.parents, evalMap) : [];
    const upstreamReq: ScreenerUpstream = isDownstream
      ? {
          base: "codes",
          codes: parentRows.map((r) => r.code),
          base_label: "上游节点",
          preset: null,
          pool: null,
          include_pools: [],
          intersect_pools: [],
          exclude_pools: [],
        }
      : selectedNode.rootUpstream;

    void (async () => {
      try {
        const { data, response } = await apiClient().POST("/api/v1/screener/run", {
          body: {
            upstream: upstreamReq,
            blocks: selectedNode.blocks,
            ranks: selectedNode.ranks,
            preview_step: previewStep,
          },
        });
        const payload = (data as { data?: ScreenerRunData } | undefined)?.data;
        if (!cancelled && response.ok && payload && Array.isArray(payload.rows)) {
          setStepPreviewRows(dedupScreenRows(payload.rows));
          return;
        }
      } catch {
        // Fall through to client preview
      }
      if (!cancelled) {
        const seedRows = isDownstream ? parentRows : dedupScreenRows(fallbackRows);
        const clientEval = applyClientBlocks(
          seedRows,
          selectedNode.blocks,
          fields,
          customMap,
          ruleMap,
          previewStep,
        );
        setStepPreviewRows(clientEval.rows);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [
    previewStep,
    selectedNode,
    selectedSteps.length,
    evalMap,
    fallbackRows,
    fields,
    customMap,
    ruleMap,
  ]);

  const displayedRows =
    previewStep !== null && stepPreviewRows !== null ? stepPreviewRows : selectedRows;

  const openSaveNodeAsPool = () => {
    if (!selectedNode) return;
    const clean = selectedNode.name
      .replace(/[·/\\]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    setSavePoolName(clean.slice(0, 32) || "画布策略池");
    setSavePoolMode("new");
    setSaveFail(null);
    setSavePoolOpen(true);
  };

  const confirmSaveNodePool = () => {
    const name = savePoolName.trim();
    if (!name) return;
    savePool.mutate(
      {
        name,
        description: `来自节点画布：${selectedNode?.name ?? ""}`,
        pool_refs: ["saved"],
        codes: selectedRows.map((r) => r.code),
        mode: savePoolMode,
      },
      {
        onSuccess: () => {
          setSavePoolOpen(false);
          setSaveFail(null);
          toast({
            text: `已将节点保存为池子『${name}』，共 ${selectedRows.length} 只`,
            action: { label: "去看看", onClick: () => navigate("/pools") },
          });
        },
        onError: (err) => setSaveFail(failureCopy(`池子『${name}』`, "保存", err)),
      },
    );
  };

  // Compute SVG edges across nodes
  const edges = useMemo(() => {
    const byId = new Map(graph.nodes.map((n) => [n.id, n]));
    const list: Array<{
      key: string;
      src: CanvasNode;
      dst: CanvasNode;
      link: CanvasParentLink;
      active: boolean;
      srcCount: number | null;
    }> = [];
    for (const dst of graph.nodes) {
      for (const link of dst.parents) {
        const src = byId.get(link.nodeId);
        if (!src) continue;
        const srcEval = evalMap.get(src.id);
        list.push({
          key: `${src.id}->${dst.id}`,
          src,
          dst,
          link,
          active: selectedId === src.id || selectedId === dst.id,
          srcCount: srcEval?.status === "ready" ? srcEval.finalCount : null,
        });
      }
    }
    return list;
  }, [graph.nodes, evalMap, selectedId]);

  const availableParentsForSelected = useMemo(() => {
    if (!selectedNode) return [];
    return graph.nodes.filter(
      (n) =>
        n.id !== selectedNode.id &&
        !selectedNode.parents.some((p) => p.nodeId === n.id) &&
        !wouldCreateCycle(graph.nodes, selectedNode.id, n.id),
    );
  }, [graph.nodes, selectedNode]);

  return (
    <section className="pc-shell" aria-label="池子节点依赖画布">
      <div className="pc-toolbar">
        <div className="pc-toolbar-left">
          <Button size="sm" variant="primary" onClick={openCreateRoot}>
            + 新建初始节点
          </Button>
          <Button size="sm" variant="quiet" onClick={() => openCreateDownstream(selectedNode)}>
            + 新建下游节点
          </Button>
          <Button size="sm" variant="ghost" onClick={autoLayout}>
            自动排版
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => {
              const fresh = buildDefaultCanvasGraph();
              setGraph(fresh);
              setSelectedId(fresh.nodes[2]?.id ?? fresh.nodes[0]?.id ?? "");
              setZoom(1);
              setPan({ x: 16, y: 16 });
            }}
          >
            恢复默认模板
          </Button>
        </div>
        <div className="pc-toolbar-right">
          <span className="hint">
            触控板双指滑动平移 · 双指捏合(Pinch)缩放 · 按住节点卡片任意拖拽排布
          </span>
        </div>
      </div>

      <div className="pc-split">
        {/* Left: Infinite Pan/Pinch-Zoom DAG Stage */}
        <div
          ref={stageRef}
          className="pc-stage"
          onPointerDown={onStagePointerDown}
          data-testid="pool-canvas-stage"
        >
          <div
            className="pc-world"
            style={{
              transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
              transformOrigin: "0 0",
            }}
          >
            <svg className="pc-edges-svg" width="2400" height="1600" aria-hidden="true">
              <defs>
                <marker
                  id="pc-arrow-muted"
                  viewBox="0 0 10 10"
                  refX="8"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse"
                >
                  <path d="M 0 1.5 L 8.5 5 L 0 8.5 z" fill="currentColor" opacity="0.45" />
                </marker>
                <marker
                  id="pc-arrow-active"
                  viewBox="0 0 10 10"
                  refX="8"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse"
                >
                  <path d="M 0 1.5 L 8.5 5 L 0 8.5 z" fill="var(--accent, #2563eb)" />
                </marker>
              </defs>
              {edges.map(({ key, src, dst, link, active }) => {
                const x1 = src.x + NODE_WIDTH;
                const y1 = src.y + NODE_HEIGHT / 2;
                const x2 = dst.x;
                const y2 = dst.y + NODE_HEIGHT / 2;
                const dx = Math.max(64, Math.abs(x2 - x1) * 0.5);
                const d = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
                return (
                  <path
                    key={key}
                    d={d}
                    className={`pc-edge${active ? " active" : ""}${link.op === "exclude" ? " dashed" : ""}`}
                    markerEnd={active ? "url(#pc-arrow-active)" : "url(#pc-arrow-muted)"}
                  />
                );
              })}
            </svg>

            {/* Edge Badges */}
            {edges.map(({ key, src, dst, link, active, srcCount }) => {
              const mx = (src.x + NODE_WIDTH + dst.x) / 2;
              const my = (src.y + dst.y + NODE_HEIGHT) / 2 - 10;
              return (
                <div
                  key={`lbl-${key}`}
                  className={`pc-edge-pill${active ? " active" : ""}`}
                  style={{ left: `${mx}px`, top: `${my}px` }}
                >
                  <span>{EDGE_OP_LABEL[link.op]}</span>
                  <span>·</span>
                  <span>{link.lookback}</span>
                  {srcCount !== null ? <strong className="num">({srcCount})</strong> : null}
                </div>
              );
            })}

            {/* Draggable Pool Nodes */}
            {graph.nodes.map((node) => {
              const ev = evalMap.get(node.id);
              const isSelected = selectedNode?.id === node.id;
              const enabledBlocks = node.blocks.filter((b) => b.enabled !== false).length;
              return (
                <div
                  key={node.id}
                  data-canvas-node={node.id}
                  className={`pc-node kind-${node.kind}${isSelected ? " selected" : ""}`}
                  style={{ left: `${node.x}px`, top: `${node.y}px`, width: `${NODE_WIDTH}px` }}
                  onPointerDown={(e) => onNodePointerDown(e, node)}
                >
                  <div className="pc-node-head">
                    <span className={`pc-dot kind-${node.kind}`} />
                    <strong className="pc-node-title" title={node.name}>
                      {node.name}
                    </strong>
                    <button
                      type="button"
                      className="pc-mini-btn"
                      title="以此节点为上游新建下游节点"
                      aria-label={`从 ${node.name} 新建下游`}
                      onClick={() => openCreateDownstream(node)}
                    >
                      +下游
                    </button>
                  </div>

                  <div className="pc-node-sub">
                    {node.parents.length === 0
                      ? `源范围：${rootScopeLabel(node.rootUpstream, presets)}`
                      : `上游 ${node.parents.length} 路 · ${enabledBlocks} 条过滤条件`}
                  </div>

                  <div className="pc-node-foot">
                    <span className="hint">{node.trigger}</span>
                    <span className="pc-node-count num">
                      {ev?.status === "loading" && ev.finalCount === 0
                        ? "计算中…"
                        : `${ev?.finalCount ?? (node.rootUpstream.base === "all_market" && node.blocks.length === 0 && allMarketCount > 0 ? allMarketCount : 0)} 只`}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Floating Trackpad Zoom HUD */}
          <div className="pc-hud" data-canvas-hud="true">
            <button
              type="button"
              className="pc-hud-btn"
              aria-label="缩小画布"
              onClick={() =>
                setZoom((z) => clamp(Math.round((z - 0.15) * 100) / 100, MIN_ZOOM, MAX_ZOOM))
              }
            >
              −
            </button>
            <button
              type="button"
              className="pc-hud-btn mono"
              aria-label="重置缩放 100%"
              onClick={() => {
                setZoom(1);
                setPan({ x: 16, y: 16 });
              }}
            >
              {Math.round(zoom * 100)}%
            </button>
            <button
              type="button"
              className="pc-hud-btn"
              aria-label="放大画布"
              onClick={() =>
                setZoom((z) => clamp(Math.round((z + 0.15) * 100) / 100, MIN_ZOOM, MAX_ZOOM))
              }
            >
              +
            </button>
            <button type="button" className="pc-hud-btn" onClick={fitCanvas}>
              适应屏幕
            </button>
          </div>
        </div>

        {/* Right: Selected Node Inspector Drawer */}
        {selectedNode ? (
          <aside className="pc-inspector" aria-label="节点配置抽屉">
            <div className="pc-insp-head">
              <div className="pc-insp-title-row">
                <input
                  className="pc-name-input"
                  aria-label="选中节点名称"
                  value={selectedNode.name}
                  onChange={(e) => updateNode(selectedNode.id, { name: e.target.value })}
                />
                <div className="pc-insp-actions">
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={selectedRows.length === 0}
                    onClick={openSaveNodeAsPool}
                  >
                    保存为池子
                  </Button>
                  {graph.nodes.length > 1 ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label="删除当前节点"
                      onClick={() => deleteNode(selectedNode.id)}
                    >
                      删除节点
                    </Button>
                  ) : null}
                </div>
              </div>
              <div className="pc-insp-meta">
                <span>
                  输入基数 <strong className="num">{selectedEval?.inputCount ?? 0}</strong> 只 →
                  筛出 <strong className="num">{selectedEval?.finalCount ?? 0}</strong> 只
                </span>
                {tradeDate ? <span className="hint">交易日 {tradeDate}</span> : null}
              </div>

              <div className="pc-meta-grid">
                <label className="pc-field">
                  <span className="hint">触发时机</span>
                  <select
                    className="select"
                    aria-label="节点触发时机"
                    value={selectedNode.trigger}
                    onChange={(e) => updateNode(selectedNode.id, { trigger: e.target.value })}
                  >
                    {TRIGGER_OPTIONS.map((opt) => (
                      <option key={opt} value={opt}>
                        {opt}
                      </option>
                    ))}
                    {!TRIGGER_OPTIONS.includes(
                      selectedNode.trigger as (typeof TRIGGER_OPTIONS)[number],
                    ) ? (
                      <option value={selectedNode.trigger}>{selectedNode.trigger}</option>
                    ) : null}
                  </select>
                </label>
                <label className="pc-field">
                  <span className="hint">触发动作</span>
                  <select
                    className="select"
                    aria-label="节点触发动作"
                    value={selectedNode.actions}
                    onChange={(e) => updateNode(selectedNode.id, { actions: e.target.value })}
                  >
                    {ACTION_OPTIONS.map((opt) => (
                      <option key={opt} value={opt}>
                        {opt}
                      </option>
                    ))}
                    {!ACTION_OPTIONS.includes(
                      selectedNode.actions as (typeof ACTION_OPTIONS)[number],
                    ) ? (
                      <option value={selectedNode.actions}>{selectedNode.actions}</option>
                    ) : null}
                  </select>
                </label>
              </div>
            </div>

            <div className="pc-insp-body">
              {/* Upstream Source or Parent Links */}
              <section className="pc-sec" aria-label="节点上游输入">
                <h4 className="sub">上游输入连接</h4>
                {selectedNode.parents.length === 0 ? (
                  <div className="pc-root-scope">
                    <select
                      className="select"
                      aria-label="初始节点范围"
                      value={selectedNode.rootUpstream.base}
                      onChange={(e) => {
                        const base = e.target.value as ScreenerUpstream["base"];
                        updateNode(selectedNode.id, {
                          rootUpstream: {
                            ...selectedNode.rootUpstream,
                            base,
                            preset: base === "preset" ? (presets[0] ?? null) : null,
                            pool: base === "pool" ? (userPools[0]?.name ?? null) : null,
                          },
                        });
                      }}
                    >
                      <option value="all_market">
                        全市场 A 股{allMarketCount > 0 ? ` (${allMarketCount})` : ""}
                      </option>
                      <option value="presets">今日候选池（全预设去重）</option>
                      {presets.length > 0 ? <option value="preset">指定系统预设</option> : null}
                      {userPools.length > 0 ? (
                        <option value="pool">已有自定义池 / 竞价池</option>
                      ) : null}
                      <option value="watchlist">自选股</option>
                    </select>
                    {selectedNode.rootUpstream.base === "preset" && presets.length > 0 ? (
                      <select
                        className="select"
                        aria-label="初始节点指定预设"
                        value={selectedNode.rootUpstream.preset ?? presets[0] ?? ""}
                        onChange={(e) =>
                          updateNode(selectedNode.id, {
                            rootUpstream: { ...selectedNode.rootUpstream, preset: e.target.value },
                          })
                        }
                      >
                        {presets.map((p) => (
                          <option key={p} value={p}>
                            {presetLabel(p)}
                          </option>
                        ))}
                      </select>
                    ) : null}
                    {selectedNode.rootUpstream.base === "pool" && userPools.length > 0 ? (
                      <select
                        className="select"
                        aria-label="初始节点指定池子"
                        value={selectedNode.rootUpstream.pool ?? userPools[0]?.name ?? ""}
                        onChange={(e) =>
                          updateNode(selectedNode.id, {
                            rootUpstream: { ...selectedNode.rootUpstream, pool: e.target.value },
                          })
                        }
                      >
                        {userPools.map((p) => (
                          <option key={p.name} value={p.name}>
                            {p.name} ({p.members.length})
                          </option>
                        ))}
                      </select>
                    ) : null}
                  </div>
                ) : (
                  <div className="pc-parent-list">
                    {selectedNode.parents.map((link, idx) => {
                      const pNode = graph.nodes.find((n) => n.id === link.nodeId);
                      return (
                        <div key={link.nodeId} className="pc-parent-row">
                          <span className="pc-parent-name">{pNode?.name ?? link.nodeId}</span>
                          <select
                            className="select wb-sm-select"
                            aria-label={`上游运算 ${idx + 1}`}
                            value={link.op}
                            onChange={(e) => {
                              const nextOp = e.target.value as CanvasEdgeOp;
                              updateNode(selectedNode.id, (prev) => ({
                                ...prev,
                                parents: prev.parents.map((item, j) =>
                                  j === idx ? { ...item, op: nextOp } : item,
                                ),
                              }));
                            }}
                          >
                            <option value="inherit">继承</option>
                            <option value="union">+ 并集</option>
                            <option value="intersect">∩ 交集</option>
                            <option value="exclude">− 排除</option>
                          </select>
                          <select
                            className="select wb-sm-select"
                            aria-label={`上游回溯 ${idx + 1}`}
                            value={link.lookback}
                            onChange={(e) => {
                              const lb = e.target.value;
                              updateNode(selectedNode.id, (prev) => ({
                                ...prev,
                                parents: prev.parents.map((item, j) =>
                                  j === idx ? { ...item, lookback: lb } : item,
                                ),
                              }));
                            }}
                          >
                            {LOOKBACK_OPTIONS.map((opt) => (
                              <option key={opt} value={opt}>
                                {opt}
                              </option>
                            ))}
                          </select>
                          <Button
                            size="sm"
                            variant="ghost"
                            aria-label={`断开上游 ${pNode?.name ?? link.nodeId}`}
                            onClick={() =>
                              updateNode(selectedNode.id, (prev) => ({
                                ...prev,
                                parents: prev.parents.filter((_, j) => j !== idx),
                              }))
                            }
                          >
                            ×
                          </Button>
                        </div>
                      );
                    })}
                  </div>
                )}

                {availableParentsForSelected.length > 0 ? (
                  <div className="pc-add-parent">
                    <select
                      className="select wb-sm-select"
                      aria-label="连接其他画布上游节点"
                      defaultValue=""
                      onChange={(e) => {
                        const pid = e.target.value;
                        if (!pid) return;
                        e.target.value = "";
                        updateNode(selectedNode.id, (prev) => ({
                          ...prev,
                          kind: prev.kind === "root" ? "daily" : prev.kind,
                          parents: [
                            ...prev.parents,
                            {
                              nodeId: pid,
                              op: prev.parents.length === 0 ? "inherit" : "union",
                              lookback: LOOKBACK_OPTIONS[0],
                            },
                          ],
                        }));
                      }}
                    >
                      <option value="">+ 接入画布其他节点作为上游…</option>
                      {availableParentsForSelected.map((n) => (
                        <option key={n.id} value={n.id}>
                          {n.name}
                        </option>
                      ))}
                    </select>
                  </div>
                ) : null}
              </section>

              {/* Natural Language Condition Generator (auto-syncs to selectable ★ 我的积木!) */}
              <section className="pc-sec" aria-label="节点自然语言条件生成">
                <h4 className="sub">一句话新增条件（自动同步到可选积木块）</h4>
                <NlWhiteBoxBar
                  key={`nl-${selectedNode.id}`}
                  defaultSyncLibrary={true}
                  onCustomBlockSaved={onCustomBlockSaved}
                  onAppendBlocks={(added) =>
                    updateNode(selectedNode.id, (prev) => ({
                      ...prev,
                      blocks: [...prev.blocks, ...added],
                    }))
                  }
                />
              </section>

              {/* Ordered Filter Block Stack + Funnel */}
              <section className="pc-sec" aria-label="节点筛选条件积木栈">
                <h4 className="sub">当前节点筛选积木栈</h4>
                <BlockListWorkbench
                  blocks={selectedNode.blocks}
                  onChange={(nextBlocks) => updateNode(selectedNode.id, { blocks: nextBlocks })}
                  fields={fields}
                  rules={rules}
                  customBlocks={customBlocks}
                  onCustomBlockSaved={onCustomBlockSaved}
                />
                {selectedSteps.length > 0 ? (
                  <FunnelStrip
                    steps={selectedSteps}
                    activeStep={previewStep ?? Math.max(0, selectedSteps.length - 1)}
                    previewStep={previewStep}
                    onSelectStep={setPreviewStep}
                  />
                ) : null}
              </section>

              {/* Surviving Stocks Preview Table */}
              <section className="pc-sec" aria-label="节点命中股票列表">
                <div className="pc-stocks-head">
                  <h4 className="sub">
                    {previewStep !== null && previewStep < selectedSteps.length - 1
                      ? `第 ${previewStep} 层预览（去重后 ${displayedRows.length} 只）`
                      : `节点命中股票（去重后 ${displayedRows.length} 只）`}
                  </h4>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => openCreateDownstream(selectedNode)}
                  >
                    以此结果建下游池 →
                  </Button>
                </div>
                {displayedRows.length === 0 ? (
                  <p className="cond-none">当前节点条件下暂无命中股票。</p>
                ) : (
                  <div className="pc-stock-table-wrap">
                    <table className="pc-stock-table">
                      <thead>
                        <tr>
                          <th>股票</th>
                          <th className="num">现价</th>
                          <th className="num">涨跌幅</th>
                          <th>来源</th>
                          <th />
                        </tr>
                      </thead>
                      <tbody>
                        {displayedRows.slice(0, 60).map((row) => {
                          const isWatched = watchedCodes.has(row.code);
                          return (
                            <tr key={row.code}>
                              <td>
                                <StockLink code={row.code} />
                                {row.name ? <span className="pc-sname">{row.name}</span> : null}
                              </td>
                              <td className="num">{formatPrice(row.close)}</td>
                              <td className="num">
                                <ChangeText value={row.pct_chg ?? null} />
                              </td>
                              <td className="hint">{presetLabel(row.preset)}</td>
                              <td className="num">
                                {isWatched ? (
                                  <span className="faint">✓ 自选</span>
                                ) : (
                                  <Button
                                    size="sm"
                                    variant="ghost"
                                    disabled={addWatch.isPending}
                                    onClick={() => onWatch(row)}
                                  >
                                    +自选
                                  </Button>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </section>
            </div>
          </aside>
        ) : null}
      </div>

      {/* Create Initial or Downstream Node Drawer (with direct inline condition editor!) */}
      <AppDrawer
        open={createMode !== null}
        title={createMode === "root" ? "新建初始节点" : "新建下游池子节点"}
        label={createMode === "root" ? "新建初始节点" : "新建下游池子节点"}
        onClose={() => setCreateMode(null)}
        footer={
          <>
            <Button variant="ghost" onClick={() => setCreateMode(null)}>
              取消
            </Button>
            <Button
              variant="primary"
              disabled={
                !draftName.trim() || (createMode === "downstream" && draftParents.length === 0)
              }
              onClick={confirmCreateNode}
            >
              {createMode === "root" ? "创建初始节点" : "创建下游节点并连线"}
            </Button>
          </>
        }
      >
        <div className="form-stack">
          <div className="field">
            <label htmlFor="canvas-new-node-name">节点名称</label>
            <input
              id="canvas-new-node-name"
              value={draftName}
              onChange={(e) => setDraftName(e.target.value)}
              placeholder="如：全A小盘初筛池 / Pool 2-3 放量突破"
            />
          </div>

          {createMode === "root" ? (
            <div className="field">
              <label htmlFor="canvas-root-base">初始股票范围</label>
              <select
                id="canvas-root-base"
                className="select"
                value={draftRootBase}
                onChange={(e) => {
                  const b = e.target.value as ScreenerUpstream["base"];
                  setDraftRootBase(b);
                  if (b === "preset") setDraftRootSub(presets[0] ?? "");
                  if (b === "pool") setDraftRootSub(userPools[0]?.name ?? "");
                }}
              >
                <option value="all_market">
                  全市场 A 股{allMarketCount > 0 ? ` (${allMarketCount})` : ""}
                </option>
                <option value="presets">今日候选池（全预设去重）</option>
                {presets.length > 0 ? <option value="preset">单个系统预设</option> : null}
                {userPools.length > 0 ? <option value="pool">已有池子 / 竞价池</option> : null}
                <option value="watchlist">自选股</option>
              </select>
              {draftRootBase === "preset" && presets.length > 0 ? (
                <select
                  className="select"
                  aria-label="选择系统预设"
                  value={draftRootSub}
                  onChange={(e) => setDraftRootSub(e.target.value)}
                >
                  {presets.map((p) => (
                    <option key={p} value={p}>
                      {presetLabel(p)}
                    </option>
                  ))}
                </select>
              ) : null}
              {draftRootBase === "pool" && userPools.length > 0 ? (
                <select
                  className="select"
                  aria-label="选择输入池"
                  value={draftRootSub}
                  onChange={(e) => setDraftRootSub(e.target.value)}
                >
                  {userPools.map((p) => (
                    <option key={p.name} value={p.name}>
                      {p.name} ({p.members.length})
                    </option>
                  ))}
                </select>
              ) : null}
            </div>
          ) : (
            <div className="field">
              <span className="field-label">上游父节点与回溯方式</span>
              <div className="pc-parent-list">
                {draftParents.map((p, idx) => (
                  <div key={p.nodeId} className="pc-parent-row">
                    <select
                      className="select"
                      aria-label={`新建节点上游 ${idx + 1}`}
                      value={p.nodeId}
                      onChange={(e) =>
                        setDraftParents(
                          draftParents.map((item, j) =>
                            j === idx ? { ...item, nodeId: e.target.value } : item,
                          ),
                        )
                      }
                    >
                      {graph.nodes.map((n) => (
                        <option key={n.id} value={n.id}>
                          {n.name}
                        </option>
                      ))}
                    </select>
                    <select
                      className="select wb-sm-select"
                      aria-label={`新建节点集合方式 ${idx + 1}`}
                      value={p.op}
                      onChange={(e) =>
                        setDraftParents(
                          draftParents.map((item, j) =>
                            j === idx ? { ...item, op: e.target.value as CanvasEdgeOp } : item,
                          ),
                        )
                      }
                    >
                      <option value="inherit">继承</option>
                      <option value="union">+ 并集</option>
                      <option value="intersect">∩ 交集</option>
                      <option value="exclude">− 排除</option>
                    </select>
                    <select
                      className="select wb-sm-select"
                      aria-label={`新建节点回溯窗口 ${idx + 1}`}
                      value={p.lookback}
                      onChange={(e) =>
                        setDraftParents(
                          draftParents.map((item, j) =>
                            j === idx ? { ...item, lookback: e.target.value } : item,
                          ),
                        )
                      }
                    >
                      {LOOKBACK_OPTIONS.map((opt) => (
                        <option key={opt} value={opt}>
                          {opt}
                        </option>
                      ))}
                    </select>
                  </div>
                ))}
              </div>
              {graph.nodes.length > draftParents.length ? (
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => {
                    const unused = graph.nodes.find(
                      (n) => !draftParents.some((dp) => dp.nodeId === n.id),
                    );
                    if (unused) {
                      setDraftParents([
                        ...draftParents,
                        { nodeId: unused.id, op: "union", lookback: LOOKBACK_OPTIONS[0] },
                      ]);
                    }
                  }}
                >
                  + 叠加另一路上游节点（并集 / 交集 / 排除）
                </Button>
              ) : null}
            </div>
          )}

          <div className="pc-meta-grid">
            <label className="pc-field">
              <span className="hint">触发时机</span>
              <select
                className="select"
                value={draftTrigger}
                onChange={(e) => {
                  const v = e.target.value;
                  setDraftTrigger(v);
                  setDraftKind(v.includes("盘中") || v.includes("竞价") ? "intraday" : "daily");
                }}
              >
                {TRIGGER_OPTIONS.map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            </label>
            <label className="pc-field">
              <span className="hint">触发动作</span>
              <select
                className="select"
                value={draftActions}
                onChange={(e) => setDraftActions(e.target.value)}
              >
                {ACTION_OPTIONS.map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <section className="pc-sec" aria-label="新建节点直接编辑筛选条件">
            <h4 className="sub">筛选条件（可直接配置积木、公式或一句话生成）</h4>
            <NlWhiteBoxBar
              defaultSyncLibrary={true}
              onCustomBlockSaved={onCustomBlockSaved}
              onAppendBlocks={(added) => setDraftBlocks((prev) => [...prev, ...added])}
            />
            <BlockListWorkbench
              blocks={draftBlocks}
              onChange={setDraftBlocks}
              fields={fields}
              rules={rules}
              customBlocks={customBlocks}
              onCustomBlockSaved={onCustomBlockSaved}
            />
          </section>
        </div>
      </AppDrawer>

      {/* Save Selected Node Output to Persistent Pool */}
      <AppDrawer
        open={savePoolOpen}
        title="保存当前节点结果为池子"
        label="保存当前节点结果为池子"
        onClose={() => {
          setSavePoolOpen(false);
          setSaveFail(null);
        }}
        footer={
          <>
            <Button
              variant="ghost"
              onClick={() => {
                setSavePoolOpen(false);
                setSaveFail(null);
              }}
            >
              取消
            </Button>
            <Button
              variant="primary"
              disabled={!savePoolName.trim() || savePool.isPending}
              onClick={confirmSaveNodePool}
            >
              {savePool.isPending ? "保存中…" : "确认保存"}
            </Button>
          </>
        }
      >
        <div className="form-stack">
          {saveFail ? (
            <FailureNotice
              copy={saveFail}
              busy={savePool.isPending}
              onRetry={confirmSaveNodePool}
            />
          ) : null}
          <div className="field">
            <label htmlFor="canvas-save-pool-name">池子名称</label>
            <input
              id="canvas-save-pool-name"
              value={savePoolName}
              onChange={(e) => setSavePoolName(e.target.value)}
            />
            <span className="hint">
              将把节点『{selectedNode?.name}』当前筛出的 {selectedRows.length}{" "}
              只股票保存至「池子」页
            </span>
          </div>
        </div>
      </AppDrawer>
    </section>
  );
}
