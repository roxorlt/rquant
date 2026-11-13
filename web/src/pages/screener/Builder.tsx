import type {
  CustomBlockItem,
  FieldCatalogGroup,
  RuleCatalogItem,
  ScreenerBlock,
  ScreenerFunnelStep,
  ScreenerUpstream,
  ScreenRow,
} from "@/api/endpoints";

/** A built-in numeric column, or "f:<factor name>" for a factor's cross-section percentile. */
export type Field = string;

export const FIELD_GROUPS: [string, Record<string, string>][] = [
  [
    "行情",
    {
      pct_chg: "涨跌幅 %",
      close: "收盘价",
      amount: "成交额(亿)",
      amplitude: "振幅 %",
      vol_ratio: "量比",
      ret5: "5日涨幅 %",
      ret20: "20日涨幅 %",
      turnover_rate: "换手率 %",
      turnover_rate_f: "换手率(自由流通) %",
    },
  ],
  ["估值", { pe_ttm: "PE(TTM)", pb: "PB", total_mv: "总市值(亿)", circ_mv: "流通市值(亿)" }],
  [
    "财务",
    {
      roe: "ROE %",
      grossprofit_margin: "毛利率 %",
      netprofit_margin: "净利率 %",
      netprofit_yoy: "净利同比 %",
      or_yoy: "营收同比 %",
      debt_to_assets: "资产负债率 %",
    },
  ],
  ["资金", { main_net: "主力净流入(亿)", main_net_5d: "5日主力净流入(亿)" }],
];

export const BASE_FIELDS: Record<string, string> = Object.assign(
  {},
  ...FIELD_GROUPS.map(([, f]) => f),
);

/** Kept for callers that only need labels of the built-ins. */
export const FIELDS = BASE_FIELDS;

export const DEFAULT_UPSTREAM: ScreenerUpstream = {
  base: "presets",
  preset: null,
  pool: null,
  include_pools: [],
  intersect_pools: [],
  exclude_pools: [],
};

/** Built-in fallback rule catalog so the picker renders immediately before /catalog resolves. */
export const FALLBACK_RULES: RuleCatalogItem[] = [
  {
    name: "not_st",
    title: "排除 ST / *ST",
    description: "剔除 ST、*ST 风险警示股",
    category: "filter",
    category_label: "基础过滤",
    examples: ["排除ST"],
    params: [],
  },
  {
    name: "not_bj",
    title: "排除北交所",
    description: "剔除北交所标的",
    category: "filter",
    category_label: "基础过滤",
    examples: ["排除北交所"],
    params: [],
  },
  {
    name: "circ_mv_lt",
    title: "流通市值上限",
    description: "流通市值小于指定亿元阈值",
    category: "filter",
    category_label: "基础过滤",
    examples: ["流通市值小于80亿"],
    params: [
      {
        name: "threshold_yi",
        label: "市值上限(亿)",
        kind: "float",
        required: true,
        default: 80,
        min: 1,
      },
      { name: "offset", label: "交易日偏移", kind: "int", required: false, default: 0, min: 0 },
    ],
  },
  {
    name: "board_in",
    title: "板块白名单",
    description: "仅保留指定板块（主板/创业板/科创板/北交所）",
    category: "filter",
    category_label: "基础过滤",
    examples: ["只看主板和创业板"],
    params: [
      {
        name: "boards",
        label: "板块",
        kind: "boards",
        required: true,
        default: ["main", "gem"],
        options: [
          { value: "main", label: "主板" },
          { value: "gem", label: "创业板" },
          { value: "star", label: "科创板" },
          { value: "bj", label: "北交所" },
        ],
      },
    ],
  },
  {
    name: "first_limit_up",
    title: "某日首板",
    description: "指定偏移日首次涨停",
    category: "state",
    category_label: "涨跌停与连板",
    examples: ["昨日首板"],
    params: [
      { name: "offset", label: "交易日偏移", kind: "int", required: false, default: 0, min: 0 },
    ],
  },
  {
    name: "limit_up",
    title: "某日涨停",
    description: "指定偏移日收盘涨停",
    category: "state",
    category_label: "涨跌停与连板",
    examples: ["今日涨停"],
    params: [
      { name: "offset", label: "交易日偏移", kind: "int", required: false, default: 0, min: 0 },
    ],
  },
  {
    name: "above_ma",
    title: "收盘站上均线",
    description: "收盘价高于 N 日均线",
    category: "indicator",
    category_label: "技术指标",
    examples: ["站上20日线"],
    params: [
      { name: "period", label: "周期", kind: "int", required: true, default: 20, min: 2, max: 250 },
      { name: "offset", label: "交易日偏移", kind: "int", required: false, default: 0, min: 0 },
    ],
  },
  {
    name: "volume_ratio_gte",
    title: "放量倍数 (量比)",
    description: "成交量不低于前 N 日均量的指定倍数",
    category: "indicator",
    category_label: "技术指标",
    examples: ["放量2倍"],
    params: [
      { name: "n", label: "阈值", kind: "float", required: true, default: 2.0, min: 0.1 },
      { name: "window", label: "回看天数", kind: "int", required: false, default: 5, min: 2 },
      { name: "offset", label: "交易日偏移", kind: "int", required: false, default: 0, min: 0 },
    ],
  },
  {
    name: "no_limit_down_in_window",
    title: "近N日无跌停",
    description: "最近 N 个交易日内无跌停记录",
    category: "aggregate",
    category_label: "窗口统计",
    examples: ["近30日无跌停"],
    params: [
      { name: "window", label: "回看天数", kind: "int", required: false, default: 30, min: 1 },
    ],
  },
];

let blockSeq = 1;
export function newBlockId(): string {
  blockSeq += 1;
  return `blk-${Date.now().toString(36)}-${blockSeq}`;
}

/** Built-in fields plus catalog factor definitions plus any factor seen in rows. */
export function fieldsFor(
  rows: readonly ScreenRow[],
  catalogGroups?: readonly FieldCatalogGroup[],
): Record<string, string> {
  const out = { ...BASE_FIELDS };
  if (catalogGroups) {
    for (const g of catalogGroups) {
      for (const item of g.items ?? []) {
        if (!item?.field) continue;
        if (item.field.startsWith("f:")) {
          const ic =
            item.mean_ic != null
              ? ` · IC ${item.mean_ic >= 0 ? "+" : ""}${item.mean_ic.toFixed(3)}`
              : "";
          out[item.field] = `因子·${item.label.replace(/ \(分位\)$/, "")}${ic}`;
        } else {
          out[item.field] = item.label;
        }
      }
    }
  }
  for (const r of rows) {
    for (const name of Object.keys(r.factors ?? {})) {
      if (!out[`f:${name}`]) out[`f:${name}`] = `因子·${name} 分位`;
    }
  }
  return out;
}

/** Groups for the picker: built-ins, then chip factors, then the other factor percentiles. */
export function groupsFor(fields: Record<string, string>): [string, [string, string][]][] {
  const fac = Object.entries(fields).filter(([k]) => k.startsWith("f:"));
  return [
    ...FIELD_GROUPS.map(([g, f]) => [g, Object.entries(f)] as [string, [string, string][]]),
    ["筹码(分位)", fac.filter(([k]) => k.includes("筹码"))],
    ["已验证因子(分位)", fac.filter(([k]) => !k.includes("筹码"))],
  ];
}

export interface Filter {
  field: Field;
  op: ">=" | "<=";
  value: number;
}
export interface Rank {
  field: Field;
  desc: boolean;
  weight: number;
}

export function filtersToBlocks(filters: readonly Filter[]): ScreenerBlock[] {
  return filters.map((f, idx) => ({
    id: `legacy-${idx}-${f.field}`,
    type: "field",
    enabled: true,
    field: f.field,
    op: f.op,
    value: f.value,
  }));
}

export function blocksToLegacyFilters(blocks: readonly ScreenerBlock[]): Filter[] {
  const out: Filter[] = [];
  for (const b of blocks) {
    if (b.enabled !== false && b.type === "field" && (b.op === ">=" || b.op === "<=")) {
      out.push({
        field: b.field ?? "pct_chg",
        op: b.op,
        value: Number(b.value ?? 0),
      });
    }
  }
  return out;
}

export function defaultBlockForRule(rule: RuleCatalogItem): ScreenerBlock {
  const args: Record<string, unknown> = {};
  for (const p of rule.params ?? []) {
    if (p.default !== undefined && p.default !== null) {
      args[p.name] = Array.isArray(p.default) ? [...p.default] : p.default;
    } else if (p.kind === "boards") {
      args[p.name] = ["main", "gem"];
    } else if (p.kind === "ma_line") {
      args[p.name] = p.name === "fast" ? "MA5" : "MA20";
    } else if (p.kind === "operand") {
      args[p.name] = "CLOSE[0]";
    } else if (p.kind === "int" || p.kind === "float") {
      args[p.name] = p.min ?? 0;
    }
  }
  return {
    id: newBlockId(),
    type: "rule",
    enabled: true,
    rule_name: rule.name,
    args,
    op: ">=",
    value: 0,
  };
}

export function formatClientBlockLabel(
  b: ScreenerBlock,
  fields: Record<string, string>,
  customMap: ReadonlyMap<string, CustomBlockItem>,
  ruleMap: ReadonlyMap<string, RuleCatalogItem>,
): string {
  if (b.label?.trim()) return b.label.trim();
  if (b.type === "custom") {
    const cb = customMap.get(b.custom_id ?? "");
    return cb ? `★ ${cb.name}` : `★ ${b.custom_id ?? "自定义"}`;
  }
  if (b.type === "tdx") {
    const raw = (b.tdx ?? "").trim();
    return raw ? `TDX: ${raw.length > 28 ? `${raw.slice(0, 25)}…` : raw}` : "TDX 公式";
  }
  if (b.type === "field") {
    const fld = b.field ?? "pct_chg";
    const lbl = (fields[fld] ?? fld).split(" · IC")[0];
    const op = b.op ?? ">=";
    const valNum = b.value ?? 0;
    if (op === "between") return `${lbl} ∈ [${valNum}, ${b.value2 ?? valNum}]`;
    if (op === "top_pct") return `${lbl} 前 ${valNum}%`;
    if (op === "bottom_pct") return `${lbl} 后 ${valNum}%`;
    return `${lbl} ${op} ${valNum}`;
  }
  const r = ruleMap.get(b.rule_name ?? "");
  return r?.title ?? b.rule_name ?? "条件";
}

export const val = (row: ScreenRow, f: Field): number | null => {
  if (f.startsWith("f:")) return row.factors?.[f.slice(2)] ?? null;
  const v = (row as unknown as Record<string, unknown>)[f];
  return typeof v === "number" ? v : null;
};

export function applyFilters(rows: ScreenRow[], filters: Filter[]) {
  const steps: number[] = [rows.length];
  let out = rows;
  for (const f of filters) {
    out = out.filter((r) => {
      const v = val(r, f.field);
      return v !== null && (f.op === ">=" ? v >= f.value : v <= f.value);
    });
    steps.push(out.length);
  }
  return { rows: out, steps };
}

/** Client-side fallback funnel when server /screener/run is unavailable (e.g. static export). */
export function applyClientBlocks(
  rows: ScreenRow[],
  blocks: readonly ScreenerBlock[],
  fields: Record<string, string>,
  customMap: ReadonlyMap<string, CustomBlockItem>,
  ruleMap: ReadonlyMap<string, RuleCatalogItem>,
  previewStep: number | null,
): { rows: ScreenRow[]; steps: ScreenerFunnelStep[]; activeStep: number } {
  const steps: ScreenerFunnelStep[] = [
    {
      step: 0,
      block_id: null,
      label: "候选池",
      remaining: rows.length,
      dropped: 0,
      dropped_pct: 0,
      error: null,
    },
  ];
  const slices: ScreenRow[][] = [rows];
  let current = rows;
  let stepIdx = 0;

  for (const b of blocks) {
    if (b.enabled === false) continue;
    stepIdx += 1;
    const prevN = current.length;
    if (b.type === "field") {
      const fld = b.field ?? "pct_chg";
      const op = b.op ?? ">=";
      const v0 = Number(b.value ?? 0);
      if (op === "top_pct" || op === "bottom_pct") {
        const pct = Math.max(0, Math.min(100, v0));
        const scored = current
          .map((r) => [r, val(r, fld)] as const)
          .filter((item): item is [ScreenRow, number] => item[1] !== null)
          .sort((x, y) => (op === "top_pct" ? y[1] - x[1] : x[1] - y[1]));
        const keepN =
          pct <= 0 || scored.length === 0 ? 0 : Math.max(1, Math.ceil((scored.length * pct) / 100));
        const keepCodes = new Set(scored.slice(0, keepN).map(([r]) => r.code));
        current = current.filter((r) => keepCodes.has(r.code));
      } else if (op === "between") {
        const hi = Number(b.value2 ?? v0);
        const loV = Math.min(v0, hi);
        const hiV = Math.max(v0, hi);
        current = current.filter((r) => {
          const x = val(r, fld);
          return x !== null && x >= loV && x <= hiV;
        });
      } else {
        current = current.filter((r) => {
          const x = val(r, fld);
          return x !== null && (op === ">=" ? x >= v0 : x <= v0);
        });
      }
    } else if (b.type === "rule" && b.rule_name === "not_st") {
      current = current.filter((r) => !(r.name ?? "").toUpperCase().includes("ST"));
    } else if (b.type === "rule" && b.rule_name === "not_bj") {
      current = current.filter((r) => !r.code.toUpperCase().endsWith(".BJ"));
    }
    const dropped = Math.max(0, prevN - current.length);
    const droppedPct = prevN > 0 ? Math.round((dropped / prevN) * 1000) / 10 : 0;
    steps.push({
      step: stepIdx,
      block_id: b.id ?? `step-${stepIdx}`,
      label: formatClientBlockLabel(b, fields, customMap, ruleMap),
      remaining: current.length,
      dropped,
      dropped_pct: droppedPct,
      error: null,
    });
    slices.push(current);
  }

  const activeStep =
    previewStep !== null && previewStep >= 0 && previewStep < slices.length
      ? previewStep
      : slices.length - 1;
  return { rows: slices[activeStep] ?? current, steps, activeStep };
}

/** Weighted percentile-rank score in [0, 100]. */
export function score(rows: ScreenRow[], ranks: Rank[]): Map<ScreenRow, number> {
  const total = ranks.reduce((s, r) => s + r.weight, 0) || 1;
  const out = new Map<ScreenRow, number>(rows.map((r) => [r, 0]));
  for (const rk of ranks) {
    const sorted = [...rows].sort((a, b) => {
      const x = val(a, rk.field) ?? Number.NEGATIVE_INFINITY;
      const y = val(b, rk.field) ?? Number.NEGATIVE_INFINITY;
      return rk.desc ? x - y : y - x;
    });
    sorted.forEach((r, i) => {
      const pct = rows.length > 1 ? i / (rows.length - 1) : 1;
      out.set(r, (out.get(r) ?? 0) + (pct * 100 * rk.weight) / total);
    });
  }
  return out;
}
