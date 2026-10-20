/**
 * GitHub Pages has no API. This module is pulled in only by the static-preview
 * Vite build (virtual:static-preview). Production resolves that module to an
 * empty stub, so this file is not in the production bundle.
 */
export const STATIC_PREVIEW_WRITE_MOCK = "STATIC_PREVIEW_WRITE_MOCK";

const serving = {
  state: "ready",
  generation_id: "preview",
  generated_at: "2026-10-20T07:00:00Z",
  message: null,
};

function envelope(data: unknown): unknown {
  return { data, serving };
}

const alertId = "a".repeat(64);
const t = ["09:35", "10:00", "11:30", "14:00", "15:00"];

function sector(name: string, netYi: number, pct: number) {
  return {
    name,
    net: netYi * 1e8,
    amount: 2.4e9,
    n: 20,
    limit_up: 0,
    pct,
    series: t.map((_, i) => Number((netYi * ((i + 1) / t.length)).toFixed(2))),
    ratio: t.map(() => 0.01),
    pct_series: t.map(() => pct),
  };
}

const FIXTURES: Record<string, unknown> = {
  "/api/v1/meta": envelope({
    app: "rQuant",
    version: "preview",
    role: "admin",
    user: "preview",
    notice: null,
    generation: serving,
  }),
  "/api/v1/overview": envelope({
    trade_date: "2026-10-20",
    kpis: [{ key: "candidates", label: "候选股", value: "2", tone: null }],
    signals: [],
  }),
  "/api/v1/health": envelope({
    services: [],
    freshness: [],
    layers: [{ key: "signal", title: "盯盘", state: "ok", detail: ["运行中"] }],
  }),
  "/api/v1/alerts": envelope({
    items: [
      {
        alert_id: alertId,
        at: "2026-10-20T06:38:00Z",
        code: "600519.SH",
        name: "贵州茅台",
        level: "L1",
        trade_date: "2026-10-20",
        trigger_type: "跳空高开",
        trigger_price: 1500,
        level_price: 1480,
        pool: "核心观察",
        acked_at: null,
        acked_by: null,
      },
    ],
  }),
  "/api/v1/alert-rules": envelope({
    rules: [
      {
        rule_id: "core",
        title: "核心池提醒",
        enabled: true,
        pools: ["pool1"],
        levels: ["L1"],
        cooldown_minutes: 0,
      },
    ],
  }),
  "/api/v1/pools": envelope({
    trade_date: "2026-10-20",
    pools: [{ pool_id: "p1", name: "核心观察", size: 1, kind: "manual", members: [] }],
  }),
  "/api/v1/screener-schemes": envelope({ schemes: [] }),
  "/api/v1/screen": envelope({
    trade_date: "2026-10-20",
    presets: ["breakout"],
    rows: [
      {
        trade_date: "2026-10-20",
        code: "600519.SH",
        name: "贵州茅台",
        preset: "breakout",
        close: 1500,
        pct_chg: 1.2,
        industry: "白酒",
        pe_ttm: 20,
        pb: 8,
        total_mv: 18000,
        roe: 20,
        netprofit_yoy: 8,
        factors: { 动量: 80 },
      },
    ],
  }),
  "/api/v1/panorama": envelope({
    as_of: "2026-10-20T07:00:00Z",
    pulse: { up: 1, down: 1, flat: 0, limit_up: 0, limit_down: 0 },
    boards: [],
  }),
  "/api/v1/panorama/sector-flow": envelope({
    date: "2026-10-20",
    days: ["2026-10-20"],
    time: "15:00",
    t,
    warming: false,
    unchanged: false,
    version: "preview",
    leaders: [],
    sectors: [sector("半导体", 2.4, 1.2), sector("银行", -1.1, -0.4)],
    summary: { amount: 1, up: 1, down: 1, limit_up: 0, limit_down: 0 },
  }),
};

function failRequested(): boolean {
  if (typeof location === "undefined") return false;
  return /(?:\?|&)fail=1(?:&|#|$)/.test(`${location.search}${location.hash}`);
}

function fixtureFor(url: string): unknown | undefined {
  const path = (
    url.startsWith("http") ? new URL(url).pathname : (url.split("?")[0] ?? url)
  ).replace(/\/$/, "");
  const key = Object.keys(FIXTURES)
    .sort((a, b) => b.length - a.length)
    .find((item) => path.endsWith(item));
  return key ? FIXTURES[key] : undefined;
}

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export function installStaticPreview(): void {
  const current = globalThis.fetch;
  if (current.name === "staticPreviewFetch") return;
  const native = current.bind(globalThis);
  globalThis.fetch = async function staticPreviewFetch(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const method = (
      init?.method ?? (input instanceof Request ? input.method : "GET")
    ).toUpperCase();
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (method !== "GET" && method !== "HEAD") {
      if (failRequested()) {
        return jsonResponse({ detail: "暂时不能写", marker: STATIC_PREVIEW_WRITE_MOCK }, 503);
      }
      return jsonResponse(
        { command_id: "preview", status: "succeeded", marker: STATIC_PREVIEW_WRITE_MOCK },
        200,
      );
    }
    const hit = fixtureFor(url);
    if (hit !== undefined) return jsonResponse(hit, 200);
    return native(input, init);
  };
}
