import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g1", generated_at: "2026-10-09T07:00:00Z" };

const BODIES: Record<string, unknown> = {
  meta: { app: "rQuant", version: "test", generation: serving },
  overview: {
    trade_date: "2026-10-09",
    kpis: [{ key: "candidates", label: "候选股", value: "12", tone: null }],
    signals: [],
  },
  health: {
    services: [
      {
        service_id: "rquant-monitor",
        plane: "live",
        status: "running",
        stale: false,
        heartbeat_at: null,
        backlog_count: 0,
        consecutive_failures: 0,
        last_error: null,
      },
    ],
    freshness: [],
  },
  panorama: {
    as_of: null,
    pulse: { up: 1, down: 2, flat: 0, limit_up: 0, limit_down: 0 },
    boards: [],
  },
  screen: {
    trade_date: "2026-10-09",
    presets: ["breakout"],
    rows: [
      {
        trade_date: "2026-10-09",
        code: "600519.SH",
        name: "贵州茅台",
        preset: "breakout",
        close: 1500,
        pct_chg: 1.2,
      },
    ],
  },
  pools: { trade_date: null, pools: [] },
  backtests: { runs: [] },
  "portfolio-backtests": { runs: [] },
  alerts: { items: [] },
  paper: { accounts: [] },
  factors: { factors: [] },
  strategies: { strategies: [] },
  "data-center": {
    datasets: [
      {
        dataset_id: "daily_bar",
        table_name: "daily_bar",
        name: "股票日线",
        purpose: "看日线",
        category: "行情",
        sources: ["Tushare Pro"],
        update_note: "",
        visibility_note: "",
        primary_key: [],
        schema_available: true,
        sample_available: false,
        fields: [],
      },
    ],
    audit: null,
  },
};

function stubApi() {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const key = url.pathname.split("/api/v1/")[1]?.split("/")[0] ?? "";
    const data = BODIES[key];
    const body = { data, serving };
    return new Response(JSON.stringify(body), {
      status: data === undefined ? 404 : 200,
      headers: { "Content-Type": "application/json" },
    });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => vi.unstubAllGlobals());

describe("MVP pages render from the API", () => {
  it.each([
    ["/overview", "候选股"],
    ["/health", "rquant-monitor"],
    ["/panorama", "跌停"],
    ["/screener", "贵州茅台"],
    ["/pools", "还没有池子"],
    ["/backtest", "还没有已发布的回测"],
    ["/monitor", "没有告警"],
    ["/paper", "没有模拟账户"],
    ["/data", "股票日线"],
    ["/factor", "还没有因子检验结果"],
  ])("%s", async (path, text) => {
    stubApi();
    renderApp(path);
    expect((await screen.findAllByText(text)).length).toBeGreaterThan(0);
  });
});
