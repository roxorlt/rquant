import { screen } from "@testing-library/react";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

/** ASCII hyphen immediately before a digit, after dates and clock times are removed. */
function hyphenBeforeDigit(text: string): string[] {
  const stripped = text
    .replace(/\d{4}-\d{2}(?:-\d{2})?/g, "")
    .replace(/\d{2}:\d{2}(?::\d{2})?/g, "");
  return [...stripped.matchAll(/(^|[^\d])-(\d)/g)].map((hit) => hit[0]);
}

function json(data: unknown): Response {
  return new Response(JSON.stringify({ data, serving }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

const block = {
  rank_ic: { mean: -0.028, ir: -1.2, win: 0.4, n: 10 },
  ls_ann_net: -0.12,
  long_excess_ann_net: -0.04,
  turnover_top: 0.2,
  quintile_ann: [],
};

function bodyFor(path: string): unknown {
  if (path.endsWith("/meta")) {
    return { app: "rQuant", version: "t", role: "admin", generation: serving };
  }
  if (path.includes("/macro")) {
    return {
      available: true,
      holders: [],
      options: {},
      options_latest: [],
      futures: {},
      breadth_picks: [],
      north_regime_change: "2024-08-19",
      studies: [
        {
          study: "SMARTMONEY上穿MA20",
          horizon: 5,
          n: 12,
          mean: -0.0284,
          median: -0.01,
          win: 0.4,
          base_mean: -0.002,
          base_win: 0.48,
          split: "test",
        },
      ],
    };
  }
  if (path.includes("/data-center")) {
    return {
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
        {
          dataset_id: "daily_basic",
          table_name: "daily_basic",
          name: "每日指标",
          purpose: "估值",
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
    };
  }
  if (path.includes("/fundamentals")) {
    return {
      fetched_at: "2026-10-20T07:00:00Z",
      fields: { netprofit_yoy: "净利同比" },
      rows: [{ code: "600519.SH", end_date: "20250630", values: { netprofit_yoy: -2.84 } }],
    };
  }
  if (path.includes("/screen")) {
    return {
      trade_date: "2026-10-20",
      presets: ["breakout"],
      rows: [
        {
          trade_date: "2026-10-20",
          code: "600519.SH",
          name: "贵州茅台",
          preset: "breakout",
          close: 1500,
          pct_chg: -2.84,
          pe_ttm: -12.5,
          industry: "白酒",
        },
      ],
    };
  }
  if (path.includes("/factors/correlation")) return { names: [] };
  if (path.includes("/factors")) {
    return {
      factors: [
        {
          factor_id: "mom",
          name: "演示动量",
          expression: "cs_rank(ts_delta(close, 5))",
          start: "2026-01-01",
          end: "2026-10-20",
          mean_ic: -0.028,
          recent_ic: -0.01,
          ic_ir: -0.4,
        },
      ],
    };
  }
  if (path.includes("/report-factors")) {
    return {
      results: {
        generated_at: "2026-10-20T07:00:00Z",
        hold_days: 5,
        cost_per_side: 0.001,
        catalog: [],
        factors: {
          mom: {
            name: "动量",
            data: "daily",
            catalog: "c",
            sign: 1,
            split: "2024-01-01",
            train: block,
            test: block,
            size_neutral_rank_ic_test: -0.01,
            horizon_rank_ic: { "5": -0.02 },
            decay: false,
            validated: false,
            latest_date: "2026-10-20",
            series: {
              dates: ["2026-10-01", "2026-10-02"],
              rank_ic: [-0.02, -0.03],
              q_nav: [],
              ls_nav_net: [1, 0.98],
              long_excess_nav_net: [1, 0.99],
            },
          },
        },
      },
      summary: null,
    };
  }
  if (path.includes("/market/live") || path.includes("/panorama/live")) {
    return {
      time: "15:00",
      pulse: {
        up: 1,
        down: 2,
        flat: 0,
        limit_up: 0,
        limit_down: 1,
        broken: 0,
        up_ratio: -0.028,
        median_pct: -2.84,
        amount: -1.2e8,
      },
      curve: {
        t: ["09:30", "15:00"],
        up_ratio: [-0.02, -0.028],
        limit_up: [0, 0],
        broken: [0, 0],
      },
      boards: [],
    };
  }
  if (path.includes("/panorama")) {
    return {
      as_of: "2026-10-20T07:00:00Z",
      pulse: { up: 1, down: 2, flat: 0, limit_up: 0, limit_down: 1 },
      boards: [
        {
          system: "dc_industry",
          board_code: "BK02",
          board_name: "银行",
          amount: 1.5e10,
          main_net_amount: -1e8,
          pct_chg_median: -2.84,
          limit_up_count: 0,
          stock_count: 10,
          leading_stock: "平安银行",
        },
      ],
    };
  }
  if (path.includes("/stocks/")) {
    return {
      code: "600519.SH",
      name: "贵州茅台",
      industry: "白酒",
      market: "主板",
      list_date: "2001-08-27",
      pools: ["watch"],
      alerts: [],
      bars: [
        {
          date: "2026-10-20",
          open: 1500,
          high: 1510,
          low: 1480,
          close: 1490,
          vol: 1e6,
          amount: 1e9,
          pct_chg: -2.84,
        },
      ],
      flow: [],
      fundamentals: { netprofit_yoy: -2.84 },
      fundamentals_fields: { netprofit_yoy: "净利同比" },
      fundamentals_period: "20250630",
      quote: {},
      returns: { "5日": -0.0284 },
      valuation: { pe_ttm: -12.5 },
    };
  }
  if (path.includes("/conditions")) return { runs: [] };
  if (path.includes("/screener-schemes")) return { schemes: [] };
  if (path.includes("/pools")) return { trade_date: "2026-10-20", pools: [] };
  return {};
}

function stub(): void {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(input instanceof Request ? input.url : String(input), "http://localhost");
      return json(bodyFor(url.pathname));
    }),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const pages = [
  ["/macro", "聪明钱上穿MA20"],
  ["/data", "日线行情"],
  ["/screener", "−2.84%"],
  ["/factor", "−0.028"],
  ["/reportmining", "−0.028"],
  ["/panorama", "−2.84%"],
  ["/stock/600519.SH", "自选"],
] as const;

it.each(pages)("%s shows numbers with a minus sign and no raw hyphen", async (path, marker) => {
  stub();
  renderApp(path);
  expect((await screen.findAllByText(marker, { exact: false })).length).toBeGreaterThan(0);
  expect(hyphenBeforeDigit(document.body.textContent ?? "")).toEqual([]);
});

it("labels daily_basic as 每日指标 on the data page", async () => {
  stub();
  renderApp("/data");
  expect((await screen.findAllByText("每日指标")).length).toBeGreaterThan(0);
  expect(document.body.textContent ?? "").not.toContain("daily_basic");
  expect(document.body.textContent ?? "").not.toContain("daily_bar");
});
