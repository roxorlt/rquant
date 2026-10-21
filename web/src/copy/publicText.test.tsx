import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { QueryView } from "@/pages/shared";
import { renderApp } from "@/test/render";
import {
  ALERT_POOL_ORDER,
  ALERT_POOL_TIP,
  actorLabel,
  alertPoolLabel,
  alertPoolLabels,
  boardSystemLabel,
  datasetLabel,
  entryModeLabel,
  poolDisplayName,
  poolRefLabel,
  presetLabel,
  publicLine,
  studyLabel,
  TRIGGER_LABEL,
  triggerLabel,
  variantLabel,
} from "./publicText";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

const BANNED = [
  "gap_up",
  "volume_surge",
  "API 0.31",
  "rquant-daily",
  "落后 0 个工作日",
  "落后 1 个工作日",
  "Serving",
  "东财板块热力图",
  "dc_industry",
  "同花顺原条件，非买入信号",
  "T-1=",
  "可能是 Serving",
  "idle",
  "daily_bar",
  "stop_loss",
  "page-control",
  "snapshot-collector",
  "reference-slow",
  "report-factors",
  "SMARTMONEY",
  "daily_basic",
  "cn_gdp",
  "market.duckdb",
  "auction-gap",
  "volume-surge",
  "--strategy",
  "systemd",
  "systemctl",
  "fina_indicator",
  "当前代",
  "degraded",
];

function assertClean(text: string): void {
  for (const term of BANNED) {
    expect(text, term).not.toContain(term);
  }
}

describe("public copy", () => {
  it("translates the internal phrases", () => {
    expect(presetLabel("auction-gap")).toBe("竞价高开");
    expect(presetLabel("breakout")).toBe("突破");
    expect(presetLabel("volume-surge")).toBe("放量");
    expect(triggerLabel("gap_up")).toBe("跳空高开");
    expect(triggerLabel("volume_surge")).toBe("放量");
    expect(triggerLabel("stop_loss")).toBe("止损");
    expect(triggerLabel("not_a_type")).toBe("其他");
    for (const [key, label] of Object.entries(TRIGGER_LABEL)) {
      expect(label, key).not.toMatch(/[A-Za-z]/);
      expect(triggerLabel(key)).toBe(label);
    }
    expect(publicLine("当前代发布于 0.0 小时前")).toContain("当前版本");
    expect(publicLine("没有已发布的代")).toBe("没有已发布的版本");
    expect(publicLine("rquant-daily：idle")).toBe("日线采集：空闲");
    expect(publicLine("日线最新 2026-10-20（落后 0 个工作日）")).toBe("日线最新 2026-10-20");
    expect(publicLine("daily_bar 缺 1 天")).toBe("日线行情缺 1 天");
    expect(presetLabel("auction-pick")).toBe("竞价选股");
    expect(presetLabel("n-shape-pool1")).toBe("N字初筛");
    expect(presetLabel("n-shape-pool2")).toBe("N字精选");
    expect(alertPoolLabel("pool1")).toBe("N字初筛");
    expect(alertPoolLabel("pool2")).toBe("N字精选");
    expect(alertPoolLabel("manual")).toBe("自选");
    expect(alertPoolLabel(null)).toBe("—");
    expect(alertPoolLabel("核心观察")).toBe("—");
    expect(alertPoolLabels(["manual", "pool1"])).toBe("N字初筛、自选");
    expect(alertPoolLabels(["pool2", "pool1"])).toBe("N字初筛、N字精选");
    expect(alertPoolLabels([])).toBe("全部");
    expect(alertPoolLabels(undefined)).toBe("全部");
    expect(ALERT_POOL_ORDER.map((id) => alertPoolLabel(id))).toEqual([
      "N字初筛",
      "N字精选",
      "自选",
    ]);
    expect(ALERT_POOL_TIP).toBe(
      "N字初筛：每个交易日 17:00 后，按 N 字形态自动筛出的股票，每天整批更新。N字精选：从最近的 N字初筛里再筛出的股票，最多跟踪 6 个交易日，跌破止损位会提前移出。两者都自动更新，不用手动维护。",
    );
    expect(poolRefLabel("watch")).toBe("自选");
    expect(poolRefLabel("auction-pick")).toBe("竞价选股");
    expect(entryModeLabel("open")).toBe("开盘");
    expect(entryModeLabel("close")).toBe("收盘");
    expect(variantLabel("tight")).toBe("收紧");
    expect(variantLabel("base")).toBe("基准");
    expect(actorLabel("owner")).toBe("本机");
    expect(studyLabel("SMARTMONEY上穿MA20")).toBe("聪明钱上穿MA20");
    expect(datasetLabel("daily_bar")).toBe("日线行情");
    expect(datasetLabel("daily_basic")).toBe("每日指标");
    expect(publicLine("API 0.31 正常响应")).toBeNull();
    expect(boardSystemLabel("dc_industry")).toBe("行业");
    expect(poolDisplayName("观察名单（同花顺原条件，非买入信号）")).toBe("观察名单");
    expect(poolDisplayName("竞价（T-1=2026-10-17）")).toBe("竞价");
  });

  it("does not show the internal phrases on the overview, panorama, or pools", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(input instanceof Request ? input.url : String(input));
        const key = url.pathname.split("/api/v1/")[1]?.split("/")[0] ?? "";
        const data =
          key === "overview"
            ? {
                trade_date: "2026-10-20",
                kpis: [{ key: "candidates", label: "候选股", value: "1", tone: null }],
                signals: [],
              }
            : key === "health"
              ? {
                  services: [],
                  freshness: [],
                  layers: [
                    {
                      key: "ingest",
                      title: "数据采集",
                      state: "ok",
                      detail: [
                        "rquant-daily：idle",
                        "daily_bar 缺 1 天",
                        "日线最新 2026-10-20（落后 0 个工作日）",
                      ],
                    },
                    {
                      key: "serving",
                      title: "Serving",
                      state: "ok",
                      detail: ["API 0.31 正常响应"],
                    },
                  ],
                }
              : key === "alerts"
                ? {
                    items: [
                      {
                        alert_id: "a".repeat(64),
                        trade_date: "2026-10-20",
                        at: "2026-10-20T06:00:00Z",
                        code: "600519.SH",
                        name: "贵州茅台",
                        level: "L1",
                        trigger_type: "gap_up",
                        trigger_price: 10,
                        level_price: 10,
                        pool: null,
                        acked_at: null,
                        acked_by: null,
                      },
                      {
                        alert_id: "b".repeat(64),
                        trade_date: "2026-10-20",
                        at: "2026-10-20T05:00:00Z",
                        code: "000001.SZ",
                        name: "平安银行",
                        level: "L1",
                        trigger_type: "volume_surge",
                        trigger_price: 10,
                        level_price: 10,
                        pool: null,
                        acked_at: null,
                        acked_by: null,
                      },
                    ],
                  }
                : key === "panorama"
                  ? {
                      as_of: "2026-10-20T07:00:00Z",
                      pulse: { up: 1, down: 1, flat: 0, limit_up: 0, limit_down: 0 },
                      boards: [
                        {
                          system: "dc_industry",
                          board_code: "BK01",
                          board_name: "白酒",
                          amount: 1,
                          main_net_amount: 1,
                          pct_chg_median: 1,
                          pct_chg_mean: 1,
                          limit_up_count: 0,
                          limit_down_count: 0,
                          member_count: 1,
                          up_ratio: 0.5,
                          leader_name: "贵州茅台",
                        },
                      ],
                    }
                  : key === "pools"
                    ? {
                        trade_date: "2026-10-20",
                        pools: [
                          {
                            name: "观察名单（同花顺原条件，非买入信号）",
                            description: "条件（T-1=2026-10-17）",
                            pool_refs: [],
                            updated_at: null,
                            members: [],
                          },
                        ],
                      }
                    : key === "meta"
                      ? { app: "rQuant", version: "0.31.0", role: "admin", generation: serving }
                      : {};
        return new Response(JSON.stringify({ data, serving }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }),
    );
    const overview = renderApp("/overview");
    await screen.findByText("跳空高开");
    expect(screen.getByText("放量")).toBeInTheDocument();
    expect(screen.getByText("数据发布")).toBeInTheDocument();
    expect(document.body.textContent ?? "").toContain("日线采集：空闲");
    expect(document.body.textContent ?? "").toContain("日线行情缺 1 天");
    assertClean(document.body.textContent ?? "");
    overview.unmount();

    const panorama = renderApp("/panorama");
    await screen.findByText(/板块热力图/);
    expect(screen.getAllByText("行业").length).toBeGreaterThan(0);
    assertClean(document.body.textContent ?? "");
    panorama.unmount();

    const pools = renderApp("/pools");
    await screen.findByText("观察名单");
    assertClean(document.body.textContent ?? "");
    pools.unmount();
  });

  it("keeps the internal phrases out of page source", () => {
    const root = join(process.cwd(), "src/pages");
    const files: string[] = [];
    const walk = (dir: string) => {
      for (const name of readdirSync(dir)) {
        const path = join(dir, name);
        if (statSync(path).isDirectory()) walk(path);
        else if (/\.tsx?$/.test(name) && !name.includes(".test.")) files.push(path);
      }
    };
    walk(root);
    const banned = BANNED.filter((term) => term !== "idle" && term !== "Serving");
    const hits: string[] = [];
    for (const file of files) {
      const text = readFileSync(file, "utf8");
      for (const term of banned) {
        if (text.includes(term)) hits.push(`${file} contains ${term}`);
      }
    }
    expect(hits).toEqual([]);
  });

  it("explains a failed read without naming the publisher", () => {
    render(
      <QueryView
        query={{
          data: undefined,
          serving: undefined,
          isLoading: false,
          isFetching: false,
          error: new Error("暂时读不到数据"),
          refetch: () => undefined,
        }}
      >
        {() => null}
      </QueryView>,
    );
    assertClean(document.body.textContent ?? "");
    expect(screen.getByText(/请稍后再试/)).toBeInTheDocument();
  });
});
