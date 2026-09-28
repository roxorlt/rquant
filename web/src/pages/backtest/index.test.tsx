import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

const serving = metaEnvelope().serving;
const runs: Schemas["BacktestRun"][] = [
  {
    run_id: "run-later",
    computed_at: "2026-09-24T07:26:00Z",
    start_date: "2026-07-01",
    end_date: "2026-07-31",
    max_hold_days: 3,
    candidates: 12,
    trades: 0,
    configurations: 1,
  },
  {
    run_id: "run-earlier",
    computed_at: "2026-09-24T07:21:00Z",
    start_date: "2026-07-01",
    end_date: "2026-07-31",
    max_hold_days: 3,
    candidates: 100,
    trades: 23,
    configurations: 2,
  },
];
const groups: Schemas["BacktestGroup"][] = [
  {
    entry_mode: "break_retest",
    entry_mode_label: "突破回踩确认",
    profile_variant: "vp_90",
    profile_variant_label: "价量过滤与风控",
    candidates: 100,
    trades: 1,
    trigger_rate_pct: 1,
    mean_ret_pct: -2,
    median_ret_pct: -2,
    win_rate_pct: 0,
    best_ret_pct: -2,
    worst_ret_pct: -2,
    gap_stop_rate_pct: 0,
  },
  {
    entry_mode: "first_break",
    entry_mode_label: "第一次突破",
    profile_variant: "baseline",
    profile_variant_label: "基础风控",
    candidates: 100,
    trades: 22,
    trigger_rate_pct: 22,
    mean_ret_pct: 2,
    median_ret_pct: 2,
    win_rate_pct: 100,
    best_ret_pct: 2,
    worst_ret_pct: 2,
    gap_stop_rate_pct: 0,
  },
];
const trade: Schemas["BacktestTrade"] = {
  trade_id: "trade-3",
  entry_mode: "break_retest",
  entry_mode_label: "突破回踩确认",
  profile_variant: "vp_90",
  profile_variant_label: "价量过滤与风控",
  signal_date: "2026-07-31",
  ts_code: "600001.SH",
  name: "样本01",
  entry_time: "2026-07-31T01:31:00Z",
  entry_price: 10.1,
  exit_time: "2026-07-31T06:31:00Z",
  exit_price: 10.3,
  exit_reason: "stop_loss",
  exit_reason_label: "止损",
  ret_pct: -2,
};

function stockDrawer() {
  server.use(
    http.get("*/api/v1/stocks/600001.SH/summary", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", price: 11, as_of: null, pools: [] },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
      HttpResponse.json({ data: { ts_code: "600001.SH", name: "样本01", bars: [] }, serving }),
    ),
  );
}

function results() {
  const detailRequests: string[] = [];
  server.use(
    http.get("*/api/v1/backtests", ({ request }) => {
      const url = new URL(request.url);
      const offset = Number(url.searchParams.get("offset") ?? "0");
      return HttpResponse.json({
        data: {
          available: true,
          runs: runs.slice(offset, offset + 20),
          total: 2,
          next_offset: null,
        },
        serving,
      });
    }),
    http.get("*/api/v1/backtests/:runId", ({ params, request }) => {
      const url = new URL(request.url);
      detailRequests.push(url.search);
      const run = runs.find((item) => item.run_id === params.runId);
      const selected = url.searchParams.get("entry_mode");
      const offset = Number(url.searchParams.get("offset") ?? "0");
      const total = run?.trades === 0 ? 0 : selected ? (selected === "first_break" ? 22 : 1) : 23;
      return HttpResponse.json({
        data: {
          summary_available: true,
          trades_available: true,
          run,
          groups: run?.trades === 0 ? [groups[1]] : groups,
          trades: total && offset === 0 ? [trade] : [],
          total_trades: total,
          next_offset: total > 1 && offset === 0 ? 20 : null,
        },
        serving,
      });
    }),
  );
  return detailRequests;
}

describe("回测结果", () => {
  it("切换运行、查看配置与交易分页，交易详情可继续打开个股", async () => {
    const requests = results();
    stockDrawer();
    const user = userEvent.setup();
    const { container } = renderApp("/backtest");

    expect(await screen.findByRole("heading", { level: 1, name: "回测" })).toBeInTheDocument();
    expect(await screen.findByText("这次回放没有触发交易")).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "回放记录" }), "run-earlier");
    const stats = await screen.findByRole("table", { name: "配置统计" });
    expect(within(stats).getByText("突破回踩确认")).toBeInTheDocument();
    expect(within(stats).getByText("第一次突破")).toBeInTheDocument();
    expect(screen.getByText("这次回放未产出逐日净值")).toBeInTheDocument();
    expect(screen.getByText("回撤、持仓与基准也尚未发布。")).toBeInTheDocument();
    const trades = screen.getByRole("table", { name: "交易明细" });
    await user.click(within(trades).getByText("样本01"));
    const transaction = await screen.findByRole("dialog");
    expect(within(transaction).getByText("买入时间")).toBeVisible();
    expect(within(transaction).getByText("10.10")).toBeVisible();
    expect(within(transaction).getByText("卖出时间")).toBeVisible();
    expect(within(transaction).getByText("10.30")).toBeVisible();
    expect(within(transaction).getByText("止损")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "关闭" }));
    within(trades).getByText("样本01").closest("tr")?.focus();
    await user.keyboard("{Enter}");
    expect(await screen.findByRole("button", { name: "查看个股" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "查看个股" }));
    expect(await screen.findByText("日 K")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "关闭" }));
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await waitFor(() => expect(requests.at(-1)).toContain("offset=20"));
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(container.querySelector("main")?.textContent).not.toContain("run-earlier");
    expect(container.querySelector("main")?.textContent).not.toContain("vp_90");
  });

  it("未发布的回放结果如实显示空态", async () => {
    server.use(
      http.get("*/api/v1/backtests", () =>
        HttpResponse.json({
          data: { available: false, runs: [], total: 0, next_offset: null },
          serving,
        }),
      ),
    );
    renderApp("/backtest");
    expect(await screen.findByText("还没有可查看的回放结果")).toBeInTheDocument();
  });

  it("运行列表切代时用原代请求下一批，冲突后清掉配置与交易页码并重取首批", async () => {
    const manyRuns = Array.from({ length: 21 }, (_, index) => ({
      ...runs[1],
      run_id: `run-${index}`,
    }));
    const currentServing = metaEnvelope({ generationId: "b".repeat(64) }).serving;
    const listRequests: { offset: number; generation: string | null }[] = [];
    const detailRequests: string[] = [];
    let switched = false;
    server.use(
      http.get("*/api/v1/backtests", ({ request }) => {
        const url = new URL(request.url);
        const offset = Number(url.searchParams.get("offset") ?? "0");
        listRequests.push({ offset, generation: url.searchParams.get("generation_id") });
        if (offset === 20) {
          switched = true;
          return HttpResponse.json({ detail: "数据已更新" }, { status: 409 });
        }
        return HttpResponse.json({
          data: {
            available: true,
            runs: switched
              ? manyRuns
                  .slice(0, 20)
                  .map((run, index) =>
                    index === 0 ? { ...run, computed_at: "2026-09-25T07:26:00Z" } : run,
                  )
              : manyRuns.slice(0, 20),
            total: switched ? 20 : 21,
            next_offset: switched ? null : 20,
          },
          serving: switched ? currentServing : serving,
        });
      }),
      http.get("*/api/v1/backtests/:runId", ({ params, request }) => {
        const url = new URL(request.url);
        detailRequests.push(url.search);
        const offset = Number(url.searchParams.get("offset") ?? "0");
        return HttpResponse.json({
          data: {
            summary_available: true,
            trades_available: true,
            run: manyRuns.find((run) => run.run_id === params.runId),
            groups,
            trades: [trade],
            total_trades: 23,
            next_offset: offset === 0 ? 20 : null,
          },
          serving: switched ? currentServing : serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    const stats = await screen.findByRole("table", { name: "配置统计" });
    await user.click(within(stats).getByText("第一次突破"));
    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByText("第 2 页", { exact: false })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "下一批" }));
    await waitFor(() => expect(listRequests.at(-1)?.offset).toBe(0));
    expect(listRequests.find((item) => item.offset === 20)?.generation).toBe(serving.generation_id);
    await waitFor(() => {
      const latest = new URLSearchParams(detailRequests.at(-1) ?? "");
      expect(latest.get("generation_id")).toBe(currentServing.generation_id);
      expect(latest.get("offset")).toBe("0");
      expect(latest.has("entry_mode")).toBe(false);
    });
    expect(screen.getByRole("combobox", { name: "回放记录" })).toHaveTextContent("09-25");
    expect(screen.getByText("第 1 页", { exact: false })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "全部交易" })).not.toBeInTheDocument();
  });

  it("交易翻页切代时自动从首批和未筛选状态重新加载", async () => {
    const currentServing = metaEnvelope({ generationId: "b".repeat(64) }).serving;
    const listRequests: number[] = [];
    const detailRequests: string[] = [];
    let switched = false;
    server.use(
      http.get("*/api/v1/backtests", ({ request }) => {
        listRequests.push(Number(new URL(request.url).searchParams.get("offset") ?? "0"));
        return HttpResponse.json({
          data: { available: true, runs: [runs[1]], total: 1, next_offset: null },
          serving: switched ? currentServing : serving,
        });
      }),
      http.get("*/api/v1/backtests/:runId", ({ request }) => {
        const url = new URL(request.url);
        detailRequests.push(url.search);
        const offset = Number(url.searchParams.get("offset") ?? "0");
        if (offset === 20) {
          switched = true;
          return HttpResponse.json({ detail: "数据已更新" }, { status: 409 });
        }
        return HttpResponse.json({
          data: {
            summary_available: true,
            trades_available: true,
            run: runs[1],
            groups,
            trades: [trade],
            total_trades: 23,
            next_offset: 20,
          },
          serving: switched ? currentServing : serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    const stats = await screen.findByRole("table", { name: "配置统计" });
    await user.click(within(stats).getByText("第一次突破"));
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await waitFor(() => expect(listRequests.length).toBeGreaterThan(1));
    await waitFor(() => {
      const latest = new URLSearchParams(detailRequests.at(-1) ?? "");
      expect(latest.get("generation_id")).toBe(currentServing.generation_id);
      expect(latest.get("offset")).toBe("0");
      expect(latest.has("entry_mode")).toBe(false);
    });
    expect(listRequests.every((offset) => offset === 0)).toBe(true);
    expect(screen.queryByRole("button", { name: "全部交易" })).not.toBeInTheDocument();
  });
});
