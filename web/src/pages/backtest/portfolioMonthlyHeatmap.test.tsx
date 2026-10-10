import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import {
  portfolioCapabilities,
  portfolioDaily,
  portfolioHoldings,
  portfolioJobs,
  portfolioLog,
  portfolioMonthly,
  portfolioNav,
  portfolioSummary,
  portfolioTrades,
} from "./portfolio.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

type Month = Schemas["PortfolioMonthRow"];
const endpoint = "*/api/v1/backtests/portfolio";
const resultHash = portfolioSummary.data.result_hash;

function months(firstYear: number, lastYear: number): Month[] {
  return Array.from({ length: lastYear - firstYear + 1 }, (_, year) =>
    Array.from({ length: 12 }, (_, month) => ({
      year: firstYear + year,
      month: month + 1,
      return_rate: null,
    })),
  ).flat();
}

function originalMonthPages(
  values: Month[],
  period: [string, string],
  changePage?: (
    page: Schemas["Envelope_PortfolioRowsData_"],
    offset: number,
  ) => Schemas["Envelope_PortfolioRowsData_"],
) {
  const offsets: number[] = [];
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: false, can_generate: false },
      }),
    ),
    http.post("*/api/v1/ai/interpretations/read", () =>
      HttpResponse.json({ detail: "解读尚未配置。" }, { status: 503 }),
    ),
    http.get(`${endpoint}/capabilities`, () => HttpResponse.json(portfolioCapabilities)),
    http.get(`${endpoint}/runs`, () =>
      HttpResponse.json({
        ...portfolioJobs,
        data: {
          ...portfolioJobs.data,
          jobs: portfolioJobs.data.jobs.map((job) => ({
            ...job,
            start_date: period[0],
            end_date: period[1],
          })),
        },
      }),
    ),
    http.get(`${endpoint}/runs/:job`, () =>
      HttpResponse.json({
        ...portfolioSummary,
        data: {
          ...portfolioSummary.data,
          job: { ...portfolioSummary.data.job, start_date: period[0], end_date: period[1] },
        },
      }),
    ),
    http.get(`${endpoint}/runs/:job/nav`, () => HttpResponse.json(portfolioNav)),
    http.get(`${endpoint}/runs/:job/rows`, ({ request }) => {
      const params = new URL(request.url).searchParams;
      const view = params.get("view");
      if (view === "monthly") {
        const offset = Number(params.get("offset"));
        offsets.push(offset);
        expect(params.get("limit")).toBe("50");
        expect(params.get("result_hash")).toBe(resultHash);
        const rows = values.slice(offset, offset + 50);
        const page: Schemas["Envelope_PortfolioRowsData_"] = {
          ...portfolioMonthly,
          data: {
            ...portfolioMonthly.data,
            monthly: rows,
            total: values.length,
            next_offset: offset + rows.length < values.length ? offset + rows.length : null,
          },
        };
        return HttpResponse.json(changePage ? changePage(page, offset) : page);
      }
      return HttpResponse.json(
        view === "holdings"
          ? portfolioHoldings
          : view === "daily"
            ? portfolioDaily
            : view === "log"
              ? portfolioLog
              : portfolioTrades,
      );
    }),
  );
  return offsets;
}

async function showMonths() {
  const user = userEvent.setup();
  renderApp("/backtest");
  await screen.findByRole("img", { name: "组合与基准净值" });
  await user.click(screen.getByRole("tab", { name: "月度" }));
  return user;
}

describe("月度热力", () => {
  it("M7-HEAT-01 原跨年月收益与缺值进入矩阵，原明细仍可用", async () => {
    const rows = months(2025, 2026);
    rows[11] = { year: 2025, month: 12, return_rate: -0.03 };
    rows[12] = { year: 2026, month: 1, return_rate: 0.075 };
    rows[13] = { year: 2026, month: 2, return_rate: 0 };
    originalMonthPages(rows, ["2025-12-01", "2026-03-31"]);
    await showMonths();
    const grid = await screen.findByRole("group", { name: "月度热力" });
    expect(within(grid).getByRole("button", { name: "2025年12月，月收益 -3.00%" })).toHaveAttribute(
      "data-tone",
      "down",
    );
    expect(within(grid).getByRole("button", { name: "2026年1月，月收益 7.50%" })).toHaveAttribute(
      "data-tone",
      "up",
    );
    expect(within(grid).getByRole("button", { name: "2026年2月，月收益 0.00%" })).toHaveAttribute(
      "data-tone",
      "flat",
    );
    expect(within(grid).getByRole("button", { name: "2026年3月，暂无月收益" })).toHaveAttribute(
      "data-tone",
      "unknown",
    );
    expect(within(grid).getByRole("button", { name: "2026年4月，不在本次区间" })).toHaveTextContent(
      "—",
    );
    expect(screen.getByRole("table", { name: "月度" })).toBeVisible();
  });

  it("M7-HEAT-02 极限原日期窗的84个月全部读齐，两页不重复计算收益", async () => {
    const rows = months(2019, 2025);
    rows[11] = { year: 2019, month: 12, return_rate: -0.0123 };
    rows[83] = { year: 2025, month: 12, return_rate: null };
    rows[72] = { year: 2025, month: 1, return_rate: 0.025 };
    const offsets = originalMonthPages(rows, ["2019-12-31", "2025-01-02"]);
    await showMonths();
    const grid = await screen.findByRole("group", { name: "月度热力" });
    expect(
      await within(grid).findByRole("button", { name: "2025年1月，月收益 2.50%" }),
    ).toBeVisible();
    expect(within(grid).getAllByRole("button")).toHaveLength(84);
    expect(offsets).toEqual([0, 50]);
    expect(screen.getByRole("table", { name: "月度" })).toBeVisible();
  });

  it.each(["missing", "result", "generation", "total"])(
    "M7-HEAT-03 第二页%s时不把局部数据拼成完整矩阵",
    async (failure) => {
      originalMonthPages(months(2019, 2025), ["2019-12-31", "2025-01-02"], (page, offset) => {
        if (offset === 0) return page;
        if (failure === "missing")
          return { ...page, data: { ...page.data, monthly: page.data.monthly.slice(0, 4) } };
        if (failure === "result")
          return { ...page, data: { ...page.data, result_hash: "b".repeat(64) } };
        if (failure === "generation")
          return { ...page, serving: { ...page.serving, generation_id: "b".repeat(64) } };
        return { ...page, data: { ...page.data, total: 83 } };
      });
      await showMonths();
      expect(await screen.findByText("月度数据暂不可用")).toBeVisible();
      const grid = screen.getByRole("group", { name: "月度热力" });
      expect(within(grid).queryByRole("button", { name: /年\d+月/ })).not.toBeInTheDocument();
      expect(screen.getByRole("table", { name: "月度" })).toBeVisible();
    },
  );

  it.each([85, 101])("M7-HEAT-04 超过原月表上限%s时不继续分页或显示完整矩阵", async (total) => {
    const offsets = originalMonthPages(
      months(2019, 2025),
      ["2019-12-31", "2025-01-02"],
      (page) => ({
        ...page,
        data: { ...page.data, total },
      }),
    );
    await showMonths();
    expect(await screen.findByText("月度数据暂不可用")).toBeVisible();
    expect(offsets).toEqual([0]);
    expect(
      within(screen.getByRole("group", { name: "月度热力" })).queryByRole("button", {
        name: /年\d+月/,
      }),
    ).not.toBeInTheDocument();
  });

  it("M7-HEAT-05 没有原月收益时显示空态，不填零", async () => {
    originalMonthPages([], ["2026-08-10", "2026-08-11"]);
    await showMonths();
    expect(await screen.findByText("暂无月度数据")).toBeVisible();
    expect(
      within(screen.getByRole("group", { name: "月度热力" })).queryByText("0.00%"),
    ).not.toBeInTheDocument();
  });

  it("M7-HEAT-06 键盘聚焦可查看原月收益说明", async () => {
    const rows = months(2026, 2026);
    rows[7] = { year: 2026, month: 8, return_rate: 0.025 };
    originalMonthPages(rows, ["2026-08-10", "2026-08-11"]);
    const user = await showMonths();
    const grid = await screen.findByRole("group", { name: "月度热力" });
    const previous = await within(grid).findByRole("button", { name: "2026年7月，不在本次区间" });
    act(() => previous.focus());
    await user.tab();
    const august = within(grid).getByRole("button", { name: "2026年8月，月收益 2.50%" });
    await waitFor(() => expect(august).toHaveFocus());
    await waitFor(() =>
      expect(screen.getAllByRole("tooltip").map((tip) => tip.textContent)).toContain(
        "2026年8月 · 月收益 2.50%",
      ),
    );
  });

  it("M7-HEAT-07 触屏点按原Tip，说明不编造交易笔数", async () => {
    const originalMatch = window.matchMedia;
    vi.spyOn(window, "matchMedia").mockImplementation((query) => ({
      ...originalMatch(query),
      matches: query === "(hover: none)",
    }));
    const rows = months(2026, 2026);
    rows[7] = { year: 2026, month: 8, return_rate: 0 };
    originalMonthPages(rows, ["2026-08-10", "2026-08-11"]);
    const user = await showMonths();
    const grid = await screen.findByRole("group", { name: "月度热力" });
    await user.click(await within(grid).findByRole("button", { name: "2026年8月，月收益 0.00%" }));
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("2026年8月 · 月收益 0.00%");
    expect(tip).not.toHaveTextContent("交易笔数");
  });
});
