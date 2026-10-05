import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { findJargon } from "@/test/jargon";
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

const base = "*/api/v1/backtests/portfolio";
const jobId = portfolioSummary.data.job.job_id;
const resultHash = portfolioSummary.data.result_hash;

function results() {
  server.use(
    http.get(`${base}/capabilities`, () => HttpResponse.json(portfolioCapabilities)),
    http.get(`${base}/runs`, () => HttpResponse.json(portfolioJobs)),
    http.get(`${base}/runs/:job`, () => HttpResponse.json(portfolioSummary)),
    http.get(`${base}/runs/:job/nav`, () => HttpResponse.json(portfolioNav)),
    http.get(`${base}/runs/:job/rows`, ({ request }) => {
      const view = new URL(request.url).searchParams.get("view");
      const rows = {
        trades: portfolioTrades,
        holdings: portfolioHoldings,
        daily: portfolioDaily,
        monthly: portfolioMonthly,
        log: portfolioLog,
      };
      return HttpResponse.json(
        view === "holdings"
          ? rows.holdings
          : view === "daily"
            ? rows.daily
            : view === "monthly"
              ? rows.monthly
              : view === "log"
                ? rows.log
                : rows.trades,
      );
    }),
  );
}

describe("日线组合回测", () => {
  it("PB-UI-PRICE-01 主表价格两位小数，悬停与详情保留精确成交价", async () => {
    results();
    server.use(
      http.get(`${base}/runs/:job/rows`, () =>
        HttpResponse.json({
          ...portfolioTrades,
          data: {
            ...portfolioTrades.data,
            trades: portfolioTrades.data.trades.map((row, index) =>
              index === 0 ? { ...row, price: "10.0001" } : row,
            ),
          },
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    const table = await screen.findByRole("table", { name: "成交" });
    const first = within(table).getAllByRole("row")[1];
    expect(first).toBeDefined();
    if (first === undefined) throw new Error("成交首行缺失");
    const price = within(first).getByText("10.00", { exact: true });
    expect(within(first).queryByText("10.0001", { exact: true })).not.toBeInTheDocument();
    await user.hover(price);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("完整成交价：10.0001");
    await user.unhover(price);
    await user.click(within(first).getByText("600000.SH"));
    const detail = await screen.findByRole("dialog");
    expect(within(detail).getByText("10.0001", { exact: true })).toBeVisible();
    expect(portfolioTrades.data.trades[0]?.price).toBe("10.0000");
  });

  it("默认显示真实净值与五个结果标签，报告绑定同一结果", async () => {
    results();
    const user = userEvent.setup();
    const { container } = renderApp("/backtest");
    expect(await screen.findByRole("button", { name: "新建回测" })).toBeVisible();
    expect(await screen.findByRole("img", { name: "组合与基准净值" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "组合回撤" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
      "href",
      expect.stringContaining(`/runs/${jobId}/report.html?result_hash=${resultHash}`),
    );
    for (const label of ["成交", "持仓", "每日", "月度", "日志"]) {
      await user.click(screen.getByRole("tab", { name: label }));
      expect(await screen.findByRole("table", { name: label })).toBeInTheDocument();
    }
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(container.querySelector("main")?.textContent).not.toContain(resultHash);
  });

  it("表单发送完整 v3 成本和回撤配置，未知回执重试原请求", async () => {
    results();
    const bodies: Schemas["PortfolioCreateRequest"][] = [];
    server.use(
      http.post<never, Schemas["PortfolioCreateRequest"]>(`${base}/runs`, async ({ request }) => {
        const body: Schemas["PortfolioCreateRequest"] = await request.json();
        bodies.push(body);
        if (bodies.length === 1) return new HttpResponse(null, { status: 503 });
        return HttpResponse.json({
          command_id: body.command_id,
          status: "submitted",
          message: "已提交",
          job_id: jobId,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    await waitFor(() => expect(screen.getByRole("button", { name: "新建回测" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "新建回测" }));
    const dialog = await screen.findByRole("dialog");
    const cash = within(dialog).getByLabelText("初始资金（元）");
    await user.clear(cash);
    await user.type(cash, "250000");
    const minimum = within(dialog).getByLabelText("最低佣金（元）");
    await user.clear(minimum);
    await user.type(minimum, "8");
    const commission = within(dialog).getByLabelText("佣金费率（%）");
    await user.clear(commission);
    await user.type(commission, "0.03");
    await user.click(within(dialog).getByRole("checkbox", { name: "启用回撤限制" }));
    await user.click(within(dialog).getByRole("button", { name: "运行回测" }));
    await user.click(await screen.findByRole("button", { name: "重试原请求" }));
    await waitFor(() => expect(bodies).toHaveLength(2));
    expect(bodies[0]).toEqual(bodies[1]);
    const config = bodies[0]?.config;
    expect(config?.initial_cash).toBe("250000");
    expect(config?.execution_cost_spec.schema_version).toBe(3);
    expect(config?.execution_cost_spec.commission_rules[0]?.minimum_amount).toBe("8");
    expect(config?.execution_cost_spec.commission_rules[0]?.rate_bps).toBe("3");
    expect(config?.execution_cost_spec).not.toHaveProperty("minimum_commission");
    expect(config?.execution_cost_spec.cost_spec_id).toBeNull();
    expect(config?.drawdown_rule?.trigger_drawdown).toBe("0.1");
  });

  it("缺少受信来源时明确不可运行", async () => {
    results();
    server.use(
      http.get(`${base}/capabilities`, () =>
        HttpResponse.json({
          ...portfolioCapabilities,
          data: {
            ...portfolioCapabilities.data,
            can_run: false,
            sources: [],
            default_config: null,
            message: "尚无可用回测来源，请先准备完整候选与行情。",
          },
        }),
      ),
    );
    renderApp("/backtest");
    expect(await screen.findByText("尚无可用回测来源，请先准备完整候选与行情。")).toBeVisible();
    expect(screen.getByRole("button", { name: "新建回测" })).toBeDisabled();
  });

  it("迟到的旧详情不能替换当前已选回测", async () => {
    results();
    const secondId = "22222222-2222-4222-8222-222222222222";
    const second = {
      ...portfolioSummary,
      data: {
        ...portfolioSummary.data,
        job: {
          ...portfolioSummary.data.job,
          job_id: secondId,
          start_date: "2026-07-01",
          end_date: "2026-07-31",
        },
      },
    };
    let resolveFirst: (() => void) | undefined;
    server.use(
      http.get(`${base}/runs`, () =>
        HttpResponse.json({
          ...portfolioJobs,
          data: { ...portfolioJobs.data, jobs: [...portfolioJobs.data.jobs, second.data.job] },
        }),
      ),
      http.get(`${base}/runs/:job`, async ({ params }) => {
        if (params.job === secondId) return HttpResponse.json(second);
        await new Promise<void>((resolve) => {
          resolveFirst = resolve;
        });
        return HttpResponse.json(portfolioSummary);
      }),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    await waitFor(() => expect(resolveFirst).toBeDefined());
    await user.click(screen.getByText("2026-07-01"));
    expect(await screen.findByRole("heading", { name: "2026-07-01 — 2026-07-31" })).toBeVisible();
    resolveFirst?.();
    await waitFor(() =>
      expect(
        screen.queryByRole("heading", { name: "2026-08-10 — 2026-08-11" }),
      ).not.toBeInTheDocument(),
    );
    expect(screen.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
      "href",
      expect.stringContaining(secondId),
    );
  });

  it("提交期间改选其他记录，旧回执不会把当前目标切回去", async () => {
    results();
    const secondId = "22222222-2222-4222-8222-222222222222";
    const second = {
      ...portfolioSummary,
      data: {
        ...portfolioSummary.data,
        job: {
          ...portfolioSummary.data.job,
          job_id: secondId,
          start_date: "2026-07-01",
          end_date: "2026-07-31",
        },
      },
    };
    let finish: (() => void) | undefined;
    server.use(
      http.get(`${base}/runs`, () =>
        HttpResponse.json({
          ...portfolioJobs,
          data: { ...portfolioJobs.data, jobs: [...portfolioJobs.data.jobs, second.data.job] },
        }),
      ),
      http.get(`${base}/runs/:job`, ({ params }) =>
        HttpResponse.json(params.job === secondId ? second : portfolioSummary),
      ),
      http.post<never, Schemas["PortfolioCreateRequest"]>(`${base}/runs`, async ({ request }) => {
        const body = await request.json();
        await new Promise<void>((resolve) => {
          finish = resolve;
        });
        return HttpResponse.json({
          command_id: body.command_id,
          status: "submitted",
          message: "已提交",
          job_id: jobId,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    await waitFor(() => expect(screen.getByRole("button", { name: "新建回测" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "新建回测" }));
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "运行回测" }));
    await waitFor(() => expect(finish).toBeDefined());
    await user.keyboard("{Escape}");
    await user.click(screen.getByText("2026-07-01"));
    expect(await screen.findByRole("heading", { name: "2026-07-01 — 2026-07-31" })).toBeVisible();
    finish?.();
    await screen.findByRole("status");
    expect(screen.getByRole("heading", { name: "2026-07-01 — 2026-07-31" })).toBeVisible();
  });
});
