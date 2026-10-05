import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
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
  it.each([
    ["10.0500", "10.1234", "10.05", "10.12", "成本价"],
    ["10.0500", "10.1234", "10.05", "10.12", "市价"],
    ["10.0050", "9.9999", "10.01", "10.00", "成本价"],
    ["10.0050", "9.9999", "10.01", "10.00", "市价"],
    ["0.0049", "0.0050", "0.00", "0.01", "成本价"],
    ["0.0049", "0.0050", "0.00", "0.01", "市价"],
  ])(
    "PB-FINAL-05 持仓成本 %s 和市价 %s 两位显示，保留精确提示和详情",
    async (cost, market, shownCost, shownMarket, tipKind) => {
      results();
      server.use(
        http.get(`${base}/runs/:job/rows`, () =>
          HttpResponse.json({
            ...portfolioHoldings,
            data: {
              ...portfolioHoldings.data,
              holdings: portfolioHoldings.data.holdings.slice(0, 1).map((row) => ({
                ...row,
                average_cost: cost,
                market_price: market,
              })),
            },
          }),
        ),
      );
      const user = userEvent.setup();
      renderApp("/backtest");
      await screen.findByRole("img", { name: "组合与基准净值" });
      await user.click(screen.getByRole("tab", { name: "持仓" }));
      const table = await screen.findByRole("table", { name: "持仓" });
      const first = within(table).getAllByRole("row")[1];
      if (first === undefined) throw new Error("持仓首行缺失");
      const cells = within(first).getAllByRole("cell");
      const costCell = cells.at(-2);
      const marketCell = cells.at(-1);
      if (costCell === undefined || marketCell === undefined) throw new Error("持仓价格列缺失");
      expect(costCell).toHaveTextContent(shownCost);
      expect(marketCell).toHaveTextContent(shownMarket);
      expect(costCell.textContent).toBe(shownCost);
      expect(marketCell.textContent).toBe(shownMarket);
      const tipCell = tipKind === "成本价" ? costCell : marketCell;
      const tipValue = tipKind === "成本价" ? cost : market;
      const tipAnchor = tipCell.querySelector<HTMLElement>(".tip-anchor");
      if (tipAnchor === null) throw new Error("价格提示入口缺失");
      act(() => tipAnchor.focus());
      await waitFor(() => {
        const tip = screen.getByText(`完整${tipKind}：${tipValue}`);
        expect(tip).toHaveAttribute("role", "tooltip");
      });
      act(() => tipAnchor.blur());
      await user.click(within(first).getByText("600000.SH"));
      const detail = await screen.findByRole("dialog");
      expect(within(detail).getByText(cost, { exact: true })).toBeVisible();
      expect(within(detail).getByText(market, { exact: true })).toBeVisible();
      expect(portfolioHoldings.data.holdings[0]?.average_cost).toBe("10.0500");
    },
  );

  it("PB-FINAL-04 新任务各阶段保留上一份结果，控制和报告分别绑定，新成功才替换", async () => {
    results();
    const nextId = "22222222-2222-4222-8222-222222222222";
    const nextHash = "2".repeat(64);
    let stage: Schemas["PortfolioJob"]["status"] = "queued";
    let submitted = false;
    let matchingGeneration = true;
    const exports: Schemas["PortfolioExportRequest"][] = [];
    const controls: Schemas["LabControlRequest"][] = [];
    const rowRequests: { job: string; hash: string | null }[] = [];
    function next(): Schemas["Envelope_PortfolioSummaryData_"] {
      const complete = stage === "completed";
      return {
        ...portfolioSummary,
        data: {
          ...portfolioSummary.data,
          job: {
            ...portfolioSummary.data.job,
            job_id: nextId,
            status: stage,
            label: {
              queued: "排队中",
              running: "运行中",
              paused: "已暂停",
              sealing: "正在保存",
              completed: "已完成",
              failed: "失败",
              cancelled: "已取消",
            }[stage],
            version: 7,
            start_date: "2026-09-01",
            end_date: "2026-09-02",
            result_hash: complete ? nextHash : null,
            progress: ["queued", "running", "sealing"].includes(stage) ? 0.1 : null,
            can_pause: stage === "running",
            can_resume: stage === "paused",
            can_cancel: ["queued", "running", "paused", "sealing"].includes(stage),
            can_retry: stage === "failed",
          },
          available: complete,
          result_hash: complete ? nextHash : null,
          result_status: complete ? "complete" : null,
          performance: complete ? portfolioSummary.data.performance : null,
          can_report: complete,
          message: complete ? null : "回测正在运行，完成后可查看结果。",
        },
        serving: {
          ...portfolioSummary.serving,
          generation_id: complete ? (matchingGeneration ? nextHash : "3".repeat(64)) : null,
          state: complete ? "ready" : "unavailable",
        },
      };
    }
    server.use(
      http.get(`${base}/capabilities`, () =>
        HttpResponse.json({
          ...portfolioCapabilities,
          data: { ...portfolioCapabilities.data, can_export: true },
        }),
      ),
      http.get(`${base}/runs`, () =>
        HttpResponse.json(
          submitted
            ? {
                ...portfolioJobs,
                data: {
                  ...portfolioJobs.data,
                  jobs: [next().data.job, ...portfolioJobs.data.jobs],
                },
              }
            : portfolioJobs,
        ),
      ),
      http.get(`${base}/runs/:job`, ({ params }) =>
        HttpResponse.json(params.job === nextId ? next() : portfolioSummary),
      ),
      http.get(`${base}/runs/:job/nav`, ({ params, request }) => {
        const hash = new URL(request.url).searchParams.get("result_hash");
        rowRequests.push({ job: String(params.job), hash });
        return HttpResponse.json(
          params.job === nextId
            ? {
                ...portfolioNav,
                data: { ...portfolioNav.data, result_hash: nextHash },
                serving: { ...portfolioNav.serving, generation_id: nextHash },
              }
            : portfolioNav,
        );
      }),
      http.get(`${base}/runs/:job/rows`, ({ params, request }) => {
        const hash = new URL(request.url).searchParams.get("result_hash");
        rowRequests.push({ job: String(params.job), hash });
        return HttpResponse.json(
          params.job === nextId
            ? {
                ...portfolioTrades,
                data: {
                  ...portfolioTrades.data,
                  result_hash: nextHash,
                  trades: portfolioTrades.data.trades
                    .slice(0, 1)
                    .map((row) => ({ ...row, ts_code: "600123.SH" })),
                },
                serving: { ...portfolioTrades.serving, generation_id: nextHash },
              }
            : portfolioTrades,
        );
      }),
      http.post<never, Schemas["PortfolioCreateRequest"]>(`${base}/runs`, async ({ request }) => {
        const body = await request.json();
        submitted = true;
        return HttpResponse.json({
          command_id: body.command_id,
          status: "submitted",
          message: "已提交",
          job_id: nextId,
        });
      }),
      http.post<never, Schemas["PortfolioExportRequest"]>(
        `${base}/exports`,
        async ({ request }) => {
          const body = await request.json();
          exports.push(body);
          expect(request.headers.get("x-rquant-csrf")).toBe("1");
          return HttpResponse.json({
            command_id: body.command_id,
            status: "exported",
            message: "报告已准备好。",
            job_id: body.job_id,
            result_hash: body.result_hash,
            zip_request_id: "33333333-3333-4333-8333-333333333333",
          });
        },
      ),
      http.post<never, Schemas["LabControlRequest"]>(
        "*/api/v1/tasks/jobs/commands",
        async ({ request }) => {
          const body = await request.json();
          controls.push(body);
          expect(request.headers.get("x-rquant-csrf")).toBe("1");
          return HttpResponse.json({
            command_id: body.command_id,
            status: "submitted",
            message: "操作已提交。",
            job_id: body.job_id,
          });
        },
      ),
    );
    const user = userEvent.setup();
    renderApp("/backtest");
    await screen.findByRole("img", { name: "组合与基准净值" });
    await user.click(screen.getByRole("button", { name: "新建回测" }));
    await user.click(
      within(await screen.findByRole("dialog")).getByRole("button", { name: "运行回测" }),
    );
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    await screen.findByRole("heading", { name: "2026-09-01 — 2026-09-02" });
    for (const phase of [
      "queued",
      "running",
      "paused",
      "sealing",
      "failed",
      "cancelled",
    ] as const) {
      stage = phase;
      await user.click(screen.getByRole("button", { name: "刷新" }));
      await waitFor(() =>
        expect(screen.getAllByText(next().data.job.label).length).toBeGreaterThan(0),
      );
      expect(screen.getByText("上一份结果")).toBeVisible();
      expect(screen.getByRole("img", { name: "组合与基准净值" })).toBeInTheDocument();
      expect(screen.getByRole("table", { name: "成交" })).toBeInTheDocument();
      expect(screen.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
        "href",
        expect.stringContaining(`/runs/${jobId}/report.html?result_hash=${resultHash}`),
      );
      if (phase === "running") {
        await user.click(screen.getByRole("button", { name: "暂停" }));
        await waitFor(() => expect(controls).toHaveLength(1));
        expect(controls[0]).toMatchObject({ job_id: nextId, expected_version: 7, action: "pause" });
        await user.click(screen.getByRole("button", { name: "导出 ZIP" }));
        await waitFor(() => expect(exports).toHaveLength(1));
        expect(exports[0]).toMatchObject({ job_id: jobId, result_hash: resultHash });
        expect(exports[0]).not.toHaveProperty("owner");
        expect(screen.getByRole("link", { name: "下载 ZIP" })).toHaveAttribute(
          "href",
          expect.stringContaining(`/runs/${jobId}/exports/`),
        );
      }
    }
    expect(
      rowRequests.every((request) => request.job === jobId && request.hash === resultHash),
    ).toBe(true);
    stage = "completed";
    matchingGeneration = false;
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() => expect(screen.getAllByText("已完成").length).toBeGreaterThan(1));
    expect(screen.getByText("上一份结果")).toBeVisible();
    expect(screen.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
      "href",
      expect.stringContaining(jobId),
    );
    matchingGeneration = true;
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() =>
      expect(screen.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
        "href",
        expect.stringContaining(`/runs/${nextId}/report.html?result_hash=${nextHash}`),
      ),
    );
    expect(screen.queryByText("上一份结果")).not.toBeInTheDocument();
    expect(await screen.findByText("600123.SH")).toBeVisible();
    expect(screen.queryByRole("link", { name: "下载 ZIP" })).not.toBeInTheDocument();
    expect(rowRequests.some((request) => request.job === nextId && request.hash === nextHash)).toBe(
      true,
    );
  });

  it("PB-FINAL-04 已认证访问身份改变后不继续显示或导出原身份结果", async () => {
    results();
    const { queryClient } = renderApp("/backtest");
    await screen.findByRole("img", { name: "组合与基准净值" });
    queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "another-researcher" }));
    expect(await screen.findByText("访问身份已变，请刷新页面。")).toBeVisible();
    expect(screen.queryByRole("img", { name: "组合与基准净值" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "HTML 报告" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "导出 ZIP" })).not.toBeInTheDocument();
  });

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
