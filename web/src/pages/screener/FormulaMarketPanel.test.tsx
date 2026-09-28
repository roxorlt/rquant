import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { FormulaPreviewDialog } from "./FormulaPreviewDialog";

const serving = metaEnvelope().serving;
const taskId = "a".repeat(32);
const formula = "CLOSE>MA(CLOSE,2)";
const tradeDate = "2026-09-24";
const job: Schemas["FormulaMarketJobItem"] = {
  task_id: taskId,
  status: "succeeded",
  status_label: "已完成",
  hint: "可以查看命中股票。",
  formula,
  trade_date: tradeDate,
  created_at: "2026-09-24T07:30:00Z",
  updated_at: "2026-09-24T07:31:00Z",
  result_available: true,
};
const summary: Schemas["FormulaMarketResultSummary"] = {
  market_total: 100,
  listed_count: 100,
  paused_count: 0,
  match_count: 51,
  no_match_count: 40,
  unknown_count: 9,
  unknown_reasons: [{ reason: "missing_value", label: "行情字段缺失", count: 9 }],
};

function setupSource(): void {
  server.use(
    http.get("*/api/v1/screen/tdx/preview/source", () =>
      HttpResponse.json({
        available: true,
        dates: [tradeDate],
        source: { identity: "b".repeat(64), updated_at: "2026-09-24T07:31:00Z" },
      }),
    ),
    http.post("*/api/v1/screen/tdx/parse", () =>
      HttpResponse.json({
        syntax_version: "tdx-v1",
        status: "parsed",
        capability: "parse_only",
        ast: null,
        translation: null,
        issues: [],
        unsupported: [],
      }),
    ),
  );
}

function renderPreview(seedMeta = false, viewer = "tester") {
  const queryClient = testQueryClient();
  if (seedMeta) queryClient.setQueryData(["meta"], metaEnvelope({ viewer }));
  return render(
    <AppProviders queryClient={queryClient}>
      <FormulaPreviewDialog onClose={() => undefined} />
    </AppProviders>,
  );
}

async function checkAndChooseDate(user: ReturnType<typeof userEvent.setup>): Promise<HTMLElement> {
  const drawer = await screen.findByRole("dialog", { name: "公式预览" });
  await user.type(within(drawer).getByRole("textbox", { name: "通达信公式" }), formula);
  await user.click(within(drawer).getByRole("button", { name: "检查公式" }));
  await within(drawer).findByText("公式检查通过，可预览或批量运行。");
  await user.type(within(drawer).getByLabelText("运行日期"), tradeDate);
  return drawer;
}

describe("全市场公式选股", () => {
  it("确认后提交一次；任务尚未发布时保留回执，发布后展示真实分页和历史公式", async () => {
    setupSource();
    let published = false;
    const submitted: Schemas["FormulaMarketCommandRequest"][] = [];
    const cursors: (string | null)[] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        HttpResponse.json({
          data: {
            availability: published ? "ready" : "empty",
            available_at: "2026-09-24T07:32:00Z",
            has_older_tasks: false,
            jobs: published ? [job] : [],
            message: published ? "" : "还没有选股任务。",
            total_task_count: published ? 1 : 0,
          },
          serving,
        }),
      ),
      http.post("*/api/v1/screen/tdx/market/commands", async ({ request }) => {
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        submitted.push((await request.json()) as Schemas["FormulaMarketCommandRequest"]);
        return HttpResponse.json({
          command_id: submitted[0]?.command_id,
          status: "queued",
          task_id: taskId,
          message: "已提交，等待选股结果。",
        });
      }),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}`, () =>
        published
          ? HttpResponse.json({ data: { job, summary }, serving })
          : HttpResponse.json({ detail: "没有找到这项选股任务。" }, { status: 404 }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}/matches`, ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        cursors.push(cursor);
        return HttpResponse.json({
          data: {
            task_id: taskId,
            total: 51,
            offset: cursor ? 50 : 0,
            match_codes: cursor ? ["600051.SH"] : ["600001.SH"],
            next_cursor: cursor ? null : "page-2",
          },
          serving,
        });
      }),
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
    const user = userEvent.setup();
    renderPreview();
    const drawer = await checkAndChooseDate(user);
    await user.click(within(drawer).getByRole("button", { name: "运行全市场" }));
    expect(submitted).toHaveLength(0);
    expect(await screen.findByText(/将用 2026-09-24 的已归档 A 股计算/)).toHaveTextContent(
      tradeDate,
    );
    await user.click(screen.getByRole("button", { name: "确认运行" }));
    expect(await within(drawer).findByText("已提交，等待任务出现")).toBeVisible();
    expect(submitted).toHaveLength(1);
    expect(submitted[0]).toMatchObject({ formula, trade_date: tradeDate });
    expect(submitted[0]?.command_id).toBeTruthy();
    expect(drawer.textContent).not.toContain(taskId);

    published = true;
    await user.click(within(drawer).getByRole("button", { name: "刷新任务" }));
    expect(await within(drawer).findByRole("region", { name: "市场结果" })).toHaveTextContent("51");
    expect(within(drawer).getByRole("region", { name: "市场结果" })).toHaveTextContent("100");
    expect(within(drawer).getByText("行情字段缺失")).toBeVisible();
    await user.click(within(drawer).getByRole("button", { name: /600001.SH/ }));
    const stockTitle = await screen.findByText(/样本01/);
    const stockDrawer = stockTitle.closest('[role="dialog"]');
    expect(stockDrawer).not.toBeNull();
    await user.click(within(stockDrawer as HTMLElement).getByRole("button", { name: "关闭" }));

    await user.click(within(drawer).getByRole("button", { name: "下一页" }));
    expect(await within(drawer).findByRole("button", { name: /600051.SH/ })).toBeVisible();
    expect(cursors).toEqual([null, "page-2"]);
    await user.type(within(drawer).getByRole("textbox", { name: "通达信公式" }), " AND OPEN>0");
    expect(within(drawer).getByText("历史公式与日期")).toBeVisible();
    expect(await within(drawer).findByRole("button", { name: /600001.SH/ })).toBeVisible();
    expect(within(drawer).getByRole("button", { name: "运行全市场" })).toBeDisabled();
    expect(findJargon(drawer.textContent ?? "")).toEqual([]);
  });

  it("待确认请求离开后仍能同编号重试，不生成第二条命令", async () => {
    setupSource();
    const submitted: Schemas["FormulaMarketCommandRequest"][] = [];
    server.use(
      http.post("*/api/v1/screen/tdx/market/commands", async ({ request }) => {
        submitted.push((await request.json()) as Schemas["FormulaMarketCommandRequest"]);
        if (submitted.length === 1) {
          return HttpResponse.json(
            { detail: "提交状态待确认，请使用原请求重试。" },
            { status: 503 },
          );
        }
        return HttpResponse.json({
          command_id: submitted[1]?.command_id,
          status: "queued",
          task_id: taskId,
          message: "已提交，等待选股结果。",
        });
      }),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}`, () =>
        HttpResponse.json({ detail: "没有找到这项选股任务。" }, { status: 404 }),
      ),
    );
    const user = userEvent.setup();
    const view = renderPreview();
    const drawer = await checkAndChooseDate(user);
    await user.click(within(drawer).getByRole("button", { name: "运行全市场" }));
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    expect(await within(drawer).findByText("提交状态待确认")).toBeVisible();
    view.unmount();

    renderPreview();
    const restored = await screen.findByRole("dialog", { name: "公式预览" });
    expect(within(restored).getByText("提交状态待确认")).toBeVisible();
    await user.click(within(restored).getByRole("button", { name: "重试原请求" }));
    expect(await within(restored).findByText("已提交，等待任务出现")).toBeVisible();
    expect(submitted).toHaveLength(2);
    expect(submitted[1]).toEqual(submitted[0]);
  });

  it("失败的历史任务须再次确认，并用新编号重试原公式与日期", async () => {
    setupSource();
    const failed: Schemas["FormulaMarketJobItem"] = {
      ...job,
      status: "failed",
      status_label: "未完成",
      hint: "请检查公式和数据后重试。",
      result_available: false,
    };
    const submitted: Schemas["FormulaMarketCommandRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:32:00Z",
            has_older_tasks: false,
            jobs: [failed],
            message: "",
            total_task_count: 1,
          },
          serving,
        }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}`, () =>
        HttpResponse.json({ data: { job: failed, summary: null }, serving }),
      ),
      http.post("*/api/v1/screen/tdx/market/commands", async ({ request }) => {
        submitted.push((await request.json()) as Schemas["FormulaMarketCommandRequest"]);
        return HttpResponse.json({
          command_id: submitted[0]?.command_id,
          status: "failed",
          task_id: null,
          message: "提交失败，请检查公式和数据后重试。",
        });
      }),
    );
    const user = userEvent.setup();
    renderPreview();
    const drawer = await screen.findByRole("dialog", { name: "公式预览" });
    await user.click(await within(drawer).findByRole("button", { name: /CLOSE>MA/ }));
    expect(await within(drawer).findByText("请检查公式和数据后重试。")).toBeVisible();
    await user.click(within(drawer).getByRole("button", { name: "重新运行此公式" }));
    expect(submitted).toHaveLength(0);
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    await waitFor(() => expect(submitted).toHaveLength(1));
    expect(submitted[0]).toMatchObject({ formula, trade_date: tradeDate });
    expect(submitted[0]?.command_id).not.toBe(taskId);
  });

  it("任务回读改为结果不可用时立即遮住旧摘要和命中，恢复后重新读取", async () => {
    setupSource();
    let readable = true;
    let nextResult = false;
    const detailReads: number[] = [];
    const matchReads: number[] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:32:00Z",
            has_older_tasks: false,
            jobs: [{ ...job, result_available: readable }],
            message: "",
            total_task_count: 1,
          },
          serving,
        }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}`, () => {
        detailReads.push(1);
        return HttpResponse.json({
          data: {
            job,
            summary: { ...summary, match_count: nextResult ? 1 : 51 },
          },
          serving,
        });
      }),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}/matches`, () => {
        matchReads.push(1);
        return HttpResponse.json({
          data: {
            task_id: taskId,
            total: 1,
            offset: 0,
            match_codes: [nextResult ? "600002.SH" : "600001.SH"],
            next_cursor: null,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderPreview();
    const drawer = await screen.findByRole("dialog", { name: "公式预览" });
    await user.click(await within(drawer).findByRole("button", { name: /CLOSE>MA/ }));
    expect(await within(drawer).findByRole("button", { name: /600001.SH/ })).toBeVisible();
    expect(within(drawer).getByRole("region", { name: "市场结果" })).toHaveTextContent("51");

    readable = false;
    await user.click(within(drawer).getAllByRole("button", { name: "刷新" })[0] as HTMLElement);
    expect(await within(drawer).findByText("结果暂时无法读取")).toBeVisible();
    expect(within(drawer).queryByRole("region", { name: "市场结果" })).toBeNull();
    expect(within(drawer).queryByRole("button", { name: /600001.SH/ })).toBeNull();

    nextResult = true;
    readable = true;
    await user.click(within(drawer).getAllByRole("button", { name: "刷新" })[1] as HTMLElement);
    expect(await within(drawer).findByRole("button", { name: /600002.SH/ })).toBeVisible();
    expect(within(drawer).getByRole("region", { name: "市场结果" })).toHaveTextContent("1");
    expect(within(drawer).queryByRole("button", { name: /600001.SH/ })).toBeNull();
    expect(detailReads).toHaveLength(2);
    expect(matchReads).toHaveLength(2);
  });

  it("命中游标失效后可从第一页重看，并实际重新取第一页", async () => {
    setupSource();
    let firstPageReads = 0;
    const cursors: (string | null)[] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:32:00Z",
            has_older_tasks: false,
            jobs: [job],
            message: "",
            total_task_count: 1,
          },
          serving,
        }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}`, () =>
        HttpResponse.json({ data: { job, summary }, serving }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}/matches`, ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        cursors.push(cursor);
        if (cursor !== null) {
          return HttpResponse.json({ detail: "选股结果已更新，请重新打开查看。" }, { status: 409 });
        }
        firstPageReads += 1;
        return HttpResponse.json({
          data: {
            task_id: taskId,
            total: 51,
            offset: 0,
            match_codes: [firstPageReads === 1 ? "600001.SH" : "600002.SH"],
            next_cursor: "page-2",
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderPreview();
    const drawer = await screen.findByRole("dialog", { name: "公式预览" });
    await user.click(await within(drawer).findByRole("button", { name: /CLOSE>MA/ }));
    expect(await within(drawer).findByRole("button", { name: /600001.SH/ })).toBeVisible();
    await user.click(within(drawer).getByRole("button", { name: "下一页" }));
    expect(await within(drawer).findByText("结果已更新")).toBeVisible();
    await user.click(within(drawer).getByRole("button", { name: "从第一页重看" }));
    expect(await within(drawer).findByRole("button", { name: /600002.SH/ })).toBeVisible();
    expect(within(drawer).getByText("第 1 页")).toBeVisible();
    expect(cursors).toEqual([null, "page-2", null]);
  });

  it("保存状态不明时跨页面保留原命令；成功后等精确版本发布才提示可查看", async () => {
    setupSource();
    const submitted: Schemas["FormulaPoolSaveCommandRequest"][] = [];
    let published = false;
    let unreadable = false;
    let recentVisible = true;
    let jobsUnavailable = false;
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        jobsUnavailable
          ? HttpResponse.json({ detail: "暂不可用" }, { status: 503 })
          : HttpResponse.json({
              data: {
                availability: "ready",
                available_at: "2026-09-24T07:32:00Z",
                has_older_tasks: false,
                jobs: recentVisible ? [job] : [],
                message: "",
                total_task_count: 1,
              },
              serving,
            }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}`, () =>
        HttpResponse.json({ data: { job, summary }, serving }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${taskId}/matches`, () =>
        HttpResponse.json({
          data: {
            task_id: taskId,
            total: 51,
            offset: 0,
            match_codes: ["600001.SH"],
            next_cursor: null,
          },
          serving,
        }),
      ),
      http.get("*/api/v1/pools/formula", () =>
        HttpResponse.json({
          data: {
            availability: unreadable ? "unavailable" : published ? "ready" : "empty",
            available_at: null,
            message: "",
            pools: published
              ? [
                  {
                    pool_name: "user/趋势池",
                    display_name: "趋势池",
                    formula,
                    syntax_version: "tdx-v1",
                    created_at: "2026-09-24T07:31:00Z",
                    status_label: "尚未运行",
                    latest_result: null,
                    version: "f".repeat(64),
                  },
                ]
              : [],
          },
          serving,
        }),
      ),
      http.post("*/api/v1/pools/formula/commands", async ({ request }) => {
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        submitted.push((await request.json()) as Schemas["FormulaPoolSaveCommandRequest"]);
        if (submitted.length === 1)
          return HttpResponse.json({ detail: "请重试原请求" }, { status: 503 });
        unreadable = true;
        return HttpResponse.json({
          command_id: submitted[0]?.command_id,
          status: "succeeded",
          pool_name: "user/趋势池",
          version: "f".repeat(64),
          message: "已保存",
        });
      }),
    );
    const user = userEvent.setup();
    const view = renderPreview(true);
    const drawer = await screen.findByRole("dialog", { name: "公式预览" });
    await user.click(await within(drawer).findByRole("button", { name: /CLOSE>MA/ }));
    expect(await within(drawer).findByRole("region", { name: "市场结果" })).toBeVisible();
    await user.type(within(drawer).getByRole("textbox", { name: "池子名称" }), "趋势池");
    await user.click(within(drawer).getByRole("button", { name: "保存为池子" }));
    expect(await within(drawer).findByText("保存状态待确认")).toBeVisible();
    expect(submitted[0]).toMatchObject({
      base_name: "趋势池",
      display_name: "趋势池",
      task_id: taskId,
      expected_version: null,
    });
    view.unmount();

    const other = renderPreview(true, "another-viewer");
    const otherDrawer = await screen.findByRole("dialog", { name: "公式预览" });
    expect(within(otherDrawer).queryByText("保存状态待确认")).toBeNull();
    other.unmount();

    recentVisible = false;
    const absent = renderPreview(true);
    const absentDrawer = await screen.findByRole("dialog", { name: "公式预览" });
    expect(await within(absentDrawer).findByText("保存状态待确认")).toBeVisible();
    expect(within(absentDrawer).getByRole("button", { name: "继续核对" })).toBeVisible();
    absent.unmount();

    jobsUnavailable = true;
    renderPreview(true);
    const restored = await screen.findByRole("dialog", { name: "公式预览" });
    expect(await within(restored).findByText("保存状态待确认")).toBeVisible();
    expect(await within(restored).findByText("最近运行暂不可用")).toBeVisible();
    await user.click(within(restored).getByRole("button", { name: "继续核对" }));
    expect(await within(restored).findByText("已保存，暂无法确认池子")).toBeVisible();
    expect(submitted[1]).toEqual(submitted[0]);
    unreadable = false;
    await user.click(within(restored).getByRole("button", { name: "重试读取" }));
    expect(await within(restored).findByText("已保存，等待池子发布")).toBeVisible();
    published = true;
    await user.click(within(restored).getByRole("button", { name: "检查发布" }));
    expect(await within(restored).findByText("已保存，可在池子画布查看")).toBeVisible();
    await user.click(within(restored).getByRole("button", { name: "保存另一个" }));
    expect(within(restored).queryByRole("region", { name: "保存公式池" })).toBeNull();
    expect(findJargon(restored.textContent ?? "")).toEqual([]);
  });

  it("待确认的池子任务仍在列表但结果不可读时，可用原命令继续核对", async () => {
    setupSource();
    const request: Schemas["FormulaPoolSaveCommandRequest"] = {
      base_name: "趋势池",
      display_name: "趋势池",
      task_id: taskId,
      expected_version: null,
      command_id: "c".repeat(32),
      requested_at: "2026-09-24T07:34:00Z",
    };
    window.localStorage.setItem(
      "rquant-formula-pool-save-v1",
      JSON.stringify({
        viewer: "tester",
        request,
        status: "pending",
        poolName: null,
        version: null,
      }),
    );
    const submitted: Schemas["FormulaPoolSaveCommandRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:32:00Z",
            has_older_tasks: false,
            jobs: [{ ...job, result_available: false }],
            message: "",
            total_task_count: 1,
          },
          serving,
        }),
      ),
      http.post("*/api/v1/pools/formula/commands", async ({ request: received }) => {
        submitted.push((await received.json()) as Schemas["FormulaPoolSaveCommandRequest"]);
        return HttpResponse.json({ detail: "请使用原请求重试。" }, { status: 503 });
      }),
    );
    const user = userEvent.setup();
    renderPreview(true);
    const drawer = await screen.findByRole("dialog", { name: "公式预览" });
    expect(await within(drawer).findByText("结果暂时无法读取")).toBeVisible();
    expect(within(drawer).getAllByRole("region", { name: "保存公式池" })).toHaveLength(1);
    await user.click(within(drawer).getByRole("button", { name: "继续核对" }));
    await waitFor(() => {
      expect(submitted).toEqual([request]);
      expect(within(drawer).getByRole("button", { name: "继续核对" })).toBeEnabled();
    });
    expect(within(drawer).getAllByRole("region", { name: "保存公式池" })).toHaveLength(1);
    expect(within(drawer).queryByText("保存状态待确认，请继续核对。")).toBeNull();
  });

  it("选看另一个任务时待确认保存只出现一次，结束后立即显示所选任务表单", async () => {
    setupSource();
    const otherTaskId = "b".repeat(32);
    const otherJob: Schemas["FormulaMarketJobItem"] = {
      ...job,
      task_id: otherTaskId,
      formula: "OPEN>0",
    };
    const request: Schemas["FormulaPoolSaveCommandRequest"] = {
      base_name: "趋势池",
      display_name: "趋势池",
      task_id: taskId,
      expected_version: null,
      command_id: "c".repeat(32),
      requested_at: "2026-09-24T07:34:00Z",
    };
    window.localStorage.setItem(
      "rquant-formula-pool-save-v1",
      JSON.stringify({
        viewer: "tester",
        request,
        status: "pending",
        poolName: null,
        version: null,
      }),
    );
    const submitted: Schemas["FormulaPoolSaveCommandRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/market/jobs", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:32:00Z",
            has_older_tasks: true,
            jobs: [otherJob],
            message: "",
            total_task_count: 2,
          },
          serving,
        }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${otherTaskId}`, () =>
        HttpResponse.json({ data: { job: otherJob, summary }, serving }),
      ),
      http.get(`*/api/v1/screen/tdx/market/jobs/${otherTaskId}/matches`, () =>
        HttpResponse.json({
          data: {
            task_id: otherTaskId,
            total: 51,
            offset: 0,
            match_codes: ["600001.SH"],
            next_cursor: null,
          },
          serving,
        }),
      ),
      http.get("*/api/v1/pools/formula", () =>
        HttpResponse.json({
          data: { availability: "empty", available_at: null, message: "", pools: [] },
          serving,
        }),
      ),
      http.post("*/api/v1/pools/formula/commands", async ({ request: received }) => {
        submitted.push((await received.json()) as Schemas["FormulaPoolSaveCommandRequest"]);
        return HttpResponse.json({
          command_id: request.command_id,
          status: "succeeded",
          pool_name: "user/趋势池",
          version: "f".repeat(64),
          message: "已保存",
        });
      }),
    );
    const user = userEvent.setup();
    renderPreview(true);
    const drawer = await screen.findByRole("dialog", { name: "公式预览" });
    await user.click(await within(drawer).findByRole("button", { name: /OPEN>0/ }));
    expect(await within(drawer).findByRole("region", { name: "市场结果" })).toBeVisible();
    expect(within(drawer).getAllByRole("region", { name: "保存公式池" })).toHaveLength(1);
    expect(within(drawer).getByText("保存状态待确认")).toBeVisible();
    expect(within(drawer).queryByRole("textbox", { name: "池子名称" })).toBeNull();
    await user.click(within(drawer).getByRole("button", { name: "继续核对" }));
    await waitFor(() => expect(submitted).toEqual([request]));
    await user.click(await within(drawer).findByRole("button", { name: "保存另一个" }));
    expect(within(drawer).getAllByRole("region", { name: "保存公式池" })).toHaveLength(1);
    expect(within(drawer).getByRole("textbox", { name: "池子名称" })).toBeVisible();
    expect(within(drawer).queryByText("保存状态待确认")).toBeNull();
  });
});
