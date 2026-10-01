import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const generation = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const nextGeneration = "d".repeat(64);
const runKey = "rquant.factor.run-operation.v1";
const factor: Schemas["FactorDefinitionItem"] = {
  factor_id: "price_volume_factor",
  name_zh: "价量动量",
  category: "technical",
  category_label: "技术",
  direction: "higher_is_better",
  direction_label: "偏好高值",
  version: 2,
  content_sha256: "a".repeat(64),
  earliest_available_date: null,
  archived: false,
  expression: "ref(close, 2)",
  dependency_columns: ["close"],
  max_history_window: 2,
};
const availability: Schemas["FactorRunAvailability"] = {
  enabled: true,
  reason: null,
  start_date: "2026-09-01",
  end_date: "2026-09-23",
  pools: [
    { selection: "all", label: "全市场（沪深非 ST）", available: true, reason: null },
    { selection: "gem", label: "创业板与科创板", available: true, reason: null },
    { selection: "hs300", label: "沪深300", available: false, reason: "缺少历史成分记录" },
    { selection: "zz1000", label: "中证1000", available: true, reason: null },
  ],
};

function publish(id = generation, rows = [factor], options = availability): void {
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ generationId: id }))),
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: rows.length ? "populated" : "empty",
          available_at: "2026-09-24T07:31:00Z",
          can_save: false,
          can_archive: true,
          definitions: rows,
        },
        serving: metaEnvelope({ generationId: id }).serving,
      }),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({ data: options, serving: metaEnvelope({ generationId: id }).serving }),
    ),
  );
}

function outcome(
  original: Schemas["FactorRunRequest"],
  status: Schemas["FactorRunOperationResult"]["status"],
  overrides: Partial<Schemas["FactorRunOperationResult"]> = {},
) {
  return {
    data: {
      original_request: original,
      status,
      reason: status === "rejected" ? "日期范围缺少历史行情" : null,
      job_id: status === "submitted" ? "b".repeat(32) : null,
      spec_sha256: status === "submitted" ? "c".repeat(64) : null,
      ...overrides,
    },
    serving: metaEnvelope().serving,
  };
}

async function confirmRun(): Promise<ReturnType<typeof userEvent.setup>> {
  const user = userEvent.setup();
  const panel = await screen.findByRole("region", { name: "检验参数" });
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
  );
  await user.click(within(panel).getByRole("button", { name: "运行检验" }));
  const dialog = await screen.findByRole("dialog", { name: "运行因子检验" });
  expect(dialog).toHaveTextContent("价量动量");
  expect(dialog).toHaveTextContent("第 2 版");
  expect(dialog).toHaveTextContent("2026-09-01 至 2026-09-23");
  await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
  return user;
}

beforeEach(() => {
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (
        name: string,
        _options: unknown,
        callback: (lock: { name: string; mode: string }) => unknown,
      ) => callback({ name, mode: "exclusive" }),
    },
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

describe("因子运行入口", () => {
  it.each([
    { name: "命令冲突", statuses: [409, 409, 409] },
    { name: "续查尚无记录", statuses: [503, 404, 409] },
    { name: "网络失联", statuses: [0, 0, 0] },
  ])("$name 不能当作可信未入队拒绝，重载仍保留完整原操作", async ({ statuses }) => {
    publish();
    const seen: Schemas["FactorRunRequest"][] = [];
    for (const [index, suffix] of ["", "/resume", "/retry"].entries()) {
      server.use(
        http.post(`*/api/v1/factors/runs${suffix}`, async ({ request }) => {
          seen.push((await request.json()) as Schemas["FactorRunRequest"]);
          const status = statuses[index] ?? 503;
          return status === 0
            ? HttpResponse.error()
            : HttpResponse.json(
                {
                  detail:
                    status === 404
                      ? "原请求尚未确认，请重试原请求。"
                      : "原请求暂不可推进，请核对参数和来源。",
                },
                { status },
              );
        }),
      );
    }
    const view = renderApp("/factors");
    const user = await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    await user.click(screen.getByRole("button", { name: "刷新检验状态" }));
    await waitFor(() => expect(seen).toHaveLength(2));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "用原请求重试检验" }));
    await waitFor(() => expect(seen).toHaveLength(3));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled(),
    );
    view.unmount();
    renderApp("/factors");
    await waitFor(() => expect(seen).toHaveLength(4));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled(),
    );
    expect(seen).toEqual([seen[0], seen[0], seen[0], seen[0]]);
    const stored: {
      request: Schemas["FactorRunRequest"];
      result: Schemas["FactorRunOperationResult"];
    } = JSON.parse(localStorage.getItem(runKey) ?? "null");
    expect(stored.request).toEqual(seen[0]);
    expect(stored.result.status).toBe("uncertain");
    expect(stored.result.job_id).toBeNull();
    expect(screen.queryByRole("button", { name: "修改检验参数" })).toBeNull();
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
    for (const button of screen.getAllByRole("button", { name: "运行检验" }))
      expect(button).toBeDisabled();
  });
  it.each(["123", "000"])("时间精度 %s 与真实模型回执一致，原请求严格恢复", async (fraction) => {
    publish();
    vi.spyOn(Date.prototype, "toISOString").mockReturnValue(`2026-10-01T00:00:00.${fraction}Z`);
    let body: Schemas["FactorRunRequest"] | null = null;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        body = (await request.json()) as Schemas["FactorRunRequest"];
        const canonical = body.requested_at
          .replace(/\.000(?:000)?Z$/, "Z")
          .replace(/\.(\d{3})Z$/, (_match, digits: string) => `.${digits}000Z`);
        return HttpResponse.json(outcome({ ...body, requested_at: canonical }, "submitted"));
      }),
    );
    renderApp("/factors");
    await confirmRun();
    await screen.findByText("已提交，等待更新。");
    expect(JSON.parse(localStorage.getItem(runKey) ?? "null").request).toEqual(body);
  });
  it("两个运行入口同一参数，确认前不发请求，提交前保存完整原请求", async () => {
    publish();
    const seen: Schemas["FactorRunRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        expect(JSON.parse(localStorage.getItem(runKey) ?? "null")).toMatchObject({ request: body });
        seen.push(body);
        return HttpResponse.json(outcome(body, "submitted"));
      }),
    );
    const { container } = renderApp("/factors");
    const panel = await screen.findByRole("region", { name: "检验参数" });
    expect(within(panel).getByRole("combobox", { name: "股票池" })).toHaveValue("all");
    expect(within(panel).getByRole("combobox", { name: "调仓周期" })).toHaveValue("5");
    expect(within(panel).getByRole("combobox", { name: "分组数" })).toHaveValue("5");
    expect(within(panel).getByRole("button", { name: "RankIC" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(seen).toEqual([]);
    await confirmRun();
    await waitFor(() => expect(seen).toHaveLength(1));
    expect(seen[0]).toMatchObject({
      serving_generation_id: generation,
      parameters: {
        factor_id: factor.factor_id,
        expected_head: { version: 2, content_sha256: factor.content_sha256 },
        selection: "all",
        start_date: "2026-09-01",
        end_date: "2026-09-23",
        holding_sessions: 5,
        group_count: 5,
        ic_method: "rank",
        neutralization: "none",
      },
    });
    expect(await screen.findByText("已提交，等待更新。")).toBeInTheDocument();
    expect(screen.queryByText("检验完成。")).toBeNull();
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
  });

  it("网络不确定、重载换代后续查与重试保持原 UUID、旧版本及完整参数", async () => {
    publish();
    const seen: Schemas["FactorRunRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        seen.push((await request.json()) as Schemas["FactorRunRequest"]);
        return new HttpResponse(null, { status: 503 });
      }),
      http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        seen.push(body);
        return HttpResponse.json(outcome(body, "uncertain"));
      }),
      http.post("*/api/v1/factors/runs/retry", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        seen.push(body);
        return HttpResponse.json(outcome(body, "submitted"));
      }),
    );
    const view = renderApp("/factors");
    const user = await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    publish(nextGeneration, [{ ...factor, version: 3, content_sha256: "f".repeat(64) }]);
    view.unmount();
    renderApp("/factors");
    await waitFor(() => expect(seen).toHaveLength(2));
    await user.click(await screen.findByRole("button", { name: "用原请求重试检验" }));
    await waitFor(() => expect(seen).toHaveLength(3));
    expect(seen).toEqual([seen[0], seen[0], seen[0]]);
    expect(seen[2]?.parameters.expected_head.version).toBe(2);
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
  });

  it.each([401, 403])("HTTP %s 保留未知操作，账号失效后禁止继续写入", async (status) => {
    publish();
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/runs", () => {
        posts += 1;
        return new HttpResponse(null, { status });
      }),
    );
    const view = renderApp("/factors");
    await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    expect(localStorage.getItem(runKey)).not.toBeNull();
    act(() => view.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: null })));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeDisabled(),
    );
    expect(posts).toBe(1);
  });

  it("明确拒绝的原操作跨刷新仍可修改参数，不丢四项选择", async () => {
    publish();
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) =>
        HttpResponse.json(
          outcome((await request.json()) as Schemas["FactorRunRequest"], "rejected"),
        ),
      ),
    );
    const view = renderApp("/factors");
    const user = await confirmRun();
    await screen.findByText("日期范围缺少历史行情");
    view.unmount();
    renderApp("/factors");
    await user.click(await screen.findByRole("button", { name: "修改检验参数" }));
    expect(localStorage.getItem(runKey)).toBeNull();
    expect(screen.getByRole("combobox", { name: "股票池" })).toHaveValue("all");
    expect(screen.getByLabelText("开始日期")).toHaveValue("2026-09-01");
  });

  it("缺历史来源显示真实原因，中性化与缺成员池不可选择", async () => {
    publish(generation, [factor], { ...availability, enabled: false, reason: "尚未准备历史行情" });
    renderApp("/factors");
    expect(await screen.findByText("尚未准备历史行情")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "运行检验" })).toBeNull();
  });

  it("无可用数据代仍可用原请求续查，缺来源不会另建请求", async () => {
    publish();
    const seen: Schemas["FactorRunRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        seen.push(body);
        return HttpResponse.json(outcome(body, "uncertain"));
      }),
    );
    const view = renderApp("/factors");
    await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    const missing = metaEnvelope({ state: "unavailable" });
    missing.data.generation = null;
    missing.serving.generation_id = null;
    server.use(
      http.get("*/api/v1/meta", () => HttpResponse.json(missing)),
      http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        seen.push(body);
        return HttpResponse.json(outcome(body, "uncertain"));
      }),
    );
    view.unmount();
    renderApp("/factors");
    await waitFor(() => expect(seen).toHaveLength(2));
    expect(seen[1]).toEqual(seen[0]);
  });

  it("确认双击仅创建一次；未知操作期间运行、归档均不能交错", async () => {
    publish();
    let resolveResponse: (() => void) | undefined;
    let posts = 0;
    const pending = new Promise<void>((resolve) => {
      resolveResponse = resolve;
    });
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        posts += 1;
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        await pending;
        return HttpResponse.json(outcome(body, "uncertain"));
      }),
    );
    const user = userEvent.setup();
    renderApp("/factors");
    const panel = await screen.findByRole("region", { name: "检验参数" });
    await waitFor(() =>
      expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
    );
    await user.click(within(panel).getByRole("button", { name: "运行检验" }));
    const confirm = within(await screen.findByRole("dialog", { name: "运行因子检验" })).getByRole(
      "button",
      { name: "确认运行" },
    );
    act(() => {
      fireEvent.click(confirm);
      fireEvent.click(confirm);
    });
    await waitFor(() => expect(posts).toBe(1));
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
    for (const button of screen.getAllByRole("button", { name: "运行检验" }))
      expect(button).toBeDisabled();
    resolveResponse?.();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    expect(posts).toBe(1);
  });

  it("提交前存储失败不发请求，并保留参数供重载", async () => {
    publish();
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/runs", () => {
        posts += 1;
        return new HttpResponse(null, { status: 503 });
      }),
    );
    const user = userEvent.setup();
    renderApp("/factors");
    const panel = await screen.findByRole("region", { name: "检验参数" });
    await waitFor(() =>
      expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
    );
    await user.click(within(panel).getByRole("button", { name: "运行检验" }));
    const originalSet = Storage.prototype.setItem;
    const broken = vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (
      this: Storage,
      key,
      value,
    ) {
      if (key === runKey) throw new DOMException("quota", "QuotaExceededError");
      originalSet.call(this, key, value);
    });
    await user.click(
      within(await screen.findByRole("dialog", { name: "运行因子检验" })).getByRole("button", {
        name: "确认运行",
      }),
    );
    expect(
      await screen.findByText("无法保留本次操作，请检查浏览器存储后重新加载。"),
    ).toBeInTheDocument();
    expect(posts).toBe(0);
    expect(localStorage.getItem(runKey)).toBeNull();
    broken.mockRestore();
  });

  it("回执改变任何原参数都不当作拒绝或完成，旧请求保留", async () => {
    publish();
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        return HttpResponse.json(
          outcome(
            { ...body, parameters: { ...body.parameters, holding_sessions: 10 } },
            "rejected",
          ),
        );
      }),
    );
    renderApp("/factors");
    await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    expect(screen.queryByRole("button", { name: "修改检验参数" })).toBeNull();
    expect(
      JSON.parse(localStorage.getItem(runKey) ?? "null").request.parameters.holding_sessions,
    ).toBe(5);
  });

  it("十组、NormalIC 与中性化不可用项按参数提交，Tip 可由键盘打开", async () => {
    publish();
    let body: Schemas["FactorRunRequest"] | null = null;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        body = (await request.json()) as Schemas["FactorRunRequest"];
        return HttpResponse.json(outcome(body, "submitted"));
      }),
    );
    const user = userEvent.setup();
    renderApp("/factors");
    const panel = await screen.findByRole("region", { name: "检验参数" });
    await waitFor(() =>
      expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
    );
    expect(within(panel).getByRole("option", { name: "沪深300" })).toBeDisabled();
    expect(within(panel).getByRole("option", { name: "行业（暂不可用）" })).toBeDisabled();
    expect(within(panel).getByRole("option", { name: "行业 + 市值（暂不可用）" })).toBeDisabled();
    await user.selectOptions(within(panel).getByRole("combobox", { name: "分组数" }), "10");
    await user.click(within(panel).getByRole("button", { name: "NormalIC" }));
    const tip = within(panel).getByText("中性化说明").closest<HTMLElement>("[tabindex]");
    act(() => tip?.focus());
    await screen.findByRole("tooltip");
    act(() => tip?.blur());
    await waitFor(() => expect(screen.queryByRole("tooltip")).toBeNull(), { timeout: 2000 });
    await confirmRun();
    await waitFor(() =>
      expect(body).toMatchObject({
        parameters: { group_count: 10, ic_method: "normal", neutralization: "none" },
      }),
    );
  });

  it("提交回执不能确认完成；后来同任务、参数与旧定义的详情匹配才完成并自动选中原结果", async () => {
    publish();
    let original: Schemas["FactorRunRequest"] | null = null;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        original = (await request.json()) as Schemas["FactorRunRequest"];
        return HttpResponse.json(outcome(original, "submitted"));
      }),
      http.post("*/api/v1/factors/runs/resume", async ({ request }) =>
        HttpResponse.json(
          outcome((await request.json()) as Schemas["FactorRunRequest"], "submitted"),
        ),
      ),
    );
    const view = renderApp("/factors");
    const user = await confirmRun();
    await screen.findByText("已提交，等待更新。");
    expect(screen.queryByText("检验完成。")).toBeNull();
    const current = {
      ...factor,
      version: 3,
      name_zh: "新版价量",
      content_sha256: "f".repeat(64),
      archived: true,
    };
    const second = { ...factor, factor_id: "second_factor", name_zh: "另一个因子" };
    publish(nextGeneration, [current, second]);
    const item: Schemas["FactorResultItem"] = {
      job_id: "b".repeat(32),
      spec_sha256: "c".repeat(64),
      definition_content_sha256: factor.content_sha256,
      factor_id: factor.factor_id,
      factor_version: 2,
      factor_name_zh: "价量动量",
      definition_status: "historical_unavailable",
      status: "succeeded",
      status_label: "已完成",
      failure_message: null,
      updated_at: "2026-09-24T07:35:00Z",
      as_of_time: "2026-09-23T07:00:00Z",
      display_status: "available",
      display_message: "结果已发布。",
    };
    const emptySummary: Schemas["ICSeriesSummary"] = {
      status: "no_valid_days",
      mean: null,
      sample_std: null,
      ir: null,
      positive_rate: null,
      strong_signal_rate: null,
      t_value: null,
      p_value: null,
      skewness: null,
      excess_kurtosis: null,
      source_day_count: 0,
      valid_day_count: 0,
      insufficient_day_count: 0,
      zero_variance_day_count: 0,
    };
    const research: Schemas["FactorStreamResearchDisplay"] = {
      schema_version: 2,
      neutralization: "none",
      neutralization_label: "无",
      basis_label: "收盘价到下一次调仓收盘价",
      pool_label: "全市场（沪深非 ST）",
      return_price_basis: "raw",
      holding_sessions: 5,
      summary_status: "no_samples",
      ic_summary: { normal_ic: emptySummary, rank_ic: emptySummary },
      coverage_days: [],
      decay_periods: [],
      ic_points: [],
      portfolio_days: [],
      portfolio_status: "insufficient_data",
    };
    let listItem: Schemas["FactorResultItem"] = { ...item, spec_sha256: "e".repeat(64) };
    let detailItem: Schemas["FactorResultItem"] = {
      ...item,
      definition_content_sha256: "e".repeat(64),
    };
    server.use(
      http.get("*/api/v1/factors/results", () =>
        HttpResponse.json({
          data: { availability: "populated", available_at: item.updated_at, results: [listItem] },
          serving: metaEnvelope({ generationId: nextGeneration }).serving,
        }),
      ),
      http.get("*/api/v1/factors/results/:jobId", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: item.updated_at,
            result: detailItem,
            research,
          },
          serving: metaEnvelope({ generationId: nextGeneration }).serving,
        }),
      ),
    );
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await screen.findByRole("row", { name: /另一个因子/ });
    await user.click(screen.getByRole("row", { name: /另一个因子/ }));
    expect(screen.queryByText("检验完成。")).toBeNull();
    listItem = item;
    await act(async () => {
      await view.queryClient.invalidateQueries({
        queryKey: ["factors", "results", nextGeneration],
      });
    });
    await waitFor(() =>
      expect(
        view.queryClient.getQueryData(["factors", "result", nextGeneration, item.job_id]),
      ).toBeDefined(),
    );
    expect(screen.queryByText("检验完成。")).toBeNull();
    detailItem = item;
    await act(async () => {
      await view.queryClient.invalidateQueries({
        queryKey: ["factors", "result", nextGeneration, item.job_id],
      });
    });
    await screen.findByText("检验完成。");
    const area = await screen.findByRole("region", { name: "检验结果" });
    expect(area).toHaveTextContent("价量动量 · 第 2 版检验 · 历史版本");
    expect(within(area).getByRole("button", { name: "RankIC" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByRole("region", { name: "因子详情" })).toHaveTextContent("新版价量");
    expect(JSON.parse(localStorage.getItem(runKey) ?? "null").request).toEqual(original);
  });

  it("其他标签恢复的原操作阻止新运行；晚到旧回执不能覆盖新原操作", async () => {
    publish();
    let resolveResponse: (() => void) | undefined;
    const pending = new Promise<void>((resolve) => {
      resolveResponse = resolve;
    });
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        posts += 1;
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        await pending;
        return HttpResponse.json(outcome(body, "submitted"));
      }),
    );
    renderApp("/factors");
    await confirmRun();
    await waitFor(() => expect(posts).toBe(1));
    const record = JSON.parse(localStorage.getItem(runKey) ?? "null");
    const replacement = {
      ...record,
      factorName: "另一次检验",
      request: { ...record.request, command_id: crypto.randomUUID() },
      result: {
        ...outcome(record.request, "rejected").data,
        original_request: { ...record.request, command_id: "" },
      },
    };
    replacement.result.original_request = replacement.request;
    act(() => {
      localStorage.setItem(runKey, JSON.stringify(replacement));
      window.dispatchEvent(new StorageEvent("storage", { key: runKey }));
    });
    await screen.findByText(/另一次检验/);
    for (const button of screen.getAllByRole("button", { name: "运行检验" }))
      expect(button).toBeDisabled();
    resolveResponse?.();
    await waitFor(() => expect(screen.getByRole("button", { name: "修改检验参数" })).toBeEnabled());
    expect(JSON.parse(localStorage.getItem(runKey) ?? "null")).toEqual(replacement);
    expect(posts).toBe(1);
  });

  it("账号切换不能续查另一个账号的操作，显式刷新重验后原账号仍使用原请求", async () => {
    publish();
    const requests: Schemas["FactorRunRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        requests.push((await request.json()) as Schemas["FactorRunRequest"]);
        return new HttpResponse(null, { status: 403 });
      }),
      http.post("*/api/v1/factors/runs/retry", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        requests.push(body);
        return HttpResponse.json(outcome(body, "submitted"));
      }),
    );
    const view = renderApp("/factors");
    const user = await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeDisabled(),
    );
    act(() => view.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "second-user" })));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeDisabled(),
    );
    expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent(
      "请切回提交本次检验的账号继续查看。",
    );
    expect(requests).toHaveLength(1);
    act(() => view.queryClient.setQueryData(["meta"], metaEnvelope()));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeDisabled(),
    );
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "用原请求重试检验" }));
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1]).toEqual(requests[0]);
  });

  it("确认期间因子或数据换代禁止提交，关闭后重新确认才采用新版本", async () => {
    publish();
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        posts += 1;
        return HttpResponse.json(
          outcome((await request.json()) as Schemas["FactorRunRequest"], "submitted"),
        );
      }),
    );
    const view = renderApp("/factors");
    const user = userEvent.setup();
    const params = await screen.findByRole("region", { name: "检验参数" });
    await waitFor(() =>
      expect(within(params).getByRole("button", { name: "运行检验" })).toBeEnabled(),
    );
    await user.click(within(params).getByRole("button", { name: "运行检验" }));
    const dialog = await screen.findByRole("dialog", { name: "运行因子检验" });
    publish(nextGeneration, [{ ...factor, version: 3, content_sha256: "f".repeat(64) }]);
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: nextGeneration })),
    );
    await waitFor(() =>
      expect(within(dialog).getByRole("button", { name: "确认运行" })).toBeDisabled(),
    );
    expect(dialog).toHaveTextContent("第 2 版");
    expect(dialog).toHaveTextContent("因子或检验条件已变化，请关闭后重新确认。");
    expect(posts).toBe(0);
    expect(localStorage.getItem(runKey)).toBeNull();
  });

  it("已恢复的保存操作阻止运行生成新请求", async () => {
    publish();
    localStorage.setItem("rquant.factor.save-command.v1", "unreadable-original-command");
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/runs", () => {
        posts += 1;
        return new HttpResponse(null, { status: 503 });
      }),
    );
    renderApp("/factors");
    const params = await screen.findByRole("region", { name: "检验参数" });
    await waitFor(() => expect(params).toHaveTextContent("请先完成保存或归档操作。"));
    expect(within(params).getByRole("button", { name: "运行检验" })).toBeDisabled();
    expect(posts).toBe(0);
    expect(localStorage.getItem(runKey)).toBeNull();
    expect(localStorage.getItem("rquant.factor.save-command.v1")).toBe(
      "unreadable-original-command",
    );
  });

  it("刷新未知检验仅续查完整原请求，不查询尚不存在的任务详情", async () => {
    publish();
    const seen: Schemas["FactorRunRequest"][] = [];
    let missingDetailQueries = 0;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        seen.push(body);
        return HttpResponse.json(outcome(body, "uncertain"));
      }),
      http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        seen.push(body);
        return HttpResponse.json(outcome(body, "uncertain"));
      }),
      http.get("*/api/v1/factors/results/", () => {
        missingDetailQueries += 1;
        return new HttpResponse(null, { status: 404 });
      }),
    );
    renderApp("/factors");
    const user = await confirmRun();
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    await user.click(screen.getByRole("button", { name: "刷新检验状态" }));
    await waitFor(() => expect(seen).toHaveLength(2));
    await waitFor(() => expect(screen.getByRole("button", { name: "刷新检验状态" })).toBeEnabled());
    expect(missingDetailQueries).toBe(0);
    expect(seen[1]).toEqual(seen[0]);
  });
});
