import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import {
  diagnosticAvailability,
  diagnosticFactor,
  diagnosticResearch,
  diagnosticResult,
  diagnosticStatistics,
} from "./factorDiagnostics.fixture";
import {
  RUN_OPERATION_KEY,
  readRun,
  sameRunRequest,
  validRunRequest,
  validRunResult,
} from "./factorRunState";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const generation = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const nextGeneration = "d".repeat(64);

function publish(id = generation, viewer = "tester") {
  const metadata = metaEnvelope({ generationId: id, viewer });
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(metadata)),
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: metadata.serving.built_at,
          can_save: false,
          can_archive: true,
          definitions: [diagnosticFactor],
        },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({ data: diagnosticAvailability, serving: metadata.serving }),
    ),
  );
}

function original(mad?: number): Schemas["FactorRunRequest"] {
  const value = {
    command_id: "88888888-8888-4888-8888-888888888888",
    requested_at: "2026-10-02T00:00:00Z",
    serving_generation_id: generation,
    parameters: {
      factor_id: diagnosticFactor.factor_id,
      expected_head: {
        version: diagnosticFactor.version,
        content_sha256: diagnosticFactor.content_sha256,
      },
      selection: "all",
      start_date: "2026-09-17",
      end_date: "2026-09-22",
      holding_sessions: 5,
      group_count: 5,
      ic_method: "rank",
      neutralization: "none",
      ...(mad === undefined ? {} : { mad_multiple: mad, extended_statistics: true }),
    },
  };
  if (!validRunRequest(value)) throw new Error("合成原请求无效");
  return value;
}

function submitted(body: Schemas["FactorRunRequest"]) {
  const result: Schemas["FactorRunOperationResult"] = {
    original_request: body,
    status: "submitted",
    reason: null,
    job_id: "b".repeat(32),
    spec_sha256: "c".repeat(64),
  };
  return { data: result, serving: metaEnvelope().serving };
}

function store(body: unknown) {
  const raw = JSON.stringify({
    viewer: "tester",
    request: body,
    factorName: "价量动量",
    poolLabel: "全市场",
    result: null,
    denied: false,
  });
  localStorage.setItem(RUN_OPERATION_KEY, raw);
  return raw;
}

async function ready() {
  const panel = await screen.findByRole("region", { name: "检验参数" });
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
  );
  return within(panel);
}

function results(research = diagnosticResearch, older?: Schemas["FactorStreamResearchDisplay"]) {
  const latest = diagnosticResult();
  const previous = diagnosticResult({
    job_id: "1".repeat(32),
    factor_version: 1,
    definition_status: "historical_unavailable",
    updated_at: "2026-09-20T07:31:00Z",
  });
  server.use(
    http.get("*/api/v1/factors/results", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: latest.updated_at,
          results: older ? [latest, previous] : [latest],
        },
        serving: metaEnvelope().serving,
      }),
    ),
    http.get("*/api/v1/factors/results/:jobId", ({ params }) =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: latest.updated_at,
          result: params.jobId === previous.job_id ? previous : latest,
          research: params.jobId === previous.job_id ? older : research,
        },
        serving: metaEnvelope().serving,
      }),
    ),
  );
}

beforeEach(() =>
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: { request: async (_name: string, _options: unknown, action: () => unknown) => action() },
  }),
);
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

it("MAD 的真实选择、确认、持久化与失联重载仍使用全部原参数", async () => {
  publish();
  const sent: Schemas["FactorRunRequest"][] = [];
  server.use(
    ...["", "/resume", "/retry"].map((suffix) =>
      http.post(`*/api/v1/factors/runs${suffix}`, async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        sent.push(body);
        expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(body);
        return suffix === "/retry" ? HttpResponse.json(submitted(body)) : HttpResponse.error();
      }),
    ),
  );
  const user = userEvent.setup();
  let view = renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "离群值处理" }), "mad");
  expect(panel.getByRole("spinbutton", { name: "MAD 倍数" })).toHaveValue(3);
  fireEvent.change(panel.getByRole("spinbutton", { name: "MAD 倍数" }), {
    target: { value: "4.5" },
  });
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  const dialog = await screen.findByRole("dialog", { name: "运行因子检验" });
  expect(dialog).toHaveTextContent("MAD 4.5 倍");
  expect(sent).toHaveLength(0);
  await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
  await screen.findByText("检验结果暂未确认，请保留本次操作。");
  const request = sent[0];
  expect(request?.parameters).toMatchObject({ mad_multiple: 4.5, extended_statistics: true });
  expect(readRun()?.request).toEqual(request);
  await user.selectOptions(panel.getByRole("combobox", { name: "离群值处理" }), "none");
  fireEvent.change(panel.getByLabelText("开始日期"), { target: { value: "2026-09-18" } });
  expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent("MAD 4.5 倍");
  publish(nextGeneration);
  view.unmount();
  view = renderApp("/factors");
  await waitFor(() => expect(sent).toHaveLength(2));
  const retry = await screen.findByRole("button", { name: "用原请求重试检验" });
  await waitFor(() => expect(retry).toBeEnabled());
  await user.click(retry);
  await screen.findByText("已提交，等待更新。");
  expect(sent).toEqual([request, request, request]);
  expect(readRun()?.request).toEqual(request);
  expect(screen.queryByText("检验完成。")).toBeNull();
  expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
});

it("关闭离群值处理的新检验固定开启扩展统计，并省略空倍数", async () => {
  publish();
  let body: Schemas["FactorRunRequest"] | undefined;
  server.use(
    http.post("*/api/v1/factors/runs", async ({ request }) => {
      body = (await request.json()) as Schemas["FactorRunRequest"];
      return HttpResponse.json(submitted(body));
    }),
  );
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  expect(panel.getByRole("combobox", { name: "离群值处理" })).toHaveValue("none");
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  expect(screen.getByRole("dialog")).toHaveTextContent("不处理离群值");
  await user.click(screen.getByRole("button", { name: "确认运行" }));
  await screen.findByText("已提交，等待更新。");
  expect(body?.parameters.extended_statistics).toBe(true);
  expect(body?.parameters).not.toHaveProperty("mad_multiple");
});

it.each(["0", "-1", ""])("MAD 倍数 %s 无效时不能确认或发请求", async (value) => {
  publish();
  let posts = 0;
  server.use(
    http.post("*/api/v1/factors/runs", () => {
      posts += 1;
      return HttpResponse.error();
    }),
  );
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "离群值处理" }), "mad");
  fireEvent.change(panel.getByRole("spinbutton", { name: "MAD 倍数" }), { target: { value } });
  expect(panel.getByRole("button", { name: "运行检验" })).toBeDisabled();
  expect(panel.getByText("MAD 倍数须为大于 0 的有限数值。")).toBeInTheDocument();
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(posts).toBe(0);
});

it("确认后变更 MAD 倍数会禁用原确认，不能替换冻结参数", async () => {
  publish();
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "离群值处理" }), "mad");
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  expect(screen.getByRole("dialog")).toHaveTextContent("MAD 3 倍");
  fireEvent.change(panel.getByRole("spinbutton", { name: "MAD 倍数" }), { target: { value: "5" } });
  expect(screen.getByRole("button", { name: "确认运行" })).toBeDisabled();
  expect(screen.getByRole("dialog")).toHaveTextContent("MAD 3 倍");
});

it("合法的小 MAD 倍数在确认中保持精度，不显示为 0", async () => {
  publish();
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "离群值处理" }), "mad");
  fireEvent.change(panel.getByRole("spinbutton", { name: "MAD 倍数" }), {
    target: { value: "0.00001" },
  });
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  expect(screen.getByRole("dialog")).toHaveTextContent("MAD 0.00001 倍");
});

it.each(["mad", "extended"] as const)("拒绝 %s 参数不一致的回执，保留原操作", async (changed) => {
  publish();
  let body: Schemas["FactorRunRequest"] | undefined;
  server.use(
    http.post("*/api/v1/factors/runs", async ({ request }) => {
      body = (await request.json()) as Schemas["FactorRunRequest"];
      return HttpResponse.json(
        submitted({
          ...body,
          parameters: {
            ...body.parameters,
            ...(changed === "mad" ? { mad_multiple: 6 } : { extended_statistics: false }),
          },
        }),
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "离群值处理" }), "mad");
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  await user.click(screen.getByRole("button", { name: "确认运行" }));
  await screen.findByText("检验结果暂未确认，请保留本次操作。");
  expect(readRun()?.request).toEqual(body);
  expect(readRun()?.result).toBeNull();
  expect(screen.queryByText("已提交，等待更新。")).toBeNull();
});

it.each([0, -3, Infinity, NaN, "3"])("非正有限数 %s 的原请求和回执不可信", (mad) => {
  const request = original(3);
  const bad = { ...request, parameters: { ...request.parameters, mad_multiple: mad } };
  expect(validRunRequest(request)).toBe(true);
  expect(validRunRequest(bad)).toBe(false);
  expect(validRunResult(submitted(bad as Schemas["FactorRunRequest"]).data, request)).toBe(false);
});

it("旧请求没有新增字段时原字节恢复，不自动开启或添加 MAD", async () => {
  publish();
  const body = original();
  const bytes = store(body);
  expect(validRunRequest(body)).toBe(true);
  expect(readRun()?.request).toEqual(body);
  expect(localStorage.getItem(RUN_OPERATION_KEY)).toBe(bytes);
  let sent: unknown;
  server.use(
    http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
      sent = await request.json();
      return HttpResponse.error();
    }),
  );
  renderApp("/factors");
  await waitFor(() => expect(sent).toEqual(body));
  expect(readRun()?.request).toEqual(body);
  expect(readRun()?.request.parameters).not.toHaveProperty("mad_multiple");
  expect(readRun()?.request.parameters).not.toHaveProperty("extended_statistics");
});

it("同 ID 的其他 MAD 意图不匹配；存储损坏仍占住操作槽", async () => {
  const body = original(3);
  expect(sameRunRequest(body, original(4))).toBe(false);
  const damaged = { ...body, parameters: { ...body.parameters, extended_statistics: "true" } };
  store(damaged);
  publish();
  renderApp("/factors");
  expect(
    await screen.findByText("本机有未能恢复的检验操作，请保留浏览器记录后重新加载。"),
  ).toBeInTheDocument();
  expect(localStorage.getItem(RUN_OPERATION_KEY)).not.toBeNull();
});

it("MAD 原请求切账号后只读，晚回执不更新操作", async () => {
  publish();
  const body = original(3);
  store(body);
  let release = () => {};
  let entered = false;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  server.use(
    http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
      expect(await request.json()).toEqual(body);
      entered = true;
      await pending;
      return HttpResponse.json(submitted(body));
    }),
  );
  const view = renderApp("/factors");
  await waitFor(() => expect(entered).toBe(true));
  await act(async () => {
    view.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" }));
  });
  await screen.findAllByText("请切回提交本次检验的账号继续查看。");
  release();
  await waitFor(() => expect(screen.getByRole("button", { name: "刷新检验状态" })).toBeDisabled());
  expect(readRun()?.request).toEqual(body);
  expect(readRun()?.result).toBeNull();
});

it("行业 IC 沿该次方法和摘要、覆盖展示，历史切换不套当前表单", async () => {
  publish();
  const older = {
    ...diagnosticResearch,
    mad_multiple: 7,
    extended_statistics: { ...diagnosticStatistics, ic_method: "normal" as const },
  };
  results(diagnosticResearch, older);
  const user = userEvent.setup();
  const view = renderApp("/factors");
  const area = await screen.findByRole("region", { name: "检验结果" });
  await within(area).findByText("MAD 2.5 倍");
  const form = await ready();
  await user.selectOptions(form.getByRole("combobox", { name: "离群值处理" }), "mad");
  fireEvent.change(form.getByRole("spinbutton", { name: "MAD 倍数" }), { target: { value: "9" } });
  expect(area).toHaveTextContent("MAD 2.5 倍");
  const industry = within(area).getByRole("region", { name: "行业 IC" });
  expect(industry).toHaveTextContent("RankIC");
  await user.click(within(industry).getByText("查看行业 IC 明细"));
  const table = within(industry).getByRole("table", { name: "行业 IC 明细" });
  expect(table).toHaveTextContent("农林牧渔");
  expect(table).toHaveTextContent("+0.0451");
  expect(table).toHaveTextContent("−0.0142");
  expect(table).toHaveTextContent("3 / 4");
  await user.click(within(industry).getByText("查看行业覆盖"));
  const coverage = within(industry).getByRole("table", { name: "行业覆盖" });
  expect(coverage).toHaveTextContent("2026-09-16");
  expect(coverage).toHaveTextContent("6 / 8");
  expect(coverage).toHaveTextContent("标签缺失 1");
  expect(coverage).toHaveTextContent("边界待核对 1");
  fireEvent.focus(within(industry).getByText("样本说明"));
  expect(await screen.findByRole("tooltip")).toHaveTextContent(
    diagnosticStatistics.industry_reason ?? "",
  );
  fireEvent.blur(within(industry).getByText("样本说明"));
  await user.click(within(area).getByRole("cell", { name: "第 1 版" }));
  await within(area).findByText("MAD 7 倍");
  expect(within(area).getByRole("region", { name: "行业 IC" })).toHaveTextContent("NormalIC");
  expect(area).toHaveTextContent("历史版本");
  expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
});

it("行业来源未配置不隐藏相邻评价期自相关，空点明细说明真实原因", async () => {
  publish();
  results({
    ...diagnosticResearch,
    extended_statistics: {
      ...diagnosticStatistics,
      industry_status: "unavailable",
      industry_reason: "缺少可核验的行业来源，未生成行业 IC。",
      industry_summaries: [],
      industry_coverage_days: [],
    },
  });
  const user = userEvent.setup();
  renderApp("/factors");
  expect(await screen.findByText("缺少可核验的行业来源，未生成行业 IC。")).toBeInTheDocument();
  const area = screen.getByRole("region", { name: "因子自相关" });
  expect(area).toHaveTextContent("相邻评价期");
  await user.click(within(area).getByText("查看自相关明细"));
  const table = within(area).getByRole("table", { name: "自相关明细" });
  expect(table).toHaveTextContent("2026-09-18");
  expect(table).toHaveTextContent("+0.3000");
  expect(table).toHaveTextContent("相邻两期没有共同样本");
  expect(table).toHaveTextContent("首期没有可比较的前一期");
});

it("旧结果没有扩展统计时诚实空状态，改 MAD 表单不会伪造新图表", async () => {
  publish();
  const {
    extended_statistics: _statistics,
    mad_multiple: _multiple,
    ...legacy
  } = diagnosticResearch;
  results(legacy);
  const user = userEvent.setup();
  renderApp("/factors");
  expect(await screen.findByText("这次检验未生成行业 IC 和自相关。")).toBeInTheDocument();
  const form = await ready();
  await user.selectOptions(form.getByRole("combobox", { name: "离群值处理" }), "mad");
  expect(screen.queryByRole("img", { name: "行业 IC 均值" })).toBeNull();
  expect(screen.queryByRole("img", { name: "相邻评价期因子自相关" })).toBeNull();
  expect(screen.getByRole("region", { name: "检验结果" })).toHaveTextContent("不处理离群值");
});
