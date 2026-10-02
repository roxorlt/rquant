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
const modes: Schemas["FactorRunNeutralizationOption"][] = [
  { neutralization: "none", label: "无", available: true, reason: null },
  { neutralization: "industry", label: "行业", available: true, reason: null },
  { neutralization: "industry_size", label: "行业 + 市值", available: true, reason: null },
];
const availability: Schemas["FactorRunAvailability"] = {
  enabled: true,
  reason: null,
  start_date: "2026-09-01",
  end_date: "2026-09-23",
  pools: [{ selection: "all", label: "全市场（沪深非 ST）", available: true, reason: null }],
  neutralizations: modes,
};

function publish(options = availability, id = generation, viewer = "tester"): void {
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
          definitions: [factor],
        },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({ data: options, serving: metadata.serving }),
    ),
  );
}

function request(
  mode: Schemas["FactorRunParameters"]["neutralization"],
): Schemas["FactorRunRequest"] {
  return {
    command_id: "88888888-8888-4888-8888-888888888888",
    requested_at: "2026-10-02T00:00:00Z",
    serving_generation_id: generation,
    parameters: {
      factor_id: factor.factor_id,
      expected_head: { version: factor.version, content_sha256: factor.content_sha256 },
      selection: "all",
      start_date: "2026-09-01",
      end_date: "2026-09-23",
      holding_sessions: 5,
      group_count: 5,
      ic_method: "rank",
      neutralization: mode,
    },
  };
}

function submitted(original: Schemas["FactorRunRequest"], id = generation) {
  const result: Schemas["FactorRunOperationResult"] = {
    original_request: original,
    status: "submitted",
    reason: null,
    job_id: "b".repeat(32),
    spec_sha256: "c".repeat(64),
  };
  return { data: result, serving: metaEnvelope({ generationId: id }).serving };
}

async function ready() {
  const panel = await screen.findByRole("region", { name: "检验参数" });
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
  );
  return within(panel);
}

beforeEach(() => {
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (_name: string, _options: unknown, action: () => unknown) => action(),
    },
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

it.each(modes.filter((option) => option.neutralization !== "none"))(
  "$label 由真实表单确认，失联/改参/换代/来源关闭/重载仍恢复完整原请求与任务",
  async ({ neutralization, label }) => {
    publish();
    const sent: Schemas["FactorRunRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        sent.push(body);
        expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(body);
        return HttpResponse.error();
      }),
      http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        sent.push(body);
        return sent.length === 2
          ? HttpResponse.error()
          : HttpResponse.json(submitted(body, nextGeneration));
      }),
      http.post("*/api/v1/factors/runs/retry", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        sent.push(body);
        return HttpResponse.json(submitted(body, nextGeneration));
      }),
    );
    const user = userEvent.setup();
    let view = renderApp("/factors");
    const panel = await ready();
    await user.selectOptions(panel.getByRole("combobox", { name: "中性化" }), neutralization);
    expect(panel.getByRole("combobox", { name: "中性化" })).toHaveValue(neutralization);
    await user.click(panel.getByRole("button", { name: "运行检验" }));
    const dialog = await screen.findByRole("dialog", { name: "运行因子检验" });
    expect(dialog).toHaveTextContent(`${label}中性化`);
    expect(sent).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    const original = sent[0];
    if (original === undefined) throw new Error("缺少原提交");
    expect(original.parameters.neutralization).toBe(neutralization);
    expect(readRun()?.request).toEqual(original);
    expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent(`${label}中性化`);
    await user.selectOptions(panel.getByRole("combobox", { name: "中性化" }), "none");
    expect(readRun()?.request.parameters.neutralization).toBe(neutralization);
    publish(
      {
        ...availability,
        neutralizations: modes.map((option) => ({
          ...option,
          available: option.neutralization === "none",
          reason: option.neutralization === "none" ? null : "缺少相应历史数据",
        })),
      },
      nextGeneration,
    );
    view.unmount();
    view = renderApp("/factors");
    await waitFor(() => expect(sent).toHaveLength(2));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled(),
    );
    expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent(`${label}中性化`);
    await user.click(screen.getByRole("button", { name: "用原请求重试检验" }));
    await screen.findByText("已提交，等待更新。");
    expect(screen.queryByText("检验完成。")).toBeNull();
    view.unmount();
    view = renderApp("/factors");
    await waitFor(() => expect(sent).toHaveLength(4));
    expect(sent).toEqual([original, original, original, original]);
    expect(readRun()?.result?.job_id).toBe("b".repeat(32));
    expect(readRun()?.request.parameters.neutralization).toBe(neutralization);
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
    expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
  },
);

it("已打开确认框的行业选项失效后不提交，也不改回无", async () => {
  publish();
  let posts = 0;
  server.use(
    http.post("*/api/v1/factors/runs", () => {
      posts += 1;
      return HttpResponse.error();
    }),
  );
  const user = userEvent.setup();
  const view = renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "中性化" }), "industry");
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  const dialog = await screen.findByRole("dialog", { name: "运行因子检验" });
  publish({
    ...availability,
    neutralizations: modes.map((option) => ({
      ...option,
      available: option.neutralization !== "industry",
      reason: "缺少行业记录",
    })),
  });
  await act(async () => {
    await view.queryClient.invalidateQueries({
      predicate: (query) => query.queryKey[1] === "run-availability",
    });
  });
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: "确认运行" })).toBeDisabled(),
  );
  expect(panel.getByRole("combobox", { name: "中性化" })).toHaveValue("industry");
  expect(dialog).toHaveTextContent("行业中性化");
  await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
  expect(posts).toBe(0);
  expect(localStorage.getItem(RUN_OPERATION_KEY)).toBeNull();
});

it("确认后改选另一中性化方式须重新确认，捕获的原参数保持不变", async () => {
  publish();
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "中性化" }), "industry");
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  const dialog = await screen.findByRole("dialog", { name: "运行因子检验" });
  fireEvent.change(panel.getByRole("combobox", { name: "中性化" }), {
    target: { value: "industry_size" },
  });
  expect(within(dialog).getByRole("button", { name: "确认运行" })).toBeDisabled();
  expect(dialog).toHaveTextContent("行业中性化");
  expect(localStorage.getItem(RUN_OPERATION_KEY)).toBeNull();
});

it("缺来源选项给API中文原因与键盘Tip，旧无字段仅无可提交", async () => {
  publish({
    ...availability,
    neutralizations: [
      { neutralization: "none", label: "无", available: true, reason: null },
      { neutralization: "industry", label: "行业", available: false, reason: "缺少历史行业" },
      {
        neutralization: "industry_size",
        label: "行业 + 市值",
        available: false,
        reason: "缺少历史市值",
      },
    ],
  });
  const view = renderApp("/factors");
  const panel = await ready();
  expect(panel.getByRole("option", { name: "行业（暂不可用）" })).toBeDisabled();
  const tip = panel.getByText("中性化说明").closest<HTMLElement>("[tabindex]");
  act(() => tip?.focus());
  expect(await screen.findByRole("tooltip")).toHaveTextContent("缺少历史行业");
  expect(screen.getByRole("tooltip")).toHaveTextContent("缺少历史市值");
  act(() => tip?.blur());
  view.unmount();
  const { neutralizations: _unused, ...legacy } = availability;
  publish(legacy);
  renderApp("/factors");
  const old = await ready();
  expect(old.getByRole("combobox", { name: "中性化" })).toHaveValue("none");
  expect(old.getByRole("option", { name: "行业（暂不可用）" })).toBeDisabled();
  expect(old.getByRole("option", { name: "行业 + 市值（暂不可用）" })).toBeDisabled();
});

it.each([null, undefined])(
  "已保存组合模式遇旧可用性 %s 不改回无、不开放新提交",
  async (neutralizations) => {
    const original = request("industry_size");
    const { factor_id: _factor, expected_head: _head, ...draft } = original.parameters;
    localStorage.setItem("rquant.factor.run-draft.v1", JSON.stringify(draft));
    publish({ ...availability, neutralizations });
    renderApp("/factors");
    const panel = await screen.findByRole("region", { name: "检验参数" });
    await screen.findByRole("combobox", { name: "中性化" });
    await waitFor(() =>
      expect(within(panel).getByRole("combobox", { name: "中性化" })).toHaveValue("industry_size"),
    );
    expect(within(panel).getByRole("button", { name: "运行检验" })).toBeDisabled();
    expect(
      JSON.parse(localStorage.getItem("rquant.factor.run-draft.v1") ?? "null").neutralization,
    ).toBe("industry_size");
  },
);

it.each(["industry", "industry_size"] as const)(
  "%s 原请求可严格恢复，未知enum与异模式回执拒绝",
  (mode) => {
    const original = request(mode);
    expect(validRunRequest(original)).toBe(true);
    localStorage.setItem(
      RUN_OPERATION_KEY,
      JSON.stringify({
        viewer: "tester",
        request: original,
        factorName: "价量动量",
        poolLabel: "全市场",
        result: null,
        denied: false,
      }),
    );
    expect(readRun()?.request).toEqual(original);
    const different = {
      ...original,
      parameters: { ...original.parameters, neutralization: "none" as const },
    };
    expect(sameRunRequest(original, different)).toBe(false);
    expect(validRunResult(submitted(different).data, original)).toBe(false);
    const unknown = {
      ...original,
      parameters: { ...original.parameters, neutralization: "custom" },
    };
    expect(validRunRequest(unknown)).toBe(false);
  },
);

it("旧无模式原操作字节不变，未知模式原操作保留并阻止另建", async () => {
  const original = request("none");
  const record = JSON.stringify({
    viewer: "tester",
    request: original,
    factorName: "价量动量",
    poolLabel: "全市场",
    result: null,
    denied: false,
  });
  localStorage.setItem(RUN_OPERATION_KEY, record);
  expect(readRun()?.request).toEqual(original);
  expect(localStorage.getItem(RUN_OPERATION_KEY)).toBe(record);
  localStorage.setItem(
    RUN_OPERATION_KEY,
    JSON.stringify({
      ...JSON.parse(record),
      request: { ...original, parameters: { ...original.parameters, neutralization: "custom" } },
    }),
  );
  publish();
  renderApp("/factors");
  expect(
    await screen.findByText("本机有未能恢复的检验操作，请保留浏览器记录后重新加载。"),
  ).toBeInTheDocument();
  expect(readRun()).toBeNull();
  expect(localStorage.getItem(RUN_OPERATION_KEY)).not.toBeNull();
});

it.each([
  { selected: "industry", receipt: "industry_size" },
  { selected: "industry_size", receipt: "none" },
] as const)(
  "$selected 提交拒绝异模式 $receipt 回执，原操作不被替换",
  async ({ selected, receipt }) => {
    publish();
    let original: Schemas["FactorRunRequest"] | undefined;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        original = (await request.json()) as Schemas["FactorRunRequest"];
        return HttpResponse.json(
          submitted({
            ...original,
            parameters: { ...original.parameters, neutralization: receipt },
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/factors");
    const panel = await ready();
    await user.selectOptions(panel.getByRole("combobox", { name: "中性化" }), selected);
    await user.click(panel.getByRole("button", { name: "运行检验" }));
    await user.click(screen.getByRole("button", { name: "确认运行" }));
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    expect(original?.parameters.neutralization).toBe(selected);
    expect(readRun()?.request).toEqual(original);
    expect(readRun()?.result).toBeNull();
    expect(screen.queryByText("已提交，等待更新。")).toBeNull();
    expect(screen.queryByText("检验完成。")).toBeNull();
  },
);

it("另一个标签的组合操作保留原模式，晚到行业回执不能覆盖它", async () => {
  publish();
  let release = () => {};
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  let original: Schemas["FactorRunRequest"] | undefined;
  server.use(
    http.post("*/api/v1/factors/runs", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorRunRequest"];
      original = body;
      await pending;
      return HttpResponse.json(submitted(body));
    }),
  );
  const user = userEvent.setup();
  renderApp("/factors");
  const panel = await ready();
  await user.selectOptions(panel.getByRole("combobox", { name: "中性化" }), "industry");
  await user.click(panel.getByRole("button", { name: "运行检验" }));
  await user.click(screen.getByRole("button", { name: "确认运行" }));
  await waitFor(() => expect(original?.parameters.neutralization).toBe("industry"));
  const other = request("industry_size");
  const rejected: Schemas["FactorRunOperationResult"] = {
    original_request: other,
    status: "rejected",
    reason: "开始日期前的历史数据不足，请调整开始日期。",
    job_id: null,
    spec_sha256: null,
  };
  const replacement = {
    viewer: "tester",
    request: other,
    factorName: "另一次检验",
    poolLabel: "全市场",
    neutralizationLabel: "行业 + 市值",
    result: rejected,
    denied: false,
  };
  act(() => {
    localStorage.setItem(RUN_OPERATION_KEY, JSON.stringify(replacement));
    window.dispatchEvent(new StorageEvent("storage", { key: RUN_OPERATION_KEY }));
  });
  expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent("行业 + 市值中性化");
  for (const button of screen.getAllByRole("button", { name: "运行检验" }))
    expect(button).toBeDisabled();
  release();
  await waitFor(() => expect(screen.getByRole("button", { name: "修改检验参数" })).toBeEnabled());
  expect(readRun()).toEqual(replacement);
  expect(screen.queryByText("检验完成。")).toBeNull();
});

it("账号改变时原组合操作只读，晚回执不能替换原请求或完成", async () => {
  publish();
  const original = request("industry_size");
  localStorage.setItem(
    RUN_OPERATION_KEY,
    JSON.stringify({
      viewer: "tester",
      request: original,
      factorName: "价量动量",
      poolLabel: "全市场",
      result: null,
      denied: false,
    }),
  );
  let release: (() => void) | undefined;
  let entered = false;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  server.use(
    http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorRunRequest"];
      expect(body).toEqual(original);
      entered = true;
      await pending;
      return HttpResponse.json(submitted(body));
    }),
  );
  const view = renderApp("/factors");
  await screen.findByRole("region", { name: "本次检验" });
  await waitFor(() => expect(entered).toBe(true));
  expect(screen.getByText("等待检验。")).toBeInTheDocument();
  await act(async () => {
    view.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" }));
  });
  await screen.findAllByText("请切回提交本次检验的账号继续查看。");
  release?.();
  await screen.findByText("检验结果暂未确认，请保留本次操作。");
  await waitFor(() => expect(screen.getByRole("button", { name: "刷新检验状态" })).toBeDisabled());
  expect(readRun()?.request).toEqual(original);
  expect(readRun()?.result).toBeNull();
  expect(screen.queryByText("检验完成。")).toBeNull();
});
