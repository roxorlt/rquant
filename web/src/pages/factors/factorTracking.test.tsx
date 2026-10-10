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
import { dailyCapability } from "./factorDailyFields.fixture";
import {
  anotherFactor,
  trackedFactor,
  trackingEnvelope,
  trackingGeneration,
  trackingKey,
  trackingNextGeneration,
  trackingPanel,
  trackingRequest,
  trackingResult,
  trackingSummary,
} from "./factorTracking.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));
function publish(panel = trackingPanel(), rows = [trackedFactor], generation = trackingGeneration) {
  server.use(
    http.get("*/api/v1/factors/capabilities", () =>
      HttpResponse.json({
        data: {
          ...dailyCapability,
          can_save: false,
          version: "daily_v1",
          fields: dailyCapability.fields.slice(0, 6),
        },
        serving: metaEnvelope({ generationId: generation }).serving,
      }),
    ),
    http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ generationId: generation }))),
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: rows.length ? "populated" : "empty",
          available_at: "2026-09-24T07:31:00Z",
          can_save: false,
          can_archive: true,
          definitions: rows,
        },
        serving: metaEnvelope({ generationId: generation }).serving,
      }),
    ),
    http.get("*/api/v1/factors/:factorId/tracking", ({ params }) =>
      HttpResponse.json(
        trackingEnvelope({ ...panel, factor_id: String(params.factorId) }, generation),
      ),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({
        data: {
          enabled: true,
          reason: null,
          start_date: "2026-09-01",
          end_date: "2026-09-23",
          pools: [{ selection: "all", label: "全市场", available: true, reason: null }],
        },
        serving: metaEnvelope({ generationId: generation }).serving,
      }),
    ),
  );
}
function store(
  request = trackingRequest(),
  result: Schemas["FactorTrackingOperationResult"] | null = null,
) {
  const record = {
    viewer: "tester",
    factorName: trackedFactor.name_zh,
    request,
    result,
    denied: false,
  };
  localStorage.setItem(trackingKey, JSON.stringify(record));
  return record;
}
async function openJoin() {
  await screen.findByRole("button", { name: "加入跟踪" });
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "加入跟踪" }));
  return screen.findByRole("dialog", { name: "加入因子跟踪" });
}
beforeEach(() =>
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (
        _name: string,
        _options: unknown,
        action: (lock: { name: string }) => unknown,
      ) => action({ name: "rquant.factor.command" }),
    },
  }),
);
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

it("加入确认捕获保存定义与固定意图，先存完整请求，双击只发一次，applied不假称已跟踪", async () => {
  publish();
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorTrackingRequest"];
      expect(request.headers.get("x-rquant-csrf")).toBe("1");
      expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(body);
      seen.push(body);
      return HttpResponse.json({ data: trackingResult(body), serving: metaEnvelope().serving });
    }),
  );
  renderApp("/factors");
  const parameters = await screen.findByRole("region", { name: "检验参数" });
  await userEvent.selectOptions(within(parameters).getByRole("combobox", { name: "分组数" }), "10");
  const dialog = await openJoin();
  expect(dialog).toHaveTextContent("价量动量 · 第 2 版");
  expect(seen).toHaveLength(0);
  const confirm = within(dialog).getByRole("button", { name: "确认加入" });
  fireEvent.click(confirm);
  fireEvent.click(confirm);
  await waitFor(() => expect(seen).toHaveLength(1));
  expect(Object.keys(seen[0] ?? {}).sort()).toEqual([
    "command_id",
    "expected_head",
    "expected_tracking_generation",
    "factor_id",
    "requested_at",
    "serving_generation_id",
    "tracked",
  ]);
  expect(seen[0]).toMatchObject({
    factor_id: trackedFactor.factor_id,
    tracked: true,
    expected_head: { version: 2, content_sha256: trackedFactor.content_sha256 },
  });
  await screen.findByText("已保存，等待同步。");
  expect(screen.queryByRole("button", { name: "取消跟踪" })).not.toBeInTheDocument();
});

it("原回执必须同代且跟踪代/head/布尔匹配，确认后才切换取消入口", async () => {
  const request = trackingRequest();
  const result = trackingResult(request);
  store(request, result);
  publish(
    trackingPanel({
      availability: "tracked",
      status: "waiting",
      tracked: true,
      tracking_generation: "c".repeat(32),
    }),
  );
  server.use(
    http.post("*/api/v1/factors/tracking/commands/resume", () =>
      HttpResponse.json({ data: result, serving: metaEnvelope().serving }),
    ),
  );
  const view = renderApp("/factors");
  await screen.findByText("已保存，等待同步。");
  expect(screen.queryByRole("button", { name: "取消跟踪" })).not.toBeInTheDocument();
  await within(await screen.findByRole("region", { name: "因子跟踪" })).findByText(
    "本次跟踪尚未同步，请刷新状态。",
  );
  expect(screen.getByRole("region", { name: "因子跟踪" })).not.toHaveTextContent("已跟踪");
  publish(
    trackingPanel({
      availability: "tracked",
      status: "waiting",
      tracked: true,
      tracking_generation: result.receipt?.tracking_generation,
    }),
    [trackedFactor],
    trackingNextGeneration,
  );
  act(() =>
    view.queryClient.setQueryData(
      META_QUERY_KEY,
      metaEnvelope({ generationId: trackingNextGeneration }),
    ),
  );
  await screen.findByText("已加入跟踪。");
  await userEvent.click(screen.getByRole("button", { name: "继续查看跟踪" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "取消跟踪" })).toBeEnabled());
});

it("失联及lookup404重载后保留原ID/payload，换代重试也不改写意图", async () => {
  publish();
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      seen.push((await request.json()) as Schemas["FactorTrackingRequest"]);
      return HttpResponse.error();
    }),
    http.post("*/api/v1/factors/tracking/commands/resume", async ({ request }) => {
      seen.push((await request.json()) as Schemas["FactorTrackingRequest"]);
      return new HttpResponse(null, { status: 404 });
    }),
    http.post("*/api/v1/factors/tracking/commands/retry", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorTrackingRequest"];
      seen.push(body);
      return HttpResponse.json({
        data: trackingResult(body, "uncertain"),
        serving: metaEnvelope().serving,
      });
    }),
  );
  const first = renderApp("/factors");
  await userEvent.click(within(await openJoin()).getByRole("button", { name: "确认加入" }));
  await screen.findByText("跟踪状态暂未确认，请保留本次操作。");
  first.unmount();
  publish(trackingPanel(), [trackedFactor], trackingNextGeneration);
  renderApp("/factors");
  const retry = await screen.findByRole("button", { name: "用原请求重试跟踪" });
  await waitFor(() => expect(seen).toHaveLength(2));
  await userEvent.click(retry);
  await waitFor(() => expect(seen).toHaveLength(3));
  expect(seen).toEqual([seen[0], seen[0], seen[0]]);
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(seen[0]);
});

it("可信拒绝保留原命令供查看，明确修改后才释放操作槽", async () => {
  const request = trackingRequest();
  store(request, trackingResult(request, "rejected"));
  publish();
  renderApp("/factors");
  await screen.findByText("定义已变化，请刷新后重新确认。");
  expect(localStorage.getItem(trackingKey)).not.toBeNull();
  await userEvent.click(screen.getByRole("button", { name: "重新确认跟踪" }));
  await waitFor(() => expect(localStorage.getItem(trackingKey)).toBeNull());
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
});

it("取消捕获false与当前跟踪代，保存后不提前展示已取消", async () => {
  publish(
    trackingPanel({
      availability: "tracked",
      status: "active",
      tracked: true,
      tracking_generation: "b".repeat(32),
      summary: trackingSummary,
      actual_start_date: "2026-08-27",
    }),
  );
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorTrackingRequest"];
      seen.push(body);
      return HttpResponse.json({ data: trackingResult(body), serving: metaEnvelope().serving });
    }),
  );
  renderApp("/factors");
  await userEvent.click(await screen.findByRole("button", { name: "取消跟踪" }));
  await waitFor(() => expect(seen).toHaveLength(1));
  expect(seen[0]).toMatchObject({ tracked: false, expected_tracking_generation: "b".repeat(32) });
  await screen.findByText("已保存，等待同步。");
  expect(screen.queryByText("已取消跟踪。")).not.toBeInTheDocument();
});

it("面板仅显示后台统计/实际日期/覆盖，不重算或再次翻方向；策略Tip有18:40", async () => {
  publish(
    trackingPanel({
      availability: "tracked",
      status: "active",
      tracked: true,
      tracking_generation: "b".repeat(32),
      summary: trackingSummary,
      actual_start_date: "2026-08-27",
      updated_at: "2026-09-24T07:31:00Z",
    }),
  );
  renderApp("/factors");
  const panel = await screen.findByRole("region", { name: "因子跟踪" });
  await within(panel).findByText("+1.23%");
  expect(panel).toHaveTextContent("−2.56%");
  expect(panel).toHaveTextContent("+7.89%");
  expect(panel).toHaveTextContent("0.0312");
  expect(panel).toHaveTextContent("0.0412");
  expect(panel).toHaveTextContent("0.4120");
  expect(panel).toHaveTextContent("2026-08-27");
  expect(panel).toHaveTextContent("20 / 20");
  await userEvent.tab();
  const strategy = within(panel).getByText("跟踪策略");
  act(() => strategy.focus());
  await waitFor(() => expect(screen.getByRole("tooltip")).toHaveTextContent("18:40"));
  expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
});

it("暂停旧版本保留历史统计，重新加入绑定新版head并请求新段", async () => {
  const current = { ...trackedFactor, version: 3, content_sha256: "c".repeat(64) };
  publish(
    trackingPanel({
      availability: "tracked",
      status: "paused",
      tracked: true,
      tracking_generation: "b".repeat(32),
      reason: "定义已更新，请重新加入跟踪。",
      summary: trackingSummary,
    }),
    [current],
  );
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorTrackingRequest"];
      seen.push(body);
      return HttpResponse.json({ data: trackingResult(body), serving: metaEnvelope().serving });
    }),
  );
  renderApp("/factors");
  await screen.findByRole("button", { name: "重新加入" });
  await waitFor(() => expect(screen.getByRole("button", { name: "重新加入" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "重新加入" }));
  const dialog = await screen.findByRole("dialog", { name: "加入因子跟踪" });
  expect(dialog).toHaveTextContent("第 3 版");
  await userEvent.click(within(dialog).getByRole("button", { name: "确认加入" }));
  await waitFor(() => expect(seen).toHaveLength(1));
  expect(seen[0]?.expected_head).toEqual({ version: 3, content_sha256: current.content_sha256 });
});

it("总览深链准确选择并聚焦目标面板，未知目标不选首因子", async () => {
  publish(trackingPanel(), [trackedFactor, anotherFactor]);
  const view = renderApp("/factors?factor_id=flow_factor&panel=tracking");
  const panel = await screen.findByRole("region", { name: "因子跟踪" });
  await within(panel).findByText("成交变化");
  await waitFor(() => expect(panel).toHaveFocus());
  await userEvent.click(
    within(screen.getByRole("table", { name: "因子列表" })).getByText("价量动量"),
  );
  await act(async () => view.router.navigate("/factors?factor_id=flow_factor&panel=tracking"));
  await waitFor(() =>
    expect(screen.getByRole("region", { name: "因子详情" })).toHaveTextContent("成交变化"),
  );
  view.unmount();
  const missing = renderApp("/factors?factor_id=missing_factor&panel=tracking");
  await screen.findByText("目标因子暂时不可查看，请从因子列表选择。");
  expect(screen.queryByRole("region", { name: "因子详情" })).not.toBeInTheDocument();
  missing.unmount();
  publish(trackingPanel(), []);
  renderApp("/factors?factor_id=missing_factor&panel=tracking");
  await screen.findByText("目标因子暂时不可查看，请从因子列表选择。");
});

it("存储失败不发请求，原保存/归档/运行占槽时禁用跟踪", async () => {
  publish();
  const send = vi.fn(() => HttpResponse.error());
  server.use(http.post("*/api/v1/factors/tracking/commands", send));
  const view = renderApp("/factors");
  const dialog = await openJoin();
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new Error("quota");
  });
  await userEvent.click(within(dialog).getByRole("button", { name: "确认加入" }));
  expect(send).not.toHaveBeenCalled();
  view.unmount();
  vi.restoreAllMocks();
  localStorage.setItem("rquant.factor.save-command.v1", "{}");
  renderApp("/factors");
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeDisabled());
});

it("401/403后原命令保留且禁重试，切账号不重新提交", async () => {
  publish();
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      seen.push((await request.json()) as Schemas["FactorTrackingRequest"]);
      return new HttpResponse(null, { status: 403 });
    }),
  );
  const view = renderApp("/factors");
  await userEvent.click(within(await openJoin()).getByRole("button", { name: "确认加入" }));
  await waitFor(() =>
    expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").denied).toBe(true),
  );
  act(() =>
    view.queryClient.setQueryData(
      META_QUERY_KEY,
      metaEnvelope({ generationId: trackingGeneration, viewer: "other" }),
    ),
  );
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "用原请求重试跟踪" })).toBeDisabled(),
  );
  expect(seen).toHaveLength(1);
});

it("缺表和坏投影不冒充未跟踪或提供可写入口", async () => {
  publish(
    trackingPanel({
      availability: "unavailable",
      status: "unavailable",
      can_set_tracked: false,
      definition_head: null,
      reason: "跟踪数据尚未发布。",
    }),
  );
  const view = renderApp("/factors");
  await screen.findByText("跟踪数据尚未发布。");
  expect(screen.getByRole("button", { name: "加入跟踪" })).toBeDisabled();
  view.unmount();
  publish(
    trackingPanel({
      availability: "tracked",
      status: "active",
      tracked: true,
      tracking_generation: "broken",
    }),
  );
  const broken = renderApp("/factors");
  await screen.findByText("跟踪数据暂时无法核对，请刷新后查看。");
  expect(screen.getByRole("button", { name: "加入跟踪" })).toBeDisabled();
  broken.unmount();
  publish();
  server.use(
    http.get("*/api/v1/factors/:factorId/tracking", () =>
      HttpResponse.json({
        data: {
          ...trackingPanel({
            availability: "tracked",
            tracked: true,
            tracking_generation: "b".repeat(32),
          }),
          status: ["active"],
        },
        serving: metaEnvelope({ generationId: trackingGeneration }).serving,
      }),
    ),
  );
  renderApp("/factors");
  await screen.findByText("跟踪数据暂时无法核对，请刷新后查看。");
  expect(screen.queryByRole("button", { name: "取消跟踪" })).not.toBeInTheDocument();
});

it.each(["head", "generation", "permission", "actor"] as const)(
  "确认期间%s变化不提交旧确认",
  async (change) => {
    publish();
    const send = vi.fn(() => HttpResponse.error());
    server.use(http.post("*/api/v1/factors/tracking/commands", send));
    const view = renderApp("/factors");
    const dialog = await openJoin();
    if (change === "generation") {
      publish(trackingPanel(), [trackedFactor], trackingNextGeneration);
      act(() =>
        view.queryClient.setQueryData(
          META_QUERY_KEY,
          metaEnvelope({ generationId: trackingNextGeneration }),
        ),
      );
    } else if (change === "actor") {
      act(() =>
        view.queryClient.setQueryData(
          META_QUERY_KEY,
          metaEnvelope({ generationId: trackingGeneration, viewer: "other" }),
        ),
      );
    } else if (change === "permission") {
      act(() =>
        view.queryClient.setQueryData(
          ["factors", "tracking", trackingGeneration, trackedFactor.factor_id, "tester", 0],
          trackingEnvelope(trackingPanel({ can_set_tracked: false })),
        ),
      );
    } else {
      act(() =>
        view.queryClient.setQueryData(["factors", "definitions", trackingGeneration, "tester", 0], {
          data: {
            availability: "populated",
            available_at: "2026-09-24T07:31:00Z",
            can_save: false,
            can_archive: true,
            definitions: [{ ...trackedFactor, version: 3, content_sha256: "c".repeat(64) }],
          },
          serving: metaEnvelope({ generationId: trackingGeneration }).serving,
        }),
      );
    }
    await waitFor(() =>
      expect(within(dialog).getByRole("button", { name: "确认加入" })).toBeDisabled(),
    );
    await userEvent.click(within(dialog).getByRole("button", { name: "确认加入" }));
    expect(send).not.toHaveBeenCalled();
    expect(localStorage.getItem(trackingKey)).toBeNull();
  },
);

it("获得浏览器锁后重新核对身份，等待锁期间切账号不落请求", async () => {
  publish();
  const send = vi.fn(() => HttpResponse.error());
  server.use(http.post("*/api/v1/factors/tracking/commands", send));
  const view = renderApp("/factors");
  const dialog = await openJoin();
  let unlock: (() => void) | undefined;
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (
        _name: string,
        _options: unknown,
        action: (lock: { name: string }) => unknown,
      ) => {
        await new Promise<void>((resolve) => {
          unlock = resolve;
        });
        return action({ name: "rquant.factor.command" });
      },
    },
  });
  fireEvent.click(within(dialog).getByRole("button", { name: "确认加入" }));
  await waitFor(() => expect(unlock).toBeDefined());
  act(() =>
    view.queryClient.setQueryData(
      META_QUERY_KEY,
      metaEnvelope({ generationId: trackingGeneration, viewer: "other" }),
    ),
  );
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: "确认加入" })).toBeDisabled(),
  );
  await act(async () => unlock?.());
  expect(send).not.toHaveBeenCalled();
  expect(localStorage.getItem(trackingKey)).toBeNull();
});

it("其他标签替换原操作后，旧回调不能覆盖新的完整请求", async () => {
  publish();
  let release: (() => void) | undefined;
  let original: Schemas["FactorTrackingRequest"] | undefined;
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      original = (await request.json()) as Schemas["FactorTrackingRequest"];
      await new Promise<void>((resolve) => {
        release = resolve;
      });
      return HttpResponse.json({ data: trackingResult(original), serving: metaEnvelope().serving });
    }),
  );
  renderApp("/factors");
  await userEvent.click(within(await openJoin()).getByRole("button", { name: "确认加入" }));
  await waitFor(() => expect(release).toBeDefined());
  const newer = {
    ...trackingRequest(false),
    command_id: "newer-tracking-2",
    factor_id: anotherFactor.factor_id,
  };
  const replacement = {
    viewer: "other",
    factorName: anotherFactor.name_zh,
    request: newer,
    result: null,
    denied: false,
  };
  localStorage.setItem(trackingKey, JSON.stringify(replacement));
  act(() => window.dispatchEvent(new StorageEvent("storage", { key: trackingKey })));
  await act(async () => release?.());
  await waitFor(() =>
    expect(screen.getByRole("region", { name: "本次跟踪" })).toHaveTextContent(
      anotherFactor.name_zh,
    ),
  );
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null")).toEqual(replacement);
  expect(original?.command_id).not.toBe(newer.command_id);
});

it.each(["intent", "head", "request", "segment"] as const)(
  "%s不绑定原意图的回执保留请求且不宣布已保存",
  async (difference) => {
    publish();
    let original: Schemas["FactorTrackingRequest"] | undefined;
    server.use(
      http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
        original = (await request.json()) as Schemas["FactorTrackingRequest"];
        const good = trackingResult(original);
        const bad =
          difference === "request"
            ? { ...good, original_request: { ...original, requested_at: "2026-10-01T00:00:00Z" } }
            : {
                ...good,
                receipt: {
                  ...good.receipt,
                  ...(difference === "intent"
                    ? { tracked: false }
                    : difference === "head"
                      ? { definition_head: { version: 8, content_sha256: "c".repeat(64) } }
                      : { segment_id: "c".repeat(32) }),
                },
              };
        return HttpResponse.json({ data: bad, serving: metaEnvelope().serving });
      }),
    );
    renderApp("/factors");
    await userEvent.click(within(await openJoin()).getByRole("button", { name: "确认加入" }));
    await screen.findByText("跟踪状态暂未确认，请保留本次操作。");
    expect(screen.queryByText("已保存，等待同步。")).not.toBeInTheDocument();
    expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(original);
  },
);

it("已有可信回执不被后来的其他跟踪段回执替换", async () => {
  const request = trackingRequest();
  const accepted = trackingResult(request);
  store(request, accepted);
  publish();
  server.use(
    http.post("*/api/v1/factors/tracking/commands/resume", () =>
      HttpResponse.json({
        data: {
          ...accepted,
          receipt: {
            ...accepted.receipt,
            tracking_generation: "c".repeat(32),
            segment_id: "c".repeat(32),
          },
        },
        serving: metaEnvelope().serving,
      }),
    ),
  );
  renderApp("/factors");
  await screen.findByText("跟踪状态暂未确认，请保留本次操作。");
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").result).toEqual(accepted);
});

it("损坏缓存的非布尔意图占住操作槽，禁止另造命令或删除原记录", async () => {
  publish();
  const raw = JSON.stringify({
    viewer: "tester",
    factorName: trackedFactor.name_zh,
    request: { ...trackingRequest(), tracked: "true" },
    result: null,
    denied: false,
  });
  localStorage.setItem(trackingKey, raw);
  renderApp("/factors");
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeDisabled());
  expect(localStorage.getItem(trackingKey)).toBe(raw);
  expect(screen.queryByRole("button", { name: "归档" })).not.toBeInTheDocument();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});

it("拒绝提示显示期间另一标签变为已保存，旧修改按钮不能清除未确认回执", async () => {
  const request = trackingRequest();
  store(request, trackingResult(request, "rejected"));
  publish();
  server.use(
    http.post("*/api/v1/factors/tracking/commands/resume", () =>
      HttpResponse.json({ data: trackingResult(request), serving: metaEnvelope().serving }),
    ),
  );
  renderApp("/factors");
  const correction = await screen.findByRole("button", { name: "重新确认跟踪" });
  const latest = store(request, trackingResult(request));
  await userEvent.click(correction);
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null")).toEqual(latest);
});

it("覆盖不足的缺失收益显示破折号，前端不以负均值自行判定失效", async () => {
  const partial: Schemas["FactorTrackingSummary"] = {
    ...trackingSummary,
    yesterday_ic: null,
    yesterday_long_short: null,
    week_long_short: null,
    cumulative_long_short: null,
    complete_day_count: 19,
    week_complete_day_count: 4,
    invalidated: false,
    reason: "近20日覆盖不足，暂不判断失效。",
    ic_20: { ...trackingSummary.ic_20, mean: -0.02 },
  };
  publish(
    trackingPanel({
      availability: "tracked",
      status: "active",
      tracked: true,
      tracking_generation: "b".repeat(32),
      summary: partial,
    }),
  );
  renderApp("/factors");
  const panel = await screen.findByRole("region", { name: "因子跟踪" });
  await within(panel).findByText("-0.0200");
  expect(panel.querySelectorAll("dd")).toHaveLength(6);
  expect(within(panel).getAllByText("—").length).toBeGreaterThanOrEqual(4);
  expect(panel).toHaveTextContent("19 / 20");
  expect(panel).toHaveTextContent("4 / 5");
  expect(panel).not.toHaveTextContent("跟踪已失效");
});

it("多标签的跟踪原请求即时禁止新的检验和归档", async () => {
  publish();
  const view = renderApp("/factors");
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
  expect(screen.getByRole("button", { name: "归档" })).toBeInTheDocument();
  const request = trackingRequest();
  store(request, trackingResult(request, "uncertain"));
  server.use(
    http.post("*/api/v1/factors/tracking/commands/resume", () =>
      HttpResponse.json({
        data: trackingResult(request, "uncertain"),
        serving: metaEnvelope().serving,
      }),
    ),
  );
  act(() => window.dispatchEvent(new StorageEvent("storage", { key: trackingKey })));
  await screen.findByRole("region", { name: "本次跟踪" });
  await waitFor(() =>
    expect(screen.queryByRole("button", { name: "归档" })).not.toBeInTheDocument(),
  );
  for (const button of screen.getAllByRole("button", { name: "运行检验" }))
    expect(button).toBeDisabled();
  view.unmount();
});

it("跟踪面板失联展示中文原因，不能暴露原始网络错误或加入", async () => {
  publish();
  server.use(http.get("*/api/v1/factors/:factorId/tracking", () => HttpResponse.error()));
  renderApp("/factors");
  await screen.findByText("跟踪数据暂时无法核对，请刷新后查看。");
  expect(screen.getByRole("button", { name: "加入跟踪" })).toBeDisabled();
  expect(document.querySelector("main")).not.toHaveTextContent("Failed to fetch");
});
