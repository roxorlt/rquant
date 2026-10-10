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
import { diagnosticAvailability } from "./factorDiagnostics.fixture";
import { storedTrackingCapability, storedTrackingFactor } from "./factorStoredTracking.fixture";
import {
  trackingGeneration,
  trackingKey,
  trackingNextGeneration,
  trackingPanel,
  trackingResult,
} from "./factorTracking.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

function publish(
  capability: Schemas["FactorCapabilitiesData"] = storedTrackingCapability,
  panel = trackingPanel(),
  rows = [storedTrackingFactor],
  generation = trackingGeneration,
) {
  const metadata = metaEnvelope({ generationId: generation });
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(metadata)),
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: metadata.serving.built_at,
          definitions: rows,
          can_save: true,
          can_archive: true,
        },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/capabilities", () =>
      HttpResponse.json({ data: capability, serving: metadata.serving }),
    ),
    http.get("*/api/v1/factors/:factorId/tracking", () =>
      HttpResponse.json({ data: panel, serving: metadata.serving }),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({ data: diagnosticAvailability, serving: metadata.serving }),
    ),
  );
}

async function openJoin() {
  await screen.findByRole("button", { name: "加入跟踪" });
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "加入跟踪" }));
  return screen.findByRole("dialog", { name: "加入因子跟踪" });
}

function shrink() {
  return {
    ...storedTrackingCapability,
    version: "daily_v1" as const,
    fields: storedTrackingCapability.fields.slice(0, 6),
  };
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

it("可信库存能力允许加入，固定策略与保存head独立于左栏，持久完整请求且applied等待同步", async () => {
  publish();
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorTrackingRequest"];
      expect(request.headers.get("x-rquant-csrf")).toBe("1");
      expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(body);
      seen.push(body);
      return HttpResponse.json({
        data: trackingResult(body),
        serving: metaEnvelope({ generationId: trackingGeneration }).serving,
      });
    }),
  );
  renderApp("/factors");
  const params = await screen.findByRole("region", { name: "检验参数" });
  await userEvent.selectOptions(within(params).getByRole("combobox", { name: "分组数" }), "10");
  const dialog = await openJoin();
  expect(dialog).toHaveTextContent("库存价量 · 第 2 版");
  expect(dialog).toHaveTextContent("全市场 · 每日 · 5组 · RankIC · 无运行后中性化");
  const confirm = within(dialog).getByRole("button", { name: "确认加入" });
  fireEvent.click(confirm);
  fireEvent.click(confirm);
  await waitFor(() => expect(seen).toHaveLength(1));
  expect(seen[0]).toMatchObject({
    tracked: true,
    factor_id: storedTrackingFactor.factor_id,
    serving_generation_id: trackingGeneration,
    expected_head: { version: 2, content_sha256: storedTrackingFactor.content_sha256 },
  });
  expect(Object.keys(seen[0] ?? {}).sort()).toEqual([
    "command_id",
    "expected_head",
    "expected_tracking_generation",
    "factor_id",
    "requested_at",
    "serving_generation_id",
    "tracked",
  ]);
  await screen.findByText("已保存，等待同步。");
  expect(screen.queryByText("已加入跟踪。")).toBeNull();
  expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
});

it.each(["能力缩减", "明确不可用"] as const)("确认后%s不能发新命令或改写定义", async (state) => {
  publish();
  const app = renderApp("/factors");
  const dialog = await openJoin();
  act(() =>
    app.queryClient.setQueryData(["factors", "capabilities", trackingGeneration, "tester", 0], {
      data: state === "能力缩减" ? shrink() : dailyCapability,
      serving: metaEnvelope({ generationId: trackingGeneration }).serving,
    }),
  );
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: "确认加入" })).toBeDisabled(),
  );
  expect(localStorage.getItem(trackingKey)).toBeNull();
  expect(screen.getByText(storedTrackingFactor.expression, { exact: true })).toBeInTheDocument();
});

it.each(["来源缺失", "错代", "接口错误"] as const)(
  "库存字段%s不开放新加入但已有取消仍可用",
  async (state) => {
    const panel = trackingPanel({
      tracked: true,
      availability: "tracked",
      status: "active",
      tracking_generation: "8".repeat(32),
    });
    publish(shrink(), panel);
    if (state === "错代")
      server.use(
        http.get("*/api/v1/factors/capabilities", () =>
          HttpResponse.json({
            data: storedTrackingCapability,
            serving: metaEnvelope({ generationId: trackingNextGeneration }).serving,
          }),
        ),
      );
    if (state === "接口错误")
      server.use(
        http.get("*/api/v1/factors/capabilities", () => new HttpResponse(null, { status: 503 })),
      );
    const seen: Schemas["FactorTrackingRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorTrackingRequest"];
        seen.push(body);
        return HttpResponse.json({
          data: trackingResult(body),
          serving: metaEnvelope({ generationId: trackingGeneration }).serving,
        });
      }),
    );
    renderApp("/factors");
    await screen.findByRole("button", { name: "取消跟踪" });
    await waitFor(() => expect(screen.getByRole("button", { name: "取消跟踪" })).toBeEnabled());
    await userEvent.click(screen.getByRole("button", { name: "取消跟踪" }));
    await waitFor(() => expect(seen).toHaveLength(1));
    expect(seen[0]).toMatchObject({
      tracked: false,
      expected_tracking_generation: "8".repeat(32),
      expected_head: { version: 2, content_sha256: storedTrackingFactor.content_sha256 },
    });
    expect(screen.queryByText("已取消跟踪。")).toBeNull();
  },
);

it("失联重载、来源消失与目录换版仍用完整原因子请求续查和重试，旧head回执才能确认", async () => {
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
        data: trackingResult(body),
        serving: metaEnvelope({ generationId: trackingNextGeneration }).serving,
      });
    }),
  );
  const first = renderApp("/factors");
  await userEvent.click(within(await openJoin()).getByRole("button", { name: "确认加入" }));
  await screen.findByText("跟踪状态暂未确认，请保留本次操作。");
  first.unmount();
  const updated = { ...storedTrackingFactor, version: 3, content_sha256: "c".repeat(64) };
  publish(shrink(), trackingPanel(), [updated], trackingNextGeneration);
  const second = renderApp("/factors");
  await screen.findByRole("button", { name: "用原请求重试跟踪" });
  await waitFor(() => expect(seen).toHaveLength(2));
  await userEvent.click(screen.getByRole("button", { name: "用原请求重试跟踪" }));
  await screen.findByText("已保存，等待同步。");
  expect(seen).toEqual([seen[0], seen[0], seen[0]]);
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(seen[0]);
  publish(
    shrink(),
    trackingPanel({
      tracked: true,
      availability: "tracked",
      status: "waiting",
      tracking_generation: "b".repeat(32),
    }),
    [updated],
    "d".repeat(64),
  );
  act(() =>
    second.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ generationId: "d".repeat(64) })),
  );
  await screen.findByText("已加入跟踪。");
  expect(screen.getByRole("region", { name: "本次跟踪" })).toHaveTextContent("第 2 版");
});

it("账号变化禁续查和重试，原库存请求保留不交给新账号", async () => {
  publish();
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      seen.push((await request.json()) as Schemas["FactorTrackingRequest"]);
      return HttpResponse.error();
    }),
  );
  const app = renderApp("/factors");
  await userEvent.click(within(await openJoin()).getByRole("button", { name: "确认加入" }));
  await screen.findByText("跟踪状态暂未确认，请保留本次操作。");
  const original = JSON.parse(localStorage.getItem(trackingKey) ?? "null");
  server.use(
    http.get("*/api/v1/meta", () =>
      HttpResponse.json(metaEnvelope({ generationId: trackingGeneration, viewer: "other" })),
    ),
  );
  act(() =>
    app.queryClient.setQueryData(
      META_QUERY_KEY,
      metaEnvelope({ generationId: trackingGeneration, viewer: "other" }),
    ),
  );
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "用原请求重试跟踪" })).toBeDisabled(),
  );
  expect(screen.getByRole("button", { name: "刷新跟踪状态" })).toBeDisabled();
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null")).toEqual(original);
  expect(seen).toHaveLength(1);
});
