import { act, renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { createElement, type PropsWithChildren } from "react";
import { AppProviders } from "@/app/App";
import { nShapeParameters, parameterSet } from "@/pages/backtest/MinuteParameterControls.fixture";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import type { Schemas } from "./client";
import * as minuteApi from "./minuteBacktests";
import { META_QUERY_KEY } from "./useMeta";

function capabilityHookHarness() {
  const queryClient = testQueryClient();
  queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "researcher" }));
  function Wrapper({ children }: PropsWithChildren) {
    return createElement(AppProviders, { queryClient, children });
  }
  return { wrapper: Wrapper, queryClient };
}

it("capability request intent: defers the original GET while disabled and reads its response when enabled", async () => {
  const reads: string[] = [];
  const data = {
    available: true,
    can_run: false,
    sources: [],
    source_unavailable_count: 3,
  } satisfies Schemas["MinuteStudyCapabilitiesData"];
  const serving = metaEnvelope().serving;
  server.use(
    http.get("*/api/v1/backtests/minute-runtime/studies/capabilities", ({ request }) => {
      reads.push(new URL(request.url).pathname);
      return HttpResponse.json({ data, serving });
    }),
  );
  const { wrapper } = capabilityHookHarness();
  const current = renderHook(
    ({ enabled }: { enabled: boolean }) => minuteApi.useMinuteStudyCapabilities(enabled),
    { initialProps: { enabled: false }, wrapper },
  );
  await act(async () => undefined);
  await waitFor(() => expect(current.result.current.isFetching).toBe(false));
  expect(reads).toEqual([]);
  expect(current.result.current.data).toBeUndefined();
  current.rerender({ enabled: true });
  await waitFor(() => expect(current.result.current.data).toEqual(data));
  expect(current.result.current.serving).toEqual(serving);
  expect(reads).toHaveLength(1);
  expect(reads[0]).toMatch(/\/api\/v1\/backtests\/minute-runtime\/studies\/capabilities$/);
});

it("capability request intent: keeps the default read and isolates its response after an owner change", async () => {
  let reads = 0;
  server.use(
    http.get("*/api/v1/backtests/minute-runtime/studies/capabilities", () => {
      reads += 1;
      return HttpResponse.json({
        data: {
          available: true,
          can_run: false,
          sources: [],
          source_unavailable_count: reads,
        } satisfies Schemas["MinuteStudyCapabilitiesData"],
        serving: metaEnvelope().serving,
      });
    }),
  );
  const { wrapper, queryClient } = capabilityHookHarness();
  const current = renderHook(() => minuteApi.useMinuteStudyCapabilities(), { wrapper });
  await waitFor(() => expect(current.result.current.data?.source_unavailable_count).toBe(1));
  await act(async () => {
    queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "another-researcher" }));
  });
  await waitFor(() => expect(current.result.current.data?.source_unavailable_count).toBe(2));
  expect(reads).toBe(2);
});

it("capability request intent: preserves the original API error when the enabled GET is denied", async () => {
  let reads = 0;
  server.use(
    http.get("*/api/v1/backtests/minute-runtime/studies/capabilities", () => {
      reads += 1;
      return HttpResponse.json({ detail: "role unavailable" }, { status: 403 });
    }),
  );
  const { wrapper } = capabilityHookHarness();
  const current = renderHook(() => minuteApi.useMinuteStudyCapabilities(true), { wrapper });
  await waitFor(() => expect(current.result.current.error).not.toBeNull());
  expect(current.result.current.error).toMatchObject({
    name: "ApiError",
    status: 403,
    message: "分钟回测暂时无法加载，请稍后重试。",
  });
  expect(current.result.current.data).toBeUndefined();
  expect(reads).toBe(1);
});

// Synthetic public DTOs exercise the browser boundary; no source or worker authority is claimed.
const request: Schemas["MinuteStudyCreateRequest"] = {
  command_id: "00000000-0000-4000-8000-000000000137",
  requested_at: "2026-10-07T00:00:00Z",
  source_key: "synthetic.ui.study.facts",
  source_version: 7,
  full_input_hash: "7".repeat(64),
  parameters: parameterSet({ ...nShapeParameters, max_hold_days: 13 }),
  protocol: {
    train_range: { start_date: "2026-01-05", end_date: "2026-02-27" },
    validation_range: { start_date: "2026-03-02", end_date: "2026-04-30" },
    frozen_outer_test_range: { start_date: "2026-05-04", end_date: "2026-08-03" },
  },
  settings: [{ score_profile: "synthetic.training-score", top_n: 7, min_trades: 13 }],
  random_seed: 137,
  deadline: "2026-10-08T00:00:00Z",
  mode: "grid",
  search: {
    base: parameterSet({ ...nShapeParameters, max_hold_days: 13 }),
    axes: [
      { path: "max_hold_days", values: [3, 7, 13] },
      { path: "volume_profile.enabled", values: [false, true] },
      {
        path: "volume_profile.lookback_days",
        values: [
          [1, 3],
          [5, 10],
        ],
      },
    ],
    mode: "grid",
    seed: 137,
    requested_trials: null,
  },
  walk_forward: null,
};

it.each(["grid", "random", "ablation", "walk_forward"] as const)(
  "restores the complete original %s research body without creating a UUID or replacing non-default values",
  (mode) => {
    expect(minuteApi.restoreMinuteStudyRequest).toBeTypeOf("function");
    if (!request.search) throw Error("Missing complete search fixture");
    const body: Schemas["MinuteStudyCreateRequest"] = {
      ...request,
      mode,
      search:
        mode === "grid" || mode === "random"
          ? { ...request.search, mode, requested_trials: mode === "random" ? 2 : null }
          : null,
      walk_forward:
        mode === "walk_forward"
          ? { fold_count: 4, min_training_dates: 31, validation_date_count: 7 }
          : null,
    };
    const encoded = JSON.stringify(body);
    expect(JSON.stringify(minuteApi.restoreMinuteStudyRequest(encoded))).toBe(encoded);
  },
);

it("refuses malformed, private and oversized saved requests without filling their missing fields", () => {
  expect(minuteApi.restoreMinuteStudyRequest).toBeTypeOf("function");
  for (const body of [
    { ...request, command_id: "new-uuid" },
    { ...request, actor_id: "another-owner" },
    { ...request, marker: "/private/original/effect" },
    { ...request, settings: [] },
    { ...request, random_seed: -1 },
    {
      ...request,
      search: { ...request.search, axes: [{ path: "max_hold_days", values: ["13"] }] },
    },
    {
      ...request,
      walk_forward: { fold_count: 4, min_training_dates: 31, validation_date_count: 7 },
    },
    {
      ...request,
      settings: Array.from({ length: 100 }, () => ({
        score_profile: "中".repeat(100),
        top_n: 3,
        min_trades: 8,
      })),
    },
  ])
    expect(minuteApi.restoreMinuteStudyRequest(JSON.stringify(body))).toBeNull();
  expect(minuteApi.restoreMinuteStudyRequest("{")).toBeNull();
});

it("uses the original CSRF and timeout when a lost submit response is retried with identical bytes", async () => {
  expect(minuteApi.submitMinuteStudy).toBeTypeOf("function");
  const bodies: string[] = [];
  const timeout = vi.spyOn(AbortSignal, "timeout");
  server.use(
    http.post("*/api/v1/backtests/minute-runtime/studies", async ({ request: original }) => {
      bodies.push(await original.text());
      expect(original.headers.get("X-Rquant-Csrf")).toBe("1");
      if (bodies.length === 1) return HttpResponse.error();
      return HttpResponse.json({
        command_id: request.command_id,
        status: "pending",
        plan_id: null,
        jobs: [],
        unavailable_reasons: [],
        message: "研究正在准备。",
      } satisfies Schemas["MinuteStudyCommandReceipt"]);
    }),
  );
  await expect(minuteApi.submitMinuteStudy(request)).rejects.toThrow("原请求");
  const restored = minuteApi.restoreMinuteStudyRequest(JSON.stringify(request));
  expect(restored).not.toBeNull();
  if (restored === null) throw Error("Original body was not restored");
  expect(await minuteApi.submitMinuteStudy(restored)).toMatchObject({
    command_id: request.command_id,
    status: "pending",
  });
  expect(bodies).toEqual([JSON.stringify(request), JSON.stringify(request)]);
  expect(timeout).toHaveBeenCalledWith(12_000);
  timeout.mockRestore();
});

it("keeps a conflict receipt and refuses a foreign parent receipt or denied permission", async () => {
  expect(minuteApi.submitMinuteStudy).toBeTypeOf("function");
  const receipt: Schemas["MinuteStudyCommandReceipt"] = {
    command_id: request.command_id,
    status: "conflict",
    plan_id: null,
    jobs: [],
    unavailable_reasons: [],
    message: "原请求内容不同。",
  };
  server.use(
    http.post("*/api/v1/backtests/minute-runtime/studies", () =>
      HttpResponse.json(receipt, { status: 409 }),
    ),
  );
  expect(await minuteApi.submitMinuteStudy(request)).toEqual(receipt);
  server.use(
    http.post("*/api/v1/backtests/minute-runtime/studies", () =>
      HttpResponse.json({ ...receipt, command_id: "00000000-0000-4000-8000-000000000138" }),
    ),
  );
  await expect(minuteApi.submitMinuteStudy(request)).rejects.toThrow("待确认");
  server.use(
    http.post("*/api/v1/backtests/minute-runtime/studies", () =>
      HttpResponse.json({ detail: "role unavailable" }, { status: 403 }),
    ),
  );
  await expect(minuteApi.submitMinuteStudy(request)).rejects.toThrow("当前账号");
});
