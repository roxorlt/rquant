import { act, renderHook } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { AIGenerateRequest, AIRequestView } from "@/api/aiAssistance";
import { metaEnvelope } from "@/test/fixtures";
import { server } from "@/test/server";
import { forgetOriginal, readOriginal, saveOriginal, useAiGeneration } from "./aiAssistanceSession";

const body: AIGenerateRequest = {
  purpose: "screen",
  request_id: "e28878d8-2d82-4c92-93ab-6c1e963f8530",
  instruction: "市值较小",
  source_kind: "replica",
  source_identity: "a".repeat(64),
  trade_date: "2026-09-24",
  include_ranking: true,
};

it("persists exact UUID/body before dispatch and isolates each viewer", () => {
  saveOriginal("alice", "screen", body);
  expect(readOriginal("alice", "screen")).toEqual(body);
  expect(readOriginal("bob", "screen")).toBeNull();
  expect(readOriginal("alice", "pool")).toBeNull();
  expect(() => saveOriginal("alice", "screen", { ...body, instruction: "不同条件" })).toThrow();
  forgetOriginal("alice", "screen");
  expect(readOriginal("alice", "screen")).toBeNull();
});

it("uses an independent frozen copy and rejects malformed stored requests", () => {
  saveOriginal("alice", "screen", body);
  const first = readOriginal("alice", "screen");
  expect(Object.isFrozen(first)).toBe(true);
  expect(first?.request_id).toBe(body.request_id);
  for (const key of Object.keys(sessionStorage))
    sessionStorage.setItem(key, '{"purpose":"screen"}');
  expect(readOriginal("alice", "screen")).toBeNull();
});

it.each([404, 503])(
  "retains an unresolved original after lookup %s across reset and reopen",
  async (status) => {
    saveOriginal("alice", "screen", body);
    const generated: unknown[] = [];
    const looked: unknown[] = [];
    server.use(
      http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
        looked.push(await request.json());
        return HttpResponse.json({ detail: "找不到原请求。" }, { status });
      }),
      http.post("*/api/v1/ai/requests", async ({ request }) => {
        generated.push(await request.json());
        const view: AIRequestView = {
          request_id: body.request_id,
          purpose: body.purpose,
          state: "completed",
          created_at: "2026-10-09T00:00:00Z",
          result: null,
        };
        return HttpResponse.json({ serving: metaEnvelope().serving, data: view });
      }),
    );
    const first = renderHook(() => useAiGeneration("alice", "screen"));
    await act(() => first.result.current.lookup());
    expect(first.result.current.absent).toBe(false);
    expect(first.result.current.original).toEqual(body);
    act(() => first.result.current.reset());
    expect(first.result.current.original).toEqual(body);
    expect(readOriginal("alice", "screen")).toEqual(body);
    await act(() => first.result.current.generate({ ...body, request_id: crypto.randomUUID() }));
    expect(generated).toEqual([]);
    expect(first.result.current.original).toEqual(body);
    first.unmount();

    const reopened = renderHook(() => useAiGeneration("alice", "screen"));
    expect(reopened.result.current.original).toEqual(body);
    expect(generated).toEqual([]);
    await act(() => reopened.result.current.lookup());
    expect(looked).toEqual([body, body]);
    if (status === 404) {
      await act(() => reopened.result.current.generate(body));
      expect(generated).toEqual([body]);
      expect(reopened.result.current.view?.state).toBe("completed");
      act(() => reopened.result.current.reset());
      expect(readOriginal("alice", "screen")).toBeNull();
    } else {
      act(() => reopened.result.current.reset());
      expect(readOriginal("alice", "screen")).toEqual(body);
      expect(generated).toEqual([]);
    }
  },
);

it.each(["not_dispatched", "completed"] as const)(
  "allows a new request only after explicit %s",
  async (state) => {
    saveOriginal("alice", "screen", body);
    server.use(
      http.post("*/api/v1/ai/requests/lookup", () => {
        const view: AIRequestView = {
          request_id: body.request_id,
          purpose: body.purpose,
          state,
          created_at: "2026-10-09T00:00:00Z",
          result: null,
        };
        return HttpResponse.json({ serving: metaEnvelope().serving, data: view });
      }),
    );
    const hook = renderHook(() => useAiGeneration("alice", "screen"));
    await act(() => hook.result.current.lookup());
    act(() => hook.result.current.reset());
    expect(hook.result.current.original).toBeNull();
    expect(readOriginal("alice", "screen")).toBeNull();
  },
);
