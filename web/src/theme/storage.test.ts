import { act, renderHook } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";
import {
  hydratePreferences,
  oneOf,
  readPreference,
  syncedPreference,
  usePreference,
  writePreference,
} from "./storage";

afterEach(() => {
  vi.unstubAllGlobals();
});

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

test("account-wide keys are synced; theme and rail stay in this browser", () => {
  expect(syncedPreference("cols.选股结果")).toBe(true);
  expect(syncedPreference("sort.告警时间线")).toBe(true);
  expect(syncedPreference("query.history")).toBe(true);
  expect(syncedPreference("pref.monitor.show")).toBe(true);
  expect(syncedPreference("theme")).toBe(false);
  expect(syncedPreference("rail")).toBe(false);
  expect(syncedPreference("screener.schemes")).toBe(false);
});

test("hydrate copies the server's settings into this browser", async () => {
  vi.stubGlobal("fetch", async (req: Request) => {
    expect(new URL(req.url).pathname).toMatch(/\/api\/v1\/ui-preferences$/);
    return json({ data: { prefs: { "cols.选股结果": "pe", theme: "dark" } } });
  });
  expect(await hydratePreferences()).toBe(1);
  expect(readPreference("cols.选股结果")).toBe("pe");
  expect(readPreference("theme")).toBeNull();
});

test("server down or slow: the browser copy is kept and nothing throws", async () => {
  writePreference("theme", "dark");
  window.localStorage.setItem("rq.cols.选股结果", "pb");
  vi.stubGlobal("fetch", async () => json({ detail: "down" }, 503));
  expect(await hydratePreferences()).toBe(-1);
  vi.stubGlobal("fetch", () => Promise.reject(new TypeError("offline")));
  expect(await hydratePreferences()).toBe(-1);
  vi.stubGlobal("fetch", () => new Promise<Response>(() => undefined));
  expect(await hydratePreferences(20)).toBe(-1);
  expect(readPreference("cols.选股结果")).toBe("pb");
});

test("writes go to this browser and, for synced keys, to the server", async () => {
  const sent: unknown[] = [];
  vi.stubGlobal("fetch", async (req: Request) => {
    sent.push({ method: req.method, body: await req.json() });
    return json({ command_id: null, status: "succeeded" });
  });
  writePreference("rail", "min");
  writePreference("query.history", "[]");
  await vi.waitFor(() => expect(sent).toHaveLength(1));
  expect(sent[0]).toEqual({ method: "PUT", body: { key: "query.history", value: "[]" } });
  expect(readPreference("rail")).toBe("min");
});

test("a failed server write is silent and the setting still applies", async () => {
  vi.stubGlobal("fetch", () => Promise.reject(new TypeError("offline")));
  const { result } = renderHook(() =>
    usePreference<"all" | "open">("monitor.show", "all", oneOf("all", "open")),
  );
  act(() => result.current[1]("open"));
  expect(result.current[0]).toBe("open");
  expect(readPreference("pref.monitor.show")).toBe('"open"');
});

test("an invalid stored value falls back to the default", () => {
  window.localStorage.setItem("rq.pref.monitor.show", '"nonsense"');
  const { result } = renderHook(() =>
    usePreference<"all" | "open">("monitor.show", "all", oneOf("all", "open")),
  );
  expect(result.current[0]).toBe("all");
});
