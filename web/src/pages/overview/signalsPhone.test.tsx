import { screen, within } from "@testing-library/react";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-21T07:00:00Z" };

function media(matches: (query: string) => boolean): typeof window.matchMedia {
  return ((query: string) => ({
    matches: matches(query),
    media: query,
    onchange: null,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    addListener: () => undefined,
    removeListener: () => undefined,
    dispatchEvent: () => false,
  })) as typeof window.matchMedia;
}

beforeEach(() => {
  vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
    const url = new URL(input instanceof Request ? input.url : String(input), "http://test");
    const path = url.pathname;
    const data = path.endsWith("/overview")
      ? {
          trade_date: "2026-10-21",
          kpis: [],
          signals: [
            {
              sequence: 1,
              at: "2026-10-21T07:44:00Z",
              code: "600519.SH",
              name: "贵州茅台",
              strategy_id: "volume-surge",
              action: "b_intent",
            },
            {
              sequence: 2,
              at: "2026-10-20T06:38:00Z",
              code: "daily-summary:2026-10-20",
              name: null,
              strategy_id: "breakout",
              action: "watch",
            },
          ],
        }
      : path.endsWith("/meta")
        ? { app: "rQuant", version: "test", role: "admin", generation: serving }
        : path.endsWith("/health")
          ? { services: [], freshness: [], layers: [] }
          : path.endsWith("/alerts")
            ? { items: [] }
            : {};
    return new Response(JSON.stringify({ data, serving }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  });
});

it("lists phone signals on two lines with a sort button and no header", async () => {
  const original = window.matchMedia;
  window.matchMedia = media((query) => query.includes("max-width: 599px"));
  try {
    renderApp("/overview");
    const list = await screen.findByRole("list", { name: "最新信号" });
    expect(screen.queryByRole("table", { name: "最新信号" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "排序" })).toBeInTheDocument();
    const items = within(list).getAllByRole("listitem");
    const today = items.find((item) => item.textContent?.includes("贵州茅台"));
    const older = items.find((item) => item.textContent?.includes("突破"));
    expect(today).toHaveTextContent("15:44 · 放量");
    expect(today).toHaveTextContent("买入观察");
    expect(within(today as HTMLElement).getByText("买入观察")).toHaveClass("up");
    expect(older).toHaveTextContent("10-20 14:38 · 突破");
    expect(older).toHaveTextContent("重点观察");
    expect(older).not.toHaveTextContent("daily-summary");
    expect(within(older as HTMLElement).getAllByText("—").length).toBeGreaterThan(0);
  } finally {
    window.matchMedia = original;
  }
});
