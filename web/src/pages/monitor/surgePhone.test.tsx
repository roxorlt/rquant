import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "@/test/render";
import { surgeSubtitle } from "./surgeCopy";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-21T07:00:00Z" };
const RULES = {
  ratio_min: 8,
  amount_min: 1e8,
  acc5_min: 10,
  win_amount_min: 5e7,
  acc_ratio_min: 3,
  start_m: 5,
  cooldown_s: 900,
};

const ITEMS = [
  {
    code: "600519.SH",
    name: "贵州茅台",
    kind: "ratio",
    time: "14:55",
    ratio: 8.2,
    acc5: 3,
    pct: 0.012,
    amount: 2e8,
  },
  {
    code: "000001.SZ",
    name: "平安银行",
    kind: "accel",
    time: "14:11",
    ratio: 4.5,
    acc5: 11,
    pct: -0.006,
    amount: 8e7,
  },
];

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

function stub(): void {
  vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const key = url.pathname.split("/api/v1/")[1]?.split("/")[0] ?? "";
    const data =
      key === "surge"
        ? { date: "2026-10-21", items: ITEMS, rules: RULES }
        : key === "alerts"
          ? { items: [] }
          : key === "alert-rules"
            ? { rules: [] }
            : key === "meta"
              ? { app: "rQuant", version: "test", role: "admin", generation: serving }
              : key === "health"
                ? { services: [], freshness: [], layers: [] }
                : key === "operations"
                  ? { items: [] }
                  : null;
    if (!data) return new Response("missing", { status: 404 });
    return new Response(JSON.stringify({ data, serving }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  });
}

beforeEach(() => {
  stub();
});

it("uses a two-line surge list on a phone and opens the title tip", async () => {
  const original = window.matchMedia;
  window.matchMedia = media(
    (query) => query.includes("max-width: 599px") || query.includes("hover: none"),
  );
  try {
    const user = userEvent.setup();
    renderApp("/monitor");
    const list = await screen.findByRole("list", { name: "量能异动" });
    expect(screen.queryByRole("table", { name: "量能异动" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "量比" })).not.toBeInTheDocument();
    expect(screen.queryByText(/\d+ 行/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "量能异动规则" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^列/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "导出 CSV" })).toBeInTheDocument();
    const first = within(list).getAllByRole("listitem")[0];
    expect(first).toHaveTextContent("贵州茅台");
    expect(first).toHaveTextContent("600519.SH");
    expect(first).toHaveTextContent("14:55 · 量比");
    expect(first?.querySelector(".v")).toHaveTextContent("8.2");
    const second = within(list).getAllByRole("listitem")[1];
    expect(second).toHaveTextContent("14:11 · 加速");
    const title = screen.getByRole("heading", { name: /量能异动/ });
    expect(within(title).getByText("量能异动")).toHaveClass("has-tip");
    await user.click(within(title).getByText("量能异动"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(surgeSubtitle(RULES));
    expect(
      screen.queryByText(surgeSubtitle(RULES).slice(0, 8), { selector: ".sub" }),
    ).not.toBeInTheDocument();
  } finally {
    window.matchMedia = original;
  }
});

it("keeps the surge table and the ? on a wide screen", async () => {
  renderApp("/monitor");
  const table = await screen.findByRole("table", { name: "量能异动" });
  expect(screen.queryByRole("list", { name: "量能异动" })).not.toBeInTheDocument();
  expect(within(table).getByRole("columnheader", { name: "量比" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "量能异动规则" })).toBeInTheDocument();
  expect(screen.getByText(/今天的成交量比昨天同一时刻放大很多时提醒/)).toBeInTheDocument();
  expect(screen.getByText("2 行")).toBeInTheDocument();
});
