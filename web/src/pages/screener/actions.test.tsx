import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { renderApp } from "@/test/render";
import { resetRememberedWatch } from "./index";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

function row(code: string, name: string) {
  return {
    trade_date: "2026-10-20",
    code,
    name,
    preset: "breakout",
    close: 10,
    pct_chg: 1.2,
    industry: "材料",
  };
}

const scheme = {
  name: "放量突破",
  filters: [{ field: "pct_chg", op: ">=", value: 3 }],
  ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
  saved_at: "2026-10-20T07:00:00Z",
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  resetRememberedWatch();
  vi.unstubAllGlobals();
});

function stub(opts: { failWatch?: boolean } = {}) {
  const puts: unknown[] = [];
  const schemes = [scheme];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input), "http://localhost");
      const method = (
        init?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
      const path = url.pathname;
      if (method === "POST" && path.endsWith("/watchlist")) {
        if (opts.failWatch) return json({ detail: "暂时不能写" }, 503);
        return json({ command_id: "w", status: "succeeded" });
      }
      if (method === "DELETE" && path.endsWith("/screener-schemes")) {
        return json({ command_id: "d", status: "succeeded" });
      }
      if (method === "PUT" && path.endsWith("/screener-schemes")) {
        const raw =
          typeof init?.body === "string"
            ? init.body
            : input instanceof Request
              ? await input.clone().text()
              : "{}";
        puts.push(JSON.parse(raw || "{}"));
        return json({ command_id: "s", status: "succeeded" });
      }
      const key = path.split("/api/v1/")[1]?.split("/")[0] ?? "";
      const data =
        key === "screen"
          ? {
              trade_date: "2026-10-20",
              presets: ["breakout"],
              rows: [row("XQ0001.BJ", "星河股份"), row("XQ0003.BJ", "青田材料")],
            }
          : key === "screener-schemes"
            ? { schemes }
            : key === "conditions"
              ? { runs: [] }
              : key === "pools"
                ? { trade_date: "2026-10-20", pools: [] }
                : key === "meta"
                  ? { app: "rQuant", version: "t", role: "admin", generation: serving }
                  : {};
      return json({ data, serving });
    }),
  );
  return { puts };
}

it("keeps 已在自选 after leaving the page until a full reload", async () => {
  stub();
  const first = renderApp("/screener");
  fireEvent.click((await screen.findAllByRole("button", { name: "加自选" }))[0] as HTMLElement);
  expect(await screen.findByText("✓ 已在自选")).toBeInTheDocument();
  first.unmount();
  renderApp("/screener");
  expect(await screen.findByText("✓ 已在自选")).toBeInTheDocument();
});

it("merges two watchlist failures and keeps a retry on each row", async () => {
  stub({ failWatch: true });
  renderApp("/screener");
  const star = await screen.findByRole("row", { name: /星河股份/ });
  fireEvent.click(within(star).getByRole("button", { name: "加自选" }));
  expect(
    await screen.findByText("星河股份（XQ0001.BJ）没加进自选：这项操作暂时用不了。"),
  ).toBeInTheDocument();
  expect(screen.getAllByText("没加进自选").length).toBeGreaterThan(0);
  fireEvent.click(screen.getByRole("button", { name: "加自选" }));
  expect(
    await screen.findByText("2 只股票没加进自选：这项操作暂时用不了。可以在每一行点「重试」。"),
  ).toBeInTheDocument();
  expect(screen.getAllByRole("button", { name: "重试" }).length).toBeGreaterThanOrEqual(2);
  expect(screen.getByRole("link", { name: "看系统健康" })).toBeInTheDocument();
});

it("restores a deleted scheme when 撤销 is clicked", async () => {
  const { puts } = stub();
  renderApp("/screener");
  fireEvent.click(await screen.findByRole("button", { name: "删除方案 放量突破" }));
  expect(await screen.findByText("已删除方案『放量突破』")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "撤销" }));
  await waitFor(() => expect(puts).toHaveLength(1));
  expect(puts[0]).toMatchObject({
    name: "放量突破",
    filters: [{ field: "pct_chg", op: ">=", value: 3 }],
    ranks: [{ field: "pct_chg", desc: true, weight: 100 }],
  });
});
