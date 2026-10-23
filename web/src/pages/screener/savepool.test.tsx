import { fireEvent, screen, waitFor } from "@testing-library/react";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

afterEach(() => vi.unstubAllGlobals());

it("sends POST /api/v1/pools when 保存 is clicked", async () => {
  const bodies: string[] = [];
  vi.unstubAllGlobals();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      const method = (
        init?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
      if (method === "POST" && url.pathname.endsWith("/pools")) {
        const raw =
          typeof init?.body === "string"
            ? init.body
            : input instanceof Request
              ? await input.clone().text()
              : "";
        bodies.push(raw);
        return new Response(JSON.stringify({ command_id: "p", status: "succeeded" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      const key = url.pathname.split("/api/v1/")[1]?.split("/")[0] ?? "";
      const data =
        key === "screen"
          ? {
              trade_date: "2026-10-20",
              presets: ["breakout"],
              rows: [
                {
                  trade_date: "2026-10-20",
                  code: "600519.SH",
                  name: "贵州茅台",
                  preset: "breakout",
                  close: 1,
                  pct_chg: 1,
                },
              ],
            }
          : key === "meta"
            ? { app: "rQuant", version: "t", role: "admin", generation: serving }
            : {};
      return new Response(JSON.stringify({ data, serving }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
  renderApp("/screener");
  const open = await screen.findByRole("button", { name: "保存为池子" });
  await waitFor(() => expect(open).toBeEnabled());
  fireEvent.click(open);
  fireEvent.click(await screen.findByRole("button", { name: "保存" }));
  await waitFor(() => expect(bodies.length).toBe(1));
  expect(bodies[0]).toContain("选股 10-20");
  expect(bodies[0]).toContain("breakout");
});
