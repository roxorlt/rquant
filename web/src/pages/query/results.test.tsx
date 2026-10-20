import { screen } from "@testing-library/react";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

it("shows the query error notice when the response has no rows", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input), "http://localhost");
      const method = (
        init?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
      if (method === "POST" && url.pathname.endsWith("/api/v1/query")) {
        return json({ data: { columns: ["code"], truncated: false }, serving });
      }
      if (url.pathname.endsWith("/api/v1/meta")) {
        return json({
          data: { app: "rQuant", version: "t", role: "admin", generation: serving },
          serving,
        });
      }
      if (url.pathname.endsWith("/api/v1/query/schema")) {
        return json({ data: { stock_basic: [["code", "VARCHAR"]] }, serving });
      }
      return json({ data: {}, serving });
    }),
  );

  renderApp("/query");
  const run = await screen.findByRole("button", { name: "运行" });
  run.click();
  expect(await screen.findByText("查询没有运行")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
  expect(screen.queryByRole("table", { name: "查询结果" })).not.toBeInTheDocument();
});
