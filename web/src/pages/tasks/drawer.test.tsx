import { fireEvent, screen, waitFor } from "@testing-library/react";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

afterEach(() => vi.unstubAllGlobals());

it("closes the log drawer with the browser back button", async () => {
  vi.unstubAllGlobals();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      const path = url.pathname;
      const data = path.includes("/log")
        ? { unit: "rquant-daily.service", lines: ["ok"], error: null }
        : path.endsWith("/tasks")
          ? [
              {
                unit: "rquant-daily.service",
                kind: "service",
                active: "active",
                sub: "running",
                next_run: null,
                last_run: null,
                description: "日线",
              },
            ]
          : path.includes("/meta")
            ? { app: "rQuant", version: "t", role: "admin", generation: serving }
            : {};
      return new Response(JSON.stringify({ data, serving }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
  const view = renderApp("/tasks");
  fireEvent.click(await screen.findByText("日线采集服务"));
  expect(await screen.findByRole("dialog")).toBeInTheDocument();
  await waitFor(() => expect(view.router.state.location.search).toContain("unit="));
  await view.router.navigate(-1);
  await waitFor(() => expect(view.router.state.location.search).not.toContain("unit="));
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
});
