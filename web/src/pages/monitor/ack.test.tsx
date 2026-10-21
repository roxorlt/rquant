import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { renderApp } from "@/test/render";

const ALERT = "a".repeat(64);
const OTHER = "b".repeat(64);
const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

function item(id: string, acked: boolean) {
  return {
    alert_id: id,
    trade_date: "2026-10-20",
    at: "2026-10-20T06:00:00Z",
    code: id === ALERT ? "999001.SH" : "999002.SH",
    name: id === ALERT ? "澜屿科技" : "样例电子",
    level: "L1",
    trigger_type: "跳空高开",
    trigger_price: 10,
    level_price: 10,
    pool: null,
    acked_at: acked ? "2026-10-20T06:05:00Z" : null,
    acked_by: acked ? "owner" : null,
  };
}

type FetchImpl = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

let currentFetch: FetchImpl = async () => new Response("missing", { status: 404 });

const fetchGateway: FetchImpl = (input, init) => currentFetch(input, init);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchGateway);
});

function stub(opts: { failAck?: boolean } = {}) {
  const acked = new Set<string>();
  const posts: string[] = [];
  const fetchMock: FetchImpl = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const path = url.pathname;
    const method = (
      init?.method ?? (input instanceof Request ? input.method : "GET")
    ).toUpperCase();
    const rawBody =
      typeof init?.body === "string"
        ? init.body
        : input instanceof Request
          ? await input.clone().text()
          : "";
    if (method === "POST" && path.endsWith("/alerts/ack")) {
      posts.push("ack");
      if (opts.failAck) {
        return new Response("no", { status: 405 });
      }
      const body = JSON.parse(rawBody || "{}") as { alert_id: string };
      acked.add(body.alert_id);
      return new Response(JSON.stringify({ command_id: "c1", status: "succeeded" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    if (method === "POST" && path.endsWith("/alerts/unack")) {
      posts.push("unack");
      const body = JSON.parse(rawBody || "{}") as { alert_id: string };
      acked.delete(body.alert_id);
      return new Response(JSON.stringify({ command_id: "c2", status: "succeeded" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    const key = path.split("/api/v1/")[1]?.split("/")[0] ?? "";
    const data =
      key === "alerts"
        ? { items: [item(ALERT, acked.has(ALERT)), item(OTHER, acked.has(OTHER))] }
        : key === "meta"
          ? { app: "rQuant", version: "test", role: "admin", generation: serving }
          : key === "alert-rules"
            ? { rules: [] }
            : key === "operations"
              ? { items: [] }
              : key === "surge"
                ? { date: null, items: [] }
                : key === "health"
                  ? { services: [], freshness: [], layers: [] }
                  : null;
    if (data === null) {
      return new Response("missing", { status: 404 });
    }
    return new Response(JSON.stringify({ data, serving }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
  currentFetch = fetchMock;
  return { posts };
}

afterEach(() => {
  currentFetch = async () => new Response("missing", { status: 404 });
});

it("confirms once on a double click, replaces the toast, and matches the server after refresh", async () => {
  const { posts } = stub();
  renderApp("/monitor");
  const buttons = await screen.findAllByRole("button", { name: "确认" });
  fireEvent.click(buttons[0] as HTMLElement);
  fireEvent.click(buttons[0] as HTMLElement);
  await screen.findByRole("status");
  await waitFor(() => expect(posts.filter((item) => item === "ack")).toHaveLength(1));
  expect(screen.getByRole("status")).toHaveTextContent("已确认 1 条告警");
  await waitFor(() => expect(screen.getAllByText(/已确认/).length).toBeGreaterThan(0));

  const again = screen.getAllByRole("button", { name: "确认" });
  fireEvent.click(again[0] as HTMLElement);
  await waitFor(() => expect(screen.getAllByRole("status")).toHaveLength(1));
});

it("does not show 已确认 when the write fails, and offers 重试 on the row", async () => {
  stub({ failAck: true });
  renderApp("/monitor");
  fireEvent.click(
    await screen.findAllByRole("button", { name: "确认" }).then((n) => n[0] as HTMLElement),
  );
  const row = await screen.findByRole("row", { name: /澜屿科技/ });
  expect(within(row).getByText("没确认上")).toBeInTheDocument();
  expect(within(row).getByRole("button", { name: "重试" })).toBeInTheDocument();
  expect(within(row).getByText("未确认")).toBeInTheDocument();
  expect(screen.queryByText("已确认 1 条告警")).not.toBeInTheDocument();
  expect(screen.queryByText(/HTTP/)).not.toBeInTheDocument();
});

it("names the stock and keeps the next step when several confirms fail", async () => {
  stub({ failAck: true });
  renderApp("/monitor");
  const first = (await screen.findAllByRole("button", { name: "确认" }))[0] as HTMLElement;
  fireEvent.click(first);
  expect(await screen.findByText(/澜屿科技（999001\.SH）/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "看系统健康" })).toBeInTheDocument();
  const second = screen.getAllByRole("button", { name: "确认" })[0] as HTMLElement;
  fireEvent.click(second);
  expect(
    await screen.findByText("2 条告警没确认上：这项操作暂时用不了。稍后再试，或去看系统健康"),
  ).toBeInTheDocument();
});

it("undo restores the alert and says 已撤销确认", async () => {
  stub();
  renderApp("/monitor");
  fireEvent.click((await screen.findAllByRole("button", { name: "确认" }))[0] as HTMLElement);
  fireEvent.click(await screen.findByRole("button", { name: "撤销" }));
  expect(await screen.findByText("已撤销确认")).toBeInTheDocument();
  await waitFor(() => expect(screen.getAllByRole("button", { name: "确认" }).length).toBe(2));
});
