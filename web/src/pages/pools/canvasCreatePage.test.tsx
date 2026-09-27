import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { CANVAS_CREATE_JOURNAL_KEY } from "./canvasCreateSession";

const HASH = "a".repeat(64);
const publishedCanvas = {
  name: "晨盘观察",
  description: "观察候选池",
  pool_keys: [],
  refs_truncated: false,
};
const editableCanvas = {
  name: "晨盘观察",
  description: "观察候选池",
  version: "b".repeat(64),
  command_id: "canvas-browser-test",
  record_hash: HASH,
  pool_refs: [],
};

beforeEach(() => window.sessionStorage.clear());

function respondEmpty(viewer: string | null = "tester") {
  const serving = metaEnvelope().serving;
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ viewer }))),
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: {
          state: "no_data",
          latest_trade_date: null,
          definitions_available: true,
          rules_available: true,
          canvases: [],
          canvases_truncated: false,
          pools_truncated: false,
          pools: [],
        } satisfies Schemas["PoolsData"],
        serving,
      }),
    ),
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({
        data: {
          state: "ready",
          canvas_create_available: true,
          pools: [],
          copy_sources: [],
          canvases: [],
        } satisfies Schemas["PoolEditorData"],
        serving,
      }),
    ),
  );
}

it("creates a blank canvas from the keyboard, waits for matching publication, then opens it", async () => {
  let published = false;
  let sent: Schemas["CreateCanvasCommand"] | null = null;
  server.use(
    http.get("*/api/v1/meta", () =>
      HttpResponse.json(metaEnvelope({ generationId: published ? "g2" : "g1" })),
    ),
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: {
          state: "no_data",
          latest_trade_date: null,
          definitions_available: true,
          rules_available: true,
          canvases: published ? [publishedCanvas] : [],
          canvases_truncated: false,
          pools_truncated: false,
          pools: [],
        } satisfies Schemas["PoolsData"],
        serving: metaEnvelope({ generationId: published ? "g2" : "g1" }).serving,
      }),
    ),
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({
        data: {
          state: "ready",
          canvas_create_available: true,
          pools: [],
          copy_sources: [],
          canvases: published ? [{ ...editableCanvas, command_id: sent?.command_id ?? "" }] : [],
        } satisfies Schemas["PoolEditorData"],
        serving: metaEnvelope({ generationId: published ? "g2" : "g1" }).serving,
      }),
    ),
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      sent = (await request.json()) as Schemas["CreateCanvasCommand"];
      const stored = JSON.parse(window.sessionStorage.getItem(CANVAS_CREATE_JOURNAL_KEY) ?? "{}");
      expect(sent).toEqual(stored.body);
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      return HttpResponse.json({
        command_id: sent.command_id,
        status: "succeeded",
        message: "画布已保存，等待发布",
        canvas_name: sent.name,
        canvas_record_hash: HASH,
      });
    }),
  );
  const user = userEvent.setup();
  const first = renderApp("/pools");
  const add = await screen.findByRole("button", { name: "新建画布" });
  add.focus();
  await user.keyboard("{Enter}");
  const dialog = screen.getByRole("dialog", { name: "新建画布" });
  await user.type(within(dialog).getByRole("textbox", { name: "画布名称" }), "晨盘观察");
  await user.type(within(dialog).getByRole("textbox", { name: /简短说明/ }), "观察候选池");
  await user.click(within(dialog).getByRole("button", { name: "创建画布" }));
  await waitFor(() => expect(sent).not.toBeNull());
  await within(dialog).findByText("已保存，等待发布");
  expect(within(dialog).queryByText("画布已可用")).not.toBeInTheDocument();
  expect(findJargon(dialog.textContent ?? "")).toEqual([]);
  first.unmount();

  published = true;
  const second = renderApp("/pools");
  const available = await screen.findByRole("status", { name: "画布创建状态" });
  await waitFor(() => expect(available).toHaveTextContent("画布已可用"));
  await user.click(within(available).getByRole("button", { name: "打开画布" }));
  expect(await screen.findByRole("combobox", { name: "选择画布" })).toHaveValue("晨盘观察");
  expect(screen.getByText("这张画布还是空的")).toBeInTheDocument();
  expect(screen.getByText("先发布一只池子，再来添加条件节点。")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "添加条件节点" })).toHaveAttribute(
    "aria-description",
    "还没有可作为来源的池子，先发布一只池子。",
  );
  expect(findJargon(second.container.textContent ?? "")).toEqual([]);
});

it("keeps the create action unavailable until a viewer is signed in", async () => {
  const post = vi.fn();
  respondEmpty(null);
  server.use(http.post("*/api/v1/pools/editor/commands", post));
  renderApp("/pools");
  expect(await screen.findByRole("button", { name: "新建画布" })).toBeDisabled();
  expect(post).not.toHaveBeenCalled();
});

it("explains why an unsigned viewer cannot resume a saved request", async () => {
  window.sessionStorage.setItem(
    CANVAS_CREATE_JOURNAL_KEY,
    JSON.stringify({
      schema: 1,
      body: {
        kind: "create_canvas",
        command_id: "canvas-before-login",
        requested_at: "2026-09-27T07:00:00.000Z",
        name: "晨盘观察",
        description: "",
      },
      status: "unknown",
      recordHash: null,
      reason: null,
    }),
  );
  const post = vi.fn();
  respondEmpty(null);
  server.use(http.post("*/api/v1/pools/editor/commands", post));
  renderApp("/pools");
  const status = await screen.findByRole("status", { name: "画布创建状态" });
  await waitFor(() =>
    expect(within(status).getByRole("button", { name: "继续核对" })).toHaveAttribute(
      "aria-description",
      "请先登录，才能新建画布。",
    ),
  );
  expect(post).not.toHaveBeenCalled();
});

it("shows a name conflict and lets the user submit a corrected name with a fresh request", async () => {
  respondEmpty();
  const sent: Schemas["CreateCanvasCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["CreateCanvasCommand"];
      sent.push(body);
      return HttpResponse.json(
        sent.length === 1
          ? {
              command_id: body.command_id,
              status: "failed",
              message: "画布名称已被使用，请换一个名称。",
            }
          : {
              command_id: body.command_id,
              status: "succeeded",
              message: "画布已保存，等待发布",
              canvas_name: body.name,
              canvas_record_hash: HASH,
            },
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "新建画布" }));
  const dialog = screen.getByRole("dialog", { name: "新建画布" });
  const name = within(dialog).getByRole("textbox", { name: "画布名称" });
  await user.type(name, "晨盘观察");
  await user.click(within(dialog).getByRole("button", { name: "创建画布" }));
  await within(dialog).findByText("画布名称已被使用，请换一个名称。");
  await user.clear(name);
  await user.type(name, "晨盘观察二");
  await user.click(within(dialog).getByRole("button", { name: "创建画布" }));
  await within(dialog).findByText("已保存，等待发布");
  expect(sent.map((body) => body.name)).toEqual(["晨盘观察", "晨盘观察二"]);
  expect(sent[1]?.command_id).not.toBe(sent[0]?.command_id);
});

it("keeps an uncertain create request and checks it with the same identity", async () => {
  respondEmpty();
  const sent: Schemas["CreateCanvasCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["CreateCanvasCommand"];
      sent.push(body);
      return sent.length === 1
        ? HttpResponse.json({ detail: "连接暂不可用，状态待确认。" }, { status: 503 })
        : HttpResponse.json({
            command_id: body.command_id,
            status: "succeeded",
            message: "画布已保存，等待发布",
            canvas_name: body.name,
            canvas_record_hash: HASH,
          });
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "新建画布" }));
  const dialog = screen.getByRole("dialog", { name: "新建画布" });
  await user.type(within(dialog).getByRole("textbox", { name: "画布名称" }), "晨盘观察");
  await user.click(within(dialog).getByRole("button", { name: "创建画布" }));
  await within(dialog).findByText("创建状态待确认");
  await user.click(within(dialog).getByRole("button", { name: "继续核对" }));
  await within(dialog).findByText("已保存，等待发布");
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
});
