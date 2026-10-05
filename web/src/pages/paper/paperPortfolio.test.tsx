import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { paperEnvelope } from "./paperPortfolio.fixture";
import fixture from "./paperPortfolio.fixture.json";
import type { PaperPause, PaperRun, PaperSave } from "./paperPortfolioApi";

const generation = fixture.catalog.serving.generation_id;
const account = fixture.detail.data.configuration.account_id;
const base = "*/api/v1/paper-portfolios";

beforeEach(() => {
  server.use(
    metaHandler(metaEnvelope({ viewer: "alice", generationId: generation })),
    http.get(base, () => HttpResponse.json(fixture.catalog)),
    http.get(`${base}/${account}`, () => HttpResponse.json(fixture.detail)),
    http.get(`${base}/${account}/history`, () => HttpResponse.json(fixture.history)),
  );
});

it("uses the published account and opens actual portfolio rules instead of fixed-order controls", async () => {
  const user = userEvent.setup();
  renderApp("/paper");
  await user.click(await screen.findByRole("button", { name: "查看模拟账户 1" }));
  const edit = await screen.findByRole("button", { name: "设置仓位" });
  await user.click(edit);
  expect(await screen.findByRole("dialog", { name: "仓位与回撤" })).toBeVisible();
  expect(screen.getByLabelText("最多持股数")).toHaveValue(1);
  expect(screen.getByLabelText("现金保留（%）")).toHaveValue(10);
  expect(screen.getByRole("button", { name: "保存规则" })).toBeEnabled();
});

async function openAccount(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("button", { name: "查看模拟账户 1" }));
  await screen.findByRole("button", { name: "设置仓位" });
}

it("known 422 unlocks the draft; unknown receipt keeps the exact request and can retry it", async () => {
  const sent: PaperSave[] = [];
  const recovered: PaperSave[] = [];
  server.use(
    http.post(`${base}/${account}/configuration`, async ({ request }) => {
      const body = (await request.json()) as PaperSave;
      sent.push(body);
      if (sent.length === 1) return HttpResponse.json({ detail: "规则错误" }, { status: 422 });
      if (sent.length === 2) return HttpResponse.error();
      return HttpResponse.json(
        paperEnvelope({
          command_id: body.command_id,
          account_id: account,
          status: "published",
          message: "已保存。",
        }),
      );
    }),
    http.post(`${base}/${account}/recover`, async ({ request }) => {
      recovered.push((await request.json()) as PaperSave);
      return HttpResponse.json(
        paperEnvelope({
          command_id: recovered[0]?.command_id,
          account_id: account,
          status: "uncertain",
          message: "结果待确认，请继续查看。",
        }),
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/paper");
  await openAccount(user);
  await user.click(screen.getByRole("button", { name: "设置仓位" }));
  await user.click(screen.getByRole("button", { name: "保存规则" }));
  await screen.findByText("规则有误，请检查后重试。");
  expect(screen.getByRole("button", { name: "保存规则" })).toBeEnabled();
  fireEvent.change(screen.getByLabelText("最多持股数"), { target: { value: "2" } });
  await user.click(screen.getByRole("button", { name: "保存规则" }));
  await screen.findByText("结果待确认，请继续查看。");
  expect(screen.getByRole("button", { name: "保存规则" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "继续查看" }));
  await waitFor(() => expect(recovered).toHaveLength(1));
  expect(recovered[0]).toEqual(sent[1]);
  await user.click(screen.getByRole("button", { name: "重试原操作" }));
  await screen.findByText("已保存。");
  expect(sent).toHaveLength(3);
  expect(sent[2]).toEqual(sent[1]);
  expect(sent[1]?.command_id).not.toEqual(sent[0]?.command_id);
});

it("pause requires a server preview and typed confirmation; waiting does not claim application", async () => {
  const prepared: PaperPause[] = [];
  const confirmed: { request: PaperPause; confirmation_id: string }[] = [];
  server.use(
    http.post(`${base}/${account}/pause/prepare`, async ({ request }) => {
      const body = (await request.json()) as PaperPause;
      prepared.push(body);
      return HttpResponse.json(
        paperEnvelope({
          command: body,
          confirmation_id: "synthetic-preview",
          expires_at: new Date(Date.now() + 300000).toISOString(),
        }),
      );
    }),
    http.post(`${base}/${account}/pause/confirm`, async ({ request }) => {
      const body = (await request.json()) as { request: PaperPause; confirmation_id: string };
      confirmed.push(body);
      return HttpResponse.json(
        paperEnvelope({
          command_id: body.request.command_id,
          account_id: account,
          status: "waiting_application",
          sequence: 2,
          message: "已提交，等待应用。",
        }),
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/paper");
  await openAccount(user);
  await user.click(screen.getByRole("button", { name: "暂停新入场" }));
  const dialog = await screen.findByRole("dialog", { name: "暂停新入场" });
  expect(prepared).toHaveLength(1);
  expect(confirmed).toHaveLength(0);
  expect(within(dialog).getByRole("button", { name: "确认执行" })).toBeDisabled();
  await user.type(within(dialog).getByRole("textbox"), "自选策略");
  await user.click(within(dialog).getByRole("button", { name: "确认执行" }));
  await screen.findByText("已提交，等待应用。");
  expect(confirmed[0]?.request).toEqual(prepared[0]);
  expect(confirmed[0]?.confirmation_id).toBe("synthetic-preview");
  expect(screen.getByText("运行中")).toBeVisible();
  expect(screen.queryByText("已暂停")).not.toBeInTheDocument();
});

it("tips do not toggle a rule; invalid drawdown stays editable and Escape returns to the action", async () => {
  const user = userEvent.setup();
  renderApp("/paper");
  await openAccount(user);
  const action = screen.getByRole("button", { name: "设置仓位" });
  await user.click(action);
  await user.click(screen.getByRole("checkbox", { name: "回撤限制" }));
  await user.click(screen.getByRole("button", { name: "回撤限制说明" }));
  expect(screen.getByRole("checkbox", { name: "回撤限制" })).toBeChecked();
  fireEvent.change(screen.getByLabelText("触发回撤（%）"), { target: { value: "100" } });
  await user.click(screen.getByRole("button", { name: "保存规则" }));
  expect(screen.getByRole("alert")).toHaveTextContent("须小于 100%");
  fireEvent.change(screen.getByLabelText("触发回撤（%）"), { target: { value: "8.5" } });
  await user.keyboard("{Escape}");
  await waitFor(() =>
    expect(screen.queryByRole("dialog", { name: "仓位与回撤" })).not.toBeInTheDocument(),
  );
  await waitFor(() => expect(action).toHaveFocus());
});

it("band close restores its action after the original submission releases the button", async () => {
  let respond: ((response: Response) => void) | undefined;
  let requestBody: PaperRun | undefined;
  server.use(
    http.get(`${base}/${account}`, () =>
      HttpResponse.json({
        ...fixture.detail,
        data: {
          ...fixture.detail.data,
          can_band: true,
          backtests: [
            {
              job_id: "11111111-1111-4111-8111-111111111111",
              name: "同版已封存回测",
              completed_at: "2026-07-31T01:32:00Z",
            },
          ],
        },
      }),
    ),
    http.post(`${base}/${account}/band`, async ({ request }) => {
      requestBody = (await request.json()) as PaperRun;
      return new Promise<Response>((resolve) => {
        respond = resolve;
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/paper");
  await openAccount(user);
  const action = screen.getByRole("button", { name: "计算回测区间" });
  await user.click(action);
  await user.click(screen.getByRole("button", { name: "提交计算" }));
  await waitFor(() => expect(respond).toBeDefined());
  expect(action).toBeDisabled();
  await user.keyboard("{Escape}");
  await waitFor(() =>
    expect(screen.queryByRole("dialog", { name: "计算回测区间" })).not.toBeInTheDocument(),
  );
  respond?.(
    HttpResponse.json(
      paperEnvelope({
        command_id: requestBody?.command_id,
        account_id: account,
        status: "submitted",
        job_id: requestBody?.command_id,
        message: "研究已提交，等待封存。",
      }),
    ),
  );
  await waitFor(() => expect(action).toBeEnabled());
  await waitFor(() => expect(action).toHaveFocus());
});

it("late replies cannot enter another viewer's page", async () => {
  let respond: ((response: Response) => void) | undefined;
  server.use(
    http.post(
      `${base}/${account}/configuration`,
      async () =>
        new Promise<Response>((resolve) => {
          respond = resolve;
        }),
    ),
  );
  const user = userEvent.setup();
  const { queryClient } = renderApp("/paper");
  await openAccount(user);
  await user.click(screen.getByRole("button", { name: "设置仓位" }));
  await user.click(screen.getByRole("button", { name: "保存规则" }));
  await waitFor(() => expect(respond).toBeDefined());
  server.use(
    metaHandler(metaEnvelope({ viewer: "bob", generationId: generation })),
    http.get(base, () =>
      HttpResponse.json(paperEnvelope({ availability: "empty", available_at: null, accounts: [] })),
    ),
  );
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
  });
  await screen.findByText("还没有模拟账户");
  const body = JSON.parse(sessionStorage.getItem("rquant.paper-portfolio.pending.alice") ?? "null");
  const original = body?.command ?? body;
  await act(async () => {
    respond?.(
      HttpResponse.json(
        paperEnvelope({
          command_id: original.command_id,
          account_id: account,
          status: "published",
          message: "旧用户已保存。",
        }),
      ),
    );
  });
  expect(screen.queryByText("旧用户已保存。")).not.toBeInTheDocument();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(sessionStorage.getItem("rquant.paper-portfolio.pending.bob")).toBeNull();
});

it("full history follows the original cursor and clears details on a generation change", async () => {
  const queries: string[] = [];
  const first = fixture.history.data.records[0];
  if (!first) throw new Error("fixture record missing");
  server.use(
    http.get(`${base}/${account}/history`, ({ request }) => {
      const cursor = new URL(request.url).searchParams.get("cursor");
      queries.push(cursor ?? "first");
      return HttpResponse.json(
        paperEnvelope({
          ...fixture.history.data,
          total_orders: 201,
          records: cursor
            ? [{ ...first, sequence: 1, order: { ...first.order, filled_quantity: 100 } }]
            : [{ ...first, sequence: 201 }],
          next_cursor: cursor ? null : "original-published-cursor",
        }),
      );
    }),
  );
  const user = userEvent.setup();
  const { queryClient } = renderApp("/paper");
  await openAccount(user);
  await user.click(await screen.findByRole("button", { name: "完整历史" }));
  await user.click(screen.getByRole("button", { name: "下一页" }));
  const table = await screen.findByRole("table", { name: "完整模拟指令" });
  await waitFor(() => expect(table).toHaveTextContent("100 / 800"));
  expect(queries).toEqual(["first", "original-published-cursor"]);
  const row = within(table).getByRole("row", { name: /600000/ });
  act(() => row.focus());
  await user.keyboard("{Enter}");
  expect(await screen.findByRole("dialog", { name: "模拟指令详情" })).toBeVisible();
  await user.keyboard("{Escape}");
  await waitFor(() => expect(row).toHaveFocus());
  await user.keyboard("{Enter}");
  server.use(
    metaHandler(metaEnvelope({ viewer: "alice", generationId: "b".repeat(64) })),
    http.get(base, () =>
      HttpResponse.json(paperEnvelope({ ...fixture.catalog.data, accounts: [] }, "b".repeat(64))),
    ),
  );
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
  });
  await screen.findByText("还没有模拟账户");
  expect(screen.queryByRole("dialog", { name: "模拟指令详情" })).not.toBeInTheDocument();
});

it("keyboard account navigation returns to its exact list entry", async () => {
  const user = userEvent.setup();
  renderApp("/paper");
  const card = await screen.findByRole("button", { name: "查看模拟账户 1" });
  act(() => card.focus());
  await user.keyboard("{Enter}");
  await user.click(await screen.findByRole("button", { name: "账户列表" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "查看模拟账户 1" })).toHaveFocus());
});

it("missing generation gives an empty state and metadata failure gives a retry", async () => {
  const noGeneration = metaEnvelope({ viewer: "alice" });
  noGeneration.data.generation = null;
  server.use(metaHandler(noGeneration));
  const { queryClient } = renderApp("/paper");
  await screen.findByText("账户数据尚未发布");
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json({ detail: "unavailable" }, { status: 503 })),
  );
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
  });
  await screen.findByText("模拟账户暂时无法加载");
});
