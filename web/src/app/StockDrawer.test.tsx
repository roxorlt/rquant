import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { MANUAL_WATCHLIST_JOURNAL_KEY } from "@/api/manualWatchlistCommand";
import { fetchMeta } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { metaHandler, server } from "@/test/server";
import { StockDrawer } from "./StockDrawer";

beforeEach(() => {
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          available: false,
          can_generate: false,
          can_prepare_backtest: false,
          message: "调用尚未启用。",
        },
      }),
    ),
    http.get("*/api/v1/ai/news/:code", ({ params }) =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          stock_code: params.code,
          state: "missing",
          content: null,
          coverage: [],
          nightly_enabled: false,
          message: "原文尚未采集。",
        },
      }),
    ),
  );
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (_name: string, _options: unknown, task: () => Promise<void>) => task(),
    },
  });
});

const serving = metaEnvelope().serving;
type Exact = Schemas["Envelope_ManualWatchlistExactData_"];
type Status = NonNullable<Exact["data"]["status"]>;

function exact(status: Status, code = "600001.SH"): Exact {
  return {
    serving,
    data: {
      availability: "ready",
      available_at: "2026-09-24T07:31:00Z",
      message: "",
      ts_code: code,
      status,
      version: status === "absent" ? null : 2,
      source: status === "absent" || status === "deleted" ? null : "detail",
      price_levels: status === "active" || status === "expired" ? ["10.00"] : [],
      expires_at: status === "expired" ? "2026-09-24T07:30:00Z" : null,
      updated_at: status === "absent" || status === "deleted" ? null : "2026-09-24T07:30:00Z",
    },
  };
}

function stockHandlers() {
  server.use(
    http.get("*/api/v1/stocks/:code/summary", ({ params }) =>
      HttpResponse.json({
        data: {
          ts_code: params.code,
          name: "样本股票",
          price: 8.8,
          as_of: "2026-09-24T07:30:00Z",
          pools: ["自动池"],
        },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/:code/daily", ({ params }) =>
      HttpResponse.json({ data: { ts_code: params.code, name: "样本股票", bars: [] }, serving }),
    ),
  );
}

describe("个股抽屉手动盯盘", () => {
  it.each([
    ["active", "已加入盯盘"],
    ["expired", "已到期"],
    ["deleted", "已移出盯盘"],
    ["absent", "尚未加入"],
  ] as const)("显示可信的 %s 状态与对应操作", async (status, label) => {
    stockHandlers();
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact(status))));
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    expect(await within(drawer).findByText(label)).toBeInTheDocument();
    const action = status === "active" ? "移出盯盘" : "加入盯盘";
    expect(within(drawer).getByRole("button", { name: action })).toBeEnabled();
    expect(
      within(drawer).queryByRole("button", { name: status === "active" ? "加入盯盘" : "移出盯盘" }),
    ).toBeNull();
  });

  it("切换股票或登录用户时立刻隐藏旧肯定状态", async () => {
    stockHandlers();
    let hold = false;
    let release: () => void = () => undefined;
    const waiting = new Promise<void>((resolve) => {
      release = resolve;
    });
    server.use(
      http.get("*/api/v1/watchlist/:code", async ({ params }) => {
        if (hold) await waiting;
        return params.code === "600001.SH"
          ? HttpResponse.json(exact("active"))
          : HttpResponse.json(exact("absent", String(params.code)));
      }),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    const view = render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    expect(await screen.findByText("已加入盯盘")).toBeInTheDocument();

    view.rerender(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600002.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    expect(screen.queryByText("已加入盯盘")).toBeNull();
    expect(await screen.findByText("尚未加入")).toBeInTheDocument();
    await waitFor(() => expect(queryClient.isFetching({ queryKey: ["meta"] })).toBe(0));

    server.use(metaHandler(metaEnvelope({ viewer: "other-user" })));
    hold = true;
    act(() => {
      queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "other-user" }));
    });
    await waitFor(() => expect(screen.queryByText("尚未加入")).toBeNull());
    release();
    await waitFor(() => expect(screen.getByText("尚未加入")).toBeInTheDocument());
  });

  it("名单不可用或身份校验失败时不沿用已加入状态", async () => {
    stockHandlers();
    let available = true;
    server.use(
      http.get("*/api/v1/watchlist/:code", () =>
        HttpResponse.json(
          available
            ? exact("active")
            : {
                data: {
                  ...exact("active").data,
                  availability: "unavailable",
                  status: null,
                  version: null,
                  message: "名单暂不可用，请稍后重试。",
                },
                serving,
              },
        ),
      ),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    expect(await screen.findByText("已加入盯盘")).toBeInTheDocument();
    available = false;
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: ["private-watchlist"] });
    });
    expect(await screen.findByText("名单暂不可用，请稍后重试")).toBeInTheDocument();
    expect(screen.queryByText("已加入盯盘")).toBeNull();
    act(() => {
      queryClient.setQueryData(["meta"], metaEnvelope({ viewer: null }));
    });
    expect(screen.queryByText("已加入盯盘")).toBeNull();
  });

  it("已有肯定状态后 /meta 失败也隐藏私有结论", async () => {
    stockHandlers();
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact("active"))));
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    expect(await screen.findByText("已加入盯盘")).toBeInTheDocument();
    server.use(http.get("*/api/v1/meta", () => new HttpResponse(null, { status: 503 })));
    await act(async () => {
      await expect(
        queryClient.fetchQuery({ queryKey: ["meta"], queryFn: fetchMeta, retry: false }),
      ).rejects.toThrow();
    });
    await waitFor(() => expect(screen.queryByText("已加入盯盘")).toBeNull());
    expect(screen.getByText("名单暂不可用，请稍后重试")).toBeInTheDocument();
  });

  it("到期时自动撤销缓存中的已加入状态", async () => {
    stockHandlers();
    const soon = exact("active");
    soon.data.expires_at = "2026-09-24T07:31:31Z";
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(soon)));
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    expect(await screen.findByText("已加入盯盘")).toBeInTheDocument();
    expect(await screen.findByText("已到期", {}, { timeout: 2500 })).toBeInTheDocument();
    expect(screen.queryByText("已加入盯盘")).toBeNull();
  });

  it("加入命令保存后仍等待下一代核对，不提前显示已加入", async () => {
    stockHandlers();
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact("absent"))));
    let sent: unknown;
    server.use(
      http.post("*/api/v1/watchlist/commands", async ({ request }) => {
        sent = await request.json();
        const body = sent as { command_id: string };
        return HttpResponse.json({
          command_id: body.command_id,
          ts_code: "600001.SH",
          action: "add",
          status: "saved_syncing",
          version: 1,
          message: "已保存，正在同步。",
        });
      }),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    fireEvent.click(await within(drawer).findByRole("button", { name: "加入盯盘" }));
    expect(await within(drawer).findByText("已保存，正在同步")).toBeInTheDocument();
    expect(within(drawer).queryByText("已加入盯盘")).toBeNull();
    expect(sent).toMatchObject({
      action: "add",
      expected_version: null,
      source: "detail",
      price_levels: [],
    });
  });

  it("published 回执也要下一代同股同版本 GET 才显示最终状态", async () => {
    stockHandlers();
    let current = exact("absent");
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(current)));
    server.use(
      http.post("*/api/v1/watchlist/commands", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        return HttpResponse.json({
          command_id: body.command_id,
          ts_code: "600001.SH",
          action: "add",
          status: "published",
          version: 1,
          message: "已加入盯盘。",
        });
      }),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    fireEvent.click(await within(drawer).findByRole("button", { name: "加入盯盘" }));
    expect(await within(drawer).findByText("已保存，正在同步")).toBeInTheDocument();
    expect(within(drawer).queryByText("已加入盯盘")).toBeNull();
    const next = "b".repeat(64);
    current = exact("active");
    current.data.version = 1;
    current.serving = metaEnvelope({ generationId: next }).serving;
    server.use(metaHandler(metaEnvelope({ generationId: next })));
    act(() => queryClient.setQueryData(["meta"], metaEnvelope({ generationId: next })));
    expect(await within(drawer).findByText("已加入盯盘")).toBeInTheDocument();
  });

  it("提交前重验的用户元信息时钟坏了就不发送命令", async () => {
    stockHandlers();
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact("absent"))));
    const post = vi.fn(() => HttpResponse.json({}));
    server.use(http.post("*/api/v1/watchlist/commands", post));
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    const button = await within(drawer).findByRole("button", { name: "加入盯盘" });
    await waitFor(() => expect(queryClient.isFetching({ queryKey: ["meta"] })).toBe(0));
    const invalid = metaEnvelope();
    invalid.data.server_time = "invalid-time";
    server.use(metaHandler(invalid));
    fireEvent.click(button);
    await waitFor(() =>
      expect(within(drawer).getByText("名单暂不可用，请稍后重试")).toBeInTheDocument(),
    );
    expect(post).not.toHaveBeenCalled();
  });

  it("提交前单股版本变化时阻止旧 CAS 命令", async () => {
    stockHandlers();
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact("absent"))));
    const post = vi.fn(() => HttpResponse.json({}));
    server.use(http.post("*/api/v1/watchlist/commands", post));
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    const button = await within(drawer).findByRole("button", { name: "加入盯盘" });
    await waitFor(() => expect(queryClient.isFetching({ queryKey: ["meta"] })).toBe(0));
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact("active"))));
    fireEvent.click(button);
    await waitFor(() =>
      expect(within(drawer).getByText("名单已更新，请刷新后重试。")).toBeInTheDocument(),
    );
    expect(post).not.toHaveBeenCalled();
  });

  it.each([
    ["conflict", "名单已更新，请刷新后重试。"],
    ["capacity", "名单已满，请先移出其他股票。"],
  ] as const)("给 %s 回执明确的下一步且不冒称加入", async (status, message) => {
    stockHandlers();
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(exact("absent"))));
    server.use(
      http.post("*/api/v1/watchlist/commands", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        return HttpResponse.json(
          {
            command_id: body.command_id,
            ts_code: "600001.SH",
            action: "add",
            status,
            version: null,
            message,
          },
          { status: 409 },
        );
      }),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    fireEvent.click(await within(drawer).findByRole("button", { name: "加入盯盘" }));
    expect(await within(drawer).findByText(message)).toBeInTheDocument();
    expect(within(drawer).queryByText("已加入盯盘")).toBeNull();
    expect(within(drawer).getByRole("button", { name: "刷新名单" })).toBeEnabled();
  });

  it("已发布的加入后来到期，可按当前可信版本重新加入", async () => {
    stockHandlers();
    const prior = exact("expired");
    prior.serving = metaEnvelope().serving;
    server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(prior)));
    let sent: unknown = null;
    server.use(
      http.post("*/api/v1/watchlist/commands", async ({ request }) => {
        sent = await request.json();
        const body = sent as { command_id: string };
        return HttpResponse.json({
          command_id: body.command_id,
          ts_code: "600001.SH",
          action: "add",
          status: "pending",
          version: null,
          message: "正在处理",
        });
      }),
    );
    window.localStorage.setItem(
      `${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:600001.SH`,
      JSON.stringify({
        schema: 1,
        body: {
          command_id: "web-prior",
          requested_at: "2026-09-24T07:00:00.000Z",
          generation_id: "b".repeat(64),
          ts_code: "600001.SH",
          action: "add",
          expected_version: 1,
          source: "detail",
          price_levels: [],
        },
        status: "published",
        version: 2,
      }),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(["meta"], metaEnvelope());
    render(
      <AppProviders queryClient={queryClient}>
        <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
      </AppProviders>,
    );
    const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
    expect(await within(drawer).findByText("已到期")).toBeInTheDocument();
    const add = within(drawer).getByRole("button", { name: "加入盯盘" });
    expect(add).toBeEnabled();
    fireEvent.click(add);
    await waitFor(() => expect(sent).toMatchObject({ action: "add", expected_version: 2 }));
  });

  it.each([
    ["add", "deleted", "已移出盯盘", "加入盯盘"],
    ["remove", "active", "已加入盯盘", "移出盯盘"],
  ] as const)(
    "旧已发布 %s 被更高版本覆盖后以当前 %s 为准",
    async (priorAction, currentStatus, label, action) => {
      stockHandlers();
      const current = exact(currentStatus);
      current.data.version = 4;
      server.use(http.get("*/api/v1/watchlist/:code", () => HttpResponse.json(current)));
      let sent: unknown = null;
      server.use(
        http.post("*/api/v1/watchlist/commands", async ({ request }) => {
          sent = await request.json();
          const next = sent as { command_id: string };
          return HttpResponse.json({
            command_id: next.command_id,
            ts_code: "600001.SH",
            action: priorAction === "add" ? "add" : "remove",
            status: "pending",
            version: null,
            message: "正在处理",
          });
        }),
      );
      const body = {
        command_id: "web-prior",
        requested_at: "2026-09-24T07:00:00.000Z",
        generation_id: "b".repeat(64),
        ts_code: "600001.SH",
        action: priorAction,
        expected_version: 2,
        ...(priorAction === "add" ? { source: "detail", price_levels: [] } : {}),
      };
      window.localStorage.setItem(
        `${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:600001.SH`,
        JSON.stringify({ schema: 1, body, status: "published", version: 3 }),
      );
      const queryClient = testQueryClient();
      queryClient.setQueryData(["meta"], metaEnvelope());
      render(
        <AppProviders queryClient={queryClient}>
          <StockDrawer tsCode="600001.SH" onClose={() => undefined} />
        </AppProviders>,
      );
      const drawer = await screen.findByRole("dialog", { name: /样本股票/ });
      expect(await within(drawer).findByText(label)).toBeInTheDocument();
      const button = within(drawer).getByRole("button", { name: action });
      expect(button).toBeEnabled();
      expect(within(drawer).queryByText("已保存，正在同步")).toBeNull();
      fireEvent.click(button);
      await waitFor(() => expect(sent).toMatchObject({ expected_version: 4, action: priorAction }));
    },
  );
});
