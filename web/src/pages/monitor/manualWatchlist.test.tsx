import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { MANUAL_WATCHLIST_JOURNAL_KEY } from "@/api/manualWatchlistCommand";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

type List = Schemas["Envelope_ManualWatchlistListData_"];

beforeEach(() => {
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (_name: string, _options: unknown, task: () => Promise<void>) => task(),
    },
  });
});

function list(): List {
  return {
    serving: metaEnvelope().serving,
    data: {
      availability: "ready",
      available_at: "2026-09-24T07:31:00Z",
      message: "",
      items: [
        {
          ts_code: "600001.SH",
          version: 2,
          source: "detail",
          price_levels: ["10.00"],
          expires_at: "2026-09-24T07:40:00Z",
          updated_at: "2026-09-24T07:30:00Z",
        },
        {
          ts_code: "600002.SH",
          version: 1,
          source: "pool_member",
          price_levels: [],
          expires_at: "2026-09-24T07:31:00Z",
          updated_at: "2026-09-24T07:30:00Z",
        },
      ],
    },
  };
}

describe("盯盘页手动名单", () => {
  it("只展示可信有效成员，和自动策略时间线分开", async () => {
    server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(list())));
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByText("600001.SH")).toBeInTheDocument();
    expect(within(section).queryByText("600002.SH")).toBeNull();
    expect(within(section).getByText("来自个股详情")).toBeInTheDocument();
    expect(within(section).queryByText("正在告警")).toBeNull();
    expect(await screen.findByRole("list", { name: "告警时间线" })).toHaveTextContent("竞价跳空");
  });

  it("身份或数据代变化时不短暂展示前一份名单", async () => {
    let hold = false;
    let release: () => void = () => undefined;
    const waiting = new Promise<void>((resolve) => {
      release = resolve;
    });
    server.use(
      http.get("*/api/v1/watchlist", async () => {
        if (hold) await waiting;
        return HttpResponse.json(list());
      }),
    );
    const view = renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByText("600001.SH")).toBeInTheDocument();
    await waitFor(() => expect(view.queryClient.isFetching({ queryKey: ["meta"] })).toBe(0));

    server.use(metaHandler(metaEnvelope({ viewer: "other-user" })));
    hold = true;
    act(() => {
      view.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "other-user" }));
    });
    await waitFor(() => expect(within(section).queryByText("600001.SH")).toBeNull());
    release();
    await waitFor(() => expect(within(section).getByText("600001.SH")).toBeInTheDocument());

    const next = "b".repeat(64);
    server.use(metaHandler(metaEnvelope({ viewer: "other-user", generationId: next })));
    act(() => {
      view.queryClient.setQueryData(
        ["meta"],
        metaEnvelope({ viewer: "other-user", generationId: next }),
      );
    });
    await waitFor(() => expect(within(section).queryByText("600001.SH")).toBeNull());
  });

  it("不可用时提示重试，不把未知名单当成空名单", async () => {
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByText("名单暂不可用，请稍后重试")).toBeInTheDocument();
    expect(within(section).queryByText("名单为空")).toBeNull();
    expect(within(section).getByRole("button", { name: "重试" })).toBeEnabled();
  });

  it("成员到期时间格式损坏时不猜成空名单", async () => {
    const invalid = list();
    const first = invalid.data.items[0];
    if (first === undefined) throw new Error("missing test stock");
    first.expires_at = "invalid-time";
    server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(invalid)));
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByText("名单暂不可用，请稍后重试")).toBeInTheDocument();
    expect(within(section).queryByText("暂无手动盯盘股票")).toBeNull();
  });

  it("成员到期后不在当前手动名单继续显示", async () => {
    const soon = list();
    const first = soon.data.items[0];
    if (first === undefined) throw new Error("missing test stock");
    first.expires_at = "2026-09-24T07:31:31Z";
    server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(soon)));
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByText("600001.SH")).toBeInTheDocument();
    await waitFor(() => expect(within(section).queryByText("600001.SH")).toBeNull(), {
      timeout: 2500,
    });
    expect(within(section).getByText("暂无手动盯盘股票")).toBeInTheDocument();
  });

  it("手动成员可直接移出，先用同代单股 GET 核对版本且不提前宣称完成", async () => {
    server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(list())));
    server.use(
      http.get("*/api/v1/watchlist/:code", () =>
        HttpResponse.json({
          serving: metaEnvelope().serving,
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:31:00Z",
            message: "",
            ts_code: "600001.SH",
            status: "active",
            version: 2,
            source: "detail",
            price_levels: ["10.00"],
            expires_at: null,
            updated_at: "2026-09-24T07:30:00Z",
          },
        }),
      ),
    );
    let sent: unknown;
    server.use(
      http.post("*/api/v1/watchlist/commands", async ({ request }) => {
        sent = await request.json();
        const body = sent as { command_id: string };
        return HttpResponse.json({
          command_id: body.command_id,
          ts_code: "600001.SH",
          action: "remove",
          status: "saved_syncing",
          version: 3,
          message: "已保存，正在同步。",
        });
      }),
    );
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    fireEvent.click(await within(section).findByRole("button", { name: "移出 600001.SH" }));
    expect(await within(section).findByText("已保存，正在同步")).toBeInTheDocument();
    expect(within(section).queryByText("已移出盯盘")).toBeNull();
    expect(sent).toMatchObject({ action: "remove", expected_version: 2, ts_code: "600001.SH" });
    expect(sent).not.toHaveProperty("price_levels");
    expect(sent).not.toHaveProperty("source");
  });

  it("从详情加入已发布且本代名单证实后，列表允许移出", async () => {
    const body = {
      action: "add",
      command_id: "web-prior",
      requested_at: "2026-09-24T07:30:00.000Z",
      generation_id: "b".repeat(64),
      ts_code: "600001.SH",
      expected_version: null,
      source: "detail",
      price_levels: [],
    };
    window.localStorage.setItem(
      `${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:600001.SH`,
      JSON.stringify({ schema: 1, body, status: "published", version: 2 }),
    );
    server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(list())));
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByRole("button", { name: "移出 600001.SH" })).toBeEnabled();
    expect(within(section).getByText("已加入")).toBeInTheDocument();
    expect(within(section).queryByText("已保存，正在同步")).toBeNull();
  });

  it.each(["add", "remove"] as const)(
    "名单行消失后仍可续查原 %s 命令，且只显示当前用户",
    async (action) => {
      const empty = list();
      empty.data.items = [];
      server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(empty)));
      const body = {
        action,
        command_id: `web-recover-${action}`,
        requested_at: "2026-09-24T07:30:00.000Z",
        generation_id: "b".repeat(64),
        ts_code: "600003.SH",
        expected_version: action === "add" ? null : 2,
        ...(action === "add" ? { source: "detail", price_levels: [] } : {}),
      };
      window.localStorage.setItem(
        `${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:600003.SH`,
        JSON.stringify({ schema: 1, body, status: "unknown", version: null }),
      );
      window.localStorage.setItem(
        `${MANUAL_WATCHLIST_JOURNAL_KEY}:other-user:600004.SH`,
        JSON.stringify({
          schema: 1,
          body: { ...body, ts_code: "600004.SH" },
          status: "unknown",
          version: null,
        }),
      );
      window.localStorage.setItem(`${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:600005.SH`, "{invalid");
      const sent: unknown[] = [];
      server.use(
        http.post("*/api/v1/watchlist/commands", async ({ request }) => {
          const requestBody = await request.json();
          sent.push(requestBody);
          return HttpResponse.json({
            command_id: body.command_id,
            ts_code: body.ts_code,
            action,
            status: "pending",
            version: null,
            message: "正在处理",
          });
        }),
      );
      const view = renderApp("/monitor");
      const section = await screen.findByRole("region", { name: "手动盯盘" });
      const recovery = await within(section).findByRole("list", { name: "待核对操作" });
      expect(within(recovery).getByText("600003.SH")).toBeInTheDocument();
      expect(within(section).queryByText("600004.SH")).toBeNull();
      await waitFor(() => expect(sent).toHaveLength(1));
      fireEvent.click(within(recovery).getByRole("button", { name: "继续核对 600003.SH" }));
      await waitFor(() => expect(sent).toHaveLength(2));
      expect(sent).toEqual([body, body]);
      expect(within(section).getByText("暂无手动盯盘股票")).toBeInTheDocument();
      await waitFor(() => expect(view.queryClient.isFetching({ queryKey: ["meta"] })).toBe(0));
      server.use(metaHandler(metaEnvelope({ viewer: "other-user" })));
      act(() => {
        view.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "other-user" }));
      });
      await waitFor(() => expect(within(section).queryByText("600003.SH")).toBeNull());
      expect(await within(section).findByText("600004.SH")).toBeInTheDocument();
    },
  );

  it("旧已发布移出被当前更高版本覆盖时，名单行按当前状态允许再次移出", async () => {
    const current = list();
    const item = current.data.items[0];
    if (!item) throw new Error("missing test stock");
    item.version = 4;
    window.localStorage.setItem(
      `${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:600001.SH`,
      JSON.stringify({
        schema: 1,
        body: {
          action: "remove",
          command_id: "web-prior",
          requested_at: "2026-09-24T07:30:00.000Z",
          generation_id: "b".repeat(64),
          ts_code: "600001.SH",
          expected_version: 2,
        },
        status: "published",
        version: 3,
      }),
    );
    server.use(http.get("*/api/v1/watchlist", () => HttpResponse.json(current)));
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "手动盯盘" });
    expect(await within(section).findByText("已加入")).toBeInTheDocument();
    expect(within(section).getByRole("button", { name: "移出 600001.SH" })).toBeEnabled();
    expect(within(section).queryByText("已保存，正在同步")).toBeNull();
  });
});
