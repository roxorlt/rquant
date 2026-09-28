import { act, screen, waitFor, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

type List = Schemas["Envelope_ManualWatchlistListData_"];

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
});
