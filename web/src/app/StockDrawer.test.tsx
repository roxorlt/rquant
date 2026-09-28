import { act, render, screen, waitFor, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { fetchMeta } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { metaHandler, server } from "@/test/server";
import { StockDrawer } from "./StockDrawer";

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

describe("个股抽屉手动盯盘只读状态", () => {
  it.each([
    ["active", "已加入盯盘"],
    ["expired", "已到期"],
    ["deleted", "已移出盯盘"],
    ["absent", "尚未加入"],
  ] as const)("显示可信的 %s 状态且不开放写按钮", async (status, label) => {
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
    expect(within(drawer).queryByRole("button", { name: "加入盯盘" })).toBeNull();
    expect(within(drawer).queryByRole("button", { name: "移出盯盘" })).toBeNull();
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
});
