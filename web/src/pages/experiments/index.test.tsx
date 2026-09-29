import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

const serving = metaEnvelope().serving;
const first: Schemas["ExperimentItem"] = {
  experiment_id: "a".repeat(64),
  hypothesis_family: "均线研究",
  registered_at: "2026-09-24T07:20:00Z",
  status: "succeeded",
  completed_at: "2026-09-24T07:25:00Z",
  trade_count: 12,
  net_return_pct: 7.5,
  max_drawdown_pct: 3.25,
  win_rate_pct: 60,
};
const second: Schemas["ExperimentItem"] = {
  experiment_id: "b".repeat(64),
  hypothesis_family: "突破研究",
  registered_at: "2026-09-23T07:20:00Z",
  status: "registered",
  completed_at: null,
  trade_count: null,
  net_return_pct: null,
  max_drawdown_pct: null,
  win_rate_pct: null,
};

describe("实验记录", () => {
  it("元数据未返回前不请求或展示实验，核验后只请求当前数据代", async () => {
    const generation = "f".repeat(64);
    let releaseMeta = () => {};
    const metaGate = new Promise<void>((resolve) => {
      releaseMeta = resolve;
    });
    const requests: (string | null)[] = [];
    server.use(
      http.get("*/api/v1/meta", async () => {
        await metaGate;
        return HttpResponse.json(metaEnvelope({ generationId: generation }));
      }),
      http.get("*/api/v1/experiments", ({ request }) => {
        requests.push(new URL(request.url).searchParams.get("generation_id"));
        return HttpResponse.json({
          data: {
            available: true,
            items: [first],
            retained_count: 1,
            truncated: false,
            oldest_registered_at: first.registered_at,
            next_cursor: null,
          },
          serving: metaEnvelope({ generationId: generation }).serving,
        });
      }),
    );
    renderApp("/experiments");
    expect(await screen.findByRole("heading", { name: "实验记录", level: 1 })).toBeVisible();
    expect(await screen.findByRole("status", { name: "正在加载实验记录" })).toBeVisible();
    expect(screen.queryByRole("table", { name: "实验记录" })).not.toBeInTheDocument();
    expect(requests).toEqual([]);

    releaseMeta();
    expect(await screen.findByRole("table", { name: "实验记录" })).toBeVisible();
    expect(requests).toEqual([generation]);
  });

  it("元数据没有可用数据代时不请求或展示实验", async () => {
    const noGeneration = metaEnvelope();
    noGeneration.data.generation = null;
    noGeneration.serving = { ...noGeneration.serving, generation_id: null, state: "unavailable" };
    const requests: string[] = [];
    server.use(
      metaHandler(noGeneration),
      http.get("*/api/v1/experiments", ({ request }) => {
        requests.push(request.url);
        return HttpResponse.json({
          data: {
            available: true,
            items: [first],
            retained_count: 1,
            truncated: false,
            oldest_registered_at: first.registered_at,
            next_cursor: null,
          },
          serving,
        });
      }),
    );
    renderApp("/experiments");
    expect(await screen.findByText("实验记录暂时不可用")).toBeVisible();
    expect(screen.queryByRole("table", { name: "实验记录" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "实验对比" })).not.toBeInTheDocument();
    expect(requests).toEqual([]);
  });

  it("展示真实结果、未发布字段和稳定分页，不把内部编号放进正文", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/experiments", ({ request }) => {
        const url = new URL(request.url);
        requests.push(url.search);
        const later = url.searchParams.has("cursor");
        return HttpResponse.json({
          data: {
            available: true,
            items: later ? [second] : [first],
            retained_count: 2,
            truncated: false,
            oldest_registered_at: second.registered_at,
            next_cursor: later ? null : "opaque-next",
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    const { container } = renderApp("/experiments");

    expect(await screen.findByRole("heading", { name: "实验记录", level: 1 })).toBeVisible();
    const table = await screen.findByRole("table", { name: "实验记录" });
    expect(within(table).getByText("均线研究")).toBeVisible();
    expect(within(table).getByText("+7.50%")).toBeVisible();
    expect(within(table).getByText("12")).toBeVisible();
    expect(screen.getByText("仅展示已发布的结果")).toBeVisible();
    expect(screen.queryByRole("button", { name: "新建实验" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "对比所选" })).toBeDisabled();
    expect(container.querySelector("main")?.textContent).not.toContain(first.experiment_id);
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);

    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByText("突破研究")).toBeVisible();
    expect(screen.queryByText("均线研究")).not.toBeInTheDocument();
    expect(requests.at(-1)).toContain("cursor=opaque-next");
    await user.click(screen.getByRole("button", { name: "上一页" }));
    expect(await screen.findByText("均线研究")).toBeVisible();
  });

  it("区分未发布、可信空记录和最近窗口截断", async () => {
    server.use(
      http.get("*/api/v1/experiments", () =>
        HttpResponse.json({
          data: {
            available: false,
            items: [],
            retained_count: 0,
            truncated: false,
            oldest_registered_at: null,
            next_cursor: null,
          },
          serving,
        }),
      ),
    );
    const { unmount } = renderApp("/experiments");
    expect(await screen.findByText("实验记录暂时读不到")).toBeVisible();
    unmount();

    server.use(
      http.get("*/api/v1/experiments", () =>
        HttpResponse.json({
          data: {
            available: true,
            items: [],
            retained_count: 0,
            truncated: false,
            oldest_registered_at: null,
            next_cursor: null,
          },
          serving,
        }),
      ),
    );
    const empty = renderApp("/experiments");
    expect(await screen.findByText("还没有登记的实验")).toBeVisible();
    empty.unmount();

    server.use(
      http.get("*/api/v1/experiments", () =>
        HttpResponse.json({
          data: {
            available: true,
            items: [first],
            retained_count: 500,
            truncated: true,
            oldest_registered_at: first.registered_at,
            next_cursor: null,
          },
          serving,
        }),
      ),
    );
    renderApp("/experiments");
    expect(await screen.findByText(/仅显示最近 500 条实验/)).toHaveTextContent(
      "仅显示最近 500 条实验 · 最早登记于 2026-09-24 15:20（北京时间）",
    );
  });

  it("覆盖起点无效时不显示错误日期", async () => {
    server.use(
      http.get("*/api/v1/experiments", () =>
        HttpResponse.json({
          data: {
            available: true,
            items: [first],
            retained_count: 500,
            truncated: true,
            oldest_registered_at: "invalid-time",
            next_cursor: null,
          },
          serving,
        }),
      ),
    );
    renderApp("/experiments");
    expect(await screen.findByText(/仅显示最近 500 条实验/)).toHaveTextContent(
      "仅显示最近 500 条实验 · 覆盖起点暂不可用",
    );
    expect(screen.queryByText(/Invalid Date|NaN/)).not.toBeInTheDocument();
  });

  it("跨页选两条后只并排展示已发布的指标，并可移除选择", async () => {
    server.use(
      http.get("*/api/v1/experiments", ({ request }) =>
        HttpResponse.json({
          data: {
            available: true,
            items: new URL(request.url).searchParams.has("cursor") ? [second] : [first],
            retained_count: 2,
            truncated: false,
            oldest_registered_at: second.registered_at,
            next_cursor: new URL(request.url).searchParams.has("cursor") ? null : "next",
          },
          serving,
        }),
      ),
    );
    const user = userEvent.setup();
    const { container } = renderApp("/experiments");
    const firstTable = await screen.findByRole("table", { name: "实验记录" });
    await user.click(within(firstTable).getByRole("checkbox", { name: "选择均线研究" }));
    await user.click(screen.getByRole("button", { name: "下一页" }));
    const secondTable = await screen.findByRole("table", { name: "实验记录" });
    await user.click(within(secondTable).getByRole("checkbox", { name: "选择突破研究" }));
    await user.click(screen.getByRole("button", { name: "对比所选" }));

    const comparison = screen.getByRole("region", { name: "实验对比" });
    expect(within(comparison).getByText("均线研究")).toBeVisible();
    expect(within(comparison).getByText("突破研究")).toBeVisible();
    expect(within(comparison).getByText("+7.50%")).toBeVisible();
    expect(within(comparison).getAllByText("—").length).toBeGreaterThan(0);
    expect(within(comparison).getByText(/暂不计算差值/)).toBeVisible();
    expect(container.querySelector("main")?.textContent).not.toContain(first.experiment_id);

    await user.click(screen.getByRole("button", { name: "移除突破研究" }));
    expect(screen.queryByRole("region", { name: "实验对比" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "对比所选" })).toBeDisabled();
  });

  it("已核验的数据代变化时直接隐藏旧记录与对比，再展示新代结果", async () => {
    const firstGeneration = serving.generation_id;
    const nextGeneration = "f".repeat(64);
    let releaseNext = () => {};
    const nextGate = new Promise<void>((resolve) => {
      releaseNext = resolve;
    });
    const requests: (string | null)[] = [];
    server.use(
      http.get("*/api/v1/experiments", async ({ request }) => {
        const generation = new URL(request.url).searchParams.get("generation_id");
        requests.push(generation);
        if (generation === nextGeneration) await nextGate;
        return HttpResponse.json({
          data: {
            available: true,
            items: generation === nextGeneration ? [second] : [first, second],
            retained_count: generation === nextGeneration ? 1 : 2,
            truncated: false,
            oldest_registered_at: second.registered_at,
            next_cursor: null,
          },
          serving: { ...serving, generation_id: generation },
        });
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/experiments");
    await waitFor(() => expect(requests).toContain(firstGeneration));
    const table = await screen.findByRole("table", { name: "实验记录" });
    await user.click(within(table).getByRole("checkbox", { name: "选择均线研究" }));
    await user.click(within(table).getByRole("checkbox", { name: "选择突破研究" }));
    await user.click(screen.getByRole("button", { name: "对比所选" }));
    expect(screen.getByRole("region", { name: "实验对比" })).toBeVisible();

    const changed = metaEnvelope({ generationId: nextGeneration });
    server.use(metaHandler(changed));
    act(() => queryClient.setQueryData(META_QUERY_KEY, changed));
    expect(await screen.findByRole("status", { name: "正在加载实验记录" })).toBeVisible();
    expect(screen.queryByRole("table", { name: "实验记录" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "实验对比" })).not.toBeInTheDocument();
    releaseNext();
    expect(await screen.findByRole("table", { name: "实验记录" })).toHaveTextContent("突破研究");
    expect(screen.getByRole("table", { name: "实验记录" })).not.toHaveTextContent("均线研究");
    expect(screen.getByRole("button", { name: "对比所选" })).toBeDisabled();
    expect(requests).toEqual([firstGeneration, nextGeneration]);
  });

  it("元数据首次成功后轮询失败时隐藏旧实验对比，恢复后可重新加载", async () => {
    server.use(
      http.get("*/api/v1/experiments", () =>
        HttpResponse.json({
          data: {
            available: true,
            items: [first, second],
            retained_count: 2,
            truncated: false,
            oldest_registered_at: second.registered_at,
            next_cursor: null,
          },
          serving,
        }),
      ),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/experiments");
    const table = await screen.findByRole("table", { name: "实验记录" });
    await user.click(within(table).getByRole("checkbox", { name: "选择均线研究" }));
    await user.click(within(table).getByRole("checkbox", { name: "选择突破研究" }));
    await user.click(screen.getByRole("button", { name: "对比所选" }));
    expect(screen.getByRole("region", { name: "实验对比" })).toBeVisible();

    server.use(http.get("*/api/v1/meta", () => new HttpResponse(null, { status: 503 })));
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: META_QUERY_KEY });
    });
    expect(await screen.findByText("实验记录暂时无法核对，请稍后重试。")).toBeVisible();
    expect(screen.queryByRole("table", { name: "实验记录" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "实验对比" })).not.toBeInTheDocument();

    server.use(metaHandler(metaEnvelope()));
    await user.click(screen.getByRole("button", { name: "重新加载" }));
    expect(await screen.findByRole("table", { name: "实验记录" })).toBeVisible();
  });
});
