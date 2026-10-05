import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

const firstGeneration = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const nextGeneration = "b".repeat(64);
beforeEach(() => {
  server.use(
    http.get("*/api/v1/strategy-templates", () =>
      HttpResponse.json({
        data: { availability: "unavailable", available_at: null, templates: [], can_create: false },
        serving: metaEnvelope().serving,
      }),
    ),
    http.get("*/api/v1/strategy-templates/sources", () =>
      HttpResponse.json({
        data: {
          availability: "unavailable",
          pools: [],
          signals: [],
          conditions: [],
          comparison_fields: [],
          can_create: false,
        },
        serving: metaEnvelope().serving,
      }),
    ),
  );
});
const strategies: Schemas["StrategyCatalogItem"][] = [
  {
    strategy_id: "auction_gap",
    name: "竞价跳空",
    version: 2,
    registered_at: "2026-09-24T07:00:00Z",
    parameters: [{ key: "min_gap", label: "跳空幅度下限", display_value: "1.5%" }],
  },
  {
    strategy_id: "n_shape",
    name: "N 字形态",
    version: 1,
    registered_at: "2026-09-24T07:01:00Z",
    parameters: [{ key: "expires", label: "信号有效期", display_value: "120 秒" }],
  },
];

function catalog(
  items: Schemas["StrategyCatalogItem"][],
  generationId = firstGeneration,
  available = true,
): Schemas["Envelope_StrategyCatalogData_"] {
  return {
    data: { available, strategies: items },
    serving: metaEnvelope({ generationId }).serving,
  };
}

function publish(items = strategies, generationId = firstGeneration, available = true) {
  server.use(
    http.get("*/api/v1/strategies", () =>
      HttpResponse.json(catalog(items, generationId, available)),
    ),
  );
}

describe("策略目录", () => {
  it("只展示已核验定义，键盘选择后详情包含版本、登记时间和当前参数", async () => {
    publish();
    const user = userEvent.setup();
    const { container } = renderApp("/strategies");
    const list = await screen.findByRole("table", { name: "策略列表" });
    expect(within(list).getByText("竞价跳空")).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "当前参数" })).toHaveTextContent("1.5%");
    const next = within(list).getByRole("row", { name: /N 字形态/ });
    next.focus();
    await user.keyboard("{Enter}");
    const detail = screen.getByRole("table", { name: "当前参数" }).closest("section");
    expect(detail).not.toBeNull();
    expect(detail).toHaveTextContent("第 1 版");
    expect(detail).toHaveTextContent("登记时间");
    expect(within(detail as HTMLElement).getByText("120 秒")).toBeInTheDocument();
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(container.querySelector("main")?.textContent).not.toContain("n_shape");
    expect(container.querySelector("main")?.textContent).not.toContain("年化");
    expect(container.querySelector("main")?.textContent).not.toContain("晋级");
  });

  it("等待网页的数据代核验完成后才请求并展示策略", async () => {
    let releaseMeta = () => {};
    const metaGate = new Promise<void>((resolve) => {
      releaseMeta = resolve;
    });
    let catalogRequests = 0;
    server.use(
      http.get("*/api/v1/meta", async () => {
        await metaGate;
        return HttpResponse.json(metaEnvelope());
      }),
      http.get("*/api/v1/strategies", () => {
        catalogRequests += 1;
        return HttpResponse.json(catalog(strategies));
      }),
    );
    renderApp("/strategies");
    expect(await screen.findByRole("heading", { level: 1, name: "策略" })).toBeInTheDocument();
    expect(screen.getByRole("status", { name: "正在加载策略目录" })).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "策略列表" })).toBeNull();
    expect(catalogRequests).toBe(0);
    releaseMeta();
    expect(await screen.findByRole("table", { name: "策略列表" })).toBeInTheDocument();
    expect(catalogRequests).toBe(1);
  });

  it("数据代变化后清除旧选择，仅展示新代核验的策略详情", async () => {
    server.use(
      http.get("*/api/v1/strategies", ({ request }) => {
        const generationId = new URL(request.url).searchParams.get("generation_id");
        const next = generationId === nextGeneration;
        return HttpResponse.json(
          catalog(
            next ? strategies : [...strategies].reverse(),
            next ? nextGeneration : firstGeneration,
          ),
        );
      }),
    );
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    const list = await screen.findByRole("table", { name: "策略列表" });
    await user.click(within(list).getByRole("row", { name: /N 字形态/ }));
    expect(screen.getByRole("table", { name: "当前参数" })).toHaveTextContent("120 秒");

    server.use(metaHandler(metaEnvelope({ generationId: nextGeneration })));
    act(() => {
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: nextGeneration }));
    });
    await waitFor(() =>
      expect(screen.getByRole("table", { name: "当前参数" })).toHaveTextContent("1.5%"),
    );
    expect(screen.getByRole("table", { name: "当前参数" })).not.toHaveTextContent("120 秒");
  });

  it("来源不可用或请求失败时隐藏详情，并可重试", async () => {
    publish([], firstGeneration, false);
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    expect(await screen.findByText("策略目录暂时不可用")).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "当前参数" })).toBeNull();

    server.use(http.get("*/api/v1/strategies", () => new HttpResponse(null, { status: 503 })));
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["strategies", "catalog"] });
    });
    expect(await screen.findByText("策略目录暂时无法加载，请稍后重试。")).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "当前参数" })).toBeNull();
    publish();
    await user.click(screen.getByRole("button", { name: "重新加载" }));
    expect(await screen.findByRole("table", { name: "策略列表" })).toBeInTheDocument();
  });

  it("网页状态轮询失败后隐藏旧详情，恢复时重新核对", async () => {
    publish();
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    expect(await screen.findByRole("table", { name: "当前参数" })).toHaveTextContent("1.5%");

    server.use(http.get("*/api/v1/meta", () => new HttpResponse(null, { status: 503 })));
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["meta"] });
    });
    expect(await screen.findByText("策略目录暂时无法核对，请稍后重试。")).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "策略列表" })).toBeNull();
    expect(screen.queryByRole("table", { name: "当前参数" })).toBeNull();

    server.use(metaHandler(metaEnvelope()));
    await user.click(screen.getByRole("button", { name: "重新加载" }));
    expect(await screen.findByRole("table", { name: "当前参数" })).toHaveTextContent("1.5%");
  });
});
