import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

const firstGeneration = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const nextGeneration = "b".repeat(64);
const definitions = [
  {
    factor_id: "price_volume_factor",
    name_zh: "价量动量",
    category_label: "技术",
    direction: "higher_is_better",
    direction_label: "偏好高值",
    version: 2,
    earliest_available_date: "2024-01-02",
    archived: false,
    expression: "ts_mean(close, 5) / ref(volume, 2)",
    dependency_columns: ["close", "volume"],
    max_history_window: 5,
  },
  {
    factor_id: "old_factor",
    name_zh: "成交变化",
    category_label: "技术",
    direction: "lower_is_better",
    direction_label: "偏好低值",
    version: 1,
    earliest_available_date: "2025-03-04",
    archived: true,
    expression: "ts_mean(volume, 3)",
    dependency_columns: ["volume"],
    max_history_window: 3,
  },
];

function catalog(
  rows = definitions,
  generationId = firstGeneration,
  availability: "populated" | "empty" | "unavailable" = "populated",
) {
  return {
    data: {
      availability,
      available_at: availability === "unavailable" ? null : "2026-09-24T07:31:00Z",
      definitions: rows,
    },
    serving: metaEnvelope({ generationId }).serving,
  };
}

function publish(
  rows = definitions,
  availability: "populated" | "empty" | "unavailable" = "populated",
) {
  server.use(
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json(catalog(rows, firstGeneration, availability)),
    ),
  );
}

describe("因子库", () => {
  it("只读已发布定义，可用键盘选择归档因子，正文无内部标识或假结果", async () => {
    publish();
    const { container } = renderApp("/factors");
    const list = await screen.findByRole("table", { name: "因子列表" });
    expect(within(list).getByText("价量动量")).toBeInTheDocument();
    expect(screen.getByText("ts_mean(close, 5) / ref(volume, 2)")).toBeInTheDocument();
    const archived = within(list).getByRole("row", { name: /成交变化/ });
    archived.focus();
    await userEvent.setup().keyboard("{Enter}");
    const detail = screen.getByRole("region", { name: "因子详情" });
    expect(detail).toHaveTextContent("成交变化");
    expect(detail).toHaveTextContent("已归档");
    expect(detail).toHaveTextContent("ts_mean(volume, 3)");
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(container.querySelector("main")?.textContent).not.toContain("old_factor");
    expect(screen.queryByRole("button", { name: /运行检验|加入跟踪/ })).toBeNull();
    expect(screen.queryByText(/IC|分组收益|换手/)).toBeNull();
  });

  it("先核对数据代再加载，换代后清除旧详情", async () => {
    let releaseMeta = () => {};
    const gate = new Promise<void>((resolve) => {
      releaseMeta = resolve;
    });
    let requests = 0;
    server.use(
      http.get("*/api/v1/meta", async () => {
        await gate;
        return HttpResponse.json(metaEnvelope());
      }),
      http.get("*/api/v1/factors/definitions", ({ request }) => {
        requests += 1;
        const next = new URL(request.url).searchParams.get("generation_id") === nextGeneration;
        return HttpResponse.json(
          catalog(
            next ? definitions.slice(0, 1) : definitions,
            next ? nextGeneration : firstGeneration,
          ),
        );
      }),
    );
    const view = renderApp("/factors");
    expect(await screen.findByRole("status", { name: "正在加载因子库" })).toBeInTheDocument();
    expect(requests).toBe(0);
    releaseMeta();
    const list = await screen.findByRole("table", { name: "因子列表" });
    await userEvent.setup().click(within(list).getByRole("row", { name: /成交变化/ }));
    expect(screen.getByRole("region", { name: "因子详情" })).toHaveTextContent("成交变化");
    server.use(metaHandler(metaEnvelope({ generationId: nextGeneration })));
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: nextGeneration })),
    );
    await waitFor(() =>
      expect(screen.getByRole("region", { name: "因子详情" })).toHaveTextContent("价量动量"),
    );
    expect(screen.getByRole("region", { name: "因子详情" })).not.toHaveTextContent("成交变化");
  });

  it.each(["重新加载", "刷新"])("因子 GET 409 后点击%s先核对新数据代并恢复列表", async (action) => {
    let servingGeneration = firstGeneration;
    const requested: string[] = [];
    const nextDefinitions = definitions.map((item, index) =>
      index === 0
        ? { ...item, name_zh: "新版价量动量", expression: "ts_mean(close, 8)" }
        : { ...item, name_zh: "新版成交变化" },
    );
    server.use(
      http.get("*/api/v1/meta", () => {
        requested.push(`meta:${servingGeneration}`);
        return HttpResponse.json(metaEnvelope({ generationId: servingGeneration }));
      }),
      http.get("*/api/v1/factors/definitions", ({ request }) => {
        const requestedGeneration = new URL(request.url).searchParams.get("generation_id");
        requested.push(`catalog:${requestedGeneration}`);
        if (requestedGeneration !== servingGeneration) {
          return new HttpResponse(null, { status: 409 });
        }
        return HttpResponse.json(
          catalog(
            servingGeneration === firstGeneration ? definitions : nextDefinitions,
            servingGeneration,
          ),
        );
      }),
    );
    const user = userEvent.setup();
    const view = renderApp("/factors");
    const list = await screen.findByRole("table", { name: "因子列表" });
    await user.click(within(list).getByRole("row", { name: /成交变化/ }));
    expect(screen.getByRole("region", { name: "因子详情" })).toHaveTextContent("成交变化");

    servingGeneration = nextGeneration;
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["factors", "definitions"] });
    });
    expect(await screen.findByText("数据已更新，请重新查看因子。")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "因子详情" })).toBeNull();
    const beforeRetry = requested.length;
    await user.click(screen.getByRole("button", { name: action }));
    await waitFor(() =>
      expect(screen.getByRole("region", { name: "因子详情" })).toHaveTextContent("新版价量动量"),
    );
    expect(screen.getByRole("region", { name: "因子详情" })).not.toHaveTextContent("新版成交变化");
    expect(requested.slice(beforeRetry)[0]).toBe(`meta:${nextGeneration}`);
    expect(requested.slice(beforeRetry).filter((entry) => entry.startsWith("catalog:"))).toEqual(
      expect.arrayContaining([`catalog:${nextGeneration}`]),
    );
    expect(requested.slice(beforeRetry)).not.toContain(`catalog:${firstGeneration}`);
  });

  it("未发布、可信空库和错误分别给出可操作反馈", async () => {
    publish([], "unavailable");
    const user = userEvent.setup();
    const view = renderApp("/factors");
    expect(await screen.findByText("因子库暂时无法查看")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "因子详情" })).toBeNull();
    publish([], "empty");
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["factors", "definitions"] });
    });
    expect(await screen.findByText("还没有因子")).toBeInTheDocument();
    server.use(
      http.get("*/api/v1/factors/definitions", () => new HttpResponse(null, { status: 503 })),
    );
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["factors", "definitions"] });
    });
    expect(await screen.findByText("因子库暂时无法加载，请稍后重试。")).toBeInTheDocument();
    publish();
    await user.click(screen.getByRole("button", { name: "重新加载" }));
    expect(await screen.findByRole("table", { name: "因子列表" })).toBeInTheDocument();
  });
});
