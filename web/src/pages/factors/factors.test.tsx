import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
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
    content_sha256: "a".repeat(64),
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
    content_sha256: "b".repeat(64),
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
      can_archive: availability === "populated",
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
  it("清除被拒绝的命令后，延迟的目录刷新不续查旧命令", async () => {
    publish();
    let releaseMeta = () => {};
    const metaGate = new Promise<void>((resolve) => {
      releaseMeta = resolve;
    });
    let metaRequests = 0;
    let resumeRequests = 0;
    server.use(
      http.get("*/api/v1/meta", async () => {
        metaRequests += 1;
        if (metaRequests > 1) await metaGate;
        return HttpResponse.json(metaEnvelope());
      }),
      http.post("*/api/v1/factors/definitions/price_volume_factor/archive", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        return HttpResponse.json({
          data: {
            status: "rejected",
            command_id: body.command_id,
            factor_id: "price_volume_factor",
            version: 2,
            content_sha256: "a".repeat(64),
            current_head_updated: false,
            message: "归档未受理，请刷新后重试。",
          },
          serving: metaEnvelope().serving,
        });
      }),
      http.post("*/api/v1/factors/definitions/price_volume_factor/archive/resume", () => {
        resumeRequests += 1;
        return HttpResponse.json({ data: { status: "rejected" }, serving: metaEnvelope().serving });
      }),
    );
    renderApp("/factors");
    await screen.findByRole("button", { name: "归档" });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "归档" }));
    await user.click(screen.getByRole("button", { name: "确认归档" }));
    await screen.findByText("归档未受理，请刷新后重试。");
    await user.click(screen.getByRole("button", { name: "刷新当前版本" }));
    await waitFor(() => expect(metaRequests).toBe(2));
    await act(async () => releaseMeta());
    await waitFor(() => expect(screen.getByRole("button", { name: "归档" })).toBeVisible());
    expect(resumeRequests).toBe(0);
  });

  it("旧续查响应晚于新命令时不能覆盖新状态或清掉新命令", async () => {
    publish();
    let releaseMeta = () => {};
    const metaGate = new Promise<void>((resolve) => {
      releaseMeta = resolve;
    });
    let metaRequests = 0;
    let releaseOldResponse = () => {};
    const oldResponseGate = new Promise<void>((resolve) => {
      releaseOldResponse = resolve;
    });
    let submitRequests = 0;
    let resumeRequests = 0;
    server.use(
      http.get("*/api/v1/meta", async () => {
        metaRequests += 1;
        if (metaRequests > 1) await metaGate;
        return HttpResponse.json(metaEnvelope());
      }),
      http.post("*/api/v1/factors/definitions/price_volume_factor/archive", async ({ request }) => {
        submitRequests += 1;
        const body = (await request.json()) as { command_id: string };
        return HttpResponse.json({
          data: {
            status: "pending",
            command_id: body.command_id,
            factor_id: "price_volume_factor",
            version: 2,
            content_sha256: "a".repeat(64),
            current_head_updated: false,
            message: submitRequests === 1 ? "归档正在处理，请稍后查看。" : "新命令正在处理。",
          },
          serving: metaEnvelope().serving,
        });
      }),
      http.post(
        "*/api/v1/factors/definitions/price_volume_factor/archive/resume",
        async ({ request }) => {
          resumeRequests += 1;
          if (resumeRequests === 2) await oldResponseGate;
          const body = (await request.json()) as { command_id: string };
          return HttpResponse.json({
            data: {
              status: "rejected",
              command_id: body.command_id,
              factor_id: "price_volume_factor",
              version: 2,
              content_sha256: "a".repeat(64),
              current_head_updated: false,
              message: "归档未受理，请刷新后重试。",
            },
            serving: metaEnvelope().serving,
          });
        },
      ),
    );
    renderApp("/factors");
    await screen.findByRole("button", { name: "归档" });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "归档" }));
    await user.click(screen.getByRole("button", { name: "确认归档" }));
    await screen.findByText("归档正在处理，请稍后查看。");
    const refresh = screen.getByRole("button", { name: "刷新状态" });
    await act(async () => {
      fireEvent.click(refresh);
      fireEvent.click(refresh);
    });
    await waitFor(() => expect(resumeRequests).toBe(2));
    await screen.findByText("归档未受理，请刷新后重试。");
    await user.click(screen.getByRole("button", { name: "刷新当前版本" }));
    await waitFor(() => expect(metaRequests).toBe(2));
    await user.click(screen.getByRole("button", { name: "归档" }));
    await user.click(screen.getByRole("button", { name: "确认归档" }));
    await screen.findByText("新命令正在处理。");
    const newCommand = JSON.parse(
      window.localStorage.getItem("rquant.factor.archive-command.v1") ?? "{}",
    ) as {
      command: { command_id: string };
    };
    await act(async () => releaseOldResponse());
    expect(screen.getByText("新命令正在处理。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "刷新当前版本" })).toBeNull();
    expect(
      JSON.parse(window.localStorage.getItem("rquant.factor.archive-command.v1") ?? "{}") as {
        command: { command_id: string };
      },
    ).toEqual(newCommand);
    await act(async () => releaseMeta());
  });

  it("确认当前版本归档后保留原命令并续查发布", async () => {
    publish();
    const ids: string[] = [];
    server.use(
      http.post("*/api/v1/factors/definitions/price_volume_factor/archive", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        ids.push(body.command_id);
        return HttpResponse.json({
          data: {
            status: "succeeded_waiting_publication",
            command_id: body.command_id,
            factor_id: "price_volume_factor",
            version: 2,
            content_sha256: "a".repeat(64),
            current_head_updated: false,
            message: "已提交，等待更新。",
          },
          serving: metaEnvelope().serving,
        });
      }),
      http.post(
        "*/api/v1/factors/definitions/price_volume_factor/archive/resume",
        async ({ request }) => {
          const body = (await request.json()) as { command_id: string };
          ids.push(body.command_id);
          return HttpResponse.json({
            data: {
              status: "published",
              command_id: body.command_id,
              factor_id: "price_volume_factor",
              version: 2,
              content_sha256: "a".repeat(64),
              current_head_updated: false,
              message: "已归档，历史记录仍会保留。",
            },
            serving: metaEnvelope({ generationId: nextGeneration }).serving,
          });
        },
      ),
    );
    renderApp("/factors");
    await screen.findByRole("region", { name: "因子详情" });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "归档" }));
    expect(screen.getByText("归档当前定义，历史记录仍会保留。")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "确认归档" }));
    expect(await screen.findByText("已提交，等待更新。")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "刷新状态" }));
    expect(await screen.findByText("已归档，历史记录仍会保留。")).toBeInTheDocument();
    expect(ids).toHaveLength(2);
    expect(ids[0]).toBe(ids[1]);
    expect(screen.getByRole("button", { name: "继续查看因子" })).toBeDisabled();
    server.use(
      metaHandler(metaEnvelope({ generationId: nextGeneration })),
      http.get("*/api/v1/factors/definitions", () =>
        HttpResponse.json(
          catalog(
            [
              { ...definitions[0]!, archived: true },
              {
                ...definitions[1]!,
                archived: false,
                factor_id: "another_factor",
                name_zh: "新因子",
              },
            ],
            nextGeneration,
          ),
        ),
      ),
    );
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "继续查看因子" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "继续查看因子" }));
    await user.click(screen.getByRole("row", { name: /新因子/ }));
    expect(screen.getByRole("button", { name: "归档" })).toBeInTheDocument();
  });
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
