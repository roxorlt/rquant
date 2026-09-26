import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { tasksEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server, tasksHandler } from "@/test/server";

const second = {
  job_id: "00000000-0000-0000-0000-000000000002",
  strategy_name: "历史回放",
  job_type_label: "策略回放",
  resource_label: "快速",
  status: { state: "waiting" as const, label: "排队中", reason: "正在等待研究资源" },
  progress_fraction: 0,
  terminal_shards: 0,
  total_shards: 4,
  eta_at: null,
  eta_low: null,
  eta_high: null,
  eta_label: "排队中",
  updated_at: "2026-09-24T07:29:00Z",
};

describe("研究任务队列", () => {
  it("shows published counts, progress and ETA without technical IDs or fake controls", async () => {
    const user = userEvent.setup();
    renderApp("/tasks");
    const table = await screen.findByRole("table", { name: "研究任务队列" });
    expect(table).toHaveTextContent("动量参数搜索");
    expect(table).toHaveTextContent("参数搜索");
    expect(table).toHaveTextContent("运行中");
    expect(table).toHaveTextContent("25%");
    expect(table).toHaveTextContent("1 / 4");
    expect(table).toHaveTextContent("15:35");
    expect(screen.getByRole("region", { name: "任务状态概况" })).toHaveTextContent("运行中1");
    expect(screen.getByRole("region", { name: "任务状态概况" })).toHaveTextContent("排队中1");
    expect(table).not.toHaveTextContent("00000000-0000-0000-0000-000000000001");
    expect(screen.queryByRole("button", { name: /暂停|重试|日志|立即运行/ })).toBeNull();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
    await user.hover(within(table).getByText("动量参数搜索"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "00000000-0000-0000-0000-000000000001",
    );
  });

  it("pages in order and refreshes from the first page", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/jobs", ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        requests.push(cursor ?? "first");
        return HttpResponse.json(
          cursor ? tasksEnvelope({ items: [second], next_cursor: null }) : tasksEnvelope(),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await screen.findByRole("table", { name: "研究任务队列" });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await screen.findByText("第 2 页");
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("历史回放");
    expect(requests).toEqual(["first", "fixture-next"]);
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await screen.findByText("第 1 页");
    await waitFor(() => expect(requests.at(-1)).toBe("first"));
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("动量参数搜索");
  });

  it("drops old counts and cursor after a generation change", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/jobs", ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        requests.push(cursor ?? "first");
        if (cursor) {
          return HttpResponse.json(
            { detail: "任务数据已更新，请从第一页重新查看。" },
            { status: 409 },
          );
        }
        return HttpResponse.json(tasksEnvelope());
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await screen.findByRole("table", { name: "研究任务队列" });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await screen.findByText("数据已更新，请从第一页重新查看。");
    expect(screen.queryByRole("region", { name: "任务状态概况" })).toBeNull();
    expect(screen.queryByRole("table", { name: "研究任务队列" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "返回最新" }));
    await screen.findByRole("table", { name: "研究任务队列" });
    expect(requests).toEqual(["first", "fixture-next", "first"]);
  });

  it.each([
    ["not_published", "研究任务尚未发布", null],
    ["empty", "还没有研究任务", 0],
    ["unavailable", "暂时读不到页面数据", null],
  ] as const)("explains %s without inventing a zero count", async (state, label, total) => {
    server.use(
      tasksHandler(
        tasksEnvelope({
          source_state: state,
          source_label: label,
          total,
          counts: total === null ? null : tasksEnvelope().data.counts,
          items: [],
          next_cursor: null,
        }),
      ),
    );
    renderApp("/tasks");
    expect(await screen.findByText(label)).toBeInTheDocument();
    if (total === null) {
      expect(screen.queryByRole("region", { name: "任务状态概况" })).toBeNull();
    }
  });

  it("shows delayed data and offers a real retry when loading fails", async () => {
    const delayed = tasksEnvelope({
      source_note: "研究任务数据更新延迟，以下记录可能不是最新的。",
    });
    server.use(tasksHandler(delayed));
    const user = userEvent.setup();
    renderApp("/tasks");
    expect(
      await screen.findByText("研究任务数据更新延迟，以下记录可能不是最新的。"),
    ).toBeInTheDocument();

    server.use(http.get("*/api/v1/tasks/jobs", () => HttpResponse.error()));
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await screen.findByText("研究任务暂时无法加载")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "任务状态概况" })).toBeNull();
  });
});
