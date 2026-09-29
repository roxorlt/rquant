import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const firstGeneration = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const nextGeneration = "b".repeat(64);
const definitions: Schemas["FactorDefinitionItem"][] = [
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

type ResultItem = Schemas["FactorResultItem"];
type Research = Schemas["FactorResearchDisplay"];

const icSummary: Schemas["ICSeriesSummary"] = {
  status: "ok",
  mean: 0.0312,
  sample_std: 0.11,
  ir: 0.2836,
  positive_rate: 0.5,
  strong_signal_rate: 0.5,
  t_value: 0.4,
  p_value: 0.72,
  skewness: null,
  excess_kurtosis: null,
  source_day_count: 2,
  valid_day_count: 1,
  insufficient_day_count: 1,
  zero_variance_day_count: 0,
};

const research: Research = {
  basis_label: "收盘价到下一次调仓收盘价",
  pool_label: "固定样本",
  return_price_basis: "raw",
  holding_sessions: 5,
  summary_status: "evaluated",
  ic_summary: { normal_ic: icSummary, rank_ic: { ...icSummary, mean: -0.0142 } },
  coverage_days: [
    {
      decision_date: "2026-09-21",
      status: "evaluated",
      coverage: {
        expected_count: 4,
        valid_count: 3,
        factor_missing_count: 1,
        return_missing_count: 0,
        factor_missing_by_reason: [{ reason: "insufficient_history", count: 1 }],
        return_missing_by_reason: [],
      },
    },
    {
      decision_date: "2026-09-22",
      status: "no_samples",
      coverage: {
        expected_count: 4,
        valid_count: 0,
        factor_missing_count: 4,
        return_missing_count: 0,
        factor_missing_by_reason: [{ reason: "insufficient_history", count: 4 }],
        return_missing_by_reason: [],
      },
    },
  ],
  decay_periods: Array.from({ length: 10 }, (_, index) => ({
    lag: index + 1,
    status: index === 1 ? ("no_valid_days" as const) : ("evaluated" as const),
    ic_summary: index === 1 ? null : { normal_ic: icSummary, rank_ic: icSummary },
    source_day_count: 2,
    valid_pair_count: index === 1 ? 0 : 3,
  })),
  ic_points: [
    {
      decision_date: "2026-09-21",
      normal_ic: { status: "ok", value: 0.0312, source_sample_count: 4, effective_sample_count: 3 },
      rank_ic: { status: "ok", value: -0.0142, source_sample_count: 4, effective_sample_count: 3 },
      normal_ic_cumulative_sum: 0.0312,
      rank_ic_cumulative_sum: -0.0142,
    },
    {
      decision_date: "2026-09-22",
      normal_ic: {
        status: "insufficient_samples",
        value: null,
        source_sample_count: 4,
        effective_sample_count: 0,
      },
      rank_ic: {
        status: "insufficient_samples",
        value: null,
        source_sample_count: 4,
        effective_sample_count: 0,
      },
      normal_ic_cumulative_sum: null,
      rank_ic_cumulative_sum: null,
    },
  ],
  portfolio_status: "available_partial",
  portfolio_days: [
    {
      decision_at: "2026-09-21T07:00:00Z",
      decision_date: "2026-09-21",
      return_end_at: "2026-09-28T07:00:00Z",
      source_sample_count: 4,
      effective_sample_count: 3,
      groupings: [3, 5].map((count) => ({
        group_count: count,
        status: "ok" as const,
        source_sample_count: 4,
        effective_sample_count: 3,
        long_short_return: 0.02,
        long_short_cumulative_spread: 0.02,
        groups: Array.from({ length: count }, (_, index) => ({
          group_number: index + 1,
          member_count: 1,
          period_return: index * 0.01,
          cumulative_return: index * 0.01,
          target_weight_turnover: index === 1 ? null : 0.25,
        })),
      })),
    },
  ],
};

function result(jobId: string, overrides: Partial<ResultItem> = {}): ResultItem {
  return {
    job_id: jobId,
    factor_id: "price_volume_factor",
    factor_version: 2,
    factor_name_zh: "价量动量",
    definition_status: "current",
    status: "succeeded",
    status_label: "已完成",
    failure_message: null,
    updated_at: "2026-09-29T07:00:00Z",
    as_of_time: "2026-09-28T07:00:00Z",
    display_status: "available",
    display_message: "结果已发布。",
    ...overrides,
  };
}

function publishResults(
  items: ResultItem[],
  detailResearch: Research | null = research,
  generationId = firstGeneration,
) {
  server.use(
    http.get("*/api/v1/factors/results", () =>
      HttpResponse.json({
        data: { availability: "populated", available_at: "2026-09-29T07:00:00Z", results: items },
        serving: metaEnvelope({ generationId }).serving,
      }),
    ),
    http.get("*/api/v1/factors/results/:jobId", ({ params }) =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: "2026-09-29T07:00:00Z",
          result: items.find((item) => item.job_id === params.jobId) ?? null,
          research: detailResearch,
        },
        serving: metaEnvelope({ generationId }).serving,
      }),
    ),
  );
}

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
  it("只展示当前定义的最近成功检验，切换 IC 和分组并展开真实缺值", async () => {
    publish();
    const older = result("1".repeat(32), { updated_at: "2026-09-25T07:00:00Z" });
    const newest = result("2".repeat(32));
    publishResults([
      result("3".repeat(32), { factor_version: 1, updated_at: "2026-09-30T07:00:00Z" }),
      result("4".repeat(32), {
        definition_status: "historical_unavailable",
        updated_at: "2026-09-30T08:00:00Z",
      }),
      older,
      newest,
    ]);
    const { container } = renderApp("/factors");
    const area = await screen.findByRole("region", { name: "检验结果" });
    await waitFor(() => expect(area).toHaveTextContent("+0.0312"));
    expect(area).toHaveTextContent("历史回溯研究");
    expect(area).toHaveTextContent("固定样本");
    expect(area).toHaveTextContent("2026-09-21");
    expect(area).toHaveTextContent("5 个交易日");
    expect(area).toHaveTextContent("+0.0312");
    expect(within(area).getByText("标准差").nextElementSibling).toHaveTextContent("0.1100");
    expect(within(area).getByText("标准差").nextElementSibling).not.toHaveTextContent("+");
    expect(area).toHaveTextContent("部分日期可计算");
    expect(within(area).getByRole("img", { name: "IC 时序与累计 IC" })).toBeInTheDocument();
    expect(within(area).getByRole("img", { name: "分组累计收益" })).toBeInTheDocument();
    expect(within(area).getByRole("button", { name: "NormalIC" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const user = userEvent.setup();
    await user.click(within(area).getByRole("button", { name: "RankIC" }));
    expect(area).toHaveTextContent("−0.0142");
    await user.click(within(area).getByRole("button", { name: "5 组" }));
    expect(within(area).getByRole("button", { name: "5 组" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const icDisclosure = within(area).getByText("查看 IC 明细");
    await user.click(icDisclosure);
    expect(within(area).getByRole("table", { name: "IC 明细" })).toHaveTextContent("—");
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(area).not.toHaveTextContent("有效（");
    expect(area).not.toHaveTextContent("偏弱");
    expect(area).not.toHaveTextContent(newest.job_id);
  });

  it("可切换同一当前定义的历史检验；换因子和换代立即清空旧图", async () => {
    publish();
    const older = result("1".repeat(32), { updated_at: "2026-09-25T07:00:00Z" });
    const newest = result("2".repeat(32));
    publishResults([older, newest]);
    const view = renderApp("/factors");
    const area = await screen.findByRole("region", { name: "检验结果" });
    await waitFor(() => expect(area).toHaveTextContent("+0.0312"));
    const user = userEvent.setup();
    const runs = within(area).getByRole("table", { name: "最近检验" });
    const olderRow = within(runs).getAllByRole("row")[2];
    if (!olderRow) throw new Error("旧检验记录未显示");
    await user.click(olderRow);
    expect(olderRow).toHaveAttribute("aria-selected", "true");
    await user.click(screen.getByRole("row", { name: /成交变化/ }));
    expect(area).not.toHaveTextContent("+0.0312");
    expect(area).toHaveTextContent("还没有检验记录");
    await user.click(screen.getByRole("row", { name: /价量动量/ }));
    await waitFor(() => expect(area).toHaveTextContent("+0.0312"));
    server.use(metaHandler(metaEnvelope({ generationId: nextGeneration })));
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: nextGeneration })),
    );
    await waitFor(() => expect(screen.queryAllByText("+0.0312")).toHaveLength(0));
  });

  it("结果详情来自另一数据代时不沿用旧统计，并给出重新加载", async () => {
    publish();
    const item = result("1".repeat(32));
    publishResults([item], research, nextGeneration);
    renderApp("/factors");
    const area = await screen.findByRole("region", { name: "检验结果" });
    await within(area).findByText("数据已更新");
    expect(area).not.toHaveTextContent("0.0312");
    expect(within(area).getByRole("button", { name: "重新加载结果" })).toBeInTheDocument();
  });

  it("分组收益与换手明细在中间无样本日保留日期和缺值", async () => {
    publish();
    const firstDay = research.portfolio_days[0];
    const firstCoverage = research.coverage_days[0];
    if (!firstDay || !firstCoverage) throw new Error("缺少合成检验基准日");
    const withGap: Research = {
      ...research,
      coverage_days: [...research.coverage_days, { ...firstCoverage, decision_date: "2026-09-23" }],
      portfolio_days: [
        firstDay,
        {
          ...firstDay,
          decision_date: "2026-09-23",
          groupings: firstDay.groupings.map((grouping) => ({
            ...grouping,
            groups: grouping.groups.map((group) => ({
              ...group,
              cumulative_return: group.cumulative_return + 0.01,
              target_weight_turnover: 0.35,
            })),
          })),
        },
      ],
    };
    publishResults([result("1".repeat(32))], withGap);
    renderApp("/factors");
    const area = await screen.findByRole("region", { name: "检验结果" });
    await within(area).findByRole("img", { name: "分组累计收益" });
    const user = userEvent.setup();
    await user.click(within(area).getByText("查看分组收益明细"));
    await user.click(within(area).getByText("查看换手明细"));
    const returns = within(area).getByRole("table", { name: "分组收益明细" });
    const turnover = within(area).getByRole("table", { name: "换手明细" });
    const missingReturn = within(returns).getByRole("row", { name: /2026-09-22/ });
    const missingTurnover = within(turnover).getByRole("row", { name: /2026-09-22/ });
    expect(missingReturn).toHaveTextContent("—");
    expect(missingTurnover).toHaveTextContent("—");
    expect(within(returns).getAllByRole("row")).toHaveLength(4);
    expect(within(turnover).getAllByRole("row")).toHaveLength(4);
  });

  it.each([
    ["queued", "not_ready", "检验进行中"],
    ["running", "not_ready", "检验进行中"],
    ["failed", "not_ready", "本次检验未完成"],
    ["succeeded", "display_unavailable", "这次检验没有可展示的图表"],
    ["succeeded", "not_published", "这次结果尚未收录图表"],
  ] as const)("%s / %s 显示独立状态，不沿用图表", async (status, displayStatus, message) => {
    publish();
    publishResults([result("1".repeat(32), { status, display_status: displayStatus })]);
    const view = renderApp("/factors");
    const area = await screen.findByRole("region", { name: "检验结果" });
    await within(area).findByText(message);
    expect(within(area).queryByRole("img", { name: "IC 时序与累计 IC" })).toBeNull();
    expect(view.container.querySelector("main")?.textContent).not.toContain("1".repeat(32));
  });

  it("普通加载错误与同代但任务不匹配的详情均不显示旧统计", async () => {
    publish();
    const item = result("1".repeat(32));
    server.use(http.get("*/api/v1/factors/results", () => new HttpResponse(null, { status: 503 })));
    const view = renderApp("/factors");
    const area = await screen.findByRole("region", { name: "检验结果" });
    await within(area).findByText("检验结果暂时无法加载，请稍后重试。");
    publishResults([item]);
    server.use(
      http.get("*/api/v1/factors/results/:jobId", () =>
        HttpResponse.json({
          data: {
            availability: "ready",
            available_at: "2026-09-29T07:00:00Z",
            result: result("2".repeat(32)),
            research,
          },
          serving: metaEnvelope().serving,
        }),
      ),
    );
    await act(async () => view.queryClient.invalidateQueries({ queryKey: ["factors", "results"] }));
    await within(area).findByText("检验详情暂不可用");
    expect(within(area).queryByRole("img", { name: "IC 时序与累计 IC" })).toBeNull();
  });

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

  it("起日未知时显示待检验，提示保留在悬停层，已知日期标记为记录起日", async () => {
    publish(
      definitions.map((row, index) =>
        index === 0 ? { ...row, earliest_available_date: null } : row,
      ),
    );
    renderApp("/factors");
    const detail = await screen.findByRole("region", { name: "因子详情" });
    expect(within(detail).getByText("记录起日")).toBeInTheDocument();
    const unknown = within(detail).getByText("待检验");
    const user = userEvent.setup();
    await user.hover(unknown);
    expect(await screen.findByText("保存公式后，运行检验时核对实际数据起日")).toBeInTheDocument();
    await user.click(screen.getByRole("row", { name: /成交变化/ }));
    expect(within(detail).getByText("2025-03-04")).toBeInTheDocument();
    expect(within(detail).queryByText("待检验")).toBeNull();
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
