import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { portfolioCapabilities } from "../backtest/portfolio.fixture";
import { experimentFixture as fixture } from "./formal.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

// jsdom leaves nested portal motion in its prepare state; the real browser suite
// keeps the original motion and checks visible modal controls and focus.
vi.mock("@/ui/theme", async (original) => {
  const theme = await original<typeof import("@/ui/theme")>();
  return {
    ...theme,
    antdThemeFor: (...args: Parameters<typeof theme.antdThemeFor>) => {
      const value = theme.antdThemeFor(...args);
      return { ...value, token: { ...value.token, motion: false } };
    },
  };
});
afterEach(() => vi.unstubAllEnvs());

function sealed() {
  server.use(
    metaHandler(metaEnvelope({ viewer: "alice", generationId: fixture.generation_id })),
    http.get("*/api/v1/experiments/capabilities", () => HttpResponse.json(fixture.capabilities)),
    http.get("*/api/v1/experiments/mine", () => HttpResponse.json(fixture.mine)),
    http.get("*/api/v1/experiments/families/:family", () => HttpResponse.json(fixture.family)),
    http.get("*/api/v1/experiments/results/:experiment", ({ params }) =>
      HttpResponse.json(fixture.results[String(params.experiment)]),
    ),
    http.get("*/api/v1/experiments/results/:experiment/statistics", ({ params }) =>
      HttpResponse.json(fixture.statistics[String(params.experiment)]),
    ),
    http.get("*/api/v1/experiments/families/:family/heatmap", () =>
      HttpResponse.json(fixture.heatmap),
    ),
    http.get("*/api/v1/experiments/compare", () => HttpResponse.json(fixture.comparison)),
  );
}

const meta = metaEnvelope({ viewer: "alice" });
const config = portfolioCapabilities.data.default_config;
if (!config) throw new Error("fixture config missing");
const caps: Schemas["ExperimentCapabilities"] = {
  available: true,
  can_search: true,
  can_search_templates: false,
  can_unseal: true,
  can_edit_policy: true,
  message: null,
  default_config: config,
  policy: { version: 1, months: 0, updated_at: "2026-10-05T08:00:00Z" },
  sources: [
    {
      key: "verified-screen",
      version: 1,
      label: "每日候选",
      start_date: "2026-08-10",
      end_date: "2026-08-17",
      trading_dates: [
        "2026-08-07",
        "2026-08-10",
        "2026-08-11",
        "2026-08-12",
        "2026-08-13",
        "2026-08-14",
        "2026-08-17",
      ],
      available: true,
      message: null,
    },
  ],
};
const item = (index: number): Schemas["ExperimentAttemptRow"] => ({
  experiment_id: String(index).repeat(64),
  family_id: "experiment-search:abc",
  family_name: "仓位研究",
  phase: "search",
  registered_at: "2026-10-05T08:00:00Z",
  status: "registered",
  label: "已登记",
  index,
  configuration: config,
  job_id: `00000000-0000-0000-0000-${String(index).padStart(12, "0")}`,
  result_hash: null,
  message: null,
  cancellation_pending: false,
  strategy_name: "组合回测",
  strategy_version: 1,
  metrics:
    fixture.family.data.items[0]?.metrics?.map((metric) => ({ ...metric, value: null })) ?? [],
});

function ready(items = [item(1)]) {
  server.use(
    metaHandler(meta),
    http.get("*/api/v1/experiments/capabilities", () =>
      HttpResponse.json({ data: caps, serving: meta.serving }),
    ),
    http.get("*/api/v1/experiments/mine", () =>
      HttpResponse.json({
        data: {
          available: true,
          items,
          retained_count: items.length,
          truncated: false,
          oldest_registered_at: items[0]?.registered_at ?? null,
          next_cursor: null,
        },
        serving: meta.serving,
      }),
    ),
  );
}

describe("正式实验", () => {
  it("逐日净值正文显示四位小数，提示保留原值", async () => {
    sealed();
    renderApp("/experiments");
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await userEvent.click(within(mine).getByRole("button", { name: "仓位实验 · 1" }));
    expect(await screen.findByRole("img", { name: "实验与基准净值" })).toBeVisible();
    await userEvent.click(screen.getByText("逐日净值"));
    const table = await screen.findByRole("table", { name: "实验1逐日净值" });
    const result = fixture.results[fixture.family.data.items[0]?.experiment_id ?? ""];
    const point = result?.data.curves[0];
    if (!point) throw new Error("original normalized daily value is missing");
    const row = within(table).getAllByRole("row")[1];
    if (!row) throw new Error("original daily row is missing");
    const cells = within(row).getAllByRole("cell");
    expect(cells[1]).toHaveTextContent("0.9983");
    expect(table.textContent).not.toContain(String(point.nav));
    for (const cell of cells) expect(findJargon(cell.textContent ?? "")).toEqual([]);
    if (point.benchmark_nav != null)
      expect(cells[3]).toHaveTextContent(
        point.benchmark_nav.toLocaleString("zh-CN", {
          minimumFractionDigits: 4,
          maximumFractionDigits: 4,
        }),
      );
    const anchor = cells[1]?.querySelector<HTMLElement>(".tip-anchor");
    if (!anchor) throw new Error("original precision tip is missing");
    await userEvent.hover(anchor);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(`完整净值：${point.nav}`);
    await userEvent.unhover(anchor);
    await waitFor(() => expect(screen.queryByRole("tooltip")).not.toBeInTheDocument());
    await act(async () => anchor.focus());
    expect(await screen.findByRole("tooltip")).toHaveTextContent(`完整净值：${point.nav}`);
  });

  it("本人列表与完整参数指标表保留四项、原指标、策略版本和缺值原因", async () => {
    sealed();
    const items = fixture.mine.data.items.map((item, index) => {
      const result = fixture.results[item.experiment_id];
      if (!result) throw new Error("actual typed result is missing");
      const blank = index === 1 || index === 2;
      return {
        ...item,
        strategy_name: "原入场策略",
        strategy_version: 3,
        rules: null,
        status:
          index === 1 ? ("failed" as const) : index === 2 ? ("cancelled" as const) : item.status,
        label: index === 1 ? "运行失败" : index === 2 ? "已取消" : item.label,
        result_hash: blank ? null : item.result_hash,
        message:
          index === 1 ? "运行失败，历史尝试保留。" : index === 2 ? "已取消，历史尝试保留。" : null,
        metrics: result.data.metrics.map((metric, metricIndex) => ({
          ...metric,
          value: blank || (index === 3 && metricIndex === 0) ? null : metric.value,
        })),
      };
    });
    server.use(
      http.get("*/api/v1/experiments/mine", () =>
        HttpResponse.json({
          ...fixture.mine,
          data: { ...fixture.mine.data, items },
        }),
      ),
      http.get("*/api/v1/experiments/families/:family", () =>
        HttpResponse.json({
          ...fixture.family,
          data: { ...fixture.family.data, items, failed_count: 1, cancelled_count: 1 },
        }),
      ),
    );
    renderApp("/experiments");
    const mine = await screen.findByRole("table", { name: "我的实验" });
    expect(within(mine).getByRole("columnheader", { name: "策略 / 版本" })).toBeVisible();
    expect(within(mine).getByRole("columnheader", { name: "净收益" })).toBeVisible();
    expect(within(mine).getAllByText("原入场策略 · 第 3 版")).toHaveLength(4);
    expect(within(mine).getAllByRole("row")).toHaveLength(5);
    await userEvent.click(within(mine).getByRole("button", { name: "仓位实验 · 1" }));
    const matrix = await screen.findByRole("table", { name: "完整参数与指标" });
    expect(within(matrix).getAllByRole("row")).toHaveLength(5);
    for (const metric of items[0]?.metrics ?? []) {
      expect(within(matrix).getByRole("columnheader", { name: metric.label })).toBeVisible();
    }
    expect(within(matrix).getByText("运行失败，历史尝试保留。")).toBeVisible();
    expect(within(matrix).getByText("已取消，历史尝试保留。")).toBeVisible();
    expect(within(matrix).getAllByRole("row")[2]?.textContent).toContain("—");
    expect(within(matrix).getAllByRole("row")[3]?.textContent).toContain("—");
    const row = within(matrix).getAllByRole("row")[1];
    if (!row) throw new Error("first original planned row is missing");
    await userEvent.click(within(row).getByText("全部参数"));
    expect(within(row).getByText("单股上限")).toBeVisible();
    await userEvent.click(within(row).getByText("全部指标"));
    expect(within(row).getByText("波动率")).toBeVisible();
  });
  it.each([
    [401, "capabilities"],
    [403, "capabilities"],
    [409, "capabilities"],
    [403, "mine"],
  ] as const)("缓存能力后重查 %s %s 撤下私有视图和写入，保留原请求", async (status, query) => {
    sealed();
    const original: Schemas["ExperimentCancelWrite"] = {
      kind: "cancel_experiment_family",
      command_id: "00000000-0000-0000-0000-000000780041",
      requested_at: "2026-10-05T08:00:00Z",
      family_id: fixture.family.data.family_id,
    };
    const key = "rquant:experiment-request:v1:alice";
    sessionStorage.setItem(key, JSON.stringify({ owner: "alice", body: original }));
    const app = renderApp("/experiments");
    const table = await screen.findByRole("table", { name: "我的实验" });
    await userEvent.click(within(table).getByRole("checkbox", { name: "选择仓位实验第1项" }));
    await userEvent.click(screen.getByRole("button", { name: "仓位实验 · 1" }));
    expect(await screen.findByRole("img", { name: "实验与基准净值" })).toBeVisible();
    server.use(http.get(`*/api/v1/experiments/${query}`, () => HttpResponse.json({}, { status })));
    await act(async () => {
      await app.queryClient.invalidateQueries({
        queryKey: ["formal-experiments", "alice", fixture.generation_id, query],
      });
    });
    await waitFor(() => {
      expect(screen.queryByRole("table", { name: "我的实验" })).not.toBeInTheDocument();
      expect(screen.queryByRole("img", { name: "实验与基准净值" })).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "新建实验" })).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "核对原请求" })).not.toBeInTheDocument();
    });
    expect(screen.getByRole("alert")).toBeVisible();
    expect(JSON.parse(sessionStorage.getItem(key) ?? "{}")).toEqual({
      owner: "alice",
      body: original,
    });
  });

  it.each([
    [true, true],
    [true, false],
    [false, false],
  ])("私有可用 %s 写入 %s 仍能单独查看旧共享记录", async (available, canSearch) => {
    ready();
    let legacyReads = 0;
    server.use(
      http.get("*/api/v1/experiments/capabilities", () =>
        HttpResponse.json({
          data: { ...caps, available, can_search: canSearch },
          serving: meta.serving,
        }),
      ),
      http.get("*/api/v1/experiments", () => {
        legacyReads += 1;
        return HttpResponse.json({
          data: {
            available: true,
            items: [
              {
                experiment_id: "e".repeat(64),
                hypothesis_family: "旧均线记录",
                registered_at: "2026-09-24T07:20:00Z",
                status: "succeeded",
                completed_at: "2026-09-24T07:25:00Z",
                trade_count: 12,
                net_return_pct: 7.5,
                max_drawdown_pct: 3.25,
                win_rate_pct: 60,
              },
            ],
            retained_count: 1,
            truncated: false,
            oldest_registered_at: "2026-09-24T07:20:00Z",
            next_cursor: null,
          },
          serving: meta.serving,
        });
      }),
    );
    renderApp("/experiments");
    if (available) {
      const mine = await screen.findByRole("table", { name: "我的实验" });
      expect(within(mine).getByRole("button", { name: "仓位研究 · 2" })).toBeVisible();
      expect(screen.getByRole("button", { name: "新建实验" }).hasAttribute("disabled")).toBe(
        !canSearch,
      );
      expect(legacyReads).toBe(0);
      await userEvent.click(screen.getByRole("button", { name: "旧共享记录" }));
    }
    const shared = await screen.findByRole("table", { name: "实验记录" });
    expect(within(shared).getByText("旧均线记录")).toBeVisible();
    expect(screen.queryByRole("table", { name: "我的实验" })).not.toBeInTheDocument();
    expect(shared.textContent).not.toContain("仓位研究");
    expect(legacyReads).toBe(1);
    if (available) {
      await userEvent.click(screen.getByRole("button", { name: "我的实验" }));
      expect(await screen.findByRole("table", { name: "我的实验" })).toBeVisible();
    }
  });

  it("显示本人完整尝试；新建包含真实三个区间与参数范围，不显示内部编号", async () => {
    ready();
    renderApp("/experiments");
    expect(await screen.findByRole("heading", { name: "我的实验", level: 1 })).toBeVisible();
    expect(await screen.findByRole("table", { name: "我的实验" })).toBeVisible();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
    await userEvent.click(screen.getByRole("button", { name: "新建实验" }));
    const dialog = await screen.findByRole("dialog");
    for (const label of [
      "实验名称",
      "训练开始",
      "训练结束",
      "验证开始",
      "验证结束",
      "样本外开始",
      "样本外结束",
      "搜索方式",
    ]) {
      expect(within(dialog).getByLabelText(label)).toBeVisible();
    }
    await userEvent.keyboard("{Escape}");
    await waitFor(() => expect(screen.getByRole("button", { name: "新建实验" })).toHaveFocus());
  });

  it("未知回执保存同请求，刷新后恢复原正文，不再生成另一命令", async () => {
    ready([]);
    const bodies: unknown[] = [];
    server.use(
      http.post("*/api/v1/experiments/commands", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        bodies.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          status: bodies.length === 1 ? "unknown" : "registered",
          message: bodies.length === 1 ? "正在核对提交结果。" : "已登记，等待运行。",
          family_id: "experiment-search:abc",
          job_ids: [],
          planned_count: 4,
          version: null,
        });
      }),
    );
    const mounted = renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "新建实验" }));
    await userEvent.type(screen.getByLabelText("实验名称"), "仓位研究");
    await userEvent.click(screen.getByRole("button", { name: "开始搜索" }));
    expect(await screen.findByRole("button", { name: "核对原请求" })).toBeVisible();
    await userEvent.keyboard("{Escape}");
    await waitFor(() => expect(screen.getByRole("button", { name: "核对原请求" })).toHaveFocus());
    mounted.unmount();
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "核对原请求" }));
    await waitFor(() => expect(bodies).toHaveLength(2));
    expect(bodies[0]).toEqual(bodies[1]);
  });

  it("只提供已登记热图轴；完整曲线、缺统计原因和键盘邻格保持原结果", async () => {
    sealed();
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "仓位实验 · 1" }));
    const dialog = await screen.findByRole("dialog");
    expect(await within(dialog).findByRole("img", { name: "实验与基准净值" })).toBeVisible();
    const axes = within(dialog).getByLabelText("热图横轴");
    expect(
      within(axes)
        .getAllByRole("option")
        .map((option) => option.textContent),
    ).toEqual(["最多持仓", "现金保留"]);
    const heat = await within(dialog).findByRole("table", { name: "参数热力图" });
    const buttons = within(heat).getAllByRole("button");
    buttons[0]?.focus();
    await userEvent.keyboard("{ArrowRight}");
    expect(buttons[1]).toHaveFocus();
    await userEvent.keyboard("{Enter}");
    expect(
      await within(dialog).findByRole("heading", { name: "第 2 项 · 完整结果" }),
    ).toBeVisible();
    expect(within(dialog).getByText("邻域 3 / 3 格")).toBeVisible();
    expect(within(dialog).getAllByText("暂不可计算 · 查看原因").length).toBeGreaterThan(0);
  });

  it("只选两份封存结果，第三份不可选；对比展示完整曲线和参数", async () => {
    sealed();
    renderApp("/experiments");
    const table = await screen.findByRole("table", { name: "我的实验" });
    await userEvent.click(within(table).getByRole("checkbox", { name: "选择仓位实验第1项" }));
    await waitFor(() =>
      expect(within(table).getByRole("checkbox", { name: "选择仓位实验第1项" })).toHaveFocus(),
    );
    await userEvent.click(within(table).getByRole("checkbox", { name: "选择仓位实验第2项" }));
    expect(within(table).getByRole("checkbox", { name: "选择仓位实验第3项" })).toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "对比所选" }));
    expect(await screen.findByRole("img", { name: "两份实验净值" })).toBeVisible();
    expect(screen.getByRole("table", { name: "参数差异" })).toBeVisible();
  });

  it("模板规则差异显示短中文与百分比，来源标识只在提示中", async () => {
    sealed();
    server.use(
      http.get("*/api/v1/experiments/compare", () =>
        HttpResponse.json({
          ...fixture.comparison,
          data: {
            ...fixture.comparison.data,
            differences: [
              { path: "template.exit.stop_loss", a: null, b: "0.05" },
              { path: "template.exit.max_holding_days", a: "5", b: "10" },
              { path: "template.entry.body_hash", a: "a".repeat(64), b: "b".repeat(64) },
            ],
          },
        } satisfies Schemas["Envelope_ExperimentComparisonData_"]),
      ),
    );
    renderApp("/experiments");
    const table = await screen.findByRole("table", { name: "我的实验" });
    await userEvent.click(within(table).getByRole("checkbox", { name: "选择仓位实验第1项" }));
    await userEvent.click(within(table).getByRole("checkbox", { name: "选择仓位实验第2项" }));
    await userEvent.click(screen.getByRole("button", { name: "对比所选" }));
    const changes = await screen.findByRole("table", { name: "参数差异" });
    expect(within(changes).getByRole("row", { name: "止损 未启用 5.00%" })).toBeVisible();
    expect(
      within(changes).getByRole("row", { name: "持有上限 5 个交易日 10 个交易日" }),
    ).toBeVisible();
    expect(changes.textContent).not.toContain("a".repeat(64));
    expect(changes.textContent).not.toContain("b".repeat(64));
  });

  it("请求处理中关闭抽屉，未知回执后恢复核对按钮焦点", async () => {
    ready();
    let release: (() => void) | undefined;
    server.use(
      http.post("*/api/v1/experiments/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["ExperimentSearchWrite"];
        await new Promise<void>((resolve) => {
          release = resolve;
        });
        return HttpResponse.json({
          command_id: body.command_id,
          status: "unknown",
          message: "提交状态待确认，请重试原请求。",
          family_id: null,
          job_ids: [],
          planned_count: null,
          version: null,
        } satisfies Schemas["ExperimentWriteReceipt"]);
      }),
    );
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "新建实验" }));
    await userEvent.type(screen.getByLabelText("实验名称"), "原请求研究");
    await userEvent.click(screen.getByRole("button", { name: "开始搜索" }));
    await waitFor(() => expect(release).toBeDefined());
    await userEvent.keyboard("{Escape}");
    const retry = await screen.findByRole("button", { name: "核对原请求" });
    expect(retry).toBeDisabled();
    await act(async () => release?.());
    await waitFor(() => expect(retry).toBeEnabled());
    await waitFor(() => expect(retry).toHaveFocus());
  });

  it("迟到备注回执保留新草稿，并按已保存版本继续写入", async () => {
    sealed();
    let release: (() => void) | undefined;
    let savedText = "";
    let version = 0;
    const bodies: Schemas["ExperimentNoteWrite"][] = [];
    server.use(
      http.get("*/api/v1/experiments/families/:family", () =>
        HttpResponse.json({
          ...fixture.family,
          data: { ...fixture.family.data, note: savedText, note_version: version },
        }),
      ),
      http.post("*/api/v1/experiments/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["ExperimentNoteWrite"];
        bodies.push(body);
        if (bodies.length === 1)
          await new Promise<void>((resolve) => {
            release = resolve;
          });
        savedText = body.text;
        version += 1;
        return HttpResponse.json({
          command_id: body.command_id,
          status: "note_saved",
          message: "备注已保存。",
          family_id: body.family_id,
          job_ids: [],
          planned_count: null,
          version,
        } satisfies Schemas["ExperimentWriteReceipt"]);
      }),
    );
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "仓位实验 · 1" }));
    const note = await screen.findByLabelText("研究备注");
    await userEvent.type(note, "第一稿");
    await userEvent.click(screen.getByRole("button", { name: "保存备注" }));
    await waitFor(() => expect(release).toBeDefined());
    await userEvent.clear(note);
    await userEvent.type(note, "继续研究的新稿");
    await act(async () => release?.());
    await waitFor(() => expect(screen.getByRole("button", { name: "保存备注" })).toBeEnabled());
    expect(note).toHaveValue("继续研究的新稿");
    await userEvent.click(screen.getByRole("button", { name: "保存备注" }));
    await waitFor(() => expect(bodies).toHaveLength(2));
    expect(bodies[1]).toMatchObject({ expected_version: 1, text: "继续研究的新稿" });
    expect(bodies[0]?.command_id).not.toBe(bodies[1]?.command_id);
    await waitFor(() => expect(screen.getByRole("button", { name: "保存备注" })).toBeDisabled());
  });

  it("解封需准确确认名称；取消保留原次数，成功后不能再发另一请求", async () => {
    // rc-component returns the same aria title id for all portals in test mode.
    // Use its actual React ids so the nested confirmation has its own name.
    vi.stubEnv("NODE_ENV", "development");
    sealed();
    let admitted = false;
    const bodies: Schemas["ExperimentUnsealWrite"][] = [];
    server.use(
      http.get("*/api/v1/experiments/families/:family", () =>
        HttpResponse.json({
          ...fixture.family,
          data: { ...fixture.family.data, outer_admitted: admitted },
        }),
      ),
      http.post("*/api/v1/experiments/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["ExperimentUnsealWrite"];
        bodies.push(body);
        admitted = true;
        return HttpResponse.json({
          command_id: body.command_id,
          status: "outer_admitted",
          message: "已准入样本外，等待运行。",
          family_id: body.family_id,
          job_ids: [],
          planned_count: 1,
          version: null,
        } satisfies Schemas["ExperimentWriteReceipt"]);
      }),
    );
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "仓位实验 · 1" }));
    const trigger = await screen.findByRole("button", { name: "解封样本外" });
    await waitFor(() => expect(trigger).toBeEnabled());
    await userEvent.click(trigger);
    await waitFor(() => expect(screen.getByText(/运行失败或取消也不会恢复次数/)).toBeVisible());
    let dialog = await screen.findByRole("dialog", { name: "解封样本外" });
    expect(within(dialog).getByText(/运行失败或取消也不会恢复次数/)).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "确认解封" })).toBeDisabled();
    await userEvent.type(within(dialog).getByRole("textbox"), "别的实验");
    expect(within(dialog).getByRole("button", { name: "确认解封" })).toBeDisabled();
    await userEvent.click(within(dialog).getByRole("button", { name: /取\s*消/ }));
    await waitFor(() => expect(trigger).toHaveFocus());
    expect(bodies).toEqual([]);
    await userEvent.click(trigger);
    dialog = await screen.findByRole("dialog", { name: "解封样本外" });
    await userEvent.type(within(dialog).getByRole("textbox"), fixture.family.data.name);
    await userEvent.click(within(dialog).getByRole("button", { name: "确认解封" }));
    await waitFor(() => expect(bodies).toHaveLength(1));
    expect(bodies[0]).toMatchObject({
      kind: "unseal_experiment_outer_test",
      family_id: fixture.family.data.family_id,
      experiment_id: fixture.family.data.items[0]?.experiment_id,
      result_hash: fixture.family.data.items[0]?.result_hash,
      confirmed: true,
    });
    expect(await screen.findByRole("button", { name: "已解封" })).toBeDisabled();
    expect(bodies).toHaveLength(1);
    await userEvent.keyboard("{Escape}");
    await waitFor(() => expect(screen.getByRole("button", { name: "仓位实验 · 1" })).toHaveFocus());
  });

  it("输入乱序或越界参数时保留草稿且不提交", async () => {
    ready();
    let writes = 0;
    server.use(
      http.post("*/api/v1/experiments/commands", () => {
        writes += 1;
        return HttpResponse.json({}, { status: 400 });
      }),
    );
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "新建实验" }));
    await userEvent.type(screen.getByLabelText("实验名称"), "非法参数");
    const positions = screen.getByLabelText("最多持仓范围");
    await userEvent.clear(positions);
    await userEvent.type(positions, "2, 1");
    await userEvent.click(screen.getByRole("button", { name: "开始搜索" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("请检查参数范围");
    expect(writes).toBe(0);
    expect(positions).toHaveValue("2, 1");
    await userEvent.clear(positions);
    await userEvent.type(positions, "501");
    await userEvent.click(screen.getByRole("button", { name: "开始搜索" }));
    expect(writes).toBe(0);
  });

  it("统计409撤下完整曲线；迟到的Alice回执不进入Bob页面", async () => {
    sealed();
    server.use(
      http.get("*/api/v1/experiments/results/:experiment/statistics", () =>
        HttpResponse.json({}, { status: 409 }),
      ),
    );
    renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "仓位实验 · 1" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("数据已更新");
    expect(screen.queryByRole("img", { name: "实验与基准净值" })).not.toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "我的实验" })).not.toBeInTheDocument();
  });

  it("账号切换拒绝迟到回执，并保留原账号待核正文", async () => {
    ready([]);
    let release: (() => void) | undefined;
    server.use(
      http.post("*/api/v1/experiments/commands", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        await new Promise<void>((resolve) => {
          release = resolve;
        });
        return HttpResponse.json({
          command_id: body.command_id,
          status: "registered",
          message: "Alice已登记",
          family_id: null,
          job_ids: [],
          planned_count: 4,
          version: null,
        });
      }),
    );
    const app = renderApp("/experiments");
    await userEvent.click(await screen.findByRole("button", { name: "新建实验" }));
    await userEvent.type(screen.getByLabelText("实验名称"), "Alice实验");
    await userEvent.click(screen.getByRole("button", { name: "开始搜索" }));
    await waitFor(() => expect(release).toBeDefined());
    const original = sessionStorage.getItem("rquant:experiment-request:v1:alice");
    expect(original).toContain("Alice实验");
    await act(async () => {
      app.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "bob" }));
    });
    await waitFor(() => expect(screen.queryByLabelText("实验名称")).not.toBeInTheDocument());
    await act(async () => release?.());
    expect(screen.queryByText("Alice已登记")).not.toBeInTheDocument();
    expect(sessionStorage.getItem("rquant:experiment-request:v1:alice")).toBe(original);
  });
});
