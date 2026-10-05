import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import {
  templateCatalog,
  templateDetail,
  templateEnvelope,
  templateGeneration,
  templateHead,
  templateId,
  templateSources,
} from "./template.fixture";

const base = "*/api/v1/strategy-templates";

function setup(detail = templateDetail, sources = templateSources) {
  server.use(
    http.get("*/api/v1/strategies", () =>
      HttpResponse.json(templateEnvelope({ available: true, strategies: [] })),
    ),
    http.get(base, () => HttpResponse.json(templateEnvelope(templateCatalog(detail)))),
    http.get(`${base}/sources`, () => HttpResponse.json(templateEnvelope(sources))),
    http.get(`${base}/:strategyId`, () => HttpResponse.json(templateEnvelope(detail))),
    http.get(`${base}/:strategyId/versions`, () =>
      HttpResponse.json(
        templateEnvelope({
          strategy_id: templateId,
          current_head: detail.current_head,
          versions: [
            {
              head: detail.head,
              saved_at: detail.saved_at,
              change_note: detail.change_note,
              is_head: detail.head.version === detail.current_head.version,
              latest_run: null,
            },
          ],
          next_before_version: null,
        }),
      ),
    ),
  );
}

async function create(user: ReturnType<typeof userEvent.setup>) {
  await waitFor(() => expect(screen.getByRole("button", { name: "新建策略" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "新建策略" }));
  const drawer = screen.getByRole("dialog", { name: "新建策略" });
  await user.type(within(drawer).getByLabelText("策略名称"), "完整观察");
  return drawer;
}

describe("策略模板完整操作", () => {
  it("点按说明保留定时退出和指数过滤的勾选及设置", async () => {
    setup();
    const user = userEvent.setup();
    renderApp("/strategies");
    const drawer = await create(user);
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    await user.click(within(drawer).getByRole("checkbox", { name: /定时退出/ }));
    await user.click(within(drawer).getByRole("img", { name: "定时退出说明" }));
    expect(within(drawer).getByRole("checkbox", { name: /定时退出/ })).toBeChecked();
    expect(within(drawer).getByLabelText("退出时间")).toHaveValue("14:50");
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    await user.click(within(drawer).getByRole("checkbox", { name: /指数过滤/ }));
    await user.click(within(drawer).getByRole("img", { name: "指数过滤说明" }));
    expect(within(drawer).getByRole("checkbox", { name: /指数过滤/ })).toBeChecked();
    expect(within(drawer).getByLabelText("指数代码")).toHaveValue("000300.SH");
  });
  it("历史版编辑必须说明改动；保留极小和接近上限的原比例，并按当前版比较保存", async () => {
    const currentHead = { ...templateHead, version: 2, record_hash: "7".repeat(64) };
    setup({
      ...templateDetail,
      current_head: currentHead,
      rules: {
        ...templateDetail.rules,
        exit: { stop_loss: "0.999999999999999999", take_profit: "1e-120" },
      },
    });
    const saved: Schemas["SaveStrategyTemplate"][] = [];
    server.use(
      http.post(`${base}/commands`, async ({ request }) => {
        const body = (await request.json()) as Schemas["SaveStrategyTemplate"];
        saved.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: body.command_id,
            status: "rejected",
            message: "当前版本已变化。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/strategies");
    await user.click(await screen.findByRole("row", { name: /低位观察/ }));
    const detail = await screen.findByRole("dialog", { name: "低位观察" });
    expect(await within(detail).findByText("1e-118%")).toBeInTheDocument();
    await user.click(within(detail).getByRole("button", { name: "保存新版本" }));
    const editor = screen.getByRole("dialog", { name: "保存新版本" });
    await user.click(within(editor).getByRole("button", { name: "下一步" }));
    expect(within(editor).getByLabelText("改动说明")).toBeInTheDocument();
    await user.type(within(editor).getByLabelText("改动说明"), "保留原规则并重命名");
    for (let step = 0; step < 3; step += 1)
      await user.click(within(editor).getByRole("button", { name: "下一步" }));
    expect(within(editor).getByText(/新增第 3 版/)).toBeInTheDocument();
    await user.click(within(editor).getByRole("button", { name: "保存新版本" }));
    await screen.findByText("当前版本已变化。");
    expect(saved[0]).toMatchObject({
      strategy_id: templateId,
      expected_head: currentHead,
      change_note: "保留原规则并重命名",
      rules: { exit: { stop_loss: "0.999999999999999999", take_profit: "1e-120" } },
    });
  });

  it("原比较积木可选择固定字段的前 30 个交易日，并保留完整规则正文", async () => {
    const parameter = (key: string, label: string) => ({
      key,
      label,
      input: "operand" as const,
      initial: "CLOSE[0]",
      required: true,
      minimum: null,
      maximum: null,
      scale: 1,
      hint: null,
      custom_ma: false,
      options: [
        { value: "CLOSE[0]", label: "收盘价" },
        { value: "MA5[0]", label: "5 日均线" },
      ],
    });
    setup(templateDetail, {
      ...templateSources,
      conditions: [
        ...templateSources.conditions,
        {
          key: "gt",
          label: "大于",
          parameter_schema: {},
          block: {
            key: "gt",
            label: "大于",
            hint: "左侧须大于右侧",
            category: "compare",
            category_label: "比较",
            parameters: [parameter("left", "左侧"), parameter("right", "右侧")],
          },
        },
      ],
    });
    const saved: Schemas["SaveStrategyTemplate"][] = [];
    server.use(
      http.post(`${base}/commands`, async ({ request }) => {
        const body = (await request.json()) as Schemas["SaveStrategyTemplate"];
        saved.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: body.command_id,
            status: "rejected",
            message: "保留草稿。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/strategies");
    const drawer = await create(user);
    await user.selectOptions(within(drawer).getByLabelText("条件 1"), "gt");
    await user.selectOptions(within(drawer).getByLabelText("右侧"), "MA5[0]");
    fireEvent.change(within(drawer).getByLabelText("左侧相对日期"), { target: { value: "30" } });
    for (let step = 0; step < 3; step += 1)
      await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    expect(within(drawer).getByText(/收盘价（前 30 日）/)).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "保存策略" }));
    await screen.findByText("保留草稿。");
    expect(saved[0]?.rules.entry).toEqual({
      kind: "conditions",
      conditions: [{ key: "gt", args: { left: "CLOSE[30]", right: "MA5[0]" } }],
    });
  });

  it("信号来源、五种退出、得分仓位、调仓和指数过滤一起进入请求", async () => {
    setup();
    const saved: Schemas["SaveStrategyTemplate"][] = [];
    server.use(
      http.post(`${base}/commands`, async ({ request }) => {
        const body = (await request.json()) as Schemas["SaveStrategyTemplate"];
        saved.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: body.command_id,
            status: "rejected",
            message: "保留草稿。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/strategies");
    const drawer = await create(user);
    await user.selectOptions(within(drawer).getByLabelText("入场方式"), "pool");
    expect(within(drawer).getByRole("option", { name: "重点观察 · 第 2 版" })).toBeInTheDocument();
    await user.selectOptions(within(drawer).getByLabelText("入场方式"), "signal");
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    for (const label of ["止损", "止盈", "移动止盈", "持有上限", /定时退出/])
      await user.click(within(drawer).getByRole("checkbox", { name: label }));
    const stop = within(drawer).getByLabelText("止损幅度（%）");
    await user.clear(stop);
    await user.type(stop, "8.5");
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    await user.selectOptions(within(drawer).getByLabelText("分配方式"), "rank_score");
    fireEvent.change(within(drawer).getByLabelText("行业上限（%，空为不限）"), {
      target: { value: "30" },
    });
    await user.selectOptions(within(drawer).getByLabelText("调仓频率"), "every_n");
    fireEvent.change(within(drawer).getByLabelText("调仓间隔（交易日）"), {
      target: { value: "3" },
    });
    await user.click(within(drawer).getByRole("checkbox", { name: /指数过滤/ }));
    await user.selectOptions(within(drawer).getByLabelText("指数方向"), "below");
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    await user.click(within(drawer).getByRole("button", { name: "保存策略" }));
    await screen.findByText("保留草稿。");
    expect(saved[0]?.rules).toMatchObject({
      entry: {
        kind: "signal",
        strategy_id: "auction_gap",
        version: 1,
        source_hash: "6".repeat(64),
        action: "b_confirm",
      },
      exit: {
        stop_loss: "0.085",
        take_profit: "0.2",
        trailing_profit: "0.1",
        max_holding_days: 5,
        exit_time: "14:50",
      },
      weight_rule: { method: "rank_score", max_industry_weight: "0.3" },
      rebalance_rule: { kind: "every_n", every_n_days: 3 },
      index_filter: { benchmark_code: "000300.SH", ma_days: 20, direction: "below" },
    });
  });

  it("归档须确认；保存回执不受浏览器存储删除失败影响，历史规则仍可读", async () => {
    setup();
    const saved: Schemas["ArchiveStrategyTemplate"][] = [];
    server.use(
      http.post(`${base}/commands`, async ({ request }) => {
        const body = (await request.json()) as Schemas["ArchiveStrategyTemplate"];
        saved.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: body.command_id,
            status: "published",
            strategy_id: templateId,
            head: templateHead,
            current_head_updated: true,
            message: "已归档。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/strategies");
    await user.click(await screen.findByRole("row", { name: /低位观察/ }));
    const drawer = await screen.findByRole("dialog", { name: "低位观察" });
    await within(drawer).findByText("排除 ST");
    expect(within(drawer).getByRole("button", { name: "归档策略" })).toBeEnabled();
    await user.click(within(drawer).getByRole("button", { name: "归档策略" }));
    // AntD's test-mode ID is shared by simultaneous overlays; the real browser
    // suite also checks the production dialog's accessible name.
    const description = await screen.findByText("归档后关闭新编辑和新回测，历史版本仍可查看。");
    const confirm = description.closest<HTMLElement>('[role="dialog"]');
    expect(confirm).not.toBeNull();
    expect(saved).toHaveLength(0);
    vi.spyOn(Storage.prototype, "removeItem").mockImplementation(() => {
      throw new Error("blocked storage");
    });
    if (!confirm) throw new Error("missing archive confirmation");
    await user.click(within(confirm).getByRole("button", { name: "确认归档" }));
    await screen.findByText("已归档。");
    expect(screen.queryByText("结果待确认，请继续查看。")).toBeNull();
    expect(saved[0]).toMatchObject({
      kind: "archive_strategy_template",
      strategy_id: templateId,
      expected_head: templateHead,
    });
    expect(within(drawer).getByText("排除 ST")).toBeInTheDocument();
  });

  it("同用户换代重挂后恢复原请求；错 command 回执保留待确认状态", async () => {
    setup();
    const sent: unknown[] = [];
    const resumed: unknown[] = [];
    server.use(
      http.post(`${base}/commands`, async ({ request }) => {
        const body = (await request.json()) as Schemas["SaveStrategyTemplate"];
        sent.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: "ffffffff-ffff-4fff-8fff-ffffffffffff",
            status: "published",
            message: "错误回执不显示。",
          }),
        );
      }),
      http.post(`${base}/commands/resume`, async ({ request }) => {
        const body = (await request.json()) as Schemas["SaveStrategyTemplate"];
        resumed.push(body);
        return HttpResponse.json(
          templateEnvelope(
            {
              command_id: body.command_id,
              status: "published",
              strategy_id: templateId,
              head: templateHead,
              message: "原请求已完成。",
              current_head_updated: false,
            },
            "b".repeat(64),
          ),
        );
      }),
    );
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    const drawer = await create(user);
    for (let step = 0; step < 3; step += 1)
      await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    await user.click(within(drawer).getByRole("button", { name: "保存策略" }));
    await screen.findByText("结果待确认，请继续查看。");
    expect(screen.queryByText("错误回执不显示。")).toBeNull();
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: "b".repeat(64) })),
    );
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "新建策略" })).toBeNull());
    await user.click(await screen.findByRole("button", { name: "继续查看结果" }));
    await screen.findByText("原请求已完成。");
    expect(resumed).toEqual(sent);
    expect((resumed[0] as Schemas["SaveStrategyTemplate"]).generation_id).toBe(templateGeneration);
  });
});
