import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import {
  templateCatalog,
  templateDetail,
  templateEnvelope,
  templateGeneration,
  templateHead,
  templateId,
  templateSources,
} from "./template.fixture";

const prefix = "*/api/v1/strategy-templates";

function publishTemplates() {
  server.use(
    http.get("*/api/v1/strategies", () =>
      HttpResponse.json(templateEnvelope({ available: true, strategies: [] })),
    ),
    http.get(prefix, () => HttpResponse.json(templateEnvelope(templateCatalog()))),
    http.get(`${prefix}/sources`, () => HttpResponse.json(templateEnvelope(templateSources))),
    http.get(`${prefix}/:strategyId/versions`, () =>
      HttpResponse.json(
        templateEnvelope({
          strategy_id: templateId,
          current_head: templateHead,
          versions: [
            {
              head: templateHead,
              saved_at: templateDetail.saved_at,
              change_note: "首次保存",
              is_head: true,
              latest_run: null,
            },
          ],
          next_before_version: null,
        }),
      ),
    ),
    http.get(`${prefix}/:strategyId`, () => HttpResponse.json(templateEnvelope(templateDetail))),
  );
}

describe("策略模板", () => {
  it("按入场、退出、仓位和调仓填写，再提交完整正文；关闭回到新建入口", async () => {
    publishTemplates();
    const sent: unknown[] = [];
    server.use(
      http.post(`${prefix}/commands`, async ({ request }) => {
        const body = await request.json();
        sent.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: (body as { command_id: string }).command_id,
            status: "succeeded_waiting_publication",
            strategy_id: templateId,
            head: templateHead,
            current_head_updated: false,
            message: "已提交，等待数据更新。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    await waitFor(() => expect(screen.getByRole("button", { name: "新建策略" })).toBeEnabled());
    const create = screen.getByRole("button", { name: "新建策略" });
    await user.click(create);
    let drawer = await screen.findByRole("dialog", { name: "新建策略" });
    await user.type(within(drawer).getByLabelText("策略名称"), "每日观察");
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    expect(within(drawer).getByText("退出条件")).toBeInTheDocument();
    await user.click(within(drawer).getByLabelText("止损"));
    await user.clear(within(drawer).getByLabelText("止损幅度（%）"));
    await user.type(within(drawer).getByLabelText("止损幅度（%）"), "8");
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    expect(within(drawer).getByLabelText("持仓上限（只）")).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    expect(within(drawer).getByText("保存前确认")).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "保存策略" }));
    expect(await screen.findByText("已提交，等待数据更新。")).toBeInTheDocument();
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      kind: "save_strategy_template",
      name: "每日观察",
      strategy_id: null,
      expected_head: null,
      generation_id: templateGeneration,
      rules: {
        template_contract: "strategy-template/v1",
        entry: { kind: "conditions", conditions: [{ key: "not_st", args: {} }] },
        exit: { stop_loss: "0.08" },
        weight_rule: { method: "equal" },
        rebalance_rule: { kind: "daily" },
      },
    });
    expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
    drawer = screen.getByRole("dialog", { name: "新建策略" });
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "新建策略" })).toBeNull());
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "新建策略" }).closest(".tip-anchor")).toHaveFocus(),
    );
  });

  it("查看完整规则和历史；编辑新增版本，回测绑定当前所选精确版本", async () => {
    publishTemplates();
    const runs: unknown[] = [];
    server.use(
      http.post(`${prefix}/:strategyId/runs`, async ({ request }) => {
        const body = await request.json();
        runs.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: (body as { command_id: string }).command_id,
            status: "submitted",
            job_id: (body as { command_id: string }).command_id,
            message: "回测已提交。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/strategies");
    const list = await screen.findByRole("table", { name: "我的策略" });
    const row = within(list).getByRole("row", { name: /低位观察/ });
    row.focus();
    await user.keyboard("{Enter}");
    const drawer = await screen.findByRole("dialog", { name: "低位观察" });
    expect(await within(drawer).findByText("排除 ST")).toBeInTheDocument();
    expect(within(drawer).getByText("版本历史")).toBeInTheDocument();
    expect(within(drawer).getAllByText("暂无回测").length).toBeGreaterThan(0);
    await user.click(within(drawer).getByRole("button", { name: "运行回测" }));
    const run = screen.getByRole("dialog", { name: "运行回测" });
    await user.type(within(run).getByLabelText("开始日期"), "2026-09-01");
    await user.type(within(run).getByLabelText("结束日期"), "2026-09-23");
    await user.click(within(run).getByRole("button", { name: "提交回测" }));
    expect(await screen.findByText("回测已提交。")).toBeInTheDocument();
    expect(runs[0]).toMatchObject({
      kind: "run_strategy_template",
      strategy_id: templateId,
      head: templateHead,
      expected_head: templateHead,
      start_date: "2026-09-01",
      end_date: "2026-09-23",
    });
  });

  it("未知结果保留原请求续查；换用户隐藏旧详情与迟到结果", async () => {
    publishTemplates();
    const sent: unknown[] = [];
    const retries: unknown[] = [];
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    server.use(
      http.post(`${prefix}/commands`, async ({ request }) => {
        const body = await request.json();
        sent.push(body);
        return HttpResponse.json(
          templateEnvelope({
            command_id: (body as { command_id: string }).command_id,
            status: "uncertain",
            message: "结果待确认，请保留这次操作并继续查看。",
          }),
        );
      }),
      http.post(`${prefix}/commands/resume`, async ({ request }) => {
        const body = await request.json();
        retries.push(body);
        await gate;
        return HttpResponse.json(
          templateEnvelope({
            command_id: (body as { command_id: string }).command_id,
            status: "published",
            strategy_id: templateId,
            head: templateHead,
            current_head_updated: false,
            message: "已保存。",
          }),
        );
      }),
    );
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    await waitFor(() => expect(screen.getByRole("button", { name: "新建策略" })).toBeEnabled());
    const create = screen.getByRole("button", { name: "新建策略" });
    await user.click(create);
    const drawer = screen.getByRole("dialog", { name: "新建策略" });
    await user.type(within(drawer).getByLabelText("策略名称"), "待确认策略");
    for (let step = 0; step < 3; step += 1)
      await user.click(within(drawer).getByRole("button", { name: "下一步" }));
    await user.click(within(drawer).getByRole("button", { name: "保存策略" }));
    await user.click(await screen.findByRole("button", { name: "继续查看结果" }));
    await waitFor(() => expect(retries).toHaveLength(1));
    expect(retries[0]).toEqual(sent[0]);
    server.use(
      metaHandler(metaEnvelope({ viewer: "bob" })),
      http.get(prefix, () =>
        HttpResponse.json(
          templateEnvelope({
            availability: "empty",
            available_at: null,
            templates: [],
            can_create: false,
          }),
        ),
      ),
    );
    act(() => view.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "bob" })));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "新建策略" })).toBeNull());
    release();
    await waitFor(() => expect(screen.queryByText("已保存。")).toBeNull());
    expect(screen.queryByText("待确认策略")).toBeNull();
  });
});
