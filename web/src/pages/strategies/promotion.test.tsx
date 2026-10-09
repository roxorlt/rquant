import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY, useMeta } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import StrategiesPage from "./index";
import { PROMOTION_PENDING_KEY } from "./promotionCommands";
import { StrategyPromotionPanel } from "./StrategyPromotionPanel";
import { templateGeneration, templateHead, templateId } from "./template.fixture";

type Result = Schemas["StrategyPromotionCommandData"];
type Command = Result["original_request"];
type Review = Schemas["StrategyPromotionReview-Output"];
const roleHash = "b".repeat(64);
const target: Schemas["StrategyPromotionTarget"] = {
  source_kind: "template",
  owner_id: "tester",
  strategy_id: "template_private_candidate",
  name: "低位观察·验证版本",
  head: templateHead,
  parameter_fingerprint: "5".repeat(64),
  cost_fingerprint: "6".repeat(64),
};
const candidate: Schemas["StrategyPromotionCandidateReference"] = {
  family_name: "固定版本验证",
  target,
  template_parent: { strategy_id: templateId, head: templateHead },
  has_sealed_reference: true,
  is_current: true,
  parent_count: 4,
  input_hash: "7".repeat(64),
  spec_hash: "8".repeat(64),
  manifest_hash: "9".repeat(64),
  result_hash: "a".repeat(64),
  job_id: "11111111-1111-4111-8111-111111111111",
  selection: {
    family_id: "original-family",
    experiment_id: "c".repeat(64),
    walk_forward_id: null,
    paper_account_id: null,
    band_job_id: null,
  },
  train_window: { start_date: "2026-01-01", end_date: "2026-02-28" },
  validation_window: { start_date: "2026-03-01", end_date: "2026-04-30" },
};
function currentRole(
  role: Schemas["RoleEntry"]["role"] = "admin",
  username = "tester",
): Schemas["CollaborationMe"] {
  return {
    available: true,
    mode: "enforced",
    username,
    role,
    revision: 1,
    state_sha256: roleHash,
    can_manage_users: role === "admin",
    can_research: role !== "viewer",
    can_read_audit: true,
  };
}
function data(stage: Schemas["PromotionStage"] = "exploratory"): Schemas["StrategyPromotionData"] {
  const revision = ["exploratory", "comparable", "paper_candidate", "monitor_approved"].indexOf(
    stage,
  );
  return {
    availability: "populated",
    source_kind: "template",
    strategy_id: templateId,
    available_at: "2026-10-06T01:00:00Z",
    states: revision
      ? [
          {
            owner_id: "tester",
            target_key: "d".repeat(64),
            state: {
              target,
              stage,
              revision,
              latest_approval_hash: "e".repeat(64),
              paper_approval_hash: stage === "paper_candidate" ? "f".repeat(64) : null,
              paper_approved_at: stage === "paper_candidate" ? "2026-09-01T01:00:00Z" : null,
            },
            applied_at: "2026-10-06T01:00:00Z",
          },
        ]
      : [],
    reviews: [],
    next_offset: null,
    candidates: [candidate],
    walk_forward: [],
    paper_accounts: [],
    can_evaluate: true,
    can_prepare_approval: true,
    can_run_walk_forward: true,
    reason: "",
  };
}
function envelope<T>(value: T, generation = templateGeneration) {
  return { data: value, serving: metaEnvelope({ generationId: generation }).serving };
}
function review(
  command: Schemas["RequestPromotionReview"],
  status: Schemas["PromotionGate-Output"]["status"] = "satisfied",
): Review {
  return {
    command_id: command.command_id,
    actor_id: "tester",
    target: command.target,
    metadata_identity: {
      instance_id: "synthetic-private-owner",
      path: "/synthetic/private/metadata.sqlite",
      st_dev: 1,
      st_ino: 1,
    },
    from_stage: ["exploratory", "comparable", "paper_candidate"][
      command.expected_revision
    ] as Review["from_stage"],
    to_stage: ["comparable", "paper_candidate", "monitor_approved"][
      command.expected_revision
    ] as Review["to_stage"],
    expected_revision: command.expected_revision,
    policy_hash: "1".repeat(64),
    evidence_hash: "2".repeat(64),
    review_id: "3".repeat(64),
    selection: command.selection,
    observed_at: new Date().toISOString(),
    gates: [
      {
        key: "validation_trades",
        status,
        message: "验证期完整交易至少30笔",
        value: status === "satisfied" ? "32" : null,
      },
    ],
  };
}
function setup(initial = data(), role = currentRole()) {
  const submitted: Command[] = [];
  const looked: Command[] = [];
  const resumed: Command[] = [];
  const gets: URL[] = [];
  let published = initial;
  let lastReview: Review | null = null;
  let prepared: Schemas["PreparedPromotionApproval-Output"] | null = null;
  server.use(
    http.get("*/api/v1/collaboration/me", () => HttpResponse.json(envelope(role))),
    http.get("*/api/v1/strategy-promotions/:strategy", ({ request }) => {
      gets.push(new URL(request.url));
      return HttpResponse.json(envelope(published));
    }),
    http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body = (await request.json()) as Command;
      submitted.push(body);
      let result: Result = { original_request: body, status: "completed", message: "操作已完成。" };
      if (body.kind === "request_promotion_review") {
        lastReview = review(body);
        result = { ...result, review: lastReview, message: "评估完成，请核对证据。" };
      } else if (body.kind === "prepare_promotion_approval") {
        if (!lastReview) throw new Error("评估在确认前");
        prepared = {
          actor_id: "tester",
          preparation_id: body.command_id,
          review: lastReview,
          role_revision: 1,
          role_state_hash: roleHash,
          issued_at: new Date().toISOString(),
          expires_at: new Date(Date.now() + 120_000).toISOString(),
          issuance_proof: "4".repeat(64),
        };
        result = { ...result, preparation: prepared, message: "请核对阶段，并输入策略名称确认。" };
      } else if (body.kind === "approve_promotion") {
        const original = body.preparation.review;
        const after: Schemas["StrategyPromotionState"] = {
          target: body.target,
          stage: original.to_stage,
          revision: original.expected_revision + 1,
          latest_approval_hash: "e".repeat(64),
        };
        published = {
          ...published,
          states: [
            {
              owner_id: "tester",
              target_key: "d".repeat(64),
              state: after,
              applied_at: body.requested_at,
            },
          ],
        };
        result = {
          ...result,
          status: "published",
          message: "阶段已批准。",
          approval: {
            actor_id: "tester",
            after,
            review: original,
            command_id: body.command_id,
            effect_id: body.command_id,
            original_request_hash: "5".repeat(64),
            approval_id: "6".repeat(64),
            applied_at: body.requested_at,
          },
        };
      } else
        result = {
          ...result,
          message: "验证任务已提交，请等待全部结果。",
          walk_forward: { command_id: body.command_id, plan_hash: "7".repeat(64), receipts: [] },
        };
      return HttpResponse.json(envelope(result));
    }),
    http.post("*/api/v1/strategy-promotions/commands/lookup", async ({ request }) => {
      const body = (await request.json()) as Command;
      looked.push(body);
      return HttpResponse.json(
        envelope({
          original_request: body,
          status: "uncertain",
          message: "结果待确认，请保留原操作。",
        } satisfies Result),
      );
    }),
    http.post("*/api/v1/strategy-promotions/commands/resume", async ({ request }) => {
      const body = (await request.json()) as Command;
      resumed.push(body);
      return HttpResponse.json(
        envelope({
          original_request: body,
          status: "uncertain",
          message: "结果待确认，请保留原操作。",
        } satisfies Result),
      );
    }),
  );
  return { submitted, looked, resumed, gets, getPrepared: () => prepared };
}
function mount(generation = templateGeneration, viewer = "tester") {
  const queryClient = testQueryClient();
  queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer, generationId: generation }));
  function element(nextGeneration = generation, nextViewer = viewer) {
    return (
      <AppProviders queryClient={queryClient}>
        <StrategyPromotionPanel
          viewer={nextViewer}
          generation={nextGeneration}
          ready
          sourceKind="template"
          strategyId={templateId}
          head={templateHead}
        />
      </AppProviders>
    );
  }
  const result = render(element());
  return { ...result, queryClient, element };
}
async function evaluate() {
  const user = userEvent.setup();
  const button = await screen.findByRole("button", { name: "评估下一阶段" });
  await waitFor(() => expect(button).toBeEnabled());
  await user.click(button);
  await screen.findByRole("table", { name: "阶段证据" });
  return user;
}
async function prepare() {
  const user = await evaluate();
  await user.click(screen.getByRole("button", { name: "批准晋级" }));
  const dialog = await screen.findByRole("dialog", { name: "批准晋级" });
  return { user, dialog };
}

describe("原版本的阶段评估和手动晋级", () => {
  it("四阶段只读展示，不在挂载时评估、运行或批准", async () => {
    const calls = setup();
    mount();
    await screen.findByLabelText("验证版本");
    for (const stage of ["探索", "可比", "模拟候选", "监控批准"])
      expect(screen.getByText(stage, { exact: true })).toBeVisible();
    expect(calls.submitted).toHaveLength(0);
    expect(screen.queryByText(target.strategy_id)).not.toBeInTheDocument();
    expect(screen.queryByText(candidate.result_hash ?? "")).not.toBeInTheDocument();
  });
  it("选择原独立验证版本，评估绑定其子ID/head、完整父族和原证据", async () => {
    const second = {
      ...candidate,
      target: { ...target, strategy_id: "template_private_second", name: "低位观察·另一版本" },
      selection: { ...candidate.selection, experiment_id: "e".repeat(64) },
    };
    const calls = setup({ ...data(), candidates: [candidate, second] });
    mount();
    const user = userEvent.setup();
    await user.selectOptions(
      await screen.findByLabelText("验证版本"),
      screen.getByRole("option", { name: "方案 2 · 第 1 版" }),
    );
    await evaluate();
    expect(calls.submitted[0]).toMatchObject({
      kind: "request_promotion_review",
      target: second.target,
      selection: second.selection,
      expected_revision: 0,
      generation_id: templateGeneration,
    });
    expect(calls.submitted[0]?.target.strategy_id).not.toBe(templateId);
  });
  it("评估通过仍停在探索，只有人工批准才改变阶段", async () => {
    const calls = setup();
    mount();
    await evaluate();
    expect(screen.getByLabelText("当前阶段")).toHaveTextContent("探索");
    expect(screen.getByRole("table", { name: "阶段证据" })).toHaveTextContent("已满足");
    expect(calls.submitted.map((value) => value.kind)).toEqual(["request_promotion_review"]);
  });
  it.each(["missing", "failed"] as const)("证据%s时显示原因并禁用批准", async (status) => {
    setup();
    server.use(
      http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestPromotionReview"];
        return HttpResponse.json(
          envelope({
            original_request: body,
            status: "completed",
            message: "评估完成。",
            review: review(body, status),
          } satisfies Result),
        );
      }),
    );
    mount();
    await evaluate();
    expect(screen.getByRole("button", { name: "批准晋级" })).toBeDisabled();
    expect(screen.getByRole("table", { name: "阶段证据" })).toHaveTextContent(
      status === "missing" ? "待补证据" : "未通过",
    );
  });
  it("准备不生效；取消不批准并返回原按钮焦点", async () => {
    const calls = setup();
    mount();
    const { user, dialog } = await prepare();
    expect(calls.submitted.map((value) => value.kind)).toEqual([
      "request_promotion_review",
      "prepare_promotion_approval",
    ]);
    await user.click(within(dialog).getByRole("button", { name: /^取\s*消$/ }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    await waitFor(() => expect(screen.getByRole("button", { name: "批准晋级" })).toHaveFocus());
    expect(calls.submitted).toHaveLength(2);
  });
  it("准确名称后提交原签发准备；新UUID仅用于批准请求", async () => {
    const calls = setup();
    mount();
    const { user, dialog } = await prepare();
    const confirm = within(dialog).getByRole("button", { name: "确认晋级" });
    expect(confirm).toBeDisabled();
    await user.type(within(dialog).getByRole("textbox"), `${target.name}x`);
    expect(confirm).toBeDisabled();
    await user.clear(within(dialog).getByRole("textbox"));
    await user.type(within(dialog).getByRole("textbox"), target.name);
    await user.click(confirm);
    await screen.findByText("阶段已批准。", { exact: true });
    const body = calls.submitted.at(-1);
    expect(body).toMatchObject({
      kind: "approve_promotion",
      target,
      entered_name: target.name,
      preparation: calls.getPrepared(),
    });
    expect(new Set(calls.submitted.map((value) => value.command_id)).size).toBe(3);
    await waitFor(() => expect(screen.getByLabelText("当前阶段")).toHaveTextContent("可比"));
  });
  it.each(["过期", "操作人", "角色版本", "角色摘要", "策略版本"])(
    "签发%s不匹配时拒绝确认",
    async (change) => {
      setup();
      mount();
      await evaluate();
      server.use(
        http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
          const body = (await request.json()) as Schemas["PreparePromotionApproval"];
          const prior = review({
            ...body,
            kind: "request_promotion_review",
            expected_revision: 0,
            selection: candidate.selection,
          });
          const issued: Schemas["PreparedPromotionApproval-Output"] = {
            actor_id: change === "操作人" ? "other" : "tester",
            role_revision: change === "角色版本" ? 2 : 1,
            role_state_hash: change === "角色摘要" ? "a".repeat(64) : roleHash,
            preparation_id: body.command_id,
            issuance_proof: "4".repeat(64),
            issued_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + (change === "过期" ? -1 : 120_000)).toISOString(),
            review:
              change === "策略版本"
                ? { ...prior, target: { ...target, head: { ...templateHead, version: 2 } } }
                : prior,
          };
          return HttpResponse.json(
            envelope({
              original_request: body,
              preparation: issued,
              status: "completed",
              message: "请确认。",
            } satisfies Result),
          );
        }),
      );
      await userEvent.setup().click(screen.getByRole("button", { name: "批准晋级" }));
      expect(await screen.findByRole("alert")).toHaveTextContent("确认");
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    },
  );
  it("回执丢失后重载，只查原UUID与正文，不自动重新提交", async () => {
    const calls = setup();
    server.use(
      http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
        calls.submitted.push((await request.json()) as Command);
        return HttpResponse.error();
      }),
    );
    const view = mount();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "评估下一阶段" }));
    await screen.findByText("结果待确认，请查看原操作。", { exact: true });
    const original = calls.submitted[0];
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toContain(
      original?.command_id ?? "missing",
    );
    view.unmount();
    mount();
    await waitFor(() => expect(calls.looked).toContainEqual(original));
    expect(calls.submitted).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "恢复原操作" }));
    await waitFor(() => expect(calls.resumed).toContainEqual(original));
    expect(calls.submitted).toHaveLength(1);
  });
  it("服务器返回另一UUID，保留原请求并拒绝使用该评估", async () => {
    const calls = setup();
    server.use(
      http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestPromotionReview"];
        calls.submitted.push(body);
        return HttpResponse.json(
          envelope({
            original_request: { ...body, command_id: "99999999-9999-4999-8999-999999999999" },
            status: "completed",
            message: "评估完成。",
            review: review(body),
          } satisfies Result),
        );
      }),
    );
    mount();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "评估下一阶段" }));
    await screen.findByText("结果待确认，请查看原操作。", { exact: true });
    expect(screen.queryByRole("table", { name: "阶段证据" })).not.toBeInTheDocument();
    expect(calls.submitted).toHaveLength(1);
  });
  it.each(["同一时刻", "不同微秒"])("原Pyd六位时间：%s不改写原恢复正文", async (time) => {
    const calls = setup();
    server.use(
      http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestPromotionReview"];
        calls.submitted.push(body);
        const timestamp = body.requested_at.replace(
          /\.(\d{3})Z$/,
          time === "同一时刻" ? ".$1000Z" : ".$1001Z",
        );
        return HttpResponse.json(
          envelope({
            original_request: { ...body, requested_at: timestamp },
            status: "completed",
            message: "评估完成。",
            review: review(body),
          } satisfies Result),
        );
      }),
    );
    mount();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "评估下一阶段" }));
    if (time === "同一时刻") {
      await screen.findByRole("table", { name: "阶段证据" });
      expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBeNull();
    } else {
      await screen.findByText("结果待确认，请查看原操作。", { exact: true });
      const original = JSON.parse(sessionStorage.getItem(PROMOTION_PENDING_KEY) ?? "null") as {
        body: Command;
      };
      expect(original.body).toEqual(calls.submitted[0]);
      expect(screen.queryByRole("table", { name: "阶段证据" })).not.toBeInTheDocument();
    }
  });
  it("显式运行原六折；提交回执不表示验证通过", async () => {
    const calls = setup(data("comparable"));
    mount();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "运行六折验证" }));
    await screen.findByText("验证任务已提交，请等待全部结果。", { exact: true });
    expect(calls.submitted[0]).toMatchObject({
      kind: "run_strategy_walk_forward",
      fold_count: 6,
      target,
      selection: candidate.selection,
    });
    expect(screen.getByLabelText("当前阶段")).toHaveTextContent("可比");
  });
  it("模拟候选评估绑定原账户和完整band任务，不用浏览器收益值", async () => {
    const original = data("paper_candidate");
    original.paper_accounts = [
      {
        target_key: "d".repeat(64),
        account_id: "original-paper",
        band_jobs: ["22222222-2222-4222-8222-222222222222"],
      },
    ];
    const calls = setup(original);
    mount();
    await evaluate();
    expect(calls.submitted[0]).toMatchObject({
      kind: "request_promotion_review",
      expected_revision: 2,
      selection: {
        ...candidate.selection,
        paper_account_id: "original-paper",
        band_job_id: original.paper_accounts[0]?.band_jobs[0],
      },
    });
    expect(calls.submitted[0]).not.toHaveProperty("net_return");
  });
  it("当前角色只读仍能看原阶段与证据，不能评估或恢复写入", async () => {
    setup(
      {
        ...data("comparable"),
        can_evaluate: false,
        can_prepare_approval: false,
        can_run_walk_forward: false,
        reason: "当前角色只能查看评估记录。",
      },
      currentRole("viewer"),
    );
    mount();
    await screen.findByLabelText("验证版本");
    expect(screen.getByRole("button", { name: "评估下一阶段" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "运行六折验证" })).toBeDisabled();
    expect(screen.getByLabelText("当前阶段")).toHaveTextContent("可比");
  });
  it("换账号立即清私有评估、确认和原待确认正文", async () => {
    setup();
    const view = mount();
    await prepare();
    sessionStorage.setItem(
      PROMOTION_PENDING_KEY,
      JSON.stringify({
        viewer: "tester",
        body: {
          kind: "request_promotion_review",
          command_id: "33333333-3333-4333-8333-333333333333",
          requested_at: new Date().toISOString(),
          generation_id: templateGeneration,
          target,
          expected_revision: 0,
          selection: candidate.selection,
        },
      }),
    );
    server.use(
      http.get("*/api/v1/collaboration/me", () =>
        HttpResponse.json(envelope(currentRole("viewer", "other"))),
      ),
      http.get("*/api/v1/strategy-promotions/:strategy", () =>
        HttpResponse.json(
          envelope({
            ...data(),
            candidates: [],
            can_evaluate: false,
            can_prepare_approval: false,
            can_run_walk_forward: false,
          }),
        ),
      ),
    );
    act(() => view.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" })));
    view.rerender(view.element(templateGeneration, "other"));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.queryByRole("table", { name: "阶段证据" })).not.toBeInTheDocument();
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBeNull();
  });
  it("代际变化关闭原确认，清旧评估；不自动批准", async () => {
    const calls = setup();
    const view = mount();
    await prepare();
    const next = "a".repeat(64);
    server.use(
      http.get("*/api/v1/collaboration/me", () => HttpResponse.json(envelope(currentRole(), next))),
      http.get("*/api/v1/strategy-promotions/:strategy", () =>
        HttpResponse.json(envelope(data(), next)),
      ),
    );
    act(() => view.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ generationId: next })));
    view.rerender(view.element(next));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.queryByRole("table", { name: "阶段证据" })).not.toBeInTheDocument();
    expect(calls.submitted).toHaveLength(2);
  });
  it("撤销管理员权限后旧确认不能继续", async () => {
    const calls = setup();
    const view = mount();
    await prepare();
    server.use(
      http.get("*/api/v1/collaboration/me", () =>
        HttpResponse.json(
          envelope({ ...currentRole("researcher"), revision: 2, state_sha256: "a".repeat(64) }),
        ),
      ),
    );
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["collaboration", "me"] });
    });
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(calls.submitted).toHaveLength(2);
  });
  it("关闭默认有明确空态；来源不匹配时不操作", async () => {
    const calls = setup({
      ...data(),
      availability: "unavailable",
      candidates: [],
      can_evaluate: false,
      can_prepare_approval: false,
      can_run_walk_forward: false,
      reason: "人工晋级未启用。",
    });
    mount();
    expect(await screen.findByText("人工晋级未启用。", { exact: true })).toBeVisible();
    expect(calls.submitted).toHaveLength(0);
  });
});

const builtinTarget: Schemas["StrategyPromotionTarget"] = {
  ...target,
  source_kind: "builtin",
  strategy_id: "n_shape",
  name: "N 字形态",
};
function originalForPage(): Schemas["RequestPromotionReview"] {
  return {
    kind: "request_promotion_review",
    command_id: "33333333-3333-4333-8333-333333333333",
    requested_at: "2026-10-06T01:00:00.123Z",
    generation_id: templateGeneration,
    target: builtinTarget,
    expected_revision: 0,
    selection: candidate.selection,
  };
}
function pageCatalog(generation = templateGeneration): Schemas["Envelope_StrategyCatalogData_"] {
  return envelope(
    {
      available: true,
      strategies: [
        {
          strategy_id: builtinTarget.strategy_id,
          name: builtinTarget.name,
          version: 1,
          registered_at: "2026-10-06T01:00:00Z",
          parameters: [{ key: "window", label: "观察窗口", display_value: "5 分钟" }],
        },
      ],
    },
    generation,
  );
}
function setupPage() {
  const calls = setup({
    ...data(),
    source_kind: "builtin",
    strategy_id: builtinTarget.strategy_id,
    candidates: [{ ...candidate, target: builtinTarget, template_parent: null }],
  });
  const privateGets: string[] = [];
  server.use(
    http.get("*/api/v1/strategies", () => HttpResponse.json(pageCatalog())),
    http.get("*/api/v1/collaboration/me", () => {
      privateGets.push("role");
      return HttpResponse.json(envelope(currentRole()));
    }),
    http.get("*/api/v1/strategy-templates", () => {
      privateGets.push("templates");
      const empty: Schemas["StrategyTemplateCatalogData"] = {
        availability: "unavailable",
        available_at: null,
        templates: [],
        can_create: false,
      };
      return HttpResponse.json(envelope(empty));
    }),
    http.get("*/api/v1/strategy-templates/sources", () => {
      privateGets.push("sources");
      const empty: Schemas["StrategyTemplateSourcesData"] = {
        availability: "unavailable",
        pools: [],
        signals: [],
        conditions: [],
        comparison_fields: [],
        can_create: false,
      };
      return HttpResponse.json(envelope(empty));
    }),
  );
  return { ...calls, privateGets };
}
function mountPage() {
  const queryClient = testQueryClient();
  function BootPage() {
    useMeta();
    return <StrategiesPage />;
  }
  const element = (
    <AppProviders queryClient={queryClient}>
      <BootPage />
    </AppProviders>
  );
  return { ...render(element), queryClient, element };
}

describe("策略整页启动与数据更新", () => {
  it("身份延迟时保留原记录且不读私有内容；同人确认后只查原UUID与正文", async () => {
    const calls = setupPage();
    const original = originalForPage();
    const stored = JSON.stringify({ viewer: "tester", body: original });
    sessionStorage.setItem(PROMOTION_PENDING_KEY, stored);
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let metaRequests = 0;
    server.use(
      http.get("*/api/v1/meta", async () => {
        metaRequests += 1;
        await gate;
        return HttpResponse.json(metaEnvelope());
      }),
    );
    mountPage();
    await waitFor(() => expect(metaRequests).toBe(1));
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBe(stored);
    expect(screen.queryByRole("region", { name: "阶段评估" })).not.toBeInTheDocument();
    expect(calls.privateGets).toHaveLength(0);
    expect(calls.gets).toHaveLength(0);
    expect(calls.looked).toHaveLength(0);
    release();
    await waitFor(() => expect(calls.looked).toEqual([original]));
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBe(stored);
    expect(calls.submitted).toHaveLength(0);
    expect(calls.resumed).toHaveLength(0);
  });
  it.each(["原请求", "不透明"])("启动身份请求失败时保留%s，不展示或发送其中内容", async (kind) => {
    const calls = setupPage();
    const stored =
      kind === "原请求"
        ? JSON.stringify({ viewer: "tester", body: originalForPage() })
        : "opaque-original-request-that-must-not-be-parsed";
    sessionStorage.setItem(PROMOTION_PENDING_KEY, stored);
    server.use(http.get("*/api/v1/meta", () => new HttpResponse(null, { status: 503 })));
    mountPage();
    await screen.findByText("策略目录暂时无法核对，请稍后重试。", { exact: true });
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBe(stored);
    expect(screen.queryByText(stored)).not.toBeInTheDocument();
    expect(calls.privateGets).toHaveLength(0);
    expect(calls.gets).toHaveLength(0);
    expect(calls.looked).toHaveLength(0);
    expect(calls.submitted).toHaveLength(0);
  });
  it.each(["other", null])("启动核对账号%s后清理另一人的原记录", async (viewer) => {
    const calls = setupPage();
    sessionStorage.setItem(
      PROMOTION_PENDING_KEY,
      JSON.stringify({ viewer: "tester", body: originalForPage() }),
    );
    server.use(
      http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ viewer }))),
      http.get("*/api/v1/collaboration/me", () =>
        HttpResponse.json(envelope(currentRole("viewer", viewer ?? ""))),
      ),
    );
    mountPage();
    await screen.findByRole("table", { name: "当前参数" });
    await waitFor(() => expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBeNull());
    expect(calls.looked).toHaveLength(0);
    expect(calls.submitted).toHaveLength(0);
    expect(screen.queryByRole("button", { name: "恢复原操作" })).not.toBeInTheDocument();
  });
  it.each(["other", null])(
    "整页同人恢复后切换为%s，立即撤下原私有视图并清理原记录",
    async (viewer) => {
      const calls = setupPage();
      const original = originalForPage();
      sessionStorage.setItem(
        PROMOTION_PENDING_KEY,
        JSON.stringify({ viewer: "tester", body: original }),
      );
      server.use(http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope())));
      const view = mountPage();
      await waitFor(() => expect(calls.looked).toEqual([original]));
      await screen.findByRole("button", { name: "恢复原操作" });
      server.use(
        http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ viewer }))),
        http.get("*/api/v1/collaboration/me", () =>
          HttpResponse.json(envelope(currentRole("viewer", viewer ?? ""))),
        ),
      );
      act(() => view.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer })));
      await waitFor(() => expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBeNull());
      expect(screen.queryByRole("button", { name: "恢复原操作" })).not.toBeInTheDocument();
      expect(screen.queryByRole("table", { name: "阶段证据" })).not.toBeInTheDocument();
      expect(calls.looked).toEqual([original]);
      expect(calls.submitted).toHaveLength(0);
    },
  );
  it("发布后目录409立即核对meta、撤下旧私有内容，并按新代恢复原UUID", async () => {
    const calls = setupPage();
    const original = originalForPage();
    const stored = JSON.stringify({ viewer: "tester", body: original });
    sessionStorage.setItem(PROMOTION_PENDING_KEY, stored);
    let metaRequests = 0;
    let changed = false;
    const next = "e".repeat(64);
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const catalogRequests: (string | null)[] = [];
    server.use(
      http.get("*/api/v1/meta", async () => {
        metaRequests += 1;
        if (changed) await gate;
        return HttpResponse.json(
          metaEnvelope({ generationId: changed ? next : templateGeneration }),
        );
      }),
      http.get("*/api/v1/strategies", ({ request }) => {
        const generation = new URL(request.url).searchParams.get("generation_id");
        catalogRequests.push(generation);
        if (changed && generation !== next) return new HttpResponse(null, { status: 409 });
        const value = pageCatalog(generation ?? templateGeneration);
        if (changed)
          value.data.strategies = value.data.strategies.map((item) => ({
            ...item,
            parameters: item.parameters.map((parameter) => ({
              ...parameter,
              display_value: "10 分钟",
            })),
          }));
        return HttpResponse.json(value);
      }),
      http.get("*/api/v1/collaboration/me", () =>
        HttpResponse.json(envelope(currentRole(), changed ? next : templateGeneration)),
      ),
    );
    const view = mountPage();
    await waitFor(() => expect(calls.looked).toEqual([original]));
    expect(screen.getByRole("table", { name: "当前参数" })).toHaveTextContent("5 分钟");
    expect(screen.getByRole("heading", { name: "我的策略" })).toBeVisible();
    changed = true;
    await act(async () => {
      await view.queryClient.invalidateQueries({ queryKey: ["strategies", "catalog"] });
    });
    await waitFor(() => expect(metaRequests).toBe(2));
    expect(screen.queryByRole("table", { name: "当前参数" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "我的策略" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "阶段评估" })).not.toBeInTheDocument();
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBe(stored);
    release();
    await waitFor(() =>
      expect(screen.getByRole("table", { name: "当前参数" })).toHaveTextContent("10 分钟"),
    );
    await waitFor(() =>
      expect(view.queryClient.getQueryState(META_QUERY_KEY)?.fetchStatus).toBe("idle"),
    );
    expect(metaRequests).toBe(2);
    expect(catalogRequests).toContain(templateGeneration);
    expect(catalogRequests).toContain(next);
    expect(calls.looked.every((body) => JSON.stringify(body) === JSON.stringify(original))).toBe(
      true,
    );
    expect(sessionStorage.getItem(PROMOTION_PENDING_KEY)).toBe(stored);
    expect(calls.submitted).toHaveLength(0);
  });
  it("目录409核对后仍是旧代，只刷新一次meta，不自动重发目录或显示旧状态", async () => {
    setupPage();
    let metaRequests = 0;
    let catalogRequests = 0;
    server.use(
      http.get("*/api/v1/meta", () => {
        metaRequests += 1;
        return HttpResponse.json(metaEnvelope());
      }),
      http.get("*/api/v1/strategies", () => {
        catalogRequests += 1;
        return new HttpResponse(null, { status: 409 });
      }),
    );
    const view = mountPage();
    await waitFor(() => expect(metaRequests).toBe(2));
    await waitFor(() =>
      expect(view.queryClient.getQueryState(META_QUERY_KEY)?.fetchStatus).toBe("idle"),
    );
    view.rerender(view.element);
    expect(metaRequests).toBe(2);
    expect(catalogRequests).toBe(1);
    expect(screen.queryByRole("table", { name: "当前参数" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "我的策略" })).not.toBeInTheDocument();
    expect(screen.getByText("数据已更新，请重新查看策略。", { exact: true })).toBeVisible();
  });
  it("目录503不触发meta刷新或宽泛自动重试", async () => {
    setupPage();
    let metaRequests = 0;
    let catalogRequests = 0;
    server.use(
      http.get("*/api/v1/meta", () => {
        metaRequests += 1;
        return HttpResponse.json(metaEnvelope());
      }),
      http.get("*/api/v1/strategies", () => {
        catalogRequests += 1;
        return new HttpResponse(null, { status: 503 });
      }),
    );
    mountPage();
    await screen.findByText("策略目录暂时无法加载，请稍后重试。", { exact: true });
    expect(metaRequests).toBe(1);
    expect(catalogRequests).toBe(1);
  });
});
