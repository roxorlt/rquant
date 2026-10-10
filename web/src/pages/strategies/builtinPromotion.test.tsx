import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { PROMOTION_PENDING_KEY } from "./promotionCommands";

// Public generated DTO fixtures demonstrate UI boundaries, not sealed market or worker results.
type Command = Schemas["StrategyPromotionCommandData"]["original_request"];
type Candidate = Schemas["StrategyPromotionCandidateReference"];
type Review = Schemas["StrategyPromotionReview-Output"];
type Preparation = Schemas["PreparedPromotionApproval-Output"];
const generation = "a".repeat(64);
const roleHash = "b".repeat(64);
const targetKey = "c".repeat(64);
const target: Schemas["StrategyPromotionTarget"] = {
  source_kind: "builtin",
  owner_id: "tester",
  strategy_id: "n_shape",
  name: "N 字形态",
  head: {
    version: 1,
    registration_fingerprint: "1".repeat(64),
    record_hash: "2".repeat(64),
    spec_fingerprint: "3".repeat(64),
  },
  parameter_fingerprint: "4".repeat(64),
  cost_fingerprint: "5".repeat(64),
};
const first: Candidate = {
  target,
  family_name: "固定验证",
  has_sealed_reference: true,
  is_current: true,
  parent_count: 4,
  input_hash: "6".repeat(64),
  spec_hash: "7".repeat(64),
  manifest_hash: "8".repeat(64),
  result_hash: "9".repeat(64),
  job_id: "11111111-1111-4111-8111-111111111111",
  selection: {
    family_id: "original-native-family-a",
    experiment_id: "d".repeat(64),
    walk_forward_id: null,
    paper_account_id: null,
    band_job_id: null,
  },
  train_window: { start_date: "2026-01-01", end_date: "2026-02-28" },
  validation_window: { start_date: "2026-03-01", end_date: "2026-04-30" },
};
const second: Candidate = {
  ...first,
  family_name: "复核验证",
  job_id: "22222222-2222-4222-8222-222222222222",
  selection: {
    ...first.selection,
    family_id: "original-native-family-b",
    experiment_id: "e".repeat(64),
  },
};
const role: Schemas["CollaborationMe"] = {
  available: true,
  mode: "enforced",
  username: "tester",
  role: "admin",
  revision: 1,
  state_sha256: roleHash,
  can_manage_users: true,
  can_research: true,
  can_read_audit: true,
};
function envelope<T>(data: T, id = generation) {
  return { data, serving: metaEnvelope({ viewer: "tester", generationId: id }).serving };
}
function source(
  stage: Schemas["PromotionStage"] = "exploratory",
): Schemas["StrategyPromotionData"] {
  const revision = ["exploratory", "comparable", "paper_candidate", "monitor_approved"].indexOf(
    stage,
  );
  return {
    source_kind: "builtin",
    strategy_id: target.strategy_id,
    availability: "populated",
    available_at: "2026-10-07T01:00:00Z",
    states: revision
      ? [
          {
            owner_id: "tester",
            target_key: targetKey,
            state: { target, stage, revision, latest_approval_hash: "f".repeat(64) },
            applied_at: "2026-10-07T01:00:00Z",
          },
        ]
      : [],
    reviews: [],
    next_offset: null,
    candidates: [first, second],
    walk_forward: [],
    paper_accounts: [],
    can_evaluate: true,
    can_prepare_approval: true,
    can_run_walk_forward: true,
    reason: "",
  };
}
function review(
  body: Schemas["RequestPromotionReview"],
  status: Schemas["PromotionGate-Output"]["status"] = "satisfied",
): Review {
  const stages: Schemas["PromotionStage"][] = [
    "exploratory",
    "comparable",
    "paper_candidate",
    "monitor_approved",
  ];
  return {
    command_id: body.command_id,
    actor_id: "tester",
    target: body.target,
    metadata_identity: {
      instance_id: "synthetic-ui-owner",
      path: "/synthetic/native-owner.sqlite",
      st_dev: 1,
      st_ino: 2,
    },
    from_stage: stages[body.expected_revision] ?? "exploratory",
    to_stage: stages[body.expected_revision + 1] ?? "comparable",
    expected_revision: body.expected_revision,
    policy_hash: "1".repeat(64),
    evidence_hash: "2".repeat(64),
    review_id: "3".repeat(64),
    selection: body.selection,
    observed_at: new Date().toISOString(),
    gates: [
      {
        key: "validation_trades",
        status,
        message: "原完整交易不足，不能批准。",
        value: status === "satisfied" ? "32" : null,
      },
    ],
  };
}
function setup(initial = source()) {
  let published = initial;
  let lastReview: Review | null = null;
  let prepared: Preparation | null = null;
  const gets: URL[] = [];
  const submitted: Command[] = [];
  const looked: unknown[] = [];
  const resumed: unknown[] = [];
  function command(value: unknown): Command {
    if (
      value === null ||
      typeof value !== "object" ||
      !("kind" in value) ||
      !("command_id" in value) ||
      typeof value.command_id !== "string" ||
      !("requested_at" in value) ||
      typeof value.requested_at !== "string" ||
      !("generation_id" in value) ||
      typeof value.generation_id !== "string" ||
      !("target" in value)
    )
      throw new Error("Original typed command required");
    expect(value.target).toEqual(target);
    const base = {
      command_id: value.command_id,
      requested_at: value.requested_at,
      generation_id: value.generation_id,
      target,
    };
    if (value.kind === "prepare_promotion_approval") {
      if (!("review_id" in value) || typeof value.review_id !== "string")
        throw new Error("Original review required");
      return { ...base, kind: value.kind, review_id: value.review_id };
    }
    if (value.kind === "approve_promotion") {
      if (
        !prepared ||
        !("entered_name" in value) ||
        typeof value.entered_name !== "string" ||
        !("preparation" in value)
      )
        throw new Error("Issued preparation required");
      expect(value.preparation).toEqual(prepared);
      return { ...base, kind: value.kind, preparation: prepared, entered_name: value.entered_name };
    }
    if (
      !("selection" in value) ||
      value.selection === null ||
      typeof value.selection !== "object" ||
      !("family_id" in value.selection)
    )
      throw new Error("Original selection required");
    const familyId = value.selection.family_id;
    const chosen = published.candidates.find((item) => item.selection.family_id === familyId);
    if (!chosen) throw new Error("Original candidate required");
    const raw = value.selection;
    if (!("walk_forward_id" in raw) || !("paper_account_id" in raw) || !("band_job_id" in raw))
      throw new Error("Complete evidence selection required");
    const { walk_forward_id, paper_account_id, band_job_id } = raw;
    if (
      (walk_forward_id !== null && typeof walk_forward_id !== "string") ||
      (paper_account_id !== null && typeof paper_account_id !== "string") ||
      (band_job_id !== null && typeof band_job_id !== "string")
    )
      throw new Error("Typed evidence reference required");
    const selection = { ...chosen.selection, walk_forward_id, paper_account_id, band_job_id };
    expect(raw).toEqual(selection);
    if (
      value.kind === "request_promotion_review" &&
      "expected_revision" in value &&
      typeof value.expected_revision === "number"
    )
      return { ...base, kind: value.kind, selection, expected_revision: value.expected_revision };
    if (
      value.kind === "run_strategy_walk_forward" &&
      "fold_count" in value &&
      typeof value.fold_count === "number"
    )
      return { ...base, kind: value.kind, selection, fold_count: value.fold_count };
    throw new Error("Supported original command required");
  }
  server.use(
    http.get("*/api/v1/meta", () =>
      HttpResponse.json(metaEnvelope({ viewer: "tester", generationId: generation })),
    ),
    http.get("*/api/v1/collaboration/me", () => HttpResponse.json(envelope(role))),
    http.get("*/api/v1/strategy-templates", () =>
      HttpResponse.json(
        envelope({
          availability: "empty",
          available_at: null,
          templates: [],
          can_create: false,
        } satisfies Schemas["StrategyTemplateCatalogData"]),
      ),
    ),
    http.get("*/api/v1/strategy-templates/sources", () =>
      HttpResponse.json(
        envelope({
          availability: "unavailable",
          pools: [],
          signals: [],
          conditions: [],
          comparison_fields: [],
          can_create: false,
        } satisfies Schemas["StrategyTemplateSourcesData"]),
      ),
    ),
    http.get("*/api/v1/strategies", () =>
      HttpResponse.json(
        envelope({
          available: true,
          strategies: [
            {
              strategy_id: target.strategy_id,
              name: target.name,
              version: 1,
              registered_at: "2026-10-07T01:00:00Z",
              parameters: [{ key: "max_drawdown", label: "回撤上限", display_value: "10%" }],
            },
            {
              strategy_id: "auction_gap",
              name: "竞价跳空",
              version: 2,
              registered_at: "2026-10-07T01:00:00Z",
              parameters: [],
            },
          ],
        } satisfies Schemas["StrategyCatalogData"]),
      ),
    ),
    http.get("*/api/v1/strategy-promotions/:strategy", ({ request, params }) => {
      gets.push(new URL(request.url));
      return HttpResponse.json(
        envelope(
          params.strategy === target.strategy_id
            ? published
            : { ...source(), strategy_id: "auction_gap", candidates: [], availability: "empty" },
        ),
      );
    }),
    http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body = command(await request.json());
      submitted.push(body);
      let result: Schemas["StrategyPromotionCommandData"] = {
        original_request: body,
        status: "completed",
        message: "操作已完成。",
      };
      if (body.kind === "request_promotion_review") {
        lastReview = review(body);
        result = { ...result, review: lastReview };
      } else if (body.kind === "prepare_promotion_approval") {
        if (!lastReview) throw new Error("Prior review required");
        const issuedAt = Date.now();
        prepared = {
          actor_id: "tester",
          preparation_id: body.command_id,
          review: lastReview,
          role_revision: 1,
          role_state_hash: roleHash,
          issuance_proof: "4".repeat(64),
          issued_at: new Date(issuedAt).toISOString(),
          expires_at: new Date(issuedAt + 120_000).toISOString(),
        };
        result = { ...result, preparation: prepared };
      } else if (body.kind === "approve_promotion") {
        const prior = body.preparation.review;
        const state: Schemas["StrategyPromotionState"] = {
          target,
          stage: prior.to_stage,
          revision: prior.expected_revision + 1,
          latest_approval_hash: "f".repeat(64),
        };
        published = {
          ...published,
          states: [
            { owner_id: "tester", target_key: targetKey, state, applied_at: body.requested_at },
          ],
        };
        result = {
          ...result,
          status: "published",
          message: "阶段已批准。",
          approval: {
            actor_id: "tester",
            command_id: body.command_id,
            effect_id: body.command_id,
            original_request_hash: "5".repeat(64),
            review: prior,
            after: state,
            approval_id: "6".repeat(64),
            applied_at: body.requested_at,
          },
        };
      } else
        result = {
          ...result,
          message: "六折任务已提交，完整结果仍需核对。",
          walk_forward: { command_id: body.command_id, plan_hash: "7".repeat(64), receipts: [] },
        };
      return HttpResponse.json(envelope(result));
    }),
    http.post("*/api/v1/strategy-promotions/commands/lookup", async ({ request }) => {
      const body: unknown = await request.json();
      looked.push(body);
      return HttpResponse.json(
        envelope({
          original_request: body,
          status: "uncertain",
          message: "结果待确认，请保留原操作。",
        }),
      );
    }),
    http.post("*/api/v1/strategy-promotions/commands/resume", async ({ request }) => {
      const body: unknown = await request.json();
      resumed.push(body);
      return HttpResponse.json(
        envelope({
          original_request: body,
          status: "uncertain",
          message: "结果待确认，请保留原操作。",
        }),
      );
    }),
  );
  return { gets, submitted, looked, resumed };
}
async function selectSecond(user: ReturnType<typeof userEvent.setup>) {
  const control = await screen.findByLabelText("验证版本");
  await user.selectOptions(
    control,
    within(control).getByRole("option", { name: "方案 2 · 第 1 版" }),
  );
}
async function evaluate(user: ReturnType<typeof userEvent.setup>) {
  const button = await screen.findByRole("button", { name: "评估下一阶段" });
  await waitFor(() => expect(button).toBeEnabled());
  await user.click(button);
  await screen.findByRole("table", { name: "阶段证据" });
}
async function prepare(user: ReturnType<typeof userEvent.setup>) {
  await evaluate(user);
  await user.click(screen.getByRole("button", { name: "批准晋级" }));
  return screen.findByRole("dialog", { name: "批准晋级" });
}

describe("内置策略完整验证方案", () => {
  it("同一内置ID与head的第二份完整方案使用自己的原selection", async () => {
    const calls = setup();
    const user = userEvent.setup();
    renderApp("/strategies");
    await selectSecond(user);
    await evaluate(user);
    expect(calls.submitted[0]).toMatchObject({
      kind: "request_promotion_review",
      target,
      selection: second.selection,
      expected_revision: 0,
      generation_id: generation,
    });
    expect(screen.getByRole("region", { name: "阶段评估" })).toHaveTextContent("复核验证");
  });
  it("切完整方案清掉旧评估，不把同ID旧family的结果混入新方案", async () => {
    setup();
    const user = userEvent.setup();
    renderApp("/strategies");
    await evaluate(user);
    await selectSecond(user);
    await waitFor(() =>
      expect(screen.queryByRole("table", { name: "阶段证据" })).not.toBeInTheDocument(),
    );
    expect(screen.getByRole("button", { name: "批准晋级" })).toBeDisabled();
    expect(screen.getByRole("region", { name: "阶段评估" })).toHaveTextContent("复核验证");
  });
  it("目录按键选择以原builtin和version取数，GET不评估也不批准", async () => {
    const calls = setup();
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    await screen.findByLabelText("验证版本");
    expect(calls.gets[0]?.pathname).toContain("/n_shape");
    expect(calls.gets[0]?.searchParams.get("source_kind")).toBe("builtin");
    expect(calls.gets[0]?.searchParams.get("version")).toBe("1");
    expect(calls.gets[0]?.searchParams.get("generation_id")).toBe(generation);
    const list = screen.getByRole("table", { name: "策略列表" });
    within(list)
      .getByRole("row", { name: /竞价跳空/ })
      .focus();
    await user.keyboard("{Enter}");
    await waitFor(() =>
      expect(
        calls.gets.some(
          (url) => url.pathname.endsWith("/auction_gap") && url.searchParams.get("version") === "2",
        ),
      ).toBe(true),
    );
    expect(calls.submitted).toEqual([]);
    expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(view.container.querySelector("main")?.textContent).not.toContain("n_shape");
  });
  it("准备和取消不批准，取消恢复焦点，切方案撤下旧确认", async () => {
    const calls = setup();
    const user = userEvent.setup();
    renderApp("/strategies");
    const dialog = await prepare(user);
    expect(calls.submitted.map((item) => item.kind)).toEqual([
      "request_promotion_review",
      "prepare_promotion_approval",
    ]);
    await user.click(within(dialog).getByRole("button", { name: /^取\s*消$/ }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    await waitFor(() => expect(screen.getByRole("button", { name: "批准晋级" })).toHaveFocus());
    await user.click(screen.getByRole("button", { name: "批准晋级" }));
    await screen.findByRole("dialog", { name: "批准晋级" });
    await selectSecond(user);
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(calls.submitted.every((item) => item.kind !== "approve_promotion")).toBe(true);
  });
  it("只能输入准确原名称后批准该方案，评估与准备不改变阶段", async () => {
    const calls = setup();
    const user = userEvent.setup();
    renderApp("/strategies");
    await selectSecond(user);
    const dialog = await prepare(user);
    expect(screen.getByLabelText("当前阶段")).toHaveTextContent("探索");
    const confirm = within(dialog).getByRole("button", { name: "确认晋级" });
    await user.type(within(dialog).getByRole("textbox"), `${target.name}x`);
    expect(confirm).toBeDisabled();
    await user.clear(within(dialog).getByRole("textbox"));
    await user.type(within(dialog).getByRole("textbox"), target.name);
    await user.click(confirm);
    await screen.findByText("阶段已批准。", { exact: true });
    expect(calls.submitted.at(-1)).toMatchObject({
      kind: "approve_promotion",
      entered_name: target.name,
      target,
      preparation: { review: { selection: second.selection } },
    });
    expect(new Set(calls.submitted.map((item) => item.command_id)).size).toBe(3);
  });
  it("六窗只取同target和原family，运行仍不自动晋级", async () => {
    const initial = source("comparable");
    initial.walk_forward = [
      {
        target_key: targetKey,
        family_id: first.selection.family_id,
        experiment_id: first.selection.experiment_id,
        command_id: "33333333-3333-4333-8333-333333333333",
        fold_count: 6,
        submitted: true,
      },
      {
        target_key: "f".repeat(64),
        family_id: first.selection.family_id,
        experiment_id: first.selection.experiment_id,
        command_id: "44444444-4444-4444-8444-444444444444",
        fold_count: 2,
        submitted: true,
      },
      {
        target_key: targetKey,
        family_id: second.selection.family_id,
        experiment_id: second.selection.experiment_id,
        command_id: "55555555-5555-4555-8555-555555555555",
        fold_count: 3,
        submitted: true,
      },
    ];
    const calls = setup(initial);
    const user = userEvent.setup();
    renderApp("/strategies");
    const control = await screen.findByLabelText("验证结果");
    expect(within(control).queryByRole("option", { name: /2 折|3 折/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "运行六折验证" }));
    await waitFor(() => expect(calls.submitted).toHaveLength(1));
    expect(calls.submitted[0]).toMatchObject({
      kind: "run_strategy_walk_forward",
      target,
      fold_count: 6,
      selection: { ...first.selection, walk_forward_id: initial.walk_forward[0]?.command_id },
    });
    expect(screen.getByLabelText("当前阶段")).toHaveTextContent("可比");
  });
  it("账户与原收益区间只取同target，发送原owner引用而非浏览器数值", async () => {
    const initial = source("paper_candidate");
    initial.paper_accounts = [
      { target_key: targetKey, account_id: "original-paper-a", band_jobs: ["original-band-a"] },
      { target_key: "f".repeat(64), account_id: "foreign-paper", band_jobs: ["foreign-band"] },
    ];
    const calls = setup(initial);
    const user = userEvent.setup();
    renderApp("/strategies");
    const account = await screen.findByLabelText("模拟账户");
    expect(within(account).getAllByRole("option")).toHaveLength(2);
    await evaluate(user);
    expect(calls.submitted[0]).toMatchObject({
      expected_revision: 2,
      selection: {
        ...first.selection,
        paper_account_id: "original-paper-a",
        band_job_id: "original-band-a",
      },
    });
    expect(JSON.stringify(calls.submitted[0])).not.toContain("foreign");
  });
  it("丢回执重挂载查询并恢复第二方案原UUID和完整body，不自动新提交", async () => {
    const calls = setup();
    const sent: unknown[] = [];
    server.use(
      http.post("*/api/v1/strategy-promotions/commands", async ({ request }) => {
        sent.push(await request.json());
        return HttpResponse.error();
      }),
    );
    const user = userEvent.setup();
    const firstView = renderApp("/strategies");
    await selectSecond(user);
    await user.click(screen.getByRole("button", { name: "评估下一阶段" }));
    await screen.findByText("结果待确认，请查看原操作。", { exact: true });
    const saved = sessionStorage.getItem(PROMOTION_PENDING_KEY);
    expect(saved).not.toBeNull();
    firstView.unmount();
    renderApp("/strategies");
    await waitFor(() => expect(calls.looked).toContainEqual(sent[0]));
    await user.click(await screen.findByRole("button", { name: "恢复原操作" }));
    await waitFor(() => expect(calls.resumed).toContainEqual(sent[0]));
    expect(sent).toHaveLength(1);
    expect(JSON.parse(saved ?? "null").body).toEqual(sent[0]);
    expect(sent[0]).toMatchObject({ target, selection: second.selection });
  });
  it.each(["角色", "数据代"])("%s变化撤下旧确认，保留原owner读写栅栏", async (change) => {
    setup();
    const user = userEvent.setup();
    const view = renderApp("/strategies");
    await prepare(user);
    act(() => {
      if (change === "角色")
        view.queryClient.setQueryData(
          ["collaboration", "me", "tester", generation],
          envelope({
            ...role,
            revision: 2,
            state_sha256: "d".repeat(64),
            role: "viewer",
            can_research: false,
            can_manage_users: false,
          }),
        );
      else
        view.queryClient.setQueryData(
          META_QUERY_KEY,
          metaEnvelope({ viewer: "tester", generationId: "e".repeat(64) }),
        );
    });
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });
});
