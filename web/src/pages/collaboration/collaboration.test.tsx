import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { FactorResults } from "@/pages/factors/FactorResults";
import {
  diagnosticFactor,
  diagnosticResearch,
  diagnosticResult,
} from "@/pages/factors/factorDiagnostics.fixture";
import {
  templateCatalog,
  templateDetail,
  templateEnvelope,
  templateHead,
  templateId,
  templateSources,
} from "@/pages/strategies/template.fixture";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

type Role = Schemas["RoleEntry"]["role"];
const sha = "b".repeat(64);
const base = "*/api/v1/collaboration";
const pendingKey = "rquant.role.pending.v1";
const state: Schemas["RoleState"] = {
  schema_version: 1,
  revision: 1,
  content_sha256: sha,
  users: [
    { username: "tester", role: "admin" },
    { username: "reader", role: "viewer" },
    { username: "analyst", role: "researcher" },
  ],
};
function me(role: Role = "admin", viewer = "tester"): Schemas["CollaborationMe"] {
  return {
    available: true,
    mode: "enforced",
    username: viewer,
    role,
    revision: 1,
    state_sha256: sha,
    can_manage_users: role === "admin",
    can_research: role !== "viewer",
    can_read_audit: true,
  };
}
function envelope<T>(data: T, generation = sha) {
  return { data, serving: { ...metaEnvelope().serving, generation_id: generation } };
}
function issued(
  request: Schemas["SetUserRoleRequest"],
  expired = false,
): Schemas["IssuedRolePreparation"] {
  const now = Date.now();
  return {
    issuance_proof: "e".repeat(64),
    preparation: {
      schema_version: 1,
      request,
      request_sha256: "c".repeat(64),
      confirmation: {
        schema_version: 1,
        command_id: request.command_id,
        request_sha256: "c".repeat(64),
        actor_id: request.actor_id,
        target_id: request.target_id,
        old_role: "viewer",
        new_role: request.new_role,
        expected_revision: request.expected_revision,
        expected_state_sha256: request.expected_state_sha256,
        issued_at: new Date(now - (expired ? 121_000 : 0)).toISOString(),
        expires_at: new Date(now + (expired ? -1000 : 120_000)).toISOString(),
        content_sha256: "d".repeat(64),
      },
    },
  };
}
function setup(current = me(), expired = false) {
  const prepared: Schemas["SetUserRoleRequest"][] = [];
  const submitted: Schemas["CollaborationRoleSubmit"][] = [];
  const recovered: Schemas["CollaborationRoleLookup"][] = [];
  server.use(
    http.get(`${base}/me`, () => HttpResponse.json(envelope(current))),
    http.get(`${base}/users`, () => HttpResponse.json(envelope(state))),
    http.post(`${base}/roles/prepare`, async ({ request }) => {
      const body = (await request.json()) as Schemas["SetUserRoleRequest"];
      prepared.push(body);
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      return HttpResponse.json(envelope(issued(body, expired)));
    }),
    http.post(`${base}/roles/commands`, async ({ request }) => {
      const body = (await request.json()) as Schemas["CollaborationRoleSubmit"];
      submitted.push(body);
      return HttpResponse.json(
        envelope({
          command_id: body.command.command_id,
          status: "succeeded",
          enqueued_at: body.command.requested_at,
          completed_at: new Date().toISOString(),
          result: null,
          error: null,
        } satisfies Schemas["PageControlReceipt"]),
      );
    }),
    http.post(`${base}/roles/lookup`, async ({ request }) => {
      const body = (await request.json()) as Schemas["CollaborationRoleLookup"];
      recovered.push(body);
      return HttpResponse.json(
        envelope({
          found: true,
          receipt: {
            command_id: body.command.command_id,
            status: "succeeded",
            enqueued_at: body.command.requested_at,
          },
        } satisfies Schemas["RoleLookupData"]),
      );
    }),
  );
  return { prepared, submitted, recovered };
}
async function prepareReader() {
  const user = userEvent.setup();
  await screen.findByRole("table", { name: "用户与权限" });
  await user.selectOptions(screen.getByLabelText("reader 的角色"), "researcher");
  await user.click(screen.getByRole("button", { name: "修改 reader 的角色" }));
  const dialog = await screen.findByRole("dialog");
  return { user, dialog };
}

describe("用户与权限的真实确认和原请求恢复", () => {
  it.each(["六位小数", "整秒"])("服务器使用%s时间格式，保留签发正文后确认", async (format) => {
    if (format === "整秒") {
      const original = Date.prototype.toISOString;
      vi.spyOn(Date.prototype, "toISOString").mockImplementation(function (this: Date) {
        return original.call(this).replace(/\.\d{3}Z$/, ".000Z");
      });
    }
    const calls = setup();
    let response: Schemas["IssuedRolePreparation"] | undefined;
    server.use(
      http.post(`${base}/roles/prepare`, async ({ request }) => {
        const body = (await request.json()) as Schemas["SetUserRoleRequest"];
        calls.prepared.push(body);
        response = issued({
          ...body,
          requested_at:
            format === "整秒"
              ? body.requested_at.replace(".000Z", "Z")
              : body.requested_at.replace(/(\.\d{3})Z$/, "$1000Z"),
        });
        return HttpResponse.json(envelope(response));
      }),
    );
    renderApp("/users");
    const { user, dialog } = await prepareReader();
    await user.type(within(dialog).getByRole("textbox"), "reader");
    await user.click(within(dialog).getByRole("button", { name: "确认修改" }));
    await screen.findByText("角色已更新。");
    expect(calls.submitted).toHaveLength(1);
    expect(calls.submitted[0]?.command.preparation).toEqual(response?.preparation);
    expect(calls.submitted[0]?.command.requested_at).toBe(
      response?.preparation.request.requested_at,
    );
    expect(calls.submitted[0]?.issuance_proof).toBe(response?.issuance_proof);
    expect(response?.preparation.request.requested_at).not.toBe(calls.prepared[0]?.requested_at);
  });
  it.each(["不同毫秒", "不同微秒", "不同角色", "不同版本"])(
    "签发请求有%s差异，拒绝打开确认或提交",
    async (difference) => {
      const calls = setup();
      server.use(
        http.post(`${base}/roles/prepare`, async ({ request }) => {
          const body = (await request.json()) as Schemas["SetUserRoleRequest"];
          const changed = { ...body };
          if (difference === "不同毫秒")
            changed.requested_at = new Date(Date.parse(body.requested_at) + 1).toISOString();
          if (difference === "不同微秒")
            changed.requested_at = body.requested_at.replace(/(\.\d{3})Z$/, "$1001Z");
          if (difference === "不同角色") changed.new_role = "admin";
          if (difference === "不同版本") changed.expected_revision += 1;
          return HttpResponse.json(envelope(issued(changed)));
        }),
      );
      renderApp("/users");
      const user = userEvent.setup();
      await user.selectOptions(await screen.findByLabelText("reader 的角色"), "researcher");
      await user.click(screen.getByRole("button", { name: "修改 reader 的角色" }));
      await screen.findByRole("alert");
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
      expect(calls.submitted).toHaveLength(0);
    },
  );
  it("只在我的菜单展示当前可用权限入口", async () => {
    setup();
    renderApp();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "我的" }));
    await user.click(await screen.findByRole("menuitem", { name: "用户与权限" }));
    expect(await screen.findByRole("heading", { name: "用户与权限" })).toBeVisible();
  });
  it("准备不生效；输入准确账号后提交原签发正文与UUID", async () => {
    const calls = setup();
    renderApp("/users");
    const { user, dialog } = await prepareReader();
    expect(calls.submitted).toHaveLength(0);
    expect(calls.prepared[0]).toMatchObject({
      actor_id: "tester",
      target_id: "reader",
      new_role: "researcher",
      expected_revision: 1,
      expected_state_sha256: sha,
    });
    const confirm = within(dialog).getByRole("button", { name: "确认修改" });
    expect(confirm).toBeDisabled();
    await user.type(within(dialog).getByRole("textbox"), "readerx");
    expect(confirm).toBeDisabled();
    await user.clear(within(dialog).getByRole("textbox"));
    await user.type(within(dialog).getByRole("textbox"), "reader");
    await user.click(confirm);
    await screen.findByText("角色已更新。");
    expect(calls.submitted).toHaveLength(1);
    expect(calls.submitted[0]?.command).toMatchObject({
      ...calls.prepared[0],
      entered_target: "reader",
    });
    expect(calls.submitted[0]?.command.preparation.request).toEqual(calls.prepared[0]);
    expect(calls.submitted[0]?.issuance_proof).toBe("e".repeat(64));
    expect(sessionStorage.getItem(pendingKey)).toBeNull();
  });
  it("取消不提交，焦点回到原修改按钮", async () => {
    const calls = setup();
    renderApp("/users");
    const { user, dialog } = await prepareReader();
    await user.click(within(dialog).getByRole("button", { name: /^取\s*消$/ }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "修改 reader 的角色" })).toHaveFocus(),
    );
    expect(calls.submitted).toHaveLength(0);
  });
  it("服务器过期确认不允许提交", async () => {
    const calls = setup(me(), true);
    renderApp("/users");
    const { user, dialog } = await prepareReader();
    await user.type(within(dialog).getByRole("textbox"), "reader");
    expect(within(dialog).getByRole("button", { name: "确认修改" })).toBeDisabled();
    expect(within(dialog).getByRole("alert")).toHaveTextContent("确认已过期");
    expect(calls.submitted).toHaveLength(0);
  });
  it("确认后通信未知，刷新只读取原UUID，不创建第二次操作", async () => {
    const calls = setup();
    server.use(
      http.post(`${base}/roles/commands`, async ({ request }) => {
        calls.submitted.push((await request.json()) as Schemas["CollaborationRoleSubmit"]);
        return HttpResponse.error();
      }),
    );
    const first = renderApp("/users");
    const { user, dialog } = await prepareReader();
    await user.type(within(dialog).getByRole("textbox"), "reader");
    await user.click(within(dialog).getByRole("button", { name: "确认修改" }));
    await screen.findByRole("button", { name: "查看原操作" });
    expect(calls.submitted).toHaveLength(1);
    const original = calls.submitted[0];
    expect(sessionStorage.getItem(pendingKey)).not.toBeNull();
    first.unmount();
    renderApp("/users");
    await waitFor(() => expect(calls.recovered).toHaveLength(1));
    expect(calls.recovered[0]?.command).toEqual(original?.command);
    await screen.findByText("角色已更新。");
    expect(calls.submitted).toHaveLength(1);
    expect(calls.prepared).toHaveLength(1);
  });
  it("尚未找到原操作时保留正文，不重发新UUID", async () => {
    const calls = setup();
    server.use(
      http.post(`${base}/roles/commands`, async ({ request }) => {
        calls.submitted.push((await request.json()) as Schemas["CollaborationRoleSubmit"]);
        return HttpResponse.json(
          envelope({
            command_id: calls.submitted[0]?.command.command_id,
            status: "ambiguous",
            enqueued_at: new Date().toISOString(),
          }),
        );
      }),
      http.post(`${base}/roles/lookup`, async ({ request }) => {
        calls.recovered.push((await request.json()) as Schemas["CollaborationRoleLookup"]);
        return HttpResponse.json(envelope({ found: false, receipt: null }));
      }),
    );
    renderApp("/users");
    const { user, dialog } = await prepareReader();
    await user.type(within(dialog).getByRole("textbox"), "reader");
    await user.click(within(dialog).getByRole("button", { name: "确认修改" }));
    await user.click(await screen.findByRole("button", { name: "查看原操作" }));
    await screen.findByText("尚未找到原操作，请稍后再查。");
    expect(sessionStorage.getItem(pendingKey)).not.toBeNull();
    expect(calls.submitted).toHaveLength(1);
    expect(calls.prepared).toHaveLength(1);
  });
  it("换账号立即关闭旧确认、清理私有视图且不提交旧操作", async () => {
    const calls = setup();
    const app = renderApp("/users");
    await prepareReader();
    server.use(http.get(`${base}/me`, () => HttpResponse.json(envelope(me("viewer", "other")))));
    act(() => app.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" })));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.queryByRole("table", { name: "用户与权限" })).not.toBeInTheDocument();
    expect(sessionStorage.getItem(pendingKey)).toBeNull();
    expect(calls.submitted).toHaveLength(0);
  });
  it("权限状态改变使旧确认失效", async () => {
    const calls = setup();
    const app = renderApp("/users");
    await prepareReader();
    server.use(
      http.get(`${base}/me`, () =>
        HttpResponse.json(envelope({ ...me("viewer"), revision: 2, state_sha256: "f".repeat(64) })),
      ),
    );
    await act(async () => {
      await app.queryClient.invalidateQueries({ queryKey: ["collaboration", "me"] });
    });
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.queryByRole("table", { name: "用户与权限" })).not.toBeInTheDocument();
    expect(calls.submitted).toHaveLength(0);
  });
  it.each(["viewer", "researcher"] as const)("%s 不能管理用户", async (role) => {
    setup(me(role));
    renderApp("/users");
    expect(await screen.findByText("当前账号不能管理用户。")).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
  it("未启用协作时显示真实原因", async () => {
    setup({
      ...me(),
      available: false,
      mode: "legacy",
      role: null,
      revision: null,
      state_sha256: null,
      can_manage_users: false,
      can_research: false,
      can_read_audit: false,
    });
    renderApp("/users");
    expect(await screen.findByText("协作权限尚未启用。")).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
  it("过期列表或CAS冲突拒绝，不显示成功", async () => {
    setup();
    server.use(
      http.post(`${base}/roles/commands`, () =>
        HttpResponse.json({ detail: "权限已更新，请刷新后重试。" }, { status: 409 }),
      ),
    );
    renderApp("/users");
    const { user, dialog } = await prepareReader();
    await user.type(within(dialog).getByRole("textbox"), "reader");
    await user.click(within(dialog).getByRole("button", { name: "确认修改" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("权限已更新");
    expect(screen.queryByText("角色已更新。")).not.toBeInTheDocument();
  });
  it("最后一名管理员不能选降权角色", async () => {
    setup();
    renderApp("/users");
    const roles = await screen.findByLabelText("tester 的角色");
    expect(within(roles).getByRole("option", { name: "研究者" })).toBeDisabled();
    expect(within(roles).getByRole("option", { name: "查看者" })).toBeDisabled();
  });
  it("当前权限读取失败不保留旧管理视图", async () => {
    setup();
    const app = renderApp("/users");
    await screen.findByRole("table", { name: "用户与权限" });
    server.use(
      http.get(`${base}/me`, () =>
        HttpResponse.json({ detail: "权限暂不可用。" }, { status: 503 }),
      ),
    );
    await act(async () => {
      await app.queryClient.invalidateQueries({ queryKey: ["collaboration", "me"] });
    });
    expect(await screen.findByText("暂时读不到权限。")).toBeVisible();
    expect(screen.queryByRole("table", { name: "用户与权限" })).not.toBeInTheDocument();
  });
});

const auditItem: Schemas["CommandAuditItem"] = {
  schema_version: 1,
  command_id: "original-audit-command",
  command_kind: "run_strategy_template",
  command_hash: "c".repeat(64),
  actor_id: "tester",
  actor_label: "tester",
  command_status: "succeeded",
  effect_status: "succeeded",
  enqueued_at: "2026-10-06T01:00:00Z",
  completed_at: "2026-10-06T01:00:01Z",
  effect_started_at: "2026-10-06T01:00:00Z",
  effect_completed_at: "2026-10-06T01:00:01Z",
  outcome: "accepted",
  summary: "已接受请求",
};
function audit(items = [auditItem], next: string | null = null) {
  return envelope({
    schema_version: 1,
    role_revision: 1,
    source_generation: sha,
    items,
    next_cursor: next,
  } satisfies Schemas["CommandAuditPage"]);
}
describe("原命令操作记录", () => {
  it.each([
    ["submit_portfolio_backtest", "运行组合回测"],
    ["submit_factor_run", "运行因子研究"],
    ["save_factor_definition", "保存因子"],
    ["save_user_pool", "保存规则池子"],
    ["save_user_pool_v2", "保存条件池子"],
    ["save_user_pool_v3", "保存池子"],
    ["save_formula_pool_v1", "保存公式池子"],
    ["prepare_unit_run", "确认任务运行"],
    ["request_unit_run", "运行任务"],
    ["set_lab_scheduling_paused", "修改调度"],
  ])("真实命令 %s 显示短中文并用原类型筛选", async (kind, label) => {
    setup();
    const urls: URL[] = [];
    const original = { ...auditItem, command_kind: kind };
    server.use(
      http.get(`${base}/audit`, ({ request }) => {
        const url = new URL(request.url);
        urls.push(url);
        const selected = url.searchParams.get("command_kind");
        return HttpResponse.json(audit(!selected || selected === kind ? [original] : []));
      }),
    );
    renderApp("/audit");
    const user = userEvent.setup();
    const table = await screen.findByRole("table", { name: "操作记录" });
    expect(table).toHaveTextContent(label);
    expect(table).not.toHaveTextContent(kind);
    await user.selectOptions(screen.getByLabelText("操作"), kind);
    await user.click(screen.getByRole("button", { name: "筛选" }));
    await waitFor(() => expect(urls.at(-1)?.searchParams.get("command_kind")).toBe(kind));
    expect(await screen.findByRole("table", { name: "操作记录" })).toHaveTextContent(label);
    const options = Array.from((screen.getByLabelText("操作") as HTMLSelectElement).options);
    for (const removed of [
      "create_portfolio_backtest",
      "run_factor_research",
      "define_factor",
      "apply_pool_edit",
      "pause_scheduling",
      "resume_scheduling",
      "create_strategy_template",
      "export_portfolio_report",
      "generate_ai_request",
      "ai_assistance_request",
    ]) {
      expect(options.map((option) => option.value)).not.toContain(removed);
    }
  });
  it("接受请求与任务完成分开；内部编号只在详情", async () => {
    setup();
    server.use(http.get(`${base}/audit`, () => HttpResponse.json(audit())));
    renderApp("/audit");
    const user = userEvent.setup();
    const table = await screen.findByRole("table", { name: "操作记录" });
    expect(table).toHaveTextContent("已提交");
    expect(table).not.toHaveTextContent("回测完成");
    expect(table).not.toHaveTextContent(auditItem.command_id);
    expect(table).not.toHaveTextContent(auditItem.command_kind);
    await user.click(within(table).getByText("tester"));
    const detail = await screen.findByRole("dialog");
    expect(detail).toHaveTextContent(auditItem.command_id);
  });
  it("按原cursor翻页和返回；筛选重置cursor并发准确UTC时间", async () => {
    setup();
    const urls: URL[] = [];
    server.use(
      http.get(`${base}/audit`, ({ request }) => {
        const url = new URL(request.url);
        urls.push(url);
        return HttpResponse.json(
          url.searchParams.has("cursor")
            ? audit([{ ...auditItem, command_id: "second", actor_label: "analyst" }])
            : audit([auditItem], "original-cursor"),
        );
      }),
    );
    renderApp("/audit");
    const user = userEvent.setup();
    await screen.findByRole("table", { name: "操作记录" });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await waitFor(() => expect(urls.at(-1)?.searchParams.get("cursor")).toBe("original-cursor"));
    await user.click(screen.getByRole("button", { name: "上一页" }));
    await user.type(screen.getByLabelText("操作人"), "analyst");
    await user.selectOptions(screen.getByLabelText("操作"), "set_user_role");
    await user.click(screen.getByRole("button", { name: "筛选" }));
    await waitFor(() => expect(urls.at(-1)?.searchParams.get("actor_id")).toBe("analyst"));
    expect(urls.at(-1)?.searchParams.has("cursor")).toBe(false);
    expect(urls.at(-1)?.searchParams.get("command_kind")).toBe("set_user_role");
  });
  it("换账号不展示上一人的审计行", async () => {
    setup();
    server.use(http.get(`${base}/audit`, () => HttpResponse.json(audit())));
    const app = renderApp("/audit");
    await screen.findByRole("table", { name: "操作记录" });
    server.use(
      http.get(`${base}/me`, () => HttpResponse.json(envelope(me("viewer", "other")))),
      http.get(`${base}/audit`, () => HttpResponse.json(audit([], null))),
    );
    act(() => app.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" })));
    await waitFor(() =>
      expect(screen.queryByRole("table", { name: "操作记录" })).not.toBeInTheDocument(),
    );
    await screen.findByText("还没有操作记录。");
    expect(screen.queryByText(auditItem.command_id)).not.toBeInTheDocument();
  });
  it("旧未知操作人仍显示未知；原游标失效可回首页", async () => {
    setup();
    let invalid = false;
    server.use(
      http.get(`${base}/audit`, ({ request }) => {
        if (new URL(request.url).searchParams.has("cursor")) {
          invalid = true;
          return HttpResponse.json({ detail: "记录已更新，请从第一页查看。" }, { status: 409 });
        }
        return HttpResponse.json(
          audit([{ ...auditItem, actor_id: null, actor_label: "未知操作人" }], "old-cursor"),
        );
      }),
    );
    renderApp("/audit");
    const user = userEvent.setup();
    expect(await screen.findByRole("table", { name: "操作记录" })).toHaveTextContent("未知操作人");
    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("记录已更新");
    await user.click(screen.getByRole("button", { name: "返回第一页" }));
    await screen.findByRole("table", { name: "操作记录" });
    expect(invalid).toBe(true);
  });
  it("非管理员的操作人固定为自己，上海时间转成明确UTC并可清空", async () => {
    setup(me("viewer"));
    const urls: URL[] = [];
    server.use(
      http.get(`${base}/audit`, ({ request }) => {
        urls.push(new URL(request.url));
        return HttpResponse.json(audit());
      }),
    );
    renderApp("/audit");
    const user = userEvent.setup();
    await screen.findByRole("table", { name: "操作记录" });
    expect(screen.getByLabelText("操作人")).toBeDisabled();
    expect(urls.at(-1)?.searchParams.get("actor_id")).toBe("tester");
    await user.type(screen.getByLabelText(/开始时间/), "2026-10-06T09:00");
    await user.type(screen.getByLabelText(/结束时间/), "2026-10-06T10:00");
    await user.click(screen.getByRole("button", { name: "筛选" }));
    await waitFor(() =>
      expect(urls.at(-1)?.searchParams.get("time_from")).toBe("2026-10-06T01:00:00.000Z"),
    );
    expect(urls.at(-1)?.searchParams.get("time_until")).toBe("2026-10-06T02:00:00.000Z");
    expect(screen.getByLabelText("开始时间（上海）")).toBeVisible();
    expect(screen.getByLabelText("结束时间（上海）")).toBeVisible();
    await user.clear(screen.getByLabelText(/开始时间/));
    await user.clear(screen.getByLabelText(/结束时间/));
    await user.click(screen.getByRole("button", { name: "筛选" }));
    await waitFor(() => expect(urls.at(-1)?.searchParams.has("time_from")).toBe(false));
    expect(urls.at(-1)?.searchParams.has("time_until")).toBe(false);
    expect(urls.at(-1)?.searchParams.get("actor_id")).toBe("tester");
  });
  it.each([
    ["2026-10-06T09:00", "2026-10-06T09:00"],
    ["2026-10-06T10:00", "2026-10-06T09:00"],
  ])("上海时间非法范围 %s 到 %s 不发新筛选", async (start, end) => {
    setup();
    const urls: URL[] = [];
    server.use(
      http.get(`${base}/audit`, ({ request }) => {
        urls.push(new URL(request.url));
        return HttpResponse.json(audit());
      }),
    );
    renderApp("/audit");
    const user = userEvent.setup();
    await screen.findByRole("table", { name: "操作记录" });
    const calls = urls.length;
    await user.type(screen.getByLabelText(/开始时间/), start);
    await user.type(screen.getByLabelText(/结束时间/), end);
    await user.click(screen.getByRole("button", { name: "筛选" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("开始时间须早于结束时间。");
    expect(urls).toHaveLength(calls);
  });
});

describe("原完整封存结果的只读下载入口", () => {
  function factor(canReport: boolean) {
    const metadata = metaEnvelope();
    const result = diagnosticResult();
    server.use(
      http.get("*/api/v1/factors/results", () =>
        HttpResponse.json({
          serving: metadata.serving,
          data: { availability: "populated", results: [result] },
        }),
      ),
      http.get("*/api/v1/factors/results/:job", () =>
        HttpResponse.json({
          serving: metadata.serving,
          data: {
            availability: "ready",
            available_at: result.updated_at,
            message: null,
            result,
            research: diagnosticResearch,
            can_report: canReport,
          },
        }),
      ),
    );
    const queryClient = testQueryClient();
    queryClient.setQueryData(META_QUERY_KEY, metadata);
    const view = render(
      <AppProviders queryClient={queryClient}>
        <FactorResults
          factor={diagnosticFactor}
          generationId={metadata.serving.generation_id ?? ""}
          onRefresh={() => undefined}
        />
      </AppProviders>,
    );
    return { ...view, queryClient };
  }
  it("因子下载绑定原32位job和当前完整代际，换账号立即清链接", async () => {
    setup();
    const view = factor(true);
    const link = await screen.findByRole("link", { name: "导出只读页面" });
    expect(link).toHaveAttribute(
      "href",
      expect.stringContaining(
        `/api/v1/factors/results/${"b".repeat(32)}/report?generation_id=${metaEnvelope().serving.generation_id}`,
      ),
    );
    expect(link).toHaveAttribute("download");
    act(() => view.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" })));
    await waitFor(() =>
      expect(screen.queryByRole("link", { name: "导出只读页面" })).not.toBeInTheDocument(),
    );
  });
  it("缺完整结果或归属能力不显示因子下载", async () => {
    setup();
    factor(false);
    await screen.findByRole("region", { name: "IC 统计" });
    expect(screen.queryByRole("link", { name: "导出只读页面" })).not.toBeInTheDocument();
  });
  function strategy(owner = "tester", head = templateHead) {
    const complete = "f".repeat(64);
    const detail: Schemas["StrategyTemplateDetailData"] = {
      ...templateDetail,
      latest_run: {
        owner_id: owner,
        strategy_id: templateId,
        head,
        job_id: "55555555-5555-4555-8555-555555555555",
        spec_hash: "1".repeat(64),
        manifest_hash: "2".repeat(64),
        result_hash: "3".repeat(64),
        complete_result_hash: complete,
        input_hash: "4".repeat(64),
        completed_at: "2026-10-06T01:00:00Z",
      },
    };
    const prefix = "*/api/v1/strategy-templates";
    server.use(
      http.get("*/api/v1/strategies", () =>
        HttpResponse.json(templateEnvelope({ available: true, strategies: [] })),
      ),
      http.get(prefix, () => HttpResponse.json(templateEnvelope(templateCatalog(detail)))),
      http.get(`${prefix}/sources`, () => HttpResponse.json(templateEnvelope(templateSources))),
      http.get(`${prefix}/:id/versions`, () =>
        HttpResponse.json(
          templateEnvelope({
            strategy_id: templateId,
            current_head: templateHead,
            versions: [],
            next_before_version: null,
          }),
        ),
      ),
      http.get(`${prefix}/:id`, () => HttpResponse.json(templateEnvelope(detail))),
    );
    return complete;
  }
  it("普通策略下载使用原完整SHA，不混用结果payload SHA", async () => {
    setup();
    const fullHash = strategy();
    renderApp("/strategies");
    const user = userEvent.setup();
    await user.click(await screen.findByText("低位观察"));
    const link = await screen.findByRole("link", { name: "导出只读页面" });
    expect(link).toHaveAttribute(
      "href",
      expect.stringContaining(
        `/experiments/template-results/55555555-5555-4555-8555-555555555555/report.html?result_hash=${fullHash}`,
      ),
    );
    expect(link).not.toHaveAttribute("href", expect.stringContaining("3".repeat(64)));
  });
  it.each(["other", "changed-head"])("普通策略不导出 %s 的结果", async (bad) => {
    setup();
    strategy(
      bad === "other" ? "other" : "tester",
      bad === "changed-head" ? { ...templateHead, version: 2 } : templateHead,
    );
    renderApp("/strategies");
    const user = userEvent.setup();
    await user.click(await screen.findByText("低位观察"));
    await screen.findByRole("heading", { name: "最近回测" });
    expect(screen.queryByRole("link", { name: "导出只读页面" })).not.toBeInTheDocument();
  });
});
