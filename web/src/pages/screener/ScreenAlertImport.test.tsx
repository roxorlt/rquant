import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type {
  ScreenAlertDraft,
  ScreenAlertDraftRequest,
  ScreenExecutionView,
  ScreenQueryDefinition,
} from "@/api/screen";
import * as api from "@/api/screen";
import { AppProviders } from "@/app/App";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { ScreenAlertImport } from "./ScreenAlertImport";

const scope = "1".repeat(64);
const definition: ScreenQueryDefinition = {
  schema_version: 1,
  description: "完整选股",
  mode: "daily",
  trade_date: "2026-09-30",
  cutoff: null,
  source_kind: "replica",
  source_identity: "a".repeat(64),
  conditions: [{ name: "above_ma", args: { period: 37, offset: 0 } }],
  ranking: { top_n: 13, conditions: [{ metric: "CIRC_MV[0]", ascending: true, weight: 100 }] },
};
const execution: ScreenExecutionView = {
  execution_id: "screen-1",
  sequence: 1,
  command_hash: "b".repeat(64),
  plan_hash: "c".repeat(64),
  original_command: {
    kind: "execute_screen_query",
    command_id: "screen-1",
    requested_at: "2026-10-01T01:00:00Z",
    page_size: 20,
    definition,
  },
  definition,
  source: {
    mode: "daily",
    identity: definition.source_identity,
    updated_at: "2026-10-01T01:00:00Z",
  },
  status: "succeeded",
  completed_at: "2026-10-01T01:00:01Z",
  base_count: 21,
  total: 13,
  unknown_count: 0,
  ranked_count: 13,
  steps: [],
  artifact_sha256: "d".repeat(64),
  member_rank_sha256: "e".repeat(64),
  failure_code: null,
};
function draft(request: ScreenAlertDraftRequest): ScreenAlertDraft {
  const now = Date.now();
  const draftId = "f".repeat(24);
  return {
    schema_version: 1,
    draft_id: draftId,
    created_at: new Date(now).toISOString(),
    expires_at: new Date(now + 24 * 60 * 60 * 1000).toISOString(),
    suggested_name: "完整选股",
    definition,
    conditions: definition.conditions,
    ranking: definition.ranking ?? null,
    preferred_scope: { kind: "market", universe_policy: "trusted_current" },
    source_policy: {
      daily_anchor: "previous_closed_session",
      condition_semantics_version: "screen-registry/v1",
      intraday_contract_id: "intraday-pit",
      minimum_intraday_contract_version: 3,
    },
    origin: {
      draft_id: draftId,
      execution_id: request.execution_id,
      command_hash: request.command_hash,
      definition_hash: "c".repeat(64),
      result_digest: execution.artifact_sha256 ?? "",
      member_rank_digest: execution.member_rank_sha256 ?? "",
      source_identity: definition.source_identity,
      mode: "daily",
      trade_date: definition.trade_date,
      cutoff: null,
    },
    capabilities: {
      condition_count: 1,
      ranking_imported: true,
      consumer_state: "awaiting_consumer",
      message: "提醒草稿已生成，尚未生效。",
    },
    content_hash: "a".repeat(64),
  };
}
function view(
  onOpen = vi.fn(),
  current: ScreenExecutionView | null = execution,
  ownerScope = scope,
) {
  return render(
    <AppProviders queryClient={testQueryClient()}>
      <ScreenAlertImport ownerScope={ownerScope} execution={current} onOpen={onOpen} />
    </AppProviders>,
  );
}

it("回包失联后重开只恢复原草稿请求；真实草稿确认后才打开提醒", async () => {
  const requests: ScreenAlertDraftRequest[] = [];
  server.use(
    http.post("*/api/v1/screen/query/alert-draft", async ({ request }) => {
      const body = (await request.json()) as ScreenAlertDraftRequest;
      requests.push(body);
      expect(window.sessionStorage.getItem(`rquant.screen-alert-draft.v1:${scope}`)).toContain(
        body.command_id,
      );
      return requests.length === 1
        ? HttpResponse.error()
        : HttpResponse.json({
            available: true,
            owner_scope_tag: scope,
            alert_draft: draft(body),
            presets: [],
          });
    }),
  );
  const open = vi.fn();
  const first = view(open);
  await userEvent.click(screen.getByRole("button", { name: "设置提醒" }));
  expect(await screen.findByText("草稿待确认，请恢复原请求。")).toBeVisible();
  expect(open).not.toHaveBeenCalled();
  first.unmount();
  view(open, null);
  await userEvent.click(screen.getByRole("button", { name: "恢复原请求" }));
  await waitFor(() => expect(open).toHaveBeenCalledWith("f".repeat(24)));
  expect(requests).toHaveLength(2);
  expect(requests[0]).toEqual(requests[1]);
  expect(Object.keys(requests[0] ?? {}).sort()).toEqual([
    "command_hash",
    "command_id",
    "execution_id",
  ]);
  expect(screen.getByText("提醒草稿已生成，尚未生效。")).toBeVisible();
});

it("拒绝其他归属和缺失草稿；未知结果不显示生效", async () => {
  server.use(
    http.post("*/api/v1/screen/query/alert-draft", async ({ request }) =>
      HttpResponse.json({
        available: true,
        owner_scope_tag: "2".repeat(64),
        alert_draft: draft((await request.json()) as ScreenAlertDraftRequest),
        presets: [],
      }),
    ),
  );
  const open = vi.fn();
  view(open);
  await userEvent.click(screen.getByRole("button", { name: "设置提醒" }));
  expect(await screen.findByText("草稿待确认，请恢复原请求。")).toBeVisible();
  expect(open).not.toHaveBeenCalled();
  expect(screen.queryByText("提醒草稿已生成，尚未生效。")).toBeNull();
});

it("离开当前归属后忽略迟到回包；新账号不读取旧请求", async () => {
  let release!: (data: api.ScreenQueryReadData) => void;
  const response = new Promise<api.ScreenQueryReadData>((resolve) => {
    release = resolve;
  });
  const create = vi.spyOn(api, "createScreenAlertDraft").mockImplementation(() => response);
  const open = vi.fn();
  const first = view(open);
  await userEvent.click(screen.getByRole("button", { name: "设置提醒" }));
  const request = create.mock.calls[0]?.[0];
  expect(request).toBeDefined();
  first.unmount();
  view(open, null, "2".repeat(64));
  await act(async () => {
    if (request)
      release({
        available: true,
        owner_scope_tag: scope,
        daily_run_evidence: [],
        alert_draft: draft(request),
        presets: [],
      });
    await response;
  });
  expect(open).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: "恢复原请求" })).toBeNull();
  create.mockRestore();
});

it("缺少真实执行回证时说明原因，键盘不能发起草稿", async () => {
  const open = vi.fn();
  view(open, { ...execution, total: null, artifact_sha256: null });
  const button = screen.getByRole("button", { name: "设置提醒" });
  expect(button).toBeDisabled();
  expect(button).toHaveAttribute("aria-description", "先确认完整筛选结果。");
  await userEvent.keyboard("{Enter}");
  expect(open).not.toHaveBeenCalled();
});
