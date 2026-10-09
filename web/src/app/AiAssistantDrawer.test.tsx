import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { AppProviders } from "@/app/App";
import { portfolioCapabilities } from "@/pages/backtest/portfolio.fixture";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { AiAssistantDrawer, AiScreenBacktest } from "./AiAssistantDrawer";

it("explains default closed state and preserves manual entry", async () => {
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        data: {
          available: false,
          can_generate: false,
          message: "助手尚未配置，可继续手动编辑。",
          can_prepare_backtest: false,
        },
        serving: metaEnvelope().serving,
      }),
    ),
  );
  render(
    <AppProviders queryClient={testQueryClient()}>
      <AiAssistantDrawer open viewer="researcher" onClose={() => {}} />
    </AppProviders>,
  );
  expect(await screen.findByText("助手尚未配置，可继续手动编辑。")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "打开条件筛选" })).toHaveAttribute("href", "#/screener");
});

const command: Schemas["ExecuteScreenQuery"] = {
  kind: "execute_screen_query",
  command_id: "0e3b6675-7282-4b6c-adc9-e27a52ae9256",
  requested_at: "2026-10-06T06:00:00Z",
  page_size: 20,
  definition: {
    schema_version: 1,
    mode: "daily",
    description: "原描述",
    trade_date: "2026-09-24",
    source_kind: "replica",
    source_identity: "a".repeat(64),
    conditions: [{ name: "not_st", args: {} }],
    ranking: null,
  },
};
const execution: Schemas["ScreenExecutionView"] = {
  execution_id: command.command_id,
  command_hash: "b".repeat(64),
  plan_hash: "c".repeat(64),
  sequence: 1,
  status: "succeeded",
  definition: command.definition,
  original_command: command,
  artifact_sha256: "d".repeat(64),
  base_count: 2,
  total: 1,
  unknown_count: 0,
  ranked_count: 1,
  steps: [],
};
function capabilities(available = true) {
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          available: true,
          can_generate: available,
          can_prepare_backtest: available,
          daily_limit: available ? 10 : 0,
          remaining_calls: available ? 10 : 0,
          message: available ? null : "调用尚未启用。",
        },
      }),
    ),
  );
}
function catalog(nl = false) {
  server.use(
    http.get("*/api/v1/screen/blocks", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          available: true,
          source_kind: "replica",
          source: { mode: "daily", identity: "e".repeat(64), updated_at: "2026-10-06T06:00:00Z" },
          dates: ["2026-10-05"],
          ranking_metrics: [{ value: "CIRC_MV[0]", label: "流通市值" }],
          blocks: [
            {
              key: "not_st",
              label: "排除 ST",
              category: "filter",
              category_label: "股票范围",
              parameters: [],
            },
          ],
          nl_generate_available: nl,
        },
      }),
    ),
  );
}
it("applies an editable suggestion and keeps undo available until a manual change", async () => {
  capabilities();
  catalog(true);
  server.use(
    http.post("*/api/v1/ai/requests", async ({ request }) => {
      const body = (await request.json()) as Schemas["AIScreenRequest"];
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          request_id: body.request_id,
          purpose: "screen",
          state: "completed",
          created_at: "2026-10-06T06:00:00Z",
          result: {
            purpose: "screen",
            definition: {
              schema_version: 1,
              mode: "daily",
              description: body.instruction,
              trade_date: body.trade_date,
              source_kind: body.source_kind,
              source_identity: body.source_identity,
              conditions: [{ name: "not_st", args: {} }],
              ranking: {
                conditions: [{ metric: "CIRC_MV[0]", ascending: true, weight: 1 }],
                top_n: 1,
              },
            },
          },
        },
      });
    }),
  );
  const user = userEvent.setup();
  render(
    <AppProviders queryClient={testQueryClient()}>
      <AiAssistantDrawer open viewer="alice" onClose={() => {}} />
    </AppProviders>,
  );
  await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "排除 ST，按市值排名");
  await user.click(screen.getByRole("button", { name: "生成建议" }));
  await user.click(await screen.findByRole("button", { name: "应用到条件" }));
  expect(screen.getByRole("spinbutton", { name: "第 1 项权重" })).toHaveValue(1);
  await user.click(await screen.findByRole("button", { name: "撤销应用" }));
  expect(screen.queryByRole("spinbutton", { name: "第 1 项权重" })).not.toBeInTheDocument();
  expect(screen.getByRole("textbox", { name: "选股描述" })).toHaveValue("排除 ST，按市值排名");
});
it("restores the exact original screen before current draft and changed source checks", async () => {
  capabilities(false);
  catalog();
  const original = { action: "execute", command };
  sessionStorage.setItem("rquant.ai.execute:alice", JSON.stringify(original));
  const requests: unknown[] = [];
  server.use(
    http.post("*/api/v1/screen/query/lookup", async ({ request }) => {
      requests.push(await request.json());
      return HttpResponse.json({
        available: true,
        owner_scope_tag: "f".repeat(64),
        execution,
        receipt: { status: "succeeded" },
      });
    }),
    http.get("*/api/v1/screen/query/executions/:id", () =>
      HttpResponse.json({ available: true, owner_scope_tag: "f".repeat(64), execution }),
    ),
    http.get("*/api/v1/screen/query/executions/:id/results", () =>
      HttpResponse.json({
        available: true,
        owner_scope_tag: "f".repeat(64),
        results: {
          execution_id: execution.execution_id,
          artifact_sha256: execution.artifact_sha256,
          rows: [
            {
              ts_code: "000001.SZ",
              name: "平安银行",
              close: 12.3456,
              pct_chg: 0.5,
              rank_position: 1,
              ranking_score: 88.25,
            },
          ],
          next_cursor: null,
        },
      }),
    ),
  );
  const user = userEvent.setup();
  render(
    <AppProviders queryClient={testQueryClient()}>
      <AiAssistantDrawer open viewer="alice" onClose={() => {}} />
    </AppProviders>,
  );
  await user.click(await screen.findByRole("button", { name: "继续查看筛选" }));
  await waitFor(() => expect(requests).toHaveLength(1));
  expect(requests[0]).toEqual({ action: "lookup", original });
  expect(await screen.findByText("筛选已完成，共 1 只股票。")).toBeInTheDocument();
  expect(screen.getByRole("table", { name: "助手筛选结果" })).toHaveTextContent("平安银行");
  expect(screen.getByRole("table", { name: "助手筛选结果" })).toHaveTextContent("12.35");
});
it("reads the original complete interval with new preparation disabled and confirms explicitly", async () => {
  capabilities(false);
  const body = {
    request_id: "01d21677-0ef2-49bc-9397-16f8274f1d27",
    execution_id: execution.execution_id,
    start_date: "2026-08-10",
    end_date: "2026-08-11",
  };
  sessionStorage.setItem(
    `rquant.ai.backtest:alice:${execution.execution_id}`,
    JSON.stringify(body),
  );
  const config = portfolioCapabilities.data.default_config;
  if (!config) throw new Error("original portfolio fixture missing");
  const prepares: unknown[] = [];
  const confirms: unknown[] = [];
  server.use(
    http.post("*/api/v1/ai/backtests/prepare/lookup", async ({ request }) => {
      prepares.push(await request.json());
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          ...body,
          complete: true,
          config,
          config_sha256: "1".repeat(64),
          proof_sha256: "2".repeat(64),
          material_sha256: "3".repeat(64),
          trading_days: 2,
          candidate_count: 1,
          created_at: "2026-10-06T06:00:00Z",
          message: "完整区间已准备。",
        },
      });
    }),
    http.post("*/api/v1/ai/backtests/confirm", async ({ request }) => {
      const body = (await request.json()) as Schemas["AIBacktestConfirmRequest"];
      confirms.push(body);
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          job_id: "a22b6359-00c6-4fbc-811e-720bb4de91b9",
          receipt: { status: "succeeded", command_id: body.command_id },
        },
      });
    }),
  );
  const user = userEvent.setup();
  render(
    <AppProviders queryClient={testQueryClient()}>
      <AiScreenBacktest viewer="alice" execution={execution} />
    </AppProviders>,
  );
  await user.click(screen.getByRole("button", { name: "继续查看原区间" }));
  expect(await screen.findByText("完整交易日")).toBeInTheDocument();
  expect(prepares).toEqual([body]);
  expect(confirms).toHaveLength(0);
  await user.click(screen.getByRole("button", { name: "核对并确认回测" }));
  expect(screen.getByRole("dialog", { name: "确认组合回测" })).toHaveTextContent("每日");
  expect(confirms).toHaveLength(0);
  await user.click(screen.getByRole("button", { name: "确认回测" }));
  expect(await screen.findByRole("link", { name: "查看运行与结果" })).toHaveAttribute(
    "href",
    "#/backtest?tab=portfolio&job=a22b6359-00c6-4fbc-811e-720bb4de91b9",
  );
  expect(confirms).toHaveLength(1);
});
