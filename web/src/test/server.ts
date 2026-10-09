import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import type { Schemas } from "@/api/client";
import { dailyCapability } from "../pages/factors/factorDailyFields.fixture";
import {
  channelsEnvelope,
  healthEnvelope,
  metaEnvelope,
  monitorEnvelope,
  overviewEnvelope,
  paperEnvelope,
  tasksEnvelope,
} from "./fixtures";

export const metaHandler = (envelope = metaEnvelope()) =>
  http.get("*/api/v1/meta", () => HttpResponse.json(envelope));

export const overviewHandler = (envelope = overviewEnvelope()) =>
  http.get("*/api/v1/overview", () => HttpResponse.json(envelope));

export const healthHandler = (envelope = healthEnvelope()) =>
  http.get("*/api/v1/health", () => HttpResponse.json(envelope));

export const monitorHandler = (envelope = monitorEnvelope()) =>
  http.get("*/api/v1/monitor/timeline", () => HttpResponse.json(envelope));

export const channelsHandler = (envelope = channelsEnvelope()) =>
  http.get("*/api/v1/monitor/channels", () => HttpResponse.json(envelope));

export const tasksHandler = (envelope = tasksEnvelope()) =>
  http.get("*/api/v1/tasks/jobs", () => HttpResponse.json(envelope));

export const logCapabilitiesHandler = () =>
  http.get("*/api/v1/tasks/services/log-capabilities", () => HttpResponse.json({ units: [] }));

export const paperHandler = (envelope = paperEnvelope()) =>
  http.get("*/api/v1/paper/accounts", () => HttpResponse.json(envelope));

export const poolEditorHandler = () =>
  http.get("*/api/v1/pools/editor", () =>
    HttpResponse.json({
      data: { state: "unavailable", pools: [], copy_sources: [], canvases: [] },
      serving: metaEnvelope().serving,
    }),
  );

export const formulaMarketJobsHandler = () =>
  http.get("*/api/v1/screen/tdx/market/jobs", () =>
    HttpResponse.json({
      data: {
        availability: "not_published",
        available_at: null,
        has_older_tasks: false,
        jobs: [],
        message: "选股任务尚未发布。",
        total_task_count: 0,
      },
      serving: metaEnvelope().serving,
    }),
  );

export const formulaPoolsHandler = () =>
  http.get("*/api/v1/pools/formula", () =>
    HttpResponse.json({
      data: { availability: "empty", available_at: null, message: "还没有公式池。", pools: [] },
      serving: metaEnvelope().serving,
    }),
  );

export const factorResultsUnavailableHandler = () =>
  http.get("*/api/v1/factors/results", () =>
    HttpResponse.json({
      data: { availability: "unavailable", available_at: null, results: [] },
      serving: metaEnvelope().serving,
    }),
  );

export const factorRunUnavailableHandler = () =>
  http.get("*/api/v1/factors/run-availability", () =>
    HttpResponse.json({
      data: {
        enabled: false,
        reason: "尚未准备历史行情",
        start_date: null,
        end_date: null,
        pools: [],
      },
      serving: metaEnvelope().serving,
    }),
  );

export const factorTrackingUnavailableHandler = () =>
  http.get("*/api/v1/factors/:factorId/tracking", ({ params }) => {
    const data: Schemas["FactorTrackingPanel"] = {
      factor_id: String(params.factorId),
      availability: "unavailable",
      status: "unavailable",
      tracked: false,
      tracking_generation: null,
      definition_head: null,
      actual_start_date: null,
      updated_at: null,
      summary: null,
      reason: "跟踪数据尚未发布。",
      can_set_tracked: false,
      policy_version: 1,
      policy_label: "全市场（剔除北交所、ST） · 每日 · 5组 · RankIC · 无运行后中性化",
      basis_label: "历史回顾研究诊断；累计从实际起日计算，并非实盘收益。",
    };
    return HttpResponse.json({ data, serving: metaEnvelope().serving });
  });

export const factorCapabilitiesHandler = () =>
  http.get("*/api/v1/factors/capabilities", () =>
    HttpResponse.json({
      data: {
        ...dailyCapability,
        can_save: false,
        version: "daily_v1",
        fields: dailyCapability.fields.slice(0, 6),
      },
      serving: metaEnvelope().serving,
    }),
  );

export const manualWatchlistUnavailableHandler = () =>
  http.get("*/api/v1/watchlist", () =>
    HttpResponse.json({
      data: {
        availability: "unavailable",
        available_at: null,
        message: "名单暂不可用，请稍后重试。",
        items: [],
      },
      serving: metaEnvelope().serving,
    }),
  );

export const manualWatchlistExactUnavailableHandler = () =>
  http.get("*/api/v1/watchlist/:code", ({ params }) =>
    HttpResponse.json({
      data: {
        availability: "unavailable",
        available_at: null,
        message: "名单暂不可用，请稍后重试。",
        price_levels: [],
        source: null,
        status: null,
        ts_code: params.code,
        version: null,
        expires_at: null,
        updated_at: null,
      },
      serving: metaEnvelope().serving,
    }),
  );

/** MSW server for component tests; every test starts with ready responses. */
export const server = setupServer(
  http.get("*/api/v1/strategy-promotions/:strategy_id", ({ params, request }) => {
    const envelope: Schemas["Envelope_StrategyPromotionData_"] = {
      serving: metaEnvelope().serving,
      data: {
        availability: "unavailable",
        source_kind:
          new URL(request.url).searchParams.get("source_kind") === "builtin"
            ? "builtin"
            : "template",
        strategy_id: String(params.strategy_id),
        available_at: null,
        states: [],
        reviews: [],
        next_offset: null,
        candidates: [],
        walk_forward: [],
        paper_accounts: [],
        can_evaluate: false,
        can_prepare_approval: false,
        can_run_walk_forward: false,
        reason: "人工晋级未启用。",
      },
    };
    return HttpResponse.json(envelope);
  }),
  http.get("*/api/v1/collaboration/me", () => {
    const envelope: Schemas["Envelope_CollaborationMe_"] = {
      serving: { ...metaEnvelope().serving, generation_id: null },
      data: {
        available: false,
        mode: "legacy",
        username: metaEnvelope().data.viewer,
        role: null,
        revision: null,
        state_sha256: null,
        can_manage_users: false,
        can_research: false,
        can_read_audit: false,
        message: "协作权限尚未启用。",
      },
    };
    return HttpResponse.json(envelope);
  }),
  http.get("*/api/v1/monitor/condition-rules", () => {
    const envelope: Schemas["Envelope_ConditionAlertRuleListData_"] = {
      serving: metaEnvelope().serving,
      data: {
        availability: "not_activated",
        available_at: null,
        message: "规则尚未开放。",
        can_write: false,
        can_enable: false,
        write_message: "规则操作暂未开放。",
        enable_message: "提醒暂不可运行，可先保存为停用。",
        items: [],
        scopes: [],
        blocks: [],
        ranking_metrics: [],
        triggers: [],
      },
    };
    return HttpResponse.json(envelope);
  }),
  metaHandler(),
  overviewHandler(),
  healthHandler(),
  monitorHandler(),
  channelsHandler(),
  tasksHandler(),
  logCapabilitiesHandler(),
  paperHandler(),
  poolEditorHandler(),
  formulaMarketJobsHandler(),
  formulaPoolsHandler(),
  factorResultsUnavailableHandler(),
  factorRunUnavailableHandler(),
  factorTrackingUnavailableHandler(),
  factorCapabilitiesHandler(),
  manualWatchlistUnavailableHandler(),
  manualWatchlistExactUnavailableHandler(),
);
