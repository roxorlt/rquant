import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
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

export const priceRulesUnavailableHandler = () =>
  http.get("*/api/v1/monitor/rules", () =>
    HttpResponse.json({
      data: {
        availability: "not_ready",
        available_at: null,
        evaluation_running: false,
        items: [],
        message: "价格规则尚未就绪。",
      },
      serving: metaEnvelope().serving,
    }),
  );

/** MSW server for component tests; every test starts with ready responses. */
export const server = setupServer(
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
  manualWatchlistUnavailableHandler(),
  manualWatchlistExactUnavailableHandler(),
  priceRulesUnavailableHandler(),
);
