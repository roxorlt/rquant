import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import {
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

export const tasksHandler = (envelope = tasksEnvelope()) =>
  http.get("*/api/v1/tasks/jobs", () => HttpResponse.json(envelope));

export const paperHandler = (envelope = paperEnvelope()) =>
  http.get("*/api/v1/paper/accounts", () => HttpResponse.json(envelope));

/** MSW server for component tests; every test starts with ready responses. */
export const server = setupServer(
  metaHandler(),
  overviewHandler(),
  healthHandler(),
  monitorHandler(),
  tasksHandler(),
  paperHandler(),
);
