import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import { healthEnvelope, metaEnvelope, overviewEnvelope } from "./fixtures";

export const metaHandler = (envelope = metaEnvelope()) =>
  http.get("*/api/v1/meta", () => HttpResponse.json(envelope));

export const overviewHandler = (envelope = overviewEnvelope()) =>
  http.get("*/api/v1/overview", () => HttpResponse.json(envelope));

export const healthHandler = (envelope = healthEnvelope()) =>
  http.get("*/api/v1/health", () => HttpResponse.json(envelope));

/** MSW server for component tests; every test starts with ready responses. */
export const server = setupServer(metaHandler(), overviewHandler(), healthHandler());
