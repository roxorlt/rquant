import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import { metaEnvelope } from "./fixtures";

export const metaHandler = (envelope = metaEnvelope()) =>
  http.get("*/api/v1/meta", () => HttpResponse.json(envelope));

/** MSW server for component tests; every test starts with a ready /meta. */
export const server = setupServer(metaHandler());
