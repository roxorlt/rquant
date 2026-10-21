import createClient, { type Client } from "openapi-fetch";
import type { components, paths } from "./schema";

/**
 * Typed client for the web API. Types come only from the generated schema
 * (web/src/api/schema.d.ts ← openapi.json ← `rquant web-openapi`); hand-written
 * response types are not allowed.
 *
 * The app is served at /app/ and nginx maps /app/api/ to the API's /api/, so
 * requests go relative to the page: <page directory>/api/v1/...
 */
export type ApiClient = Client<paths>;
export type Schemas = components["schemas"];
export type ServingMeta = Schemas["ServingMeta"];
export type MetaData = Schemas["MetaData"];
export type MetaEnvelope = Schemas["Envelope_MetaData_"];

export function apiBaseUrl(base: string = document.baseURI): string {
  return new URL(".", base).href.replace(/\/$/, "");
}

function previewFailRequested(): boolean {
  if (typeof window === "undefined") return false;
  return /(?:\?|&)fail=1(?:&|#|$)/.test(`${window.location.search}${window.location.hash}`);
}

export function createApiClient(baseUrl: string = apiBaseUrl()): ApiClient {
  return createClient<paths>({
    baseUrl,
    headers: { Accept: "application/json" },
    fetch: (request: Request) => {
      if (!previewFailRequested()) return fetch(request);
      const headers = new Headers(request.headers);
      headers.set("X-Rquant-Preview-Fail", "1");
      return fetch(new Request(request, { headers }));
    },
  });
}

let sharedClient: ApiClient | null = null;
let sharedBase = "";

export function apiClient(): ApiClient {
  const base = apiBaseUrl();
  if (sharedClient === null || sharedBase !== base) {
    sharedClient = createApiClient(base);
    sharedBase = base;
  }
  return sharedClient;
}

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}
