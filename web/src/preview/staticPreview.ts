/**
 * GitHub Pages has no API. This module is pulled in only by the static-preview
 * Vite build (virtual:static-preview). Production resolves that module to an
 * empty stub, so this file is not in the production bundle.
 *
 * GET and HEAD go to the frozen files the static export wrote next to the page.
 * Each preview directory (empty, error, viewer, stale, normal) keeps its own
 * files; this module does not substitute a bundled sample. Successful writes
 * are remembered in memory and applied on top of those files, so a later
 * refresh of the same page still shows the write. `?fail=1` makes writes 503
 * and does not remember them.
 */
export const STATIC_PREVIEW_WRITE_MOCK = "STATIC_PREVIEW_WRITE_MOCK";

interface AlertMark {
  acked: boolean;
  at: string;
}

interface WriteOverlay {
  alerts: Map<string, AlertMark>;
  schemes: Map<string, Record<string, unknown> | null>;
  pools: Map<string, Record<string, unknown>>;
  rules: Map<string, Record<string, unknown>>;
}

function emptyOverlay(): WriteOverlay {
  return {
    alerts: new Map(),
    schemes: new Map(),
    pools: new Map(),
    rules: new Map(),
  };
}

let overlay: WriteOverlay = emptyOverlay();

export function resetStaticPreviewWrites(): void {
  overlay = emptyOverlay();
}

function failRequested(): boolean {
  if (typeof location === "undefined") return false;
  return /(?:\?|&)fail=1(?:&|#|$)/.test(`${location.search}${location.hash}`);
}

function requestUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") return input;
  if (input instanceof URL) return input.href;
  return input.url;
}

function parsedUrl(url: string): URL {
  if (url.startsWith("http://") || url.startsWith("https://")) return new URL(url);
  return new URL(url, "http://preview.local");
}

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function readJsonBody(input: RequestInfo | URL, init?: RequestInit): Promise<unknown> {
  try {
    if (typeof init?.body === "string") return JSON.parse(init.body);
    if (input instanceof Request) {
      const text = await input.clone().text();
      return text ? JSON.parse(text) : null;
    }
  } catch {
    return null;
  }
  return null;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function rememberWrite(url: string, body: unknown): void {
  const parsed = parsedUrl(url);
  const path = parsed.pathname.replace(/\/$/, "");
  const payload = record(body);
  if (path.endsWith("/api/v1/alerts/ack") || path.endsWith("/api/v1/alerts/unack")) {
    const id = payload?.alert_id;
    if (typeof id === "string") {
      overlay.alerts.set(id, {
        acked: path.endsWith("/ack"),
        at: new Date().toISOString(),
      });
    }
    return;
  }
  if (path.endsWith("/api/v1/screener-schemes")) {
    const name = payload?.name ?? parsed.searchParams.get("name");
    if (typeof name !== "string" || !name) return;
    overlay.schemes.set(name, payload ? { ...payload, saved_at: new Date().toISOString() } : null);
    return;
  }
  if (path.endsWith("/api/v1/pools") && payload && typeof payload.name === "string") {
    overlay.pools.set(payload.name, {
      name: payload.name,
      description: typeof payload.description === "string" ? payload.description : "",
      pool_refs: Array.isArray(payload.pool_refs) ? payload.pool_refs : [],
      updated_at: new Date().toISOString(),
      members: [],
    });
    return;
  }
  if (path.endsWith("/api/v1/alert-rules") && payload && typeof payload.rule_id === "string") {
    overlay.rules.set(payload.rule_id, { ...payload, saved_at: new Date().toISOString() });
  }
}

function overlayEmpty(): boolean {
  return (
    overlay.alerts.size === 0 &&
    overlay.schemes.size === 0 &&
    overlay.pools.size === 0 &&
    overlay.rules.size === 0
  );
}

function applyOverlay(url: string, payload: unknown): unknown {
  const root = record(payload);
  const data = record(root?.data);
  if (!root || !data) return payload;
  const path = parsedUrl(url).pathname.replace(/\/$/, "");
  if (path.endsWith("/api/v1/alerts") && Array.isArray(data.items)) {
    data.items = data.items.map((item) => {
      const row = record(item);
      const id = row?.alert_id;
      if (!row || typeof id !== "string") return item;
      const mark = overlay.alerts.get(id);
      if (!mark) return item;
      return mark.acked
        ? { ...row, acked_at: mark.at, acked_by: "我" }
        : { ...row, acked_at: null, acked_by: null };
    });
  }
  if (path.endsWith("/api/v1/screener-schemes") && Array.isArray(data.schemes)) {
    const schemes = data.schemes.filter((item) => {
      const row = record(item);
      return !(row && typeof row.name === "string" && overlay.schemes.has(row.name));
    });
    for (const row of overlay.schemes.values()) {
      if (row) schemes.push(row);
    }
    data.schemes = schemes;
  }
  if (path.endsWith("/api/v1/pools") && Array.isArray(data.pools)) {
    const pools = data.pools.filter((item) => {
      const row = record(item);
      return !(row && typeof row.name === "string" && overlay.pools.has(row.name));
    });
    for (const row of overlay.pools.values()) pools.push(row);
    data.pools = pools;
  }
  if (path.endsWith("/api/v1/alert-rules") && Array.isArray(data.rules)) {
    const rules = data.rules.filter((item) => {
      const row = record(item);
      return !(row && typeof row.rule_id === "string" && overlay.rules.has(row.rule_id));
    });
    for (const row of overlay.rules.values()) rules.push(row);
    data.rules = rules;
  }
  return root;
}

export function installStaticPreview(): void {
  const current = globalThis.fetch;
  if (current.name === "staticPreviewFetch") return;
  const native = current.bind(globalThis);
  globalThis.fetch = async function staticPreviewFetch(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const method = (
      init?.method ?? (input instanceof Request ? input.method : "GET")
    ).toUpperCase();
    const url = requestUrl(input);
    if (method !== "GET" && method !== "HEAD") {
      if (failRequested()) {
        return jsonResponse({ detail: "暂时不能写", marker: STATIC_PREVIEW_WRITE_MOCK }, 503);
      }
      rememberWrite(url, await readJsonBody(input, init));
      return jsonResponse(
        { command_id: "preview", status: "succeeded", marker: STATIC_PREVIEW_WRITE_MOCK },
        200,
      );
    }
    const response = await native(input, init);
    if (method === "HEAD" || overlayEmpty() || !response.ok) return response;
    try {
      const payload: unknown = JSON.parse(await response.clone().text());
      return jsonResponse(applyOverlay(url, payload), response.status);
    } catch {
      return response;
    }
  };
}
