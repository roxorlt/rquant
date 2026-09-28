import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { ApiError, apiClient, type Schemas } from "./client";
import { fetchMeta, META_QUERY_KEY } from "./useMeta";

export type ManualWatchlistCommandBody = Schemas["ManualWatchlistCommandRequest"];
type Receipt = Schemas["ManualWatchlistCommandReceipt"];
export const MANUAL_WATCHLIST_JOURNAL_KEY = "rquant.manual-watchlist-command.v1";
const JOURNAL_EVENT = "rquant:manual-watchlist-command";
const HASH = /^[0-9a-f]{64}$/;
const CODE = /^[0-9]{6}\.(?:SH|SZ|BJ)$/;
const COMMAND_ID = /^[A-Za-z0-9._-]{1,128}$/;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;
const TERMINAL = new Set(["published", "conflict", "capacity", "failed"]);
const STATUSES = new Set([
  "pending",
  "processing",
  "saved_syncing",
  "published",
  "conflict",
  "capacity",
  "failed",
  "uncertain",
  "unknown",
]);

export interface ManualWatchlistCommandEntry {
  body: ManualWatchlistCommandBody;
  status: Receipt["status"] | "unknown";
  version: number | null;
}
export interface ManualWatchlistCommandSnapshot {
  record: ManualWatchlistCommandEntry | null;
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
}
export type WatchlistBasis = {
  action: "add" | "remove";
  generationId: string;
  expectedVersion: number | null;
  observedStatus: "active" | "expired" | "deleted" | "absent";
  source?: "detail" | "screen_result";
};

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validReceipt(value: unknown): value is Receipt {
  return (
    object(value) &&
    typeof value.command_id === "string" &&
    typeof value.ts_code === "string" &&
    (value.action === "add" || value.action === "remove") &&
    typeof value.status === "string" &&
    STATUSES.has(value.status) &&
    value.status !== "unknown" &&
    typeof value.message === "string" &&
    (value.version == null || (Number.isInteger(value.version) && Number(value.version) >= 1))
  );
}

/** A lost or invalid reply remains uncertain; a typed HTTP error can be a terminal receipt. */
export async function submitManualWatchlistCommand(
  body: ManualWatchlistCommandBody,
): Promise<Receipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/watchlist/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "状态待核对，请继续核对本次请求。");
    });
  const receipt: unknown = data ?? error;
  if (!validReceipt(receipt))
    throw new ApiError(response.status, "状态待核对，请继续核对本次请求。");
  return receipt;
}

function validBody(value: unknown, tsCode: string): value is ManualWatchlistCommandBody {
  if (!object(value)) return false;
  if (
    value.ts_code !== tsCode ||
    typeof value.command_id !== "string" ||
    !COMMAND_ID.test(value.command_id) ||
    typeof value.generation_id !== "string" ||
    !HASH.test(value.generation_id) ||
    typeof value.requested_at !== "string" ||
    !UTC.test(value.requested_at) ||
    !Number.isFinite(Date.parse(value.requested_at)) ||
    !(
      value.expected_version === null ||
      (Number.isInteger(value.expected_version) && Number(value.expected_version) >= 1)
    )
  )
    return false;
  const keys = Object.keys(value).sort().join(",");
  if (value.action === "add")
    return (
      keys ===
        "action,command_id,expected_version,generation_id,price_levels,requested_at,source,ts_code" &&
      (value.source === "detail" || value.source === "screen_result") &&
      Array.isArray(value.price_levels) &&
      value.price_levels.length === 0
    );
  return (
    value.action === "remove" &&
    keys === "action,command_id,expected_version,generation_id,requested_at,ts_code" &&
    typeof value.expected_version === "number"
  );
}

function validEntry(
  value: unknown,
  tsCode: string,
): value is ManualWatchlistCommandEntry & { schema: 1 } {
  if (
    !object(value) ||
    value.schema !== 1 ||
    !validBody(value.body, tsCode) ||
    typeof value.status !== "string" ||
    !STATUSES.has(value.status)
  )
    return false;
  return value.status === "saved_syncing" || value.status === "published"
    ? Number.isInteger(value.version) && Number(value.version) >= 1
    : value.version === null;
}

export function publishedMatches(
  entry: ManualWatchlistCommandEntry,
  exact: { generationId: string | null; status: string; version: number | null },
): boolean {
  return (
    entry.status === "published" &&
    exact.generationId !== null &&
    exact.generationId !== entry.body.generation_id &&
    exact.version === entry.version &&
    exact.status === (entry.body.action === "add" ? "active" : "deleted")
  );
}

export function publishedSuperseded(
  entry: ManualWatchlistCommandEntry,
  exact: { generationId: string | null; version: number | null },
): boolean {
  return (
    entry.status === "published" &&
    exact.generationId !== null &&
    exact.generationId !== entry.body.generation_id &&
    exact.version !== null &&
    entry.version !== null &&
    exact.version > entry.version
  );
}

function unresolvedCodes(viewer: string | null): string[] {
  if (viewer === null) return [];
  try {
    const storage = window.localStorage;
    const prefix = `${MANUAL_WATCHLIST_JOURNAL_KEY}:${encodeURIComponent(viewer)}:`;
    const codes: string[] = [];
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (!key?.startsWith(prefix)) continue;
      const code = key.slice(prefix.length);
      if (!CODE.test(code)) continue;
      const raw = storage.getItem(key);
      if (raw === null) continue;
      try {
        const entry: unknown = JSON.parse(raw);
        if (validEntry(entry, code) && !TERMINAL.has(entry.status)) codes.push(code);
      } catch {
        // A damaged entry must not hide a different command that can still be resumed.
      }
    }
    return codes.sort();
  } catch {
    return [];
  }
}

export function useUnresolvedManualWatchlistCodes(viewer: string | null): string[] {
  const [state, setState] = useState<{ viewer: string | null; codes: string[] }>({
    viewer: null,
    codes: [],
  });
  useEffect(() => {
    const refresh = () => setState({ viewer, codes: unresolvedCodes(viewer) });
    refresh();
    window.addEventListener("storage", refresh);
    window.addEventListener(JOURNAL_EVENT, refresh);
    return () => {
      window.removeEventListener("storage", refresh);
      window.removeEventListener(JOURNAL_EVENT, refresh);
    };
  }, [viewer]);
  return state.viewer === viewer ? state.codes : [];
}

interface SessionOptions {
  storage: Storage | null;
  viewer: string | null;
  tsCode: string;
  post: (body: ManualWatchlistCommandBody) => Promise<Receipt>;
  verifyBasis: (basis: WatchlistBasis) => Promise<"ready" | "stale" | "unavailable">;
  verifyOwner?: () => Promise<boolean>;
  nextId: () => string;
  now: () => string;
  withLock: (name: string, task: () => Promise<void>) => Promise<void>;
}

export class ManualWatchlistCommandSession {
  private current: ManualWatchlistCommandSnapshot;
  private listeners = new Set<() => void>();
  private readonly key: string;

  constructor(private readonly options: SessionOptions) {
    this.key =
      MANUAL_WATCHLIST_JOURNAL_KEY +
      ":" +
      encodeURIComponent(options.viewer ?? "") +
      ":" +
      options.tsCode;
    let record: ManualWatchlistCommandEntry | null = null;
    let storageAvailable =
      options.viewer !== null && options.storage !== null && CODE.test(options.tsCode);
    if (storageAvailable) {
      try {
        record = this.read();
      } catch {
        storageAvailable = false;
      }
    }
    this.current = {
      record,
      busy: false,
      storageAvailable,
      message: storageAvailable ? null : "浏览器记录不可用，暂时无法安全操作。",
    };
  }

  snapshot = (): ManualWatchlistCommandSnapshot => this.current;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    if (this.listeners.size === 1) {
      window.addEventListener("storage", this.onStorage);
      window.addEventListener(JOURNAL_EVENT, this.onLocalStorage);
    }
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) {
        window.removeEventListener("storage", this.onStorage);
        window.removeEventListener(JOURNAL_EVENT, this.onLocalStorage);
      }
    };
  };
  private onStorage = (event: StorageEvent): void => {
    if (
      event.storageArea === this.options.storage &&
      (event.key === this.key || event.key === null)
    )
      this.refresh();
  };
  private onLocalStorage = (event: Event): void => {
    if (event instanceof CustomEvent && event.detail === this.key) this.refresh();
  };
  private emit(changes: Partial<ManualWatchlistCommandSnapshot>): void {
    this.current = { ...this.current, ...changes };
    for (const listener of this.listeners) listener();
  }
  private read(): ManualWatchlistCommandEntry | null {
    const saved = this.options.storage?.getItem(this.key);
    if (saved == null) return null;
    const parsed: unknown = JSON.parse(saved);
    if (!validEntry(parsed, this.options.tsCode)) throw new Error("invalid journal");
    return { body: parsed.body, status: parsed.status, version: parsed.version };
  }
  private refresh(): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      this.emit({ record: this.read() });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录无法核对，请勿重复操作。" });
      return false;
    }
  }
  private persist(entry: ManualWatchlistCommandEntry): boolean {
    if (!this.options.storage || !this.current.storageAvailable) return false;
    try {
      const existing = this.read();
      if (
        existing &&
        existing.body.command_id === entry.body.command_id &&
        JSON.stringify(existing.body) !== JSON.stringify(entry.body)
      )
        throw new Error("command identity collision");
      if (
        existing &&
        existing.body.command_id !== entry.body.command_id &&
        !TERMINAL.has(existing.status)
      )
        throw new Error("another request is unresolved");
      if (
        existing?.body.command_id === entry.body.command_id &&
        TERMINAL.has(existing.status) &&
        !TERMINAL.has(entry.status)
      ) {
        this.emit({ record: existing });
        return false;
      }
      const saved = JSON.stringify({ schema: 1, ...entry });
      this.options.storage.setItem(this.key, saved);
      if (this.options.storage.getItem(this.key) !== saved) throw new Error("journal not durable");
      this.emit({ record: entry, message: null });
      window.dispatchEvent(new CustomEvent(JOURNAL_EVENT, { detail: this.key }));
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录不可用，无法安全核对。" });
      return false;
    }
  }
  private async send(body: ManualWatchlistCommandBody): Promise<void> {
    try {
      const receipt = await this.options.post(body);
      if (
        receipt.command_id !== body.command_id ||
        receipt.ts_code !== body.ts_code ||
        receipt.action !== body.action ||
        !validReceipt(receipt) ||
        (receipt.status === "saved_syncing" || receipt.status === "published"
          ? !(
              typeof receipt.version === "number" &&
              Number.isInteger(receipt.version) &&
              receipt.version >= 1
            )
          : receipt.version != null)
      )
        throw new Error("invalid receipt");
      this.persist({ body, status: receipt.status, version: receipt.version ?? null });
    } catch {
      this.persist({ body, status: "unknown", version: null });
      this.emit({ message: "状态待核对，请继续核对本次请求。" });
    }
  }
  private async locked(task: () => Promise<void>): Promise<void> {
    if (!this.current.storageAvailable || this.current.busy) return;
    this.emit({ busy: true });
    try {
      await this.options.withLock(this.key, task);
    } catch {
      this.emit({ message: "暂时无法安全操作，请稍后重试。" });
    } finally {
      this.emit({ busy: false });
    }
  }
  private async ownerReady(): Promise<boolean> {
    if (!this.options.verifyOwner) return true;
    return this.options.verifyOwner().catch(() => false);
  }
  async start(basis: WatchlistBasis): Promise<void> {
    await this.locked(async () => {
      if (!this.refresh()) return;
      if (this.current.record && !TERMINAL.has(this.current.record.status)) return;
      if (
        !HASH.test(basis.generationId) ||
        (basis.action === "remove" &&
          (basis.observedStatus !== "active" || basis.expectedVersion === null)) ||
        (basis.action === "add" &&
          !["absent", "expired", "deleted"].includes(basis.observedStatus)) ||
        (basis.observedStatus === "absent" && basis.expectedVersion !== null) ||
        (basis.observedStatus !== "absent" &&
          !(Number.isInteger(basis.expectedVersion) && Number(basis.expectedVersion) >= 1))
      ) {
        this.emit({ message: "名单状态已变化，请刷新后重试。" });
        return;
      }
      const verification = await this.options
        .verifyBasis(basis)
        .catch(() => "unavailable" as const);
      if (verification !== "ready") {
        this.emit({
          message:
            verification === "stale" ? "名单已更新，请刷新后重试。" : "名单暂不可用，请稍后重试。",
        });
        return;
      }
      if (!(await this.ownerReady())) {
        this.emit({ message: "登录身份已变化，请切回原账户核对。" });
        return;
      }
      const common = {
        command_id: this.options.nextId(),
        requested_at: this.options.now(),
        generation_id: basis.generationId,
        ts_code: this.options.tsCode,
        expected_version: basis.expectedVersion,
      };
      const body: ManualWatchlistCommandBody =
        basis.action === "add"
          ? { ...common, action: "add", source: basis.source ?? "detail", price_levels: [] }
          : { ...common, action: "remove", expected_version: basis.expectedVersion };
      if (!validBody(body, this.options.tsCode)) {
        this.emit({ message: "暂时无法创建请求，请重试。" });
        return;
      }
      if (this.persist({ body, status: "pending", version: null })) await this.send(body);
    });
  }
  async advance(): Promise<void> {
    await this.locked(async () => {
      if (!this.refresh()) return;
      const entry = this.current.record;
      if (entry && !TERMINAL.has(entry.status)) {
        if (!(await this.ownerReady())) {
          this.emit({ message: "登录身份已变化，请切回原账户核对。" });
          return;
        }
        await this.send(entry.body);
      }
    });
  }
}

async function browserLock(name: string, task: () => Promise<void>): Promise<void> {
  if (!navigator.locks?.request) throw new Error("browser lock unavailable");
  await navigator.locks.request(name, { mode: "exclusive" }, task);
}
function randomId(): string {
  return (
    "web-" +
    Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) =>
      byte.toString(16).padStart(2, "0"),
    ).join("")
  );
}

export type TrustedWatchlistBasis = {
  status: "active" | "expired" | "deleted" | "absent";
  version: number | null;
};

/** A fresh authenticated meta and same-generation exact read back each batch decision. */
export async function readTrustedWatchlistBasis(
  viewer: string,
  generationId: string,
  tsCode: string,
  updateMeta: (meta: Awaited<ReturnType<typeof fetchMeta>>) => void,
): Promise<
  { state: "ready"; basis: TrustedWatchlistBasis } | { state: "stale" | "unavailable"; basis: null }
> {
  try {
    if (!CODE.test(tsCode) || !HASH.test(generationId))
      return { state: "unavailable", basis: null };
    const meta = await fetchMeta();
    updateMeta(meta);
    if (
      meta.data.viewer !== viewer ||
      meta.serving.state !== "ready" ||
      meta.serving.generation_id !== generationId ||
      meta.data.generation?.generation_id !== generationId ||
      !Number.isFinite(Date.parse(meta.data.server_time))
    )
      return { state: "stale", basis: null };
    const { data } = await apiClient().GET("/api/v1/watchlist/{ts_code}", {
      params: { path: { ts_code: tsCode } },
      signal: AbortSignal.timeout(12_000),
    });
    if (
      data?.serving.state !== "ready" ||
      data.serving.generation_id !== generationId ||
      data.data.availability !== "ready" ||
      data.data.ts_code !== tsCode
    )
      return { state: "unavailable", basis: null };
    const fact = data.data;
    const expires = fact.expires_at === null ? null : Date.parse(fact.expires_at);
    if (expires !== null && !Number.isFinite(expires)) return { state: "unavailable", basis: null };
    const currentStatus =
      fact.status === "active" && expires !== null && expires <= Date.parse(meta.data.server_time)
        ? "expired"
        : fact.status;
    if (currentStatus === "absent" && fact.version === null)
      return { state: "ready", basis: { status: "absent", version: null } };
    if (
      (currentStatus === "active" || currentStatus === "expired" || currentStatus === "deleted") &&
      Number.isInteger(fact.version) &&
      Number(fact.version) >= 1
    )
      return { state: "ready", basis: { status: currentStatus, version: fact.version } };
    return { state: "unavailable", basis: null };
  } catch {
    return { state: "unavailable", basis: null };
  }
}

async function verifyBasis(
  viewer: string,
  tsCode: string,
  basis: WatchlistBasis,
  updateMeta: (meta: Awaited<ReturnType<typeof fetchMeta>>) => void,
): Promise<"ready" | "stale" | "unavailable"> {
  const current = await readTrustedWatchlistBasis(viewer, basis.generationId, tsCode, updateMeta);
  if (current.state !== "ready") return current.state;
  return current.basis.status === basis.observedStatus &&
    current.basis.version === basis.expectedVersion
    ? "ready"
    : "stale";
}

export function createBrowserManualWatchlistSession(
  viewer: string | null,
  tsCode: string | null,
  updateMeta: (meta: Awaited<ReturnType<typeof fetchMeta>>) => void,
  verifyOwner?: () => Promise<boolean>,
): ManualWatchlistCommandSession {
  return new ManualWatchlistCommandSession({
    storage: (() => {
      try {
        return typeof navigator.locks !== "undefined" ? window.localStorage : null;
      } catch {
        return null;
      }
    })(),
    viewer,
    tsCode: tsCode ?? "",
    post: submitManualWatchlistCommand,
    verifyBasis: (basis) => verifyBasis(viewer ?? "", tsCode ?? "", basis, updateMeta),
    verifyOwner,
    nextId: randomId,
    now: () => new Date().toISOString(),
    withLock: browserLock,
  });
}

export function useManualWatchlistCommand(
  viewer: string | null,
  tsCode: string | null,
  generationId: string | null,
) {
  const queryClient = useQueryClient();
  const session = useMemo(
    () =>
      createBrowserManualWatchlistSession(viewer, tsCode, (meta) =>
        queryClient.setQueryData(META_QUERY_KEY, meta),
      ),
    [queryClient, viewer, tsCode],
  );
  const state = useSyncExternalStore(session.subscribe, session.snapshot, session.snapshot);
  const recordStatus = state.record?.status;
  const recordGeneration = state.record?.body.generation_id;
  useEffect(() => {
    if (viewer) void session.advance();
  }, [viewer, session]);
  useEffect(() => {
    if (
      viewer &&
      recordStatus === "saved_syncing" &&
      generationId &&
      generationId !== recordGeneration
    )
      void session.advance();
  }, [viewer, session, generationId, recordGeneration, recordStatus]);
  useEffect(() => {
    if (viewer && recordStatus === "published")
      void queryClient
        .fetchQuery({ queryKey: META_QUERY_KEY, queryFn: fetchMeta, staleTime: 0 })
        .catch(() => undefined);
  }, [viewer, recordStatus, queryClient]);
  return { session, ...state };
}
