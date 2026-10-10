import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import {
  createBrowserManualWatchlistSession,
  type ManualWatchlistCommandEntry,
  type ManualWatchlistCommandSession,
  publishedMatches,
  publishedSuperseded,
  readTrustedWatchlistBasis,
  type TrustedWatchlistBasis,
} from "./manualWatchlistCommand";
import type { ScreenRunData } from "./screen";
import { fetchMeta, META_QUERY_KEY } from "./useMeta";

export const MANUAL_WATCHLIST_BATCH_KEY = "rquant.manual-watchlist-batch.v1";
const EVENT = "rquant:manual-watchlist-batch";
const CODE = /^[0-9]{6}\.(?:SH|SZ|BJ)$/;
const HASH = /^[0-9a-f]{64}$/;
const DAY = /^\d{4}-\d{2}-\d{2}$/;
const STATES = new Set([
  "queued",
  "already",
  "added",
  "processing",
  "syncing",
  "uncertain",
  "conflict",
  "capacity",
  "failed",
  "unavailable",
]);
const OUTSTANDING = new Set(["processing", "syncing", "uncertain"]);

export type BatchItemStatus =
  | "queued"
  | "already"
  | "added"
  | "processing"
  | "syncing"
  | "uncertain"
  | "conflict"
  | "capacity"
  | "failed"
  | "unavailable";

export interface BatchCandidate {
  key: string;
  tradeDate: string;
  pageIndex: number;
  sourceIdentity: string;
  codes: string[];
}

export interface BatchItem {
  code: string;
  status: BatchItemStatus;
  commandId: string | null;
  fromBatch: boolean;
}

export interface BatchManifest {
  schema: 1;
  viewer: string;
  generationId: string;
  candidate: BatchCandidate;
  items: BatchItem[];
}

export function freezeScreenWatchlistPage(
  data: ScreenRunData | null,
  pageIndex: number,
  revision: string,
): BatchCandidate | null {
  if (
    data?.status !== "ready" ||
    !data.source ||
    !HASH.test(data.source.identity) ||
    !DAY.test(data.trade_date) ||
    !Number.isInteger(pageIndex) ||
    pageIndex < 0 ||
    data.rows.length < 1 ||
    data.rows.length > 20
  )
    return null;
  const codes = Array.from(new Set(data.rows.map((row) => row.ts_code)));
  if (codes.length < 1 || codes.some((code) => !CODE.test(code))) return null;
  return {
    key: JSON.stringify([revision, data.source.identity, data.trade_date, pageIndex, codes]),
    tradeDate: data.trade_date,
    pageIndex,
    sourceIdentity: data.source.identity,
    codes,
  };
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validCandidate(value: unknown): value is BatchCandidate {
  return (
    object(value) &&
    typeof value.key === "string" &&
    value.key.length > 0 &&
    typeof value.tradeDate === "string" &&
    DAY.test(value.tradeDate) &&
    Number.isInteger(value.pageIndex) &&
    Number(value.pageIndex) >= 0 &&
    typeof value.sourceIdentity === "string" &&
    HASH.test(value.sourceIdentity) &&
    Array.isArray(value.codes) &&
    value.codes.length > 0 &&
    value.codes.length <= 20 &&
    value.codes.every((code: unknown) => typeof code === "string" && CODE.test(code)) &&
    new Set(value.codes).size === value.codes.length
  );
}

function validManifest(value: unknown, viewer: string): value is BatchManifest {
  if (
    !object(value) ||
    value.schema !== 1 ||
    value.viewer !== viewer ||
    typeof value.generationId !== "string" ||
    !HASH.test(value.generationId) ||
    !validCandidate(value.candidate) ||
    !Array.isArray(value.items)
  )
    return false;
  const candidate = value.candidate;
  return (
    value.items.length === candidate.codes.length &&
    value.items.every(
      (item: unknown, index: number) =>
        object(item) &&
        item.code === candidate.codes[index] &&
        typeof item.status === "string" &&
        STATES.has(item.status) &&
        (item.commandId === null ||
          (typeof item.commandId === "string" && /^[A-Za-z0-9._-]{1,128}$/.test(item.commandId))) &&
        typeof item.fromBatch === "boolean",
    )
  );
}

export function batchCounts(manifest: BatchManifest): Record<BatchItemStatus, number> {
  const counts: Record<BatchItemStatus, number> = {
    queued: 0,
    already: 0,
    added: 0,
    processing: 0,
    syncing: 0,
    uncertain: 0,
    conflict: 0,
    capacity: 0,
    failed: 0,
    unavailable: 0,
  };
  for (const item of manifest.items) counts[item.status] += 1;
  return counts;
}

function classify(record: ManualWatchlistCommandEntry | null): BatchItemStatus {
  switch (record?.status) {
    case "pending":
    case "processing":
      return "processing";
    case "unknown":
    case "uncertain":
      return "uncertain";
    case "saved_syncing":
    case "published":
      return "syncing";
    case "conflict":
      return "conflict";
    case "capacity":
      return "capacity";
    case "failed":
      return "failed";
    default:
      return "unavailable";
  }
}

function priorSettled(
  record: ManualWatchlistCommandEntry,
  generationId: string,
  fact: TrustedWatchlistBasis,
): boolean {
  return (
    publishedMatches(record, { generationId, status: fact.status, version: fact.version }) ||
    publishedSuperseded(record, { generationId, version: fact.version }) ||
    (record.status === "published" &&
      record.body.action === "add" &&
      generationId !== record.body.generation_id &&
      fact.status === "expired" &&
      fact.version === record.version)
  );
}

type BatchOptions = {
  viewer: string;
  storage: Storage | null;
  readBasis: (
    viewer: string,
    generationId: string,
    code: string,
  ) => ReturnType<typeof readTrustedWatchlistBasis>;
  single: (viewer: string, code: string) => ManualWatchlistCommandSession;
  withLock: (name: string, task: () => Promise<void>) => Promise<void>;
  isCurrent: (candidate: BatchCandidate, generationId: string) => boolean;
  isTrustedViewer: () => boolean;
  verifyViewer: () => Promise<boolean>;
};

export class ManualWatchlistBatchSession {
  private readonly key: string;
  private readonly lockKey: string;
  private listeners = new Set<() => void>();
  private state: {
    manifest: BatchManifest | null;
    busy: boolean;
    storageAvailable: boolean;
    message: string | null;
  };

  constructor(private readonly options: BatchOptions) {
    this.key = `${MANUAL_WATCHLIST_BATCH_KEY}:${encodeURIComponent(options.viewer)}`;
    this.lockKey = `${MANUAL_WATCHLIST_BATCH_KEY}:lock:${encodeURIComponent(options.viewer)}`;
    try {
      this.state = {
        manifest: this.read(),
        busy: false,
        storageAvailable: options.storage !== null,
        message: options.storage === null ? "浏览器记录不可用，暂时无法批量操作。" : null,
      };
    } catch {
      this.state = {
        manifest: null,
        busy: false,
        storageAvailable: false,
        message: "浏览器记录无法核对，暂时无法批量操作。",
      };
    }
  }

  snapshot = (): typeof this.state => this.state;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    if (this.listeners.size === 1) {
      window.addEventListener("storage", this.onStorage);
      window.addEventListener(EVENT, this.onLocal);
    }
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) {
        window.removeEventListener("storage", this.onStorage);
        window.removeEventListener(EVENT, this.onLocal);
      }
    };
  };
  private emit(change: Partial<typeof this.state>): void {
    this.state = { ...this.state, ...change };
    for (const listener of this.listeners) listener();
  }
  private onStorage = (event: StorageEvent): void => {
    if (
      event.storageArea === this.options.storage &&
      (event.key === this.key || event.key === null)
    )
      this.refresh();
  };
  private onLocal = (event: Event): void => {
    if (event instanceof CustomEvent && event.detail === this.key) this.refresh();
  };
  private read(): BatchManifest | null {
    const raw = this.options.storage?.getItem(this.key);
    if (raw == null) return null;
    const parsed: unknown = JSON.parse(raw);
    if (!validManifest(parsed, this.options.viewer)) throw new Error("invalid batch journal");
    return parsed;
  }
  private refresh(): BatchManifest | null {
    if (!this.state.storageAvailable) return null;
    try {
      const manifest = this.read();
      this.emit({ manifest });
      return manifest;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录无法核对，请勿重复操作。" });
      return null;
    }
  }
  private save(manifest: BatchManifest): void {
    if (!this.options.storage || !this.state.storageAvailable)
      throw new Error("storage unavailable");
    const raw = JSON.stringify(manifest);
    this.options.storage.setItem(this.key, raw);
    if (this.options.storage.getItem(this.key) !== raw)
      throw new Error("batch journal not durable");
    this.emit({ manifest });
    window.dispatchEvent(new CustomEvent(EVENT, { detail: this.key }));
  }
  private update(
    manifest: BatchManifest,
    index: number,
    change: Partial<BatchItem>,
  ): BatchManifest {
    const items = manifest.items.map((item, position) =>
      position === index ? { ...item, ...change } : item,
    );
    const next = { ...manifest, items };
    this.save(next);
    return next;
  }
  private async locked(task: () => Promise<void>): Promise<void> {
    if (this.state.busy || !this.state.storageAvailable) return;
    this.emit({ busy: true, message: null });
    try {
      await this.options.withLock(this.lockKey, task);
    } catch {
      this.emit({ message: "暂时无法安全操作，请稍后重试。" });
    } finally {
      this.emit({ busy: false });
    }
  }
  private async ownerCurrent(): Promise<boolean> {
    if (!this.options.isTrustedViewer()) return false;
    try {
      return (await this.options.verifyViewer()) && this.options.isTrustedViewer();
    } catch {
      return false;
    }
  }
  async begin(candidate: BatchCandidate, generationId: string): Promise<void> {
    await this.locked(async () => {
      if (!(await this.ownerCurrent())) return;
      if (
        !validCandidate(candidate) ||
        !HASH.test(generationId) ||
        !this.options.isCurrent(candidate, generationId)
      ) {
        this.emit({ message: "结果已变化，请重新确认本页股票。" });
        return;
      }
      const existing = this.refresh();
      if (!this.state.storageAvailable) return;
      if (existing?.items.some((item) => OUTSTANDING.has(item.status))) {
        this.emit({ message: "先核对上次操作，再加入本页股票。" });
        return;
      }
      let manifest: BatchManifest = {
        schema: 1,
        viewer: this.options.viewer,
        generationId,
        candidate,
        items: candidate.codes.map((code) => ({
          code,
          status: "queued",
          commandId: null,
          fromBatch: false,
        })),
      };
      this.save(manifest);
      for (let index = 0; index < manifest.items.length; index += 1) {
        if (!(await this.ownerCurrent())) break;
        if (!this.options.isCurrent(candidate, generationId)) break;
        const item = manifest.items[index];
        if (!item) break;
        const single = this.options.single(this.options.viewer, item.code);
        const prior = single.snapshot().record;
        if (
          prior &&
          ["pending", "processing", "saved_syncing", "uncertain", "unknown"].includes(prior.status)
        ) {
          await single.advance();
          if (!(await this.ownerCurrent())) break;
          const record = single.snapshot().record;
          manifest = this.update(manifest, index, {
            status: classify(record),
            commandId: record?.body.command_id ?? prior.body.command_id,
            fromBatch: false,
          });
          continue;
        }
        const current = await this.options.readBasis(this.options.viewer, generationId, item.code);
        if (!(await this.ownerCurrent())) break;
        if (!this.options.isCurrent(candidate, generationId)) break;
        if (current.state !== "ready") {
          manifest = this.update(manifest, index, { status: "unavailable" });
          continue;
        }
        if (prior?.status === "published" && !priorSettled(prior, generationId, current.basis)) {
          manifest = this.update(manifest, index, {
            status: "syncing",
            commandId: prior.body.command_id,
            fromBatch: false,
          });
          continue;
        }
        if (current.basis.status === "active") {
          manifest = this.update(manifest, index, { status: "already" });
          continue;
        }
        await single.start({
          action: "add",
          source: "screen_result",
          generationId,
          expectedVersion: current.basis.version,
          observedStatus: current.basis.status,
        });
        if (!(await this.ownerCurrent())) break;
        const record = single.snapshot().record;
        manifest = this.update(manifest, index, {
          status: classify(record),
          commandId: record?.body.command_id ?? null,
          fromBatch:
            record?.body.source === "screen_result" &&
            record.body.generation_id === generationId &&
            record.body.command_id !== prior?.body.command_id,
        });
      }
    });
  }

  async reconcile(generationId: string | null): Promise<void> {
    await this.locked(async () => {
      if (!(await this.ownerCurrent())) return;
      let manifest = this.refresh();
      if (
        !manifest ||
        !this.state.storageAvailable ||
        generationId === null ||
        !HASH.test(generationId)
      )
        return;
      for (let index = 0; index < manifest.items.length; index += 1) {
        if (!(await this.ownerCurrent())) break;
        const item = manifest.items[index];
        if (
          !item ||
          ["added", "already", "conflict", "capacity", "failed", "unavailable"].includes(
            item.status,
          )
        )
          continue;
        const single = this.options.single(this.options.viewer, item.code);
        let record = single.snapshot().record;
        if (!record || (item.commandId !== null && record.body.command_id !== item.commandId)) {
          if (item.status !== "queued")
            manifest = this.update(manifest, index, { status: "uncertain" });
          continue;
        }
        if (item.commandId === null) {
          if (
            record.body.action !== "add" ||
            record.body.source !== "screen_result" ||
            record.body.generation_id !== manifest.generationId
          )
            continue;
          manifest = this.update(manifest, index, {
            commandId: record.body.command_id,
            fromBatch: true,
          });
        }
        if (
          ["pending", "processing", "saved_syncing", "uncertain", "unknown"].includes(record.status)
        ) {
          await single.advance();
          if (!(await this.ownerCurrent())) break;
          record = single.snapshot().record;
        }
        let status = classify(record);
        if (
          record?.status === "published" &&
          record.body.action === "add" &&
          generationId !== record.body.generation_id
        ) {
          const current = await this.options.readBasis(
            this.options.viewer,
            generationId,
            item.code,
          );
          if (!(await this.ownerCurrent())) break;
          if (
            current.state === "ready" &&
            current.basis.status === "active" &&
            current.basis.version === record.version
          )
            status = item.fromBatch ? "added" : "already";
          else if (
            current.state === "ready" &&
            publishedSuperseded(record, {
              generationId,
              version: current.basis.version,
            })
          )
            status = "unavailable";
        }
        manifest = this.update(manifest, index, { status });
      }
    });
  }
}

async function browserBatchLock(name: string, task: () => Promise<void>): Promise<void> {
  if (!navigator.locks?.request) throw new Error("browser lock unavailable");
  await navigator.locks.request(name, { mode: "exclusive" }, task);
}

export function useManualWatchlistBatch(
  viewer: string | null,
  generationId: string | null,
  candidate: BatchCandidate | null,
  ready: boolean,
) {
  const queryClient = useQueryClient();
  const live = useRef({ viewer, generationId, key: candidate?.key ?? null, ready });
  live.current = { viewer, generationId, key: candidate?.key ?? null, ready };
  const session = useMemo(() => {
    if (viewer === null) return null;
    const owner = viewer;
    const verifyViewer = async (): Promise<boolean> => {
      if (live.current.viewer !== owner) return false;
      try {
        const meta = await fetchMeta();
        queryClient.setQueryData(META_QUERY_KEY, meta);
        return (
          live.current.viewer === owner &&
          meta.data.viewer === owner &&
          meta.serving.state === "ready" &&
          meta.data.generation?.generation_id === meta.serving.generation_id &&
          Number.isFinite(Date.parse(meta.data.server_time))
        );
      } catch {
        return false;
      }
    };
    return new ManualWatchlistBatchSession({
      viewer: owner,
      storage: (() => {
        try {
          return typeof navigator.locks !== "undefined" ? window.localStorage : null;
        } catch {
          return null;
        }
      })(),
      readBasis: (owner, generation, code) =>
        readTrustedWatchlistBasis(owner, generation, code, (meta) =>
          queryClient.setQueryData(META_QUERY_KEY, meta),
        ),
      single: (owner, code) =>
        createBrowserManualWatchlistSession(
          owner,
          code,
          (meta) => queryClient.setQueryData(META_QUERY_KEY, meta),
          verifyViewer,
        ),
      withLock: browserBatchLock,
      isTrustedViewer: () => live.current.viewer === owner,
      verifyViewer,
      isCurrent: (snapshot, generation) =>
        live.current.viewer === owner &&
        live.current.generationId === generation &&
        live.current.key === snapshot.key &&
        live.current.ready,
    });
  }, [queryClient, viewer]);
  const state = useSyncExternalStore(
    session?.subscribe ?? (() => () => undefined),
    session?.snapshot ?? (() => null),
    session?.snapshot ?? (() => null),
  );
  useEffect(() => {
    if (session && generationId) void session.reconcile(generationId);
  }, [session, generationId]);
  return { session, state };
}
