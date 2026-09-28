import type { Schemas } from "@/api/client";

export type ScreenPoolSaveCommand = Schemas["SaveRankedPoolCommand"];
type Receipt = Schemas["PoolEditorReceipt"];
type Status = "pending" | "processing" | "succeeded" | "failed" | "ambiguous" | "unknown";

export interface ScreenPoolSaveJournal {
  schema: 1;
  viewer: string;
  body: ScreenPoolSaveCommand;
  status: Status;
  version: string | null;
  message: string | null;
}

export interface ScreenPoolSaveSnapshot {
  journal: ScreenPoolSaveJournal | null;
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

const KEY_PREFIX = "rquant.screen-pool-save.v1:";
const VERSION = /^[0-9a-f]{64}$/;
const STATUSES = new Set<Status>([
  "pending",
  "processing",
  "succeeded",
  "failed",
  "ambiguous",
  "unknown",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJournal(value: unknown, viewer: string): value is ScreenPoolSaveJournal {
  if (!isRecord(value) || !isRecord(value.body)) return false;
  const body = value.body;
  if (
    value.schema !== 1 ||
    value.viewer !== viewer ||
    !STATUSES.has(value.status as Status) ||
    (value.version !== null &&
      (typeof value.version !== "string" || !VERSION.test(value.version))) ||
    (value.message !== null && typeof value.message !== "string") ||
    body.kind !== "save_user_pool_v3" ||
    typeof body.command_id !== "string" ||
    typeof body.requested_at !== "string" ||
    typeof body.base_name !== "string" ||
    typeof body.display_name !== "string" ||
    !Array.isArray(body.rule_calls) ||
    !Array.isArray(body.include_columns) ||
    (body.expected_version !== null && typeof body.expected_version !== "string")
  )
    return false;
  if (body.ranking !== null) {
    if (!isRecord(body.ranking) || !Array.isArray(body.ranking.conditions)) return false;
    if (
      typeof body.ranking.top_n !== "number" ||
      !Number.isInteger(body.ranking.top_n) ||
      body.ranking.top_n < 1 ||
      body.ranking.top_n > 100 ||
      body.ranking.conditions.length < 1 ||
      body.ranking.conditions.length > 4 ||
      !body.ranking.conditions.every(
        (item: unknown) =>
          isRecord(item) &&
          typeof item.metric === "string" &&
          typeof item.ascending === "boolean" &&
          typeof item.weight === "number" &&
          Number.isFinite(item.weight),
      )
    )
      return false;
  }
  return true;
}

export function screenPoolSaveStorage(): Storage {
  try {
    return window.sessionStorage;
  } catch {
    return {
      getItem: () => {
        throw new Error("storage unavailable");
      },
    } as unknown as Storage;
  }
}

/** One tab's immutable save request; an uncertain reply is checked using the original body. */
export class ScreenPoolSaveSession {
  private current: ScreenPoolSaveSnapshot;
  private listeners = new Set<() => void>();
  private readonly key: string | null;

  constructor(
    viewer: string | null,
    private readonly storage: Storage,
    private readonly post: (body: ScreenPoolSaveCommand) => Promise<Receipt>,
  ) {
    this.key = viewer === null ? null : `${KEY_PREFIX}${encodeURIComponent(viewer)}`;
    let journal: ScreenPoolSaveJournal | null = null;
    let storageAvailable = true;
    let message: string | null = null;
    if (this.key !== null && viewer !== null) {
      try {
        const raw = storage.getItem(this.key);
        if (raw !== null) {
          const parsed: unknown = JSON.parse(raw);
          if (!isJournal(parsed, viewer)) throw new Error("invalid save journal");
          journal = parsed;
        }
      } catch {
        storageAvailable = false;
        message = "上次保存记录无法核对，请检查浏览器存储。";
      }
    }
    this.current = { journal, busy: false, storageAvailable, message, revision: 0 };
  }

  snapshot = (): ScreenPoolSaveSnapshot => this.current;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private emit(change: Partial<ScreenPoolSaveSnapshot>): void {
    this.current = { ...this.current, ...change, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private persist(journal: ScreenPoolSaveJournal): boolean {
    if (this.key === null || !this.current.storageAvailable) return false;
    try {
      const serialized = JSON.stringify(journal);
      this.storage.setItem(this.key, serialized);
      if (this.storage.getItem(this.key) !== serialized) throw new Error("not stored");
      this.emit({ journal, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法安全提交。" });
      return false;
    }
  }

  async start(body: ScreenPoolSaveCommand): Promise<void> {
    if (
      !this.key ||
      !this.current.storageAvailable ||
      this.current.busy ||
      this.current.journal !== null
    )
      return;
    const viewer = decodeURIComponent(this.key.slice(KEY_PREFIX.length));
    const journal: ScreenPoolSaveJournal = {
      schema: 1,
      viewer,
      body,
      status: "pending",
      version: null,
      message: null,
    };
    if (this.persist(journal)) await this.advance();
  }

  async advance(): Promise<void> {
    const journal = this.current.journal;
    if (
      !journal ||
      this.current.busy ||
      !this.current.storageAvailable ||
      journal.status === "succeeded" ||
      journal.status === "failed"
    )
      return;
    this.emit({ busy: true });
    try {
      const receipt = await this.post(journal.body);
      if (receipt.command_id !== journal.body.command_id) throw new Error("receipt mismatch");
      if (receipt.status === "succeeded") {
        if (typeof receipt.pool_version !== "string" || !VERSION.test(receipt.pool_version))
          throw new Error("invalid pool version");
        this.persist({
          ...journal,
          status: "succeeded",
          version: receipt.pool_version,
          message: null,
        });
      } else {
        this.persist({
          ...journal,
          status: receipt.status,
          message:
            receipt.status === "failed" || receipt.status === "ambiguous" ? receipt.message : null,
        });
      }
    } catch {
      this.persist({
        ...journal,
        status: "unknown",
        message: "保存状态待确认，请继续核对。",
      });
    } finally {
      this.emit({ busy: false });
    }
  }

  clear(): boolean {
    if (
      this.key === null ||
      !this.current.storageAvailable ||
      this.current.busy ||
      (this.current.journal !== null &&
        !["succeeded", "failed"].includes(this.current.journal.status))
    )
      return false;
    try {
      this.storage.removeItem(this.key);
      this.emit({ journal: null, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法清理上次请求。" });
      return false;
    }
  }
}
