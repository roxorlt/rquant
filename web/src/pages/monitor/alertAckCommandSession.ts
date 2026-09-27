import { ApiError, type Schemas } from "@/api/client";

type Command = Schemas["AckCommandRequest"];
type Receipt = Schemas["AckCommandReceipt"];
type Status = Receipt["status"] | "unknown";

export interface AckJournalEntry {
  body: Command;
  status: Status;
  confirmationId: string | null;
  failureKind: "stale_generation" | null;
}

interface StoredAckJournal extends AckJournalEntry {
  schema: 2;
}

export interface AckCommandSnapshot {
  entries: Record<string, AckJournalEntry>;
  busyAlerts: readonly string[];
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const ACK_JOURNAL_KEY = "rquant.alert-ack-command.v2";
const HASH = /^[0-9a-f]{64}$/;
const COMMAND_ID = /^[A-Za-z0-9._-]{1,128}$/;
const REQUESTED_AT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;
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

function validEntry(value: unknown, alertId: string, commandId: string): value is StoredAckJournal {
  if (!isRecord(value) || value.schema !== 2 || !isRecord(value.body)) return false;
  const { body } = value;
  return (
    Object.keys(body).length === 4 &&
    typeof body.alert_id === "string" &&
    body.alert_id === alertId &&
    HASH.test(body.alert_id) &&
    typeof body.generation_id === "string" &&
    HASH.test(body.generation_id) &&
    typeof body.command_id === "string" &&
    body.command_id === commandId &&
    COMMAND_ID.test(body.command_id) &&
    typeof body.requested_at === "string" &&
    REQUESTED_AT.test(body.requested_at) &&
    Number.isFinite(Date.parse(body.requested_at)) &&
    STATUSES.has(value.status as Status) &&
    (value.failureKind === null ||
      (value.status === "failed" && value.failureKind === "stale_generation")) &&
    (value.status === "succeeded"
      ? typeof value.confirmationId === "string" &&
        value.confirmationId.length >= 1 &&
        value.confirmationId.length <= 128
      : value.confirmationId === null)
  );
}

function preferred(current: AckJournalEntry | undefined, next: AckJournalEntry): AckJournalEntry {
  if (!current) return next;
  const priority = (entry: AckJournalEntry) =>
    entry.status === "succeeded" ? 3 : entry.status === "failed" ? 1 : 2;
  if (priority(next) !== priority(current))
    return priority(next) > priority(current) ? next : current;
  return next.body.requested_at >= current.body.requested_at ? next : current;
}

/** Exact requests survive tab closure; separate command keys cannot overwrite another tab. */
export class AlertAckCommandSession {
  private current: AckCommandSnapshot;
  private records: Record<string, AckJournalEntry> = {};
  private listeners = new Set<() => void>();
  private readonly prefix: string;

  constructor(
    private readonly storage: Storage | null,
    viewer: string | null,
    private readonly post: (body: Command) => Promise<Receipt>,
    private readonly nextId: () => string,
    private readonly now: () => string,
  ) {
    this.prefix = `${ACK_JOURNAL_KEY}:${viewer ?? ""}:`;
    let storageAvailable = viewer !== null && storage !== null;
    let message: string | null = null;
    if (storageAvailable) {
      try {
        this.records = this.readRecords();
      } catch {
        storageAvailable = false;
        message = "浏览器记录无法核对，请勿重复确认。";
      }
    }
    this.current = {
      entries: this.visibleEntries(),
      busyAlerts: [],
      storageAvailable,
      message,
      revision: 0,
    };
  }

  snapshot = (): AckCommandSnapshot => this.current;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    if (this.listeners.size === 1) window.addEventListener("storage", this.onStorage);
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) window.removeEventListener("storage", this.onStorage);
    };
  };

  private onStorage = (event: StorageEvent): void => {
    if (
      (event.storageArea !== null && event.storageArea !== this.storage) ||
      (event.key !== null && !event.key.startsWith(this.prefix))
    )
      return;
    this.refreshFromStorage(true);
  };

  private emit(changes: Partial<AckCommandSnapshot>): void {
    this.current = { ...this.current, ...changes, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private key(body: Command): string {
    return `${this.prefix}${body.alert_id}:${body.command_id}`;
  }

  private readRecords(): Record<string, AckJournalEntry> {
    const records: Record<string, AckJournalEntry> = {};
    if (!this.storage) return records;
    for (let index = 0; index < this.storage.length; index += 1) {
      const key = this.storage.key(index);
      if (!key?.startsWith(this.prefix)) continue;
      const [alertId, commandId, ...extra] = key.slice(this.prefix.length).split(":");
      const saved = this.storage.getItem(key);
      if (!alertId || !commandId || extra.length > 0 || saved === null)
        throw new Error("invalid acknowledgment journal key");
      const parsed: unknown = JSON.parse(saved);
      if (!validEntry(parsed, alertId, commandId))
        throw new Error("invalid acknowledgment journal");
      records[key] = parsed;
    }
    return records;
  }

  private visibleEntries(): Record<string, AckJournalEntry> {
    const entries: Record<string, AckJournalEntry> = {};
    for (const entry of Object.values(this.records)) {
      const alertId = entry.body.alert_id;
      entries[alertId] = preferred(entries[alertId], entry);
    }
    return entries;
  }

  private refreshFromStorage(announce = false): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      const previous = new Set(Object.keys(this.records));
      const next = this.readRecords();
      const changedElsewhere = announce && Object.keys(next).some((key) => !previous.has(key));
      this.records = next;
      this.emit({
        entries: this.visibleEntries(),
        message: changedElsewhere
          ? "另一标签页已有确认请求，正在核对同一条告警。"
          : this.current.message,
      });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录无法核对，请勿重复确认。" });
      return false;
    }
  }

  private persist(entry: AckJournalEntry): boolean {
    if (!this.current.storageAvailable || !this.storage) return false;
    const key = this.key(entry.body);
    try {
      const prior = this.storage.getItem(key);
      if (prior !== null) {
        const parsed: unknown = JSON.parse(prior);
        if (!validEntry(parsed, entry.body.alert_id, entry.body.command_id))
          throw new Error("invalid existing request");
        if (JSON.stringify(parsed.body) !== JSON.stringify(entry.body))
          throw new Error("command identity collision");
        if (parsed.status === "succeeded" && entry.status !== "succeeded") {
          this.refreshFromStorage(true);
          return false;
        }
      }
      const serialized = JSON.stringify({ schema: 2, ...entry } satisfies StoredAckJournal);
      this.storage.setItem(key, serialized);
      if (this.storage.getItem(key) !== serialized) throw new Error("request not stored");
      this.records = { ...this.records, [key]: entry };
      this.emit({ entries: this.visibleEntries(), message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录不可用，无法安全核对。" });
      return false;
    }
  }

  async start(generationId: string, alertId: string): Promise<void> {
    if (!this.current.storageAvailable || this.current.busyAlerts.includes(alertId)) return;
    if (!this.refreshFromStorage()) return;
    const previous = this.current.entries[alertId];
    if (previous && previous.status !== "failed") return;
    if (
      previous?.failureKind === "stale_generation" &&
      generationId === previous.body.generation_id
    )
      return;
    if (!HASH.test(generationId) || !HASH.test(alertId)) {
      this.emit({ message: "确认信息暂不可用，请刷新后重试。" });
      return;
    }
    try {
      const body: Command = {
        command_id: this.nextId(),
        requested_at: this.now(),
        generation_id: generationId,
        alert_id: alertId,
      };
      if (
        !COMMAND_ID.test(body.command_id) ||
        Object.values(this.records).some((item) => item.body.command_id === body.command_id) ||
        !REQUESTED_AT.test(body.requested_at) ||
        !Number.isFinite(Date.parse(body.requested_at))
      )
        throw new Error("invalid command identity");
      if (this.persist({ body, status: "pending", confirmationId: null, failureKind: null }))
        await this.advance(alertId, body.command_id);
    } catch {
      this.emit({ message: "暂时无法创建确认请求，请重试。" });
    }
  }

  async advance(alertId: string, firstCommandId?: string): Promise<void> {
    if (
      !this.current.storageAvailable ||
      this.current.busyAlerts.includes(alertId) ||
      !this.refreshFromStorage()
    )
      return;
    const unresolved = Object.values(this.records)
      .filter(
        (entry) =>
          entry.body.alert_id === alertId &&
          entry.status !== "succeeded" &&
          entry.status !== "failed",
      )
      .sort((a, b) => a.body.requested_at.localeCompare(b.body.requested_at));
    if (!unresolved.length) return;
    this.emit({ busyAlerts: [...this.current.busyAlerts, alertId], message: null });
    try {
      for (const entry of unresolved) {
        const latest = this.records[this.key(entry.body)];
        if (!latest || latest.status === "succeeded" || latest.status === "failed") continue;
        try {
          const receipt = await this.post(entry.body);
          if (receipt.command_id !== entry.body.command_id)
            throw new Error("receipt identity mismatch");
          if (receipt.status === "succeeded") {
            if (
              typeof receipt.confirmation_id !== "string" ||
              receipt.confirmation_id.length < 1 ||
              receipt.confirmation_id.length > 128
            )
              throw new Error("confirmation identity missing");
            this.persist({
              ...entry,
              status: "succeeded",
              confirmationId: receipt.confirmation_id,
            });
          } else if (["pending", "processing", "ambiguous", "failed"].includes(receipt.status)) {
            if (receipt.confirmation_id != null)
              throw new Error("unexpected confirmation identity");
            this.persist({
              ...entry,
              status: receipt.status,
              confirmationId: null,
              failureKind: null,
            });
          } else {
            throw new Error("unrecognized receipt status");
          }
        } catch (error) {
          const firstRejected =
            error instanceof ApiError &&
            error.status === 409 &&
            firstCommandId === entry.body.command_id &&
            entry.status === "pending";
          this.persist({
            ...entry,
            status: firstRejected ? "failed" : "unknown",
            confirmationId: null,
            failureKind: firstRejected ? "stale_generation" : null,
          });
          this.emit({
            message: firstRejected
              ? "数据已更新，请刷新后重新确认。"
              : error instanceof ApiError && error.status === 401
                ? "请先登录，再继续核对本次请求。"
                : error instanceof ApiError && error.status === 403
                  ? "当前账号无权确认，请继续核对本次请求。"
                  : error instanceof ApiError && error.status === 409
                    ? "告警状态已变化，请刷新并核对本次请求。"
                    : "确认状态待核对，请继续核对本次请求。",
          });
        }
      }
    } finally {
      this.emit({ busyAlerts: this.current.busyAlerts.filter((id) => id !== alertId) });
    }
  }

  async resumePending(): Promise<void> {
    if (!this.refreshFromStorage()) return;
    const alerts = new Set(
      Object.values(this.records)
        .filter((entry) => entry.status !== "succeeded" && entry.status !== "failed")
        .map((entry) => entry.body.alert_id),
    );
    for (const alertId of alerts) await this.advance(alertId);
  }
}
