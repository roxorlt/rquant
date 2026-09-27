import { ApiError, type Schemas } from "@/api/client";

type Command = Schemas["AckCommandRequest"];
type Receipt = Schemas["AckCommandReceipt"];
type Status = Receipt["status"] | "unknown";

export interface AckJournalEntry {
  body: Command;
  status: Status;
  confirmationId: string | null;
}

interface AckJournal {
  schema: 1;
  entries: Record<string, AckJournalEntry>;
}

export interface AckCommandSnapshot {
  entries: Record<string, AckJournalEntry>;
  busyAlerts: readonly string[];
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const ACK_JOURNAL_KEY = "rquant.alert-ack-command.v1";
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

function validEntry(value: unknown, alertId: string): value is AckJournalEntry {
  if (!isRecord(value) || !isRecord(value.body)) return false;
  const { body } = value;
  return (
    Object.keys(body).length === 4 &&
    typeof body.alert_id === "string" &&
    body.alert_id === alertId &&
    HASH.test(body.alert_id) &&
    typeof body.generation_id === "string" &&
    HASH.test(body.generation_id) &&
    typeof body.command_id === "string" &&
    COMMAND_ID.test(body.command_id) &&
    typeof body.requested_at === "string" &&
    REQUESTED_AT.test(body.requested_at) &&
    Number.isFinite(Date.parse(body.requested_at)) &&
    STATUSES.has(value.status as Status) &&
    (value.status === "succeeded"
      ? typeof value.confirmationId === "string" &&
        value.confirmationId.length >= 1 &&
        value.confirmationId.length <= 128
      : value.confirmationId === null)
  );
}

function validJournal(value: unknown): value is AckJournal {
  return (
    isRecord(value) &&
    value.schema === 1 &&
    isRecord(value.entries) &&
    Object.entries(value.entries).every(([alertId, entry]) => validEntry(entry, alertId))
  );
}

/** One browser tab keeps exact original requests under the server-authenticated viewer. */
export class AlertAckCommandSession {
  private current: AckCommandSnapshot;
  private listeners = new Set<() => void>();
  private readonly key: string;

  constructor(
    private readonly storage: Storage | null,
    viewer: string | null,
    private readonly post: (body: Command) => Promise<Receipt>,
    private readonly nextId: () => string,
    private readonly now: () => string,
  ) {
    this.key = `${ACK_JOURNAL_KEY}:${viewer ?? ""}`;
    let entries: Record<string, AckJournalEntry> = {};
    let storageAvailable = viewer !== null && storage !== null;
    let message: string | null = null;
    if (storageAvailable && storage) {
      try {
        const saved = storage.getItem(this.key);
        if (saved !== null) {
          const parsed: unknown = JSON.parse(saved);
          if (!validJournal(parsed)) throw new Error("invalid acknowledgment journal");
          entries = parsed.entries;
        }
      } catch {
        storageAvailable = false;
        message = "浏览器记录无法核对，请勿重复确认。";
      }
    }
    this.current = { entries, busyAlerts: [], storageAvailable, message, revision: 0 };
  }

  snapshot = (): AckCommandSnapshot => this.current;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private emit(changes: Partial<AckCommandSnapshot>): void {
    this.current = { ...this.current, ...changes, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private persist(alertId: string, entry: AckJournalEntry): boolean {
    if (!this.current.storageAvailable || !this.storage) return false;
    const entries = { ...this.current.entries, [alertId]: entry };
    try {
      const serialized = JSON.stringify({ schema: 1, entries } satisfies AckJournal);
      this.storage.setItem(this.key, serialized);
      if (this.storage.getItem(this.key) !== serialized) throw new Error("request not stored");
      this.emit({ entries, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录不可用，无法安全核对。" });
      return false;
    }
  }

  async start(generationId: string, alertId: string): Promise<void> {
    if (!this.current.storageAvailable || this.current.busyAlerts.includes(alertId)) return;
    const previous = this.current.entries[alertId];
    if (previous && previous.status !== "failed") return;
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
        body.command_id === previous?.body.command_id ||
        !REQUESTED_AT.test(body.requested_at) ||
        !Number.isFinite(Date.parse(body.requested_at))
      )
        throw new Error("invalid command identity");
      if (this.persist(alertId, { body, status: "pending", confirmationId: null }))
        await this.advance(alertId);
    } catch {
      this.emit({ message: "暂时无法创建确认请求，请重试。" });
    }
  }

  async advance(alertId: string): Promise<void> {
    const entry = this.current.entries[alertId];
    if (
      !entry ||
      !this.current.storageAvailable ||
      this.current.busyAlerts.includes(alertId) ||
      entry.status === "succeeded" ||
      entry.status === "failed"
    )
      return;
    this.emit({ busyAlerts: [...this.current.busyAlerts, alertId], message: null });
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
        this.persist(alertId, {
          ...entry,
          status: "succeeded",
          confirmationId: receipt.confirmation_id,
        });
      } else if (["pending", "processing", "ambiguous", "failed"].includes(receipt.status)) {
        if (receipt.confirmation_id != null) throw new Error("unexpected confirmation identity");
        this.persist(alertId, { ...entry, status: receipt.status, confirmationId: null });
      } else {
        throw new Error("unrecognized receipt status");
      }
    } catch (error) {
      this.persist(alertId, { ...entry, status: "unknown", confirmationId: null });
      this.emit({
        message:
          error instanceof ApiError && error.status === 401
            ? "请先登录，再继续核对本次请求。"
            : error instanceof ApiError && error.status === 403
              ? "当前账号无权确认，请继续核对本次请求。"
              : error instanceof ApiError && error.status === 409
                ? "告警状态已变化，请刷新并核对本次请求。"
                : "确认状态待核对，请继续核对本次请求。",
      });
    } finally {
      this.emit({ busyAlerts: this.current.busyAlerts.filter((id) => id !== alertId) });
    }
  }

  async resumePending(): Promise<void> {
    for (const [alertId, entry] of Object.entries(this.current.entries)) {
      if (entry.status !== "succeeded" && entry.status !== "failed") await this.advance(alertId);
    }
  }
}
