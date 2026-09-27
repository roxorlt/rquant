import { ApiError, type Schemas } from "@/api/client";

export type CreateCanvasCommand = Schemas["CreateCanvasCommand"];
type Receipt = Schemas["PoolEditorReceipt"];
type CommandStatus = "pending" | "processing" | "succeeded" | "failed" | "ambiguous" | "unknown";

export interface CanvasCreateJournal {
  schema: 1;
  body: CreateCanvasCommand;
  status: CommandStatus;
  recordHash: string | null;
  reason: string | null;
}

export interface CanvasCreateSnapshot {
  journal: CanvasCreateJournal | null;
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const CANVAS_CREATE_JOURNAL_KEY = "rquant.canvas-create-command.v1";
const NAME = /^[\w\u4e00-\u9fff-]{1,80}$/u;
const ID = /^[A-Za-z0-9._-]{1,128}$/;
const HASH = /^[0-9a-f]{64}$/;
const STATUSES = new Set<CommandStatus>([
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

function isJournal(value: unknown): value is CanvasCreateJournal {
  if (!isRecord(value) || value.schema !== 1 || !isRecord(value.body)) return false;
  const { body } = value;
  return (
    Object.keys(body).length === 5 &&
    body.kind === "create_canvas" &&
    typeof body.command_id === "string" &&
    ID.test(body.command_id) &&
    typeof body.requested_at === "string" &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(body.requested_at) &&
    typeof body.name === "string" &&
    NAME.test(body.name) &&
    typeof body.description === "string" &&
    body.description.length <= 1024 &&
    STATUSES.has(value.status as CommandStatus) &&
    ((value.status === "succeeded" &&
      typeof value.recordHash === "string" &&
      HASH.test(value.recordHash)) ||
      (value.status !== "succeeded" && value.recordHash === null)) &&
    (value.reason === null || (typeof value.reason === "string" && value.reason.length <= 200))
  );
}

/** One tab's durable create request; uncertain responses always reuse the stored body. */
export class CanvasCreateSession {
  private current: CanvasCreateSnapshot;
  private listeners = new Set<() => void>();

  constructor(
    private readonly storage: Storage,
    private readonly post: (body: CreateCanvasCommand) => Promise<Receipt>,
    private readonly nextId: () => string,
    private readonly now: () => string,
  ) {
    let journal: CanvasCreateJournal | null = null;
    let storageAvailable = true;
    let message: string | null = null;
    try {
      const saved = storage.getItem(CANVAS_CREATE_JOURNAL_KEY);
      if (saved !== null) {
        const parsed: unknown = JSON.parse(saved);
        if (!isJournal(parsed)) throw new Error("invalid journal");
        journal = parsed;
      }
    } catch {
      storageAvailable = false;
      message = "浏览器存储不可用，上次请求无法核对。";
    }
    this.current = { journal, busy: false, storageAvailable, message, revision: 0 };
  }

  snapshot = (): CanvasCreateSnapshot => this.current;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private emit(changes: Partial<CanvasCreateSnapshot>): void {
    this.current = { ...this.current, ...changes, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private persist(journal: CanvasCreateJournal): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      const serialized = JSON.stringify(journal);
      this.storage.setItem(CANVAS_CREATE_JOURNAL_KEY, serialized);
      if (this.storage.getItem(CANVAS_CREATE_JOURNAL_KEY) !== serialized)
        throw new Error("request not stored");
      this.emit({ journal, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法安全核对。" });
      return false;
    }
  }

  async start(name: string, description: string): Promise<void> {
    if (!this.current.storageAvailable || this.current.busy) return;
    if (this.current.journal && this.current.journal.status !== "failed") {
      this.emit({ message: "请先确认上一次画布请求。" });
      return;
    }
    const cleanName = name.trim();
    const cleanDescription = description.trim();
    if (!NAME.test(cleanName) || cleanDescription.length > 1024) {
      this.emit({ message: "请检查画布名称和说明。" });
      return;
    }
    try {
      const journal: CanvasCreateJournal = {
        schema: 1,
        body: {
          kind: "create_canvas",
          command_id: this.nextId(),
          requested_at: this.now(),
          name: cleanName,
          description: cleanDescription,
        },
        status: "pending",
        recordHash: null,
        reason: null,
      };
      if (this.persist(journal)) await this.advance();
    } catch {
      this.emit({ message: "暂时无法生成创建请求，请重试。" });
    }
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
      if (receipt.command_id !== journal.body.command_id)
        throw new Error("receipt identity mismatch");
      if (receipt.status === "succeeded") {
        if (
          receipt.canvas_name !== journal.body.name ||
          typeof receipt.canvas_record_hash !== "string" ||
          !HASH.test(receipt.canvas_record_hash)
        )
          throw new Error("publication identity mismatch");
        this.persist({
          ...journal,
          status: "succeeded",
          recordHash: receipt.canvas_record_hash,
          reason: null,
        });
      } else {
        this.persist({
          ...journal,
          status: receipt.status,
          recordHash: null,
          reason: ["failed", "ambiguous"].includes(receipt.status) ? receipt.message : null,
        });
      }
    } catch (error) {
      const invalid = error instanceof ApiError && error.status === 422;
      this.persist({
        ...journal,
        status: invalid ? "failed" : "unknown",
        recordHash: null,
        reason: invalid && error instanceof ApiError ? error.message : null,
      });
      this.emit({
        message:
          invalid && error instanceof ApiError ? error.message : "状态待确认，请用原请求继续核对。",
      });
    } finally {
      this.emit({ busy: false });
    }
  }

  clearTerminal(): boolean {
    const journal = this.current.journal;
    if (!journal || this.current.busy || !["succeeded", "failed"].includes(journal.status))
      return false;
    try {
      this.storage.removeItem(CANVAS_CREATE_JOURNAL_KEY);
      this.emit({ journal: null, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法结束请求。" });
      return false;
    }
  }
}
