import { ApiError, type Schemas } from "@/api/client";

export type SaveCommand = Schemas["SavePoolCommand"];
export type AttachCommand = Schemas["AttachPoolCommand"];
export type EditorCommand = SaveCommand | AttachCommand;
export type EditorReceipt = Schemas["PoolEditorReceipt"];
export type SaveInput = Omit<SaveCommand, "kind" | "command_id" | "requested_at">;

type CommandStatus = "pending" | "processing" | "succeeded" | "failed" | "ambiguous" | "unknown";
type AttachStatus = CommandStatus | "idle";

export interface EditorJournal {
  schema: 1;
  save: SaveCommand;
  canvasName: string | null;
  saveVersion: string | null;
  saveStatus: CommandStatus;
  attach: AttachCommand | null;
  attachStatus: AttachStatus;
}

export interface EditorSessionSnapshot {
  journal: EditorJournal | null;
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const POOL_EDITOR_JOURNAL_KEY = "rquant.pool-editor-command.v1";
const VERSION = /^[0-9a-f]{64}$/;
const ID = /^[A-Za-z0-9._-]{1,128}$/;
const SAVE_STATUSES = new Set<CommandStatus>([
  "pending",
  "processing",
  "succeeded",
  "failed",
  "ambiguous",
  "unknown",
]);
const ATTACH_STATUSES = new Set<AttachStatus>([...SAVE_STATUSES, "idle"]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJournal(value: unknown): value is EditorJournal {
  if (!isRecord(value) || value.schema !== 1 || !isRecord(value.save)) return false;
  const save = value.save;
  if (
    save.kind !== "save_user_pool_v2" ||
    typeof save.command_id !== "string" ||
    !ID.test(save.command_id) ||
    typeof save.requested_at !== "string" ||
    typeof save.base_name !== "string" ||
    typeof save.display_name !== "string" ||
    !Array.isArray(save.rule_calls) ||
    !Array.isArray(save.include_columns) ||
    (value.canvasName !== null && typeof value.canvasName !== "string") ||
    (value.saveVersion !== null &&
      (typeof value.saveVersion !== "string" || !VERSION.test(value.saveVersion))) ||
    !SAVE_STATUSES.has(value.saveStatus as CommandStatus) ||
    !ATTACH_STATUSES.has(value.attachStatus as AttachStatus)
  )
    return false;
  if (value.attach === null) return value.attachStatus === "idle";
  if (!isRecord(value.attach)) return false;
  const attach = value.attach;
  return (
    attach.kind === "add_pool_to_canvas" &&
    typeof attach.command_id === "string" &&
    ID.test(attach.command_id) &&
    typeof attach.requested_at === "string" &&
    attach.canvas_name === value.canvasName &&
    attach.pool_name === `user/${save.base_name}` &&
    attach.expected_pool_version === value.saveVersion &&
    value.saveStatus === "succeeded"
  );
}

/** One browser tab's durable two-command workflow. Every POST reads its already stored body. */
export class PoolEditorSession {
  private current: EditorSessionSnapshot;
  private listeners = new Set<() => void>();

  constructor(
    private readonly storage: Storage,
    private readonly post: (body: EditorCommand) => Promise<EditorReceipt>,
    private readonly nextId: () => string,
    private readonly now: () => string,
  ) {
    let journal: EditorJournal | null = null;
    let storageAvailable = true;
    let message: string | null = null;
    try {
      const saved = storage.getItem(POOL_EDITOR_JOURNAL_KEY);
      if (saved !== null) {
        const parsed: unknown = JSON.parse(saved);
        if (isJournal(parsed)) journal = parsed;
        else throw new Error("invalid journal");
      }
    } catch {
      storageAvailable = false;
      message = "浏览器存储不可用，上次请求无法核对。";
    }
    this.current = { journal, busy: false, storageAvailable, message, revision: 0 };
  }

  snapshot = (): EditorSessionSnapshot => this.current;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private emit(changes: Partial<EditorSessionSnapshot>): void {
    this.current = { ...this.current, ...changes, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private persist(journal: EditorJournal): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      const serialized = JSON.stringify(journal);
      this.storage.setItem(POOL_EDITOR_JOURNAL_KEY, serialized);
      if (this.storage.getItem(POOL_EDITOR_JOURNAL_KEY) !== serialized)
        throw new Error("not saved");
      this.emit({ journal, message: null });
      return true;
    } catch {
      this.emit({
        storageAvailable: false,
        message: "浏览器存储不可用，无法安全提交。",
      });
      return false;
    }
  }

  private attachBody(save: SaveCommand, canvasName: string, version: string): AttachCommand {
    return {
      kind: "add_pool_to_canvas",
      command_id: this.nextId(),
      requested_at: this.now(),
      canvas_name: canvasName,
      pool_name: `user/${save.base_name}`,
      expected_pool_version: version,
    };
  }

  async startSave(input: SaveInput, canvasName: string | null): Promise<void> {
    if (!this.current.storageAvailable || this.current.busy) return;
    const previous = this.current.journal;
    if (previous && !["failed", "succeeded"].includes(previous.saveStatus)) {
      this.emit({ message: "请先确认上一次保存请求。" });
      return;
    }
    if (
      previous?.saveStatus === "succeeded" &&
      !["idle", "succeeded"].includes(previous.attachStatus)
    ) {
      this.emit({ message: "请先确认上一次画布请求。" });
      return;
    }
    try {
      const save: SaveCommand = {
        ...input,
        kind: "save_user_pool_v2",
        command_id: this.nextId(),
        requested_at: this.now(),
      };
      const journal: EditorJournal = {
        schema: 1,
        save,
        canvasName,
        saveVersion: null,
        saveStatus: "pending",
        attach: null,
        attachStatus: "idle",
      };
      if (this.persist(journal)) await this.advance();
    } catch {
      this.emit({ message: "暂时无法生成保存请求，请重试。" });
    }
  }

  async retryAttachment(): Promise<void> {
    const journal = this.current.journal;
    if (
      this.current.busy ||
      !this.current.storageAvailable ||
      journal?.saveStatus !== "succeeded" ||
      journal.saveVersion === null ||
      journal.canvasName === null ||
      journal.attachStatus !== "failed"
    )
      return;
    try {
      const attach = this.attachBody(journal.save, journal.canvasName, journal.saveVersion);
      if (this.persist({ ...journal, attach, attachStatus: "pending" })) await this.advance();
    } catch {
      this.emit({ message: "暂时无法生成画布请求，请重试。" });
    }
  }

  async advance(): Promise<void> {
    const journal = this.current.journal;
    if (!journal || this.current.busy || !this.current.storageAvailable) return;
    const savePhase = journal.saveStatus !== "succeeded";
    const status = savePhase ? journal.saveStatus : journal.attachStatus;
    if (status === "failed" || status === "succeeded" || status === "idle") return;
    const body = savePhase ? journal.save : journal.attach;
    if (body === null) return;
    this.emit({ busy: true });
    let nextAttachment = false;
    try {
      const receipt = await this.post(body);
      if (receipt.command_id !== body.command_id) throw new Error("receipt identity mismatch");
      if (receipt.status === "succeeded") {
        if (savePhase) {
          const version = receipt.pool_version;
          if (typeof version !== "string" || !VERSION.test(version))
            throw new Error("invalid save version");
          const saved: EditorJournal = {
            ...journal,
            saveVersion: version,
            saveStatus: "succeeded",
          };
          if (!this.persist(saved)) return;
          if (saved.canvasName !== null) {
            const attach = this.attachBody(saved.save, saved.canvasName, version);
            nextAttachment = this.persist({ ...saved, attach, attachStatus: "pending" });
          }
        } else if (body.kind === "add_pool_to_canvas") {
          if (
            receipt.pool_version !== body.expected_pool_version ||
            receipt.canvas_name !== body.canvas_name
          ) {
            throw new Error("attachment identity mismatch");
          }
          this.persist({ ...journal, attachStatus: "succeeded" });
        }
      } else if (savePhase) {
        this.persist({ ...journal, saveStatus: receipt.status });
        this.emit({
          message:
            receipt.status === "failed" || receipt.status === "ambiguous" ? receipt.message : null,
        });
      } else {
        this.persist({ ...journal, attachStatus: receipt.status });
        this.emit({
          message:
            receipt.status === "failed" || receipt.status === "ambiguous" ? receipt.message : null,
        });
      }
    } catch (error) {
      const invalid = error instanceof ApiError && (error.status === 409 || error.status === 422);
      const next = savePhase
        ? { ...journal, saveStatus: invalid ? ("failed" as const) : ("unknown" as const) }
        : { ...journal, attachStatus: invalid ? ("failed" as const) : ("unknown" as const) };
      this.persist(next);
      this.emit({
        message:
          invalid && error instanceof ApiError ? error.message : "状态待确认，请用原请求继续核对。",
      });
    } finally {
      this.emit({ busy: false });
    }
    if (nextAttachment) await this.advance();
  }

  clear(): void {
    if (!this.current.storageAvailable || this.current.busy) return;
    try {
      this.storage.removeItem(POOL_EDITOR_JOURNAL_KEY);
      this.emit({ journal: null, message: null });
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法清理上次请求。" });
    }
  }
}
