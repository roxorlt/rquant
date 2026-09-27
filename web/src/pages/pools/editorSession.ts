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
  saveConflict?: boolean;
  attach: AttachCommand | null;
  attachStatus: AttachStatus;
  attachConflict?: boolean;
}

export interface EditorSessionSnapshot {
  journal: EditorJournal | null;
  deferred: EditorJournal[];
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const POOL_EDITOR_JOURNAL_KEY = "rquant.pool-editor-command.v1";
export const POOL_EDITOR_DEFERRED_KEY = "rquant.pool-editor-deferred.v1";
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
    (value.saveConflict !== undefined && typeof value.saveConflict !== "boolean") ||
    !ATTACH_STATUSES.has(value.attachStatus as AttachStatus) ||
    (value.attachConflict !== undefined && typeof value.attachConflict !== "boolean")
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
    typeof attach.expected_pool_version === "string" &&
    VERSION.test(attach.expected_pool_version) &&
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
    let deferred: EditorJournal[] = [];
    let storageAvailable = true;
    let message: string | null = null;
    try {
      const saved = storage.getItem(POOL_EDITOR_JOURNAL_KEY);
      if (saved !== null) {
        const parsed: unknown = JSON.parse(saved);
        if (isJournal(parsed)) journal = parsed;
        else throw new Error("invalid journal");
      }
      const archived = storage.getItem(POOL_EDITOR_DEFERRED_KEY);
      if (archived !== null) {
        const parsed: unknown = JSON.parse(archived);
        if (!Array.isArray(parsed) || parsed.length > 20 || !parsed.every(isJournal))
          throw new Error("invalid deferred requests");
        deferred = parsed.filter((item) => item.attach?.command_id !== journal?.attach?.command_id);
      }
    } catch {
      storageAvailable = false;
      message = "浏览器存储不可用，上次请求无法核对。";
    }
    this.current = { journal, deferred, busy: false, storageAvailable, message, revision: 0 };
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

  private persistDeferred(deferred: EditorJournal[]): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      const serialized = JSON.stringify(deferred);
      this.storage.setItem(POOL_EDITOR_DEFERRED_KEY, serialized);
      if (this.storage.getItem(POOL_EDITOR_DEFERRED_KEY) !== serialized)
        throw new Error("not saved");
      this.emit({ deferred, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法安全核对。" });
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
    if (
      this.current.deferred.some(
        (item) =>
          item.save.base_name === input.base_name &&
          ["pending", "processing", "ambiguous", "unknown"].includes(item.attachStatus),
      )
    ) {
      this.emit({ message: "这只池子的画布请求待确认，请先继续核对。" });
      return;
    }
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
        saveConflict: false,
        attach: null,
        attachStatus: "idle",
      };
      if (this.persist(journal)) await this.advance();
    } catch {
      this.emit({ message: "暂时无法生成保存请求，请重试。" });
    }
  }

  async retryAttachment(verifiedVersion?: string): Promise<void> {
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
    const version = verifiedVersion ?? journal.saveVersion;
    if (
      !VERSION.test(version) ||
      (journal.attachConflict && version === journal.attach?.expected_pool_version)
    ) {
      this.emit({ message: "请先核对已发布的新规则版本，或结束本次挂接。" });
      return;
    }
    try {
      const attach = this.attachBody(journal.save, journal.canvasName, version);
      if (this.persist({ ...journal, attach, attachStatus: "pending", attachConflict: false }))
        await this.advance();
    } catch {
      this.emit({ message: "暂时无法生成画布请求，请重试。" });
    }
  }

  discardFailedAttachment(): void {
    const journal = this.current.journal;
    if (
      !journal ||
      this.current.busy ||
      journal.saveStatus !== "succeeded" ||
      journal.attachStatus !== "failed"
    )
      return;
    this.persist({
      ...journal,
      canvasName: null,
      attach: null,
      attachStatus: "idle",
      attachConflict: false,
    });
  }

  deferAttachment(): void {
    const journal = this.current.journal;
    if (
      !journal ||
      this.current.busy ||
      !this.current.storageAvailable ||
      journal.saveStatus !== "succeeded" ||
      !["ambiguous", "unknown"].includes(journal.attachStatus) ||
      !journal.attach ||
      this.current.deferred.length >= 20
    )
      return;
    if (!this.persistDeferred([...this.current.deferred, journal])) return;
    try {
      this.storage.removeItem(POOL_EDITOR_JOURNAL_KEY);
      this.emit({ journal: null, message: "画布请求已留待核对，可继续编辑其他池子。" });
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法安全暂存请求。" });
    }
  }

  async advanceDeferred(commandId: string): Promise<void> {
    const journal = this.current.deferred.find((item) => item.attach?.command_id === commandId);
    const body = journal?.attach;
    if (
      !journal ||
      !body ||
      this.current.busy ||
      !this.current.storageAvailable ||
      !["pending", "processing", "ambiguous", "unknown"].includes(journal.attachStatus)
    )
      return;
    this.emit({ busy: true });
    try {
      const receipt = await this.post(body);
      if (receipt.command_id !== body.command_id) throw new Error("receipt identity mismatch");
      if (
        receipt.status === "succeeded" &&
        (receipt.pool_version !== body.expected_pool_version ||
          receipt.canvas_name !== body.canvas_name)
      )
        throw new Error("attachment identity mismatch");
      this.persistDeferred(
        this.current.deferred.map((item) =>
          item.attach?.command_id === commandId ? { ...item, attachStatus: receipt.status } : item,
        ),
      );
      if (["failed", "ambiguous"].includes(receipt.status)) this.emit({ message: receipt.message });
    } catch (error) {
      const invalid = error instanceof ApiError && (error.status === 409 || error.status === 422);
      this.persistDeferred(
        this.current.deferred.map((item) =>
          item.attach?.command_id === commandId
            ? {
                ...item,
                attachStatus: invalid ? "failed" : "unknown",
                attachConflict: error instanceof ApiError && error.status === 409,
              }
            : item,
        ),
      );
      this.emit({
        message:
          invalid && error instanceof ApiError ? error.message : "状态待确认，请用原请求继续核对。",
      });
    } finally {
      this.emit({ busy: false });
    }
  }

  dismissDeferred(commandId: string): void {
    if (this.current.busy) return;
    const journal = this.current.deferred.find((item) => item.attach?.command_id === commandId);
    if (journal?.attachStatus !== "failed" && journal?.attachStatus !== "succeeded") return;
    this.persistDeferred(
      this.current.deferred.filter((item) => item.attach?.command_id !== commandId),
    );
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
          this.persist({ ...journal, attachStatus: "succeeded", attachConflict: false });
        }
      } else if (savePhase) {
        this.persist({ ...journal, saveStatus: receipt.status });
        this.emit({
          message:
            receipt.status === "failed" || receipt.status === "ambiguous" ? receipt.message : null,
        });
      } else {
        this.persist({ ...journal, attachStatus: receipt.status, attachConflict: false });
        this.emit({
          message:
            receipt.status === "failed" || receipt.status === "ambiguous" ? receipt.message : null,
        });
      }
    } catch (error) {
      const invalid = error instanceof ApiError && (error.status === 409 || error.status === 422);
      const next = savePhase
        ? {
            ...journal,
            saveStatus: invalid ? ("failed" as const) : ("unknown" as const),
            saveConflict: error instanceof ApiError && error.status === 409,
          }
        : {
            ...journal,
            attachStatus: invalid ? ("failed" as const) : ("unknown" as const),
            attachConflict: error instanceof ApiError && error.status === 409,
          };
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
