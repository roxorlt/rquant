import type { ScreenOriginalAction, ScreenQueryReadData } from "@/api/screen";

type Operation = "submit" | "lookup" | "resume";
type Status = "idle" | "pending" | "processing" | "unknown" | "succeeded" | "failed";
export interface ScreenQueryCommandSnapshot {
  original: ScreenOriginalAction | null;
  status: Status;
  data: ScreenQueryReadData | null;
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
}
const PREFIX = "rquant.screen-command.v1:";
const SHA = /^[0-9a-f]{64}$/;
const MAX_BYTES = 64 * 1024;
type Transport = (
  original: ScreenOriginalAction,
  operation: Operation,
  signal: AbortSignal,
) => Promise<ScreenQueryReadData>;
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function storedOriginal(value: unknown, scope: string): ScreenOriginalAction | null {
  if (!record(value) || value.schema !== 1 || value.scope !== scope || !record(value.original))
    return null;
  const original = value.original;
  const command =
    original.action === "execute"
      ? original.command
      : original.action === "presets_save"
        ? original.request
        : null;
  if (
    !record(command) ||
    typeof command.command_id !== "string" ||
    typeof command.requested_at !== "string" ||
    !Number.isFinite(Date.parse(command.requested_at))
  )
    return null;
  const definition =
    original.action === "execute"
      ? command.definition
      : record(command.preset)
        ? command.preset.definition
        : null;
  if (
    !record(definition) ||
    !Array.isArray(definition.conditions) ||
    definition.conditions.length < 1 ||
    definition.conditions.length > 26 ||
    typeof definition.source_identity !== "string" ||
    !SHA.test(definition.source_identity)
  )
    return null;
  if ("owner_id" in command || "counts" in command || "outcome" in command) return null;
  return original as ScreenOriginalAction;
}

/** A tab stores only an immutable pending request. Success always needs server readback. */
export class ScreenQueryCommandSession {
  private state: ScreenQueryCommandSnapshot;
  private listeners = new Set<() => void>();
  private controller: AbortController | null = null;
  private disposed = false;
  private readonly key: string;
  constructor(
    private readonly scope: string,
    private readonly storage: Storage,
    private readonly transport: Transport,
  ) {
    if (!SHA.test(scope)) throw new Error("invalid private screen scope");
    this.key = PREFIX + scope;
    let original: ScreenOriginalAction | null = null;
    let storageAvailable = true;
    try {
      const raw = storage.getItem(this.key);
      if (raw !== null) {
        if (new TextEncoder().encode(raw).length > MAX_BYTES) throw new Error("journal too large");
        original = storedOriginal(JSON.parse(raw), scope);
        if (original === null) throw new Error("invalid original request");
      }
    } catch {
      storageAvailable = false;
    }
    this.state = {
      original,
      status: original === null ? "idle" : "unknown",
      busy: false,
      data: null,
      storageAvailable,
      message: !storageAvailable
        ? "浏览器存储不可用，请先核对历史。"
        : original !== null
          ? "结果待确认，请核对原请求。"
          : null,
    };
  }
  snapshot = (): ScreenQueryCommandSnapshot => this.state;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  private emit(change: Partial<ScreenQueryCommandSnapshot>): void {
    if (this.disposed) return;
    this.state = { ...this.state, ...change };
    for (const listener of this.listeners) listener();
  }
  async submit(original: ScreenOriginalAction, operation: Operation = "submit"): Promise<void> {
    if (
      this.disposed ||
      this.state.busy ||
      this.state.original !== null ||
      !this.state.storageAvailable
    )
      return;
    try {
      const raw = JSON.stringify({ schema: 1, scope: this.scope, original });
      if (new TextEncoder().encode(raw).length > MAX_BYTES) throw new Error("request too large");
      const frozen = storedOriginal(JSON.parse(raw), this.scope);
      if (frozen === null) throw new Error("invalid request");
      this.storage.setItem(this.key, raw);
      if (this.storage.getItem(this.key) !== raw) throw new Error("request not retained");
      this.emit({ original: frozen, status: "pending", data: null, message: null });
    } catch {
      this.emit({ storageAvailable: false, message: "原请求未保存，无法安全提交。" });
      return;
    }
    await this.advance(operation);
  }
  async recover(operation: "lookup" | "resume"): Promise<void> {
    await this.advance(operation);
  }
  private confirmed(data: ScreenQueryReadData, original: ScreenOriginalAction): boolean {
    if (data.receipt?.status !== "succeeded") return false;
    if (original.action === "presets_save") {
      const saved = data.presets.find(
        (item) => item.preset_id === original.request.preset.preset_id,
      );
      return (
        !!saved &&
        saved.version === (original.request.expected_version ?? 0) + 1 &&
        SHA.test(saved.command_hash)
      );
    }
    const execution = data.execution;
    const results = data.results;
    return (
      !!execution &&
      !!results &&
      execution.status === "succeeded" &&
      execution.execution_id === original.command.command_id &&
      results.execution_id === execution.execution_id &&
      execution.definition.source_identity === original.command.definition.source_identity &&
      execution.definition.trade_date === original.command.definition.trade_date &&
      execution.artifact_sha256 === results.artifact_sha256 &&
      SHA.test(results.artifact_sha256) &&
      execution.member_rank_sha256 != null &&
      SHA.test(execution.member_rank_sha256) &&
      execution.base_count != null &&
      execution.total != null &&
      execution.unknown_count != null
    );
  }
  private async advance(operation: Operation): Promise<void> {
    const original = this.state.original;
    if (this.disposed || !original || this.state.busy || !this.state.storageAvailable) return;
    const controller = new AbortController();
    this.controller = controller;
    this.emit({ busy: true, message: null });
    try {
      const data = await this.transport(original, operation, controller.signal);
      if (this.disposed || controller.signal.aborted) return;
      const id =
        original.action === "execute" ? original.command.command_id : original.request.command_id;
      if (data.owner_scope_tag !== this.scope || data.receipt?.command_id !== id)
        throw new Error("receipt scope changed");
      const failed = data.receipt.status === "failed";
      if (failed || this.confirmed(data, original)) {
        this.storage.removeItem(this.key);
        this.emit({
          original: null,
          data,
          status: failed ? "failed" : "succeeded",
          message: failed ? "本次未完成，请查看原请求。" : null,
        });
      } else if (data.receipt.status === "pending" || data.receipt.status === "processing") {
        this.emit({
          data: null,
          status: data.receipt.status,
          message: "结果待确认，请核对原请求。",
        });
      } else throw new Error("server result is incomplete");
    } catch {
      this.emit({ status: "unknown", data: null, message: "结果待确认，请核对原请求。" });
    } finally {
      this.controller = null;
      this.emit({ busy: false });
    }
  }
  clear(): boolean {
    return !this.state.busy && this.state.original === null;
  }
  dispose(clearPrivateRequest = false): void {
    this.controller?.abort();
    if (clearPrivateRequest) {
      try {
        this.storage.removeItem(this.key);
      } catch {
        /* Server history remains authoritative. */
      }
    }
    this.disposed = true;
    this.state = { ...this.state, original: null, data: null, busy: false };
    this.listeners.clear();
  }
}
