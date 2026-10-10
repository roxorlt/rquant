import type { DataCenterCommand, DataCenterCommandReceipt } from "@/api/endpoints";

const KEY = "rquant-data-center-command-v1";
const KINDS = new Set([
  "prepare_backfill_execution",
  "execute_backfill_plan",
  "prepare_financial_collection",
  "execute_financial_collection",
  "pause_data_center_execution",
  "resume_data_center_execution",
]);

interface SavedCommand {
  owner: string;
  generation: string;
  body: DataCenterCommand;
}

interface CommandSnapshot {
  body: DataCenterCommand | null;
  receipt: DataCenterCommandReceipt | null;
  busy: boolean;
  uncertain: boolean;
  storageAvailable: boolean;
  message: string | null;
}

function savedCommand(value: unknown): value is SavedCommand {
  if (
    typeof value !== "object" ||
    value === null ||
    !("body" in value) ||
    !("owner" in value) ||
    !("generation" in value)
  )
    return false;
  const body = value.body;
  return (
    typeof value.owner === "string" &&
    typeof value.generation === "string" &&
    typeof body === "object" &&
    body !== null &&
    "kind" in body &&
    typeof body.kind === "string" &&
    KINDS.has(body.kind) &&
    "command_id" in body &&
    typeof body.command_id === "string" &&
    /^[A-Za-z0-9-]{1,128}$/.test(body.command_id) &&
    "requested_at" in body &&
    typeof body.requested_at === "string" &&
    Number.isFinite(Date.parse(body.requested_at))
  );
}

function receiptMatches(body: DataCenterCommand, receipt: DataCenterCommandReceipt): boolean {
  if (receipt.command_id !== body.command_id) return false;
  if (["pending", "processing", "ambiguous", "failed"].includes(receipt.status))
    return !receipt.confirmation && !receipt.execution && !receipt.execution_id;
  if (receipt.status === "prepared") {
    const value = receipt.confirmation;
    if (
      !value ||
      value.prepare_command_id !== body.command_id ||
      receipt.execution ||
      receipt.execution_id
    )
      return false;
    if (
      !/^[0-9a-f]{64}$/.test(value.execution_id) ||
      !/^[0-9a-f]{64}$/.test(value.intent_id) ||
      !/^[0-9a-f]{64}$/.test(value.plan_hash) ||
      !Number.isFinite(Date.parse(value.expires_at))
    )
      return false;
    if (body.kind === "prepare_backfill_execution")
      return (
        value.kind === "backfill" &&
        value.plan_task_id === body.plan_task_id &&
        value.plan_hash === body.plan_hash &&
        /^[0-9a-f]{64}$/.test(value.exact_dates_sha256 ?? "") &&
        Number.isInteger(value.missing_date_count) &&
        (value.missing_date_count ?? 0) > 0
      );
    if (body.kind === "prepare_financial_collection")
      return (
        value.kind === "financial" &&
        value.start_date === body.start_date &&
        value.end_date === body.end_date &&
        JSON.stringify(value.report_periods) === JSON.stringify(body.report_periods) &&
        Number.isInteger(value.security_count) &&
        (value.security_count ?? 0) > 0 &&
        Number.isInteger(value.query_count) &&
        (value.query_count ?? 0) > 0 &&
        (body.security_scope === "available_securities" ||
          value.security_count === body.selected_securities?.length)
      );
    return false;
  }
  if (receipt.status === "queued")
    return (
      (body.kind === "execute_backfill_plan" || body.kind === "execute_financial_collection") &&
      receipt.execution_id === body.execution_id &&
      !receipt.confirmation &&
      (!receipt.execution || receipt.execution.execution_id === body.execution_id)
    );
  return (
    receipt.status === "control_accepted" &&
    (body.kind === "pause_data_center_execution" || body.kind === "resume_data_center_execution") &&
    receipt.execution_id === body.execution_id &&
    !receipt.confirmation &&
    receipt.execution?.execution_id === body.execution_id &&
    receipt.execution.control_sequence === body.expected_sequence + 1
  );
}

/** Persist the exact request before dispatch. A missing response never creates another ID. */
export class DataCenterCommandSession {
  private saved: SavedCommand | null = null;
  private owner: string | null = null;
  private generation: string | null = null;
  private listeners = new Set<() => void>();
  private current: CommandSnapshot = {
    body: null,
    receipt: null,
    busy: false,
    uncertain: false,
    storageAvailable: true,
    message: null,
  };

  constructor(
    private readonly storage: Storage | null,
    private readonly post: (body: DataCenterCommand) => Promise<DataCenterCommandReceipt>,
  ) {
    try {
      if (!storage) throw new Error("storage unavailable");
      const raw = storage.getItem(KEY);
      if (raw !== null) {
        const parsed: unknown = JSON.parse(raw);
        if (raw.length > 4096 || !savedCommand(parsed)) throw new Error("invalid saved request");
        this.saved = parsed;
      }
    } catch {
      this.current = {
        ...this.current,
        storageAvailable: false,
        message: "浏览器存储不可用，无法安全提交。",
      };
    }
  }

  snapshot = (): CommandSnapshot => this.current;
  matchesContext(owner: string | null, generation: string | null): boolean {
    return Boolean(
      owner &&
        generation &&
        this.owner === owner &&
        (!this.current.body || this.saved?.owner === owner) &&
        (this.current.receipt?.status !== "prepared" || this.saved?.generation === generation),
    );
  }
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  private emit(change: Partial<CommandSnapshot>): void {
    this.current = { ...this.current, ...change };
    for (const listener of this.listeners) listener();
  }

  sync(owner: string | null, generation: string | null): void {
    if (this.owner === owner && this.generation === generation) return;
    const sameOwner = this.owner === owner;
    this.owner = owner;
    this.generation = generation;
    if (!owner || !generation || this.saved?.owner !== owner) {
      this.emit({
        body: null,
        receipt: null,
        uncertain: false,
        message: this.current.storageAvailable ? null : this.current.message,
      });
    } else if (!sameOwner || this.current.receipt === null) {
      this.emit({
        body: this.saved.body,
        receipt: null,
        uncertain: true,
        message: "上次请求状态待确认。",
      });
    } else if (this.current.receipt.status === "prepared" && this.saved.generation !== generation) {
      this.clear();
    }
  }

  clear(): void {
    if (this.current.busy || this.current.uncertain) return;
    try {
      this.storage?.removeItem(KEY);
      this.saved = null;
      this.emit({ body: null, receipt: null, message: null });
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法安全提交。" });
    }
  }

  async start(body: DataCenterCommand): Promise<void> {
    if (
      !this.owner ||
      !this.generation ||
      this.current.busy ||
      this.current.uncertain ||
      !this.current.storageAvailable ||
      !this.storage
    )
      return;
    const saved = { owner: this.owner, generation: this.generation, body };
    try {
      const raw = JSON.stringify(saved);
      if (raw.length > 4096) throw new Error("request capacity exceeded");
      this.storage.setItem(KEY, raw);
      if (this.storage.getItem(KEY) !== raw) throw new Error("request was not retained");
      this.saved = saved;
      this.emit({ body, receipt: null, uncertain: true, message: null });
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器存储不可用，无法安全提交。" });
      return;
    }
    await this.retry();
  }

  async retry(): Promise<void> {
    const saved = this.saved;
    if (
      !saved ||
      saved.owner !== this.owner ||
      !this.generation ||
      this.current.busy ||
      !this.current.storageAvailable
    )
      return;
    this.emit({ busy: true });
    try {
      const receipt = await this.post(saved.body);
      if (saved.owner !== this.owner) return;
      if (!receiptMatches(saved.body, receipt))
        throw new Error("receipt differs from the original request");
      if (receipt.status === "prepared" && saved.generation !== this.generation) {
        this.emit({ busy: false, uncertain: false, receipt: null });
        this.clear();
        this.emit({ message: "数据已更新，请重新确认范围。" });
        return;
      }
      const uncertain = ["pending", "processing", "ambiguous"].includes(receipt.status);
      this.emit({ receipt, uncertain, message: receipt.message });
    } catch {
      if (saved.owner === this.owner)
        this.emit({ uncertain: true, message: "提交状态待确认，请使用原请求重试。" });
    } finally {
      this.emit({ busy: false });
    }
  }
}

export function createDataCenterCommandSession(
  post: (body: DataCenterCommand) => Promise<DataCenterCommandReceipt>,
): DataCenterCommandSession {
  let storage: Storage | null = null;
  try {
    storage = window.sessionStorage;
  } catch {
    /* Submission remains disabled. */
  }
  return new DataCenterCommandSession(storage, post);
}
