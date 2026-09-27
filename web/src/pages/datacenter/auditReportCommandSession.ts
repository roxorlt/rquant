import { ApiError, type Schemas } from "@/api/client";
import { formatShanghaiTime, shanghaiDate } from "@/format/time";

export type AuditReportCommand = Schemas["AuditReportCommandRequest"];
type Receipt = Schemas["AuditReportCommandReceipt"];
type CommandStatus = Receipt["status"] | "unknown";

export interface AuditReportJournal {
  schema: 1;
  body: AuditReportCommand;
  status: CommandStatus;
  taskId: string | null;
}

export interface AuditReportCommandSnapshot {
  journal: AuditReportJournal | null;
  busy: boolean;
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const AUDIT_REPORT_JOURNAL_KEY = "rquant.audit-report-command.v1";
const COMMAND_ID = /^[A-Za-z0-9._-]{1,128}$/;
const TASK_ID = /^[0-9a-f]{32}$/;
const REQUESTED_AT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;
const STATUSES = new Set<CommandStatus>([
  "queued",
  "pending",
  "processing",
  "failed",
  "ambiguous",
  "unknown",
]);

function dateMillis(value: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const millis = Date.parse(`${value}T00:00:00Z`);
  return Number.isFinite(millis) && new Date(millis).toISOString().slice(0, 10) === value
    ? millis
    : null;
}

export function latestClosedAuditDate(now: Date): string {
  const today = shanghaiDate(now).date;
  if (formatShanghaiTime(now) >= "15:00") return today;
  const midnight = dateMillis(today);
  return new Date((midnight ?? 0) - 86_400_000).toISOString().slice(0, 10);
}

export function validateAuditReportRange(start: string, end: string, now: Date): string | null {
  const first = dateMillis(start);
  const last = dateMillis(end);
  if (first === null || last === null) return "请选择有效日期。";
  const days = (last - first) / 86_400_000 + 1;
  if (days < 1) return "结束日期不能早于开始日期。";
  if (days > 3660) return "一次最多核对 3660 天。";
  if (end > latestClosedAuditDate(now)) return "结束日期须在收盘之后。";
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJournal(value: unknown): value is AuditReportJournal {
  if (!isRecord(value) || value.schema !== 1 || !isRecord(value.body)) return false;
  const { body } = value;
  return (
    Object.keys(body).length === 4 &&
    typeof body.command_id === "string" &&
    COMMAND_ID.test(body.command_id) &&
    typeof body.requested_at === "string" &&
    REQUESTED_AT.test(body.requested_at) &&
    Number.isFinite(Date.parse(body.requested_at)) &&
    typeof body.audit_start === "string" &&
    dateMillis(body.audit_start) !== null &&
    typeof body.observed_through === "string" &&
    dateMillis(body.observed_through) !== null &&
    validateAuditReportRange(
      body.audit_start,
      body.observed_through,
      new Date(body.requested_at),
    ) === null &&
    STATUSES.has(value.status as CommandStatus) &&
    ((value.status === "queued" &&
      typeof value.taskId === "string" &&
      TASK_ID.test(value.taskId)) ||
      (value.status !== "queued" && value.taskId === null))
  );
}

/** Keeps one immutable browser request until the submit result is certain. */
export class AuditReportCommandSession {
  private current: AuditReportCommandSnapshot;
  private listeners = new Set<() => void>();

  constructor(
    private readonly storage: Storage,
    private readonly post: (body: AuditReportCommand) => Promise<Receipt>,
    private readonly nextId: () => string,
    private readonly now: () => string,
  ) {
    let journal: AuditReportJournal | null = null;
    let storageAvailable = true;
    let message: string | null = null;
    try {
      const saved = storage.getItem(AUDIT_REPORT_JOURNAL_KEY);
      if (saved !== null) {
        const parsed: unknown = JSON.parse(saved);
        if (!isJournal(parsed)) throw new Error("invalid request journal");
        journal = parsed;
      }
    } catch {
      storageAvailable = false;
      message = "浏览器记录无法核对，请勿重复提交。";
    }
    this.current = { journal, busy: false, storageAvailable, message, revision: 0 };
  }

  snapshot = (): AuditReportCommandSnapshot => this.current;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private emit(changes: Partial<AuditReportCommandSnapshot>): void {
    this.current = { ...this.current, ...changes, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private persist(journal: AuditReportJournal): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      const serialized = JSON.stringify(journal);
      this.storage.setItem(AUDIT_REPORT_JOURNAL_KEY, serialized);
      if (this.storage.getItem(AUDIT_REPORT_JOURNAL_KEY) !== serialized)
        throw new Error("request not stored");
      this.emit({ journal, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录不可用，无法安全核对。" });
      return false;
    }
  }

  async start(start: string, end: string, at: Date): Promise<void> {
    if (!this.current.storageAvailable || this.current.busy) return;
    if (this.current.journal && !["queued", "failed"].includes(this.current.journal.status)) {
      this.emit({ message: "请先核对上一次请求。" });
      return;
    }
    const invalid = validateAuditReportRange(start, end, at);
    if (invalid) {
      this.emit({ message: invalid });
      return;
    }
    try {
      const body: AuditReportCommand = {
        command_id: this.nextId(),
        requested_at: this.now(),
        audit_start: start,
        observed_through: end,
      };
      if (
        !COMMAND_ID.test(body.command_id) ||
        body.command_id === this.current.journal?.body.command_id ||
        !REQUESTED_AT.test(body.requested_at) ||
        !Number.isFinite(Date.parse(body.requested_at))
      )
        throw new Error("invalid command identity");
      if (this.persist({ schema: 1, body, status: "pending", taskId: null })) await this.advance();
    } catch {
      this.emit({ message: "暂时无法创建请求，请重试。" });
    }
  }

  async advance(): Promise<void> {
    const journal = this.current.journal;
    if (
      !journal ||
      this.current.busy ||
      !this.current.storageAvailable ||
      ["queued", "failed"].includes(journal.status)
    )
      return;
    this.emit({ busy: true });
    try {
      const receipt = await this.post(journal.body);
      if (receipt.command_id !== journal.body.command_id)
        throw new Error("receipt identity mismatch");
      if (receipt.status === "queued") {
        if (typeof receipt.task_id !== "string" || !TASK_ID.test(receipt.task_id))
          throw new Error("task identity missing");
        this.persist({ ...journal, status: "queued", taskId: receipt.task_id });
      } else if (["pending", "processing", "failed", "ambiguous"].includes(receipt.status)) {
        if (receipt.task_id != null) throw new Error("unexpected task identity");
        this.persist({ ...journal, status: receipt.status, taskId: null });
        if (receipt.status === "failed")
          this.emit({ message: "请求未通过检查，请调整日期后重试。" });
        else if (receipt.status === "processing")
          this.emit({ message: "正在提交，稍后继续核对。" });
        else this.emit({ message: "提交状态待确认，请继续核对本次请求。" });
      } else {
        throw new Error("unrecognized receipt status");
      }
    } catch (error) {
      const rejected = error instanceof ApiError && [401, 403, 413, 422].includes(error.status);
      this.persist({ ...journal, status: rejected ? "failed" : "unknown", taskId: null });
      this.emit({
        message: rejected
          ? error.status === 401
            ? "请先登录，再重新发起。"
            : error.status === 403
              ? "当前账号无权发起。"
              : "请求未通过检查，请调整日期后重试。"
          : "提交状态待确认，请继续核对本次请求。",
      });
    } finally {
      this.emit({ busy: false });
    }
  }
}
