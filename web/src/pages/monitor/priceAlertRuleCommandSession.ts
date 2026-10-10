import type { Schemas } from "@/api/client";

type Command = Schemas["PriceAlertRuleCommandRequest"];
type Receipt = Schemas["PriceAlertRuleCommandReceipt"];
export const PRICE_RULE_JOURNAL = "rquant.price-rule-command.v1";
export const PRICE_RULE_EVENT = "rquant:price-rule-command";
const TERMINAL = new Set([
  "published",
  "superseded",
  "conflict",
  "capacity",
  "scope_invalid",
  "failed",
  "rejected",
]);
const STATUSES = new Set([
  "pending",
  "processing",
  "saved_syncing",
  "published",
  "superseded",
  "conflict",
  "capacity",
  "scope_invalid",
  "failed",
  "rejected",
  "uncertain",
  "not_found",
  "unknown",
]);
const HASH = /^[0-9a-f]{64}$/;
const CODE = /^[0-9]{6}\.(SH|SZ|BJ)$/;
const WALL_TIME = /^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,6})?)?$/;
const PROGRESS: Record<string, number> = {
  unknown: 0,
  uncertain: 0,
  not_found: 0,
  pending: 1,
  processing: 2,
  saved_syncing: 3,
};

export interface PriceRuleEntry {
  body: Command;
  status: Receipt["status"] | "unknown";
  version: number | null;
}
export interface PriceRuleSnapshot {
  entries: PriceRuleEntry[];
  busy: string[];
  storageAvailable: boolean;
  message: string | null;
}
export interface EditingIdentity {
  owner: string;
  sessionId: string;
  ruleId: string;
  readVersion: number | null;
}
export type PriceRuleLock = <T>(name: string, action: () => Promise<T>) => Promise<T>;

export function matchesEditingSession(
  origin: EditingIdentity,
  current: EditingIdentity | null,
): boolean {
  return (
    current !== null &&
    origin.owner === current.owner &&
    origin.sessionId === current.sessionId &&
    origin.ruleId === current.ruleId &&
    origin.readVersion === current.readVersion
  );
}
export function unresolved(entry: PriceRuleEntry): boolean {
  return !TERMINAL.has(entry.status);
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function positiveVersion(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) > 0;
}
function validBody(value: unknown): value is Command {
  if (
    !object(value) ||
    typeof value.command_id !== "string" ||
    !value.command_id.length ||
    value.command_id.length > 128 ||
    typeof value.rule_id !== "string" ||
    !value.rule_id.length ||
    value.rule_id.length > 128 ||
    typeof value.generation_id !== "string" ||
    !HASH.test(value.generation_id) ||
    typeof value.requested_at !== "string" ||
    !Number.isFinite(Date.parse(value.requested_at)) ||
    (value.expected_version != null && !positiveVersion(value.expected_version))
  )
    return false;
  const common = [
    "command_id",
    "rule_id",
    "generation_id",
    "requested_at",
    "expected_version",
    "action",
  ];
  if (value.action === "save") {
    const rule = value.rule;
    return (
      Object.keys(value).every((key) =>
        [...common, "ts_code", "membership_version", "rule"].includes(key),
      ) &&
      typeof value.ts_code === "string" &&
      CODE.test(value.ts_code) &&
      positiveVersion(value.membership_version) &&
      object(rule) &&
      Object.keys(rule).sort().join(",") ===
        "comparison,enabled,name,priority,threshold,valid_from,valid_until" &&
      typeof rule.name === "string" &&
      rule.name.trim().length > 0 &&
      rule.name.length <= 80 &&
      ["P0", "P1", "P2", "P3"].includes(String(rule.priority)) &&
      typeof rule.enabled === "boolean" &&
      ["gte", "lte"].includes(String(rule.comparison)) &&
      typeof rule.threshold === "string" &&
      rule.threshold.length > 0 &&
      rule.threshold.length <= 1024 &&
      typeof rule.valid_from === "string" &&
      WALL_TIME.test(rule.valid_from) &&
      typeof rule.valid_until === "string" &&
      WALL_TIME.test(rule.valid_until)
    );
  }
  if (!positiveVersion(value.expected_version)) return false;
  return value.action === "delete"
    ? Object.keys(value).every((key) => common.includes(key))
    : value.action === "set_enabled" &&
        typeof value.enabled === "boolean" &&
        Object.keys(value).every((key) => [...common, "enabled"].includes(key));
}

export function validPriceRuleReceipt(value: unknown, body: Command): value is Receipt {
  if (
    !object(value) ||
    value.command_id !== body.command_id ||
    value.rule_id !== body.rule_id ||
    value.action !== body.action ||
    typeof value.status !== "string" ||
    value.status === "unknown" ||
    !STATUSES.has(value.status) ||
    typeof value.message !== "string" ||
    value.message.length > 512 ||
    (value.version != null && !positiveVersion(value.version))
  )
    return false;
  return (
    !["saved_syncing", "published", "superseded"].includes(value.status) ||
    value.version === (body.expected_version ?? 0) + 1
  );
}

export class PriceAlertRuleCommandSession {
  private state: PriceRuleSnapshot = {
    entries: [],
    busy: [],
    storageAvailable: true,
    message: null,
  };
  private listeners = new Set<() => void>();
  private active = new Set<string>();
  private live = true;
  constructor(
    private storage: Storage | null,
    private owner: string | null,
    private post: (body: Command, resume: boolean) => Promise<Receipt>,
    private lock: PriceRuleLock | null,
  ) {
    this.refresh();
  }
  snapshot = (): PriceRuleSnapshot => this.state;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  setActive(value: boolean): void {
    this.live = value;
  }
  private prefix(): string {
    return `${PRICE_RULE_JOURNAL}:${encodeURIComponent(this.owner ?? "")}:`;
  }
  private key(id: string): string {
    return `${this.prefix()}${encodeURIComponent(id)}`;
  }
  private read(): PriceRuleEntry[] {
    if (this.storage === null) throw new Error("storage unavailable");
    if (this.owner === null) return [];
    const entries: PriceRuleEntry[] = [];
    for (let index = 0; index < this.storage.length; index++) {
      const key = this.storage.key(index);
      if (!key?.startsWith(this.prefix())) continue;
      const raw = this.storage.getItem(key);
      if (raw === null || raw.length > 16_384) throw new Error("invalid record");
      const value: unknown = JSON.parse(raw);
      if (
        !object(value) ||
        Object.keys(value).sort().join(",") !== "body,status,version" ||
        !validBody(value.body) ||
        key !== this.key(value.body.command_id) ||
        typeof value.status !== "string" ||
        !STATUSES.has(value.status) ||
        (value.version !== null && !positiveVersion(value.version))
      )
        throw new Error("invalid record");
      if (
        ["saved_syncing", "published", "superseded"].includes(value.status) &&
        value.version !== (value.body.expected_version ?? 0) + 1
      )
        throw new Error("invalid version");
      entries.push(value as unknown as PriceRuleEntry);
    }
    return entries;
  }
  private update(entries: PriceRuleEntry[], message: string | null = null, available = true): void {
    this.state = { entries, busy: [...this.active], storageAvailable: available, message };
    this.listeners.forEach((listener) => {
      listener();
    });
  }
  refresh = (): void => {
    try {
      this.update(this.read(), null, this.storage !== null);
    } catch {
      this.update([], "浏览器记录无法读取，请恢复记录后再保存。", false);
    }
  };
  private write(entry: PriceRuleEntry): boolean {
    try {
      if (this.storage === null || this.owner === null) throw new Error("storage unavailable");
      const raw = JSON.stringify(entry);
      const prior = this.read().find((value) => value.body.command_id === entry.body.command_id);
      if (prior && (PROGRESS[prior.status] ?? 4) > (PROGRESS[entry.status] ?? 4)) {
        this.update(this.read());
        return true;
      }
      this.storage.setItem(this.key(entry.body.command_id), raw);
      if (this.storage.getItem(this.key(entry.body.command_id)) !== raw)
        throw new Error("record changed");
      this.update(this.read());
      window.dispatchEvent(new Event(PRICE_RULE_EVENT));
      return true;
    } catch {
      this.update(this.state.entries, "浏览器记录无法保存，请恢复记录后再试。", false);
      return false;
    }
  }
  async start(body: Command): Promise<boolean> {
    if (!this.live || this.owner === null || this.lock === null || !validBody(body)) {
      this.update(
        this.state.entries,
        "浏览器暂不支持安全保存，请刷新或换浏览器。",
        this.state.storageAvailable,
      );
      return false;
    }
    if (new TextEncoder().encode(JSON.stringify(body)).length > 8192) return false;
    const frozen = structuredClone(body);
    let created = false;
    try {
      await this.lock(`${PRICE_RULE_JOURNAL}:${this.owner}`, async () => {
        const entries = this.read();
        const pending = entries.filter(unresolved);
        if (
          entries.some((entry) => entry.body.command_id === frozen.command_id) ||
          pending.some((entry) => entry.body.rule_id === frozen.rule_id) ||
          pending.length >= 100
        ) {
          this.update(entries, "请先核对未完成的操作。");
          return;
        }
        for (const old of entries) {
          if (!unresolved(old)) this.storage?.removeItem(this.key(old.body.command_id));
        }
        created = this.write({ body: frozen, status: "unknown", version: null });
      });
    } catch {
      this.update(this.state.entries, "浏览器记录无法保存，请恢复记录后再试。", false);
    }
    if (created) await this.send(frozen.command_id, false, frozen);
    return created;
  }
  private async send(id: string, resume: boolean, frozen?: Command): Promise<void> {
    if (!this.live || this.owner === null || this.active.has(id)) return;
    let original: PriceRuleEntry | undefined;
    try {
      original = this.read().find((entry) => entry.body.command_id === id);
    } catch {
      this.refresh();
      return;
    }
    if (original === undefined || !unresolved(original)) return;
    if (frozen !== undefined && JSON.stringify(original.body) !== JSON.stringify(frozen)) {
      this.update(this.state.entries, "浏览器记录已变化，请恢复原记录后再保存。", false);
      return;
    }
    this.active.add(id);
    this.update(this.state.entries);
    let next: PriceRuleEntry = original;
    try {
      const receipt = await this.post(structuredClone(original.body), resume);
      if (validPriceRuleReceipt(receipt, original.body))
        next = { body: original.body, status: receipt.status, version: receipt.version ?? null };
    } catch {
      /* Preserve the durable original for lookup-only recovery. */
    }
    this.active.delete(id);
    this.write(next);
  }
  async advance(id: string): Promise<void> {
    await this.send(id, true);
  }
  async resumePending(): Promise<void> {
    for (const entry of this.snapshot().entries.filter(unresolved)) {
      if (!this.live) break;
      await this.advance(entry.body.command_id);
    }
  }
}
