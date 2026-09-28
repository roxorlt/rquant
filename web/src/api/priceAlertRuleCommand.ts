import { ApiError, apiClient, type Schemas } from "./client";

type Save = Schemas["SavePriceAlertRuleRequest"];
type Toggle = Schemas["SetPriceAlertRuleEnabledRequest"];
type Delete = Schemas["DeletePriceAlertRuleRequest"];
export type PriceRuleCommandBody = Save | Toggle | Delete;
export type PriceRuleCommandDraft =
  | Omit<Save, "command_id" | "requested_at">
  | Omit<Toggle, "command_id" | "requested_at">
  | Omit<Delete, "command_id" | "requested_at">;
type Receipt = Schemas["PriceAlertRuleCommandReceipt"];
type RuleItem = Schemas["PriceAlertRuleItemData"];
type Reason = NonNullable<Receipt["reason"]>;

export interface PriceRuleCommandEntry {
  body: PriceRuleCommandBody;
  status: Receipt["status"] | "unknown";
  version: number | null;
  reason: Reason | null;
}

export interface PriceRuleCommandSnapshot {
  entries: Record<string, PriceRuleCommandEntry>;
  busyRuleIds: readonly string[];
  storageAvailable: boolean;
  message: string | null;
  revision: number;
}

export const PRICE_RULE_JOURNAL_KEY = "rquant.price-rule-command.v1";
const HASH = /^[0-9a-f]{64}$/;
const CODE = /^[0-9]{6}\.(?:SH|SZ|BJ)$/;
const COMMAND_ID = /^[A-Za-z0-9._-]{1,128}$/;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;
const STATUSES = new Set<PriceRuleCommandEntry["status"]>([
  "pending",
  "processing",
  "saved_syncing",
  "published",
  "conflict",
  "capacity",
  "failed",
  "uncertain",
  "unknown",
]);
const REASONS = new Set<Reason>([
  "command_conflict",
  "generation_changed",
  "version_conflict",
  "membership_changed",
  "capacity_exceeded",
]);
const NO_EFFECT = new Set<Reason>([
  "generation_changed",
  "version_conflict",
  "membership_changed",
  "capacity_exceeded",
]);

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function ruleId(body: PriceRuleCommandBody | PriceRuleCommandDraft): string {
  return body.kind === "save_price_alert_rule" ? body.rule.rule_id : body.rule_id;
}

function validBody(value: unknown, expectedRuleId: string): value is PriceRuleCommandBody {
  if (!object(value)) return false;
  if (
    typeof value.command_id !== "string" ||
    !COMMAND_ID.test(value.command_id) ||
    typeof value.requested_at !== "string" ||
    !UTC.test(value.requested_at) ||
    !Number.isFinite(Date.parse(value.requested_at)) ||
    typeof value.generation_id !== "string" ||
    !HASH.test(value.generation_id)
  )
    return false;
  if (value.kind === "save_price_alert_rule") {
    const rule = value.rule;
    return (
      object(rule) &&
      rule.rule_id === expectedRuleId &&
      typeof rule.name === "string" &&
      typeof rule.enabled === "boolean" &&
      ["P0", "P1", "P2", "P3"].includes(String(rule.priority)) &&
      (rule.comparison === "gte" || rule.comparison === "lte") &&
      (typeof rule.threshold === "string" || typeof rule.threshold === "number") &&
      typeof rule.valid_from === "string" &&
      typeof rule.valid_until === "string" &&
      typeof value.ts_code === "string" &&
      CODE.test(value.ts_code) &&
      Number.isInteger(value.membership_version) &&
      Number(value.membership_version) >= 1 &&
      (value.expected_version === null ||
        (Number.isInteger(value.expected_version) && Number(value.expected_version) >= 1))
    );
  }
  return (
    (value.kind === "set_price_alert_rule_enabled" || value.kind === "delete_price_alert_rule") &&
    value.rule_id === expectedRuleId &&
    Number.isInteger(value.expected_version) &&
    Number(value.expected_version) >= 1 &&
    (value.kind === "delete_price_alert_rule" || typeof value.enabled === "boolean")
  );
}

function validEntry(
  value: unknown,
  expectedRuleId: string,
): value is PriceRuleCommandEntry & { schema: 1 } {
  return (
    object(value) &&
    value.schema === 1 &&
    validBody(value.body, expectedRuleId) &&
    STATUSES.has(value.status as PriceRuleCommandEntry["status"]) &&
    (value.version === null || (Number.isInteger(value.version) && Number(value.version) >= 1)) &&
    (value.reason === null || REASONS.has(value.reason as Reason))
  );
}

function validReceipt(value: unknown, body: PriceRuleCommandBody): value is Receipt {
  if (!object(value) || value.command_id !== body.command_id || value.kind !== body.kind)
    return false;
  if (value.rule_id !== ruleId(body) || !STATUSES.has(value.status as Receipt["status"]))
    return false;
  if (value.status === "unknown" || typeof value.message !== "string") return false;
  const version = value.version ?? null;
  if (value.status === "saved_syncing" || value.status === "published") {
    if (version !== (body.expected_version ?? 0) + 1) return false;
  } else if (version !== null) return false;
  return value.reason == null || REASONS.has(value.reason as Reason);
}

/** A typed error receipt is useful only when it matches the saved original command. */
export async function submitPriceRuleCommand(body: PriceRuleCommandBody): Promise<Receipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/monitor/rules/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "规则状态待核对。");
    });
  const receipt: unknown = data ?? error;
  if (!validReceipt(receipt, body)) throw new ApiError(response.status, "规则状态待核对。");
  return receipt;
}

export function projectedRuleMatches(
  entry: PriceRuleCommandEntry,
  item: RuleItem | undefined,
  generationId: string | null,
  builtAt: string | null,
): boolean {
  const { body, version } = entry;
  if (
    item === undefined ||
    version === null ||
    !["saved_syncing", "published"].includes(entry.status) ||
    generationId === null ||
    generationId === body.generation_id ||
    builtAt === null ||
    !Number.isFinite(Date.parse(builtAt)) ||
    Date.parse(builtAt) <= Date.parse(body.requested_at) ||
    !Number.isFinite(Date.parse(item.updated_at)) ||
    Date.parse(item.updated_at) < Date.parse(body.requested_at) ||
    item.rule_id !== ruleId(body) ||
    item.version !== version
  )
    return false;
  if (body.kind === "delete_price_alert_rule") return item.deleted;
  if (item.deleted) return false;
  if (body.kind === "set_price_alert_rule_enabled") return item.enabled === body.enabled;
  const rule = body.rule;
  return (
    item.ts_code === body.ts_code &&
    item.membership_version === body.membership_version &&
    item.name === rule.name &&
    item.priority === rule.priority &&
    item.enabled === rule.enabled &&
    item.comparison === rule.comparison &&
    Number(item.threshold) === Number(rule.threshold) &&
    item.valid_from === rule.valid_from &&
    item.valid_until === rule.valid_until
  );
}

type VerifyResult = "ready" | "stale" | "unavailable";
export interface PriceRuleCommandOptions {
  storage: Storage | null;
  viewer: string | null;
  post: (body: PriceRuleCommandBody) => Promise<Receipt>;
  verifyNew: (draft: PriceRuleCommandDraft) => Promise<VerifyResult>;
  verifyOwner: () => Promise<boolean>;
  nextId: () => string;
  now: () => string;
  withLock: (name: string, task: () => Promise<void>) => Promise<void>;
}

export function priceRuleProvedNoEffect(entry: PriceRuleCommandEntry): boolean {
  return (
    (entry.status === "conflict" || entry.status === "capacity") &&
    entry.reason !== null &&
    NO_EFFECT.has(entry.reason)
  );
}

export class PriceRuleCommandSession {
  private readonly prefix: string;
  private records: Record<string, PriceRuleCommandEntry> = {};
  private listeners = new Set<() => void>();
  private current: PriceRuleCommandSnapshot;

  constructor(private readonly options: PriceRuleCommandOptions) {
    this.prefix = `${PRICE_RULE_JOURNAL_KEY}:${options.viewer ?? ""}:`;
    let storageAvailable = options.viewer !== null && options.storage !== null;
    if (storageAvailable) {
      try {
        this.records = this.readAll();
      } catch {
        storageAvailable = false;
      }
    }
    this.current = {
      entries: this.records,
      busyRuleIds: [],
      storageAvailable,
      message: storageAvailable ? null : "浏览器记录暂不可用，无法安全保存规则。",
      revision: 0,
    };
  }

  snapshot = (): PriceRuleCommandSnapshot => this.current;

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
      (event.storageArea !== null && event.storageArea !== this.options.storage) ||
      (event.key !== null && !event.key.startsWith(this.prefix))
    )
      return;
    this.refresh(true);
  };

  private emit(changes: Partial<PriceRuleCommandSnapshot>): void {
    this.current = { ...this.current, ...changes, revision: this.current.revision + 1 };
    for (const listener of this.listeners) listener();
  }

  private key(id: string): string {
    return `${this.prefix}${id}`;
  }

  private readAll(): Record<string, PriceRuleCommandEntry> {
    const records: Record<string, PriceRuleCommandEntry> = {};
    const storage = this.options.storage;
    if (!storage) return records;
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (!key?.startsWith(this.prefix)) continue;
      const id = key.slice(this.prefix.length);
      const saved = storage.getItem(key);
      if (!id || saved === null) throw new Error("invalid rule journal key");
      const parsed: unknown = JSON.parse(saved);
      if (!validEntry(parsed, id)) throw new Error("invalid rule journal");
      records[id] = parsed;
    }
    return records;
  }

  private refresh(announce = false): boolean {
    if (!this.current.storageAvailable) return false;
    try {
      const previous = new Set(Object.keys(this.records));
      this.records = this.readAll();
      const changedElsewhere =
        announce && Object.keys(this.records).some((id) => !previous.has(id));
      this.emit({
        entries: this.records,
        message: changedElsewhere
          ? "另一标签页已有规则操作，正在核对原请求。"
          : this.current.message,
      });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录无法核对，请勿重复操作。" });
      return false;
    }
  }

  private persist(entry: PriceRuleCommandEntry): boolean {
    const storage = this.options.storage;
    if (!storage || !this.current.storageAvailable) return false;
    const id = ruleId(entry.body);
    try {
      const previous = storage.getItem(this.key(id));
      if (previous !== null) {
        const parsed: unknown = JSON.parse(previous);
        if (!validEntry(parsed, id)) throw new Error("invalid existing rule request");
        if (
          parsed.body.command_id === entry.body.command_id &&
          JSON.stringify(parsed.body) !== JSON.stringify(entry.body)
        )
          throw new Error("command identity collision");
        if (
          parsed.body.command_id !== entry.body.command_id &&
          !priceRuleProvedNoEffect(parsed) &&
          parsed.status !== "failed"
        ) {
          this.refresh(true);
          return false;
        }
        if (
          parsed.body.command_id === entry.body.command_id &&
          (parsed.status === "published" || priceRuleProvedNoEffect(parsed)) &&
          entry.status === "unknown"
        ) {
          this.refresh(true);
          return false;
        }
      }
      const saved = JSON.stringify({ schema: 1, ...entry });
      storage.setItem(this.key(id), saved);
      if (storage.getItem(this.key(id)) !== saved) throw new Error("journal not durable");
      this.records = { ...this.records, [id]: entry };
      this.emit({ entries: this.records, message: null });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录不可用，无法安全核对。" });
      return false;
    }
  }

  private async locked(id: string, task: () => Promise<void>): Promise<void> {
    if (!this.current.storageAvailable || this.current.busyRuleIds.includes(id)) return;
    this.emit({ busyRuleIds: [...this.current.busyRuleIds, id] });
    try {
      await this.options.withLock(this.key(id), task);
    } catch {
      this.emit({ message: "暂时无法安全操作，请稍后重试。" });
    } finally {
      this.emit({ busyRuleIds: this.current.busyRuleIds.filter((value) => value !== id) });
    }
  }

  async start(draft: PriceRuleCommandDraft): Promise<void> {
    const id = ruleId(draft);
    await this.locked(id, async () => {
      if (!this.refresh()) return;
      const previous = this.records[id];
      if (
        previous &&
        ((!priceRuleProvedNoEffect(previous) && previous.status !== "failed") ||
          previous.body.generation_id === draft.generation_id)
      )
        return;
      const verification = await this.options.verifyNew(draft).catch(() => "unavailable" as const);
      if (verification !== "ready") {
        this.emit({
          message:
            verification === "stale"
              ? "规则或名单已更新，请刷新后重试。"
              : "规则暂不可用，请稍后重试。",
        });
        return;
      }
      if (!(await this.options.verifyOwner().catch(() => false))) {
        this.emit({ message: "登录身份已变化，请切回原账户核对。" });
        return;
      }
      const body: PriceRuleCommandBody = {
        ...draft,
        command_id: this.options.nextId(),
        requested_at: this.options.now(),
      } as PriceRuleCommandBody;
      if (!validBody(body, id)) {
        this.emit({ message: "暂时无法创建规则请求，请重试。" });
        return;
      }
      if (this.persist({ body, status: "pending", version: null, reason: null }))
        await this.send(body);
    });
  }

  private async send(body: PriceRuleCommandBody): Promise<void> {
    try {
      const receipt = await this.options.post(body);
      if (!validReceipt(receipt, body)) throw new Error("invalid rule receipt");
      this.persist({
        body,
        status: receipt.status,
        version: receipt.version ?? null,
        reason: receipt.reason ?? null,
      });
    } catch {
      this.persist({ body, status: "unknown", version: null, reason: null });
      this.emit({ message: "规则状态待核对，请继续核对原请求。" });
    }
  }

  async advance(id: string): Promise<void> {
    await this.locked(id, async () => {
      if (!this.refresh()) return;
      const entry = this.records[id];
      if (
        !entry ||
        entry.status === "published" ||
        entry.status === "failed" ||
        priceRuleProvedNoEffect(entry)
      )
        return;
      if (!(await this.options.verifyOwner().catch(() => false))) {
        this.emit({ message: "登录身份已变化，请切回原账户核对。" });
        return;
      }
      await this.send(entry.body);
    });
  }

  async resumePending(): Promise<void> {
    if (!this.refresh()) return;
    for (const id of Object.keys(this.records)) await this.advance(id);
  }

  resolveProjected(
    id: string,
    item: RuleItem | undefined,
    generationId: string | null,
    builtAt: string | null,
  ): boolean {
    const current = this.records[id];
    if (!current || !projectedRuleMatches(current, item, generationId, builtAt)) return false;
    if (!this.refresh()) return false;
    const entry = this.records[id];
    if (!entry || !projectedRuleMatches(entry, item, generationId, builtAt)) return false;
    try {
      this.options.storage?.removeItem(this.key(id));
      if (this.options.storage?.getItem(this.key(id)) !== null)
        throw new Error("journal not removed");
      const next = { ...this.records };
      delete next[id];
      this.records = next;
      this.emit({
        entries: next,
        message: entry.body.kind === "delete_price_alert_rule" ? "规则已删除。" : "规则已保存。",
      });
      return true;
    } catch {
      this.emit({ storageAvailable: false, message: "浏览器记录不可用，无法安全核对。" });
      return false;
    }
  }
}
