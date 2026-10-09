import type { Schemas } from "@/api/client";
import type { TaskControlRequest } from "@/api/taskControls";
import { settledTask, TaskControlMemory, type TaskPending } from "../tasks/taskControlRecovery";

export const monitorRecoveryKey = "rquant.monitor-controls.pending.v1";
const keys = [
  "mode",
  "pool2_levels",
  "pool_attack",
  "surge",
  "pulse",
  "rquant-notify-test.service",
];
const maxBytes = 32 * 1024;
const message = "操作结果待确认，请核验原请求。";
type RequestEntry = { key: string; body: TaskControlRequest };
type StoredRequests = {
  version: 1;
  actor: string;
  generation: string | null;
  entries: RequestEntry[];
};
type RequestStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

function sessionStorage(): RequestStorage | null {
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function fields(
  value: Record<string, unknown>,
  required: string[],
  optional: string[] = [],
): boolean {
  return (
    required.every((key) => Object.hasOwn(value, key)) &&
    Object.keys(value).every((key) => [...required, ...optional].includes(key))
  );
}

function bounded(value: unknown, length: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= length;
}

function common(value: Record<string, unknown>): boolean {
  return (
    typeof value.command_id === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value.command_id) &&
    bounded(value.generation_id, 128) &&
    bounded(value.requested_at, 64) &&
    /(?:Z|[+-]\d\d:\d\d)$/.test(value.requested_at) &&
    Number.isFinite(Date.parse(value.requested_at))
  );
}

function revision(value: unknown): boolean {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function validRequest(value: unknown, key: string): value is TaskControlRequest {
  if (
    !object(value) ||
    !keys.includes(key) ||
    !common(value) ||
    new TextEncoder().encode(JSON.stringify(value)).length > 4096
  )
    return false;
  const base = ["kind", "command_id", "requested_at", "generation_id"];
  if (value.kind === "prepare_notifier_delivery_mode" || value.kind === "prepare_unit_run") {
    const run = value.run;
    if (
      !fields(value, [...base, "run"]) ||
      !object(run) ||
      !common(run) ||
      run.command_id === value.command_id ||
      run.generation_id !== value.generation_id
    )
      return false;
    return value.kind === "prepare_unit_run"
      ? key === "rquant-notify-test.service" &&
          run.unit === key &&
          fields(run, ["command_id", "requested_at", "generation_id", "unit"])
      : key === "mode" &&
          ["shadow", "live"].includes(String(run.mode)) &&
          revision(run.expected_revision) &&
          fields(run, ["command_id", "requested_at", "generation_id", "mode", "expected_revision"]);
  }
  if (value.kind === "request_unit_run")
    return (
      key === "rquant-notify-test.service" &&
      value.unit === key &&
      fields(value, [...base, "unit"], ["confirmation_id"]) &&
      (value.confirmation_id == null || bounded(value.confirmation_id, 128))
    );
  if (value.kind === "set_notifier_delivery_mode")
    return (
      key === "mode" &&
      fields(value, [...base, "mode", "expected_revision", "confirmation_id"]) &&
      ["shadow", "live"].includes(String(value.mode)) &&
      revision(value.expected_revision) &&
      bounded(value.confirmation_id, 128)
    );
  return (
    value.kind === "set_monitor_builtin_enabled" &&
    key === value.builtin_id &&
    fields(value, [...base, "builtin_id", "expected_revision", "enabled"]) &&
    revision(value.expected_revision) &&
    typeof value.enabled === "boolean"
  );
}

function decode(raw: string): StoredRequests {
  if (new TextEncoder().encode(raw).length > maxBytes)
    throw new Error("request storage exceeds its budget");
  const value: unknown = JSON.parse(raw);
  if (
    !object(value) ||
    !fields(value, ["version", "actor", "generation", "entries"]) ||
    value.version !== 1 ||
    !bounded(value.actor, 128) ||
    !(value.generation === null || bounded(value.generation, 128)) ||
    !Array.isArray(value.entries) ||
    value.entries.length > keys.length
  )
    throw new Error("invalid request storage");
  const entries: RequestEntry[] = [];
  for (const entry of value.entries) {
    if (
      !object(entry) ||
      !fields(entry, ["key", "body"]) ||
      typeof entry.key !== "string" ||
      entries.some((row) => row.key === entry.key) ||
      !validRequest(entry.body, entry.key)
    )
      throw new Error("invalid stored original request");
    entries.push({ key: entry.key, body: entry.body });
  }
  return { version: 1, actor: value.actor, generation: value.generation, entries };
}

export class MonitorControlPersistenceError extends Error {
  override name = "MonitorControlPersistenceError";
}

/** Only original request bodies are durable. Responses and confirmations are rechecked. */
export class MonitorControlMemory extends TaskControlMemory {
  storageAvailable = false;
  private actor: string | null = null;
  private generation: string | null = null;
  private visible = false;
  private recordEpoch = 0;
  private recordListeners = new Set<() => void>();
  private durable: StoredRequests | null = null;
  subscribeRecords = (listener: () => void): (() => void) => {
    this.recordListeners.add(listener);
    return () => {
      this.recordListeners.delete(listener);
    };
  };
  recordsSnapshot = (): number => this.recordEpoch;

  constructor(private storage: RequestStorage | null = sessionStorage()) {
    super();
    try {
      if (storage === null) return;
      storage.setItem(`${monitorRecoveryKey}.probe`, "1");
      storage.removeItem(`${monitorRecoveryKey}.probe`);
      this.storageAvailable = true;
    } catch {
      this.storageAvailable = false;
    }
  }

  private changed(): void {
    this.recordEpoch += 1;
    for (const listener of this.recordListeners) listener();
  }

  suspend(): void {
    this.visible = false;
    super.activate(null);
    this.changed();
  }

  isCurrent(actor: string | null, generation: string | null): boolean {
    return this.visible && actor !== null && actor === this.actor && generation === this.generation;
  }

  confirmActor(actor: string | null, generation: string | null): void {
    if (actor === null) {
      this.clearStored();
      this.actor = null;
      this.generation = null;
      this.visible = false;
      super.activate(null);
      this.changed();
      return;
    }
    if (this.visible && actor === this.actor && generation === this.generation) return;
    const previous = this.actor === actor ? super.entries(actor) : [];
    try {
      const raw = this.storage?.getItem(monitorRecoveryKey);
      const stored = raw ? decode(raw) : null;
      if (stored?.actor !== actor && stored !== null) this.clearStored();
      else this.durable = stored;
      if (this.durable?.actor === actor) {
        // A known, unsubmitted preview may be cancelled on a generation change.
        const retired = previous
          .filter(
            ([, row]) =>
              row.result?.status === "prepared" &&
              row.body.generation_id !== generation &&
              ["prepare_unit_run", "prepare_notifier_delivery_mode"].includes(row.body.kind),
          )
          .map(([key]) => key);
        if (retired.length)
          this.persist({
            ...this.durable,
            generation,
            entries: this.durable.entries.filter((row) => !retired.includes(row.key)),
          });
      }
    } catch {
      this.storageAvailable = false;
      this.durable = null;
    }
    this.actor = actor;
    this.generation = generation;
    this.visible = true;
    super.activate(null);
    super.activate(actor);
    for (const entry of this.durable?.entries ?? [])
      super.put(actor, entry.key, {
        body: entry.body,
        result: null,
        message,
        ...(entry.key === "rquant-notify-test.service" ? { unitName: "测试推送" } : {}),
      });
    this.changed();
  }

  private persist(value: StoredRequests | null): void {
    const raw = value === null || value.entries.length === 0 ? null : JSON.stringify(value);
    if (raw !== null) decode(raw);
    if (JSON.stringify(this.durable) === JSON.stringify(value)) return;
    try {
      if (this.storage === null || !this.storageAvailable)
        throw new Error("request storage unavailable");
      if (raw === null) this.storage.removeItem(monitorRecoveryKey);
      else this.storage.setItem(monitorRecoveryKey, raw);
      this.durable = value;
    } catch {
      this.storageAvailable = false;
      this.changed();
      throw new MonitorControlPersistenceError("原请求无法保存，请刷新后重试。");
    }
  }

  private clearStored(): void {
    try {
      this.storage?.removeItem(monitorRecoveryKey);
    } catch {
      this.storageAvailable = false;
    }
    this.durable = null;
  }

  override clear(viewer: string): void {
    if (this.actor !== viewer) return;
    this.clearStored();
    super.clear(viewer);
    this.changed();
  }

  override put(viewer: string, key: string, record: TaskPending | null): void {
    if (!this.visible || viewer !== this.actor) return;
    if (record !== null && !validRequest(record.body, key))
      throw new MonitorControlPersistenceError("原请求无法保存，请刷新后重试。");
    const entries = (this.durable?.actor === viewer ? this.durable.entries : []).filter(
      (row) => row.key !== key,
    );
    if (record !== null) entries.push({ key, body: structuredClone(record.body) });
    entries.sort((left, right) => left.key.localeCompare(right.key));
    this.persist({ version: 1, actor: viewer, generation: this.generation, entries });
    super.put(viewer, key, record);
    this.changed();
  }
}

export function monitorApplication(
  pending: TaskPending,
  data: Schemas["MonitorRuntimeData"] | undefined,
  controls: Schemas["TaskControlCapabilitiesData"] | undefined,
): boolean {
  const { body, result } = pending;
  if (
    !result ||
    !["submitted", "succeeded"].includes(result.status) ||
    result.desired_revision !== ("expected_revision" in body ? body.expected_revision + 1 : null) ||
    !result.desired_installation_sha256 ||
    !data ||
    !controls
  )
    return false;
  const install = result.desired_installation_sha256;
  if (body.kind === "set_notifier_delivery_mode")
    return (
      data.state === "ready" &&
      data.mode === body.mode &&
      data.applied_revision === result.desired_revision &&
      data.applied_command_id === body.command_id &&
      data.monitor_installation_sha256 === install &&
      controls.notifier_mode.available &&
      controls.notifier_mode.mode === body.mode &&
      controls.notifier_mode.revision === result.desired_revision &&
      controls.notifier_mode.installation_sha256 === install
    );
  if (body.kind !== "set_monitor_builtin_enabled") return false;
  const actual = data.builtins?.find((row) => row.builtin_id === body.builtin_id);
  const current = controls.monitor_builtins.find((row) => row.builtin_id === body.builtin_id);
  return (
    actual?.enabled === body.enabled &&
    actual.applied_revision === result.desired_revision &&
    actual.applied_command_id === body.command_id &&
    actual.monitor_installation_sha256 === install &&
    current?.enabled === body.enabled &&
    current.revision === result.desired_revision &&
    current.installation_sha256 === install
  );
}

export function settledMonitorRequest(
  pending: TaskPending,
  data: Schemas["MonitorRuntimeData"] | undefined,
  controls: Schemas["TaskControlCapabilitiesData"] | undefined,
): boolean {
  if (pending.refused || ["rejected", "failed"].includes(pending.result?.status ?? "")) return true;
  return ["set_monitor_builtin_enabled", "set_notifier_delivery_mode"].includes(pending.body.kind)
    ? monitorApplication(pending, data, controls)
    : settledTask(pending);
}
