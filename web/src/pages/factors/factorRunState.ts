import type {
  FactorResultItem,
  FactorRunOperationResult,
  FactorRunParameters,
  FactorRunRequest,
} from "@/api/factors";

export const RUN_OPERATION_KEY = "rquant.factor.run-operation.v1";
const RUN_DRAFT_KEY = "rquant.factor.run-draft.v1";
export const ARCHIVE_COMMAND_KEY = "rquant.factor.archive-command.v1";
const SAVE_COMMAND_KEY = "rquant.factor.save-command.v1";

export type RunDraft = Omit<FactorRunParameters, "factor_id" | "expected_head">;
export type StoredRun = {
  viewer: string;
  request: FactorRunRequest;
  factorName: string;
  poolLabel: string;
  result: FactorRunOperationResult | null;
  denied: boolean;
};

const requestKeys = ["command_id", "requested_at", "serving_generation_id", "parameters"] as const;
const parameterKeys = [
  "factor_id",
  "expected_head",
  "selection",
  "start_date",
  "end_date",
  "holding_sessions",
  "group_count",
  "ic_method",
  "neutralization",
] as const satisfies readonly (keyof FactorRunParameters)[];
const digest = /^[a-f0-9]{64}$/;
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validDraft(value: unknown): value is RunDraft & Record<string, unknown> {
  return (
    object(value) &&
    ["all", "gem", "hs300", "zz1000"].includes(String(value.selection)) &&
    typeof value.start_date === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(value.start_date) &&
    typeof value.end_date === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(value.end_date) &&
    typeof value.holding_sessions === "number" &&
    [1, 5, 10, 20].includes(value.holding_sessions) &&
    typeof value.group_count === "number" &&
    [3, 5, 10].includes(value.group_count) &&
    (value.ic_method === "rank" || value.ic_method === "normal") &&
    value.neutralization === "none"
  );
}

export function validRunRequest(value: unknown): value is FactorRunRequest {
  if (
    !object(value) ||
    Object.keys(value).length !== requestKeys.length ||
    !requestKeys.every((key) => key in value) ||
    typeof value.command_id !== "string" ||
    !uuid.test(value.command_id) ||
    typeof value.requested_at !== "string" ||
    !Number.isFinite(Date.parse(value.requested_at)) ||
    !/(Z|[+-]\d\d:\d\d)$/.test(value.requested_at) ||
    typeof value.serving_generation_id !== "string" ||
    !digest.test(value.serving_generation_id) ||
    !object(value.parameters)
  )
    return false;
  const p = value.parameters;
  return (
    validDraft(p) &&
    Object.keys(p).length === parameterKeys.length &&
    parameterKeys.every((key) => key in p) &&
    typeof p.factor_id === "string" &&
    /^[a-z][a-z0-9_]{0,63}$/.test(p.factor_id) &&
    object(p.expected_head) &&
    Object.keys(p.expected_head).length === 2 &&
    Number.isInteger(p.expected_head.version) &&
    Number(p.expected_head.version) > 0 &&
    typeof p.expected_head.content_sha256 === "string" &&
    digest.test(p.expected_head.content_sha256)
  );
}

export function sameRunRequest(a: FactorRunRequest, b: FactorRunRequest): boolean {
  return (
    a.command_id === b.command_id &&
    a.requested_at === b.requested_at &&
    a.serving_generation_id === b.serving_generation_id &&
    parameterKeys.every((key) =>
      key === "expected_head"
        ? a.parameters.expected_head.version === b.parameters.expected_head.version &&
          a.parameters.expected_head.content_sha256 === b.parameters.expected_head.content_sha256
        : a.parameters[key] === b.parameters[key],
    )
  );
}

export function validRunResult(
  value: unknown,
  request: FactorRunRequest,
): value is FactorRunOperationResult {
  return (
    object(value) &&
    validRunRequest(value.original_request) &&
    sameRunRequest(value.original_request, request) &&
    ["pending", "processing", "submitted", "uncertain", "rejected"].includes(
      String(value.status),
    ) &&
    (value.reason === null || typeof value.reason === "string") &&
    ((value.job_id === null && value.spec_sha256 === null && value.status !== "submitted") ||
      (typeof value.job_id === "string" &&
        /^[a-f0-9]{32}$/.test(value.job_id) &&
        typeof value.spec_sha256 === "string" &&
        digest.test(value.spec_sha256)))
  );
}

export function readRun(): StoredRun | null {
  try {
    const raw = localStorage.getItem(RUN_OPERATION_KEY);
    if (raw === null) return null;
    const value: unknown = JSON.parse(raw);
    if (
      !object(value) ||
      typeof value.viewer !== "string" ||
      !validRunRequest(value.request) ||
      typeof value.factorName !== "string" ||
      typeof value.poolLabel !== "string" ||
      typeof value.denied !== "boolean" ||
      (value.result !== null && !validRunResult(value.result, value.request))
    )
      return null;
    return value as StoredRun;
  } catch {
    return null;
  }
}

export function hasRun(): boolean {
  try {
    return localStorage.getItem(RUN_OPERATION_KEY) !== null;
  } catch {
    return true;
  }
}

export function hasDefinitionCommand(): boolean {
  try {
    return (
      localStorage.getItem(SAVE_COMMAND_KEY) !== null ||
      localStorage.getItem(ARCHIVE_COMMAND_KEY) !== null
    );
  } catch {
    return true;
  }
}

export function persistRun(value: StoredRun): boolean {
  try {
    const serialized = JSON.stringify(value);
    localStorage.setItem(RUN_OPERATION_KEY, serialized);
    return localStorage.getItem(RUN_OPERATION_KEY) === serialized;
  } catch {
    return false;
  }
}

export function clearRun(value: StoredRun): boolean {
  try {
    const current = readRun();
    if (
      current === null ||
      current.viewer !== value.viewer ||
      !sameRunRequest(current.request, value.request)
    )
      return false;
    localStorage.removeItem(RUN_OPERATION_KEY);
    return localStorage.getItem(RUN_OPERATION_KEY) === null;
  } catch {
    return false;
  }
}

export function readRunDraft(): RunDraft | null {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(RUN_DRAFT_KEY) ?? "null");
    return validDraft(value) ? value : null;
  } catch {
    return null;
  }
}

export function persistRunDraft(value: RunDraft): boolean {
  try {
    const serialized = JSON.stringify(value);
    localStorage.setItem(RUN_DRAFT_KEY, serialized);
    return localStorage.getItem(RUN_DRAFT_KEY) === serialized;
  } catch {
    return false;
  }
}

// All three commands claim the same durable slot while holding the browser lock.
export async function withFactorCommandLock(
  action: () => void | Promise<void>,
  requireLock = false,
): Promise<boolean> {
  if (navigator.locks === undefined) {
    if (requireLock) return false;
    await action();
    return true;
  }
  return navigator.locks.request("rquant.factor.command", { ifAvailable: true }, async (lock) => {
    if (lock === null) return false;
    await action();
    return true;
  });
}

export function matchesRunResult(
  item: FactorResultItem | null | undefined,
  operation: StoredRun,
): boolean {
  const result = operation.result;
  const p = operation.request.parameters;
  return (
    item !== null &&
    item !== undefined &&
    result?.job_id != null &&
    result.spec_sha256 != null &&
    item.job_id === result.job_id &&
    item.spec_sha256 === result.spec_sha256 &&
    item.factor_id === p.factor_id &&
    item.factor_version === p.expected_head.version &&
    item.definition_content_sha256 === p.expected_head.content_sha256
  );
}
