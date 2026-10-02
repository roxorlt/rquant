import type {
  FactorTrackingOperationResult,
  FactorTrackingPanel,
  FactorTrackingReceipt,
  FactorTrackingRequest,
  FactorTrackingSummary,
} from "@/api/factors";
import { TRACKING_OPERATION_KEY } from "./factorRunState";

export type StoredTracking = {
  viewer: string;
  factorName: string;
  request: FactorTrackingRequest;
  result: FactorTrackingOperationResult | null;
  denied: boolean;
};

const digest = /^[a-f0-9]{64}$/;
const generation = /^[a-f0-9]{32}$/;
const requestKeys = [
  "command_id",
  "requested_at",
  "serving_generation_id",
  "factor_id",
  "tracked",
  "expected_head",
  "expected_tracking_generation",
] as const satisfies readonly (keyof FactorTrackingRequest)[];

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function head(value: unknown): value is FactorTrackingRequest["expected_head"] {
  return (
    object(value) &&
    Object.keys(value).length === 2 &&
    Number.isInteger(value.version) &&
    Number(value.version) > 0 &&
    typeof value.content_sha256 === "string" &&
    digest.test(value.content_sha256)
  );
}
export function sameTrackingHead(
  a: FactorTrackingRequest["expected_head"] | null | undefined,
  b: FactorTrackingRequest["expected_head"] | null | undefined,
): boolean {
  return a != null && b != null && a.version === b.version && a.content_sha256 === b.content_sha256;
}
function utc(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)$/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}
function date(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^\d{4}-\d\d-\d\d$/.test(value) &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString().slice(0, 10) === value
  );
}
function nullableText(value: unknown, limit = 120): boolean {
  return value == null || (typeof value === "string" && value.length <= limit);
}

export function validTrackingRequest(value: unknown): value is FactorTrackingRequest {
  return (
    object(value) &&
    Object.keys(value).length === requestKeys.length &&
    requestKeys.every((key) => key in value) &&
    typeof value.command_id === "string" &&
    /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value.command_id) &&
    utc(value.requested_at) &&
    typeof value.serving_generation_id === "string" &&
    digest.test(value.serving_generation_id) &&
    typeof value.factor_id === "string" &&
    /^[a-z][a-z0-9_]{0,63}$/.test(value.factor_id) &&
    typeof value.tracked === "boolean" &&
    head(value.expected_head) &&
    (value.expected_tracking_generation === null ||
      (typeof value.expected_tracking_generation === "string" &&
        generation.test(value.expected_tracking_generation)))
  );
}

export function sameTrackingRequest(a: FactorTrackingRequest, b: FactorTrackingRequest): boolean {
  return requestKeys.every((key) =>
    key === "expected_head"
      ? sameTrackingHead(a.expected_head, b.expected_head)
      : a[key] === b[key],
  );
}

function receipt(value: unknown, request: FactorTrackingRequest): value is FactorTrackingReceipt {
  return (
    object(value) &&
    value.command_id === request.command_id &&
    value.factor_id === request.factor_id &&
    value.tracked === request.tracked &&
    typeof value.tracking_generation === "string" &&
    generation.test(value.tracking_generation) &&
    head(value.definition_head) &&
    sameTrackingHead(value.definition_head, request.expected_head) &&
    (request.tracked ? value.segment_id === value.tracking_generation : value.segment_id == null)
  );
}
export function sameTrackingReceipt(a: FactorTrackingReceipt, b: FactorTrackingReceipt): boolean {
  return (
    a.command_id === b.command_id &&
    a.factor_id === b.factor_id &&
    a.tracked === b.tracked &&
    a.tracking_generation === b.tracking_generation &&
    a.segment_id === b.segment_id &&
    sameTrackingHead(a.definition_head, b.definition_head)
  );
}
export function validTrackingResult(
  value: unknown,
  request: FactorTrackingRequest,
): value is FactorTrackingOperationResult {
  return (
    object(value) &&
    validTrackingRequest(value.original_request) &&
    sameTrackingRequest(value.original_request, request) &&
    typeof value.status === "string" &&
    ["applied", "pending", "uncertain", "rejected"].includes(value.status) &&
    nullableText(value.reason) &&
    (value.status === "applied" ? receipt(value.receipt, request) : value.receipt == null)
  );
}

const icNumbers = [
  "mean",
  "sample_std",
  "ir",
  "positive_rate",
  "strong_signal_rate",
  "t_value",
  "p_value",
  "skewness",
  "excess_kurtosis",
] as const satisfies readonly (keyof FactorTrackingSummary["ic_20"])[];
const icCounts = [
  "source_day_count",
  "valid_day_count",
  "insufficient_day_count",
  "zero_variance_day_count",
] as const satisfies readonly (keyof FactorTrackingSummary["ic_20"])[];
const summaryNumbers = [
  "yesterday_ic",
  "yesterday_long_short",
  "week_long_short",
  "cumulative_long_short",
] as const satisfies readonly (keyof FactorTrackingSummary)[];
function finiteOrNull(value: unknown): boolean {
  return value === null || (typeof value === "number" && Number.isFinite(value));
}
function count(value: unknown, max: number): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= max;
}
function summary(value: unknown): value is FactorTrackingSummary {
  if (!object(value) || !object(value.ic_20)) return false;
  const ic = value.ic_20;
  return (
    (value.latest_trade_date === null || date(value.latest_trade_date)) &&
    summaryNumbers.every((key) => finiteOrNull(value[key])) &&
    typeof value.invalidated === "boolean" &&
    nullableText(value.reason) &&
    count(value.complete_day_count, 20) &&
    count(value.week_day_count, 5) &&
    count(value.week_complete_day_count, 5) &&
    value.week_complete_day_count <= value.week_day_count &&
    typeof ic.status === "string" &&
    ["ok", "no_valid_days", "insufficient_samples", "zero_variance", "precision_limit"].includes(
      ic.status,
    ) &&
    icNumbers.every((key) => finiteOrNull(ic[key])) &&
    icCounts.every((key) => count(ic[key], 20)) &&
    Number(ic.valid_day_count) <= Number(ic.source_day_count) &&
    value.complete_day_count <= Number(ic.source_day_count)
  );
}

export function validTrackingPanel(value: unknown, factorId: string): value is FactorTrackingPanel {
  if (
    !object(value) ||
    value.factor_id !== factorId ||
    typeof value.tracked !== "boolean" ||
    typeof value.can_set_tracked !== "boolean" ||
    value.policy_version !== 1 ||
    typeof value.policy_label !== "string" ||
    value.policy_label.length === 0 ||
    typeof value.basis_label !== "string" ||
    value.basis_label.length === 0 ||
    !nullableText(value.reason) ||
    !(value.updated_at == null || utc(value.updated_at)) ||
    !(value.actual_start_date == null || date(value.actual_start_date)) ||
    !(value.definition_head == null || head(value.definition_head)) ||
    !(
      value.tracking_generation == null ||
      (typeof value.tracking_generation === "string" && generation.test(value.tracking_generation))
    ) ||
    !(value.summary == null || summary(value.summary))
  )
    return false;
  if (value.availability === "unavailable")
    return (
      value.status === "unavailable" &&
      !value.tracked &&
      !value.can_set_tracked &&
      value.summary == null
    );
  if (value.availability === "not_tracked")
    return (
      value.status === "not_tracked" &&
      !value.tracked &&
      head(value.definition_head) &&
      value.summary == null
    );
  return (
    value.availability === "tracked" &&
    value.tracked &&
    typeof value.status === "string" &&
    ["waiting", "active", "paused"].includes(value.status) &&
    head(value.definition_head) &&
    typeof value.tracking_generation === "string"
  );
}

export function matchesTrackingPanel(
  panel: FactorTrackingPanel,
  operation: StoredTracking,
): boolean {
  const r = operation.result?.receipt;
  return (
    operation.result?.status === "applied" &&
    r != null &&
    panel.availability !== "unavailable" &&
    panel.factor_id === r.factor_id &&
    panel.tracked === r.tracked &&
    panel.tracking_generation === r.tracking_generation &&
    sameTrackingHead(panel.definition_head, r.definition_head)
  );
}

export function readTracking(): StoredTracking | null {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(TRACKING_OPERATION_KEY) ?? "null");
    if (
      !object(value) ||
      typeof value.viewer !== "string" ||
      value.viewer.length === 0 ||
      typeof value.factorName !== "string" ||
      typeof value.denied !== "boolean" ||
      !validTrackingRequest(value.request) ||
      (value.result !== null && !validTrackingResult(value.result, value.request))
    )
      return null;
    return value as StoredTracking;
  } catch {
    return null;
  }
}
export function persistTracking(value: StoredTracking): boolean {
  try {
    const raw = JSON.stringify(value);
    localStorage.setItem(TRACKING_OPERATION_KEY, raw);
    return localStorage.getItem(TRACKING_OPERATION_KEY) === raw;
  } catch {
    return false;
  }
}
export function clearTracking(value: StoredTracking): boolean {
  try {
    const current = readTracking();
    if (current?.viewer !== value.viewer || !sameTrackingRequest(current.request, value.request))
      return false;
    localStorage.removeItem(TRACKING_OPERATION_KEY);
    return localStorage.getItem(TRACKING_OPERATION_KEY) === null;
  } catch {
    return false;
  }
}
