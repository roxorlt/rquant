import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";
import { ApiError, apiClient, type Schemas } from "./client";
import { useCurrentMeta } from "./useMeta";

export type ConditionCommand = Schemas["ConditionAlertRuleCommandRequest"];
export type ConditionReceipt = Schemas["ConditionAlertRuleCommandReceipt"];
export type ConditionRule = Schemas["ConditionAlertRuleDefinition-Input"];
export type ConditionItem = Schemas["ConditionAlertRuleItem"];
export type ConditionList = Schemas["ConditionAlertRuleListData"];
export type ConditionTrigger = Schemas["ConditionAlertTriggerItem"];

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
const HASH = /^[0-9a-f]{64}$/;
const statuses = new Set<ConditionReceipt["status"]>([
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
]);
export function isConditionReceipt(
  value: unknown,
  body: ConditionCommand,
): value is ConditionReceipt {
  return (
    record(value) &&
    value.command_id === body.command_id &&
    value.rule_id === body.rule_id &&
    value.action === body.action &&
    typeof value.status === "string" &&
    Array.from(statuses).some((status) => status === value.status) &&
    typeof value.message === "string" &&
    (value.consumer_state === "unknown" || value.consumer_state === "active") &&
    (value.version == null ||
      (Number.isSafeInteger(value.version) &&
        typeof value.version === "number" &&
        value.version > 0)) &&
    (!["published", "saved_syncing"].includes(value.status) ||
      (typeof value.version === "number" && value.version === (body.expected_version ?? 0) + 1))
  );
}

function scope(value: unknown): value is ConditionRule["scope"] {
  if (!record(value)) return false;
  return value.kind === "market"
    ? value.universe_policy === "trusted_current"
    : value.kind === "watchlist"
      ? typeof value.membership_version === "string" && HASH.test(value.membership_version)
      : value.kind === "pool"
        ? typeof value.pool_name === "string" &&
          typeof value.definition_version === "string" &&
          HASH.test(value.definition_version) &&
          typeof value.result_version === "string" &&
          HASH.test(value.result_version)
        : value.kind === "sector" &&
          (value.sector_system === "industry" || value.sector_system === "concept") &&
          typeof value.sector_code === "string" &&
          typeof value.component_source_version === "string" &&
          HASH.test(value.component_source_version);
}
function rule(value: unknown): value is ConditionRule {
  if (
    !record(value) ||
    value.schema_version !== 1 ||
    typeof value.rule_id !== "string" ||
    typeof value.name !== "string" ||
    typeof value.enabled !== "boolean" ||
    !["P0", "P1", "P2", "P3"].includes(String(value.priority)) ||
    !scope(value.scope) ||
    !Array.isArray(value.conditions) ||
    value.conditions.length < 1 ||
    value.conditions.length > 26 ||
    !value.conditions.every(
      (call: unknown) => record(call) && typeof call.name === "string" && record(call.args),
    )
  )
    return false;
  if (
    !record(value.frequency) ||
    !(
      value.frequency.kind === "every_evaluation" ||
      (value.frequency.kind === "bar_close" && value.frequency.bar_size === "1min") ||
      (value.frequency.kind === "per_symbol_minutes" &&
        typeof value.frequency.minutes === "number" &&
        Number.isSafeInteger(value.frequency.minutes) &&
        value.frequency.minutes >= 1 &&
        value.frequency.minutes <= 60)
    )
  )
    return false;
  if (
    !record(value.governance) ||
    !Array.isArray(value.governance.channels) ||
    !value.governance.channels.length ||
    !value.governance.channels.every(
      (channel: unknown) => channel === "pushdeer" || channel === "pushplus",
    ) ||
    typeof value.governance.dedup_window_seconds !== "number" ||
    typeof value.governance.notify_recovery !== "boolean"
  )
    return false;
  if (
    !record(value.trading_hours) ||
    value.trading_hours.timezone !== "Asia/Shanghai" ||
    !Array.isArray(value.trading_hours.windows) ||
    !value.trading_hours.windows.every(
      (window: unknown) =>
        record(window) && typeof window.start === "string" && typeof window.end === "string",
    )
  )
    return false;
  if (
    !record(value.source_policy) ||
    value.source_policy.condition_semantics_version !== "screen-registry/v1" ||
    value.source_policy.daily_anchor !== "previous_closed_session" ||
    value.source_policy.intraday_contract_id !== "intraday-pit" ||
    ![3, 4].includes(Number(value.source_policy.minimum_intraday_contract_version))
  )
    return false;
  if (
    value.ranking != null &&
    (!record(value.ranking) ||
      typeof value.ranking.top_n !== "number" ||
      !Array.isArray(value.ranking.conditions) ||
      !value.ranking.conditions.every(
        (item: unknown) =>
          record(item) &&
          typeof item.metric === "string" &&
          typeof item.weight === "number" &&
          typeof item.ascending === "boolean",
      ))
  )
    return false;
  if (
    value.origin != null &&
    (!record(value.origin) ||
      typeof value.origin.execution_id !== "string" ||
      ![
        "command_hash",
        "definition_hash",
        "member_rank_digest",
        "result_digest",
        "source_identity",
      ].every(
        (key) =>
          typeof value.origin === "object" &&
          value.origin !== null &&
          record(value.origin) &&
          typeof value.origin[key] === "string" &&
          HASH.test(value.origin[key]),
      ) ||
      !["daily", "intraday"].includes(String(value.origin.mode)) ||
      typeof value.origin.trade_date !== "string" ||
      !(value.origin.draft_id == null || typeof value.origin.draft_id === "string") ||
      !(value.origin.cutoff == null || typeof value.origin.cutoff === "string"))
  )
    return false;
  return true;
}
export function readConditionCommand(raw: string): ConditionCommand | null {
  if (raw.length > 64 * 1024) return null;
  const value: unknown = JSON.parse(raw);
  if (
    !record(value) ||
    typeof value.command_id !== "string" ||
    value.command_id.length > 128 ||
    typeof value.rule_id !== "string" ||
    typeof value.requested_at !== "string" ||
    !Number.isFinite(Date.parse(value.requested_at)) ||
    typeof value.generation_id !== "string" ||
    !HASH.test(value.generation_id) ||
    !(
      value.expected_version == null ||
      (typeof value.expected_version === "number" &&
        Number.isSafeInteger(value.expected_version) &&
        value.expected_version > 0)
    )
  )
    return null;
  const expected_version =
    typeof value.expected_version === "number" ? value.expected_version : null;
  const allowed = new Set([
    "action",
    "command_id",
    "rule_id",
    "generation_id",
    "requested_at",
    "expected_version",
    ...(value.action === "save" ? ["rule"] : value.action === "set_enabled" ? ["enabled"] : []),
  ]);
  if (Object.keys(value).some((key) => !allowed.has(key))) return null;
  const base = {
    command_id: value.command_id,
    rule_id: value.rule_id,
    generation_id: value.generation_id,
    requested_at: value.requested_at,
    expected_version,
  };
  if (value.action === "save" && rule(value.rule) && value.rule.rule_id === value.rule_id)
    return { ...base, action: "save", rule: value.rule };
  if (
    value.action === "set_enabled" &&
    typeof value.enabled === "boolean" &&
    expected_version !== null
  )
    return { ...base, action: "set_enabled", enabled: value.enabled };
  if (value.action === "delete" && expected_version !== null) return { ...base, action: "delete" };
  return null;
}

export async function postConditionRule(
  body: ConditionCommand,
  resume: boolean,
): Promise<ConditionReceipt> {
  const path = resume
    ? "/api/v1/monitor/condition-rules/commands/resume"
    : "/api/v1/monitor/condition-rules/commands";
  const { data, error, response } = await apiClient().POST(path, {
    body,
    headers: { "X-Rquant-Csrf": "1" },
    signal: AbortSignal.timeout(12_000),
  });
  const result: unknown = data ?? error;
  if (!isConditionReceipt(result, body))
    throw new ApiError(response.status, "状态待核对，请继续核对原操作。");
  return result;
}
export function useConditionRules() {
  const meta = useCurrentMeta();
  useEffect(() => {
    void meta.refetch();
  }, [meta.refetch]);
  const owner = meta.isFetchedAfterMount && !meta.isError ? (meta.data?.data.viewer ?? null) : null;
  const generation =
    meta.isFetchedAfterMount && !meta.isError && meta.data?.serving.state === "ready"
      ? (meta.data.data.generation?.generation_id ?? null)
      : null;
  const query = useQuery<Schemas["Envelope_ConditionAlertRuleListData_"]>({
    queryKey: ["private-condition-rules", owner, generation],
    enabled: owner !== null && generation !== null,
    staleTime: 0,
    queryFn: async () => {
      const { data, response } = await apiClient().GET("/api/v1/monitor/condition-rules");
      if (data === undefined) throw new ApiError(response.status, "条件规则暂不可用，请稍后重试。");
      return data;
    },
  });
  const data =
    !query.isError &&
    query.data?.serving.state === "ready" &&
    query.data.serving.generation_id === generation
      ? query.data.data
      : null;
  return {
    owner,
    generation,
    data,
    serverTime: meta.data?.data.server_time ?? null,
    loading:
      (!meta.isFetchedAfterMount && !meta.isError) ||
      (owner !== null && generation !== null && query.isPending),
    refresh: () => {
      void meta.refetch();
      void query.refetch();
    },
  };
}
