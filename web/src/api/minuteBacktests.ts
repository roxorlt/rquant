import { ApiError, apiBaseUrl, apiClient, type Schemas } from "./client";
import { useCurrentMeta } from "./useMeta";
import { useServingQuery } from "./useServingQuery";

export type MinuteParameterSource = Schemas["MinuteParameterFactSourceOption"];
export type MinuteParameters = Schemas["MinuteParameterSet"];
export type MinuteSource = Schemas["MinuteSourceOption"];
export type MinuteJob = Schemas["MinuteJob"];
export type MinuteCreate = Schemas["MinuteCreateRequest"];
export type MinuteReceipt = Schemas["MinuteCommandReceipt"];
export type MinuteExport = Schemas["MinuteExportRequest"];
export type MinuteNav = Schemas["MinuteNavData"];
export type MinuteRows = Schemas["MinuteRowsData"];
export type MinuteTable = MinuteRows["table"];
export type MinuteStudyCreate = Schemas["MinuteStudyCreateRequest"];
export type MinuteStudyReceipt = Schemas["MinuteStudyCommandReceipt"];
export type MinuteStudySource = Schemas["MinuteStudySourceCapability"];
export type MinuteStudyResult = Schemas["MinuteStudyResultData"];
export type MinuteStudyTrial = Schemas["MinuteStudyTrialData"];
export type MinuteStudyHeatmap = Schemas["MinuteStudyHeatmapData"];

export function minuteParameterCreateRecipe(value: MinuteParameters): MinuteParameters {
  if (value.parameters.family !== "auction_gap") return value;
  return {
    ...value,
    schema_version: 2,
    parameters: {
      ...value.parameters,
      next_day_price_policy: "keep_candidate_mark_unavailable",
    },
  };
}

function useMinuteIdentity() {
  const meta = useCurrentMeta();
  const viewer = meta.error === null ? (meta.data?.data.viewer ?? null) : null;
  return {
    key: ["minute-runtime", viewer, meta.data?.data.generation?.generation_id ?? null],
    ready: viewer !== null && meta.error === null,
  };
}

function requireMinute<T>(data: T | undefined, response: Response): T {
  if (data !== undefined) return data;
  throw new ApiError(
    response.status,
    response.status === 409
      ? "结果已变化或尚未保存完成，请刷新后重试。"
      : "分钟回测暂时无法加载，请稍后重试。",
  );
}

export function useMinuteCapabilities() {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "capabilities"],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/capabilities",
      );
      return requireMinute(data, response);
    },
    { enabled: identity.ready, staleTime: 15_000 },
  );
}

export function useMinuteSources() {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "sources"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/minute-runtime/sources");
      return requireMinute(data, response);
    },
    { enabled: identity.ready, staleTime: 15_000 },
  );
}

export function useMinuteParameterSources(enabled = true) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "parameter-sources"],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/parameter-sources",
      );
      return requireMinute(data, response);
    },
    { enabled: identity.ready && enabled, staleTime: 15_000 },
  );
}

export function useMinuteStudyCapabilities(enabled = true) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "study-capabilities"],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/studies/capabilities",
      );
      return requireMinute(data, response);
    },
    { enabled: identity.ready && enabled, staleTime: 15_000 },
  );
}

export function useMinuteStudies(cursor: string | null, refresh: number) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "studies", cursor, refresh],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/minute-runtime/studies", {
        params: { query: { cursor: cursor ?? undefined, limit: 20 } },
      });
      return requireMinute(data, response);
    },
    { enabled: identity.ready, refetchInterval: 5_000 },
  );
}

export function useMinuteStudyResult(commandId: string | null, poll: boolean) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "study-result", commandId],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/studies/{command_id}",
        { params: { path: { command_id: commandId ?? "" } } },
      );
      const result = requireMinute(data, response);
      if (result.data.command_id !== commandId || result.data.request.command_id !== commandId)
        throw new ApiError(409, "研究已变化，请重新选择。");
      return result;
    },
    { enabled: identity.ready && commandId !== null, refetchInterval: poll ? 2_000 : false },
  );
}

export function useMinuteStudyHeatmap(
  commandId: string | null,
  planId: string | null,
  trialIndex: number | null,
  studyId: string | null,
  xParameter: string | null,
  yParameter: string | null,
  resultSetKey: string | null = null,
) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [
      ...identity.key,
      "study-heatmap",
      commandId,
      planId,
      trialIndex,
      studyId,
      xParameter,
      yParameter,
      resultSetKey,
    ],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/studies/{command_id}/heatmap",
        {
          params: {
            path: { command_id: commandId ?? "" },
            query: {
              current_trial_index: trialIndex ?? 0,
              x_parameter: xParameter ?? "",
              y_parameter: yParameter ?? "",
            },
          },
        },
      );
      const result = requireMinute(data, response);
      if (
        result.data.command_id !== commandId ||
        result.data.plan_id !== planId ||
        result.data.heatmap.current_study_id !== studyId ||
        result.data.heatmap.x_axis.parameter_name !== xParameter ||
        result.data.heatmap.y_axis.parameter_name !== yParameter
      )
        throw new ApiError(409, "研究图已变化，请刷新原研究。");
      return result;
    },
    {
      enabled:
        identity.ready &&
        commandId !== null &&
        planId !== null &&
        trialIndex !== null &&
        studyId !== null &&
        xParameter !== null &&
        yParameter !== null &&
        xParameter !== yParameter,
      staleTime: 15_000,
    },
  );
}

export async function submitMinuteStudy(body: MinuteStudyCreate): Promise<MinuteStudyReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/backtests/minute-runtime/studies", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认，请重试原请求。");
    });
  const receipt = data ?? error;
  if (
    receipt !== undefined &&
    "command_id" in receipt &&
    receipt.command_id === body.command_id &&
    ["pending", "processing", "unknown", "submitted", "unavailable", "failed", "conflict"].includes(
      receipt.status,
    ) &&
    receipt.jobs.every((job, index) => job.index === index && uuid(job.job_id))
  )
    return receipt;
  throw new ApiError(
    response.status,
    response.status === 403
      ? "当前账号不能提交研究。"
      : response.status === 422
        ? "研究配置不完整，请检查原来源与参数。"
        : "提交状态待确认，请重试原请求。",
  );
}

export function restoreMinuteStudyRequest(encoded: string | null): MinuteStudyCreate | null {
  if (encoded === null || new TextEncoder().encode(encoded).length > 32 * 1024) return null;
  try {
    const body: unknown = JSON.parse(encoded);
    return studyRequest(body) ? body : null;
  } catch {
    return null;
  }
}

// Saved request structure is checked here; source, role, legal combinations and enumeration stay with the owner.
function studyRequest(value: unknown): value is MinuteStudyCreate {
  if (
    !plainRecord(value) ||
    !exactShape(
      value,
      [
        "command_id",
        "requested_at",
        "source_key",
        "source_version",
        "full_input_hash",
        "parameters",
        "protocol",
        "settings",
        "random_seed",
        "deadline",
        "mode",
      ],
      ["search", "walk_forward"],
    ) ||
    !uuid(value.command_id) ||
    typeof value.requested_at !== "string" ||
    !Number.isFinite(Date.parse(value.requested_at)) ||
    !isParameterRunConfig({
      kind: "minute_parameter_replay",
      source_key: value.source_key,
      source_version: value.source_version,
      full_input_hash: value.full_input_hash,
      parameters: value.parameters,
      protocol: value.protocol,
      random_seed: value.random_seed,
      deadline: value.deadline,
    }) ||
    !Array.isArray(value.settings) ||
    value.settings.length === 0 ||
    value.settings.length > 20_000 ||
    !value.settings.every(
      (setting) =>
        plainRecord(setting) &&
        exactShape(setting, ["score_profile", "top_n", "min_trades"]) &&
        typeof setting.score_profile === "string" &&
        setting.score_profile.length > 0 &&
        setting.score_profile.length <= 128 &&
        positiveInteger(setting.top_n) &&
        positiveInteger(setting.min_trades),
    ) ||
    !["single", "grid", "random", "ablation", "walk_forward"].includes(String(value.mode))
  )
    return false;
  if (value.search !== undefined && value.search !== null) {
    const search = value.search;
    if (
      !plainRecord(search) ||
      !exactShape(search, ["base", "axes", "mode", "seed"], ["requested_trials"]) ||
      !parameterRecipe(search.base) ||
      (search.mode !== "grid" && search.mode !== "random") ||
      search.seed !== value.random_seed ||
      !Array.isArray(search.axes) ||
      search.axes.length === 0 ||
      !search.axes.every(
        (axis) =>
          plainRecord(axis) &&
          exactShape(axis, ["path", "values"]) &&
          typeof axis.path === "string" &&
          /^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*$/.test(axis.path) &&
          Array.isArray(axis.values) &&
          axis.values.length > 0 &&
          axis.values.every(
            (item) =>
              item === null ||
              typeof item === "boolean" ||
              finiteNumber(item) ||
              (Array.isArray(item) && item.length > 0 && item.every(positiveInteger)),
          ),
      ) ||
      (search.requested_trials !== undefined &&
        search.requested_trials !== null &&
        !positiveInteger(search.requested_trials)) ||
      (value.mode !== "grid" && value.mode !== "random" && value.mode !== "walk_forward") ||
      ((value.mode === "grid" || value.mode === "random") && search.mode !== value.mode)
    )
      return false;
  } else if (value.mode === "grid" || value.mode === "random") return false;
  if (value.walk_forward !== undefined && value.walk_forward !== null) {
    const windows = value.walk_forward;
    if (
      value.mode !== "walk_forward" ||
      !plainRecord(windows) ||
      !exactShape(windows, ["fold_count", "min_training_dates", "validation_date_count"]) ||
      !Object.values(windows).every(positiveInteger)
    )
      return false;
  } else if (value.mode === "walk_forward") return false;
  return true;
}

export function useMinuteJobs(cursor: string | null, refresh: number) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "jobs", cursor, refresh],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/minute-runtime/runs", {
        params: { query: { cursor: cursor ?? undefined, limit: 20 } },
      });
      const result = requireMinute(data, response);
      if (!result.data.jobs.every(parameterMetadataMatches))
        throw new ApiError(409, "回放资料已变化，请刷新后重试。");
      return result;
    },
    { enabled: identity.ready, refetchInterval: 5_000 },
  );
}

export function useMinuteSummary(jobId: string | null, poll: boolean) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "summary", jobId],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/runs/{job_id}",
        {
          params: { path: { job_id: jobId ?? "" } },
        },
      );
      const result = requireMinute(data, response);
      if (
        !parameterMetadataMatches(result.data.job) ||
        !parameterMetadataMatches(result.data.source)
      )
        throw new ApiError(409, "回放资料已变化，请刷新后重试。");
      return result;
    },
    { enabled: identity.ready && jobId !== null, refetchInterval: poll ? 2_000 : false },
  );
}

export function useMinuteNav(jobId: string | null, resultHash: string | null) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "nav", jobId, resultHash],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/runs/{job_id}/nav",
        {
          params: { path: { job_id: jobId ?? "" }, query: { result_hash: resultHash ?? "" } },
        },
      );
      return requireMinute(data, response);
    },
    { enabled: identity.ready && jobId !== null && resultHash !== null, staleTime: Infinity },
  );
}

export function useMinuteRows(
  jobId: string | null,
  resultHash: string | null,
  table: MinuteTable,
  offset: number,
) {
  const identity = useMinuteIdentity();
  return useServingQuery(
    [...identity.key, "rows", jobId, resultHash, table, offset],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/minute-runtime/runs/{job_id}/rows",
        {
          params: {
            path: { job_id: jobId ?? "" },
            query: { result_hash: resultHash ?? "", table, offset, limit: 20 },
          },
        },
      );
      return requireMinute(data, response);
    },
    { enabled: identity.ready && jobId !== null && resultHash !== null, staleTime: Infinity },
  );
}

export async function submitMinuteRun(body: MinuteCreate): Promise<MinuteReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/backtests/minute-runtime/runs", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认，请重试原请求。");
    });
  const receipt = data ?? error;
  if (
    receipt !== undefined &&
    "command_id" in receipt &&
    receipt.command_id === body.command_id &&
    (receipt.status !== "submitted" ||
      (typeof receipt.job_id === "string" &&
        /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(receipt.job_id)))
  )
    return receipt;
  throw new ApiError(
    response.status,
    response.status === 422
      ? "配置不完整，请检查策略版本与登记区间。"
      : "提交状态待确认，请重试原请求。",
  );
}

export function restoreMinuteRequest(encoded: string | null): MinuteCreate | null {
  if (encoded === null || encoded.length > 32 * 1024) return null;
  try {
    const body: unknown = JSON.parse(encoded);
    const record = (value: unknown): value is Record<string, unknown> =>
      value !== null && typeof value === "object" && !Array.isArray(value);
    const fields = (value: Record<string, unknown>, names: string[]) =>
      Object.keys(value).length === names.length && names.every((name) => name in value);
    const iso = (value: unknown): value is string =>
      typeof value === "string" && Number.isFinite(Date.parse(value));
    if (
      !record(body) ||
      !fields(body, ["command_id", "requested_at", "config"]) ||
      typeof body.command_id !== "string" ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(body.command_id) ||
      !iso(body.requested_at) ||
      !record(body.config)
    )
      return null;
    const config = body.config;
    if (config.kind === "minute_parameter_replay") {
      if (new TextEncoder().encode(encoded).length > 32 * 1024 || !isParameterRunConfig(config))
        return null;
      return { command_id: body.command_id, requested_at: body.requested_at, config };
    }
    if (
      !fields(config, [
        "source_key",
        "source_version",
        "full_input_hash",
        "native_id",
        "native_version",
        "protocol",
        "random_seed",
        "deadline",
      ]) ||
      typeof config.source_key !== "string" ||
      !/^[a-zA-Z0-9_.:-]{1,128}$/.test(config.source_key) ||
      typeof config.full_input_hash !== "string" ||
      !/^[0-9a-f]{64}$/.test(config.full_input_hash) ||
      typeof config.source_version !== "number" ||
      !Number.isSafeInteger(config.source_version) ||
      config.source_version < 1 ||
      typeof config.native_version !== "number" ||
      !Number.isSafeInteger(config.native_version) ||
      config.native_version < 1 ||
      (config.native_id !== "n_shape" &&
        config.native_id !== "auction_gap" &&
        config.native_id !== "growth_board_surge") ||
      typeof config.random_seed !== "number" ||
      !Number.isSafeInteger(config.random_seed) ||
      config.random_seed < 0 ||
      !iso(config.deadline) ||
      !record(config.protocol)
    )
      return null;
    const protocol = config.protocol;
    const range = (value: unknown): value is { start_date: string; end_date: string } =>
      record(value) &&
      fields(value, ["start_date", "end_date"]) &&
      typeof value.start_date === "string" &&
      /^\d{4}-\d{2}-\d{2}$/.test(value.start_date) &&
      typeof value.end_date === "string" &&
      /^\d{4}-\d{2}-\d{2}$/.test(value.end_date);
    if (
      !fields(protocol, ["train_range", "validation_range", "frozen_outer_test_range"]) ||
      !range(protocol.train_range) ||
      !range(protocol.validation_range) ||
      !range(protocol.frozen_outer_test_range)
    )
      return null;
    return {
      command_id: body.command_id,
      requested_at: body.requested_at,
      config: {
        source_key: config.source_key,
        source_version: config.source_version,
        full_input_hash: config.full_input_hash,
        native_id: config.native_id,
        native_version: config.native_version,
        random_seed: config.random_seed,
        deadline: config.deadline,
        protocol: {
          train_range: protocol.train_range,
          validation_range: protocol.validation_range,
          frozen_outer_test_range: protocol.frozen_outer_test_range,
        },
      },
    };
  } catch {
    return null;
  }
}

function isParameterRunConfig(value: unknown): value is Schemas["MinuteParameterRunConfig"] {
  if (
    !plainRecord(value) ||
    !exactShape(value, [
      "kind",
      "source_key",
      "source_version",
      "full_input_hash",
      "parameters",
      "protocol",
      "random_seed",
      "deadline",
    ])
  )
    return false;
  if (
    value.kind !== "minute_parameter_replay" ||
    typeof value.source_key !== "string" ||
    !/^[a-zA-Z0-9_.:-]{1,128}$/.test(value.source_key) ||
    typeof value.full_input_hash !== "string" ||
    !/^[0-9a-f]{64}$/.test(value.full_input_hash) ||
    !positiveInteger(value.source_version) ||
    typeof value.random_seed !== "number" ||
    !Number.isSafeInteger(value.random_seed) ||
    value.random_seed < 0 ||
    typeof value.deadline !== "string" ||
    !Number.isFinite(Date.parse(value.deadline))
  )
    return false;
  const protocol = value.protocol;
  return (
    plainRecord(protocol) &&
    exactShape(protocol, ["train_range", "validation_range", "frozen_outer_test_range"]) &&
    Object.values(protocol).every(
      (range) =>
        plainRecord(range) &&
        exactShape(range, ["start_date", "end_date"]) &&
        dateString(range.start_date) &&
        dateString(range.end_date),
    ) &&
    parameterRecipe(value.parameters)
  );
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function exactShape(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): boolean {
  return (
    required.every((name) => name in value) &&
    Object.keys(value).every((name) => required.includes(name) || optional.includes(name))
  );
}
const positiveInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 1;
const dateString = (value: unknown): value is string =>
  typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
const localTime = (value: unknown): value is string =>
  typeof value === "string" && /^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,6})?)?$/.test(value);
const finiteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);
const nullableNumber = (value: unknown): boolean =>
  value === null || value === undefined || finiteNumber(value);
const parameterFrequencies = ["1min", "5min", "15min", "30min", "60min"];

// This checks saved wire structure only. Original owner admission owns parameter ranges and source authority.
function parameterRecipe(value: unknown): value is Schemas["MinuteParameterSet"] {
  if (
    !plainRecord(value) ||
    !exactShape(value, ["kind", "schema_version", "parameters"]) ||
    value.kind !== "minute-parameter-set" ||
    (value.schema_version !== 1 && value.schema_version !== 2) ||
    !plainRecord(value.parameters)
  )
    return false;
  const p = value.parameters;
  if (value.schema_version === 2 && p.family !== "auction_gap") return false;
  if (
    typeof p.freq !== "string" ||
    !parameterFrequencies.includes(p.freq) ||
    !positiveInteger(p.max_hold_days)
  )
    return false;
  if (p.paper !== undefined && !paperRecipe(p.paper)) return false;
  const numbers = (names: readonly string[]) => names.every((name) => finiteNumber(p[name]));
  if (p.family === "n_shape") {
    const numeric = [
      "amount_surge_lookback",
      "amount_surge_min_prior_minutes",
      "amount_surge_ratio",
      "break_high_ratio",
      "carry_close_ratio",
      "carry_low_ratio",
      "factor_score_threshold",
      "price_discontinuity_pct",
      "retest_tolerance_pct",
      "vwap_buffer_pct",
    ] satisfies readonly (keyof Schemas["MinuteNShapeParameters"])[];
    return (
      exactShape(
        p,
        [
          "family",
          "freq",
          "max_hold_days",
          "entry_mode",
          "preset_name",
          "late_confirm_at",
          ...numeric,
        ],
        ["paper", "volume_profile"],
      ) &&
      numbers(numeric) &&
      ["n-shape-pool1", "n-shape-pool2", "n-shape-combined"].includes(String(p.preset_name)) &&
      [
        "first_break",
        "break_retest",
        "late_confirm",
        "vwap_confirm",
        "amount_surge",
        "factor_confirm",
      ].includes(String(p.entry_mode)) &&
      localTime(p.late_confirm_at) &&
      (p.volume_profile === undefined || profileRecipe(p.volume_profile))
    );
  }
  if (p.family === "auction_gap") {
    const numeric = [
      "min_auction_vol_ratio_5d",
      "max_auction_vol_ratio_5d",
      "entry_pullback_tolerance_pct",
      "entry_vwap_buffer_pct",
      "min_limit_progress_pct",
      "next_auction_weak_gap_pct",
      "strong_seal_min_close_minutes",
      "strong_seal_weak_gap_pct",
      "next_morning_vwap_break_buffer_pct",
      "price_tol",
      "seal_hold_max_days",
      "seal_hold_max_open_times",
    ] satisfies readonly (keyof Schemas["MinuteAuctionGapParameters"])[];
    return (
      exactShape(
        p,
        [
          "family",
          "freq",
          "max_hold_days",
          "entry_mode",
          "gap_mode",
          "st_filter",
          "start_date",
          "end_date",
          "entry_start_time",
          "next_morning_exit_until",
          "seal_hold_enabled",
          ...(value.schema_version === 2 ? ["next_day_price_policy"] : []),
          ...numeric,
        ],
        ["paper", "factor_score_threshold", "seal_hold_min_fd_to_circ_pct"],
      ) &&
      numbers(numeric) &&
      p.entry_mode === "vwap_push" &&
      ["close", "strict_high"].includes(String(p.gap_mode)) &&
      ["case_insensitive", "literal_lower", "none"].includes(String(p.st_filter)) &&
      dateString(p.start_date) &&
      dateString(p.end_date) &&
      localTime(p.entry_start_time) &&
      localTime(p.next_morning_exit_until) &&
      typeof p.seal_hold_enabled === "boolean" &&
      (value.schema_version === 1 ||
        p.next_day_price_policy === "keep_candidate_mark_unavailable") &&
      nullableNumber(p.factor_score_threshold) &&
      nullableNumber(p.seal_hold_min_fd_to_circ_pct)
    );
  }
  if (p.family === "growth_board_surge") {
    const numeric = [
      "board_hist_days",
      "factor_score_threshold",
      "fresh_lookback_days",
      "fresh_max_prior_volume_ratio",
      "lookback_days",
      "max_inner_outer_ratio",
      "min_amount_accel_5m",
      "min_board_auction_amount_ratio",
      "min_board_gap_up_ratio",
      "min_cum_amount_ratio",
      "min_hist_days",
      "min_large_net_vol",
      "min_listing_trading_days",
      "min_same_minute_amount_ratio",
      "price_tol",
      "vwap_buffer_pct",
    ] satisfies readonly (keyof Schemas["MinuteGrowthParameters"])[];
    const toggles = [
      "enable_factor_confirm",
      "require_board_favor",
      "require_fresh_surge",
      "require_inner_outer",
      "require_large_net_vol",
      "require_vwap_strength",
      "use_accel_surge",
      "use_same_minute_surge",
    ] satisfies readonly (keyof Schemas["MinuteGrowthParameters"])[];
    return (
      exactShape(
        p,
        ["family", "freq", "max_hold_days", "min_signal_time", ...numeric, ...toggles],
        ["paper"],
      ) &&
      numbers(numeric) &&
      toggles.every((name) => typeof p[name] === "boolean") &&
      localTime(p.min_signal_time)
    );
  }
  return false;
}

function parameterMetadataMatches(value: unknown): boolean {
  if (!plainRecord(value) || value.kind !== "minute_parameter_replay") return true;
  return (
    parameterRecipe(value.parameters) &&
    value.native_version === 1 &&
    value.family === value.parameters.parameters.family &&
    value.evaluator_semantic_version === (value.parameters.schema_version === 2 ? "2.1.0" : "2.0.0")
  );
}

function paperRecipe(value: unknown): boolean {
  const fields = [
    "entry_buffer_pct",
    "entry_slippage_pct",
    "stop_loss_pct",
    "take_profit_pct",
    "trailing_stop_pct",
  ] satisfies readonly (keyof Schemas["MinutePaperParameters"])[];
  return (
    plainRecord(value) &&
    exactShape(value, ["candidate_id", ...fields]) &&
    typeof value.candidate_id === "string" &&
    fields.every((name) => finiteNumber(value[name]))
  );
}
function profileRecipe(value: unknown): boolean {
  const numeric = [
    "fallback_take_profit_pct",
    "max_stop_distance_pct",
    "min_reclaimed_poc_count",
    "min_reward_risk",
    "min_take_profit_pct",
    "resistance_buffer_pct",
    "support_buffer_pct",
    "trailing_stop_pct",
  ] satisfies readonly (keyof Schemas["MinuteVolumeProfileParameters"])[];
  const toggles = [
    "enabled",
    "filter_entry",
    "require_profile",
  ] satisfies readonly (keyof Schemas["MinuteVolumeProfileParameters"])[];
  return (
    plainRecord(value) &&
    exactShape(value, ["lookback_days", ...numeric, ...toggles], ["bin_ratio"]) &&
    numeric.every((name) => finiteNumber(value[name])) &&
    toggles.every((name) => typeof value[name] === "boolean") &&
    Array.isArray(value.lookback_days) &&
    value.lookback_days.length > 0 &&
    value.lookback_days.every(positiveInteger) &&
    nullableNumber(value.bin_ratio)
  );
}

const uuid = (value: unknown): value is string =>
  typeof value === "string" && /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value);
const hash = (value: unknown): value is string =>
  typeof value === "string" && /^[0-9a-f]{64}$/.test(value);

export function minuteHtmlUrl(jobId: string, resultHash: string): string | null {
  if (!uuid(jobId) || !hash(resultHash)) return null;
  return `${apiBaseUrl()}/api/v1/backtests/minute-runtime/runs/${jobId}/report.html?result_hash=${resultHash}`;
}

export function minuteZipUrl(body: MinuteExport, receipt: MinuteReceipt): string | null {
  if (
    receipt.status !== "exported" ||
    receipt.command_id !== body.command_id ||
    receipt.job_id !== body.job_id ||
    receipt.result_hash !== body.result_hash ||
    !uuid(receipt.zip_request_id) ||
    !hash(receipt.sha256) ||
    receipt.byte_size == null ||
    !Number.isSafeInteger(receipt.byte_size) ||
    receipt.byte_size <= 0 ||
    receipt.byte_size > 32 * 1024 * 1024 ||
    !uuid(body.job_id) ||
    !hash(body.result_hash)
  )
    return null;
  return `${apiBaseUrl()}/api/v1/backtests/minute-runtime/runs/${body.job_id}/exports/${receipt.zip_request_id}.zip?result_hash=${body.result_hash}`;
}

export async function submitMinuteExport(body: MinuteExport): Promise<MinuteReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/backtests/minute-runtime/exports", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(20_000),
    })
    .catch(() => {
      throw new ApiError(503, "导出状态待确认，请重试原导出。");
    });
  const receipt = data ?? error;
  if (
    receipt !== undefined &&
    "command_id" in receipt &&
    receipt.command_id === body.command_id &&
    ["pending", "processing", "unknown", "failed", "conflict", "exported"].includes(
      receipt.status,
    ) &&
    (receipt.status !== "exported" || minuteZipUrl(body, receipt) !== null)
  )
    return receipt;
  throw new ApiError(
    response.status,
    response.status === 403
      ? "当前身份不能准备报告。"
      : response.status === 422
        ? "导出资料不完整，请重新选择已完成结果。"
        : "导出状态待确认，请重试原导出。",
  );
}

export function restoreMinuteExportRequest(encoded: string | null): MinuteExport | null {
  if (encoded === null || encoded.length > 32 * 1024) return null;
  try {
    const body: unknown = JSON.parse(encoded);
    if (body === null || typeof body !== "object" || Array.isArray(body)) return null;
    if (
      Object.keys(body).length !== 4 ||
      !("command_id" in body) ||
      !("job_id" in body) ||
      !("result_hash" in body) ||
      !("requested_at" in body) ||
      !uuid(body.command_id) ||
      !uuid(body.job_id) ||
      !hash(body.result_hash) ||
      typeof body.requested_at !== "string" ||
      !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]00:00)$/.test(body.requested_at) ||
      !Number.isFinite(Date.parse(body.requested_at)) ||
      new Date(body.requested_at).toISOString().slice(0, 19) !== body.requested_at.slice(0, 19)
    )
      return null;
    return {
      command_id: body.command_id,
      job_id: body.job_id,
      result_hash: body.result_hash,
      requested_at: body.requested_at,
    };
  } catch {
    return null;
  }
}
