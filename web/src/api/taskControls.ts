import { useQuery } from "@tanstack/react-query";
import { ApiError, apiClient, type Schemas } from "./client";

export type TaskControlCapabilities = Schemas["TaskControlCapabilitiesData"];
export type TaskControlResult = Schemas["TaskControlCommandData"];
export type TaskControlRequest = TaskControlResult["original_request"];
export type UnitRunRequest = Schemas["RequestUnitRun"];
export type UnitPrepareRequest = Schemas["PrepareUnitRun"];
export type SchedulingRequest = Schemas["SetLabSchedulingPaused"];
export type SchedulingView = Schemas["TaskSchedulingView"];
export type UnitControlChoice = Schemas["TaskUnitControlChoice"];

function fail(status: number): never {
  throw new ApiError(
    status,
    status === 422
      ? "请求内容有误，请检查后重试。"
      : status === 409
        ? "数据已变化，请刷新后再操作。"
        : "运行结果待确认，请核验原请求。",
  );
}

function originalIdentity(body: TaskControlRequest): string {
  const common = [body.kind, body.command_id, Date.parse(body.requested_at), body.generation_id];
  if (body.kind === "prepare_unit_run") {
    const run = body.run;
    return JSON.stringify([
      ...common,
      run.command_id,
      Date.parse(run.requested_at),
      run.generation_id,
      run.unit,
    ]);
  }
  if (body.kind === "request_unit_run")
    return JSON.stringify([...common, body.unit, body.confirmation_id ?? null]);
  if (body.kind === "set_lab_scheduling_paused")
    return JSON.stringify([...common, body.expected_version, body.paused]);
  throw new ApiError(503, "原请求暂无法核验。");
}

function boundResult(
  body: TaskControlRequest,
  result: TaskControlResult | undefined,
  response: Response,
): TaskControlResult {
  if (!response.ok || result === undefined) fail(response.status);
  if (
    result.command_id !== body.command_id ||
    originalIdentity(result.original_request) !== originalIdentity(body)
  ) {
    throw new ApiError(503, "原请求暂无法核验。");
  }
  return result;
}

export function useTaskControlCapabilities(
  viewer: string | null,
  generationId: string | null,
  refreshKey: number,
) {
  return useQuery({
    queryKey: ["task-controls", viewer, generationId, refreshKey],
    enabled: viewer !== null && generationId !== null,
    retry: false,
    queryFn: async ({ signal }) => {
      if (generationId === null) throw new ApiError(503, "任务状态暂无法核验。");
      const { data, response } = await apiClient().GET("/api/v1/tasks/control-capabilities", {
        params: { query: { generation_id: generationId } },
        signal,
      });
      if (!response.ok || data === undefined) fail(response.status);
      if (data.generation_id != null && data.generation_id !== generationId) {
        throw new ApiError(409, "数据已更新，请刷新任务。");
      }
      return data;
    },
  });
}

export async function submitTaskControl(
  body: TaskControlRequest,
  signal: AbortSignal,
): Promise<TaskControlResult> {
  const headers = { "X-Rquant-Csrf": "1" };
  if (body.kind === "prepare_unit_run") {
    const { data, response } = await apiClient().POST("/api/v1/tasks/units/{unit}/run/prepare", {
      params: { path: { unit: body.run.unit } },
      body,
      headers,
      signal,
    });
    return boundResult(body, data, response);
  }
  if (body.kind === "request_unit_run") {
    const { data, response } = await apiClient().POST("/api/v1/tasks/units/{unit}/run", {
      params: { path: { unit: body.unit } },
      body,
      headers,
      signal,
    });
    return boundResult(body, data, response);
  }
  if (body.kind === "set_lab_scheduling_paused") {
    const { data, response } = await apiClient().POST("/api/v1/tasks/scheduling/commands", {
      body,
      headers,
      signal,
    });
    return boundResult(body, data, response);
  }
  throw new ApiError(422, "请求内容有误。");
}

export async function recoverTaskControl(
  body: TaskControlRequest,
  mode: "lookup" | "resume",
  signal: AbortSignal,
): Promise<TaskControlResult> {
  const headers = { "X-Rquant-Csrf": "1" };
  const { data, response } =
    mode === "lookup"
      ? await apiClient().POST("/api/v1/tasks/controls/lookup", { body, headers, signal })
      : await apiClient().POST("/api/v1/tasks/controls/resume", { body, headers, signal });
  return boundResult(body, data, response);
}
