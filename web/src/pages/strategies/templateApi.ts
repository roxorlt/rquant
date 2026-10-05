import { ApiError, apiClient, type Schemas } from "@/api/client";
import { useServingQuery } from "@/api/useServingQuery";

export type TemplateRules = Schemas["StrategyTemplate-Input"];
export type TemplateDetail = Schemas["StrategyTemplateDetailData"];
export type TemplateSources = Schemas["StrategyTemplateSourcesData"];
export type TemplateHead = Schemas["StrategyTemplateHead"];
export type SaveTemplate = Schemas["SaveStrategyTemplate"];
export type ArchiveTemplate = Schemas["ArchiveStrategyTemplate"];
export type RunTemplate = Schemas["RunStrategyTemplate"];
export type TemplateOperation = SaveTemplate | ArchiveTemplate | RunTemplate;
export type TemplateResult =
  | Schemas["StrategyTemplateCommandData"]
  | Schemas["StrategyTemplateRunCommandData"];

function requireData<T>(data: T | undefined, response: Response): T {
  if (data !== undefined) return data;
  throw new ApiError(
    response.status,
    response.status === 409
      ? "数据已更新，请重新查看策略。"
      : response.status === 401 || response.status === 403
        ? "当前账号无法查看策略。"
        : response.status === 422
          ? "策略内容有误，请检查后重试。"
          : "策略暂时无法加载，请稍后重试。",
  );
}

export function useTemplateCatalog(viewer: string, generation: string | null) {
  return useServingQuery(
    ["strategy-templates", viewer, generation, "catalog"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/strategy-templates", {
        params: { query: { generation_id: generation ?? undefined } },
      });
      return requireData(data, response);
    },
    { enabled: generation !== null },
  );
}

export function useTemplateSources(viewer: string, generation: string | null) {
  return useServingQuery(
    ["strategy-templates", viewer, generation, "sources"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/strategy-templates/sources", {
        params: { query: { generation_id: generation ?? undefined } },
      });
      return requireData(data, response);
    },
    { enabled: generation !== null },
  );
}

export function useTemplateDetail(
  viewer: string,
  generation: string | null,
  strategyId: string | null,
  version: number | null,
) {
  return useServingQuery(
    ["strategy-templates", viewer, generation, strategyId, version, "detail"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/strategy-templates/{strategy_id}", {
        params: {
          path: { strategy_id: strategyId ?? "" },
          query: { generation_id: generation ?? undefined, version: version ?? undefined },
        },
      });
      return requireData(data, response);
    },
    { enabled: generation !== null && strategyId !== null },
  );
}

export function useTemplateVersions(
  viewer: string,
  generation: string | null,
  strategyId: string | null,
  before: number | null,
) {
  return useServingQuery(
    ["strategy-templates", viewer, generation, strategyId, before, "versions"],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/strategy-templates/{strategy_id}/versions",
        {
          params: {
            path: { strategy_id: strategyId ?? "" },
            query: {
              generation_id: generation ?? undefined,
              before_version: before ?? undefined,
              limit: 25,
            },
          },
        },
      );
      return requireData(data, response);
    },
    { enabled: generation !== null && strategyId !== null },
  );
}

export async function postTemplateOperation(
  body: TemplateOperation,
  resume: boolean,
): Promise<TemplateResult> {
  const headers = { "X-Rquant-Csrf": "1" };
  if (body.kind === "run_strategy_template") {
    const result = resume
      ? await apiClient().POST("/api/v1/strategy-templates/{strategy_id}/runs/resume", {
          body,
          headers,
          params: { path: { strategy_id: body.strategy_id } },
        })
      : await apiClient().POST("/api/v1/strategy-templates/{strategy_id}/runs", {
          body,
          headers,
          params: { path: { strategy_id: body.strategy_id } },
        });
    const checked = requireData(result.data, result.response).data;
    if (
      checked.command_id !== body.command_id ||
      (checked.status === "submitted" && checked.job_id !== body.command_id)
    )
      throw new ApiError(503, "操作回执暂时无法核验。");
    return checked;
  }
  const result = resume
    ? await apiClient().POST("/api/v1/strategy-templates/commands/resume", { body, headers })
    : await apiClient().POST("/api/v1/strategy-templates/commands", { body, headers });
  const checked = requireData(result.data, result.response).data;
  if (checked.command_id !== body.command_id) throw new ApiError(503, "操作回执暂时无法核验。");
  return checked;
}
