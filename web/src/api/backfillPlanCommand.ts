import { ApiError, apiClient, type Schemas } from "./client";

export async function submitBackfillPlanCommand(
  body: Schemas["BackfillPlanCommandRequest"],
): Promise<Schemas["BackfillPlanCommandReceipt"]> {
  const { data, response } = await apiClient()
    .POST("/api/v1/data/backfill-plans/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认。");
    });
  if (data === undefined) throw new ApiError(response.status, "提交状态待确认。");
  return data;
}
