import { ApiError, apiClient, type Schemas } from "./client";

/** Retries must pass the original request without changing its identity or dates. */
export async function submitAuditReportCommand(
  body: Schemas["AuditReportCommandRequest"],
): Promise<Schemas["AuditReportCommandReceipt"]> {
  const { data, response } = await apiClient()
    .POST("/api/v1/data/audit-report/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认。");
    });
  if (data === undefined) throw new ApiError(response.status, "请求未完成。");
  return data;
}
