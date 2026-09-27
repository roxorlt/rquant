import { ApiError, apiClient, type Schemas } from "./client";

/** Send the browser's saved request unchanged; a lost response remains uncertain. */
export async function submitAlertAckCommand(
  body: Schemas["AckCommandRequest"],
): Promise<Schemas["AckCommandReceipt"]> {
  const { data, response } = await apiClient()
    .POST("/api/v1/monitor/ack", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "确认状态待核对。");
    });
  if (data === undefined) throw new ApiError(response.status, "确认状态待核对。");
  return data;
}
