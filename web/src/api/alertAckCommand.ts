import { ApiError, apiClient, type Schemas } from "./client";

export class AckNoEffectError extends ApiError {
  constructor() {
    super(409, "旧数据上的确认请求未受理。");
    this.name = "AckNoEffectError";
  }
}

/** Send the browser's saved request unchanged; a lost response remains uncertain. */
export async function submitAlertAckCommand(
  body: Schemas["AckCommandRequest"],
): Promise<Schemas["AckCommandReceipt"]> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/monitor/ack", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "确认状态待核对。");
    });
  if (data === undefined) {
    if (
      response.status === 409 &&
      error &&
      "code" in error &&
      error.code === "stale_generation_no_effect"
    )
      throw new AckNoEffectError();
    throw new ApiError(response.status, "确认状态待核对。");
  }
  return data;
}
