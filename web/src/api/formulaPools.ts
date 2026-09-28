import { ApiError, apiClient, type Schemas } from "./client";
import { useServingQuery } from "./useServingQuery";

export type FormulaPoolSaveRequest = Schemas["FormulaPoolSaveCommandRequest"];
export type FormulaPoolSaveReceipt = Schemas["FormulaPoolSaveCommandReceipt"];
export type FormulaPoolItem = Schemas["FormulaPoolItem"];
export type FormulaPoolMembers = Schemas["FormulaPoolMembersData"];
export type FormulaPoolList = Schemas["FormulaPoolListData"];

function message(error: unknown, fallback: string): string {
  if (typeof error === "object" && error !== null && "detail" in error) {
    if (typeof error.detail === "string") return error.detail;
  }
  return fallback;
}

export async function submitFormulaPoolSave(
  body: FormulaPoolSaveRequest,
): Promise<FormulaPoolSaveReceipt> {
  const { data, error, response } = await apiClient().POST("/api/v1/pools/formula/commands", {
    // openapi-fetch treats a required literal null field as an omitted field.
    body: body as never,
    headers: { "X-Rquant-Csrf": "1" },
  });
  if (data !== undefined) return data;
  if (response.status === 409 && error && "status" in error && error.status === "conflict") {
    return error;
  }
  throw new ApiError(response.status, message(error, "保存状态待确认，请重试原请求。"));
}

export function useFormulaPools() {
  return useServingQuery<FormulaPoolList>(["formula-pools", "list"], async () => {
    const { data, error, response } = await apiClient().GET("/api/v1/pools/formula");
    if (data === undefined) {
      throw new ApiError(response.status, message(error, "公式池暂时无法读取。"));
    }
    return data;
  });
}

export function useFormulaPoolMembers(
  baseName: string | null,
  tradeDate: string | null,
  cursor: string | null,
  generation: string | null | undefined,
  epoch: number,
  enabled: boolean,
) {
  return useServingQuery<FormulaPoolMembers>(
    ["formula-pools", "members", baseName, tradeDate, cursor, generation, epoch],
    async () => {
      if (baseName === null) throw new Error("未选择公式池");
      const { data, error, response } = await apiClient().GET(
        "/api/v1/pools/formula/{base_name}/members",
        { params: { path: { base_name: baseName }, query: { cursor, page_size: 50 } } },
      );
      if (data === undefined) {
        throw new ApiError(response.status, message(error, "成员暂时无法读取。"));
      }
      return data;
    },
    { enabled: baseName !== null && enabled },
  );
}
