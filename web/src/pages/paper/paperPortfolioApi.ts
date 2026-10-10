import { ApiError, apiClient, type Schemas } from "@/api/client";
import { useServingQuery } from "@/api/useServingQuery";

export type PaperItem = Schemas["PaperPortfolioItem"];
export type PaperDetail = Schemas["PaperPortfolioDetailData"];
export type PaperConfiguration = Schemas["PaperConfigurationView"];
export type PaperHistoryPage = Schemas["PaperPortfolioHistoryPageView"];
export type PaperSave = Schemas["SavePaperPortfolioConfiguration"];
export type PaperPause = Schemas["SetPaperAccountPaused"];
export type PaperRun = Schemas["RunPaperPortfolioResearch"];
export type PaperOperation = PaperSave | PaperPause | PaperRun;
export type PaperReceipt = Schemas["PaperPortfolioCommandData"];
export type PaperPreparation = Schemas["PaperPausePreparationData"];

function requireData<T>(data: T | undefined, response: Response): T {
  if (data !== undefined) return data;
  throw new ApiError(
    response.status,
    response.status === 422
      ? "规则有误，请检查后重试。"
      : response.status === 409
        ? "账户已更新，请重新查看。"
        : [401, 403].includes(response.status)
          ? "当前账号无法操作这个账户。"
          : "账户暂时无法加载，请稍后重试。",
  );
}

export function usePaperPortfolios(viewer: string, generation: string | null) {
  return useServingQuery(
    ["paper-portfolios", viewer, generation, "catalog"],
    async () => {
      const result = await apiClient().GET("/api/v1/paper-portfolios", {
        params: { query: { generation_id: generation ?? undefined } },
      });
      return requireData(result.data, result.response);
    },
    { enabled: generation !== null },
  );
}

export function usePaperPortfolio(viewer: string, generation: string, account: string | null) {
  return useServingQuery(
    ["paper-portfolios", viewer, generation, account, "detail"],
    async () => {
      const result = await apiClient().GET("/api/v1/paper-portfolios/{account_id}", {
        params: { path: { account_id: account ?? "" }, query: { generation_id: generation } },
      });
      return requireData(result.data, result.response);
    },
    { enabled: account !== null },
  );
}

export function usePaperFullHistory(
  viewer: string,
  generation: string,
  account: string,
  cursor: string | null,
  enabled: boolean,
) {
  return useServingQuery(
    ["paper-portfolios", viewer, generation, account, cursor, "history"],
    async () => {
      const result = await apiClient().GET("/api/v1/paper-portfolios/{account_id}/history", {
        params: {
          path: { account_id: account },
          query: { generation_id: generation, cursor: cursor ?? undefined, limit: 200 },
        },
      });
      return requireData(result.data, result.response);
    },
    { enabled },
  );
}

export async function preparePaperPause(body: PaperPause): Promise<PaperPreparation> {
  const result = await apiClient().POST("/api/v1/paper-portfolios/{account_id}/pause/prepare", {
    body,
    headers: { "X-Rquant-Csrf": "1" },
    params: { path: { account_id: body.account_id } },
  });
  const prepared = requireData(result.data, result.response).data;
  const original = prepared.command;
  if (
    original.kind !== body.kind ||
    original.command_id !== body.command_id ||
    original.account_id !== body.account_id ||
    original.configuration_fingerprint !== body.configuration_fingerprint ||
    original.expected_sequence !== body.expected_sequence ||
    original.expected_paused !== body.expected_paused ||
    original.paused !== body.paused ||
    original.generation_id !== body.generation_id ||
    Date.parse(original.requested_at) !== Date.parse(body.requested_at) ||
    !prepared.confirmation_id ||
    !Number.isFinite(Date.parse(prepared.expires_at))
  ) {
    throw new ApiError(503, "确认内容暂时无法核验，请重试。");
  }
  return prepared;
}

export async function postPaperOperation(
  body: PaperOperation,
  resume: boolean,
  confirmationId?: string,
): Promise<PaperReceipt> {
  const options = {
    headers: { "X-Rquant-Csrf": "1" },
    params: { path: { account_id: body.account_id } },
  };
  const result = resume
    ? await apiClient().POST("/api/v1/paper-portfolios/{account_id}/recover", { ...options, body })
    : body.kind === "set_paper_account_paused"
      ? await apiClient().POST("/api/v1/paper-portfolios/{account_id}/pause/confirm", {
          ...options,
          body: { request: body, confirmation_id: confirmationId ?? "" },
        })
      : body.kind === "save_paper_portfolio_configuration"
        ? await apiClient().POST("/api/v1/paper-portfolios/{account_id}/configuration", {
            ...options,
            body,
          })
        : body.task_name === "paper_reconcile"
          ? await apiClient().POST("/api/v1/paper-portfolios/{account_id}/reconcile", {
              ...options,
              body,
            })
          : await apiClient().POST("/api/v1/paper-portfolios/{account_id}/band", {
              ...options,
              body,
            });
  const receipt = requireData(result.data, result.response).data;
  if (
    receipt.command_id !== body.command_id ||
    receipt.account_id !== body.account_id ||
    (receipt.status === "submitted" && receipt.job_id !== body.command_id)
  ) {
    throw new ApiError(503, "操作回执暂时无法核验。");
  }
  return receipt;
}

export function usePaperResearch(
  viewer: string,
  generation: string,
  account: string,
  job: string | null,
) {
  return useServingQuery(
    ["paper-portfolios", viewer, generation, account, job, "research"],
    async () => {
      const result = await apiClient().GET(
        "/api/v1/paper-portfolios/{account_id}/research/{job_id}",
        {
          params: {
            path: { account_id: account, job_id: job ?? "" },
            query: { generation_id: generation },
          },
        },
      );
      return requireData(result.data, result.response);
    },
    { enabled: job !== null },
  );
}

export async function downloadPaperResearch(account: string, job: string): Promise<void> {
  const result = await apiClient().GET(
    "/api/v1/paper-portfolios/{account_id}/research/{job_id}/download",
    {
      params: { path: { account_id: account, job_id: job } },
      parseAs: "blob",
    },
  );
  const file = requireData(result.data, result.response);
  if (!(file instanceof Blob) || file.size === 0 || file.size > 16 * 1024 * 1024)
    throw new ApiError(503, "结果暂时无法下载，请稍后重试。");
  const url = URL.createObjectURL(file);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "模拟盘研究结果.zip";
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}
