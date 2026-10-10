import { ApiError, apiClient, type Schemas } from "./client";
import { useServingQuery } from "./useServingQuery";

export type QueryResult = Schemas["QueryResult"];
export type QueryValue = QueryResult["rows"][number][number];
export type SavedResearchQuery = Schemas["SavedResearchQuery"];
export type SaveResearchQuery = Schemas["SaveResearchQuery"];

function errorMessage(status: number): string {
  if (status === 401) return "请先登录。";
  if (status === 403) return "当前账号没有查询权限。";
  if (status === 422) return "查询内容有误，请检查 SQL。";
  return "查询服务暂时不可用，请稍后重试。";
}

export function useQueryCatalog(viewer: string | null | undefined) {
  return useServingQuery(
    ["research", "catalog", viewer],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/research/catalog");
      if (data === undefined) throw new ApiError(response.status, errorMessage(response.status));
      return data;
    },
    { enabled: typeof viewer === "string" },
  );
}

export function useSavedQueries(viewer: string | null | undefined) {
  return useServingQuery(
    ["research", "queries", viewer],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/research/queries");
      if (data === undefined) throw new ApiError(response.status, errorMessage(response.status));
      return data;
    },
    { enabled: typeof viewer === "string" },
  );
}

export async function executeQuery(
  sql: string,
  mode: "query" | "explain",
  signal: AbortSignal,
): Promise<QueryResult> {
  const { data, response } = await apiClient().POST("/api/v1/research/query", {
    body: { sql, mode },
    signal,
    headers: { "X-Rquant-Csrf": "1" },
  });
  if (data === undefined) throw new ApiError(response.status, errorMessage(response.status));
  return data.data;
}

export async function saveQuery(
  body: SaveResearchQuery,
  resume = false,
): Promise<Schemas["QuerySaveData"]> {
  const options = { body, headers: { "X-Rquant-Csrf": "1" } };
  const { data, response } = resume
    ? await apiClient().POST("/api/v1/research/queries/resume", options)
    : await apiClient().POST("/api/v1/research/queries/save", options);
  if (data === undefined)
    throw new ApiError(
      response.status,
      response.status === 409
        ? "原保存命令不匹配，请重新载入查询。"
        : "保存结果尚未确认，请恢复原命令。",
    );
  return data.data;
}

export function queryValueText(value: QueryValue | undefined): string {
  if (value == null) return "—";
  return typeof value === "object" ? value.text : String(value);
}

function csvCell(value: QueryValue): string {
  const raw = value === null ? "" : queryValueText(value);
  const safe = /^[\s]*[=+\-@]/u.test(raw) ? `'${raw}` : raw;
  return /[",\r\n]/u.test(safe) ? `"${safe.replaceAll('"', '""')}"` : safe;
}

export function queryResultCsv(result: QueryResult): string {
  return `${[
    result.columns.map((column) => csvCell(column.name)).join(","),
    ...result.rows.map((row) => row.map(csvCell).join(",")),
  ].join("\r\n")}\r\n`;
}

export function downloadQueryCsv(result: QueryResult): void {
  const url = URL.createObjectURL(
    new Blob(["\uFEFF", queryResultCsv(result)], { type: "text/csv;charset=utf-8" }),
  );
  try {
    const link = document.createElement("a");
    link.href = url;
    link.download = "查询结果.csv";
    link.click();
  } finally {
    URL.revokeObjectURL(url);
  }
}

export function queryJournalKey(viewer: string): string {
  return `rquant.query-save.${encodeURIComponent(viewer)}.v1`;
}

export function readQueryJournal(viewer: string): SaveResearchQuery | null {
  const text = sessionStorage.getItem(queryJournalKey(viewer));
  if (text === null) return null;
  if (text.length > 256 * 1024) throw new Error("保存记录未通过核验。");
  const body: unknown = JSON.parse(text);
  if (body === null || typeof body !== "object" || Array.isArray(body))
    throw new Error("保存记录未通过核验。");
  const fields = body as Record<string, unknown>;
  const allowed = new Set([
    "kind",
    "command_id",
    "requested_at",
    "query_id",
    "name",
    "sql",
    "expected_version",
  ]);
  if (
    Object.keys(fields).some((key) => !allowed.has(key)) ||
    fields.kind !== "save_research_query" ||
    typeof fields.command_id !== "string" ||
    !/^[A-Za-z0-9._-]{1,128}$/u.test(fields.command_id) ||
    typeof fields.query_id !== "string" ||
    !/^[A-Za-z0-9._-]{1,64}$/u.test(fields.query_id) ||
    typeof fields.requested_at !== "string" ||
    !Number.isFinite(Date.parse(fields.requested_at)) ||
    typeof fields.name !== "string" ||
    Array.from(fields.name).length > 60 ||
    !fields.name.trim() ||
    typeof fields.sql !== "string" ||
    new TextEncoder().encode(fields.sql).length > 32 * 1024 ||
    (fields.expected_version != null &&
      (typeof fields.expected_version !== "number" ||
        !Number.isInteger(fields.expected_version) ||
        fields.expected_version < 1))
  ) {
    throw new Error("保存记录未通过核验。");
  }
  return fields as SaveResearchQuery;
}
