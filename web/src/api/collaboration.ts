import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef } from "react";
import { ApiError, apiClient, type Schemas } from "./client";
import type { paths } from "./schema";
import { useCurrentMeta } from "./useMeta";

export type CollaborationMe = Schemas["CollaborationMe"];
export type Role = Schemas["RoleEntry"]["role"];
export type RoleSubmit = Schemas["CollaborationRoleSubmit"];
export type AuditQuery = NonNullable<
  paths["/api/v1/collaboration/audit"]["get"]["parameters"]["query"]
>;
export const ROLE_LABELS: Record<Role, string> = {
  admin: "管理员",
  researcher: "研究者",
  viewer: "查看者",
};
export const ROLE_PENDING_KEY = "rquant.role.pending.v1";
const csrf = { "X-Rquant-Csrf": "1" };

function checked<T>(data: T | undefined, error: unknown, status: number): T {
  if (data !== undefined) return data;
  const detail =
    typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
  throw new ApiError(status, typeof detail === "string" ? detail : "暂时读不到，请稍后重试。");
}
export async function fetchCollaborationMe(signal?: AbortSignal) {
  const { data, error, response } = await apiClient().GET("/api/v1/collaboration/me", { signal });
  return checked(data, error, response.status);
}
export function hasCurrentRole(
  me: CollaborationMe | undefined,
  viewer: string | null | undefined,
): boolean {
  return Boolean(
    viewer &&
      me?.available &&
      me.mode === "enforced" &&
      me.username === viewer &&
      me.role &&
      me.revision &&
      /^[a-f0-9]{64}$/.test(me.state_sha256 ?? ""),
  );
}
export function useCollaboration() {
  const meta = useCurrentMeta();
  const viewer = meta.data?.data.viewer ?? null;
  const generation = meta.data?.data.generation?.generation_id ?? null;
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: ["collaboration", "me", viewer, generation],
    queryFn: ({ signal }) => fetchCollaborationMe(signal),
    enabled: viewer !== null,
    staleTime: 0,
    gcTime: 0,
    retry: false,
    refetchInterval: 15_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: "always",
  });
  const me = !query.isError && query.data?.data.username === viewer ? query.data.data : undefined;
  const identity = `${viewer ?? ""}:${me?.state_sha256 ?? ""}`;
  const previous = useRef({ viewer, identity });
  useEffect(() => {
    const old = previous.current;
    if (old.viewer !== null && old.viewer !== viewer) {
      sessionStorage.removeItem(ROLE_PENDING_KEY);
      queryClient.removeQueries({
        predicate: (entry) => entry.queryKey[0] === "collaboration" && entry.queryKey[2] !== viewer,
      });
    } else if (old.identity !== identity) {
      queryClient.removeQueries({
        predicate: (entry) =>
          entry.queryKey[0] === "collaboration" &&
          entry.queryKey[1] !== "me" &&
          entry.queryKey[3] !== me?.state_sha256,
      });
    }
    previous.current = { viewer, identity };
    if (viewer && sessionStorage.getItem(ROLE_PENDING_KEY) && !readPendingRole(viewer))
      sessionStorage.removeItem(ROLE_PENDING_KEY);
  }, [viewer, identity, me?.state_sha256, queryClient]);
  return {
    viewer,
    generation,
    me,
    identity,
    current: hasCurrentRole(me, viewer),
    isLoading: viewer !== null && query.isPending,
    error: query.error,
    refetch: query.refetch,
  };
}
export function useRoleUsers(context: ReturnType<typeof useCollaboration>) {
  return useQuery({
    queryKey: [
      "collaboration",
      "users",
      context.viewer,
      context.me?.state_sha256,
      context.generation,
    ],
    queryFn: async ({ signal }) => {
      const { data, error, response } = await apiClient().GET("/api/v1/collaboration/users", {
        signal,
      });
      return checked(data, error, response.status);
    },
    enabled: context.current && context.me?.can_manage_users === true,
    staleTime: 0,
    gcTime: 0,
    retry: false,
  });
}
export function useRefreshCollaboration() {
  const client = useQueryClient();
  return useCallback(() => {
    void client.invalidateQueries({ queryKey: ["collaboration", "me"] });
    void client.invalidateQueries({ queryKey: ["collaboration", "users"] });
  }, [client]);
}
export function useCommandAudit(context: ReturnType<typeof useCollaboration>, filters: AuditQuery) {
  return useQuery({
    queryKey: [
      "collaboration",
      "audit",
      context.viewer,
      context.me?.state_sha256,
      context.generation,
      filters,
    ],
    queryFn: async ({ signal }) => {
      const { data, error, response } = await apiClient().GET("/api/v1/collaboration/audit", {
        params: { query: filters },
        signal,
      });
      return checked(data, error, response.status);
    },
    enabled: context.current && context.me?.can_read_audit === true,
    staleTime: 0,
    gcTime: 0,
    retry: false,
  });
}
export async function prepareRole(body: Schemas["SetUserRoleRequest"]) {
  const { data, error, response } = await apiClient().POST("/api/v1/collaboration/roles/prepare", {
    body,
    headers: csrf,
  });
  return checked(data, error, response.status).data;
}
export async function submitRole(body: RoleSubmit) {
  const { data, error, response } = await apiClient().POST("/api/v1/collaboration/roles/commands", {
    body,
    headers: csrf,
  });
  return checked(data, error, response.status).data;
}
export async function lookupRole(command: Schemas["SetUserRoleCommand"]) {
  const { data, error, response } = await apiClient().POST("/api/v1/collaboration/roles/lookup", {
    body: { command },
    headers: csrf,
  });
  return checked(data, error, response.status).data;
}

export function readPendingRole(viewer: string | null): RoleSubmit | null {
  try {
    const raw = sessionStorage.getItem(ROLE_PENDING_KEY);
    if (!raw || raw.length > 32_768) return null;
    const value = JSON.parse(raw) as { viewer?: unknown; body?: RoleSubmit };
    const body = value.body;
    const command = body?.command;
    if (
      !viewer ||
      value.viewer !== viewer ||
      command?.actor_id !== viewer ||
      command.kind !== "set_user_role" ||
      !/^[a-f0-9-]{36}$/.test(command.command_id) ||
      !/^[a-f0-9]{64}$/.test(body?.issuance_proof ?? "") ||
      command.preparation?.request.command_id !== command.command_id ||
      command.preparation.request.actor_id !== viewer ||
      command.target_id !== command.entered_target ||
      command.preparation.request.target_id !== command.target_id
    )
      return null;
    return body ?? null;
  } catch {
    return null;
  }
}
export function storePendingRole(viewer: string, body: RoleSubmit): void {
  sessionStorage.setItem(ROLE_PENDING_KEY, JSON.stringify({ viewer, body }));
}
export function roleError(error: unknown): string {
  return error instanceof ApiError ? error.message : "暂时读不到结果，请查看原操作。";
}
