import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router";
import { ApiError, type Schemas } from "@/api/client";
import {
  lookupRole,
  prepareRole,
  ROLE_LABELS,
  ROLE_PENDING_KEY,
  type Role,
  type RoleSubmit,
  readPendingRole,
  roleError,
  storePendingRole,
  submitRole,
  useCollaboration,
  useRefreshCollaboration,
  useRoleUsers,
} from "@/api/collaboration";
import { Button, ConfirmDialog, EmptyState, PageHeader, Panel, SkeletonRows, Tip } from "@/ui";
import "./collaboration.css";

type Preview = { issued: Schemas["IssuedRolePreparation"]; identity: string };
function canonicalRoleTime(value: string): string | null {
  const parts = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/.exec(value);
  if (!parts || !Number.isFinite(Date.parse(value))) return null;
  return `${parts[1]}.${(parts[2] ?? "").padEnd(6, "0")}Z`;
}
function statusOf(value: unknown, id: string): Schemas["PageControlStatus"] | null {
  if (
    !value ||
    typeof value !== "object" ||
    !("command_id" in value) ||
    value.command_id !== id ||
    !("status" in value)
  )
    return null;
  return ["pending", "processing", "succeeded", "failed", "ambiguous"].includes(
    String(value.status),
  )
    ? (value.status as Schemas["PageControlStatus"])
    : null;
}

export default function UsersPage() {
  const context = useCollaboration();
  const users = useRoleUsers(context);
  const refresh = useRefreshCollaboration();
  const [roles, setRoles] = useState<Record<string, Role>>({});
  const [preview, setPreview] = useState<Preview | null>(null);
  const [pending, setPending] = useState<RoleSubmit | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const section = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLElement | null>(null);
  const triggerLabel = useRef<string | null>(null);
  const active = useRef(context);
  active.current = context;
  const restored = useRef<string | null>(null);
  const previousIdentity = useRef(context.identity);
  const admin = context.current && context.me?.can_manage_users === true;
  const state =
    admin &&
    !users.isError &&
    users.data &&
    users.data.data.revision === context.me?.revision &&
    users.data.data.content_sha256 === context.me?.state_sha256
      ? users.data.data
      : null;
  const currentPreview = preview?.identity === context.identity && admin ? preview.issued : null;
  const ownPending = pending?.command.actor_id === context.viewer ? pending : null;
  const locked = busy || ownPending !== null;

  const finish = useCallback(
    (value: unknown, body: RoleSubmit) => {
      if (active.current.viewer !== body.command.actor_id) return;
      const status = statusOf(value, body.command.command_id);
      if (status === "succeeded" || status === "failed") {
        sessionStorage.removeItem(ROLE_PENDING_KEY);
        setPending(null);
        setMessage(status === "succeeded" ? "角色已更新。" : null);
        setError(status === "failed" ? "角色未修改，请刷新后重新发起。" : null);
        refresh();
      } else {
        setMessage(status ? "原操作尚未确认，请继续查看。" : "暂时不能核对原操作，请稍后再查。");
      }
    },
    [refresh],
  );
  const recover = useCallback(
    async (body: RoleSubmit) => {
      const owner = body.command.actor_id;
      setBusy(true);
      setError(null);
      try {
        const result = await lookupRole(body.command);
        if (active.current.viewer !== owner) return;
        if (result.found) finish(result.receipt, body);
        else setMessage("尚未找到原操作，请稍后再查。");
      } catch (failure) {
        if (active.current.viewer === owner) setError(roleError(failure));
      } finally {
        if (active.current.viewer === owner) setBusy(false);
      }
    },
    [finish],
  );
  useEffect(() => {
    setPreview(null);
    setRoles({});
    setError(null);
    setMessage(null);
    setBusy(false);
    const body = readPendingRole(context.viewer);
    setPending(body);
    restored.current = null;
  }, [context.viewer]);
  useEffect(() => {
    if (previousIdentity.current === context.identity) return;
    previousIdentity.current = context.identity;
    setPreview(null);
    setRoles({});
    setBusy(false);
  }, [context.identity]);
  useEffect(() => {
    if (!ownPending || !context.me || restored.current === ownPending.command.command_id) return;
    restored.current = ownPending.command.command_id;
    void recover(ownPending);
  }, [ownPending, context.me, recover]);

  async function prepare(target: Schemas["RoleEntry"], button: HTMLElement) {
    if (
      !state ||
      !admin ||
      locked ||
      !context.viewer ||
      !context.me?.state_sha256 ||
      !context.me.revision
    )
      return;
    const newRole = roles[target.username] ?? target.role;
    if (newRole === target.role) return;
    const identity = context.identity;
    trigger.current = button;
    triggerLabel.current = button.getAttribute("aria-label");
    setBusy(true);
    setMessage(null);
    setError(null);
    const request: Schemas["SetUserRoleRequest"] = {
      schema_version: 1,
      kind: "set_user_role",
      command_id: crypto.randomUUID(),
      actor_id: context.viewer,
      target_id: target.username,
      new_role: newRole,
      expected_revision: state.revision,
      expected_state_sha256: state.content_sha256,
      requested_at: new Date().toISOString(),
    };
    try {
      const result = await prepareRole(request);
      if (active.current.identity !== identity || !active.current.me?.can_manage_users) return;
      if (
        Object.entries(request).some(([key, value]) => {
          const returned = result.preparation.request[key as keyof typeof request];
          if (key !== "requested_at") return returned !== value;
          const instant = canonicalRoleTime(String(returned));
          return instant === null || instant !== canonicalRoleTime(String(value));
        })
      )
        throw new Error("preparation mismatch");
      if (
        result.preparation.confirmation.command_id !== request.command_id ||
        result.preparation.confirmation.target_id !== request.target_id ||
        result.preparation.confirmation.expected_state_sha256 !== request.expected_state_sha256
      )
        throw new Error("confirmation mismatch");
      setPreview({ issued: result, identity });
    } catch (failure) {
      if (active.current.identity === identity) {
        setError(roleError(failure));
        refresh();
      }
    } finally {
      if (active.current.identity === identity) setBusy(false);
    }
  }
  async function confirm() {
    if (!currentPreview || !context.viewer || busy || ownPending) return;
    const original = currentPreview.preparation;
    if (new Date(original.confirmation.expires_at).getTime() <= Date.now()) return;
    const body: RoleSubmit = {
      issuance_proof: currentPreview.issuance_proof,
      command: {
        ...original.request,
        preparation: original,
        entered_target: original.request.target_id,
      },
    };
    try {
      storePendingRole(context.viewer, body);
    } catch {
      setError("浏览器未能保存原操作，请刷新后重试。");
      return;
    }
    restored.current = body.command.command_id;
    setPending(body);
    setPreview(null);
    setBusy(true);
    setError(null);
    try {
      finish(await submitRole(body), body);
    } catch (failure) {
      if (active.current.viewer !== body.command.actor_id) return;
      if (failure instanceof ApiError && failure.status >= 400 && failure.status < 500) {
        sessionStorage.removeItem(ROLE_PENDING_KEY);
        setPending(null);
        refresh();
      }
      setError(roleError(failure));
    } finally {
      if (active.current.viewer === body.command.actor_id) setBusy(false);
    }
  }
  function restoreFocus() {
    const currentButton = Array.from(
      section.current?.querySelectorAll<HTMLButtonElement>("button") ?? [],
    ).find((button) => button.getAttribute("aria-label") === triggerLabel.current);
    const target = trigger.current?.isConnected
      ? trigger.current
      : (currentButton ??
        section.current?.querySelector<HTMLElement>("h1") ??
        document.querySelector<HTMLElement>("button[aria-label='我的']"));
    if (target?.tagName === "H1") target.setAttribute("tabindex", "-1");
    target?.focus();
  }

  return (
    <div className="collaboration-page" ref={section}>
      <PageHeader
        eyebrow="我的"
        title="用户与权限"
        actions={
          <Button onClick={refresh} disabled={busy}>
            刷新权限
          </Button>
        }
      />
      {error ? (
        <p className="collaboration-notice crit-text" role="alert">
          {error}
        </p>
      ) : null}
      {message ? (
        <p className="collaboration-notice" role="status">
          {message}
        </p>
      ) : null}
      {ownPending ? (
        <Panel title="原操作">
          <p>请先核对这次角色修改的结果。</p>
          <Button onClick={() => void recover(ownPending)} disabled={busy}>
            查看原操作
          </Button>
        </Panel>
      ) : null}
      {context.isLoading ? (
        <SkeletonRows rows={3} />
      ) : context.error ? (
        <EmptyState
          title="暂时读不到权限。"
          hint={<Button onClick={() => void context.refetch()}>重试</Button>}
        />
      ) : !context.current ? (
        <EmptyState
          title={context.me?.mode === "legacy" ? "协作权限尚未启用。" : "当前账号的权限尚未确认。"}
        />
      ) : !admin ? (
        <EmptyState
          title="当前账号不能管理用户。"
          hint={<Link to="/audit">查看自己的操作记录</Link>}
        />
      ) : users.isPending ? (
        <SkeletonRows rows={3} />
      ) : !state ? (
        <EmptyState
          title="用户列表已更新或暂不可用。"
          hint={<Button onClick={refresh}>刷新权限</Button>}
        />
      ) : (
        <Panel
          title="用户"
          actions={
            <Tip content="管理员可管理权限；研究者可开展研究；查看者只读。原功能开关和结果归属仍生效。">
              <span className="muted">角色说明</span>
            </Tip>
          }
        >
          <div className="collaboration-table-wrap">
            <table className="collaboration-table" aria-label="用户与权限">
              <thead>
                <tr>
                  <th>账号</th>
                  <th>当前角色</th>
                  <th>修改角色</th>
                  <th>
                    <span className="sr-only">操作</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {state.users.map((entry) => {
                  const selected = roles[entry.username] ?? entry.role;
                  const lastAdmin =
                    entry.role === "admin" &&
                    state.users.filter((item) => item.role === "admin").length === 1;
                  return (
                    <tr key={entry.username}>
                      <td>
                        <span>{entry.username}</span>
                        {entry.username === context.viewer ? <small>我</small> : null}
                      </td>
                      <td>{ROLE_LABELS[entry.role]}</td>
                      <td>
                        <select
                          className="inp"
                          aria-label={`${entry.username} 的角色`}
                          value={selected}
                          disabled={locked}
                          onChange={(event) =>
                            setRoles((old) => ({
                              ...old,
                              [entry.username]: event.target.value as Role,
                            }))
                          }
                        >
                          {Object.entries(ROLE_LABELS).map(([value, label]) => (
                            <option
                              key={value}
                              value={value}
                              disabled={lastAdmin && value !== "admin"}
                            >
                              {label}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>
                        <Button
                          size="sm"
                          aria-label={`修改 ${entry.username} 的角色`}
                          onClick={(event) => void prepare(entry, event.currentTarget)}
                          disabledReason={
                            locked
                              ? "先查看原操作"
                              : selected === entry.role
                                ? "先选择新的角色"
                                : undefined
                          }
                        >
                          修改
                        </Button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Panel>
      )}
      {currentPreview ? (
        <ConfirmDialog
          open
          level="high"
          title="修改角色"
          confirmName={currentPreview?.preparation.request.target_id}
          expiresAt={
            currentPreview
              ? new Date(currentPreview.preparation.confirmation.expires_at)
              : undefined
          }
          confirmLabel="确认修改"
          busy={busy}
          disabled={!admin}
          description={
            currentPreview ? (
              <p>
                将 <b>{currentPreview.preparation.request.target_id}</b> 从
                {ROLE_LABELS[currentPreview.preparation.confirmation.old_role]}改为
                {ROLE_LABELS[currentPreview.preparation.confirmation.new_role]}。新的权限立即生效。
              </p>
            ) : null
          }
          onConfirm={() => void confirm()}
          onCancel={() => {
            setPreview(null);
            queueMicrotask(restoreFocus);
          }}
          afterClose={restoreFocus}
        />
      ) : null}
    </div>
  );
}
