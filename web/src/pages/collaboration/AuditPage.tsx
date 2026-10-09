import { type FormEvent, useEffect, useRef, useState } from "react";
import type { Schemas } from "@/api/client";
import { type AuditQuery, roleError, useCollaboration, useCommandAudit } from "@/api/collaboration";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  EmptyState,
  PageHeader,
  Panel,
  RelativeTime,
  SideDrawer,
  SkeletonRows,
  Tip,
} from "@/ui";
import "./collaboration.css";

type Item = Schemas["CommandAuditItem"];
const ACTIONS: Record<string, string> = {
  set_user_role: "修改角色",
  run_strategy_template: "运行策略回测",
  save_strategy_template: "保存策略",
  archive_strategy_template: "归档策略",
  submit_portfolio_backtest: "运行组合回测",
  export_portfolio_backtest_zip: "导出组合数据",
  save_factor_definition: "保存因子",
  submit_factor_run: "运行因子研究",
  save_user_pool: "保存规则池子",
  save_user_pool_v2: "保存条件池子",
  save_user_pool_v3: "保存池子",
  save_formula_pool_v1: "保存公式池子",
  prepare_unit_run: "确认任务运行",
  request_unit_run: "运行任务",
  set_lab_scheduling_paused: "修改调度",
};
const OUTCOMES: Record<Item["outcome"], string> = {
  pending: "等待处理",
  processing: "正在处理",
  accepted: "已提交",
  processed: "处理完成",
  failed: "处理失败",
  ambiguous: "结果待确认",
};
const columns: DataColumn<Item>[] = [
  {
    id: "at",
    header: "时间",
    value: (item) => item.enqueued_at,
    cell: (item) => <RelativeTime at={item.enqueued_at} />,
  },
  { id: "actor", header: "操作人", value: (item) => item.actor_label },
  {
    id: "action",
    header: "操作",
    value: (item) => ACTIONS[item.command_kind] ?? "其他操作",
    cell: (item) => (
      <Tip content={item.command_kind}>{ACTIONS[item.command_kind] ?? "其他操作"}</Tip>
    ),
  },
  {
    id: "outcome",
    header: "结果",
    value: (item) => OUTCOMES[item.outcome],
    cell: (item) => (
      <Tip
        content={
          item.outcome === "accepted" ? "请求已提交；任务结果请在任务页面查看。" : item.summary
        }
      >
        {OUTCOMES[item.outcome]}
      </Tip>
    ),
  },
];
function shanghaiIso(local: string): string | undefined {
  if (!local) return undefined;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,3})?)?$/.test(local)) return undefined;
  const date = new Date(`${local}+08:00`);
  if (!Number.isFinite(date.getTime())) return undefined;
  const shanghai = new Date(date.getTime() + 8 * 60 * 60 * 1000).toISOString();
  return shanghai.startsWith(local) ? date.toISOString() : undefined;
}
export default function AuditPage() {
  const context = useCollaboration();
  const [actor, setActor] = useState("");
  const [action, setAction] = useState("");
  const [from, setFrom] = useState("");
  const [until, setUntil] = useState("");
  const [filters, setFilters] = useState<AuditQuery>({ limit: 20 });
  const [filterOwner, setFilterOwner] = useState(context.identity);
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<{ identity: string; item: Item } | null>(null);
  const focus = useRef<HTMLDivElement>(null);
  const currentFilters = filterOwner === context.identity ? filters : { limit: 20 };
  const cursor = filterOwner === context.identity ? cursors.at(-1) : undefined;
  const audit = useCommandAudit(context, {
    ...currentFilters,
    actor_id: context.me?.can_manage_users
      ? currentFilters.actor_id
      : (context.viewer ?? undefined),
    cursor,
  });
  const data =
    context.current &&
    !audit.isError &&
    context.me?.can_read_audit &&
    audit.data &&
    audit.data.data.role_revision === context.me.revision &&
    audit.data.serving.generation_id === audit.data.data.source_generation
      ? audit.data.data
      : null;
  const detail = selected?.identity === context.identity && data ? selected.item : null;
  useEffect(() => {
    setActor("");
    setAction("");
    setFrom("");
    setUntil("");
    setFilters({ limit: 20 });
    setFilterOwner(context.identity);
    setCursors([undefined]);
    setSelected(null);
    setError(null);
  }, [context.identity]);
  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const start = shanghaiIso(from);
    const end = shanghaiIso(until);
    if ((from && !start) || (until && !end) || (start && end && start >= end)) {
      setError("开始时间须早于结束时间。");
      return;
    }
    setError(null);
    setCursors([undefined]);
    setFilterOwner(context.identity);
    setFilters({
      limit: 20,
      actor_id: actor.trim() || undefined,
      command_kind: action || undefined,
      time_from: start,
      time_until: end,
    });
  }
  const allowed = context.current && context.me?.can_read_audit === true;
  return (
    <div className="collaboration-page" ref={focus}>
      <PageHeader
        eyebrow="我的"
        title="操作记录"
        actions={
          <Button
            onClick={() => {
              setCursors([undefined]);
              void context.refetch();
              void audit.refetch();
            }}
          >
            刷新记录
          </Button>
        }
      />
      {context.isLoading ? (
        <SkeletonRows rows={3} />
      ) : context.error ? (
        <EmptyState
          title="暂时读不到权限。"
          hint={<Button onClick={() => void context.refetch()}>重试</Button>}
        />
      ) : !allowed ? (
        <EmptyState
          title={
            context.me?.mode === "legacy" ? "协作权限尚未启用。" : "当前账号不能查看操作记录。"
          }
        />
      ) : (
        <>
          <form className="collaboration-filters" onSubmit={apply}>
            <label className="field">
              <span>操作人</span>
              <input
                className="inp"
                value={context.me?.can_manage_users ? actor : (context.viewer ?? "")}
                disabled={!context.me?.can_manage_users}
                maxLength={64}
                onChange={(event) => setActor(event.target.value)}
                placeholder="全部操作人"
              />
            </label>
            <label className="field">
              <span>操作</span>
              <select
                className="inp"
                value={action}
                onChange={(event) => setAction(event.target.value)}
              >
                <option value="">全部操作</option>
                {Object.entries(ACTIONS).map(([kind, label]) => (
                  <option key={kind} value={kind}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span>开始时间（上海）</span>
              <input
                className="inp"
                type="datetime-local"
                value={from}
                onChange={(event) => setFrom(event.target.value)}
              />
            </label>
            <label className="field">
              <span>结束时间（上海）</span>
              <input
                className="inp"
                type="datetime-local"
                value={until}
                onChange={(event) => setUntil(event.target.value)}
              />
            </label>
            <Button type="submit">筛选</Button>
          </form>
          {error ? (
            <p role="alert" className="crit-text">
              {error}
            </p>
          ) : null}
          {audit.error ? (
            <Panel>
              <p role="alert">{roleError(audit.error)}</p>
              <Button
                onClick={() => {
                  setCursors([undefined]);
                  if (!cursor) void audit.refetch();
                }}
              >
                返回第一页
              </Button>
            </Panel>
          ) : audit.isPending ? (
            <SkeletonRows rows={4} />
          ) : !data ? (
            <EmptyState title="记录已更新，请刷新后查看。" />
          ) : data.items.length === 0 ? (
            <EmptyState title="还没有操作记录。" hint="请调整筛选条件，或完成一次操作后再看。" />
          ) : (
            <Panel flush>
              <DataTable
                rows={data.items}
                columns={columns}
                rowKey={(item) => item.command_id}
                label="操作记录"
                onSelect={(item) => setSelected({ identity: context.identity, item })}
                selectedKey={detail?.command_id}
              />
            </Panel>
          )}
          <nav className="collaboration-paging" aria-label="记录翻页">
            <Button
              disabled={cursors.length <= 1 || audit.isFetching}
              onClick={() => setCursors((old) => old.slice(0, -1))}
            >
              上一页
            </Button>
            <span>第 {cursors.length} 页</span>
            <Button
              disabled={!data?.next_cursor || audit.isFetching}
              onClick={() => {
                const next = data?.next_cursor;
                if (next) setCursors((old) => [...old, next]);
              }}
            >
              下一页
            </Button>
          </nav>
        </>
      )}
      <SideDrawer
        open={detail !== null}
        title="操作详情"
        onClose={() => setSelected(null)}
        afterOpenChange={(open) => {
          if (open) return;
          const heading = focus.current?.querySelector<HTMLElement>("h1");
          heading?.setAttribute("tabindex", "-1");
          heading?.focus();
        }}
      >
        {detail ? (
          <dl className="collaboration-details">
            <dt>操作人</dt>
            <dd>{detail.actor_label}</dd>
            <dt>操作</dt>
            <dd>{ACTIONS[detail.command_kind] ?? "其他操作"}</dd>
            <dt>结果</dt>
            <dd>{OUTCOMES[detail.outcome]}</dd>
            <dt>提交时间</dt>
            <dd>
              <RelativeTime at={detail.enqueued_at} />
            </dd>
            <dt>处理时间</dt>
            <dd>{detail.completed_at ? <RelativeTime at={detail.completed_at} /> : "—"}</dd>
            <dt>请求编号</dt>
            <dd className="mono">{detail.command_id}</dd>
            <dt>操作代号</dt>
            <dd className="mono">{detail.command_kind}</dd>
            <dt>正文校验</dt>
            <dd className="mono">{detail.command_hash}</dd>
          </dl>
        ) : null}
      </SideDrawer>
    </div>
  );
}
