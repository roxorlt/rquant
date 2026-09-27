import { useState } from "react";
import { ApiError } from "@/api/client";
import {
  type BackfillPlanDetail,
  type BackfillPlanItem,
  useBackfillPlanDetail,
  useBackfillPlans,
} from "@/api/endpoints";
import { formatCount } from "@/format/number";
import { shanghaiDateOf } from "@/format/time";
import { Button, EmptyState, PageSkeleton, Panel, RelativeTime, StatusBadge, Tip } from "@/ui";

interface PagePosition {
  cursors: (string | null)[];
  index: number;
  generation: string | null;
}

const FIRST_PAGE: PagePosition = { cursors: [null], index: 0, generation: null };

function estimatedTime(value: string): string {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds === 0) return "0 分钟";
  const minutes = Math.ceil(seconds / 60);
  if (minutes < 60) return `${minutes} 分钟`;
  const hours = Math.floor(minutes / 60);
  return `${hours} 小时${minutes % 60 ? ` ${minutes % 60} 分钟` : ""}`;
}

function PlanListItem({
  plan,
  active,
  onSelect,
}: {
  plan: BackfillPlanItem;
  active: boolean;
  onSelect: () => void;
}) {
  return (
    <li>
      <button
        className="dc-plan-item"
        type="button"
        aria-label={`第 ${plan.rank + 1} 份计划`}
        aria-current={active ? "true" : undefined}
        onClick={onSelect}
      >
        <span className="dc-plan-item-top">
          <strong>股票日线</strong>
          <span>{plan.rank === 0 ? "最近" : "历史"}</span>
        </span>
        <span className="dc-plan-item-range mono">
          {plan.audit_start} — {plan.completed_through}
        </span>
        <span className="dc-plan-item-foot">
          <span className="num">缺 {formatCount(plan.missing_day_count)} 天</span>
          <span className="mono">{shanghaiDateOf(plan.published_at).slice(5)} 发布</span>
        </span>
      </button>
    </li>
  );
}

function MissingDays({ plan }: { plan: BackfillPlanDetail }) {
  if (plan.missing_day_count === 0) {
    return (
      <EmptyState title="这段时间没有整日缺失" hint="只代表每个交易日有记录，不代表逐股齐全" />
    );
  }
  const byMonth = new Map<string, string[]>();
  for (const date of plan.missing_dates) {
    const month = date.slice(0, 7);
    const dates = byMonth.get(month) ?? [];
    dates.push(date);
    byMonth.set(month, dates);
  }
  return (
    <div className="dc-plan-months">
      {[...byMonth].map(([month, dates]) => {
        const published = plan.monthly.find((item) => item.month.startsWith(month));
        return (
          <section className="dc-plan-month" key={month} aria-label={`${month} 缺失日期`}>
            <div className="dc-plan-month-head">
              <h4>{month}</h4>
              {published ? (
                <span className="num">
                  缺 {formatCount(published.missing_open_days)} /{" "}
                  {formatCount(published.expected_open_days)} 天
                </span>
              ) : null}
            </div>
            <ul className="dc-plan-dates">
              {dates.map((date) => (
                <li className="mono" key={date}>
                  {date}
                </li>
              ))}
            </ul>
          </section>
        );
      })}
    </div>
  );
}

function PlanDetail({ plan }: { plan: BackfillPlanDetail }) {
  const operations = plan.estimate.logical_operations;
  return (
    <div className="dc-plan-detail">
      <div className="dc-plan-detail-head">
        <div>
          <p className="dc-plan-kicker">计划详情</p>
          <h3>股票日线回补</h3>
        </div>
        <Tip
          content={
            <span>
              计划编号：{plan.plan_hash}
              <br />
              来源标记：{plan.snapshot_label}
            </span>
          }
        >
          <span className="dc-plan-tech">核对信息</span>
        </Tip>
      </div>
      <dl className="dc-plan-facts">
        <div>
          <dt>核对区间</dt>
          <dd className="mono">
            {plan.audit_start} — {plan.completed_through}
          </dd>
        </div>
        <div>
          <dt>来源时间</dt>
          <dd>
            <RelativeTime at={plan.cutoff_observed_at} />
          </dd>
        </div>
        <div>
          <dt>缺失交易日</dt>
          <dd className="num">{formatCount(plan.missing_day_count)} 天</dd>
        </div>
        <div>
          <dt>预计耗时</dt>
          <dd className="num">
            <Tip
              content={
                <span>
                  按日线 {formatCount(operations.daily)}、每日指标{" "}
                  {formatCount(operations.daily_basic)}、复权因子{" "}
                  {formatCount(operations.adj_factor)}、名称变更窗口{" "}
                  {formatCount(operations.namechange_windows)}、特殊股票状态最多{" "}
                  {formatCount(operations.stock_st_upper_bound)} 项估算，共{" "}
                  {formatCount(operations.total)} 次逻辑操作。实际请求次数与配额扣额待确认。
                </span>
              }
            >
              <span className="dc-plan-estimate">预计 {estimatedTime(plan.estimated_seconds)}</span>
            </Tip>
          </dd>
        </div>
      </dl>
      <div className="dc-plan-checks">
        <span>
          <StatusBadge state="warn" label="注意" reason="来源身份与采集完成情况尚未核验" />{" "}
          来源待核验
        </span>
        <Tip content="当前只能估算适配器的逻辑操作，无法据此确认真实请求次数和扣额">
          <span>配额待确认</span>
        </Tip>
      </div>
      <p className="dc-plan-scope">
        <Tip content="只核对整个交易日有无日线记录。某日有记录，不代表每只股票的数据都齐全。">
          <span>仅核对整日空洞</span>
        </Tip>
      </p>
      <section className="dc-plan-section" aria-label="缺失交易日">
        <div className="dc-plan-section-head">
          <h3>缺失交易日</h3>
          <span className="num">{formatCount(plan.gap_count)} 段</span>
        </div>
        <MissingDays plan={plan} />
      </section>
      <section className="dc-plan-section" aria-label="任务进度">
        <div className="dc-plan-section-head">
          <h3>任务进度</h3>
        </div>
        <EmptyState title="暂无进度信息" hint="有执行记录后会显示进度与日志" />
      </section>
    </div>
  );
}

export function BackfillPlanPanel() {
  const [page, setPage] = useState<PagePosition>(FIRST_PAGE);
  const [selectedHash, setSelectedHash] = useState<string | null>(null);
  const cursor = page.cursors[page.index] ?? null;
  const plans = useBackfillPlans(cursor, page.generation);
  const generation = page.generation ?? plans.serving?.generation_id ?? null;
  const items = plans.data?.source_state === "ready" ? plans.data.items : [];
  const activeHash = items.some((item) => item.plan_hash === selectedHash)
    ? selectedHash
    : (items[0]?.plan_hash ?? null);
  const selected = items.find((item) => item.plan_hash === activeHash);
  const detail = useBackfillPlanDetail(activeHash, generation);

  function reload() {
    setPage(FIRST_PAGE);
    setSelectedHash(null);
    if (page.index === 0 && page.generation === null) plans.refetch();
  }

  if (plans.isLoading) return <PageSkeleton label="回补计划加载中" />;
  if (plans.error) {
    const changed = plans.error instanceof ApiError && plans.error.status === 409;
    return (
      <Panel
        title="回补计划"
        actions={
          <Button size="sm" onClick={reload}>
            {changed ? "重新加载计划" : "刷新计划"}
          </Button>
        }
      >
        <EmptyState
          title={changed ? "计划列表已更新" : "暂时读不到回补计划"}
          hint={changed ? "重新加载后查看最新计划" : "稍后重试"}
        />
      </Panel>
    );
  }
  if (plans.data?.source_state !== "ready") {
    const state = plans.data?.source_state;
    const title =
      state === "empty"
        ? "还没有回补计划"
        : state === "not_published"
          ? "回补计划尚未发布"
          : "回补计划暂时不可用";
    return (
      <Panel
        title="回补计划"
        actions={
          <Button size="sm" onClick={reload}>
            刷新计划
          </Button>
        }
      >
        <EmptyState
          title={title}
          hint={
            state === "empty"
              ? "生成并发布计划后会在这里显示"
              : state === "not_published"
                ? "发布后会在这里显示"
                : "稍后刷新页面再试"
          }
        />
      </Panel>
    );
  }

  return (
    <Panel
      title="回补计划"
      sub={`共 ${formatCount(plans.data.total ?? items.length)} 份`}
      actions={
        <Button size="sm" onClick={reload}>
          刷新计划
        </Button>
      }
      flush
    >
      <div className="dc-plan-layout">
        <section className="dc-plan-list" aria-label="计划列表">
          <ul>
            {items.map((item) => (
              <PlanListItem
                key={item.plan_hash}
                plan={item}
                active={item.plan_hash === activeHash}
                onSelect={() => setSelectedHash(item.plan_hash)}
              />
            ))}
          </ul>
          <div className="dc-plan-pager">
            <Button
              size="sm"
              variant="ghost"
              disabled={page.index === 0}
              onClick={() => setPage((previous) => ({ ...previous, index: previous.index - 1 }))}
            >
              上一页
            </Button>
            <span className="num">第 {page.index + 1} 页</span>
            <Button
              size="sm"
              variant="ghost"
              disabled={!plans.data.next_cursor}
              onClick={() => {
                const next = plans.data?.next_cursor;
                if (!next || !generation) return;
                setPage((previous) => ({
                  cursors: [...previous.cursors.slice(0, previous.index + 1), next],
                  index: previous.index + 1,
                  generation,
                }));
              }}
            >
              下一页
            </Button>
          </div>
        </section>
        <section className="dc-plan-preview" aria-label="计划详情">
          {activeHash === null ? (
            <EmptyState title="这页没有计划" hint="刷新计划后再试" />
          ) : detail.isLoading ? (
            <PageSkeleton label="计划详情加载中" />
          ) : detail.error ? (
            <EmptyState
              title={
                detail.error instanceof ApiError && detail.error.status === 404
                  ? "这份计划已不在当前列表"
                  : detail.error instanceof ApiError && detail.error.status === 409
                    ? "计划详情已更新"
                    : "暂时读不到计划详情"
              }
              hint="刷新计划后重试"
            />
          ) : detail.data?.source_state === "ready" && detail.data.plan ? (
            <>
              {selected && selected.rank > 0 ? <p className="dc-plan-older">历史计划</p> : null}
              <PlanDetail plan={detail.data.plan} />
            </>
          ) : (
            <EmptyState title="计划详情暂时不可用" hint="刷新计划后重试" />
          )}
        </section>
      </div>
    </Panel>
  );
}
