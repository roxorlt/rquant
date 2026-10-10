import { type FinancialSummary, useFinancialSummary } from "@/api/financialSummary";
import { formatCount } from "@/format/number";
import { Button, EmptyState, PageSkeleton, RelativeTime, Tip } from "@/ui";
import { BackfillExecutionPanel } from "./BackfillExecutionPanel";
import type { DataCenterCommandSession } from "./dataCenterCommandSession";

type FinancialField = FinancialSummary["fields"][number];

const FIELD_EXPLANATIONS: Record<FinancialField["key"], string> = {
  pe_ttm: "当前价格与最近四季每股收益的比值。亏损或证据不足时保持未知。",
  pb: "当前价格与最近一期每股净资产的比值。公告可见后才计入。",
  dv_ttm: "最近四季股息与当前价格的比率。缺少可信分红记录时保持未知。",
  roe: "最近可见报告期的净资产收益率。新报告未披露时不提前使用。",
  or_yoy: "最近可见报告期营业收入的同比变化。缺值时不回退旧报告。",
  netprofit_yoy: "最近可见报告期归母净利润的同比变化。缺值时不回退旧报告。",
};

function FinancialFieldCard({ field }: { field: FinancialField }) {
  const reasons = field.reasons.length
    ? field.reasons.map((reason) => `${reason.label} ${formatCount(reason.count)}`).join("；")
    : "暂无缺数原因";
  return (
    <li className="dc-fin-field">
      <div className="dc-fin-field-head">
        <Tip content={FIELD_EXPLANATIONS[field.key]}>
          <strong>{field.label}</strong>
        </Tip>
        <span className="dc-fin-unit">{field.unit}</span>
      </div>
      <dl className="dc-fin-field-counts">
        <div>
          <dt>有值</dt>
          <dd className="num">{formatCount(field.known_count)}</dd>
        </div>
        <div>
          <dt>
            <Tip content={reasons}>未知</Tip>
          </dt>
          <dd className="num">{formatCount(field.unknown_count)}</dd>
        </div>
      </dl>
    </li>
  );
}

function UnavailableSummary({ data }: { data: FinancialSummary }) {
  const copy = (
    {
      not_configured: ["财务数据尚未接入", "接入只读数据后会在这里显示。"],
      calendar_unavailable: ["交易日历待核验", "完成核验后再显示记录。"],
      no_records: ["该日暂无可核验记录", "有合格记录后会在这里显示。"],
      ready: ["暂时读不到财务数据", "请刷新后重试。"],
    } as const
  )[data.status];
  return (
    <div className="dc-fin-empty">
      {data.decision_date ? (
        <p className="dc-fin-empty-date">
          记录核对日 <span className="mono">{data.decision_date}</span>
        </p>
      ) : null}
      {data.waiting_for_today ? <span className="dc-fin-wait">等待今日 17:00</span> : null}
      <EmptyState title={copy[0]} hint={copy[1]} />
      {data.source ? (
        <p className="dc-fin-empty-source">
          <span>副本同步时间</span> <RelativeTime at={data.source.updated_at} />
        </p>
      ) : null}
    </div>
  );
}

export function FinancialPanel({
  executionSession,
}: {
  executionSession?: DataCenterCommandSession;
} = {}) {
  const summary = useFinancialSummary();
  const data = summary.phase === "ready" ? summary.data : null;
  return (
    <section className="dc-financial" aria-label="财务概况">
      <BackfillExecutionPanel mode="financial" executionSession={executionSession} />
      <div className="dc-fin-heading">
        <div>
          <p className="dc-fin-eyebrow">数据中心 / 财务</p>
          <h2>财务概况</h2>
        </div>
        <Button
          size="sm"
          variant="ghost"
          aria-label="刷新财务数据"
          onClick={() => void summary.refresh()}
        >
          刷新
        </Button>
      </div>
      {summary.phase === "loading" ? (
        <PageSkeleton label="财务概况加载中" />
      ) : summary.phase === "error" ? (
        <div className="dc-fin-empty">
          <EmptyState title="暂时读不到财务数据" hint="请稍后刷新重试。" />
        </div>
      ) : data?.status !== "ready" ? (
        data ? (
          <UnavailableSummary data={data} />
        ) : null
      ) : (
        <>
          <div className="dc-fin-summary">
            <div className="dc-fin-decision">
              <span className="dc-fin-label">记录核对日</span>
              <strong className="mono">{data.decision_date}</strong>
              {data.waiting_for_today ? <span className="dc-fin-wait">等待今日 17:00</span> : null}
            </div>
            <div className="dc-fin-records">
              <span className="dc-fin-label">已核验记录</span>
              <strong className="num">{formatCount(data.record_count)}</strong>
              <span className="dc-fin-count-unit">只股票</span>
            </div>
          </div>
          <div className="dc-fin-provenance">
            <span>
              副本同步时间 <RelativeTime at={data.source?.updated_at} />
            </span>
            <Tip content="这里只统计已核验的记录，不能据此判断全市场是否齐全。">
              <span>{data.coverage_note}</span>
            </Tip>
          </div>
          <ul className="dc-fin-fields" aria-label="财务字段记录数">
            {data.fields.map((field) => (
              <FinancialFieldCard key={field.key} field={field} />
            ))}
          </ul>
        </>
      )}
    </section>
  );
}
