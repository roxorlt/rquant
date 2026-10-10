import type { ReactNode } from "react";
import { type PortfolioRows, usePortfolioRows } from "@/api/backtests";
import { toneOf } from "@/format/color";
import { Button, EmptyState, PageSkeleton, Tip } from "@/ui";
import { portfolioPercent } from "./portfolioFormat";
import "./portfolioMonthlyHeatmap.css";

type Month = PortfolioRows["monthly"][number];
const months = Array.from({ length: 12 }, (_, index) => index + 1);

function windowBounds(startDate: string, endDate: string) {
  const dates = [startDate, endDate].map((value) => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
    const stamp = Date.parse(`${value}T00:00:00Z`);
    return Number.isFinite(stamp) && new Date(stamp).toISOString().slice(0, 10) === value
      ? stamp
      : null;
  });
  const start = dates[0];
  const end = dates[1];
  if (start == null || end == null || end < start || (end - start) / 86_400_000 + 1 > 5 * 366)
    return null;
  const firstYear = Number(startDate.slice(0, 4));
  const lastYear = Number(endDate.slice(0, 4));
  return {
    firstYear,
    lastYear,
    firstMonth: firstYear * 12 + Number(startDate.slice(5, 7)),
    lastMonth: lastYear * 12 + Number(endDate.slice(5, 7)),
    maxRows: (lastYear - firstYear + 1) * 12,
  };
}

function validPage(
  data: PortfolioRows | undefined,
  generation: string | null | undefined,
  expected: string | null,
  offset: number,
  maxRows: number,
): boolean {
  if (
    data === undefined ||
    expected === null ||
    data.result_hash !== expected ||
    generation !== expected ||
    data.view !== "monthly" ||
    !Number.isInteger(data.total) ||
    data.total < offset ||
    data.total > maxRows ||
    data.monthly.length !== Math.min(50, data.total - offset)
  )
    return false;
  const next = offset + data.monthly.length;
  return data.next_offset === (next < data.total ? next : null);
}

export function PortfolioMonthlyHeatmap({
  jobId,
  resultHash,
  startDate,
  endDate,
}: {
  jobId: string | null;
  resultHash: string | null;
  startDate: string;
  endDate: string;
}) {
  const bounds = windowBounds(startDate, endDate);
  const first = usePortfolioRows(bounds ? jobId : null, resultHash, "monthly", 0);
  const firstValid = validPage(
    first.data,
    first.serving?.generation_id,
    resultHash,
    0,
    bounds?.maxRows ?? 0,
  );
  const needsSecond = firstValid && first.data?.next_offset === 50;
  const second = usePortfolioRows(needsSecond ? jobId : null, resultHash, "monthly", 50);
  const secondValid =
    !needsSecond ||
    (validPage(second.data, second.serving?.generation_id, resultHash, 50, bounds?.maxRows ?? 0) &&
      second.data?.total === first.data?.total);
  const loading = first.isLoading || (needsSecond && second.isLoading);
  const values =
    firstValid && secondValid
      ? [...(first.data?.monthly ?? []), ...(needsSecond ? (second.data?.monthly ?? []) : [])]
      : null;
  const seen = new Set<string>();
  const validValues =
    bounds !== null &&
    values !== null &&
    values.length === first.data?.total &&
    values.every((row) => {
      const key = `${row.year}-${row.month}`;
      const date = row.year * 12 + row.month;
      const valid =
        Number.isInteger(row.year) &&
        row.year >= bounds.firstYear &&
        row.year <= bounds.lastYear &&
        Number.isInteger(row.month) &&
        row.month >= 1 &&
        row.month <= 12 &&
        !seen.has(key) &&
        (row.return_rate === null || Number.isFinite(row.return_rate)) &&
        (row.return_rate === null || (date >= bounds.firstMonth && date <= bounds.lastMonth));
      seen.add(key);
      return valid;
    });
  let content: ReactNode;
  if (loading) content = <PageSkeleton />;
  else if (!validValues || first.error || (needsSecond && second.error))
    content = (
      <EmptyState
        title="月度数据暂不可用"
        hint={
          <Button
            size="sm"
            onClick={() => {
              first.refetch();
              if (needsSecond) second.refetch();
            }}
          >
            重新加载月度
          </Button>
        }
      />
    );
  else if (values.length === 0) content = <EmptyState title="暂无月度数据" />;
  else {
    const byMonth = new Map<string, Month>(values.map((row) => [`${row.year}-${row.month}`, row]));
    const years = [...new Set(values.map((row) => row.year))].sort((left, right) => right - left);
    content = (
      <>
        <div className="pbm-columns" aria-hidden="true">
          <span />
          <div className="pbm-months">
            {months.map((month) => (
              <span key={month}>{month}月</span>
            ))}
          </div>
        </div>
        {years.map((year) => (
          <div className="pbm-year" key={year}>
            <h4 className="num">{year}</h4>
            <div className="pbm-months">
              {months.map((month) => {
                const record = byMonth.get(`${year}-${month}`);
                const inPeriod =
                  year * 12 + month >= bounds.firstMonth && year * 12 + month <= bounds.lastMonth;
                const value = inPeriod ? (record?.return_rate ?? null) : null;
                const detail = !inPeriod
                  ? "不在本次区间"
                  : value === null
                    ? "暂无月收益"
                    : `月收益 ${portfolioPercent(value)}`;
                return (
                  <Tip interactive content={`${year}年${month}月 · ${detail}`} key={month}>
                    <button
                      type="button"
                      className="pbm-cell"
                      data-tone={value === null ? "unknown" : toneOf(value)}
                      aria-label={`${year}年${month}月，${detail}`}
                    >
                      <span className="pbm-cell-month" aria-hidden="true">
                        {month}月
                      </span>
                      <span className="num">{portfolioPercent(value)}</span>
                    </button>
                  </Tip>
                );
              })}
            </div>
          </div>
        ))}
      </>
    );
  }
  return (
    <fieldset className="pbm" aria-label="月度热力">
      <div className="pbm-heading">
        <h3>月度热力</h3>
        <div className="pbm-legend">
          <span className="up">正收益</span>
          <span className="down">负收益</span>
        </div>
      </div>
      {content}
    </fieldset>
  );
}
