import { useState } from "react";
import type { FactorResearchDisplayV2 } from "@/api/factors";
import { formatCount } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Tip } from "@/ui";

type CoverageDay = NonNullable<FactorResearchDisplayV2["daily_feature_coverage_days"]>[number];

export function FactorDailyFeatures({ research }: { research: FactorResearchDisplayV2 | null }) {
  const [requestedField, setRequestedField] = useState<string | null>(null);
  const source = research?.daily_features;
  if (!source) return null;
  const selectedField =
    source.fields.find((field) => field.column === requestedField) ?? source.fields[0];
  const days = research?.daily_feature_coverage_days;
  const count = (day: CoverageDay) =>
    day.counts.find((item) => item.column === selectedField?.column);
  const columns: DataColumn<CoverageDay>[] = [
    {
      id: "date",
      header: "检验日期",
      value: (day) => day.trade_date,
      cell: (day) => <span className="num">{day.trade_date}</span>,
    },
    {
      id: "panel",
      header: "字段日期",
      value: (day) => day.panel_date,
      cell: (day) => <span className="num">{day.panel_date}</span>,
    },
    {
      id: "valid",
      header: "有效 / 范围",
      value: (day) => count(day)?.valid ?? null,
      numeric: true,
      cell: (day) => (
        <span className="num">
          {formatCount(count(day)?.valid)} / {formatCount(day.computation_stock_count)}
        </span>
      ),
    },
    {
      id: "missing",
      header: "缺行",
      value: (day) => count(day)?.missing ?? null,
      numeric: true,
      cell: (day) => <span className="num">{formatCount(count(day)?.missing)}</span>,
    },
    {
      id: "null",
      header: "空值",
      value: (day) => count(day)?.null ?? null,
      numeric: true,
      cell: (day) => <span className="num">{formatCount(count(day)?.null)}</span>,
    },
    {
      id: "nonfinite",
      header: "无效值",
      value: (day) => count(day)?.non_finite ?? null,
      numeric: true,
      cell: (day) => <span className="num">{formatCount(count(day)?.non_finite)}</span>,
    },
  ];
  return (
    <section className="factor-result-section" aria-label="日线字段来源">
      <div className="factor-section-head">
        <h3>日线字段</h3>
        <Tip
          content={
            <>
              已存日线原值，未重新计算；缺值不补填。
              {source.price_basis === "unverified" ||
              source.recursive_initialization === "unverified" ? (
                <>
                  <br />
                  指标价格基准与初始化未核验。
                </>
              ) : null}
              {source.fields.map((field) => (
                <div key={field.column}>
                  {field.name_zh}：{field.description_zh}
                </div>
              ))}
            </>
          }
        >
          日线字段口径
        </Tip>
      </div>
      <details className="factor-disclosure">
        <summary>查看字段覆盖</summary>
        <div className="factor-daily-coverage-head">
          <label>
            <span>字段</span>
            <select
              className="inp"
              aria-label="覆盖字段"
              value={selectedField?.column ?? ""}
              onChange={(event) => setRequestedField(event.target.value)}
            >
              {source.fields.map((field) => (
                <option key={field.column} value={field.column}>
                  {field.name_zh}
                </option>
              ))}
            </select>
          </label>
          <Tip content="按本次冻结范围逐日记录；字段取自检验日前一交易日，范围内缺行、空值与非有限数值分别保留，不补零。">
            覆盖说明
          </Tip>
        </div>
        {!days || days.length === 0 ? (
          <EmptyState title="暂无字段覆盖" hint="当前结果未提供字段覆盖记录。" />
        ) : (
          <DataTable
            rows={[...days].sort((a, b) => b.trade_date.localeCompare(a.trade_date))}
            columns={columns}
            rowKey={(day) => day.trade_date}
            label="日线字段覆盖"
          />
        )}
      </details>
    </section>
  );
}
