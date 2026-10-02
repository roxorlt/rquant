import { useState } from "react";
import type { FactorResearchDisplayV2 } from "@/api/factors";
import { formatCount } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Tip } from "@/ui";

type CoverageDay = NonNullable<FactorResearchDisplayV2["daily_feature_coverage_days"]>[number];
type CoverageReason = NonNullable<CoverageDay["counts"][number]["reasons"]>[number]["reason"];

const reasonLabels: Record<CoverageReason, string> = {
  insufficient_window: "窗口不足",
  no_initialization: "缺少初始化历史",
  history_break: "历史断裂",
  missing_observation: "缺少观察记录",
  derived_non_finite: "推导值无效",
};

export function FactorDailyFeatures({ research }: { research: FactorResearchDisplayV2 | null }) {
  const [requestedField, setRequestedField] = useState<string | null>(null);
  const source = research?.daily_features;
  if (!source) return null;
  const hasDerived = source.fields.some((field) => field.value_semantics === "history_derived");
  const hasTechnical = source.fields.some((field) => field.table === "daily_indicator");
  const history = hasDerived ? source.technical_history : null;
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
      cell: (day) => {
        const item = count(day);
        const value = `${formatCount(item?.valid)} / ${formatCount(day.computation_stock_count)}`;
        return item?.reasons?.length ? (
          <Tip
            className="num"
            content={item.reasons.map((reason) => (
              <div key={reason.reason}>
                {reasonLabels[reason.reason]}：{formatCount(reason.count)}
              </div>
            ))}
          >
            {value}
          </Tip>
        ) : (
          <span className="num">{value}</span>
        );
      },
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
              {hasDerived
                ? "按各字段实际来源展示；缺值不补填。"
                : "已存日线原值，未重新计算；缺值不补填。"}
              {hasDerived &&
              source.recursive_initialization === "first_valid_observation_no_restart" ? (
                <div>从首个有效历史观察初始化；历史断裂后不重新初始化，不补K线。</div>
              ) : null}
              {history ? (
                <div>
                  历史起点 {history.source_history_start ?? "—"}；已初始化{" "}
                  {formatCount(history.initialized_codes)}，未初始化{" "}
                  {formatCount(history.uninitialized_codes)}，历史断裂{" "}
                  {formatCount(history.broken_codes)}。
                </div>
              ) : null}
              {hasTechnical &&
              (source.price_basis === "unverified" ||
                source.recursive_initialization === "unverified") ? (
                <>
                  <br />
                  指标价格基准与初始化未核验。
                </>
              ) : null}
              {source.fields.map((field) => (
                <div key={field.column}>
                  {field.name_zh}（
                  {field.value_semantics === "history_derived" ? "历史推导" : "库存原值"}
                  ）：{field.description_zh}
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
