import { useState } from "react";
import type { FactorResearchDisplayV2 } from "@/api/factors";
import { formatCount } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Tip } from "@/ui";
import { FactorFieldInfo, factorFieldName } from "./FactorFieldInfo";

type CoverageDay = NonNullable<FactorResearchDisplayV2["daily_feature_coverage_days"]>[number];
type CoverageReason = NonNullable<CoverageDay["counts"][number]["reasons"]>[number]["reason"];

const reasonLabels: Record<CoverageReason, string> = {
  insufficient_window: "窗口不足",
  no_initialization: "缺少初始化历史",
  history_break: "历史断裂",
  missing_observation: "缺少观察记录",
  derived_non_finite: "推导值无效",
  missing_reference_factor: "缺少参考日复权因子",
  non_finite_reference_factor: "参考日复权因子无效",
  non_positive_reference_factor: "参考日复权因子非正数",
  missing_required_factor: "缺少窗口复权因子",
  non_finite_required_factor: "窗口复权因子无效",
  non_positive_required_factor: "窗口复权因子非正数",
  insufficient_history: "观察数不足",
  missing_daily_data: "缺少日线记录",
  undefined_statistic: "统计量无值",
  missing_target_minute: "缺少15:00分钟",
  missing_history: "无历史分钟记录",
  missing_same_minute_history: "无历史同分钟记录",
  zero_same_minute_baseline: "同分钟基准为零",
  zero_cumulative_baseline: "累计基准为零",
  not_applicable: "此口径不适用",
  no_acceleration_history: "无可用加速历史",
};

export function FactorDailyFeatures({ research }: { research: FactorResearchDisplayV2 | null }) {
  const [requestedField, setRequestedField] = useState<string | null>(null);
  const source = research?.daily_features;
  if (!source) return null;
  const hasTechnicalDerived = source.fields.some(
    (field) => field.value_semantics === "history_derived",
  );
  const hasStock = source.fields.some(
    (field) => field.value_semantics === "stock_features_derived",
  );
  const hasMinute = source.fields.some(
    (field) => field.value_semantics === "minute_features_derived",
  );
  const hasDaily = source.fields.some(
    (field) => field.value_semantics !== "minute_features_derived",
  );
  const minute = hasMinute ? source.minute_features : null;
  const hasDerived = hasTechnicalDerived || hasStock;
  const history = hasTechnicalDerived ? source.technical_history : null;
  const stock = hasStock ? source.stock_features : null;
  const hasUnverifiedTechnical = source.fields.some(
    (field) => field.table === "daily_indicator" && field.value_semantics !== "history_derived",
  );
  const otherFields = source.fields.filter(
    (field) =>
      field.value_semantics !== "stock_features_derived" &&
      field.value_semantics !== "minute_features_derived",
  );
  const historyFields = otherFields.filter((field) => field.value_semantics === "history_derived");
  const storedFields = otherFields.filter((field) => field.value_semantics !== "history_derived");
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
        const reasons = [
          ...(item?.reasons ?? []),
          ...(item?.stock_reasons ?? []),
          ...(item?.minute_reasons ?? []),
        ];
        return reasons.length ? (
          <Tip
            className="num"
            content={reasons.map((reason) => (
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
    <section className="factor-result-section" aria-label={hasMinute ? "字段来源" : "日线字段来源"}>
      <div className="factor-section-head">
        <h3>{hasMinute ? (hasDaily ? "字段来源" : "分钟字段") : "日线字段"}</h3>
        {hasDaily ? (
          <Tip
            content={
              <>
                {hasDerived
                  ? "按各字段实际来源展示；缺值不补填。"
                  : "已存日线原值，未重新计算；缺值不补填。"}
                {history?.policy.initialization === "first_valid_observation_no_restart" ? (
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
                {stock ? (
                  <>
                    <div>
                      选股派生：价格窗口含参考日，最近最多 {stock.policy.price_windows.join(" / ")}{" "}
                      个实际观察；短窗口按实际观察数计算。
                    </div>
                    <div>
                      吸筹窗口取参考日前 {stock.policy.accumulation_window} 个观察，不含参考日。
                    </div>
                    <div>
                      均线排列需 {stock.policy.ma_alignment_observations} 个观察，收盘百分位需{" "}
                      {stock.policy.percentile_observations} 个观察。
                    </div>
                    <div>
                      历史起点 {stock.source_history_start ?? "—"}；有日线{" "}
                      {formatCount(stock.codes_with_history)}，无日线{" "}
                      {formatCount(stock.codes_without_history)}。
                    </div>
                  </>
                ) : null}
                {hasUnverifiedTechnical ? (
                  <>
                    <br />
                    指标价格基准与初始化未核验。
                  </>
                ) : null}
                {hasStock ? (
                  <>
                    {historyFields.length ? (
                      <div>
                        历史推导：
                        {historyFields.length === 1
                          ? historyFields[0]?.name_zh
                          : `${formatCount(historyFields.length)}项`}
                        。
                      </div>
                    ) : null}
                    {storedFields.length ? (
                      <div>
                        库存原值：
                        {storedFields.length === 1
                          ? storedFields[0]?.name_zh
                          : `${formatCount(storedFields.length)}项`}
                        。
                      </div>
                    ) : null}
                    {otherFields.length ? <div>各字段名称与口径见覆盖。</div> : null}
                  </>
                ) : (
                  otherFields.map((field) => (
                    <div key={field.column}>
                      {field.name_zh}（
                      {field.value_semantics === "history_derived" ? "历史推导" : "库存原值"}
                      ）：{field.description_zh}
                    </div>
                  ))
                )}
              </>
            }
          >
            日线字段口径
          </Tip>
        ) : null}
        {hasMinute ? (
          <Tip
            content={
              minute ? (
                <>
                  <div>
                    前一交易日精确{minute.policy.panel_clock.slice(0, 5)}
                    ，下一交易日09:25评价，均为上海时间。
                  </div>
                  <div>
                    缺少该分钟不以14:59替代，也不改用其他时点；累计按当日截至目标时点的实际记录计算。
                  </div>
                  <div>
                    历史严格早于字段日期，取此前最多{minute.policy.lookback_days}
                    个实际观察日；短窗口保留实际日数。
                  </div>
                  <div>仅用于历史回顾，不代表当时已知。</div>
                  <div>
                    历史起点 {minute.source_history_start ?? "—"}；有分钟历史{" "}
                    {formatCount(minute.codes_with_history)}，无分钟历史{" "}
                    {formatCount(minute.codes_without_history)}。
                  </div>
                </>
              ) : (
                "本次结果未提供分钟口径。"
              )
            }
          >
            分钟字段口径
          </Tip>
        ) : null}
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
                  {factorFieldName(field)}
                </option>
              ))}
            </select>
          </label>
          <Tip
            content={
              <>
                <div>
                  按本次冻结范围逐日记录；字段取自检验日前一交易日，范围内缺行、空值与非有限数值分别保留，不补零。
                </div>
                {selectedField ? (
                  <div>
                    {factorFieldName(selectedField)}（
                    {selectedField.value_semantics === "minute_features_derived"
                      ? "分钟派生"
                      : selectedField.value_semantics === "stock_features_derived"
                        ? "选股派生"
                        : selectedField.value_semantics === "history_derived"
                          ? "历史推导"
                          : "库存原值"}
                    ）：
                    <FactorFieldInfo field={selectedField} />
                  </div>
                ) : null}
                {selectedField?.value_semantics === "minute_features_derived" &&
                selectedField.unit === "observations" ? (
                  <div>实际历史日数可为0；计数有效不代表历史相对量可用。</div>
                ) : null}
                {selectedField?.column === "signal_opening_segment" ? (
                  <div>目标分钟存在时0有效；开盘段成交额在此时点不适用。</div>
                ) : null}
                {selectedField?.value_semantics === "stock_features_derived" &&
                selectedField.unit === "observations" ? (
                  <div>计数有效不代表窗口可用；其他字段的覆盖与缺因分别保留。</div>
                ) : null}
              </>
            }
          >
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
            label={hasMinute ? "字段覆盖" : "日线字段覆盖"}
          />
        )}
      </details>
    </section>
  );
}
