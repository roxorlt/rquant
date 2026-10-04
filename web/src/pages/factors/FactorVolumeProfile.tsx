import { useState } from "react";
import type { FactorResearchDisplayV2 } from "@/api/factors";
import { formatCount, formatNumber } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Tip } from "@/ui";
import { FactorFieldInfo } from "./FactorFieldInfo";

type Source = NonNullable<FactorResearchDisplayV2["daily_features"]>;
type Day = NonNullable<FactorResearchDisplayV2["daily_feature_coverage_days"]>[number];
type Stock = NonNullable<Day["volume_profile_values"]>[number];
type Value = Stock["values"][number];

export const volumeProfileReasonLabels: Record<NonNullable<Value["reason"]>, string> = {
  no_trading_dates: "没有窗口日期",
  missing_or_invalid_reference_price: "参考日收盘价缺失或无效",
  missing_minute_data: "没有窗口分钟记录",
  missing_reference_factor: "缺少参考日复权因子",
  non_finite_reference_factor: "参考日复权因子无效",
  non_positive_reference_factor: "参考日复权因子非正数",
  missing_required_factor: "缺少窗口复权因子",
  non_finite_required_factor: "窗口复权因子无效",
  non_positive_required_factor: "窗口复权因子非正数",
  unmapped_price_basis_ratio: "分钟复权比例缺失",
  empty_price_bins: "没有可用价格桶",
  non_positive_profile_totals: "成交量或成交额总和非正数",
  volume_profile_non_finite: "成交分布数值无效",
  invalid_volume_profile_data: "分钟数据无法计算成交分布",
};

export function FactorVolumeProfileBasis({ source }: { source: Source }) {
  return (
    <Tip
      content={
        source.volume_profile ? (
          <>
            <div>
              沿用原90日分钟成交分布。窗口取严格早于参考日的最近90个实际日线日期；参考日为检验日前一完整开市日，不使用参考日或未来分钟。
            </div>
            <div>
              分钟价格取正成交额除以正股数，否则取收盘价；这是分钟成交近似。价格按参考日复权，股数逆向换算，成交额保留原始元值。
            </div>
            <div>
              保留0.5%分桶、峰值并列和连续70%价值区规则。覆盖显示实际有分钟的窗口日数；不足90日仍按可用记录计算，缺失输出不补零。
            </div>
            <div>仅用于历史回顾，没有原始首次观察证明；字段可用性仍按各次结果核对。</div>
            {source.volume_profile.lake?.catalog_status === "degraded" ? (
              <div>研究来源有质量缺口；仅核验具名分钟分区和当前记录，不代表整体生产权威。</div>
            ) : null}
          </>
        ) : (
          "当前结果未提供成交分布口径。"
        )
      }
    >
      成交分布口径
    </Tip>
  );
}

export function FactorVolumeProfile({
  source,
  days,
}: {
  source: Source;
  days: FactorResearchDisplayV2["daily_feature_coverage_days"];
}) {
  const [requestedDate, setRequestedDate] = useState<string | null>(null);
  const [requestedField, setRequestedField] = useState<string | null>(null);
  const available = [...(days ?? [])]
    .filter((day) => day.volume_profile_values?.length)
    .sort((a, b) => b.trade_date.localeCompare(a.trade_date));
  const day = available.find((item) => item.trade_date === requestedDate) ?? available[0];
  const fields = source.fields.filter(
    (field) => field.value_semantics === "volume_profile_derived",
  );
  const field =
    fields.find((item) => item.column === requestedField) ??
    fields.find((item) => item.column === "vp90_vwap") ??
    fields[0];
  const columns: DataColumn<Stock>[] = [
    {
      id: "code",
      header: "股票",
      value: (row) => row.stock_code,
      cell: (row) => <span className="mono">{row.stock_code}</span>,
    },
    {
      id: "value",
      header: field?.name_zh ?? "原值",
      numeric: true,
      value: (row) => row.values.find((v) => v.column === field?.column)?.value ?? null,
      cell: (row) => {
        const value = row.values.find((item) => item.column === field?.column);
        const valid =
          value?.status === "valid" && value.value !== null && Number.isFinite(value.value);
        const unit = field?.unit === "shares" ? "股" : field?.unit === "percent" ? "%" : "元";
        return (
          <Tip
            className="num"
            content={
              <>
                <div>{field ? <FactorFieldInfo field={field} /> : null}</div>
                <div>
                  {valid
                    ? `原值：${value.value}${unit}。`
                    : value?.reason
                      ? volumeProfileReasonLabels[value.reason]
                      : "当前示例未提供该原值。"}
                </div>
                <div>参考日期：{row.diagnostic.reference_date}。</div>
                {row.diagnostic.unavailable_factor_dates?.length ? (
                  <div>
                    缺失或无效复权日期：{row.diagnostic.unavailable_factor_dates.join("、")}。
                  </div>
                ) : null}
              </>
            }
          >
            {valid
              ? field?.unit === "shares"
                ? formatNumber(value.value, 2)
                : `${formatNumber(value.value, 2)}${field?.unit === "percent" ? "%" : ""}`
              : "—"}
          </Tip>
        );
      },
    },
    {
      id: "coverage",
      header: "已用日期",
      numeric: true,
      value: (row) => row.diagnostic.observed_days,
      cell: (row) => (
        <Tip
          className="num"
          content={
            <>
              <div>
                窗口有{row.diagnostic.window_days}个实际日线日期，其中{row.diagnostic.observed_days}
                日有分钟记录，共{formatCount(row.diagnostic.minute_rows)}条。
              </div>
              <div>
                窗口：{row.diagnostic.window_start_date ?? "—"} 至{" "}
                {row.diagnostic.window_end_date ?? "—"}；分钟覆盖不代表每分钟完整。
              </div>
              {row.diagnostic.outside_window_days ? (
                <div>
                  原日期范围还含{row.diagnostic.outside_window_days}
                  个非日线日期的分钟日，按原算法保留并单独记录。
                </div>
              ) : null}
            </>
          }
        >
          {row.diagnostic.observed_days} / 90 日
        </Tip>
      ),
    },
  ];
  return (
    <details className="factor-disclosure">
      <summary>成交分布示例（{formatCount(day?.volume_profile_values?.length ?? 0)}只）</summary>
      {!day ? (
        <EmptyState title="暂无原值示例" hint="当前结果未提供成交分布示例；覆盖统计仍见下方。" />
      ) : (
        <>
          <div className="factor-daily-coverage-head">
            <label>
              <span>检验日期</span>
              <select
                className="inp"
                aria-label="成交分布检验日期"
                value={day.trade_date}
                onChange={(event) => setRequestedDate(event.target.value)}
              >
                {available.map((item) => (
                  <option key={item.trade_date} value={item.trade_date}>
                    {item.trade_date}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>字段</span>
              <select
                className="inp"
                aria-label="成交分布字段"
                value={field?.column ?? ""}
                onChange={(event) => setRequestedField(event.target.value)}
              >
                {fields.map((item) => (
                  <option key={item.column} value={item.column}>
                    {item.name_zh}
                  </option>
                ))}
              </select>
            </label>
            <Tip
              content={
                <>
                  展示最近最多32个检验日、每日至多10只股票的原值示例。完整范围为
                  {formatCount(day.computation_stock_count)}
                  只，已全部参与计算；覆盖与缺因按完整范围统计。
                </>
              }
            >
              示例说明
            </Tip>
          </div>
          <DataTable
            rows={day.volume_profile_values ?? []}
            columns={columns}
            rowKey={(row) => row.stock_code}
            label="成交分布原值示例"
          />
        </>
      )}
    </details>
  );
}
