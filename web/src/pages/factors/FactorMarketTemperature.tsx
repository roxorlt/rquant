import type { FactorResearchDisplayV2 } from "@/api/factors";
import { formatPercent } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Tip } from "@/ui";

type Source = NonNullable<FactorResearchDisplayV2["daily_features"]>;
type CoverageDay = NonNullable<FactorResearchDisplayV2["daily_feature_coverage_days"]>[number];
type MarketValue = NonNullable<CoverageDay["market_temperature_values"]>[number];

export const marketTemperatureReasonLabels: Record<NonNullable<MarketValue["reason"]>, string> = {
  missing_market_temperature: "缺少市场温度记录",
  market_temperature_null: "市场温度为空",
  market_temperature_non_finite: "市场温度数值无效",
  invalid_market_percentage: "市场温度超出0–100%",
};

function usable(value: MarketValue | undefined): value is MarketValue & { value: number } {
  return (
    value?.status === "valid" &&
    value.value !== null &&
    Number.isFinite(value.value) &&
    value.value >= 0 &&
    value.value <= 100
  );
}

function missingReason(value: MarketValue | undefined): string {
  if (!value) return "本次结果未提供该市场日值。";
  if (value.reason) return marketTemperatureReasonLabels[value.reason];
  if (value.value !== null && !Number.isFinite(value.value))
    return marketTemperatureReasonLabels.market_temperature_non_finite;
  if (value.value !== null && (value.value < 0 || value.value > 100))
    return marketTemperatureReasonLabels.invalid_market_percentage;
  if (value.status === "missing") return marketTemperatureReasonLabels.missing_market_temperature;
  if (value.status === "non_finite")
    return marketTemperatureReasonLabels.market_temperature_non_finite;
  return marketTemperatureReasonLabels.market_temperature_null;
}

export function FactorMarketTemperatureBasis({ source }: { source: Source }) {
  return (
    <Tip
      content={
        source.market_temperature ? (
          <>
            <div>全市场日值，同一天所有股票使用同一值，不按所选股票池重算。</div>
            <div>取前一完整SSE开市日，以检验日09:25回顾；字段日期见各日数值。</div>
            <div>百分比原值，不乘100；0和100有效。缺行、空值及无效值不补零，也不借邻日填值。</div>
            <div>仅用于历史回顾，不代表当时已知。</div>
          </>
        ) : (
          "本次结果未提供市场温度口径。"
        )
      }
    >
      市场温度口径
    </Tip>
  );
}

export function FactorMarketTemperature({
  source,
  days,
}: {
  source: Source;
  days: FactorResearchDisplayV2["daily_feature_coverage_days"];
}) {
  const fields = source.fields.filter(
    (field) => field.value_semantics === "market_temperature_stored",
  );
  const columns: DataColumn<CoverageDay>[] = [
    {
      id: "date",
      header: "检验日期",
      value: (day) => day.trade_date,
      cell: (day) => <span className="num">{day.trade_date}</span>,
    },
    ...fields.map(
      (field): DataColumn<CoverageDay> => ({
        id: field.column,
        header: field.name_zh,
        numeric: true,
        value: (day) => {
          const value = day.market_temperature_values?.find((item) => item.column === field.column);
          return usable(value) ? value.value : null;
        },
        cell: (day) => {
          const value = day.market_temperature_values?.find((item) => item.column === field.column);
          const valid = usable(value);
          return (
            <Tip
              className="num"
              content={
                <>
                  <div>字段日期：{day.panel_date}（前一完整SSE开市日）。</div>
                  <div>{valid ? `全市场日值；原值：${value.value}%。` : missingReason(value)}</div>
                </>
              }
            >
              {valid ? formatPercent(value.value) : "—"}
            </Tip>
          );
        },
      }),
    ),
  ];
  return (
    <details className="factor-disclosure">
      <summary>查看市场温度</summary>
      {!days?.length ? (
        <EmptyState title="暂无市场温度" hint="当前结果未提供市场日值记录。" />
      ) : (
        <DataTable
          rows={[...days].sort((a, b) => b.trade_date.localeCompare(a.trade_date))}
          columns={columns}
          rowKey={(day) => day.trade_date}
          label="市场温度日值"
        />
      )}
    </details>
  );
}
