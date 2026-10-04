import { useState } from "react";
import type { FactorResearchDisplayV2 } from "@/api/factors";
import { formatCount, formatNumber } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Tip } from "@/ui";

type Source = NonNullable<FactorResearchDisplayV2["daily_features"]>;
type Day = NonNullable<FactorResearchDisplayV2["daily_feature_coverage_days"]>[number];
type Stock = NonNullable<Day["auction_values"]>[number];
type Value = Stock["values"][number];

export const auctionReasonLabels: Record<NonNullable<Value["reason"]>, string> = {
  missing_board_membership: "缺少题材成员记录",
  missing_board_auction: "缺少题材竞价",
  missing_previous_close: "缺少昨收",
  missing_auction_history: "缺少竞价历史",
  zero_auction_baseline: "竞价历史基准为零",
  auction_null: "竞价值为空",
  auction_non_finite: "竞价值无效",
  invalid_auction_price: "竞价价格无效",
  invalid_auction_amount: "竞价金额无效",
  invalid_previous_close: "昨收无效",
};

export function FactorAuctionBasis({ source }: { source: Source }) {
  return (
    <Tip
      content={
        source.auction ? (
          <>
            <div>
              原题材完整成员用于计算，不按所选股票池重算。金额比最高的题材用于该股；并列取原输入中的第一个。
            </div>
            <div>
              字段取前一完整SSE开市日，下一日09:25检验；该字段日按09:30截止计算。官方竞价09:26可见，当日分钟回补09:31才可见，不能提前使用。
            </div>
            <div>
              成员取字段日前30日内最近记录；历史金额固定当前成员，先取最近最多20个实际竞价日期，再丢空值日取中位数。短窗口按实际日数计算。
            </div>
            <div>
              昨收取日线全局最近日期，严格早于字段日。金额比为倍数，占比为0–1原值，成员数为家数；合法零不当缺值。
            </div>
            <div>
              仅为历史回顾，没有原始首次观察证明，不代表当时已采集。缺值保留原因，不补零、不借邻日填值。
            </div>
            {source.auction.lake?.catalog_status === "degraded" ? (
              <div>研究来源有质量缺口；仅核验具名分区和当前记录，不代表整体生产权威。</div>
            ) : null}
          </>
        ) : (
          "当前结果未提供竞价字段口径。"
        )
      }
    >
      竞价字段口径
    </Tip>
  );
}

export function FactorAuction({
  source,
  days,
}: {
  source: Source;
  days: FactorResearchDisplayV2["daily_feature_coverage_days"];
}) {
  const [requestedDate, setRequestedDate] = useState<string | null>(null);
  const available = [...(days ?? [])]
    .filter((day) => day.auction_values?.length)
    .sort((a, b) => b.trade_date.localeCompare(a.trade_date));
  const day = available.find((item) => item.trade_date === requestedDate) ?? available[0];
  const fields = source.fields.filter((field) => field.value_semantics === "auction_derived");
  const columns: DataColumn<Stock>[] = [
    {
      id: "code",
      header: "股票",
      value: (row) => row.stock_code,
      cell: (row) => <span className="mono">{row.stock_code}</span>,
    },
    ...fields.map(
      (field): DataColumn<Stock> => ({
        id: field.column,
        header: field.name_zh,
        numeric: true,
        value: (row) => row.values.find((value) => value.column === field.column)?.value ?? null,
        cell: (row) => {
          const value = row.values.find((item) => item.column === field.column);
          const valid =
            value?.status === "valid" && value.value !== null && Number.isFinite(value.value);
          const unit =
            field.column === "board_member_count"
              ? "家"
              : field.column === "board_gap_up_ratio"
                ? "（0–1）"
                : "倍";
          return (
            <Tip
              className="num"
              content={
                <>
                  <div>
                    字段日期：{day?.panel_date ?? "—"}；题材：{row.diagnostic.board_name ?? "—"}。
                  </div>
                  <div>
                    {valid
                      ? `原值：${value.value}${unit}。`
                      : value?.reason
                        ? auctionReasonLabels[value.reason]
                        : "当前示例未提供该原值。"}
                  </div>
                  <div>
                    成员日期：{row.diagnostic.membership_date ?? "—"}；昨收日期：
                    {row.diagnostic.previous_close_date ?? "—"}；实际历史日数：
                    {row.diagnostic.historical_observation_days}。
                  </div>
                </>
              }
            >
              {valid
                ? field.column === "board_member_count"
                  ? formatCount(value.value)
                  : formatNumber(value.value, 4)
                : "—"}
            </Tip>
          );
        },
      }),
    ),
  ];
  return (
    <details className="factor-disclosure">
      <summary>原值示例（{formatCount(day?.auction_values?.length ?? 0)}只）</summary>
      {!day ? (
        <EmptyState title="暂无原值示例" hint="当前结果未提供竞价原值示例；覆盖统计仍见下方。" />
      ) : (
        <>
          <div className="factor-daily-coverage-head">
            <label>
              <span>检验日期</span>
              <select
                className="inp"
                aria-label="原值检验日期"
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
            <Tip
              content={
                <>
                  展示最近最多32个检验日、每日至多10只股票的原值示例。完整范围为
                  {formatCount(day.computation_stock_count)}
                  只，已全部参与计算；各日覆盖与缺因按完整范围统计。
                </>
              }
            >
              示例说明
            </Tip>
          </div>
          <DataTable
            rows={day.auction_values ?? []}
            columns={columns}
            rowKey={(row) => row.stock_code}
            label="竞价原值示例"
          />
        </>
      )}
    </details>
  );
}
