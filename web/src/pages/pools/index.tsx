import { Link } from "react-router";
import { inTradingHours, useAuctionPoolLive, usePools } from "@/api/endpoints";
import { useUrlState } from "@/app/useUrlState";
import { formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { EmptyState, FlowSteps, PageHeader, Panel, Segmented } from "@/ui";
import { ChangeText } from "@/ui/ChangeText";
import { Sparkline } from "@/ui/Sparkline";
import { StockLink } from "@/ui/StockLink";
import { QueryView } from "../shared";

const AUCTION_POOL = "竞价选股";
type Member = NonNullable<ReturnType<typeof usePools>["data"]>["pools"][number]["members"][number];

export default function PoolsPage() {
  const query = usePools();
  const [selected, setSelected] = useUrlState("pool");
  const first = query.data?.pools[0]?.name;
  const isAuction = (selected ?? first) === AUCTION_POOL;
  const live = useAuctionPoolLive(isAuction);
  const liveBy = new Map((live.data?.rows ?? []).map((r) => [r.code, r]));
  const pctOf = (code: string) => liveBy.get(code)?.pct ?? null;
  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="池子"
        note={query.data?.trade_date ?? undefined}
        actions={<FlowSteps />}
      />
      <QueryView query={query}>
        {(data) => {
          if (!data.pools.length)
            return (
              <EmptyState
                title="还没有池子"
                hint={
                  <>
                    池子是选股结果的快照，是回测与模拟盘的输入。<Link to="/screener">去选股器</Link>
                    筛选后点「保存为池子」。
                  </>
                }
              />
            );
          const pool = data.pools.find((p) => p.name === selected) ?? data.pools[0];
          if (!pool) return null;
          return (
            <>
              <Segmented
                label="池子"
                value={pool.name}
                onChange={setSelected}
                options={data.pools.map((p) => ({ value: p.name, label: p.name }))}
              />
              <Panel
                title={
                  pool.name === AUCTION_POOL && live.data?.date ? (
                    <>
                      {inTradingHours() ? <span className="live-dot" aria-hidden /> : null}
                      {`${pool.name} · ${pool.members.length} 只`}
                      <span
                        className="faint"
                        style={{ fontWeight: 400, marginLeft: 8, fontSize: 12 }}
                      >
                        {inTradingHours() ? "盘中每 10 秒刷新" : "已收盘"}
                        {live.data.rows.find((r) => r.time)?.time
                          ? ` · 行情 ${live.data.rows.reduce((m, r) => (r.time && r.time > m ? r.time : m), "")}`
                          : ""}
                      </span>
                    </>
                  ) : (
                    `${pool.name} · ${pool.members.length} 只`
                  )
                }
                sub={[
                  pool.description,
                  pool.pool_refs.join(" + "),
                  pool.updated_at ? `更新于 ${formatShanghaiDateTime(pool.updated_at)}` : "",
                ]
                  .filter(Boolean)
                  .join(" · ")}
                actions={
                  pool.pool_refs.length ? (
                    <Link
                      className="btn sm"
                      to={`/experiments?preset=${encodeURIComponent(pool.pool_refs[0] ?? "")}`}
                    >
                      看此池的回测 →
                    </Link>
                  ) : undefined
                }
                flush
              >
                <DataTable
                  label="池子成员"
                  tools="池子成员"
                  rows={pool.members}
                  rowKey={(row) => `${row.preset}:${row.code}`}
                  emptyText="今天没有成员：去「选股器」筛选后点「保存为池子」。"
                  columns={[
                    {
                      id: "code",
                      header: "代码",
                      value: (row) => row.code,
                      cell: (row) => <StockLink code={row.code} />,
                    },
                    { id: "name", header: "名称", value: (row) => row.name ?? null },
                    ...(pool.name === AUCTION_POOL
                      ? [
                          {
                            id: "stage",
                            header: "阶段",
                            value: (row: Member) =>
                              row.detail?.stage === "pick"
                                ? `终选 #${row.detail?.rank ?? ""}`
                                : "候选",
                            cell: (row: Member) =>
                              row.detail?.stage === "pick" ? (
                                <span className="up">终选 #{String(row.detail?.rank ?? "")}</span>
                              ) : (
                                <span className="muted">候选</span>
                              ),
                          },
                          {
                            id: "pct",
                            header: "最新涨跌",
                            numeric: true,
                            value: (row: Member) => pctOf(row.code),
                            cell: (row: Member) => {
                              const r = liveBy.get(row.code);
                              return (
                                <span title={r?.time ? `${r.price ?? ""} @ ${r.time}` : undefined}>
                                  <ChangeText value={r?.pct ?? null} />
                                </span>
                              );
                            },
                          },
                          {
                            id: "intraday",
                            header: "日内涨幅",
                            numeric: true,
                            value: (row: Member) => liveBy.get(row.code)?.intraday_pct ?? null,
                            cell: (row: Member) => {
                              const r = liveBy.get(row.code);
                              return (
                                <span title={r?.open ? `开盘 ${r.open}` : undefined}>
                                  <ChangeText value={r?.intraday_pct ?? null} />
                                </span>
                              );
                            },
                          },
                          {
                            id: "spark",
                            header: "分时",
                            sortable: false,
                            value: (row: Member) => pctOf(row.code),
                            cell: (row: Member) => {
                              const r = liveBy.get(row.code);
                              return r ? (
                                <Sparkline
                                  values={r.spark}
                                  times={r.spark_t}
                                  base={r.pre_close ?? null}
                                  label={`${row.name ?? row.code} 分时`}
                                />
                              ) : (
                                <span className="faint">{live.isLoading ? "…" : "—"}</span>
                              );
                            },
                          },
                          {
                            id: "gap",
                            header: "竞价涨幅",
                            numeric: true,
                            secondary: true,
                            value: (row: Member) =>
                              typeof row.detail?.gap === "number" ? row.detail.gap * 100 : null,
                            cell: (row: Member) => (
                              <ChangeText
                                value={
                                  typeof row.detail?.gap === "number" ? row.detail.gap * 100 : null
                                }
                              />
                            ),
                          },
                        ]
                      : [{ id: "preset", header: "来源", value: (row: Member) => row.preset }]),
                  ]}
                />
              </Panel>
            </>
          );
        }}
      </QueryView>
    </>
  );
}
