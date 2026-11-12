import { useCallback } from "react";
import { useLocation, useNavigate, useSearchParams } from "react-router";
import { inTradingHours, useMarketLive } from "@/api/endpoints";
import { TimeChart } from "@/charts/Chart";
import { formatAmount, formatCount, formatFixed } from "@/format/number";
import { DataTable } from "@/table/DataTable";
import { oneOf, usePreference } from "@/theme/storage";
import { ChangeText, KpiStrip, Panel, Segmented } from "@/ui";
import { QueryView } from "../shared";
import { BoardDetail } from "./BoardDetail";
import { Heatmap } from "./Heatmap";
import { PHONE_QUERY, useMedia } from "./useMedia";

const pct = (v: number) => `${formatFixed(v * 100, 0)}%`;

type Lens = "breadth" | "limit";

/** Live 市场全景 from the research 3-second snapshots (rebuild of the old Streamlit page). */
export function LivePanorama() {
  const [lens, setLens] = usePreference<Lens>(
    "panorama.lens",
    "breadth",
    oneOf("breadth", "limit"),
  );
  // The open board lives in the URL (?board=…) as its own history entry, so the iOS
  // back-swipe / browser back closes the detail instead of leaving the page.
  const [params, setParams] = useSearchParams();
  const board = params.get("board");
  const navigate = useNavigate();
  const location = useLocation();
  const phone = useMedia(PHONE_QUERY);
  const open = useCallback(
    (name: string) => {
      if (name === board) return;
      setParams({ board: name }, { replace: board != null, state: { sheet: true } });
    },
    [board, setParams],
  );
  const close = useCallback(() => {
    if ((location.state as { sheet?: boolean } | null)?.sheet) navigate(-1);
    else setParams({}, { replace: true });
  }, [location.state, navigate, setParams]);
  const query = useMarketLive(null);
  return (
    <QueryView query={query}>
      {(data) => {
        const p = data.pulse;
        if (!p || !data.curve) return <p className="hint pad">今天还没有快照。</p>;
        const c = data.curve;
        const live = inTradingHours();
        const boards = data.boards;
        const heatPanel = (
          <Panel
            title="行业热力图"
            sub={`面积 = 成交额，颜色 = 行业内个股涨跌中位数（±3% 封顶）· ${phone ? "轻点" : "点击"}色块看成分股`}
            flush
          >
            <Heatmap
              phone={phone}
              selected={board}
              onSelect={(b) => open(b.board_name)}
              boards={boards.map((b) => ({
                system: "行业",
                board_code: b.name,
                board_name: b.name,
                amount: b.amount,
                main_net_amount: null,
                pct_chg_median: b.pct_median ?? null,
                limit_up_count: b.limit_up,
                stock_count: b.n,
                leading_stock: b.leader ?? null,
              }))}
            />
          </Panel>
        );
        return (
          <>
            <KpiStrip
              label="市场脉搏"
              compact
              items={[
                {
                  key: "ratio",
                  label: "上涨占比",
                  value: p.up_ratio == null ? "—" : pct(p.up_ratio),
                  tone: (p.up_ratio ?? 0) >= 0.5 ? "up" : "down",
                  tip: "上涨家数 ÷ 有成交的全部 A 股",
                },
                {
                  key: "ud",
                  label: "涨 / 跌",
                  value: `${formatCount(p.up)} / ${formatCount(p.down)}`,
                  sub: `平 ${formatCount(p.flat)}`,
                },
                {
                  key: "med",
                  label: "涨跌中位数",
                  value: <ChangeText value={p.median_pct ?? null} />,
                  tip: "全部个股涨跌幅的中位数，比指数更能代表“大多数股票”",
                },
                { key: "lu", label: "涨停", value: formatCount(p.limit_up), tone: "up" },
                {
                  key: "br",
                  label: "炸板",
                  value: formatCount(p.broken),
                  tip: "今天摸到过涨停、现在没封住的股票",
                  sub:
                    p.limit_up + p.broken
                      ? `封板率 ${pct(p.limit_up / (p.limit_up + p.broken))}`
                      : undefined,
                },
                { key: "ld", label: "跌停", value: formatCount(p.limit_down), tone: "down" },
                {
                  key: "amt",
                  label: "成交额",
                  value: formatAmount(p.amount),
                  sub: data.time ?? "",
                },
              ]}
            />
            {phone ? heatPanel : null}
            <div className="card-grid">
              <Panel
                title="盘中"
                sub={`上涨家数占比；50% 以上说明多数股票在涨${live ? " · 每 15 秒刷新" : ""}`}
                actions={
                  <Segmented
                    label="盘中图"
                    value={lens}
                    onChange={setLens}
                    options={[
                      { value: "breadth", label: "赚钱效应" },
                      { value: "limit", label: "涨停炸板" },
                    ]}
                  />
                }
              >
                {lens === "breadth" ? (
                  <TimeChart
                    label="上涨占比"
                    dates={c.t}
                    height={150}
                    baseline={0.5}
                    format={pct}
                    series={[
                      { name: "上涨占比", values: c.up_ratio, color: "var(--accent)", area: true },
                    ]}
                  />
                ) : (
                  <TimeChart
                    label="涨停与炸板家数"
                    dates={c.t}
                    height={150}
                    format={(v) => formatFixed(v, 0)}
                    series={[
                      { name: "涨停", values: c.limit_up, color: "var(--up)" },
                      { name: "炸板", values: c.broken, color: "var(--muted)", dashed: true },
                    ]}
                  />
                )}
              </Panel>
              {phone ? null : heatPanel}
              <Panel
                title={`行业 · ${boards.length}`}
                sub={phone ? "轻点一行看成分股" : "点一行看成分股"}
                flush
              >
                <DataTable
                  label="行业"
                  rows={boards}
                  rowKey={(r) => r.name}
                  height={420}
                  onSelect={(r) => open(r.name)}
                  selectedKey={board}
                  initialSort={{ id: "amount", desc: true }}
                  columns={[
                    { id: "name", header: "行业", value: (r) => r.name },
                    {
                      id: "pct",
                      header: "涨跌中位",
                      numeric: true,
                      value: (r) => r.pct_median ?? null,
                      cell: (r) => <ChangeText value={r.pct_median ?? null} />,
                    },
                    {
                      id: "up",
                      header: "上涨占比",
                      numeric: true,
                      value: (r) => r.up_ratio ?? null,
                      cell: (r) => (r.up_ratio == null ? "—" : pct(r.up_ratio)),
                    },
                    {
                      id: "amount",
                      header: "成交额",
                      numeric: true,
                      value: (r) => r.amount,
                      cell: (r) => formatAmount(r.amount),
                    },
                    { id: "lu", header: "涨停", numeric: true, value: (r) => r.limit_up },
                    {
                      id: "lead",
                      header: "领涨",
                      secondary: true,
                      value: (r) => r.leader_pct ?? null,
                      cell: (r) => (
                        <span>
                          {r.leader ?? "—"} <ChangeText value={r.leader_pct ?? null} />
                        </span>
                      ),
                    },
                  ]}
                />
              </Panel>
            </div>
            <BoardDetail board={board} boards={boards} phone={phone} onClose={close} />
          </>
        );
      }}
    </QueryView>
  );
}
