import { useState } from "react";
import { Link } from "react-router";
import { useMarketLive } from "@/api/endpoints";
import type { components } from "@/api/schema";
import { formatAmount } from "@/format/number";
import { AppDrawer, ChangeText, SkeletonRows } from "@/ui";

type LiveBoard = components["schemas"]["LiveBoard"];
type Sort = "pct" | "amount";
const pctText = (v: number | null | undefined) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(2)}%`;
const tone = (v: number | null | undefined) =>
  v == null || v === 0 ? "flat" : v > 0 ? "up" : "down";

/** Industry drill-down: bottom sheet on iPhone, right side panel on desktop. */
export function BoardDetail({
  board,
  boards,
  onClose,
}: {
  board: string | null;
  /** Board summaries already on the page, so the header renders before members load. */
  boards: readonly LiveBoard[];
  phone?: boolean;
  onClose: () => void;
}) {
  const [sort, setSort] = useState<Sort>("pct");
  const query = useMarketLive(board);
  const data = query.data?.board === board ? query.data : undefined;
  const info = (data?.boards ?? boards).find((b) => b.name === board);
  const members = [...(data?.members ?? [])].sort((a, b) =>
    sort === "pct" ? (b.pct ?? -999) - (a.pct ?? -999) : (b.amount ?? 0) - (a.amount ?? 0),
  );
  const ur = info?.up_ratio;

  const summary = (
    <div className="bd-stats">
      <span>
        成交 <b className="num">{formatAmount(info?.amount)}</b>
      </span>
      <span>
        上涨 <b className="num">{ur == null ? "—" : `${Math.round(ur * 100)}%`}</b>
      </span>
      <span>
        涨停 <b className={info?.limit_up ? "num up" : "num"}>{info?.limit_up ?? "—"}</b>
      </span>
      {info?.broken ? (
        <span>
          炸板 <b className="num">{info.broken}</b>
        </span>
      ) : null}
      <span>
        <b className="num">{info?.n ?? members.length}</b> 只
      </span>
    </div>
  );
  const sorter = (
    <fieldset className="bd-sort" aria-label="排序">
      {(
        [
          ["pct", "涨跌幅"],
          ["amount", "成交额"],
        ] as const
      ).map(([v, l]) => (
        <button
          key={v}
          type="button"
          aria-pressed={sort === v}
          className={sort === v ? "on" : ""}
          onClick={() => setSort(v)}
        >
          {l}
        </button>
      ))}
    </fieldset>
  );
  const list = !data ? (
    <div className="pad">
      <SkeletonRows rows={8} />
    </div>
  ) : !members.length ? (
    <p className="hint pad">这个行业暂时没有成分股快照。</p>
  ) : (
    <ol className="bd-list">
      {members.map((m, i) => (
        <li key={m.code}>
          <Link className="bd-row" to={`/stock/${encodeURIComponent(m.code)}`}>
            <span className="bd-rank num">{i + 1}</span>
            <span className="bd-id">
              <span className="bd-name">
                {m.name ?? m.code}
                {m.state ? (
                  <em
                    className={`bd-state ${m.state === "跌停" ? "down" : m.state === "涨停" ? "up" : ""}`}
                  >
                    {m.state}
                  </em>
                ) : null}
              </span>
              <span className="bd-code mono">
                {m.code} · {formatAmount(m.amount)}
              </span>
            </span>
            <span className={`bd-pct num ${tone(m.pct)}`}>{pctText(m.pct)}</span>
            <svg className="bd-chev" viewBox="0 0 24 24" width="14" height="14" aria-hidden>
              <path d="M9 6l6 6-6 6" fill="none" stroke="currentColor" strokeWidth="2" />
            </svg>
          </Link>
        </li>
      ))}
    </ol>
  );

  return (
    <AppDrawer
      open={board != null}
      onClose={onClose}
      label={`${board ?? ""} 成分股`}
      title={
        <span className="bd-titlerow">
          {board} <ChangeText value={info?.pct_median ?? null} />
        </span>
      }
    >
      <div className="bd-desk">
        {summary}
        {sorter}
        {list}
      </div>
    </AppDrawer>
  );
}
