import { useState } from "react";
import { ApiError } from "@/api/client";
import { type FormulaPoolItem, useFormulaPoolMembers } from "@/api/formulaPools";
import { formatCount } from "@/format/number";
import { Button, EmptyState, Panel } from "@/ui";

type Page = { key: string; index: number; cursors: (string | null)[] };
const STOCK_CODE = /^\d{6}\.(SH|SZ|BJ)$/;

export function FormulaPoolDetail({
  pool,
  generation,
  onSelectStock,
}: {
  pool: FormulaPoolItem;
  generation: string | null | undefined;
  onSelectStock: (code: string) => void;
}) {
  const [page, setPage] = useState<Page>({ key: "", index: 0, cursors: [null] });
  const [readEpoch, setReadEpoch] = useState(0);
  const result = pool.latest_result;
  const key = JSON.stringify([pool.pool_name, result?.trade_date, generation]);
  const currentPage = page.key === key ? page : { key, index: 0, cursors: [null] };
  const cursor = currentPage.cursors[currentPage.index] ?? null;
  const members = useFormulaPoolMembers(
    pool.pool_name.startsWith("user/") ? pool.pool_name.slice(5) : null,
    result?.trade_date ?? null,
    cursor,
    generation,
    readEpoch,
    result !== null && result.match_count > 0 && generation != null,
  );
  const currentMembers =
    result !== null &&
    members.serving?.generation_id === generation &&
    members.data?.pool_name === pool.pool_name &&
    members.data.trade_date === result.trade_date &&
    members.data.total === result.match_count &&
    members.data.offset === currentPage.index * 50 &&
    members.data.match_codes.every((code) => STOCK_CODE.test(code))
      ? members.data
      : null;
  const mismatch =
    !members.isLoading && !members.error && members.data !== undefined && !currentMembers;
  const cursorConflict = members.error instanceof ApiError && members.error.status === 409;

  function restart(): void {
    setPage({ key, index: 0, cursors: [null] });
    setReadEpoch((value) => value + 1);
  }

  return (
    <div className="formula-pool-detail">
      <Panel title="公式条件" sub="公式池" label="公式条件">
        <h3>{pool.display_name}</h3>
        <code className="formula-pool-definition mono">{pool.formula}</code>
      </Panel>
      <Panel title="最近运行" sub={result?.trade_date ?? "尚未运行"} label="公式池最近运行">
        {result === null ? (
          <EmptyState title="尚未运行" hint="保存的是可重算公式；完成一次运行后会显示结果。" />
        ) : (
          <>
            <time className="num" dateTime={result.trade_date}>
              选股日期 · {result.trade_date}
            </time>
            <div className="formula-pool-counts">
              <span>
                命中 <strong className="num">{formatCount(result.match_count)}</strong>
              </span>
              <span>
                未命中 <strong className="num">{formatCount(result.no_match_count)}</strong>
              </span>
              <span>
                未能判断 <strong className="num">{formatCount(result.unknown_count)}</strong>
              </span>
            </div>
            {result.unknown_reasons.length > 0 ? (
              <ul className="formula-pool-unknown" aria-label="未能判断的原因">
                {result.unknown_reasons.map((reason) => (
                  <li key={reason.reason}>
                    <span>{reason.label}</span>
                    <strong className="num">{formatCount(reason.count)}</strong>
                  </li>
                ))}
              </ul>
            ) : null}
            {result.match_count === 0 ? (
              <EmptyState title="本次没有命中股票" hint="可在公式选股中调整条件后重新运行。" />
            ) : null}
          </>
        )}
      </Panel>
      {result !== null && result.match_count > 0 ? (
        <Panel
          title="命中股票"
          sub={`第 ${formatCount(currentPage.index + 1)} 页`}
          label="公式池成员"
        >
          {members.error || mismatch ? (
            <div className="formula-pool-member-state">
              <EmptyState
                title={cursorConflict ? "结果已更新" : "成员暂时无法读取"}
                hint={cursorConflict ? "请从第一页重新查看。" : "稍后刷新再试。"}
              />
              <Button size="sm" onClick={restart}>
                {cursorConflict ? "从第一页重看" : "重试读取"}
              </Button>
            </div>
          ) : currentMembers === null ? (
            <p className="hint" role="status">
              正在读取成员…
            </p>
          ) : (
            <>
              <ul className="formula-pool-members" aria-label="公式池命中股票">
                {currentMembers.match_codes.map((code) => (
                  <li key={code}>
                    <Button size="sm" variant="ghost" onClick={() => onSelectStock(code)}>
                      <span className="mono">{code}</span>
                      <span aria-hidden="true">↗</span>
                    </Button>
                  </li>
                ))}
              </ul>
              <div className="formula-pool-pagination">
                <Button
                  size="sm"
                  disabled={currentPage.index === 0}
                  onClick={() => setPage({ ...currentPage, index: currentPage.index - 1 })}
                >
                  上一页
                </Button>
                <Button
                  size="sm"
                  disabled={currentMembers.next_cursor === null}
                  onClick={() => {
                    if (!currentMembers.next_cursor) return;
                    setPage({
                      key,
                      index: currentPage.index + 1,
                      cursors: [
                        ...currentPage.cursors.slice(0, currentPage.index + 1),
                        currentMembers.next_cursor,
                      ],
                    });
                  }}
                >
                  下一页
                </Button>
              </div>
            </>
          )}
        </Panel>
      ) : null}
    </div>
  );
}
