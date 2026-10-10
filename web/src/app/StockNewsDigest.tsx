import { useAiCapabilities, useAiNews } from "@/api/aiAssistance";
import { Button, EmptyState, Panel, RelativeTime, SkeletonRows, Tip } from "@/ui";
import { useAiGeneration } from "./aiAssistanceSession";

const names = { announcement: "公告", news: "新闻", research: "研报" };
const nature = { actual: "已披露事实", forecast: "预测与展望", context: "背景" };
export function StockNewsDigest({
  viewer,
  stockCode,
}: {
  viewer: string | null;
  stockCode: string | null;
}) {
  return <News key={`${viewer}:${stockCode}`} viewer={viewer} stockCode={stockCode} />;
}
function News({ viewer, stockCode }: { viewer: string | null; stockCode: string | null }) {
  const capability = useAiCapabilities(viewer, stockCode !== null);
  const waiting =
    capability.isFetching || (capability.data === undefined && capability.error === null);
  const canRead =
    viewer !== null &&
    stockCode !== null &&
    !waiting &&
    capability.error === null &&
    capability.data?.available === true;
  const query = useAiNews(viewer, stockCode, canRead);
  const readMessage = !viewer
    ? "请先登录"
    : !stockCode
      ? "先选择一只股票"
      : waiting
        ? "正在读取助手状态"
        : (capability.error?.message ?? capability.data?.message ?? "原文摘要暂不可用。");
  const request = useAiGeneration(viewer, `news:${stockCode}`);
  const data = query.data;
  const content = data?.content?.digest.owner_uid === viewer ? data.content : null;
  const progress = data?.progress;
  return (
    <Panel
      title="原文摘要"
      actions={
        <Button
          size="sm"
          variant="ghost"
          disabledReason={canRead ? undefined : readMessage}
          onClick={() => {
            if (canRead) query.refetch();
          }}
        >
          刷新摘要
        </Button>
      }
    >
      {!viewer ? (
        <EmptyState title="请先登录" />
      ) : !stockCode ? (
        <EmptyState title="先选择一只股票" />
      ) : !canRead ? (
        <EmptyState title={readMessage} />
      ) : query.isLoading ? (
        <SkeletonRows rows={3} />
      ) : query.error ? (
        <EmptyState title={query.error.message} />
      ) : (
        <>
          {content ? (
            <>
              {!content.digest.coverage_complete ? (
                <p className="hint" role="status">
                  原文覆盖有限
                </p>
              ) : null}
              {content.digest.statements.length === 0 ? (
                <EmptyState
                  title={content.digest.status === "empty" ? "本次采集未发现原文" : "原文暂不可用"}
                  hint="查看来源覆盖后再判断。"
                />
              ) : (
                ["actual", "forecast", "context"].map((kind) => {
                  const rows = content.digest.statements.filter(
                    (statement) => statement.nature === kind,
                  );
                  return rows.length ? (
                    <section key={kind} aria-label={nature[kind as keyof typeof nature]}>
                      <h3>{nature[kind as keyof typeof nature]}</h3>
                      {rows.map((statement, index) => (
                        // biome-ignore lint/suspicious/noArrayIndexKey: The sealed digest fixes paragraph order; these text rows hold no mutable state.
                        <p key={`${kind}:${index}`}>
                          {statement.content.text}{" "}
                          {statement.content.citations.map((identifier) => {
                            const citation = content.citations.find(
                              (item) => item.fact_id === identifier,
                            );
                            const source = content.sources.find(
                              (item) => item.document_id === citation?.document_id,
                            );
                            return citation ? (
                              <Tip
                                key={identifier}
                                interactive
                                content={
                                  <>
                                    <p>{citation.quote}</p>
                                    <p>
                                      原文范围 {citation.body_start}–{citation.body_end} ·{" "}
                                      {citation.body_sha256}
                                    </p>
                                    {source ? (
                                      <a
                                        href={source.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                      >
                                        查看原文
                                      </a>
                                    ) : null}
                                  </>
                                }
                              >
                                <button
                                  className="btn ghost sm"
                                  type="button"
                                  aria-label="查看摘要依据"
                                >
                                  依据
                                </button>
                              </Tip>
                            ) : null;
                          })}
                        </p>
                      ))}
                    </section>
                  ) : null;
                })
              )}
              <details>
                <summary>来源与日期</summary>
                {content.sources.map((source) => (
                  <div
                    key={`${source.provider}:${source.source_kind}:${source.document_id}`}
                    className="screen-nl-preview"
                  >
                    <a href={source.url} target="_blank" rel="noopener noreferrer">
                      {source.title}
                    </a>
                    <dl className="kv">
                      <dt>平台发布日期</dt>
                      <dd>
                        {source.published_at ? (
                          <Tip content={source.published_at}>
                            <span>{source.published_date ?? "—"}</span>
                          </Tip>
                        ) : (
                          (source.published_date ?? "—")
                        )}
                      </dd>
                      <dt>正文日期</dt>
                      <dd>{source.body_date ?? "—"}</dd>
                      <dt>首次采集</dt>
                      <dd>
                        <RelativeTime at={source.first_collected_at} />
                      </dd>
                    </dl>
                  </div>
                ))}
              </details>
            </>
          ) : (
            <EmptyState title={data?.message ?? "原文尚未采集。"} />
          )}
          <details>
            <summary>采集覆盖</summary>
            {(data?.coverage ?? []).map((record) => (
              <p key={`${record.provider}:${record.source_kind}`}>
                <b>{names[record.source_kind]}</b> ·{" "}
                {record.status === "available"
                  ? record.complete
                    ? "完整"
                    : "有限"
                  : record.status === "permission_denied"
                    ? "无权限"
                    : "暂不可用"}
                <Tip
                  content={`来源 ${record.provider}；区间 ${record.start_date} 至 ${record.end_date}；已采集 ${record.collected_pages.length} 页；返回 ${record.returned_documents ?? "未知"} 篇。平台时间、正文时间和采集时间分别保留。`}
                >
                  <span
                    className="screen-help"
                    role="img"
                    aria-label={`${names[record.source_kind]}覆盖说明`}
                  >
                    ⓘ
                  </span>
                </Tip>
              </p>
            ))}
          </details>
          {data?.nightly_enabled ? (
            progress ? (
              <p className="hint">
                已完成 {progress.completed.toLocaleString("zh-CN")} · 待处理{" "}
                {progress.pending.toLocaleString("zh-CN")}
                <Tip
                  content={`完整范围 ${progress.total.toLocaleString("zh-CN")} 只；${progress.unknown.toLocaleString("zh-CN")} 只结果未知；${progress.start_date} 至 ${progress.end_date}。当前池子与本人盯盘的完整并集。未知请求不会重发。`}
                >
                  <span className="screen-help" role="img" aria-label="夜间研究进度说明">
                    ⓘ
                  </span>
                </Tip>
              </p>
            ) : (
              <p className="hint">等待夜间采集</p>
            )
          ) : (
            <p className="hint">夜间采集未启用</p>
          )}
          {!content && data?.context_sha256 && !request.original ? (
            <Button
              size="sm"
              disabledReason={
                capability.data?.can_generate
                  ? undefined
                  : (capability.data?.message ?? "调用尚未启用。")
              }
              onClick={() =>
                void request
                  .generate({
                    purpose: "news_digest",
                    request_id: crypto.randomUUID(),
                    stock_code: stockCode,
                    context_sha256: data.context_sha256 ?? "",
                  })
                  .then(query.refetch)
              }
            >
              生成摘要
            </Button>
          ) : null}
          {request.original ? (
            <div className="row">
              <Button
                size="sm"
                onClick={() => {
                  if (request.errorStatus === 404 && request.original) {
                    void request.generate(request.original).then(query.refetch);
                  } else {
                    void request.lookup().then(query.refetch);
                  }
                }}
                disabled={request.busy}
              >
                {request.errorStatus === 404 ? "继续生成原请求" : "继续查看原请求"}
              </Button>
              {request.view?.state === "completed" || request.absent ? (
                <Button size="sm" variant="ghost" onClick={request.reset}>
                  新建摘要
                </Button>
              ) : null}
            </div>
          ) : null}
          {request.error ? (
            <p role="status" className="hint">
              {request.error}
            </p>
          ) : null}
        </>
      )}
    </Panel>
  );
}
