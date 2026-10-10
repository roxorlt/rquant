import { useEffect, useRef } from "react";
import {
  type AIInterpretationContextRequest,
  readAiInterpretation,
  useAiCapabilities,
} from "@/api/aiAssistance";
import { useCurrentGeneration } from "@/api/useMeta";
import { useServingQuery } from "@/api/useServingQuery";
import { useAiGeneration } from "@/app/aiAssistanceSession";
import { Button, EmptyState, Panel, SkeletonRows, Tip } from "@/ui";

const sections = { overview: "概要", annual: "分年表现", risk: "风险", suggestions: "下一步建议" };
export function AiInterpretation({
  viewer,
  sourceKind,
  jobId,
  resultHash,
}: {
  viewer: string | null;
  sourceKind: AIInterpretationContextRequest["source_kind"];
  jobId: string | null;
  resultHash: string | null;
}) {
  const generation = useCurrentGeneration();
  return (
    <Interpretation
      key={`${viewer}:${sourceKind}:${jobId}:${resultHash}:${generation}`}
      viewer={viewer}
      sourceKind={sourceKind}
      jobId={jobId}
      resultHash={resultHash}
      generation={generation}
    />
  );
}
function Interpretation({
  viewer,
  sourceKind,
  jobId,
  resultHash,
  generation,
}: {
  viewer: string | null;
  sourceKind: AIInterpretationContextRequest["source_kind"];
  jobId: string | null;
  resultHash: string | null;
  generation: string | null | undefined;
}) {
  const capability = useAiCapabilities(viewer, jobId !== null && resultHash !== null);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const hasResult = viewer !== null && jobId !== null && resultHash !== null;
  const canRead =
    hasResult && !capability.isFetching && !capability.error && capability.data?.available === true;
  const readAllowed = useRef(canRead);
  readAllowed.current = canRead;
  function mayRead() {
    return mounted.current && readAllowed.current;
  }
  const read = useServingQuery(
    ["ai", "interpretation", viewer, sourceKind, jobId, resultHash, generation],
    () => {
      if (!mayRead()) return Promise.reject(new Error("解读状态尚未确认。"));
      return readAiInterpretation({
        source_kind: sourceKind,
        job_id: jobId ?? "",
        result_sha256: resultHash ?? "",
      });
    },
    { enabled: canRead, staleTime: 0 },
  );
  function refresh() {
    if (mayRead()) read.refetch();
  }
  const request = useAiGeneration(viewer, `interpretation:${sourceKind}:${jobId}:${resultHash}`);
  const data =
    canRead &&
    read.data?.binding.owner_uid === viewer &&
    read.data.binding.job_id === jobId &&
    read.data.binding.result_sha256 === resultHash
      ? read.data
      : null;
  const generated =
    request.view?.result?.purpose === "interpretation" ? request.view.result.interpretation : null;
  const candidate = generated ?? data?.content;
  const content =
    candidate?.binding.result.owner_uid === viewer &&
    candidate.binding.result.job_id === jobId &&
    candidate.binding.result.result_sha256 === resultHash &&
    candidate.binding.result.spec_sha256 === data?.binding.spec_sha256 &&
    candidate.binding.result.manifest_sha256 === data.binding.manifest_sha256
      ? candidate
      : null;
  return (
    <Panel
      title="AI 解读"
      actions={
        <Button size="sm" variant="ghost" onClick={refresh} disabled={!canRead || read.isFetching}>
          刷新解读
        </Button>
      }
    >
      {!viewer ? (
        <EmptyState title="请先登录" />
      ) : !jobId || !resultHash ? (
        <EmptyState title="完成回测后可生成解读" hint="解读只使用已封存的完整结果。" />
      ) : capability.isLoading || capability.isFetching ? (
        <EmptyState title="正在检查解读状态…" />
      ) : capability.error ? (
        <EmptyState title="解读状态暂不可用" hint="请稍后刷新页面。" />
      ) : !canRead ? (
        <EmptyState title="AI 解读尚未配置" hint="可继续查看回测结果。" />
      ) : read.isLoading ? (
        <SkeletonRows rows={3} />
      ) : read.error ? (
        <EmptyState title={read.error.message} />
      ) : content ? (
        content.sections.map((section) => (
          <section key={section.key} aria-label={sections[section.key]}>
            <h3>{sections[section.key]}</h3>
            {section.paragraphs.map((paragraph, index) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: This keyed sealed result fixes paragraph order; text rows hold no mutable state.
              <p key={`${section.key}:${index}`}>
                {paragraph.text}{" "}
                {paragraph.citations.map((identifier) => {
                  const fact = data?.facts.find((item) => item.fact_id === identifier);
                  return fact ? (
                    <Tip
                      interactive
                      key={identifier}
                      content={
                        <>
                          <p>
                            {fact.label} · 封存原值 {fact.value ?? "未知"}
                            {fact.unit === "%" ? "（小数比例）" : fact.unit}
                          </p>
                          <p>
                            {fact.source_path} · {fact.source_sha256}
                          </p>
                        </>
                      }
                    >
                      <button
                        type="button"
                        className="btn ghost sm"
                        aria-label={`查看${fact.label}依据`}
                      >
                        依据
                      </button>
                    </Tip>
                  ) : null;
                })}
              </p>
            ))}
          </section>
        ))
      ) : (
        <p className="hint">{data?.message ?? "完整结果暂不可用。"}</p>
      )}
      <div className="row">
        {content ? (
          <>
            <p className="hint">解读已保存，可刷新查看。</p>
            {request.original ? (
              <Button
                size="sm"
                onClick={() => {
                  if (mayRead()) void request.lookup().then(refresh);
                }}
                disabled={request.busy || !canRead}
              >
                继续查看原请求
              </Button>
            ) : null}
          </>
        ) : request.original ? (
          <>
            <Button
              size="sm"
              onClick={() => {
                if (!mayRead()) return;
                if (
                  (request.view?.state === "reserved" || request.errorStatus === 404) &&
                  request.original
                ) {
                  void request.generate(request.original).then(refresh);
                } else {
                  void request.lookup().then(refresh);
                }
              }}
              disabled={request.busy || !canRead}
            >
              {request.view?.state === "reserved" || request.errorStatus === 404
                ? "继续生成原请求"
                : "继续查看原请求"}
            </Button>
            {request.view?.state === "completed" || request.absent ? (
              <Button size="sm" variant="ghost" onClick={request.reset}>
                新建解读
              </Button>
            ) : null}
          </>
        ) : (
          <Button
            onClick={() =>
              mayRead() &&
              capability.data?.can_generate === true &&
              data &&
              void request.generate({
                purpose: "interpretation",
                request_id: crypto.randomUUID(),
                source_kind: data.binding.source_kind,
                job_id: data.binding.job_id,
                spec_sha256: data.binding.spec_sha256,
                manifest_sha256: data.binding.manifest_sha256,
                result_sha256: data.binding.result_sha256,
              })
            }
            disabled={request.busy || !canRead || !data}
            disabledReason={
              capability.data?.can_generate
                ? undefined
                : (capability.data?.message ?? "调用尚未启用。")
            }
          >
            生成解读
          </Button>
        )}
        <Tip content="所有数值均引用本次封存结果。此页提供研究建议，不会自动修改策略或发起交易。">
          <span className="screen-help" role="img" aria-label="解读说明">
            ⓘ
          </span>
        </Tip>
      </div>
      {request.error ? (
        <p role="status" className="hint">
          {request.error}
        </p>
      ) : null}
    </Panel>
  );
}
