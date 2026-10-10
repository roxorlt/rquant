import { useEffect, useState } from "react";
import { TemplateRulesSummary } from "@/pages/strategies/TemplateRules";
import {
  type TemplateDetail,
  type TemplateHead,
  useTemplateCatalog,
  useTemplateDetail,
  useTemplateSources,
  useTemplateVersions,
} from "@/pages/strategies/templateApi";
import { Button, EmptyState, PageSkeleton } from "@/ui";

function sameHead(a: TemplateHead | undefined, b: TemplateHead): boolean {
  return (
    !!a &&
    a.version === b.version &&
    a.registration_fingerprint === b.registration_fingerprint &&
    a.record_hash === b.record_hash &&
    a.spec_fingerprint === b.spec_fingerprint
  );
}

export function ExperimentTemplatePicker({
  owner,
  generation,
  onChange,
}: {
  owner: string;
  generation: string;
  onChange: (value: TemplateDetail | null) => void;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [version, setVersion] = useState<number | null>(null);
  const [before, setBefore] = useState<number | null>(null);
  const [older, setOlder] = useState<
    NonNullable<ReturnType<typeof useTemplateVersions>["data"]>["versions"]
  >([]);
  const catalog = useTemplateCatalog(owner, generation);
  const versions = useTemplateVersions(owner, generation, selected, before);
  const detail = useTemplateDetail(owner, generation, selected, version);
  const sources = useTemplateSources(owner, generation);
  const available =
    catalog.serving?.generation_id === generation
      ? (catalog.data?.templates.filter((item) => !item.archived) ?? [])
      : [];
  const knownVersions = [
    ...older,
    ...(versions.serving?.generation_id === generation ? (versions.data?.versions ?? []) : []),
  ];
  const selectedHead =
    version === null
      ? available.find((item) => item.strategy_id === selected)?.head
      : knownVersions.find((item) => item.head.version === version)?.head;
  const actual =
    !catalog.error &&
    !versions.error &&
    !sources.error &&
    sources.serving?.generation_id === generation &&
    catalog.serving?.generation_id === generation &&
    versions.serving?.generation_id === generation &&
    available.some((item) => item.strategy_id === selected) &&
    detail.serving?.generation_id === generation &&
    !detail.error &&
    detail.data?.strategy_id === selected &&
    !detail.data.archived &&
    sameHead(selectedHead, detail.data.head) &&
    (version === null || detail.data.head.version === version)
      ? detail.data
      : null;
  useEffect(() => {
    onChange(actual);
  }, [actual, onChange]);
  if (catalog.error) return <EmptyState title="策略暂时无法加载" hint="刷新后重新选择策略。" />;
  if (catalog.isLoading) return <PageSkeleton label="正在核对策略" />;
  return (
    <section aria-label="策略选择">
      <label className="field">
        策略
        <select
          className="inp"
          aria-label="实验策略"
          value={selected ?? ""}
          onChange={(event) => {
            onChange(null);
            setSelected(event.target.value || null);
            setVersion(null);
            setBefore(null);
            setOlder([]);
          }}
        >
          <option value="">选择已保存策略</option>
          {available.map((item) => (
            <option key={item.strategy_id} value={item.strategy_id}>
              {item.name}
            </option>
          ))}
        </select>
      </label>
      {selected ? (
        <>
          <label className="field">
            版本
            <select
              className="inp"
              aria-label="实验策略版本"
              value={version ?? ""}
              onChange={(event) => {
                onChange(null);
                setVersion(event.target.value ? Number(event.target.value) : null);
              }}
            >
              <option value="">最新版本</option>
              {knownVersions
                .filter(
                  (item, index, all) =>
                    all.findIndex((value) => value.head.version === item.head.version) === index,
                )
                .map((item) => (
                  <option key={item.head.version} value={item.head.version}>
                    第 {item.head.version} 版
                  </option>
                ))}
            </select>
          </label>
          {versions.data?.next_before_version != null ? (
            <Button
              disabled={versions.isFetching}
              onClick={() => {
                setOlder((current) => [...current, ...(versions.data?.versions ?? [])]);
                setBefore(versions.data?.next_before_version ?? null);
              }}
            >
              更早版本
            </Button>
          ) : null}
          {detail.error || versions.error ? (
            <EmptyState title="该版本暂时无法核对" hint="请重新选择，或刷新后重试。" />
          ) : detail.isLoading ? (
            <PageSkeleton label="正在核对策略规则" />
          ) : actual ? (
            <TemplateRulesSummary
              rules={actual.rules}
              sources={sources.serving?.generation_id === generation ? sources.data : undefined}
            />
          ) : null}
        </>
      ) : available.length === 0 ? (
        <EmptyState title="还没有可用策略" hint="先在策略页面保存一份策略。" />
      ) : null}
    </section>
  );
}
