import { Pill } from "./Pill";

export interface PagePlaceholderProps {
  /** Status line, e.g. "M1 开发中" or "计划第 3–4 周开发". */
  status: string;
  /** True for pages of the milestone now in development. */
  inDevelopment: boolean;
  summary: string;
}

/** Stand-in body for a page whose milestone has not shipped yet. */
export function PagePlaceholder({ status, inDevelopment, summary }: PagePlaceholderProps) {
  return (
    <section className="panel" aria-label="页面状态">
      <div className="placeholder">
        <div className="row">
          <Pill kind={inDevelopment ? "acc" : "idle"}>{status}</Pill>
          <span className="hint">页面框架已就绪，内容还没有接入数据。</span>
        </div>
        <p className="muted">{summary}</p>
      </div>
    </section>
  );
}
