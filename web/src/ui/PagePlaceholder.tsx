import { Pill } from "./Pill";

export interface PagePlaceholderProps {
  /** One line on what the page will do. */
  summary: string;
}

/** The body of a page that is not built yet: a clean 即将上线 card. */
export function PagePlaceholder({ summary }: PagePlaceholderProps) {
  return (
    <section className="panel soon" aria-label="即将上线">
      <Pill kind="acc">即将上线</Pill>
      <p className="soon-text">{summary}</p>
    </section>
  );
}
