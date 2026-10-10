import { PageHeader } from "@/ui";

/**
 * A fixed HTML report shown in a same-origin iframe. The sandbox has no
 * allow-scripts: reports are static documents. allow-popups lets their source
 * links open in a new tab.
 */
export function HtmlReport({ title, date, file }: { title: string; date: string; file: string }) {
  return (
    <>
      <PageHeader
        eyebrow="报告"
        title={title}
        note={`固定快照，${date}。`}
        actions={
          <a className="btn" href={file} target="_blank" rel="noopener">
            单独打开
          </a>
        }
      />
      <iframe
        className="report-frame"
        src={file}
        title={title}
        sandbox="allow-popups allow-popups-to-escape-sandbox"
        referrerPolicy="same-origin"
      />
    </>
  );
}
