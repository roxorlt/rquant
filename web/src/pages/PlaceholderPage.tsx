import { type PageId, pageById } from "@/app/pages";
import { PageHeader, PagePlaceholder } from "@/ui";

/** The M0 body of every page: its header and when it will be built. */
export function PlaceholderPage({ id }: { id: PageId }) {
  const page = pageById(id);
  return (
    <>
      <PageHeader
        eyebrow={page.group}
        title={page.title}
        note={`对应差距表模块：${page.modules.join("、")}`}
      />
      <PagePlaceholder status={page.schedule} inDevelopment={page.current} summary={page.summary} />
    </>
  );
}
