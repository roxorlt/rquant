import { type PageId, pageById } from "@/app/pages";
import { PageHeader, PagePlaceholder } from "@/ui";

/** A page that is not built yet: its header and a 即将上线 card. */
export function PlaceholderPage({ id }: { id: PageId }) {
  const page = pageById(id);
  return (
    <>
      <PageHeader eyebrow={page.group} title={page.title} />
      <PagePlaceholder summary={page.summary} />
    </>
  );
}
