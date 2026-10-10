import { isRouteErrorResponse, useRouteError } from "react-router";
import { Button, PageHeader } from "@/ui";

/** Shown when a page fails to load, e.g. a chunk replaced by a newer release. */
export function RouteError() {
  const error = useRouteError();
  const detail = isRouteErrorResponse(error)
    ? `${error.status} ${error.statusText}`
    : error instanceof Error
      ? error.message
      : "未知错误";
  return (
    <main className="content">
      <div className="page">
        <PageHeader
          eyebrow="出错了"
          title="页面加载失败"
          note={`可能是网页刚刚发布了新版本。${detail}`}
        />
        <div className="row">
          <Button variant="primary" onClick={() => window.location.reload()}>
            刷新页面
          </Button>
        </div>
      </div>
    </main>
  );
}
