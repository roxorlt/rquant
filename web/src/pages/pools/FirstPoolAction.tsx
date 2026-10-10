import type { Schemas } from "@/api/client";
import { catalogUsableForGeneration, useScreenCatalog } from "@/api/screen";
import { Button } from "@/ui";

export function FirstPoolAction({
  canvas,
  editor,
  editorReady,
  editorNotice,
  generationId,
  viewer,
  definitionsAvailable,
  storageAvailable,
  onCreate,
}: {
  canvas: Schemas["SavedCanvas"] | undefined;
  editor: Schemas["PoolEditorData"] | undefined;
  editorReady: boolean;
  editorNotice: string;
  generationId: string | null | undefined;
  viewer: string | null | undefined;
  definitionsAvailable: boolean;
  storageAvailable: boolean;
  onCreate: () => void;
}) {
  const catalog = useScreenCatalog();
  const editableCanvas = editor?.canvases.find((item) => item.name === canvas?.name);
  const canvasReady =
    !!canvas && definitionsAvailable && !canvas.refs_truncated && canvas.pool_keys.length === 0;
  const editableCanvasReady = !!editableCanvas && editableCanvas.pool_refs.length === 0;
  const catalogReady =
    !catalog.error &&
    !!catalog.data?.blocks.length &&
    catalogUsableForGeneration(catalog.data, catalog.serving?.generation_id, generationId);
  const disabledReason = !canvasReady
    ? "请先打开已发布的空画布。"
    : !editorReady
      ? `${editorNotice}，暂时无法创建池子。`
      : !editableCanvasReady
        ? "画布资料正在更新，暂时无法创建池子。"
        : !viewer
          ? "请先登录，才能创建池子。"
          : !storageAvailable
            ? "浏览器存储不可用，无法安全提交。"
            : !catalogReady
              ? "条件目录暂不可用，请稍后重试。"
              : undefined;

  return (
    <Button
      className="pools-add-button"
      variant="primary"
      size="sm"
      disabledReason={disabledReason}
      onClick={onCreate}
    >
      创建首只池子
    </Button>
  );
}
