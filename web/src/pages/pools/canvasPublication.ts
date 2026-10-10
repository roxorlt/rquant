import type { Schemas } from "@/api/client";
import type { CanvasCreateJournal } from "./canvasCreateSession";

/** A command receipt alone is not proof that the same canvas is readable yet. */
export function canvasPublicationStage(
  journal: CanvasCreateJournal | null,
  editor: Schemas["PoolEditorData"] | undefined,
  pools: Schemas["PoolsData"] | undefined,
  editorGeneration: string | null | undefined,
  poolsGeneration: string | null | undefined,
  newestGeneration: string | null | undefined,
): "waiting" | "available" {
  if (
    journal?.status !== "succeeded" ||
    !journal.recordHash ||
    editor?.state !== "ready" ||
    pools?.definitions_available !== true ||
    editorGeneration == null ||
    poolsGeneration == null ||
    newestGeneration == null ||
    editorGeneration !== poolsGeneration ||
    editorGeneration !== newestGeneration
  )
    return "waiting";

  const match = editor.canvases.find((canvas) => canvas.name === journal.body.name);
  const visible = pools.canvases.find((canvas) => canvas.name === journal.body.name);
  return match?.command_id === journal.body.command_id &&
    match.record_hash === journal.recordHash &&
    match.pool_refs.length === 0 &&
    visible !== undefined &&
    visible.pool_keys.length === 0
    ? "available"
    : "waiting";
}
