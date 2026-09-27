import type { Schemas } from "@/api/client";
import type { EditorJournal } from "./editorSession";

export type PublicationStage = "pending" | "saved" | "attached" | "published" | "result";

/** Never infer new-rule results from an older or differently versioned read. */
export function publicationStage(
  journal: EditorJournal | null,
  editor: Schemas["PoolEditorData"] | undefined,
  pools: Schemas["PoolsData"] | undefined,
  editorGeneration: string | null | undefined,
  poolsGeneration: string | null | undefined,
  newestGeneration: string | null | undefined,
): PublicationStage {
  if (journal?.saveStatus !== "succeeded" || journal.saveVersion === null) return "pending";
  if (journal.canvasName !== null && journal.attachStatus !== "succeeded") return "saved";
  const accepted: PublicationStage = journal.canvasName === null ? "saved" : "attached";
  if (
    editorGeneration == null ||
    poolsGeneration == null ||
    newestGeneration == null ||
    editorGeneration !== poolsGeneration ||
    editorGeneration !== newestGeneration ||
    editor?.state !== "ready" ||
    pools?.state !== "ready"
  )
    return accepted;

  const key = `user/${journal.save.base_name}`;
  const targetVersion =
    journal.attachStatus === "succeeded"
      ? (journal.attach?.expected_pool_version ?? journal.saveVersion)
      : journal.saveVersion;
  const publishedEditorPool = editor.pools.find((pool) => pool.key === key);
  const publishedPool = pools.pools.find((pool) => pool.key === key);
  if (
    publishedEditorPool?.version !== targetVersion ||
    publishedPool?.definition?.state !== "available"
  )
    return accepted;
  if (journal.canvasName !== null) {
    const editCanvas = editor.canvases.find((canvas) => canvas.name === journal.canvasName);
    const viewCanvas = pools.canvases.find((canvas) => canvas.name === journal.canvasName);
    if (!editCanvas?.pool_refs.includes(key) || !viewCanvas?.pool_keys.includes(key)) {
      return accepted;
    }
  }
  return publishedPool.result.state === "current_rules" ? "result" : "published";
}
