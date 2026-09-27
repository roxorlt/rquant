import type { Schemas } from "@/api/client";
import type { CanvasCreateJournal } from "./canvasCreateSession";
import { canvasPublicationStage } from "./canvasPublication";

const HASH = "a".repeat(64);
const journal: CanvasCreateJournal = {
  schema: 1,
  body: {
    kind: "create_canvas",
    command_id: "canvas-1",
    requested_at: "2026-09-27T07:00:00.000Z",
    name: "晨盘观察",
    description: "",
  },
  status: "succeeded",
  recordHash: HASH,
  reason: null,
};
const editor: Schemas["PoolEditorData"] = {
  state: "ready",
  canvas_create_available: true,
  pools: [],
  copy_sources: [],
  canvases: [
    {
      name: "晨盘观察",
      description: "",
      version: "b".repeat(64),
      command_id: "canvas-1",
      record_hash: HASH,
      pool_refs: [],
    },
  ],
};
const pools: Schemas["PoolsData"] = {
  state: "no_data",
  latest_trade_date: null,
  definitions_available: true,
  rules_available: true,
  canvases: [{ name: "晨盘观察", description: "", pool_keys: [], refs_truncated: false }],
  canvases_truncated: false,
  pools_truncated: false,
  pools: [],
};
const publishedEditorCanvas = editor.canvases[0];
if (!publishedEditorCanvas) throw new Error("fixture canvas missing");

it("waits for the same current data generation and matching create identity before declaring a canvas usable", () => {
  expect(canvasPublicationStage(journal, editor, pools, "g1", "g1", "g1")).toBe("available");
  expect(
    canvasPublicationStage(journal, editor, { ...pools, state: "unavailable" }, "g1", "g1", "g1"),
  ).toBe("available");
  expect(
    canvasPublicationStage(
      journal,
      editor,
      { ...pools, definitions_available: false },
      "g1",
      "g1",
      "g1",
    ),
  ).toBe("waiting");
  expect(canvasPublicationStage(journal, editor, pools, "g1", "g0", "g1")).toBe("waiting");
  expect(canvasPublicationStage(journal, editor, pools, "g1", "g1", "g2")).toBe("waiting");
  expect(
    canvasPublicationStage(journal, editor, { ...pools, canvases: [] }, "g1", "g1", "g1"),
  ).toBe("waiting");
  expect(
    canvasPublicationStage(
      journal,
      { ...editor, canvases: [{ ...publishedEditorCanvas, command_id: "other" }] },
      pools,
      "g1",
      "g1",
      "g1",
    ),
  ).toBe("waiting");
  expect(
    canvasPublicationStage(
      journal,
      { ...editor, canvases: [{ ...publishedEditorCanvas, record_hash: "c".repeat(64) }] },
      pools,
      "g1",
      "g1",
      "g1",
    ),
  ).toBe("waiting");
  expect(
    canvasPublicationStage(
      journal,
      { ...editor, canvases: [{ ...publishedEditorCanvas, pool_refs: ["user/other"] }] },
      pools,
      "g1",
      "g1",
      "g1",
    ),
  ).toBe("waiting");
});

it("cannot publish a failed or unverified request", () => {
  expect(
    canvasPublicationStage({ ...journal, status: "failed" }, editor, pools, "g1", "g1", "g1"),
  ).toBe("waiting");
  expect(
    canvasPublicationStage({ ...journal, recordHash: null }, editor, pools, "g1", "g1", "g1"),
  ).toBe("waiting");
  expect(canvasPublicationStage(null, editor, pools, "g1", "g1", "g1")).toBe("waiting");
});
