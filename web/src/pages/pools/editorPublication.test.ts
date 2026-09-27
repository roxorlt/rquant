import type { Schemas } from "@/api/client";
import { publicationStage } from "./editorPublication";
import type { EditorJournal } from "./editorSession";

const VERSION = "c".repeat(64);
const GENERATION = "a".repeat(64);
const journal: EditorJournal = {
  schema: 1,
  save: {
    kind: "save_user_pool_v2",
    command_id: "save",
    requested_at: "2026-09-27T07:00:00Z",
    base_name: "放量确认",
    display_name: "放量确认",
    description: "",
    rule_calls: [{ name: "not_st", args: {} }],
    include_columns: [],
    depends_on: "n-shape-pool1",
    delay_days: 1,
    expected_version: null,
  },
  canvasName: "观察画布",
  saveVersion: VERSION,
  saveStatus: "succeeded",
  attach: {
    kind: "add_pool_to_canvas",
    command_id: "attach",
    requested_at: "2026-09-27T07:01:00Z",
    canvas_name: "观察画布",
    pool_name: "user/放量确认",
    expected_pool_version: VERSION,
  },
  attachStatus: "succeeded",
};
const editor: Schemas["PoolEditorData"] = {
  state: "ready",
  pools: [
    {
      key: "user/放量确认",
      display_name: "放量确认",
      description: "",
      version: VERSION,
      depends_on: "n-shape-pool1",
      delay_days: 1,
      rule_calls: [],
      include_columns: [],
    },
  ],
  canvases: [
    { name: "观察画布", description: "", version: "d".repeat(64), pool_refs: ["user/放量确认"] },
  ],
};
const pools: Schemas["PoolsData"] = {
  state: "ready",
  latest_trade_date: "2026-09-23",
  definitions_available: true,
  rules_available: true,
  canvases: [
    { name: "观察画布", description: "", pool_keys: ["user/放量确认"], refs_truncated: false },
  ],
  canvases_truncated: false,
  pools_truncated: false,
  pools: [
    {
      key: "user/放量确认",
      name: "放量确认",
      state: "current",
      trade_date: "2026-09-23",
      member_count: 1,
      gain_verified_count: 0,
      gain_sample_avg_pct: null,
      steps: [],
      steps_truncated: false,
      members: [],
      members_truncated: false,
      definition: {
        name: "放量确认",
        state: "available",
        status_label: "已发布",
        reason_label: null,
        source_label: "自建池",
        description: "",
        depends_on: "n-shape-pool1",
        delay_label: "延后 1 日",
        rules: [],
      },
      result: {
        state: "current_rules",
        status_label: "结果已按当前规则更新",
        trade_date: "2026-09-23",
        hit_count: 1,
      },
    },
  ],
};

it("requires one generation, target version, canvas membership, and current-rules receipt", () => {
  const editorPool = editor.pools.at(0);
  const publishedPool = pools.pools.at(0);
  if (!editorPool || !publishedPool) throw new Error("test fixture missing pool");
  expect(publicationStage(journal, editor, pools, GENERATION, GENERATION, GENERATION)).toBe(
    "result",
  );
  expect(publicationStage(journal, editor, pools, GENERATION, "b".repeat(64), GENERATION)).toBe(
    "attached",
  );
  expect(
    publicationStage(
      journal,
      { ...editor, pools: [{ ...editorPool, version: "e".repeat(64) }] },
      pools,
      GENERATION,
      GENERATION,
      GENERATION,
    ),
  ).toBe("attached");
  expect(
    publicationStage(
      journal,
      { ...editor, canvases: [] },
      pools,
      GENERATION,
      GENERATION,
      GENERATION,
    ),
  ).toBe("attached");
  expect(
    publicationStage(
      journal,
      editor,
      {
        ...pools,
        pools: [{ ...publishedPool, result: { ...publishedPool.result, state: "unverified" } }],
      },
      GENERATION,
      GENERATION,
      GENERATION,
    ),
  ).toBe("published");
  expect(
    publicationStage(
      { ...journal, attachStatus: "failed" },
      editor,
      pools,
      GENERATION,
      GENERATION,
      GENERATION,
    ),
  ).toBe("saved");
});
