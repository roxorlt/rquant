import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { POOL_EDITOR_JOURNAL_KEY } from "./editorSession";

const serving = metaEnvelope().serving;
const VERSION = "a".repeat(64);
const NEXT_VERSION = "b".repeat(64);
const catalog: Schemas["ScreenCatalogData"] = {
  source_kind: "serving",
  blocks: [
    {
      key: "not_st",
      label: "排除 ST",
      hint: "排除风险股票",
      category: "filter",
      category_label: "股票范围",
      parameters: [],
    },
    {
      key: "volume_ratio_gte",
      label: "成交量放大",
      hint: "比近期更活跃",
      category: "indicator",
      category_label: "指标",
      parameters: [
        {
          key: "n",
          label: "放量倍数",
          input: "number",
          initial: 2,
          required: true,
          minimum: 1,
          maximum: 10,
          scale: 1,
          options: [],
          hint: null,
        },
        {
          key: "window",
          label: "回看交易日",
          input: "integer",
          initial: 5,
          required: true,
          minimum: 1,
          maximum: 60,
          scale: 1,
          options: [],
          hint: null,
        },
      ],
    },
  ],
  dates: [],
  available: false,
  ranking_metrics: [],
  source: null,
};
const published: Schemas["PoolsData"] = {
  state: "ready",
  latest_trade_date: "2026-09-23",
  definitions_available: true,
  rules_available: true,
  canvases: [
    {
      name: "观察画布",
      description: "日终观察",
      pool_keys: ["n-shape-pool1", "user/自建观察"],
      refs_truncated: false,
    },
  ],
  canvases_truncated: false,
  pools_truncated: false,
  pools: [
    {
      key: "n-shape-pool1",
      name: "首板池",
      state: "current",
      trade_date: "2026-09-23",
      member_count: 0,
      gain_verified_count: 0,
      gain_sample_avg_pct: null,
      steps: [],
      steps_truncated: false,
      members: [],
      members_truncated: false,
      definition: {
        name: "首板池",
        state: "available",
        status_label: "已发布",
        reason_label: null,
        source_label: "内置规则",
        description: "",
        depends_on: null,
        delay_label: null,
        rules: [],
      },
      result: {
        state: "current_rules",
        status_label: "结果已按当前规则更新",
        trade_date: "2026-09-23",
        hit_count: 0,
      },
    },
    {
      key: "user/自建观察",
      name: "自建观察",
      state: "current",
      trade_date: "2026-09-23",
      member_count: 0,
      gain_verified_count: 0,
      gain_sample_avg_pct: null,
      steps: [],
      steps_truncated: false,
      members: [],
      members_truncated: false,
      definition: {
        name: "自建观察",
        state: "available",
        status_label: "已发布",
        reason_label: null,
        source_label: "自建规则",
        description: "",
        depends_on: "n-shape-pool1",
        delay_label: "延后 1 日",
        rules: [],
      },
      result: {
        state: "current_rules",
        status_label: "结果已按当前规则更新",
        trade_date: "2026-09-23",
        hit_count: 0,
      },
    },
  ],
};
const editor: Schemas["PoolEditorData"] = {
  state: "ready",
  pools: [
    {
      key: "user/自建观察",
      display_name: "自建观察",
      description: "",
      version: VERSION,
      depends_on: "n-shape-pool1",
      delay_days: 1,
      rule_calls: [{ name: "volume_ratio_gte", args: { n: 2, window: 5 } }],
      include_columns: [],
    },
  ],
  canvases: [
    {
      name: "观察画布",
      description: "日终观察",
      version: "c".repeat(64),
      pool_refs: ["n-shape-pool1", "user/自建观察"],
    },
  ],
};

beforeEach(() => window.sessionStorage.clear());

function respond(options: { editor?: Schemas["PoolEditorData"]; editorGeneration?: string } = {}) {
  server.use(
    http.get("*/api/v1/pools", () => HttpResponse.json({ data: published, serving })),
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({
        data: options.editor ?? editor,
        serving: { ...serving, generation_id: options.editorGeneration ?? serving.generation_id },
      }),
    ),
    http.get("*/api/v1/screen/blocks", () => HttpResponse.json({ data: catalog, serving })),
  );
}

it("creates a child condition, previews the exact parent and rules, saves, then attaches to selected canvas", async () => {
  respond();
  const commands: Array<Schemas["SavePoolCommand"] | Schemas["AttachPoolCommand"]> = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as
        | Schemas["SavePoolCommand"]
        | Schemas["AttachPoolCommand"];
      commands.push(body);
      const stored = JSON.parse(window.sessionStorage.getItem(POOL_EDITOR_JOURNAL_KEY) ?? "{}");
      expect(body).toEqual(body.kind === "save_user_pool_v2" ? stored.save : stored.attach);
      return HttpResponse.json(
        body.kind === "save_user_pool_v2"
          ? {
              command_id: body.command_id,
              status: "succeeded",
              message: "池子已保存",
              pool_version: NEXT_VERSION,
            }
          : {
              command_id: body.command_id,
              status: "succeeded",
              message: "池子已加入当前画布",
              pool_version: NEXT_VERSION,
              canvas_name: "观察画布",
            },
      );
    }),
  );
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "添加条件节点" }));
  const dialog = screen.getByRole("dialog", { name: "添加条件节点" });
  await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "放量确认");
  await user.selectOptions(within(dialog).getByRole("combobox", { name: "父池" }), "n-shape-pool1");
  await user.clear(within(dialog).getByRole("spinbutton", { name: "延后交易日" }));
  await user.type(within(dialog).getByRole("spinbutton", { name: "延后交易日" }), "2");
  await user.selectOptions(
    within(dialog).getByRole("combobox", { name: "条件目录" }),
    "volume_ratio_gte",
  );
  await user.click(within(dialog).getByRole("button", { name: "添加条件" }));
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("首板池");
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("成交量放大");
  expect(screen.queryByText("池子已保存")).not.toBeInTheDocument();
  expect(screen.queryByText("已加入当前画布")).not.toBeInTheDocument();
  await user.click(within(dialog).getByRole("button", { name: "保存并加入画布" }));
  expect(await within(dialog).findByText("已加入当前画布")).toBeInTheDocument();
  expect(screen.getByText("池子已保存")).toBeInTheDocument();
  expect(commands).toHaveLength(2);
  expect(commands[0]).toMatchObject({
    base_name: "放量确认",
    depends_on: "n-shape-pool1",
    delay_days: 2,
    expected_version: null,
  });
  expect(commands[1]).toMatchObject({
    kind: "add_pool_to_canvas",
    canvas_name: "观察画布",
    expected_pool_version: NEXT_VERSION,
  });
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("edits only the verified custom version and does not offer direct builtin editing", async () => {
  respond();
  const commands: Schemas["SavePoolCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["SavePoolCommand"];
      commands.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        message: "池子已保存",
        pool_version: NEXT_VERSION,
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "查看 自建观察条件" }));
  await user.click(screen.getByRole("button", { name: "编辑规则" }));
  const dialog = screen.getByRole("dialog", { name: "编辑规则" });
  expect(within(dialog).getByRole("textbox", { name: "池子名称" })).toHaveValue("自建观察");
  await user.clear(within(dialog).getByRole("spinbutton", { name: "放量倍数" }));
  await user.type(within(dialog).getByRole("spinbutton", { name: "放量倍数" }), "3");
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  await user.click(within(dialog).getByRole("button", { name: "保存规则" }));
  expect(commands).toHaveLength(1);
  expect(commands[0]).toMatchObject({
    base_name: "自建观察",
    expected_version: VERSION,
    rule_calls: [{ name: "volume_ratio_gte", args: { n: 3, window: 5 } }],
  });
  await user.click(within(dialog).getByRole("button", { name: "返回画布" }));
  await user.click(screen.getByRole("button", { name: "查看 首板池条件" }));
  expect(screen.getByRole("button", { name: "复制为自建池" })).toBeDisabled();
});

it("keeps an existing independent pool independent when editing its conditions", async () => {
  const original = editor.pools.at(0);
  if (!original) throw new Error("test fixture missing custom pool");
  respond({
    editor: {
      ...editor,
      pools: [{ ...original, depends_on: null, delay_days: 0 }],
    },
  });
  const commands: Schemas["SavePoolCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["SavePoolCommand"];
      commands.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        message: "池子已保存",
        pool_version: NEXT_VERSION,
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "查看 自建观察条件" }));
  await user.click(screen.getByRole("button", { name: "编辑规则" }));
  const dialog = screen.getByRole("dialog", { name: "编辑规则" });
  expect(within(dialog).getByRole("combobox", { name: "父池" })).toHaveValue("");
  expect(within(dialog).getByRole("spinbutton", { name: "延后交易日" })).toHaveValue(0);
  await user.clear(within(dialog).getByRole("spinbutton", { name: "放量倍数" }));
  await user.type(within(dialog).getByRole("spinbutton", { name: "放量倍数" }), "4");
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("独立筛选");
  await user.click(within(dialog).getByRole("button", { name: "保存规则" }));
  expect(commands).toHaveLength(1);
  expect(commands[0]).toMatchObject({ depends_on: null, delay_days: 0, expected_version: VERSION });
});

it("blocks editing when the editor's Serving generation differs", async () => {
  respond({ editorGeneration: "f".repeat(64) });
  renderApp("/pools");
  expect(await screen.findByRole("button", { name: "添加条件节点" })).toBeDisabled();
  expect(screen.getByText("池子数据正在更新")).toBeInTheDocument();
});
