import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { type EditorJournal, POOL_EDITOR_JOURNAL_KEY } from "./editorSession";

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
          custom_ma: false,
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
          custom_ma: false,
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
      name: "N 形态一池",
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
        name: "N 形态一池",
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
  canvas_create_available: true,
  copy_sources: [],
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
      command_id: "canvas-observe",
      record_hash: "e".repeat(64),
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

it("池子编辑目录不提供尚未接入池子执行的数据项", async () => {
  respond();
  server.use(
    http.get("*/api/v1/screen/blocks", () =>
      HttpResponse.json({
        data: {
          ...catalog,
          blocks: [
            ...catalog.blocks,
            {
              key: "gt",
              label: "大于",
              hint: "比较两项数据",
              category: "compare",
              category_label: "数值比较",
              parameters: (["left", "right"] as const).map((key) => ({
                key,
                label: key === "left" ? "左侧" : "右侧",
                input: "operand" as const,
                initial: "CLOSE[0]",
                required: true,
                scale: 1,
                custom_ma: true,
                options: [
                  { value: "CLOSE[0]", label: "收盘价" },
                  { value: "PE_TTM[0]", label: "市盈率（倍）" },
                ],
              })),
            },
          ],
        },
        serving,
      }),
    ),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "添加条件节点" }));
  const dialog = screen.getByRole("dialog", { name: "添加条件节点" });
  await user.selectOptions(within(dialog).getByRole("combobox", { name: "条件目录" }), "gt");
  await user.click(within(dialog).getByRole("button", { name: "添加条件" }));
  expect(within(dialog).getAllByRole("option", { name: "收盘价" })).toHaveLength(2);
  expect(within(dialog).queryAllByRole("option", { name: "市盈率（倍）" })).toHaveLength(0);
});

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
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("N 形态一池");
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("成交量放大");
  expect(screen.queryByText("池子已保存")).not.toBeInTheDocument();
  expect(screen.queryByText("已加入当前画布")).not.toBeInTheDocument();
  await user.click(within(dialog).getByRole("button", { name: "保存并加入画布" }));
  expect(await within(dialog).findByText("加入请求已完成，等待画布更新")).toBeInTheDocument();
  expect(within(dialog).getByText("池子已保存")).toBeInTheDocument();
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

it("keeps a published attachment tied to its actual canvas after switching the visible canvas", async () => {
  const otherCanvas = {
    name: "备用画布",
    description: "",
    pool_keys: [],
    refs_truncated: false,
  };
  respond({
    editor: {
      ...editor,
      canvases: [
        ...editor.canvases,
        {
          name: otherCanvas.name,
          description: "",
          version: "d".repeat(64),
          command_id: "canvas-other",
          record_hash: "e".repeat(64),
          pool_refs: [],
        },
      ],
    },
  });
  server.use(
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: { ...published, canvases: [...published.canvases, otherCanvas] },
        serving,
      }),
    ),
  );
  const journal: EditorJournal = {
    schema: 1,
    save: {
      kind: "save_user_pool_v2",
      command_id: "save-first",
      requested_at: "2026-09-27T07:00:00Z",
      base_name: "自建观察",
      display_name: "自建观察",
      description: "",
      depends_on: "n-shape-pool1",
      delay_days: 1,
      rule_calls: [{ name: "not_st", args: {} }],
      include_columns: [],
      expected_version: null,
    },
    canvasName: "观察画布",
    saveVersion: VERSION,
    saveStatus: "succeeded",
    attach: {
      kind: "add_pool_to_canvas",
      command_id: "attach-first",
      requested_at: "2026-09-27T07:00:01Z",
      canvas_name: "观察画布",
      pool_name: "user/自建观察",
      expected_pool_version: VERSION,
    },
    attachStatus: "succeeded",
  };
  window.sessionStorage.setItem(POOL_EDITOR_JOURNAL_KEY, JSON.stringify(journal));
  const user = userEvent.setup();
  renderApp("/pools");
  expect(await screen.findByText("已加入「观察画布」")).toBeInTheDocument();
  await user.selectOptions(screen.getByRole("combobox", { name: "选择画布" }), "备用画布");
  expect(screen.getByText("这张画布还是空的")).toBeInTheDocument();
  expect(screen.getByText("已加入「观察画布」")).toBeInTheDocument();
  expect(screen.queryByText("已加入当前画布")).not.toBeInTheDocument();
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
  await user.click(screen.getByRole("button", { name: "查看 N 形态一池条件" }));
  expect(screen.getByRole("button", { name: "复制为自建池" })).toBeDisabled();
});

const copySource: Schemas["BuiltinPoolCopySource"] = {
  key: "n-shape-pool1",
  display_name: "N 形态一池",
  description: "",
  version: "e".repeat(64),
  depends_on: null,
  delay_mode: "none",
  delay_days: 0,
  rule_calls: [{ name: "not_st", args: {} }],
  include_columns: [],
  copyable: true,
  copy_block_reason: null,
};

const option = (value: string, label: string): Schemas["ScreenOption"] => ({ value, label });
const parameter = (
  key: string,
  label: string,
  input: Schemas["ScreenParameter"]["input"],
  options: Schemas["ScreenOption"][] = [],
): Schemas["ScreenParameter"] => ({
  key,
  label,
  input,
  initial: options[0]?.value ?? 0,
  required: true,
  minimum: null,
  maximum: null,
  scale: 1,
  options,
  hint: "",
  custom_ma: false,
});
const block = (
  key: string,
  label: string,
  parameters: Schemas["ScreenParameter"][] = [],
): Schemas["ScreenBlock"] => ({
  key,
  label,
  hint: "",
  category: "filter",
  category_label: "条件",
  parameters,
});
const builtinOneRules: Schemas["BuiltinPoolCopySource"]["rule_calls"] = [
  { name: "not_st", args: {} },
  { name: "not_bj", args: {} },
  { name: "first_limit_up", args: { offset: 1 } },
  { name: "not_limit_up", args: { offset: 0 } },
  { name: "not_yiziban", args: { offset: 1 } },
  { name: "gt", args: { left: "HIGH[0]", right: "CLOSE[1]" } },
  { name: "circ_mv_lt", args: { threshold_yi: 150 } },
  { name: "has_lower_shadow", args: { min_ratio: 0.5, min_amplitude: 0.02, offset: 0 } },
  { name: "no_consec_ups_in_window", args: { threshold: 3, window: 8 } },
  { name: "no_limit_down_in_window", args: { window: 30 } },
  { name: "has_prior_limit_up", args: { window: 120, exclude_offset: 1 } },
];
const numberParam = (key: string, label: string) => parameter(key, label, "number");
const intParam = (key: string, label: string) => parameter(key, label, "integer");
const builtinOneCatalog: Schemas["ScreenBlock"][] = [
  block("not_st", "排除 ST"),
  block("not_bj", "排除北交所"),
  block("first_limit_up", "首板", [intParam("offset", "相对日期")]),
  block("not_limit_up", "未涨停", [intParam("offset", "相对日期")]),
  block("not_yiziban", "非一字板", [intParam("offset", "相对日期")]),
  block("gt", "大于", [
    parameter("left", "左侧", "operand", [option("HIGH[0]", "最高价")]),
    parameter("right", "右侧", "operand", [option("CLOSE[0]", "收盘价")]),
  ]),
  block("circ_mv_lt", "流通市值低于", [numberParam("threshold_yi", "市值上限")]),
  block("has_lower_shadow", "明显下影线", [
    numberParam("min_ratio", "下影线倍数"),
    numberParam("min_amplitude", "最小振幅"),
    intParam("offset", "相对日期"),
  ]),
  block("no_consec_ups_in_window", "近期无高连板", [
    numberParam("threshold", "连板下限"),
    intParam("window", "回看交易日"),
  ]),
  block("no_limit_down_in_window", "近期无跌停", [intParam("window", "回看交易日")]),
  block("has_prior_limit_up", "近期曾涨停", [
    intParam("window", "回看交易日"),
    intParam("exclude_offset", "排除前几日"),
  ]),
];

it("copies every verified builtin-one rule, including the prior close operand", async () => {
  respond({
    editor: { ...editor, copy_sources: [{ ...copySource, rule_calls: builtinOneRules }] },
  });
  server.use(
    http.get("*/api/v1/screen/blocks", () =>
      HttpResponse.json({
        data: { ...catalog, blocks: builtinOneCatalog },
        serving,
      }),
    ),
  );
  const commands: Schemas["SavePoolCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["SavePoolCommand"];
      commands.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        message: "已保存",
        pool_version: NEXT_VERSION,
      });
    }),
  );
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "查看 N 形态一池条件" }));
  await user.click(screen.getByRole("button", { name: "复制为自建池" }));
  const dialog = screen.getByRole("dialog", { name: "复制为自建池" });
  expect(within(dialog).getByRole("combobox", { name: "右侧" })).toHaveValue("CLOSE[1]");
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent(
    "前一交易日收盘价",
  );
  await user.click(within(dialog).getByRole("button", { name: "保存并加入画布" }));
  expect(commands[0]?.rule_calls).toEqual(builtinOneRules);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("edits existing numeric moving-average and RSI periods without changing their types", async () => {
  const original = editor.pools[0];
  if (!original) throw new Error("missing custom pool");
  respond({
    editor: {
      ...editor,
      pools: [
        {
          ...original,
          rule_calls: [
            { name: "above_ma", args: { period: 20, offset: 0 } },
            { name: "rsi_oversold", args: { period: 14, threshold: 30, offset: 0 } },
          ],
        },
      ],
    },
  });
  server.use(
    http.get("*/api/v1/screen/blocks", () =>
      HttpResponse.json({
        data: {
          ...catalog,
          blocks: [
            block("above_ma", "收盘价高于均线", [
              parameter("period", "指标周期", "choice", [
                option("5", "5 日均线"),
                option("20", "20 日均线"),
              ]),
              intParam("offset", "相对日期"),
            ]),
            block("rsi_oversold", "RSI 超卖", [
              parameter("period", "指标周期", "choice", [
                option("6", "6 日 RSI"),
                option("14", "14 日 RSI"),
              ]),
              numberParam("threshold", "RSI 门槛"),
              intParam("offset", "相对日期"),
            ]),
          ],
        },
        serving,
      }),
    ),
  );
  const commands: Schemas["SavePoolCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["SavePoolCommand"];
      commands.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        message: "已保存",
        pool_version: NEXT_VERSION,
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "查看 自建观察条件" }));
  await user.click(screen.getByRole("button", { name: "编辑规则" }));
  const dialog = screen.getByRole("dialog", { name: "编辑规则" });
  expect(
    within(dialog)
      .getAllByRole("combobox", { name: "指标周期" })
      .map((item) => (item as HTMLSelectElement).value),
  ).toEqual(["20", "14"]);
  const firstPeriod = within(dialog).getAllByRole("combobox", { name: "指标周期" })[0];
  if (!firstPeriod) throw new Error("missing period control");
  await user.selectOptions(firstPeriod, "5");
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  await user.click(within(dialog).getByRole("button", { name: "保存规则" }));
  expect(commands[0]?.rule_calls).toEqual([
    { name: "above_ma", args: { period: 5, offset: 0 } },
    { name: "rsi_oversold", args: { period: 14, threshold: 30, offset: 0 } },
  ]);
});

it("copies verified builtin rules as a new custom pool without guessing its delay", async () => {
  respond({ editor: { ...editor, copy_sources: [copySource] } });
  const commands: Array<Schemas["SavePoolCommand"] | Schemas["AttachPoolCommand"]> = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as
        | Schemas["SavePoolCommand"]
        | Schemas["AttachPoolCommand"];
      commands.push(body);
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
  await user.click(await screen.findByRole("button", { name: "查看 N 形态一池条件" }));
  await user.click(screen.getByRole("button", { name: "复制为自建池" }));
  const dialog = screen.getByRole("dialog", { name: "复制为自建池" });
  expect(within(dialog).getByRole("textbox", { name: "池子名称" })).toHaveValue("N形态一池副本");
  expect(within(dialog).getByRole("combobox", { name: "父池" })).toHaveValue("");
  expect(within(dialog).getByRole("spinbutton", { name: "延后交易日" })).toHaveValue(0);
  expect(within(dialog).getByRole("region", { name: "筛选条件" })).toHaveTextContent("排除 ST");
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent(
    "来自「N 形态一池」",
  );
  await user.click(within(dialog).getByRole("button", { name: "保存并加入画布" }));
  expect(await within(dialog).findByText("加入请求已完成，等待画布更新")).toBeInTheDocument();
  expect(commands[0]).toMatchObject({
    kind: "save_user_pool_v2",
    base_name: "N形态一池副本",
    depends_on: null,
    delay_days: 0,
    rule_calls: copySource.rule_calls,
    include_columns: [],
    expected_version: null,
  });
  expect(commands[1]).toMatchObject({
    kind: "add_pool_to_canvas",
    expected_pool_version: NEXT_VERSION,
  });
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("explains why an old windowed builtin cannot be copied", async () => {
  respond({
    editor: {
      ...editor,
      copy_sources: [
        {
          ...copySource,
          delay_mode: "legacy_window",
          copyable: false,
          copy_block_reason: "旧版时间窗口与精确延后日不同，暂不能无损复制。",
        },
      ],
    },
  });
  const user = userEvent.setup();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "查看 N 形态一池条件" }));
  const copy = screen.getByRole("button", { name: "复制为自建池" });
  expect(copy).toBeDisabled();
  expect(copy).toHaveAttribute(
    "aria-description",
    "旧版时间窗口与精确延后日不同，暂不能无损复制。",
  );
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

it("shows an attachment resume action after reload and keeps its original request", async () => {
  respond();
  const attach = {
    kind: "add_pool_to_canvas" as const,
    command_id: "attach-original",
    requested_at: "2026-09-27T07:00:00.000Z",
    canvas_name: "观察画布",
    pool_name: "user/放量确认",
    expected_pool_version: NEXT_VERSION,
  };
  window.sessionStorage.setItem(
    POOL_EDITOR_JOURNAL_KEY,
    JSON.stringify({
      schema: 1,
      save: {
        kind: "save_user_pool_v2",
        command_id: "save-original",
        requested_at: "2026-09-27T07:00:00.000Z",
        base_name: "放量确认",
        display_name: "放量确认",
        description: "",
        depends_on: "n-shape-pool1",
        delay_days: 1,
        rule_calls: [{ name: "not_st", args: {} }],
        include_columns: [],
        expected_version: null,
      },
      canvasName: "观察画布",
      saveVersion: NEXT_VERSION,
      saveStatus: "succeeded",
      attach,
      attachStatus: "ambiguous",
    }),
  );
  const seen: unknown[] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = await request.json();
      seen.push(body);
      return HttpResponse.json({
        command_id: attach.command_id,
        status: seen.length === 1 ? "unknown" : "succeeded",
        message: "待确认",
        pool_version: NEXT_VERSION,
        canvas_name: "观察画布",
      });
    }),
  );
  const user = userEvent.setup();
  const first = renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "继续核对画布" }));
  await waitFor(() => expect(seen).toEqual([attach]));
  first.unmount();
  renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "继续核对画布" }));
  await waitFor(() => expect(seen).toEqual([attach, attach]));
  expect(await screen.findByText("加入请求已完成，等待画布更新")).toBeInTheDocument();
});

it("retains a draft through a data-generation change and requires a fresh preview", async () => {
  respond();
  const user = userEvent.setup();
  const { queryClient } = renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "添加条件节点" }));
  const dialog = screen.getByRole("dialog", { name: "添加条件节点" });
  await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "待保留草稿");
  await user.selectOptions(
    within(dialog).getByRole("combobox", { name: "条件目录" }),
    "volume_ratio_gte",
  );
  await user.click(within(dialog).getByRole("button", { name: "添加条件" }));
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("待保留草稿");
  const nextGeneration = "f".repeat(64);
  server.use(
    http.get("*/api/v1/meta", () =>
      HttpResponse.json(metaEnvelope({ generationId: nextGeneration })),
    ),
  );
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
  });
  await waitFor(() =>
    expect(document.querySelector(".gen-tag")).toHaveAttribute(
      "data-generation",
      nextGeneration.slice(0, 12),
    ),
  );
  expect(screen.getByRole("dialog", { name: "添加条件节点" })).toBeInTheDocument();
  expect(within(dialog).getByRole("textbox", { name: "池子名称" })).toHaveValue("待保留草稿");
  expect(within(dialog).getByRole("button", { name: "保存并加入画布" })).toBeDisabled();
  server.use(
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: published,
        serving: { ...serving, generation_id: nextGeneration },
      }),
    ),
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({ data: editor, serving: { ...serving, generation_id: nextGeneration } }),
    ),
    http.get("*/api/v1/screen/blocks", () =>
      HttpResponse.json({ data: catalog, serving: { ...serving, generation_id: nextGeneration } }),
    ),
  );
  await act(async () => {
    await queryClient.invalidateQueries();
  });
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: "预览变更" })).toBeEnabled(),
  );
  expect(within(dialog).getByRole("textbox", { name: "池子名称" })).toHaveValue("待保留草稿");
  expect(within(dialog).getByRole("button", { name: "保存并加入画布" })).toBeDisabled();
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("button", { name: "保存并加入画布" })).toBeEnabled();
});

it("never attaches a different V2 rule set after the saved V1 attachment conflicts", async () => {
  let currentVersion = VERSION;
  respond();
  server.use(
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({
        data: {
          ...editor,
          pools: editor.pools.map((pool) =>
            currentVersion === VERSION
              ? pool
              : {
                  ...pool,
                  version: currentVersion,
                  depends_on: null,
                  delay_days: 0,
                  rule_calls: [{ name: "volume_ratio_gte", args: { n: 7, window: 5 } }],
                },
          ),
        },
        serving,
      }),
    ),
  );
  window.sessionStorage.setItem(
    POOL_EDITOR_JOURNAL_KEY,
    JSON.stringify({
      schema: 1,
      save: {
        kind: "save_user_pool_v2",
        command_id: "save-old",
        requested_at: "2026-09-27T07:00:00Z",
        base_name: "自建观察",
        display_name: "自建观察",
        description: "",
        depends_on: "n-shape-pool1",
        delay_days: 1,
        rule_calls: [{ name: "not_st", args: {} }],
        include_columns: [],
        expected_version: null,
      },
      canvasName: "观察画布",
      saveVersion: VERSION,
      saveStatus: "succeeded",
      attach: {
        kind: "add_pool_to_canvas",
        command_id: "attach-old",
        requested_at: "2026-09-27T07:00:01Z",
        canvas_name: "观察画布",
        pool_name: "user/自建观察",
        expected_pool_version: VERSION,
      },
      attachStatus: "failed",
      attachConflict: true,
    }),
  );
  const seen: Schemas["AttachPoolCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["AttachPoolCommand"];
      seen.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        message: "已加入",
        pool_version: body.expected_pool_version,
        canvas_name: body.canvas_name,
      });
    }),
  );
  const user = userEvent.setup();
  const { queryClient } = renderApp("/pools");
  const retry = await screen.findByRole("button", { name: "按本次保存规则重试" });
  expect(screen.getByText("画布挂接失败")).toBeInTheDocument();
  expect(retry).toBeDisabled();
  currentVersion = NEXT_VERSION;
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["pools", "editor"] });
  });
  expect(screen.getByRole("button", { name: "按本次保存规则重试" })).toBeDisabled();
  expect(seen).toHaveLength(0);
  await user.click(screen.getByRole("button", { name: "结束本次挂接" }));
  expect(seen).toHaveLength(0);
  expect(screen.queryByText("已加入当前画布")).not.toBeInTheDocument();
  expect(document.querySelector(".pools-editor-evidence")).toHaveTextContent("等待新规则选股");
});

it("keeps a failed V1 draft until explicitly reopening actual V2 rules and parent", async () => {
  let currentVersion = VERSION;
  respond();
  server.use(
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({
        data: {
          ...editor,
          pools: editor.pools.map((pool) =>
            currentVersion === VERSION
              ? pool
              : {
                  ...pool,
                  version: currentVersion,
                  depends_on: null,
                  delay_days: 0,
                  rule_calls: [{ name: "volume_ratio_gte", args: { n: 7, window: 5 } }],
                },
          ),
        },
        serving,
      }),
    ),
  );
  const commands: Schemas["SavePoolCommand"][] = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["SavePoolCommand"];
      commands.push(body);
      return commands.length === 1
        ? HttpResponse.json({ detail: "规则已更新" }, { status: 409 })
        : HttpResponse.json({
            command_id: body.command_id,
            status: "succeeded",
            message: "已保存",
            pool_version: "d".repeat(64),
          });
    }),
  );
  const user = userEvent.setup();
  const { queryClient } = renderApp("/pools");
  await user.click(await screen.findByRole("button", { name: "查看 自建观察条件" }));
  await user.click(screen.getByRole("button", { name: "编辑规则" }));
  const dialog = screen.getByRole("dialog", { name: "编辑规则" });
  await user.clear(within(dialog).getByRole("spinbutton", { name: "放量倍数" }));
  await user.type(within(dialog).getByRole("spinbutton", { name: "放量倍数" }), "4");
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("button", { name: "保存规则" })).toBeEnabled();
  await user.click(within(dialog).getByRole("button", { name: "保存规则" }));
  expect(commands).toHaveLength(1);
  expect(within(dialog).getByRole("spinbutton", { name: "放量倍数" })).toHaveValue(4);
  expect(
    JSON.parse(window.sessionStorage.getItem(POOL_EDITOR_JOURNAL_KEY) ?? "{}").saveStatus,
  ).toBe("failed");
  expect(within(dialog).getByRole("button", { name: "保存规则" })).toBeDisabled();
  currentVersion = NEXT_VERSION;
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["pools", "editor"] });
  });
  const restart = await within(dialog).findByRole("button", { name: "重新打开最新规则" });
  await waitFor(() => expect(restart).toBeEnabled());
  expect(within(dialog).getByRole("spinbutton", { name: "放量倍数" })).toHaveValue(4);
  expect(within(dialog).getByRole("button", { name: "保存规则" })).toBeDisabled();
  expect(commands).toHaveLength(1);
  await user.click(restart);
  const latest = screen.getByRole("dialog", { name: "编辑规则" });
  expect(within(latest).getByRole("combobox", { name: "父池" })).toHaveValue("");
  expect(within(latest).getByRole("spinbutton", { name: "放量倍数" })).toHaveValue(7);
  expect(commands).toHaveLength(1);
  await user.clear(within(latest).getByRole("spinbutton", { name: "放量倍数" }));
  await user.type(within(latest).getByRole("spinbutton", { name: "放量倍数" }), "8");
  await user.click(within(latest).getByRole("button", { name: "预览变更" }));
  await user.click(within(latest).getByRole("button", { name: "保存规则" }));
  expect(commands[1]?.expected_version).toBe(NEXT_VERSION);
  expect(commands[1]?.depends_on).toBeNull();
  expect(commands[1]?.rule_calls).toEqual([
    { name: "volume_ratio_gte", args: { n: 8, window: 5 } },
  ]);
});

it("offers a safe exit after reloading a failed save, then opens the real V2 definition", async () => {
  const original = editor.pools[0];
  if (!original) throw new Error("missing custom pool");
  respond({
    editor: {
      ...editor,
      pools: [
        {
          ...original,
          version: NEXT_VERSION,
          depends_on: null,
          delay_days: 0,
          rule_calls: [{ name: "volume_ratio_gte", args: { n: 7, window: 5 } }],
        },
      ],
    },
  });
  window.sessionStorage.setItem(
    POOL_EDITOR_JOURNAL_KEY,
    JSON.stringify({
      schema: 1,
      save: {
        kind: "save_user_pool_v2",
        command_id: "save-before-reload",
        requested_at: "2026-09-27T07:00:00Z",
        base_name: "自建观察",
        display_name: "自建观察",
        description: "",
        depends_on: "n-shape-pool1",
        delay_days: 1,
        rule_calls: [{ name: "volume_ratio_gte", args: { n: 4, window: 5 } }],
        include_columns: [],
        expected_version: VERSION,
      },
      canvasName: null,
      saveVersion: null,
      saveStatus: "failed",
      saveConflict: true,
      attach: null,
      attachStatus: "idle",
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  expect(await screen.findByText("上次保存未完成")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "结束本次编辑" }));
  expect(window.sessionStorage.getItem(POOL_EDITOR_JOURNAL_KEY)).toBeNull();
  await user.click(screen.getByRole("button", { name: "查看 自建观察条件" }));
  await user.click(screen.getByRole("button", { name: "编辑规则" }));
  const latest = screen.getByRole("dialog", { name: "编辑规则" });
  expect(within(latest).getByRole("combobox", { name: "父池" })).toHaveValue("");
  expect(within(latest).getByRole("spinbutton", { name: "放量倍数" })).toHaveValue(7);
});
