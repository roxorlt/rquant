import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { POOL_EDITOR_JOURNAL_KEY } from "./editorSession";

const VERSION = "b".repeat(64);
const CANVAS = "晨盘观察";
const BLOCKS: Schemas["ScreenCatalogData"] = {
  source_kind: "serving",
  nl_generate_available: false,
  blocks: [
    {
      key: "not_st",
      label: "排除 ST",
      hint: "排除风险股票",
      category: "filter",
      category_label: "股票范围",
      parameters: [],
    },
  ],
  dates: [],
  available: false,
  ranking_metrics: [],
  source: null,
};
const BUILTIN_POOL: Schemas["PublishedPool"] = {
  key: "n-shape-pool1",
  name: "N 形态一池",
  state: "unpublished",
  trade_date: null,
  member_count: null,
  gain_verified_count: 0,
  gain_sample_avg_pct: null,
  steps: [],
  steps_truncated: false,
  members: [],
  members_truncated: false,
  result: { state: "not_run", status_label: "尚无选股结果", trade_date: null, hit_count: null },
};

beforeEach(() => window.sessionStorage.clear());

function respondFirstPool(
  options: {
    published?: () => boolean;
    catalog?: Schemas["ScreenCatalogData"] | null;
    editorState?: "ready" | "unavailable";
    editorCanvasAvailable?: boolean;
    builtinPools?: boolean;
    generation?: () => string;
    catalogGeneration?: string;
  } = {},
) {
  const isPublished = options.published ?? (() => false);
  const generation = () => options.generation?.() ?? (isPublished() ? "g2" : "g1");
  const serving = () => metaEnvelope({ generationId: generation() }).serving;
  const key = "user/首只观察";
  server.use(
    http.get("*/api/v1/meta", () =>
      HttpResponse.json(metaEnvelope({ generationId: generation() })),
    ),
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: {
          state: isPublished() || options.builtinPools ? "ready" : "no_data",
          latest_trade_date: null,
          definitions_available: true,
          rules_available: true,
          canvases: [
            {
              name: CANVAS,
              description: "观察候选池",
              pool_keys: isPublished() ? [key] : [],
              refs_truncated: false,
            },
          ],
          canvases_truncated: false,
          pools_truncated: false,
          pools: [
            ...(options.builtinPools
              ? [BUILTIN_POOL, { ...BUILTIN_POOL, key: "n-shape-pool2", name: "N 形态二池" }]
              : []),
            ...(isPublished()
              ? [
                  {
                    key,
                    name: "首只观察",
                    state: "unpublished",
                    trade_date: null,
                    member_count: null,
                    gain_verified_count: 0,
                    gain_sample_avg_pct: null,
                    steps: [],
                    steps_truncated: false,
                    members: [],
                    members_truncated: false,
                    definition: {
                      name: "首只观察",
                      state: "available",
                      status_label: "已发布",
                      reason_label: null,
                      source_label: "自建规则",
                      description: "",
                      depends_on: null,
                      delay_label: null,
                      rules: [{ label: "排除 ST", parameters: [] }],
                    },
                    result: {
                      state: "not_run",
                      status_label: "尚无选股结果",
                      trade_date: null,
                      hit_count: null,
                    },
                  } satisfies Schemas["PublishedPool"],
                ]
              : []),
          ],
        } satisfies Schemas["PoolsData"],
        serving: serving(),
      }),
    ),
    http.get("*/api/v1/pools/editor", () =>
      HttpResponse.json({
        data: {
          state: options.editorState ?? "ready",
          canvas_create_available: true,
          nl_preview_available: false,
          pools: isPublished()
            ? [
                {
                  key,
                  display_name: "首只观察",
                  description: "",
                  version: VERSION,
                  save_kind: "save_user_pool_v2",
                  depends_on: null,
                  delay_days: 0,
                  rule_calls: [{ name: "not_st", args: {} }],
                  include_columns: [],
                },
              ]
            : [],
          copy_sources: [],
          canvases:
            options.editorCanvasAvailable === false
              ? []
              : [
                  {
                    name: CANVAS,
                    description: "观察候选池",
                    version: "c".repeat(64),
                    command_id: "create-canvas",
                    record_hash: "d".repeat(64),
                    pool_refs: isPublished() ? [key] : [],
                  },
                ],
        } satisfies Schemas["PoolEditorData"],
        serving: serving(),
      }),
    ),
    http.get("*/api/v1/screen/blocks", () =>
      options.catalog === null
        ? HttpResponse.json({ detail: "目录不可用" }, { status: 503 })
        : HttpResponse.json({
            data: options.catalog ?? BLOCKS,
            serving: metaEnvelope({ generationId: options.catalogGeneration ?? generation() })
              .serving,
          }),
    ),
  );
}

it("creates the first independent pool on a published empty canvas and waits for its publication", async () => {
  let published = false;
  respondFirstPool({ published: () => published });
  const commands: Array<Schemas["SavePoolCommand"] | Schemas["AttachPoolCommand"]> = [];
  server.use(
    http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
      const body = (await request.json()) as
        | Schemas["SavePoolCommand"]
        | Schemas["AttachPoolCommand"];
      commands.push(body);
      const stored = JSON.parse(window.sessionStorage.getItem(POOL_EDITOR_JOURNAL_KEY) ?? "{}");
      expect(body).toEqual(body.kind === "save_user_pool_v2" ? stored.save : stored.attach);
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      return HttpResponse.json(
        body.kind === "save_user_pool_v2"
          ? {
              command_id: body.command_id,
              status: "succeeded",
              message: "池子已保存",
              pool_version: VERSION,
            }
          : {
              command_id: body.command_id,
              status: "succeeded",
              message: "加入请求已完成",
              pool_version: VERSION,
              canvas_name: CANVAS,
            },
      );
    }),
  );
  const user = userEvent.setup();
  const { container, queryClient } = renderApp("/pools");
  await screen.findByRole("button", { name: "创建首只池子" });
  await waitFor(() => expect(screen.getByRole("button", { name: "创建首只池子" })).toBeEnabled());
  screen.getByRole("button", { name: "创建首只池子" }).focus();
  await user.keyboard("{Enter}");
  const dialog = screen.getByRole("dialog", { name: "创建首只池子" });
  expect(within(dialog).getByRole("combobox", { name: "筛选来源" })).toHaveValue("");
  expect(within(dialog).getByRole("combobox", { name: "目标画布" })).toHaveValue(CANVAS);
  await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "首只观察");
  expect(within(dialog).getByRole("button", { name: "预览变更" })).toBeDisabled();
  await user.selectOptions(within(dialog).getByRole("combobox", { name: "条件目录" }), "not_st");
  await user.click(within(dialog).getByRole("button", { name: "添加条件" }));
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("独立筛选");
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent(
    `保存后加入「${CANVAS}」`,
  );
  expect(commands).toHaveLength(0);
  await user.click(within(dialog).getByRole("button", { name: "保存并加入画布" }));
  await waitFor(() => expect(commands).toHaveLength(2));
  expect(commands[0]).toMatchObject({
    kind: "save_user_pool_v2",
    base_name: "首只观察",
    depends_on: null,
    delay_days: 0,
    rule_calls: [{ name: "not_st", args: {} }],
    expected_version: null,
  });
  expect(commands[1]).toMatchObject({
    kind: "add_pool_to_canvas",
    canvas_name: CANVAS,
    pool_name: "user/首只观察",
    expected_pool_version: VERSION,
  });
  expect(within(dialog).getByText("加入请求已完成，等待画布更新")).toBeInTheDocument();
  expect(within(dialog).queryByText("已加入当前画布")).not.toBeInTheDocument();
  await user.click(within(dialog).getByRole("button", { name: "返回画布" }));
  expect(screen.getByRole("status")).toHaveTextContent("等待画布更新");
  published = true;
  await act(async () => {
    await queryClient.invalidateQueries();
  });
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent(`已加入「${CANVAS}」`));
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("creates an independent first node while builtin pools exist outside the selected empty canvas", async () => {
  respondFirstPool({ builtinPools: true });
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
              pool_version: VERSION,
            }
          : {
              command_id: body.command_id,
              status: "succeeded",
              message: "加入请求已完成",
              pool_version: VERSION,
              canvas_name: CANVAS,
            },
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  await screen.findByRole("button", { name: "创建首只池子" });
  await waitFor(() => expect(screen.getByRole("button", { name: "创建首只池子" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "创建首只池子" }));
  const dialog = screen.getByRole("dialog", { name: "创建首只池子" });
  expect(within(dialog).getByRole("combobox", { name: "筛选来源" })).toHaveValue("");
  await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "首只观察");
  await user.selectOptions(within(dialog).getByRole("combobox", { name: "条件目录" }), "not_st");
  await user.click(within(dialog).getByRole("button", { name: "添加条件" }));
  await user.click(within(dialog).getByRole("button", { name: "预览变更" }));
  expect(within(dialog).getByRole("region", { name: "变更预览" })).toHaveTextContent("独立筛选");
  await user.click(within(dialog).getByRole("button", { name: "保存并加入画布" }));
  await waitFor(() => expect(commands).toHaveLength(2));
  expect(commands[0]).toMatchObject({ depends_on: null, delay_days: 0 });
  expect(commands[1]).toMatchObject({ kind: "add_pool_to_canvas", canvas_name: CANVAS });
});

it("keeps an independent replica condition catalog usable across a Serving change without reload", async () => {
  let generation = "g1";
  respondFirstPool({
    generation: () => generation,
    catalog: { ...BLOCKS, source_kind: "replica" },
    catalogGeneration: "g1",
  });
  const user = userEvent.setup();
  const { queryClient } = renderApp("/pools");
  await screen.findByRole("button", { name: "创建首只池子" });
  await waitFor(() => expect(screen.getByRole("button", { name: "创建首只池子" })).toBeEnabled());
  generation = "g2";
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
  });
  await waitFor(() =>
    expect(document.querySelector(".gen-tag")).toHaveAttribute("data-generation", "g2"),
  );
  await waitFor(() => expect(screen.getByRole("button", { name: "创建首只池子" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "创建首只池子" }));
  const dialog = screen.getByRole("dialog", { name: "创建首只池子" });
  await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "首只观察");
  await user.selectOptions(within(dialog).getByRole("combobox", { name: "条件目录" }), "not_st");
  await user.click(within(dialog).getByRole("button", { name: "添加条件" }));
  expect(within(dialog).getByRole("button", { name: "预览变更" })).toBeEnabled();
});

it.each([
  {
    name: "condition catalog is unavailable",
    options: { catalog: null },
    reason: "条件目录暂不可用",
  },
  {
    name: "condition catalog is empty",
    options: { catalog: { ...BLOCKS, blocks: [] } },
    reason: "条件目录暂不可用",
  },
  {
    name: "editor authority is unavailable",
    options: { editorState: "unavailable" as const },
    reason: "编辑资料暂不可用",
  },
  {
    name: "the selected canvas is missing from editor authority",
    options: { editorCanvasAvailable: false },
    reason: "画布资料正在更新",
  },
])("keeps first-pool creation unavailable when $name", async ({ options, reason }) => {
  respondFirstPool(options);
  const post = vi.fn();
  server.use(http.post("*/api/v1/pools/editor/commands", post));
  renderApp("/pools");
  await screen.findByRole("button", { name: "创建首只池子" });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "创建首只池子" })).toHaveAttribute(
      "aria-description",
      expect.stringContaining(reason),
    ),
  );
  expect(screen.getByRole("button", { name: "创建首只池子" })).toBeDisabled();
  expect(post).not.toHaveBeenCalled();
});
