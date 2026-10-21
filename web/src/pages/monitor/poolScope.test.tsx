import { readFileSync } from "node:fs";
import { join } from "node:path";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import {
  ALERT_POOL_TIP,
  ALERT_POOL_TIP_PARAGRAPHS,
  ALERT_POOL_TIP_WIDTH,
  alertPoolLabel,
  alertPoolLabels,
} from "@/copy/publicText";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-20T07:00:00Z" };

function alert(id: string, name: string, pool: string | null, trigger: string) {
  return {
    alert_id: id.repeat(64).slice(0, 64),
    trade_date: "2026-10-20",
    at: "2026-10-20T06:00:00Z",
    code: `99900${id}.SH`,
    name,
    level: "L1",
    trigger_type: trigger,
    trigger_price: 10,
    level_price: 10,
    pool,
    acked_at: null,
    acked_by: null,
  };
}

function rule(ruleId: string, title: string, pools: string[]) {
  return {
    rule_id: ruleId,
    title,
    enabled: true,
    pools,
    levels: ["L1"],
    cooldown_minutes: 30,
  };
}

const RULES = [
  rule("r1", "初筛与精选", ["pool2", "pool1"]),
  rule("r2", "只看自选", ["manual"]),
  rule("r3", "未限定", []),
];

const ALERTS = [
  alert("1", "澜屿科技", "pool1", "gap_up"),
  alert("2", "样例电子", "pool2", "stop_loss"),
  alert("3", "北交样本", "manual", "volume_surge"),
  alert("4", "空池样本", null, "breakout"),
  alert("5", "第五样本", "pool1", "take_profit"),
  alert("6", "第六样本", "pool2", "gap_up"),
];

type FetchImpl = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

function json(data: unknown): Response {
  return new Response(JSON.stringify({ data, serving }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

let posts: string[] = [];

function stub(): void {
  posts = [];
  const fetchMock: FetchImpl = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const path = url.pathname;
    const method = (
      init?.method ?? (input instanceof Request ? input.method : "GET")
    ).toUpperCase();
    if (method === "POST" && path.endsWith("/alert-rules")) {
      const raw =
        typeof init?.body === "string"
          ? init.body
          : input instanceof Request
            ? await input.clone().text()
            : "";
      posts.push(raw);
      return new Response(JSON.stringify({ command_id: "c1", status: "succeeded" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    const key = path.split("/api/v1/")[1]?.split("/")[0] ?? "";
    if (key === "alerts") return json({ items: ALERTS });
    if (key === "alert-rules") return json({ rules: RULES });
    if (key === "meta")
      return json({ app: "rQuant", version: "test", role: "admin", generation: serving });
    if (key === "operations") return json({ items: [] });
    if (key === "surge") return json({ date: null, items: [] });
    if (key === "health") return json({ services: [], freshness: [], layers: [] });
    return new Response("missing", { status: 404 });
  };
  vi.stubGlobal("fetch", fetchMock);
}

beforeEach(() => {
  stub();
});

async function expectOpenTip(): Promise<void> {
  const tips = await screen.findAllByRole("tooltip");
  expect(tips.length).toBeGreaterThan(0);
  for (const tip of tips) {
    expect(tip.textContent).toBe(ALERT_POOL_TIP);
    const paragraphs = [...tip.querySelectorAll("p")].map((paragraph) => paragraph.textContent);
    expect(paragraphs).toEqual([...ALERT_POOL_TIP_PARAGRAPHS]);
    const bubble = tip.closest(".ant-tooltip");
    expect(bubble).toHaveStyle({ maxWidth: `${ALERT_POOL_TIP_WIDTH}px` });
    const box = tip.querySelector(".ant-tooltip-container") ?? tip;
    expect(box).toHaveStyle({
      width: `${ALERT_POOL_TIP_WIDTH}px`,
      maxWidth: `${ALERT_POOL_TIP_WIDTH}px`,
    });
  }
}

function media(matches: (query: string) => boolean): typeof window.matchMedia {
  return ((query: string) => ({
    matches: matches(query),
    media: query,
    onchange: null,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    addListener: () => undefined,
    removeListener: () => undefined,
    dispatchEvent: () => false,
  })) as typeof window.matchMedia;
}

it("shows the three scope names on the rules table, the drawer and the timeline", async () => {
  const user = userEvent.setup();
  renderApp("/monitor");

  const rules = await screen.findByRole("table", { name: "告警规则" });
  const both = within(rules).getByRole("row", { name: /初筛与精选/ });
  expect(both).toHaveTextContent("N字初筛、N字精选");
  expect(both).not.toHaveTextContent("pool");
  expect(within(rules).getByRole("row", { name: /只看自选/ })).toHaveTextContent("自选");
  expect(within(rules).getByRole("row", { name: /未限定/ })).toHaveTextContent("全部");
  const rulesHeader = within(rules).getByRole("columnheader", { name: "范围" });
  expect(within(rulesHeader).getByText("范围")).toHaveClass("has-tip");

  const timeline = screen.getByRole("table", { name: "告警时间线" });
  expect(within(timeline).getByRole("row", { name: /澜屿科技/ })).toHaveTextContent("N字初筛");
  expect(within(timeline).getByRole("row", { name: /样例电子/ })).toHaveTextContent("N字精选");
  expect(within(timeline).getByRole("row", { name: /北交样本/ })).toHaveTextContent("自选");
  expect(within(timeline).getByRole("row", { name: /空池样本/ })).toHaveTextContent("—");
  const timelineHeader = within(timeline).getByRole("columnheader", { name: "范围" });
  expect(within(timelineHeader).getByText("范围")).toHaveClass("has-tip");
  expect(timeline.textContent ?? "").not.toMatch(/pool1|pool2|一号池|二号池|已加入/);

  await user.hover(within(rulesHeader).getByText("范围"));
  await expectOpenTip();

  await user.hover(within(timelineHeader).getByText("范围"));
  await expectOpenTip();

  await user.click(screen.getByRole("button", { name: "新建规则" }));
  const drawer = await screen.findByRole("dialog", { name: "新建规则" });
  const scope = within(drawer).getByRole("group", { name: "范围" });
  expect(within(scope).getByText("范围")).toHaveClass("has-tip");
  const boxes = within(scope).getAllByRole("checkbox");
  expect(boxes.map((box) => box.closest("label")?.textContent)).toEqual([
    "N字初筛",
    "N字精选",
    "自选",
  ]);
  for (const box of boxes) {
    expect(box.closest("label")?.textContent).not.toMatch(/[()（）]/);
  }
  expect(within(drawer).getByLabelText("冷却分钟")).toHaveValue(30);
  await user.hover(within(scope).getByText("范围"));
  await expectOpenTip();

  await user.click(within(scope).getByRole("checkbox", { name: "自选" }));
  await user.click(within(scope).getByRole("checkbox", { name: "N字初筛" }));
  await user.type(within(drawer).getByLabelText("规则名称"), "顺序");
  await user.click(within(drawer).getByRole("button", { name: "保存规则" }));
  await waitFor(() => expect(posts).toHaveLength(1));
  expect(JSON.parse(posts[0] ?? "{}").pools).toEqual(["pool1", "manual"]);
});

it("opens the scope explanation on tap when the pointer cannot hover", async () => {
  const original = window.matchMedia;
  window.matchMedia = media((query) => query.includes("hover: none"));
  try {
    const user = userEvent.setup();
    renderApp("/monitor");
    const rules = await screen.findByRole("table", { name: "告警规则" });
    const header = within(rules).getByRole("columnheader", { name: "范围" });
    await user.click(within(header).getByText("范围"));
    await expectOpenTip();
  } finally {
    window.matchMedia = original;
  }
});

it("keeps 类型 before 范围 in the desktop timeline", async () => {
  renderApp("/monitor");
  const timeline = await screen.findByRole("table", { name: "告警时间线" });
  const type = within(timeline).getByRole("columnheader", { name: "类型" });
  const scope = within(timeline).getByRole("columnheader", { name: "范围" });
  expect(type.compareDocumentPosition(scope) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  const row = within(timeline).getByRole("row", { name: /澜屿科技/ });
  const cells = [...row.querySelectorAll("td")];
  expect(cells[4]).toHaveTextContent("跳空高开");
  expect(cells[6]).toHaveTextContent("N字初筛");
  expect(alertPoolLabels(["manual", "pool2", "pool1"])).toBe("N字初筛、N字精选、自选");
  expect(alertPoolLabel("pool1")).toBe("N字初筛");
});

it("uses a two-line list for the phone timeline and keeps the title short", async () => {
  const original = window.matchMedia;
  window.matchMedia = media((query) => query.includes("max-width: 599px"));
  try {
    renderApp("/monitor");
    const list = await screen.findByRole("list", { name: "告警时间线" });
    expect(screen.queryByRole("table", { name: "告警时间线" })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "告警时间线" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /共 / })).not.toBeInTheDocument();
    const row = within(list)
      .getAllByRole("listitem")
      .find((item) => item.textContent?.includes("澜屿科技"));
    expect(row).toBeDefined();
    expect(row).toHaveTextContent(/14:00 · 10\.00 · L1 · 跳空高开/);
    expect(row).toHaveTextContent("N字初筛");
    expect(row?.querySelector(".alert-meta-keep")?.textContent).toMatch(
      /^(?:\d{2}-\d{2} )?14:00 · 10\.00 · L1 · 跳空高开 ·$/,
    );
    expect(row?.querySelector(".alert-meta-pool")).toHaveTextContent("N字初筛");
    expect(within(row as HTMLElement).queryByText("未确认")).not.toBeInTheDocument();
    expect(within(row as HTMLElement).getByRole("button", { name: "确认" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "排序" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^未确认 / })).toBeInTheDocument();
    expect(within(list).getAllByRole("listitem").length).toBeGreaterThanOrEqual(5);
    const surge = screen.getByRole("heading", { name: /量能异动/ });
    expect(timelineHeadingBefore(surge)).toBeTruthy();
    expect(screen.queryByRole("table", { name: "量能异动" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "量能异动规则" })).not.toBeInTheDocument();
    expect(screen.queryByText(/\d+ 行/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^列/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "导出 CSV" })).toBeInTheDocument();
  } finally {
    window.matchMedia = original;
  }
});

it("keeps the alert timeline ahead of 量能异动 on desktop", async () => {
  renderApp("/monitor");
  const timeline = await screen.findByRole("heading", { name: /告警时间线/ });
  const surge = screen.getByRole("heading", { name: /量能异动/ });
  expect(timelineHeadingBefore(surge, timeline)).toBeTruthy();
  expect(screen.getByRole("table", { name: "量能异动" })).toBeInTheDocument();
});

function timelineHeadingBefore(surge: HTMLElement, timeline?: HTMLElement): boolean {
  const head = timeline ?? screen.getByRole("heading", { name: "告警时间线" });
  return Boolean(head.compareDocumentPosition(surge) & Node.DOCUMENT_POSITION_FOLLOWING);
}

it("keeps scope options in ink at 44px without extra row gaps", () => {
  const css = readFileSync(join(process.cwd(), "src/styles/components.css"), "utf8");
  expect(css).toMatch(/\.rule-check\s*\{[^}]*color:\s*var\(--c-ink\)/);
  expect(css).toMatch(/\.rule-check\s*\{[^}]*height:\s*44px/);
  expect(css).toMatch(/fieldset:has\(\.rule-check\)\s*\{[^}]*gap:\s*0/);
  expect(css).toMatch(/\.alert-meta-keep\s*\{[^}]*flex:\s*none/);
  expect(css).toMatch(/\.li2 \.meta\.alert-meta\s*\{[^}]*gap:\s*0\.25em/);
  expect(css).toMatch(/\.alert-meta-pool\s*\{[^}]*text-overflow:\s*ellipsis/);
  expect(css).toMatch(/\.li2 \.v\s*\{[^}]*color:\s*var\(--c-ink\)/);
  expect(css).toMatch(/\.li2 \.v\s*\{[^}]*font-variant-numeric:\s*tabular-nums/);
});
