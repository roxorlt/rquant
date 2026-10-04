import { act, fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

const catalog: Schemas["QueryCatalogData"] = {
  available: true,
  save_enabled: true,
  source_at: "2026-09-30T08:00:00Z",
  message: "",
  tables: [
    {
      name: "daily_bar",
      label: "日线行情",
      columns: [
        { name: "ts_code", data_type: "VARCHAR", description: "股票代码，如 600001.SH" },
        { name: "close", data_type: "DOUBLE", description: "收盘价" },
      ],
      row_count: 2,
      earliest_date: "2026-09-29",
      latest_date: "2026-09-30",
    },
  ],
};
const result: Schemas["QueryResult"] = {
  status: "ready",
  columns: [
    { name: "值", data_type: "VARCHAR" },
    { name: "值", data_type: "VARCHAR" },
  ],
  rows: [["<script>alert(1)</script>", "=1+1"]],
  elapsed_ms: 25,
  source_at: catalog.source_at,
  snapshot_sha256: "a".repeat(64),
  message: "",
};
const envelope = <T,>(data: T) => ({ data, serving: metaEnvelope().serving });

beforeEach(() => {
  server.use(
    http.get("*/api/v1/research/catalog", () => HttpResponse.json(envelope(catalog))),
    http.get("*/api/v1/research/queries", () =>
      HttpResponse.json(envelope({ available: true, items: [], message: "" })),
    ),
    http.post("*/api/v1/research/query", () => HttpResponse.json(envelope(result))),
  );
});

test("查询页可以用键盘运行，结果文本安全且重复列可见", async () => {
  renderApp("/query");
  const editor = await screen.findByRole("textbox", { name: "SQL 查询" });
  await waitFor(() => expect(screen.getByRole("button", { name: "运行" })).toBeEnabled());
  fireEvent.change(editor, { target: { value: "SELECT 1" } });
  fireEvent.keyDown(editor, { key: "Enter", ctrlKey: true });
  expect(await screen.findByText("<script>alert(1)</script>")).toBeVisible();
  expect(screen.getAllByRole("columnheader", { name: "值" })).toHaveLength(2);
  expect(document.querySelector("script")).toBeNull();
  expect(screen.getByRole("button", { name: "导出 CSV" })).toBeEnabled();
  expect(findJargon(document.body.innerText ?? document.body.textContent ?? "")).toEqual([]);
});

test("先前请求不会覆盖新 SQL 的结果", async () => {
  let firstResolve: (() => void) | undefined;
  server.use(
    http.post("*/api/v1/research/query", async ({ request }) => {
      const body = (await request.json()) as { sql: string };
      if (body.sql === "SELECT 'old'")
        await new Promise<void>((resolve) => {
          firstResolve = resolve;
        });
      return HttpResponse.json(envelope({ ...result, rows: [[body.sql]] }));
    }),
  );
  const user = userEvent.setup();
  renderApp("/query");
  const editor = await screen.findByRole("textbox", { name: "SQL 查询" });
  await waitFor(() => expect(screen.getByRole("button", { name: "运行" })).toBeEnabled());
  fireEvent.change(editor, { target: { value: "SELECT 'old'" } });
  await user.click(screen.getByRole("button", { name: "运行" }));
  await waitFor(() => expect(firstResolve).toBeDefined());
  fireEvent.change(editor, { target: { value: "SELECT 'new'" } });
  await user.click(screen.getByRole("button", { name: "运行" }));
  expect(await screen.findByText("SELECT 'new'", { selector: "span" })).toBeVisible();
  await act(async () => firstResolve?.());
  expect(screen.queryByText("SELECT 'old'")).not.toBeInTheDocument();
});

test("保存失联后使用原命令恢复，确认写入后才显示已保存", async () => {
  let original: unknown;
  server.use(
    http.post("*/api/v1/research/queries/save", async ({ request }) => {
      original = await request.json();
      return HttpResponse.json({ detail: "不可用" }, { status: 503 });
    }),
    http.post("*/api/v1/research/queries/resume", async ({ request }) => {
      const body = await request.json();
      expect(body).toEqual(original);
      const command = body as Schemas["SaveResearchQuery"];
      return HttpResponse.json(
        envelope({
          receipt: {
            command_id: command.command_id,
            status: "succeeded",
            enqueued_at: command.requested_at,
            completed_at: command.requested_at,
            result: { query_id: command.query_id, version: 1, code: "saved" },
            error: null,
          },
          message: "",
        }),
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/query");
  await screen.findByRole("textbox", { name: "SQL 查询" });
  await user.type(screen.getByRole("textbox", { name: "查询名称" }), "我的行情");
  await user.click(screen.getByRole("button", { name: "保存" }));
  expect(await screen.findByText("保存结果尚未确认，请恢复原命令。")).toBeVisible();
  expect(screen.queryByText("已保存")).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "恢复保存" }));
  expect(await screen.findByText("已保存")).toBeVisible();
});

test("缺服务、无结果与执行失败有不同提示", async () => {
  server.use(
    http.get("*/api/v1/research/catalog", () =>
      HttpResponse.json(envelope({ ...catalog, available: false, message: "查询服务尚未启用。" })),
    ),
  );
  renderApp("/query");
  expect(await screen.findByText("查询服务尚未启用。")).toBeVisible();
  expect(screen.getByRole("button", { name: "运行" })).toBeDisabled();
});

test("切换账号后清除上个账号的 SQL 和名称", async () => {
  const { queryClient } = renderApp("/query");
  const editor = await screen.findByRole("textbox", { name: "SQL 查询" });
  await waitFor(() => expect(screen.getByRole("button", { name: "运行" })).toBeEnabled());
  fireEvent.change(editor, { target: { value: "SELECT 'private-editor'" } });
  fireEvent.change(screen.getByRole("textbox", { name: "查询名称" }), {
    target: { value: "私有查询" },
  });
  await act(async () => queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "bob" })));
  await waitFor(() =>
    expect(screen.getByRole("textbox", { name: "SQL 查询" })).not.toHaveValue(
      "SELECT 'private-editor'",
    ),
  );
  expect(screen.getByRole("textbox", { name: "查询名称" })).toHaveValue("");
});

test("原命令尚未找到且重试失败时，保留记录并提示继续恢复", async () => {
  const commands: unknown[] = [];
  server.use(
    http.post("*/api/v1/research/queries/save", async ({ request }) => {
      commands.push(await request.json());
      return HttpResponse.json({ detail: "不可用" }, { status: 503 });
    }),
    http.post("*/api/v1/research/queries/resume", () =>
      HttpResponse.json(envelope({ receipt: null, message: "" })),
    ),
  );
  const user = userEvent.setup();
  renderApp("/query");
  await screen.findByRole("textbox", { name: "SQL 查询" });
  await user.type(screen.getByRole("textbox", { name: "查询名称" }), "恢复查询");
  await user.click(screen.getByRole("button", { name: "保存" }));
  await screen.findByText("保存结果尚未确认，请恢复原命令。");
  await user.click(screen.getByRole("button", { name: "恢复保存" }));
  await screen.findByText("原命令尚未找到，可再次提交原命令。");
  await user.click(screen.getByRole("button", { name: "重试原保存" }));
  expect(await screen.findByText("保存结果尚未确认，请恢复原命令。")).toBeVisible();
  expect(commands).toHaveLength(2);
  expect(commands[1]).toEqual(commands[0]);
  expect(screen.getByRole("button", { name: "恢复保存" })).toBeEnabled();
  expect(screen.queryByText("已保存")).not.toBeInTheDocument();
  expect(sessionStorage.getItem("rquant.query-save.tester.v1")).not.toBeNull();
});

test("空白 SQL 不能保存", async () => {
  const user = userEvent.setup();
  renderApp("/query");
  const editor = await screen.findByRole("textbox", { name: "SQL 查询" });
  await user.type(screen.getByRole("textbox", { name: "查询名称" }), "空查询");
  fireEvent.change(editor, { target: { value: "  " } });
  expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
});

test("载入后由用户运行，空结果和计划使用实际返回", async () => {
  const requests: Array<{ sql: string; mode: string }> = [];
  server.use(
    http.get("*/api/v1/research/queries", () =>
      HttpResponse.json(
        envelope({
          available: true,
          message: "",
          items: [
            {
              query_id: "saved-query",
              name: "我的空查询",
              sql: "SELECT close FROM daily_bar WHERE false",
              version: 2,
              updated_at: "2026-09-30T08:00:00Z",
            },
          ],
        }),
      ),
    ),
    http.post("*/api/v1/research/query", async ({ request }) => {
      const body = (await request.json()) as { sql: string; mode: string };
      requests.push(body);
      return HttpResponse.json(
        envelope({ ...result, rows: body.mode === "explain" ? [["实际扫描计划"]] : [] }),
      );
    }),
  );
  const user = userEvent.setup();
  renderApp("/query");
  await user.click(await screen.findByRole("button", { name: "我的空查询" }));
  expect(screen.getByRole("textbox", { name: "SQL 查询" })).toHaveValue(
    "SELECT close FROM daily_bar WHERE false",
  );
  expect(requests).toEqual([]);
  await user.click(screen.getByRole("button", { name: "运行" }));
  expect(await screen.findByText("没有符合条件的数据，请调整 SQL。")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "查看计划" }));
  expect(await screen.findByText("实际扫描计划")).toBeVisible();
  expect(requests.map((item) => item.mode)).toEqual(["query", "explain"]);
});
