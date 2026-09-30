import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const generation = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const laterGeneration = "b".repeat(64);
const savedFactor: Schemas["FactorDefinitionItem"] = {
  factor_id: "price_volume_factor",
  content_sha256: "a".repeat(64),
  name_zh: "价量动量",
  category: "technical",
  category_label: "技术",
  direction: "higher_is_better",
  direction_label: "偏好高值",
  version: 2,
  earliest_available_date: null,
  archived: false,
  expression: "ts_mean(close, 5)",
  dependency_columns: ["close"],
  max_history_window: 5,
};
const fields: Schemas["DailyFactorField"][] = [
  ["open", "开盘价"],
  ["high", "最高价"],
  ["low", "最低价"],
  ["close", "收盘价"],
  ["vol", "成交量"],
  ["amount", "成交额"],
].map(([column, name_zh]) => ({
  column: column ?? "",
  name_zh: name_zh ?? "",
  description_zh: `${name_zh}日线值`,
}));
const capability: Schemas["FactorCapabilitiesData"] = {
  can_save: true,
  coverage_note_zh: "字段和算子支持不代表历史数据已覆盖。",
  fields,
  runnable_operators: ["+", "ts_mean", "ref"],
  unavailable_operators: [{ name: "industry_neutralize", reason_zh: "缺行业归属" }],
  source_mode: "historical_retrospective",
  version: "daily_v1",
};
function catalog(
  rows: Schemas["FactorDefinitionItem"][] = [savedFactor],
  canSave = true,
  id = generation,
) {
  return {
    data: {
      availability: rows.length ? "populated" : "empty",
      available_at: "2026-09-29T07:00:00Z",
      can_archive: true,
      can_save: canSave,
      definitions: rows,
    },
    serving: metaEnvelope({ generationId: id }).serving,
  };
}
function publish(
  rows: Schemas["FactorDefinitionItem"][] = [savedFactor],
  canSave = true,
  capabilitySave = true,
  id = generation,
  capabilityId = id,
) {
  server.use(
    http.get("*/api/v1/factors/definitions", () => HttpResponse.json(catalog(rows, canSave, id))),
    http.get("*/api/v1/factors/capabilities", () =>
      HttpResponse.json({
        data: { ...capability, can_save: capabilitySave },
        serving: metaEnvelope({ generationId: capabilityId }).serving,
      }),
    ),
  );
}
function receipt(
  command: Schemas["FactorSaveDraft"],
  status: Schemas["FactorSaveCommandData"]["status"],
  overrides: Partial<Schemas["FactorSaveCommandData"]> = {},
) {
  return {
    data: {
      command_id: command.command_id,
      status,
      message: status === "rejected" ? "公式未受理，请修改后重试。" : "正在保存，请稍后查看。",
      factor_id: status === "published" ? "new_factor" : null,
      version: status === "published" ? 1 : null,
      content_sha256: status === "published" ? "c".repeat(64) : null,
      current_head_updated: false,
      ...overrides,
    },
    serving: metaEnvelope().serving,
  };
}
async function openCreate() {
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "新建因子" }));
  return user;
}
async function fillCreate(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByRole("textbox", { name: "中文名" }), "收盘均值");
  await user.type(screen.getByRole("textbox", { name: "表达式" }), "ts_mean(close, 5)");
}

describe("因子新建与编辑", () => {
  it("可信空库能新建，提交前持久化完整原请求，并从字段帮助插入", async () => {
    publish([]);
    let request: Schemas["FactorSaveDraft"] | null = null;
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request: incoming }) => {
        request = (await incoming.json()) as Schemas["FactorSaveDraft"];
        expect(JSON.parse(localStorage.getItem("rquant.factor.save-command.v1") ?? "null")).toEqual(
          request,
        );
        return HttpResponse.json(receipt(request, "pending"));
      }),
    );
    const { container } = renderApp("/factors");
    const user = await openCreate();
    expect(screen.getByText("还没有因子")).toBeInTheDocument();
    expect(screen.getByText("保存公式后，实际数据范围在检验时核对")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /industry_neutralize/ })).toBeNull();
    await user.type(screen.getByRole("textbox", { name: "中文名" }), "收盘均值");
    await user.click(screen.getByRole("button", { name: "插入收盘价" }));
    expect(screen.getByRole("textbox", { name: "表达式" })).toHaveValue("close");
    await user.click(screen.getByRole("button", { name: "保存因子" }));
    await waitFor(() => expect(request).not.toBeNull());
    expect(request).toMatchObject({
      generation_id: generation,
      mode: "create",
      factor_id: null,
      expected_head: null,
      name_zh: "收盘均值",
      category: "技术",
      direction: "higher_is_better",
      expression: "close",
    });
    expect(request).toEqual(
      expect.objectContaining({ command_id: expect.any(String), requested_at: expect.any(String) }),
    );
    expect(request).not.toHaveProperty("category_label");
    expect(await screen.findByText("正在保存，请稍后查看。")).toBeInTheDocument();
    expect(findJargon(container.querySelector("main")?.textContent ?? "")).toEqual([]);
  });

  it("编辑只开放当前未归档版本，分类显示中文但未改时保留原值", async () => {
    publish([
      savedFactor,
      { ...savedFactor, factor_id: "archived", name_zh: "旧因子", archived: true },
    ]);
    let request: Schemas["FactorSaveDraft"] | null = null;
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request: incoming }) => {
        request = (await incoming.json()) as Schemas["FactorSaveDraft"];
        return HttpResponse.json(receipt(request, "pending"));
      }),
    );
    renderApp("/factors");
    const user = userEvent.setup();
    await screen.findByRole("region", { name: "因子详情" });
    await user.click(await screen.findByRole("button", { name: "编辑" }));
    expect(screen.getByRole("combobox", { name: "分类" })).toHaveValue("technical");
    expect(screen.getByRole("combobox", { name: "分类" })).toHaveTextContent("技术");
    await user.clear(screen.getByRole("textbox", { name: "表达式" }));
    await user.type(screen.getByRole("textbox", { name: "表达式" }), "ref(close, 2)");
    await user.click(screen.getByRole("button", { name: "保存新版本" }));
    await waitFor(() => expect(request).not.toBeNull());
    expect(request).toMatchObject({
      mode: "edit",
      factor_id: savedFactor.factor_id,
      expected_head: { version: 2, content_sha256: savedFactor.content_sha256 },
      category: "technical",
      expression: "ref(close, 2)",
    });
    await user.click(screen.getByRole("row", { name: /旧因子/ }));
    expect(screen.queryByRole("button", { name: "编辑" })).toBeNull();
  });

  it.each([
    [false, true, generation],
    [true, false, generation],
    [true, true, laterGeneration],
  ])(
    "任一保存权限或同代核验缺失时没有新建入口",
    async (catalogSave, capabilitySave, capabilityId) => {
      publish([], catalogSave, capabilitySave, generation, capabilityId);
      renderApp("/factors");
      await screen.findByText("还没有因子");
      expect(screen.queryByRole("button", { name: "新建因子" })).toBeNull();
    },
  );

  it("表单给出字段反馈，服务端明确拒绝后可修改原草稿", async () => {
    publish([]);
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) =>
        HttpResponse.json(
          receipt((await request.json()) as Schemas["FactorSaveDraft"], "rejected"),
        ),
      ),
    );
    renderApp("/factors");
    const user = await openCreate();
    await user.click(screen.getByRole("button", { name: "保存因子" }));
    expect(screen.getByText("请填写中文名")).toBeInTheDocument();
    expect(screen.getByText("请填写表达式")).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "中文名" })).toHaveFocus();
    await fillCreate(user);
    await user.click(screen.getByRole("button", { name: "保存因子" }));
    expect(await screen.findByText("公式未受理，请修改后重试。")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "修改草稿" }));
    expect(await screen.findByRole("textbox", { name: "表达式" })).toHaveValue("ts_mean(close, 5)");
  });

  it("浏览器存储失败时禁止保存且不发送写请求", async () => {
    publish([]);
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/definitions/save", () => {
        posts += 1;
        return new HttpResponse(null, { status: 500 });
      }),
    );
    const spy = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("storage unavailable");
    });
    try {
      renderApp("/factors");
      const user = await openCreate();
      await fillCreate(user);
      expect(screen.getByRole("button", { name: "保存因子" })).toBeDisabled();
      expect(screen.getByText("浏览器无法保存草稿，暂不能提交。")).toBeInTheDocument();
      expect(posts).toBe(0);
    } finally {
      spy.mockRestore();
    }
  });

  it("超时、首次续查未命中后仍用完整原请求恢复和重试", async () => {
    publish([]);
    const seen: { path: string; body: Schemas["FactorSaveDraft"] }[] = [];
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) => {
        seen.push({ path: "save", body: (await request.json()) as Schemas["FactorSaveDraft"] });
        return new HttpResponse(null, { status: 503 });
      }),
      http.post("*/api/v1/factors/definitions/save/resume", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorSaveDraft"];
        seen.push({ path: "resume", body });
        return HttpResponse.json(receipt(body, "uncertain"));
      }),
      http.post("*/api/v1/factors/definitions/save/retry", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorSaveDraft"];
        seen.push({ path: "retry", body });
        return HttpResponse.json(receipt(body, "pending"));
      }),
    );
    const view = renderApp("/factors");
    const user = await openCreate();
    await fillCreate(user);
    await user.click(screen.getByRole("button", { name: "保存因子" }));
    expect(await screen.findByText("保存结果尚未确认，请保留这次操作。")).toBeInTheDocument();
    view.unmount();
    renderApp("/factors");
    await waitFor(() => expect(seen.some((item) => item.path === "resume")).toBe(true));
    expect(await screen.findByText("保存结果尚未确认，请保留这次操作。")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "用原请求重试" }));
    await waitFor(() => expect(seen.some((item) => item.path === "retry")).toBe(true));
    expect(seen.map((item) => item.body)).toEqual([seen[0]?.body, seen[0]?.body, seen[0]?.body]);
    expect(localStorage.getItem("rquant.factor.save-command.v1")).not.toBeNull();
  });

  it("换代保留草稿和旧请求基准，明确比对后才可提交新命令", async () => {
    publish();
    const requests: Schemas["FactorSaveDraft"][] = [];
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorSaveDraft"];
        requests.push(body);
        return HttpResponse.json(receipt(body, "pending"));
      }),
    );
    const view = renderApp("/factors");
    const user = userEvent.setup();
    await screen.findByRole("region", { name: "因子详情" });
    await user.click(await screen.findByRole("button", { name: "编辑" }));
    await user.clear(screen.getByRole("textbox", { name: "表达式" }));
    await user.type(screen.getByRole("textbox", { name: "表达式" }), "ref(close, 2)");
    server.use(
      http.get("*/api/v1/factors/definitions", () =>
        HttpResponse.json(
          catalog(
            [{ ...savedFactor, version: 3, content_sha256: "d".repeat(64) }],
            true,
            laterGeneration,
          ),
        ),
      ),
      http.get("*/api/v1/factors/capabilities", () =>
        HttpResponse.json({
          data: capability,
          serving: metaEnvelope({ generationId: laterGeneration }).serving,
        }),
      ),
    );
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: laterGeneration })),
    );
    expect(await screen.findByText("因子已更新，请比对当前版本。")).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "表达式" })).toHaveValue("ref(close, 2)");
    expect(screen.getByRole("button", { name: "保存新版本" })).toBeDisabled();
    expect(JSON.parse(localStorage.getItem("rquant.factor.save-draft.v1") ?? "null")).toMatchObject(
      { generation_id: generation, expected_head: { version: 2 } },
    );
    await user.click(screen.getByRole("button", { name: "比对当前版本" }));
    const comparison = await screen.findByRole("region", { name: "当前版本比对" });
    expect(comparison).toHaveTextContent("第 3 版");
    expect(within(comparison).getByText(savedFactor.expression)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "保存新版本" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "使用当前版本为基准" }));
    expect(screen.getByRole("textbox", { name: "表达式" })).toHaveValue("ref(close, 2)");
    expect(screen.getByRole("button", { name: "保存新版本" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "保存新版本" }));
    await waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0]).toMatchObject({
      generation_id: laterGeneration,
      expected_head: { version: 3, content_sha256: "d".repeat(64) },
      expression: "ref(close, 2)",
    });
  });

  it.each(["definitions", "capabilities"])(
    "%s 刷新失败后不能用缓存权限提交草稿",
    async (resource) => {
      publish([]);
      const view = renderApp("/factors");
      const user = await openCreate();
      await fillCreate(user);
      server.use(
        http.get(`*/api/v1/factors/${resource}`, () => new HttpResponse(null, { status: 503 })),
      );
      await act(async () => {
        await view.queryClient.refetchQueries({ queryKey: ["factors", resource, generation] });
      });
      await waitFor(() => expect(screen.getByRole("button", { name: "保存因子" })).toBeDisabled());
      expect(screen.getByText("暂不能保存，请刷新后重试。")).toBeInTheDocument();
    },
  );

  it.each([401, 403, 409, 422])(
    "首次保存 HTTP %s 的明确拒绝保留草稿，可结束原操作后修改",
    async (status) => {
      publish([]);
      server.use(
        http.post("*/api/v1/factors/definitions/save", () => new HttpResponse(null, { status })),
      );
      renderApp("/factors");
      const user = await openCreate();
      await fillCreate(user);
      await user.click(screen.getByRole("button", { name: "保存因子" }));
      await user.click(await screen.findByRole("button", { name: "修改草稿" }));
      expect(await screen.findByRole("textbox", { name: "表达式" })).toHaveValue(
        "ts_mean(close, 5)",
      );
      expect(localStorage.getItem("rquant.factor.save-command.v1")).toBeNull();
    },
  );

  it("提交瞬间的持久化失败不发写请求，草稿仍可见", async () => {
    publish([]);
    let posts = 0;
    server.use(
      http.post("*/api/v1/factors/definitions/save", () => {
        posts += 1;
        return new HttpResponse(null, { status: 503 });
      }),
    );
    renderApp("/factors");
    const user = await openCreate();
    await fillCreate(user);
    const setItem = Storage.prototype.setItem;
    const spy = vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (
      this: Storage,
      key,
      value,
    ) {
      if (key === "rquant.factor.save-command.v1") throw new Error("full");
      setItem.call(this, key, value);
    });
    try {
      await user.click(screen.getByRole("button", { name: "保存因子" }));
      expect(screen.getByRole("textbox", { name: "表达式" })).toHaveValue("ts_mean(close, 5)");
      expect(screen.getByRole("button", { name: "保存因子" })).toBeDisabled();
      expect(posts).toBe(0);
    } finally {
      spy.mockRestore();
    }
  });

  it("原请求重试核验失败仍保留命令，不开放另一项保存或归档", async () => {
    publish();
    const seen: Schemas["FactorSaveDraft"][] = [];
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorSaveDraft"];
        seen.push(body);
        return HttpResponse.json(receipt(body, "uncertain"));
      }),
      http.post("*/api/v1/factors/definitions/save/retry", async ({ request }) => {
        seen.push((await request.json()) as Schemas["FactorSaveDraft"]);
        return new HttpResponse(null, { status: 409 });
      }),
    );
    renderApp("/factors");
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "编辑" }));
    await user.click(screen.getByRole("button", { name: "保存新版本" }));
    await user.click(await screen.findByRole("button", { name: "用原请求重试" }));
    expect(await screen.findByText("保存结果尚未确认，请保留这次操作。")).toBeInTheDocument();
    await waitFor(() => expect(seen).toHaveLength(2));
    expect(seen[1]).toEqual(seen[0]);
    expect(JSON.parse(localStorage.getItem("rquant.factor.save-command.v1") ?? "null")).toEqual(
      seen[0],
    );
    expect(screen.queryByRole("button", { name: "修改草稿" })).toBeNull();
    expect(screen.queryByRole("button", { name: /新建因子|^编辑$|^归档$/ })).toBeNull();
  });

  it("已有未确认的归档时保留续查入口并阻止保存", async () => {
    publish();
    localStorage.setItem(
      "rquant.factor.archive-command.v1",
      JSON.stringify({
        factorId: savedFactor.factor_id,
        command: {
          generation_id: generation,
          command_id: "archive-first",
          requested_at: "2026-09-29T07:00:00Z",
          expected_head: {
            version: savedFactor.version,
            content_sha256: savedFactor.content_sha256,
          },
        },
      }),
    );
    server.use(
      http.post("*/api/v1/factors/definitions/:factorId/archive/resume", () =>
        HttpResponse.json({
          data: {
            status: "pending",
            message: "正在归档，请稍后查看。",
            command_id: "archive-first",
          },
          serving: metaEnvelope().serving,
        }),
      ),
    );
    renderApp("/factors");
    await screen.findByText("正在归档，请稍后查看。");
    expect(screen.getByRole("button", { name: "刷新状态" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /新建因子|^编辑$/ })).toBeNull();
  });

  it("只有新数据代目录印证回执后显示已保存，未确认保存阻止归档", async () => {
    publish();
    let body: Schemas["FactorSaveDraft"] | null = null;
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) => {
        body = (await request.json()) as Schemas["FactorSaveDraft"];
        return HttpResponse.json(
          receipt(body, "succeeded_waiting_publication", {
            factor_id: savedFactor.factor_id,
            version: 3,
            content_sha256: "c".repeat(64),
          }),
        );
      }),
      http.post("*/api/v1/factors/definitions/save/resume", async ({ request }) =>
        HttpResponse.json(
          receipt((await request.json()) as Schemas["FactorSaveDraft"], "published", {
            factor_id: savedFactor.factor_id,
            version: 3,
            content_sha256: "c".repeat(64),
          }),
        ),
      ),
    );
    const view = renderApp("/factors");
    const user = userEvent.setup();
    await screen.findByRole("region", { name: "因子详情" });
    await user.click(await screen.findByRole("button", { name: "编辑" }));
    await user.click(screen.getByRole("button", { name: "保存新版本" }));
    expect(await screen.findByText("已提交，等待更新。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "刷新状态" }));
    expect(screen.queryByText("已保存。")).toBeNull();
    server.use(
      http.get("*/api/v1/meta", () =>
        HttpResponse.json(metaEnvelope({ generationId: laterGeneration })),
      ),
      http.get("*/api/v1/factors/definitions", () =>
        HttpResponse.json(
          catalog(
            [{ ...savedFactor, version: 3, content_sha256: "c".repeat(64) }],
            true,
            laterGeneration,
          ),
        ),
      ),
      http.get("*/api/v1/factors/capabilities", () =>
        HttpResponse.json({
          data: capability,
          serving: metaEnvelope({ generationId: laterGeneration }).serving,
        }),
      ),
    );
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: laterGeneration })),
    );
    await user.click(screen.getByRole("button", { name: "刷新状态" }));
    expect(await screen.findByText("已保存。")).toBeInTheDocument();
    expect(body).not.toBeNull();
  });

  it("保存已发布后当前存在更新的归档版本，也能结束原保存操作", async () => {
    publish();
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) =>
        HttpResponse.json(
          receipt((await request.json()) as Schemas["FactorSaveDraft"], "published", {
            factor_id: savedFactor.factor_id,
            version: 3,
            content_sha256: "c".repeat(64),
            current_head_updated: true,
          }),
        ),
      ),
    );
    const view = renderApp("/factors");
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "编辑" }));
    await user.click(screen.getByRole("button", { name: "保存新版本" }));
    await screen.findByText("已提交，等待更新。");
    server.use(
      http.get("*/api/v1/meta", () =>
        HttpResponse.json(metaEnvelope({ generationId: laterGeneration })),
      ),
      http.get("*/api/v1/factors/definitions", () =>
        HttpResponse.json(
          catalog(
            [{ ...savedFactor, version: 4, content_sha256: "d".repeat(64), archived: true }],
            true,
            laterGeneration,
          ),
        ),
      ),
      http.get("*/api/v1/factors/capabilities", () =>
        HttpResponse.json({
          data: capability,
          serving: metaEnvelope({ generationId: laterGeneration }).serving,
        }),
      ),
    );
    act(() =>
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: laterGeneration })),
    );
    await screen.findByText("已保存，当前已有新版本。");
    await user.click(screen.getByRole("button", { name: "继续查看因子" }));
    expect(localStorage.getItem("rquant.factor.save-command.v1")).toBeNull();
    expect(screen.queryByRole("button", { name: "编辑" })).toBeNull();
  });

  it("晚到的旧命令续查不能覆盖已经开始的新保存", async () => {
    publish();
    const requests: Schemas["FactorSaveDraft"][] = [];
    let releaseOld: () => void = () => undefined;
    const oldBlocked = new Promise<void>((resolve) => {
      releaseOld = resolve;
    });
    let oldReceived = false;
    let oldResponse: () => void = () => undefined;
    const oldResponded = new Promise<void>((resolve) => {
      oldResponse = resolve;
    });
    const observeResponse = ({ request }: { request: Request }) => {
      if (request.url.endsWith("/save/resume")) oldResponse();
    };
    server.events.on("response:mocked", observeResponse);
    server.use(
      http.post("*/api/v1/factors/definitions/save", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorSaveDraft"];
        requests.push(body);
        return HttpResponse.json(
          receipt(body, requests.length === 1 ? "published" : "pending", {
            factor_id: savedFactor.factor_id,
            version: 3,
            content_sha256: "c".repeat(64),
          }),
        );
      }),
      http.post("*/api/v1/factors/definitions/save/resume", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorSaveDraft"];
        oldReceived = true;
        await oldBlocked;
        return HttpResponse.json(receipt(body, "rejected"));
      }),
    );
    try {
      const view = renderApp("/factors");
      const user = userEvent.setup();
      await user.click(await screen.findByRole("button", { name: "编辑" }));
      await user.click(screen.getByRole("button", { name: "保存新版本" }));
      await screen.findByText("已提交，等待更新。");
      await user.click(screen.getByRole("button", { name: "刷新状态" }));
      await waitFor(() => expect(oldReceived).toBe(true));
      server.use(
        http.get("*/api/v1/meta", () =>
          HttpResponse.json(metaEnvelope({ generationId: laterGeneration })),
        ),
        http.get("*/api/v1/factors/definitions", () =>
          HttpResponse.json(
            catalog(
              [{ ...savedFactor, version: 3, content_sha256: "c".repeat(64) }],
              true,
              laterGeneration,
            ),
          ),
        ),
        http.get("*/api/v1/factors/capabilities", () =>
          HttpResponse.json({
            data: capability,
            serving: metaEnvelope({ generationId: laterGeneration }).serving,
          }),
        ),
      );
      act(() =>
        view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: laterGeneration })),
      );
      await screen.findByText("已保存。");
      await user.click(screen.getByRole("button", { name: "继续查看因子" }));
      await user.click(await screen.findByRole("button", { name: "编辑" }));
      await user.click(screen.getByRole("button", { name: "保存新版本" }));
      await screen.findByText("正在保存，请稍后查看。");
      await act(async () => {
        releaseOld();
        await oldResponded;
      });
      expect(screen.getByText("正在保存，请稍后查看。")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "修改草稿" })).toBeNull();
      expect(JSON.parse(localStorage.getItem("rquant.factor.save-command.v1") ?? "null")).toEqual(
        requests[1],
      );
      expect(requests[0]?.command_id).not.toBe(requests[1]?.command_id);
    } finally {
      releaseOld();
      server.events.removeListener("response:mocked", observeResponse);
    }
  });
});
