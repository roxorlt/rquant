import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import contract from "./factorDiagnosticsRejection.fixture.json";
import { RUN_OPERATION_KEY, validRunRequest, validRunResult } from "./factorRunState";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

// Captured through the real factory, private Unix transport and authenticated Web route.
// The initial rejection and lookup replies below are replayed without modifying their bodies.
const original = contract.request as Schemas["FactorRunRequest"];
const availability = contract.availability as Schemas["FactorRunAvailability"];
const nextId = "66666666-6666-4666-8666-666666666666";

function publish(): void {
  const metadata = metaEnvelope({
    generationId: original.serving_generation_id,
    viewer: contract.actor,
  });
  const factor: Schemas["FactorDefinitionItem"] = {
    factor_id: original.parameters.factor_id,
    name_zh: "源校验因子",
    category: "technical",
    category_label: "技术",
    direction: "higher_is_better",
    direction_label: "偏好高值",
    version: original.parameters.expected_head.version,
    content_sha256: original.parameters.expected_head.content_sha256,
    earliest_available_date: null,
    archived: false,
    expression: "ref(close, 2)",
    dependency_columns: ["close"],
    max_history_window: 2,
  };
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(metadata)),
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: metadata.serving.built_at,
          can_save: false,
          can_archive: true,
          definitions: [factor],
        },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({ data: availability, serving: metadata.serving }),
    ),
  );
}

beforeEach(() => {
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (_name: string, _options: unknown, action: () => unknown) => action(),
    },
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

it.each([false, true])(
  "FR-FINAL-01 真实未入队拒绝回执可恢复、改参和创建下一命令（失联=%s）",
  async (lost) => {
    expect(validRunRequest(original)).toBe(true);
    const { submit, resume, retry } = contract.responses;
    const rejection = submit.body as Schemas["Envelope_FactorRunOperationResult_"];
    expect(submit.status).toBe(200);
    expect(validRunResult(rejection.data, original)).toBe(true);
    expect(rejection.data.status).toBe("rejected");
    expect(rejection.data.job_id).toBeNull();
    expect(rejection.data.spec_sha256).toBeNull();
    expect(resume.status).toBe(404);
    const retryRejection = retry.body as Schemas["Envelope_FactorRunOperationResult_"];
    expect(retry.status).toBe(200);
    expect(validRunResult(retryRejection.data, original)).toBe(true);
    expect(retryRejection.data.status).toBe("rejected");
    expect(retryRejection.data.job_id).toBeNull();
    expect(retryRejection.data.spec_sha256).toBeNull();
    publish();
    vi.spyOn(crypto, "randomUUID")
      .mockReturnValueOnce(original.command_id as ReturnType<typeof crypto.randomUUID>)
      .mockReturnValue(nextId);
    vi.spyOn(Date.prototype, "toISOString").mockReturnValue(original.requested_at);
    const sent: Schemas["FactorRunRequest"][] = [];
    let resumed = 0;
    let retried = 0;
    server.use(
      http.post("*/api/v1/factors/runs", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorRunRequest"];
        sent.push(body);
        expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(body);
        if (sent.length > 1) return HttpResponse.error();
        expect(body).toEqual(original);
        return lost
          ? HttpResponse.error()
          : HttpResponse.json(submit.body, { status: submit.status });
      }),
      http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
        expect(await request.json()).toEqual(original);
        resumed += 1;
        return HttpResponse.json(resume.body, { status: resume.status });
      }),
      http.post("*/api/v1/factors/runs/retry", async ({ request }) => {
        expect(await request.json()).toEqual(original);
        retried += 1;
        return HttpResponse.json(retry.body, { status: retry.status });
      }),
    );
    const user = userEvent.setup();
    let view = renderApp("/factors");
    const panel = await screen.findByRole("region", { name: "检验参数" });
    await waitFor(() =>
      expect(within(panel).getByRole("button", { name: "运行检验" })).toBeEnabled(),
    );
    await user.selectOptions(
      within(panel).getByRole("combobox", { name: "股票池" }),
      original.parameters.selection,
    );
    await user.selectOptions(
      within(panel).getByRole("combobox", { name: "调仓周期" }),
      String(original.parameters.holding_sessions),
    );
    await user.selectOptions(
      within(panel).getByRole("combobox", { name: "分组数" }),
      String(original.parameters.group_count),
    );
    await user.click(
      within(panel).getByRole("button", {
        name: original.parameters.ic_method === "rank" ? "RankIC" : "NormalIC",
      }),
    );
    fireEvent.change(within(panel).getByLabelText("开始日期"), {
      target: { value: original.parameters.start_date },
    });
    fireEvent.change(within(panel).getByLabelText("结束日期"), {
      target: { value: original.parameters.end_date },
    });
    await user.click(within(panel).getByRole("button", { name: "运行检验" }));
    await user.click(
      within(await screen.findByRole("dialog", { name: "运行因子检验" })).getByRole("button", {
        name: "确认运行",
      }),
    );
    if (lost) {
      await screen.findByText("检验结果暂未确认，请保留本次操作。");
      view.unmount();
      view = renderApp("/factors");
      await waitFor(() => expect(resumed).toBe(1));
      await waitFor(() =>
        expect(screen.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled(),
      );
      expect(screen.queryByRole("button", { name: "修改检验参数" })).toBeNull();
      expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(
        original,
      );
      await user.click(screen.getByRole("button", { name: "用原请求重试检验" }));
      await waitFor(() => expect(retried).toBe(1));
    }
    await screen.findByRole("button", { name: "修改检验参数" });
    expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent(
      rejection.data.reason ?? "",
    );
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
    const beforeReload = localStorage.getItem(RUN_OPERATION_KEY);
    expect(JSON.parse(beforeReload ?? "null").request).toEqual(original);
    expect(JSON.parse(beforeReload ?? "null").result.status).toBe("rejected");
    view.unmount();
    view = renderApp("/factors");
    await user.click(await screen.findByRole("button", { name: "修改检验参数" }));
    expect(resumed).toBe(lost ? 1 : 0);
    expect(beforeReload).not.toBeNull();
    expect(localStorage.getItem(RUN_OPERATION_KEY)).toBeNull();
    expect(await screen.findByRole("button", { name: "归档" })).toBeEnabled();
    expect(screen.getByLabelText("开始日期")).toHaveValue(original.parameters.start_date);
    expect(screen.getByRole("combobox", { name: "股票池" })).toHaveValue(
      original.parameters.selection,
    );
    const updated = screen.getByRole("region", { name: "检验参数" });
    fireEvent.change(within(updated).getByLabelText("开始日期"), {
      target: { value: original.parameters.end_date },
    });
    await user.click(within(updated).getByRole("button", { name: "运行检验" }));
    await user.click(
      within(await screen.findByRole("dialog", { name: "运行因子检验" })).getByRole("button", {
        name: "确认运行",
      }),
    );
    await screen.findByText("检验结果暂未确认，请保留本次操作。");
    expect(sent).toHaveLength(2);
    expect(sent[1]).toEqual({
      ...original,
      command_id: nextId,
      parameters: { ...original.parameters, start_date: original.parameters.end_date },
    });
    expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(sent[1]);
    expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
    expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
  },
);
