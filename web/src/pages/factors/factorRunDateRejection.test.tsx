import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import startDateContract from "./factorDiagnosticsStartDateRejection.fixture.json";
import { RUN_OPERATION_KEY, validRunRequest, validRunResult } from "./factorRunState";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

beforeEach(() => {
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: { request: async (_name: string, _options: unknown, action: () => unknown) => action() },
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

it("FR-FINAL-01 开始日期越界的真实拒绝可重载改参并持久化下一命令", async () => {
  const contract = startDateContract;
  const original = contract.request as Schemas["FactorRunRequest"];
  const availability = contract.availability as Schemas["FactorRunAvailability"];
  const { submit, resume, retry } = contract.responses;
  const rejection = submit.body as Schemas["Envelope_FactorRunOperationResult_"];
  const retryRejection = retry.body as Schemas["Envelope_FactorRunOperationResult_"];
  const start = availability.start_date;
  const end = availability.end_date;
  if (start == null || end == null) throw new Error("实际捕获缺少来源日期区间");
  expect(validRunRequest(original)).toBe(true);
  expect(original.parameters.start_date < start).toBe(true);
  expect(submit.status).toBe(200);
  expect(retry.status).toBe(200);
  expect(resume.status).toBe(404);
  for (const response of [rejection, retryRejection]) {
    expect(validRunResult(response.data, original)).toBe(true);
    expect(response.data.status).toBe("rejected");
    expect(response.data.job_id).toBeNull();
    expect(response.data.spec_sha256).toBeNull();
  }
  expect(contract.resources.jobs).toBe(0);
  expect(contract.resources.execution_copies).toBe(0);
  expect(contract.resources.outbox_receipt).toBeNull();
  const metadata = metaEnvelope({
    generationId: original.serving_generation_id,
    viewer: contract.actor,
  });
  const factor: Schemas["FactorDefinitionItem"] = {
    factor_id: original.parameters.factor_id,
    name_zh: "日期校验因子",
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
  const nextId = "77777777-7777-4777-8777-777777777777";
  vi.spyOn(crypto, "randomUUID")
    .mockReturnValueOnce(original.command_id as ReturnType<typeof crypto.randomUUID>)
    .mockReturnValue(nextId);
  vi.spyOn(Date.prototype, "toISOString").mockReturnValue(original.requested_at);
  const sent: Schemas["FactorRunRequest"][] = [];
  let resumed = 0;
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
    http.post("*/api/v1/factors/runs", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorRunRequest"];
      sent.push(body);
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(body);
      if (sent.length > 1) return HttpResponse.error();
      expect(body).toEqual(original);
      // Replay the captured authenticated HTTP→Unix→factory rejection body unchanged.
      return HttpResponse.json(submit.body, { status: submit.status });
    }),
    http.post("*/api/v1/factors/runs/resume", () => {
      resumed += 1;
      return HttpResponse.json(resume.body, { status: resume.status });
    }),
  );
  const user = userEvent.setup();
  let view = renderApp("/factors");
  const params = await screen.findByRole("region", { name: "检验参数" });
  await waitFor(() =>
    expect(within(params).getByRole("button", { name: "运行检验" })).toBeEnabled(),
  );
  await user.selectOptions(
    within(params).getByRole("combobox", { name: "股票池" }),
    original.parameters.selection,
  );
  await user.selectOptions(
    within(params).getByRole("combobox", { name: "调仓周期" }),
    String(original.parameters.holding_sessions),
  );
  await user.selectOptions(
    within(params).getByRole("combobox", { name: "分组数" }),
    String(original.parameters.group_count),
  );
  await user.click(
    within(params).getByRole("button", {
      name: original.parameters.ic_method === "rank" ? "RankIC" : "NormalIC",
    }),
  );
  fireEvent.change(within(params).getByLabelText("开始日期"), {
    target: { value: original.parameters.start_date },
  });
  fireEvent.change(within(params).getByLabelText("结束日期"), {
    target: { value: original.parameters.end_date },
  });
  await user.click(within(params).getByRole("button", { name: "运行检验" }));
  const confirmation = await screen.findByRole("dialog", { name: "运行因子检验" });
  expect(confirmation).toHaveTextContent(
    `${original.parameters.start_date} 至 ${original.parameters.end_date}`,
  );
  expect(sent).toHaveLength(0);
  await user.click(within(confirmation).getByRole("button", { name: "确认运行" }));
  const modify = await screen.findByRole("button", { name: "修改检验参数" });
  await waitFor(() => expect(modify).toBeEnabled());
  expect(screen.getByRole("region", { name: "本次检验" })).toHaveTextContent(
    rejection.data.reason ?? "",
  );
  expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
  const retained = JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null");
  expect(retained.request).toEqual(original);
  expect(retained.result).toEqual(rejection.data);
  view.unmount();
  view = renderApp("/factors");
  const restored = await screen.findByRole("button", { name: "修改检验参数" });
  await waitFor(() => expect(restored).toBeEnabled());
  expect(screen.getByLabelText("开始日期")).toHaveValue(original.parameters.start_date);
  expect(screen.getByLabelText("结束日期")).toHaveValue(original.parameters.end_date);
  expect(resumed).toBe(0);
  expect(sent).toEqual([original]);
  await user.click(restored);
  expect(localStorage.getItem(RUN_OPERATION_KEY)).toBeNull();
  expect(await screen.findByRole("button", { name: "归档" })).toBeEnabled();
  const corrected = screen.getByRole("region", { name: "检验参数" });
  fireEvent.change(within(corrected).getByLabelText("开始日期"), { target: { value: start } });
  fireEvent.change(within(corrected).getByLabelText("结束日期"), { target: { value: end } });
  await user.click(within(corrected).getByRole("button", { name: "运行检验" }));
  await user.click(
    within(await screen.findByRole("dialog", { name: "运行因子检验" })).getByRole("button", {
      name: "确认运行",
    }),
  );
  await screen.findByText("检验结果暂未确认，请保留本次操作。");
  expect(sent).toEqual([
    original,
    {
      ...original,
      command_id: nextId,
      parameters: { ...original.parameters, start_date: start, end_date: end },
    },
  ]);
  expect(JSON.parse(localStorage.getItem(RUN_OPERATION_KEY) ?? "null").request).toEqual(sent[1]);
  expect(screen.queryByRole("button", { name: "归档" })).toBeNull();
  expect(findJargon(view.container.querySelector("main")?.textContent ?? "")).toEqual([]);
});
