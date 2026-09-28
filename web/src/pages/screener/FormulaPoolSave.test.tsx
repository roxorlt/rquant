import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { FormulaPoolSave, readFormulaPoolSaveTaskId } from "./FormulaPoolSave";

const taskA = "a".repeat(32);
const taskB = "b".repeat(32);
const pendingA: Schemas["FormulaPoolSaveCommandRequest"] = {
  base_name: "趋势池",
  display_name: "趋势池",
  task_id: taskA,
  expected_version: null,
  command_id: "c".repeat(32),
  requested_at: "2026-09-24T07:34:00Z",
};

function renderSave(viewer: string, candidate: Parameters<typeof FormulaPoolSave>[0]["candidate"]) {
  const queryClient = testQueryClient();
  queryClient.setQueryData(["meta"], metaEnvelope({ viewer }));
  return render(
    <AppProviders queryClient={queryClient}>
      <FormulaPoolSave candidate={candidate} />
    </AppProviders>,
  );
}

function seedPendingA(): void {
  window.localStorage.setItem(
    "rquant-formula-pool-save-v1",
    JSON.stringify({
      viewer: "viewer-a",
      request: pendingA,
      status: "pending",
      poolName: null,
      version: null,
    }),
  );
}

describe("公式池保存恢复", () => {
  for (const outcome of ["conflict", "failed", "http-409"] as const) {
    it(`结果暂不可读时 ${outcome} 终态保留原因和下一步`, async () => {
      seedPendingA();
      server.use(
        http.post("*/api/v1/pools/formula/commands", () => {
          if (outcome === "http-409") {
            return HttpResponse.json({ detail: "名称冲突" }, { status: 409 });
          }
          return HttpResponse.json({
            command_id: pendingA.command_id,
            status: outcome,
            message: outcome === "failed" ? "保存失败，请稍后重试。" : "名称冲突",
          });
        }),
      );
      const user = userEvent.setup();
      renderSave("viewer-a", null);
      await user.click(await screen.findByRole("button", { name: "继续核对" }));
      expect(
        await screen.findByText(
          outcome === "failed" ? "保存失败，请稍后重试。" : "这个名称已被使用，请换一个名称。",
        ),
      ).toBeVisible();
      expect(screen.getByText("刷新最近运行，结果可读后可重新保存。")).toBeVisible();
      expect(screen.getByRole("region", { name: "保存公式池" })).toBeVisible();
      expect(screen.queryByRole("button", { name: "继续核对" })).toBeNull();
    });
  }

  it("另一用户新保存时仍保留原用户待确认命令", async () => {
    seedPendingA();
    const submitted: Schemas["FormulaPoolSaveCommandRequest"][] = [];
    server.use(
      http.post("*/api/v1/pools/formula/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["FormulaPoolSaveCommandRequest"];
        submitted.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          status: "pending",
          message: "待确认",
        });
      }),
    );
    const user = userEvent.setup();
    const other = renderSave("viewer-b", {
      taskId: taskB,
      formula: "OPEN>0",
      tradeDate: "2026-09-24",
      matchCount: 12,
      unknownCount: 0,
    });
    expect(screen.queryByText("保存状态待确认")).toBeNull();
    await user.type(await screen.findByRole("textbox", { name: "池子名称" }), "观察池");
    await user.click(screen.getByRole("button", { name: "保存为池子" }));
    await waitFor(() => expect(submitted).toHaveLength(1));
    expect(submitted[0]).toMatchObject({ base_name: "观察池", task_id: taskB });
    expect(readFormulaPoolSaveTaskId("viewer-a")).toBe(taskA);
    expect(readFormulaPoolSaveTaskId("viewer-b")).toBe(taskB);
    other.unmount();

    renderSave("viewer-a", null);
    await user.click(await screen.findByRole("button", { name: "继续核对" }));
    await waitFor(() => expect(submitted).toHaveLength(2));
    expect(submitted[1]).toEqual(pendingA);
    expect(readFormulaPoolSaveTaskId("viewer-b")).toBe(taskB);
  });
});
