import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { server } from "@/test/server";
import { TemplateEditor } from "./TemplateEditor";
import {
  templateDetail,
  templateEnvelope,
  templateGeneration,
  templateSources,
} from "./template.fixture";
import type { SaveTemplate } from "./templateApi";
import { useTemplateCommands } from "./templateCommands";

const viewer = "targeted-synthetic-alice";
const key = `rquant.strategy-template.pending.${viewer}`;
const base = "*/api/v1/strategy-templates/commands";

function Harness() {
  const commands = useTemplateCommands(viewer);
  return (
    <>
      <TemplateEditor
        sources={templateSources}
        generation={templateGeneration}
        locked={commands.busy || commands.pending !== null}
        onSave={(body) => void commands.submit(body)}
      />
      <span role="status">{commands.result?.message}</span>
      <span data-testid="pending">{commands.pending?.command_id ?? "none"}</span>
      <button type="button" onClick={() => void commands.resume()}>
        恢复原请求
      </button>
    </>
  );
}

async function reachSave(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText("策略名称"), "拒绝后可修改");
  for (let index = 0; index < 3; index += 1)
    await user.click(screen.getByRole("button", { name: "下一步" }));
}

describe("明确拒绝与原未知请求", () => {
  it.each(["止损", "移动止盈"])("%s 在 100%% 时留在退出步骤，修正后可继续", async (label) => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.type(screen.getByLabelText("策略名称"), "比例边界");
    await user.click(screen.getByRole("button", { name: "下一步" }));
    await user.click(screen.getByRole("checkbox", { name: label }));
    fireEvent.change(screen.getByLabelText(`${label}幅度（%）`), { target: { value: "100" } });
    await user.click(screen.getByRole("button", { name: "下一步" }));
    expect(screen.getByRole("alert")).toHaveTextContent("止损和移动止盈须小于 100%。");
    expect(screen.getByLabelText(`${label}幅度（%）`)).toBeInTheDocument();
    expect(screen.queryByLabelText("分配方式")).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(`${label}幅度（%）`), { target: { value: "8.5" } });
    await user.click(screen.getByRole("button", { name: "下一步" }));
    expect(screen.getByLabelText("分配方式")).toBeInTheDocument();
  });

  it("422 解除原请求锁，草稿可改正并以新 ID 保存", async () => {
    const sent: SaveTemplate[] = [];
    server.use(
      http.post(base, async ({ request }) => {
        const body = (await request.json()) as SaveTemplate;
        sent.push(body);
        return sent.length === 1
          ? HttpResponse.json({ detail: "策略内容有误，请检查后重试。" }, { status: 422 })
          : HttpResponse.json(
              templateEnvelope({
                command_id: body.command_id,
                status: "published",
                message: "已保存。",
              }),
            );
      }),
    );
    const user = userEvent.setup();
    render(<Harness />);
    await reachSave(user);
    await user.click(screen.getByRole("button", { name: "保存策略" }));
    await waitFor(() => expect(screen.getByTestId("pending")).toHaveTextContent("none"));
    expect(screen.getByRole("status")).toHaveTextContent("策略内容有误，请检查后重试。");
    expect(sessionStorage.getItem(key)).toBeNull();
    expect(screen.getByRole("button", { name: "上一步" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "保存策略" })).toBeEnabled();
    for (let index = 0; index < 3; index += 1)
      await user.click(screen.getByRole("button", { name: "上一步" }));
    fireEvent.change(screen.getByLabelText("策略名称"), { target: { value: "修改后的观察" } });
    for (let index = 0; index < 3; index += 1)
      await user.click(screen.getByRole("button", { name: "下一步" }));
    await user.click(screen.getByRole("button", { name: "保存策略" }));
    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("已保存。"));
    expect(sent).toHaveLength(2);
    expect(sent[1]?.command_id).not.toBe(sent[0]?.command_id);
    expect(sent[1]?.name).toBe("修改后的观察");
  });

  it("刷新保留的无效原正文续查收到 422 后也能改正", async () => {
    const original: SaveTemplate = {
      kind: "save_strategy_template",
      command_id: crypto.randomUUID(),
      requested_at: "2026-10-05T00:00:00Z",
      generation_id: templateGeneration,
      name: "旧无效草稿",
      change_note: "",
      strategy_id: null,
      expected_head: null,
      rules: { ...templateDetail.rules, exit: { stop_loss: "1" } },
    };
    sessionStorage.setItem(key, JSON.stringify(original));
    const resumed: SaveTemplate[] = [];
    server.use(
      http.post(`${base}/resume`, async ({ request }) => {
        resumed.push((await request.json()) as SaveTemplate);
        return HttpResponse.json({ detail: "策略内容有误，请检查后重试。" }, { status: 422 });
      }),
    );
    const user = userEvent.setup();
    render(<Harness />);
    expect(screen.getByTestId("pending")).toHaveTextContent(original.command_id);
    await user.click(screen.getByRole("button", { name: "恢复原请求" }));
    await waitFor(() => expect(screen.getByTestId("pending")).toHaveTextContent("none"));
    expect(resumed).toEqual([original]);
    expect(sessionStorage.getItem(key)).toBeNull();
    expect(screen.getByRole("button", { name: "下一步" })).toBeEnabled();
  });

  it.each(["503", "disconnect", "wrong-receipt"])(
    "%s 继续保留原身份，恢复不会新建",
    async (failure) => {
      const sent: SaveTemplate[] = [];
      const resumed: SaveTemplate[] = [];
      server.use(
        http.post(base, async ({ request }) => {
          const body = (await request.json()) as SaveTemplate;
          sent.push(body);
          if (failure === "503") return HttpResponse.json({ detail: "等待恢复" }, { status: 503 });
          if (failure === "disconnect") return HttpResponse.error();
          return HttpResponse.json(
            templateEnvelope({
              command_id: crypto.randomUUID(),
              status: "published",
              message: "错配",
            }),
          );
        }),
        http.post(`${base}/resume`, async ({ request }) => {
          const body = (await request.json()) as SaveTemplate;
          resumed.push(body);
          return HttpResponse.json(
            templateEnvelope({
              command_id: body.command_id,
              status: "rejected",
              message: "原请求已确认。",
            }),
          );
        }),
      );
      const user = userEvent.setup();
      render(<Harness />);
      await reachSave(user);
      await user.click(screen.getByRole("button", { name: "保存策略" }));
      await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("结果待确认"));
      expect(screen.getByTestId("pending")).toHaveTextContent(sent[0]?.command_id ?? "missing");
      expect(screen.getByRole("button", { name: "上一步" })).toBeDisabled();
      expect(JSON.parse(sessionStorage.getItem(key) ?? "null")).toEqual(sent[0]);
      await user.click(screen.getByRole("button", { name: "恢复原请求" }));
      await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("原请求已确认。"));
      expect(resumed).toEqual(sent);
      expect(sent).toHaveLength(1);
    },
  );
});
