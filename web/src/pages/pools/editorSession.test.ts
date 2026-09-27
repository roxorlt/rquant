import { ApiError, type Schemas } from "@/api/client";
import { POOL_EDITOR_JOURNAL_KEY, PoolEditorSession } from "./editorSession";

type Command = Schemas["SavePoolCommand"] | Schemas["AttachPoolCommand"];
type Receipt = Schemas["PoolEditorReceipt"];

const VERSION = "b".repeat(64);
const NEW_VERSION = "c".repeat(64);
const ids = ["save-1", "attach-1", "attach-2"];

function makeSession(post: (body: Command) => Promise<Receipt>) {
  let index = 0;
  return new PoolEditorSession(
    window.sessionStorage,
    post,
    () => ids[index++] ?? `extra-${index}`,
    () => "2026-09-27T07:00:00.000Z",
  );
}

const saveInput = {
  base_name: "放量确认",
  display_name: "放量确认",
  description: "",
  depends_on: "n-shape-pool1",
  delay_days: 2,
  rule_calls: [{ name: "volume_ratio_gte", args: { n: 2, window: 5 } }],
  include_columns: [],
  expected_version: null,
} satisfies Omit<Schemas["SavePoolCommand"], "kind" | "command_id" | "requested_at">;

beforeEach(() => window.sessionStorage.clear());

it("persists each complete immutable body before POST and never repeats a succeeded save", async () => {
  const seen: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    const persisted = JSON.parse(window.sessionStorage.getItem(POOL_EDITOR_JOURNAL_KEY) ?? "{}");
    expect(body).toEqual(body.kind === "save_user_pool_v2" ? persisted.save : persisted.attach);
    seen.push(body);
    return body.kind === "save_user_pool_v2"
      ? {
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已保存",
          pool_version: VERSION,
        }
      : {
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已加入当前画布",
          pool_version: VERSION,
          canvas_name: "观察画布",
        };
  });
  const session = makeSession(post);
  await session.startSave(saveInput, "观察画布");
  expect(seen.map((body) => body.kind)).toEqual(["save_user_pool_v2", "add_pool_to_canvas"]);
  expect(seen[1]).toMatchObject({ expected_pool_version: VERSION, pool_name: "user/放量确认" });
  expect(session.snapshot().journal?.saveVersion).toBe(VERSION);
  expect(session.snapshot().journal?.attachStatus).toBe("succeeded");
});

it("keeps the exact save body after a 503, pending receipt, and reload", async () => {
  const seen: Command[] = [];
  let attempt = 0;
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    seen.push(body);
    attempt += 1;
    if (attempt === 1) throw new ApiError(503, "连接暂不可用");
    if (attempt === 2) return { command_id: body.command_id, status: "pending", message: "已受理" };
    return {
      command_id: body.command_id,
      status: "succeeded",
      message: "池子已保存",
      pool_version: VERSION,
    };
  });
  const first = makeSession(post);
  await first.startSave(saveInput, null);
  expect(first.snapshot().journal?.saveStatus).toBe("unknown");
  expect(first.snapshot().journal?.saveVersion).toBeNull();
  await first.advance();
  expect(first.snapshot().journal?.saveStatus).toBe("pending");
  const restored = makeSession(post);
  await restored.advance();
  expect(seen).toHaveLength(3);
  expect(seen[0]).toEqual(seen[1]);
  expect(seen[1]).toEqual(seen[2]);
  expect(restored.snapshot().journal?.saveVersion).toBe(VERSION);
});

it("keeps an ambiguous save command unchanged until its original receipt is confirmed", async () => {
  const seen: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    seen.push(body);
    return {
      command_id: body.command_id,
      status: seen.length === 1 ? "ambiguous" : "succeeded",
      message: "状态待确认",
      pool_version: seen.length === 1 ? null : VERSION,
    };
  });
  const session = makeSession(post);
  await session.startSave(saveInput, null);
  expect(session.snapshot().journal?.saveStatus).toBe("ambiguous");
  const restored = makeSession(post);
  await restored.advance();
  expect(seen).toHaveLength(2);
  expect(seen[1]).toEqual(seen[0]);
  expect(restored.snapshot().journal?.saveVersion).toBe(VERSION);
});

it("keeps ambiguous save identity and retries only attachment with a new ID after terminal failure", async () => {
  const seen: Command[] = [];
  let attachAttempts = 0;
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    seen.push(body);
    if (body.kind === "save_user_pool_v2") {
      return {
        command_id: body.command_id,
        status: "succeeded",
        message: "池子已保存",
        pool_version: VERSION,
      };
    }
    attachAttempts += 1;
    return attachAttempts === 1
      ? { command_id: body.command_id, status: "failed", message: "画布已变化" }
      : {
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已加入当前画布",
          pool_version: VERSION,
          canvas_name: "观察画布",
        };
  });
  const session = makeSession(post);
  await session.startSave(saveInput, "观察画布");
  expect(session.snapshot().journal?.attachStatus).toBe("failed");
  expect(session.snapshot().journal?.saveVersion).toBe(VERSION);
  await session.retryAttachment();
  expect(seen.map((body) => body.command_id)).toEqual(["save-1", "attach-1", "attach-2"]);
  expect(seen[2]).toMatchObject({ expected_pool_version: VERSION });
  expect(session.snapshot().journal?.attachStatus).toBe("succeeded");
});

it("does not POST when storage cannot durably accept the body", async () => {
  const post = vi.fn<(_body: Command) => Promise<Receipt>>();
  const storage = {
    getItem: () => null,
    setItem: () => {
      throw new Error("quota");
    },
    removeItem: () => {},
  } as unknown as Storage;
  const session = new PoolEditorSession(
    storage,
    post,
    () => "save-1",
    () => "2026-09-27T07:00:00.000Z",
  );
  await session.startSave(saveInput, null);
  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().message).toMatch(/浏览器存储/);
});

it("keeps an unresolved attachment resumable after reload and frees other saves only after durable deferral", async () => {
  const seen: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    seen.push(body);
    if (body.kind === "save_user_pool_v2")
      return {
        command_id: body.command_id,
        status: "succeeded",
        message: "已保存",
        pool_version: VERSION,
      };
    return {
      command_id: body.command_id,
      status: "ambiguous",
      message: "待核对",
      pool_version: null,
    };
  });
  const first = makeSession(post);
  await first.startSave(saveInput, "观察画布");
  const originalAttach = first.snapshot().journal?.attach;
  expect(first.snapshot().journal?.attachStatus).toBe("ambiguous");
  const restored = makeSession(post);
  await restored.advance();
  expect(seen[2]).toEqual(originalAttach);
  expect(restored.snapshot().journal?.attachStatus).toBe("ambiguous");
  await restored.deferAttachment();
  expect(restored.snapshot().deferred).toHaveLength(1);
  expect(restored.snapshot().deferred[0]?.attach).toEqual(originalAttach);
  await restored.startSave({ ...saveInput, base_name: "另一只池", display_name: "另一只池" }, null);
  expect(seen.at(-1)?.kind).toBe("save_user_pool_v2");
  const afterReload = makeSession(post);
  expect(afterReload.snapshot().deferred[0]?.attach).toEqual(originalAttach);
  await afterReload.advanceDeferred(originalAttach?.command_id ?? "");
  expect(seen.at(-1)).toEqual(originalAttach);
});

it("uses a newly verified version for a fresh attachment after conflict, or ends failed attachment", async () => {
  const seen: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    seen.push(body);
    if (body.kind === "save_user_pool_v2")
      return {
        command_id: body.command_id,
        status: "succeeded",
        message: "已保存",
        pool_version: VERSION,
      };
    if (body.expected_pool_version === VERSION) throw new ApiError(409, "池子规则已更新");
    return {
      command_id: body.command_id,
      status: "succeeded",
      message: "已加入",
      pool_version: NEW_VERSION,
      canvas_name: "观察画布",
    };
  });
  const session = makeSession(post);
  await session.startSave(saveInput, "观察画布");
  expect(session.snapshot().journal?.attachStatus).toBe("failed");
  await session.retryAttachment(VERSION);
  expect(seen).toHaveLength(2);
  await session.retryAttachment(NEW_VERSION);
  expect(seen[2]).toMatchObject({ expected_pool_version: NEW_VERSION });
  expect(session.snapshot().journal?.attachStatus).toBe("succeeded");

  const failed = makeSession(async (body) =>
    body.kind === "save_user_pool_v2"
      ? {
          command_id: body.command_id,
          status: "succeeded",
          message: "已保存",
          pool_version: VERSION,
        }
      : { command_id: body.command_id, status: "failed", message: "加入失败", pool_version: null },
  );
  await failed.startSave({ ...saveInput, base_name: "新池", display_name: "新池" }, "观察画布");
  expect(failed.snapshot().journal?.attachStatus).toBe("failed");
  failed.discardFailedAttachment();
  expect(failed.snapshot().journal?.canvasName).toBeNull();
  await failed.startSave({ ...saveInput, base_name: "后续池", display_name: "后续池" }, null);
  expect(failed.snapshot().journal?.save.base_name).toBe("后续池");
});
