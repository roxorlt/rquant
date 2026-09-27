import { ApiError, type Schemas } from "@/api/client";
import { POOL_EDITOR_JOURNAL_KEY, PoolEditorSession } from "./editorSession";

type Command = Schemas["SavePoolCommand"] | Schemas["AttachPoolCommand"];
type Receipt = Schemas["PoolEditorReceipt"];

const VERSION = "b".repeat(64);
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
