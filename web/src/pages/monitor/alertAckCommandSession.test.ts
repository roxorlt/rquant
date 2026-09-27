import { ApiError, type Schemas } from "@/api/client";
import { ACK_JOURNAL_KEY, AlertAckCommandSession } from "./alertAckCommandSession";

type Command = Schemas["AckCommandRequest"];
type Receipt = Schemas["AckCommandReceipt"];
const ALERT = "a".repeat(64);
const GENERATION = "b".repeat(64);
const CONFIRMATION = "confirmed-first";

beforeEach(() => window.sessionStorage.clear());

function session(
  post: (body: Command) => Promise<Receipt>,
  viewer = "tester",
  storage = window.sessionStorage,
) {
  let number = 0;
  return new AlertAckCommandSession(
    storage,
    viewer,
    post,
    () => `web-${++number}`,
    () => "2026-09-28T07:00:00.000Z",
  );
}

it("saves the four-field original request before POST and records only a verified success", async () => {
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    const saved = JSON.parse(window.sessionStorage.getItem(`${ACK_JOURNAL_KEY}:tester`) ?? "{}");
    expect(saved.entries[ALERT].body).toEqual(body);
    return {
      command_id: body.command_id,
      status: "succeeded",
      confirmation_id: CONFIRMATION,
      message: "已受理，正在同步",
    };
  });
  const current = session(post);
  await current.start(GENERATION, ALERT);
  expect(Object.keys(post.mock.calls[0]?.[0] ?? {}).sort()).toEqual([
    "alert_id",
    "command_id",
    "generation_id",
    "requested_at",
  ]);
  expect(current.snapshot().entries[ALERT]).toMatchObject({
    status: "succeeded",
    confirmationId: CONFIRMATION,
  });
  await current.advance(ALERT);
  expect(post).toHaveBeenCalledTimes(1);
});

it("retries a lost response after reload with exactly the stored request", async () => {
  const seen: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    seen.push(body);
    if (seen.length === 1) throw new ApiError(503, "连接暂不可用");
    return {
      command_id: body.command_id,
      status: "succeeded",
      confirmation_id: CONFIRMATION,
      message: "已受理，正在同步",
    };
  });
  const first = session(post);
  await first.start(GENERATION, ALERT);
  expect(first.snapshot().entries[ALERT]?.status).toBe("unknown");
  const restored = session(post);
  await restored.resumePending();
  expect(seen).toHaveLength(2);
  expect(seen[1]).toEqual(seen[0]);
  expect(restored.snapshot().entries[ALERT]?.status).toBe("succeeded");
});

it.each(["pending", "processing", "ambiguous"] as const)(
  "continues a durable %s request using the same body",
  async (status) => {
    const seen: Command[] = [];
    const post = vi.fn(async (body: Command): Promise<Receipt> => {
      seen.push(body);
      return seen.length === 1
        ? { command_id: body.command_id, status, message: "稍后核对" }
        : {
            command_id: body.command_id,
            status: "succeeded",
            confirmation_id: CONFIRMATION,
            message: "已受理，正在同步",
          };
    });
    const current = session(post);
    await current.start(GENERATION, ALERT);
    await current.advance(ALERT);
    expect(seen[1]).toEqual(seen[0]);
    expect(current.snapshot().entries[ALERT]?.status).toBe("succeeded");
  },
);

it("keeps a mismatched or incomplete success uncertain", async () => {
  let attempt = 0;
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    attempt += 1;
    return attempt === 1
      ? {
          command_id: "someone-else",
          status: "succeeded",
          confirmation_id: CONFIRMATION,
          message: "已受理",
        }
      : { command_id: body.command_id, status: "succeeded", message: "已受理" };
  });
  const current = session(post);
  await current.start(GENERATION, ALERT);
  expect(current.snapshot().entries[ALERT]).toMatchObject({
    status: "unknown",
    confirmationId: null,
  });
  await current.advance(ALERT);
  expect(current.snapshot().entries[ALERT]).toMatchObject({
    status: "unknown",
    confirmationId: null,
  });
  expect(post.mock.calls[1]?.[0]).toEqual(post.mock.calls[0]?.[0]);
});

it("does not POST without durable storage and keeps each viewer's requests separate", async () => {
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "pending",
      message: "等待处理",
    }),
  );
  const blocked = session(post, "tester", {
    getItem: () => null,
    setItem: () => {
      throw new Error("storage unavailable");
    },
    removeItem: () => undefined,
  } as unknown as Storage);
  await blocked.start(GENERATION, ALERT);
  expect(post).not.toHaveBeenCalled();
  expect(blocked.snapshot().storageAvailable).toBe(false);

  const first = session(post);
  await first.start(GENERATION, ALERT);
  const second = session(post, "other");
  expect(second.snapshot().entries[ALERT]).toBeUndefined();
  expect(second.snapshot().storageAvailable).toBe(true);
});

it("rejects a malformed saved journal instead of overwriting an uncertain command", async () => {
  window.sessionStorage.setItem(`${ACK_JOURNAL_KEY}:tester`, "{broken");
  const post = vi.fn();
  const current = session(post);
  await current.start(GENERATION, ALERT);
  expect(current.snapshot().storageAvailable).toBe(false);
  expect(post).not.toHaveBeenCalled();
});

it("only a durable failed receipt permits a fresh command", async () => {
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "failed",
      message: "未完成",
    }),
  );
  const current = session(post);
  await current.start(GENERATION, ALERT);
  await current.start(GENERATION, ALERT);
  expect(post.mock.calls.map(([body]) => body.command_id)).toEqual(["web-1", "web-2"]);
});

it("does not send a second request while the first receipt is pending", async () => {
  let complete!: (receipt: Receipt) => void;
  const post = vi.fn(
    (_: Command) =>
      new Promise<Receipt>((resolve) => {
        complete = resolve;
      }),
  );
  const current = session(post);
  const first = current.start(GENERATION, ALERT);
  await current.start(GENERATION, ALERT);
  await current.advance(ALERT);
  expect(post).toHaveBeenCalledTimes(1);
  complete({
    command_id: "web-1",
    status: "succeeded",
    confirmation_id: CONFIRMATION,
    message: "已受理，正在同步",
  });
  await first;
  expect(current.snapshot().entries[ALERT]?.status).toBe("succeeded");
});
