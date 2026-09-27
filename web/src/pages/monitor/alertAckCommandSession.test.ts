import { AckNoEffectError } from "@/api/alertAckCommand";
import { ApiError, type Schemas } from "@/api/client";
import { ACK_JOURNAL_KEY, AlertAckCommandSession } from "./alertAckCommandSession";

type Command = Schemas["AckCommandRequest"];
type Receipt = Schemas["AckCommandReceipt"];
const ALERT = "a".repeat(64);
const GENERATION = "b".repeat(64);
const CONFIRMATION = "confirmed-first";
let idSequence = 0;

beforeEach(() => {
  idSequence = 0;
  window.localStorage.clear();
  window.sessionStorage.clear();
});

function session(
  post: (body: Command) => Promise<Receipt>,
  viewer = "tester",
  storage = window.localStorage,
) {
  return new AlertAckCommandSession(
    storage,
    viewer,
    post,
    () => `web-${++idSequence}`,
    () => "2026-09-28T07:00:00.000Z",
  );
}

it("saves the four-field original request before POST and records only a verified success", async () => {
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    const saved = JSON.parse(
      window.localStorage.getItem(`${ACK_JOURNAL_KEY}:tester:${ALERT}:${body.command_id}`) ?? "{}",
    );
    expect(saved.body).toEqual(body);
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

it("retries a lost response after the tab closes and reopens with exactly the stored request", async () => {
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
  window.sessionStorage.clear();
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
  window.localStorage.setItem(`${ACK_JOURNAL_KEY}:tester:${ALERT}:web-1`, "{broken");
  const post = vi.fn();
  const current = session(post);
  await current.start(GENERATION, ALERT);
  expect(current.snapshot().storageAvailable).toBe(false);
  expect(post).not.toHaveBeenCalled();
});

it("ends a proved no-effect request with no observed receipt", async () => {
  const post = vi.fn(async () => {
    throw new AckNoEffectError();
  });
  const current = session(post);
  await current.start(GENERATION, ALERT);
  expect(current.snapshot().entries[ALERT]).toMatchObject({
    status: "failed",
    failureKind: "stale_generation",
  });
  const reopened = session(post);
  expect(reopened.snapshot().entries[ALERT]).toMatchObject({
    status: "failed",
    failureKind: "stale_generation",
  });
});

it("treats an older saved request without receipt evidence as uncertain", async () => {
  const body: Command = {
    command_id: "web-1",
    requested_at: "2026-09-28T07:00:00.000Z",
    generation_id: GENERATION,
    alert_id: ALERT,
  };
  window.localStorage.setItem(
    `${ACK_JOURNAL_KEY}:tester:${ALERT}:${body.command_id}`,
    JSON.stringify({
      schema: 2,
      body,
      status: "unknown",
      confirmationId: null,
      failureKind: null,
    }),
  );
  const post = vi.fn(async () => {
    throw new AckNoEffectError();
  });
  const current = session(post);
  await current.advance(ALERT);
  expect(current.snapshot().entries[ALERT]).toMatchObject({
    status: "unknown",
    failureKind: null,
  });
  await current.start("c".repeat(64), ALERT);
  expect(post).toHaveBeenCalledTimes(1);
});

it("ends a lost original request only after a later retry proves no effect", async () => {
  const nextGeneration = "c".repeat(64);
  const sent: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    sent.push(body);
    if (sent.length === 1) throw new ApiError(503, "连接断开");
    if (sent.length === 2) throw new AckNoEffectError();
    return {
      command_id: body.command_id,
      status: "succeeded",
      confirmation_id: CONFIRMATION,
      message: "已受理，正在同步",
    };
  });
  await session(post).start(GENERATION, ALERT);
  const reopened = session(post);
  await reopened.advance(ALERT);
  expect(sent[1]).toEqual(sent[0]);
  expect(reopened.snapshot().entries[ALERT]).toMatchObject({
    status: "failed",
    failureKind: "stale_generation",
  });
  await reopened.start(nextGeneration, ALERT);
  expect(sent).toHaveLength(3);
  expect(sent[2]?.generation_id).toBe(nextGeneration);
  expect(sent[2]?.command_id).not.toBe(sent[0]?.command_id);
});

it.each(["pending", "processing"] as const)(
  "retains an original request after a durable %s receipt despite later no-effect reply",
  async (status) => {
    const sent: Command[] = [];
    const post = vi.fn(async (body: Command): Promise<Receipt> => {
      sent.push(body);
      if (sent.length === 1) return { command_id: body.command_id, status, message: "稍后核对" };
      if (sent.length === 2) throw new ApiError(503, "连接断开");
      throw new AckNoEffectError();
    });
    const current = session(post);
    await current.start(GENERATION, ALERT);
    await current.advance(ALERT);
    const reopened = session(post);
    await reopened.advance(ALERT);
    expect(sent[1]).toEqual(sent[0]);
    expect(sent[2]).toEqual(sent[0]);
    expect(reopened.snapshot().entries[ALERT]).toMatchObject({
      status: "unknown",
      failureKind: null,
    });
    await reopened.start("c".repeat(64), ALERT);
    expect(sent).toHaveLength(3);
  },
);

it("keeps another tab's observed receipt when a stale request returns no effect", async () => {
  let rejectStale!: () => void;
  const firstPost = vi.fn(
    (_body: Command) =>
      new Promise<Receipt>((_resolve, reject) => {
        rejectStale = () => reject(new AckNoEffectError());
      }),
  );
  const otherPost = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "pending",
      message: "等待处理",
    }),
  );
  const first = session(firstPost);
  const firstRequest = first.start(GENERATION, ALERT);
  const otherTab = session(otherPost);
  await otherTab.advance(ALERT);
  expect(otherPost.mock.calls[0]?.[0]).toEqual(firstPost.mock.calls[0]?.[0]);
  rejectStale();
  await firstRequest;
  const reopened = session(otherPost);
  expect(reopened.snapshot().entries[ALERT]).toMatchObject({
    status: "unknown",
    failureKind: null,
  });
  await reopened.start("c".repeat(64), ALERT);
  expect(otherPost).toHaveBeenCalledTimes(1);
});

it("does not turn a later 409 into failure after an uncertain first effect", async () => {
  let attempts = 0;
  const post = vi.fn(async (_body: Command) => {
    attempts += 1;
    throw new ApiError(attempts === 1 ? 503 : 409, "状态不明");
  });
  const current = session(post);
  await current.start(GENERATION, ALERT);
  await current.advance(ALERT);
  expect(current.snapshot().entries[ALERT]?.status).toBe("unknown");
  expect(post.mock.calls[1]?.[0]).toEqual(post.mock.calls[0]?.[0]);
});

it("keeps an ordinary first 409 uncertain and retries only the saved command", async () => {
  const post = vi.fn(async (_body: Command) => {
    throw new ApiError(409, "准入冲突但效果待核对");
  });
  const current = session(post);
  await current.start(GENERATION, ALERT);
  expect(current.snapshot().entries[ALERT]?.status).toBe("unknown");
  await current.advance(ALERT);
  expect(post.mock.calls[1]?.[0]).toEqual(post.mock.calls[0]?.[0]);
});

it("does not let another tab's late uncertain reply replace a proved no-effect result", async () => {
  let rejectStale!: () => void;
  let rejectUnknown!: () => void;
  const firstPost = vi.fn(
    (_body: Command) =>
      new Promise<Receipt>((_resolve, reject) => {
        rejectStale = () => reject(new AckNoEffectError());
      }),
  );
  const secondPost = vi.fn(
    (_body: Command) =>
      new Promise<Receipt>((_resolve, reject) => {
        rejectUnknown = () => reject(new ApiError(503, "连接断开"));
      }),
  );
  const first = session(firstPost);
  const firstRequest = first.start(GENERATION, ALERT);
  const second = session(secondPost);
  const secondRequest = second.advance(ALERT);
  rejectStale();
  await firstRequest;
  rejectUnknown();
  await secondRequest;
  expect(session(secondPost).snapshot().entries[ALERT]).toMatchObject({
    status: "failed",
    failureKind: "stale_generation",
  });
});

it("keeps different tabs' commands in independent records and adopts an existing alert request", async () => {
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "pending",
      message: "等待处理",
    }),
  );
  const firstTab = session(post);
  const secondTab = session(post);
  const unsubscribe = secondTab.subscribe(vi.fn());
  await firstTab.start(GENERATION, ALERT);
  window.dispatchEvent(
    new StorageEvent("storage", {
      key: `${ACK_JOURNAL_KEY}:tester:${ALERT}:web-1`,
      storageArea: window.localStorage,
    }),
  );
  expect(secondTab.snapshot().message).toContain("另一标签页");
  await secondTab.start(GENERATION, ALERT);
  expect(post).toHaveBeenCalledTimes(1);
  expect(secondTab.snapshot().entries[ALERT]?.body).toEqual(
    firstTab.snapshot().entries[ALERT]?.body,
  );

  const otherAlert = "c".repeat(64);
  await secondTab.start(GENERATION, otherAlert);
  const reopened = session(post);
  expect(reopened.snapshot().entries[ALERT]).toBeDefined();
  expect(reopened.snapshot().entries[otherAlert]).toBeDefined();
  expect(
    Object.keys(window.localStorage).filter((key) => key.startsWith(`${ACK_JOURNAL_KEY}:tester:`)),
  ).toHaveLength(2);
  unsubscribe();
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
