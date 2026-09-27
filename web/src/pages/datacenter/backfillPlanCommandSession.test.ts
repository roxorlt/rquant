import { ApiError, type Schemas } from "@/api/client";
import {
  BACKFILL_PLAN_JOURNAL_KEY,
  BackfillPlanCommandSession,
  validateBackfillRange,
} from "./backfillPlanCommandSession";

type Command = Schemas["BackfillPlanCommandRequest"];
type Receipt = Schemas["BackfillPlanCommandReceipt"];
const TASK = "a".repeat(32);

beforeEach(() => window.sessionStorage.clear());

function makeSession(post: (body: Command) => Promise<Receipt>, storage = window.sessionStorage) {
  let number = 0;
  return new BackfillPlanCommandSession(
    storage,
    post,
    () => `web-${++number}`,
    () => "2026-09-27T07:00:00.000Z",
  );
}

it("persists the exact four-field request before POST and accepts only a verified queued receipt", async () => {
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    expect(
      JSON.parse(window.sessionStorage.getItem(BACKFILL_PLAN_JOURNAL_KEY) ?? "{}").body,
    ).toEqual(body);
    return { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
  });
  const session = makeSession(post);
  await session.start("2024-09-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
  expect(Object.keys(post.mock.calls[0]?.[0] ?? {}).sort()).toEqual([
    "audit_start",
    "command_id",
    "completed_through",
    "requested_at",
  ]);
  expect(session.snapshot().journal).toMatchObject({ status: "queued", taskId: TASK });
  await session.advance();
  expect(post).toHaveBeenCalledTimes(1);
});

it("retries an uncertain request after reload with its original body and ID", async () => {
  const sent: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    sent.push(body);
    if (sent.length === 1) throw new ApiError(503, "状态不明");
    return { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
  });
  const session = makeSession(post);
  await session.start("2024-09-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
  expect(session.snapshot().journal?.status).toBe("unknown");
  const restored = makeSession(post);
  await restored.advance();
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
  expect(restored.snapshot().journal).toMatchObject({ status: "queued", taskId: TASK });
});

it.each(["pending", "processing", "ambiguous"] as const)(
  "retries %s using the stored request",
  async (status) => {
    const sent: Command[] = [];
    const post = vi.fn(async (body: Command): Promise<Receipt> => {
      sent.push(body);
      return sent.length === 1
        ? { command_id: body.command_id, status, message: "稍后核对" }
        : { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
    });
    const session = makeSession(post);
    await session.start("2024-09-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
    expect(session.snapshot().journal?.status).toBe(status);
    await session.advance();
    expect(sent[1]).toEqual(sent[0]);
    expect(session.snapshot().journal?.taskId).toBe(TASK);
  },
);

it("keeps a mismatched or taskless queued receipt uncertain", async () => {
  let attempt = 0;
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    attempt += 1;
    return attempt === 1
      ? { command_id: "someone-else", status: "queued", task_id: TASK, message: "已排队" }
      : { command_id: body.command_id, status: "queued", task_id: "short", message: "已排队" };
  });
  const session = makeSession(post);
  await session.start("2024-09-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
  expect(session.snapshot().journal).toMatchObject({ status: "unknown", taskId: null });
  await session.advance();
  expect(session.snapshot().journal).toMatchObject({ status: "unknown", taskId: null });
  expect(post.mock.calls[1]?.[0]).toEqual(post.mock.calls[0]?.[0]);
});

it("does not POST when storage fails, and can issue a new request after a definitive failure", async () => {
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "failed",
      message: "失败",
    }),
  );
  const unavailable = makeSession(post, {
    getItem: () => null,
    setItem: () => {
      throw new Error("no storage");
    },
    removeItem: () => undefined,
  } as unknown as Storage);
  await unavailable.start("2024-09-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
  expect(post).not.toHaveBeenCalled();
  expect(unavailable.snapshot().storageAvailable).toBe(false);

  const session = makeSession(post);
  await session.start("2024-09-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
  expect(session.snapshot().journal?.status).toBe("failed");
  await session.start("2024-10-01", "2025-04-30", new Date("2026-09-27T07:00:00Z"));
  expect(post.mock.calls.map(([body]) => body.command_id)).toEqual(["web-1", "web-2"]);
});

it("validates real dates, range length, and the Shanghai close before creating an ID", () => {
  const noon = new Date("2026-09-27T06:59:00Z");
  expect(validateBackfillRange("2024-09-01", "2025-04-30", noon)).toBeNull();
  expect(validateBackfillRange("2025-02-30", "2025-04-30", noon)).not.toBeNull();
  expect(validateBackfillRange("2025-05-01", "2025-04-30", noon)).not.toBeNull();
  expect(validateBackfillRange("2010-01-01", "2025-04-30", noon)).not.toBeNull();
  expect(validateBackfillRange("2026-09-01", "2026-09-27", noon)).not.toBeNull();
  expect(
    validateBackfillRange("2026-09-01", "2026-09-27", new Date("2026-09-27T07:00:00Z")),
  ).toBeNull();
});
