import { HttpResponse, http } from "msw";
import { server } from "@/test/server";
import {
  MANUAL_WATCHLIST_JOURNAL_KEY,
  type ManualWatchlistCommandBody,
  ManualWatchlistCommandSession,
  publishedMatches,
  submitManualWatchlistCommand,
} from "./manualWatchlistCommand";

const CODE = "600001.SH";
const GENERATION = "a".repeat(64);
const NEXT_GENERATION = "b".repeat(64);
const AT = "2026-09-28T07:00:00.000Z";
const add: ManualWatchlistCommandBody = {
  command_id: "web-1",
  requested_at: AT,
  generation_id: GENERATION,
  ts_code: CODE,
  action: "add",
  expected_version: null,
  source: "detail",
  price_levels: [],
};
const remove: ManualWatchlistCommandBody = {
  command_id: "web-2",
  requested_at: AT,
  generation_id: GENERATION,
  ts_code: CODE,
  action: "remove",
  expected_version: 2,
};

beforeEach(() => window.localStorage.clear());

it("发送带同源校验的原请求，移出 JSON 不含加入字段", async () => {
  const sent: unknown[] = [];
  const csrf: string[] = [];
  server.use(
    http.post("*/api/v1/watchlist/commands", async ({ request }) => {
      sent.push(await request.json());
      csrf.push(request.headers.get("X-Rquant-Csrf") ?? "");
      return HttpResponse.json({
        command_id: remove.command_id,
        ts_code: CODE,
        action: "remove",
        status: "saved_syncing",
        version: 3,
        message: "已保存，正在同步。",
      });
    }),
  );
  await submitManualWatchlistCommand(remove);
  expect(sent).toEqual([remove]);
  expect(csrf).toEqual(["1"]);
  expect(sent[0]).not.toHaveProperty("price_levels");
  expect(sent[0]).not.toHaveProperty("source");
  expect(sent[0]).not.toHaveProperty("expires_at");
});

it("从选股本页加入时持久命令保留选股来源", async () => {
  const post = vi.fn(async (body: ManualWatchlistCommandBody) => ({
    command_id: body.command_id,
    ts_code: CODE,
    action: "add" as const,
    status: "pending" as const,
    version: null,
    message: "正在处理",
  }));
  await session(post).start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
    source: "screen_result",
  });
  expect(post).toHaveBeenCalledWith(expect.objectContaining({ source: "screen_result" }));
  expect(window.localStorage.getItem(`${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:${CODE}`)).toContain(
    '"source":"screen_result"',
  );
});

it("把 409 的有类型容量或冲突回执交给原命令核对", async () => {
  server.use(
    http.post("*/api/v1/watchlist/commands", () =>
      HttpResponse.json(
        {
          command_id: add.command_id,
          ts_code: CODE,
          action: "add",
          status: "capacity",
          version: null,
          message: "盯盘名单已满。",
        },
        { status: 409 },
      ),
    ),
  );
  await expect(submitManualWatchlistCommand(add)).resolves.toMatchObject({ status: "capacity" });
});

function exclusive(_name: string, task: () => Promise<void>) {
  return task();
}

function session(
  post: (body: ManualWatchlistCommandBody) => Promise<{
    command_id: string;
    ts_code: string;
    action: "add" | "remove";
    status:
      | "pending"
      | "processing"
      | "saved_syncing"
      | "published"
      | "conflict"
      | "capacity"
      | "failed"
      | "uncertain";
    version?: number | null;
    message: string;
  }>,
  viewer = "tester",
  storage: Storage | null = window.localStorage,
  withLock = exclusive,
) {
  return new ManualWatchlistCommandSession({
    storage,
    viewer,
    tsCode: CODE,
    post,
    verifyBasis: async () => "ready" as const,
    nextId: () => "web-1",
    now: () => AT,
    withLock,
  });
}

it("先持久保存原请求，丢响应后刷新只续查同一 ID 与内容", async () => {
  const seen: ManualWatchlistCommandBody[] = [];
  const post = vi.fn(async (body: ManualWatchlistCommandBody) => {
    seen.push(body);
    const saved = window.localStorage.getItem(`${MANUAL_WATCHLIST_JOURNAL_KEY}:tester:${CODE}`);
    expect(saved).toContain(body.command_id);
    if (seen.length === 1) throw new Error("连接断开");
    return {
      command_id: body.command_id,
      ts_code: CODE,
      action: "add" as const,
      status: "saved_syncing" as const,
      version: 1,
      message: "已保存，正在同步。",
    };
  });
  await session(post).start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  expect(session(post).snapshot().record?.status).toBe("unknown");
  const reopened = session(post);
  await reopened.advance();
  expect(seen).toHaveLength(2);
  expect(seen[1]).toEqual(seen[0]);
  expect(reopened.snapshot().record).toMatchObject({ status: "saved_syncing", version: 1 });
  await reopened.start({
    action: "add",
    generationId: NEXT_GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  expect(post).toHaveBeenCalledTimes(2);
});

it("错配回执保持待核对，跨用户不读取旧命令", async () => {
  const post = vi.fn(async () => ({
    command_id: "wrong",
    ts_code: CODE,
    action: "add" as const,
    status: "published" as const,
    version: 1,
    message: "已加入盯盘。",
  }));
  const first = session(post);
  await first.start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  expect(first.snapshot().record?.status).toBe("unknown");
  expect(session(post, "other").snapshot().record).toBeNull();
});

it("没有持久存储或跨标签互斥能力时不发新命令", async () => {
  const post = vi.fn();
  const noStorage = session(post, "tester", null);
  await noStorage.start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  expect(noStorage.snapshot().storageAvailable).toBe(false);
  const noLock = session(post, "tester", window.localStorage, async () => {
    throw new Error("lock unavailable");
  });
  await noLock.start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  expect(post).not.toHaveBeenCalled();
  expect(window.localStorage.length).toBe(0);
});

it("并发标签串行读取持久命令，只产生一个新 ID", async () => {
  let release!: () => void;
  const firstReply = new Promise<void>((resolve) => {
    release = resolve;
  });
  let tail = Promise.resolve();
  const withLock = async (_name: string, task: () => Promise<void>) => {
    const previous = tail;
    let done!: () => void;
    tail = new Promise<void>((resolve) => {
      done = resolve;
    });
    await previous;
    try {
      await task();
    } finally {
      done();
    }
  };
  const post = vi.fn(async (body: ManualWatchlistCommandBody) => {
    await firstReply;
    return {
      command_id: body.command_id,
      ts_code: CODE,
      action: "add" as const,
      status: "pending" as const,
      message: "正在处理。",
    };
  });
  const first = session(post, "tester", window.localStorage, withLock);
  const second = session(post, "tester", window.localStorage, withLock);
  const one = first.start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  const two = second.start({
    action: "add",
    generationId: GENERATION,
    expectedVersion: null,
    observedStatus: "absent",
  });
  release();
  await Promise.all([one, two]);
  expect(post).toHaveBeenCalledTimes(1);
  expect(second.snapshot().record?.body.command_id).toBe("web-1");
});

it("只有 published 且下一代单股版本与动作都匹配才给最终状态", () => {
  const entry = { body: add, status: "published" as const, version: 1 };
  expect(publishedMatches(entry, { generationId: GENERATION, status: "active", version: 1 })).toBe(
    false,
  );
  expect(
    publishedMatches(entry, { generationId: NEXT_GENERATION, status: "active", version: 2 }),
  ).toBe(false);
  expect(
    publishedMatches(entry, { generationId: NEXT_GENERATION, status: "expired", version: 1 }),
  ).toBe(false);
  expect(
    publishedMatches(entry, { generationId: NEXT_GENERATION, status: "active", version: 1 }),
  ).toBe(true);
});
