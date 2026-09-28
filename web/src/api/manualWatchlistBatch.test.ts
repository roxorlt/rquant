import type { Schemas } from "./client";
import {
  type BatchCandidate,
  batchCounts,
  freezeScreenWatchlistPage,
  ManualWatchlistBatchSession,
} from "./manualWatchlistBatch";
import {
  MANUAL_WATCHLIST_JOURNAL_KEY,
  type ManualWatchlistCommandBody,
  ManualWatchlistCommandSession,
  type TrustedWatchlistBasis,
} from "./manualWatchlistCommand";

const OLD = "a".repeat(64);
const NEW = "b".repeat(64);
const SOURCE = "c".repeat(64);
const CODES = ["600001.SH", "600002.SH", "600003.SH", "600004.SH", "600005.SH"] as const;

function candidate(codes: readonly string[] = CODES, revision = "first"): BatchCandidate {
  const result = freezeScreenWatchlistPage(
    {
      status: "ready",
      trade_date: "2026-09-24",
      base_count: 80,
      total: 43,
      unknown_count: 0,
      steps: [],
      rows: codes.map((ts_code) => ({ ts_code, name: ts_code, close: 11, pct_chg: 1 })),
      next_cursor: "next-page",
      source: { identity: SOURCE, updated_at: "2026-09-24T07:30:00Z" },
    },
    0,
    revision,
  );
  if (!result) throw new Error("invalid test candidate");
  return result;
}

function exclusive(_name: string, task: () => Promise<void>): Promise<void> {
  return task();
}

function harness(options: {
  viewer?: string;
  read: (generation: string, code: string) => TrustedWatchlistBasis | null;
  post: (body: ManualWatchlistCommandBody) => Promise<Schemas["ManualWatchlistCommandReceipt"]>;
  current?: () => boolean;
  viewerNow?: () => string | null;
  withLock?: (name: string, task: () => Promise<void>) => Promise<void>;
}) {
  let nextId = 0;
  const viewer = options.viewer ?? "tester";
  const viewerMatches = () => (options.viewerNow?.() ?? viewer) === viewer;
  return new ManualWatchlistBatchSession({
    viewer,
    storage: window.localStorage,
    readBasis: async (_viewer, generation, code) => {
      const basis = options.read(generation, code);
      return basis
        ? { state: "ready" as const, basis }
        : { state: "unavailable" as const, basis: null };
    },
    single: (viewer, code) =>
      new ManualWatchlistCommandSession({
        viewer,
        tsCode: code,
        storage: window.localStorage,
        post: options.post,
        verifyBasis: async () => "ready",
        verifyOwner: async () => viewerMatches(),
        nextId: () => `web-batch-${++nextId}`,
        now: () => "2026-09-28T07:00:00.000Z",
        withLock: options.withLock ?? exclusive,
      }),
    withLock: options.withLock ?? exclusive,
    isCurrent: () => options.current?.() ?? true,
    isTrustedViewer: viewerMatches,
    verifyViewer: async () => viewerMatches(),
  });
}

function counts(batch: ManualWatchlistBatchSession) {
  const manifest = batch.snapshot().manifest;
  if (manifest === null) throw new Error("missing batch manifest");
  return batchCounts(manifest);
}

beforeEach(() => window.localStorage.clear());

it("仅冻结本页有序去重的至多 20 只，条件、页码和来源变化产生新候选", () => {
  const frozen = candidate([CODES[0], CODES[1], CODES[0]]);
  expect(frozen.codes).toEqual([CODES[0], CODES[1]]);
  expect(frozen.tradeDate).toBe("2026-09-24");
  expect(frozen.pageIndex).toBe(0);
  expect(candidate(CODES, "changed").key).not.toBe(candidate(CODES).key);
  expect(
    candidate(Array.from({ length: 20 }, (_, n) => `${String(n).padStart(6, "0")}.SH`)).codes,
  ).toHaveLength(20);
  expect(
    freezeScreenWatchlistPage(
      {
        status: "ready",
        trade_date: "2026-09-24",
        base_count: 80,
        total: 43,
        unknown_count: 0,
        steps: [],
        rows: Array.from({ length: 21 }, (_, n) => ({
          ts_code: `${String(n).padStart(6, "0")}.SH`,
          name: "样本",
          close: 11,
          pct_chg: 1,
        })),
        next_cursor: null,
        source: { identity: SOURCE, updated_at: "2026-09-24T07:30:00Z" },
      },
      0,
      "first",
    ),
  ).toBeNull();
});

it("逐只核对并统计已在名单、墓碑重加、容量、冲突和不可用；仅后续匹配新代才计已加入", async () => {
  const sent: ManualWatchlistCommandBody[] = [];
  const current = new Map<string, TrustedWatchlistBasis | null>([
    [CODES[0], { status: "active", version: 3 }],
    [CODES[1], { status: "deleted", version: 4 }],
    [CODES[2], { status: "absent", version: null }],
    [CODES[3], { status: "expired", version: 2 }],
    [CODES[4], null],
  ]);
  const batch = harness({
    read: (generation, code) =>
      generation === NEW && code === CODES[1]
        ? { status: "active", version: 5 }
        : (current.get(code) ?? null),
    post: async (body) => {
      sent.push(body);
      const status =
        body.ts_code === CODES[1]
          ? "published"
          : body.ts_code === CODES[2]
            ? "capacity"
            : "conflict";
      return {
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: "add",
        status,
        version: status === "published" ? 5 : null,
        message: "已核对",
      };
    },
  });
  await batch.begin(candidate(), OLD);
  expect(sent.map((body) => [body.ts_code, body.expected_version, body.source])).toEqual([
    [CODES[1], 4, "screen_result"],
    [CODES[2], null, "screen_result"],
    [CODES[3], 2, "screen_result"],
  ]);
  expect(counts(batch)).toMatchObject({
    already: 1,
    syncing: 1,
    capacity: 1,
    conflict: 1,
    unavailable: 1,
    added: 0,
  });
  await batch.reconcile(OLD);
  expect(counts(batch).syncing).toBe(1);
  await batch.reconcile(NEW);
  expect(counts(batch)).toMatchObject({
    already: 1,
    added: 1,
    capacity: 1,
    conflict: 1,
    unavailable: 1,
    syncing: 0,
  });
  expect(Object.values(counts(batch)).reduce((a, b) => a + b, 0)).toBe(5);
});

it("丢回执后刷新只续查持久原 ID，不新增第二条副作用", async () => {
  const seen: ManualWatchlistCommandBody[] = [];
  const post = async (
    body: ManualWatchlistCommandBody,
  ): Promise<Schemas["ManualWatchlistCommandReceipt"]> => {
    seen.push(body);
    if (seen.length === 1) throw new Error("reply lost");
    return {
      command_id: body.command_id,
      ts_code: body.ts_code,
      action: "add",
      status: "saved_syncing",
      version: 1,
      message: "已保存，正在同步",
    };
  };
  const options = { read: () => ({ status: "absent" as const, version: null }), post };
  const first = harness(options);
  await first.begin(candidate([CODES[0]]), OLD);
  expect(first.snapshot().manifest?.items[0]?.status).toBe("uncertain");
  const reopened = harness(options);
  await reopened.reconcile(OLD);
  expect(seen).toHaveLength(2);
  expect(seen[1]).toEqual(seen[0]);
  expect(reopened.snapshot().manifest?.items[0]?.status).toBe("syncing");
  expect(reopened.snapshot().manifest?.items[0]?.commandId).toBe(seen[0]?.command_id);
});

it("来源或条件变化时停止后续股票，不自动执行尚未提交的候选", async () => {
  let current = true;
  const sent: ManualWatchlistCommandBody[] = [];
  const batch = harness({
    current: () => current,
    read: () => ({ status: "absent", version: null }),
    post: async (body) => {
      sent.push(body);
      current = false;
      return {
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: "add",
        status: "saved_syncing",
        version: 1,
        message: "已保存，正在同步",
      };
    },
  });
  await batch.begin(candidate(CODES.slice(0, 2)), OLD);
  expect(sent).toHaveLength(1);
  expect(batch.snapshot().manifest?.items.map((item) => item.status)).toEqual([
    "syncing",
    "queued",
  ]);
  await batch.reconcile(OLD);
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
  expect(batch.snapshot().manifest?.items[1]?.status).toBe("queued");
});

it("逐只核对期间结果变更时不提交尚未开始的股票", async () => {
  let current = true;
  const post = vi.fn(
    async (
      body: ManualWatchlistCommandBody,
    ): Promise<Schemas["ManualWatchlistCommandReceipt"]> => ({
      command_id: body.command_id,
      ts_code: body.ts_code,
      action: "add",
      status: "saved_syncing",
      version: 1,
      message: "已保存，正在同步",
    }),
  );
  const batch = harness({
    current: () => current,
    read: () => {
      current = false;
      return { status: "absent", version: null };
    },
    post,
  });
  await batch.begin(candidate([CODES[0]]), OLD);
  expect(post).not.toHaveBeenCalled();
  expect(batch.snapshot().manifest?.items[0]?.status).toBe("queued");
});

it("跨标签批量锁串行，上次仍待同步时不创建第二批命令", async () => {
  const tails = new Map<string, Promise<void>>();
  const withLock = async (name: string, task: () => Promise<void>) => {
    const prior = tails.get(name) ?? Promise.resolve();
    let release!: () => void;
    tails.set(
      name,
      new Promise<void>((resolve) => {
        release = resolve;
      }),
    );
    await prior;
    try {
      await task();
    } finally {
      release();
    }
  };
  const sent: ManualWatchlistCommandBody[] = [];
  const options = {
    withLock,
    read: () => ({ status: "absent" as const, version: null }),
    post: async (
      body: ManualWatchlistCommandBody,
    ): Promise<Schemas["ManualWatchlistCommandReceipt"]> => {
      sent.push(body);
      return {
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: "add",
        status: "saved_syncing",
        version: 1,
        message: "已保存，正在同步",
      };
    },
  };
  await Promise.all([
    harness(options).begin(candidate([CODES[0]]), OLD),
    harness(options).begin(candidate([CODES[0]]), OLD),
  ]);
  expect(sent).toHaveLength(1);
});

it("两只原命令续查中切换用户，停止第二只；切回原用户仍用原 ID 续查", async () => {
  let viewer: string | null = "alice";
  const viewerNow = () => viewer;
  const original: ManualWatchlistCommandBody[] = [];
  const pending = (body: ManualWatchlistCommandBody): Schemas["ManualWatchlistCommandReceipt"] => ({
    command_id: body.command_id,
    ts_code: body.ts_code,
    action: "add",
    status: "pending",
    version: null,
    message: "正在处理",
  });
  await harness({
    viewer: "alice",
    viewerNow,
    read: () => ({ status: "absent", version: null }),
    post: async (body) => {
      original.push(body);
      return pending(body);
    },
  }).begin(candidate(CODES.slice(0, 2)), OLD);
  expect(original).toHaveLength(2);

  let release!: () => void;
  const delayed = new Promise<void>((resolve) => {
    release = resolve;
  });
  let started!: () => void;
  const firstStarted = new Promise<void>((resolve) => {
    started = resolve;
  });
  const resumed: ManualWatchlistCommandBody[] = [];
  const batch = harness({
    viewer: "alice",
    viewerNow,
    current: () => false,
    read: () => ({ status: "absent", version: null }),
    post: async (body) => {
      resumed.push(body);
      if (body.ts_code === CODES[0] && resumed.length === 1) {
        started();
        await delayed;
      }
      const status = body.ts_code === CODES[1] && viewer === "bob" ? "conflict" : "saved_syncing";
      return {
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: "add",
        status,
        version: status === "saved_syncing" ? 1 : null,
        message: status === "conflict" ? "冲突" : "已保存，正在同步",
      };
    },
  });
  const running = batch.reconcile(OLD);
  await firstStarted;
  viewer = "bob";
  release();
  await running;
  expect(resumed.map((body) => body.ts_code)).toEqual([CODES[0]]);
  const aliceSecond = window.localStorage.getItem(
    `rquant.manual-watchlist-command.v1:alice:${CODES[1]}`,
  );
  expect(aliceSecond).toContain('"status":"pending"');
  expect(batch.snapshot().manifest?.items[1]?.status).toBe("processing");

  viewer = "alice";
  await batch.reconcile(OLD);
  expect(resumed.map((body) => body.ts_code)).toEqual([CODES[0], CODES[0], CODES[1]]);
  expect(resumed[2]).toEqual(original[1]);
  expect(batch.snapshot().manifest?.items[1]?.status).toBe("syncing");
});

it("等待单股互斥锁时切换用户，原命令不在新身份下发送", async () => {
  let viewer: string | null = "alice";
  const viewerNow = () => viewer;
  const original: ManualWatchlistCommandBody[] = [];
  await harness({
    viewer: "alice",
    viewerNow,
    read: () => ({ status: "absent", version: null }),
    post: async (body) => {
      original.push(body);
      return {
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: "add",
        status: "pending",
        version: null,
        message: "正在处理",
      };
    },
  }).begin(candidate([CODES[0]]), OLD);
  let release!: () => void;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let waiting!: () => void;
  const atLock = new Promise<void>((resolve) => {
    waiting = resolve;
  });
  const sent: ManualWatchlistCommandBody[] = [];
  const batch = harness({
    viewer: "alice",
    viewerNow,
    read: () => ({ status: "absent", version: null }),
    withLock: async (name, task) => {
      if (name.startsWith(MANUAL_WATCHLIST_JOURNAL_KEY)) {
        waiting();
        await held;
      }
      await task();
    },
    post: async (body) => {
      sent.push(body);
      return {
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: "add",
        status: "saved_syncing",
        version: 1,
        message: "已保存，正在同步",
      };
    },
  });
  const running = batch.reconcile(OLD);
  await atLock;
  viewer = "bob";
  release();
  await running;
  expect(sent).toHaveLength(0);
  expect(
    window.localStorage.getItem(`${MANUAL_WATCHLIST_JOURNAL_KEY}:alice:${CODES[0]}`),
  ).toContain('"status":"pending"');
  viewer = "alice";
  await batch.reconcile(OLD);
  expect(sent).toEqual(original);
});
