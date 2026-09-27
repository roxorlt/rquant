import { ApiError, type Schemas } from "@/api/client";
import { CANVAS_CREATE_JOURNAL_KEY, CanvasCreateSession } from "./canvasCreateSession";

type Command = Schemas["CreateCanvasCommand"];
type Receipt = Schemas["PoolEditorReceipt"];
const HASH = "a".repeat(64);

beforeEach(() => window.sessionStorage.clear());

function makeSession(post: (body: Command) => Promise<Receipt>, storage = window.sessionStorage) {
  let next = 0;
  return new CanvasCreateSession(
    storage,
    post,
    () => `canvas-${++next}`,
    () => "2026-09-27T07:00:00.000Z",
  );
}

it("stores the complete create request before sending and keeps the publication identity", async () => {
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    expect(
      JSON.parse(window.sessionStorage.getItem(CANVAS_CREATE_JOURNAL_KEY) ?? "{}").body,
    ).toEqual(body);
    return {
      command_id: body.command_id,
      status: "succeeded",
      message: "画布已保存，等待发布",
      canvas_name: body.name,
      canvas_record_hash: HASH,
    };
  });
  const session = makeSession(post);
  await session.start("晨盘观察", "观察候选池");
  expect(post).toHaveBeenCalledTimes(1);
  expect(post.mock.calls[0]?.[0]).toMatchObject({
    kind: "create_canvas",
    name: "晨盘观察",
    description: "观察候选池",
  });
  expect(session.snapshot().journal).toMatchObject({ status: "succeeded", recordHash: HASH });
});

it("recovers an uncertain request after reload with the exact original body", async () => {
  const sent: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    sent.push(body);
    if (sent.length === 1) throw new ApiError(503, "连接暂不可用");
    return {
      command_id: body.command_id,
      status: "succeeded",
      message: "画布已保存，等待发布",
      canvas_name: body.name,
      canvas_record_hash: HASH,
    };
  });
  const first = makeSession(post);
  await first.start("晨盘观察", "");
  expect(first.snapshot().journal?.status).toBe("unknown");
  const restored = makeSession(post);
  await restored.advance();
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
  expect(restored.snapshot().journal?.status).toBe("succeeded");
});

it("keeps an ambiguous request for manual lookup and rejects a mismatched success receipt", async () => {
  let attempt = 0;
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    attempt += 1;
    return attempt === 1
      ? { command_id: body.command_id, status: "ambiguous", message: "状态待确认" }
      : {
          command_id: body.command_id,
          status: "succeeded",
          message: "画布已保存",
          canvas_name: "别人的画布",
          canvas_record_hash: HASH,
        };
  });
  const session = makeSession(post);
  await session.start("晨盘观察", "");
  expect(session.snapshot().journal?.status).toBe("ambiguous");
  await session.advance();
  expect(post.mock.calls[1]?.[0]).toEqual(post.mock.calls[0]?.[0]);
  expect(session.snapshot().journal?.status).toBe("unknown");
  expect(session.snapshot().journal?.recordHash).toBeNull();
});

it("uses a fresh identity only after a terminal failure and an edited retry", async () => {
  const sent: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    sent.push(body);
    return sent.length === 1
      ? {
          command_id: body.command_id,
          status: "failed",
          message: "画布名称已被使用，请换一个名称。",
        }
      : {
          command_id: body.command_id,
          status: "succeeded",
          message: "画布已保存",
          canvas_name: body.name,
          canvas_record_hash: HASH,
        };
  });
  const session = makeSession(post);
  await session.start("晨盘观察", "");
  expect(session.snapshot().journal?.status).toBe("failed");
  await session.start("晨盘观察二", "");
  expect(sent.map((body) => body.command_id)).toEqual(["canvas-1", "canvas-2"]);
  expect(session.snapshot().journal?.status).toBe("succeeded");
});

it("never sends if browser storage cannot retain the immutable request", async () => {
  const post = vi.fn<(_body: Command) => Promise<Receipt>>();
  const storage = {
    getItem: () => null,
    setItem: () => {
      throw new Error("storage unavailable");
    },
    removeItem: () => {},
  } as unknown as Storage;
  const session = makeSession(post, storage);
  await session.start("晨盘观察", "");
  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().storageAvailable).toBe(false);
});
