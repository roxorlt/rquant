import { ApiError } from "@/api/client";
import { type ScreenPoolSaveCommand, ScreenPoolSaveSession } from "./screenPoolSaveSession";

const command: ScreenPoolSaveCommand = {
  kind: "save_user_pool_v3",
  command_id: "screen-save-original",
  requested_at: "2026-09-28T08:00:00Z",
  base_name: "每日观察",
  display_name: "每日观察",
  description: "",
  depends_on: null,
  delay_days: 0,
  rule_calls: [{ name: "not_st", args: {} }],
  include_columns: [],
  expected_version: null,
  ranking: null,
};

beforeEach(() => window.sessionStorage.clear());

it.each([409, 422])("保存失联后收到 HTTP %i 仍以原命令继续核对", async (status) => {
  const seen: ScreenPoolSaveCommand[] = [];
  const post = async (body: ScreenPoolSaveCommand) => {
    seen.push(body);
    if (seen.length === 1) throw new ApiError(503, "回包丢失");
    if (seen.length === 2) throw new ApiError(status, "暂时无法确认");
    return {
      command_id: body.command_id,
      status: "succeeded" as const,
      message: "池子已保存",
      pool_version: "b".repeat(64),
    };
  };
  const first = new ScreenPoolSaveSession("viewer", window.sessionStorage, post);
  await first.start(command);
  expect(first.snapshot().journal?.status).toBe("unknown");
  expect(first.clear()).toBe(false);

  const resumed = new ScreenPoolSaveSession("viewer", window.sessionStorage, post);
  await resumed.advance();
  expect(resumed.snapshot().journal?.status).toBe("unknown");
  expect(resumed.snapshot().journal?.body).toEqual(command);
  expect(resumed.clear()).toBe(false);

  const reloaded = new ScreenPoolSaveSession("viewer", window.sessionStorage, post);
  await reloaded.advance();
  expect(seen).toEqual([command, command, command]);
  expect(reloaded.snapshot().journal?.status).toBe("succeeded");
  expect(reloaded.snapshot().journal?.version).toBe("b".repeat(64));
});
