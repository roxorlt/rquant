import { vi } from "vitest";
import type { BuiltinControlRequest } from "@/api/taskControls";
import {
  MonitorControlMemory,
  MonitorControlPersistenceError,
  monitorRecoveryKey,
} from "./monitorControlRecovery";

const first: BuiltinControlRequest = {
  kind: "set_monitor_builtin_enabled",
  command_id: "d1f85ad0-d5cd-4d26-b061-1a2374261f54",
  requested_at: "2026-09-24T02:00:00Z",
  generation_id: "a".repeat(64),
  builtin_id: "pool2_levels",
  expected_revision: 2,
  enabled: false,
};

beforeEach(() => window.sessionStorage.clear());

describe("原监控请求恢复", () => {
  it("keeps only exact bodies durable and hides responses during unknown identity and remount", () => {
    const memory = new MonitorControlMemory(window.sessionStorage);
    memory.confirmActor("alice", first.generation_id);
    memory.put("alice", first.builtin_id, {
      body: first,
      result: null,
      message: "私有响应不能持久化。",
    });
    const raw = window.sessionStorage.getItem(monitorRecoveryKey);
    expect(raw).toContain(first.command_id);
    expect(raw).not.toContain("私有响应");
    memory.suspend();
    expect(memory.entries("alice")).toEqual([]);
    expect(window.sessionStorage.getItem(monitorRecoveryKey)).toEqual(raw);
    const mounted = new MonitorControlMemory(window.sessionStorage);
    mounted.suspend();
    expect(mounted.entries("alice")).toEqual([]);
    mounted.confirmActor("alice", first.generation_id);
    expect(mounted.get("alice", first.builtin_id)).toMatchObject({ body: first, result: null });
    expect(mounted.get("alice", first.builtin_id)?.message).not.toContain("私有响应");
  });

  it("clears confirmed logout, another actor, and explicit permission revocation", () => {
    const memory = new MonitorControlMemory(window.sessionStorage);
    const save = () => {
      memory.confirmActor("alice", first.generation_id);
      memory.put("alice", first.builtin_id, { body: first, result: null, message: "待核验" });
    };
    save();
    memory.confirmActor("bob", first.generation_id);
    expect(memory.entries("alice")).toEqual([]);
    expect(memory.entries("bob")).toEqual([]);
    expect(window.sessionStorage.getItem(monitorRecoveryKey)).toBeNull();
    save();
    memory.confirmActor(null, null);
    expect(window.sessionStorage.getItem(monitorRecoveryKey)).toBeNull();
    save();
    memory.clear("alice");
    expect(memory.entries("alice")).toEqual([]);
    expect(window.sessionStorage.getItem(monitorRecoveryKey)).toBeNull();
  });

  it("changes visible generation without rewriting a possibly submitted body", () => {
    const memory = new MonitorControlMemory(window.sessionStorage);
    memory.confirmActor("alice", first.generation_id);
    memory.put("alice", first.builtin_id, { body: first, result: null, message: "旧响应" });
    memory.confirmActor("alice", "b".repeat(64));
    expect(memory.get("alice", first.builtin_id)?.body).toEqual(first);
    expect(memory.get("alice", first.builtin_id)?.message).not.toBe("旧响应");
    expect(memory.isCurrent("alice", first.generation_id)).toBe(false);
  });

  it("refuses a new body before POST when saving fails and retains a previously saved UUID", () => {
    const storage = {
      getItem: (key: string) => window.sessionStorage.getItem(key),
      removeItem: (key: string) => window.sessionStorage.removeItem(key),
      setItem: vi.fn((key: string, value: string) => window.sessionStorage.setItem(key, value)),
    };
    const memory = new MonitorControlMemory(storage);
    memory.confirmActor("alice", first.generation_id);
    const old = { body: first, result: null, message: "待核验" };
    memory.put("alice", first.builtin_id, old);
    storage.setItem.mockImplementation(() => {
      throw new Error("synthetic quota exhausted");
    });
    const next = { ...first, command_id: "161b7c3e-40e8-4e09-a4c2-349ddc273113" };
    expect(() => memory.put("alice", first.builtin_id, { ...old, body: next })).toThrow(
      MonitorControlPersistenceError,
    );
    expect(memory.storageAvailable).toBe(false);
    expect(memory.get("alice", first.builtin_id)?.body).toEqual(first);
    expect(window.sessionStorage.getItem(monitorRecoveryKey)).not.toContain(next.command_id);
    // Lookup still uses the already durable body; it needs no new write or UUID.
    expect(() =>
      memory.put("alice", first.builtin_id, { ...old, message: "已核验原请求" }),
    ).not.toThrow();
  });

  it("does not notify the shared leaf operation epoch when storing a request", () => {
    const memory = new MonitorControlMemory(window.sessionStorage);
    memory.confirmActor("alice", first.generation_id);
    const originalEpoch = memory.snapshot();
    const changed = vi.fn();
    memory.subscribeRecords(changed);
    memory.put("alice", first.builtin_id, { body: first, result: null, message: "待核验" });
    expect(changed).toHaveBeenCalledOnce();
    expect(memory.snapshot()).toBe(originalEpoch);
  });

  it("refuses malformed, oversized, duplicate, and cross-kind stored bodies without exposing them", () => {
    const invalid = [
      {
        version: 1,
        actor: "alice",
        generation: first.generation_id,
        entries: [{ key: "surge", body: first }],
      },
      {
        version: 1,
        actor: "alice",
        generation: first.generation_id,
        entries: [
          { key: first.builtin_id, body: first },
          { key: first.builtin_id, body: first },
        ],
      },
      {
        version: 1,
        actor: "alice",
        generation: first.generation_id,
        entries: [{ key: first.builtin_id, body: { ...first, owner_id: "bob" } }],
      },
    ];
    for (const value of invalid) {
      window.sessionStorage.setItem(monitorRecoveryKey, JSON.stringify(value));
      const memory = new MonitorControlMemory(window.sessionStorage);
      memory.confirmActor("alice", first.generation_id);
      expect(memory.storageAvailable).toBe(false);
      expect(memory.entries("alice")).toEqual([]);
    }
    window.sessionStorage.setItem(monitorRecoveryKey, " ".repeat(32 * 1024 + 1));
    const oversized = new MonitorControlMemory(window.sessionStorage);
    oversized.confirmActor("alice", first.generation_id);
    expect(oversized.storageAvailable).toBe(false);
    expect(oversized.entries("alice")).toEqual([]);
  });
});
