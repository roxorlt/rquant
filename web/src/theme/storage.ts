/**
 * Per-viewer preferences in localStorage (key prefix `rq.`).
 * Storage can be unavailable (private mode, blocked site data, some in-app
 * browsers), so every access is guarded and the app works without it.
 *
 * Keys that follow the account (column visibility, sort orders, query history and
 * other `pref.*` page settings) are also kept on the server (`/api/v1/ui-preferences`,
 * the web store). Before the first render `hydratePreferences()` copies the server
 * values into this browser; every write goes to this browser first and then to the
 * server in the background. If the server is down or the account is read-only,
 * the browser copy is used and nothing is shown. Theme and the collapsed rail stay
 * per device.
 */
import { useCallback, useState } from "react";
import { apiClient } from "@/api/client";

const PREFIX = "rq.";
const SYNCED = /^(cols|sort|pref)\.\S{1,60}$|^query\.history$/;

export function syncedPreference(key: string): boolean {
  return SYNCED.test(key);
}

function readLocal(key: string): string | null {
  try {
    return window.localStorage.getItem(PREFIX + key);
  } catch {
    return null;
  }
}

function writeLocal(key: string, value: string | null): void {
  try {
    if (value === null) {
      window.localStorage.removeItem(PREFIX + key);
    } else {
      window.localStorage.setItem(PREFIX + key, value);
    }
  } catch {
    // Storage unavailable: the preference lasts for this page view only.
  }
}

/** In-memory copy for when localStorage is unavailable. */
const memory = new Map<string, string | null>();

export function readPreference(key: string): string | null {
  const local = readLocal(key);
  if (local !== null) return local;
  return memory.get(key) ?? null;
}

function pushToServer(key: string, value: string | null): void {
  try {
    void apiClient()
      .PUT("/api/v1/ui-preferences", { body: { key, value } })
      .catch(() => undefined);
  } catch {
    // No API (tests without fetch, static files): the browser copy is enough.
  }
}

export function writePreference(key: string, value: string | null): void {
  writeLocal(key, value);
  if (readLocal(key) !== value) memory.set(key, value);
  if (syncedPreference(key)) pushToServer(key, value);
}

/**
 * Copy the server's page settings into this browser. Resolves within `timeoutMs`
 * whatever happens; returns how many keys came from the server (-1 = not reachable).
 */
export async function hydratePreferences(timeoutMs = 1500): Promise<number> {
  const load = (async () => {
    try {
      const { data, response } = await apiClient().GET("/api/v1/ui-preferences");
      if (!response.ok || !data) return -1;
      const prefs = (data as { data?: { prefs?: Record<string, string> } }).data?.prefs ?? {};
      let n = 0;
      for (const [key, value] of Object.entries(prefs)) {
        if (!syncedPreference(key) || typeof value !== "string") continue;
        writeLocal(key, value);
        if (readLocal(key) !== value) memory.set(key, value);
        n += 1;
      }
      return n;
    } catch {
      return -1;
    }
  })();
  const timeout = new Promise<number>((resolve) => setTimeout(() => resolve(-1), timeoutMs));
  return Promise.race([load, timeout]);
}

/** useState that remembers a JSON value under `pref.<key>` (account-wide). */
export function usePreference<T>(
  key: string,
  initial: T,
  valid: (value: unknown) => value is T = (_v: unknown): _v is T => true,
): [T, (next: T) => void] {
  const full = `pref.${key}`;
  const [value, setValue] = useState<T>(() => {
    const raw = readPreference(full);
    if (raw === null) return initial;
    try {
      const parsed: unknown = JSON.parse(raw);
      return valid(parsed) ? parsed : initial;
    } catch {
      return initial;
    }
  });
  const set = useCallback(
    (next: T) => {
      setValue(next);
      writePreference(full, JSON.stringify(next));
    },
    [full],
  );
  return [value, set];
}

/** Type guard for a value from a fixed list of strings. */
export function oneOf<T extends string>(...options: readonly T[]) {
  return (value: unknown): value is T =>
    typeof value === "string" && (options as readonly string[]).includes(value);
}

/** Type guard for a list of strings. */
export function stringList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}
