/**
 * Per-viewer preferences (theme, collapsed rail) in localStorage.
 * Storage can be unavailable (private mode, blocked site data, some in-app
 * browsers), so every access is guarded and the app works without it.
 */

const PREFIX = "rq.";

export function readPreference(key: string): string | null {
  try {
    return window.localStorage.getItem(PREFIX + key);
  } catch {
    return null;
  }
}

export function writePreference(key: string, value: string | null): void {
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
