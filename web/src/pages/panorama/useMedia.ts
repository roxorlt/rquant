import { useSyncExternalStore } from "react";

/** Phone layout breakpoint — matches the shell's phone nav (max-width: 760px). */
export const PHONE_QUERY = "(max-width: 760px)";

export function useMedia(query: string): boolean {
  return useSyncExternalStore(
    (cb) => {
      if (typeof window.matchMedia !== "function") return () => {};
      const m = window.matchMedia(query);
      m.addEventListener("change", cb);
      return () => m.removeEventListener("change", cb);
    },
    () => typeof window.matchMedia === "function" && window.matchMedia(query).matches,
    () => false,
  );
}
