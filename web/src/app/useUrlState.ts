import { useCallback } from "react";
import { useSearchParams } from "react-router";

/**
 * A string selection kept in the URL query (inside the hash route) so a selected
 * backtest / factor / pool can be shared, bookmarked and restored with Back.
 */
export function useUrlState(key: string): [string | null, (value: string | null) => void] {
  const [params, setParams] = useSearchParams();
  const value = params.get(key);
  const set = useCallback(
    (next: string | null) =>
      setParams(
        (prev) => {
          const p = new URLSearchParams(prev);
          if (next === null || next === "") p.delete(key);
          else p.set(key, next);
          return p;
        },
        { replace: false },
      ),
    [key, setParams],
  );
  return [value, set];
}
