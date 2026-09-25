import type { ReactNode } from "react";

export type PillKind = "ok" | "warn" | "crit" | "idle" | "acc" | "plain";

export function Pill({ kind = "plain", children }: { kind?: PillKind; children: ReactNode }) {
  return (
    <span className={kind === "plain" ? "pill" : `pill ${kind}`}>
      <span className="d" aria-hidden="true" />
      {children}
    </span>
  );
}
