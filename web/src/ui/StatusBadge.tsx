import type { components } from "@/api/schema";
import { Tip } from "./Tip";

export type UserState = components["schemas"]["UserState"];

function Glyph({ state }: { state: UserState }) {
  switch (state) {
    case "ok":
      return (
        <>
          <circle cx="6" cy="6" r="5.25" fill="currentColor" />
          <path
            d="M3.6 6.1 5.2 7.7 8.4 4.5"
            fill="none"
            stroke="var(--surface)"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </>
      );
    case "warn":
      return (
        <>
          <path d="M6 .9 11.4 10.6H.6Z" fill="currentColor" />
          <path d="M6 4.3v3" stroke="var(--surface)" strokeWidth="1.4" strokeLinecap="round" />
          <circle cx="6" cy="8.9" r=".75" fill="var(--surface)" />
        </>
      );
    case "crit":
      return (
        <>
          <circle cx="6" cy="6" r="5.25" fill="currentColor" />
          <path
            d="m4.1 4.1 3.8 3.8m0-3.8-3.8 3.8"
            stroke="var(--surface)"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </>
      );
    case "waiting":
      return (
        <>
          <circle cx="6" cy="6" r="4.8" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <path
            d="M6 3.4V6l1.8 1.1"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinecap="round"
          />
        </>
      );
    default:
      return (
        <>
          <circle cx="6" cy="6" r="4.8" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <path d="M3.8 6h4.4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
        </>
      );
  }
}

function Icon({ state }: { state: UserState }) {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true" focusable="false">
      <Glyph state={state} />
    </svg>
  );
}

export interface StatusBadgeProps {
  state: UserState;
  label: string;
  /** One plain sentence, shown on hover / tap. */
  reason?: string | null;
}

/** Colour + icon + a short word, with the reason in a tooltip. */
export function StatusBadge({ state, label, reason }: StatusBadgeProps) {
  const badge = (
    <span className="status" data-state={state}>
      <Icon state={state} />
      <span>{label}</span>
    </span>
  );
  return reason ? <Tip content={reason}>{badge}</Tip> : badge;
}
