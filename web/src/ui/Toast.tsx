import { type ReactNode, useEffect, useRef, useState, useSyncExternalStore } from "react";

export interface ToastAction {
  label: string;
  onClick: () => void;
}

export interface ToastNotice {
  text: string;
  action?: ToastAction;
  duration?: number;
  /** Element to focus after Esc dismisses this toast. */
  returnFocus?: () => HTMLElement | null;
}

interface Current {
  id: number;
  text: string;
  action?: ToastAction;
  duration: number;
  returnFocus?: () => HTMLElement | null;
  focusAction: boolean;
}

let current: Current | null = null;
let timer: number | null = null;
let remaining = 0;
let started = 0;
let paused = false;
let seq = 0;
const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

function clearTimer(): void {
  if (timer !== null) window.clearTimeout(timer);
  timer = null;
}

function arm(ms: number): void {
  clearTimer();
  remaining = ms;
  started = Date.now();
  paused = false;
  timer = window.setTimeout(() => {
    current = null;
    timer = null;
    emit();
  }, ms);
}

export function showToast(input: string | ToastNotice): void {
  const notice = typeof input === "string" ? { text: input } : input;
  const duration = notice.duration ?? (notice.action?.label === "撤销" ? 6000 : 4000);
  current = {
    id: ++seq,
    text: notice.text,
    action: notice.action,
    duration,
    returnFocus: notice.returnFocus,
    focusAction: notice.action?.label === "撤销",
  };
  arm(duration);
  emit();
}

export function dismissToast(): void {
  clearTimer();
  current = null;
  paused = false;
  emit();
}

export function pauseToast(): void {
  if (!current || paused || timer === null) return;
  clearTimer();
  remaining = Math.max(0, remaining - (Date.now() - started));
  paused = true;
}

export function resumeToast(): void {
  if (!current || !paused) return;
  arm(remaining);
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useToast(): (notice: string | ToastNotice) => void {
  return showToast;
}

/** One bottom toast. Hover or focus pauses the timer; a new toast replaces this one. */
export function ToastHost(): ReactNode {
  const snap = useSyncExternalStore(
    subscribe,
    () => current,
    () => null,
  );
  const [, setTick] = useState(0);
  const skipPause = useRef(false);
  useEffect(() => subscribe(() => setTick((n) => n + 1)), []);
  useEffect(() => {
    if (!snap) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      if (document.querySelector('[role="dialog"]')) return;
      event.preventDefault();
      const back = snap.returnFocus?.() ?? null;
      dismissToast();
      back?.focus();
    };
    window.addEventListener("keydown", onKey);
    if (snap.focusAction) {
      // Moving focus for the keyboard does not count as the user pausing the timer.
      skipPause.current = true;
      document.querySelector<HTMLElement>(".toast .act")?.focus();
      skipPause.current = false;
    }
    return () => window.removeEventListener("keydown", onKey);
  }, [snap]);
  if (!snap) return null;
  return (
    <div
      key={snap.id}
      className="toast"
      role="status"
      onMouseEnter={pauseToast}
      onMouseLeave={resumeToast}
      onFocus={() => {
        if (skipPause.current) return;
        pauseToast();
      }}
      onBlur={resumeToast}
    >
      <span>{snap.text}</span>
      {snap.action ? (
        <button type="button" className="act" onClick={snap.action.onClick}>
          {snap.action.label}
        </button>
      ) : null}
    </div>
  );
}
