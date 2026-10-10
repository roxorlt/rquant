import type { TaskControlRequest, TaskControlResult } from "@/api/taskControls";

export interface TaskPending {
  body: TaskControlRequest;
  result: TaskControlResult | null;
  message: string;
  refused?: boolean;
  unitName?: string;
}

export function settledTask(pending: TaskPending | null): boolean {
  return (
    pending !== null &&
    (pending.refused === true ||
      (pending.result !== null &&
        ["succeeded", "failed", "rejected"].includes(pending.result.status)))
  );
}

export function taskRequestError(error: unknown): "revoked" | "refused" | "unknown" {
  if (error !== null && typeof error === "object" && "status" in error) {
    if (error.status === 401 || error.status === 403) return "revoked";
    if (error.status === 422 || error.status === 409) return "refused";
  }
  return "unknown";
}

export class TaskControlMemory {
  private viewer: string | null = null;
  private records = new Map<string, TaskPending>();
  private epoch = 0;
  private listeners = new Set<() => void>();
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  snapshot = (): number => this.epoch;

  activate(viewer: string | null): void {
    if (viewer !== this.viewer) {
      this.viewer = viewer;
      this.records.clear();
      this.epoch += 1;
    }
  }

  clear(viewer: string): void {
    if (viewer !== this.viewer) return;
    this.records.clear();
    this.epoch += 1;
    for (const listener of this.listeners) listener();
  }

  entries(viewer: string): [string, TaskPending][] {
    return viewer === this.viewer ? Array.from(this.records.entries()) : [];
  }

  get(viewer: string, key: string): TaskPending | null {
    return viewer === this.viewer ? (this.records.get(key) ?? null) : null;
  }

  put(viewer: string, key: string, record: TaskPending | null): void {
    if (viewer !== this.viewer) return;
    if (record === null) this.records.delete(key);
    else if (this.records.has(key) || this.records.size < 33)
      this.records.set(key, structuredClone(record));
  }
}

export function restoreTaskControlFocus(
  trigger: HTMLButtonElement | null,
  region: HTMLDivElement | null,
): void {
  window.requestAnimationFrame(() => {
    if (trigger?.isConnected && !trigger.disabled) trigger.focus();
    else {
      const next =
        Array.from(region?.querySelectorAll<HTMLButtonElement>("button:not(:disabled)") ?? []).find(
          (button) =>
            (button.getAttribute("aria-label") ?? button.textContent ?? "").startsWith("核验"),
        ) ?? document.querySelector<HTMLButtonElement>('[aria-label="任务总览刷新"] button');
      next?.focus();
    }
  });
}
