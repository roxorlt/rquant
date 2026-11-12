import "@testing-library/jest-dom/vitest";
import { cleanup, configure } from "@testing-library/react";
import { afterEach } from "vitest";
import { dismissToast } from "@/ui/Toast";

// 2026-11-12: default asyncUtilTimeout (1s) is too tight on shared CI runners;
// the poolScope tests occasionally timed out without a real assertion failure.
configure({ asyncUtilTimeout: 5000 });

function mediaQueryList(query: string): MediaQueryList {
  return {
    matches: false,
    media: query,
    onchange: null,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    addListener: () => undefined,
    removeListener: () => undefined,
    dispatchEvent: () => false,
  };
}

Object.defineProperty(window, "matchMedia", { writable: true, value: mediaQueryList });
Object.defineProperty(window, "scrollTo", { writable: true, value: () => undefined });

class NoopResizeObserver implements ResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}
globalThis.ResizeObserver ??= NoopResizeObserver;

afterEach(() => {
  cleanup();
  dismissToast();
  window.localStorage.clear();
  window.sessionStorage.clear();
  document.documentElement.removeAttribute("data-theme");
});
