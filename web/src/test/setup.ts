import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";
import { dismissToast } from "@/ui/Toast";

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
