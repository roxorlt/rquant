import { useEffect, useRef } from "react";

/**
 * Finger drag scrubs a chart crosshair.
 *
 * Native touch listeners with { passive: false } (React's are passive): once a touch has
 * moved past a small slop and is more horizontal than vertical, every touchmove is
 * preventDefault()-ed and fed to `onScrub`, so the page does not scroll or cancel the
 * gesture (iOS Safari ignores `touch-action` on SVG and cancels pointer streams as soon
 * as it guesses a scroll). A mostly vertical drag is left alone and scrolls the page.
 * A press held still for `holdMs` also locks into scrubbing.
 */
export function useTouchScrub(
  el: Element | null,
  onScrub: (clientX: number, clientY: number) => void,
  { slop = 6, holdMs = 280 }: { slop?: number; holdMs?: number } = {},
) {
  const cb = useRef(onScrub);
  cb.current = onScrub;
  useEffect(() => {
    if (!el) return;
    let start: { x: number; y: number } | null = null;
    let mode: "pending" | "scrub" | "scroll" = "pending";
    let timer: ReturnType<typeof setTimeout> | undefined;
    const clear = () => {
      if (timer) clearTimeout(timer);
      timer = undefined;
    };
    const onStart = (e: Event) => {
      const t = (e as TouchEvent).touches;
      clear();
      if (t.length !== 1) {
        start = null;
        mode = "scroll";
        return;
      }
      const p = t[0] as Touch;
      start = { x: p.clientX, y: p.clientY };
      mode = "pending";
      cb.current(p.clientX, p.clientY);
      timer = setTimeout(() => {
        if (mode === "pending") mode = "scrub";
      }, holdMs);
    };
    const onMove = (e: Event) => {
      const te = e as TouchEvent;
      if (!start || mode === "scroll" || te.touches.length !== 1) return;
      const p = te.touches[0] as Touch;
      if (mode === "pending") {
        const dx = Math.abs(p.clientX - start.x);
        const dy = Math.abs(p.clientY - start.y);
        if (dx < slop && dy < slop) {
          if (te.cancelable) te.preventDefault();
          return;
        }
        clear();
        mode = dx >= dy ? "scrub" : "scroll";
        if (mode === "scroll") return;
      }
      if (te.cancelable) te.preventDefault();
      cb.current(p.clientX, p.clientY);
    };
    const onEnd = () => {
      clear();
      start = null;
      mode = "pending";
    };
    const opts: AddEventListenerOptions = { passive: false };
    el.addEventListener("touchstart", onStart, opts);
    el.addEventListener("touchmove", onMove, opts);
    el.addEventListener("touchend", onEnd);
    el.addEventListener("touchcancel", onEnd);
    return () => {
      clear();
      el.removeEventListener("touchstart", onStart, opts);
      el.removeEventListener("touchmove", onMove, opts);
      el.removeEventListener("touchend", onEnd);
      el.removeEventListener("touchcancel", onEnd);
    };
  }, [el, slop, holdMs]);
}
