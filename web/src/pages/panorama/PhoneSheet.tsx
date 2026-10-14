import { type ReactNode, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

/**
 * Native-feeling iPhone bottom sheet: slides up, max 75% of the screen, its own scroll
 * area, drag the grabber/header down (or tap the backdrop / ✕) to dismiss. The page
 * behind stays put (scroll locked). Back navigation is handled by the caller via the URL.
 */
export function PhoneSheet({
  open,
  onClose,
  label,
  header,
  children,
}: {
  open: boolean;
  onClose: () => void;
  label: string;
  header: ReactNode;
  children: ReactNode;
}) {
  const [shown, setShown] = useState(false); // drives the slide-in transition
  const [mounted, setMounted] = useState(open);
  const [drag, setDrag] = useState(0);
  const start = useRef<{ y: number; t: number } | null>(null);
  const panel = useRef<HTMLDivElement>(null);
  const body = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) {
      setMounted(true);
      const id = requestAnimationFrame(() => requestAnimationFrame(() => setShown(true)));
      return () => cancelAnimationFrame(id);
    }
    setShown(false);
    const t = window.setTimeout(() => setMounted(false), 260);
    return () => window.clearTimeout(t);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const html = document.documentElement;
    const prev = html.style.overflow;
    html.style.overflow = "hidden";
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    panel.current?.focus({ preventScroll: true });
    return () => {
      html.style.overflow = prev;
      window.removeEventListener("keydown", onKey);
    };
  }, [open, onClose]);

  if (!mounted) return null;

  const onDown = (e: React.PointerEvent) => {
    if ((e.target as HTMLElement).closest("button")) return;
    start.current = { y: e.clientY, t: performance.now() };
  };
  const onMove = (e: React.PointerEvent) => {
    if (!start.current) return;
    const dy = e.clientY - start.current.y;
    if (dy > 0) setDrag(dy);
    else if (drag) setDrag(0);
  };
  const onUp = (e: React.PointerEvent) => {
    if (!start.current) return;
    const dy = e.clientY - start.current.y;
    const v = dy / Math.max(1, performance.now() - start.current.t);
    start.current = null;
    setDrag(0);
    if (dy > 110 || (dy > 30 && v > 0.6)) onClose();
  };
  const handlers = {
    onPointerDown: onDown,
    onPointerMove: onMove,
    onPointerUp: onUp,
    onPointerCancel: () => {
      start.current = null;
      setDrag(0);
    },
  };

  return createPortal(
    <div className="psheet-root" data-open={shown}>
      <div className="psheet-backdrop" onClick={onClose} aria-hidden />
      <div
        ref={panel}
        className="psheet"
        role="dialog"
        aria-modal="true"
        aria-label={label}
        tabIndex={-1}
        style={drag ? { transform: `translateY(${drag}px)`, transition: "none" } : undefined}
      >
        <div className="psheet-head" {...handlers}>
          <div className="psheet-grabber" aria-hidden />
          <div className="psheet-headrow">
            <div className="psheet-title">{header}</div>
            <button type="button" className="psheet-close" aria-label="关闭" onClick={onClose}>
              <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden>
                <path
                  d="M6 6l12 12M18 6L6 18"
                  stroke="currentColor"
                  strokeWidth="2.2"
                  strokeLinecap="round"
                />
              </svg>
            </button>
          </div>
        </div>
        <div ref={body} className="psheet-body">
          {children}
        </div>
      </div>
    </div>,
    document.body,
  );
}
