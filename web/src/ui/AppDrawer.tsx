import {
  type ReactNode,
  type PointerEvent as ReactPointerEvent,
  useEffect,
  useId,
  useRef,
} from "react";
import { createPortal } from "react-dom";

export interface AppDrawerProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  /** Accessible name when the title is not plain text. */
  label: string;
  extra?: ReactNode;
  /** Fixed actions. Only the body scrolls. */
  footer?: ReactNode;
  children: ReactNode;
}

function focusable(root: HTMLElement): HTMLElement[] {
  return [
    ...root.querySelectorAll<HTMLElement>(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
    ),
  ].filter((el) => !el.hasAttribute("disabled") && el.getAttribute("aria-hidden") !== "true");
}

/**
 * Desktop right drawer (full viewport height) / phone bottom sheet (flush to the screen edge).
 * Closes from the button, the scrim, Esc, and a downward swipe on the grabber or header.
 * Focus starts on the title, stays inside the dialog, and returns to the opener.
 */
export function AppDrawer({
  open,
  onClose,
  title,
  label,
  extra,
  footer,
  children,
}: AppDrawerProps) {
  const titleId = useId();
  const panel = useRef<HTMLElement>(null);
  const opener = useRef<HTMLElement | null>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const drag = useRef<{ y: number; t: number } | null>(null);

  useEffect(() => {
    if (!open) return;
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const shell = document.querySelector(".shell");
    if (shell instanceof HTMLElement) shell.inert = true;
    const html = document.documentElement;
    const prev = html.style.overflow;
    html.style.overflow = "hidden";
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseRef.current();
        return;
      }
      if (event.key !== "Tab" || !panel.current) return;
      const items = focusable(panel.current);
      if (items.length === 0) {
        event.preventDefault();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;
      const onTitle = active instanceof HTMLElement && active.id === titleId;
      if (onTitle || !panel.current.contains(active)) {
        event.preventDefault();
        (event.shiftKey ? last : first)?.focus();
        return;
      }
      if (event.shiftKey && active === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    panel.current?.querySelector<HTMLElement>(".drawer-h h2")?.focus();
    return () => {
      if (shell instanceof HTMLElement) shell.inert = false;
      html.style.overflow = prev;
      window.removeEventListener("keydown", onKey);
      const back = opener.current;
      opener.current = null;
      back?.focus();
    };
  }, [open, titleId]);

  if (!open) return null;

  const onDown = (event: ReactPointerEvent) => {
    if ((event.target as HTMLElement).closest("button")) return;
    drag.current = { y: event.clientY, t: performance.now() };
  };
  const onUp = (event: ReactPointerEvent) => {
    if (!drag.current) return;
    const dy = event.clientY - drag.current.y;
    const dt = Math.max(1, performance.now() - drag.current.t);
    drag.current = null;
    const height = panel.current?.getBoundingClientRect().height ?? 1;
    if (dy > height * 0.25 || (dy > 8 && dy / dt > 0.5)) onCloseRef.current();
  };

  return createPortal(
    <>
      <button
        type="button"
        className="scrim drawer-scrim"
        aria-label="关闭遮罩"
        onClick={onClose}
      />
      <aside
        ref={panel}
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-label={typeof title === "string" ? undefined : label}
      >
        <div className="drawer-grab" aria-hidden="true" onPointerDown={onDown} onPointerUp={onUp} />
        <div className="drawer-h" onPointerDown={onDown} onPointerUp={onUp}>
          <h2 id={titleId} tabIndex={-1}>
            {title}
          </h2>
          {extra}
          <button type="button" className="x" aria-label="关闭" onClick={onClose}>
            ×
          </button>
        </div>
        <div className="drawer-b">{children}</div>
        {footer ? <div className="drawer-f">{footer}</div> : null}
      </aside>
    </>,
    document.body,
  );
}
