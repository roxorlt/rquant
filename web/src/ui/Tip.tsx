import { Tooltip } from "antd";
import { type ReactNode, useSyncExternalStore } from "react";

const TOUCH_QUERY = "(hover: none)";

function subscribe(onChange: () => void): () => void {
  const list = window.matchMedia(TOUCH_QUERY);
  list.addEventListener("change", onChange);
  return () => list.removeEventListener("change", onChange);
}

function touchOnly(): boolean {
  return window.matchMedia(TOUCH_QUERY).matches;
}

/** True on phones and tablets, where there is no hover. */
export function useTouchOnly(): boolean {
  return useSyncExternalStore(subscribe, touchOnly, () => false);
}

export interface TipProps {
  /** The explanation; nothing is rendered around the children when it is empty. */
  content: ReactNode;
  children: ReactNode;
  placement?: "top" | "bottom" | "left" | "right" | "topLeft" | "bottomLeft";
  /**
   * The children are themselves focusable (a button, a link). Otherwise they are wrapped
   * in a focusable span so the tip also opens from the keyboard.
   */
  interactive?: boolean;
  className?: string;
  /** Visible bubble width in pixels. Omit it and the tip keeps the default width. */
  width?: number;
  /** Keep the bubble at least this many pixels inside the viewport. */
  inset?: number;
  /** When false, the bubble stays on `placement` instead of flipping onto nearby fields. */
  autoAdjustOverflow?: boolean;
  /** CSS selector for fields the bubble must sit above, such as the name input. */
  avoid?: string;
  /** CSS selector for the label the bubble should stay against. */
  pinTo?: string;
}

export function overlaps(
  a: { left: number; top: number; right: number; bottom: number },
  b: { left: number; top: number; right: number; bottom: number },
): boolean {
  return !(a.right <= b.left || a.left >= b.right || a.bottom <= b.top || a.top >= b.bottom);
}

/** Move the bubble above the first field it covers. */
export function shiftAbove(
  rect: { left: number; top: number; right: number; bottom: number },
  blockers: { left: number; top: number; right: number; bottom: number }[],
  gap: number,
): { x: number; y: number } {
  let y = 0;
  for (const block of blockers) {
    const moved = {
      left: rect.left,
      right: rect.right,
      top: rect.top + y,
      bottom: rect.bottom + y,
    };
    if (!overlaps(moved, block)) continue;
    y += block.top - gap - moved.bottom;
  }
  return { x: 0, y };
}

function nudge(el: HTMLElement, dx: number, dy: number): void {
  if (!dx && !dy) return;
  const left = el.style.left;
  const right = el.style.right;
  const top = el.style.top;
  const bottom = el.style.bottom;
  if (dx && left && left !== "auto") el.style.left = `${parseFloat(left) + dx}px`;
  else if (dx && right && right !== "auto") el.style.right = `${parseFloat(right) - dx}px`;
  if (dy && top && top !== "auto") el.style.top = `${parseFloat(top) + dy}px`;
  else if (dy && bottom && bottom !== "auto") el.style.bottom = `${parseFloat(bottom) - dy}px`;
}

type Box = { left: number; top: number; right: number; bottom: number };

/** Keep the bubble against the label: above it, or below when above would leave the viewport. */
export function pinBeside(
  tip: Box,
  anchor: Box,
  viewport: { width: number; height: number },
  inset: number,
  gap: number,
): { x: number; y: number } {
  const aboveY = anchor.top - gap - tip.bottom;
  const aboveTop = tip.top + aboveY;
  const belowY = anchor.bottom + gap - tip.top;
  const belowBottom = tip.bottom + belowY;
  const y = aboveTop >= inset || belowBottom > viewport.height - inset ? aboveY : belowY;
  const moved = {
    left: tip.left,
    top: tip.top + y,
    right: tip.right,
    bottom: tip.bottom + y,
  };
  const clamped = clampShift(moved, viewport, inset);
  return { x: clamped.x, y: y + clamped.y };
}

export function clampShift(
  rect: { left: number; top: number; right: number; bottom: number },
  viewport: { width: number; height: number },
  inset: number,
): { x: number; y: number } {
  const x =
    rect.left < inset
      ? inset - rect.left
      : rect.right > viewport.width - inset
        ? viewport.width - inset - rect.right
        : 0;
  const y =
    rect.top < inset
      ? inset - rect.top
      : rect.bottom > viewport.height - inset
        ? viewport.height - inset - rect.bottom
        : 0;
  return { x, y };
}

/**
 * The one way to explain something: a tooltip on hover or keyboard focus, and on tap
 * where there is no hover. Technical detail (ids, raw errors) lives here, never in the
 * page text (web/CLAUDE.md 「界面与文案原则」).
 */
export function Tip({
  content,
  children,
  placement = "top",
  interactive,
  className,
  width,
  inset,
  autoAdjustOverflow,
  avoid,
  pinTo,
}: TipProps) {
  const touch = useTouchOnly();
  if (content === null || content === undefined || content === "") {
    return <>{children}</>;
  }
  const anchor = interactive ? (
    children
  ) : (
    // biome-ignore lint/a11y/noNoninteractiveTabindex: the anchor opens its tooltip on keyboard focus.
    <span className={className ? `tip-anchor ${className}` : "tip-anchor"} tabIndex={0}>
      {children}
    </span>
  );
  return (
    <Tooltip
      title={content}
      placement={placement}
      autoAdjustOverflow={autoAdjustOverflow}
      trigger={touch ? ["click"] : ["hover", "focus"]}
      mouseEnterDelay={0.15}
      destroyOnHidden
      onOpenChange={(open) => {
        if (inset === undefined && !pinTo) return;
        const apply = (tries = 0) => {
          const el = document.querySelector<HTMLElement>(".rq-tip:not(.ant-tooltip-hidden)");
          if (!el) {
            if (open && tries < 8) requestAnimationFrame(() => apply(tries + 1));
            return;
          }
          if (!open) return;
          const rect = el.getBoundingClientRect();
          const viewport = { width: window.innerWidth, height: window.innerHeight };
          const anchor = pinTo ? document.querySelector(pinTo) : null;
          if (anchor && inset !== undefined) {
            const shift = pinBeside(rect, anchor.getBoundingClientRect(), viewport, inset, 4);
            nudge(el, shift.x, shift.y);
            return;
          }
          if (inset === undefined) return;
          const blockers = avoid
            ? [...document.querySelectorAll(avoid)].map((node) => node.getBoundingClientRect())
            : [];
          const clear = shiftAbove(rect, blockers, 8);
          const moved = {
            left: rect.left + clear.x,
            top: rect.top + clear.y,
            right: rect.right + clear.x,
            bottom: rect.bottom + clear.y,
          };
          const shift = clampShift(moved, viewport, inset);
          nudge(el, clear.x + shift.x, clear.y + shift.y);
        };
        window.setTimeout(() => apply(0), 80);
      }}
      classNames={{ root: "rq-tip" }}
      styles={
        width === undefined
          ? undefined
          : {
              root: { maxWidth: width },
              container: { width, maxWidth: width, boxSizing: "border-box" },
            }
      }
    >
      {anchor}
    </Tooltip>
  );
}
