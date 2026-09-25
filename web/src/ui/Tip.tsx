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
}

/**
 * The one way to explain something: a tooltip on hover or keyboard focus, and on tap
 * where there is no hover. Technical detail (ids, raw errors) lives here, never in the
 * page text (web/CLAUDE.md 「界面与文案原则」).
 */
export function Tip({ content, children, placement = "top", interactive, className }: TipProps) {
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
      trigger={touch ? ["click"] : ["hover", "focus"]}
      mouseEnterDelay={0.15}
      destroyOnHidden
      classNames={{ root: "rq-tip" }}
    >
      {anchor}
    </Tooltip>
  );
}
