import type { ButtonHTMLAttributes, ReactNode } from "react";
import { Tip } from "./Tip";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "default" | "primary" | "ghost" | "quiet";
  size?: "md" | "sm";
  /**
   * Why the button cannot be used right now. Disables it and shows the reason in a
   * tooltip and as its accessible description (v2: no silent grey buttons).
   */
  disabledReason?: string;
  /**
   * Stays in the tab order (read-only writes). The click is ignored.
   * Native `disabled` is for a control that is busy or has nothing to do.
   */
  ariaDisabled?: boolean;
  children: ReactNode;
}

export function Button({
  variant = "default",
  size = "md",
  disabledReason,
  disabled,
  ariaDisabled,
  type = "button",
  title,
  className,
  children,
  onClick,
  ...rest
}: ButtonProps) {
  const classes = [
    "btn",
    variant === "default" ? "" : variant,
    size === "sm" ? "sm" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  const soft = ariaDisabled === true && !disabled && disabledReason === undefined;
  const button = (
    <button
      {...rest}
      type={type}
      className={classes}
      disabled={disabled || disabledReason !== undefined}
      aria-disabled={soft ? true : undefined}
      title={disabledReason === undefined ? title : undefined}
      aria-description={disabledReason}
      onClick={(event) => {
        if (soft) {
          event.preventDefault();
          return;
        }
        onClick?.(event);
      }}
    >
      {children}
    </button>
  );
  // A disabled button gets no pointer events, so the tip hangs on a wrapper.
  return disabledReason === undefined ? button : <Tip content={disabledReason}>{button}</Tip>;
}
