import type { ButtonHTMLAttributes, ReactNode } from "react";
import { Tip } from "./Tip";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "default" | "primary" | "ghost";
  size?: "md" | "sm";
  /**
   * Why the button cannot be used right now. Disables it and shows the reason in a
   * tooltip and as its accessible description (v2: no silent grey buttons).
   */
  disabledReason?: string;
  children: ReactNode;
}

export function Button({
  variant = "default",
  size = "md",
  disabledReason,
  disabled,
  type = "button",
  title,
  className,
  children,
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
  const button = (
    <button
      {...rest}
      type={type}
      className={classes}
      disabled={disabled || disabledReason !== undefined}
      title={disabledReason === undefined ? title : undefined}
      aria-description={disabledReason}
    >
      {children}
    </button>
  );
  // A disabled button gets no pointer events, so the tip hangs on a wrapper.
  return disabledReason === undefined ? button : <Tip content={disabledReason}>{button}</Tip>;
}
