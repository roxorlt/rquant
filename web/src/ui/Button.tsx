import type { ButtonHTMLAttributes, ReactNode } from "react";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "default" | "primary" | "ghost";
  size?: "md" | "sm";
  /**
   * Why the button cannot be used right now. Disables it and shows the reason as
   * its tooltip and accessible description (v2: no silent grey buttons).
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
  return (
    <button
      {...rest}
      type={type}
      className={classes}
      disabled={disabled || disabledReason !== undefined}
      title={disabledReason ?? title}
      aria-description={disabledReason}
    >
      {children}
    </button>
  );
}
