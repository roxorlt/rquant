import { Link } from "react-router";
import type { FailureCopy } from "./failure";

/** One in-flow failure for a panel. A later failure replaces this one. */
export function FailureNotice({
  copy,
  busy,
  onRetry,
  pin = false,
}: {
  copy: FailureCopy;
  busy?: boolean;
  onRetry?: () => void;
  /** Stick to the top of the visible page when the notice would scroll away. */
  pin?: boolean;
}) {
  return (
    <div className={pin ? "notice crit pin" : "notice crit"} role="alert">
      <span>{copy.text}</span>
      {copy.retry || copy.health ? (
        <span className="acts">
          {copy.retry && onRetry ? (
            <button type="button" className="act" onClick={onRetry} aria-busy={busy || undefined}>
              重试
            </button>
          ) : null}
          {copy.health ? (
            <Link className="act" to="/health">
              看系统健康
            </Link>
          ) : null}
        </span>
      ) : null}
    </div>
  );
}
