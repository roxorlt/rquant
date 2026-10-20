import { Link } from "react-router";
import { useReadOnlyReason } from "@/api/useMeta";
import type { FailureCopy } from "./failure";

/** One in-flow failure for a panel. A later failure replaces this one. */
export function FailureNotice({
  copy,
  busy,
  onRetry,
}: {
  copy: FailureCopy;
  busy?: boolean;
  onRetry?: () => void;
}) {
  return (
    <div className="notice crit" role="alert">
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

export function ReadOnlyBanner() {
  const readOnly = useReadOnlyReason();
  if (!readOnly) return null;
  return (
    <div className="notice" id="readonly-notice" role="note">
      当前是只读账号，可以查看，不能修改
    </div>
  );
}
