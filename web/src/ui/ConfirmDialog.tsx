import { Modal } from "antd";
import { type ReactNode, useEffect, useState } from "react";

/**
 * The confirmation shapes of v2 §1.3:
 * - "heavy": says what will happen and how long it takes, one confirm click.
 * - "high": two-step. The caller first asks the server for a preview (what will
 *   happen, plus a confirmation token valid for two minutes) and passes it in;
 *   the dialog then requires the object's name to be typed exactly and refuses
 *   once the token has expired.
 * Ordinary writes need no dialog.
 */
export type ConfirmLevel = "heavy" | "high";

export interface ConfirmDialogProps {
  open: boolean;
  level: ConfirmLevel;
  title: string;
  /** What will happen (server-generated for "high"). */
  description: ReactNode;
  /** "high": the name the operator has to type, e.g. the unit or account name. */
  confirmName?: string;
  /** "high": when the server's confirmation token expires. */
  expiresAt?: Date;
  confirmLabel?: string;
  busy?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
  /** Injectable clock for tests. */
  now?: () => Date;
}

export function ConfirmDialog({
  open,
  level,
  title,
  description,
  confirmName,
  expiresAt,
  confirmLabel = "确认执行",
  busy = false,
  onConfirm,
  onCancel,
  now = () => new Date(),
}: ConfirmDialogProps) {
  const [typed, setTyped] = useState("");
  const [, setTick] = useState(0);

  useEffect(() => {
    if (!open) {
      setTyped("");
    }
  }, [open]);

  useEffect(() => {
    if (!open || level !== "high") {
      return undefined;
    }
    const timer = window.setInterval(() => setTick((value) => value + 1), 1000);
    return () => window.clearInterval(timer);
  }, [open, level]);

  const expired = level === "high" && (expiresAt === undefined || now() >= expiresAt);
  const nameMatches = level !== "high" || (confirmName !== undefined && typed === confirmName);
  const secondsLeft =
    expiresAt === undefined
      ? 0
      : Math.max(0, Math.ceil((expiresAt.getTime() - now().getTime()) / 1000));

  return (
    <Modal
      open={open}
      title={title}
      onCancel={onCancel}
      onOk={onConfirm}
      okText={confirmLabel}
      cancelText="取消"
      okButtonProps={{ danger: level === "high", disabled: expired || !nameMatches, loading: busy }}
      destroyOnHidden
    >
      <div className="confirm-body">
        <div className="confirm-desc">{description}</div>
        {level === "high" ? (
          <>
            <label className="field">
              <span className="lbl">
                请输入 <b className="mono">{confirmName}</b> 以确认
              </span>
              <input
                className="inp mono"
                value={typed}
                onChange={(event) => setTyped(event.target.value)}
                autoComplete="off"
                spellCheck={false}
              />
            </label>
            <p className={expired ? "hint crit-text" : "hint"} role={expired ? "alert" : undefined}>
              {expired ? "确认已过期，请关闭后重新发起。" : `确认在 ${secondsLeft} 秒后过期。`}
            </p>
          </>
        ) : null}
      </div>
    </Modal>
  );
}
