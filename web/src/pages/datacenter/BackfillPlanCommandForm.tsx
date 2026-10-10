import { useState } from "react";
import { Button, ConfirmDialog, Tip } from "@/ui";
import {
  type BackfillPlanCommandSession,
  type BackfillPlanCommandSnapshot,
  latestClosedDate,
  validateBackfillRange,
} from "./backfillPlanCommandSession";

export function BackfillPlanCommandForm({
  session,
  snapshot,
  canSubmit,
  onClose,
}: {
  session: BackfillPlanCommandSession;
  snapshot: BackfillPlanCommandSnapshot;
  canSubmit: boolean;
  onClose: () => void;
}) {
  const [start, setStart] = useState(snapshot.journal?.body.audit_start ?? "2024-09-01");
  const [end, setEnd] = useState(snapshot.journal?.body.completed_through ?? "2025-04-30");
  const [confirmOpen, setConfirmOpen] = useState(false);
  const now = new Date();
  const invalid = validateBackfillRange(start, end, now);
  const locked =
    !canSubmit ||
    !snapshot.storageAvailable ||
    snapshot.busy ||
    (snapshot.journal !== null && !["queued", "failed"].includes(snapshot.journal.status));

  return (
    <section className="dc-plan-command-form" aria-label="生成回补计划">
      <div className="dc-plan-command-form-head">
        <h2>核对日期</h2>
        <Button size="sm" variant="ghost" onClick={onClose}>
          收起
        </Button>
      </div>
      <div className="dc-plan-date-fields">
        <label className="field">
          <span className="lbl">开始日期</span>
          <input
            className="inp mono"
            type="date"
            value={start}
            max={end || latestClosedDate(now)}
            onChange={(event) => setStart(event.target.value)}
          />
        </label>
        <label className="field">
          <span className="lbl">结束日期</span>
          <input
            className="inp mono"
            type="date"
            value={end}
            max={latestClosedDate(now)}
            onChange={(event) => setEnd(event.target.value)}
          />
        </label>
        <Button
          variant="primary"
          disabled={locked || invalid !== null}
          onClick={() => setConfirmOpen(true)}
        >
          核对并生成
        </Button>
      </div>
      {invalid ? (
        <p className="dc-plan-form-error" role="status">
          {invalid}
        </p>
      ) : null}
      <Tip content="优先核对 2024 年 9 月至 2025 年 4 月；可按需要调整，最多 3660 天。">
        <span className="dc-plan-form-tip">日期范围说明</span>
      </Tip>
      <ConfirmDialog
        open={confirmOpen}
        level="heavy"
        title="生成回补计划"
        confirmLabel="确认排队"
        busy={snapshot.busy}
        description={
          <p>
            将只读核对 {start} 至 {end}{" "}
            的交易日和日线记录。确认后排队，耗时可能从数分钟到更久；不会写入日线。
          </p>
        }
        onCancel={() => setConfirmOpen(false)}
        onConfirm={() => {
          if (locked || validateBackfillRange(start, end, new Date()) !== null) return;
          setConfirmOpen(false);
          onClose();
          void session.start(start, end, new Date());
        }}
      />
    </section>
  );
}
