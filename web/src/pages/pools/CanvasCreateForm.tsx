import { useState } from "react";
import { Button, SideDrawer, Tip } from "@/ui";
import type { CanvasCreateSession, CanvasCreateSnapshot } from "./canvasCreateSession";
import "./canvasCreate.css";

export function canvasCreateLabel(
  snapshot: CanvasCreateSnapshot,
  available: boolean,
): string | null {
  const journal = snapshot.journal;
  if (!journal) return null;
  if (journal.status === "failed") return journal.reason ?? "画布创建失败，请检查后重试。";
  if (["ambiguous", "unknown"].includes(journal.status)) return "创建状态待确认";
  if (journal.status !== "succeeded") return "正在创建画布";
  return available ? "画布已可用" : "已保存，等待发布";
}

export function CanvasCreateForm({
  session,
  snapshot,
  available,
  canCreate,
  unavailableReason,
  existingNames,
  onClose,
  onOpen,
  onRefresh,
}: {
  session: CanvasCreateSession;
  snapshot: CanvasCreateSnapshot;
  available: boolean;
  canCreate: boolean;
  unavailableReason: string;
  existingNames: string[];
  onClose: () => void;
  onOpen: () => void;
  onRefresh: () => void;
}) {
  const journal = snapshot.journal;
  const [name, setName] = useState(journal?.body.name ?? "");
  const [description, setDescription] = useState(journal?.body.description ?? "");
  const cleanName = name.trim();
  const occupied = existingNames.includes(cleanName);
  const validName = /^[\w\u4e00-\u9fff-]{1,80}$/u.test(cleanName);
  const locked = !!journal && journal.status !== "failed";
  const reason = !canCreate
    ? unavailableReason
    : !snapshot.storageAvailable
      ? "浏览器存储不可用，无法安全提交。"
      : locked
        ? "请先确认上一次创建请求。"
        : !validName
          ? "请输入 1 至 80 个字的名称，使用中文、字母、数字、横线或下划线。"
          : occupied
            ? "画布名称已被使用，请换一个名称。"
            : undefined;
  const status = canvasCreateLabel(snapshot, available);

  return (
    <SideDrawer
      open
      onClose={onClose}
      title="新建画布"
      footer={
        <div className="canvas-create-footer">
          {status ? (
            <div className="canvas-create-status" role="status">
              <strong>{status}</strong>
              {journal?.status === "failed" && snapshot.message ? (
                <span>{snapshot.message}</span>
              ) : null}
              {["ambiguous", "unknown"].includes(journal?.status ?? "") ? (
                <span>请继续核对本次请求。</span>
              ) : null}
            </div>
          ) : null}
          {!snapshot.storageAvailable ? (
            <p className="canvas-create-error" role="alert">
              {snapshot.message ?? "浏览器存储不可用，无法安全提交。"}
            </p>
          ) : null}
          <div className="canvas-create-actions">
            <Button onClick={onClose}>返回画布</Button>
            {available ? (
              <Button variant="primary" onClick={onOpen}>
                打开画布
              </Button>
            ) : journal &&
              ["pending", "processing", "unknown", "ambiguous"].includes(journal.status) ? (
              <Button
                disabledReason={
                  !canCreate ? unavailableReason : snapshot.busy ? "正在核对，请稍候。" : undefined
                }
                onClick={() => void session.advance()}
              >
                继续核对
              </Button>
            ) : journal?.status === "succeeded" ? (
              <Button onClick={onRefresh}>检查发布</Button>
            ) : (
              <Button
                variant="primary"
                disabledReason={reason}
                onClick={() => void session.start(name, description)}
              >
                创建画布
              </Button>
            )}
          </div>
        </div>
      }
    >
      <div className="canvas-create-form">
        <p className="pool-editor-lead">先建一张空画布，再加入池子。</p>
        <label className="field">
          <span className="lbl">画布名称</span>
          <input
            className="inp"
            maxLength={80}
            value={name}
            disabled={locked}
            onChange={(event) => setName(event.target.value)}
          />
        </label>
        <label className="field">
          <span className="lbl">
            简短说明 <small>选填</small>
          </span>
          <textarea
            className="inp"
            rows={3}
            maxLength={1024}
            value={description}
            disabled={locked}
            onChange={(event) => setDescription(event.target.value)}
          />
        </label>
        {occupied && !locked ? (
          <p className="canvas-create-error" role="status">
            画布名称已被使用，请换一个名称。
          </p>
        ) : null}
        <section className="canvas-create-preview" aria-label="新画布预览">
          <span className="canvas-create-preview-mark" aria-hidden="true">
            ＋
          </span>
          <strong>{cleanName || "新画布"}</strong>
          <span>{description.trim() || "创建后可加入池子"}</span>
          <Tip content="画布创建时不带池子。创建完成后，可从池子页添加条件。">
            <span className="pools-info">空画布</span>
          </Tip>
        </section>
      </div>
    </SideDrawer>
  );
}
