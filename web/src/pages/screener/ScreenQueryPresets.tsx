import { useState } from "react";
import {
  type ScreenPresetSaveRequest,
  type ScreenQueryDefinition,
  type ScreenQueryPreset,
  type ScreenQueryReadData,
  useScreenQueryPresets,
} from "@/api/screen";
import { Button, ConfirmDialog, EmptyState, RelativeTime, SideDrawer, SkeletonRows } from "@/ui";

function id(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}
export function ScreenQueryPresets({
  viewer,
  open,
  definition,
  busy,
  onClose,
  afterOpenChange,
  onRestore,
  onSave,
}: {
  viewer: string | null;
  open: boolean;
  definition: ScreenQueryDefinition | null;
  busy: boolean;
  onClose: () => void;
  onRestore: (definition: ScreenQueryDefinition) => void;
  afterOpenChange?: (open: boolean) => void;
  onSave: (request: ScreenPresetSaveRequest) => Promise<ScreenQueryReadData | null>;
}) {
  const query = useScreenQueryPresets(viewer, open);
  const [name, setName] = useState("");
  const [selected, setSelected] = useState<ScreenQueryPreset | null>(null);
  const [preview, setPreview] = useState<ScreenPresetSaveRequest | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  function prepare() {
    const effective = selected?.definition ?? definition;
    if (!effective || !name.trim() || busy) return;
    setPreview({
      command_id: id("preset-save"),
      requested_at: new Date().toISOString(),
      expected_version: selected?.version ?? null,
      preset: {
        preset_id: selected?.preset_id ?? id("preset"),
        name: name.trim(),
        definition: effective,
      },
    });
  }
  async function save() {
    if (!preview || busy) return;
    const data = await onSave(preview);
    if (
      data?.receipt?.status === "succeeded" &&
      data.presets.some(
        (item) =>
          item.preset_id === preview.preset.preset_id &&
          item.version === (preview.expected_version ?? 0) + 1,
      )
    ) {
      setMessage("条件已保存。");
      setSelected(null);
      setName("");
      void query.refetch();
    } else setMessage("保存待确认，请核对原请求。");
    setPreview(null);
  }
  return (
    <SideDrawer
      open={open}
      onClose={onClose}
      afterOpenChange={afterOpenChange}
      title="常用条件"
      wide
    >
      {query.isLoading ? (
        <SkeletonRows rows={3} />
      ) : query.error ? (
        <div role="alert">
          <p>常用条件暂不可用，请重试。</p>
          <Button
            onClick={() => {
              void query.refetch();
            }}
          >
            重试
          </Button>
        </div>
      ) : query.data?.presets.length === 0 ? (
        <EmptyState title="还没有常用条件" hint="保存后可在其他设备回填。" />
      ) : null}
      <ol className="screen-query-list">
        {(query.data?.presets ?? []).map((item) => (
          <li key={item.preset_id}>
            <div className="screen-query-head">
              <strong>{item.name}</strong>
              <RelativeTime at={item.updated_at} />
            </div>
            <div className="screen-query-facts">
              <span>{item.definition.conditions.length} 条条件</span>
              {item.definition.ranking ? <span>前 {item.definition.ranking.top_n} 只</span> : null}
            </div>
            <div className="screen-query-actions">
              <Button
                size="sm"
                aria-label={`回填${item.name}`}
                onClick={() => onRestore(item.definition)}
              >
                回填
              </Button>
              <Button
                size="sm"
                aria-label={`改名${item.name}`}
                onClick={() => {
                  setSelected(item);
                  setName(item.name);
                  setMessage(null);
                }}
              >
                改名
              </Button>
              <Button
                size="sm"
                aria-label={`覆盖${item.name}`}
                disabledReason={!definition ? "先添加可用条件。" : undefined}
                onClick={() => {
                  if (definition) {
                    setSelected({ ...item, definition });
                    setName(item.name);
                    setMessage(null);
                  }
                }}
              >
                覆盖
              </Button>
            </div>
          </li>
        ))}
      </ol>
      <div className="screen-query-save">
        <label className="field">
          <span className="lbl">条件名称</span>
          <input
            className="inp"
            value={name}
            maxLength={80}
            onChange={(event) => setName(event.target.value)}
          />
        </label>
        <div className="screen-query-actions">
          <Button
            onClick={prepare}
            disabled={busy || !!query.error}
            disabledReason={
              !name.trim()
                ? "先填写名称。"
                : !(selected?.definition ?? definition)
                  ? "先添加可用条件。"
                  : undefined
            }
          >
            保存条件
          </Button>
          {selected ? (
            <Button
              variant="ghost"
              onClick={() => {
                setSelected(null);
                setName("");
              }}
            >
              另存条件
            </Button>
          ) : null}
        </div>
      </div>
      {message ? <p role="status">{message}</p> : null}
      <ConfirmDialog
        open={preview !== null}
        level="heavy"
        title={preview?.expected_version ? "确认更新条件" : "确认保存条件"}
        description={`保存「${preview?.preset.name ?? ""}」的完整条件与排名。${preview?.expected_version ? "原记录将被更新。" : "保存后可跨设备回填。"}`}
        confirmLabel="确认保存"
        busy={busy}
        onCancel={() => setPreview(null)}
        onConfirm={() => {
          void save();
        }}
      />
    </SideDrawer>
  );
}
