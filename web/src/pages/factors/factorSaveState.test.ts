import type { FactorSaveDraft } from "@/api/factors";
import {
  persistSaveCommand,
  persistSaveRejection,
  readSaveCommand,
  readSaveRejection,
  SAVE_COMMAND_KEY,
  SAVE_REJECTION_KEY,
} from "./factorSaveState";

const original: FactorSaveDraft = {
  generation_id: "a".repeat(64),
  command_id: "882daf54-94ad-48b5-a988-4e7d2681de15",
  requested_at: "2026-09-29T07:00:00Z",
  mode: "create",
  factor_id: null,
  expected_head: null,
  name_zh: "收盘均值",
  category: "技术",
  direction: "higher_is_better",
  expression: "ts_mean(close, 5)",
};

it("重载恢复保存合同的原请求，不要求界面的分类展示名", () => {
  expect(persistSaveCommand(original)).toBe(true);
  expect(readSaveCommand()).toEqual(original);
  expect(readSaveCommand()).not.toHaveProperty("category_label");
});

it("界面专用字段不能混入恢复的保存合同", () => {
  localStorage.setItem(SAVE_COMMAND_KEY, JSON.stringify({ ...original, category_label: "技术" }));
  expect(readSaveCommand()).toBeNull();
});

it("缺少旧版本的编辑请求不能作为有效原命令恢复", () => {
  localStorage.setItem(
    SAVE_COMMAND_KEY,
    JSON.stringify({ ...original, mode: "edit", factor_id: "sample" }),
  );
  expect(readSaveCommand()).toBeNull();
});

it("明确拒绝绑定完整原请求，界面终态不混入请求合同", () => {
  expect(persistSaveCommand(original)).toBe(true);
  expect(
    persistSaveRejection(
      original,
      {
        command_id: original.command_id,
        status: "rejected",
        message: "请修改草稿。",
        current_head_updated: false,
      },
      "tester",
    ),
  ).toBe(true);
  expect(readSaveRejection(readSaveCommand())).toMatchObject({
    command: original,
    result: { status: "rejected" },
    deniedViewer: "tester",
  });
  expect(JSON.parse(localStorage.getItem(SAVE_COMMAND_KEY) ?? "null")).toEqual(original);
  expect(readSaveRejection({ ...original, command_id: "another-command" })).toBeNull();
  expect(readSaveRejection({ ...original, expression: "ref(close, 2)" })).toBeNull();
});

it("未知结果不能作为明确拒绝恢复或结束原命令", () => {
  expect(persistSaveCommand(original)).toBe(true);
  localStorage.setItem(
    SAVE_REJECTION_KEY,
    JSON.stringify({
      command: original,
      result: {
        command_id: original.command_id,
        status: "uncertain",
        message: "待核对",
        current_head_updated: false,
      },
      deniedViewer: null,
    }),
  );
  expect(readSaveRejection(original)).toBeNull();
  expect(readSaveCommand()).toEqual(original);
});
