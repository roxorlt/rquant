import type { FactorSaveDraft } from "@/api/factors";
import { persistSaveCommand, readSaveCommand, SAVE_COMMAND_KEY } from "./factorSaveState";

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
