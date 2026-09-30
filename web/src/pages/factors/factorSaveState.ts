import type { FactorSaveDraft } from "@/api/factors";
import type { FactorEditorDraft } from "./FactorEditor";

export const SAVE_COMMAND_KEY = "rquant.factor.save-command.v1";
export const SAVE_DRAFT_KEY = "rquant.factor.save-draft.v1";

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validHead(value: unknown): boolean {
  return (
    value === null ||
    (isRecord(value) &&
      Number.isInteger(value.version) &&
      (value.version as number) > 0 &&
      typeof value.content_sha256 === "string")
  );
}

function validDraftFields(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.generation_id === "string" &&
    (value.mode === "create" || value.mode === "edit") &&
    (value.factor_id === null || typeof value.factor_id === "string") &&
    validHead(value.expected_head) &&
    typeof value.name_zh === "string" &&
    typeof value.category === "string" &&
    (value.direction === "higher_is_better" || value.direction === "lower_is_better") &&
    typeof value.expression === "string" &&
    (value.mode === "create"
      ? value.factor_id === null && value.expected_head === null
      : typeof value.factor_id === "string" && value.expected_head !== null)
  );
}

function validEditor(value: unknown): value is FactorEditorDraft {
  return isRecord(value) && validDraftFields(value) && typeof value.category_label === "string";
}

const commandKeys = [
  "generation_id",
  "command_id",
  "requested_at",
  "mode",
  "factor_id",
  "expected_head",
  "name_zh",
  "category",
  "direction",
  "expression",
] as const satisfies readonly (keyof FactorSaveDraft)[];

function validSaveCommand(value: unknown): value is FactorSaveDraft {
  return (
    isRecord(value) &&
    validDraftFields(value) &&
    typeof value.command_id === "string" &&
    typeof value.requested_at === "string" &&
    Object.keys(value).length === commandKeys.length &&
    commandKeys.every((key) => key in value)
  );
}

export function readEditorDraft(): FactorEditorDraft | null {
  try {
    const raw = window.localStorage.getItem(SAVE_DRAFT_KEY);
    if (raw === null) return null;
    const value: unknown = JSON.parse(raw);
    return validEditor(value) ? value : null;
  } catch {
    return null;
  }
}

export function readSaveCommand(): FactorSaveDraft | null {
  try {
    const raw = window.localStorage.getItem(SAVE_COMMAND_KEY);
    if (raw === null) return null;
    const value: unknown = JSON.parse(raw);
    return validSaveCommand(value) ? value : null;
  } catch {
    return null;
  }
}

export function storageWritable(): boolean {
  try {
    const probe = "rquant.factor.save-storage-probe";
    window.localStorage.setItem(probe, "1");
    const ready = window.localStorage.getItem(probe) === "1";
    window.localStorage.removeItem(probe);
    return ready;
  } catch {
    return false;
  }
}

export function persistEditorDraft(draft: FactorEditorDraft): boolean {
  try {
    window.localStorage.setItem(SAVE_DRAFT_KEY, JSON.stringify(draft));
    return window.localStorage.getItem(SAVE_DRAFT_KEY) === JSON.stringify(draft);
  } catch {
    return false;
  }
}

export function persistSaveCommand(command: FactorSaveDraft): boolean {
  try {
    const serialized = JSON.stringify(command);
    window.localStorage.setItem(SAVE_COMMAND_KEY, serialized);
    return window.localStorage.getItem(SAVE_COMMAND_KEY) === serialized;
  } catch {
    return false;
  }
}

export function clearSaveCommand(): boolean {
  try {
    window.localStorage.removeItem(SAVE_COMMAND_KEY);
    return window.localStorage.getItem(SAVE_COMMAND_KEY) === null;
  } catch {
    return false;
  }
}
