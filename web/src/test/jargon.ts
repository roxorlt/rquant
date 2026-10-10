/**
 * Internal wording that must never reach the page text (web/CLAUDE.md 「界面与文案原则」):
 * technical ids, hashes, pipeline jargon and milestone codes. Tooltips and drawers are
 * not in the page text until opened, so they may carry ids.
 */
export const JARGON_PATTERNS: readonly RegExp[] = [
  /svc-/,
  /\.v\d+\b/,
  /generation_id/,
  /\b[0-9a-f]{40}\b/,
  /\b[0-9a-f]{64}\b/,
  /\b[0-9a-f]{8,}\b/,
  /projection/i,
  /watermark/i,
  /\bserving\b/i,
  /\bgeneration\b/i,
  /degraded:/,
  /\b[MS]\d{1,2}\b/,
  /第\s*\d+(\s*[–-]\s*\d+)?\s*周/,
  /开发中/,
];

/** The patterns found in `text`, each with the snippet that matched. */
export function findJargon(text: string): string[] {
  return JARGON_PATTERNS.flatMap((pattern) => {
    const match = pattern.exec(text);
    return match ? [`${pattern.source} → "${match[0]}"`] : [];
  });
}
