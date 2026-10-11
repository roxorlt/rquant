export const KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "GROUP BY",
  "ORDER BY",
  "LIMIT",
  "AND",
  "OR",
  "NOT",
  "IN",
  "JOIN",
  "LEFT JOIN",
  "ON",
  "AS",
  "DESC",
  "ASC",
  "COUNT(*)",
  "SUM",
  "AVG",
  "MAX",
  "MIN",
  "DISTINCT",
  "HAVING",
  "WITH",
  "BETWEEN",
  "IS NULL",
  "IS NOT NULL",
  "LIKE",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
];

/** The identifier-ish word ending at `pos`. */
export function wordAt(text: string, pos: number): { start: number; word: string } {
  let start = pos;
  while (start > 0 && /[\w.]/.test(text[start - 1] ?? "")) start -= 1;
  return { start, word: text.slice(start, pos) };
}

/** Prefix candidates (case-insensitive), columns of tables mentioned in the SQL first. */
export function candidates(
  sql: string,
  word: string,
  schema: Record<string, string[][]>,
): string[] {
  if (!word) return [];
  const w = word.toLowerCase();
  const mentioned = Object.keys(schema).filter((t) => new RegExp(`\\b${t}\\b`, "i").test(sql));
  const cols = [
    ...mentioned.flatMap((t) => (schema[t] ?? []).map((c) => c[0] ?? "")),
    ...Object.values(schema).flatMap((cs) => cs.map((c) => c[0] ?? "")),
  ];
  const pool = [...new Set([...cols, ...Object.keys(schema), ...KEYWORDS])];
  return pool.filter((c) => c.toLowerCase().startsWith(w) && c.toLowerCase() !== w).slice(0, 8);
}
