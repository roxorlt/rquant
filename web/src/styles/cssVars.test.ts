import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

function walk(dir: string, out: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) walk(path, out);
    else if (path.endsWith(".css")) out.push(path);
  }
  return out;
}

describe("css variables", () => {
  it("defines every custom property the stylesheets use", () => {
    const root = join(process.cwd(), "src");
    const defined = new Set<string>();
    const used = new Set<string>();
    for (const file of walk(root)) {
      const text = readFileSync(file, "utf8");
      for (const match of text.matchAll(/(^|[\s{;])(--[A-Za-z0-9-]+)\s*:/g)) {
        const name = match[2];
        if (name) defined.add(name);
      }
      for (const match of text.matchAll(/var\(\s*(--[A-Za-z0-9-]+)(\s*,|\s*\))/g)) {
        const name = match[1];
        const tail = match[2] ?? "";
        if (name && !tail.trim().startsWith(",")) used.add(name);
      }
    }
    const missing = [...used].filter((name) => !defined.has(name)).sort();
    expect(missing).toEqual([]);
    expect(defined.has("--c-warn-weak")).toBe(true);
  });
});
