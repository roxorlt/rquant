#!/usr/bin/env node
// Gzip size of what the first page load downloads: the entry script, its
// modulepreloads and the stylesheet named in dist/index.html. Fails above the
// budget (plan v2 T0.3: 550 KB for the first screen). Fonts load on demand and
// lazy page chunks are listed but not counted.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

const BUDGET_BYTES = 550 * 1024;
const dist = fileURLToPath(new URL("../dist/", import.meta.url));

const gz = (path) => gzipSync(readFileSync(path), { level: 9 }).length;
const kb = (bytes) => `${(bytes / 1024).toFixed(1)} KB`;

const html = readFileSync(join(dist, "index.html"), "utf8");
const initial = new Set();
for (const match of html.matchAll(/<(?:script|link)\b[^>]*?(?:src|href)="\.\/([^"]+)"/g)) {
  const file = match[1];
  if (file.endsWith(".js") || file.endsWith(".css")) {
    initial.add(file);
  }
}
if (initial.size === 0) {
  console.error("check-size: dist/index.html references no script or stylesheet");
  process.exit(1);
}

function walk(directory) {
  return readdirSync(directory).flatMap((name) => {
    const path = join(directory, name);
    return statSync(path).isDirectory() ? walk(path) : [path];
  });
}

let initialTotal = 0;
const rows = [];
for (const path of walk(dist)) {
  const file = relative(dist, path);
  if (!/\.(js|css)$/.test(file)) {
    continue;
  }
  const size = gz(path);
  const first = initial.has(file);
  if (first) {
    initialTotal += size;
  }
  rows.push({ file, size, first });
}
rows.sort((a, b) => Number(b.first) - Number(a.first) || b.size - a.size);
for (const row of rows) {
  console.log(`${row.first ? "首屏" : "按需"}  ${kb(row.size).padStart(9)}  ${row.file}`);
}
const lazyTotal = rows.filter((row) => !row.first).reduce((sum, row) => sum + row.size, 0);
console.log(
  `首屏合计（gzip）: ${kb(initialTotal)} / 上限 ${kb(BUDGET_BYTES)}；按需加载合计 ${kb(lazyTotal)}`,
);
if (initialTotal > BUDGET_BYTES) {
  console.error("check-size: 首屏体积超过上限");
  process.exit(1);
}
