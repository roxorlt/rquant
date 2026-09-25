#!/usr/bin/env node
// Rebuild web/dist and prove the committed copy is exactly that build (CI runs
// this in web.yml; run it locally before committing a front-end change).
//
// 1. No file under web/ is silently ignored by the root .gitignore, which drops
//    every directory named lib/, build/, dist/, env/, var/, parts/, data/, …;
//    only the dependency and test-output caches may be ignored.
// 2. `vite build` from a clean dist/.
// 3. git sees no difference in web/dist (modified, deleted or new files).
// 4. The first-screen size budget holds (scripts/check-size.mjs).
import { execFileSync, spawnSync } from "node:child_process";
import { rmSync } from "node:fs";
import { fileURLToPath } from "node:url";

const web = fileURLToPath(new URL("..", import.meta.url));
const ALLOWED_IGNORED = [
  /^web\/node_modules\//,
  /^web\/\.vite\//,
  /^web\/coverage\//,
  /^web\/playwright-report\//,
  /^web\/test-results\//,
  /(^|\/)\.DS_Store$/,
];

function git(...args) {
  return execFileSync("git", args, { cwd: web, encoding: "utf8" });
}

function fail(message, detail = "") {
  console.error(`verify-dist: ${message}`);
  if (detail) {
    console.error(detail);
  }
  process.exit(1);
}

function run(command, args) {
  const result = spawnSync(command, args, { cwd: web, stdio: "inherit" });
  if (result.status !== 0) {
    fail(`${command} ${args.join(" ")} exited with ${result.status}`);
  }
}

const ignored = git(
  "ls-files",
  "--others",
  "--ignored",
  "--exclude-standard",
  "--directory",
  "--",
  ".",
)
  .split("\n")
  .filter(Boolean)
  .filter((path) => !ALLOWED_IGNORED.some((pattern) => pattern.test(path)));
if (ignored.length > 0) {
  fail(
    "these files under web/ are ignored by .gitignore and would never be committed " +
      "(rename the directory; lib/, build/, data/, env/, var/, parts/ are reserved):",
    ignored.join("\n"),
  );
}

rmSync(new URL("../dist", import.meta.url), { recursive: true, force: true });
run("pnpm", ["exec", "vite", "build"]);

const changes = git("status", "--porcelain", "--untracked-files=all", "--", "dist");
if (changes.trim() !== "") {
  fail(
    "the rebuilt web/dist differs from the committed one; run `pnpm -C web build` and commit web/dist:",
    `${changes}\n${git("diff", "--stat", "--", "dist")}`,
  );
}

run("node", ["scripts/check-size.mjs"]);
console.log("verify-dist: web/dist is up to date");
