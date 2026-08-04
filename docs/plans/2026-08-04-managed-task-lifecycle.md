# Managed Task Lifecycle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Codex responsible for the complete rQuant delivery lifecycle while the user only participates in requirements, design, business acceptance, and explicit high-risk authorization.

**Architecture:** Put the detailed lifecycle contract in one canonical engineering document, then keep the mandatory rules synchronized in both `AGENTS.md` and `CLAUDE.md`. Extend the existing controlled-release document so successful delivery ends with safe cleanup, while this phase deliberately performs no cleanup of legacy branches or worktrees and adds no cleanup executable.

**Tech Stack:** Markdown governance documents, Git/GitHub workflow conventions, existing Python 3.11/3.12 CI and controlled production deployer.

---

### Task 1: Add the canonical managed lifecycle contract

**Files:**
- Create: `docs/engineering/task-lifecycle.md`

**Step 1: Define scope and responsibilities**

Document that the user owns requirements, design decisions, business acceptance for visible behavior, high-risk authorization, and the business decision to abandon otherwise unrecoverable work. Assign all Git, worktree, testing, PR, CI, merge, tag, deploy, rollback, verification, and cleanup operations to Codex.

**Step 2: Define naming and lifecycle states**

Use `cdx/YYYYMMDD-<kind>-<topic>` for Codex branches, `cc/YYYYMMDD-<kind>-<topic>` for Claude Code branches, and hyphen-only worktree directory names. Define `active`, `blocked`, `ready`, `merged`, `deployed`, `closed`, and `quarantined`.

**Step 3: Define safe cleanup evidence**

Require exact GitHub PR identity and merged status because squash merges invalidate ancestry-only checks. Require clean worktree state, no post-PR commits, no active lease, successful tag/deploy/health verification, and protection for main/runtime/legacy worktrees.

Until a lifecycle tool exists, require the PR lifecycle record and final receipt to be the manual source of truth; Task 2 will align `AGENTS.md` and `CLAUDE.md` with that protocol.

**Step 4: Define exception handling**

Forbid automatic `git reset --hard`, `git clean`, `git worktree remove --force`, age-based deletion, and deletion of unknown legacy work. State that this phase changes policy only and does not authorize cleanup of the existing backlog.

**Step 5: Validate reader-facing completeness**

Run:

```bash
rg -n "cdx/|cc/|quarantined|squash|worktree remove --force|存量" docs/engineering/task-lifecycle.md
```

Expected: every named contract area is present.

### Task 2: Make agent instructions authoritative and consistent

**Files:**
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `.gitignore`

**Step 1: Replace conflicting branch prefixes**

Replace the top-level `feat/*`, `fix/*`, and similar branch formats with the tool-prefixed convention while retaining change kinds and Conventional Commits.

**Step 2: Add mandatory delivery ownership**

Add a concise managed-lifecycle section linking to the canonical document. Require every new task to start from fresh `origin/main`, forbid feature development on local `main`, make delivery cleanup a mandatory task terminal state, and require quarantine instead of destructive cleanup when evidence is incomplete.

**Step 3: Move cloud validation execution to Codex**

Keep explicit user authorization for high-risk infrastructure changes, but require Codex to run `systemd-analyze` and related cloud verification and report the result instead of asking the user to execute shell commands.

**Step 4: Ignore Claude worktree contents safely**

Add `.claude/worktrees/` to `.gitignore` without ignoring all of `.claude/`.

**Step 5: Check mirrored instructions**

Run:

```bash
cmp AGENTS.md CLAUDE.md
```

Expected: exit code 0.

### Task 3: Close the controlled release workflow and record the change

**Files:**
- Modify: `docs/production-release.md`
- Modify: `CHANGELOG.md`

**Step 1: Add acceptance gates**

Separate user-visible business acceptance from purely technical maintenance. Keep infrastructure and production-data changes behind explicit authorization.

**Step 2: Add the delivery terminal state**

After successful merge and the release class's required tag, deployment, and health verification—or recorded N/A for every inapplicable requirement—require safe cleanup of the current managed task's remote branch, worktree, and local branch. If deployment or cleanup evidence fails, preserve the task as `merged` or `quarantined`; never declare it closed.

**Step 3: Record the governance change**

Add a concise `[Unreleased]` Changed entry without changing the application version.

**Step 4: Check policy links and terminology**

Run:

```bash
rg -n "task-lifecycle|业务验收|quarantined|worktree|清理" AGENTS.md CLAUDE.md docs/production-release.md CHANGELOG.md
```

Expected: the authoritative files use the same lifecycle vocabulary and link to the canonical contract.

### Task 4: Reader-test and verify the policy package

**Files:**
- Review: `docs/engineering/task-lifecycle.md`
- Review: `AGENTS.md`
- Review: `CLAUDE.md`
- Review: `docs/production-release.md`
- Review: `.gitignore`
- Review: `CHANGELOG.md`

**Step 1: Run mechanical validation**

Run:

```bash
git diff --check
cmp AGENTS.md CLAUDE.md
git status --short
```

Expected: no whitespace errors, mirrored instruction files match, and only planned files are changed.

**Step 2: Run a fresh-reader review**

Give a fresh subagent only the changed policy documents. Ask it to determine who performs Git operations, when business acceptance is required, how names are formed, how squash merges are verified, when cleanup is allowed, and what happens on uncertain or dirty work. Resolve every ambiguity it finds.

**Step 3: Review scope protection**

Confirm that no existing branch or worktree was removed, renamed, reset, cleaned, or otherwise modified, except creation and eventual cleanup of this task's own isolated worktree.

**Step 4: Reader scenarios**

Verify that a clean documentation/process task with a squash-merged PR and an auto-deleted remote head branch can record deployment N/A and enter `closed` from `merged` using retained PR evidence. Verify separately that a dirty worktree or unknown ignored file enters `quarantined` and cannot be closed or force-removed.

**Step 5: Commit intentionally**

Commit the policy package with:

```bash
git add AGENTS.md CLAUDE.md .gitignore CHANGELOG.md docs/engineering/task-lifecycle.md docs/production-release.md docs/plans/2026-08-04-managed-task-lifecycle.md
git commit -m "docs: define managed task lifecycle"
```
