# L2 Stock List Subscription Implementation Plan

> For native Codex implementers A/B under this root task. Use the existing owned worktree and OpenAI-only tools.

**Goal:** Accept stock-code lists through HTTP, serialize all market requests through one collector, measure capacity starting at20, and deploy only the independently verified service.

**Architecture:** Standalone loopback control API atomically appends to a shared desired set. The existing socket reader adds immutable three-stream bindings, applies one send-completion limiter, and publishes raw batches plus compact transition receipts. Independent consumers read the same bounded cursor API.

**Tech Stack:** Existing Python/Pydantic/FastAPI, flock+atomic JSON, immutable raw cache, systemd and guarded component deployer.

## Frozen scope

Base6e00aaa0a28bd742d692eb09c74f37f36701d47e, branch cdx/ths-l2-interface, checkout /private/tmp/cdx-rquant-l2-interface. High risk: concurrent target state, request/response identity, shared budget and service lifecycle. Full auditable threat model/SPEC is preserved with the task evidence as rquant-l2-batch-spec-rev1.json; reviewer admission is required before implementation. Root owns this plan file; do not alter or claim unknown dirty edits.

No public port, new account platform, upstream unsubscribe, distributed workers, production DB, automatic member renewal or stronger vendor event semantics. User requested initial20 then gradual capacity discovery.100 is a bounded experimental/code ceiling, not a provider maximum.

## Task1 — Core targets, protocol and scheduler (A)

Files: symbols.py/subscriptions.py/models.py/_request.py/_codec.py/cache.py/feed.py/__main__.py and focused corresponding tests under tests/unit.

1. RED: canonical symbol/market and bounded store union; concurrent additions, duplicate no-op, overcap/no partial commit, unsafe file/crash paths; API cannot raise capacity.
2. GREEN: shared pure symbols, typed target/update/receipt models, durable short-lock store and read-only capacity policy parser.
3. RED: third/new symbol's complete association, old cache compatibility, dynamic additions under continuous push, more than32 valid interleaved frames, deadline/resource limits and actual send-completion pacing.
4. GREEN: one reader/queue, stable page10117+10*slot and issued IDs, finite budget and no replay/rebind; drain during pacing; defaultzero retries.
5. RED/measure: bounded128-batch payload budget, contiguous cursor and historical prefix integrity, real I/O/latency under2k/20k small batches.
6. GREEN: minimum batching/performance change supported by measurements, transition-only runtime receipt; no DB/index framework.

Run with the existing /private/tmp/cdx-rquant-l2-testenv/bin/python. Capture real RED then focused GREEN with no network/private/.env/production access. Root reviews ownership/evidence before commit.

## Task2 — API, production component and capacity harness (B)

Files: api.py/new subscription_api.py, only web/l2.py sharedlimit parameter, focused API/web tests, component helper and tests, two units, existing workflow, current interface/release docs/OpenAPI/changelog, new investigate-only validate_batch_candidate.py.

1. RED: POST additive lists,200 duplicate/202 durable add/422 wholeinvalid/429 wholeovercap; simultaneous clients, corrupt/unsafe store, cache-only GET and no auth/upstream IO.
2. GREEN: explicit control-root router inclusion, typed responses, no public control via existing platformrouter; compact current-epoch receipt.
3. RED: dynamic3N acceptance/source/policy/seed gate; missingstream, legacyproof, oldepoch, mappedpeer/foreignwriter and rollback preservation.
4. GREEN: capacity.json and initial20 targets through solecomponent entry, bounded registration health, approvedsame2units permissions; preserve formalmain/pushCI/release gates.
5. Add a new hash-gated capacity harness; keep every historical harness/evidence immutable. Offline selftests must reject source/policy/proof/hash/capacity mismatches before real input IO.
6. Update only direct docs and CI selections for new focusedtests. Static units must be verified byroot on actual cloud beforepush.

A owns the shared store/models interfaces in the SPEC. B may proceed on independent files and mocks but runs final real-store integration only after A freezes that implementation. No shared-file concurrent writers.

## Task3 — Final review and exact CI (root + originalreviewer)

Freeze an exact commit/diff and file map. One independent final review covers the delta and direct dependencies/failure model. Original implementer repairs original findings; maxthree targeted rounds. Reuse valid closed codec/carrier work. Run full Web MVP+L2 3.11/3.12 once for the actual candidate, changing it invalidates relevant evidence. Public PR437 remains the single release vehicle.

## Task4 — Actual capacity and deployment (root only)

Preflight actual socket/systemd/tool/disk capability. Each reviewed candidate test uses one memberbootstrap/socket, pages1, <=301 sends, >=1second after each business send completes, no auto reconnect, <=1200seconds. Initial20 then40/80/100 only on preceding verified stable window. Every stream has its own actualseed, independent postseed ID baseline/window/last60 evidence; old streams must continue after additions. Duplicate/concurrent HTTP POST and multiple cursor consumers must not add upstream sends. Record latency/lag/CPU/RSS/disk and all hashes; bounded raw prefix is not full-run counting evidence.

At closed market only registration/control/cache checks can pass. Monday09:15 preflight proceeds with >=09:30 actual continuous-session capacity run; no manual phone/terminal chores and no extra recurrence. Insufficient events are not a discovered provider limit or successful continuous feed.

Deploy only an initial20-or-higher fully verified capacity through exact main/tag/full pushCI/formal release and the component helper. policymax<=verifiedtier, initialproductiondesired first20 verified symbols, preserve existingWeb/dirtycheckout/database. Actual SSH-after-disconnect, enabled/PID/version and dynamicAPI/cursor tests substantiate persistent operation; stop temporary tests, retain actualproduction services, publish truthful URL and capacity evidence.
