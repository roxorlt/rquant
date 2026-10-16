# L2 downstream WebSocket implementation plan

**Goal:** Add cache-backed WebSocket delivery for trusted agents. Keep HTTP subscription management and cursor recovery. Preserve the first20 sustained acceptance → formal20 persistent deployment → controlled growth sequence.

**Architecture:** Add one standalone API route. Each connection reads the existing committed cache with its own cursor and bounded work. It never imports the collector or opens an upstream connection. No broker, Redis, memory fanout, cache format change, or new public listener is required.

**Tech stack:** Existing FastAPI, Uvicorn 0.46.0 and normal-runtime websockets 16.0 from uv.lock; Python 3.11+. Implementation uses native Codex agents in the one existing worktree. Baseline `86a65c1bce894de8510e69522f8f04b933a81f00`; branch `cdx/ths-l2-interface`.

The exact high-risk contract, trust model and review boundary are in the task's `evidence/rquant-l2-ws-spec.json`. SPEC admission precedes implementation. The existing source and real evidence remain historical until new review/CI/pins pass.

## A. Implement standalone delivery (original implementer B)

Write only `src/rquant/l2/api.py`, `tests/unit/test_l2_web.py`, `scripts/deploy-l2-component.py`, `tests/unit/test_l2_component_deploy.py`, `docs/l2-interface.md`, `docs/production-release.md`, and `CHANGELOG.md`.

1. Add behavioral red tests for `/api/v1/l2/stream`: existing BatchPage replay/follow, independent clients, HTTP recovery, cold-cache safety, parameters and browser-Origin rejection, disconnect cleanup, slow client isolation, fixed connection/inbound bounds, and no upstream/private/config/DB access.
2. Add the minimum route implementation. Default cursor0/limit1, maximum128/12MiB. Poll only local cache at0.2s; send empty status heartbeat every5s. Bound active connections to8 and each to one in-flight page, send timeout5s. Cache I/O must not block the event loop. Await in-flight work before releasing admission. Reject application input. Preserve fixed loopback8771.
3. Set the standalone server's locked WebSocket backend and inbound1024bytes/queue1. Extend only the directly affected CLI assertion. Do not change platform Web, collector, cache, models, units or upstream frequency.
4. Red-test the v3 deployment proof. Keep every v2 predicate. Require two actual WS cursor chains equal to the complete cache, nonempty messages in all three common-window segments, no failures/extra sends and complete client/task closure. Missing/failed WS evidence must block deployment.
5. Run focused tests, then the existing full L2 test list at the final candidate. Update neutral interface/release documentation and changelog. HTTP OpenAPI remains unchanged unless actual HTTP behavior changes; verify rather than regenerate needless differences.

## B. Extend the real acceptance harness (original implementer A)

Write only the two existing nonGit files `.tools/investigation/rquant-integration/validate_batch_candidate.py` and `test_validate_batch_candidate.py`, plus dedicated `ws-*` evidence logs/manifests.

1. Freeze the v3 section with B before implementing it. Retain the original two HTTP consumers and sole market-export/observation path.
2. Add two real WebSocket consumers. Reuse the existing cursor/hash checks. Each must consume nonempty forward batches in each third of the common90s window. Heartbeats do not count as data.
3. Keep `--stop-after-tier 20` default, the 1200-second budget, 300-second smoke rejection, one upstream connection/bootstrap, unified pacing and zero reconnect. Add WS failures to the whole-run blocking state.
4. Stop the collector and fix the final head. Drain all four consumers and compare complete chains. Close/join WS and HTTP clients before stopping API. Preserve raw FD ordering and truncation accounting.
5. Add targeted failures for WS gaps/hash/late-only delivery/failed client/drain/close. Retain all existing guards and stop20 regressions. Record valid red and green commands/logs, then freeze.

## C. Integrate, verify and publish (root)

1. Prepare only needed dependencies from the lock in an isolated environment; agents do not access network, credentials or production.
2. Use actual local TCP HTTP and WS with synthetic market appends. Verify two independent WS clients plus two HTTP clients, cursor recovery, hashes, cleanup and zero upstream calls. No phone or market session is needed.
3. Have the original reviewer cover the single frozen product/harness diff and SPEC failure paths. Original owners repair only findings in scope, max three directed rounds. Do not stack per-module reviews.
4. Freeze exact source/harness/review hashes. Push the same owned branch/PR437 under prior authorization and run exact full Web/L2 Python3.11/3.12 CI once. Merge main into this branch only if required by actual source-tree provenance; do not absorb unknown dirty worktrees.
5. Create new isolated cloud source/control pins. Verify locked Linux runtime and actual WS delivery using synthetic data, with no member input. Preserve old artifacts and evidence.
6. Update current neutral docs/OpenAPI/patch/checkpoints and existing one-shot Monday automation. It must use the new source and v3 gate. On Monday validate all60 real streams through both transports, then complete the formal20 deployment before any growth.

## R2 explanation boundary

The project has an existing R2 transport and L1 snapshot archive. L2 currently has persistent batch files, not a day archive or rotation. Recommend an independent after-close immutable-prefix export and manifest, followed by upload and restore/hash verification. Do not upload credentials, delete committed cache directories, change bucket lifecycle or activate another service in this WS change. R2 design does not block first20 delivery.
