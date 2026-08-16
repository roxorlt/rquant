# ADR-PIA-001: Root-Owned Bootstrap And Immutable Generations

**Status:** Revised and frozen for implementation. This decision supersedes the global
interpreter FD authority described by the earlier draft. The filename is retained until the
migration is complete.

**Decision:** HYBRID A+C: fixed root-owned bootstrap/runtime wrappers and profile (A), combined
with root-quarantined, completely hashed immutable generations and one atomic runtime authority
record (C). Artifact signing is not part of v1.

**WRAP-P1-07 amendment:** The production `daily` role is frozen below before wrapper or adapter
code. This amendment resolves the role/argument/environment contradiction in the original frozen
text; all other decisions, including the v1 source-authenticity exclusion, remain unchanged.

**Approved structural baseline:** `WRAP-DESIGN-P1-01` through `WRAP-DESIGN-P1-04` freeze the
application-path authority, list-value grammar, signal-envelope family identity, and dependency
order below. `WRAP-DESIGN-P1-03` now uses the independently approved structural baseline below,
which supersedes the prior shared-version proposal without changing the HYBRID A+C threat model or
the other three design closures.

## Scope And Threat Model

### Assets

- The bytes executed during privileged publication, bootstrap, and systemd service startup.
- Each complete production generation: application code, dependencies, generation Python,
  `pyvenv.cfg`, resources, metadata, and manifest.
- The root-recomputed `full_manifest_hash` that exclusively identifies each published generation.
- `/var/lib/rquant/runtime-authority/current.json`, rollback generations, and audit evidence.
- Root privilege exposed by the narrowly authorized publication helper.
- Production availability across interrupted copy, publish, record update, restart, and rollback.
- The exact application-path and emitted-environment contract presented to the HYBRID `daily`
  process.
- The producer identity persisted in HYBRID daily signals, error outbox rows, and evidence.

Source authenticity is explicitly not a v1 asset. A candidate's claimed commit/tag and manifest
are untrusted audit metadata. User authorization and GitHub delivery express operational intent;
they are not evidence accepted by the root helper and do not participate in its safety proof.

### Trust Boundaries

1. The `lighthouse` account, checkout, environment, argv, writable venvs, candidate manifest, and
   candidate files are hostile inputs at the root helper boundary.
2. `/usr/bin/python3.11`, the fixed root-owned pyz files, the runtime profile, the immutable
   generation store, and their root-owned non-writable ancestors form the local trusted boundary.
3. A candidate becomes immutable production content only after the root helper copies it into a
   root-owned quarantine, closes the copied tree, reopens that tree through directory FDs, and
   independently derives its complete manifest and content address before atomic publication.
4. `current.json` is the sole mutable runtime authority. A root-owned wrapper must revalidate the
   record and named generation before launching an allowlisted role as `lighthouse`.
5. The root helper trusts only metadata and hashes it recomputes from the root-owned quarantine.
   Candidate manifest/commit fields remain opaque audit strings and cannot grant privileges,
   choose root execution, weaken policy, or determine generation identity.
6. Mutable application data beneath the frozen `lighthouse` data and log roots is outside the
   trusted-code TCB. Its paths are strict application inputs, never interpreter, profile,
   generation, authority, cwd, module, or import authority.

A complete compromise of the `lighthouse` UID is not in scope for protecting that UID's own
checkout, venv, or processes. That UID remains an actively malicious client when it supplies
requests or bytes to a root helper. No `lighthouse`-writable byte may be imported, executed, or
trusted by root. A malicious accepted generation is executed only later with UID `lighthouse`.

### Attack And Failure Paths

- Environment, `PATH`, Python variables, loader variables, argv, or working-directory injection
  selects an alternate interpreter, profile, wrapper, module, or generation.
- An application path uses an alias, symlink, nonexistent or wrong leaf, sibling escape, mutable
  trust-store overlap, or variable-to-path substitution to acquire code/runtime authority.
- A new daily success or error signal truncates a generation hash, reads Git identity from the
  environment, fabricates a zero commit, or omits the active producer identity from `signal_id`.
- A source entry changes type or identity during traversal, or uses symlinks, hard links, special
  files, path traversal, duplicate names, or mutable ancestors.
- Root imports or executes candidate Python while inspecting or copying it.
- Candidate code, dependencies, `pyvenv.cfg`, resources, or manifest change after validation.
- A crash leaves a partial quarantine, non-durable generation, partial authority temp, ambiguous
  `current.json`, or an attempted generation selected after restart failure.
- A service imports through a mutable checkout, `current` symlink, `PYTHONPATH`, user site, or
  `EnvironmentFile` after preflight validated different bytes.
- `preexec_fn` executes Python after fork, or interpreter/resource descriptors leak to descendants.
- A pure-Python process overstates its ability to attest the interpreter that started it.

### Invariants

- Production bootstrap and runtime wrappers use exactly `/usr/bin/python3.11 -I -S`; the file,
  required loader/libraries/stdlib, and all ancestors are root-owned and non-writable by
  `lighthouse`. The Python file has exact owner `root:root` and mode `0555`.
- Production Python processes execute with UID `lighthouse`, including the fixed bootstrap and
  runtime wrapper. Only the narrow quarantine/publication transaction runs with root privilege.
- Bootstrap and runtime pyz files are regular root-owned single-link files with exact mode `0555`.
  `/etc/rquant/production-runtime-profile.json` is a regular root-owned single-link file with exact
  mode `0444`. Their ancestors are root-owned and group/other non-writable.
- The shell accepts only strict business arguments. It cannot pass a path, command, interpreter,
  module, profile, wrapper, import root, release root, or environment override to the root helper.
- The root helper derives every source and destination beneath fixed profile roots. It exposes no
  arbitrary path or command parameter and never imports or executes candidate content.
- Every published generation was copied into root-owned quarantine, durably closed, and then
  independently rehashed from the root-owned FD tree. Every published file is root-owned with its
  exact manifest mode and is not writable by `lighthouse`.
- The root-recomputed v1 manifest covers all generation entries and binds the profile ID, file
  type, relative path, owner UID, exact mode, size, and SHA256. `pyvenv.cfg`, the generation
  Python, dependencies, code, resources, and metadata are mandatory covered entries. Claimed
  commit and candidate-manifest values are audit metadata outside `full_manifest_hash` identity.
- `current.json` is the only runtime authority record. Its atomic replacement and generation
  publication are fsync-complete before success is reported; it carries complete current and
  prior generation slots so one atomic write contains both forward and rollback authority.
- Systemd always starts the fixed root-owned runtime wrapper. The wrapper revalidates one exact
  generation and executes only an allowlisted role from that generation.
- The production `daily` role maps only to generation-local
  `rquant.production_daily_main`. It accepts no caller arguments or business overrides and runs
  only with the exact application environment policy frozen by the profile.
- The profile freezes exact application data/log roots and exact variable-to-path mappings. Path
  validation is anchored and no application path can overlap or select trusted-code storage.
- After the writer-cutover gate, every normal signal write uses the strict
  `rquant.signal-envelope/v1` family. A HYBRID signal binds the exact current
  `full_manifest_hash` through a revalidated authority capability as its sole producer identity;
  Git/audit metadata and raw identity strings are never substituted.
- No production path uses `preexec_fn`, a global interpreter FD authority, mutable `current`
  symlinks, checkout imports, pathname fallback, or unintended inherited FDs.
- Unsupported host facts, failed validation, incomplete durability, ambiguous recovery, and
  failed rollback are blocking failures. Skip or inability to inspect is not a pass.

### Exclusions

- Protecting `lighthouse` files or processes after complete compromise of that same UID.
- Defending against a malicious kernel/VFS, systemd, sudo implementation, root administrator,
  or root-owned Python/loader/stdlib/shared libraries.
- Proving candidate source authenticity, commit/tag provenance, GitHub identity, or correspondence
  between claimed audit metadata and the bytes received.
- Artifact signing, artifact transparency, offline signing keys, or signature verification in v1.
- Proving the integrity of an already-running Python process using only that process.
- Application database integrity, secret rotation, service sandbox redesign, trading behavior,
  or executing directly from a developer checkout.
- Trusting the mutable contents of `lighthouse` application data/log roots as executable code or
  extending root ownership/non-writability requirements to those application-write locations.

### Blocking Scope

Production deploy and service-start wiring remain blocked until quarantine copying, complete
generation verification, single-record recovery, the exact runtime wrapper, signal-family dual
reading, reset Phase A R07 decoding, reset Phase B successor contracts, reset Phase C trusted
readiness, a separately authorized activation amendment, and Linux/cloud hard gates are complete. Daily
adapter/wrapper work is additionally blocked on the path, environment, and R01-R12 contract tests
below. Root-owned installation, systemd/sudoers changes, and production deployment remain
separately authorized infrastructure operations. This ADR does not grant that authority.

## Trusted Computing Base

The residual TCB is:

- kernel and VFS semantics, mount policy, systemd, sudo, and root administration;
- `/usr/bin/python3.11`, required root-owned dynamic loader, standard library, shared libraries,
  and every ancestor from their trusted filesystem anchors;
- `/usr/local/libexec/rquant-production-deploy.pyz`, owner `root:root`, exact mode `0555`;
- `/usr/local/libexec/rquant-runtime-exec.pyz`, owner `root:root`, exact mode `0555`;
- `/etc/rquant/production-runtime-profile.json`, owner `root:root`, exact mode `0444`;
- `/etc/rquant/rquant-daily.env`, a fixed regular single-link `root:root` application-value file
  with exact mode `0400`, whose names and values are filtered by the runtime wrapper; and
- the narrow root-owned quarantine/publication helper and immutable generation store.

The versioned profile freezes a canonical file list for `/usr/bin/python3.11`, its ELF interpreter,
standard library, every loaded shared library, both pyz files, and every required ancestor. Each
file entry binds canonical absolute path, SHA256, exact owner and mode; each ancestor entry binds
canonical path, owner, mode, directory type, and non-writability. Before installation and at the
cloud hard gate, the same fields are recomputed and compared exactly.

Any real path, mode, owner, hash, ELF-loader, stdlib, shared-library closure, ancestor, or pyz
change requires a new ADR/profile version plus explicitly authorized infrastructure publication.
Installation and runtime must fail closed; the runtime profile cannot relax hard-coded minimum
schema, root ownership, exact-mode, ancestry, or no-override policy. The installer never discovers
or substitutes another Python or dynamically broadens the closure.

The profile is non-secret strict JSON. It freezes schema/platform, profile ID, the complete runtime
closure above, generation/inbox/quarantine roots, allowed operations, exact role-to-module/cwd/
import mappings, each role's exact environment name-to-grammar policy, and manifest schema. A bare
name tuple is insufficient: every permitted environment name binds required/optional presence,
grammar class, and v1 bounds. Unknown, duplicate, malformed, non-native scalar, or overridden
values are rejected. There is no GID alternative, `{root, euid}` policy, environment override,
argv override, wildcard closure, or runtime-discovered exception.

Profile schema v1 also freezes `application_data_root` as exactly
`/home/lighthouse/rquant/data` and `application_log_root` as exactly
`/home/lighthouse/rquant/logs`; both fields participate in `profile_id`. They are application-write
boundaries, not trusted-code roots. Their ownership and mutability are therefore not promoted into
the root-owned code TCB, but their spelling, type, identity, and separation from every trusted root
are validated as specified below.

## Initial Entry And Environment

The only production bootstrap form is:

```text
/usr/bin/python3.11 -I -S \
  /usr/local/libexec/rquant-production-deploy.pyz <strict-business-arguments>
```

It runs with UID `lighthouse`. The shell is a minimal argument transport: it does not source an
environment file, import checkout code, inspect a venv, search `PATH`, or accept
`RQUANT_DEPLOY_PYTHON` or any equivalent path/module/profile override. The pyz loads only the fixed
profile and converts strict business arguments into an allowlisted operation request.

Bootstrap and runtime wrappers construct a fresh environment from the profile allowlist. They do
not preserve `PATH`, any `PYTHON*`, any `LD_*`, user-site controls, caller import paths, or caller
working-directory influence. Locale/timezone and explicitly named application values may be
copied only when their names and value grammar are allowlisted. Absolute paths are used for every
exec. `-I -S` and the generation-local installed package layout determine imports.

The initial OS launcher remains residual TCB. The pyz does not claim to prove the integrity of its
own already-running Python process.

## WRAP-P1-07 Daily Role And Application Environment

The profile contains exactly one production mapping for this service:

```text
role daily -> module rquant.production_daily_main -> caller argv count 0
```

The adapter is generation-local and manifest-covered. The frozen bootstrap receives only the
literal role `daily`; immediately before `runpy` it sets module-visible `sys.argv` to exactly
`["rquant.production_daily_main"]`. The adapter rejects any other `sys.argv` shape and exposes no
argument parser. It computes the trade date from the current date in `Asia/Shanghai`, selects all
presets, enables ingestion, and disables minute backfill, equivalent to
`rquant run-daily --skip-minute-backfill`. A fixed `90` may remain only as an inert compatibility
default on a skipped minute-backfill path; callers cannot supply or alter it. The adapter may call
an existing typed or CLI function internally with these fixed values, but accepts no checkout,
manifest path, control root, commit, generation, module, cwd, environment map, or other business
override.

The adapter runs only after the immutable current generation is fully revalidated. It derives
runtime/code identity solely from the current authority slot and root-recomputed
`full_manifest_hash`, never from a Git checkout. Candidate commit remains audit-only and cannot
become code identity. Dotenv loading is disabled for this path; neither a generation `.env` nor
`/home/lighthouse/rquant/.env` is read.

The `daily` child environment contains no names outside this exact v1 set:

```text
LANG
LC_ALL
TZ
TUSHARE_TOKEN_MAIN
TUSHARE_TOKEN_BACKUP
DATA_DIR
DUCKDB_PATH
DUCKDB_READONLY_PATH
PARQUET_DIR
LOG_DIR
LOG_LEVEL
APP_ENV
PUSHDEER_KEYS
PUSHDEER_RECIPIENT_IDS
PUSHDEER_ENDPOINT
PUSHPLUS_TOKENS
PUSHPLUS_RECIPIENT_IDS
PUSHPLUS_ENDPOINT
NOTIFICATION_STATE_PATH
NOTIFICATION_STATE_BUSY_TIMEOUT_MS
NOTIFY_ENABLED
NOTIFY_DAILY_SUMMARY
NOTIFY_ERROR
NOTIFY_ERROR_COOLDOWN_SECONDS
NOTIFY_OPS_COOLDOWN_SECONDS
POOL2_MAX_AGE_DAYS
```

The profile binds each name to the following grammar and presence rule. Every raw value is strict
UTF-8 and is validated without trimming, case conversion, item dropping, or other repair. A
required scalar rejects both absence and the empty string. An optional scalar maps absence or the
empty string to one canonical absent value, which is not emitted, and otherwise requires its exact
canonical representation. No secret value is logged or placed in the profile or authority record.

| Class | Names and presence | Frozen v1 grammar |
|---|---|---|
| Locale | `LANG`, `LC_ALL` optional; `TZ` required | locale is exactly `C`, `C.UTF-8`, `en_US.UTF-8`, or `zh_CN.UTF-8`; timezone is exactly `Asia/Shanghai` or `UTC` |
| Tushare secret | `TUSHARE_TOKEN_MAIN` required; `TUSHARE_TOKEN_BACKUP` optional | printable non-control UTF-8, 32-512 bytes, no NUL/newline or leading/trailing whitespace |
| Application path | exact presence and values are frozen below | exact literal mapping plus anchored type/identity validation; there is no general path grammar |
| Logging/runtime | `LOG_LEVEL`, `APP_ENV` required | level is exactly `DEBUG`, `INFO`, `WARNING`, or `ERROR`; environment is exactly `prod` |
| Channel secrets | `PUSHDEER_KEYS`, `PUSHPLUS_TOKENS` optional | raw comma-separated list of 1-16 items; each item is 1-512 bytes of visible ASCII `0x21..0x7e` excluding comma; serialized value is at most 8192 bytes |
| Recipient IDs | corresponding `*_RECIPIENT_IDS` conditionally required | raw comma-separated list with the channel-list rules; each item matches `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`; at most 16 items and count exactly equals the corresponding key/token count |
| Endpoints | corresponding `*_ENDPOINT` optional only when its key/token list is present | absence selects that channel's profile-frozen canonical service default; a supplied value follows the exact canonical HTTPS grammar below and is at most 2048 ASCII bytes |
| Boolean | `NOTIFY_ENABLED`, `NOTIFY_DAILY_SUMMARY`, `NOTIFY_ERROR` required | exactly lowercase `true` or `false` |
| Integer | `NOTIFICATION_STATE_BUSY_TIMEOUT_MS`, both `NOTIFY_*_COOLDOWN_SECONDS`, and `POOL2_MAX_AGE_DAYS` required | strict unsigned decimal, no sign or leading zero except `0`; timeout 1-600000 ms, cooldown 0-86400 seconds, pool age 1-365 days |

For both channel-secret variables, a nonempty raw CSV rejects leading/trailing ASCII or Unicode
whitespace, every control or non-ASCII character, a leading/trailing comma, and every empty
interior item. Parsing never trims or silently drops an item. A recipient variable and endpoint
must be absent when the corresponding key/token list is absent. When that list is present, the
recipient variable is required and has exactly the same item count; an absent or empty endpoint
selects the frozen default before emission. A supplied endpoint is exactly
`https://<host><path>`: lowercase `https`, no userinfo/port/query/fragment, a lowercase ASCII DNS
host of at most 253 bytes with no empty label or trailing dot, and an absolute ASCII path matching
`/(?:[A-Za-z0-9._~-]+(?:/[A-Za-z0-9._~-]+)*)?` with no `.` or `..` segment. Percent escapes,
Unicode host aliases, repeated slashes, and noncanonical spellings are rejected. Both frozen
channel defaults are profile fields and participate in `profile_id`.

After optional omissions and endpoint-default insertion, the emitted-environment budget is exactly
`sum(len(NAME.encode("ascii")) + 1 + len(VALUE.encode("utf-8")) + 1) <= 65536`; the two added bytes
are `=` and the terminating NUL for each entry. Validation completes before constructing or
launching the child.

The profile's application mapping is exact:

```text
application_data_root = /home/lighthouse/rquant/data
application_log_root = /home/lighthouse/rquant/logs

required DATA_DIR = /home/lighthouse/rquant/data
required DUCKDB_PATH = /home/lighthouse/rquant/data/rquant.duckdb
optional DUCKDB_READONLY_PATH = absent | /home/lighthouse/rquant/data/rquant_ro.duckdb
required PARQUET_DIR = /home/lighthouse/rquant/data/parquet
required LOG_DIR = /home/lighthouse/rquant/logs
optional NOTIFICATION_STATE_PATH = absent | /home/lighthouse/rquant/data/notification_state.sqlite3
```

For either optional path, raw absence or the empty string canonicalizes to absence; every nonempty
value must equal the listed path byte-for-byte. Before environment emission, the wrapper opens a
fixed `/` directory FD and walks every nonempty component root-to-leaf with `lstat`/`fstatat`
no-follow checks and `openat(O_NOFOLLOW | O_CLOEXEC)`; directory components also use
`O_DIRECTORY`. It compares path and opened-FD device/inode/type identity at each step and again
before exec. The configured roots cannot be `/`, relative, textual aliases, or contain empty,
`.`/`..`, tilde, NUL, newline, or redundant-separator components. The two roots and every
directory-valued mapping must already exist as canonical directories. Existing database/state
file leaves must be regular files with `nlink == 1`; an absent file leaf is permitted only beneath
its already verified existing parent.

Containment and overlap use whole path components, not string prefixes. The application roots must
be disjoint from each other and must neither equal, contain, nor be contained by the fixed profile,
authority, generation, quarantine, or inbox roots. Symlink aliases, sibling escapes, wrong
variable-to-path mappings, and identity changes during the walk reject the launch. These mutable
paths are expected to be writable by `lighthouse`; validation deliberately does not require root
ownership or root-style non-writability and never treats their contents as trusted code.

The application-path contract has these mandatory red-test rows:

| Fixture | Expected result |
|---|---|
| Exact two roots and exact directory mappings exist; each file leaf is absent under its verified parent or is regular with `nlink == 1` | Accept path contract |
| Either optional path is missing or empty | Canonicalize to one absent representation and omit it |
| Any required path is missing/empty, or any variable is swapped/rebound to another allowlisted path | Reject before child construction |
| Root is `/`, relative, tilde-based, redundantly separated, dot/dot-dot-bearing, or a sibling/prefix-confusion escape | Reject before or during anchored walk |
| Data/log root, `PARQUET_DIR`, or a required parent is nonexistent or not a directory | Reject; an absent file leaf does not excuse an absent parent |
| Any ancestor, root, directory leaf, or existing file leaf is a symlink/alias | Reject even when it resolves to the expected spelling |
| An existing file leaf is a directory, hard link (`nlink != 1`), FIFO, socket, device, or other special file | Reject |
| A component is replaced between pathname check, FD open, identity check, or pre-exec recheck | Reject and close every opened FD |
| Either application root overlaps the profile/authority/generation/quarantine/inbox root in either direction | Reject |
| Ordinary mutable fixture roots owned/writable by the runtime UID satisfy all path/type rules | Accept without claiming root-owned trusted-code evidence |

The scalar/list/environment contract has these mandatory red-test rows:

| Boundary | Accept | Reject |
|---|---|---|
| Scalar presence | optional absent/empty becomes absence; required canonical nonempty value | required absent/empty; optional noncanonical nonempty value |
| Backup token | absent/empty `TUSHARE_TOKEN_BACKUP` | invalid nonempty backup or empty required `TUSHARE_TOKEN_MAIN` |
| Channel item text | visible ASCII non-comma item at exactly 1 or 512 bytes | 0 or 513 bytes; comma in item; ASCII/Unicode whitespace; control; non-ASCII |
| CSV shape | exactly 1 or 16 nonempty items and value size exactly 8192 bytes when otherwise valid | leading/trailing comma, interior empty item, 17 items, or 8193 bytes |
| Recipient | 1 or 64 bytes matching the exact regex; count equals key/token count | 65 bytes, bad first/other character, count mismatch, recipient present without key/token list |
| Endpoint | absent/empty with a present key/token list selects the frozen default; supplied canonical URL at exactly 2048 bytes | supplied without key/token list; 2049 bytes; uppercase/noncanonical host or scheme; port, userinfo, query, fragment, bad/dot path segment, percent/Unicode alias |
| Representation | exact raw canonical spelling | any value that would become valid only by trimming, case-folding, normalization, dropping, or rewriting |
| Aggregate emitted environment | sum of UTF-8 bytes of each `NAME=value` plus one NUL per entry is exactly 65536 | the same sum is 65537; names, `=`, NULs, and synthesized endpoint defaults all count |

Application paths affect only `lighthouse`-owned application data. They can never select or alter
an interpreter, profile, authority record, role, module, cwd, import root, generation, or
manifest. `PATH`, `HOME`, every `PYTHON*`, every `LD_*`, `RQUANT_CODE_COMMIT`, Tushare login
username/password/cookie fields, deploy/Lab/LLM/panorama/backup/upload variables, and arbitrary
`RQUANT_*` names are explicitly excluded and never copied to the child.

Production systemd may source application values only from the fixed
`/etc/rquant/rquant-daily.env`, a regular single-link `root:root` file with exact mode `0400` and
root-owned non-writable ancestors. The wrapper discards every non-allowlisted incoming name and
independently validates every copied value against the profile grammar before exec. It never loads
checkout `/home/lighthouse/rquant/.env`. This decision does not authorize installing or changing
the fixed file; secret rotation remains excluded and separately authorized. Any v1 name,
presence rule, grammar, or bound change requires a new profile/ADR version.

## Signal Envelope Families And Producer Identity

`WRAP-DESIGN-P1-03` uses two structurally disjoint families. There is no shared-version migration
model and no parse-then-coerce path between families.

### LegacySignalEnvelope: Permanently Read Only

`LegacySignalEnvelope` is the exact historical root shape. Its `schema_version` is any native
integer `>= 1`, and `producer_commit` is exactly 40 lowercase hexadecimal characters, including
the historical all-zero sentinel. `envelope_schema`, `producer_identity`, and
`producer_generation_id` are absent. The strict legacy parser rejects unknown/mixed fields but
otherwise preserves the historical schema-version-specific contract.

Legacy parsing preserves the original canonical bytes, legacy `signal_id` algorithm, and stored
ID exactly. It never rewrites, reserializes, upgrades, normalizes, or invents producer identity.
The read view adds one nonserialized status: `legacy_commit_claim` for a nonzero commit claim or
`legacy_zero_sentinel` for the all-zero value. Neither status is authoritative provenance and
neither can select HYBRID code/runtime identity.

Normal write APIs reject `LegacySignalEnvelope` objects. During the reader-only rollout release,
already deployed legacy writers may continue using their unchanged old API while new-family write
APIs remain disabled. Compatibility copying is not a new write: it may reuse only already-durable
canonical bytes whose store cursor is at or below the frozen legacy high-watermark, with the same
stored `signal_id` and byte hash. It may not materialize a legacy model into new JSON.

### New Signal Envelope Family

Every new-family root contains exactly:

```text
envelope_schema = rquant.signal-envelope/v1
producer_identity = one strict discriminated object
```

The `producer_identity` object is exactly one of:

```json
{"kind":"git-commit-claim-sha1/v1","producer_commit":"<nonzero 40 lowercase hex>"}
{"kind":"full-manifest-sha256/v1","producer_generation_id":"<nonzero 64 lowercase hex>"}
```

The new root omits `schema_version`, root `producer_commit`, root `producer_generation_id`, and all
inactive identity fields. Null is not an absent representation. The Git form remains explicitly a
claim and does not add source authenticity to the HYBRID threat model.

The new `signal_id` hashes the unchanged common identity fields (`strategy_id`,
`strategy_version`, `parameter_fingerprint`, `dataset_snapshot_id`, `feature_snapshot_id`,
`event_time`, `available_at`, `candidate_id`, `action`, `reason_codes`, `evidence`, and
`expires_at`) plus the exact `envelope_schema` and complete canonical `producer_identity` object.
The identity kind and field name therefore participate in the hash; no inactive/null field does.

Dispatch is structural and strict:

1. Absent `envelope_schema` dispatches only to strict `LegacySignalEnvelope`; legacy-required
   fields must be present and every new-family or mixed key is rejected.
2. Exact `envelope_schema = rquant.signal-envelope/v1` dispatches only to the strict new parser;
   the legacy root keys and every inactive/null identity field are rejected.
3. Unknown `envelope_schema`, unknown identity `kind`, both/neither identity values, malformed or
   all-zero identity values, extra fields, and objects satisfying neither family are rejected.

Normal writers accept only the new family. The HYBRID builder accepts an opaque revalidated
current-generation capability, never a raw hash/string, environment value, audit commit, or caller
path. The capability binds the current authority operation, profile, generation, and exact
`full_manifest_hash`; the persistence boundary rechecks that the authority still names that same
generation. Missing, stale, or changed authority produces no signal or outbox row. The current
`cli.py::_daily_notification_producer_commit` zero fallback is forbidden for every new writer and
is not HYBRID evidence; rows it already durably produced remain readable legacy history.

Canonical envelope JSON is already stored and outbox rows reference `signal_id`, so this family
split requires no SQLite schema migration. Existing `runtime.signal_route.spool-record` v2 bytes
and hashes remain byte-for-byte unchanged; spool v3 is emitted only for the new family and hashes
its exact new canonical envelope bytes.

The architecture-reset R01-R12 red tests are mandatory:

| ID | Frozen red-test evidence |
|---|---|
| R01 | Legacy schema versions 1, 2, and 3 with zero and nonzero 40-hex commits read and byte-roundtrip with exact original canonical bytes, legacy `signal_id`, stored ID, and nonserialized status; no serializer/migration runs. |
| R02 | A legacy row with mismatched stored `signal_id` is rejected; every normal write/canonical-serialize API rejects every `LegacySignalEnvelope`. |
| R03 | Each valid new identity variant is accepted; malformed length/case/hex, zero value, both/neither identity value, inactive/null field, and extra identity field are rejected. |
| R04 | Unknown `envelope_schema`/identity kind, mixed legacy/new root keys, and objects satisfying neither family are rejected by the explicit dispatcher. |
| R05 | Otherwise identical envelopes with Git versus manifest identity, or with distinct active identity values/kinds, have distinct deterministic new `signal_id` values. |
| R06 | Every direct and nested consumer reads historical rows through the legacy branch and new rows through the new branch; no consumer silently coerces, drops, or fabricates identity. |
| R07 | Existing spool v2 canonical bytes/hash remain exact for legacy replay; only a new-family envelope can emit spool v3, whose hash covers its exact canonical bytes. |
| R08 | Reader-only release leaves deployed legacy writers unchanged while new writers are gated; after cutover, normal APIs accept only the new family and no legacy/zero new write succeeds. |
| R09 | After writer cutover, a CLI failure without a valid new-family authority capability emits typed degraded health and creates no signal/outbox row; it never falls back to 40 zeroes. |
| R10 | HYBRID daily success and injected-error rows contain the exact capability-bound current `full_manifest_hash`; Git/env/audit/zero mutations do not affect identity. |
| R11 | High-watermark replay copies only already-durable legacy canonical bytes at or below the frozen cursor with exact stored IDs/hashes; above-boundary or reserialized legacy input is rejected. |
| R12 | Authority absence or change between capability creation and persistence fails closed with no signal/outbox row; retry requires a newly revalidated capability. |

## Signal-Family Architecture Reset: R07 Before Registry

This section is the sole active ordering for signal-family transport work. It replaces the former
overlay-first design and governs any conflicting rollout language later in this ADR. R01-R12
behavioral decisions remain frozen, but their implementation is now split into Phase A, Phase B,
and Phase C. Phase A grants only model/decoder work. No phase grants high-watermark capture, drain,
cutover, or activation.

### Phase A: R07 Pure Model And Decoder

Phase A has no registry successor, overlay, receipt, readiness, capability, or activation path. Its
only production-code deliverables are future strict models, canonical decoders, a read-only
fixture/snapshot verifier, and synthetic tests.

#### Future CurrentSignalBusRoutedRecord

The future `CurrentSignalBusRoutedRecord` is an exact, non-self-hashing model with only these fields:

```text
global_sequence: strict native int >= 1
signal_id: lowercase SHA-256
envelope_hash: lowercase SHA-256
payload_json: nonempty str
envelope: exact CurrentSignalEnvelope
received_at: aware UTC datetime
receipt: exact SignalRouteReceipt
```

The model rejects mappings in place of the exact nested models, subclasses, booleans or coerced
numbers, extra fields, duplicate JSON keys, non-UTC or naive datetimes, and every unknown envelope
family. It enforces
`signal_id == envelope.signal_id == receipt.signal_id`. Let
`E = current_signal_envelope_json_bytes(envelope)`; the UTF-8 bytes of `payload_json` equal `E`
byte-for-byte and `envelope_hash = SHA256(E)`. The record contains no routed hash, record hash,
chain hash, serializer-selected family, or other self-authentication field.

#### Future CurrentSignalRouteSpoolRecord

The future `CurrentSignalRouteSpoolRecord` is the outer v3 chain record and has only:

```text
schema_version: strict native int == 3
global_sequence: strict native int >= 1
previous_record_hash: null or lowercase SHA-256
envelope_hash: lowercase SHA-256
routed_record_hash: lowercase SHA-256
record_hash: lowercase SHA-256
record: exact CurrentSignalBusRoutedRecord
```

It rejects model subclasses, mappings substituted for `record`, extra fields, duplicate keys,
booleans, floats, strings or other integer coercions, and noncanonical JSON. Its
`global_sequence` and `envelope_hash` equal the corresponding fields in `record`.

All v3 canonical bytes use `rquant.strict_json.canonical_json_bytes`: UTF-8, sorted keys, compact
separators, `ensure_ascii=False`, `allow_nan=False`, and no trailing newline. Let
`R = canonical_json_bytes(record.model_dump(mode="json"))`;
`routed_record_hash = SHA256(R)`. The outer record-hash preimage is
`canonical_json_bytes` of all outer fields except `record_hash`, including the complete
`record`; `record_hash` is SHA-256 of that preimage. The record file is
`canonical_json_bytes` of the complete outer model including `record_hash`.

#### V2 Differential Compatibility And Dispatch

Every existing v2 function, model, parser, byte fixture, hash, exception category, sequence
diagnostic, and `ensure_ascii=True` encoding remains byte- and behavior-identical. In particular,
the future structural dispatcher first calls the untouched
`rquant.signal_route_spool._parse_record(raw_bytes, sequence=sequence)`. Only after that legacy
call rejects may it try the strict v3 decoder. If v3 also rejects, the externally visible failure
preserves the original legacy `SignalRouteSpoolIntegrityError` category and sequence text; v3
detail is subordinate diagnostic evidence and cannot redefine legacy failure behavior.

A frozen differential corpus is mandatory before the dispatcher changes. It contains canonical
valid v2 snapshots plus invalid v2 cases for schema, hash, sequence, chain, duplicate key, extra
field, coercion, whitespace, Unicode escaping, datetime spelling, `NaN`/infinity, newline, and
truncation. The old parser and candidate dispatcher MUST produce identical acceptance, model,
canonical bytes, hashes, and public failure category/sequence for every corpus entry.

The existing `SignalRouteSpoolPointer` remains schema v2, byte-identical, and family-neutral.
It never chooses a decoder or family. A visible production prefix is either all v2 or contains
exactly one v2-to-v3 transition; v2 can never follow v3. The first visible v3 record chains from the
final v2 `record_hash`. A production snapshot cannot start with v3. An isolated all-v3 fixture is
allowed only as decoder evidence and cannot satisfy production-snapshot verification.

#### Pure R07 API And Recovery

Phase A may expose only the two future models, canonical v3 decode functions, and a read-only
synthetic fixture/snapshot verifier. Existing
`rquant.signal_route_spool.ReadonlySignalRouteSpool` remains legacy-only. Phase A MUST NOT expose
an append, `publish_v3`, `create`, migration, drain, cursor, high-watermark, environment,
capability, overlay, cutover, or production snapshot API. Importing or constructing the decoder
models is allowed; obtaining a durable v3 writer is forbidden.

The crash and orphan contract is frozen at every durability boundary:

| Crash point | Required recovery behavior |
|---|---|
| record temporary write | temporary is never authority; the old pointer ignores it |
| record temporary fsync | retry may reuse only byte-identical content |
| immutable record link | one immutable sequence name wins; a byte conflict rejects |
| records-directory fsync | only the fsynced immutable name may enter a complete prefix |
| pointer temporary write | temporary pointer is never authority |
| pointer temporary fsync | the old pointer remains authoritative until replacement |
| pointer replace | the visible pointer must name a complete, fully verified prefix |
| root-directory fsync | recovery accepts the old valid pointer or the new complete pointer, never an intermediate state |

Records above the visible pointer are orphans and are ignored. A retry of an already linked record
is accepted only when its bytes are identical; different bytes for the same sequence append conflict
audit evidence and reject. Every visible record and chain edge is verified before exposure. A
temporary file, orphan, directory listing, latest timestamp, or highest sequence is never authority.

#### Future V3 Hardened Immutable Publication Contract

`RESET-R07-P2-01` freezes a future v3-only hardened immutable publication primitive for the later
writer tranche. It is a distinct primitive and MUST NOT modify, wrap, dispatch through, or change
the behavior or bytes of the untouched v2 `rquant.signal_route_spool._immutable_write_at`.

For a v3 record target that already exists after a crash between immutable link and
records-directory fsync, the future primitive opens and compares the complete existing bytes. If
they are byte-identical, it MUST fsync the records directory again before any pointer publication
can begin; merely accepting the existing link is insufficient durability evidence. If the bytes
differ, it rejects before creating, replacing, or fsyncing any pointer temporary or visible pointer.
The same directory-fsync obligation applies whether retry observed a freshly created link or an
identical pre-existing target.

This is a frozen later-writer contract and crash red test, not a Phase A implementation grant.
Phase A synthetic fixture construction remains read-only/in-memory and cannot import, call, expose,
or make this primitive reachable through `rquant.runtime_service_main.build_builtin_registry`, its
builtin factory, any production builder, or any runtime capability. No production v3 writer exists
in Phase A.

#### Complete No-Activation Inventory

Every current-family production write boundary below remains legacy-only and fails before its first
durable or filesystem mutation. Where the current implementation relies on an upstream fence, the
local preflight is a required Phase A red test and implementation prerequisite.

| Existing boundary | Frozen no-activation rule |
|---|---|
| `rquant.strategy_runner.StrategyRunnerStore.process_batch` | constructs and persists only `SignalEnvelope`; no current-family constructor is accepted |
| `rquant.daily_summary_stage.DailySummaryStage.build_signal` | summary construction remains legacy-only |
| `rquant.daily_summary_stage.DailySummaryStage._error_signals` | stage-error construction remains legacy-only |
| `rquant.daily_notification_producer.build_daily_error_signal` | CLI error construction remains legacy-only |
| `rquant.daily_notification_producer.DailyNotificationProducer.emit` | current-family input rejects before bus mutation |
| `rquant.signal_bus.SignalBusStore.ingest` | current-family input rejects before opening its write transaction |
| `rquant.signal_bus.SignalBusStore.route` | a stored current-family signal rejects before outbox mutation |
| `rquant.signal_bus.SignalBusStore.commit_source_route` | current-family input rejects before source, signal, receipt, or outbox mutation |
| `rquant.signal_router_runtime.route_runner_signals` | the full batch is family-preflighted before source binding or route commit |
| `rquant.signal_route_spool.SignalRouteSpool.publish` | the full input tuple is legacy-preflighted before opening/binding the spool or writing source, record, or pointer files |
| `rquant.signal_route_spool.publish_signal_bus_prefix` | fetched records are legacy-preflighted before even the initial empty `SignalRouteSpool.publish`; the real symbol is this module function, not a `SignalRouteSpool` method |
| `rquant.notification_state.NotificationStateStore.replicate` | all records reject before its notification transaction |
| `rquant.paper_signal_worker.PaperSignalQueueStore.ingest` direct form | exact legacy object validation precedes its queue transaction |
| `rquant.paper_signal_worker.PaperSignalQueueStore.ingest` stored-byte form | local current-family rejection is required before its queue transaction, independent of the upstream bus fence |
| `rquant.runtime_serving_snapshot.SignalDeliveryPayload` | current-family registry-writer payload rejects before authority publication |
| `rquant.runtime_builder_signal._publish_signal_authority` | no current-family serving payload may reach authority publication |
| `rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish` | current-family signal payload rejects before generation or pointer filesystem mutation |
| `rquant.runtime_service_main.build_builtin_registry` | exact source/API snapshot retains its approved signature and exports; its candidate diff may contain no v3 writer, capability, environment flag, cursor, drain, cutover, fixture-publication, or overlay symbol |
| `rquant.runtime_service_builtin.build_builtin_registry` and all signal-path builders | each named builder has the same source/API snapshot rule; dynamic loading, generated registration, unknown export, or an unapproved production diff blocks rather than requiring object-graph reachability analysis |

The builder rule covers
`rquant.runtime_service_main.build_builtin_registry`,
`rquant.runtime_service_builtin.build_builtin_registry`,
`rquant.runtime_builder_strategy.strategy_live_builder`,
`rquant.runtime_builder_signal.signal_router_builder`,
`rquant.runtime_builder_signal.notifier_builder`,
`rquant.runtime_builder_shadow.shadow_session_builder`,
`rquant.runtime_builder_paper.paper_consumer_builder`,
`rquant.runtime_builder_paper.paper_broker_builder`,
`rquant.runtime_builder_serving.serving_publisher_builder`, and
`rquant.runtime_builder_daily_orchestrator.daily_pipeline_orchestrator_builder`.
It is an exact source/API-diff rule, not a reachability claim about closures, callbacks, or arbitrary
objects returned by those builders. Read-only current-envelope parsing, model construction, and
synthetic decoder fixtures do not grant writer authority.

#### R07 Relational No-Activation Release Proof

`R07-RR-PROOF-V1` is the sole normative Phase A no-activation gate. It is a relational,
differential release proof, not a proof about arbitrary Python object graphs or program semantics.
It compares one exact candidate release to one approved, already-deployed baseline and proves two
claims only:

1. the candidate changes production source only at a frozen set of read-only v3 declarations and
   existing-boundary rejection guards; and
2. every inventory boundary rejects current-family input before its first write transaction,
   mutation-capable filesystem open, or durable mutation.

It does not claim that the baseline has no latent defect, or that arbitrary aliases, closures,
callbacks, descriptors, generated code, or future dependency behavior are globally safe. Those are
baseline risks carried by its prior production approval and preflight. A candidate that needs a
broader claim, a writer, activation, a registry successor, an overlay, a cursor, a drain, or a
cutover is outside Phase A and blocks.

##### Frozen Baseline And Release Identity

The baseline is permanently `45d0b57c4c5cbab1700fa5e3c386c6756892a7d6`, whose Git tree is
`4f67e67192855874e82baa13dc343a1d6939bd67`. It is the last approved pre-R07 commit: it is an
ancestor of every Phase A candidate, changes only this ADR, and precedes the first R07 production
change (`461309d`, which changes `src/rquant/signal_route_spool.py`). A release candidate is valid
only when its commit is a descendant of this baseline and every CI checkout, built artifact, and
deployment target names the same exact candidate commit and tree.

`R07GitTreeRefV1` has exactly `commit_sha` and `tree_sha`, each a lowercase 40-hex Git object ID.
`R07ArtifactRefV1` has exactly `kind` (`wheel`, `sdist`, or `deployment_bundle`), `filename`
(ASCII basename), `size_bytes` (strict native integer >= 1), and lowercase-hex `sha256`. Artifact
entries are sorted by `(kind, filename)`, unique by that pair, and the deployment bundle is
mandatory. Every manifest/result model in this section is frozen, extra-forbid, rejects mapping or
subclass substitution for nested models, and serializes as
`rquant.strict_json.canonical_json_bytes(model_dump(mode="json"))`; its digest is SHA-256 of those
bytes with no self-digest field in the preimage.

##### Allowed Production Diff Manifest

The repository contains one reviewed, non-generated fixture
`tests/fixtures/r07_relational_release/allowed-production-diff-v1.json`. It is an input to CI, never
rewritten by a test, build, manifest generator, or deployment step. `R07AllowedProductionDiffManifestV1`
has exactly:

```text
schema_version: strict native int == 1
baseline: exact R07GitTreeRefV1 == the frozen baseline above
production_root: literal "src/rquant"
entries: nonempty Unicode-path-sorted tuple[R07AllowedProductionDiffEntryV1, ...]
```

Each entry has exactly `path`, `change_kind` (`add` or `modify`), `baseline_file_sha256` (null only
for `add`), `candidate_file_sha256`, and a nonempty source-order tuple of
`R07AllowedDeclarationV1`. A declaration has exactly `qualified_name`, `span_start_line`,
`span_end_line`, `normalized_ast_sha256`, and `role`. `role` is exactly one of
`read_only_v3_model`, `read_only_v3_decoder`, or `legacy_boundary_reject_guard`. The normalized AST
is the declaration's `ast.dump(include_attributes=False)` UTF-8 bytes. Paths are normalized relative
paths below `src/rquant`, contain no symlink, `..`, generated source, or glob, and no declaration
range may overlap another range in its file.

CI derives a production diff from the baseline tree to the candidate tree. It blocks when any
production path is added, removed, renamed, or modified without one exact manifest entry; when a
file digest, declaration span, AST digest, or role differs; when a declaration contains a public
writer/activation API; or when `pyproject.toml`, lockfiles, build hooks, package data, generated
code, import loaders, or dependencies change. The manifest may name only the strict v3 models and
decoders in `signal_route_spool.py` plus the named local legacy-family rejection guards required by
the inventory. This deliberately does not infer callable data flow or inspect closures.

##### Boundary Behavior Manifest And Mutation Probes

`R07BoundaryBehaviorManifestV1` is the second reviewed fixture in the same directory. It has exactly
`schema_version == 1`, `inventory_rows`, and `manifest_sha256`; its digest is canonical JSON excluding
`manifest_sha256`. `inventory_rows` is a Unicode-`boundary_id`-sorted tuple with exactly one row for
each row in the Complete No-Activation Inventory above, including both registry roots. A row has
exactly `boundary_id`, `target`, `input_cases`, `snapshot_domains`, and `first_mutation_guards`.

An `input_case` has exactly `case_id`, `form` (`direct_current`, `stored_current_bytes`, or
`batch_current`), and `entrypoint`. Every API family that accepts the form supplies that form; an
API family that cannot accept it supplies an explicit nearest public rejection entrypoint instead.
Missing, skipped, synthesized-success, or `not_applicable` cases block. The complete fixture must
include all three forms and exercise every downstream parser/queue/route form that can receive it.

`snapshot_domains` are exact test-owned SQLite databases, record directories, pointer directories,
outbox/queue tables, and source/receipt state named by the boundary. `first_mutation_guards` are
exact named spies on its first write transaction, SQL write, mutation-capable `open`, link, replace,
fsync, or pointer operation. For each case, the probe records a canonical before/after snapshot and
asserts all of the following: current-family rejection is the public outcome; every first-mutation
guard remains uncalled; and every declared database row, file byte, directory entry, pointer, outbox,
queue, source, and receipt snapshot is byte-identical. A failed guard, a changed snapshot, or a
mutation attempt is `boundary_mutation`, never a passing rejection.

`R07BoundaryInputCaseV1` has exactly `case_id` (ASCII `boundary_id/form`), `form`, and `entrypoint`
(a fully qualified existing symbol). `R07SnapshotDomainV1` has exactly `domain_id`, `kind`
(`sqlite`, `record_directory`, `pointer_directory`, `outbox`, `queue`, `source`, or `receipt`), and
`canonical_snapshot_sha256`. `R07FirstMutationGuardV1` has exactly `operation_id`, `target`, and
`operation_kind` (`write_transaction`, `sql_write`, `filesystem_open`, `link`, `replace`, `fsync`, or
`pointer_write`). Domain IDs, input-case IDs, and operation IDs are unique and sorted. Probe code
may use only test-owned temporary paths/databases named in its manifest row; a missing guard or
snapshot domain is an invalid fixture, not an empty assertion.

Registry rows do not construct or inspect arbitrary runtime graphs. They take an exact source/API
snapshot of the ten named production builders, their signatures, exports, and the absence of the
forbidden v3 writer/activation symbols. Any dynamic loader, generated registration, unknown export,
or source/API change outside the allowed diff is a `blocked_diff` result.

##### Exact Evidence And Artifact Binding

CI emits one immutable external `R07RelationalReleaseProofResultV1`, rather than committing a
self-updating approval file. It has exactly:

```text
schema_version: strict native int == 1
baseline: exact R07GitTreeRefV1
candidate: exact R07GitTreeRefV1
allowed_diff_manifest_sha256: lowercase SHA-256
boundary_manifest_sha256: lowercase SHA-256
fixture_sha256: lowercase SHA-256
artifacts: sorted nonempty tuple[R07ArtifactRefV1, ...]
python_evidence: exact tuple[R07PythonEvidenceV1, R07PythonEvidenceV1]
outcome: exact one of passed, blocked_diff, blocked_artifact, boundary_mutation, blocked_evidence
failures: canonical sorted tuple[R07RelationalFailureV1, ...]
result_sha256: lowercase SHA-256
```

`python_evidence` is ordered 3.11 then 3.12. Each item has exactly `python_minor`, `runner_id`,
`test_command_sha256`, `stdout_sha256`, `junit_sha256`, and `passed`; `passed` must be true only when
the exact relational suite completes with zero skipped or deselected cases. A passing result has an
empty failure tuple; every non-passing result has at least one deterministic failure with `kind`,
`subject`, and `detail_sha256`. `result_sha256` is over the canonical result excluding itself.

`R07PythonEvidenceV1.python_minor` is exactly `3.11` or `3.12`; `runner_id` is a nonempty ASCII
identifier; every evidence digest is lowercase SHA-256. `R07RelationalFailureV1.kind` is exactly
`diff`, `artifact`, `boundary`, or `evidence`; `subject` is a nonempty canonical manifest ID; and
`detail_sha256` is lowercase SHA-256. Failure entries sort by `(kind, subject, detail_sha256)`.
`R07RelationalReleaseProofResultV1` rejects a `passed` result with failures, a non-passing result
without failures, duplicate artifact/evidence/failure keys, noncanonical ordering, or either missing
Python version.

The CI build starts from a clean checkout of `candidate.tree_sha`, validates the two fixtures and
their literal expected digests, derives the diff against `baseline.tree_sha`, runs the boundary suite
on Python 3.11 and 3.12, and builds the wheel/sdist/deployment bundle from that same tree. It records
the resulting artifact hashes in the proof result. Release packaging or deployment may not regenerate
or amend either manifest or its approved digest. The deployer verifies the exact tag/commit/tree,
the approved proof-result digest, and the selected deployment-bundle digest before installation; it
does not recompute approval material on production. A mismatch is `blocked_artifact`.

##### Failure Semantics And Superseded Approaches

The only Phase A proof failures are an allowed-diff breach, an artifact/evidence binding breach, or
a boundary mutation attempt. They do not claim to establish a general composition from a current
input to a durable sink. This resolves the former capability-analysis findings directly:

| Former finding | Relational-proof resolution |
|---|---|
| `R07-CA-P1-01` | Closure values, callback captures, aliases, and object graphs are not proof objects; an affected source change is outside the exact diff or its boundary behavior is tested. |
| `R07-CA-P1-02` | Every fixture, identity, declaration span/AST digest, snapshot, guard, result, and canonical preimage is specified above. |
| `R07-CA-P1-03` | Baseline/candidate Git trees, fixture digests, CI evidence, and built artifact digests are bound into one external result and checked again by deployment. |
| `R07-CA-P1-04` | `boundary_mutation` and `blocked_diff` replace the incoherent current-plus-sink composition verdict. |

The prior generic object walk, callable/bytecode analysis, and content-addressed capability
projection are retained below only as archived review history. They are not normative, cannot yield
`complete_clean`, and must be deleted rather than extended when this relational suite is implemented.

#### Historical Note

Earlier R07 drafts attempted generic object-graph walking, callable/bytecode analysis, and a
content-addressed capability projection. Review demonstrated that none can soundly prove arbitrary
Python closure, callback, descriptor, or dynamic-resolution behavior without becoming an unbounded
interpreter. They are superseded by `R07-RR-PROOF-V1`; their commit history remains available in Git,
but no historical helper, fixture, result type, or finding may be extended or used as a release gate.

### Phase B: Successor Base Registry, Then Staged Overlay

Phase B is blocked until the actual Phase A v3 models and strict decoder exist and the complete Phase
A red suite is green. `RuntimeSchemaContractBundle` v2 remains the sole old authority. Its parser,
catalog, canonical bytes, content hash, declaration fingerprints, physical fingerprints, and
history remain unchanged and overlay-free. Absence of either a successor bundle or its overlay means
v2-only operation and grants no current-family transport authority.

Current semantics MUST NOT be overlaid onto any v2 channel. A successor base registry/bundle uses
distinct current-family transport channel IDs and is created only after the transport model for
each channel is an importable, manifest-covered class. The successor declares those actual models,
their complete schema declarations, and their physical schemas. It rejects a declaration for a
missing model, a future qualname, a string that does not resolve to the declared class, or a
descriptor computed from a qualname alone.

`RESET-REG-P1-01` freezes four exact future schemas. In this subsection,
`canonical_sha256(value)` means
`SHA256(rquant.strict_json.canonical_json_bytes(value))`. Every SHA-256 field is a strict lowercase
64-hex string. Every other string is strict, nonempty, and accepted without normalization. Tuple
fields are exact tuples, sorted by the key stated below, duplicate-free, and nonempty unless this
section says otherwise.

`SuccessorChannelV1` has exactly:

```text
channel_id
payload_model
declaration_schema_fingerprint
physical_schema_fingerprint
model_descriptor_hash
producer_service_ids
consumer_service_ids
channel_hash
```

`producer_service_ids` and `consumer_service_ids` are independently sorted unique tuples of exact
service IDs. `payload_model` is the authoritative qualified model string and MUST resolve inside the
generation source closure to the actual class before this declaration can exist. Its exact model
descriptor preimage and hash are:

```text
model_descriptor_hash = canonical_sha256({
  "payload_model": payload_model,
  "declaration_schema_fingerprint": declaration_schema_fingerprint,
  "physical_schema_fingerprint": physical_schema_fingerprint
})
```

`channel_hash = canonical_sha256(channel.model_dump(mode="json", exclude={"channel_hash"}))`.
The declaration and physical fingerprints come from the same authoritative successor channel
contract as `payload_model`; no overlay supplies or alters them.

`SuccessorBundleV1` has exactly:

```text
schema_version: strict native int == 1
bundle_namespace: literal "rquant.signal-family.successor"
channels: exact tuple[SuccessorChannelV1, ...]
content_hash
```

`channels` is sorted by `channel_id` and has no duplicate channel ID or hash.
`content_hash = canonical_sha256(bundle.model_dump(mode="json", exclude={"content_hash"}))`.
The raw successor authority bytes MUST equal
`rquant.strict_json.canonical_json_bytes(bundle.model_dump(mode="json"))` byte-for-byte.

Only after that successor bundle is valid may a subordinate staged overlay bind its already
declared channels. `OverlayDeclarationV1` has exactly:

```text
channel_id
base_bundle_content_hash
base_declaration_fingerprint
base_physical_fingerprint
model_descriptor_hash
accepted_family_ids
pair_ids
declaration_hash
```

`accepted_family_ids` and `pair_ids` are sorted unique tuples. The channel and all four base/model
hash bindings must equal the referenced `SuccessorChannelV1` and its containing bundle.

```text
declaration_hash = canonical_sha256(
  declaration.model_dump(mode="json", exclude={"declaration_hash"})
)
```

`OverlayBundleV1` has exactly:

```text
overlay_namespace: literal "rquant.signal-family.overlay"
overlay_version: strict native int == 1
base_bundle_content_hash
declarations: exact tuple[OverlayDeclarationV1, ...]
content_hash
```

`declarations` is sorted by `channel_id` and has no duplicate channel ID or declaration hash.
`content_hash = canonical_sha256(overlay.model_dump(mode="json", exclude={"content_hash"}))`.
The raw overlay authority bytes MUST equal
`rquant.strict_json.canonical_json_bytes(overlay.model_dump(mode="json"))` byte-for-byte. If either
declaration type is serialized independently, its raw bytes have the same full-model canonical-byte
equality requirement.

All four schemas use a duplicate-free strict structural decoder. Extra keys, duplicate JSON keys,
coerced scalar types, booleans as integers, mappings or subclasses in place of exact nested models,
unsorted tuple variants, aliases, unknown channels, hash mismatch, and noncanonical whitespace,
Unicode escaping, key order, or newline reject. Bundle and declaration schemas cannot accept each
other's fields. The overlay cannot add a channel, model, producer, consumer, field, physical schema,
or family absent from its successor base. A successor or overlay declaration before its actual
payload model exists rejects.

An absent overlay grants nothing. A partial overlay can be staged but can never become `READY`.
Byte-identical replay is idempotent. Reuse of a bundle, channel, overlay, or declaration identity
with different bytes appends conflict audit evidence and rejects.

### Phase C: Root-Derived Verification And Readiness

Phase C is blocked until the successor base channels and staged overlay exist and until an immutable
in-generation verification manifest, exact service-to-runtime-role bindings, externally installed
fixed root-owned release policy, separate root-owned verifier process/fixed harness, and root-owned
append-only verification store are implemented.

#### Exact Pair And Callable-Object Allowlist

The receipt set contains exactly these five pair IDs:
`strategy-router`, `strategy-shadow`, `router-notifier`, `router-paper`, and
`notifier-serving`. Dynamic services from `RuntimeServiceKind.STRATEGY_LIVE` are producer
evidence; they never emit or stand in for reader receipts.

Let `STRATEGIES` be the nonempty sorted tuple of every service ID in the validated target production
profile whose kind is `RuntimeServiceKind.STRATEGY_LIVE`. Let `ROUTER`, `SHADOW`, `NOTIFIER`,
`PAPER_BROKER`, and `SERVING_PUBLISHER` be the service ID of the target profile's unique manifest
of kind `SIGNAL_ROUTER`, `SHADOW_SESSION`, `NOTIFIER`, `PAPER_BROKER`, and `SERVING_PUBLISHER`,
respectively. Missing or duplicate singleton kinds reject. The exact service sides of the pair map
are:

| Pair ID | Producer service IDs | Consumer service IDs |
|---|---|---|
| `strategy-router` | every ID in `STRATEGIES` | `ROUTER` |
| `strategy-shadow` | every ID in `STRATEGIES` | `SHADOW` |
| `router-notifier` | `ROUTER` | `NOTIFIER` |
| `router-paper` | `ROUTER` | `PAPER_BROKER` |
| `notifier-serving` | `NOTIFIER` | `SERVING_PUBLISHER` |

`participating_service_ids` is exactly the sorted unique union of every producer and consumer
service ID in those five rows. It therefore includes all dynamic strategy services plus router,
shadow, notifier, paper broker, and serving publisher. A handwritten subset, static strategy list,
service count, or service kind in place of those resolved IDs rejects.

The pair-to-surface map is exact. Producer surfaces prove transport production but do not count as
reader receipts. Reader surfaces are the code exercised for that pair's one verifier-issued receipt.

| Pair ID | Producer-surface evidence | Reader-receipt surfaces |
|---|---|---|
| `strategy-router` | `rquant.strategy_runner.StrategyRunnerStore.process_batch` | `rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch`; `rquant.signal_router_runtime.route_runner_signals` |
| `strategy-shadow` | `rquant.strategy_runner.StrategyRunnerStore.process_batch`; `rquant.strategy_runner.StrategyRunnerStore.publish_session_close_receipt` | `rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch`; `rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot`; `rquant.runtime_shadow_sources.isolated_signal_observations` |
| `router-notifier` | `rquant.signal_route_spool.SignalRouteSpool.publish`; `rquant.signal_route_spool.publish_signal_bus_prefix` | `rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence`; `rquant.notification_state.NotificationStateStore.replicate` |
| `router-paper` | `rquant.signal_route_spool.SignalRouteSpool.publish`; `rquant.signal_route_spool.publish_signal_bus_prefix` | `rquant.signal_route_spool.ReadonlySignalRouteSpool.signals_after_global_sequence`; `rquant.paper_signal_consumer.consume_signal_bus_to_paper`; `rquant.paper_signal_worker.PaperSignalQueueStore.ingest` |
| `notifier-serving` | `rquant.runtime_builder_signal._publish_signal_authority`; `rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish` | `rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__`; `rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble`; `rquant.serving_read_models.build_serving_read_models` |

These entries are callable objects, not free-form receipt strings. The fixed harness resolves each
object only inside the unprivileged child generation interpreter, derives
`object.__module__ + "." + object.__qualname__`, and returns its bounded result under the frozen
surface ID. The root process never imports the object; it independently checks the expected
qualname, the binding's manifest-backed source-relative path, and exact source-file hash against the
full manifest and child result. Aliases, wrappers, monkeypatches, alternate modules, unmanifested
files, noncallable objects, omitted surfaces, and additional surfaces reject. Child verification
starts through the actual manifest-backed production builders above; direct unit construction
cannot substitute for that path.

#### Exact Verification Service Bindings

`RESET-REG-P1-02` freezes `VerificationServiceBindingV1` in the immutable, root-policy-approved,
in-generation verification manifest. It has exactly:

```text
service_id
runtime_service_kind
role_name
service_manifest_fingerprint
executable_module
executable_source_relative_path
executable_source_sha256
surface_ids
binding_hash
```

`runtime_service_kind` is an exact `RuntimeServiceKind` value. Each surface ID is a member of a
closed enum whose value is the exact callable qualname shown in the pair-to-surface table, never a
free-form string. `surface_ids` is the nonempty sorted unique tuple of those enum members assigned
to that service. The path is a normalized relative POSIX path with no empty, dot, parent, absolute,
alternate-separator, or symlink-resolved escape component. All strings and hashes are strict; extra
fields and coercion reject. The exact hash is:

```text
binding_hash = canonical_sha256(
  binding.model_dump(mode="json", exclude={"binding_hash"})
)
```

The complete service-binding tuple is sorted by `service_id`, has no duplicate service ID or
binding hash, and covers exactly `participating_service_ids`. Multiple dynamic strategy services
may share the same declared role and exact surface tuple; such sharing grants no alias and each
service still requires its own fingerprinted binding.
`service_bindings_hash` is `canonical_sha256` of the complete full-model dump of that sorted tuple.
Both the tuple and `service_bindings_hash` are fields of the immutable test manifest, and both are
included in the execution-evidence hash preimage.

These values are declared by the root-policy-approved verification manifest; they are not derived
from nonexistent executable module/path fields on `RuntimeServiceManifest`. Before child execution,
the root verifier independently requires each `service_id`, `runtime_service_kind`, and
`service_manifest_fingerprint` to equal the exact manifest in the validated production profile;
requires `role_name` to exist and `executable_module` to equal
`RuntimeGenerationSlot.roles[role_name].module`; resolves the executable source path beneath the
selected generation without traversal or symlink escape; matches its relative path and SHA-256 to
the full generation manifest; and resolves every `surface_id` to the exact callable-object allowlist
entry. Missing, duplicate, cross-role, wrong-kind, wrong-module, wrong-path, wrong-source-hash,
unmanifested, omitted-surface, or extra-surface bindings reject before any child starts.

#### Fixed Root-Owned Release Verification Policy

`RESET-REG-P0-01` is anchored by the separate fixed policy file
`/etc/rquant/signal-family-verifier-policy-v1.json`. It is outside every generation, quarantine,
application data root, and `lighthouse` write authority. It is a regular file owned `root:root`,
has exact mode `0444` and `nlink == 1`, and is reached only by an anchored no-follow open from a
trusted root directory FD with `O_NOFOLLOW | O_CLOEXEC`. `/`, `/etc`, and `/etc/rquant` must be
canonical root-owned directories, never symlinks, and not group/world writable; every component's
device/inode/type/owner/mode is rechecked after open. The file's strict canonical bytes and SHA-256
are verified before any generation file is opened.

This policy is independent of `production-runtime-profile-v1`; that existing schema and its bytes,
hashes, parser, and authority remain unchanged. Neither code release, quarantine publication,
generation selection, nor a `lighthouse` process can create, replace, or amend this policy.

The future `ReleaseVerificationEntryV1` schema has exactly:

```text
successor_bundle_content_hash
overlay_content_hash
verification_manifest_sha256
vector_set_hash
expected_result_set_hash
five_pair_service_binding_set_hash
verifier_policy_max_age_seconds: null or strict native int >= 1
entry_hash
```

All hash fields are strict lowercase SHA-256. The exact entry hash is:

```text
entry_hash = canonical_sha256(
  entry.model_dump(mode="json", exclude={"entry_hash"})
)
```

The future `SignalFamilyVerifierPolicyV1` schema has exactly:

```text
schema_version: strict native int == 1
verifier_policy_id: literal "signal-family-verifier-policy-v1"
harness_identity: literal "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
harness_sha256: lowercase SHA-256
release_entries: exact tuple[ReleaseVerificationEntryV1, ...]
content_hash: lowercase SHA-256
```

`release_entries` is nonempty, sorted by
`(successor_bundle_content_hash, overlay_content_hash)`, and has no duplicate release key,
`entry_hash`, or conflicting entry for the same release key. The exact policy hash is:

```text
content_hash = canonical_sha256(
  policy.model_dump(mode="json", exclude={"content_hash"})
)
```

The raw policy-file bytes MUST equal
`rquant.strict_json.canonical_json_bytes(policy.model_dump(mode="json"))` byte-for-byte, with no
newline. The duplicate-free strict decoder rejects extra or duplicate keys, coerced scalars,
booleans as integers, mappings/subclasses replacing exact nested models, unsorted/reordered entries,
unknown versions/IDs/identities, and any entry/hash/content mismatch. The fixed harness is opened
independently with the same anchored no-follow identity checks, must be `root:root`, mode `0555`,
`nlink == 1`, and must exactly match both `harness_identity` and `harness_sha256` before launch.

For the immutable in-generation verification/test manifest, the root independently recomputes these
exact policy-bound preimages. Each vector declaration contains only frozen vector identity and input
bytes; expected result bytes/hashes are forbidden in the vector tuple and exist only in the separate
expected-result tuple:

```text
vector_set_hash = canonical_sha256({
  "vectors": [
    vector.model_dump(mode="json")
    for vector in vectors_sorted_by_vector_id
  ]
})

expected_result_set_hash = canonical_sha256({
  "expected_results": [
    {
      "vector_id": result.vector_id,
      "canonical_result_sha256": result.canonical_result_sha256
    }
    for result in expected_results_sorted_by_vector_id
  ]
})

five_pair_service_binding_set_hash = canonical_sha256({
  "pairs": [
    {
      "pair_id": pair.pair_id,
      "producer_service_ids": list(pair.producer_service_ids),
      "consumer_service_ids": list(pair.consumer_service_ids)
    }
    for pair in exact_five_pairs_sorted_by_pair_id
  ],
  "service_bindings": [
    binding.model_dump(mode="json")
    for binding in service_bindings_sorted_by_service_id
  ]
})
```

The generation verification manifest's raw SHA-256 and its independently recomputed
`vector_set_hash`, `expected_result_set_hash`, and `five_pair_service_binding_set_hash` MUST equal
one and only one policy entry selected by the exact successor-bundle and overlay content hashes.
The entry's optional `verifier_policy_max_age_seconds` is the only policy age cap used by readiness.
Missing, stale/noncurrent, zero-match, multiple-match, duplicate, or conflicting policy entries fail
closed before child execution and produce no receipt or readiness record.

A generation may retain its immutable verification/test manifest, vectors, service bindings, and
expected results for reproducibility, but those bytes grant no authority by location, generation
membership, self-consistency, or full-manifest inclusion. They become eligible only through the
single exact external root-policy match above. Policy installation or update is a separate
root-owned, explicitly user-authorized infrastructure transaction with its own anchored write,
fsync, atomic replacement, directory-fsync, ownership/mode revalidation, and audit evidence; normal
code release and quarantine flows cannot perform it.

#### Dedicated signal_family_verification Authority

Signal-family readiness MUST NOT reuse self-attested
`rquant.schema_compatibility.ConsumerCapabilityReceipt`. A dedicated future
`signal_family_verification` subsystem has a separate root-owned verifier process as the sole owner
and sole writer of the root-owned append store. Filesystem ownership and mode prevent `lighthouse`
and service processes from opening that store for write. The root verifier MUST NOT import or
execute any generation code in its own process, and it opens no append-store descriptor before the
generation child has exited.

The root verifier performs this sequence:

1. Acquire `rquant.runtime_authority.RuntimeDeploymentLock`, then anchored-open and fully validate
   `/etc/rquant/signal-family-verifier-policy-v1.json` and its fixed harness. This finishes before
   opening any generation manifest, test manifest, generation source, or child process.
2. Reopen the current runtime authority and validated `RuntimeGenerationSlot`, derive the exact
   successor-bundle and overlay content hashes, and select exactly one matching
   `ReleaseVerificationEntryV1`. A missing, stale, duplicate, multiple, or conflicting match stops
   here.
3. Load the immutable in-generation verification manifest and its immutable test manifest. Require
   the verification-manifest raw SHA-256 and independently recomputed vector-set,
   expected-result-set, and five-pair/service-binding-set hashes to equal the selected policy entry;
   then validate every `VerificationServiceBindingV1` and derive the authority operation/sequence,
   generation/full-manifest/profile, exact service manifests, policy-bound vectors, and policy age
   cap.
4. Build the child request only from the exact vector input bytes whose sorted set produced the
   policy-authorized `vector_set_hash`. Do not send expected result bytes or expected result hashes
   to the child. Launch the generation-local interpreter as a separate unprivileged `lighthouse`
   child using the policy-identified fixed root-owned harness. The child receives a sanitized fixed
   environment, fixed argv and cwd, no supplementary privilege or escalation path, and closed
   inherited file descriptors except the canonical request/result IPC pipes. It receives no store
   descriptor, store path, store capability, verifier-module import path, root module object, or
   caller-supplied Python path.
5. The fixed harness imports generation code only in the child and executes every policy-bound
   family/surface vector through the actual production builders. It cannot add, omit, reorder, or
   alter vector inputs and cannot supply or change expected results. It emits exactly one bounded
   canonical IPC result and exits; extra output, timeout, signal death, nonzero status, open pipe,
   or inherited-descriptor mismatch rejects.
6. After child exit, the root process strictly decodes the IPC result, validates the exact
   run/vector/family/surface identities and canonical result bytes/hashes, recomputes the actual
   result-set hash using the policy's expected-result-set preimage shape, and requires equality with
   the selected entry's `expected_result_set_hash`. It also independently revalidates the immutable
   test-manifest and service-binding tuple/hash, source paths/hashes, full-manifest source closure,
   service manifests, policy content/entry/harness hashes, and policy age cap. Child claims cannot
   replace root-derived or root-policy-authorized values.
7. Still under `RuntimeDeploymentLock`, anchored-reopen the external policy and fixed harness and
   reopen authority and the validated generation slot after all child validation. Any policy
   content, selected entry, harness, operation, sequence, slot, generation, full-manifest, profile,
   role, source, successor bundle, or overlay change between the initial snapshot, child
   completion, and append rejects.
8. Only then may the root process open the protected append store and write the five receipts and
   readiness transaction itself. The child, caller, generation, and services never append evidence
   or receipts.

The child's future `SignalFamilyVectorResultV1` schema is exactly `vector_id`, `pair_id`,
`family_id`, `surface_id`, `canonical_result_json`, and `canonical_result_sha256`.
`canonical_result_json` is strict nonempty UTF-8 JSON bounded to 65,536 bytes and its bytes/hash must
equal the root-policy-authorized expected result when the root compares it after child exit. The
future `SignalFamilyChildResultV1` schema is exactly:

```text
schema_version: strict native int == 1
run_id: lowercase SHA-256
test_manifest_hash: lowercase SHA-256
vector_results: exact tuple[SignalFamilyVectorResultV1, ...]
result_hash: lowercase SHA-256
```

`vector_results` is bounded by the exact test-manifest vector count, sorted by
`(pair_id, family_id, surface_id, vector_id)`, and duplicate-free. `result_hash` is
`canonical_sha256(result.model_dump(mode="json", exclude={"result_hash"}))`. The one IPC response
is at most 1,048,576 bytes and its raw bytes equal
`rquant.strict_json.canonical_json_bytes(result.model_dump(mode="json"))` with no newline. Extra or
duplicate fields, coercion, unknown vectors, unsorted results, trailing bytes, and size excess
reject.

The full manifest is the source closure; no caller- or child-claimed source closure can replace it.
`code_commit` remains audit-only and cannot prove authority, source, service, family, surface, or
test execution.

The authority epoch key is exactly:

```text
SHA256(canonical_json_bytes({
  "operation_id": authority.operation_id,
  "sequence": authority.sequence,
  "generation_id": slot.generation_id,
  "full_manifest_hash": slot.full_manifest_hash,
  "profile_id": slot.profile_id
}))
```

The canonical execution-evidence hash preimage binds that epoch key;
verification-manifest hash; exact test-manifest hash; canonical
vector-set, expected-result-set, and five-pair/service-binding-set hashes; canonical child
execution-result hash; verifier policy ID/content hash and selected entry hash; fixed harness
identity/hash; observed family and surface sets; full-manifest/source-closure hash;
service-manifest hashes; and the complete authority snapshot. The resulting execution-evidence hash
is not an input to itself. Audit records contain identifiers, hashes, timestamps, outcomes, and
bounded reason codes only. They MUST NOT contain signal payloads, environment values, secrets,
credentials, raw exception text, or verification vector inputs.

No public or service API accepts a caller-created attestation, receipt, result, evidence model, or
append request for persistence. Module privacy, import identity, object identity, sentinels, and
seals are not privilege boundaries. The external fixed root-owned policy, root-owned verifier
process, policy-hashed fixed root-owned harness, policy-matched generation manifest/source closure,
OS-separated unprivileged child, append-store ownership/mode, bounded IPC validation, and deployment
lock form the trust boundary. Copying or forging IPC fields, replacing self-consistent generation
code/manifests/vectors/results, inspecting child modules, importing a public model, or invoking a
service-local helper grants no append authority without the exact external policy match.

#### Atomic Receipts, Readiness, And Lifecycle

One successful immutable verifier run atomically emits exactly five receipts, one for each frozen
pair. The receipt uniqueness key is exactly
`(overlay_content_hash, authority_epoch_key, pair_id)`. Each receipt fingerprint binds its pair,
exact producer/consumer service-ID tuples, successor declaration hash, overlay hash, authority
epoch, generation/profile, exact producer evidence, exact reader surfaces, verification manifest,
result/evidence, verifier policy, service manifests, complete sorted service-binding tuple/hash,
policy content/entry/harness hashes, `participating_service_ids`, test-manifest hash, `verified_at`,
and `fresh_until`. An identical replay is idempotent; divergent bytes for the same key append
conflict audit evidence and reject.

`RESET-REG-P2-01` makes freshness profile-derived from the exact pair map. Resolve one exact
`RuntimeServiceManifest` for every ID in `participating_service_ids`, reject a missing, duplicate,
or additional resolution, and compute:

```text
service_freshness_seconds = min(
  manifest.stale_after_seconds
  for manifest in exact_participating_service_manifests
)
freshness_seconds = (
  min(service_freshness_seconds, selected_policy_entry.verifier_policy_max_age_seconds)
  if selected_policy_entry.verifier_policy_max_age_seconds is specified
  else service_freshness_seconds
)
fresh_until = verified_at + freshness_seconds
```

The optional frozen policy maximum is strict, positive, and itself hash-bound. There is no fixed
30-second assumption and no handwritten partial manifest list. A lower stale bound from shadow or
serving controls readiness exactly as a lower bound from any strategy, router, notifier, or paper
broker would.

A `READY` decision requires exact five-pair set equality, not a count. It binds every successor
declaration hash required by the overlay; overlay content hash; five sorted pair IDs; five sorted
receipt fingerprints and their aggregate canonical SHA-256; authority epoch, generation, full
manifest, and profile; `participating_service_ids`; complete service-binding tuple/hash; and
verifier policy ID/content hash, selected entry hash, fixed harness identity/hash, and
`verified_at`/`fresh_until`. The verifier writes the receipts, immutable decision, and
compare-and-swap state in one transaction from one consistent snapshot. A unique key over overlay
plus authority epoch makes concurrent identical finalization return the same bytes; a divergent
concurrent result appends conflict audit evidence and rejects.

The lifecycle is minimal:

```text
DECLARED -> READY
DECLARED -> REVOKED
READY -> REVOKED
READY -> ROLLED_BACK
```

There is no `ATTESTING` or `ACTIVATED` state. Authority sequence advance, operation change, any
other epoch change, or expiry invalidates readiness without deleting history. Returning later to the
same generation ID does not revive old readiness because the authority epoch also binds operation ID
and sequence. Rollback and revocation are append-only and only disable future activation
eligibility.

### Stable Reset Risks And Red Tests

The reset finding ledger is stable:

| ID | Blocking risk | Required closure |
|---|---|---|
| `RESET-R07-P0` | any production-obtainable v3 writer, implicit activation, or mutation before family rejection | exhaustive no-activation inventory and fail-before-mutation evidence |
| `RESET-R07-P1` | v3 dispatch or canonicalization changes v2 bytes, hashes, models, or public errors | frozen valid/invalid v2 differential corpus with exact equality |
| `RESET-R07-P2` | mixed-chain, pointer, retry, crash, or orphan recovery exposes a partial or ambiguous prefix | strict transition and complete crash/orphan matrix |
| `RESET-REG-P0` | caller/service-forged evidence, generation-self-authorized vectors, or stale authority can create receipts or `READY` | external root policy, OS-separated verifier/child, strict IPC, lock revalidation, and root-owned append store |
| `RESET-REG-P1` | successor or overlay grafts current semantics onto v2 or declares nonexistent/future models | unchanged-v2 evidence, actual-model descriptors, and successor-before-overlay enforcement |
| `RESET-REG-P2` | count-only readiness, concurrent divergence, expiry/rollback ambiguity, generation-return replay, or audit leakage | exact set/epoch/CAS/lifecycle tests and bounded audit schema |
| `RESET-R07-RR-P0` | a Phase A candidate changes an unapproved production path, dependency/build surface, generated source, loader, or declaration while presenting a self-consistent local fixture | `R07-RR-PROOF-V1`: exact approved baseline/candidate trees, frozen allowed production diff fixture, declaration span/AST/file digests, clean-checkout CI, and deployment artifact binding |
| `RESET-R07-RR-P1` | an existing persistence boundary accepts direct, stored-byte, or batch current-family input after a transaction or filesystem mutation begins | one-to-one boundary behavior manifest, mutation-capable operation guards, and byte-identical before/after snapshots for each applicable input form |
| `RESET-R07-RR-P2` | a proof result approves bytes other than CI-tested/deployed bytes, or a skipped/deselected interpreter run masquerades as evidence | external immutable result binds both Python 3.11/3.12 evidence, fixture and manifest digests, wheel/sdist/deployment-bundle hashes, exact tag/commit/tree, and deployment revalidation |
| `RESET-R07-P2-01` | an identical post-link retry can publish a pointer without re-establishing records-directory durability | future v3-only primitive re-fsyncs the records directory; byte conflict rejects before pointer mutation |
| `RESET-REG-P0-01` | generation code, self-consistent manifests/vectors/results, a service, or forged IPC can acquire append authority | fixed external root policy authorizes exact release hashes; root never imports generation code; unprivileged child has no store/verifier capability; root validates and writes after child exit |
| `RESET-REG-P1-01` | underspecified successor/overlay schemas permit alternate bytes, order, identity, or nonexistent models | four exact schemas, canonical preimages/raw bytes, strict structural rejection, and actual-model prerequisite |
| `RESET-REG-P1-02` | executable/service/surface claims rely on absent service-manifest fields or cross-role aliases | exact root-approved service bindings checked against profile, slot roles, full manifest, and object allowlist before execution |
| `RESET-REG-P2-01` | readiness freshness omits a pair participant or assumes a fixed/partial minimum | exact pair-derived service union and minimum stale bound, optionally capped by frozen policy |

The planned red-test matrix is exact:

| IDs | Planned test file/evidence | Required assertions |
|---|---|---|
| `RESET-R07-P1` | `tests/fixtures/signal_route_spool_v2_differential/manifest.json`; `tests/unit/test_signal_route_spool_v2_differential.py` | frozen valid and invalid corpus proves untouched v2 bytes, `ensure_ascii=True`, hashes, models, and public error category/sequence before and after dispatcher introduction |
| `RESET-R07-P1`, `RESET-R07-P2` | `tests/unit/test_current_signal_route_spool_record_v3.py` | exact `E`/`R`/outer bytes and hashes, strict JSON, exact types, all-v2 and one-way mixed chain, v3-first production rejection, isolated decoder-only all-v3 fixture, pointer neutrality, and synthetic crash/orphan/retry state-machine cases without a durable writer |
| `RESET-R07-P2-01` | future `tests/unit/test_current_signal_route_spool_v3_publication_contract.py` in the separately authorized writer tranche | v3 primitive is separate from byte-identical v2 `_immutable_write_at`; crash after link and before directory fsync makes identical retry fsync the records directory again before pointer work; differing bytes reject before pointer mutation; Phase A/builders cannot import or reach the primitive |
| `RESET-R07-RR-P0`, `RESET-R07-RR-P2` | `tests/unit/test_signal_family_relational_release.py`; `tests/fixtures/r07_relational_release/*` | fixture models reject self-update, unordered/duplicate/extra/coerced values, incorrect baseline, paths outside `src/rquant`, source/declaration digest drift, generated/import-loader/dependency/build change, candidate/tree/tag mismatch, and artifact/result mismatch; Python 3.11 and 3.12 produce a bound, zero-skip evidence result from the same clean candidate tree |
| `RESET-R07-RR-P1` | `tests/unit/test_signal_family_relational_boundaries.py` | every Complete No-Activation Inventory row has a unique manifest row and an executable direct/stored-byte/batch rejection case where applicable; spies prove no first write transaction, SQL write, mutation-capable filesystem open/link/replace/fsync/pointer operation; all declared SQLite/file/outbox/queue/source/receipt snapshots remain byte-identical; read-only v3 decoder/model construction remains allowed |
| `RESET-REG-P1`, `RESET-REG-P1-01` | `tests/unit/test_signal_family_successor_registry_reset.py` | v2 parser/catalog/bytes/hashes/history are unchanged; exact four-schema field sets, hash preimages, raw canonical bytes, strict duplicate/extra/coercion/order rejection; successor declaration rejects before the actual model exists; v2 semantic/partial/absent/conflicting overlay never becomes ready |
| `RESET-REG-P0`, `RESET-REG-P1`, `RESET-REG-P1-02` | `tests/integration/test_signal_family_verification_reset.py` | all five pair IDs resolve exact callable objects through real production builders and manifest-backed source hashes; exact service bindings cover the pair-derived service set and reject missing/duplicate/cross-role/wrong module/path/source hash before child execution; only a successful immutable child run can lead the root verifier to persist five receipts |
| `RESET-REG-P0-01` | `tests/integration/test_signal_family_root_verifier_isolation.py` | child module inspection/import cannot discover or import privileged verifier/store authority; direct store open/append fails; no inherited descriptor/path/capability exists; forged, extra, oversized, noncanonical, or wrong-result IPC rejects; caller/service evidence APIs do not exist; authority change between child completion and root append rejects |
| `RESET-REG-P0-01` | `tests/integration/test_signal_family_root_policy_anchor.py` | policy is loaded before generation files/child and requires anchored no-follow root ownership, mode `0444`, `nlink == 1`, canonical bytes/content hash, exact harness identity/hash, and one exact entry; self-consistent replacement of generation code, manifest, vectors, and expected results rejects without a separate matching policy update; policy whitespace, duplicate/extra keys, entry reorder, hash tampering, missing/stale/multiple/conflicting entries, release/quarantine update attempts, and policy/harness replacement during child execution reject with no receipt/readiness; an approved exact policy passes only through the isolated root-verifier flow |
| `RESET-REG-P0`, `RESET-REG-P2`, `RESET-REG-P2-01` | `tests/unit/test_signal_family_readiness_reset.py` | exact five-set equality and pair-derived participating-service union, atomic receipt/decision/CAS, byte-identical concurrency, divergent conflict, profile-derived expiry, lower shadow or serving stale bound controls `fresh_until`, optional policy cap, authority advance/return to same generation, rollback, revoke, and no `ATTESTING`/`ACTIVATED` state |
| `RESET-REG-P0`, `RESET-REG-P2` | `tests/unit/test_signal_family_verification_audit.py` | audit schema contains only bounded identifiers/hashes/timestamps/outcomes and excludes payloads, vector inputs, environment, secrets, credentials, and raw exception text |

### Explicit Phase Blockers

These are development prerequisites, not unresolved design questions:

1. Phase A R07 models, strict decoder, v2 differential corpus, mixed-chain verifier, synthetic
   crash/orphan state-machine matrix, and exhaustive no-activation tests MUST complete before any
   successor current-family base contract is implemented. The future v3 publication primitive and
   its filesystem crash test remain blocked until a separately authorized writer tranche.
2. Successor base channels MUST bind actual manifest-covered transport models before an overlay can
   be staged; future qualnames and v2 semantic overlays reject.
3. The immutable verification manifest, exact `VerificationServiceBindingV1` tuple, fixed root-owned
   harness, externally installed `/etc/rquant/signal-family-verifier-policy-v1.json`, OS-separated
   root verifier/unprivileged generation child, strict bounded IPC, root-owned append store, and
   deployment-lock revalidation MUST exist before overlay receipts or `READY` can be emitted. The
   policy installation/update is a separate user-authorized root infrastructure transaction;
   in-process sealing or generation inclusion cannot satisfy this blocker.
4. No high-watermark freeze, legacy drain, cursor migration, writer cutover, production v3
   publication, capability, environment flag, or activation work is authorized by this reset.

## Root Quarantine And Generation Publication

The root helper accepts only a fixed operation schema containing an operation ID and allowlisted
business fields. It derives the candidate from a profile-frozen inbox root and derives quarantine,
generation, and authority paths internally. Arbitrary paths, commands, modules, environment maps,
file descriptors, and shell fragments are not request fields.

The helper performs this transaction:

1. Open the frozen candidate/inbox anchor and every descendant with directory FDs and
   `openat`-style operations using `O_NOFOLLOW` and `O_CLOEXEC`. Directory traversal never
   concatenates or reopens caller paths.
2. Reject traversal, empty/dot components, symlinks, devices, sockets, FIFOs, unknown types,
   duplicate relative paths, dev/inode aliases, and identity changes. Every copied regular file
   must have `nlink == 1`; directories are opened with `O_DIRECTORY | O_NOFOLLOW` and checked as
   directories rather than copied as leaf objects.
3. Create a new operation-scoped quarantine beneath the fixed root-owned quarantine anchor. Copy
   bytes only from an already validated source FD to a newly created destination FD. Root never
   imports, executes, unpickles, or invokes candidate-controlled hooks.
4. Set destination ownership to root and apply the exact allowlisted mode for each entry. Reject
   setuid/setgid/sticky executable content and every mode not represented by the manifest schema.
5. Fsync each completed file, fsync directories bottom-up, and close the copied tree. A source
   identity change before copy completion rejects the operation.
6. Reopen the closed root-owned quarantine exclusively through trusted directory FDs. Recompute
   the canonical full manifest, every byte hash, entry type, size, exact mode, `nlink`, and owner
   UID. The SHA256 of this canonical root-derived manifest is `full_manifest_hash` and is the only
   generation identity input. Candidate manifest and commit claims are retained only as untrusted
   audit metadata; disagreement is recorded but is not a provenance or privilege decision.
7. After the root-owned tree is complete, durable, and verified, atomically rename it to its
   content-addressed generation name and fsync the generation-store parent. Published generations
   are root-owned, immutable to `lighthouse`, and never modified in place.

The generation ID derives only from the root-recomputed `full_manifest_hash`. v1 performs no
artifact signing and the root helper proves no Git provenance. User authorization and GitHub
delivery are operational intent outside the helper's security proof. A self-consistent malicious
candidate may be published under its actual root-recomputed identity, but root never imports or
executes it and it can later run only as UID `lighthouse`; its commit/manifest claims cannot cause
privileged execution or add authority. Root-fetch and artifact-signature provenance are future
backlog items and must not be claimed by v1 tests, audit records, UI, or operator documentation.

## Single Runtime Authority Record

`/var/lib/rquant/runtime-authority/current.json` is the only mutable runtime authority. There is
no separate selector, resolved record, dynamic drop-in, or mutable `current` symlink. Its strict
schema contains exactly:

- top-level `operation_id` and state;
- `current_generation_id`, `current_generation_path`, untrusted `current_commit` audit metadata,
  `current_full_manifest_hash`, `current_profile_id`, and complete `current_roles`;
- `prior_generation_id`, `prior_generation_path`, untrusted `prior_commit` audit metadata,
  `prior_full_manifest_hash`, `prior_profile_id`, and complete `prior_roles`; and
- an explicit absent-prior representation for first install, with no omitted or inferred fields.

Each roles value binds the role's generation-local Python, module, working directory, application
source root, site-packages roots, caller-argv policy, and exact environment name-to-grammar policy.
For `daily`, the module is only `rquant.production_daily_main` and the argv policy is zero caller
arguments. Both generation paths must be canonical descendants of the frozen store and all
current/prior fields must validate as complete self-consistent slots. Commit fields never select
content and are excluded from generation identity and trust decisions.

After a generation is durable, the helper creates a same-directory temporary record with
create-new/no-follow semantics, validates its exact schema, sets root ownership and exact mode,
fsyncs it, atomically renames it over `current.json`, and fsyncs
`/var/lib/rquant/runtime-authority`. Old complete generations are retained for rollback. Immutable
audit records may describe transactions but cannot authorize runtime behavior.

Recovery reads only `current.json`; directory ordering, temp files, audit logs, process state, and
generation timestamps never infer authority. If the record or its named generation cannot be
fully revalidated, startup and deployment fail closed.

A forward transaction writes the new generation into the complete `current_*` slot and moves the
previous complete current slot into `prior_*` in the same atomic replacement. Automatic rollback
is limited to one layer per deployment attempt: a new record promotes the complete `prior_*` slot
to `current_*` and either demotes the failed current slot to `prior_*` with state `failed`, or marks
it `retired` in that same slot according to the frozen state schema. No second automatic rollback,
directory scan, history walk, audit lookup, or process inspection may choose another generation.

## Runtime Wrapper And Systemd

Each production unit has a fixed root-owned command equivalent to:

```text
/usr/bin/python3.11 -I -S \
  /usr/local/libexec/rquant-runtime-exec.pyz --role <allowlisted-role>
```

The unit runs as UID `lighthouse`. `<allowlisted-role>` is a literal unit-owned value, not caller
input. Units require no dynamic drop-in or `daemon-reload` per generation. The `daily` unit may
load only the fixed root-owned `/etc/rquant/rquant-daily.env` application-value file described
above; no `EnvironmentFile` value can override Python, module, profile, generation, cwd, import,
or authority behavior.

The wrapper opens the fixed profile and `current.json`, verifies root ownership/modes/ancestry and
the record schema, then revalidates the exact current generation path, generation ID,
`full_manifest_hash`, complete file tree, owner UID, mode, link count, and selected role. Commit is
read only as untrusted audit metadata. The wrapper constructs the role's exact cwd and fresh
allowlisted environment only after the anchored application-path walk, exact variable mapping,
scalar/list grammar, pairwise channel rules, and emitted-byte budget all pass. It then uses an
absolute exec of the fixed generation-local venv Python with arguments equivalent to:

```text
<absolute-generation-venv-python> -I -S -c <root-pyz-frozen-bootstrap> \
  <allowlisted-role>
```

`FROZEN_BOOTSTRAP` is the `<root-pyz-frozen-bootstrap>` code frozen inside the already verified
root-owned runtime pyz; it is not generation or record text. The child bootstrap reopens the
fixed profile and `current.json` and independently repeats strict record, current-slot, generation,
`full_manifest_hash`, role, import-root, cwd, and application-environment validation. Record paths
are data only and are never interpolated into bootstrap code, environment, or argv. The bootstrap
selects the role mapping and environment grammar from the root-owned profile subject to its frozen
minimum schema, receives only the literal role, inserts only manifest-covered canonical source
and site-packages paths inside the current generation, sets the selected module's `sys.argv` as
specified above, and invokes only that module with `runpy`.

The generation-local Python, `pyvenv.cfg`, module tree, cwd, application source, and site-packages
are manifest-covered and root-owned. `pyvenv.cfg` must contain exactly
`include-system-site-packages = false`. Verification rejects every `.pth` file,
`sitecustomize.py`, `usercustomize.py`, external/escaping import path, namespace-package portion
outside the two approved roots, and unexpected site-packages entry. The fresh environment removes
`PATH`, all `PYTHON*`, all `LD_*`, user-site/home import influence, and every site/import override.
The wrapper never uses ordinary `-m` site startup, dereferences a `current` symlink, imports from
the checkout, searches `PATH`, or accepts a module/path override.

Existing `rquant.runtime_service_main` and formal-runtime paths remain untouched legacy behavior.
They are not the HYBRID `daily` adapter and cannot satisfy or provide evidence for this decision.

## State Machine

```text
UNTRUSTED_REQUEST
  -> SOURCE_FD_BOUND
  -> QUARANTINE_COPYING
  -> QUARANTINE_DURABLE
  -> QUARANTINE_REVALIDATED
  -> GENERATION_PUBLISHED
  -> GENERATION_DURABLE
  -> AUTHORITY_TEMP_DURABLE
  -> AUTHORITY_ACTIVE(current=new, prior=old)
  -> RUNTIME_REVALIDATED
  -> APPLICATION_PATHS_VALIDATED
  -> APPLICATION_ENV_VALIDATED
  -> PRODUCER_IDENTITY_BOUND(envelope_schema=rquant.signal-envelope/v1,
                             capability=current_generation)
  -> DAILY_ADAPTER_BOUND
  -> RUNNING
```

Any validation, copy, ownership, mode, hash, fsync, rename, record, wrapper, application-path,
environment, producer-identity, exec, or health-gate failure enters `REJECTED` before
`AUTHORITY_ACTIVE`, or `BLOCKED` afterward. Neither state permits fallback to
checkout/current/PATH/Git identity. Rollback is one new atomic `current.json` transaction that
promotes the validated `prior_*` slot and demotes or retires the failed `current_*` slot. It passes
through `AUTHORITY_TEMP_DURABLE -> AUTHORITY_ACTIVE(current=prior, prior=failed-or-retired) ->
RUNTIME_REVALIDATED -> APPLICATION_PATHS_VALIDATED -> APPLICATION_ENV_VALIDATED ->
PRODUCER_IDENTITY_BOUND -> RUNNING`, never mutates a generation, and cannot automatically recurse.

## Crash And Rollback Matrix

| Crash/failure point | Authoritative state | Required recovery |
|---|---|---|
| Before quarantine creation | Existing current/prior slots | Reject request; no runtime change |
| During source traversal or copy | Existing record; partial quarantine untrusted | Close FDs; quarantine by operation ID; clean only after anchored ownership checks |
| After copy, before file/directory fsync completes | Existing record; quarantine non-durable | Never verify/publish it; recopy or remove safely |
| After quarantine fsync, before close/reopen | Existing record; quarantine not independently verified | Reopen and run full root-tree verification |
| During quarantine revalidation | Existing record | Reject incomplete/unstable root tree; candidate metadata mismatch is audit-only |
| After verification, before generation rename | Existing record; complete quarantine | Rename only after repeating final identity check |
| After generation rename, before store-parent fsync | Existing record; generation durability uncertain | Revalidate and fsync parent, or quarantine generation from use |
| Generation durable, before authority temp creation | Existing record; new generation inert | Retain or collect later; do not restart it |
| During authority temp write | Existing record | Ignore/remove temp after anchored checks |
| Authority temp with complete new-current/old-current-as-prior fsynced, before rename | Existing record | Either complete validated rename or discard temp; never combine slots from two records |
| `current.json` renamed, before authority-parent fsync | New current/prior visibility or durability ambiguous | Block; reopen and validate both slots, then fsync parent before restart |
| Record durable, before restart | New current authoritative; prior is sole automatic rollback target; old service may run | Start exact current or publish one rollback record |
| Wrapper current-slot/generation revalidation fails | Current unusable; prior slot remains explicit | Do not exec; validate prior and publish at most one rollback record |
| New runtime bootstrap/import/health fails | New current authoritative; launch unsuccessful | Atomically promote prior to current and demote/retire failed current, fsync, then launch prior |
| Rollback temp/write/rename/fsync fails | Last durably validated two-slot record only | Mark blocked; never infer a generation from directories, audit, or processes |
| Rollback current revalidation/start/health fails | One rollback already attempted | Mark blocked; preserve both slots/evidence; no second automatic rollback or mutable-path fallback |
| Restart and health gate succeed | Durable two-slot record and exact running current agree | Publish non-authoritative audit success; retain prior slot/generation |

Backup and restore operate on complete immutable generations plus one validated two-slot
`current.json`. Restore never overlays files into a generation. It restores generations under
their content addresses, revalidates both slots, then atomically publishes one complete record and
verifies the exact current runtime. Missing history is not reconstructed from directories.

## Descriptor Policy And Revised Findings

There is no global production interpreter FD authority. FDs remain only for scoped capabilities:
the deployment lock, control socket, opened candidate/quarantine/authority records, and explicitly
typed resource capabilities. Ownership, inheritance, and closure are local and auditable.

| Finding | Revised disposition | Reason |
|---|---|---|
| PIA-108: pure-Python global interpreter FD authority | REJECTED by threat-model revision | It cannot attest its own initial interpreter and adds an execution framework without removing bootstrap TCB |
| PIA-109: one interpreter FD chain across bootstrap/release/systemd | REJECTED by threat-model revision | Durable restart/rollback is bound by immutable generations and the single authority record, not ephemeral cross-process FDs |
| PIA-110: require a native FD launcher | REJECTED by threat-model revision | It adds compiler/toolchain/update TCB without protecting against the trusted-root compromise already excluded |

The superseded implementation's `preexec_fn`, helper/target interpreter FD inheritance, leaked
descriptors, and unused global authority surface are still removed from production paths. Their
deadlock and capability-leak risks are independent of the revised trust decision.

## Rejected Alternatives

Pure-Python FD execution is rejected as a global trust mechanism: it cannot prove the binary,
stdlib, hooks, or code that created the running process; it also varies by platform and expands FD
leak and `preexec_fn` risk. A native launcher is rejected because its native build provenance,
ABI, packaging, update, and signing-toolchain costs do not reduce the current kernel/root/systemd
TCB. Artifact signing is deferred, not rejected permanently; v1 has no key lifecycle or verifier
and therefore makes no signing claim.

## Authorization And Implementation Batches

Installing root-owned pyz/profile/helper files, creating the root-owned generation/authority
stores, or changing systemd/sudoers/production configuration requires separate explicit user
authorization. This PR may prepare code, tests, and an installer candidate but must not install or
deploy them.

The completed FD-authority cleanup and existing runtime-authority/quarantine primitives remain
prerequisites with their own tests; they are not wrapper evidence. `WRAP-DESIGN-P1-04` retains its
four macro dependencies: (1) role/path/environment and signal-reader contracts, (2) executable/
zipapp/profile/installer planning, (3) runtime/deploy entries and the daily adapter, then (4)
Linux/root/cloud gates. The signal-family architecture reset above is an additional monotonic gate,
not a replacement for those macro dependencies: contract implementation precedes artifact
planning, artifacts precede runtime/adapter code, runtime entries remain inert, and cloud gates
remain last.

The active `WRAP-DESIGN-P1-03` implementation order is non-skippable:

1. Preserve the completed R01-R06 family dispatcher/read behavior and its direct and nested
   consumer evidence; deployed writers remain legacy-only.
2. Complete reset Phase A R07 models, strict decoder, frozen v2 differential corpus, read-only
   mixed-prefix verifier, crash/orphan matrix, and exhaustive no-activation gates.
3. Only after Phase A is green, implement reset Phase B's successor current-family base contracts
   over actual manifest-covered models, then its subordinate staged overlay.
4. Only after Phase B is complete, implement reset Phase C's root-derived verification,
   exact-five receipts, and immutable `READY` decision.

The former registry-before-R07 six-step sequence is retired. R08-R12 remain frozen behavioral
requirements for a future activation amendment, but high-watermark capture, drain/replay, writer
cutover, HYBRID enablement, and activation are not implementation steps authorized by this reset.
No phase may supply a temporary Git/zero identity, reserialize legacy bytes, broaden a dispatcher,
or use a legacy entrypoint to compensate for an unfinished earlier gate.

## macOS And Cloud Acceptance

macOS may validate strict parsing, anchored FD traversal, copy/hash/mode fixtures, atomic
rename/fsync models, record recovery, environment stripping, and fail-closed behavior. Fixtures
freeze the explicit current UID and never claim root-owned production success. Linux-only loader,
systemd, root ownership, and `/usr/bin/python3.11` evidence remain gaps, not passes.

Local WRAP acceptance follows the macro and rollout order above: every application-path and
scalar/list row, then R01-R12 in the structural signal-family order, must be red before
implementation and green at its stated rollout gate. Runtime-entry tests must then prove the exact
daily behavior and capability-bound fault identity. Legacy CLI/formal-runtime execution cannot
satisfy any gate.

The separately authorized cloud acceptance is a hard gate and must record exact commands/results:

1. `systemd-analyze verify` succeeds for every affected unit, and the loaded unit has the fixed
   `/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz --role ...` command.
2. The running UID is `lighthouse`; loaded unit, process argv/executable, cwd, import roots, profile
   ID, current authority slot, and generation all identify the same `full_manifest_hash`.
3. Malicious helper requests containing paths, commands, modules, traversal, environment maps,
   extra fields, malformed operation IDs, or FD references are rejected before privileged copy.
4. Self-consistent malicious candidate commit/manifest audit metadata may accompany a candidate
   but cannot choose root behavior, obtain root execution, add permissions, or affect identity;
   only the root-recomputed `full_manifest_hash` identifies the generation. Root-generation
   content/owner/mode/link tampering is rejected, and writes/replacement/deletion by `lighthouse`
   fail. No test claims that v1 detects a false commit provenance assertion.
5. Every crash point in the two-slot single-record matrix is fault-injected and recovers only from
   the last durably validated `current.json`. Current/prior promotion is atomic, automatic rollback
   is limited to one layer, and no directory/audit/process inference is accepted.
6. Wrapper role/path/module/cwd/import/environment injection is rejected. The loaded profile has
   the exact two application roots and variable mappings frozen above; cloud fixtures cover missing
   roots/parents, wrong mappings, `/`, aliases, dot/sibling escapes, symlink and identity swaps,
   special or multiply linked leaves, absent permitted file leaves, and overlap with every trusted
   root. They also cover every scalar/list/endpoint and exact 65536/65537-byte boundary row.
   `PATH`, `HOME`, `PYTHON*`, `LD_*`, user site, checkout, mutable `current`, and any environment
   file other than the fixed root-owned daily application file cannot affect execution. Every
   allowed application value is revalidated against the profile's exact name-to-grammar policy.
   `.pth`, `include-system-site-packages = true`, `sitecustomize`, `usercustomize`, external paths,
   and namespace-package escapes are rejected before `runpy`.
7. The real `/usr/bin/python3.11` bootstrap and `-I -S -c` runtime entry execute on Linux with zero
   skip. Cloud inspection recomputes exactly the profile's canonical Python, ELF loader, stdlib,
   shared-library and ancestor path/SHA256/owner/mode fields plus both pyz hashes. Any difference
   blocks and requires a new ADR/profile version and authorized infrastructure publication.
8. Backup restore and both successful and failed rollback paths preserve one valid authority
   record, launch the recorded exact generation, retain evidence, and never fall back to checkout.
9. The literal `daily` role selects only generation-local `rquant.production_daily_main`; the
   child sees zero caller arguments and the fixed run-daily behavior. Success and injected-error
   paths persist `rquant.signal-envelope/v1` bytes whose
   `full-manifest-sha256/v1.producer_generation_id` is the exact capability-bound current 64-hex
   `full_manifest_hash`, and `signal_id` changes with that complete identity object. Invalid argv,
   extra or malformed environment values, dotenv/checkout/Git influence, zero/truncated/raw-string
   identity, role/module substitution, authority change, and commit audit metadata are rejected
   without a signal/outbox row. All legacy schema versions remain read-only byte-preserved history.
   `runtime_service_main` and formal-runtime execution do not count as this gate.
10. Reset Phase A proves the future R07 models/decoder, frozen v2 differential corpus, strict
    mixed-prefix and crash/orphan behavior, schema-v2 family-neutral pointer, and exhaustive absence
    of every production v3 writer or activation path. Successor contracts, overlay receipts,
    `READY`, high-watermark capture, drain/replay, and cutover remain explicit non-passes until their
    later phase or separately authorized activation amendment exists. No SQLite migration occurs.

## Closure Ledger

These statuses close architecture findings only; implementation and production gates remain open.
Stable IDs are not renamed or reclassified.

| Finding | Status | Frozen evidence |
|---|---|---|
| C-P1-05 | CLOSED | Anchored no-follow quarantine copy, root ownership/modes/fsync, closed-tree rehash, and atomic generation publication |
| C-P1-06 | CLOSED | Fixed `/usr/bin/python3.11` root closure, UID `lighthouse`, stripped environment, and blocking cloud preflight |
| C-P1-07 | CLOSED | Source authenticity removed from v1 assets/TCB; root-derived hash is sole identity and candidate metadata is untrusted audit data |
| C-P1-08 | CLOSED | One atomic two-slot `current.json`, explicit current/prior promotion, one-layer rollback, and no directory inference |
| C-P1-09 | CLOSED | Fixed systemd pyz and generation venv `-I -S -c` bootstrap with manifest-bound paths, `runpy`, and site escape rejection |
| C-P2-01 | CLOSED | Hard gates test malicious metadata without provenance claims plus `.pth`, system-site, customization, namespace, crash, backup, and rollback boundaries |
| C-P2-02 | CLOSED | Versioned profile and cloud gate compare the exact canonical Python/ELF/stdlib/shared-library/ancestor closure and pyz hashes without runtime relaxation |
| WRAP-P1-07 | CLOSED by amended design; implementation gated | Generation-local zero-argument daily adapter, exact role-to-module/environment policy, fixed EnvironmentFile boundary, child authority revalidation, and capability-bound generation identity; signal work follows reset Phase A/B/C and grants no activation |
| WRAP-DESIGN-P1-01 | CLOSED by final design amendment; implementation required | Exact profile data/log roots and variable mappings, anchored path/type/identity checks, trusted-root separation, mutable-data TCB exclusion, and complete path red-test matrix |
| WRAP-DESIGN-P1-02 | CLOSED by final design amendment; implementation required | Canonical required/optional scalar rules, exact CSV/recipient/endpoint grammars, pairing constraints, 65536-byte emitted-environment budget, and boundary-plus-one red tests |
| WRAP-DESIGN-P1-03 | CLOSED by architecture reset; implementation required | Permanently read-only byte-preserved `LegacySignalEnvelope`, structurally discriminated new family, R01-R12, R07 pure model/decoder before successor registry and trusted readiness, exhaustive no-activation inventory, and no SQLite migration |
| WRAP-DESIGN-P1-04 | CLOSED by final design amendment; implementation required | Role/path/environment and signal-family contracts first, artifact/profile/installer planning second, inert runtime/deploy/adapter code third, reset-gated future activation only by a separate amendment, and Linux/root/cloud gates last |
