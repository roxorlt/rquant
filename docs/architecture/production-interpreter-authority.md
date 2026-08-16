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
| `rquant.runtime_service_builtin.build_builtin_registry` and all signal-path builders | no builder may import, construct, inject, or return a durable v3 writer, capability, fixture, overlay branch, cursor, drain, or cutover path |

The builder rule covers
`rquant.runtime_builder_strategy.strategy_live_builder`,
`rquant.runtime_builder_signal.signal_router_builder`,
`rquant.runtime_builder_signal.notifier_builder`,
`rquant.runtime_builder_shadow.shadow_session_builder`,
`rquant.runtime_builder_paper.paper_consumer_builder`,
`rquant.runtime_builder_paper.paper_broker_builder`,
`rquant.runtime_builder_serving.serving_publisher_builder`, and
`rquant.runtime_builder_daily_orchestrator.daily_pipeline_orchestrator_builder`.
Read-only current-envelope parsing, model construction, and synthetic decoder fixtures do not grant
writer authority.

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

For each successor channel, the exact model descriptor hash is:

```text
SHA256(canonical_json_bytes({
  "payload_model": channel.payload_model,
  "declaration_schema_fingerprint": channel.declaration.schema_fingerprint,
  "physical_schema_fingerprint":
      channel.physical_schema.physical_schema_fingerprint
}))
```

Here `channel.payload_model` is the authoritative string stored by the successor base bundle and
must resolve to the actual class in the generation source closure. The other two values come from
that same verified channel object. No overlay supplies or alters any of the three inputs.

Only after a successor base bundle is valid may a subordinate staged overlay bind its already
declared channels. The future overlay bundle schema is exactly
`overlay_namespace`, `overlay_version`, `base_bundle_content_hash`, sorted
`declarations`, and `content_hash`. Each declaration schema is exactly:

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

Every tuple is sorted and duplicate-free. `declaration_hash` is the SHA-256 of strict canonical
declaration bytes excluding itself. `content_hash` binds the exact namespace, version, successor
base bundle content hash, and complete sorted declaration tuple. Bundle fields and declaration
fields are distinct schemas; neither accepts the other's fields. The overlay cannot add a channel,
model, producer, consumer, field, physical schema, or family absent from its successor base.

An absent overlay grants nothing. A partial overlay can be staged but can never become `READY`.
Byte-identical replay is idempotent. Reuse of an overlay identity or declaration identity with
different bytes appends conflict audit evidence and rejects.

### Phase C: Root-Derived Verification And Readiness

Phase C is blocked until the successor base channels and staged overlay exist and until an immutable
in-generation verification manifest, exact service-to-runtime-role bindings, root/release verifier,
and protected append-only verification store are implemented.

#### Exact Pair And Callable-Object Allowlist

The receipt set contains exactly these five pair IDs:
`strategy-router`, `strategy-shadow`, `router-notifier`, `router-paper`, and
`notifier-serving`. Dynamic services from `RuntimeServiceKind.STRATEGY_LIVE` are producer
evidence; they never emit or stand in for reader receipts.

The pair-to-surface map is exact. Producer surfaces prove transport production but do not count as
reader receipts. Reader surfaces are the code exercised for that pair's one verifier-issued receipt.

| Pair ID | Producer-surface evidence | Reader-receipt surfaces |
|---|---|---|
| `strategy-router` | `rquant.strategy_runner.StrategyRunnerStore.process_batch` | `rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch`; `rquant.signal_router_runtime.route_runner_signals` |
| `strategy-shadow` | `rquant.strategy_runner.StrategyRunnerStore.process_batch`; `rquant.strategy_runner.StrategyRunnerStore.publish_session_close_receipt` | `rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch`; `rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot`; `rquant.runtime_shadow_sources.isolated_signal_observations` |
| `router-notifier` | `rquant.signal_route_spool.SignalRouteSpool.publish`; `rquant.signal_route_spool.publish_signal_bus_prefix` | `rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence`; `rquant.notification_state.NotificationStateStore.replicate` |
| `router-paper` | `rquant.signal_route_spool.SignalRouteSpool.publish`; `rquant.signal_route_spool.publish_signal_bus_prefix` | `rquant.signal_route_spool.ReadonlySignalRouteSpool.signals_after_global_sequence`; `rquant.paper_signal_consumer.consume_signal_bus_to_paper`; `rquant.paper_signal_worker.PaperSignalQueueStore.ingest` |
| `notifier-serving` | `rquant.runtime_builder_signal._publish_signal_authority`; `rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish` | `rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__`; `rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble`; `rquant.serving_read_models.build_serving_read_models` |

These entries are callable objects, not free-form receipt strings. The verifier resolves each object
inside the selected generation with its generation interpreter, derives
`object.__module__ + "." + object.__qualname__`, and requires equality with the allowlist entry.
It binds the module's manifest-backed source-relative path and exact source-file hash from the full
manifest. Aliases, wrappers, monkeypatches, alternate modules, unmanifested files, noncallable
objects, omitted surfaces, and additional surfaces reject. Verification starts through the actual
manifest-backed production builders above; direct unit construction cannot substitute for that
path.

#### Dedicated signal_family_verification Authority

Signal-family readiness MUST NOT reuse self-attested
`rquant.schema_compatibility.ConsumerCapabilityReceipt`. A dedicated future
`signal_family_verification` subsystem exposes only verifier-owned operations. The root/release
verifier:

1. acquires `rquant.runtime_authority.RuntimeDeploymentLock` and reopens the current runtime
   authority and validated `RuntimeGenerationSlot`;
2. loads an immutable verification manifest from that generation and checks its hash and path in
   the full manifest;
3. derives the authority operation ID and sequence, generation ID, `full_manifest_hash`, profile
   ID, exact service manifests, and exact service-to-runtime-role mapping;
4. executes every declared family/surface vector with the generation interpreter through the actual
   production builders and validates the strict canonical result bytes and expected hashes;
5. derives observed exact families and surfaces plus test-manifest, execution-result/evidence,
   verifier-policy, service-manifest, and full-manifest hashes; and
6. reopens the authority while still under `RuntimeDeploymentLock`, then appends evidence only if
   the complete authority snapshot is unchanged.

The full manifest is the source closure; no separate caller-claimed source closure can replace it.
Each service ID maps to exactly one runtime role named in the immutable verification manifest. That
mapping must equal the selected `RuntimeGenerationSlot.roles`, the service manifest's executable
module/path, and the full-manifest entries. Missing, duplicate, cross-role, or unmanifested service
bindings reject. `code_commit` remains audit-only and cannot prove authority, source, service,
family, surface, or test execution.

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

The verifier binds that epoch key; verification-manifest hash; exact test-manifest hash; canonical
execution-result/evidence hash; verifier-policy hash; observed family and surface sets;
full-manifest/source-closure hash; service-manifest hashes; and the complete authority snapshot.
Audit records contain identifiers, hashes, timestamps, outcomes, and bounded reason codes only.
They MUST NOT contain signal payloads, environment values, secrets, credentials, raw exception
text, or verification vector inputs.

No public API accepts a caller-created attestation, receipt, result, or evidence model. Only a
module-private evidence object carrying an unforgeable module-private seal may reach the protected
append operation. The root-owned verifier process, root-owned verification manifest and source
closure, generation interpreter, protected store ownership/mode, and deployment lock form the trust
boundary. Copying fields, importing a public model, or invoking a service-local helper grants
nothing.

#### Atomic Receipts, Readiness, And Lifecycle

One successful immutable verifier run atomically emits exactly five receipts, one for each frozen
pair. The receipt uniqueness key is exactly
`(overlay_content_hash, authority_epoch_key, pair_id)`. Each receipt fingerprint binds its pair,
successor declaration hash, overlay hash, authority epoch, generation/profile, exact producer
evidence, exact reader surfaces, verification manifest, result/evidence, verifier policy, service
manifests, test-manifest hash, `verified_at`, and `fresh_until`. An identical replay is idempotent;
divergent bytes for the same key append conflict audit evidence and reject.

Freshness is profile-derived and bounded:
`freshness_seconds = min(stale_after_seconds)` across the exact participating service manifests,
subject to the verifier policy's positive maximum. For the current production profile this evaluates
to 30 seconds because the participating live strategy, router, notifier, and paper services each
declare a 30-second stale bound; the value is derived again from every target profile and is not a
portable constant. `fresh_until = verified_at + freshness_seconds`.

A `READY` decision requires exact five-pair set equality, not a count. It binds every successor
declaration hash required by the overlay; overlay content hash; five sorted pair IDs; five sorted
receipt fingerprints and their aggregate canonical SHA-256; authority epoch, generation, full
manifest, and profile; and `verified_at`/`fresh_until`. The verifier writes the receipts,
immutable decision, and compare-and-swap state in one transaction from one consistent snapshot.
A unique key over overlay plus authority epoch makes concurrent identical finalization return the
same bytes; a divergent concurrent result appends conflict audit evidence and rejects.

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
| `RESET-REG-P0` | caller/service-forged evidence or stale authority can create receipts or `READY` | root-derived sealed verification, lock revalidation, and protected append store |
| `RESET-REG-P1` | successor or overlay grafts current semantics onto v2 or declares nonexistent/future models | unchanged-v2 evidence, actual-model descriptors, and successor-before-overlay enforcement |
| `RESET-REG-P2` | count-only readiness, concurrent divergence, expiry/rollback ambiguity, generation-return replay, or audit leakage | exact set/epoch/CAS/lifecycle tests and bounded audit schema |

The planned red-test matrix is exact:

| IDs | Planned test file/evidence | Required assertions |
|---|---|---|
| `RESET-R07-P1` | `tests/fixtures/signal_route_spool_v2_differential/manifest.json`; `tests/unit/test_signal_route_spool_v2_differential.py` | frozen valid and invalid corpus proves untouched v2 bytes, `ensure_ascii=True`, hashes, models, and public error category/sequence before and after dispatcher introduction |
| `RESET-R07-P1`, `RESET-R07-P2` | `tests/unit/test_current_signal_route_spool_record_v3.py` | exact `E`/`R`/outer bytes and hashes, strict JSON, exact types, all-v2 and one-way mixed chain, v3-first production rejection, isolated decoder-only all-v3 fixture, pointer neutrality, and every crash/orphan/retry conflict case |
| `RESET-R07-P0` | `tests/unit/test_signal_family_no_activation_reset.py` | source/API snapshots and mutation probes cover every inventory row and all production builders; decoder import/construction succeeds while no durable v3 writer/capability/flag/cursor/drain/cutover path exists |
| `RESET-REG-P1` | `tests/unit/test_signal_family_successor_registry_reset.py` | v2 parser/catalog/bytes/hashes/history are unchanged; v2 channels reject current semantic overlay; successor declaration rejects before the actual model exists; exact model descriptor derives from the verified channel declaration and physical schema; partial/absent/conflicting overlay never becomes ready |
| `RESET-REG-P0`, `RESET-REG-P1` | `tests/integration/test_signal_family_verification_reset.py` | all five pair IDs resolve the exact callable objects through real production builders and manifest-backed source hashes; caller-forged/public/service-local evidence rejects; only a successful immutable generation-interpreter run under deployment lock persists five receipts |
| `RESET-REG-P0`, `RESET-REG-P2` | `tests/unit/test_signal_family_readiness_reset.py` | exact five-set equality, atomic receipt/decision/CAS, byte-identical concurrency, divergent conflict, profile-derived expiry, authority advance, authority return to the same generation, rollback, revoke, and no `ATTESTING`/`ACTIVATED` state |
| `RESET-REG-P0`, `RESET-REG-P2` | `tests/unit/test_signal_family_verification_audit.py` | audit schema contains only bounded identifiers/hashes/timestamps/outcomes and excludes payloads, vector inputs, environment, secrets, credentials, and raw exception text |

### Explicit Phase Blockers

These are development prerequisites, not unresolved design questions:

1. Phase A R07 models, strict decoder, v2 differential corpus, mixed-chain verifier,
   crash/orphan matrix, and exhaustive no-activation tests MUST complete before any successor
   current-family base contract is implemented.
2. Successor base channels MUST bind actual manifest-covered transport models before an overlay can
   be staged; future qualnames and v2 semantic overlays reject.
3. The immutable verification manifest, exact service-role bindings, root/release verifier,
   generation-interpreter execution path, module-private sealed evidence, protected append store,
   and deployment-lock revalidation MUST exist before overlay receipts or `READY` can be emitted.
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
