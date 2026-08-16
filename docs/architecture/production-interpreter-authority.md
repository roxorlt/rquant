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
| `rquant.runtime_service_main.build_builtin_registry` | the actual top-level production registry entrypoint may not import, construct, inject, return, or make reachable a v3 writer, capability, environment flag, cursor, drain, cutover object, fixture publication path, or overlay branch |
| `rquant.runtime_service_builtin.build_builtin_registry` and all signal-path builders | the implementation factory and builders obey the same reachability prohibition and cannot hide a forbidden object behind a registry entry, closure, protocol, callback, or capability value |

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
Read-only current-envelope parsing, model construction, and synthetic decoder fixtures do not grant
writer authority.

#### R07 No-Activation Test-Only Object-Shape Contract

`RESET-R07-P0-02` freezes the proof contract for
`tests/unit/test_signal_family_no_activation_reset.py`. It is test-only: it neither expands a
production API nor authorizes a v3 writer, registry, overlay, capability, environment flag,
cursor, drain, cutover, mutation, or activation path. It replaces any implication that a best-effort
Python object walk can prove non-reachability.

The threat model is an object returned by an actual Phase A production builder or either actual
builtin registry that hides a durable current-family persistence capability behind an ordinary
alias, closure, callback, module indirection, container, or instance field. A writer shaped as a
callable whose current-family input reaches an existing durable sink is in scope even when its
field is named `sink`, `z`, or anything else. In particular, a durable capability stored in a
slot, or returned by a property, is in scope. These are not new threat classes and MUST NOT be
silently omitted from the proof.

##### Frozen Root Manifest

The proof does not accept an ad hoc root mapping. Its sole input is one immutable
`R07NoActivationRootManifestV1` with exactly these fields:

```text
schema_version: strict native int == 1
capability_profiles: exact tuple[R07CapabilityProfileV1, ...]
roots: exact tuple[R07NoActivationRootV1, ...]
manifest_sha256: lowercase SHA-256
```

`R07CapabilityProfileV1` has exactly `profile_id`, `entries`, and
`unknown_key_policy`; `unknown_key_policy` is the literal `reject`.
`R07CapabilityEntryV1` has exactly `key` and `fixed_value`, both nonempty exact strings.
`R07NoActivationRootV1` has exactly `root_name`, `builder_target`, and `arguments`.
`R07RootArgumentV1` has exactly `parameter_name` and `factory_id`. All models are frozen,
extra-forbid, reject mappings/subclasses in place of exact nested models, and reject duplicate,
unsorted, missing, or extra entries. `manifest_sha256` is SHA-256 of canonical JSON for the complete
manifest excluding `manifest_sha256`.

The only argument factory IDs are `none/v1`, `registry-clock/v1`,
`capabilities-notifier/v1`, and `capabilities-all/v1`. `registry-clock/v1` resolves by exact
identity to the test's fixed aware-UTC `_registry_clock`. A capability factory returns a fresh
exact native `dict[str, str]`; the proof hashes and revalidates that dict before root construction,
after construction, and after graph validation. It may not read `os.environ`, `.env`, a credential
file, a production capability object, or a caller-supplied mapping. `none/v1` returns exact `None`.
No other factory, callable, literal, positional argument, omitted default, or caller override is
accepted.

The capability-profile tuple order is exactly `capabilities-notifier/v1` then
`capabilities-all/v1`. The notifier profile contains the following six entries; the all profile
contains all fourteen entries below. Values are inert test strings and are never used to build or
call a service step:

```text
PUSHDEER_ENDPOINT=https://invalid.rquant.test/pushdeer
PUSHDEER_KEYS=r07-proof-pushdeer-key-v1
PUSHDEER_RECIPIENT_IDS=admin:r07-pushdeer
PUSHPLUS_ENDPOINT=https://invalid.rquant.test/pushplus
PUSHPLUS_RECIPIENT_IDS=admin:r07-pushplus
PUSHPLUS_TOKENS=r07-proof-pushplus-token-v1
RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL=r07-proof-retention-credential-v1
RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID=r07-proof-hmac-key-v1
RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX=0000000000000000000000000000000000000000000000000000000000000000
RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64=cjA3LXByb29mLXByaXZhdGUta2V5LXYx
RQ_REFERENCE_SOURCE_PUBLIC_KEY=r07-proof-public-key-v1
RQ_REFERENCE_SOURCE_SIGNING_KEY_ID=r07-proof-source-key-v1
TUSHARE_TOKEN_BACKUP=r07-proof-tushare-backup-v1
TUSHARE_TOKEN_MAIN=r07-proof-tushare-main-v1
```

Profile entries are sorted by key. A missing fixed key, a changed value, an unknown key, an extra
capability profile, or a capability mapping supplied to a root that does not declare one rejects
the manifest before any builder is called. An empty mapping never means "all capabilities" and is
not a valid factory output.

The root tuple is exact and ordered as follows. Argument bindings are sorted by the target's
declared keyword-only parameter order; every injectable parameter is present even when fixed to
`none/v1`. The target signature must have exactly the listed parameters and no newly added
injection position before root construction can proceed.

| Root | Exact builder target | Exact argument factories |
|---|---|---|
| `direct.strategy_live` | `rquant.runtime_builder_strategy.strategy_live_builder` | `clock=registry-clock/v1`; `evaluator_loader=none/v1`; `completion_attestation_signer=none/v1`; `completion_attestation_active_key_id=none/v1` |
| `direct.signal_router` | `rquant.runtime_builder_signal.signal_router_builder` | `source_loader=none/v1`; `target_resolver=none/v1`; `clock=registry-clock/v1` |
| `direct.notifier` | `rquant.runtime_builder_signal.notifier_builder` | `provider_loader=none/v1`; `capability_environment=capabilities-notifier/v1`; `clock=registry-clock/v1` |
| `direct.shadow_session` | `rquant.runtime_builder_shadow.shadow_session_builder` | `clock=registry-clock/v1`; `input_loader=none/v1`; `session_executor=none/v1` |
| `direct.paper_consumer` | `rquant.runtime_builder_paper.paper_consumer_builder` | `clock=registry-clock/v1` |
| `direct.paper_broker` | `rquant.runtime_builder_paper.paper_broker_builder` | `clock=registry-clock/v1`; `quote_resolver=none/v1`; `trade_date_resolver=none/v1` |
| `direct.serving_publisher` | `rquant.runtime_builder_serving.serving_publisher_builder` | `snapshot_loader=none/v1`; `clock=registry-clock/v1` |
| `direct.daily_orchestrator` | `rquant.runtime_builder_daily_orchestrator.daily_pipeline_orchestrator_builder` | `clock=registry-clock/v1` |
| `registry.builtin` | `rquant.runtime_service_builtin.build_builtin_registry` | exact full binding block below |
| `registry.main` | `rquant.runtime_service_main.build_builtin_registry` | exact full binding block below |

The two full registry binding blocks are:

```text
registry.builtin:
  runtime_capabilities=capabilities-all/v1
  reference_adapter_factory=none/v1
  auction_adapter_factory=none/v1
  adapter_factory=none/v1
  watchlist_quote_provider_factory=none/v1
  universe_loader=none/v1
  clock=registry-clock/v1
  evaluator_loader=none/v1
  signal_source_loader=none/v1
  target_resolver=none/v1
  provider_loader=none/v1
  paper_quote_resolver=none/v1
  trade_date_resolver=none/v1
  serving_snapshot_loader=none/v1
  daily_close_fetcher=none/v1
  shadow_input_loader=none/v1
  shadow_session_executor=none/v1
  candidate_input_loader=none/v1
  auction_candidate_input_loader=none/v1
  artifact_retention_schema_resolver=none/v1
  artifact_terminal_lifecycle_factory=none/v1
  completion_attestation_signer=none/v1
  completion_attestation_active_key_id=none/v1

registry.main:
  runtime_capabilities=capabilities-all/v1
  artifact_retention_schema_resolver=none/v1
  artifact_terminal_lifecycle_factory=none/v1
  completion_attestation_signer=none/v1
  completion_attestation_active_key_id=none/v1
```

This ten-entry manifest is in one-to-one correspondence with the ten builder/registry roots in the
complete no-activation inventory. The proof rejects a missing, duplicate, reordered, renamed, or
extra root, a target that does not resolve by already-imported module/static namespace lookup to
the exact callable identity, an argument not declared by that target, any positional invocation,
or any root result not returned by calling the exact target once with the exact materialized
keyword arguments. Root construction itself must complete without invoking any returned builder,
service step, adapter, provider, transport, filesystem mutation, or network operation.

The proof is deliberately a small, static capability analysis, not a general Python object
inspector. Before it can report a clean result, it MUST establish a complete proof over the exact
manifest root set. The result has exactly one of these outcomes:

```text
complete_clean       every reachable value satisfies this contract; no violation found
complete_violation   an exact forbidden capability composition was found
blocked_shape        a reachable value/edge is not statically supported
blocked_bound        node/depth/edge budget was exhausted
```

Only `complete_clean` may satisfy the no-activation gate. Either blocked outcome, an internal
analyser error, or an incomplete traversal is a test failure, never an empty violation list or a
skipped branch.
The contract is intentionally fail-closed: production builders must be refactored to expose a
supported shape before this Phase A gate can pass; the analyser must not acquire more dynamic
introspection to accommodate them.

The allowed static graph is limited to the following exact shapes and edges. Classification order
is normative: null/atomic, exact module, class, exact function/method/partial, exact native
mapping/sequence, then candidate instance. No later category may rescue a value rejected by an
earlier category. Only an exact module or a class may be passed to `vars(...)`. All other inspection
uses `type(...)`, `type.__getattribute__`, `object.__getattribute__`, `inspect.getattr_static`,
Python bytecode, and exact object identity only. It MUST NOT call user code, invoke a descriptor,
call a property getter, use ordinary `getattr`, iterate an arbitrary object, evaluate annotations,
import a module, or use an object's representation in evidence.

| Shape | Allowed statically visible edges |
|---|---|
| atomic values | `None`, exact native scalar values, `Path`, date/time values, enums, and exact approved read-only current models/classes; terminal only |
| mappings and sequences | exact `dict` with string keys, and exact `tuple` or `list`; mapping keys are sorted by Unicode code point and sequence positions are numeric; `set`, `frozenset`, custom mappings, and custom sequences are unsupported |
| project modules | only an exact, already-imported `rquant` `ModuleType`, through `vars(module)` and only names statically referenced by an allowed function; no import discovery or foreign module traversal |
| classes | class namespace and MRO may be read with `vars(class)` and `type.__getattribute__`; approved read-only current models/classes are terminals, while a class used as a receiver is subject to the same MRO member rules as an instance |
| functions and bound methods | exact `FunctionType`, `MethodType`, or `functools.partial`; defaults, keyword defaults, closure cells, explicit globals referenced by bytecode, and statically resolved receiver attribute reads are traversed |
| plain instances and callable instances | only after the complete instance preflight below, an exact native instance `dict` plus exact Python functions, `staticmethod`, or `classmethod` declared on its MRO |

Candidate-instance preflight is ordered and indivisible:

1. Capture `value_type = type(value)` and `metaclass = type(value_type)`. Read each metaclass-MRO
   namespace only with `type.__getattribute__(owner, "__dict__")` and require an exact native
   `mappingproxy`. Before any `vars(value_type)`, reject a metaclass MRO that defines `__getattr__`
   or overrides the inherited exact `type.__getattribute__`. Then read `value_type.__mro__` only
   with `type.__getattribute__`; reject a non-tuple, non-class member, duplicate, or inconsistent
   MRO. No metaclass descriptor or ordinary class attribute access is executed.
2. Read each MRO class namespace before the exact `object` base with `vars(class)`, nearest class
   first; exact `object` is a trusted terminal and its C-level namespace is not expanded. Before
   touching instance storage, reject any inspected class that defines `__getattr__`, overrides
   `__getattribute__` from the exact inherited implementation, declares `__slots__`, or contains
   any unsupported descriptor. Exact
   Python functions, `staticmethod`, and `classmethod` are the only executable class members
   allowed. Exact native `__dict__` and `__weakref__` storage markers may be present as terminal
   structural markers; every other property, custom descriptor, member descriptor, or get/set
   descriptor blocks regardless of whether current bytecode names it.
3. Locate `__dict__` by static class/MRO lookup. The only permitted storage marker is the nearest
   native instance-dictionary descriptor in that MRO; an inherited native marker is allowed, but a
   nearer replacement by a `property`, custom descriptor, slot/member descriptor, or a missing
   marker yields `blocked_shape`. This storage marker is not traversed or invoked.
4. Only after steps 1-3 succeed, read storage once with
   `object.__getattribute__(value, "__dict__")`. The result must have exact type `dict`, must remain
   the same exact dict identity through graph validation, and must contain exact string keys.
   Otherwise the proof blocks. Ordinary `value.__dict__`, `vars(value)`, descriptor access, and
   fallback to an empty mapping are forbidden.

The native `__dict__` storage marker is the sole structural storage primitive in the contract, not
a general descriptor handler. No property or descriptor-specific resolution branch may be added.
An object whose hostile hooks would be bypassed by `object.__getattribute__` still blocks at step 2;
the bypass is never used to reinterpret an unsupported receiver as safe.

Every other shape is unsupported. This includes `property`, member and get/set descriptors,
arbitrary descriptors, slot storage, proxy objects, custom containers, dynamic attribute hooks,
generator/iterator state, `weakref` indirection, C-extension objects outside the atomic allowlist,
and an unrecognised callable protocol. A descriptor or slot does not become allowed merely because
the analyser can see its type. The contract permits no descriptor-specific fallback logic. If a
function statically reads `self.sink` and static lookup encounters a slot or property, the proof
MUST return `blocked_shape`; it must not execute the getter, inspect a backing convention, or
pretend the edge has no value. Any unsupported descriptor or dynamic attribute hook anywhere in a
traversed receiver MRO blocks the receiver shape.

For a supported callable, the analyser determines referenced receiver attributes from static
bytecode `LOAD_ATTR`/`LOAD_METHOD` operations and resolves them only with static lookup. It does
not infer behavior from an arbitrary string in `co_names`. A direct callable edge may additionally
be reached through a partial, bound method, default, keyword default, closure cell, supported
module global, exact mapping value, or exact sequence element. Graph visits are identity-based and
cycle-safe. Bounds and canonical traversal order are frozen with `ProofResultV1` below.

##### Static Callable-Input Grammar

Whether a callable "accepts current input" is syntax, not runtime typing. The analyser operates
only on an exact Python function's code object and exact native `__annotations__` dict. For a bound
method it analyses `__func__` and statically excludes its exact first receiver parameter. For a
`staticmethod` it excludes none; for a `classmethod` it excludes the exact first class receiver.
A partial is reduced against the underlying exact function using only its exact tuple arguments
and exact string-keyed keyword dict; duplicate binding, binding a variadic parameter, or inability
to derive the remaining explicit parameter set yields `blocked_shape`.

An explicit non-receiver parameter is an exact current input only when its annotation has one of
these two forms:

1. The annotation object is exactly one of `CurrentSignalEnvelope`,
   `CurrentSignalBusRoutedRecord`, or `CurrentSignalRouteSpoolRecord`.
2. The annotation is an exact string whose complete `ast.parse(..., mode="eval")` body is one
   `Name` or a nonempty `Attribute` chain. The root name must exist in the function's exact globals
   dict; every attribute is resolved from an already-imported exact `rquant` module or class with
   static namespace lookup; the terminal object must be exactly one of those three model
   identities. Whitespace, comments, calls, subscriptions, literals, unions, tuples, or any other
   AST form reject as non-exact.

No annotation is evaluated. `typing.get_type_hints`, `eval`, import resolution, ordinary `getattr`,
forward-reference execution, aliases discovered outside the exact globals dict, and descriptor or
metaclass access are forbidden. A global alias is acceptable only because static resolution ends
at the exact model identity; its spelling has no authority.

For a supported wrapper callable from which an exact durable sink is reachable, every remaining
non-receiver parameter must be statically classified before capability composition:

| Parameter syntax | Classification |
|---|---|
| exact current input grammar above | `CURRENT_EXACT` |
| exact concrete non-current class/type identity resolved by the same grammar, excluding `object`, `typing.Any`, protocols, type variables, unions, generics, and subclasses standing in for a declared base | `NONCURRENT_EXACT` |
| missing annotation, `object`, `Any`, unresolved name/attribute, string or AST outside the grammar, union/optional/annotated/generic/protocol/type variable, or a non-type terminal | `AMBIGUOUS_INPUT` and `blocked_shape` |
| `*args` or `**kwargs`, whether annotated or not | `VARIADIC_INPUT` and `blocked_shape` |

At least one `CURRENT_EXACT` parameter plus reachability to an exact durable sink forms an
input/identity composition candidate; it becomes a violation only under the callable-operation
contract below.
All-explicit `NONCURRENT_EXACT` parameters plus a durable sink do not form current-family authority.
Any `AMBIGUOUS_INPUT` or `VARIADIC_INPUT` on that same sink-reachable wrapper blocks the proof before
it can report clean or a violation. A sink-reachable wrapper with zero non-receiver parameters is
`AMBIGUOUS_INPUT` because its accepted input surface is undeclared and therefore also blocks. An
exact frozen durable-sink object encountered as a terminal,
without a wrapping callable/input composition, remains a legacy sink alone and is not re-analysed
as its own wrapper. Return annotations and parameter names do not establish input authority.

##### Static Callable-Operation Contract

`R07-SPEC-P1-05` freezes the final operation layer. Reachability of a sink identity is necessary but
not sufficient: the analyser must prove how a call site obtains and invokes its callee. It uses
`dis.get_instructions(function, show_caches=True)` on an exact `FunctionType` and a non-executing
stack/provenance interpreter. It never calls the function, a callee, a factory, a descriptor, a
property, a user hook, or a code object; it never imports, evaluates, or executes source or
annotations.

The candidate set is fail-closed. Every supported callable with a `CURRENT_EXACT` parameter has
every `CALL` analysed. A callable with an ambiguous/variadic input is also a candidate when it has a
statically reachable durable sink or any unresolved/dynamic call; that unresolved call is potential
sink reachability and cannot be used to avoid the input block. An all-`NONCURRENT_EXACT` callable
with no current parameter is outside the current-family operation composition, except for the
already frozen legacy-sink terminal rule.

The operation interpreter analyses candidate call sites in ascending bytecode offset. A call slice
is the single basic-block stack provenance needed to produce that call's callee. It cannot cross a
jump target, branch, exception-handler boundary, loop edge, yield/await boundary, or another
unresolved stack merge. The only opcodes permitted inside a call slice are:

```text
structural: EXTENDED_ARG CACHE RESUME NOP COPY_FREE_VARS PUSH_NULL PRECALL KW_NAMES
reference:  LOAD_FAST LOAD_CONST LOAD_GLOBAL LOAD_DEREF LOAD_ATTR LOAD_METHOD
invoke:     CALL
```

`EXTENDED_ARG`, `CACHE`, `RESUME`, `NOP`, and `COPY_FREE_VARS` carry no provenance. `PUSH_NULL`,
optional `PRECALL`, and optional `KW_NAMES` must occur only in the exact Python 3.11/3.12 calling
sequence consumed by the same `CALL`. The `LOAD_GLOBAL` push-null flag is normalized to that same
single null marker; it grants no additional lookup form. `KW_NAMES` must resolve to an exact tuple
of unique exact strings whose length does not exceed the `CALL` argument count. Stack depth,
`CALL.arg`, keyword count, and static provenance must agree exactly or the call blocks. An opcode
outside this list is not approximated or skipped if it contributes to a callee. In particular
`LOAD_NAME`, `LOAD_CLASSDEREF`, `LOAD_SUPER_ATTR`,
`BINARY_SUBSCR`, `CALL_FUNCTION_EX`, `IMPORT_NAME`, `IMPORT_FROM`, `IMPORT_STAR`, `BUILD_*`,
`UNPACK_*`, `COPY`, `SWAP`, and every jump/async/generator opcode in callee provenance produce
`blocked_shape`.

The only allowed callee forms are:

```text
STATIC_BASE ("." LITERAL_ATTRIBUTE)* CALL(...)
STATIC_PARTIAL CALL(...)
```

`STATIC_BASE` is exactly one of:

1. `LOAD_GLOBAL literal_name` resolved first from the exact native function-globals dict, otherwise
   from the function's exact native builtins dict. Absence, a non-dict builtins object, or a value
   obtained by fallback/dynamic lookup blocks.
2. `LOAD_DEREF literal_name` resolved to the already captured exact closure-cell content.
3. `LOAD_FAST receiver_name` only for the statically established bound `self`/`cls` receiver. An
   arbitrary local, argument, or reassigned receiver used as a callee base blocks. This receiver
   form must have at least one literal `LOAD_ATTR`/`LOAD_METHOD`; calling the bare receiver blocks.

Each `LITERAL_ATTRIBUTE` comes directly from a `LOAD_ATTR`/`LOAD_METHOD` instruction and is resolved
through the existing allowed-shape static namespace rules. A module attribute such as
`rquant_module.literal_sink` may be allowed; `module.__dict__[name]`, `module.mapping[name]`, any
other subscript, or an attribute from a call result is not. A `STATIC_PARTIAL` must be the already
validated exact `functools.partial` shape whose underlying function and bindings are static under
the callable-input contract.

An allowed call target resolves to one exact object identity before the call is classified. A
static non-sink helper call remains an ordinary graph edge and its exact helper function is analysed
separately. A mere load, formatting use, comparison, return, container insertion, or log/error
message containing a durable-sink identity is not a call operation and cannot form a violation.
Calling an exact partial or bound method whose underlying callable is one of the frozen durable
sinks counts as calling that exact sink.

For an exact durable-sink call inside a callable with a `CURRENT_EXACT` parameter, argument
provenance is deliberately small:

```text
DIRECT_CURRENT   LOAD_FAST of that exact current parameter, optionally named by KW_NAMES
DIRECT_OTHER     LOAD_FAST of an exact NONCURRENT_EXACT parameter
STATIC_LITERAL   LOAD_CONST of an exact immutable scalar/null value
STATIC_IDENTITY  an allowed STATIC_BASE resolved to an exact non-current object
UNKNOWN_VALUE    every call result, attribute/subscript of an input, container build/unpack,
                 arithmetic/format/serialization result, stack merge, or unsupported opcode
```

At least one `DIRECT_CURRENT` argument to the exact durable sink completes the existing identity
composition and yields `complete_violation`. If no current argument reaches the sink and every
argument is `DIRECT_OTHER`, `STATIC_LITERAL`, or `STATIC_IDENTITY`, the sink call is statically
independent of current input and is not a current-family violation. Any `UNKNOWN_VALUE` in that
sink call yields `blocked_shape`; the analyser does not guess whether a serialized, transformed, or
factory-produced value contains the current record. Starred positional/keyword arguments always
block.

The following operation shapes always produce `blocked_shape` before a clean or violation result:

- `globals()[name](record)`, `locals()[name](record)`, or any namespace-call result/subscript used as
  a callee;
- `getattr(owner, name)(record)`, `vars(owner)[name](record)`, or any other dynamic attribute/name
  resolver, even when `name` is constant;
- `module.__dict__[name](record)`, `module.mapping[name](record)`, or any callee obtained through
  `BINARY_SUBSCR`;
- `factory()(record)`, `factory().sink(record)`, or any call result used directly or through an
  attribute as the next callee;
- `__import__`, `importlib.import_module`, `eval`, `exec`, or `compile` invocation, and every import
  opcode, anywhere in an operation-candidate wrapper;
- a parameter/local used as callee, alias assignment/reassignment whose exact identity cannot be
  proven, a control-flow stack merge, `CALL_FUNCTION_EX`, or any other unresolved indirect call.

Direct `obj.literal_attr(...)` syntax is the only attribute-call form; direct `mapping[key](...)`
syntax is never allowed for a callee even when the mapping and key are otherwise static graph
shapes. Exact calls to `globals`, `locals`, `vars`, `getattr`, `__import__`,
`importlib.import_module`, `eval`, `exec`, or `compile` are identified by object identity, not
spelling or alias. Their functions are never executed.

Operation blocking participates in pass 2 after complete graph/shape validation and candidate input
classification, but before any violation conclusion. Wrappers follow canonical graph-path order;
their call sites follow ascending bytecode offset. The first unsupported call returns
`blocked_shape` with the wrapper's canonical path and a stable detail ID of the form
`r07.call.<reason>/v1#<four-digit-call-ordinal>`. No new `ProofEdgeKindV1` value is introduced.
Only after every operation-candidate wrapper has a complete operation proof may exact sink
calls be composed and sorted as violations.

Capability detection is identity-based, not name-based. The test freezes the exact objects in the
durable-sink set, including the v2 filesystem primitives and all inventory persistence entrypoints.
It also freezes the exact three current persistence input model identities. A violation exists when
one supported callable composition accepts an exact current persistence input, reaches an exact
durable sink through only allowed static edges, and invokes that sink through the exact static call
operation with direct current-parameter provenance. The read-only v3 models, decoder functions,
and synthetic verifier are explicitly allowed terminals and cannot become sinks by name, class
name, annotation spelling, alias, or module path. A current input alone, a legacy durable sink
alone, and an unreferenced forbidden object do not constitute a violation; the proof must retain
the canonical evidence path that composes the two.

##### Immutable Proof Result, Ordering, And Bounds

The analyser returns one exact immutable `ProofResultV1`, never a bare violation list or an
exception interpreted as success. `ProofOutcomeV1` has exactly four values: `complete_clean`,
`complete_violation`, `blocked_shape`, and `blocked_bound`. `ProofEdgeKindV1` has exactly:
`root`, `module_member`, `class_member`, `instance_attribute`, `method_function`, `method_receiver`,
`partial_function`, `partial_argument`, `partial_keyword`, `default`, `keyword_default`, `closure`,
`global`, `mapping_key`, `mapping_value`, `sequence_item`, `receiver_attribute`, and `bound`.

The exact models are:

```text
ProofPathSegmentV1:
  edge_kind: exact ProofEdgeKindV1
  token: exact nonempty str

ProofEvidenceV1:
  root_name: exact manifest root name
  path: exact nonempty tuple[ProofPathSegmentV1, ...]
  owner_type: exact module-qualified type name
  detail_id: exact nonempty stable identifier
  current_input_identity: exact qualified identity or null
  durable_sink_identity: exact qualified identity or null

ProofResultV1:
  schema_version: strict native int == 1
  root_manifest_sha256: exact manifest SHA-256
  outcome: exact ProofOutcomeV1
  evidence: exact tuple[ProofEvidenceV1, ...]
  visited_node_count: strict native int >= 0
  max_depth_observed: strict native int >= 0
```

All four models/enums are frozen and extra-forbid; mappings, lists, subclasses, coerced values,
unknown enum values, and mutable nested values reject. For `complete_clean`, `evidence` is empty.
For `complete_violation`, it is nonempty and contains only identity-composition evidence sorted by
canonical path. For either blocked outcome, it contains exactly the first blocked evidence and the
two identity fields are null. A shape finding uses `blocked_shape`; exhaustion of an analysis bound
uses `blocked_bound`. Mixed outcomes or evidence are invalid.

Canonical result bytes are
`rquant.strict_json.canonical_json_bytes(result.model_dump(mode="json"))`, with no trailing newline.
No alternate serializer, enum spelling, field order, whitespace, or Unicode escaping is accepted
as result evidence.

Traversal is two-pass. Pass 1 constructs and validates the complete supported graph and emits no
capability conclusion. Any blocked edge ends pass 1 with the stable blocked result. Only a complete
pass 1 permits pass 2. Pass 2 first classifies every operation-candidate wrapper input in canonical
path order; any ambiguous/variadic input with static or potential sink reachability returns its
stable first `blocked_shape` before capability
conclusions. A complete input-classification subpass is followed by the operation-contract subpass;
its first unsupported call also blocks before conclusions. Only complete input and operation
subpasses compose and sort violations. Thus a violation found early cannot hide a later unsupported
shape, ambiguous input, or unresolved call.

Pass 1 is deterministic depth-first preorder. Roots follow manifest tuple order. Child order is
fixed as follows:

1. Module member names and statically referenced receiver attribute names sort by Unicode code
   point. MRO classes run nearest-to-farthest; within each class, member names sort by Unicode code
   point. Instance-dict attribute names also sort by Unicode code point.
2. A bound method visits function then receiver. A partial visits function, positional arguments
   by ascending index, then keyword arguments by Unicode-sorted key.
3. A function visits positional defaults by ascending parameter position, keyword defaults by
   Unicode-sorted parameter name, closure cells by ascending cell index, then referenced globals by
   Unicode-sorted name. Receiver attributes form the final Unicode-sorted function edge group.
4. A mapping requires exact string keys; for each Unicode-sorted key it visits the key edge then
   the value edge. A tuple/list visits ascending index. Identical objects are expanded only on their
   first canonical path; later identity references remain recorded edges but add no children.

Path tokens are the literal manifest root name, member/key name, or base-10 index appropriate to
the edge kind. They contain no address, timestamp, mapping insertion position, arbitrary `repr`, or
dynamically read value. `owner_type` comes from the statically inspected exact type/class namespace.
These rules freeze the first blocked edge across runs and Python mapping insertion orders.

The constants are exactly `MAX_REACHABILITY_NODES = 10_000`, `MAX_REACHABILITY_DEPTH = 64`, and
`MAX_EDGES_PER_NODE = 512`. Root depth is zero. Before accepting a previously unseen node, a
visited count `>= MAX_REACHABILITY_NODES` blocks; therefore exactly 10,000 unique nodes are allowed
and the 10,001st blocks. Before accepting another child edge, an edge count
`>= MAX_EDGES_PER_NODE` blocks; exactly 512 edges are allowed and the 513th blocks. A node at depth
`>= MAX_REACHABILITY_DEPTH` may be a leaf, but the existence of any child blocks before that child
is accepted. Bound checks precede identity de-duplication for child edges, so repeated references
cannot evade the per-node budget. `visited_node_count` and `max_depth_observed` report only nodes
accepted before completion or the first block.

Evidence MUST contain neither `repr(value)` nor dynamically obtained values. A violation records
the exact current-input identity and durable-sink identity; a blocked shape records the first
unsupported static edge. `R07NoActivationProofError` may exist only as an internal programming
error that fails the test; it cannot be caught and converted to `complete_clean`.

The Phase A adversarial suite is part of this contract, not optional test decoration. It MUST prove
all of the following against the same analyser used for real builder roots:

1. The unmodified concrete roots yield `complete_clean`, and adding a read-only decoder/model
   reference keeps that outcome clean.
2. Renaming a field, wrapping it in a supported dict/list/partial, binding it through a closure,
   default, keyword default, callback, or statically referenced project-module global cannot turn a
   known violation into `complete_clean`.
3. Mutating a supported plain-instance field so that the callable composition reaches every frozen
   durable sink produces `complete_violation`, with the same result regardless of alias name
   or supported mapping insertion order.
4. Replacing that field with `__slots__` storage, a `property`, a custom descriptor, or a receiver
   with hostile `__getattribute__`/`__getattr__` produces `blocked_shape`; a side-effect
   sentinel proves that no getter, descriptor, ordinary attribute access, iterator, or `repr()` was
   executed. These tests must assert the blocked outcome, not merely assert an empty result.
5. Cycles terminate by identity, while deliberately exceeding each node, depth, and edge bound
   yields `blocked_bound`. Tests cover the exact allowed boundary and its first rejected successor
   for all three constants. An unreachable mutation outside the frozen root graph cannot alter
   a complete result.
6. Each adversarial mutation first proves that the injected value is on a declared static edge of
   its test root. This prevents a false-green fixture in which the test places a sink somewhere the
   analyser was never contracted to visit.
7. Every manifest root and every listed injection parameter has an identity/signature test. Missing,
   reordered, duplicate, renamed, unknown, or extra roots/arguments reject. Both capability profiles
   accept only their exact key/value tuples; empty, missing, changed, and unknown capability keys
   reject before root construction.
8. Sink-reachable wrappers cover actual-object and string `Name`/`Attribute` exact-current
   annotations. Missing annotations, `object`, `Any`, unresolved/forward/executable expressions,
   unions/generics/protocols/type variables, non-type terminals, and `*args`/`**kwargs` each produce
   `blocked_shape`. Annotation objects with hostile evaluation hooks and module/class descriptors
   carry sentinels proving no evaluation or dynamic lookup occurred.
9. Shape-order metamorphics place a hostile `__dict__` property, dynamic attribute hook, slot, and
   descriptor at every receiver position. Each blocks before the exact native instance dict is
   read, while a plain inherited native `__dict__` remains supported. Instrumented objects prove
   only modules/classes reached `vars(...)` and no hostile hook/descriptor ran.
10. `ProofResultV1` rejects mutation, subclass/mapping/list substitution, mixed outcome/evidence,
    noncanonical ordering, and unknown enums. Root, attribute, MRO, default, closure, global,
    mapping, partial, and sequence insertion-order metamorphics retain byte-identical results and
    the same first blocked evidence.
11. Operation controls prove that direct global, closure, receiver-literal-attribute, bound-method,
    and validated-partial calls to every frozen sink compose with a direct current parameter as
    `complete_violation`, while merely loading/formatting/returning the same sink identity does not.
    The four reviewer counterexamples are exact fixtures:
    `globals()[name](record)`, `getattr(owner, name)(record)`,
    `module.mapping[name](record)`, and `factory()(record)`. Each yields `blocked_shape` with its
    stable operation detail ID. Each fixture places a side-effect sentinel in the dynamic resolver,
    factory, returned callable, descriptor, and would-be sink and proves every sentinel remains
    untouched. Additional fixtures cover `module.__dict__[name]`, `vars`, `locals`, dynamic
    import/`__import__`, `eval`, `exec`, `compile`, `CALL_FUNCTION_EX`, reassigned local callees,
    control-flow merges, unknown opcode provenance, transformed current arguments, and exact static
    sink calls whose arguments are proven current-independent.

The frozen static callable grammar closes the design requirement `R07-SPEC-P1-01`; the exact root
manifest closes `R07-SPEC-P1-02`; the ordered instance preflight closes `R07-SPEC-P1-03`; and the
immutable result/order/bound contract closes `R07-SPEC-P1-04`. The design finding
`R07-SPEC-P1-05` is closed by the static callable-operation contract and its exact adversarial
matrix. The code-quality finding `R07-CQ-P1-02` closes only when a later implementation has a
red-to-green proof for every listed mutation and the original reviewer verifies it. This amendment
does not close that code finding by itself, and it does not relax `RESET-R07-P0`, `RESET-R07-P1`, or
the frozen v2 byte-identical parser corpus.

##### Frozen Design-Review Scope

This is the second and final document revision for the R07 no-activation proof reset. Subsequent
design review of this reset is limited to a regression introduced by the `R07-SPEC-P1-05` operation
contract or materially new evidence against an existing stable-ledger invariant. Findings
`R07-SPEC-P1-01` through `R07-SPEC-P1-04` are closed and MUST NOT be reopened, renamed, split, or
reissued without such new evidence and an explicit reference to the original ledger ID and failed
invariant. Editorial restatement, a new counterexample already covered by the frozen grammar, or a
different name for the same closed condition is not a new finding. Phase B remains outside this
review scope.

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
| `RESET-R07-P0-01` | the real top-level production registry can make a v3 write/activation object reachable | construct through `runtime_service_main.build_builtin_registry` and prove exhaustive forbidden-object non-reachability |
| `RESET-R07-P0-02` | a best-effort object walk reports clean after silently skipping a slot, property, descriptor, dynamic attribute path, or traversal bound | test-only fail-closed object-shape contract: exact allowed graph, canonical static edges, structured blocked evidence, and adversarial mutations that must violate or block |
| `R07-SPEC-P1-01` | runtime annotation evaluation or ambiguous callable inputs let a sink-reachable wrapper evade current-input classification | exact static annotation grammar; current/non-current exact classification; ambiguous, variadic, unresolved, missing, `object`, and `Any` inputs block without evaluation |
| `R07-SPEC-P1-02` | ad hoc/empty root arguments omit an injection position or treat unknown capabilities as proof coverage | immutable exact ten-root manifest, signature equality, every injection parameter bound, exact fixed capability profiles, and reject-unknown policy |
| `R07-SPEC-P1-03` | shape dispatch calls `vars`/dynamic attributes too early or treats hostile `__dict__`, slots, or descriptors as empty state | normative classification order and complete MRO/static receiver preflight before the single exact native instance-dict read |
| `R07-SPEC-P1-04` | mutable/underspecified results or nondeterministic traversal change the first block or turn bound exhaustion into clean | frozen result/evidence/path models, exact enums/order, two-pass traversal, and exact inclusive-bound semantics |
| `R07-SPEC-P1-05` | identity reachability alone cannot prove that a sink is called, while dynamic/factory/subscript callees can evade a name/reference walk | exact non-executing opcode/call grammar, direct-current argument provenance, blocked dynamic operations, and operation side-effect sentinels |
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
| `RESET-R07-P0`, `RESET-R07-P0-01`, `RESET-R07-P0-02` | `tests/unit/test_signal_family_no_activation_reset.py` | source/API snapshots and mutation probes cover every inventory row and all production builders; construct the registry through `rquant.runtime_service_main.build_builtin_registry` and run the fail-closed object-shape proof over its exact roots. Supported static entries/closures/callbacks/capabilities prove no v3 writer/capability/flag/cursor/drain/cutover object is imported, injected, returned, or reachable; any unsupported shape or bound blocks rather than reports clean; decoder import/construction alone succeeds |
| `R07-SPEC-P1-01` | `tests/unit/test_signal_family_no_activation_reset.py` | exact object and static string `Name`/`Attribute` current annotations classify; missing, `object`, `Any`, variadic, union/generic/protocol/type-variable, unresolved, executable, and descriptor-backed annotations block; sentinels prove no evaluation/import/dynamic access |
| `R07-SPEC-P1-02` | `tests/unit/test_signal_family_no_activation_reset.py` | exact canonical root manifest/hash has all ten inventory roots, exact target identities/signatures, every optional injection fixed explicitly, exact six/all capability profiles, and rejection of empty/missing/changed/unknown/reordered/extra roots, arguments, profiles, keys, and values before builder invocation |
| `R07-SPEC-P1-03` | `tests/unit/test_signal_family_no_activation_reset.py` | shape classification follows the frozen order; only module/class uses `vars`; plain and inherited native instance dicts pass; slot/property/custom-descriptor/dynamic-hook/hostile-`__dict__` shapes block before instance read and execute no user code |
| `R07-SPEC-P1-04` | `tests/unit/test_signal_family_no_activation_reset.py` | exact immutable `ProofResultV1`/evidence/path schemas and enums, two-pass shape-before-capability behavior, canonical first blocked path under all ordering metamorphics, and exact 10,000/10,001 node, depth-64-child, and 512/513 edge boundaries |
| `R07-SPEC-P1-05` | `tests/unit/test_signal_family_no_activation_reset.py` | allowed direct global/closure/receiver-attribute/bound/partial call shapes and direct-current sink arguments violate; mere sink reference does not. Exact `globals()[name](record)`, `getattr(owner, name)(record)`, `module.mapping[name](record)`, and `factory()(record)` reviewer fixtures plus module-dict/subscript, dynamic import/eval/exec, call-result, starred-call, merge, and transformed-current variants block with stable detail IDs and untouched side-effect sentinels |
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
