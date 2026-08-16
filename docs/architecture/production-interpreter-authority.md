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
reading/registry/spool/high-watermark rollout, and Linux/cloud hard gates are complete. Daily
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

## Signal-Family Overlay, Attestation, And R07 Design Freeze

`REG-DESIGN-P1-01` through `REG-DESIGN-P1-06` are a normative, future-only refinement of
`WRAP-DESIGN-P1-03`. They do not authorize a writer, a rollout, a registry migration, a schema
migration, a production flag, or a cutover. In particular, the existing R07 language and the
six-step rollout retain their order: exact v2 preservation precedes any future v3 model work, and
R07 must be proven before a legacy high-watermark can be frozen. Nothing in this section permits
starting that future work or inferring its activation from its data model.

### REG-DESIGN-P1-01: Subordinate Overlay, Never A Second Authority

The authoritative base remains the exact `RuntimeSchemaContractBundle` v2. Its parser, catalog,
canonical bytes, content and physical fingerprints, history, and all v2 acceptance/rejection
behavior are frozen. A missing overlay means v2-only operation. A reader MUST load and verify the
exact canonical v2 bundle first; it MUST NOT use an overlay to discover, repair, reinterpret, or
replace a base bundle.

The future `SignalFamilyOverlay` is separately namespace- and version-scoped and is subordinate to
one verified v2 bundle. It has exactly these immutable binding fields:

```text
overlay_namespace, overlay_schema_version, base_bundle_content_hash,
channel_id, base_channel_declaration_hash, base_channel_content_hash,
base_physical_schema_fingerprint, accepted_family_ids,
payload_model_qualname, payload_model_fingerprint,
profile_derived_producer_service_ids, consumer_service_ids,
surface_ids, declaration_hash, content_hash
```

`base_channel_declaration_hash`, `base_channel_content_hash`, and
`base_physical_schema_fingerprint` bind the unchanged v2 declaration and physical schema. The
payload model qualname and model fingerprint are both exact bindings, not names chosen by a caller.
`profile_derived_producer_service_ids`, `consumer_service_ids`, and `surface_ids` are sorted,
duplicate-free tuples. `accepted_family_ids` is also a sorted, duplicate-free exact tuple. The
overlay declaration and content hashes are SHA-256 hashes of their respective strict canonical JSON
preimages, excluding only the field being calculated; the enclosing overlay content hash binds the
three declarations in sorted `channel_id` order.

There are exactly three declarations, in this exact sorted order:

1. `runtime.strategy_signal.envelope`
2. `runtime.signal_route.spool-record`
3. `runtime.serving.signals`

Their accepted-family and payload-model bindings are exact:

| Channel ID | Exact accepted family IDs | Current payload model qualname |
|---|---|---|
| `runtime.strategy_signal.envelope` | `rquant.signal-envelope/v1` | `rquant.signal_contracts.CurrentSignalEnvelope` |
| `runtime.signal_route.spool-record` | `rquant.signal-route-spool-record/v3` | future `rquant.signal_route_spool.CurrentSignalBusRoutedRecord` |
| `runtime.serving.signals` | `rquant.signal-envelope/v1` | `rquant.runtime_serving_snapshot.SignalDeliveryReadPayload` |

For every row, `payload_model_fingerprint` is the canonical model fingerprint of precisely the
qualname shown, and the declaration's producer, consumer, and surface tuples must match the exact
participant/surface rules below. The v3 name in the second row is a future type identifier only;
it does not exist in the current module and does not permit an import or construction path.

Duplicate, missing, extra, unsorted, or conflicting declarations reject. A byte-identical replay of
the same canonical overlay is idempotent; a different byte sequence or content hash for the same
overlay identity is an audited conflict and rejects. The overlay cannot add, remove, or override a
v2 channel's participants, declaration, physical schema, payload model, or retained history. It can
only attest the exact family/surface restrictions defined here. Dynamic strategy producers are
derived from the authoritative profile's `RuntimeServiceKind.STRATEGY_LIVE` manifests; they are not
reader-receipt identities. `shadow.session.production.v1` is overlay-only and MUST NOT be inserted
into the v2 catalog.

### REG-DESIGN-P1-02: Exact Participants And Surfaces

The overlay's receipt set is equality-checked, not counted. It contains exactly these five logical
pairs and no aliases:

| Pair ID | Producer / consumer | Bound declaration |
|---|---|---|
| `strategy_signal/router` | profile-derived `STRATEGY_LIVE` / `SIGNAL_ROUTER` | `runtime.strategy_signal.envelope` |
| `strategy_signal/shadow` | profile-derived `STRATEGY_LIVE` / `shadow.session.production.v1` | `runtime.strategy_signal.envelope` |
| `spool/notifier` | `SIGNAL_ROUTER` / `NOTIFIER` | `runtime.signal_route.spool-record` |
| `spool/paper` | `SIGNAL_ROUTER` / `PAPER_BROKER` | `runtime.signal_route.spool-record` |
| `serving/serving publisher` | `NOTIFIER` / `SERVING_PUBLISHER` | `runtime.serving.signals` |

The following is the complete surface allowlist. Each surface ID is an enum value resolved by the
trusted verifier to its literal, existing callable qualname. It is not a user-supplied string. A
missing, added, renamed, aliased, or differently resolved callable rejects the overlay and its
attestation.

| Surface ID | Exact existing callable qualname |
|---|---|
| `router.runner_read` | `rquant.signal_router_runtime.StrategyRunnerSignalSource.read_batch` |
| `router.route` | `rquant.signal_router_runtime.route_runner_signals` |
| `shadow.runner_fan_in` | `rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot` |
| `shadow.observation` | `rquant.runtime_shadow_sources.isolated_signals_to_shadow_observations` |
| `notifier.spool_replicate` | `rquant.notification_state.NotificationStateStore.replicate` |
| `notifier.serving_result` | `rquant.runtime_builder_signal._signal_source_result` |
| `paper.spool_to_queue` | `rquant.paper_signal_consumer.consume_signal_bus_to_paper` |
| `paper.queue_ingest` | `rquant.paper_signal_worker.PaperSignalQueueStore.ingest` |
| `paper.lifecycle` | `rquant.strategy_paper_lifecycle.PaperBrokerLifecycleReader.resolve` |
| `serving.source_result` | `rquant.runtime_serving_snapshot.SourceReadResult` |
| `serving.snapshot_assemble` | `rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble` |
| `serving.read_model_assembly` | `rquant.serving_read_models.build_serving_read_models` |

Each declaration contains precisely this producer set, consumer set, and surface subset; symbolic
service kinds resolve to the exact current profile manifest service IDs before hashing:

| Channel ID | Exact producers | Exact consumers | Exact surfaces |
|---|---|---|---|
| `runtime.strategy_signal.envelope` | profile-derived `STRATEGY_LIVE` services | `SIGNAL_ROUTER`, overlay-only `shadow.session.production.v1` | `router.runner_read`, `router.route`, `shadow.runner_fan_in`, `shadow.observation` |
| `runtime.signal_route.spool-record` | `SIGNAL_ROUTER` | `NOTIFIER`, `PAPER_BROKER` | `notifier.spool_replicate`, `paper.spool_to_queue`, `paper.queue_ingest`, `paper.lifecycle` |
| `runtime.serving.signals` | `NOTIFIER` | `SERVING_PUBLISHER` | `notifier.serving_result`, `serving.source_result`, `serving.snapshot_assemble`, `serving.read_model_assembly` |

The union across all three declarations is exactly the allowlist table above. Omission, addition,
substitution, or aliasing of a participant or surface rejects.

### REG-DESIGN-P1-03: Trusted Attestation Is Derived, Not Claimed

A service receipt is advisory evidence only. It is never an authority input. The trusted release
verifier reopens the validated current `RuntimeGenerationSlot` and its current runtime authority,
then derives, rather than accepts from a receipt: authority sequence and operation ID; current
generation ID and byte-for-byte full-manifest equality; profile ID; role, service, and service
manifest binding; executable source closure; approved test-manifest binding; and the exact overlay,
family, declaration, and surface bindings.

The future `SignalFamilyAttestation` has exactly these fields:

```text
attestation_schema_version, attestation_id, observed_at,
authority_operation_id, authority_sequence, runtime_generation_id,
full_manifest_hash, profile_id, role_id, service_id, service_manifest_hash,
overlay_namespace, overlay_schema_version, overlay_content_hash,
overlay_declaration_hash, channel_id, accepted_family_ids, surface_ids,
payload_model_qualname, payload_model_fingerprint,
source_closure_hash, approved_test_manifest_hash, code_commit
```

All hashes are lowercase SHA-256 digests over strict canonical JSON or exact bytes identified by
their field names. Ordered lists are sorted and duplicate-free before hashing. `code_commit` is
audit metadata only: it can be displayed and compared for audit, but it cannot establish authority,
generation, source closure, family, or surface eligibility. The verifier obtains every authority
field from the reopened current authority and full manifest, then compares every derived binding to
the attestation. A copied, self-issued, stale, wrong-service, wrong-generation, wrong-profile,
wrong-role, wrong-surface, wrong-test-manifest, or wrong-authority-sequence attestation fails
closed. The verifier also rejects noncanonical JSON, duplicate keys, coercion, mappings in place of
the exact model, and subclasses in any strict future model boundary.

### REG-DESIGN-P1-04: R07 Routed Record And Recovery Contract

All existing v2 serializers, parsers, fixture bytes, hashes, `SignalRouteSpoolRecord`, and
`SignalRouteSpoolPointer` are preserved byte-for-byte. The existing v2 pointer remains
family-neutral; there is no v3 pointer migration and a pointer never selects a family.

The future `CurrentSignalBusRoutedRecord` is non-self-hashing and contains an exact
`CurrentSignalEnvelope` only. Let `E = current_signal_envelope_json_bytes(envelope)`. Its
`payload_json` UTF-8 bytes MUST equal `E` exactly and `envelope_hash = SHA256(E)`. The routed record
hash uses the existing strict canonical JSON-byte algorithm and covers the receipt, targets,
timestamps, and envelope exactly. It is not derived from a reparsed, pretty-printed, or normalized
payload.

The future outer v3 record has exactly these fields:

```text
schema_version = 3, global_sequence, previous_record_hash, envelope_hash,
routed_record_hash, record_hash, record
```

`schema_version` and `global_sequence` are native strict integers; booleans, floats, strings,
coercion, mappings, subclasses, duplicate keys, and extra fields reject. `record_hash` is SHA-256
of the strict canonical preimage containing every listed field except `record_hash` itself. The
structural dispatcher is duplicate-free and never parse-then-coerces. A mixed stream has exactly
one v2-to-v3 transition and never returns to v2; the first v3 record may chain to the final v2
record hash. A v3 record cannot appear before a valid v2 prefix and an all-v3 stream is not an R07
substitute for that transition proof.

Future recovery freezes this crash matrix for the records directory and the unchanged pointer:

| Stage | Required recovery result |
|---|---|
| record temporary write | temporary file is never authority and an old pointer ignores it |
| record file fsync | a retry with identical bytes is accepted; different bytes for the same sequence reject |
| record link / publication | only one complete immutable record name may become visible |
| records-directory fsync | a visible record is part of a verified complete prefix only after directory durability |
| pointer temporary write and fsync | temporary pointer is never authority |
| pointer replace | visible pointer is accepted only when it validates the complete record prefix it names |
| root-directory fsync | crash recovery retains the old valid pointer or the new fully validated pointer, never a partial authority |

Recovery ignores orphan records beyond the visible pointer, accepts an identical retry, and records
an audited conflict for a differing retry. It validates every record in the visible prefix and every
chain edge before exposing it. Strict decoding rejects Unicode normalization changes, whitespace or
key-order changes, duplicate keys, numeric coercion, `NaN`/infinite constants, alternate datetime
spellings, and newline changes whenever the exact canonical bytes require a particular spelling.

### REG-DESIGN-P1-05: Receipt And Readiness Lifecycle

Receipts and their history are append-only. Their uniqueness key is exactly `(overlay_declaration_hash,
target_runtime_generation_id, channel_id, service_id)`. A byte-identical retry is idempotent. A
different receipt for that key appends conflict audit evidence and rejects. Acceptance requires
exact equality of the five-pair set above, current authority sequence, current slot, full manifest,
profile, overlay, service manifest, audit commit, and freshness window; a count of five is never
sufficient.

The trusted verifier creates an immutable readiness decision and updates its compare-and-swap state
in one transaction from one consistent snapshot. The decision binds the sorted selected receipt
fingerprints and their aggregate canonical SHA-256 hash, the overlay declaration/content hashes,
and the derived authority/generation/profile values. Concurrent final acknowledgements can create
only one deterministic decision for that snapshot. Authority advance or receipt expiry invalidates
readiness before any later use.

The only lifecycle states are:

```text
DECLARED -> ATTESTING -> READY -> ROLLED_BACK
DECLARED -> REVOKED
ATTESTING -> REVOKED
READY -> REVOKED
```

There is no `ACTIVATED` state. Rollback and revocation are append-only evidence and disable only
future activation eligibility; they do not rewrite receipts, decisions, records, or historical
readability.

### REG-DESIGN-P1-06: No Activation Or Production v3 Writer

R07 is limited to future model, strict decoder, and synthetic-fixture support. It creates no
production-obtainable v3 publisher, capability, constructor, environment flag, overlay branch,
cursor/high-watermark behavior, drain behavior, or cutover inference. Runtime builders MUST NOT
construct or import a v3 writer or fixture. Legacy writes remain unchanged.

The following existing current-family writer boundaries are explicitly fail-before-mutation and
MUST remain so until a separately approved activation ADR changes this section:

| Boundary | Current status and frozen fail-before-mutation requirement |
|---|---|
| `rquant.strategy_runner.StrategyRunnerStore.process_batch` | constructs and persists only `SignalEnvelope`; it has no current-family constructor path |
| `rquant.signal_bus.SignalBusStore.ingest` | `require_legacy_signal_write` executes before its write transaction |
| `rquant.signal_bus.SignalBusStore.commit_source_route` | `require_legacy_signal_write` executes before route-source persistence |
| `rquant.signal_bus.SignalBusStore.route` | parsed `CurrentSignalEnvelope` rejects before any outbox transition |
| `rquant.signal_router_runtime.route_runner_signals` | every runner record is checked by `require_legacy_signal_write` before route binding/commit |
| `rquant.notification_state.NotificationStateStore.replicate` | every routed record is checked before the notification write transaction |
| `rquant.runtime_serving_snapshot.SignalDeliveryPayload` | registry-writer validation calls `require_legacy_signal_write` before it can publish a serving payload |
| `rquant.paper_signal_worker.PaperSignalQueueStore.ingest` | its direct path already requires the exact legacy `SignalEnvelope`; its supplied stored-byte path is transitively fenced today by `SignalBusStore` and MUST gain the same local current-family rejection before any future implementation could make that path production-obtainable |

The read-capable current-family paths, including `parse_signal_envelope`, stored-record dispatch,
`CurrentRunnerSignalRecord`, shadow observation, `SourceReadResult` adaptation, and serving
read-model input, do not constitute an activation path. They cannot grant a writer capability or
select a family through a spool pointer, cursor, high-watermark, environment, runtime builder, or
readiness decision.

### Stable Red-Test Matrix

These are planned red tests. They are precise acceptance gates for the future implementation; this
design freeze adds no tests and does not make any of them green.

| ID | Planned test file | Required assertions |
|---|---|---|
| `REG-DESIGN-P1-01` | `tests/unit/test_signal_family_overlay_reg_design.py` | v2 bundle canonical bytes/parser/catalog/fingerprints/history remain unchanged; missing overlay selects v2-only; exactly the three sorted declarations bind the exact base hash/channel/declaration/physical fingerprint/model; duplicate, missing, extra, conflict, and nonidentical replay reject; byte-identical replay is idempotent. |
| `REG-DESIGN-P1-02` | `tests/unit/test_signal_family_overlay_reg_design.py` | the five-pair set equals the frozen set, dynamic `STRATEGY_LIVE` producers derive from manifests without becoming reader receipts, `shadow.session.production.v1` is absent from v2, and each surface ID resolves only to the listed existing qualname; omission/addition/alias rejects. |
| `REG-DESIGN-P1-03` | `tests/unit/test_signal_family_attestation_reg_design.py` | trusted verifier derives rather than trusts service fields; copied/self/stale/wrong service/generation/profile/role/surface/test manifest/authority sequence all fail; audit-only `code_commit` mutation cannot establish authority; canonical source-closure, test-manifest, family, and surface hashes bind exactly. |
| `REG-DESIGN-P1-04` | `tests/unit/test_signal_route_spool_r07_v3.py` | v2 fixture bytes/hashes and v2 pointer are identical; synthetic future v3 validates exact `E`, hashes, strict fields, one-way v2-to-v3 chain, and final-v2 predecessor; duplicates, Unicode/whitespace/key-order/numeric/NaN/datetime/newline variants reject; every crash stage yields only an old or complete verified prefix, ignores an orphan, accepts identical retry, and rejects conflict. |
| `REG-DESIGN-P1-05` | `tests/unit/test_signal_family_readiness_reg_design.py` | receipt uniqueness uses the four-field key; identical retry is idempotent and conflict is audited/rejected; exact five-pair equality, service-manifest/freshness/authority checks, transactional sorted fingerprint aggregate, deterministic concurrent final acknowledgement, expiry/authority invalidation, and only the frozen lifecycle transitions are enforced. |
| `REG-DESIGN-P1-06` | `tests/unit/test_signal_family_no_activation_reg_design.py` | no runtime builder imports or constructs a v3 writer/fixture; no flag/capability/overlay/cursor/high-watermark/drain/cutover activates v3; every listed writer boundary, including both `PaperSignalQueueStore.ingest` forms, rejects a current-family write before a durable mutation; legacy writer behavior and all v2 bytes remain unchanged. |
| `R07` | `tests/unit/test_signal_route_spool_r07_v3.py` | the R07 v2 preservation assertions run before any synthetic-v3 assertions, and the test proves that no production-obtainable v3 write path exists while retaining the existing high-watermark rollout order. |

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
Linux/root/cloud gates. The structural signal rollout below is an additional monotonic activation
gate, not a replacement order: contract implementation precedes artifact planning, artifacts
precede runtime/adapter code, new runtime entries remain inert until their rollout stage permits
them, and cloud gates remain last.

The `WRAP-DESIGN-P1-03` rollout is non-skippable and strictly ordered:

1. Add the dual family dispatcher/reader and R01-R06 red tests, then update every direct and nested
   consumer. Release reader support only; deployed writers remain legacy and the new writer API is
   disabled.
2. Add registry declarations and explicit consumer acknowledgement for exactly
   `runtime.strategy_signal.envelope`, `runtime.signal_route.spool-record`, and
   `runtime.serving.signals`. No writer cutover proceeds while any declared consumer lacks new-family
   acknowledgement.
3. Preserve every existing spool v2 byte/hash path and add spool v3 emission only for the new
   family. R07 must prove both branches before any high-watermark is frozen.
4. Quiesce legacy writers, freeze each registered store's inclusive maximum durable legacy cursor
   as its immutable high-watermark, drain/replay only exact bytes at or below it, and prove R08/R11.
   No legacy append is permitted after the watermark.
5. Cut over the strategy runner, daily summary, and daily error writer paths to new-family-only
   APIs; remove zero-producing new writes. HYBRID output remains disabled in this step.
6. Enable HYBRID daily success/error writers only after all registered readers acknowledge the new
   family. R09/R10/R12 and the exact authority-capability checks must be green first.

No later phase may supply a temporary Git/zero identity, reserialize legacy bytes, broaden the
dispatcher, or use a legacy entrypoint to compensate for an unfinished earlier gate.

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
10. Registry acknowledgements cover exactly the three declared contracts. Spool v2 bytes/hashes,
    spool v3 new-family-only emission, frozen per-store high-watermarks, exact legacy replay/drain,
    new-writer gates, and absence of any SQLite schema migration match R01-R12.

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
| WRAP-P1-07 | CLOSED by amended design; implementation gated | Generation-local zero-argument daily adapter, exact role-to-module/environment policy, fixed EnvironmentFile boundary, child authority revalidation, and capability-bound generation identity; implementation follows the four macro phases and six-step signal rollout |
| WRAP-DESIGN-P1-01 | CLOSED by final design amendment; implementation required | Exact profile data/log roots and variable mappings, anchored path/type/identity checks, trusted-root separation, mutable-data TCB exclusion, and complete path red-test matrix |
| WRAP-DESIGN-P1-02 | CLOSED by final design amendment; implementation required | Canonical required/optional scalar rules, exact CSV/recipient/endpoint grammars, pairing constraints, 65536-byte emitted-environment budget, and boundary-plus-one red tests |
| WRAP-DESIGN-P1-03 | CLOSED by structural baseline; implementation required | Permanently read-only byte-preserved `LegacySignalEnvelope`, structurally discriminated new family, capability-only HYBRID identity, R01-R12, registry/spool/high-watermark rollout, and no SQLite migration |
| WRAP-DESIGN-P1-04 | CLOSED by final design amendment; implementation required | Role/path/environment and signal-family contracts first, artifact/profile/installer planning second, inert runtime/deploy/adapter code third, rollout-gated activation thereafter, and Linux/root/cloud gates last |
