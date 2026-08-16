# ADR-PIA-001: Root-Owned Bootstrap And Immutable Generations

**Status:** Revised and frozen for implementation. This decision supersedes the global
interpreter FD authority described by the earlier draft. The filename is retained until the
migration is complete.

**Decision:** HYBRID A+C: fixed root-owned bootstrap/runtime wrappers and profile (A), combined
with root-quarantined, completely hashed immutable generations and one atomic runtime authority
record (C). Artifact signing is not part of v1.

## Scope And Threat Model

### Assets

- The bytes executed during privileged publication, bootstrap, and systemd service startup.
- Each complete production generation: application code, dependencies, generation Python,
  `pyvenv.cfg`, resources, metadata, and manifest.
- The root-recomputed `full_manifest_hash` that exclusively identifies each published generation.
- `/var/lib/rquant/runtime-authority/current.json`, rollback generations, and audit evidence.
- Root privilege exposed by the narrowly authorized publication helper.
- Production availability across interrupted copy, publish, record update, restart, and rollback.

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

A complete compromise of the `lighthouse` UID is not in scope for protecting that UID's own
checkout, venv, or processes. That UID remains an actively malicious client when it supplies
requests or bytes to a root helper. No `lighthouse`-writable byte may be imported, executed, or
trusted by root. A malicious accepted generation is executed only later with UID `lighthouse`.

### Attack And Failure Paths

- Environment, `PATH`, Python variables, loader variables, argv, or working-directory injection
  selects an alternate interpreter, profile, wrapper, module, or generation.
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

### Blocking Scope

Production deploy and service-start wiring remain blocked until quarantine copying, complete
generation verification, single-record recovery, the exact runtime wrapper, and Linux/cloud hard
gates are complete. Root-owned installation, systemd/sudoers changes, and production deployment
remain separately authorized infrastructure operations. This ADR does not grant that authority.

## Trusted Computing Base

The residual TCB is:

- kernel and VFS semantics, mount policy, systemd, sudo, and root administration;
- `/usr/bin/python3.11`, required root-owned dynamic loader, standard library, shared libraries,
  and every ancestor from their trusted filesystem anchors;
- `/usr/local/libexec/rquant-production-deploy.pyz`, owner `root:root`, exact mode `0555`;
- `/usr/local/libexec/rquant-runtime-exec.pyz`, owner `root:root`, exact mode `0555`;
- `/etc/rquant/production-runtime-profile.json`, owner `root:root`, exact mode `0444`;
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
closure above, generation/inbox/quarantine roots, allowed operations, allowed runtime
roles/modules, environment allowlists, and manifest schema. Unknown, duplicate, malformed,
non-native scalar, or overridden values are rejected. There is no GID alternative, `{root, euid}`
policy, environment override, argv override, wildcard closure, or runtime-discovered exception.

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
source root, and site-packages roots. Both generation paths must be canonical descendants of the
frozen store and all current/prior fields must validate as complete self-consistent slots. Commit
fields never select content and are excluded from generation identity and trust decisions.

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
input. Units require no dynamic drop-in or `daemon-reload` per generation. They do not load an
`EnvironmentFile` that can override Python, module, profile, generation, cwd, or import behavior.

The wrapper opens the fixed profile and `current.json`, verifies root ownership/modes/ancestry and
the record schema, then revalidates the exact current generation path, generation ID,
`full_manifest_hash`, complete file tree, owner UID, mode, link count, and selected role. Commit is
read only as untrusted audit metadata. The wrapper constructs the role's exact cwd and fresh
allowlisted environment, then uses an absolute exec of the fixed generation-local venv Python with
arguments equivalent to:

```text
<absolute-generation-venv-python> -I -S -c <root-pyz-frozen-bootstrap> \
  <allowlisted-role>
```

`<root-pyz-frozen-bootstrap>` is code frozen inside the already verified root-owned runtime pyz;
it is not generation or record text. It receives only the allowlisted role, inserts only the
absolute application source and site-packages paths bound by `current_roles` and covered by the
root-recomputed manifest, verifies each inserted path is canonical and inside the current
generation, then invokes the allowlisted module with `runpy`.

The generation-local Python, `pyvenv.cfg`, module tree, cwd, application source, and site-packages
are manifest-covered and root-owned. `pyvenv.cfg` must contain exactly
`include-system-site-packages = false`. Verification rejects every `.pth` file,
`sitecustomize.py`, `usercustomize.py`, external/escaping import path, namespace-package portion
outside the two approved roots, and unexpected site-packages entry. The fresh environment removes
`PATH`, all `PYTHON*`, all `LD_*`, user-site/home import influence, and every site/import override.
The wrapper never uses ordinary `-m` site startup, dereferences a `current` symlink, imports from
the checkout, searches `PATH`, or accepts a module/path override.

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
  -> RUNNING
```

Any validation, copy, ownership, mode, hash, fsync, rename, record, wrapper, exec, or health-gate
failure enters `REJECTED` before `AUTHORITY_ACTIVE`, or `BLOCKED` afterward. Neither state permits
fallback to checkout/current/PATH. Rollback is one new atomic `current.json` transaction that
promotes the validated `prior_*` slot and demotes or retires the failed `current_*` slot. It passes
through `AUTHORITY_TEMP_DURABLE -> AUTHORITY_ACTIVE(current=prior, prior=failed-or-retired) ->
RUNTIME_REVALIDATED -> RUNNING`, never mutates a generation, and cannot automatically recurse.

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

Implementation proceeds in bounded batches:

1. Revert the uncommitted authority WIP and reverse the production FD-launch behavior introduced
   by `a35c072`, `106b47c`, and `f21f5d9` using ordinary new commits. Do not reset or rewrite those
   commits; retain unrelated fixes.
2. Add fixed-profile validation, `/usr/bin/python3.11` closure preflight, root-owned bootstrap and
   runtime pyz candidates, strict argument/environment contracts, and an installer candidate.
   Profile tests bind the canonical Python/ELF-loader/stdlib/shared-library/ancestor list and both
   pyz hashes without runtime policy relaxation.
3. Add the no-arbitrary-path root helper, anchored quarantine copy, post-copy root-tree rehash,
   immutable generation publication, and tests proving `full_manifest_hash` is the sole identity
   while malicious candidate manifest/commit metadata grants no privilege or root execution.
4. Add two-slot single `current.json` publication/recovery, one-layer rollback, the complete crash
   matrix, frozen `-I -S -c` role bootstrap, and exact generation/cwd/import/env/site isolation
   tests.
5. Add Linux Python 3.11 exact-entry CI with zero skip. After separate infrastructure approval,
   run the cloud hard gate below. Production wiring remains blocked until it passes.

## macOS And Cloud Acceptance

macOS may validate strict parsing, anchored FD traversal, copy/hash/mode fixtures, atomic
rename/fsync models, record recovery, environment stripping, and fail-closed behavior. Fixtures
freeze the explicit current UID and never claim root-owned production success. Linux-only loader,
systemd, root ownership, and `/usr/bin/python3.11` evidence remain gaps, not passes.

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
6. Wrapper role/path/module/cwd/import/environment injection is rejected. `PATH`, `PYTHON*`,
   `LD_*`, user site, checkout, mutable `current`, and `EnvironmentFile` cannot affect execution.
   `.pth`, `include-system-site-packages = true`, `sitecustomize`, `usercustomize`, external paths,
   and namespace-package escapes are rejected before `runpy`.
7. The real `/usr/bin/python3.11` bootstrap and `-I -S -c` runtime entry execute on Linux with zero
   skip. Cloud inspection recomputes exactly the profile's canonical Python, ELF loader, stdlib,
   shared-library and ancestor path/SHA256/owner/mode fields plus both pyz hashes. Any difference
   blocks and requires a new ADR/profile version and authorized infrastructure publication.
8. Backup restore and both successful and failed rollback paths preserve one valid authority
   record, launch the recorded exact generation, retain evidence, and never fall back to checkout.

## Closure Ledger

These statuses close architecture findings only; implementation and production gates remain open.

| Finding | Status | Frozen evidence |
|---|---|---|
| C-P1-05 | CLOSED | Anchored no-follow quarantine copy, root ownership/modes/fsync, closed-tree rehash, and atomic generation publication |
| C-P1-06 | CLOSED | Fixed `/usr/bin/python3.11` root closure, UID `lighthouse`, stripped environment, and blocking cloud preflight |
| C-P1-07 | CLOSED | Source authenticity removed from v1 assets/TCB; root-derived hash is sole identity and candidate metadata is untrusted audit data |
| C-P1-08 | CLOSED | One atomic two-slot `current.json`, explicit current/prior promotion, one-layer rollback, and no directory inference |
| C-P1-09 | CLOSED | Fixed systemd pyz and generation venv `-I -S -c` bootstrap with manifest-bound paths, `runpy`, and site escape rejection |
| C-P2-01 | CLOSED | Hard gates test malicious metadata without provenance claims plus `.pth`, system-site, customization, namespace, crash, backup, and rollback boundaries |
| C-P2-02 | CLOSED | Versioned profile and cloud gate compare the exact canonical Python/ELF/stdlib/shared-library/ancestor closure and pyz hashes without runtime relaxation |
