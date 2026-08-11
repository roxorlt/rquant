# Trusted Runtime Code Operations

Formal Lab processes run only from a verified immutable runtime generation. A normal checkout or
linked worktree may supply bytes to the offline packager, but neither is a formal runtime root. No
formal daemon, preflight, or finalizer command may read `.git` or execute Git.

## Trust Material

Use the existing role-separated material:

- offline runtime certificate signed by `rquant_runtime_code_root`;
- private runtime signing key for `rquant_runtime_code_signer`;
- private external promotion key for `rquant_runtime_code_promotion_root`;
- public-only root, runtime, and promotion records in the root-protected bootstrap configuration;
- the existing external monotonic-root authority and its dedicated runtime-code rollback domain.

The CLI never creates keys, certificates, or external-root state implicitly. Private key paths are
accepted only by the explicit offline `package` and `rotate` ceremonies. `install`, `inspect`, and
`dry-run` use public verification material only.

## Command Shape

Every action binds to the same root-protected bootstrap configuration and authority identity:

```bash
rquant runtime-code ACTION \
  --runtime-code-config /etc/rquant/runtime-code-bootstrap.json \
  --runtime-code-trusted-base /etc/rquant \
  --runtime-code-authority-uid 0 \
  --runtime-code-authority-gid 0 \
  --format json \
  [--request /etc/rquant/runtime-code-operation.json]
```

`ACTION` is one of `package`, `install`, `rotate`, `inspect`, or `dry-run`. `inspect` has no request
file. Other actions consume a canonical JSON Pydantic contract. Use `--format text` for a concise
operator view. JSON responses always contain `action`, `status`, and `exit_code`.

Stable exit codes:

| Code | Meaning |
|---|---|
| `0` | Verified operation completed |
| `2` | Invalid or untrusted input |
| `3` | State conflict, stale sequence, legacy residue, or unmet precondition |
| `4` | Required offline crypto or authority transport unavailable |

## Package

The `rquant-runtime-code-package-ceremony/v1` request contains a
`rquant-runtime-code-package-request/v1`, the selected existing runtime key id, and explicit paths
to the runtime and promotion private keys. The package request lists every source file with its
bundle path and mode, the exact execution specification, provenance commit, validity window,
installation/platform binding, sequence, and predecessor receipt hash.

The collector reads listed files through retained descriptors and never asks Git for bytes. A
normal checkout and a linked worktree are both valid `checkout_root` values. Git may be used by an
earlier explicit build step to determine descriptive provenance, but Git metadata is not part of
the signed authority and the package command does not invoke Git.

The output directory is new and contains exactly:

```text
runtime-code.bundle
runtime-code-attestation.json
runtime-code-certificate.json
runtime-code-promotion-receipt.json
```

The command reuses the supplied certificate and keys. It does not generate trust material and does
not contact the external monotonic root. A successful result therefore reports
`external_promotion_required: true`.

## Promotion, Dry Run, And Install

After reviewing a package, publish its exact canonical promotion receipt through the existing
audited external monotonic-root CAS for the configured runtime-code subject. This is a separate
privileged authority operation; do not replace it with a local file, an automatic fallback, or a
new service. The CAS predecessor must be the current receipt hash.

Create a `rquant-runtime-code-migration-request/v1` containing the install request, every formal
service's exact immutable-bootstrap arguments, and the complete list of retired legacy paths that
must be absent. Then run `dry-run` first. It checks:

- canonical public trust material and package artifacts;
- certificate, attestation, bundle, receipt, generation id, sequence, and external current receipt;
- package and installed-generation ownership, mode, and path boundaries;
- exact formal service bootstrap arguments;
- absence of legacy checkout/Git arguments and named legacy residue.

`dry-run` does not create a generation or change `current`/`previous`. Once its JSON result is
`status: ok`, run `install` with the same request. The frozen materializer validates again, writes
and seals a new generation, fsyncs it, and atomically replaces `current`. Any validation or
publication failure leaves the old `current` pointer selected. There is no automatic fallback.

Use `inspect` after installation to verify the selected generation against the live external root
and print its generation id, promotion sequence, provenance, attestation hash, and content root.

## Rotation And Rollback

`rotate` is the recovery ceremony for a retained, already signed package. It verifies the retained
bundle, attestation, and certificate, preserves those exact bytes, and signs a new promotion receipt
with:

- a sequence strictly greater than the currently installed sequence;
- `previous_receipt_sha256` equal to the current receipt hash;
- a newly derived generation id bound to that higher sequence.

Publish the new receipt through the existing external CAS, run `dry-run`, then `install`. The
`previous` pointer is diagnostic only and is never executed automatically. Copying an old pointer,
lowering a sequence, restoring an external-root database, or starting from a retained checkout is
not rollback and must fail closed.

If installation fails after external promotion, keep both generations and stop the formal service.
Correct the package/permission/authority fault or issue another higher-sequence receipt. Never make
the old generation current by editing pointer files.

## Legacy And Service Configuration

Formal service arguments must include the exact configured values for:

```text
--runtime-code-config
--runtime-code-trusted-base
--runtime-code-authority-uid
--runtime-code-authority-gid
```

The migration preflight rejects `--checkout-root`, `--expected-checkout-root`,
`--expected-code-root`, `--trusted-git-path`, and `--release-managed-checkout`. Existing checkout
wrappers and release-generation files may remain for exploratory recovery or packaging, but they
must not appear in a formal service command or be treated as a runtime fallback.

No systemd unit or template is installed or changed by these commands.

## External Linux Gates

Run these exact gates on the supported Linux validation host before production enablement:

```bash
env RQUANT_DISABLE_DOTENV=1 \
  TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
  DATA_DIR=/tmp/rquant-stage8-tests/data \
  DUCKDB_PATH=/tmp/rquant-stage8-tests/data/rquant.duckdb \
  PARQUET_DIR=/tmp/rquant-stage8-tests/parquet \
  LOG_DIR=/tmp/rquant-stage8-tests/log \
  .venv/bin/pytest -q \
  'tests/unit/test_runtime_code_generation.py::test_p0_05_release_tree_rejects_symlink_hardlink_device_fifo_socket_escape_and_collisions[socket]' \
  tests/integration/test_runtime_code_bootstrap.py::test_linux_default_executor_executes_verified_descriptor_after_path_swap
```

If a future release changes `deploy/systemd`, also run the repository's static unit tests locally
and validate every changed unit with `systemd-analyze verify` on Linux. This Batch 4 change does not
modify or install systemd files.
