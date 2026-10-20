# Order640 recovery implementation plan

Goal: restore 20 symbols and all 60 streams with complete byte-bounded records. Preserve the existing API and cache.

Baseline: 3305322bce63ff0346e0fd57fc39ff5a82646b17 / tree87884ad20310c09ea475716d040b7b5a4e540022. This source has not passed the full runtime gate. Worktree: /private/tmp/cdx-rquant-l2-order640. Old dirty worktree is preserved.

1. Root freezes the successor SPEC and threat model. Independent SPEC reviewer checks admission. No product edits yet.
2. Original helper implementer fixes only the approved HTTP name collision. Original reviewer checks two-line diff and direct mock regression. This does not restore the source.
3. Diagnostic implementer creates one bounded scalar attribution helper. Reuse the old 68-frame input, 8-profile evidence and all resource/latency/cleanup guards. Root verifies exact source/runtime/helper pins and runs one isolated no-credential Linux diagnostic.
4. Use the measured stage/call/byte facts to pin a minimal product change and direct red regression. Update the SPEC before coding. Native implementer changes only admitted files. The phase2 candidate keeps RawCell as a three-field typed mapping and retains full Python validation in append. CELL-F1 adds only serialize_as_any=True to preserve unknown keys for rejection before commit. Local checks pass: 229 focused regressions, complete640 and34948 batches, exact canonical bytes and unchanged OpenAPI. The unchanged Linux 68-frame gate then failed at 1 GiB (OOM-F1). Admitted source repair R1 keeps one immutable sequence/time receipt per RawL2Cache and one cold-validation lock. Every head read retains full payload hash, inode, sidecar, permission and minimum-sequence checks. Every selected batch remains independently model validated; no payload or mutable model is memoized. Direct tests prove one cold model construction for repeated and concurrent status reads. Complete640/34948 canonical output is unchanged. R1 still requires independent review, exact CI and the original runtime gates; it is not a claim that OOM-F1 is closed. These results do not certify Linux concurrency or live continuity.
5. Independent final reviewer covers final diff, direct dependencies and the frozen failure model once. Fix in stable-ID directed rounds, at most three. Reuse valid evidence.
6. Root runs uninstrumented 68-frame kernel/resource acceptance, complete hashes/cursors and applicable exact CI. R2 and real-source proof retain their exact-tree boundaries. No deployment from a partial or instrumented pass.
7. After all applicable gates, use repository release and deployment flow after the allowed upgrade window. Preserve old cache/desired/policy. Then verify deployed real new data across all 60 streams for at least 20 minutes, early and late HTTP/WS windows.

No market connection is started during diagnosis. No new architecture or guessed row limit. Capacity, bulk, holdings and storage pruning are excluded.
