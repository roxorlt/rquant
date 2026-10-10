# Original private publication fixture

`original-publication-job.json` contains the unchanged persisted records for
synthetic JOB `be75c2f4-ac29-54fa-b021-d425a0b1a779`. The test constructs its own
private SQLite from the original schema, then uses the original registry reader
and writer for the grant, owner, readonly and rejection assertions.

Source: `/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/data/verification/strategy-promotion-20261006/native-domain-implementation-15/green-108-independent-replay/trust/experiments.sqlite`.
The retained source is 234,627,072 bytes, with SHA256
`28794f7f1e243b880eea9822c05d09018900acd513f9905810dc51beaa99e653`.
It was read through `ExperimentRegistryReadonlyReader` in one readonly snapshot.

The fixed JSON is 34,883 bytes, with SHA256
`33b3ce3a64ec809eb597c230c7959739659b43f344066d2907c602da1f20c312`.
It retains all original SQL for 22 user tables and 8 indexes. Twelve unchanged
rows in ten tables provide the JOB's complete outbox intent and child admission,
attempt and formal registration, family manifest and request, private owner,
publication operation, promotion policy, private schema version, holdout policy
and selected configuration. Unrelated rows and prepared replay payloads are
outside this publication fixture's data slice.

The original and reconstructed readers returned equal complete typed submission
intents, with every typed field equal and canonical SHA256
`2a5a5057e17ddee777435e22afd021ce75158872b5270cf0876d943d0ac2a4f0`.
All selected reconstructed SQLite rows also equal their original values.

The one-off extractor and comparison receipt are retained in
`data/verification/goal-progress-root/release-preconditions-20261009-01/ci-fixture-portability-01/env-fixtures-02/metadata/extract_publication_fixture.py`
and its sibling verification directory's `extraction-proof.json`.
