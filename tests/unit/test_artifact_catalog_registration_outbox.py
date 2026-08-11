from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from rquant.artifact_catalog_registration_outbox import (
    ArtifactCatalogRegistrationOutbox,
    ArtifactCatalogRegistrationRequest,
)
from rquant.artifact_retention import (
    ArtifactBundleRegistration,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    OwnerTerminalReleaseReceipt,
    StorageTier,
)

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)
CONTENT = "a" * 64


def _request() -> ArtifactCatalogRegistrationRequest:
    references = tuple(
        ObjectReference(
            owner_type=owner_type,
            owner_id=owner_id,
            content_sha256=CONTENT,
            created_at=NOW,
        )
        for owner_type, owner_id in (
            ("audit", "b" * 64),
            ("experiment", "c" * 64),
            ("job", "11111111-1111-4111-8111-111111111111"),
            ("snapshot", "d" * 64),
        )
    )
    registration = ArtifactBundleRegistration(
        object_identity=ObjectIdentity(
            content_sha256=CONTENT,
            size_bytes=1,
            object_kind="strategy_lab_shard_result_bundle",
            created_at=NOW,
        ),
        object_copy=ObjectCopy(
            content_sha256=CONTENT,
            location_id="cloud-primary",
            storage_uri="file:///tmp/artifact.bundle",
            storage_tier=StorageTier.HOT,
            verified_at=NOW,
            failure_domain="primary-volume",
            tier_entered_at=NOW,
        ),
        references=references,
    )
    job_reference = next(item for item in references if item.owner_type == "job")
    return ArtifactCatalogRegistrationRequest(
        registration=registration,
        job_terminal_receipt=OwnerTerminalReleaseReceipt(
            reference_id=job_reference.reference_id,
            owner_type="job",
            owner_id=job_reference.owner_id,
            content_sha256=CONTENT,
            terminal_state="succeeded",
            lifecycle_revision=1,
            evidence_sha256="e" * 64,
            released_at=NOW,
        ),
        enqueued_at=NOW,
    )


def test_catalog_registration_outbox_is_idempotent_and_recovers_claim(tmp_path: Path) -> None:
    outbox = ArtifactCatalogRegistrationOutbox(tmp_path / "catalog-registration-outbox")
    request = _request()

    assert outbox.enqueue(request) is True
    assert outbox.enqueue(request) is False

    claimed = outbox.claim_next(limit=1)
    assert claimed == (request,)

    restarted = ArtifactCatalogRegistrationOutbox(tmp_path / "catalog-registration-outbox")
    assert restarted.recover_claims() == 1
    assert restarted.claim_next(limit=1) == (request,)
    restarted.complete(request)
    assert restarted.claim_next(limit=1) == ()
    assert restarted.enqueue(request) is False
    assert restarted.pending_count() == 0
