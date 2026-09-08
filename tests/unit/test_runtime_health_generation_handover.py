"""#248 shape (4): one stale heartbeat from a previous generation took the payload down.

On 2026-09-09 `artifact-catalog.primary.v1` had exited immediately under #217 and left a
stopped heartbeat carrying the *previous* generation's spec fingerprint. The health
authority read it, found a fingerprint that is not the current spec's, and raised
`RuntimeHealthAuthorityIntegrityError` out of the whole read — so no payload was published
at all and the serving plane degraded behind one stopped research role.

Two rules come out of that, and they are separate:

* a **stopped** heartbeat whose fingerprint belongs to one of our own previous
  generations is *superseded*: its heartbeat is left out, the payload names it, and the
  payload is still published. A heartbeat that is still live with a fingerprint that does
  not match is a real conflict and still refuses (ruling 14 / #216);
* whatever one source's read does, it may not take the other twenty-four with it. Any
  read failure becomes that service's own DEGRADED entry, named in the reason.

The serving `RuntimeHealthPayload` does not gain a field for either: "superseded" is a
reason string, and the entry uses the `heartbeat=None` shape a missing heartbeat already
uses. `tests/unit/test_runtime_schema_release_snapshot.py` is what holds that line.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import rquant.runtime_health_authority as health_module
from rquant.runtime_health_authority import (
    RuntimeHealthAuthorityIntegrityError,
    RuntimeHealthSourceReader,
)
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServiceStatus,
    RuntimeStepResult,
)
from rquant.serving_contracts import FreshnessStatus
from tests.unit.test_runtime_health_authority import NOW, _running, _source, _spec

PREVIOUS_IDENTITY = "b" * 64
FOREIGN_IDENTITY = "c" * 64


def _stopped_with_identity(
    root: Path,
    service_id: str,
    *,
    spec_fingerprint: str,
    status: RuntimeServiceStatus = RuntimeServiceStatus.STOPPED,
    clear_stopped_at: bool = False,
) -> None:
    """Leave behind exactly what a previous generation's role leaves when it exits."""

    spec = _spec(service_id)
    control = RuntimeServiceControl(root, spec=spec, clock=lambda: NOW)
    control.start()
    control.record_success(RuntimeStepResult(input_sequence=1, output_sequence=1))
    control.stop(reason="previous generation exited")
    path = RuntimeServiceControl._path_for(root, spec)
    heartbeat = RuntimeServiceControl.read_heartbeat(root, spec)
    assert heartbeat is not None
    update: dict[str, object] = {"spec_fingerprint": spec_fingerprint, "status": status}
    if clear_stopped_at:
        update["stopped_at"] = None
        update["stop_reason"] = None
    path.write_text(heartbeat.model_copy(update=update).model_dump_json(), encoding="utf-8")


def test_a_stopped_heartbeat_from_our_own_previous_generation_is_superseded(
    tmp_path: Path,
) -> None:
    catalog_root = tmp_path / "control" / "artifact-catalogs" / "catalog"
    feature_root = tmp_path / "control" / "features" / "feature"
    _stopped_with_identity(catalog_root, "catalog", spec_fingerprint=PREVIOUS_IDENTITY)
    _running(feature_root, "feature")

    result = RuntimeHealthSourceReader(
        sources=(_source(catalog_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
        previous_spec_identities={"catalog": (PREVIOUS_IDENTITY,)},
    )(NOW)

    assert result.status is FreshnessStatus.DEGRADED
    assert result.reason is not None
    assert "superseded:catalog" in result.reason
    entries = {item.service_id: item for item in result.payload.runtime_services}
    #: the whole payload is still published, and every other role is in it untouched
    assert set(entries) == {"catalog", "feature"}
    assert entries["catalog"].heartbeat is None
    assert entries["catalog"].status is RuntimeServiceStatus.MISSING
    assert entries["catalog"].stale is True
    assert entries["feature"].status is RuntimeServiceStatus.RUNNING


def test_a_live_heartbeat_with_a_mismatching_fingerprint_is_still_a_conflict(
    tmp_path: Path,
) -> None:
    catalog_root = tmp_path / "control" / "artifact-catalogs" / "catalog"
    feature_root = tmp_path / "control" / "features" / "feature"
    _stopped_with_identity(
        catalog_root,
        "catalog",
        spec_fingerprint=PREVIOUS_IDENTITY,
        status=RuntimeServiceStatus.RUNNING,
        clear_stopped_at=True,
    )
    _running(feature_root, "feature")

    result = RuntimeHealthSourceReader(
        sources=(_source(catalog_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
        previous_spec_identities={"catalog": (PREVIOUS_IDENTITY,)},
    )(NOW)

    entries = {item.service_id: item for item in result.payload.runtime_services}
    assert entries["catalog"].status is RuntimeServiceStatus.DEGRADED
    assert entries["catalog"].heartbeat is None
    assert "unreadable:catalog" in (result.reason or "")
    #: and the conflict itself is still raised where a caller asks for that one heartbeat
    with pytest.raises(RuntimeHealthAuthorityIntegrityError, match="does not match service spec"):
        health_module._read_heartbeat(
            _source(catalog_root, "catalog"),
            max_bytes=1_000_000,
            previous_spec_identities=(PREVIOUS_IDENTITY,),
        )


def test_a_stopped_heartbeat_from_a_foreign_generation_is_still_a_conflict(
    tmp_path: Path,
) -> None:
    catalog_root = tmp_path / "control" / "artifact-catalogs" / "catalog"
    feature_root = tmp_path / "control" / "features" / "feature"
    _stopped_with_identity(catalog_root, "catalog", spec_fingerprint=FOREIGN_IDENTITY)
    _running(feature_root, "feature")

    result = RuntimeHealthSourceReader(
        sources=(_source(catalog_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
        previous_spec_identities={"catalog": (PREVIOUS_IDENTITY,)},
    )(NOW)

    entries = {item.service_id: item for item in result.payload.runtime_services}
    assert entries["catalog"].status is RuntimeServiceStatus.DEGRADED
    assert "unreadable:catalog" in (result.reason or "")
    assert entries["feature"].status is RuntimeServiceStatus.RUNNING


def test_one_unreadable_heartbeat_no_longer_fails_the_whole_payload(tmp_path: Path) -> None:
    catalog_root = tmp_path / "control" / "artifact-catalogs" / "catalog"
    feature_root = tmp_path / "control" / "features" / "feature"
    _running(catalog_root, "catalog")
    _running(feature_root, "feature")
    RuntimeServiceControl._path_for(catalog_root, _spec("catalog")).write_text(
        "{not json",
        encoding="utf-8",
    )

    result = RuntimeHealthSourceReader(
        sources=(_source(catalog_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
    )(NOW)

    entries = {item.service_id: item for item in result.payload.runtime_services}
    assert entries["catalog"].status is RuntimeServiceStatus.DEGRADED
    assert entries["catalog"].heartbeat is None
    assert "unreadable:catalog" in (result.reason or "")
    assert entries["feature"].status is RuntimeServiceStatus.RUNNING
    assert entries["feature"].heartbeat is not None


def test_a_superseded_and_an_unreadable_source_hash_differently(tmp_path: Path) -> None:
    """The per-source receipt has to say which of the two happened, not just "no heartbeat"."""

    feature_root = tmp_path / "control" / "features" / "feature"
    _running(feature_root, "feature")
    superseded_root = tmp_path / "superseded" / "catalog"
    unreadable_root = tmp_path / "unreadable" / "catalog"
    _stopped_with_identity(superseded_root, "catalog", spec_fingerprint=PREVIOUS_IDENTITY)
    _running(unreadable_root, "catalog")
    RuntimeServiceControl._path_for(unreadable_root, _spec("catalog")).write_text(
        "{not json",
        encoding="utf-8",
    )

    superseded = RuntimeHealthSourceReader(
        sources=(_source(superseded_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
        previous_spec_identities={"catalog": (PREVIOUS_IDENTITY,)},
    )(NOW)
    unreadable = RuntimeHealthSourceReader(
        sources=(_source(unreadable_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
        previous_spec_identities={"catalog": (PREVIOUS_IDENTITY,)},
    )(NOW)

    assert (
        superseded.payload.dashboard_summary_source_receipts["catalog"]
        != unreadable.payload.dashboard_summary_source_receipts["catalog"]
    )


def test_without_a_lineage_a_stopped_previous_heartbeat_is_only_degraded(
    tmp_path: Path,
) -> None:
    """No lineage means no supersede, but the payload still survives it (rule two)."""

    catalog_root = tmp_path / "control" / "artifact-catalogs" / "catalog"
    feature_root = tmp_path / "control" / "features" / "feature"
    _stopped_with_identity(catalog_root, "catalog", spec_fingerprint=PREVIOUS_IDENTITY)
    _running(feature_root, "feature")

    result = RuntimeHealthSourceReader(
        sources=(_source(catalog_root, "catalog"), _source(feature_root, "feature")),
        serving_service_id="serving",
    )(NOW)

    entries = {item.service_id: item for item in result.payload.runtime_services}
    assert entries["catalog"].status is RuntimeServiceStatus.DEGRADED
    assert "unreadable:catalog" in (result.reason or "")
