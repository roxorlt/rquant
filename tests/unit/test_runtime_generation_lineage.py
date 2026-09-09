"""#248: what counts as "our own previous generation", and what stays foreign.

Four durable artifacts refused the 2026-09-09 release because they carried the identity
the *previous* generation of the same service wrote (the strategy runner database, the
route ledger's source row, the candidate authority binding, a stopped heartbeat). Each
of them needs the same question answered, and it has to be answered from evidence that
is already on disk under the runtime root: *is this identity one of ours from an earlier
install, or does it belong to somebody else?*

The evidence is the generation tree the installer writes. Every generation directory is
named by `canonical_sha256` of its own `generation-basis.json`, and that basis carries
the sha256 of every manifest it installed, keyed by service id. So a generation
directory authenticates itself and the manifest we read out of it: a tampered basis no
longer hashes to its directory name, and a tampered manifest no longer matches the sha256
the basis records. Nothing here trusts a filename, an mtime, or an ordering.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rquant.runtime_deployment_bundle import _instance_name
from rquant.runtime_generation_lineage import (
    RuntimeGenerationLineageError,
    load_runtime_generation_tree,
)
from tests.unit.test_runtime_deployment_bundle import (
    COMMIT,
    _bundle_inputs,
    install_runtime_deployment_bundle,
    isolated_root_credential_sealer,  # noqa: F401 -- autouse fixture
)

SECOND_COMMIT = "b" * 40
THIRD_COMMIT = "c" * 40

#: one of the six services `_bundle_inputs` installs, and the one shape (3) is about
CANDIDATE_SERVICE = "candidate-n-shape"


def _install(root: Path, commit: str) -> object:
    manifests, capabilities = _bundle_inputs(root)
    return install_runtime_deployment_bundle(
        root,
        producer_commit=commit,
        manifests=tuple(
            manifest.model_copy(update={"producer_commit": commit}) for manifest in manifests
        ),
        capability_env=capabilities,
    )


@pytest.fixture
def two_generations(tmp_path: Path) -> Path:
    """The world every shape starts from: one generation installed over another."""

    root = tmp_path / "runtime"
    first = _install(root, COMMIT)
    second = _install(root, SECOND_COMMIT)
    assert first.generation_hash != second.generation_hash
    return root


def test_the_previous_generation_of_the_same_service_is_read_out_of_its_own_basis(
    two_generations: Path,
) -> None:
    tree = load_runtime_generation_tree(two_generations)
    lineage = tree.lineage(CANDIDATE_SERVICE)

    assert lineage.current.manifest.producer_commit == SECOND_COMMIT
    assert [record.manifest.producer_commit for record in lineage.previous] == [COMMIT]
    assert lineage.current.generation_id != lineage.previous[0].generation_id
    assert lineage.previous[0].manifest.service_id == CANDIDATE_SERVICE


def test_the_current_generation_is_never_one_of_its_own_previous_generations(
    two_generations: Path,
) -> None:
    tree = load_runtime_generation_tree(two_generations)
    lineage = tree.lineage(CANDIDATE_SERVICE)

    assert lineage.current.generation_id not in {
        record.generation_id for record in lineage.previous
    }


def test_three_installs_leave_two_previous_generations(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    _install(root, COMMIT)
    _install(root, SECOND_COMMIT)
    _install(root, THIRD_COMMIT)

    lineage = load_runtime_generation_tree(root).lineage(CANDIDATE_SERVICE)

    assert lineage.current.manifest.producer_commit == THIRD_COMMIT
    assert {record.manifest.producer_commit for record in lineage.previous} == {
        COMMIT,
        SECOND_COMMIT,
    }


def test_a_generation_whose_basis_was_edited_is_not_ours(two_generations: Path) -> None:
    """A basis that no longer hashes to its own directory name authenticates nothing.

    The edit is written back through the installer's own canonical serializer, so the
    document is still a valid, canonical basis and the *only* thing that rejects it is
    `canonical_sha256(basis) == <directory name>`. A tamper that merely broke the JSON
    would be caught by the parser and would say nothing about the hash binding.
    """

    from rquant.runtime_deployment_bundle import (
        _canonical_model_payload,
        _parse_generation_basis,
    )

    tree = load_runtime_generation_tree(two_generations)
    previous_id = tree.lineage(CANDIDATE_SERVICE).previous[0].generation_id
    basis = two_generations / "generations" / previous_id / "generation-basis.json"
    parsed = _parse_generation_basis(basis.read_bytes())
    #: `schema_contract_sha256` is chosen because nothing downstream cross-checks it
    #: against the manifest we then read, so the hash-to-directory-name binding is the
    #: only thing standing between this document and being believed
    edited = parsed.model_copy(update={"schema_contract_sha256": "9" * 64})
    basis.write_bytes(_canonical_model_payload(edited))
    #: the document itself is still canonical: only its hash no longer names its directory
    assert _parse_generation_basis(basis.read_bytes()).schema_contract_sha256 == "9" * 64

    assert load_runtime_generation_tree(two_generations).lineage(CANDIDATE_SERVICE).previous == ()


def test_a_manifest_whose_bytes_no_longer_match_the_basis_is_not_ours(
    two_generations: Path,
) -> None:
    tree = load_runtime_generation_tree(two_generations)
    previous_id = tree.lineage(CANDIDATE_SERVICE).previous[0].generation_id
    manifest = (
        two_generations
        / "generations"
        / previous_id
        / "manifests"
        / f"{_instance_name(CANDIDATE_SERVICE)}.json"
    )
    payload = json.loads(manifest.read_bytes())
    payload["settings"] = {**payload["settings"], "definition_fingerprint": "d" * 64}
    manifest.write_bytes(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())

    assert load_runtime_generation_tree(two_generations).lineage(CANDIDATE_SERVICE).previous == ()


def test_a_service_the_previous_generation_never_installed_has_no_previous_identity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=tuple(
            manifest
            for manifest in manifests
            if manifest.service_id != CANDIDATE_SERVICE
        ),
        capability_env={
            service_id: value
            for service_id, value in capabilities.items()
            if service_id != CANDIDATE_SERVICE
        },
    )
    _install(root, SECOND_COMMIT)

    lineage = load_runtime_generation_tree(root).lineage(CANDIDATE_SERVICE)

    assert lineage.previous == ()


def test_a_service_the_current_generation_does_not_carry_is_refused(
    two_generations: Path,
) -> None:
    tree = load_runtime_generation_tree(two_generations)

    with pytest.raises(RuntimeGenerationLineageError, match="does not carry"):
        tree.lineage("nobody/knows:this")


def test_an_absent_current_pointer_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    _install(root, COMMIT)
    (root / "current").unlink()

    with pytest.raises(RuntimeGenerationLineageError, match="current"):
        load_runtime_generation_tree(root)


def test_the_instance_directory_name_maps_back_to_its_service_id(
    two_generations: Path,
) -> None:
    """`signal_router` knows a runner database by its instance directory, not by name."""

    tree = load_runtime_generation_tree(two_generations)

    assert tree.service_id_for_instance(_instance_name(CANDIDATE_SERVICE)) == CANDIDATE_SERVICE
    assert tree.service_id_for_instance("svc-" + "0" * 64) is None
