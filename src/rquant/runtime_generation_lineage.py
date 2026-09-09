"""One shared answer to "did *we* write this state, one generation ago?" (#248).

Four durable artifacts fail closed on a release because they carry an identity derived
from the producer commit, and the new generation's own role is the one that finds it:

* `live/strategies/<svc>/runner.sqlite3` — the persisted strategy spec fingerprint;
* `live/signal-bus/signal_bus.sqlite3` — the `signal_route_source` row's generation;
* `live/candidates/<svc>/authority.json` — the definition/executable fingerprints;
* `control/<kind>/<svc>/heartbeats/*.json` — the service spec fingerprint.

Refusing is right for a foreign or corrupted artifact and wrong for the one state every
release produces: the artifact our own previous generation wrote. Telling those two apart
needs evidence, and the only evidence that is both on disk and self-authenticating is the
generation tree `install_runtime_deployment_bundle` leaves under the runtime root:

    <runtime root>/current -> generations/<generation id>
    <runtime root>/generations/<generation id>/generation-basis.json
    <runtime root>/generations/<generation id>/manifests/svc-<sha256(service id)>.json

`<generation id>` *is* `canonical_sha256` of the basis document, and the basis carries the
sha256 of every manifest it installed keyed by service id. So a directory here proves its
own name, and a manifest read out of it proves its own bytes. Editing either breaks the
chain and the generation stops counting as ours — which is the fail-closed half of the
rule, and the half every negative test in this package exercises.

What deliberately is **not** used: the receipt's `previous_generation_hash` (it is a return
value, never written to disk — see `install_runtime_deployment_bundle`), directory mtimes,
and any ordering. "Previous" here means *any installed generation of this runtime root
that is not the current one and that installed this same service id*. That is wider than
"the immediately preceding install" and narrower than "anything on disk", and it is the
right width: every one of those generations is a generation of ours whose state this role
may legitimately find, and none of them is somebody else's.

The private helpers imported from `runtime_deployment_bundle` are the installer's own
readers, used here verbatim so that "verified" means the same thing on both sides of the
generation switch. Nothing in this module writes.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_entrypoint import RuntimeServiceManifest


def _bundle_readers() -> tuple[object, object, object, object, type[Exception]]:
    """The installer's own readers, imported where they are used, not at module import.

    `runtime_deployment_bundle` pulls in `rquant.storage.duckdb` (and therefore duckdb
    itself) transitively, and `test_default_registry_does_not_import_optional_evaluators_
    or_production_storage` holds the line that building the builtin registry does not.
    Every role imports this module through its builder, so the import has to stay inside
    the functions that actually walk the generation tree.
    """

    from rquant.runtime_deployment_bundle import (
        _current_target,
        _instance_name,
        _parse_generation_basis,
        _read_owned_generation_file,
    )
    from rquant.runtime_schema_registry import RuntimeSchemaCompatibilityError

    return (
        _current_target,
        _instance_name,
        _parse_generation_basis,
        _read_owned_generation_file,
        RuntimeSchemaCompatibilityError,
    )

_GENERATION_ID = re.compile(r"^[0-9a-f]{64}$")


class RuntimeGenerationLineageError(ValueError):
    """The runtime root cannot say which generations are ours."""


@dataclass(frozen=True)
class RuntimeGenerationRecord:
    """One installed generation, as far as one service id is concerned."""

    generation_id: str
    manifest: RuntimeServiceManifest

    @property
    def service_id(self) -> str:
        return self.manifest.service_id

    @property
    def spec_identity(self) -> str:
        """The `RuntimeServiceSpec` fingerprint a heartbeat written here would carry."""

        return self.manifest.service_spec.identity

    def setting(self, name: str) -> object | None:
        return self.manifest.settings.get(name)


@dataclass(frozen=True)
class RuntimeGenerationLineage:
    """The current generation of one service, and every earlier one we installed."""

    runtime_root: Path
    service_id: str
    current: RuntimeGenerationRecord
    previous: tuple[RuntimeGenerationRecord, ...]

    def previous_with_settings(self, **expected: object) -> RuntimeGenerationRecord | None:
        """The previous generation whose manifest carries exactly these setting values.

        Every value has to come from the *same* previous manifest: a fingerprint pair
        assembled out of two different generations is not a generation of ours.
        """

        for record in self.previous:
            if all(record.setting(name) == value for name, value in expected.items()):
                return record
        return None

    def previous_with_spec_identity(self, identity: str) -> RuntimeGenerationRecord | None:
        for record in self.previous:
            if record.spec_identity == identity:
                return record
        return None


class RuntimeGenerationTree:
    """Every generation installed under one runtime root, verified once."""

    def __init__(
        self,
        runtime_root: Path,
        *,
        current_generation_id: str,
        generation_ids: tuple[str, ...],
    ) -> None:
        self.runtime_root = runtime_root
        self.current_generation_id = current_generation_id
        self.generation_ids = generation_ids

    def service_id_for_instance(self, instance: str) -> str | None:
        """The service id whose instance directory carries this name, or `None`.

        `signal_router` reads a runner database out of `live/strategies/<instance>/` and
        never learns whose it is; the current generation's basis is what turns that
        directory name back into a service id.
        """

        basis = _verified_basis(self.runtime_root, self.current_generation_id)
        if basis is None:
            return None
        for service_id, mapped in basis.instance_mapping.items():
            if mapped == instance:
                return service_id
        return None

    def lineage(self, service_id: str) -> RuntimeGenerationLineage:
        current = _verified_record(self.runtime_root, self.current_generation_id, service_id)
        if current is None:
            raise RuntimeGenerationLineageError(
                f"the current runtime generation does not carry {service_id!r}"
            )
        previous = tuple(
            record
            for record in (
                _verified_record(self.runtime_root, generation_id, service_id)
                for generation_id in self.generation_ids
                if generation_id != self.current_generation_id
            )
            if record is not None
        )
        return RuntimeGenerationLineage(
            runtime_root=self.runtime_root,
            service_id=service_id,
            current=current,
            previous=previous,
        )


def load_runtime_generation_tree(runtime_root: Path) -> RuntimeGenerationTree:
    """Resolve `current` and enumerate the generation directories beside it."""

    current_target, _, _, _, _ = _bundle_readers()
    root = Path(os.path.abspath(Path(runtime_root)))
    try:
        target = current_target(root)
    except (OSError, ValueError) as exc:
        raise RuntimeGenerationLineageError("runtime current generation is unusable") from exc
    if target is None:
        raise RuntimeGenerationLineageError("runtime current generation is missing")
    current_generation_id = Path(target).name
    generations = root / "generations"
    try:
        names = tuple(
            sorted(
                entry.name
                for entry in generations.iterdir()
                if _GENERATION_ID.fullmatch(entry.name)
            )
        )
    except OSError as exc:
        raise RuntimeGenerationLineageError("runtime generations are unreadable") from exc
    if current_generation_id not in names:
        raise RuntimeGenerationLineageError("runtime current generation is not installed")
    return RuntimeGenerationTree(
        root,
        current_generation_id=current_generation_id,
        generation_ids=names,
    )


def load_runtime_generation_lineage(
    runtime_root: Path,
    *,
    service_id: str,
) -> RuntimeGenerationLineage:
    return load_runtime_generation_tree(runtime_root).lineage(service_id)


def _lineage_or_none(
    runtime_root: Path | None,
    *,
    service_id: str,
) -> RuntimeGenerationLineage | None:
    """The lineage, or `None` when this deployment cannot say what it installed.

    Route B publishes no legacy bundle at all, and a role can be exercised with no
    runtime root. Both answer "no previous generation of ours is known", which leaves
    every one of the four checks exactly as strict as it was before #248.
    """

    if runtime_root is None:
        return None
    try:
        return load_runtime_generation_lineage(runtime_root, service_id=service_id)
    except RuntimeGenerationLineageError:
        return None


def strategy_runner_identity_lineage(
    runtime_root: Path | None,
    *,
    service_id: str,
) -> Callable[[str, str], str | None] | None:
    """Shape (1): `(spec fingerprint, evaluator fingerprint) -> our generation that used it`."""

    lineage = _lineage_or_none(runtime_root, service_id=service_id)
    if lineage is None:
        return None

    def resolve(spec_fingerprint: str, evaluator_fingerprint: str) -> str | None:
        record = lineage.previous_with_settings(
            strategy_spec_fingerprint=spec_fingerprint,
            evaluator_contract_fingerprint=evaluator_fingerprint,
        )
        return None if record is None else record.generation_id


    return resolve


def strategy_runner_identity_lineage_for_instance(
    runtime_root: Path | None,
    *,
    instance: str,
) -> Callable[[str, str], str | None] | None:
    """The same, for a reader that knows a strategy only by its instance directory.

    `signal_router` is handed `<root>/live/strategies/<instance>/runner.sqlite3` and never
    the strategy's service id; the current generation's basis is what maps one to the other.
    """

    if runtime_root is None:
        return None
    try:
        tree = load_runtime_generation_tree(runtime_root)
    except RuntimeGenerationLineageError:
        return None
    service_id = tree.service_id_for_instance(instance)
    if service_id is None:
        return None
    return strategy_runner_identity_lineage(runtime_root, service_id=service_id)


def previous_strategy_spec_generations(
    runtime_root: Path | None,
    *,
    service_ids: Sequence[str],
) -> dict[str, str]:
    """Shape (2): `strategy spec fingerprint -> our generation that published it`."""

    generations: dict[str, str] = {}
    for service_id in service_ids:
        lineage = _lineage_or_none(runtime_root, service_id=service_id)
        if lineage is None:
            continue
        for record in lineage.previous:
            fingerprint = record.setting("strategy_spec_fingerprint")
            if isinstance(fingerprint, str):
                generations.setdefault(fingerprint, record.generation_id)
    return generations


def candidate_authority_lineage(
    runtime_root: Path | None,
    *,
    service_id: str,
) -> Callable[[object], str | None] | None:
    """Shape (3): an `authority.json` on disk -> our generation that created it."""

    lineage = _lineage_or_none(runtime_root, service_id=service_id)
    if lineage is None:
        return None

    def resolve(binding: object) -> str | None:
        record = lineage.previous_with_settings(
            definition_fingerprint=getattr(binding, "definition_fingerprint", None),
            executable_fingerprint=getattr(binding, "executable_fingerprint", None),
        )
        return None if record is None else record.generation_id

    return resolve


def producer_commit_lineage(
    runtime_root: Path | None,
    *,
    service_id: str,
) -> Callable[[str], str | None] | None:
    """Shape (5): a `producer_commit` found on disk -> our generation that stamped it.

    Every role in one bundle carries the same `producer_commit`, so the lineage of any
    service we install answers "was this commit ours, one release ago?". `serving.
    publisher.v1` asks it about the `current.json` of each source authority it reads: the
    pointer is written by that authority's owner, and after a release the owner has not
    republished yet, so the pointer still carries the previous generation's commit and the
    reader refused it every iteration (#253). A commit no generation of ours ever ran
    resolves to `None` here and is still refused.
    """

    lineage = _lineage_or_none(runtime_root, service_id=service_id)
    if lineage is None:
        return None

    def resolve(producer_commit: str) -> str | None:
        for record in lineage.previous:
            if record.manifest.producer_commit == producer_commit:
                return record.generation_id
        return None

    return resolve


def previous_spec_identities(
    runtime_root: Path | None,
    *,
    service_ids: Sequence[str],
) -> dict[str, tuple[str, ...]]:
    """Shape (4): `service id -> the spec fingerprints our earlier generations ran under`."""

    identities: dict[str, tuple[str, ...]] = {}
    for service_id in service_ids:
        lineage = _lineage_or_none(runtime_root, service_id=service_id)
        if lineage is None:
            continue
        found = tuple(
            dict.fromkeys(record.spec_identity for record in lineage.previous)
        )
        if found:
            identities[service_id] = found
    return identities


def _verified_basis(runtime_root: Path, generation_id: str) -> object | None:
    """The generation's own basis, or `None` when it does not authenticate itself."""

    _, _, parse_generation_basis, read_owned_generation_file, schema_error = _bundle_readers()
    if _GENERATION_ID.fullmatch(generation_id) is None:
        return None
    generation = runtime_root / "generations" / generation_id
    try:
        observed = generation.lstat()
    except OSError:
        return None
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        return None
    try:
        basis = parse_generation_basis(
            read_owned_generation_file(
                generation / "generation-basis.json",
                label="runtime generation hash-bound basis",
            )
        )
    except (OSError, ValueError, schema_error):
        return None
    if canonical_sha256(basis.model_dump(mode="python")) != generation_id:
        return None
    return basis


def _verified_record(
    runtime_root: Path,
    generation_id: str,
    service_id: str,
) -> RuntimeGenerationRecord | None:
    _, instance_name, _, read_owned_generation_file, schema_error = _bundle_readers()
    basis = _verified_basis(runtime_root, generation_id)
    if basis is None:
        return None
    expected_sha256 = basis.manifest_sha256.get(service_id)
    instance = basis.instance_mapping.get(service_id)
    if expected_sha256 is None or instance is None or instance != instance_name(service_id):
        return None
    manifest_path = (
        runtime_root / "generations" / generation_id / "manifests" / f"{instance}.json"
    )
    try:
        payload = read_owned_generation_file(manifest_path, label=f"runtime manifest {service_id}")
    except (OSError, ValueError, schema_error):
        return None
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        return None
    try:
        manifest = RuntimeServiceManifest.model_validate_json(payload)
    except ValueError:
        return None
    if manifest.service_id != service_id or manifest.producer_commit != basis.producer_commit:
        return None
    return RuntimeGenerationRecord(generation_id=generation_id, manifest=manifest)


__all__ = [
    "RuntimeGenerationLineage",
    "candidate_authority_lineage",
    "previous_spec_identities",
    "producer_commit_lineage",
    "previous_strategy_spec_generations",
    "strategy_runner_identity_lineage",
    "strategy_runner_identity_lineage_for_instance",
    "RuntimeGenerationLineageError",
    "RuntimeGenerationRecord",
    "RuntimeGenerationTree",
    "load_runtime_generation_lineage",
    "load_runtime_generation_tree",
]
