"""`RESET-REG-P1-02`: the fixed Phase C harness, its request contract, and its surfaces.

The harness is the only half of the Phase C verification that is allowed to touch
generation code, so these tests pin the three properties that make that safe: it imports
nothing at module scope, it cannot be talked into altering the policy-hashed vector tuple,
and it reaches every surface it claims through that surface's real production builder.

The blocked surfaces are pinned too. A harness that quietly answered a surface it cannot
actually exercise inside the child would be worse than one that refuses, so
`BLOCKED_SURFACE_REASONS` is treated as part of the contract: it must cover exactly the
reader surfaces no exercise implements, and asking for one must reject the run.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import stat
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from rquant import signal_family_verifier_harness as harness
from rquant.signal_family_successor_registry import ACCEPTED_FAMILY_IDS, PAIR_IDS
from rquant.signal_family_verification import (
    MAX_CANONICAL_RESULT_BYTES,
    PRODUCER_SURFACES,
    READER_SURFACES,
    SurfaceId,
)
from rquant.signal_family_verifier_harness import __main__ as harness_main
from rquant.signal_family_verifier_harness import _canonical, _request, _resolve, _surfaces
from rquant.strict_json import canonical_json_bytes as generation_canonical_json_bytes
from tests.support import signal_family_private_root as _private_root
from tests.support.signal_family_harness_vectors import (
    GENERATION_FIXTURE_PREFIX,
    PRODUCER_FIXTURE_ROOT,
    authorized_fixtures,
    generation_fixture_declarations,
    harness_vectors,
    install_generation_fixtures,
    shadow_fixture_supported,
)

#: The three `strategy-shadow` readers are reachable only where the production reader path
#: is: `shadow_session_builder` enters them through `FilesystemShadowSessionInputLoader`
#: under `LegacyShadowFilesystemPolicy(mode="linux-production")`, and
#: `legacy_shadow_export._validate_mount_policy` refuses that mode off Linux. The harness
#: implements all thirteen; these cases are the ones that need the fixture to exist.
SHADOW_SURFACE_IDS = frozenset(
    surface.value for surface in READER_SURFACES["strategy-shadow"]
)
linux_only = pytest.mark.skipif(
    not shadow_fixture_supported(),
    reason=(
        "the strategy-shadow production reader path runs under "
        "LegacyShadowFilesystemPolicy(mode='linux-production'), which "
        "legacy_shadow_export._validate_mount_policy refuses off Linux"
    ),
)


def _surface_params() -> list[Any]:
    return [
        pytest.param(
            surface_id,
            marks=[linux_only] if surface_id in SHADOW_SURFACE_IDS else [],
            id=surface_id,
        )
        for surface_id in harness.IMPLEMENTED_SURFACE_IDS
    ]

# The Phase C ancestry walks refuse a group- or world-writable ancestor, and pytest's own
# `tmp_path` is rooted at `TMPDIR`, which defaults to a sticky `/tmp` on Linux. Rebinding both
# fixture names here roots every temporary directory in a verified-private `$HOME` root for
# this module only, and fails loudly with the offending directory if that root is not private.
signal_family_private_root = _private_root.signal_family_private_root
tmp_path = _private_root.tmp_path
REPOSITORY_ROOT = Path(__file__).resolve().parent.parent.parent
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build-signal-family-verifier-harness.py"

_MATERIALIZING_STATE_KEYS = (
    "directories",
    "files",
    "generation_files",
    "serving_authorities",
    "spool",
    "sqlite_sources",
)
RUN_ID = "a" * 64
TEST_MANIFEST_HASH = "c" * 64
POLICY_DIGEST_PATTERN = re.compile(r"policy_digest[= ][0-9a-f]{8}")


def _load_build_script() -> Any:
    import importlib.util

    spec = importlib.util.spec_from_file_location("_wp4c_build_script", BUILD_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _request_vector(vector: Any) -> harness.RequestVector:
    return harness.RequestVector(
        vector_id=vector.vector_id,
        pair_id=vector.pair_id,
        family_id=vector.family_id,
        surface_id=vector.surface_id.value,
        input_json=vector.input_json,
    )


GENERATION_ROOT = "/srv/rquant/generations/harness-request-fixture"


def _request_payload(
    vectors: tuple[Any, ...],
    *,
    generation_root: str = GENERATION_ROOT,
    generation_files: tuple[dict[str, Any], ...] | None = None,
) -> bytes:
    declared = (
        [entry.model_dump(mode="json") for entry in generation_fixture_declarations()]
        if generation_files is None
        else list(generation_files)
    )
    return _canonical.canonical_json_bytes(
        {
            "generation_files": declared,
            "generation_root": generation_root,
            "run_id": RUN_ID,
            "schema_version": 1,
            "test_manifest_hash": TEST_MANIFEST_HASH,
            "vectors": [
                {
                    "family_id": vector.family_id,
                    "input_json": vector.input_json,
                    "pair_id": vector.pair_id,
                    "surface_id": vector.surface_id.value,
                    "vector_id": vector.vector_id,
                }
                for vector in vectors
            ],
        }
    )


# ---------------------------------------------------------------------------------------
# The byte contract and the frozen identity sets
# ---------------------------------------------------------------------------------------


class TestCanonicalBytes:
    @pytest.mark.parametrize(
        "value",
        [
            {},
            {"b": 1, "a": 2},
            {"nested": {"z": [1, 2, {"y": None}], "a": True}},
            ["ascii", "中文", "emoji \U0001f600"],
            {"float": 1.5, "int": 7, "negative": -3},
        ],
    )
    def test_the_harness_encoder_matches_the_generation_encoder(self, value: Any) -> None:
        assert _canonical.canonical_json_bytes(value) == generation_canonical_json_bytes(value)

    def test_canonical_loads_rejects_duplicate_keys(self) -> None:
        with pytest.raises(_canonical.CanonicalJsonError):
            _canonical.strict_canonical_loads(b'{"a":1,"a":2}')

    @pytest.mark.parametrize(
        "payload",
        [b' {"a":1}', b'{"a":1} ', b'{"b":1,"a":2}', b'{"a": 1}', b'{"a":1}\n'],
    )
    def test_canonical_loads_rejects_noncanonical_bytes(self, payload: bytes) -> None:
        with pytest.raises(_canonical.CanonicalJsonError):
            _canonical.strict_canonical_loads(payload)

    def test_canonical_sha256_hashes_the_canonical_bytes(self) -> None:
        value = {"b": 2, "a": 1}
        expected = hashlib.sha256(generation_canonical_json_bytes(value)).hexdigest()
        assert _canonical.canonical_sha256(value) == expected


class TestFrozenIdentitySets:
    """The harness restates these sets rather than importing them; they must not drift."""

    def test_pair_ids_match_the_registry(self) -> None:
        assert _request.PAIR_IDS == PAIR_IDS

    def test_accepted_family_ids_match_the_registry(self) -> None:
        assert _request.ACCEPTED_FAMILY_IDS == ACCEPTED_FAMILY_IDS

    def test_reader_surfaces_match_the_verification_model(self) -> None:
        expected = {
            pair_id: tuple(surface.value for surface in surfaces)
            for pair_id, surfaces in READER_SURFACES.items()
        }
        assert expected == _request.READER_SURFACES

    def test_producer_surfaces_match_the_verification_model(self) -> None:
        expected = {
            pair_id: tuple(surface.value for surface in surfaces)
            for pair_id, surfaces in PRODUCER_SURFACES.items()
        }
        assert expected == _request.PRODUCER_SURFACES


class TestSurfaceCoverageBookkeeping:
    @staticmethod
    def _all_reader_surfaces() -> set[str]:
        return {
            surface.value for surfaces in READER_SURFACES.values() for surface in surfaces
        }

    def test_implemented_and_blocked_partition_every_reader_surface(self) -> None:
        implemented = set(harness.IMPLEMENTED_SURFACE_IDS)
        blocked = set(harness.BLOCKED_SURFACE_REASONS)
        assert implemented & blocked == set()
        assert implemented | blocked == self._all_reader_surfaces()

    def test_every_blocked_surface_states_a_reason(self) -> None:
        for surface_id, reason in harness.BLOCKED_SURFACE_REASONS.items():
            assert surface_id in self._all_reader_surfaces()
            assert len(reason) > 40

    def test_every_reader_surface_is_implemented(self) -> None:
        """P1-6: thirteen of thirteen, with nothing standing in for a missing exercise."""

        assert set(harness.IMPLEMENTED_SURFACE_IDS) == self._all_reader_surfaces()
        assert len(harness.IMPLEMENTED_SURFACE_IDS) == 13
        assert harness.BLOCKED_SURFACE_REASONS == {}

    def test_no_producer_surface_has_an_exercise(self) -> None:
        producers = {
            surface.value for surfaces in PRODUCER_SURFACES.values() for surface in surfaces
        }
        assert producers & set(harness.IMPLEMENTED_SURFACE_IDS) == set()

    def test_every_exercised_surface_names_its_production_builder(self) -> None:
        for surface_id in harness.IMPLEMENTED_SURFACE_IDS:
            spec = _surfaces.SURFACE_SPECS[surface_id]
            assert spec.builder.startswith("rquant.runtime_builder_")
            assert callable(spec.exercise)


# ---------------------------------------------------------------------------------------
# Import discipline
# ---------------------------------------------------------------------------------------


class TestImportDiscipline:
    @staticmethod
    def _module_level_imports(path: Path) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in tree.body:
            if isinstance(node, ast.Import):
                names.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
        return names

    def test_no_harness_module_imports_generation_code_at_module_scope(self) -> None:
        package = Path(harness.__file__).resolve().parent
        offenders: dict[str, set[str]] = {}
        for path in sorted(package.glob("*.py")):
            if path.name == "__init__.py":
                continue
            outside = {
                name
                for name in self._module_level_imports(path)
                if name not in sys.stdlib_module_names
            }
            if outside:
                offenders[path.name] = outside
        assert offenders == {}

    def test_the_package_facade_only_re_exports_its_own_modules(self) -> None:
        path = Path(harness.__file__).resolve()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                if node.module == "__future__":
                    continue
                assert node.level == 1, "the facade must use relative imports"
            elif isinstance(node, ast.Import):  # pragma: no cover - defensive
                pytest.fail("the facade must not use absolute imports")


# ---------------------------------------------------------------------------------------
# The request contract
# ---------------------------------------------------------------------------------------


class TestRequestParsing:
    def test_a_well_formed_request_round_trips_every_vector(self) -> None:
        vectors = harness_vectors()
        parsed = _request.parse_child_request(_request_payload(vectors))

        assert parsed.run_id == RUN_ID
        assert parsed.test_manifest_hash == TEST_MANIFEST_HASH
        assert tuple(vector.vector_id for vector in parsed.vectors) == tuple(
            vector.vector_id for vector in vectors
        )

    def test_the_request_carries_no_expected_result_field(self) -> None:
        payload = _request_payload(harness_vectors())
        decoded = _canonical.strict_canonical_loads(payload)

        assert "expected_results" not in decoded
        assert "expected_result_set_hash" not in decoded
        for vector in decoded["vectors"]:
            assert "canonical_result_sha256" not in vector
            assert "expected" not in "".join(vector)

    def test_a_tampered_vector_input_breaks_its_own_identity(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        decoded["vectors"][0]["input_json"] = decoded["vectors"][1]["input_json"]

        with pytest.raises(_request.ChildRequestError, match="canonical content"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_reordered_vectors_reject(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        decoded["vectors"] = list(reversed(decoded["vectors"]))

        with pytest.raises(_request.ChildRequestError, match="sorted"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_duplicated_vectors_reject(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        decoded["vectors"] = [decoded["vectors"][0], decoded["vectors"][0]]

        with pytest.raises(_request.ChildRequestError, match="duplicate-free"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_an_extra_vector_field_rejects(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        decoded["vectors"][0]["canonical_result_sha256"] = "0" * 64

        with pytest.raises(_request.ChildRequestError, match="exact frozen set"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_an_extra_request_field_rejects(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        decoded["expected_result_set_hash"] = "0" * 64

        with pytest.raises(_request.ChildRequestError, match="exact frozen set"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_a_producer_surface_vector_rejects(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        vector = dict(decoded["vectors"][0])
        vector["surface_id"] = PRODUCER_SURFACES[vector["pair_id"]][0].value
        vector["vector_id"] = _canonical.canonical_sha256(
            {key: vector[key] for key in ("family_id", "input_json", "pair_id", "surface_id")}
        )
        decoded["vectors"] = [vector]

        with pytest.raises(_request.ChildRequestError, match="producer surface"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_a_surface_outside_the_pair_rejects(self) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        vector = dict(decoded["vectors"][0])
        vector["surface_id"] = SurfaceId.ROUTE_RUNNER_SIGNALS.value
        vector["vector_id"] = _canonical.canonical_sha256(
            {key: vector[key] for key in ("family_id", "input_json", "pair_id", "surface_id")}
        )
        decoded["vectors"] = [vector]

        with pytest.raises(_request.ChildRequestError, match="reader surface of that pair"):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    @pytest.mark.parametrize(
        ("mutation", "message"),
        [
            ({"schema_version": 2}, "schema version"),
            ({"run_id": "nope"}, "run_id"),
            ({"test_manifest_hash": "A" * 64}, "test_manifest_hash"),
            ({"vectors": []}, "nonempty vector array"),
        ],
    )
    def test_malformed_requests_reject(self, mutation: dict[str, Any], message: str) -> None:
        decoded = _canonical.strict_canonical_loads(_request_payload(harness_vectors()))
        decoded.update(mutation)

        with pytest.raises(_request.ChildRequestError, match=message):
            _request.parse_child_request(_canonical.canonical_json_bytes(decoded))

    def test_noncanonical_request_bytes_reject(self) -> None:
        payload = _request_payload(harness_vectors())

        with pytest.raises(_request.ChildRequestError, match="canonical JSON"):
            _request.parse_child_request(payload + b"\n")

    def test_an_oversized_request_rejects(self) -> None:
        with pytest.raises(_request.ChildRequestError, match="bounded size"):
            _request.parse_child_request(b"x" * (_request.MAX_REQUEST_BYTES + 1))


class TestResponseBuilding:
    def _request(self) -> _request.ChildRequest:
        return _request.parse_child_request(_request_payload(harness_vectors()))

    def test_the_response_is_sorted_and_self_hashing(self) -> None:
        request = self._request()
        results = {vector.vector_id: {"seen": vector.surface_id} for vector in request.vectors}

        payload = _request.build_child_response(request, results)
        decoded = _canonical.strict_canonical_loads(payload)
        keys = [
            (row["pair_id"], row["family_id"], row["surface_id"], row["vector_id"])
            for row in decoded["vector_results"]
        ]

        assert keys == sorted(set(keys))
        without_hash = {key: value for key, value in decoded.items() if key != "result_hash"}
        assert decoded["result_hash"] == _canonical.canonical_sha256(without_hash)
        assert len(payload) <= _request.MAX_RESPONSE_BYTES

    def test_every_result_hashes_its_own_bytes(self) -> None:
        request = self._request()
        results = {vector.vector_id: {"seen": vector.surface_id} for vector in request.vectors}

        decoded = _canonical.strict_canonical_loads(
            _request.build_child_response(request, results)
        )

        for row in decoded["vector_results"]:
            raw = row["canonical_result_json"].encode("utf-8")
            assert row["canonical_result_sha256"] == hashlib.sha256(raw).hexdigest()
            assert len(raw) <= MAX_CANONICAL_RESULT_BYTES

    def test_a_missing_result_rejects(self) -> None:
        request = self._request()
        results = {vector.vector_id: {} for vector in request.vectors[:-1]}

        with pytest.raises(_request.ChildRequestError, match="did not ask for"):
            _request.build_child_response(request, results)

    def test_an_unrequested_result_rejects(self) -> None:
        request = self._request()
        results: dict[str, Any] = {vector.vector_id: {} for vector in request.vectors}
        results["f" * 64] = {}

        with pytest.raises(_request.ChildRequestError, match="did not ask for"):
            _request.build_child_response(request, results)

    def test_an_oversized_result_rejects(self) -> None:
        request = self._request()
        results: dict[str, Any] = {vector.vector_id: {} for vector in request.vectors}
        results[request.vectors[0].vector_id] = {"padding": "x" * MAX_CANONICAL_RESULT_BYTES}

        with pytest.raises(_request.ChildRequestError, match="bounded size"):
            _request.build_child_response(request, results)


# ---------------------------------------------------------------------------------------
# Ruling O2: surface resolution
# ---------------------------------------------------------------------------------------


class TestSurfaceResolution:
    @pytest.mark.parametrize("surface", list(SurfaceId))
    def test_every_frozen_surface_resolves_to_its_own_qualname(self, surface: SurfaceId) -> None:
        resolved = _resolve.resolve_surface(surface.value)

        assert callable(resolved)
        assert f"{resolved.__module__}.{resolved.__qualname__}" == surface.value

    def test_an_alias_under_another_name_rejects(self) -> None:
        with pytest.raises(_resolve.SurfaceResolutionError, match="aliases and wrappers"):
            _resolve.resolve_surface("rquant.signal_family_verifier_harness.canonical_sha256")

    def test_a_noncallable_attribute_rejects(self) -> None:
        with pytest.raises(_resolve.SurfaceResolutionError, match="callable"):
            _resolve.resolve_surface("rquant.signal_family_verification.PAIR_SURFACES")

    def test_a_missing_attribute_chain_rejects(self) -> None:
        with pytest.raises(_resolve.SurfaceResolutionError, match="attribute chain"):
            _resolve.resolve_surface("rquant.signal_route_spool.NoSuchReader.read")

    def test_a_surface_outside_rquant_rejects(self) -> None:
        with pytest.raises(_resolve.SurfaceResolutionError, match="rquant callable"):
            _resolve.resolve_surface("json.loads")

    def test_binding_proves_the_instance_runs_that_exact_function(self) -> None:
        from rquant.paper_signal_worker import PaperSignalQueueStore

        surface_id = SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST.value
        instance = object.__new__(PaperSignalQueueStore)
        bound = _resolve.bound_surface(instance, surface_id)

        assert bound.__func__ is _resolve.resolve_surface(surface_id)

    def test_binding_rejects_an_object_that_is_not_that_reader(self) -> None:
        surface_id = SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST.value

        with pytest.raises(_resolve.SurfaceResolutionError, match="not bound to"):
            _resolve.bound_surface(object(), surface_id)

    def test_a_same_named_lambda_on_the_instance_rejects(self) -> None:
        """Reviewer mutation M5, closed: attribute presence is not identity.

        Deleting the `__func__ is resolve_surface(id)` half of `bound_surface` left the whole
        suite green, because the only negative case passed a bare `object()` and was caught
        by the presence half. This is the case that needed the identity half.
        """

        from rquant.paper_signal_worker import PaperSignalQueueStore

        surface_id = SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST.value
        instance = object.__new__(PaperSignalQueueStore)
        instance.ingest = lambda *args, **kwargs: {"forged": True}  # type: ignore[method-assign]

        with pytest.raises(_resolve.SurfaceResolutionError, match="not bound to"):
            _resolve.bound_surface(instance, surface_id)

    def test_a_subclass_override_rejects(self) -> None:
        """The other shape of the same substitution: same qualname, different function."""

        from rquant.paper_signal_worker import PaperSignalQueueStore

        surface_id = SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST.value

        class Substituted(PaperSignalQueueStore):
            def ingest(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
                return {"forged": True}

        with pytest.raises(_resolve.SurfaceResolutionError, match="not bound to"):
            _resolve.bound_surface(object.__new__(Substituted), surface_id)

    def test_the_genuine_binding_is_the_resolved_function_itself(self) -> None:
        from rquant.paper_signal_worker import PaperSignalQueueStore

        surface_id = SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST.value
        bound = _resolve.bound_surface(object.__new__(PaperSignalQueueStore), surface_id)

        assert bound.__func__ is _resolve.resolve_surface(surface_id)

    def test_binding_a_dunder_surface_uses_the_type_slot(self) -> None:
        from rquant.runtime_serving_authority import ServingSourceAuthorityReader

        surface_id = SurfaceId.SERVING_SOURCE_AUTHORITY_READER_CALL.value
        instance = object.__new__(ServingSourceAuthorityReader)

        assert callable(_resolve.bound_surface(instance, surface_id))
        with pytest.raises(_resolve.SurfaceResolutionError, match="does not implement"):
            _resolve.bound_surface(object(), surface_id)


# ---------------------------------------------------------------------------------------
# The workspace
# ---------------------------------------------------------------------------------------


class TestWorkspace:
    def _workspace(self, tmp_path: Path) -> harness.VectorWorkspace:
        return harness.VectorWorkspace(tmp_path, "0" * 64)

    @pytest.mark.parametrize(
        "declared",
        ["/etc/passwd", "@workspace/../escape", "@workspace/./here", "relative/path"],
    )
    def test_unsafe_declared_paths_reject(self, tmp_path: Path, declared: str) -> None:
        workspace = self._workspace(tmp_path)

        with pytest.raises(harness.WorkspaceError):
            workspace.materialize({"directories": [declared], "files": []})

    def test_declared_files_land_inside_the_state_tree(self, tmp_path: Path) -> None:
        workspace = self._workspace(tmp_path)
        workspace.materialize(
            {"directories": ["@workspace/nested"], "files": [
                {"content": "payload", "path": "@workspace/nested/file.txt"},
            ]}
        )

        target = workspace.state / "nested" / "file.txt"
        assert target.read_text(encoding="utf-8") == "payload"
        assert workspace.state in target.parents

    def test_unknown_state_keys_reject(self, tmp_path: Path) -> None:
        workspace = self._workspace(tmp_path)

        with pytest.raises(harness.WorkspaceError, match="unknown keys"):
            workspace.materialize({"surprise": []})

    def test_the_tree_digest_notices_a_single_changed_byte(self, tmp_path: Path) -> None:
        workspace = self._workspace(tmp_path)
        workspace.materialize(
            {"directories": [], "files": [{"content": "a", "path": "@workspace/f.txt"}]}
        )
        before = harness.tree_digest(workspace.state)
        target = workspace.state / "f.txt"
        target.chmod(0o600)
        target.write_text("b", encoding="utf-8")

        assert harness.tree_digest(workspace.state) != before

    def test_sqlite_scratch_files_do_not_move_the_digest(self, tmp_path: Path) -> None:
        """A store the builder opened creates and unlinks these on its own schedule.

        Digesting them would let the read-only verdict — and therefore the canonical result
        the policy hashed — depend on when SQLite happened to close a connection.
        """

        workspace = self._workspace(tmp_path)
        workspace.materialize(
            {"directories": [], "files": [{"content": "a", "path": "@workspace/db.sqlite3"}]}
        )
        before = harness.tree_digest(workspace.state)
        for suffix in harness.VOLATILE_SUFFIXES:
            (workspace.state / f"db.sqlite3{suffix}").write_bytes(b"scratch")

        assert harness.tree_digest(workspace.state) == before

    def test_a_file_that_vanishes_mid_walk_does_not_raise(self, tmp_path: Path) -> None:
        """The other half of the same race: an entry `os.walk` listed is already gone."""

        workspace = self._workspace(tmp_path)
        workspace.materialize(
            {"directories": [], "files": [{"content": "a", "path": "@workspace/kept.txt"}]}
        )
        doomed = workspace.state / "doomed.txt"
        doomed.write_bytes(b"gone")
        real_read_bytes = Path.read_bytes

        def vanishing(self: Path) -> bytes:
            if self.name == "doomed.txt":
                self.unlink()
                raise FileNotFoundError(2, "No such file or directory", str(self))
            return real_read_bytes(self)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(Path, "read_bytes", vanishing)
            digest = harness.tree_digest(workspace.state)

        assert digest == harness.tree_digest(workspace.state)

    def test_a_writer_runs_against_a_copy_of_the_declaration(self, tmp_path: Path) -> None:
        workspace = self._workspace(tmp_path)
        workspace.materialize(
            {"directories": [], "files": [{"content": "a", "path": "@workspace/f.txt"}]}
        )

        live = workspace.live_root(writes=True)

        assert live == workspace.scratch
        assert harness.tree_digest(live) == harness.tree_digest(workspace.state)
        assert workspace.live_root(writes=False) == workspace.state


class TestServingAuthorityAncestryRule:
    """Why the child cwd had to move: the rule that made three surfaces unreachable."""

    @staticmethod
    def _publish(root: Path) -> None:
        from rquant.runtime_contracts import canonical_sha256
        from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
        from rquant.runtime_serving_snapshot import SignalDeliveryPayload, SourceReadResult
        from rquant.serving_contracts import FreshnessStatus

        published = datetime(2026, 8, 24, 7, 29, tzinfo=UTC)
        values: dict[str, Any] = {
            "dataset_id": "signals",
            "sequence": 1,
            "event_time": published,
            "published_at": published,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": SignalDeliveryPayload(),
        }
        values["generation_id"] = canonical_sha256(values)
        ServingSourceAuthorityPublisher(
            root=root,
            producer_commit="a" * 40,
            dataset_id="signals",
            payload_kind="signal_delivery",
            clock=lambda: published,
        ).publish(SourceReadResult.model_validate(values))

    def test_a_group_or_world_writable_ancestor_is_refused(self, tmp_path: Path) -> None:
        from rquant.runtime_serving_authority import ServingSourceAuthorityIntegrityError

        sticky = tmp_path / "sticky"
        sticky.mkdir(mode=0o777)
        sticky.chmod(0o1777)

        with pytest.raises(ServingSourceAuthorityIntegrityError, match="unsafe"):
            self._publish(sticky / "authority")

    def test_a_private_ancestor_chain_is_accepted(self, tmp_path: Path) -> None:
        private = tmp_path / "private"
        private.mkdir(mode=0o700)

        self._publish(private / "authority")

        assert (private / "authority").is_dir()


# ---------------------------------------------------------------------------------------
# The exercises themselves
# ---------------------------------------------------------------------------------------


_FIXTURE_CACHE: dict[Path, Any] = {}


def _session_root(root: Path) -> Path:
    """The private session root `tmp_path` was carved out of."""

    candidate = root.resolve()
    home = Path.home().resolve()
    while candidate.parent != home and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def _installed_fixtures(root: Path) -> Any:
    """One generation tree per session, shared by every exercise in this module.

    The `strategy-shadow` half is published in place by the production export publishers,
    which is neither cheap nor repeatable inside one directory, so it is built once.
    """

    session = _session_root(root)
    cached = _FIXTURE_CACHE.get(session)
    if cached is None:
        generation = session / "harness-generation"
        generation.mkdir(mode=0o700, exist_ok=True)
        cached = install_generation_fixtures(generation)
        _FIXTURE_CACHE[session] = cached
    return cached


def _generation_root(root: Path) -> Path:
    return _installed_fixtures(root).generation_root


def _harness_vectors(root: Path) -> tuple[Any, ...]:
    from tests.support.signal_family_harness_vectors import harness_vectors

    return harness_vectors(_installed_fixtures(root).shadow_descriptor)


def _workspace_reading_vectors(root: Path) -> tuple[Any, ...]:
    """The vectors that materialize something into the workspace before their surface runs."""

    selected = []
    for vector in _harness_vectors(root):
        state = json.loads(vector.input_json)["state"]
        if any(state.get(key) for key in _MATERIALIZING_STATE_KEYS):
            selected.append(vector)
    return tuple(selected)


def _reachable_surface_ids() -> tuple[str, ...]:
    """The surfaces whose production path can be entered on this host."""

    if shadow_fixture_supported():
        return harness.IMPLEMENTED_SURFACE_IDS
    return tuple(
        surface_id
        for surface_id in harness.IMPLEMENTED_SURFACE_IDS
        if surface_id not in SHADOW_SURFACE_IDS
    )


def _vector_for(surface_id: str, root: Path) -> Any:
    return next(
        item for item in _harness_vectors(root) if item.surface_id.value == surface_id
    )


def _exercise(vector: Any, root: Path) -> dict[str, Any]:
    installed = _installed_fixtures(root)
    return _surfaces.exercise_vector(
        _request_vector(vector),
        root,
        generation_root=installed.generation_root,
        authorized_fixtures=installed.authorized(),
    )


class TestSurfaceExercises:
    @pytest.mark.parametrize("surface_id", _surface_params())
    def test_each_surface_runs_through_its_production_builder(
        self,
        tmp_path: Path,
        surface_id: str,
    ) -> None:
        vector = _vector_for(surface_id, tmp_path)

        result = _exercise(vector, tmp_path)

        assert result["surface_id"] == surface_id
        assert result["builder"] == _surfaces.SURFACE_SPECS[surface_id].builder
        assert result["schema_version"] == 1
        assert result["observation"]

    @pytest.mark.parametrize("surface_id", _surface_params())
    def test_each_surface_is_byte_identical_on_a_second_run(
        self,
        tmp_path: Path,
        surface_id: str,
    ) -> None:
        vector = _vector_for(surface_id, tmp_path)
        first = (tmp_path / "first").resolve()
        second = (tmp_path / "second").resolve()
        first.mkdir()
        second.mkdir()

        assert _canonical.canonical_json_bytes(
            _exercise(vector, first)
        ) == _canonical.canonical_json_bytes(_exercise(vector, second))

    def test_a_read_only_surface_leaves_the_builder_runtime_alone(self, tmp_path: Path) -> None:
        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE.value
        vector = _vector_for(surface_id, tmp_path)

        result = _exercise(vector, tmp_path)

        assert result["declared_writer"] is False

    def test_a_read_only_surface_that_writes_is_refused(self, tmp_path: Path) -> None:
        """The verdict is enforced, not reported, so this is how it has to be observed."""

        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE.value
        vector = _vector_for(surface_id, tmp_path)
        real_tree_digest = _surfaces.tree_digest
        seen: list[str] = []

        def drifting(root: Path) -> str:
            digest = real_tree_digest(root)
            seen.append(digest)
            return f"{digest}-{len(seen)}" if root.name == "runtime" else digest

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(_surfaces, "tree_digest", drifting)
            with pytest.raises(_surfaces.SurfaceExerciseError, match="read-only surface"):
                _exercise(vector, tmp_path)

    def test_a_surface_that_rewrites_the_declaration_is_refused(self, tmp_path: Path) -> None:
        """Reviewer mutation M4a, closed: state stability is enforced, not reported.

        Turning the raise into a reported `False` left every case green, because nothing
        exercised the branch. A vector's materialized declaration is the input the policy
        hashed; a surface that rewrites it has invalidated the run, so the whole run stops
        rather than describing what happened. 裁决 4 then removed the reported field
        entirely: the enforceable half of the claim is the root's before/after digest of the
        generation, and this fail-fast is the child's own earlier, narrower half.
        """

        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_ROUTED_AFTER_GLOBAL_SEQUENCE.value
        vector = _vector_for(surface_id, tmp_path)
        real_tree_digest = _surfaces.tree_digest
        seen = 0

        def drifting(root: Path) -> str:
            nonlocal seen
            digest = real_tree_digest(root)
            if root.name != "state":
                return digest
            seen += 1
            return f"{digest}-{seen}"

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(_surfaces, "tree_digest", drifting)
            with pytest.raises(_surfaces.SurfaceExerciseError, match="materialized declaration"):
                _exercise(vector, tmp_path)

    def test_no_result_claims_its_own_state_stability(self, tmp_path: Path) -> None:
        """裁决 4: the frozen result shape carries no `state_unchanged`, in any form.

        A child that reports a constant `true` has reported nothing. The claim now lives
        with the root, which digests the in-generation fixture set before and after this
        child runs and compares the two values the child never sees.
        """

        frozen = {
            "builder",
            "declared_writer",
            "observation",
            "schema_version",
            "surface_id",
        }
        for surface_id in _reachable_surface_ids():
            vector = _vector_for(surface_id, tmp_path)
            root = tmp_path / f"state-{surface_id.rsplit('.', 1)[1]}"
            root.mkdir()

            result = _exercise(vector, root)

            assert set(result) == frozen
            assert "state_unchanged" not in result

    def test_no_result_field_depends_on_sqlite_checkpoint_timing(self, tmp_path: Path) -> None:
        """A writer's runtime tree is not byte-stable across processes; nothing may report it."""

        for surface_id in _reachable_surface_ids():
            vector = _vector_for(surface_id, tmp_path)
            root = tmp_path / surface_id.rsplit(".", 1)[1]
            root.mkdir()

            assert "runtime_unchanged" not in _exercise(vector, root)

    def test_a_writer_surface_is_declared_as_one(self, tmp_path: Path) -> None:
        surface_id = SurfaceId.CONSUME_SIGNAL_BUS_TO_PAPER.value
        vector = _vector_for(surface_id, tmp_path)

        result = _exercise(vector, tmp_path)

        assert result["declared_writer"] is True
        assert result["observation"]["summary"]["delegated_count"] == 1

    def test_the_spool_reader_reports_the_published_prefix(self, tmp_path: Path) -> None:
        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE.value
        vector = _vector_for(surface_id, tmp_path)

        observation = _exercise(vector, tmp_path)["observation"]

        assert observation["count"] == 1
        assert observation["global_sequences"] == [1]
        assert observation["source_descriptor"]["high_watermark"] == 1

    def test_the_blocked_surface_map_is_empty_and_still_fails_closed(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """All thirteen are exercised, and the refusal mechanism is still wired.

        `BLOCKED_SURFACE_REASONS` is empty, so there is no real surface to parametrize over.
        The property it protects has not gone away though: a surface that reaches the frozen
        allowlist without a working exercise must be refused, not answered with a substitute.
        Declaring one here proves the branch still rejects.
        """

        assert harness.BLOCKED_SURFACE_REASONS == {}
        surface_id = SurfaceId.ISOLATED_SIGNAL_OBSERVATIONS.value
        pair_id = next(
            pair
            for pair, surfaces in READER_SURFACES.items()
            if surface_id in {surface.value for surface in surfaces}
        )
        monkeypatch.setitem(
            _surfaces.BLOCKED_SURFACE_REASONS,
            surface_id,
            "a delivery gap declared by this case, to prove the branch still refuses",
        )
        vector = harness.RequestVector(
            vector_id="0" * 64,
            pair_id=pair_id,
            family_id=ACCEPTED_FAMILY_IDS[0],
            surface_id=surface_id,
            input_json="{}",
        )

        with pytest.raises(_surfaces.SurfaceExerciseError, match="is not exercised"):
            _surfaces.exercise_vector(vector, tmp_path)

    def test_a_malformed_vector_envelope_rejects(self, tmp_path: Path) -> None:
        vector = harness_vectors()[0]
        broken = harness.RequestVector(
            vector_id=vector.vector_id,
            pair_id=vector.pair_id,
            family_id=vector.family_id,
            surface_id=vector.surface_id.value,
            input_json='{"call":{}}',
        )

        with pytest.raises(_surfaces.SurfaceExerciseError, match="frozen envelope"):
            _surfaces.exercise_vector(broken, tmp_path)

    def test_a_vector_whose_service_kind_does_not_own_the_surface_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        vector = harness_vectors()[0]
        payload = _canonical.strict_canonical_loads(vector.input_json.encode("utf-8"))
        payload["service"]["service_kind"] = "notifier"
        broken = harness.RequestVector(
            vector_id=vector.vector_id,
            pair_id=vector.pair_id,
            family_id=vector.family_id,
            surface_id=vector.surface_id.value,
            input_json=_canonical.canonical_json_bytes(payload).decode("utf-8"),
        )

        with pytest.raises(_surfaces.SurfaceExerciseError, match="does not own"):
            _surfaces.exercise_vector(broken, tmp_path)


class TestGenerationFixtureChannel:
    """Ruling E-1, from the child's side: nothing is copied that the root did not authorize.

    The root has already checked every fixture against the immutable test manifest and the
    full generation manifest by the time the request is built, so the child's job is narrow
    and total: copy only what the request authorizes, verify the bytes it actually read
    against the *authorized* digest rather than against the vector's own claim, and refuse
    the whole run otherwise. These cases are the refusals.
    """

    @staticmethod
    def _router_vector(root: Path) -> Any:
        return _vector_for(
            SurfaceId.READONLY_STRATEGY_RUNNER_SIGNAL_SOURCE_READ_BATCH.value,
            root,
        )

    @staticmethod
    def _rebuild(vector: Any, mutate: Any) -> harness.RequestVector:
        payload = _canonical.strict_canonical_loads(vector.input_json.encode("utf-8"))
        mutate(payload)
        return harness.RequestVector(
            vector_id=vector.vector_id,
            pair_id=vector.pair_id,
            family_id=vector.family_id,
            surface_id=vector.surface_id.value,
            input_json=_canonical.canonical_json_bytes(payload).decode("utf-8"),
        )

    def test_the_router_reader_runs_over_the_copied_fixture(self, tmp_path: Path) -> None:
        """The happy path, and the proof that the copy is what the builder actually opened."""

        result = _exercise(self._router_vector(tmp_path), tmp_path)
        workspace = next(tmp_path.glob("vector-*"))

        assert result["builder"] == "rquant.runtime_builder_signal.signal_router_builder"
        assert result["observation"]["batch"]["count"] == 1
        copied = workspace / "state" / "runner-state-v1.sql"
        assert stat.S_IMODE(copied.stat().st_mode) == 0o444
        assert int(copied.stat().st_mtime) == 1_787_554_800
        assert (workspace / "state" / "runner-state.sqlite3").is_file()

    def test_a_vector_naming_an_unauthorized_fixture_is_refused(self, tmp_path: Path) -> None:
        def mutate(payload: dict[str, Any]) -> None:
            payload["state"]["generation_files"][0]["source_relative_path"] = (
                f"{GENERATION_FIXTURE_PREFIX}/strategy-router/not-authorized.json"
            )

        broken = self._rebuild(self._router_vector(tmp_path), mutate)

        with pytest.raises(_surfaces.SurfaceExerciseError, match="did not authorize"):
            _surfaces.exercise_vector(
                broken,
                tmp_path,
                generation_root=_generation_root(tmp_path),
                authorized_fixtures=authorized_fixtures(),
            )

    def test_a_vector_that_restates_the_digest_wrongly_is_refused(self, tmp_path: Path) -> None:
        """The vector may repeat the authorized digest; it may not contradict it."""

        def mutate(payload: dict[str, Any]) -> None:
            payload["state"]["generation_files"][0]["sha256"] = "0" * 64

        broken = self._rebuild(self._router_vector(tmp_path), mutate)

        with pytest.raises(_surfaces.SurfaceExerciseError, match="disagrees"):
            _surfaces.exercise_vector(
                broken,
                tmp_path,
                generation_root=_generation_root(tmp_path),
                authorized_fixtures=authorized_fixtures(),
            )

    def test_generation_bytes_that_do_not_hash_to_the_authorization_are_refused(
        self,
        tmp_path: Path,
    ) -> None:
        """The child re-hashes what it read, so a substituted generation file cannot pass.

        The substitution needs its own generation: the shared one is what every other case
        in this module reads, and poisoning it would make this case's failure everyone's.
        """

        generation = tmp_path / "substituted-generation"
        generation.mkdir(mode=0o700)
        install_generation_fixtures(generation)
        target = generation / generation_fixture_declarations()[0].relative_path
        target.chmod(0o644)
        target.write_bytes(b'{"default_no_target_reason":"substituted","rules":[]}')
        target.chmod(0o444)

        with pytest.raises(_surfaces.SurfaceExerciseError, match="authorized value"):
            _surfaces.exercise_vector(
                _request_vector(self._router_vector(tmp_path)),
                tmp_path,
                generation_root=generation,
                authorized_fixtures=authorized_fixtures(),
            )

    def test_a_run_with_no_authorized_root_cannot_answer_a_fixture_vector(
        self,
        tmp_path: Path,
    ) -> None:
        """No root-side authorization means no fixture, not a fixture read from somewhere else."""

        with pytest.raises(_surfaces.SurfaceExerciseError, match="authorized generation root"):
            _surfaces.exercise_vector(_request_vector(self._router_vector(tmp_path)), tmp_path)

    @pytest.mark.parametrize(
        ("relative", "expected"),
        [
            ("../escape.json", "normalized relative path"),
            ("signal-family/./fixtures.json", "normalized relative path"),
            ("/etc/rquant/policy.json", "normalized relative path"),
        ],
    )
    def test_a_traversing_fixture_path_is_refused(
        self,
        tmp_path: Path,
        relative: str,
        expected: str,
    ) -> None:
        """Even an authorized-looking path must still be a normalized relative one."""

        from rquant.signal_family_verifier_harness._workspace import (
            WorkspaceError,
            _read_generation_bytes,
        )

        with pytest.raises(WorkspaceError, match=expected):
            _read_generation_bytes(_generation_root(tmp_path), relative)

    def test_an_inlined_sqlite_script_is_refused(self, tmp_path: Path) -> None:
        """Reviewer finding `R2E-SPEC-02`: the executed script must be generation content.

        Before this was enforced, `script_path` resolved anywhere under `@workspace/`,
        including bytes the same vector had just written through `state.files`. Ruling E-2's
        claim — that the authoritative dump is the one the generation carries and the root
        hashes — was then a convention, not a rule.
        """

        def mutate(payload: dict[str, Any]) -> None:
            payload["state"]["generation_files"] = [
                entry
                for entry in payload["state"]["generation_files"]
                if entry["path"].endswith(".json")
            ]
            payload["state"]["files"] = [
                {"content": "CREATE TABLE t(a);", "path": "@workspace/runner-state-v1.sql"}
            ]

        broken = self._rebuild(self._router_vector(tmp_path), mutate)

        with pytest.raises(
            _surfaces.SurfaceExerciseError,
            match="not an authorized generation fixture",
        ):
            _surfaces.exercise_vector(
                broken,
                tmp_path,
                generation_root=_generation_root(tmp_path),
                authorized_fixtures=authorized_fixtures(),
            )

    def test_a_sqlite_source_that_is_not_replayable_is_refused(self, tmp_path: Path) -> None:
        """Ruling E-2: the dump is authoritative, so a dump that will not replay is fatal.

        The script here *is* an authorized generation fixture — it is the frozen routing
        policy, which is JSON — so it passes the source check and fails on its content.
        """

        def mutate(payload: dict[str, Any]) -> None:
            payload["state"]["sqlite_sources"][0]["script_path"] = "@workspace/routing-policy.json"

        broken = self._rebuild(self._router_vector(tmp_path), mutate)

        with pytest.raises(_surfaces.SurfaceExerciseError, match="not replayable"):
            _surfaces.exercise_vector(
                broken,
                tmp_path,
                generation_root=_generation_root(tmp_path),
                authorized_fixtures=authorized_fixtures(),
            )

    @pytest.mark.parametrize(
        "script",
        [
            "ATTACH DATABASE '/tmp/rquant-escape.db' AS escape;",
            "CREATE TABLE t(a);\nVACUUM INTO '/tmp/rquant-escape-vacuum.db';",
            "DETACH DATABASE main;",
        ],
        ids=["attach", "vacuum-into", "detach"],
    )
    def test_a_replayed_script_cannot_reach_outside_its_workspace(
        self,
        tmp_path: Path,
        script: str,
    ) -> None:
        """Reviewer finding `R2E-SPEC-02`: `executescript` is not a closed operation.

        `ATTACH DATABASE` and `VACUUM INTO` both name a path of the script's choosing, so a
        replay is only contained if the statement kinds are. The authorizer permits exactly
        the actions a canonical dump of the producer schema performs and denies the rest.
        """

        from rquant.signal_family_verifier_harness._workspace import replay_sql_script

        target = tmp_path / "replayed.sqlite3"
        escape = Path("/tmp/rquant-escape.db")
        vacuum_escape = Path("/tmp/rquant-escape-vacuum.db")

        with pytest.raises(_surfaces.WorkspaceError, match="not replayable"):
            replay_sql_script(target, script)

        assert not escape.exists()
        assert not vacuum_escape.exists()

    def test_the_authorizer_still_admits_the_real_producer_dump(self, tmp_path: Path) -> None:
        """The closed action set is derived from this dump, so it has to keep replaying it."""

        from rquant.signal_family_verifier_harness._workspace import replay_sql_script

        script = (
            PRODUCER_FIXTURE_ROOT / "strategy-router" / "runner-state-v1.sql"
        ).read_text(encoding="utf-8")
        target = tmp_path / "replayed.sqlite3"

        replay_sql_script(target, script)

        import sqlite3

        connection = sqlite3.connect(target)
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        finally:
            connection.close()
        assert {"runner_metadata", "runner_source_identity", "runner_signal"} <= tables

    # -- reviewer finding R2E-SPEC-01: the `@generation/` boundary ---------------------

    @staticmethod
    def _workspace_over(root: Path) -> Any:
        installed = _installed_fixtures(root)
        return _surfaces.VectorWorkspace(
            root,
            "f" * 64,
            generation_root=installed.generation_root,
            authorized_fixtures=installed.authorized(),
        )

    def test_the_authorized_directory_set_stops_at_the_fixture_root(
        self,
        tmp_path: Path,
    ) -> None:
        """The closure is the fixture set's own directories, never an ancestor of them."""

        workspace = self._workspace_over(tmp_path)
        entries = tuple(workspace.authorized_fixtures)

        directories = workspace.authorized_generation_directories()

        assert f"{GENERATION_FIXTURE_PREFIX}/strategy-router" in directories
        assert "signal-family" not in directories
        assert "" not in directories
        # Every authorized directory is a `dirname` of some fixture, and every fixture's own
        # directory is authorized. The closure is exactly the fixture set's, nothing wider.
        chains = {
            "/".join(entry.split("/")[:depth])
            for entry in entries
            for depth in range(1, len(entry.split("/")))
        }
        assert directories <= chains
        assert {entry.rsplit("/", 1)[0] for entry in entries} <= directories
        assert all(
            candidate.startswith(GENERATION_FIXTURE_PREFIX) for candidate in directories
        )

    @pytest.mark.parametrize(
        ("declared", "expected"),
        [
            ("@generation/signal-family", "did not authorize"),
            ("@generation/signal-family/test-manifest-v1.json", "did not authorize"),
            ("@generation/src/rquant/strategy_runner.py", "did not authorize"),
            ("@generation/../etc/rquant", "dot components"),
            ("@generation//etc", "inside the generation"),
        ],
        ids=["ancestor-directory", "non-entry-sibling", "unrelated-tree", "parent", "absolute"],
    )
    def test_a_generation_path_outside_the_fixture_set_is_refused(
        self,
        tmp_path: Path,
        declared: str,
        expected: str,
    ) -> None:
        """Reviewer finding `R2E-SPEC-01` and mutation M2, closed.

        `@generation/signal-family` is the one that matters: that directory also holds the
        immutable test manifest and the successor and overlay bundles, none of which are
        `generation_files` entries, so none of them is byte-checked by the root or covered by
        its before/after digest. Deleting the boundary used to leave every case green.
        """

        workspace = self._workspace_over(tmp_path)

        with pytest.raises(_surfaces.WorkspaceError, match=expected):
            workspace.generation_path(declared)

    def test_a_generation_directory_inside_the_fixture_set_is_accepted(
        self,
        tmp_path: Path,
    ) -> None:
        """The shadow readers need directories, not files, so the rule cannot be file-only."""

        workspace = self._workspace_over(tmp_path)
        declared = f"@generation/{GENERATION_FIXTURE_PREFIX}/strategy-router"

        resolved = workspace.generation_path(declared)

        assert resolved == workspace.generation_root / GENERATION_FIXTURE_PREFIX / (
            "strategy-router"
        )
        assert resolved.is_dir()

    def test_a_generation_path_through_a_symbolic_link_is_refused(
        self,
        tmp_path: Path,
    ) -> None:
        """Reading in place only means anything if the walk cannot be redirected.

        The link is planted inside a throwaway generation so the shared one, which every
        other case in this module reads, is left exactly as the root digested it.
        """

        generation = tmp_path / "linked-generation"
        generation.mkdir(mode=0o700)
        installed = install_generation_fixtures(generation)
        directory = generation / GENERATION_FIXTURE_PREFIX / "strategy-router"
        target = directory / "runner-state-v1.sql"
        linked = directory / "linked-state.sql"
        linked.symlink_to(target)
        authorized = dict(installed.authorized())
        relative = f"{GENERATION_FIXTURE_PREFIX}/strategy-router/linked-state.sql"
        authorized[relative] = authorized[
            f"{GENERATION_FIXTURE_PREFIX}/strategy-router/runner-state-v1.sql"
        ]
        workspace = _surfaces.VectorWorkspace(
            tmp_path / "linked-workspace",
            "e" * 64,
            generation_root=generation,
            authorized_fixtures=authorized,
        )

        with pytest.raises(_surfaces.WorkspaceError, match="symbolic link"):
            workspace.generation_path(f"@generation/{relative}")


class TestProducerFixtureBuild:
    """The fixture set is producer output, checked in, and byte-reproducible."""

    @staticmethod
    def _build_script() -> Any:
        import importlib.util

        script = REPOSITORY_ROOT / "scripts" / "build-signal-family-producer-fixtures.py"
        spec = importlib.util.spec_from_file_location("_wpe_fixture_builder", script)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_two_builds_produce_the_same_bytes(self, tmp_path: Path) -> None:
        module = self._build_script()
        first = tmp_path / "first"
        second = tmp_path / "second"

        module.build_fixtures(first)
        module.build_fixtures(second)

        produced = sorted(path.relative_to(first) for path in first.rglob("*") if path.is_file())
        assert produced
        for relative in produced:
            assert (first / relative).read_bytes() == (second / relative).read_bytes()

    def test_the_checked_in_fixtures_are_what_the_script_produces(self, tmp_path: Path) -> None:
        """A hand-edited fixture would be producer state nothing produced."""

        module = self._build_script()
        rebuilt = tmp_path / "rebuilt"

        module.build_fixtures(rebuilt)

        for declared in generation_fixture_declarations():
            name = declared.relative_path.rsplit("/", 2)[-2:]
            assert hashlib.sha256(
                (rebuilt / name[0] / name[1]).read_bytes()
            ).hexdigest() == declared.sha256

    def test_the_seeded_identity_ddl_matches_the_production_initializer(self) -> None:
        """Seeding the identity row means creating the table production would have created."""

        import inspect

        from rquant.strategy_runner import StrategyRunnerStore

        module = self._build_script()
        source = inspect.getsource(StrategyRunnerStore._initialize)
        normalized = " ".join(module.RUNNER_SOURCE_IDENTITY_DDL.split())

        assert normalized in " ".join(source.split())

    def test_the_fixture_carries_no_private_key_material(self) -> None:
        """No fixture in this set is signed, so none of it may carry a key at all."""

        for declared in generation_fixture_declarations():
            payload = (
                PRODUCER_FIXTURE_ROOT
                / declared.relative_path[len(GENERATION_FIXTURE_PREFIX) + 1 :]
            ).read_bytes()
            assert b"PRIVATE KEY" not in payload
            assert b"BEGIN PGP" not in payload
            assert b"ssh-ed25519" not in payload


class TestRecomputeExpectations:
    """Ruling E-7: one command, and running it twice changes nothing."""

    @staticmethod
    def _module() -> Any:
        import importlib.util

        script = REPOSITORY_ROOT / "scripts" / "signal_family_recompute_expectations.py"
        spec = importlib.util.spec_from_file_location("_wpe_recompute", script)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        # `dataclasses` resolves its owner through `sys.modules`, so the module has to be
        # registered before it executes or the first decorated class raises.
        sys.modules["_wpe_recompute"] = module
        spec.loader.exec_module(module)
        return module

    def test_the_policy_recomputation_is_idempotent(self) -> None:
        module = self._module()

        first = module.recompute_policy(write=False)
        second = module.recompute_policy(write=False)

        assert first == second
        assert POLICY_DIGEST_PATTERN.search(first.detail) is not None

    @pytest.mark.parametrize(
        ("path", "category"),
        [
            ("src/rquant/signal_family_verification.py", "production"),
            ("tests/fixtures/signal_family_producer/x.sql", "fixture"),
            ("tests/manifests/full-suite-v1/index.json", "fixture"),
            ("tests/unit/test_signal_family_verifier_harness.py", "test"),
            ("tests/support/signal_family_harness_vectors.py", "test"),
            ("deploy/systemd/rquant-runtime-router.service", "architecture"),
            ("scripts/signal_family_recompute_expectations.py", "architecture"),
            ("docs/architecture/production-interpreter-authority.md", "architecture"),
        ],
    )
    def test_the_category_rule_reproduces_the_frozen_assignments(
        self,
        path: str,
        category: str,
    ) -> None:
        """Ruling B-3 fixed `deploy/` at architecture; the rest follows the existing policy."""

        assert self._module()._category(path) == category

    def test_the_recomputation_never_writes_without_being_asked(self) -> None:
        module = self._module()
        before = module.POLICY_PATH.read_bytes()

        module.recompute_policy(write=False)
        module.recompute_producer_fixtures(write=False)

        assert module.POLICY_PATH.read_bytes() == before


class TestServingLoaderProvenance:
    """Reviewer mutation M8, closed: an injected snapshot loader is not the owner path."""

    @staticmethod
    def _injected_step(tmp_path: Path) -> Any:
        from rquant.runtime_builder_serving import serving_publisher_builder
        from rquant.runtime_service_control import RuntimeServicePlane
        from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

        def fake_loader(as_of: datetime) -> Any:  # pragma: no cover - never invoked
            raise AssertionError("the fake loader must never be reached")

        manifest = RuntimeServiceManifest(
            service_id="serving-publisher",
            service_kind=RuntimeServiceKind.SERVING_PUBLISHER,
            plane=RuntimeServicePlane.SERVING,
            interval_seconds=1.0,
            stale_after_seconds=50.0,
            producer_commit="f" * 40,
            settings={
                "schema_version": 3,
                "serving_root": str(tmp_path / "serving"),
            },
        )
        return serving_publisher_builder(
            snapshot_loader=fake_loader,
            clock=lambda: datetime(2026, 8, 24, 7, 30, tzinfo=UTC),
        )(manifest)

    def test_an_injected_loader_cannot_stand_in_for_the_assembler(self, tmp_path: Path) -> None:
        step = self._injected_step(tmp_path)

        with pytest.raises(_surfaces.SurfaceExerciseError, match="ServingSnapshotAssembler"):
            _surfaces._closure_assembler(step)

    @staticmethod
    def _step_with_loader(loader: Any) -> Any:
        """A step closure shaped exactly like the serving builder's, holding one loader."""

        resolved_snapshot_loader = loader

        def step() -> Any:  # pragma: no cover - only its closure cell is read
            return resolved_snapshot_loader

        return step

    def test_a_loader_bound_to_the_wrong_function_rejects(self) -> None:
        """`__self__` alone is not enough: the bound function must be the frozen `assemble`.

        This is the mutation the `__self__` check cannot see — a genuine assembler carrying
        somebody else's implementation.
        """

        import types

        from rquant.runtime_serving_snapshot import ServingSnapshotAssembler

        assembler = object.__new__(ServingSnapshotAssembler)

        def substituted(self: Any, as_of: Any) -> Any:  # pragma: no cover - never invoked
            raise AssertionError("the substituted assemble must never be reached")

        loader = types.MethodType(substituted, assembler)
        assert type(loader.__self__) is ServingSnapshotAssembler

        with pytest.raises(_surfaces.SurfaceExerciseError, match="frozen assemble"):
            _surfaces._closure_assembler(self._step_with_loader(loader))

    def test_the_genuine_owner_path_loader_is_accepted(self) -> None:
        import types

        from rquant.runtime_serving_snapshot import ServingSnapshotAssembler

        assembler = object.__new__(ServingSnapshotAssembler)
        loader = types.MethodType(ServingSnapshotAssembler.assemble, assembler)

        assert _surfaces._closure_assembler(self._step_with_loader(loader)) is assembler


class TestPrivateTemporaryRoot:
    """M-01: the precondition the suite depended on is now expressed in the suite."""

    def test_the_guard_names_the_writable_ancestor(self, tmp_path: Path) -> None:
        loose = tmp_path / "loose"
        loose.mkdir(mode=0o755)
        loose.chmod(0o777)
        inner = loose / "inner"
        inner.mkdir(mode=0o700)

        offenders = _private_root.unsafe_ancestors(inner)

        assert [node for node, _mode in offenders] == [loose]
        with pytest.raises(pytest.fail.Exception, match="world-writable ancestor"):
            _private_root.require_private_ancestry(inner)

    def test_a_private_chain_passes_the_guard(self, tmp_path: Path) -> None:
        assert _private_root.unsafe_ancestors(tmp_path) == ()
        _private_root.require_private_ancestry(tmp_path)

    def test_the_session_root_is_private_and_outside_tmpdir(
        self,
        signal_family_private_root: Path,
    ) -> None:
        """`$HOME`, not `TMPDIR` — `TMPDIR` is the thing that cannot be trusted here."""

        assert stat.S_IMODE(signal_family_private_root.stat().st_mode) == 0o700
        assert signal_family_private_root.parent == Path.home()
        assert _private_root.unsafe_ancestors(signal_family_private_root) == ()

    def test_tmp_path_is_rooted_in_the_session_root(
        self,
        tmp_path: Path,
        signal_family_private_root: Path,
    ) -> None:
        assert signal_family_private_root in tmp_path.parents


class TestWorkspaceAccessClass:
    """The reviewer's access-class experiment, fixed as a case.

    A uid switch is not available under pytest, so the production question — "what can the
    `lighthouse` child do with a root-owned workspace?" — is modelled by putting the same
    bits in the *owner* class and running the real exercises beneath them. The kernel applies
    the owner class to us exactly as it would apply the other class to the child, so the
    outcome per bit pattern is the production outcome.

    This is the case fix round 1 was missing. `0711` satisfied every bit-level assertion made
    at the time and still failed every surface, because both ancestry walks open every
    component with `O_RDONLY | O_DIRECTORY` and read-only on a directory needs the read bit.
    """

    @staticmethod
    def _exercise_all(gate: Path, visitor_bits: int) -> tuple[int, int]:
        """Run every workspace-reading surface beneath a gate; return (ok, failed).

        The experiment measures what the *workspace* access class does to a surface, so it
        only covers surfaces that read the workspace. The three `strategy-shadow` readers
        read an in-generation export in place and materialize nothing, so the gate has
        nothing to deny them and including them would measure nothing. Membership is derived
        from each vector's declared state rather than named, so a future vector that starts
        materializing state is covered automatically.
        """

        gate.mkdir(mode=0o700, parents=True, exist_ok=True)
        workspace = gate / "workspace"
        workspace.mkdir(mode=0o700, exist_ok=True)
        gate.chmod(visitor_bits)
        succeeded = 0
        failed = 0
        try:
            for index, vector in enumerate(_workspace_reading_vectors(gate)):
                root = workspace / f"run-{index:02d}"
                try:
                    root.mkdir(mode=0o700)
                    _exercise(vector, root)
                except Exception:  # noqa: BLE001 - the failure mode is the measurement
                    failed += 1
                else:
                    succeeded += 1
        finally:
            gate.chmod(0o700)
        return succeeded, failed

    def test_execute_only_fails_every_surface(self, tmp_path: Path) -> None:
        """`--x` for the visitor, i.e. the `0711` that fix round 1 shipped."""

        succeeded, failed = self._exercise_all(tmp_path / "gate-x", 0o100)

        assert succeeded == 0
        assert failed == len(_workspace_reading_vectors(tmp_path / "gate-x"))

    def test_read_and_execute_passes_every_surface(self, tmp_path: Path) -> None:
        """`r-x` for the visitor, i.e. the `0715` this round ships."""

        succeeded, failed = self._exercise_all(tmp_path / "gate-rx", 0o500)

        assert failed == 0
        assert succeeded == len(_workspace_reading_vectors(tmp_path / "gate-rx"))

    def test_the_frozen_mode_carries_the_bits_the_experiment_selected(self) -> None:
        from rquant.signal_family_root_verifier import CHILD_WORKSPACE_MODE

        assert CHILD_WORKSPACE_MODE & 0o007 == 0o005


# ---------------------------------------------------------------------------------------
# The child entry point
# ---------------------------------------------------------------------------------------


class TestChildEntryPoint:
    def test_run_child_answers_every_vector_exactly_once(self, tmp_path: Path) -> None:
        installed = _installed_fixtures(tmp_path)
        vectors = _harness_vectors(tmp_path)
        request = _request_payload(
            vectors,
            generation_root=str(installed.generation_root),
            generation_files=tuple(
                declared.model_dump(mode="json") for declared in installed.declarations
            ),
        )

        payload = harness_main.run_child(request, workspace_root=tmp_path)
        decoded = _canonical.strict_canonical_loads(payload)

        assert len(decoded["vector_results"]) == len(vectors)
        assert {row["vector_id"] for row in decoded["vector_results"]} == {
            vector.vector_id for vector in vectors
        }

    def test_the_response_satisfies_the_frozen_child_result_model(self, tmp_path: Path) -> None:
        from rquant.signal_family_verification import SignalFamilyChildResultV1

        installed = _installed_fixtures(tmp_path)
        vectors = _harness_vectors(tmp_path)
        payload = harness_main.run_child(
            _request_payload(
                vectors,
                generation_root=str(installed.generation_root),
                generation_files=tuple(
                    declared.model_dump(mode="json") for declared in installed.declarations
                ),
            ),
            workspace_root=tmp_path,
        )

        result = SignalFamilyChildResultV1.from_canonical_ipc_bytes(
            payload,
            max_vector_count=len(vectors),
        )

        assert result.run_id == RUN_ID
        assert result.test_manifest_hash == TEST_MANIFEST_HASH
        assert len(result.vector_results) == len(vectors)

    def test_a_rejected_request_exits_nonzero_and_writes_nothing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        request_read, request_write = os.pipe()
        result_read, result_write = os.pipe()
        os.write(request_write, b'{"schema_version":2}')
        os.close(request_write)
        monkeypatch.setenv(harness_main.REQUEST_FD_ENV_KEY, str(request_read))
        monkeypatch.setenv(harness_main.RESULT_FD_ENV_KEY, str(result_write))
        monkeypatch.chdir(tmp_path)

        code = harness_main.main()
        os.close(result_write)
        emitted = os.read(result_read, 4096)
        os.close(result_read)

        assert code == harness_main.EXIT_EXERCISE_FAILED
        assert emitted == b""

    def test_a_missing_descriptor_environment_exits_nonzero(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(harness_main.REQUEST_FD_ENV_KEY, raising=False)
        monkeypatch.delenv(harness_main.RESULT_FD_ENV_KEY, raising=False)

        assert harness_main.main() == harness_main.EXIT_REQUEST_REJECTED


# ---------------------------------------------------------------------------------------
# The deterministic build
# ---------------------------------------------------------------------------------------


class TestHarnessBuild:
    def test_two_builds_produce_the_same_bytes(self, tmp_path: Path) -> None:
        script = _load_build_script()
        first = tmp_path / "first.pyz"
        second = tmp_path / "second.pyz"

        first_digest = script.build_harness(REPOSITORY_ROOT, first)
        second_digest = script.build_harness(REPOSITORY_ROOT, second)

        assert first.read_bytes() == second.read_bytes()
        assert first_digest == second_digest
        assert first_digest == hashlib.sha256(first.read_bytes()).hexdigest()

    def test_the_archive_is_frozen_and_relocated(self, tmp_path: Path) -> None:
        script = _load_build_script()
        artifact = tmp_path / "harness.pyz"
        script.build_harness(REPOSITORY_ROOT, artifact)

        with zipfile.ZipFile(artifact) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]

        assert names == sorted(names)
        assert "__main__.py" in names
        assert all(not name.startswith("rquant/") for name in names)
        assert all("__pycache__" not in name for name in names)
        assert all(not name.endswith(".pyc") for name in names)
        assert {info.date_time for info in infos} == {script.FROZEN_TIMESTAMP}
        assert {info.external_attr for info in infos} == {script.FROZEN_EXTERNAL_ATTR}
        assert any(name.startswith(f"{script.ARCHIVE_PACKAGE_NAME}/") for name in names)

    def test_the_artifact_is_installed_read_only(self, tmp_path: Path) -> None:
        script = _load_build_script()
        artifact = tmp_path / "harness.pyz"
        script.build_harness(REPOSITORY_ROOT, artifact)

        assert artifact.stat().st_mode & 0o222 == 0

    def test_the_build_refuses_a_missing_package(self, tmp_path: Path) -> None:
        script = _load_build_script()

        with pytest.raises(SystemExit):
            script.collect_sources(script.package_root(tmp_path))
