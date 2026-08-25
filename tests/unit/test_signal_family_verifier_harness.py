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
import os
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
from tests.support.signal_family_harness_vectors import harness_vectors

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent.parent
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build-signal-family-verifier-harness.py"

RUN_ID = "a" * 64
TEST_MANIFEST_HASH = "c" * 64


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


def _request_payload(vectors: tuple[Any, ...]) -> bytes:
    return _canonical.canonical_json_bytes(
        {
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


def _exercise(vector: Any, root: Path) -> dict[str, Any]:
    return _surfaces.exercise_vector(_request_vector(vector), root)


class TestSurfaceExercises:
    @pytest.mark.parametrize(
        "surface_id",
        list(harness.IMPLEMENTED_SURFACE_IDS),
    )
    def test_each_surface_runs_through_its_production_builder(
        self,
        tmp_path: Path,
        surface_id: str,
    ) -> None:
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )

        result = _exercise(vector, tmp_path)

        assert result["surface_id"] == surface_id
        assert result["builder"] == _surfaces.SURFACE_SPECS[surface_id].builder
        assert result["state_unchanged"] is True
        assert result["schema_version"] == 1
        assert result["observation"]

    @pytest.mark.parametrize(
        "surface_id",
        list(harness.IMPLEMENTED_SURFACE_IDS),
    )
    def test_each_surface_is_byte_identical_on_a_second_run(
        self,
        tmp_path: Path,
        surface_id: str,
    ) -> None:
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )
        first = (tmp_path / "first").resolve()
        second = (tmp_path / "second").resolve()
        first.mkdir()
        second.mkdir()

        assert _canonical.canonical_json_bytes(
            _exercise(vector, first)
        ) == _canonical.canonical_json_bytes(_exercise(vector, second))

    def test_a_read_only_surface_leaves_the_builder_runtime_alone(self, tmp_path: Path) -> None:
        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE.value
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )

        result = _exercise(vector, tmp_path)

        assert result["declared_writer"] is False
        assert result["state_unchanged"] is True

    def test_a_read_only_surface_that_writes_is_refused(self, tmp_path: Path) -> None:
        """The verdict is enforced, not reported, so this is how it has to be observed."""

        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE.value
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )
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
        """Reviewer mutation M4a, closed: `state_unchanged` is enforced, not reported.

        Turning the raise into a reported `False` left every case green, because nothing
        exercised the branch. A vector's materialized declaration is the input the policy
        hashed; a surface that rewrites it has invalidated the run, so the whole run stops
        rather than describing what happened.
        """

        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_ROUTED_AFTER_GLOBAL_SEQUENCE.value
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )
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

    def test_every_result_reports_the_declaration_as_intact(self, tmp_path: Path) -> None:
        """The field is a constant because the alternative is a rejection, never a `False`."""

        for surface_id in harness.IMPLEMENTED_SURFACE_IDS:
            vector = next(
                item for item in harness_vectors() if item.surface_id.value == surface_id
            )
            root = tmp_path / f"state-{surface_id.rsplit('.', 1)[1]}"
            root.mkdir()

            assert _exercise(vector, root)["state_unchanged"] is True

    def test_no_result_field_depends_on_sqlite_checkpoint_timing(self, tmp_path: Path) -> None:
        """A writer's runtime tree is not byte-stable across processes; nothing may report it."""

        for surface_id in harness.IMPLEMENTED_SURFACE_IDS:
            vector = next(
                item for item in harness_vectors() if item.surface_id.value == surface_id
            )
            root = tmp_path / surface_id.rsplit(".", 1)[1]
            root.mkdir()

            assert "runtime_unchanged" not in _exercise(vector, root)

    def test_a_writer_surface_is_declared_as_one(self, tmp_path: Path) -> None:
        surface_id = SurfaceId.CONSUME_SIGNAL_BUS_TO_PAPER.value
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )

        result = _exercise(vector, tmp_path)

        assert result["declared_writer"] is True
        assert result["observation"]["summary"]["delegated_count"] == 1

    def test_the_spool_reader_reports_the_published_prefix(self, tmp_path: Path) -> None:
        surface_id = SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE.value
        vector = next(
            item for item in harness_vectors() if item.surface_id.value == surface_id
        )

        observation = _exercise(vector, tmp_path)["observation"]

        assert observation["count"] == 1
        assert observation["global_sequences"] == [1]
        assert observation["source_descriptor"]["high_watermark"] == 1

    @pytest.mark.parametrize("surface_id", sorted(harness.BLOCKED_SURFACE_REASONS))
    def test_a_blocked_surface_refuses_instead_of_substituting(
        self,
        tmp_path: Path,
        surface_id: str,
    ) -> None:
        pair_id = next(
            pair
            for pair, surfaces in READER_SURFACES.items()
            if surface_id in {surface.value for surface in surfaces}
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


class TestWorkspaceAccessClass:
    """The reviewer's access-class experiment, fixed as a case.

    A uid switch is not available under pytest, so the production question — "what can the
    `lighthouse` child do with a root-owned workspace?" — is modelled by putting the same
    bits in the *owner* class and running the real exercises beneath them. The kernel applies
    the owner class to us exactly as it would apply the other class to the child, so the
    outcome per bit pattern is the production outcome.

    This is the case fix round 1 was missing. `0711` satisfied every bit-level assertion made
    at the time and still failed all eight surfaces, because both ancestry walks open every
    component with `O_RDONLY | O_DIRECTORY` and read-only on a directory needs the read bit.
    """

    @staticmethod
    def _exercise_all(gate: Path, visitor_bits: int) -> tuple[int, int]:
        """Run all eight surfaces beneath a gate carrying `visitor_bits`; return (ok, failed)."""

        gate.mkdir(mode=0o700, parents=True, exist_ok=True)
        workspace = gate / "workspace"
        workspace.mkdir(mode=0o700, exist_ok=True)
        gate.chmod(visitor_bits)
        succeeded = 0
        failed = 0
        try:
            for index, vector in enumerate(harness_vectors()):
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
        assert failed == len(harness.IMPLEMENTED_SURFACE_IDS) == 8

    def test_read_and_execute_passes_every_surface(self, tmp_path: Path) -> None:
        """`r-x` for the visitor, i.e. the `0715` this round ships."""

        succeeded, failed = self._exercise_all(tmp_path / "gate-rx", 0o500)

        assert failed == 0
        assert succeeded == len(harness.IMPLEMENTED_SURFACE_IDS) == 8

    def test_the_frozen_mode_carries_the_bits_the_experiment_selected(self) -> None:
        from rquant.signal_family_root_verifier import CHILD_WORKSPACE_MODE

        assert CHILD_WORKSPACE_MODE & 0o007 == 0o005


# ---------------------------------------------------------------------------------------
# The child entry point
# ---------------------------------------------------------------------------------------


class TestChildEntryPoint:
    def test_run_child_answers_every_vector_exactly_once(self, tmp_path: Path) -> None:
        vectors = harness_vectors()

        payload = harness_main.run_child(_request_payload(vectors), workspace_root=tmp_path)
        decoded = _canonical.strict_canonical_loads(payload)

        assert len(decoded["vector_results"]) == len(vectors)
        assert {row["vector_id"] for row in decoded["vector_results"]} == {
            vector.vector_id for vector in vectors
        }

    def test_the_response_satisfies_the_frozen_child_result_model(self, tmp_path: Path) -> None:
        from rquant.signal_family_verification import SignalFamilyChildResultV1

        vectors = harness_vectors()
        payload = harness_main.run_child(_request_payload(vectors), workspace_root=tmp_path)

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
