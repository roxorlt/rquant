"""The bounded IPC request the child accepts and the one response it may emit.

The frozen identity sets below are literals rather than imports. This module is the fixed
root-owned artifact the external policy hashes, so a generation that renamed a pair,
widened the family set, or added a surface must not be able to widen what the child accepts
by supplying the constants itself. `tests/unit/test_signal_family_verifier_harness.py` pins
each literal against the generation module it mirrors, so genuine drift fails the suite
instead of silently passing through.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from ._canonical import (
    CanonicalJsonError,
    canonical_json_bytes,
    canonical_sha256,
    sha256_hex,
    strict_canonical_loads,
)

#: `authority.md` L1441 bounds the request the root may build.
MAX_REQUEST_BYTES: Final[int] = 1_048_576
#: `authority.md` L1466 bounds the single IPC response.
MAX_RESPONSE_BYTES: Final[int] = 1_048_576
#: `signal_family_verification.MAX_CANONICAL_RESULT_BYTES`.
MAX_CANONICAL_RESULT_BYTES: Final[int] = 65_536
#: `signal_family_verification.MAX_VECTOR_INPUT_BYTES`.
MAX_VECTOR_INPUT_BYTES: Final[int] = 65_536

#: `signal_family_successor_registry.PAIR_IDS`.
PAIR_IDS: Final[tuple[str, ...]] = (
    "notifier-serving",
    "router-notifier",
    "router-paper",
    "strategy-router",
    "strategy-shadow",
)

#: `signal_family_successor_registry.ACCEPTED_FAMILY_IDS`.
ACCEPTED_FAMILY_IDS: Final[tuple[str, ...]] = ("rquant.signal-envelope/v1",)

#: `signal_family_verification.READER_SURFACES`, the pair-to-surface map of L1211-1217.
READER_SURFACES: Final[dict[str, tuple[str, ...]]] = {
    "notifier-serving": (
        "rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__",
        "rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble",
        "rquant.serving_read_models.build_serving_read_models",
    ),
    "router-notifier": (
        "rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence",
        "rquant.notification_state.NotificationStateStore.replicate",
    ),
    "router-paper": (
        "rquant.signal_route_spool.ReadonlySignalRouteSpool.signals_after_global_sequence",
        "rquant.paper_signal_consumer.consume_signal_bus_to_paper",
        "rquant.paper_signal_worker.PaperSignalQueueStore.ingest",
    ),
    "strategy-router": (
        "rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch",
        "rquant.signal_router_runtime.route_runner_signals",
    ),
    "strategy-shadow": (
        "rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch",
        "rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot",
        "rquant.runtime_shadow_sources.isolated_signal_observations",
    ),
}

#: `signal_family_verification.PRODUCER_SURFACES`. A producer surface proves transport
#: production; it is never a reader receipt, so a vector may not name one.
PRODUCER_SURFACES: Final[dict[str, tuple[str, ...]]] = {
    "notifier-serving": (
        "rquant.runtime_builder_signal._publish_signal_authority",
        "rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish",
    ),
    "router-notifier": (
        "rquant.signal_route_spool.SignalRouteSpool.publish",
        "rquant.signal_route_spool.publish_signal_bus_prefix",
    ),
    "router-paper": (
        "rquant.signal_route_spool.SignalRouteSpool.publish",
        "rquant.signal_route_spool.publish_signal_bus_prefix",
    ),
    "strategy-router": ("rquant.strategy_runner.StrategyRunnerStore.process_batch",),
    "strategy-shadow": (
        "rquant.strategy_runner.StrategyRunnerStore.process_batch",
        "rquant.strategy_runner.StrategyRunnerStore.publish_session_close_receipt",
    ),
}

_REQUEST_KEYS: Final[tuple[str, ...]] = (
    "run_id",
    "schema_version",
    "test_manifest_hash",
    "vectors",
)
_VECTOR_KEYS: Final[tuple[str, ...]] = (
    "family_id",
    "input_json",
    "pair_id",
    "surface_id",
    "vector_id",
)
_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789abcdef")


class ChildRequestError(ValueError):
    """The request the root handed the child is not the one the policy authorized."""


def _require_sha256(value: Any, *, field: str) -> str:
    if type(value) is not str or len(value) != 64 or not _HEX_DIGITS.issuperset(value):
        raise ChildRequestError(f"{field} must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True)
class RequestVector:
    """One frozen vector identity plus its input bytes. Never an expected result."""

    vector_id: str
    pair_id: str
    family_id: str
    surface_id: str
    input_json: str

    @property
    def identity_preimage(self) -> dict[str, str]:
        return {
            "family_id": self.family_id,
            "input_json": self.input_json,
            "pair_id": self.pair_id,
            "surface_id": self.surface_id,
        }

    def parsed_input(self) -> Any:
        try:
            return strict_canonical_loads(self.input_json.encode("utf-8"))
        except CanonicalJsonError as exc:
            raise ChildRequestError("vector input_json is not canonical JSON") from exc


@dataclass(frozen=True)
class ChildRequest:
    """The complete bounded request, already checked against every frozen rule."""

    run_id: str
    test_manifest_hash: str
    vectors: tuple[RequestVector, ...]


def _parse_vector(raw: Any) -> RequestVector:
    if type(raw) is not dict:
        raise ChildRequestError("each vector must be a JSON object")
    if tuple(sorted(raw)) != _VECTOR_KEYS:
        raise ChildRequestError("vector fields are not the exact frozen set")
    pair_id = raw["pair_id"]
    family_id = raw["family_id"]
    surface_id = raw["surface_id"]
    input_json = raw["input_json"]
    if pair_id not in PAIR_IDS:
        raise ChildRequestError("vector names a pair outside the frozen five")
    if family_id not in ACCEPTED_FAMILY_IDS:
        raise ChildRequestError("vector family id is not a current-family id")
    if type(surface_id) is not str:
        raise ChildRequestError("vector surface_id must be a string")
    if surface_id in PRODUCER_SURFACES[pair_id]:
        raise ChildRequestError("a producer surface can never carry a reader receipt")
    if surface_id not in READER_SURFACES[pair_id]:
        raise ChildRequestError("vector surface is not a reader surface of that pair")
    if type(input_json) is not str or not input_json:
        raise ChildRequestError("vector input_json must be a nonempty string")
    if len(input_json.encode("utf-8")) > MAX_VECTOR_INPUT_BYTES:
        raise ChildRequestError("vector input_json exceeds its bounded size")
    vector = RequestVector(
        vector_id=_require_sha256(raw["vector_id"], field="vector_id"),
        pair_id=pair_id,
        family_id=family_id,
        surface_id=surface_id,
        input_json=input_json,
    )
    if vector.vector_id != canonical_sha256(vector.identity_preimage):
        raise ChildRequestError("vector id does not match its canonical content")
    return vector


def parse_child_request(payload: bytes) -> ChildRequest:
    """Decode and fully validate the one request the child is allowed to act on."""

    if type(payload) is not bytes:
        raise ChildRequestError("the request must arrive as exact bytes")
    if not payload:
        raise ChildRequestError("the request is empty")
    if len(payload) > MAX_REQUEST_BYTES:
        raise ChildRequestError("the request exceeds its bounded size")
    try:
        decoded = strict_canonical_loads(payload)
    except CanonicalJsonError as exc:
        raise ChildRequestError("the request is not canonical JSON") from exc
    if type(decoded) is not dict:
        raise ChildRequestError("the request must be a JSON object")
    if tuple(sorted(decoded)) != _REQUEST_KEYS:
        raise ChildRequestError("request fields are not the exact frozen set")
    if type(decoded["schema_version"]) is not int or decoded["schema_version"] != 1:
        raise ChildRequestError("unsupported request schema version")
    raw_vectors = decoded["vectors"]
    if type(raw_vectors) is not list or not raw_vectors:
        raise ChildRequestError("the request must carry a nonempty vector array")
    vectors = tuple(_parse_vector(raw) for raw in raw_vectors)
    identifiers = [vector.vector_id for vector in vectors]
    if identifiers != sorted(set(identifiers)):
        raise ChildRequestError("vectors must be sorted by vector_id and duplicate-free")
    return ChildRequest(
        run_id=_require_sha256(decoded["run_id"], field="run_id"),
        test_manifest_hash=_require_sha256(
            decoded["test_manifest_hash"],
            field="test_manifest_hash",
        ),
        vectors=vectors,
    )


def build_child_response(request: ChildRequest, results: dict[str, Any]) -> bytes:
    """Build the one canonical response: exactly one result per requested vector."""

    if type(results) is not dict:
        raise ChildRequestError("the result map must be a dict")
    if tuple(sorted(results)) != tuple(sorted(vector.vector_id for vector in request.vectors)):
        raise ChildRequestError("the child produced a result set the request did not ask for")
    rows: list[dict[str, Any]] = []
    for vector in request.vectors:
        canonical_result_json = canonical_json_bytes(results[vector.vector_id]).decode("utf-8")
        raw = canonical_result_json.encode("utf-8")
        if len(raw) > MAX_CANONICAL_RESULT_BYTES:
            raise ChildRequestError("a canonical result exceeds its bounded size")
        rows.append(
            {
                "canonical_result_json": canonical_result_json,
                "canonical_result_sha256": sha256_hex(raw),
                "family_id": vector.family_id,
                "pair_id": vector.pair_id,
                "surface_id": vector.surface_id,
                "vector_id": vector.vector_id,
            }
        )
    rows.sort(
        key=lambda row: (
            row["pair_id"],
            row["family_id"],
            row["surface_id"],
            row["vector_id"],
        )
    )
    body: dict[str, Any] = {
        "schema_version": 1,
        "run_id": request.run_id,
        "test_manifest_hash": request.test_manifest_hash,
        "vector_results": rows,
    }
    body["result_hash"] = canonical_sha256(body)
    payload = canonical_json_bytes(body)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ChildRequestError("the response exceeds its bounded size")
    return payload


__all__ = [
    "ACCEPTED_FAMILY_IDS",
    "MAX_CANONICAL_RESULT_BYTES",
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "MAX_VECTOR_INPUT_BYTES",
    "PAIR_IDS",
    "PRODUCER_SURFACES",
    "READER_SURFACES",
    "ChildRequest",
    "ChildRequestError",
    "RequestVector",
    "build_child_response",
    "parse_child_request",
]
