"""R207: the document that names the legacy generation an authority generation came from.

The parser is what stands between a role and a forged claim about which legacy deployment
it belongs to, so every refusal it can make is exercised here rather than only through
`runtime_service_main`.
"""

from __future__ import annotations

import pytest

from rquant.runtime_legacy_generation_binding import (
    GENERATION_LEGACY_BINDING_NAME,
    LEGACY_BINDING_MODE_BOOTSTRAP,
    LEGACY_BINDING_MODE_LEGACY,
    LEGACY_BINDING_SCHEMA_ID,
    MAX_LEGACY_BINDING_BYTES,
    LegacyGenerationBindingError,
    legacy_generation_binding_bytes,
    parse_legacy_generation_binding,
)
from rquant.strict_json import canonical_json_bytes

LEGACY = "a" * 64
ROOT = "/home/lighthouse/rquant/data/runtime"


def _document(**overrides: object) -> bytes:
    payload: dict[str, object] = {
        "schema_id": LEGACY_BINDING_SCHEMA_ID,
        "schema_version": 1,
        "mode": LEGACY_BINDING_MODE_LEGACY,
        "runtime_root": ROOT,
        "generation_id": LEGACY,
    }
    payload.update(overrides)
    for key, value in list(payload.items()):
        if value is ...:
            del payload[key]
    return canonical_json_bytes(payload, trailing_newline=True)


def test_the_document_name_is_a_generation_root_sibling_of_the_full_manifest() -> None:
    assert GENERATION_LEGACY_BINDING_NAME == "legacy-binding.json"
    assert "/" not in GENERATION_LEGACY_BINDING_NAME


def test_a_legacy_document_round_trips() -> None:
    payload = legacy_generation_binding_bytes(
        mode=LEGACY_BINDING_MODE_LEGACY, runtime_root=ROOT, generation_id=LEGACY
    )
    binding = parse_legacy_generation_binding(payload)
    assert binding.is_legacy
    assert binding.runtime_root == ROOT
    assert binding.generation_id == LEGACY
    assert payload.endswith(b"\n")
    assert canonical_json_bytes(
        {
            "generation_id": LEGACY,
            "mode": "legacy",
            "runtime_root": ROOT,
            "schema_id": LEGACY_BINDING_SCHEMA_ID,
            "schema_version": 1,
        },
        trailing_newline=True,
    ) == payload


def test_a_bootstrap_document_round_trips_and_claims_nothing() -> None:
    payload = legacy_generation_binding_bytes(
        mode=LEGACY_BINDING_MODE_BOOTSTRAP, runtime_root=None, generation_id=None
    )
    binding = parse_legacy_generation_binding(payload)
    assert not binding.is_legacy
    assert binding.runtime_root is None
    assert binding.generation_id is None


def test_the_writer_refuses_a_document_its_own_parser_would_reject() -> None:
    with pytest.raises(LegacyGenerationBindingError, match="mode is unknown"):
        legacy_generation_binding_bytes(mode="route-a", runtime_root=ROOT, generation_id=LEGACY)
    with pytest.raises(LegacyGenerationBindingError, match="cannot name a legacy deployment"):
        legacy_generation_binding_bytes(
            mode=LEGACY_BINDING_MODE_BOOTSTRAP, runtime_root=ROOT, generation_id=LEGACY
        )
    with pytest.raises(LegacyGenerationBindingError, match="64-hex deployment hash"):
        legacy_generation_binding_bytes(
            mode=LEGACY_BINDING_MODE_LEGACY, runtime_root=ROOT, generation_id="current"
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"{", "not strict JSON"),
        (b'{"schema_id": "x", "schema_id": "y"}', "not strict JSON"),
        (b"[]\n", "schema is invalid"),
        (_document(extra=1), "schema is invalid"),
        (_document(runtime_root=...), "schema is invalid"),
        (_document(schema_id="rquant-legacy-generation-binding/v2"), "schema id is unknown"),
        (_document(schema_version=2), "schema version is unknown"),
        (_document(mode="unknown"), "mode is unknown"),
        (_document(mode=1), "mode is unknown"),
        (_document(runtime_root="data/runtime"), "not one absolute path"),
        (_document(runtime_root=None), "not one absolute path"),
        (_document(runtime_root="/home/../etc"), "not canonical"),
        (_document(generation_id=LEGACY.upper()), "64-hex deployment hash"),
        (_document(generation_id="a" * 63), "64-hex deployment hash"),
        (_document(generation_id=None), "64-hex deployment hash"),
        (
            _document(mode=LEGACY_BINDING_MODE_BOOTSTRAP),
            "cannot name a legacy deployment",
        ),
    ],
)
def test_every_malformed_document_is_named_rather_than_guessed(
    payload: bytes, message: str
) -> None:
    with pytest.raises(LegacyGenerationBindingError, match=message):
        parse_legacy_generation_binding(payload)


def test_a_non_canonical_encoding_of_a_valid_document_is_refused() -> None:
    """Byte equality, not value equality: the file is hash-bound into the generation id."""

    spaced = _document().replace(b",", b", ")
    assert spaced != _document()
    with pytest.raises(LegacyGenerationBindingError, match="not canonical"):
        parse_legacy_generation_binding(spaced)


def test_an_oversized_document_is_refused_before_it_is_parsed() -> None:
    padding = b" " * MAX_LEGACY_BINDING_BYTES
    with pytest.raises(LegacyGenerationBindingError, match="exceeds its size bound"):
        parse_legacy_generation_binding(_document() + padding)


def test_the_module_imports_without_constructing_settings() -> None:
    """It is read inside the wrapper's role child, which has no environment to speak of."""

    import ast
    from pathlib import Path

    source = Path(__file__).resolve().parents[2] / "src" / "rquant"
    tree = ast.parse((source / "runtime_legacy_generation_binding.py").read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert {name for name in imported if name.startswith("rquant")} == {"rquant.strict_json"}
