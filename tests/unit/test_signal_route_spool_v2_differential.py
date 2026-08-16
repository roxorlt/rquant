"""Frozen base-commit compatibility corpus for the untouched v2 spool parser."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import rquant.signal_route_spool as spool

_MANIFEST_PATH = (
    Path(__file__).parents[1] / "fixtures" / "signal_route_spool_v2_differential" / "manifest.json"
)
_MANIFEST_SHA256 = "f23036927524fe0903b9de4d5fa741666d7ab95dfab7b25016a44fccd1d2fa92"


def _manifest() -> dict[str, Any]:
    payload = _MANIFEST_PATH.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == _MANIFEST_SHA256
    value = json.loads(payload)
    assert value["format"] == "rquant-signal-route-spool-v2-differential/v1"
    assert value["base_commit"] == "45d0b57c4c5cbab1700fa5e3c386c6756892a7d6"
    return value


@pytest.mark.parametrize("parser_name", ("legacy", "dispatcher"))
def test_frozen_v2_corpus_matches_literal_base_commit_behavior(parser_name: str) -> None:
    parser: Callable[..., spool.SignalRouteSpoolRecord]
    parser = spool._parse_record if parser_name == "legacy" else spool._parse_r07_record

    for case in _manifest()["cases"]:
        raw = base64.b64decode(case["raw_base64"], validate=True)
        sequence = case["sequence"]
        if not case["accepted"]:
            with pytest.raises(spool.SignalRouteSpoolIntegrityError) as captured:
                parser(raw, sequence=sequence)
            assert type(captured.value).__name__ == case["error_type"], case["id"]
            assert str(captured.value) == case["error_message"], case["id"]
            assert str(sequence) in str(captured.value), case["id"]
            continue

        parsed = parser(raw, sequence=sequence)
        expected_model = json.loads(base64.b64decode(case["model_json_base64"], validate=True))
        canonical = base64.b64decode(case["canonical_bytes_base64"], validate=True)
        assert type(parsed) is spool.SignalRouteSpoolRecord, case["id"]
        assert parsed.model_dump(mode="json") == expected_model, case["id"]
        assert spool._canonical_bytes(parsed) == canonical, case["id"]
        assert hashlib.sha256(canonical).hexdigest() == case["canonical_sha256"], case["id"]
        assert parsed.payload_hash == case["payload_hash"], case["id"]
        assert parsed.record_hash == case["record_hash"], case["id"]
        assert parsed.record.payload_hash == case["stored_payload_hash"], case["id"]
        assert parsed.record.signal_id == case["signal_id"], case["id"]


def test_frozen_v2_manifest_covers_the_approved_compatibility_matrix() -> None:
    cases = _manifest()["cases"]
    covered = {tag for case in cases for tag in case["coverage"]}
    assert covered == {
        "boolean",
        "chain_hash",
        "coercion",
        "corrupt_hash",
        "corrupt_payload",
        "corrupt_receipt",
        "datetime_spelling",
        "duplicate_key",
        "extra_field",
        "filename_mismatch",
        "float",
        "key_order",
        "missing_field",
        "newline",
        "nonfinite",
        "schema",
        "sequence",
        "string_numeric",
        "truncation",
        "unicode_escaped",
        "unicode_literal",
        "valid",
        "whitespace",
    }
    assert sum(case["accepted"] for case in cases) >= 2
    assert any(not case["accepted"] for case in cases)
    unicode_cases = [case for case in cases if "unicode_escaped" in case["coverage"]]
    assert unicode_cases
    for case in unicode_cases:
        canonical = base64.b64decode(case["canonical_bytes_base64"], validate=True)
        canonical.decode("ascii")
        assert b"\\u" in canonical


def test_frozen_v2_filename_and_chain_failures_keep_exact_sequence_diagnostics() -> None:
    cases = [case for case in _manifest()["cases"] if "fixture_error_type" in case]
    assert {case["id"] for case in cases} == {
        "chain-previous-hash-mismatch",
        "filename-sequence-mismatch",
    }
    for case in cases:
        raw = base64.b64decode(case["raw_base64"], validate=True)
        with pytest.raises(spool.SignalRouteSpoolIntegrityError) as captured:
            spool.verify_current_signal_route_spool_fixture(
                (raw,),
                first_sequence=case["sequence"],
            )
        assert type(captured.value).__name__ == case["fixture_error_type"], case["id"]
        assert str(captured.value) == case["fixture_error_message"], case["id"]
