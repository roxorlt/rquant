"""Explicit version two requests bind actual member files without changing v1 bytes."""

from datetime import timedelta
from pathlib import Path

import pytest

from rquant.factor.job_spec import _definition_sha256
from tests.unit.test_factor_member_stream import _archive
from tests.unit.test_factor_stream_adapter import _pools, _prepared


def _spec(tmp_path: Path, request: object, root: Path, reference: object) -> object:
    from rquant.factor.stream_job_spec import FactorStreamJobSpec

    return FactorStreamJobSpec(
        code_revision="a" * 40,
        adapter_request=request,
        member_archive=reference,
        definition_content_sha256=_definition_sha256(request.formula.definition),
        deadline=request.formula.as_of + timedelta(days=1),
    )


def test_v2_spec_binds_manifest_and_definition(tmp_path: Path) -> None:
    from rquant.factor.stream_job_spec import decode_factor_job_spec

    with _prepared(tmp_path) as (_, _, request):
        root, reference, request = _archive(tmp_path, request, _pools(request))
        spec = _spec(tmp_path, request, root, reference)
        assert spec.schema_version == 2
        assert decode_factor_job_spec(spec.model_dump(mode="json")) == spec
        assert "root" not in spec.model_dump_json()
        with pytest.raises(ValueError, match="definition"):
            spec.model_copy(update={"definition_content_sha256": "0" * 64})


@pytest.mark.parametrize("version", [None, 0, 3, True])
def test_unknown_or_missing_spec_version_is_rejected(version: object) -> None:
    from rquant.factor.stream_job_spec import decode_factor_job_spec

    with pytest.raises(ValueError, match="version"):
        decode_factor_job_spec({"schema_version": version})


def test_original_v1_canonical_bytes_and_legacy_completion_decoder_remain_exact(
    tmp_path: Path,
) -> None:
    from rquant.factor.stream_job_runner import decode_factor_completion_json
    from rquant.factor.stream_job_spec import decode_factor_job_spec_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_job_ledger import _sealed

    spec, _, completion = _sealed(tmp_path)
    spec_bytes = canonical_json_bytes(spec.model_dump(mode="json", round_trip=True))
    assert spec.spec_sha256 == "3cc03cd3b8940db3431e5e1e4d53725602642d812e63766d4b25bcebc3dc233e"
    assert (
        canonical_json_bytes(
            decode_factor_job_spec_json(spec_bytes.decode()).model_dump(
                mode="json", round_trip=True
            )
        )
        == spec_bytes
    )
    legacy = completion.model_dump(
        mode="json",
        exclude={
            "display_artifact_sha256",
            "display_artifact_filename",
            "display_artifact_byte_count",
        },
    )
    decoded = decode_factor_completion_json(canonical_json_bytes(legacy).decode())
    assert decoded.display_status == "display_unavailable"
    assert canonical_json_bytes(
        decoded.model_dump(
            mode="json",
            exclude={
                "display_artifact_sha256",
                "display_artifact_filename",
                "display_artifact_byte_count",
            },
        )
    ) == canonical_json_bytes(legacy)


def test_mixed_or_missing_v2_completion_discriminator_cannot_fall_back(tmp_path: Path) -> None:
    from rquant.factor.stream_job_runner import decode_factor_completion_json
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_stream_job_authority import _sealed

    with _sealed(tmp_path) as (_, _, _, completion, *_):
        payload = completion.model_dump(mode="json")
        for version in (None, 1, 3):
            mixed = dict(payload)
            if version is None:
                del mixed["schema_version"]
            else:
                mixed["schema_version"] = version
            with pytest.raises(ValueError):
                decode_factor_completion_json(canonical_json_bytes(mixed).decode())
