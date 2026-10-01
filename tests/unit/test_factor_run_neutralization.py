"""Public modes carry no source claims and retain the existing none payload."""

import json

import pytest
from pydantic import ValidationError

from rquant.factor import run_request as public
from rquant.factor.member_archive import _bytes
from rquant.runtime_contracts import canonical_sha256


_PARAMETERS = {
    "factor_id": "test",
    "expected_head": {"version": 1, "content_sha256": "a" * 64},
    "selection": "all",
    "start_date": "2024-01-02",
    "end_date": "2024-01-03",
    "holding_sessions": 1,
    "group_count": 5,
    "ic_method": "rank",
    "neutralization": "none",
}


def test_public_none_payload_and_hash_stay_canonical() -> None:
    from rquant.strict_json import canonical_json_bytes

    payload = canonical_json_bytes(_PARAMETERS)
    parsed = public.FactorRunParameters.model_validate_json(payload)
    assert _bytes(parsed) == payload
    assert (
        canonical_sha256(parsed)
        == "5a3080c3c2d96caa3b7ba7c60152aa03087c172497f266076bba23d4aeeb91ac"
    )
    old_availability = {
        "enabled": True,
        "reason": None,
        "pools": [],
        "start_date": None,
        "end_date": None,
    }
    assert _bytes(
        public.FactorRunAvailability.model_validate_json(json.dumps(old_availability))
    ) == canonical_json_bytes(old_availability)


@pytest.mark.parametrize("mode", ["industry", "industry_size"])
def test_public_neutralization_modes_are_typed(mode: str) -> None:
    parsed = public.FactorRunParameters.model_validate_json(
        json.dumps({**_PARAMETERS, "neutralization": mode})
    )
    assert parsed.neutralization == mode
    for extra in ({"context": {}}, {"actor_id": "alice"}, {"source_path": "/private/tmp/source"}):
        with pytest.raises(ValidationError):
            public.FactorRunParameters.model_validate_json(json.dumps({**_PARAMETERS, **extra}))


def test_public_availability_has_finite_mode_options() -> None:
    option = public.FactorRunNeutralizationOption(
        neutralization="industry_size", label="行业 + 市值", available=False, reason="缺少市值来源"
    )
    availability = public.FactorRunAvailability(enabled=True, pools=(), neutralizations=(option,))
    assert availability.neutralizations == (option,)
    with pytest.raises(ValidationError):
        public.FactorRunParameters.model_validate_json(
            json.dumps({**_PARAMETERS, "neutralization": "size"})
        )
