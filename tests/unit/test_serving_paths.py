from __future__ import annotations

from pathlib import Path

import pytest

from rquant.runtime_deployment_profile import LINUX_PRODUCTION_RUNTIME_ROOT
from rquant.serving_paths import (
    DEFAULT_SERVING_ROOT,
    SERVING_ROOT_ENV_VAR,
    serving_root_from_env,
)

_DEPLOY_ROOT = Path("/home/lighthouse/rquant")


def test_default_serving_root_is_the_publisher_directory_under_the_deploy_root() -> None:
    """The page-side default must name the directory the publisher actually writes."""

    assert LINUX_PRODUCTION_RUNTIME_ROOT / "serving" == _DEPLOY_ROOT / DEFAULT_SERVING_ROOT


def test_environment_variable_name_is_the_one_the_units_set() -> None:
    assert SERVING_ROOT_ENV_VAR == "RQUANT_SERVING_ROOT"


def test_explicit_environment_value_wins_over_the_default() -> None:
    resolved = serving_root_from_env({SERVING_ROOT_ENV_VAR: "/srv/generation-root"})

    assert resolved == "/srv/generation-root"


def test_unset_variable_falls_back_to_the_shared_default() -> None:
    assert serving_root_from_env({}) == DEFAULT_SERVING_ROOT


def test_blank_variable_is_treated_as_unset_rather_than_as_the_process_directory() -> None:
    """``Environment=RQUANT_SERVING_ROOT=`` in a unit must not resolve to ``Path('')``."""

    assert serving_root_from_env({SERVING_ROOT_ENV_VAR: ""}) == DEFAULT_SERVING_ROOT
    assert serving_root_from_env({SERVING_ROOT_ENV_VAR: "   "}) == DEFAULT_SERVING_ROOT


def test_process_environment_is_read_when_no_mapping_is_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SERVING_ROOT_ENV_VAR, "/srv/from-process-env")
    assert serving_root_from_env() == "/srv/from-process-env"

    monkeypatch.delenv(SERVING_ROOT_ENV_VAR, raising=False)
    assert serving_root_from_env() == DEFAULT_SERVING_ROOT
