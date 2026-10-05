from __future__ import annotations

from pathlib import Path

import pytest

from rquant.experiment_platform import ExperimentPlatformStore
from rquant.experiment_platform_projection import PRIVATE_TABLES
from rquant.experiment_registry import ExperimentRegistryError, ExperimentRegistryReadonlyReader
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
)
from tests.unit.test_experiment_platform import NOW, registry


@pytest.mark.parametrize("failure", ("unknown-version", "missing-preparation", "missing-quota"))
def test_exp26_incomplete_private_schema_rejects_legacy_publication(
    tmp_path: Path, failure: str
) -> None:
    store = ExperimentPlatformStore(registry(tmp_path), activate_private_schema=True)
    with store.transaction() as connection:
        if failure == "unknown-version":
            connection.execute(
                "UPDATE experiment_platform_metadata SET value='2' WHERE key='version'"
            )
        else:
            table = {
                "missing-preparation": "experiment_prepared_child",
                "missing-quota": "experiment_outer_grant",
            }[failure]
            connection.execute("DROP TABLE " + table)
    reader = ExperimentRegistryReadonlyReader(
        store.registry.path, managed_trust_root=store.registry.path.parent
    )
    with pytest.raises(ExperimentRegistryError):
        reader.read_legacy_shared_serving_snapshot(observed_at=NOW)
    with pytest.raises(ExperimentRegistryError):
        reader.read_legacy_shared_promotion_decisions(observed_at=NOW)


@pytest.mark.parametrize("missing", PRIVATE_TABLES)
def test_exp26_private_projection_collection_cannot_publish_partial(missing: str) -> None:
    payloads = tuple(
        ServingProjectionInput.bind(
            ServingProjectionPayload(table_name=name, available_at=NOW, rows=()),
            owner_dataset_id="promotions",
            owner_generation_id="a" * 64,
        )
        for name in PRIVATE_TABLES
        if name != missing
    )
    with pytest.raises(ValueError, match="private"):
        ServingReadModelInput(observed_at=NOW, projections=payloads)
