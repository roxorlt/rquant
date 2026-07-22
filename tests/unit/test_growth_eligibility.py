from __future__ import annotations

from datetime import date, timedelta

import pytest

import rquant.growth_eligibility as growth_eligibility
from rquant.storage.duckdb import DuckDBStore


def test_growth_structure_classification_uses_stable_bounded_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first = date(2026, 4, 1)
    targets = [first + timedelta(days=index) for index in range(5)]
    pairs = {
        target: target - timedelta(days=1)
        for target in reversed(targets)
    }
    calls: list[tuple[date, ...]] = []

    def classify_batch(
        _store: DuckDBStore,
        batch_pairs: dict[date, date],
    ) -> tuple[object, ...]:
        calls.append(tuple(batch_pairs))
        return ()

    monkeypatch.setattr(
        growth_eligibility,
        "_classify_growth_opening_structure_batch",
        classify_batch,
    )
    with DuckDBStore(tmp_path / "growth-structure-batches.duckdb") as store:
        result = growth_eligibility.classify_growth_opening_structure(
            store,
            pairs,
            batch_size=2,
        )

    assert result == ()
    assert calls == [
        tuple(targets[:2]),
        tuple(targets[2:4]),
        tuple(targets[4:]),
    ]


def test_growth_structure_classification_rejects_nonpositive_batch_size(
    tmp_path,
) -> None:
    with (
        DuckDBStore(tmp_path / "growth-structure-invalid-batch.duckdb") as store,
        pytest.raises(ValueError, match="batch_size must be positive"),
    ):
        growth_eligibility.classify_growth_opening_structure(
            store,
            {date(2026, 4, 1): date(2026, 3, 31)},
            batch_size=0,
        )
