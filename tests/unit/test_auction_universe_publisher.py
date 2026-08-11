from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from rquant.auction_universe_authority import load_auction_universe_authority
from rquant.auction_universe_publisher import (
    AuctionUniversePublicationError,
    publish_auction_universe_authority,
)

COMMIT = "a" * 40
TRADE_DATE = date(2026, 7, 31)
AVAILABLE_AT = datetime(2026, 7, 30, 9, 0, tzinfo=UTC)


def _publish(root: Path):
    return publish_auction_universe_authority(
        root,
        effective_trade_date=TRADE_DATE,
        reference_trade_date=date(2026, 7, 30),
        available_at=AVAILABLE_AT,
        producer_commit=COMMIT,
        source_snapshot_id="b" * 64,
        codes=("600000.SH", "000001.SZ"),
    )


def test_publisher_seals_content_addressed_generation_and_atomic_current(tmp_path: Path) -> None:
    root = (tmp_path / "auction-universe").resolve()

    receipt = _publish(root)

    assert receipt.published is True
    assert receipt.generation_path == root / "generations" / f"{receipt.content_sha256}.json"
    assert receipt.generation_path.read_bytes() == (root / "current.json").read_bytes()
    assert receipt.generation_path.stat().st_mode & 0o777 == 0o600
    assert (root / "current.json").stat().st_mode & 0o777 == 0o600
    loaded = load_auction_universe_authority(
        root / "current.json",
        expected_commit=COMMIT,
        required_trade_date=TRADE_DATE,
        as_of=AVAILABLE_AT,
    )
    assert loaded.source_snapshot_id == "b" * 64


def test_identical_publication_is_idempotent(tmp_path: Path) -> None:
    root = (tmp_path / "auction-universe").resolve()

    first = _publish(root)
    second = _publish(root)

    assert first.content_sha256 == second.content_sha256
    assert second.published is False
    assert list((root / "generations").glob("*.json")) == [first.generation_path]


def test_publisher_rejects_relative_symlink_and_hardlinked_current(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absolute"):
        _publish(Path("relative"))

    real = (tmp_path / "real").resolve()
    real.mkdir(mode=0o700)
    symlink = tmp_path / "linked"
    symlink.symlink_to(real, target_is_directory=True)
    with pytest.raises(AuctionUniversePublicationError, match="symlink|unsafe"):
        _publish(symlink)

    root = (tmp_path / "auction-universe").resolve()
    _publish(root)
    linked = root / "linked-current.json"
    os.link(root / "current.json", linked)
    with pytest.raises(AuctionUniversePublicationError, match="hard link"):
        _publish(root)
