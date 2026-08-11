from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.auction_universe_authority import (
    AuctionUniverseAuthority,
    AuctionUniverseAuthorityIntegrityError,
    load_auction_universe_authority,
)

COMMIT = "a" * 40
SOURCE_SNAPSHOT_ID = "b" * 64
TRADE_DATE = date(2026, 7, 31)
AVAILABLE_AT = datetime(2026, 7, 30, 9, 0, tzinfo=UTC)


def _write(path: Path, authority: AuctionUniverseAuthority) -> Path:
    path.write_text(
        json.dumps(
            authority.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    path.chmod(0o600)
    return path


def _authority() -> AuctionUniverseAuthority:
    return AuctionUniverseAuthority.create(
        effective_trade_date=TRADE_DATE,
        reference_trade_date=date(2026, 7, 30),
        available_at=AVAILABLE_AT,
        producer_commit=COMMIT,
        source_snapshot_id=SOURCE_SNAPSHOT_ID,
        codes=("600000.SH", "000001.SZ"),
    )


def test_authority_is_canonical_content_addressed_and_point_in_time(tmp_path: Path) -> None:
    path = _write(tmp_path / "auction-universe.json", _authority())

    loaded = load_auction_universe_authority(
        path,
        expected_commit=COMMIT,
        required_trade_date=TRADE_DATE,
        as_of=AVAILABLE_AT,
    )

    assert loaded.codes == ("000001.SZ", "600000.SH")
    assert loaded.content_sha256 == _authority().content_sha256


def test_authority_rejects_future_visibility_wrong_day_and_wrong_commit(tmp_path: Path) -> None:
    path = _write(tmp_path / "auction-universe.json", _authority())

    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="not visible"):
        load_auction_universe_authority(
            path,
            expected_commit=COMMIT,
            required_trade_date=TRADE_DATE,
            as_of=AVAILABLE_AT - timedelta(microseconds=1),
        )
    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="trade date"):
        load_auction_universe_authority(
            path,
            expected_commit=COMMIT,
            required_trade_date=date(2026, 8, 3),
            as_of=AVAILABLE_AT,
        )
    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="producer_commit"):
        load_auction_universe_authority(
            path,
            expected_commit="b" * 40,
            required_trade_date=TRADE_DATE,
            as_of=AVAILABLE_AT,
        )


def test_authority_rejects_tamper_noncanonical_and_unsafe_paths(tmp_path: Path) -> None:
    path = _write(tmp_path / "auction-universe.json", _authority())
    document = json.loads(path.read_text())
    document["codes"] = ["000002.SZ", "600000.SH"]
    path.write_text(json.dumps(document, separators=(",", ":"), sort_keys=True))

    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="invalid"):
        load_auction_universe_authority(
            path,
            expected_commit=COMMIT,
            required_trade_date=TRADE_DATE,
            as_of=AVAILABLE_AT,
        )

    path = _write(tmp_path / "auction-universe.json", _authority())
    path.write_text(json.dumps(json.loads(path.read_text()), indent=2))
    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="canonical"):
        load_auction_universe_authority(
            path,
            expected_commit=COMMIT,
            required_trade_date=TRADE_DATE,
            as_of=AVAILABLE_AT,
        )

    path = _write(tmp_path / "auction-universe.json", _authority())
    hardlink = tmp_path / "hardlink.json"
    os.link(path, hardlink)
    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="hard link"):
        load_auction_universe_authority(
            path,
            expected_commit=COMMIT,
            required_trade_date=TRADE_DATE,
            as_of=AVAILABLE_AT,
        )

    path.unlink()
    hardlink.unlink()
    target = _write(tmp_path / "target.json", _authority())
    symlink = tmp_path / "auction-universe.json"
    symlink.symlink_to(target)
    with pytest.raises(AuctionUniverseAuthorityIntegrityError, match="symlink|unsafe"):
        load_auction_universe_authority(
            symlink,
            expected_commit=COMMIT,
            required_trade_date=TRADE_DATE,
            as_of=AVAILABLE_AT,
        )


def test_authority_rejects_invalid_semantics() -> None:
    with pytest.raises(ValueError, match="reference_trade_date"):
        AuctionUniverseAuthority.create(
            effective_trade_date=TRADE_DATE,
            reference_trade_date=date(2026, 8, 1),
            available_at=AVAILABLE_AT,
            producer_commit=COMMIT,
            source_snapshot_id=SOURCE_SNAPSHOT_ID,
            codes=("600000.SH",),
        )
    with pytest.raises(ValueError, match="codes"):
        AuctionUniverseAuthority.create(
            effective_trade_date=TRADE_DATE,
            reference_trade_date=date(2026, 7, 30),
            available_at=AVAILABLE_AT,
            producer_commit=COMMIT,
            source_snapshot_id=SOURCE_SNAPSHOT_ID,
            codes=(),
        )
