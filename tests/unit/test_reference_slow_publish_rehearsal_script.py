"""`scripts/reference_slow_publish_rehearsal.py`: publish a copied batch, write only the copy."""

from __future__ import annotations

import importlib.util
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

import rquant.reference_slow_runtime as reference_slow_runtime
from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.live_spool import (
    LiveBatchSpool,
    ReferenceSourceBatchSigner,
    ReferenceSourceBatchVerifier,
)
from rquant.reference_slow_runtime import capture_reference_slow_batch
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_reference_slow_publish_window import (
    COMMIT,
    TARGET_DATE,
    _calendar,
    _reference_publication_credential,  # noqa: F401 - autouse credential fixture
    _snapshot,
    at,
)

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "reference_slow_publish_rehearsal.py"
CODES = ("300001.SZ", "600000.SH")


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("reference_slow_publish_rehearsal", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _tree(root: Path) -> dict[str, tuple[int, int]]:
    return {
        str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _runtime_root(tmp_path: Path) -> Path:
    """The production layout the script reads: one sealed batch, its calendar, auction-match.

    The batch is sealed the way v0.33.20 sealed 2026-09-24's -- visible five seconds after it
    was prepared -- because that is what the host's batch 0 looks like.
    """

    runtime = tmp_path / "runtime"
    key = tmp_path / "keys" / "source"
    key.parent.mkdir(mode=0o700, parents=True)
    import subprocess

    subprocess.run(("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)), check=True)
    key.chmod(0o600)
    spool = LiveBatchSpool(
        runtime / "live" / "reference-slow",
        source_signer=ReferenceSourceBatchSigner(key_id="k", private_key=key.read_text("ascii")),
        source_verifier=ReferenceSourceBatchVerifier(
            key_id="k", public_key=key.with_suffix(".pub").read_text("ascii").strip()
        ),
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(reference_slow_runtime, "_SOURCE_VISIBILITY_GUARD", timedelta(seconds=5))
        capture_reference_slow_batch(
            spool=spool,
            calendar=_calendar(),
            observed_at=at(9, 20, 27),
            producer_commit=COMMIT,
            producer_version="reference-slow-source-v1",
            snapshot_loader=lambda: _snapshot(captured_at=at(9, 20, 31), codes=CODES),
            completion_clock=lambda: at(9, 20, 58),
        )
    calendar = _calendar()
    generations = runtime / "authorities" / "market-calendar" / "generations"
    generations.mkdir(mode=0o700, parents=True)
    calendar_path = generations / f"{calendar.content_sha256}.json"
    calendar_path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
    calendar_path.chmod(0o600)
    frame = pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_date": TARGET_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
            for code in CODES
        ]
    )
    AuctionMatchGateway(
        spool=LiveBatchSpool(runtime / "live" / "auction-match"),
        fetcher=lambda _trade_date: frame,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-v1",
            producer_commit=COMMIT,
            min_coverage_ratio=1.0,
        ),
    ).capture_once(trade_date=TARGET_DATE, received_at=at(9, 29), expected_codes=CODES)
    return runtime


def _run(
    tmp_path: Path, runtime: Path, *extra: str, capsys: pytest.CaptureFixture[str]
) -> tuple[int, str]:
    script = _load_script()
    code = script.main(
        [
            "--runtime-root",
            str(runtime),
            "--rehearsal-root",
            str(tmp_path / "rehearsal"),
            "--round-interval-seconds",
            "0.2",
            *extra,
        ]
    )
    return code, capsys.readouterr().out


def test_the_rehearsal_publishes_a_copied_batch_visible_at_0925_and_writes_only_its_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = _runtime_root(tmp_path)
    before = _tree(runtime)

    #: the copied batch was prepared at 09:20:58 and re-seals visible 30 s later (09:21:28);
    #: starting at 09-24's first attempt also shows the "nothing visible yet" round
    code, out = _run(
        tmp_path,
        runtime,
        "--publisher-start",
        "09:21:26",
        "--slow-commit-seconds",
        "1",
        capsys=capsys,
    )

    assert code == 0, out
    assert out.rstrip().endswith("REHEARSAL OK")
    assert "not yet: current reference generation is missing" in out
    assert '"manifest_published_at": "2026-07-31 01:25:00+00:00"' in out
    assert '"records_written": 12' in out
    assert '"refused": "reference generation is future evidence"' in out
    assert '"facts": 2' in out
    assert _tree(runtime) == before
    (root,) = (tmp_path / "rehearsal").iterdir()
    assert (root / "authorities" / "reference-slow" / "reference.sqlite3").is_file()


def test_a_commit_slowed_past_0925_is_refused_and_rolled_back(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = _runtime_root(tmp_path)
    before = _tree(runtime)

    code, out = _run(
        tmp_path,
        runtime,
        "--publisher-start",
        "09:24:57",
        "--slow-commit-seconds",
        "4",
        capsys=capsys,
    )

    assert code == 1, out
    assert (
        "REHEARSAL REFUSED: publisher refused: reference slow publisher completed after 09:25"
        in out
    )
    assert "registry rolled back (no current generation): true" in out
    assert _tree(runtime) == before


def test_the_rehearsal_refuses_a_root_inside_the_runtime_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = _runtime_root(tmp_path)
    script = _load_script()

    code = script.main(
        ["--runtime-root", str(runtime), "--rehearsal-root", str(runtime / "var" / "rehearsal")]
    )

    assert code == 1
    assert "must not overlap the runtime root" in capsys.readouterr().out
    assert not (runtime / "var").exists()
