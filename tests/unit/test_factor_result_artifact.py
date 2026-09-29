"""Durable, content-addressed factor research artifacts with a private root."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import stat
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.definition import build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.historical_adapter import (
    HistoricalFactorResearch,
    HistoricalFactorSourceReceipt,
    HistoricalPanelDate,
    HistoricalReturnWindow,
)
from rquant.factor.result import (
    FactorForwardReturn,
    FactorResearchRequest,
    assemble_factor_research_result,
)
from rquant.factor.time_series import DecisionTime, FactorTimeSeriesInput, FeatureObservation
from rquant.strict_json import canonical_json_bytes

_TZ = timezone(timedelta(hours=8))
_DAY = date(2026, 7, 16)
_DECISION = datetime(2026, 7, 16, 9, 25, tzinfo=_TZ)
_END = datetime(2026, 7, 16, 15, tzinfo=_TZ)
_AS_OF = datetime(2026, 7, 17, 9, 25, tzinfo=_TZ)
_STOCKS = ("000001.SZ", "000002.SZ", "600000.SH")
_CODE_REVISION = "a" * 40


def _digest(payload: object) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _research() -> HistoricalFactorResearch:
    receipt_fields = {
        "snapshot_id": "synthetic-snapshot",
        "binding_hash": "b" * 64,
        "snapshot_as_of_time": _AS_OF,
        "source_mode": "historical_retrospective",
        "source_read_boundary": "single_snapshot_transaction",
        "visibility_basis": "retrospective_adapter_assumption",
        "pool_basis": "explicit_fixed_list",
        "pool_sha256": _digest(_STOCKS),
        "stock_codes": _STOCKS,
        "allowed_columns": ("daily_bar.close", "daily_bar.open", "adj_factor.adj_factor"),
        "feature_columns": ("close",),
        "query_start_date": _DAY - timedelta(days=1),
        "query_end_date": _DAY,
        "calculation_days": (_DAY,),
        "evaluation_days": (_DAY,),
        "panel_dates": (
            HistoricalPanelDate(
                decision_date=_DAY,
                panel_date=_DAY - timedelta(days=1),
                first_visible_at=_DECISION,
            ),
        ),
        "return_windows": (
            HistoricalReturnWindow(
                decision_date=_DAY,
                end_date=_DAY,
                return_end_at=_END,
                expected_available_at=_AS_OF,
            ),
        ),
        "return_price_basis": "forward_adjusted",
        "return_formula": "close_end*adj_end/(open_d*adj_d)-1",
        "result_kind": "research_diagnostic",
        "missing_counts": (),
    }
    unsigned = HistoricalFactorSourceReceipt.model_construct(
        **receipt_fields, source_sha256="0" * 64
    )
    receipt = HistoricalFactorSourceReceipt(
        **receipt_fields,
        source_sha256=_digest(unsigned.model_dump(mode="json", exclude={"source_sha256"})),
    )
    definition = build_factor_definition(
        factor_id="price_factor",
        name_zh="价格因子",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=_DAY - timedelta(days=1),
        expression="close",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    factor_input = FactorTimeSeriesInput(
        definition=definition,
        universe=_STOCKS,
        trading_days=(_DAY,),
        decision_times=(DecisionTime(trade_date=_DAY, decision_at=_DECISION),),
        observations=tuple(
            FeatureObservation(
                stock_code=stock,
                trade_date=_DAY,
                column="close",
                value=float(index + 1),
                first_visible_at=_DECISION - timedelta(minutes=1),
            )
            for index, stock in enumerate(_STOCKS)
        ),
    )
    request = FactorResearchRequest(
        factor_input=factor_input,
        evaluation_days=(_DAY,),
        forward_returns=tuple(
            FactorForwardReturn(
                stock_code=stock,
                decision_date=_DAY,
                decision_at=_DECISION,
                return_end_at=_END,
                value=(0.1, -0.1, 0.2)[index],
                missing_reason=None,
                first_available_at=_END,
            )
            for index, stock in enumerate(_STOCKS)
        ),
        as_of=_AS_OF,
        factor_source_id=receipt.source_sha256,
        return_source_id=receipt.source_sha256,
        return_price_basis="forward_adjusted",
        holding_sessions=1,
    )
    result = assemble_factor_research_result(request)
    return HistoricalFactorResearch(receipt=receipt, request=request, result=result)


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    return root


def test_round_trip_idempotent_publish_and_code_revision_identity(tmp_path: Path) -> None:
    from rquant.factor.result_artifact import (
        load_factor_research_artifact,
        publish_factor_research_artifact,
    )

    root = _root(tmp_path)
    research = _research()
    receipt = publish_factor_research_artifact(research, _CODE_REVISION, root)
    assert receipt.filename == f"factor-research-v1-{receipt.sha256}.json"
    assert receipt.byte_count == (root / receipt.filename).stat().st_size
    artifact = load_factor_research_artifact(root, receipt.sha256)
    assert artifact.schema_version == 1
    assert artifact.code_revision == _CODE_REVISION
    assert artifact.research == research
    assert artifact.content_sha256 == receipt.sha256
    assert publish_factor_research_artifact(research, _CODE_REVISION, root) == receipt
    revised = publish_factor_research_artifact(research, "b" * 40, root)
    assert revised.sha256 != receipt.sha256
    assert len(list(root.iterdir())) == 2


def test_publish_recomputes_result_and_rejects_invalid_pairs_before_file_access(
    tmp_path: Path,
) -> None:
    from rquant.factor.result_artifact import publish_factor_research_artifact

    root = _root(tmp_path)
    research = _research()
    changed = research.result.model_copy(update={"portfolio_status": "insufficient_data"})
    changed = changed.model_copy(
        update={"sha256": _digest(changed.model_dump(mode="json", exclude={"sha256"}))}
    )
    paired = HistoricalFactorResearch(
        receipt=research.receipt, request=research.request, result=changed
    )
    with pytest.raises(ValueError, match="recomputed"):
        publish_factor_research_artifact(paired, _CODE_REVISION, root)

    altered_receipt = research.receipt.model_copy(update={"source_sha256": "f" * 64})
    with pytest.raises(ValidationError, match="source receipt digest"):
        publish_factor_research_artifact(
            research.model_copy(update={"receipt": altered_receipt}), _CODE_REVISION, root
        )
    with pytest.raises(ValidationError, match="input"):
        publish_factor_research_artifact(
            research.model_copy(
                update={"result": research.result.model_copy(update={"input_sha256": "f" * 64})}
            ),
            _CODE_REVISION,
            root,
        )
    assert list(root.iterdir()) == []


def test_publish_rejects_revision_or_oversize_before_touching_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)
    research = _research()
    with pytest.raises(ValidationError, match="code revision"):
        module.publish_factor_research_artifact(research, "A" * 40, root)
    monkeypatch.setattr(module, "MAX_FACTOR_RESEARCH_ARTIFACT_BYTES", 64)
    with pytest.raises(ValueError, match="128 MiB"):
        module.publish_factor_research_artifact(research, _CODE_REVISION, root)
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("damage", ["truncated", "changed", "noncanonical", "wrong_name"])
def test_loader_rejects_damaged_or_misnamed_content(tmp_path: Path, damage: str) -> None:
    from rquant.factor.result_artifact import (
        load_factor_research_artifact,
        publish_factor_research_artifact,
    )

    root = _root(tmp_path)
    receipt = publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    path = root / receipt.filename
    original = path.read_bytes()
    if damage == "wrong_name":
        wrong_sha = "f" * 64
        (root / f"factor-research-v1-{wrong_sha}.json").write_bytes(original)
        (root / f"factor-research-v1-{wrong_sha}.json").chmod(0o600)
        with pytest.raises(ValueError, match="wrong name"):
            load_factor_research_artifact(root, wrong_sha)
        return
    if damage == "truncated":
        path.write_bytes(original[: len(original) // 2])
    elif damage == "changed":
        path.write_bytes(original.replace(b"price_factor", b"other_factor", 1))
    else:
        path.write_bytes(original + b" ")
    with pytest.raises(ValueError):
        load_factor_research_artifact(root, receipt.sha256)


def test_existing_target_is_verified_and_never_overwritten(tmp_path: Path) -> None:
    from rquant.factor.result_artifact import publish_factor_research_artifact

    root = _root(tmp_path)
    research = _research()
    receipt = publish_factor_research_artifact(research, _CODE_REVISION, root)
    target = root / receipt.filename
    target.write_bytes(b"occupied")
    with pytest.raises(ValueError):
        publish_factor_research_artifact(research, _CODE_REVISION, root)
    assert target.read_bytes() == b"occupied"
    assert list(root.iterdir()) == [target]

    target.unlink()
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside remains")
    target.symlink_to(outside)
    with pytest.raises(OSError):
        publish_factor_research_artifact(research, _CODE_REVISION, root)
    assert outside.read_bytes() == b"outside remains"
    assert target.is_symlink()


@pytest.mark.parametrize("unsafe_form", ["public_mode", "hardlink", "directory"])
def test_loader_rejects_non_private_or_non_regular_target(tmp_path: Path, unsafe_form: str) -> None:
    from rquant.factor.result_artifact import (
        load_factor_research_artifact,
        publish_factor_research_artifact,
    )

    root = _root(tmp_path)
    receipt = publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    target = root / receipt.filename
    if unsafe_form == "public_mode":
        target.chmod(0o644)
    elif unsafe_form == "hardlink":
        os.link(target, root / "other-link")
    else:
        target.unlink()
        target.mkdir()
    with pytest.raises((OSError, ValueError)):
        load_factor_research_artifact(root, receipt.sha256)


def test_existing_target_syncs_file_and_directory_before_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)
    research = _research()
    expected = module.publish_factor_research_artifact(research, _CODE_REVISION, root)
    real_fsync = os.fsync
    synced: list[str] = []

    def observe(descriptor: int) -> None:
        synced.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
        real_fsync(descriptor)

    monkeypatch.setattr(module.os, "fsync", observe)
    assert module.publish_factor_research_artifact(research, _CODE_REVISION, root) == expected
    assert synced[-2:] == ["file", "directory"]


def test_partial_write_failure_cleans_only_owned_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)
    real_write = os.write
    writes = 0

    def fail_after_partial_write(descriptor: int, data: bytes) -> int:
        nonlocal writes
        writes += 1
        if writes == 1:
            return real_write(descriptor, data[:7])
        raise OSError("injected write failure")

    monkeypatch.setattr(module.os, "write", fail_after_partial_write)
    with pytest.raises(OSError, match="injected write failure"):
        module.publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    assert writes == 2
    assert list(root.iterdir()) == []


def test_temporary_setup_failure_cleans_its_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)

    def fail_mode(_descriptor: int, _mode: int) -> None:
        raise OSError("injected file setup failure")

    monkeypatch.setattr(module.os, "fchmod", fail_mode)
    with pytest.raises(OSError, match="injected file setup failure"):
        module.publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    assert list(root.iterdir()) == []


def test_directory_sync_failure_does_not_return_success_and_retry_repairs_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)
    research = _research()
    real_fsync = os.fsync
    failed = False

    def fail_directory_once(descriptor: int) -> None:
        nonlocal failed
        if stat.S_ISDIR(os.fstat(descriptor).st_mode) and not failed:
            failed = True
            raise OSError("injected directory sync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(module.os, "fsync", fail_directory_once)
    with pytest.raises(OSError, match="injected directory sync failure"):
        module.publish_factor_research_artifact(research, _CODE_REVISION, root)
    assert len(list(root.glob("factor-research-v1-*.json"))) == 1
    assert not list(root.glob(".factor-research-*.tmp"))
    monkeypatch.setattr(module.os, "fsync", real_fsync)
    receipt = module.publish_factor_research_artifact(research, _CODE_REVISION, root)
    assert module.load_factor_research_artifact(root, receipt.sha256).research == research


def test_competing_publish_waits_for_its_own_directory_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)
    research = _research()
    at_unsynced_rename = threading.Event()
    release_first = threading.Event()
    real_fsync = os.fsync
    outcomes: list[object] = []

    def pause_first_directory_sync(descriptor: int) -> None:
        if threading.current_thread().name == "first-publisher" and stat.S_ISDIR(
            os.fstat(descriptor).st_mode
        ):
            at_unsynced_rename.set()
            assert release_first.wait(timeout=10)
        real_fsync(descriptor)

    def first_publish() -> None:
        try:
            outcomes.append(module.publish_factor_research_artifact(research, _CODE_REVISION, root))
        except BaseException as error:
            outcomes.append(error)

    monkeypatch.setattr(module.os, "fsync", pause_first_directory_sync)
    first = threading.Thread(target=first_publish, name="first-publisher")
    first.start()
    try:
        assert at_unsynced_rename.wait(timeout=10)
        second = module.publish_factor_research_artifact(research, _CODE_REVISION, root)
        assert not outcomes
    finally:
        release_first.set()
        first.join(timeout=10)
    assert not first.is_alive()
    assert outcomes == [second]


def test_loader_rejects_in_place_change_after_initial_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import result_artifact as module

    root = _root(tmp_path)
    receipt = module.publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    target = root / receipt.filename
    real_check = module._require_same_root

    def alter_after_read(path: Path, descriptor: int) -> None:
        target.write_bytes(b"x" * target.stat().st_size)
        real_check(path, descriptor)

    monkeypatch.setattr(module, "_require_same_root", alter_after_read)
    with pytest.raises(ValueError, match="changed"):
        module.load_factor_research_artifact(root, receipt.sha256)


@pytest.mark.parametrize("damage", ["source_digest", "recomputed_result", "schema"])
def test_loader_rejects_nested_tamper_even_with_readdressed_outer_content(
    tmp_path: Path, damage: str
) -> None:
    from rquant.factor.result_artifact import (
        load_factor_research_artifact,
        publish_factor_research_artifact,
    )

    root = _root(tmp_path)
    receipt = publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    payload = json.loads((root / receipt.filename).read_bytes())
    if damage == "source_digest":
        payload["research"]["receipt"]["source_sha256"] = "f" * 64
    elif damage == "recomputed_result":
        result = payload["research"]["result"]
        result["portfolio_status"] = "insufficient_data"
        result["sha256"] = _digest({key: value for key, value in result.items() if key != "sha256"})
    else:
        payload["schema_version"] = 2
    payload["content_sha256"] = _digest(
        {key: value for key, value in payload.items() if key != "content_sha256"}
    )
    name = f"factor-research-v1-{payload['content_sha256']}.json"
    tampered = root / name
    tampered.write_bytes(canonical_json_bytes(payload))
    tampered.chmod(0o600)
    with pytest.raises(ValidationError):
        load_factor_research_artifact(root, payload["content_sha256"])


def test_root_and_digest_must_name_a_private_direct_child(tmp_path: Path) -> None:
    from rquant.factor.result_artifact import (
        load_factor_research_artifact,
        publish_factor_research_artifact,
    )

    root = _root(tmp_path)
    research = _research()
    public = tmp_path / "public"
    public.mkdir(mode=0o755)
    public.chmod(0o755)
    for unsafe in (tmp_path / "missing", public, Path("relative")):
        with pytest.raises((OSError, ValueError)):
            publish_factor_research_artifact(research, _CODE_REVISION, unsafe)
    alias = tmp_path / "root-alias"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(OSError):
        publish_factor_research_artifact(research, _CODE_REVISION, alias)
    intermediate = tmp_path / "intermediate"
    intermediate.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(OSError):
        publish_factor_research_artifact(research, _CODE_REVISION, intermediate / "artifacts")
    for bad_sha in ("../" + "a" * 64, "A" * 64, "a" * 63, str(root / "file")):
        with pytest.raises(ValueError, match="SHA-256"):
            load_factor_research_artifact(root, bad_sha)
    (root / ".factor-research-dead.tmp").write_bytes(b"partial")
    with pytest.raises(FileNotFoundError):
        load_factor_research_artifact(root, "a" * 64)


def test_root_replacement_during_publish_prevents_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module("rquant.factor.result_artifact")
    root = _root(tmp_path)
    moved = tmp_path / "moved-root"
    real_rename = module.rename_noreplace_at

    def replace_root(*args: object) -> None:
        real_rename(*args)
        root.rename(moved)
        root.mkdir(mode=0o700)

    monkeypatch.setattr(module, "rename_noreplace_at", replace_root)
    with pytest.raises(ValueError, match="root changed"):
        module.publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    assert list(root.iterdir()) == []
    assert len(list(moved.glob("factor-research-v1-*.json"))) == 1


def test_root_replacement_during_load_prevents_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module("rquant.factor.result_artifact")
    root = _root(tmp_path)
    receipt = module.publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    moved = tmp_path / "moved-root"
    real_read = module._read_verified

    def replace_after_read(*args: object) -> object:
        read = real_read(*args)
        root.rename(moved)
        root.mkdir(mode=0o700)
        return read

    monkeypatch.setattr(module, "_read_verified", replace_after_read)
    with pytest.raises(ValueError, match="root changed"):
        module.load_factor_research_artifact(root, receipt.sha256)
    assert list(root.iterdir()) == []
