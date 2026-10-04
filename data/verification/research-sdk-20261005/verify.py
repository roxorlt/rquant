"""Preserve one synthetic SDK demonstration and observed local resource evidence."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import timedelta
from pathlib import Path

PROOF = Path(__file__).resolve().parent
ROOT = PROOF.parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(1, str(ROOT))
config = PROOF / "test-config"
os.environ.update(
    RQUANT_DISABLE_DOTENV="1",
    TUSHARE_TOKEN_MAIN="0" * 32,
    DATA_DIR=str(config / "data"),
    DUCKDB_PATH=str(config / "primary.duckdb"),
    PARQUET_DIR=str(config / "parquet"),
    LOG_DIR=str(config / "logs"),
)

from rquant import research_sdk as sdk  # noqa: E402
from tests.unit.test_research_sdk import _demo_files  # noqa: E402
from tests.unit.test_factor_source_prepare import _FIRST  # noqa: E402


class TaskInfo(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_uint64)
        for name in (
            "virtual_size", "resident_size", "total_user", "total_system",
            "threads_user", "threads_system",
        )
    ] + [
        (name, ctypes.c_int32)
        for name in (
            "policy", "faults", "pageins", "cow_faults", "messages_sent",
            "messages_received", "syscalls_mach", "syscalls_unix", "csw",
            "threadnum", "numrunning", "priority",
        )
    ]


_proc_pidinfo = ctypes.CDLL(None, use_errno=True).proc_pidinfo
_proc_pidinfo.argtypes = (
    ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int
)
_proc_pidinfo.restype = ctypes.c_int


def resources() -> tuple[int, int, int]:
    info = TaskInfo()
    size = ctypes.sizeof(info)
    count = _proc_pidinfo(os.getpid(), 4, 0, ctypes.byref(info), size)
    if count != size:
        raise OSError(ctypes.get_errno(), "own-PID task info is unavailable")
    return len(os.listdir("/dev/fd")), len(threading.enumerate()), info.threadnum


def settle(expected: tuple[int, int, int]) -> tuple[int, int, int]:
    deadline = time.monotonic() + 1
    observed = resources()
    while observed != expected and time.monotonic() < deadline:
        time.sleep(0.01)
        observed = resources()
    if observed != expected:
        raise AssertionError(f"resource drift: {expected} -> {observed}")
    return observed


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    demo = PROOF / "demo"
    demo.mkdir(mode=0o700)
    source, lake, artifact_root, research, display = _demo_files(demo)
    source_file = demo / "source.json"
    source_file.write_text(source.model_dump_json(), encoding="utf-8")
    source_file.chmod(0o600)
    inputs = dict(
        source_sha256=source.sha256,
        source_file_byte_sha256=digest(source_file),
        date=(_FIRST + timedelta(days=2)).isoformat(),
        research_content_sha256=research.sha256,
        display_content_sha256=display.sha256,
        sealed_files={
            artifact.relative_path: digest(lake / artifact.relative_path)
            for artifact in source.input_artifacts()
        },
        origin=(
            "Existing test_factor_daily_feature_source and test_factor_result_artifact "
            "factories; production prepare_factor_market_temperature_source and "
            "original artifact publishers. Eight synthetic codes, three source days; "
            "market table has two shared day rows (0/100, NULL/NaN, absent latest day)."
        ),
    )
    (demo / "inputs.json").write_text(json.dumps(inputs, indent=2) + "\n")
    query = sdk.FactorDailyFeatureQuery(
        source_sha256=source.sha256,
        trade_date=_FIRST + timedelta(days=2),
        stock_codes=("000001.SZ", "000007.SZ"),
        fields=("ma5", "market_above_ma20_ratio_pct", "market_high_60d_ratio_pct"),
    )
    # Fixture producers have closed; allow their final native thread teardown to finish.
    time.sleep(0.05)
    before = resources()
    with sdk.open_factor_daily_feature_source(source, lake_root=lake) as reader:
        batch = reader.query(query)
    assert reader.closed and not reader._private_root.exists()
    after_success = settle(before)
    try:
        with sdk.open_factor_daily_feature_source(source, lake_root=lake) as reader:
            reader.query(query.model_copy(update={"stock_codes": ("999999.SZ",)}))
    except ValueError:
        pass
    else:
        raise AssertionError("out-of-scope code was accepted")
    assert reader.closed and not reader._private_root.exists()
    after_query_error = settle(before)
    with tempfile.TemporaryDirectory(prefix=".sdk-corrupt-", dir=PROOF) as scratch:
        bad_root = Path(scratch)
        payload = (artifact_root / research.filename).read_bytes()
        (bad_root / research.filename).write_bytes(payload + b"\n")
        try:
            sdk.load_factor_research_artifact(bad_root, research.sha256)
        except ValueError:
            pass
        else:
            raise AssertionError("changed artifact bytes were accepted")
    after_artifact_error = settle(before)
    command = [
        sys.executable, "-I", "-B", "-c",
        "import runpy,sys; sys.path.insert(0,sys.argv[1]); sys.argv=sys.argv[2:]; "
        "runpy.run_path(sys.argv[0],run_name='__main__')",
        str(ROOT / "src"), str(ROOT / "docs/examples/research_sdk.py"),
        "--source", str(source_file), "--lake-root", str(lake),
        "--date", inputs["date"], "--codes", *query.stock_codes,
        "--fields", *query.fields, "--artifact-root", str(artifact_root),
        "--research-sha256", research.sha256, "--display-sha256", display.sha256,
    ]
    started = time.monotonic()
    completed = subprocess.run(command, capture_output=True, text=True, timeout=30)
    elapsed = time.monotonic() - started
    (PROOF / "example.log").write_text(completed.stdout + completed.stderr)
    if completed.returncode:
        raise AssertionError(completed.stderr)
    after_example = settle(before)
    assert not list(lake.glob(".daily-feature-reader-*"))
    assert not list(PROOF.glob(".sdk-corrupt-*"))
    assert inputs["sealed_files"] == {
        artifact.relative_path: digest(lake / artifact.relative_path)
        for artifact in source.input_artifacts()
    }
    report = dict(
        python=sys.version,
        executable=sys.executable,
        parent_commit="51d1dc5a0a3c009e05d13de184ea78723a66c429",
        product_base="09b11f4407ccd9703baa6981d25224cc71390516",
        resources_order=("file_descriptors", "python_threads", "native_threads"),
        native_thread_measurement="macOS proc_pidinfo PROC_PIDTASKINFO for own PID only",
        ps_capability="/bin/ps denied by sandbox; own-PID API available",
        resources_before=before,
        resources_after_success=after_success,
        resources_after_query_error=after_query_error,
        resources_after_artifact_error=after_artifact_error,
        resources_after_example=after_example,
        temporary_reader_directories=0,
        temporary_corrupt_roots=0,
        source_files_unchanged=True,
        example_command=command,
        example_exit_code=completed.returncode,
        example_elapsed_seconds=elapsed,
        sample_fact_count=len(batch.facts),
        synthetic_input_manifest=str(demo / "inputs.json"),
    )
    (PROOF / "resources.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in (
        "resources_before", "resources_after_success", "resources_after_query_error",
        "resources_after_artifact_error", "resources_after_example", "example_exit_code",
        "example_elapsed_seconds", "sample_fact_count",
    )}, indent=2))
    print(json.dumps({key: inputs[key] for key in (
        "date", "research_content_sha256", "display_content_sha256",
    )}, indent=2))


if __name__ == "__main__":
    main()
