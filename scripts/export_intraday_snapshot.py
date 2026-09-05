#!/usr/bin/env python3
"""Export the sealed historical minute snapshot `feature_live` reads at start-up.

`FeatureLiveRuntimeSettings.historical_minutes_snapshot_path` names a parquet file and
`historical_snapshot_id` its sha256; `runtime_builder_feature._read_immutable_parquet`
re-hashes the bytes on every start and refuses a mismatch, so the file is immutable once the
profile records it. `intraday_feature_engine.INPUT_COLUMNS` fixes the nine columns, and
`FeatureRuntimeConfig.lookback_sessions` (20 by default) fixes how many prior sessions the
same-clock medians need.

Two facts about the source table decide the query:

* `minute_bar`'s primary key is `(ts_code, trade_time, freq, source)`, so one minute can
  hold both a `tushare` row and a `tushare_rt` row. The feature engine rejects "multiple bars
  for one ts_code natural minute", so exactly one source has to be chosen.
* `data_quality.DEFAULT_MINUTE_SOURCE_SESSION_SPECS` declares `tushare` 1min bars as
  `timestamp_semantics="bar_end"` and `tushare_rt` as `provider_snapshot`. `feature_live`
  runs with `bar_timestamp_semantics="bar_end"`, so `tushare` is the only source whose
  timestamps mean what the engine assumes.

`available_at` does not exist in `minute_bar`. Under bar-end semantics the bar is complete at
`trade_time`, and the sealed history is only ever compared against a decision time in a later
session, so `available_at = trade_time` is the honest value and the default here. A publication
lag can be added with `--availability-lag-seconds`, but it would be invented data.

**Read only, and only from the replica.** DuckDB holds a single file lock: while a writer
holds it, every new connection to that file fails, `read_only=True` included. This refuses a
database named `rquant.duckdb` unless told otherwise, and never opens a write connection.

Usage on the machine that holds the minute history:

    python scripts/export_intraday_snapshot.py \\
        --database /Users/roxor/brain/30-projects/rQuant/data/rquant_ro.duckdb \\
        --output /Users/roxor/rq-snapshots/minute-history.parquet \\
        --sessions 20

It prints the sha256 to pass to `build_runtime_production_inputs.py` as
`--minutes-snapshot-sha256`, plus the session and row counts that justify it.

Two guards worth knowing about. The shaped frame is run through the feature engine's own
`_normalize_frame` before anything is written, so a suspended session's zero-price bars
fail the export instead of failing `feature_live` at start-up after the profile has
already recorded the snapshot's sha256. And `--ts-code-file` narrows the export to a
universe: twenty sessions of the whole market is several million rows, and `feature_live`
reads the entire parquet into memory on every start and copies it on every batch.

Run it with the checkout's own interpreter (`.venv/bin/python`, or `uv run`).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
if str(_REPOSITORY_ROOT / "src") not in sys.path:  # pragma: no cover - import bootstrap
    sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))

import pandas as pd  # noqa: E402

from rquant.intraday_feature_engine import (  # noqa: E402
    INPUT_COLUMNS,
    IntradayFeatureValidationError,
    _normalize_frame,
)

#: `FeatureRuntimeConfig.lookback_sessions` default.
DEFAULT_SESSIONS = 20
#: The one source whose `trade_time` is a bar end
#: (`data_quality.DEFAULT_MINUTE_SOURCE_SESSION_SPECS`).
DEFAULT_SOURCE = "tushare"
DEFAULT_FREQ = "1min"


class ExportError(RuntimeError):
    """The snapshot cannot be exported from what the operator supplied."""


def _sessions_query(*, sessions: int) -> str:
    return f"""
        SELECT DISTINCT CAST(trade_time AS DATE) AS trade_date
        FROM minute_bar
        WHERE source = ? AND freq = ?
        ORDER BY trade_date DESC
        LIMIT {int(sessions)}
    """


_ROWS_QUERY = """
    SELECT
        ts_code,
        trade_time,
        open,
        high,
        low,
        close,
        vol,
        amount
    FROM minute_bar
    WHERE source = ?
      AND freq = ?
      AND CAST(trade_time AS DATE) >= ?
      AND CAST(trade_time AS DATE) <= ?
    ORDER BY ts_code, trade_time
"""


def read_snapshot_frame(
    database: Path,
    *,
    sessions: int,
    source: str,
    freq: str,
    availability_lag_seconds: int,
    allow_primary_database: bool,
    universe: tuple[str, ...] = (),
) -> tuple[pd.DataFrame, tuple[date, ...]]:
    """Read the newest `sessions` trading days of one minute source, newest session last."""

    if database.name == "rquant.duckdb" and not allow_primary_database:
        raise ExportError(
            "refusing to open the primary DuckDB; point --database at the read-only "
            "replica (rquant_ro.duckdb) or pass --allow-primary-database"
        )
    if not database.exists():
        raise ExportError(f"minute database does not exist: {database}")
    import duckdb

    connection = duckdb.connect(str(database), read_only=True)
    try:
        session_rows = connection.execute(
            _sessions_query(sessions=sessions),
            [source, freq],
        ).fetchall()
        if len(session_rows) < sessions:
            raise ExportError(
                f"minute_bar holds {len(session_rows)} sessions for source={source} "
                f"freq={freq}, fewer than the {sessions} feature_live needs"
            )
        trade_dates = tuple(sorted(_as_date(row[0]) for row in session_rows))
        query = _ROWS_QUERY
        parameters: list[object] = [source, freq, trade_dates[0], trade_dates[-1]]
        if universe:
            # The production table holds ~67M minute rows; twenty sessions of the whole
            # market is several million, and `feature_live` reads the whole parquet into
            # memory on every start and copies it on every batch. Narrowing to the codes
            # actually subscribed is the only lever the operator has besides --sessions.
            placeholders = ", ".join("?" for _ in universe)
            query = query.replace(
                "    ORDER BY ts_code, trade_time",
                f"      AND ts_code IN ({placeholders})\n    ORDER BY ts_code, trade_time",
            )
            parameters.extend(universe)
        frame = connection.execute(query, parameters).fetch_df()
    finally:
        connection.close()
    if frame.empty:
        raise ExportError("the selected sessions hold no minute rows")
    return _shape_snapshot(frame, availability_lag_seconds=availability_lag_seconds), trade_dates


def _as_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    raise ExportError(f"minute_bar trade_time is not a date: {value!r}")


def _shape_snapshot(frame: pd.DataFrame, *, availability_lag_seconds: int) -> pd.DataFrame:
    """Give the frame exactly `INPUT_COLUMNS`, in that order, with a derived `available_at`.

    `trade_time` stays naive: `intraday_feature_engine._as_shanghai_timestamp` localizes a
    naive timestamp to Asia/Shanghai, which is what the stored values already mean, and
    localizing here would only invite a double conversion.
    """

    shaped = frame.copy()
    shaped["trade_time"] = pd.to_datetime(shaped["trade_time"])
    if getattr(shaped["trade_time"].dt, "tz", None) is not None:
        raise ExportError("minute_bar trade_time carries a timezone; expected naive local time")
    shaped["available_at"] = shaped["trade_time"] + pd.Timedelta(seconds=availability_lag_seconds)
    for column in INPUT_COLUMNS[3:]:
        shaped[column] = pd.to_numeric(shaped[column], errors="raise").astype("float64")
    shaped["ts_code"] = shaped["ts_code"].astype("string").str.strip()
    ordered = shaped.loc[:, list(INPUT_COLUMNS)]
    ordered = ordered.sort_values(["ts_code", "trade_time"], kind="stable").reset_index(drop=True)
    duplicated = ordered.duplicated(subset=["ts_code", "trade_time"])
    if bool(duplicated.any()):
        raise ExportError(
            "the selected minute rows hold more than one bar for a ts_code minute; "
            "check that only one source was selected"
        )
    _reject_frames_feature_live_would_refuse(ordered)
    return ordered


def _reject_frames_feature_live_would_refuse(frame: pd.DataFrame) -> None:
    """Run the consumer's own admission rules before the snapshot is written.

    `feature_live` hashes this parquet at start-up and then hands it to `live_compute`,
    whose `_normalize_frame` refuses the whole batch if any row has a non-positive or
    non-finite price — which is exactly the shape a suspended session's minute bars have
    in `minute_bar`. Finding that out at start-up means a service that cannot start and a
    profile that has already recorded the snapshot's sha256; finding it here means an
    export that did not happen.
    """

    try:
        _normalize_frame(frame, label="historical_minutes")
    except IntradayFeatureValidationError as exc:
        numeric = frame.loc[:, list(INPUT_COLUMNS[3:])]
        offenders = frame.loc[
            (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
            | numeric.isna().any(axis=1)
        ]
        first = offenders.iloc[0].to_dict() if not offenders.empty else None
        raise ExportError(
            "the exported rows are not admissible to feature_live "
            f"({exc}); first offending row: {first}"
        ) from exc


def write_snapshot(frame: pd.DataFrame, output: Path) -> str:
    """Write the parquet 0600 and return its sha256.

    The bytes are produced in memory first so the digest is of exactly what lands on disk,
    and so a failed write leaves no half-file for the profile to hash.
    """

    payload = frame.to_parquet(index=False)
    if payload is None:  # pragma: no cover - pandas returns bytes when path is omitted
        raise ExportError("pandas did not return the parquet payload")
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        output,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return hashlib.sha256(payload).hexdigest()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--freq", default=DEFAULT_FREQ)
    parser.add_argument("--availability-lag-seconds", type=int, default=0)
    parser.add_argument(
        "--ts-code-file",
        default=None,
        help="file of ts_code values, one per line; restricts the export to that universe",
    )
    parser.add_argument("--allow-primary-database", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        return _run(arguments)
    except ExportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


def _run(arguments: argparse.Namespace) -> int:
    if arguments.sessions < 1:
        raise ExportError("--sessions must be at least 1")
    if arguments.availability_lag_seconds < 0:
        raise ExportError("--availability-lag-seconds cannot be negative")
    output = Path(arguments.output)
    if not output.is_absolute():
        raise ExportError(f"--output must be absolute: {output}")
    if output.suffix.lower() != ".parquet":
        raise ExportError(f"--output must be a .parquet path: {output}")

    universe: tuple[str, ...] = ()
    if arguments.ts_code_file is not None:
        universe_path = Path(arguments.ts_code_file)
        try:
            lines = universe_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise ExportError(f"ts_code file is unreadable: {universe_path}") from exc
        universe = tuple(sorted({line.strip() for line in lines if line.strip()}))
        if not universe:
            raise ExportError(f"ts_code file names no codes: {universe_path}")

    frame, trade_dates = read_snapshot_frame(
        Path(arguments.database),
        sessions=arguments.sessions,
        source=arguments.source,
        freq=arguments.freq,
        availability_lag_seconds=arguments.availability_lag_seconds,
        allow_primary_database=arguments.allow_primary_database,
        universe=universe,
    )
    digest = write_snapshot(frame, output)

    span = (trade_dates[-1] - trade_dates[0]) + timedelta(days=1)
    print(f"snapshot {output}")
    print(f"sha256 {digest}")
    print(f"rows {len(frame)} ts_codes {frame['ts_code'].nunique()}")
    print(
        f"sessions {len(trade_dates)} "
        f"{trade_dates[0].isoformat()}..{trade_dates[-1].isoformat()} "
        f"(calendar span {span.days} days)"
    )
    print(f"source {arguments.source} freq {arguments.freq} (bar_end timestamps)")
    print(f"available_at = trade_time + {arguments.availability_lag_seconds}s")
    print(f"universe {len(universe) or 'all'} ts_codes")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
