"""Run with explicit sealed inputs, or import inspect_research into a notebook."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

# %% The notebook imports the same readers and calculations as production.
from rquant.research_sdk import (
    FactorDailyFeatureQuery,
    FactorDailyFeatureSource,
    assemble_factor_research_result,
    load_factor_display_artifact,
    load_factor_research_artifact,
    open_factor_daily_feature_source,
)


# %% Only this explicitly bounded query expands shared market days over codes.
def inspect_research(
    source: FactorDailyFeatureSource,
    query: FactorDailyFeatureQuery,
    *,
    lake_root: Path,
    artifact_root: Path,
    research_sha256: str,
    display_sha256: str | None = None,
) -> None:
    selected = source.select(query.fields)
    for field in selected.fields:
        print(f"{field.column}: {field.name_zh} [{field.unit}] — {field.description_zh}")
    with open_factor_daily_feature_source(source, lake_root=lake_root) as reader:
        batch = reader.query(query)
    print(batch.model_dump_json(indent=2))

    # The full reader verifies its original request/result; the same library is callable here.
    full = load_factor_research_artifact(artifact_root, research_sha256)
    result = assemble_factor_research_result(full.research.request)
    print(f"research result: {result.sha256}")
    print(f"sealed research content: {full.content_sha256}")
    if display_sha256 is not None:
        display = load_factor_display_artifact(artifact_root, display_sha256)
        if display.full_artifact_sha256 != full.content_sha256:
            raise ValueError("display and research artifacts refer to different research")
        print(f"display content: {display.content_sha256}")


# %% A command-line equivalent of the notebook cells, with no application settings.
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--lake-root", required=True, type=Path)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--codes", required=True, nargs="+")
    parser.add_argument("--fields", required=True, nargs="+")
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--research-sha256", required=True)
    parser.add_argument("--display-sha256")
    args = parser.parse_args()
    if not args.source.is_absolute():
        parser.error("--source must be an absolute path")
    # Source descriptors keep the existing 16 MiB configuration-file ceiling.
    with args.source.open("rb") as stream:
        payload = stream.read(16 * 1024 * 1024 + 1)
    if not 0 < len(payload) <= 16 * 1024 * 1024:
        parser.error("source descriptor must be nonempty and at most 16 MiB")
    source = FactorDailyFeatureSource.model_validate_json(payload)
    inspect_research(
        source,
        FactorDailyFeatureQuery(
            source_sha256=source.sha256,
            trade_date=args.date,
            stock_codes=tuple(args.codes),
            fields=tuple(args.fields),
        ),
        lake_root=args.lake_root,
        artifact_root=args.artifact_root,
        research_sha256=args.research_sha256,
        display_sha256=args.display_sha256,
    )


if __name__ == "__main__":
    main()
