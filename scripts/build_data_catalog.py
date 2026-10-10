"""Regenerate the committed, versioned web data directory offline."""

from pathlib import Path

from rquant.data_catalog.build import write_current_catalog

if __name__ == "__main__":
    artifact = Path(__file__).resolve().parents[1] / "src/rquant/data_catalog/catalog-v1.json"
    document = write_current_catalog(artifact)
    print(f"Wrote {len(document.datasets)} datasets to {artifact}")
