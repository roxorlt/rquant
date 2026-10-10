"""Strategy templates with immutable versions (roadmap module 10).

A spec = entry pool (screen preset) + weight rule + rebalance + capital. Each
save with different content becomes the next version; identical content returns
the existing version. Files: ``$RQUANT_RESEARCH_ROOT/strategy/<slug>/v<N>.json``.
A portfolio run made from a spec records ``strategy@version`` so it can be
reproduced exactly.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from rquant.backtest.engine import BacktestConfig
from rquant.backtest.store import research_root

KIND = "strategy"
_SLUG = re.compile(r"^[A-Za-z0-9_-]{1,40}$")


class StrategySpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    slug: str = Field(pattern=_SLUG.pattern)
    title: str = Field(min_length=1, max_length=60)
    preset: str = Field(min_length=1)
    config: BacktestConfig = BacktestConfig()
    note: str = ""

    def content_hash(self) -> str:
        body = self.model_dump(mode="json", exclude={"note", "title"})
        return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:12]


class StrategyVersion(BaseModel):
    version: int
    content_hash: str
    created_at: datetime
    spec: StrategySpec


def _folder(slug: str, root: Path | None) -> Path:
    if not _SLUG.match(slug):
        raise ValueError("invalid strategy slug")
    return (root or research_root()) / KIND / slug


def versions(slug: str, root: Path | None = None) -> list[StrategyVersion]:
    folder = _folder(slug, root)
    items = [StrategyVersion.model_validate_json(p.read_text()) for p in folder.glob("v*.json")]
    return sorted(items, key=lambda v: v.version)


def save_version(spec: StrategySpec, root: Path | None = None) -> StrategyVersion:
    existing = versions(spec.slug, root)
    digest = spec.content_hash()
    for item in existing:
        if item.content_hash == digest:
            return item
    version = StrategyVersion(version=(existing[-1].version + 1) if existing else 1,
                              content_hash=digest, created_at=datetime.now(UTC), spec=spec)
    folder = _folder(spec.slug, root)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"v{version.version}.json"
    with path.open("x") as fh:  # versions are never overwritten
        fh.write(version.model_dump_json())
    return version


def resolve(ref: str, root: Path | None = None) -> StrategyVersion:
    """``slug`` (latest) or ``slug@N``."""
    slug, _, number = ref.partition("@")
    items = versions(slug, root)
    if not items:
        raise LookupError(f"strategy not found: {slug}")
    if not number:
        return items[-1]
    for item in items:
        if item.version == int(number):
            return item
    raise LookupError(f"strategy version not found: {ref}")


def list_strategies(root: Path | None = None) -> dict[str, list[StrategyVersion]]:
    base = (root or research_root()) / KIND
    if not base.is_dir():
        return {}
    return {p.name: versions(p.name, root) for p in sorted(base.iterdir())
            if p.is_dir() and _SLUG.match(p.name)}
