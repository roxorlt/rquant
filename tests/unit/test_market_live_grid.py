"""numpy grid path of 市场全景 must equal the pandas reference, also when fed incrementally."""
from pathlib import Path

import numpy as np
import pandas as pd

from rquant.web import market_live as ml


def _chunks(seed: int = 0, n_codes: int = 60, n_chunks: int = 12) -> list[pd.DataFrame]:
    rng = np.random.default_rng(seed)
    codes = [f"{600000 + i:06d}.SH" for i in range(n_codes // 2)] + \
            [f"{300000 + i:06d}.SZ" for i in range(n_codes // 2)]
    names = {c: ("ST" + c[:3] if i % 9 == 0 else "N" + c[:3]) for i, c in enumerate(codes)}
    pre = {c: float(rng.uniform(5, 50)) for c in codes}
    out, sec = [], 9 * 3600 + 30 * 60
    for k in range(n_chunks):
        rows = []
        for _ in range(400):
            sec += int(rng.integers(0, 4))
            c = codes[int(rng.integers(len(codes)))]
            lim = 0.2 if c.startswith("30") else 0.1
            px = round(pre[c] * (1 + float(rng.uniform(-lim, lim))), 2)
            if rng.random() < 0.05:
                px = round(pre[c] * (1 + lim) + 1e-9, 2)
            t = f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"
            rows.append(dict(ts_code=c, name=names[c], time=t, price=px,
                             high=max(px, px * (1 + float(rng.uniform(0, 0.02)))),
                             pre_close=pre[c] if rng.random() > 0.02 else np.nan,
                             amount=float(rng.uniform(1e5, 1e7))))
        ch = ml._chunk(pd.DataFrame(rows))
        ch.attrs["rq_key"] = ("f", 0.0, k)
        out.append(ch)
    return out


def test_grid_matches_reference_incrementally(monkeypatch) -> None:
    monkeypatch.setattr(ml, "_industries", lambda root: {})
    ml._GRIDS.clear()
    chunks = _chunks()
    for n in (1, 3, 4, 8, 12):
        got = ml._compute(Path("."), "2026-10-14", chunks[:n], None)
        ref = ml._compute_ref(Path("."), "2026-10-14", chunks[:n], None)
        assert got.model_dump() == ref.model_dump(), n


def test_grid_rebuilds_on_compaction(monkeypatch) -> None:
    monkeypatch.setattr(ml, "_industries", lambda root: {c: "X" for c in []})
    ml._GRIDS.clear()
    chunks = _chunks(1)
    ml._compute(Path("."), "2026-10-14", chunks[:6], None)
    for ch in chunks:
        ch.attrs["rq_key"] = ("compacted", 1.0, ch.attrs["rq_key"][2])
    got = ml._compute(Path("."), "2026-10-14", chunks, None)
    ref = ml._compute_ref(Path("."), "2026-10-14", chunks, None)
    assert got.model_dump() == ref.model_dump()
