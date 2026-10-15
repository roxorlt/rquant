import sys, time; sys.path.insert(0, "/workspace/rquant/src")
import numpy as np, pandas as pd
from pathlib import Path
from rquant.web import market_live as ml
df = pd.read_parquet("chunks.parquet")
chunks = []
for i, g in df.groupby("_rg", sort=True):
    c = g.drop(columns="_rg").reset_index(drop=True); c.attrs["rq_key"] = ("q", 0.0, int(i)); chunks.append(c)
rng = np.random.default_rng(0); IND = {c: f"ind{rng.integers(110)}" for c in df.ts_code.unique()}
ml._industries = lambda root: IND
ml._GRIDS.clear()
for n in (100, 300, 480, 500, 503):
    t = time.perf_counter(); got = ml._compute(Path("."), "2026-10-14", chunks[:n], None); tg = time.perf_counter() - t
    t = time.perf_counter(); ref = ml._compute_ref(Path("."), "2026-10-14", chunks[:n], None); tr = time.perf_counter() - t
    print(n, "identical" if got.model_dump() == ref.model_dump() else "DIFF", "grid %.3fs ref %.3fs" % (tg, tr))
ml._GRIDS.clear(); t = time.perf_counter(); ml._compute(Path("."), "2026-10-14", chunks, None); print("cold full grid %.2fs" % (time.perf_counter() - t))
t = time.perf_counter(); ml._compute(Path("."), "2026-10-14", chunks, "ind5"); print("same chunks other board %.3fs" % (time.perf_counter() - t))
