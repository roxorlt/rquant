import sys,time,cProfile,pstats
sys.path.insert(0,"/workspace/rquant/src")
import numpy as np,pandas as pd
from rquant.web import market_live as m
df=pd.read_parquet("chunks.parquet")
chunks=[g.drop(columns="_rg") for _,g in df.groupby("_rg",sort=True)]
codes=df.ts_code.unique(); rng=np.random.default_rng(0)
IND={c:f"ind{rng.integers(110)}" for c in codes}
m._industries=lambda root: IND
from pathlib import Path
T={}
def tick(k,t0): T[k]=T.get(k,0)+time.perf_counter()-t0
# stage timing of original
r0=m._compute(Path("."),"d",chunks,None)


# ---- vectorized prototype ----
def compute_fast(chunks):
    t0=time.perf_counter()
    d=pd.concat(chunks,ignore_index=True); tick("concat",t0); t0=time.perf_counter()
    code=pd.Categorical(d["ts_code"]); mm=pd.Categorical(d["m"])
    ci=code.codes.astype(np.int64); mi=mm.codes.astype(np.int64)
    nC,nM=len(code.categories),len(mm.categories)
    # order by (minute,time) then take last per code / per (minute,code) with numpy
    order=np.arange(len(d))  # chunks chronological -> last write wins, no sort; tick("sort",t0); t0=time.perf_counter()
    ci_o,mi_o=ci[order],mi[order]
    price=d["price"].to_numpy()[order]; high=d["high"].to_numpy()[order]
    lastpos=np.full(nC,-1); lastpos[ci_o]=np.arange(len(order))  # last write wins
    hmax=np.full(nC,-np.inf); np.maximum.at(hmax,ci_o,high)
    grid=np.full((nM,nC),np.nan); grid[mi_o,ci_o]=price           # last per cell
    hg=np.full((nM,nC),-np.inf); np.maximum.at(hg,(mi_o,ci_o),high)
    tick("reduce",t0); t0=time.perf_counter()
    px=pd.DataFrame(grid).ffill().to_numpy(); hi=np.maximum.accumulate(hg,axis=0)
    src=order[lastpos]
    pre=d["pre_close"].to_numpy()[src]; nm=d["name"].to_numpy()[src]; cs=pd.Series(code.categories)
    lp=m._limit_pct(cs,pd.Series(nm)); up=np.round(pre*(1+lp)+1e-9,2)
    ok=pre>0
    lim=px>=up-1e-6; valid=(~np.isnan(px)&ok).sum(1)
    up_ratio=((px>pre)&ok).sum(1)/np.where(valid==0,np.nan,valid)
    lu=(lim&ok).sum(1); br=((hi>=up-1e-6)&~lim&ok&~np.isnan(px)).sum(1)
    tick("curve",t0); t0=time.perf_counter()
    last=pd.DataFrame({"name":nm,"price":price[lastpos],"pre_close":pre,"high":hmax,
        "amount":d["amount"].to_numpy()[src]},index=code.categories)[ok]
    last["pct"]=(last.price/last.pre_close-1)*100
    last["industry"]=last.index.map(IND)
    g=last.groupby("industry")
    b=g.agg(n=("pct","size"),pct_median=("pct","median"),amount=("amount","sum"))
    tick("boards",t0)
    return up_ratio,lu,br,b
for k in range(3):
    T.clear(); t=time.perf_counter(); ur,lu,br,b=compute_fast(chunks); print("fast %.2fs"%(time.perf_counter()-t),{k:round(v,2) for k,v in T.items()})
print("limit_up match",np.array_equal(lu,np.array(r0.curve.limit_up)),"broken diff max",np.abs(br-np.array(r0.curve.broken)).max(),
      "upratio maxdiff",np.nanmax(np.abs(np.nan_to_num(ur)-np.array(r0.curve.up_ratio))))
