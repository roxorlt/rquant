import sys,time; sys.path.insert(0,"/workspace/rquant/src")
import numpy as np,pandas as pd
from datetime import datetime,date
import rquant.surge_watch as s
rng=np.random.default_rng(0); N=5400
codes=[f"{300000+i:06d}.SZ" for i in range(N)]
snap=pd.DataFrame({"ts_code":codes,"name":np.where(rng.random(N)<.03,"*ST某","某股"),
  "pct_chg":rng.normal(0,3,N),"amount":rng.lognormal(18,1,N)})
bl=s.preload_baseline.__wrapped__ if hasattr(s.preload_baseline,"__wrapped__") else None
avg={c:float(a) for c,a in zip(codes,rng.lognormal(19,1,N))}
class B: pass
b=B(); b.avg_amount_20d=avg; b.curve=s.linear_progress_curve(); b.theme={}
cfg=s.SurgeConfig(); gi=120
def T(f,n=20):
    t=time.perf_counter()
    for _ in range(n): r=f()
    return (time.perf_counter()-t)/n*1000,r
ms,r0=T(lambda:s._rough_candidates(snap,b,cfg,gi)); print("rough loop %.1fms"%ms)
avg_s=pd.Series(avg)
def rough_vec():
    a=snap["ts_code"].map(avg_s).to_numpy(); cv=float(b.curve[gi]) or 1/s.CURVE_POINTS
    st=snap["name"].fillna("").str.contains("ST",regex=False).to_numpy()  # approx of _detect_st
    m=(a>0)&(snap.pct_chg.to_numpy()>0)&~st&(snap.amount.to_numpy()>=cfg.k_rough*a*cv)
    return snap.ts_code.to_numpy()[m].tolist()
ms,r1=T(rough_vec); print("rough vec %.1fms same=%s"%(ms,set(r0)==set(r1)))
cum={}
def upd():
    for r in snap.itertuples():
        arr=cum.get(r.ts_code)
        if arr is None: arr=cum[r.ts_code]=np.full(241,np.nan)
        arr[gi]=float(r.amount)
ms,_=T(upd); print("update_cum loop %.1fms"%ms)
idx={c:i for i,c in enumerate(codes)}; M=np.full((N,241),np.nan)
def updv():
    ii=snap.ts_code.map(idx).to_numpy(); M[ii,gi]=snap.amount.to_numpy()
ms,_=T(updv); print("update_cum vec %.2fms"%ms)
full={c:rng.lognormal(18,1,241).cumsum() for c in codes}
ms,_=T(lambda:s.series_to_frame(full),1); print("series_to_frame loop %.0fms"%ms)
def stf():
    A=np.vstack(list(full.values())); i,j=np.nonzero(~np.isnan(A))
    return pd.DataFrame({"ts_code":np.array(list(full))[i],"minute_idx":j,"cum_amount":A[i,j]})
ms,_=T(stf,3); print("series_to_frame vec %.0fms"%ms)
tt=pd.date_range("2026-10-14 09:30","2026-10-14 15:00",freq="1min")
bars=pd.DataFrame({"trade_time":np.concatenate([tt-pd.Timedelta(days=k) for k in (1,2,3)]),"amount":rng.random(len(tt)*3)})
ms,_=T(lambda:s.build_three_day_baseline(bars,date(2026,10,15)),20); print("three_day_baseline per code %.1fms"%ms)
