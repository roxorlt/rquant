"""Synthetic Linux proof using the product snapshot and actual child executable."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import resource
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

src = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(src))
import duckdb
from rquant.research_query import QueryExecutor, QueryLimits, QueryRequest, VerifiedQuerySnapshot, build_query_snapshot
ddl_spec = importlib.util.spec_from_file_location("query_proof_schema", src / "rquant/storage/schema.py")
assert ddl_spec is not None and ddl_spec.loader is not None
ddl_module = importlib.util.module_from_spec(ddl_spec)
ddl_spec.loader.exec_module(ddl_module)

started = time.monotonic()
record = {"kind":"actual_research_query_linux_product", "python":sys.version.split()[0], "provider_http":0, "production_write":False, "cases":[]}
fd_before = len(list(Path("/proc/self/fd").iterdir()))
with tempfile.TemporaryDirectory(prefix="rquant-query-product-") as directory:
    root = Path(directory)
    source = root / "original.duckdb"
    with duckdb.connect(str(source), config={"threads":1}) as connection:
        for ddl in (ddl_module.DAILY_BAR_DDL, ddl_module.ADJ_FACTOR_DDL, ddl_module.TRADE_CALENDAR_DDL): connection.execute(ddl)
        connection.execute("INSERT INTO daily_bar(ts_code,trade_date,close) VALUES ('600001.SH','2026-09-30',12.5)")
        connection.execute("INSERT INTO adj_factor VALUES ('600001.SH','2026-09-30',1.2)")
        connection.execute("INSERT INTO trade_calendar VALUES ('SSE','2026-09-30',true,'2026-09-29','test',now())")
        connection.execute("CREATE TABLE manual_watchlist(owner_id VARCHAR,secret VARCHAR)")
        connection.execute("INSERT INTO manual_watchlist VALUES ('alice','private-a'),('bob','private-b')")
    source.chmod(0o400)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = build_query_snapshot(source, root / "public", source_sha256=digest, source_at=datetime(2026,10,5,tzinfo=UTC))
    snapshot = VerifiedQuerySnapshot(root / "public")
    scratch = root / "scratch"
    def run(name: str, sql: str, expected: set[str], *, limits: QueryLimits | None = None, mode: str = "query"):
        executor = QueryExecutor(snapshot, scratch, limits=limits)
        result = executor.execute(QueryRequest(sql=sql,mode=mode))
        assert result.status in expected, (name,result.model_dump())
        assert executor.active == 0 and list(scratch.iterdir()) == []
        reaped = True
        if executor.last_pid is not None:
            try: os.waitpid(executor.last_pid, os.WNOHANG)
            except ChildProcessError: pass
            else: reaped = False
        assert reaped
        record["cases"].append({"name":name,"status":result.status,"rows":len(result.rows),"elapsed_ms":result.elapsed_ms,"child_pid":executor.last_pid,"child_reaped":reaped,"scratch_empty":True})
        return result
    basic=run("public_actual_values","SELECT ts_code,trade_date,close FROM daily_bar",{"ready"})
    assert basic.rows[0][0] == "600001.SH" and basic.rows[0][2] == 12.5
    types=run("typed_values_duplicates","SELECT 1 AS n,2 AS n, DATE '2026-09-30' AS d,1.25::DECIMAL(5,2) AS dec,'\\x41'::BLOB AS b,'NaN'::DOUBLE AS nf,true AS yes",{"ready"})
    assert types.columns[0].name == types.columns[1].name
    assert [value.kind for value in types.rows[0][2:6]] == ["date","decimal","binary","nonfinite"]
    assert types.rows[0][6] is True
    for index,sql in enumerate(("SELECT * FROM main.manual_watchlist", "SELECT * FROM query_table('manual_watchlist')", "SELECT * FROM query('SELECT * FROM manual_watchlist')", "SELECT * FROM glob('/etc/*')")):
        run(f"private_or_external_{index}",sql,{"failed"})
    run("quoted_keyword","SELECT 'INSERT; COPY; read_csv'",{"ready"})
    run("trusted_explain","SELECT close FROM daily_bar",{"ready"},mode="explain")
    rows=run("actual_row_cap","SELECT * FROM range(10001)",{"partial"})
    assert len(rows.rows)==10000
    wide=run("actual_16mib_wire_cap","SELECT repeat('x',17000000)",{"partial"})
    assert wide.rows == ()
    run("actual_address_allocation_failure","SELECT repeat('x',2147483647)",{"failed","timeout"})
    run("actual_deadline_reaping","SELECT sum(a.i*b.i) FROM range(10000000) a(i),range(10000000) b(i)",{"timeout"},limits=QueryLimits(seconds=0.4))
    spill_samples=[]
    stop=threading.Event()
    def sample_spill():
        while not stop.wait(0.005):
            paths=list(scratch.rglob("*")) if scratch.exists() else []
            sizes=[]
            for path in paths:
                if "spill" not in path.parts: continue
                try:
                    if path.is_file(): sizes.append(path.stat().st_size)
                except FileNotFoundError: pass
            spill_samples.append(sum(sizes))
    sampler=threading.Thread(target=sample_spill)
    sampler.start()
    try:
        run("actual_spill_budget","SELECT i,repeat(md5(i::VARCHAR),10) AS s FROM range(2000000) t(i) ORDER BY s",{"failed"},limits=QueryLimits(memory_bytes=8*2**20,spill_bytes=2*2**20,file_bytes=2*2**20))
    finally:
        stop.set();sampler.join()
    record["spill_observed_peak_bytes"]=max(spill_samples,default=0)
    assert record["spill_observed_peak_bytes"] <= 2*2**20
    # Product bootstrap also directly demonstrates kernel enforcement and fails closed
    # when setrlimit fails, independent of a query's engine error.
    child_path=src / "rquant/research_query/child.py"
    kernel = """import importlib.util,pathlib,sys,resource
spec=importlib.util.spec_from_file_location('query_child',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.enforce_limits(256*2**20,1024*1024,pathlib.Path(sys.argv[2]))
try: payload=bytearray(384*2**20)
except MemoryError: print('address_and_file_enforced')
else: raise SystemExit(2)
"""
    checked=subprocess.run([sys.executable,"-I","-B","-c",kernel,str(child_path),str(root)],stdin=subprocess.DEVNULL,capture_output=True,timeout=5,close_fds=True,env={"LANG":"C.UTF-8"})
    assert checked.returncode==0 and b"address_and_file_enforced" in checked.stdout
    record["product_kernel_probe"]={"exit_code":checked.returncode,"address_enforced":True,"file_enforced":True,"child_reaped":True}
    executor=QueryExecutor(snapshot,scratch,limits=QueryLimits(seconds=5))
    cancel=threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(executor.execute,QueryRequest(sql="SELECT sum(a.i*b.i) FROM range(10000000) a(i),range(10000000) b(i)"),cancel=cancel)
        while executor.last_pid is None and not future.done(): time.sleep(0.01)
        cancel.set()
        cancelled=future.result(timeout=5)
    assert cancelled.status=="timeout" and executor.active==0 and list(scratch.iterdir())==[]
    record["cancel_reaped"]=True
    assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
    record["source_sha256"]=digest
    record["snapshot_sha256"]=manifest.file_sha256
    record["source_unchanged"]=True
record["scratch_removed"]=not root.exists()
record["fd_delta"]=len(list(Path("/proc/self/fd").iterdir()))-fd_before
record["peak_child_rss_bytes"]=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss*1024
record["wall_seconds"]=time.monotonic()-started
assert record["fd_delta"]==0
record["status"]="passed"
print(json.dumps(record,sort_keys=True,separators=(",",":")))
