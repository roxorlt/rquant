from __future__ import annotations
import importlib.util, json, os, subprocess, sys, tempfile, threading, time, hashlib
from pathlib import Path
from datetime import UTC, datetime
record={"kind":"query_fd_and_actual_spill_diagnostic","stages":[],"cases":[],"production_write":False}
def stage(name):
    fds={}
    for item in list(Path("/proc/self/fd").iterdir()):
        try: fds[item.name]=os.readlink(item)
        except FileNotFoundError: pass
    record["stages"].append({"name":name,"fds":fds})
    return fds
stage("stdlib_before_any_child")
for index in range(2):
    p=subprocess.Popen([sys.executable,"-I","-B","-c","pass"],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,close_fds=True,env={"LANG":"C.UTF-8"})
    assert p.wait(timeout=5)==0
    stage(f"closed_stdlib_popen_{index}")
subprocess.run([sys.executable,"-I","-B","-c","print(1)"],capture_output=True,close_fds=True,check=True,timeout=5)
stage("closed_stdlib_capture_output")
import duckdb
stage("duckdb_import")
with duckdb.connect(":memory:",config={"threads":1}) as connection:
    connection.execute("SELECT 1").fetchone()
stage("duckdb_closed_success")
for index in range(2):
    with duckdb.connect(":memory:",config={"threads":1}) as connection:
        try: connection.execute("SELECT * FROM missing_table")
        except duckdb.Error: pass
    stage(f"duckdb_closed_error_{index}")
src=Path(sys.argv[1]).resolve();sys.path.insert(0,str(src))
from rquant.research_query import QueryExecutor,QueryLimits,QueryRequest,VerifiedQuerySnapshot,build_query_snapshot
stage("product_import")
spec=importlib.util.spec_from_file_location("proof_ddl",src/"rquant/storage/schema.py");m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
with tempfile.TemporaryDirectory(prefix="rq-fd-spill-") as directory:
    root=Path(directory);source=root/"source.duckdb"
    with duckdb.connect(str(source),config={"threads":1}) as connection:
        for ddl in (m.DAILY_BAR_DDL,m.ADJ_FACTOR_DDL,m.TRADE_CALENDAR_DDL):connection.execute(ddl)
    source.chmod(0o400);digest=hashlib.sha256(source.read_bytes()).hexdigest()
    build_query_snapshot(source,root/"public",source_sha256=digest,source_at=datetime.now(UTC))
    snapshot=VerifiedQuerySnapshot(root/"public");scratch=root/"scratch"
    stage("product_closed_snapshot_build")
    request=QueryRequest(sql="SELECT 1")
    stage("product_request_guard")
    for index in range(2):
        executor=QueryExecutor(snapshot,scratch)
        result=executor.execute(request)
        assert result.status=="ready"
        assert executor.active==0 and list(scratch.iterdir())==[]
        stage(f"product_closed_execution_{index}")
    samples=[];stop=threading.Event()
    def sample():
        while not stop.wait(0.002):
            sizes=[]
            for p in list(scratch.rglob("*")):
                if "spill" not in p.parts:continue
                try:
                    if p.is_file():sizes.append(p.stat().st_size)
                except FileNotFoundError:pass
            samples.append(sum(sizes))
    sampler=threading.Thread(target=sample);sampler.start()
    started=time.monotonic()
    try:
        limits=QueryLimits(memory_bytes=64*2**20,spill_bytes=64*2**20,file_bytes=64*2**20)
        executor=QueryExecutor(snapshot,scratch,limits=limits)
        result=executor.execute(QueryRequest(sql="SELECT i,repeat(md5(i::VARCHAR),10) AS s FROM range(2000000) t(i) ORDER BY s"))
    finally:stop.set();sampler.join()
    peak=max(samples,default=0)
    record["spill"]={"status":result.status,"elapsed_seconds":time.monotonic()-started,"observed_peak_bytes":peak,"aggregate_budget_bytes":limits.spill_bytes,"single_file_budget_bytes":limits.file_bytes,"scratch_empty":list(scratch.iterdir())==[],"active":executor.active}
    stage("product_closed_spill_failure")
    assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
record["private_directory_removed"]=not root.exists()
final=stage("final")
record["product_fd_delta_second_cycle"]=len(record["stages"][-3]["fds"])-len(record["stages"][-4]["fds"])
print(json.dumps(record,separators=(",",":"),sort_keys=True),flush=True)
assert result.status=="failed" and 0<peak<=limits.spill_bytes and executor.active==0
assert record["product_fd_delta_second_cycle"]==0
assert record["private_directory_removed"]
