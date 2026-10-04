from __future__ import annotations
import hashlib, json, os, subprocess, sys, tempfile, threading, time
from pathlib import Path
from datetime import UTC, datetime

src=Path(sys.argv[1]).resolve();sys.path.insert(0,str(src))
import duckdb
from rquant.research_query import QueryExecutor,QueryLimits,QueryRequest,VerifiedQuerySnapshot,build_query_snapshot,PUBLIC_SCHEMA
record={"kind":"bounded_spill_file_diagnostic","production_write":False,"fd_stages":[],"samples":[]}
def fd_stage(name):
    fds={}
    for path in list(Path("/proc/self/fd").iterdir()):
        try:fds[path.name]=os.readlink(path)
        except FileNotFoundError:pass
    record["fd_stages"].append({"name":name,"fds":fds})
fd_stage("initial_product_import")
with tempfile.TemporaryDirectory(prefix="rq-spill-trace-") as directory:
    root=Path(directory);source=root/"pure-control.duckdb"
    with duckdb.connect(str(source),config={"threads":1,"memory_limit":"512MiB","enable_external_access":False}) as connection:
        fd_stage("pure_disk_connect")
        connection.execute("BEGIN TRANSACTION")
        for table,columns in PUBLIC_SCHEMA.items():
            connection.execute("CREATE TABLE "+table+" ("+",".join(name+" "+kind for name,kind in columns)+")")
            connection.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",[table]).fetchall()
        fd_stage("pure_schema_and_columns")
        for name,sql in (
            ("tables","SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal ORDER BY table_name"),
            ("views","SELECT count(*) FROM duckdb_views() WHERE NOT internal"),
            ("functions","SELECT count(*) FROM duckdb_functions() WHERE NOT internal"),
            ("sequences","SELECT count(*) FROM duckdb_sequences()"),
            ("schemas","SELECT schema_name FROM duckdb_schemas() WHERE NOT internal"),
        ):
            connection.execute(sql).fetchall();fd_stage("pure_introspection_"+name)
        connection.execute("COMMIT");connection.execute("CHECKPOINT")
        fd_stage("pure_checkpoint")
    fd_stage("pure_closed")
    source.chmod(0o400);digest=hashlib.sha256(source.read_bytes()).hexdigest()
    build_query_snapshot(source,root/"public",source_sha256=digest,source_at=datetime.now(UTC))
    snapshot=VerifiedQuerySnapshot(root/"public");scratch=root/"scratch"
    fd_stage("product_published")
    # Diagnostic child is the frozen product plus one bounded exception record.
    original=(src/"rquant/research_query/child.py").read_text()
    error_path=root/"engine-error.txt"
    debug=original.replace('    except Exception:\n        result = {',
        '    except Exception as diagnostic_error:\n        Path('+repr(str(error_path))+').write_text(str(diagnostic_error)[:4000])\n        result = {')
    assert debug!=original
    debug_path=root/"diagnostic-child.py";debug_path.write_text(debug)
    original_popen=subprocess.Popen
    def diagnostic_popen(arguments,*args,**kwargs):
        rewritten=list(arguments)
        if str(src/"rquant/research_query/child.py") in rewritten:
            rewritten[rewritten.index(str(src/"rquant/research_query/child.py"))]=str(debug_path)
        return original_popen(rewritten,*args,**kwargs)
    subprocess.Popen=diagnostic_popen
    record["original_child_sha256"]=hashlib.sha256(original.encode()).hexdigest()
    record["diagnostic_child_sha256"]=hashlib.sha256(debug.encode()).hexdigest()
    limits=QueryLimits(memory_bytes=64*2**20,spill_bytes=64*2**20,file_bytes=64*2**20)
    executor=QueryExecutor(snapshot,scratch,limits=limits)
    stop=threading.Event();started=time.monotonic()
    def files():
        result={}
        for path in list(scratch.rglob("*")):
            if "spill" not in path.parts:continue
            try:
                st=path.stat()
                if path.is_file():result[str(path.relative_to(scratch))]={"inode":st.st_ino,"size":st.st_size,"blocks_bytes":st.st_blocks*512}
            except FileNotFoundError:pass
        return result
    def sample():
        peak=0
        while not stop.wait(0.002):
            before=time.monotonic();first=files();second=files();after=time.monotonic()
            total=sum(item["size"] for item in first.values())
            if total>peak or len(record["samples"])<8:
                peak=max(peak,total)
                record["samples"].append({"at_seconds":before-started,"scan_seconds":after-before,"first":first,"second":second,"stable":first==second,"logical_bytes":total,"allocated_bytes":sum(item["blocks_bytes"] for item in first.values())})
    sampler=threading.Thread(target=sample);sampler.start()
    try:result=executor.execute(QueryRequest(sql="SELECT i,repeat(md5(i::VARCHAR),10) AS s FROM range(2000000) t(i) ORDER BY s"))
    finally:stop.set();sampler.join();subprocess.Popen=original_popen
    record["result"]={"status":result.status,"elapsed_seconds":time.monotonic()-started,"active":executor.active,"scratch_empty":list(scratch.iterdir())==[],"engine_error":error_path.read_text() if error_path.exists() else None}
    record["limits"]=limits.model_dump()
    record["peak_logical_bytes"]=max((item["logical_bytes"] for item in record["samples"]),default=0)
    record["peak_stable_logical_bytes"]=max((item["logical_bytes"] for item in record["samples"] if item["stable"]),default=0)
    record["peak_allocated_bytes"]=max((item["allocated_bytes"] for item in record["samples"]),default=0)
    fd_stage("product_closed_spill")
record["private_directory_removed"]=not root.exists()
fd_stage("final")
print(json.dumps(record,separators=(",",":"),sort_keys=True),flush=True)
assert executor.active==0 and record["private_directory_removed"]
