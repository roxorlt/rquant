"""Native process evidence. Root runs these nodes in the allowed host environment."""
from __future__ import annotations

import json
import os
import select
import subprocess
import sys
from datetime import UTC,datetime,timedelta
from pathlib import Path
from threading import Timer

import duckdb
import pytest


def _child_command(tmp_path: Path,code: str,payload: object) -> list[str]:
    root=Path(__file__).resolve().parents[2]
    # Each native child gets its own private, dummy configuration before any product import.
    source="\n".join([
        'import sys,json,os',f'sys.path.insert(0,{str(root / "src")!r})',
        'from pathlib import Path','from pydantic_settings.sources import DotEnvSettingsSource',
        'DotEnvSettingsSource.__call__=lambda self:{}',
        f'work=Path({str(tmp_path)!r});os.umask(0o077)',
        'from rquant.config import Settings',
        'safe=Settings(_env_file=None,data_dir=work,duckdb_path=work/"dummy-config.duckdb",parquet_dir=work/"parquet",log_dir=work/"logs",tushare_token_main="dummy-offline-token-"*4,pushdeer_keys="",pushplus_tokens="")',
        'import rquant.config as config;config.settings=safe',
        f'payload=json.loads({json.dumps(payload)!r})',code,
    ])
    return [sys.executable,'-I','-B','-c',source]


def _ready(child: subprocess.Popen[str]) -> None:
    assert child.stdout is not None
    ready,_,_=select.select([child.stdout],[],[],10)
    assert ready,'native child did not reach the bounded ready point'
    line=child.stdout.readline()
    assert line.strip()=='ready',line


def _stop(child: subprocess.Popen[str]) -> None:
    try:
        if child.poll() is None:
            child.terminate()
        child.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        child.kill()
        child.communicate(timeout=10)


def test_native_two_process_gate_and_original_duckdb_lock_exclude_another_writer(tmp_path: Path) -> None:
    from tests.unit.test_primary_writer_gate import _config
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    config=_config(tmp_path)
    payload=config.model_dump(mode='json')
    holder="""
from rquant.storage.primary_writer_gate import PrimaryWriterGate,PrimaryWriterGateConfig
from rquant.storage.duckdb import DuckDBStore
gate=PrimaryWriterGate(PrimaryWriterGateConfig.model_validate(payload))
with gate.acquire() as lease,DuckDBStore(lease.config.primary_path,primary_writer_lease=lease) as writer:
    print('ready',flush=True)
    sys.stdin.read(1)
"""
    challenger="""
import duckdb
from rquant.storage.primary_writer_gate import PrimaryWriterGate,PrimaryWriterGateConfig,PrimaryWriterBusy
config=PrimaryWriterGateConfig.model_validate(payload)
try:
    with PrimaryWriterGate(config).acquire():
        raise AssertionError('competing process obtained the common writer gate')
except PrimaryWriterBusy:
    pass
try:
    connection=duckdb.connect(str(config.primary_path),read_only=True)
except duckdb.IOException:
    pass
else:
    connection.close()
    raise AssertionError('new readonly process opened the original locked DuckDB')
print('excluded',flush=True)
"""
    child=subprocess.Popen(_child_command(tmp_path,holder,payload),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    try:
        _ready(child)
        other=subprocess.run(_child_command(tmp_path,challenger,payload),capture_output=True,text=True,timeout=10)
        assert other.returncode==0,other.stderr
        assert other.stdout.strip()=='excluded'
        assert child.stdin is not None
        child.stdin.write('x');child.stdin.flush()
        _,error=child.communicate(timeout=10)
        assert child.returncode==0,error
        with PrimaryWriterGate(config).acquire():
            connection=duckdb.connect(str(config.primary_path),read_only=True)
            assert connection.execute('SELECT 1').fetchone()==(1,)
            connection.close()
    finally:
        _stop(child)


def test_native_original_claim_takeover_waits_until_actual_duck_commit(tmp_path: Path) -> None:
    from tests.unit.test_backfill_execute import _claimed,NOW
    from tests.unit.test_primary_writer_gate import _config
    from rquant.storage.duckdb import DuckDBStore
    state,claim=_claimed(tmp_path)
    gate=_config(tmp_path)
    payload={'gate':gate.model_dump(mode='json'),'state':str(state.path),'claim':claim.model_dump(mode='json'),'now':NOW.isoformat()}
    holder="""
from datetime import datetime
from rquant.storage.primary_writer_gate import PrimaryWriterGate,PrimaryWriterGateConfig
from rquant.storage.duckdb import DuckDBStore
from rquant.backfill_state import BackfillStateStore,ClaimedBackfillTask
now=datetime.fromisoformat(payload['now']);state=BackfillStateStore(Path(payload['state']),busy_timeout_ms=30)
claim=ClaimedBackfillTask.model_validate(payload['claim'])
with PrimaryWriterGate(PrimaryWriterGateConfig.model_validate(payload['gate'])).acquire() as lease:
    with state.commit_claim(claim,now=now) as protected,DuckDBStore(lease.config.primary_path,primary_writer_lease=lease) as writer:
        writer._conn.execute('BEGIN')
        writer._conn.execute('CREATE TABLE native_fence_fact(token VARCHAR PRIMARY KEY)')
        writer._conn.execute('INSERT INTO native_fence_fact VALUES (?)',[claim.claim_token])
        print('ready',flush=True)
        sys.stdin.read(1)
        protected.verify(now=now)
        writer._conn.execute('COMMIT')
        protected.succeed(duration_seconds=0,now=now)
"""
    challenger="""
import sqlite3
from datetime import datetime,timedelta
from rquant.backfill_state import BackfillStateStore
try:
    state=BackfillStateStore(Path(payload['state']),busy_timeout_ms=30)
    state.claim_task('owned-plan',worker_id='competing-native',lease_seconds=120,now=datetime.fromisoformat(payload['now'])+timedelta(seconds=121))
except sqlite3.OperationalError as error:
    assert 'locked' in str(error) or 'busy' in str(error)
else:
    raise AssertionError('expired-time challenger entered protected original SQLite claim')
print('excluded',flush=True)
"""
    child=subprocess.Popen(_child_command(tmp_path,holder,payload),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    try:
        _ready(child)
        other=subprocess.run(_child_command(tmp_path,challenger,payload),capture_output=True,text=True,timeout=10)
        assert other.returncode==0,other.stderr
        assert other.stdout.strip()=='excluded'
        assert child.stdin is not None
        child.stdin.write('x');child.stdin.flush()
        _,error=child.communicate(timeout=10)
        assert child.returncode==0,error
        assert state.get_task('owned-plan','day-1').status=='succeeded'
        with DuckDBStore(gate.primary_path,read_only=True) as reader:
            assert reader._conn.execute('SELECT token FROM native_fence_fact').fetchall()==[(claim.claim_token,)]
    finally:
        _stop(child)


def test_native_stop_interrupts_original_worker_sql_and_releases_both_locks(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.test_backfill_execute import _worker_setup
    from rquant.storage.duckdb import DuckDBStore
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    state,spec,current,worker,calls=_worker_setup(tmp_path)
    original=DuckDBStore.upsert_daily
    timers=[]
    def long_sql(store: DuckDBStore,frame):
        timer=Timer(0.15,worker.request_stop);timers.append(timer);timer.start()
        store._conn.execute('SELECT SUM(sin(i)) FROM range(10000000000) rows(i)').fetchone()
        return original(store,frame)
    monkeypatch.setattr(DuckDBStore,'upsert_daily',long_sql)
    try:
        result=worker.run_one(spec.execution_id,owner=spec.owner)
        assert result.execution.status=='partial' and result.execution.completed_tasks==0
        assert worker.stopped.is_set() and calls==[]
        with PrimaryWriterGate(current[0].primary_writer_gate).acquire():
            with DuckDBStore(current[0].primary_writer_gate.primary_path,read_only=True) as reader:
                assert reader._conn.execute('SELECT COUNT(*) FROM daily_bar').fetchone()==(0,)
    finally:
        for timer in timers:
            timer.cancel();timer.join(timeout=5)
            assert not timer.is_alive()


def test_native_original_deadline_supervisor_kills_reaps_and_releases_primary(tmp_path: Path) -> None:
    from tests.unit.test_primary_writer_gate import _config
    from rquant.cli import _run_deadline_supervised_process
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    from rquant.storage.duckdb import DuckDBStore
    config=_config(tmp_path)
    # The original maintenance contract uses an initialized facts/receipt database.
    with DuckDBStore(config.primary_path,primary_writer_gate=config):
        pass
    with DuckDBStore(config.primary_path,read_only=True) as reader:
        assert reader._conn.execute('SELECT max(version) FROM schema_migration').fetchone()==(17,)
        assert reader._conn.execute('SELECT COUNT(*) FROM daily_bar').fetchone()==(0,)
    marker=tmp_path/'child-pid'
    code="""
import time,os
from rquant.storage.primary_writer_gate import PrimaryWriterGate,PrimaryWriterGateConfig
from rquant.storage.duckdb import DuckDBStore
with PrimaryWriterGate(PrimaryWriterGateConfig.model_validate(payload['gate'])).acquire() as lease:
    with DuckDBStore(lease.config.primary_path,primary_writer_lease=lease):
        Path(payload['marker']).write_text(str(os.getpid()))
        while True:time.sleep(0.05)
"""
    command=_child_command(tmp_path,code,{'gate':config.model_dump(mode='json'),'marker':str(marker)})
    # Startup must reach the actual DuckDB lock. The finite local budget includes imports.
    deadline=datetime.now(UTC)+timedelta(seconds=3)
    assert _run_deadline_supervised_process(command,deadline=deadline)==2
    assert marker.is_file(),'child did not acquire the actual locks before the finite deadline'
    pid=int(marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid,0)
    with PrimaryWriterGate(config).acquire():
        connection=duckdb.connect(str(config.primary_path),read_only=True)
        assert connection.execute('SELECT 1').fetchone()==(1,)
        connection.close()
