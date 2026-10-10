from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import duckdb
import pytest

from rquant.storage.duckdb import DuckDBStore


def _config(tmp_path: Path):
    assert importlib.util.find_spec('rquant.storage.primary_writer_gate') is not None
    from rquant.storage.primary_writer_gate import PrimaryWriterGateConfig
    primary = tmp_path/'primary.duckdb'
    duckdb.connect(str(primary)).close()
    lock = tmp_path/'primary.lock'
    lock.touch(mode=0o600)
    return PrimaryWriterGateConfig.capture(primary_path=primary, lock_path=lock)


def test_gate_excludes_another_writer_before_connect(tmp_path: Path) -> None:
    config = _config(tmp_path)
    from rquant.storage.primary_writer_gate import PrimaryWriterGate, PrimaryWriterBusy
    gate = PrimaryWriterGate(config)
    with gate.acquire():
        with pytest.raises(PrimaryWriterBusy):
            DuckDBStore(config.primary_path, primary_writer_gate=config)
    with DuckDBStore(config.primary_path, primary_writer_gate=config) as store:
        assert store._conn.execute('SELECT 1').fetchone() == (1,)


def test_borrowed_lease_does_not_release_parent(tmp_path: Path) -> None:
    config = _config(tmp_path)
    from rquant.storage.primary_writer_gate import PrimaryWriterGate, PrimaryWriterBusy
    gate = PrimaryWriterGate(config)
    with gate.acquire() as lease:
        with DuckDBStore(config.primary_path, primary_writer_lease=lease) as store:
            assert store._conn.execute('SELECT 1').fetchone() == (1,)
        with pytest.raises(PrimaryWriterBusy):
            gate.acquire()
    with gate.acquire():
        pass


@pytest.mark.parametrize('change',['symlink','hardlink','replace','chmod','primary_replace'])
def test_pinned_gate_and_primary_identity_cannot_change(tmp_path: Path, change: str) -> None:
    config = _config(tmp_path)
    from rquant.storage.primary_writer_gate import PrimaryWriterGate, PrimaryWriterIdentityError
    lock = config.lock_path
    if change == 'symlink':
        renamed = tmp_path/'other.lock'
        lock.rename(renamed)
        lock.symlink_to(renamed)
    elif change == 'hardlink':
        os.link(lock,tmp_path/'alias.lock')
    elif change == 'replace':
        lock.rename(tmp_path/'original.lock')
        lock.touch(mode=0o600)
        replacement = lock.stat()
        assert (replacement.st_dev, replacement.st_ino) != (config.lock_device, config.lock_inode)
    elif change == 'chmod':
        lock.chmod(0o644)
    else:
        config.primary_path.rename(tmp_path/'old.duckdb')
        duckdb.connect(str(config.primary_path)).close()
    with pytest.raises(PrimaryWriterIdentityError):
        PrimaryWriterGate(config).acquire()


def test_constructor_failure_closes_owned_gate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(tmp_path)
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    monkeypatch.setattr(DuckDBStore,'_init_schema',lambda self: (_ for _ in ()).throw(RuntimeError('schema')))
    with pytest.raises(RuntimeError,match='schema'):
        DuckDBStore(config.primary_path,primary_writer_gate=config)
    with PrimaryWriterGate(config).acquire():
        pass


def test_original_replica_borrows_common_gate_without_relocking_or_releasing(tmp_path: Path) -> None:
    from rquant.research_sync import refresh_readonly_replica
    from rquant.storage.primary_writer_gate import PrimaryWriterGate,PrimaryWriterBusy
    config=_config(tmp_path)
    with DuckDBStore(config.primary_path):
        pass
    gate=PrimaryWriterGate(config)
    with gate.acquire() as lease:
        ok,_=refresh_readonly_replica(config.primary_path,tmp_path/'replica.duckdb',primary_writer_lease=lease)
        assert ok
        with pytest.raises(PrimaryWriterBusy):
            gate.acquire()
    with gate.acquire():
        pass


@pytest.mark.parametrize('action',['replica','sync','restore'])
def test_configured_original_raw_writers_and_copy_wait_before_connect(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,action: str) -> None:
    from rquant import research_sync
    from rquant.config import Settings
    from rquant.storage.primary_writer_gate import PrimaryWriterGate
    assert Settings.model_fields['primary_writer_gate_path'].default is None
    config=_config(tmp_path)
    profile=tmp_path/'writer-profile.json'
    profile.write_text(config.model_dump_json())
    monkeypatch.setattr(research_sync.settings,'primary_writer_gate_path',profile)
    backup=tmp_path/'backup.duckdb'
    backup.touch()
    monkeypatch.setattr(research_sync,'_rescue_stale_wal',lambda *args:pytest.fail('raw writer opened while gate was occupied'))
    with PrimaryWriterGate(config).acquire():
        if action=='replica':
            ok,_=research_sync.refresh_readonly_replica(config.primary_path,tmp_path/'replica.duckdb')
            assert not ok
        else:
            report=(research_sync.sync_from_backup(backup,config.primary_path,refresh_replica=False) if action=='sync'
                else research_sync.restore_research_tables(backup,config.primary_path,refresh_replica=False))
            assert report.has_errors
