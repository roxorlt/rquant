from __future__ import annotations

import importlib.util
import json
import threading
import sys
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def subscription_module():
    assert importlib.util.find_spec('rquant.l2.subscriptions') is not None, 'durable subscription store is missing'
    from rquant.l2 import subscriptions
    return subscriptions


def test_additive_union_is_durable_idempotent_and_capacity_atomic(tmp_path: Path):
    module = subscription_module()
    store = module.SubscriptionStore(tmp_path / 'control', max_symbols=2)
    assert store.read().symbols == ()
    assert not store.root.exists()
    added = store.add(['000001', '600000', '000001'])
    assert added.added == ('000001', '600000') and added.revision == 1
    assert store.add(['600000']).revision == 1
    assert store.add(['600000']).added == ()
    assert json.loads((store.root / 'desired.json').read_bytes()) == {'revision': 1, 'symbols': ['000001', '600000']}
    before = (store.root / 'desired.json').read_bytes()
    with pytest.raises(module.SubscriptionError) as caught:
        store.add(['300001'])
    assert caught.value.code == 'capacity_exceeded'
    assert (store.root / 'desired.json').read_bytes() == before
    assert (store.root.stat().st_mode & 0o777) == 0o700
    assert ((store.root / 'desired.json').stat().st_mode & 0o777) == 0o600


def test_concurrent_union_has_no_lost_symbols_or_revision_for_duplicates(tmp_path: Path):
    module = subscription_module()
    root = tmp_path / 'control'
    barrier = threading.Barrier(8)
    def add(index):
        barrier.wait()
        return module.SubscriptionStore(root).add([f'600{index:03}', '000001'])
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(add, range(8)))
    targets = module.SubscriptionStore(root).read()
    assert len(targets.symbols) == 9 and targets.revision == 8
    assert set(targets.symbols) == {'000001', *(f'600{i:03}' for i in range(8))}
    assert sorted(r.revision for r in results) == list(range(1, 9))


@pytest.mark.parametrize('payload', [b'{"revision":1,"revision":2,"symbols":[]}', b'{"revision":1,"symbols":["600000"],"max_symbols":100}', b'{"revision":true,"symbols":[]}', b'{"revision":1,"symbols":["600000","600000"]}', b'{"revision":', b'{"revision":0,"symbols":["600000"]}'])
def test_corrupt_desired_fails_without_replacing_it(tmp_path: Path, payload: bytes):
    module = subscription_module()
    root = tmp_path / 'control'
    root.mkdir(mode=0o700)
    target = root / 'desired.json'
    target.write_bytes(payload)
    target.chmod(0o600)
    for method in ('read', 'add'):
        with pytest.raises(module.SubscriptionError):
            getattr(module.SubscriptionStore(root), method)(*([['000001']] if method == 'add' else []))
        assert target.read_bytes() == payload


def test_symlink_and_unsafe_permissions_are_refused(tmp_path: Path):
    module = subscription_module()
    root = tmp_path / 'control'
    store = module.SubscriptionStore(root)
    store.add(['600000'])
    target = root / 'desired.json'
    target.chmod(0o644)
    with pytest.raises(module.SubscriptionError): store.read()
    target.unlink()
    other = tmp_path / 'other'
    other.write_text('{"revision":1,"symbols":["600000"]}')
    target.symlink_to(other)
    with pytest.raises(module.SubscriptionError): store.add(['000001'])
    assert json.loads(other.read_text())['symbols'] == ['600000']


def test_capacity_policy_is_typed_private_and_not_client_desired(tmp_path: Path):
    module = subscription_module()
    path = tmp_path / 'capacity.json'
    values = {'max_symbols': 20, 'source_tree': 'a' * 40, 'acceptance_sha256': 'b' * 64}
    path.write_text(json.dumps(values))
    path.chmod(0o600)
    assert module.load_capacity_policy(path).max_symbols == 20
    for values in ({**values, 'max_symbols': 101}, {**values, 'extra': 1}, {**values, 'max_symbols': True}):
        path.write_text(json.dumps(values))
        with pytest.raises(module.SubscriptionError): module.load_capacity_policy(path)


def test_collector_cli_passes_explicit_control_and_policy_without_raising_capacity(tmp_path, monkeypatch):
    from rquant.l2.__main__ import main
    from rquant.l2.models import FeedStatus
    calls = []
    class Collector:
        def __init__(self, cache, **options): calls.append(options)
        def run(self): return FeedStatus(state='stopped')
    monkeypatch.setitem(sys.modules, 'rquant.l2.feed', SimpleNamespace(FeedCollector=Collector))
    policy = tmp_path / 'capacity.json'
    policy.write_text(json.dumps({'max_symbols': 40, 'source_tree': 'a' * 40, 'acceptance_sha256': 'b' * 64}))
    policy.chmod(0o600)
    args = ['--cache-root', str(tmp_path / 'cache'), '--bootstrap-file', 'synthetic',
            '--order-template', 'synthetic', '--trade-template', 'synthetic', '--control-root', str(tmp_path / 'control')]
    assert main(args + ['--capacity-file', str(policy)]) == 0
    assert calls[0]['max_symbols'] == 40 and calls[0]['control_root'] == tmp_path / 'control'
    with pytest.raises(SystemExit): main(args + ['--capacity-file', str(policy), '--max-symbols', '100'])
    with pytest.raises(SystemExit): main(args + ['--symbols', '600000'])


def test_failed_desired_replace_preserves_previous_union(tmp_path, monkeypatch):
    module = subscription_module()
    store = module.SubscriptionStore(tmp_path / 'control')
    store.add(['600000'])
    before = (store.root / 'desired.json').read_bytes()
    def fail_replace(*args): raise OSError('SYNTHETIC_PRIVATE_ERROR')
    monkeypatch.setattr(module.os, 'replace', fail_replace)
    with pytest.raises(module.SubscriptionError, match='^control_io$'):
        store.add(['000001'])
    assert (store.root / 'desired.json').read_bytes() == before
    assert not list(store.root.glob('.desired-*'))


def test_nonregular_desired_is_refused_without_blocking(tmp_path):
    import os
    module = subscription_module()
    root = tmp_path / 'control'; root.mkdir(mode=0o700)
    os.mkfifo(root / 'desired.json', mode=0o600)
    with pytest.raises(module.SubscriptionError): module.SubscriptionStore(root).read()


def test_new_control_directory_commit_syncs_its_existing_parent(tmp_path, monkeypatch):
    module = subscription_module()
    synced = []
    original = module._fsync_dir
    def synced_dir(path):
        synced.append(path)
        original(path)
    monkeypatch.setattr(module, '_fsync_dir', synced_dir)
    store = module.SubscriptionStore(tmp_path / 'control')
    store.add(['600000'])
    assert tmp_path in synced and store.root in synced
