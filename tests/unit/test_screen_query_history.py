from __future__ import annotations

import importlib
import importlib.util
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
from rquant.web.models.screen import ScreenRow, ScreenRunData, ScreenSourceInfo

NOW = datetime(2026, 10, 5, 12, tzinfo=UTC)
OWNER = 'alice'


def contracts():
    assert importlib.util.find_spec('rquant.screen.query_contracts') is not None, 'private query contract is absent'
    return importlib.import_module('rquant.screen.query_contracts')


def definition():
    return contracts().ScreenQueryDefinition(
        description='收盘价大于十元', trade_date=date(2026, 9, 30),
        source_kind='replica', source_identity='a' * 64,
        conditions=[{'name': 'gt', 'args': {'left': 'CLOSE[0]', 'right': 10}}],
    )


def executed():
    return ScreenRunData(
        trade_date=date(2026, 9, 30), status='ready', base_count=40, total=21,
        unknown_count=3, steps=[],
        rows=[ScreenRow(ts_code=f'{i:06}.SZ', name='示例', close=11, pct_chg=1) for i in range(21)],
        next_cursor=None, source=ScreenSourceInfo(identity='a'*64, updated_at=NOW-timedelta(days=1)),
    )


def service(tmp_path: Path, result=None):
    module=contracts()
    history_module=importlib.import_module('rquant.screen.query_history')
    tmp_path.chmod(0o700)
    outbox=PageControlOutbox(tmp_path/'commands.sqlite');outbox.path.chmod(0o600)
    history=history_module.ScreenQueryHistory(outbox, cursor_key=b'x'*32)
    consumer=PageControlConsumer(outbox=outbox,data_dir=tmp_path/'data',log_dir=tmp_path/'logs',
        clock=lambda: NOW, screen_query_history=history,
        screen_query_executor=lambda definition: result or executed())
    return PageControlService(outbox=outbox,consumer=consumer),history


def command(identifier='run-1'):
    return contracts().ExecuteScreenQuery(command_id=identifier,requested_at=NOW,definition=definition())


def test_owner_is_never_browser_supplied_and_outcome_is_rejected():
    model=contracts().ExecuteScreenQuery
    for extra in [{'owner_id':'bob'}, {'outcome':'succeeded'}, {'total':12}]:
        with pytest.raises(ValidationError): model(**command().model_dump(),**extra)


def test_actual_completion_persists_full_result_across_reopen_and_owner_scope(tmp_path):
    control,history=service(tmp_path)
    receipt=control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    assert receipt.status.value=='succeeded'
    assert history.detail(OWNER,'run-1').total==21
    assert history.results(OWNER,'run-1',limit=20).next_cursor
    assert history.detail('bob','run-1') is None
    second=importlib.import_module('rquant.screen.query_history').ScreenQueryHistory(control.outbox,cursor_key=b'x'*32)
    page=second.history(OWNER,limit=20)
    assert len(page.items)==1 and page.items[0].definition==definition()
    assert len(second.results(OWNER,'run-1',limit=100).rows)==21


def test_retry_returns_original_receipt_without_reexecution(tmp_path):
    control,history=service(tmp_path)
    first=control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    control.consumer.screen_query_executor=lambda _: pytest.fail('committed execution repeated')
    assert control._resume_trusted_screen_query(command(),authenticated_actor_id=OWNER)==first
    assert control._lookup_trusted_screen_query(command(),authenticated_actor_id='bob') is None
    changed=command().model_copy(update={'definition':definition().model_copy(update={'description':'changed'})})
    with pytest.raises(ValueError): control._resume_trusted_screen_query(changed,authenticated_actor_id=OWNER)


def test_committed_submit_precedes_new_capacity_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    control, history = service(tmp_path)
    original = command()
    receipt = control._submit_trusted_screen_query(original, authenticated_actor_id=OWNER)
    control.consumer.screen_query_executor = lambda _: pytest.fail("committed run repeated")
    monkeypatch.setattr(
        "rquant.screen.query_history.os.statvfs",
        lambda _: SimpleNamespace(f_bavail=0, f_frsize=4096),
    )
    assert control._submit_trusted_screen_query(original, authenticated_actor_id=OWNER) == receipt
    assert history.detail(OWNER, original.command_id).total == 21
    with pytest.raises(ValueError, match="command"):
        control._submit_trusted_screen_query(
            original.model_copy(update={"page_size": 37}), authenticated_actor_id=OWNER
        )
    with pytest.raises(ValueError):
        control._submit_trusted_screen_query(original, authenticated_actor_id="bob")
    with pytest.raises(ValueError, match="capacity unavailable"):
        control._submit_trusted_screen_query(command("new-run"), authenticated_actor_id=OWNER)
    with control.outbox._connect() as connection:
        assert connection.execute("SELECT count(*) FROM page_control_command").fetchone()[0] == 1


def test_resumed_new_artifact_allocation_still_checks_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace
    from rquant.screen.query_contracts import _OwnedExecuteScreenQuery

    control, history = service(tmp_path)
    owned = _OwnedExecuteScreenQuery(**command().model_dump(), owner_id=OWNER)
    control.outbox.enqueue_trusted_screen_query(owned)
    monkeypatch.setattr(
        "rquant.screen.query_history.os.statvfs",
        lambda _: SimpleNamespace(f_bavail=0, f_frsize=4096),
    )
    with pytest.raises(ValueError, match="capacity unavailable"):
        control._resume_trusted_screen_query(command(), authenticated_actor_id=OWNER)
    assert list(history.artifact_root.iterdir()) == []
    assert history.detail(OWNER, "run-1").artifact_sha256 is None


def test_fixed_history_cursor_excludes_later_insert_and_other_owner(tmp_path: Path) -> None:
    control, history = service(tmp_path)
    for i in range(257):
        control._submit_trusted_screen_query(command(f"run-{i}"), authenticated_actor_id=OWNER)
    first = history.history(OWNER, limit=20)
    for i in range(3):
        control._submit_trusted_screen_query(command(f"later-{i}"), authenticated_actor_id=OWNER)
    original = list(first.items)
    cursor = first.next_cursor
    while cursor is not None:
        tail = history.history(OWNER, limit=20, cursor=cursor)
        original.extend(tail.items)
        cursor = tail.next_cursor
    assert len(original) == 257
    assert len({item.execution_id for item in original}) == 257
    assert all(item.definition == definition() for item in original)
    assert [item.sequence for item in original] == sorted(
        [item.sequence for item in original], reverse=True
    )
    inserted = {f"later-{i}" for i in range(3)}
    assert not inserted & {item.execution_id for item in original}
    fresh = history.history(OWNER, limit=20)
    current = list(fresh.items)
    cursor = fresh.next_cursor
    while cursor is not None:
        tail = history.history(OWNER, limit=20, cursor=cursor)
        current.extend(tail.items)
        cursor = tail.next_cursor
    assert len(current) == 260
    assert {item.execution_id for item in current} == (
        {item.execution_id for item in original} | inserted
    )
    history.results(OWNER, "run-0", limit=20)
    assert history.history(OWNER, limit=20) == fresh
    with pytest.raises(ValueError):
        history.history("bob", cursor=first.next_cursor)


def test_unavailable_never_commits_zero_success(tmp_path):
    unavailable=executed().model_copy(update={'status':'unavailable','base_count':None,'total':None,'rows':[]})
    control,history=service(tmp_path,unavailable)
    receipt=control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    assert receipt.status.value=='failed'
    row=history.detail(OWNER,'run-1')
    assert row.total is None and row.status!='succeeded'
    assert history.results(OWNER,'run-1') is None


def test_legacy_public_parser_cannot_accept_private_execute():
    from rquant.page_control import parse_page_control_command
    with pytest.raises(ValueError): parse_page_control_command(command().model_dump(mode='json'))


def test_registered_but_unconfirmed_execution_shows_processing_without_counts(tmp_path):
    control,history=service(tmp_path)
    from rquant.screen.query_contracts import _OwnedExecuteScreenQuery
    owned=_OwnedExecuteScreenQuery(**command().model_dump(),owner_id=OWNER)
    control.outbox.enqueue_trusted_screen_query(owned)
    control.outbox.claim_records(limit=1,owner_id='test-consumer',lease_seconds=30,now=NOW,target_command_id=owned.command_id)
    row=history.detail(OWNER,'run-1')
    assert row.status=='processing' and row.total is None


def test_actual_run_complete_is_not_first_page_or_browser_accumulation(tmp_path):
    import pandas as pd
    from types import SimpleNamespace
    from rquant.web.screen_service import ScreenApplicationService
    from rquant.web.models.screen import ScreenRunRequest
    from rquant.web import readers
    frame=pd.DataFrame({'trade_date':[date(2026,9,30)]*151,'ts_code':[f'{i:06}.SZ' for i in range(151)],'name':['示例']*151,'CLOSE[0]':[11.0]*151,'PCT_CHG[0]':[1.0]*151})
    class Cursor:
        description=[(name,) for name in frame.columns]
        def execute(self,sql,params=None): return self
        def fetchdf(self): return frame.copy()
    borrowed=SimpleNamespace(cursor=Cursor(),manifest=SimpleNamespace(generation_id='a'*64,built_at=NOW-timedelta(days=1)))
    service=ScreenApplicationService(cursor_key=b'x'*32)
    assert hasattr(service,'run_complete'), 'complete execution path is absent'
    from unittest.mock import patch
    with patch.object(readers,'table_states',return_value={'nl_screen_universe':SimpleNamespace(available=True)}):
        complete=service.run_complete(definition().model_copy(update={'source_kind':'serving'}),borrowed=borrowed,serving_unavailable=False)
        first=service.run(ScreenRunRequest(trade_date=definition().trade_date,conditions=[{'key':'gt','args':{'left':'CLOSE[0]','right':10}}],page_size=20,source_identity='a'*64),borrowed=borrowed,serving_unavailable=False)
    assert complete.total==151 and len(complete.rows)==151 and complete.next_cursor is None
    assert complete.rows[:20]==first.rows and first.total==151 and first.next_cursor


def test_result_artifact_and_counts_do_not_become_success_when_transaction_fails(tmp_path):
    control,history=service(tmp_path)
    with control.outbox._connect() as connection:
        connection.execute("CREATE TRIGGER fail_screen_effect BEFORE INSERT ON page_control_effect BEGIN SELECT RAISE(ABORT,'synthetic commit failure'); END")
    with pytest.raises(Exception): control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    row=history.detail(OWNER,'run-1')
    assert row.status=='processing' and row.total is None and row.artifact_sha256 is None
    with control.outbox._connect() as connection:
        assert connection.execute('SELECT count(*) FROM page_control_effect').fetchone()[0]==0
    assert history.results(OWNER,'run-1') is None


def test_mutated_artifact_is_not_read_back_as_original_success(tmp_path):
    control,history=service(tmp_path)
    control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    original=history.detail(OWNER,'run-1')
    file=history.artifact_root/(original.artifact_sha256+'.json')
    file.write_bytes(file.read_bytes().replace(b'11.0',b'99.0'))
    with pytest.raises(ValueError): history.results(OWNER,'run-1')


def test_source_replacement_during_recovery_never_uses_new_source(tmp_path):
    control,history=service(tmp_path)
    from rquant.screen.query_contracts import _OwnedExecuteScreenQuery
    owned=_OwnedExecuteScreenQuery(**command().model_dump(),owner_id=OWNER)
    control.outbox.enqueue_trusted_screen_query(owned)
    from rquant.web.screen_service import ScreenApplicationError
    def expired(_: object): raise ScreenApplicationError(409,'source changed')
    control.consumer.screen_query_executor=expired
    receipt=control._resume_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    assert receipt.status.value=='failed' and receipt.result['code']=='source_expired'
    assert history.detail(OWNER,'run-1').total is None


def test_private_definition_rejects_nested_extra_and_hashes_normalized_plan(tmp_path):
    value=definition().model_dump(mode='json');value['conditions'][0]['owner_id']='bob'
    with pytest.raises(ValidationError): contracts().ScreenQueryDefinition.model_validate(value)
    control,history=service(tmp_path)
    first=control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    second=command('run-2').model_copy(update={'definition':definition().model_copy(update={'description':'另一种说法'})})
    control._submit_trusted_screen_query(second,authenticated_actor_id=OWNER)
    assert history.detail(OWNER,'run-1').plan_hash==history.detail(OWNER,'run-2').plan_hash


def test_original_service_builder_explicitly_wires_private_history_without_legacy_path(tmp_path):
    from rquant.page_control_service import build_page_control_service
    tmp_path.chmod(0o700)
    control=build_page_control_service(outbox_path=tmp_path/'commands.sqlite',data_dir=tmp_path/'data',log_dir=tmp_path/'logs',allowed_lab_export_roots=(),load_default_lab_backend=False,clock=lambda:NOW,screen_query_executor=lambda _:executed(),screen_query_cursor_key=b'x'*32)
    receipt=control._submit_trusted_screen_query(command(),authenticated_actor_id=OWNER)
    assert receipt.status.value=='succeeded'
    assert control.outbox.path.stat().st_mode & 0o777==0o600
    assert len(control.consumer.screen_query_history.history(OWNER).items)==1


def test_new_device_reads_exact_original_command_and_server_execution_time(tmp_path: Path) -> None:
    control, history = service(tmp_path)
    original = command().model_copy(update={"requested_at": NOW - timedelta(days=30), "page_size": 37})
    control._submit_trusted_screen_query(original, authenticated_actor_id=OWNER)
    reopened = importlib.import_module('rquant.screen.query_history').ScreenQueryHistory(control.outbox, cursor_key=b'x'*32)
    entry = reopened.detail(OWNER, original.command_id)
    assert entry.original_command == original
    assert entry.started_at == NOW and entry.completed_at == NOW
    assert entry.started_at != original.requested_at


@pytest.mark.parametrize(
    "factory_name",
    ["build_page_control_service", "build_page_control_service_with_dependencies"],
)
def test_original_factory_keeps_paper_control_and_private_history_on_same_outbox(
    tmp_path: Path, factory_name: str
) -> None:
    from rquant import page_control_service
    from rquant.screen.query_history import prepare_private_screen_outbox
    from tests.unit.test_paper_portfolio_page_control import (
        paused_request,
        service_for,
        submit,
    )

    tmp_path.chmod(0o700)
    root_dir = tmp_path / "paper"
    root_dir.mkdir(mode=0o700)
    prepare_private_screen_outbox(root_dir / "journal.sqlite")
    old_control, backend, operator, _ = service_for(root_dir)
    factory = getattr(page_control_service, factory_name)
    control = factory(
        outbox_path=old_control.outbox.path,
        data_dir=tmp_path / "private-data",
        log_dir=tmp_path / "private-logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        paper_portfolio_backend=backend,
        screen_query_executor=lambda _: executed(),
        screen_query_cursor_key=b"x" * 32,
        clock=lambda: NOW,
    )
    assert control.outbox.path == old_control.outbox.path
    assert control.consumer.paper_portfolio_backend is backend
    original = command("joint-private-run")
    screen_receipt = control._submit_trusted_screen_query(
        original, authenticated_actor_id=OWNER
    )
    assert screen_receipt.status.value == "succeeded"
    history = control.consumer.screen_query_history
    assert history.detail(OWNER, original.command_id).total == 21
    assert history.detail("bob", original.command_id) is None
    value = paused_request(operator)
    confirmation = backend.prepare_confirmation(
        value, authenticated_actor_id=OWNER, expected_identity=operator.state.identity()
    )
    paper_receipt = submit(
        control, backend, value, confirmation_id=confirmation.confirmation_id
    )
    assert paper_receipt.status.value == "succeeded"
    assert operator.current().status == "waiting"
    control.consumer.screen_query_executor = lambda _: pytest.fail("original run repeated")
    assert control._resume_trusted_screen_query(
        original, authenticated_actor_id=OWNER
    ) == screen_receipt
    assert control._resume_trusted_paper_portfolio(
        value, authenticated_actor_id=OWNER
    ) == paper_receipt
    with pytest.raises(PermissionError):
        control._lookup_trusted_paper_portfolio(value, authenticated_actor_id="bob")
    with control.outbox._connect() as connection:
        assert connection.execute("SELECT count(*) FROM page_control_command").fetchone()[0] == 2
    assert len(history.history(OWNER).items) == 1
