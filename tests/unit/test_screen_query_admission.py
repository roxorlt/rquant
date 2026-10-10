from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.unit.test_screen_query_history import OWNER,NOW,command,definition,service


def models():
    assert importlib.util.find_spec('rquant.web.models.screen_history') is not None, 'typed private query actions are absent'
    return importlib.import_module('rquant.web.models.screen_history')


def admission():
    assert importlib.util.find_spec('rquant.screen.query_admission') is not None, 'trusted private query admission is absent'
    return importlib.import_module('rquant.screen.query_admission')


def test_private_action_returns_server_facts_without_internal_owner(tmp_path):
    control,history=service(tmp_path)
    action=models().ScreenExecuteAction(command=command())
    result=admission().dispatch_screen_query_action(control,authenticated_actor_id=OWNER,allowed_users=frozenset({OWNER}),action=action)
    assert result.receipt.status.value=='succeeded'
    read=admission().dispatch_screen_query_action(control,authenticated_actor_id=OWNER,allowed_users=frozenset({OWNER}),action=models().ScreenHistoryAction())
    assert read.history.items[0].total==21
    assert 'owner_id' not in read.model_dump_json()
    with pytest.raises(ValueError): admission().dispatch_screen_query_action(control,authenticated_actor_id='bob',allowed_users=frozenset({OWNER}),action=action)


def test_private_preset_cas_and_name_collision_preserve_original_legacy_protocol(tmp_path):
    control,history=service(tmp_path)
    m=models();a=admission()
    body=m.ScreenPresetSaveRequest(command_id='save-1',requested_at=NOW,preset={'preset_id':'preset-1','name':'我的条件','definition':definition().model_dump(mode='json')})
    result=a.dispatch_screen_query_action(control,authenticated_actor_id=OWNER,allowed_users=frozenset({OWNER}),action=m.ScreenPresetSaveAction(request=body))
    assert result.receipt.status.value=='succeeded'
    assert history.presets(OWNER)[0].version==1 and history.presets('bob')==()
    collision=body.model_copy(update={'command_id':'save-2','preset':body.preset.model_copy(update={'preset_id':'preset-2'})})
    failed=a.dispatch_screen_query_action(control,authenticated_actor_id=OWNER,allowed_users=frozenset({OWNER}),action=m.ScreenPresetSaveAction(request=collision))
    assert failed.receipt.result['code']=='version_conflict'
    replacement=body.model_copy(update={'command_id':'save-3','expected_version':1,'preset':body.preset.model_copy(update={'name':'改名'})})
    updated=a.dispatch_screen_query_action(control,authenticated_actor_id=OWNER,allowed_users=frozenset({OWNER}),action=m.ScreenPresetSaveAction(request=replacement))
    assert updated.receipt.result['version']==2
    assert not (tmp_path/'data/user_presets/preset-1.json').exists()
    from rquant.page_control import SaveNlPreset,AppendNlQueryLog,parse_page_control_command
    legacy=SaveNlPreset(command_id='legacy',requested_at=NOW,name='legacy',rule_calls=definition().conditions)
    assert parse_page_control_command(legacy.model_dump(mode='json'))==legacy
    log=AppendNlQueryLog(command_id='legacy-log',requested_at=NOW,query='旧描述',outcome='success')
    assert parse_page_control_command(log.model_dump(mode='json'))==log


def test_same_uid_private_server_is_rejected_before_socket_access(tmp_path):
    with pytest.raises(ValueError,match='distinct'):
        admission().ScreenQueryPrivateServer(Path('/private/tmp/rq-screen-test/query.sock'),allowed_users=frozenset({OWNER}),trusted_web_uid=os.geteuid(),shared_gid=os.getegid(),control=None)


def test_public_preset_body_rejects_owner_outcome_or_browser_counts():
    m=models()
    for extra in [{'owner_id':'alice'},{'outcome':'success'},{'total':99}]:
        with pytest.raises(ValidationError): m.ScreenPresetSaveRequest(command_id='save',requested_at=NOW,preset={'preset_id':'preset','name':'常用','definition':definition().model_dump(mode='json')},**extra)


def test_screen_private_config_is_explicit_and_rejects_shared_identity():
    a=admission()
    assert hasattr(a,'ScreenQueryPrivateConfig'), 'private role config is absent'
    with pytest.raises(ValidationError): a.ScreenQueryPrivateConfig(socket_path='/private/tmp/rq-screen/q.sock',serving_root='/private/tmp/rq-screen-source',allowed_users=[OWNER],trusted_web_uid=os.geteuid(),shared_gid=os.getegid())
    from rquant.page_control_service import build_parser
    parsed=build_parser().parse_args(['--manifest','/private/tmp/screen-role.json','--control-root','/private/tmp/screen-control','--expected-commit','a'*40,'--expected-generation','a'*64,'--screen-query-config','/private/tmp/screen-private.json'])
    assert parsed.screen_query_config==Path('/private/tmp/screen-private.json')
