from __future__ import annotations

import importlib
import importlib.util
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response
from starlette.requests import Request

from tests.unit.test_screen_query_history import OWNER,command,service


def routes():
    assert importlib.util.find_spec('rquant.web.routes.screen_history') is not None, 'private screen API is absent'
    return importlib.import_module('rquant.web.routes.screen_history')


def test_private_history_route_uses_actor_and_no_store(tmp_path):
    control,history=service(tmp_path)
    from rquant.screen.query_admission import dispatch_screen_query_action
    class Client:
        def request(self,action,*,authenticated_actor_id):
            return dispatch_screen_query_action(control,authenticated_actor_id=authenticated_actor_id,allowed_users=frozenset({OWNER,'bob'}),action=action)
    request=Request({'type':'http','app':SimpleNamespace(state=SimpleNamespace(web=SimpleNamespace(screen_query_client=Client(),settings=SimpleNamespace(screen_query_users=frozenset({OWNER,'bob'}))))),'headers':[]})
    r=routes();response=Response()
    saved=r.execute_screen_query(request,response,command(),OWNER,None)
    assert saved.receipt.status.value=='succeeded' and response.headers['Cache-Control']=='no-store'
    rows=r.get_screen_history(request,Response(),OWNER,20,None)
    assert len(rows.history.items)==1 and rows.history.items[0].total==21
    assert r.get_screen_history(request,Response(),'bob',20,None).history.items==()
    with pytest.raises(HTTPException) as e: r.get_screen_execution(request,Response(),'run-1','bob')
    assert e.value.status_code==404


def test_private_routes_auth_csrf_and_lookup_original_are_registered():
    r=routes()
    paths={route.path:route for route in r.router.routes}
    assert set(paths)=={'/screen/query/history','/screen/query/presets','/screen/query/executions/{execution_id}','/screen/query/executions/{execution_id}/results','/screen/query/execute','/screen/query/lookup','/screen/query/resume','/screen/query/presets/save'}
    for route in paths.values():
        calls=[d.call.__name__ for d in route.dependant.dependencies]
        assert 'require_current_user' in calls
        if 'POST' in route.methods: assert 'require_csrf' in calls


def test_app_registers_private_query_endpoints_without_starting_runtime(tmp_path):
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    app=create_app(WebSettings(serving_root=tmp_path/'serving'),background=False)
    assert '/api/v1/screen/query/history' in set(app.openapi()['paths'])


def test_private_screen_settings_require_peer_and_proof_together(tmp_path):
    from pydantic import ValidationError
    from rquant.web.settings import WebSettings
    with pytest.raises(ValidationError): WebSettings(serving_root=tmp_path/'serving',screen_query_users=frozenset({'alice'}))


def test_private_validation_does_not_echo_query_or_actor(tmp_path):
    from fastapi.exceptions import RequestValidationError
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    app=create_app(WebSettings(serving_root=tmp_path/'serving'),background=False)
    req=Request({'type':'http','path':'/api/v1/screen/query/execute','headers':[]})
    error=RequestValidationError([{'type':'extra_forbidden','loc':('body','owner_id'),'msg':'extra','input':'PRIVATE_MARKER'}])
    coroutine=app.exception_handlers[RequestValidationError](req,error)
    try: coroutine.send(None)
    except StopIteration as done: response=done.value
    else: coroutine.close();pytest.fail('validation handler unexpectedly needs runtime')
    assert response.status_code==422
    assert b'PRIVATE_MARKER' not in response.body
    assert response.headers['Cache-Control']=='no-store'
