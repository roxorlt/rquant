"""Private action checks without opening a socket or replacing the actual budget owner."""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import httpx
import pytest

from tests.unit.test_ai_assistance import _owner, _response


def test_private_admission_rejects_unlisted_owner_before_provider(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_assistance_admission")
    from rquant.web.models.ai_assistance import AIGenerateAction
    calls = []
    owner, request, adapter = _owner(tmp_path, lambda request: calls.append(request))
    admission = module.AIAssistanceAdmission(owner=owner, allowed_users=frozenset({"researcher"}))
    with pytest.raises(module.AIAdmissionRejected):
        admission.dispatch(AIGenerateAction(request=request), authenticated_actor_id="other")
    assert calls == [] and not owner.outbox.ai_usage_request_exists(request.request_id)
    adapter.close()


def test_private_config_is_off_by_default_and_uses_distinct_original_peer(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_assistance_admission")
    config = module.AIPrivateConfig(socket_path=Path("/private/tmp/unused-ai/socket"), trusted_web_uid=os.geteuid() + 1, shared_gid=os.getegid(), allowed_users=frozenset({"researcher"}), account_id="shared", model_id="gpt-test")
    assert config.daily_limit == 0 and config.api_key_file is None
    with pytest.raises(ValueError):
        module.AIPrivateConfig.model_validate({**config.model_dump(), "trusted_web_uid": os.geteuid()})
    with pytest.raises(ValueError):
        module.AIPrivateConfig.model_validate({**config.model_dump(), "daily_limit": True})
    with pytest.raises(ValueError):
        module.AIPrivateConfig.model_validate({**config.model_dump(), "daily_limit": 1})


def test_private_lookup_and_usage_keep_original_request_and_private_counts(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_assistance_admission")
    from rquant.web.models.ai_assistance import AIGenerateAction, AILookupAction, AIUsageAction
    import json
    raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]}
    owner, request, adapter = _owner(tmp_path, lambda request: httpx.Response(200, json=_response(usage={"prompt_tokens": 8, "completion_tokens": 3}, arguments=json.dumps(raw))))
    admission = module.AIAssistanceAdmission(owner=owner, allowed_users=frozenset({"researcher", "other"}))
    generated = admission.dispatch(AIGenerateAction(request=request), authenticated_actor_id="researcher")
    looked = admission.dispatch(AILookupAction(original=request), authenticated_actor_id="researcher")
    assert looked == generated
    with pytest.raises(LookupError):
        admission.dispatch(AILookupAction(original=request), authenticated_actor_id="other")
    usage = AIUsageAction(start_date=request.trade_date, end_date=request.trade_date)
    assert admission.dispatch(usage, authenticated_actor_id="researcher").usage.summary.calls == 1
    assert admission.dispatch(usage, authenticated_actor_id="other").usage.summary.calls == 0
    adapter.close()


def test_original_page_control_factory_installs_same_budget_owner(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_assistance_admission")
    from rquant.page_control_service import build_page_control_service_with_dependencies
    owner, _, adapter = _owner(tmp_path, lambda request: httpx.Response(200, json=_response()))
    config = module.AIPrivateConfig(socket_path=Path("/private/tmp/unused-ai/socket"), trusted_web_uid=os.geteuid() + 1, shared_gid=os.getegid(), allowed_users=frozenset({"researcher"}), account_id="shared", model_id="gpt-test")
    service = build_page_control_service_with_dependencies(outbox_path=tmp_path / "factory.sqlite3", data_dir=tmp_path / "data", log_dir=tmp_path / "logs", allowed_lab_export_roots=(tmp_path / "exports",), load_default_lab_backend=False, screen_query_executor=owner.contexts.screen, screen_query_cursor_key=owner.contexts.screen.cursor_key, ai_config=config, ai_provider=adapter)
    assert service.ai_assistance.outbox is service.outbox
    assert service.ai_assistance.contexts.screen is owner.contexts.screen
    assert not service.ai_assistance.capabilities("researcher").can_generate
    adapter.close()
