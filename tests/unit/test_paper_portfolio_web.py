"""Private paper Web consumes one published graph and the original journal."""

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.paper_operator_commands import SetPaperAccountPaused
from rquant.paper_portfolio_projection import PaperPortfolioSnapshot, paper_portfolio_projections
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import ServingProjectionInput, ServingReadModelInput, SERVING_TABLE_SPECS, build_serving_read_models
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import PROXY_HEADERS, ProofTestClient, with_test_proxy_identity
from tests.support.web_serving_fixture import _generation_ids, _watermarks
from tests.unit.test_paper_research_submission import fixture
from tests.unit.test_paper_signal_worker import EXECUTION_TIME

PREFIX = "/api/v1/paper-portfolios"
WRITE = {"x-rquant-user": "alice", "x-rquant-csrf": "1", **PROXY_HEADERS}
_WRITERS = []


@pytest.fixture(autouse=True)
def close_own_fixture_writers():
    try:
        yield
    finally:
        while _WRITERS:
            _WRITERS.pop().close()


def setup(tmp_path):
    from rquant.paper_portfolio_admission import PaperPortfolioAdmission
    from rquant.web.paper_portfolio_reader import PaperPortfolioServingSource
    page, backend, runtime, request, jobs = fixture(tmp_path)
    source = backend.research_backend.preparer.source_for(request.account_id, "alice")
    _WRITERS.append(source.broker._connect())
    root = tmp_path/"serving"
    generations = _generation_ids("baseline", 0)
    view = source.read(as_of=EXECUTION_TIME)
    snapshot = PaperPortfolioSnapshot(available_at=EXECUTION_TIME, accounts=(view,))
    tables = build_serving_read_models(ServingReadModelInput(observed_at=EXECUTION_TIME, paper_accounts=(view.frame.account,),
                projections=tuple(ServingProjectionInput.bind(p, owner_dataset_id="paper_accounts", owner_generation_id=generations["paper_accounts"])
                                  for p in paper_portfolio_projections(snapshot))))
    manifest = ServingPublisher(root, producer_commit="0"*40, schema_version=3, table_specs=SERVING_TABLE_SPECS).publish(
        tables, watermarks=tuple(mark.model_copy(update={"event_time": min(mark.event_time, EXECUTION_TIME), "published_at": min(mark.published_at, EXECUTION_TIME)})
                                for mark in _watermarks("baseline", built_at=EXECUTION_TIME, generations=generations, sequence=0)), source_generations=generations, built_at=EXECUTION_TIME)
    backend.admission_source = PaperPortfolioServingSource(root, clock=lambda: EXECUTION_TIME)
    admission = PaperPortfolioAdmission(page, backend=backend)
    raw = with_test_proxy_identity(WebSettings(serving_root=root)).model_dump()
    settings = WebSettings.model_validate({**raw, "paper_portfolio_enabled": True, "paper_portfolio_users": {"alice"}})
    app = create_app(settings, paper_portfolio_gateway=admission, clock=lambda: EXECUTION_TIME, background=False)
    from rquant.web.paper_portfolio_reader import read_paper_portfolios
    app.state.web.tracker.refresh()
    with app.state.web.tracker.borrow() as borrowed:
        state_row=borrowed.cursor.execute("SELECT * FROM paper_portfolio_state").fetchone()
        print("PAPER_READER_STATE",state_row,"ORIGINAL_TIME",snapshot.available_at.isoformat(),"ORIGINAL_HASH",snapshot.fingerprint)
        assert read_paper_portfolios(borrowed) is not None
    return SimpleNamespace(app=app, runtime=runtime, request=request.model_copy(update={"generation_id": manifest.generation_id}),
                           manifest=manifest, page=page, backend=backend, admission=admission, view=view, jobs=jobs)


def test_paper_settings_default_closed_and_require_verified_private_identity(tmp_path):
    settings = WebSettings(serving_root=tmp_path)
    assert settings.paper_portfolio_enabled is False and settings.paper_portfolio_users == frozenset()
    with pytest.raises(ValueError, match="private"):
        WebSettings(serving_root=tmp_path, paper_portfolio_enabled=True, paper_portfolio_users={"alice"})


def test_owner_reads_are_same_generation_and_omit_private_metadata(tmp_path):
    ctx = setup(tmp_path)
    with ProofTestClient(ctx.app, headers={"x-rquant-user": "alice"}) as client:
        response = client.get(PREFIX)
        assert response.status_code == 200 and len(response.json()["data"]["accounts"]) == 1
        assert "instance_id" not in response.text and "st_ino" not in response.text and str(ctx.runtime.state.path) not in response.text
        detail = client.get(f"{PREFIX}/{ctx.request.account_id}")
        assert detail.status_code == 200 and Decimal(detail.json()["data"]["account"]["cash"]) == 195
        history = client.get(f"{PREFIX}/{ctx.request.account_id}/history")
        assert history.status_code == 200 and history.json()["data"]["coverage"] == "complete" and history.json()["data"]["total_orders"] == 1
        assert client.get(PREFIX, params={"generation_id": "b"*64}).status_code == 409
        assert client.get(PREFIX, headers={"x-rquant-user": "bob"}).json()["data"]["accounts"] == []
        assert client.get(f"{PREFIX}/{ctx.request.account_id}", headers={"x-rquant-user": "bob"}).status_code == 404
        assert client.get(f"{PREFIX}/{ctx.request.account_id}/research/{uuid4()}").status_code == 404
        assert client.get(f"{PREFIX}/{ctx.request.account_id}/research/{uuid4()}/download").status_code == 503


def test_original_run_recovery_precedes_generation_and_does_not_duplicate_lab_queue(tmp_path, monkeypatch):
    ctx = setup(tmp_path)
    with ProofTestClient(ctx.app, headers=WRITE) as client:
        response = client.post(f"{PREFIX}/{ctx.request.account_id}/reconcile", json=ctx.request.model_dump(mode="json"))
        assert response.status_code == 200 and response.json()["data"]["status"] == "submitted"
        ctx.runtime.state.start_configuration(ctx.runtime.state.configuration.model_copy(update={"version": 2, "configured_at": EXECUTION_TIME+timedelta(minutes=1)}))
        monkeypatch.setattr(ctx.backend.research_backend.preparer, "prepare", lambda *_a, **_k: pytest.fail("original UUID must never recompile"))
        response = client.post(f"{PREFIX}/{ctx.request.account_id}/recover", json=ctx.request.model_dump(mode="json"))
        assert response.status_code == 200 and response.json()["data"]["status"] == "submitted"
        assert len(ctx.backend.research_backend.facade.spool.pending()) == 1
        changed = ctx.request.model_copy(update={"command_id": str(uuid4()), "generation_id": "b"*64})
        assert client.post(f"{PREFIX}/{ctx.request.account_id}/reconcile", json=changed.model_dump(mode="json")).status_code == 409


def test_pause_is_real_two_step_and_known_invalid_or_unauthenticated_request_has_no_effect(tmp_path):
    ctx = setup(tmp_path)
    current = ctx.runtime.operator.current()
    request = SetPaperAccountPaused(command_id=str(uuid4()), requested_at=EXECUTION_TIME, generation_id=ctx.manifest.generation_id,
                account_id=ctx.request.account_id, configuration_fingerprint=ctx.request.configuration_fingerprint,
                expected_sequence=current.sequence, expected_paused=current.paused, paused=not current.paused)
    with ProofTestClient(ctx.app, headers=WRITE) as client:
        prepared = client.post(f"{PREFIX}/{request.account_id}/pause/prepare", json=request.model_dump(mode="json"))
        assert prepared.status_code == 200
        challenge = prepared.json()["data"]
        assert ctx.page.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id="alice") is None
        confirmed = client.post(f"{PREFIX}/{request.account_id}/pause/confirm", json={"request": request.model_dump(mode="json"), "confirmation_id": challenge["confirmation_id"]})
        assert confirmed.status_code == 200 and confirmed.json()["data"]["status"] == "waiting_application"
        assert ctx.runtime.operator.current().sequence == current.sequence
        assert client.post(f"{PREFIX}/{request.account_id}/reconcile", json=ctx.request.model_dump(mode="json"), headers={**WRITE,"x-rquant-csrf":"0"}).status_code == 403
        bad = ctx.request.model_dump(mode="json"); bad["owner_id"]="bob"
        assert client.post(f"{PREFIX}/{request.account_id}/reconcile", json=bad).status_code == 422


def test_configuration_creates_one_version_and_body_caps_reject_before_typed_decode(tmp_path):
    from rquant.paper_operator_commands import SavePaperPortfolioConfiguration
    ctx=setup(tmp_path)
    config=ctx.runtime.state.configuration
    command=SavePaperPortfolioConfiguration(command_id=str(uuid4()),requested_at=EXECUTION_TIME,generation_id=ctx.manifest.generation_id,
        account_id=ctx.request.account_id,expected_configuration_fingerprint=config.fingerprint,
        weight_rule=config.weight_rule.model_copy(update={"max_positions":2}),drawdown_rule=config.drawdown_rule)
    with ProofTestClient(ctx.app,headers=WRITE) as client:
        result=client.post(f"{PREFIX}/{command.account_id}/configuration",json=command.model_dump(mode="json"))
        assert result.status_code==200 and result.json()["data"]["status"]=="waiting_publication"
        assert ctx.runtime.state.configuration.version==2
        assert client.post(f"{PREFIX}/{command.account_id}/recover",json=command.model_dump(mode="json")).json()["data"]["configuration_version"]==2
        assert ctx.runtime.state.configuration.version==2
        for suffix,cap in (("configuration",16384),("recover",16384),("pause/prepare",4096),("pause/confirm",4096),("reconcile",4096),("band",4096)):
            assert client.post(f"{PREFIX}/{command.account_id}/{suffix}",content='{"invalid":"'+'x'*cap+'"}',headers={**WRITE,"content-type":"application/json"}).status_code==413


def test_complete_history_keeps_original_financial_fields_and_server_chinese_labels(tmp_path):
    ctx = setup(tmp_path)
    with ProofTestClient(ctx.app, headers={"x-rquant-user": "alice"}) as client:
        response = client.get(f"{PREFIX}/{ctx.request.account_id}/history")
        assert response.status_code == 200
        row = response.json()["data"]["records"][0]
        assert row["side_label"] == "买入" and row["status_label"] == "已成交"
        assert row["reject_message"] is None and row["order"]["filled_quantity"] == 800
        assert Decimal(row["fills"][0]["price"]) == 1


def test_configuration_application_reopens_published_control_actions(tmp_path):
    from rquant.paper_operator_commands import SavePaperPortfolioConfiguration
    from rquant.web.routes.paper_portfolio import _item
    from tests.unit.test_paper_portfolio_view_source import market
    ctx = setup(tmp_path)
    config = ctx.runtime.state.configuration
    command = SavePaperPortfolioConfiguration(command_id=str(uuid4()), requested_at=EXECUTION_TIME,
        generation_id=ctx.manifest.generation_id, account_id=ctx.request.account_id,
        expected_configuration_fingerprint=config.fingerprint,
        weight_rule=config.weight_rule.model_copy(update={"max_positions": 2}), drawdown_rule=config.drawdown_rule)
    with ProofTestClient(ctx.app, headers=WRITE) as client:
        assert client.post(f"{PREFIX}/{command.account_id}/configuration", json=command.model_dump(mode="json")).status_code == 200
    source = ctx.backend.research_backend.preparer.source_for(command.account_id, "alice")
    market(ctx.runtime)
    waiting = source.read(as_of=EXECUTION_TIME)
    assert waiting.operator.paused and not _item(waiting, True).can_pause
    applied = ctx.runtime.operator.apply(observed_at=EXECUTION_TIME)
    published = source.read(as_of=EXECUTION_TIME)
    assert published.configuration.version == 2 and published.operator == applied
    assert applied.status == "applied" and not applied.paused and _item(published, True).can_pause
