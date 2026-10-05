"""Original S4 receives a complete finite graph, never detached account rows."""

from pathlib import Path

import pytest

from rquant.serving_read_models import ServingProjectionInput, ServingReadModelInput
from tests.unit.test_paper_portfolio_ledger_views import filled, ledger_source
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, _signal


def publication(tmp_path: Path):
    from rquant.paper_portfolio_projection import PaperPortfolioPublishedAccount, PaperPortfolioSnapshot

    broker, basis, operator, runtime = filled(tmp_path)
    raw = runtime.materials.read_for(_signal(), decision_at=EXECUTION_TIME)
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME,
                                     prices={item.ts_code: item.valuation_price for item in raw.facts})
    return PaperPortfolioSnapshot(available_at=EXECUTION_TIME, accounts=(PaperPortfolioPublishedAccount(
        configuration=basis.configuration, operator=operator.current(), frame=frame, market_material=raw,
        status="complete", nav=()),))


def test_original_serving_allowlist_accepts_exact_paper_graph(tmp_path: Path) -> None:
    from rquant.paper_portfolio_projection import paper_portfolio_projections, validate_paper_portfolio_projections
    from rquant.runtime_serving_snapshot import PaperAccountsPayload

    value = publication(tmp_path)
    tables = paper_portfolio_projections(value)
    assert validate_paper_portfolio_projections({item.table_name: item for item in tables}) == value
    payload = PaperAccountsPayload(paper_accounts=(value.accounts[0].frame.account,), projections=tables)
    bound = tuple(ServingProjectionInput.bind(item, owner_dataset_id="paper_accounts", owner_generation_id="a"*64) for item in tables)
    assert ServingReadModelInput(observed_at=EXECUTION_TIME, projections=bound).projections == bound
    assert payload.projections == tables


def test_original_owner_payload_and_s4_reject_detached_or_mixed_graph(tmp_path: Path) -> None:
    from rquant.paper_portfolio_projection import paper_portfolio_projections
    from rquant.runtime_serving_snapshot import PaperAccountsPayload

    value = publication(tmp_path)
    tables = paper_portfolio_projections(value)
    for omitted in range(3):
        selected = tuple(item for index, item in enumerate(tables) if index != omitted)
        with pytest.raises(ValueError):
            PaperAccountsPayload(paper_accounts=(value.accounts[0].frame.account,), projections=selected)
        with pytest.raises(ValueError):
            ServingReadModelInput(observed_at=EXECUTION_TIME, projections=tuple(ServingProjectionInput.bind(item, owner_dataset_id="paper_accounts", owner_generation_id="a"*64) for item in selected))
    bound = tuple(ServingProjectionInput.bind(item, owner_dataset_id="paper_accounts", owner_generation_id=("a" if index != 2 else "b")*64) for index, item in enumerate(tables))
    with pytest.raises(ValueError):
        ServingReadModelInput(observed_at=EXECUTION_TIME, projections=bound)


def test_chunk_swap_or_wrong_account_index_refuses_even_with_correct_size(tmp_path: Path) -> None:
    from rquant.paper_portfolio_projection import paper_portfolio_projections, validate_paper_portfolio_projections
    from rquant.serving_read_models import ServingProjectionPayload

    value = publication(tmp_path)
    tables = {item.table_name: item for item in paper_portfolio_projections(value)}
    index = tables["paper_portfolio_account"]
    corrupt = ServingProjectionPayload(table_name=index.table_name, available_at=index.available_at,
                                      rows=tuple({**dict(row), "owner_id": "bob"} for row in index.rows))
    with pytest.raises(ValueError):
        validate_paper_portfolio_projections({**tables, index.table_name: corrupt})
    raw = tables["paper_portfolio_material"]
    corrupt = ServingProjectionPayload(table_name=raw.table_name, available_at=raw.available_at,
                                      rows=tuple({**dict(row), "payload": str(row["payload"]).replace("alice", "xxxxx")} for row in raw.rows))
    with pytest.raises(ValueError):
        validate_paper_portfolio_projections({**tables, raw.table_name: corrupt})
