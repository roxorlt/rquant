"""Export synthetic public response bytes for the related React fixtures."""

import json
from pathlib import Path

from tests.support.web_proxy_identity import ProofTestClient
from tests.unit.test_paper_portfolio_web import PREFIX, _WRITERS, setup


def test_export_public_paper_fixture(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    ctx = setup(tmp_path)
    try:
        with ProofTestClient(ctx.app, headers={"x-rquant-user": "alice"}) as client:
            responses = {"catalog": client.get(PREFIX),
                         "detail": client.get(f"{PREFIX}/{ctx.request.account_id}"),
                         "history": client.get(f"{PREFIX}/{ctx.request.account_id}/history")}
            assert all(response.status_code == 200 for response in responses.values())
            body = {key: response.json() for key, response in responses.items()}
            assert "st_ino" not in json.dumps(body)
            (root / "web/src/pages/paper/paperPortfolio.fixture.json").write_text(
                json.dumps(body, ensure_ascii=False, indent=2) + "\n")
    finally:
        while _WRITERS:
            _WRITERS.pop().close()
    print("PUBLIC_FIXTURE_FROM_ACTUAL_SYNTHETIC_WEB=True; fixture_writers_closed=True")
