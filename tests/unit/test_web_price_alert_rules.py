from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from rquant.alert_price_rule import PriceAlertRule
from rquant.price_alert_rule_store import PriceAlertRuleEntry
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    build_manual_watchlist_projections,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleAuthoritySnapshot,
    PriceAlertRuleProjectionRow,
    build_price_alert_rule_projections,
)
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import (
    ProofTestClient,
    create_proof_test_app,
    with_test_proxy_identity,
)
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

PATH = "/api/v1/monitor/price-rules"
NOW = FIXTURE_BUILT_AT + timedelta(minutes=5)
HEADERS = {"x-rquant-user": "alice"}
WRITE_HEADERS = {**HEADERS, "x-rquant-csrf": "1", "origin": "http://testserver"}


def member(
    owner: str = "alice", *, version: int = 1, deleted: bool = False, expiry: datetime | None = None
) -> ManualWatchlistProjectionRow:
    return ManualWatchlistProjectionRow(
        owner_id=owner,
        ts_code="600001.SH",
        version=version,
        deleted=deleted,
        source=None if deleted else "detail",
        price_levels_json="[]",
        expires_at=None if deleted else expiry,
        updated_at=None if deleted else FIXTURE_BUILT_AT - timedelta(minutes=1),
    )


def rule(
    owner: str = "alice",
    *,
    rule_id: str = "rule/a",
    version: int = 1,
    enabled: bool = True,
    deleted: bool = False,
    threshold: str = "10.123456",
    updated: datetime | None = None,
) -> PriceAlertRuleProjectionRow:
    return PriceAlertRuleProjectionRow.from_entry(
        PriceAlertRuleEntry(
            owner_id=owner,
            rule_id=rule_id,
            version=version,
            deleted=deleted,
            ts_code=None if deleted else "600001.SH",
            membership_version=None if deleted else 1,
            rule=None
            if deleted
            else PriceAlertRule(
                rule_id=rule_id,
                name="到价提醒",
                priority="P2",
                enabled=enabled,
                comparison="gte",
                threshold=Decimal(threshold),
                valid_from=time(9, 30, 1, 123456),
                valid_until=time(14, 57, 2),
            ),
            updated_at=updated or FIXTURE_BUILT_AT - timedelta(minutes=1),
        )
    )


def publish(
    root: Path,
    *,
    sequence: int = 0,
    rules: tuple[PriceAlertRuleProjectionRow, ...] = (),
    members: tuple[ManualWatchlistProjectionRow, ...] = (member(),),
    activated: bool = True,
    watchlist_ready: bool = True,
) -> str:
    built = FIXTURE_BUILT_AT + timedelta(minutes=sequence)
    price = (
        PriceAlertRuleAuthoritySnapshot.create(
            activated_at=FIXTURE_BUILT_AT - timedelta(days=1),
            rows=rules,
        )
        if activated
        else None
    )
    watchlist = (
        ManualWatchlistAuthoritySnapshot.create(
            activated_at=FIXTURE_BUILT_AT - timedelta(days=1),
            rows=members,
        )
        if watchlist_ready
        else None
    )
    manifest = build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        signal_projections=(
            *build_price_alert_rule_projections(price, observed_at=built),
            *build_manual_watchlist_projections(watchlist, observed_at=built),
        ),
    )
    return manifest.generation_id


def client_for(
    root: Path,
    *,
    admission: object | None = None,
    private: bool = True,
    clock: Callable[[], datetime] | None = None,
) -> ProofTestClient:
    settings = WebSettings(
        serving_root=root, ingress_socket_path=root.parent / "web.sock" if private else None
    )
    if admission is not None:
        settings = with_test_proxy_identity(settings)
        settings = WebSettings.model_validate(
            {
                **settings.model_dump(),
                "price_alert_admission_socket_path": Path("/tmp/rquant-price-synthetic/price.sock"),
                "price_alert_admission_service_uid": os.geteuid() + 1,
                "price_alert_admission_shared_gid": os.getegid(),
            }
        )
    app = create_proof_test_app(
        settings,
        clock=clock or (lambda: NOW),
        background=False,
        price_alert_admission_client=admission,
    )
    return ProofTestClient(app)


def test_private_identity_and_owner_query_do_not_read_rows(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publish(root, rules=(rule(),))
    with client_for(root, private=False) as client:
        assert client.get(PATH, headers=HEADERS).status_code == 503
    with client_for(root) as client:
        assert client.get(PATH).status_code == 401
        assert (
            client.get(PATH, headers={**HEADERS, "x-rquant-proxy-proof": "bad"}).status_code == 401
        )
        assert client.get(PATH + "?owner_id=bob", headers=HEADERS).status_code == 422
        assert (
            client.get(PATH + "/head?rule_id=rule%2Fa&owner=bob", headers=HEADERS).status_code
            == 422
        )


def test_same_stock_and_rule_id_are_owner_isolated_and_precision_is_kept(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publish(root, rules=(rule(), rule("bob", threshold="99.9")), members=(member(), member("bob")))
    with client_for(root) as client:
        response = client.get(PATH, headers=HEADERS)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["availability"] == "ready"
        assert len(data["items"]) == 1
        assert data["items"][0]["threshold"] == "10.123456"
        assert data["items"][0]["valid_from"] == "09:30:01.123456"
        assert data["items"][0]["status_label"] == "未运行"
        assert "行情评估接通后" in data["items"][0]["scope_message"]
        assert data["can_write"] is False
        assert "owner_id" not in str(data)
        bob = client.get(
            PATH + "/head", params={"rule_id": "rule/a"}, headers={"x-rquant-user": "bob"}
        ).json()["data"]
        assert bob["item"]["threshold"] == "99.9"
        absent = client.get(PATH + "/head", params={"rule_id": "missing"}, headers=HEADERS).json()[
            "data"
        ]
        assert absent["availability"] == "ready" and absent["status"] == "absent"


def test_zero_unactivated_missing_and_stale_are_distinct(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    with client_for(root) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "unavailable"
    publish(root, activated=False)
    with client_for(root) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "not_activated"
    publish(root, sequence=1)
    with client_for(root) as client:
        data = client.get(PATH, headers=HEADERS).json()["data"]
        assert data["availability"] == "ready" and data["items"] == []
    with client_for(root, clock=lambda: NOW + timedelta(hours=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "unavailable"


def test_scope_loss_expiry_readd_and_disabled_have_honest_causes(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    for sequence, members, expected in (
        (0, (member(deleted=True),), "removed"),
        (1, (member(expiry=NOW),), "expired"),
        (2, (member(version=3),), "changed"),
        (3, (), "removed"),
    ):
        publish(root, sequence=sequence, rules=(rule(),), members=members)
        with client_for(root) as client:
            item = client.get(PATH, headers=HEADERS).json()["data"]["items"][0]
            assert item["scope_status"] == expected
            assert item["status_label"] == "未运行"
    publish(root, sequence=4, rules=(rule(enabled=False),))
    with client_for(root) as client:
        item = client.get(PATH, headers=HEADERS).json()["data"]["items"][0]
        assert item["enabled"] is False and item["scope_status"] == "disabled"


def test_tombstone_is_exact_evidence_and_not_in_list(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publish(root, rules=(rule(version=2, deleted=True),))
    with client_for(root) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["items"] == []
        head = client.get(PATH + "/head", params={"rule_id": "rule/a"}, headers=HEADERS).json()[
            "data"
        ]
        assert head["status"] == "deleted" and head["version"] == 2


def test_unknown_member_source_disables_new_writes_without_green_scope(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publish(root, rules=(rule(),), watchlist_ready=False)
    with client_for(root, admission=object()) as client:
        data = client.get(PATH, headers=HEADERS).json()["data"]
        assert data["can_write"] is False
        assert data["members"] == []
        assert data["items"][0]["scope_status"] == "unavailable"


def test_manifest_count_and_corrupt_snapshot_digest_cannot_be_read_as_healthy(
    tmp_path: Path,
) -> None:
    import pytest

    from rquant.web.price_alert_read import read_price_alert_rules

    root = tmp_path / "serving"
    publish(root, rules=(rule(),))
    tracker = GenerationTracker(root)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            counts = {**borrowed.manifest.row_counts, "price_alert_rule": 2}
            broken = replace(
                borrowed, manifest=borrowed.manifest.model_copy(update={"row_counts": counts})
            )
            with pytest.raises(ValueError, match="manifest and status differ"):
                read_price_alert_rules(broken, owner_id="alice", now=NOW)

            class CorruptDigestCursor:
                def execute(self, sql: str, *args: object) -> CorruptDigestCursor:
                    self.state = "FROM price_alert_rule_state " in sql
                    self.result = borrowed.cursor.execute(sql, *args)
                    return self

                def fetchall(self) -> list[tuple[object, ...]]:
                    values = self.result.fetchall()
                    return [(*values[0][:-1], "0" * 64)] if self.state else values

            with pytest.raises(ValueError, match="count or digest mismatch"):
                read_price_alert_rules(
                    replace(borrowed, cursor=CorruptDigestCursor()), owner_id="alice", now=NOW
                )
    finally:
        tracker.close()


def test_owner_scope_overcapacity_and_degraded_generation_stay_unavailable(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    members = tuple(
        member().model_copy(update={"ts_code": f"{600000 + index:06d}.SH"}) for index in range(501)
    )
    publish(root, members=members)
    with client_for(root, admission=object()) as client:
        data = client.get(PATH, headers=HEADERS).json()["data"]
        assert data["availability"] == "unavailable" and not data["can_write"]
        assert data["members"] == []
    publish(root, sequence=1, rules=(rule(),))
    with client_for(root) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "ready"
        client.app.state.web.tracker._record_failure("synthetic newer invalid generation")
        unavailable = client.get(PATH, headers=HEADERS).json()
        assert unavailable["data"]["items"] == [] and unavailable["serving"]["state"] == "degraded"
        assert "synthetic newer invalid generation" not in str(unavailable)
