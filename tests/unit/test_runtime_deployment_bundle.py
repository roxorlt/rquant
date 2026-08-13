from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_bundle import (
    RuntimeSchemaBootstrapRequiredError,
    RuntimeSchemaV1MigrationAuthorization,
    load_runtime_schema_rollout,
    prepare_runtime_schema_rollout,
    rollback_runtime_schema_rollout,
    validate_runtime_deployment_bundle,
)
from rquant.runtime_deployment_bundle import (
    install_runtime_deployment_bundle as _install_runtime_deployment_bundle,
)
from rquant.runtime_schema_registry import (
    RuntimeSchemaCompatibilityError,
    RuntimeSchemaV1LifecycleReview,
    build_runtime_schema_contract_bundle,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
)
from rquant.schema_compatibility import RolloutPhase
from tests.paper_cost_fixtures import paper_execution_cost_spec

COMMIT = "a" * 40
CALENDAR_SHA256 = "c" * 64


def _calendar_generation_path(root: Path) -> Path:
    return root / "authorities" / "market-calendar" / "generations" / f"{CALENDAR_SHA256}.json"


def _service_instance(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode()).hexdigest()


def _strategy_release_version(service_id: str, strategy_version: int = 1) -> str:
    return f"strategy-live/{service_id}/strategy-v{strategy_version}/commit-{COMMIT}"


def install_runtime_deployment_bundle(*args: object, **kwargs: object) -> object:
    root = Path(args[0]) if args else Path(str(kwargs["runtime_root"]))
    current = root / "current"
    current_contract = current / "schema-contracts.json"
    if not current_contract.is_file():
        kwargs.setdefault("schema_bootstrap_reason", "explicit unit-test bootstrap")
    return _install_runtime_deployment_bundle(*args, **kwargs)  # type: ignore[arg-type]


class _CredentialTransactionStub:
    def __init__(
        self,
        credentials: dict[str, bytes],
        *,
        visible: dict[str, bytes] | None = None,
        fail_rollback: bool = False,
    ) -> None:
        self.sealed_instances = tuple(sorted(credentials))
        self._visible = visible
        self._previous = dict(visible or {})
        self._credentials = dict(credentials)
        self._fail_rollback = fail_rollback
        self.committed = False
        self.rolled_back = False
        if visible is not None:
            visible.update(credentials)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        if self._fail_rollback:
            raise RuntimeError("simulated credential rollback failure")
        if self._visible is not None:
            self._visible.clear()
            self._visible.update(self._previous)
        self.rolled_back = True


class _CredentialRecoveryStub:
    def __init__(
        self,
        *,
        outcome: str = "none",
        transaction_id: str | None = None,
    ) -> None:
        self.outcome = outcome
        self.transaction_id = transaction_id


@pytest.fixture(autouse=True)
def isolated_root_credential_sealer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._recover_runtime_credentials",
        lambda **_kwargs: _CredentialRecoveryStub(),
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda credentials: _CredentialTransactionStub(dict(credentials)),
    )


def _manifest(
    root: Path,
    *,
    service_id: str,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
    interval_seconds: float = 1,
) -> RuntimeServiceManifest:
    instance = "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()
    if kind is RuntimeServiceKind.REFERENCE_SLOW_SOURCE:
        settings: dict[str, object] = {
            "database_path": str(root.parent / "operational" / "rquant_ro.duckdb"),
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
            "spool_root": str(root / "live" / "reference-slow"),
            "quota_path": str(root / "live" / "reference-slow" / "quota.sqlite3"),
            "quota_units_per_window": 500,
            "quota_cost_per_capture": 6,
            "producer_version": "reference-source-v1",
        }
    elif kind is RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER:
        settings = {
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
            "spool_root": str(root / "live" / "reference-slow"),
            "registry_path": str(root / "authorities" / "reference-slow" / "reference.sqlite3"),
            "cursor_root": str(
                root / "control" / "reference-slow-publishers" / instance / "cursors"
            ),
            "consumer_id": "reference-slow-publisher",
        }
    elif kind is RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER:
        settings: dict[str, object] = {
            "database_path": str(root.parent / "operational" / "rquant_ro.duckdb"),
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
            "authority_root": str(root / "authorities" / "auction-universe"),
        }
    elif kind is RuntimeServiceKind.AUCTION_MATCH_SOURCE:
        settings: dict[str, object] = {
            "spool_root": str(root / "live" / "auction-match"),
            "quota_path": str(root / "live" / "auction-match" / "quota.sqlite3"),
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
            "universe_path": str(root / "authorities" / "auction-universe" / "current.json"),
        }
    elif kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
        settings: dict[str, object] = {
            "spool_root": str(root / "live" / "market-minute"),
            "quota_path": str(root / "live" / "market-minute" / "quota.sqlite3"),
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
        }
    elif kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE:
        settings = {
            "spool_root": str(root / "live" / "watchlist-quote"),
            "quota_path": str(root / "live" / "watchlist-quote" / "quota.sqlite3"),
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
            "rollout_mode": "candidate",
        }
    elif kind is RuntimeServiceKind.FEATURE_LIVE:
        settings = {
            "raw_spool_root": str(root / "live" / "market-minute"),
            "feature_spool_root": str(root / "live" / "features"),
            "historical_minutes_snapshot_path": str(
                root / "research" / "snapshots" / "minute.parquet"
            ),
        }
    elif kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
        settings = {
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "candidate_input_path": str(root.parent / "inputs" / "n-shape.json"),
            "snapshot_root": str(root / "live" / "candidates" / instance),
        }
    elif kind is RuntimeServiceKind.STRATEGY_LIVE:
        paper_instance = "svc-" + hashlib.sha256(b"paper-broker").hexdigest()
        settings = {
            "feature_spool_root": str(root / "live" / "features"),
            "runner_state_path": str(root / "live" / "strategies" / instance / "runner.sqlite3"),
            "definition_registry_root": str(root / "live" / "definitions"),
            "strategy_registration_fingerprint": "9" * 64,
            "candidate_snapshot_root": str(root / "live" / "candidates" / "source"),
            "paper_broker_path": str(
                root / "live" / "paper-brokers" / paper_instance / "broker.sqlite3"
            ),
            "paper_account_id": "paper-main",
            "candidate_max_age_seconds": 120,
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "batch_limit": 128,
            "calendar_path": str(_calendar_generation_path(root)),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
            "signal_bus_path": str(root / "live" / "signal-bus" / "signal_bus.sqlite3"),
            "routing_policy_fingerprint": "8" * 64,
            "strategy_spec_fingerprint": "7" * 64,
            "evaluator_contract_fingerprint": "6" * 64,
            "producer_instance_id": instance,
            "producer_version": _strategy_release_version(service_id),
        }
    elif kind is RuntimeServiceKind.SIGNAL_ROUTER:
        settings = {
            "signal_bus_path": str(root / "live" / "signal-bus" / "signal_bus.sqlite3"),
            "signal_spool_root": str(root / "live" / "signal-bus" / "spool"),
            "runner_state_path": str(root / "live" / "strategies" / "source" / "runner.sqlite3"),
            "routing_policy_path": str(root.parent / "policies" / "routing.json"),
        }
    elif kind is RuntimeServiceKind.NOTIFIER:
        settings = {
            "signal_spool_root": str(root / "live" / "signal-bus" / "spool"),
            "notification_state_path": str(
                root / "live" / "notifications" / instance / "notification_state.sqlite3"
            ),
            "serving_authority_root": str(
                root / "live" / "notifications" / instance / "serving-authority"
            ),
        }
    elif kind is RuntimeServiceKind.PAPER_CONSUMER:
        settings = {
            "signal_bus_path": str(root / "live" / "signal-bus" / "signal_bus.sqlite3"),
            "queue_path": str(root / "live" / "paper" / "queue.sqlite3"),
            "consumer_state_path": str(root / "live" / "paper" / "consumer.sqlite3"),
            "broker_path": str(root / "live" / "paper" / "broker.sqlite3"),
            "raw_spool_root": str(root / "live" / "market-minute"),
            "trade_calendar_path": str(root.parent / "authorities" / "calendar.json"),
            "execution_constraint_root": str(root / "authorities" / "paper-execution"),
        }
    elif kind is RuntimeServiceKind.PAPER_BROKER:
        paper_root = root / "live" / "paper-brokers" / instance
        settings = {
            "signal_spool_root": str(root / "live" / "signal-bus" / "spool"),
            "queue_path": str(paper_root / "queue.sqlite3"),
            "consumer_state_path": str(paper_root / "consumer.sqlite3"),
            "broker_path": str(paper_root / "broker.sqlite3"),
            "raw_spool_root": str(root / "live" / "market-minute"),
            "trade_calendar_path": str(root.parent / "authorities" / "calendar.json"),
            "execution_constraint_root": str(root / "authorities" / "paper-execution"),
            "serving_authority_root": str(paper_root / "serving-authority"),
        }
    elif kind is RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER:
        settings = {
            "minute_spool_root": str(root / "live" / "market-minute"),
            "reference_registry_path": str(
                root / "authorities" / "reference-slow" / "reference.sqlite3"
            ),
            "authority_root": str(root / "authorities" / "paper-execution"),
        }
    elif kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER:
        settings = {
            "authority_root": str(root / "control" / "authority-runtime-health"),
            "sources": [
                {
                    "control_root": str(root / "control" / "features" / "source"),
                    "service_id": "feature-source",
                    "plane": "live",
                    "stale_after_seconds": 30,
                    "producer_commit": COMMIT,
                }
            ],
        }
    elif kind is RuntimeServiceKind.LAB_JOBS_PUBLISHER:
        settings = {
            "lab_jobs_path": str(root / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(root / "research" / "serving-authorities" / "lab-jobs"),
        }
    elif kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
        settings = {
            "research_root": str(root / "research"),
            "artifact_root": str(root / "research" / "final-artifacts"),
            "state_root": str(root / "research" / "artifact-catalogs" / instance),
            "lab_jobs_path": str(root / "research" / "lab_jobs.sqlite3"),
            "dataset_authority_path": str(root / "research" / "research_ro.duckdb"),
            "experiment_registry_path": str(root / "research" / "experiment_registry.sqlite3"),
            "location_id": "cloud-primary",
            "failure_domain": "tencent-shanghai",
            "max_bundles": 32,
            "max_discovery_entries": 128,
        }
    elif kind is RuntimeServiceKind.PROMOTIONS_PUBLISHER:
        settings = {
            "experiment_registry_path": str(root / "research" / "experiment_registry.sqlite3"),
            "experiment_registry_managed_trust_root": str(root / "research"),
            "authority_root": str(root / "research" / "serving-authorities" / "promotions"),
        }
    elif kind is RuntimeServiceKind.SERVING_PUBLISHER:
        notifier_instance = _service_instance("notifier.admin.shadow.v1")
        broker_instance = _service_instance("paper-broker.shadow-main.v1")
        settings = {
            "serving_root": str(root / "serving"),
            "schema_version": 3,
            "source_authorities": [
                {
                    "dataset_id": "signals",
                    "root": str(
                        root / "live" / "notifications" / notifier_instance / "serving-authority"
                    ),
                },
                {
                    "dataset_id": "paper_accounts",
                    "root": str(
                        root / "live" / "paper-brokers" / broker_instance / "serving-authority"
                    ),
                },
                {
                    "dataset_id": "runtime_health",
                    "root": str(root / "control" / "authority-runtime-health"),
                },
                {
                    "dataset_id": "lab_jobs",
                    "root": str(root / "research" / "serving-authorities" / "lab-jobs"),
                },
                {
                    "dataset_id": "promotions",
                    "root": str(root / "research" / "serving-authorities" / "promotions"),
                },
                {
                    "dataset_id": "reference_slow_authority",
                    "root": str(root / "live" / "reference-slow" / "serving-authority"),
                },
            ],
        }
    else:
        settings = {}
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=plane,
        interval_seconds=interval_seconds,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings=settings,
    )


def _bundle_inputs(
    root: Path,
) -> tuple[
    tuple[RuntimeServiceManifest, ...],
    dict[str, dict[str, str]],
]:
    manifests = (
        _manifest(
            root,
            service_id="auction/universe:publisher",
            kind=RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="auction/source:primary",
            kind=RuntimeServiceKind.AUCTION_MATCH_SOURCE,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="minute/source:primary",
            kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="candidate-n-shape",
            kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="notifier-admin",
            kind=RuntimeServiceKind.NOTIFIER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="serving-publisher",
            kind=RuntimeServiceKind.SERVING_PUBLISHER,
            plane=RuntimeServicePlane.SERVING,
        ),
    )
    capabilities = {
        "auction/universe:publisher": {},
        "auction/source:primary": {
            "TUSHARE_TOKEN_MAIN": "main-token",
        },
        "minute/source:primary": {
            "TUSHARE_TOKEN_MAIN": "main-token",
            "TUSHARE_TOKEN_BACKUP": "backup-token",
        },
        "candidate-n-shape": {},
        "notifier-admin": {
            "PUSHDEER_KEYS": "pushdeer-key",
            "PUSHPLUS_TOKENS": "pushplus-token",
            "PUSHDEER_ENDPOINT": "https://pushdeer.invalid/send",
            "PUSHPLUS_ENDPOINT": "https://pushplus.invalid/send",
        },
        "serving-publisher": {},
    }
    return manifests, capabilities


def test_candidate_bundle_owns_only_snapshot_output_and_receives_no_secrets(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    assert Path(manifest.settings["candidate_input_path"]).is_relative_to(tmp_path / "inputs")
    instance = receipt.instance_mapping[manifest.service_id]
    assert Path(manifest.settings["snapshot_root"]) == root / "live" / "candidates" / instance
    assert instance.startswith("svc-")
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-candidate@{instance}.service"
    )
    assert not (root / "current" / "credentials").exists()
    assert Path(manifest.settings["snapshot_root"]).is_dir()
    assert (root / "control" / "candidates" / instance).is_dir()

    overprivileged = {manifest.service_id: {"TUSHARE_TOKEN_MAIN": "forbidden"}}
    with pytest.raises(ValueError, match="unknown capability"):
        install_runtime_deployment_bundle(
            tmp_path / "other-runtime",
            producer_commit=COMMIT,
            manifests=(
                manifest.model_copy(
                    update={
                        "settings": {
                            **manifest.model_dump(mode="json")["settings"],
                            "snapshot_root": str(
                                tmp_path / "other-runtime" / "live" / "candidates" / instance
                            ),
                        }
                    }
                ),
            ),
            capability_env=overprivileged,
        )


def test_auction_candidate_bundle_rejects_inputs_outside_shared_authorities(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    service_id = "candidate-auction-gap"
    instance = "svc-" + hashlib.sha256(service_id.encode()).hexdigest()
    manifest = _manifest(
        root,
        service_id=service_id,
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    ).model_copy(
        update={
            "settings": {
                "strategy_id": "auction_gap",
                "strategy_version": 1,
                "input_mode": "auction_live",
                "auction_spool_root": str(root / "live" / "candidates" / instance / "raw"),
                "daily_database_path": str(tmp_path / "operational" / "rquant_ro.duckdb"),
                "reference_registry_path": str(
                    root / "authorities" / "reference-slow" / "reference.sqlite3"
                ),
                "calendar_path": str(_calendar_generation_path(root)),
                "calendar_expected_commit": COMMIT,
                "calendar_content_sha256": CALENDAR_SHA256,
                "snapshot_root": str(root / "live" / "candidates" / instance),
            }
        }
    )

    with pytest.raises(ValueError, match="auction_spool_root|auction-match"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_auction_universe_publisher_has_exclusive_authority_and_no_secrets(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="auction-universe-publisher",
        kind=RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-auction-universe@{instance}.service"
    )
    assert (root / "authorities" / "auction-universe").is_dir()
    assert (root / "control" / "auction-universe-publishers" / instance).is_dir()
    assert not (root / "current" / "credentials").exists()


def test_reference_source_and_publisher_separate_token_raw_and_authority_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    source = _manifest(
        root,
        service_id="reference-slow-source",
        kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        plane=RuntimeServicePlane.LIVE,
    )
    publisher = _manifest(
        root,
        service_id="reference-slow-publisher",
        kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )

    captured: dict[str, bytes] = {}
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda credentials: (
            captured.update(credentials) or _CredentialTransactionStub(dict(credentials))
        ),
    )
    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(source, publisher),
        capability_env={
            source.service_id: {
                "TUSHARE_TOKEN_MAIN": "main-token",
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
                "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": "cHJpdmF0ZS1rZXk=",
            },
            publisher.service_id: {
                "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
                "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
                "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "ssh-ed25519 AAAAtest reference-source",
            },
        },
    )

    source_instance = receipt.instance_mapping[source.service_id]
    publisher_instance = receipt.instance_mapping[publisher.service_id]
    assert receipt.unit_mapping[source.service_id] == (
        f"rquant-runtime-reference-slow-source@{source_instance}.service"
    )
    assert receipt.unit_mapping[publisher.service_id] == (
        f"rquant-runtime-reference-slow-publisher@{publisher_instance}.service"
    )
    assert (root / "authorities" / "reference-slow").is_dir()
    assert (root / "live" / "reference-slow").is_dir()
    assert (root / "control" / "reference-slow-sources" / source_instance).is_dir()
    assert (root / "control" / "reference-slow-publishers" / publisher_instance).is_dir()
    source_credential = json.loads(captured[source_instance])
    publisher_credential = json.loads(captured[publisher_instance])
    source_keys = set(source_credential["capabilities"])
    publisher_keys = set(publisher_credential["capabilities"])
    assert "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64" in source_keys
    assert "RQ_REFERENCE_SOURCE_PUBLIC_KEY" not in source_keys
    assert "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64" not in publisher_keys
    assert "RQ_REFERENCE_SOURCE_PUBLIC_KEY" in publisher_keys


def test_reference_publisher_seals_its_declared_publication_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    publisher = _manifest(
        root,
        service_id="reference-slow-publisher",
        kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )
    captured: dict[str, bytes] = {}
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda credentials: (
            captured.update(credentials) or _CredentialTransactionStub(dict(credentials))
        ),
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(publisher,),
        capability_env={
            publisher.service_id: {
                "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
                "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
                "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
                "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "ssh-ed25519 AAAAtest reference-source",
            }
        },
    )

    publisher_instance = receipt.instance_mapping[publisher.service_id]
    assert tuple(captured) == (publisher_instance,)
    assert b"RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX" in captured[publisher_instance]


def test_lab_artifact_catalog_uses_dedicated_research_writer_and_unit(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="lab-artifact-catalog",
        kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        plane=RuntimeServicePlane.RESEARCH,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-artifact-catalog@{instance}.service"
    )
    assert (root / "research" / "artifact-catalogs" / instance).is_dir()
    assert (
        root
        / "research"
        / "artifact-retention"
        / "svc-248ba9b29fdc243fcd4f7d09641fbdedd61871ffeea693ea4eb26f36f264b349"
        / "catalog-registration-outbox"
    ).is_dir()
    assert (root / "control" / "artifact-catalogs" / instance).is_dir()


@pytest.mark.parametrize(
    ("kind", "template", "control_bucket"),
    (
        (RuntimeServiceKind.SIGNAL_ROUTER, "signal-router", "signal-routers"),
        (RuntimeServiceKind.NOTIFIER, "notifier", "notifiers"),
        (RuntimeServiceKind.PAPER_BROKER, "paper-broker", "paper-brokers"),
    ),
)
def test_signal_and_paper_services_use_dedicated_units_and_state_roots(
    tmp_path: Path,
    kind: RuntimeServiceKind,
    template: str,
    control_bucket: str,
) -> None:
    root = tmp_path / kind.value / "runtime"
    manifest = _manifest(
        root,
        service_id=f"service-{kind.value}",
        kind=kind,
        plane=RuntimeServicePlane.LIVE,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-{template}@{instance}.service"
    )
    assert (root / "control" / control_bucket / instance).is_dir()
    if kind is RuntimeServiceKind.SIGNAL_ROUTER:
        assert (root / "live" / "signal-bus").is_dir()
    elif kind is RuntimeServiceKind.NOTIFIER:
        assert (root / "live" / "notifications" / instance).is_dir()
    else:
        assert (root / "live" / "paper-brokers" / instance).is_dir()


@pytest.mark.parametrize(
    ("kind", "template", "control_bucket", "owner_root"),
    (
        (
            RuntimeServiceKind.AUCTION_MATCH_SOURCE,
            "auction-match",
            "auction-match-sources",
            "auction-match",
        ),
        (
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            "market-minute",
            "market-minute-sources",
            "market-minute",
        ),
        (
            RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
            "watchlist-quote",
            "watchlist-quote-sources",
            "watchlist-quote",
        ),
        (RuntimeServiceKind.FEATURE_LIVE, "feature", "features", "features"),
        (
            RuntimeServiceKind.SERVING_PUBLISHER,
            "serving",
            "serving-publishers",
            None,
        ),
    ),
)
def test_shared_runtime_writers_use_dedicated_units_and_control_roots(
    tmp_path: Path,
    kind: RuntimeServiceKind,
    template: str,
    control_bucket: str,
    owner_root: str | None,
) -> None:
    root = tmp_path / kind.value / "runtime"
    plane = (
        RuntimeServicePlane.SERVING
        if kind is RuntimeServiceKind.SERVING_PUBLISHER
        else RuntimeServicePlane.LIVE
    )
    manifest = _manifest(
        root,
        service_id=f"service-{kind.value}",
        kind=kind,
        plane=plane,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-{template}@{instance}.service"
    )
    assert (root / "control" / control_bucket / instance).is_dir()
    if owner_root is not None:
        assert (root / "live" / owner_root).is_dir()


@pytest.mark.parametrize(
    ("kind", "plane", "template", "control_bucket", "owner_path"),
    (
        (
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
            RuntimeServicePlane.LIVE,
            "paper-constraint",
            "paper-constraints",
            "authorities/paper-execution",
        ),
        (
            RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
            RuntimeServicePlane.SERVING,
            "runtime-health",
            "runtime-health-publishers",
            "control/authority-runtime-health",
        ),
        (
            RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            RuntimeServicePlane.RESEARCH,
            "lab-jobs",
            "lab-jobs-publishers",
            "research/serving-authorities/lab-jobs",
        ),
        (
            RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            RuntimeServicePlane.RESEARCH,
            "promotions",
            "promotions-publishers",
            "research/serving-authorities/promotions",
        ),
    ),
)
def test_authority_publishers_use_dedicated_units_and_exact_owner_roots(
    tmp_path: Path,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
    template: str,
    control_bucket: str,
    owner_path: str,
) -> None:
    root = tmp_path / kind.value / "runtime"
    manifest = _manifest(
        root,
        service_id=f"service-{kind.value}",
        kind=kind,
        plane=plane,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-{template}@{instance}.service"
    )
    assert (root / "control" / control_bucket / instance).is_dir()
    assert (root / owner_path).is_dir()


@pytest.mark.parametrize(
    ("kind", "plane", "setting_name", "bad_path"),
    (
        (
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
            RuntimeServicePlane.LIVE,
            "minute_spool_root",
            "live/features",
        ),
        (
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
            RuntimeServicePlane.LIVE,
            "reference_registry_path",
            "authorities/reference-slow/other.sqlite3",
        ),
        (
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
            RuntimeServicePlane.LIVE,
            "authority_root",
            "authorities/other",
        ),
        (
            RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
            RuntimeServicePlane.SERVING,
            "authority_root",
            "control/other-health",
        ),
        (
            RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            RuntimeServicePlane.RESEARCH,
            "lab_jobs_path",
            "research/other-lab.sqlite3",
        ),
        (
            RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            RuntimeServicePlane.RESEARCH,
            "authority_root",
            "research/serving-authorities/other-lab",
        ),
        (
            RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            RuntimeServicePlane.RESEARCH,
            "experiment_registry_path",
            "research/other-experiments.sqlite3",
        ),
        (
            RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            RuntimeServicePlane.RESEARCH,
            "authority_root",
            "research/serving-authorities/other-promotions",
        ),
    ),
)
def test_authority_publishers_reject_non_systemd_owner_paths(
    tmp_path: Path,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
    setting_name: str,
    bad_path: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id=f"service-{kind.value}",
        kind=kind,
        plane=plane,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    settings[setting_name] = str(root / bad_path)
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="systemd path|authority path|owned by"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_runtime_health_rejects_unknown_or_writable_control_source(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="runtime-health",
        kind=RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    settings["sources"][0]["control_root"] = str(
        root / "control" / "runtime-health-publishers" / "source"
    )
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="read-only|known control root"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    ("kind", "plane"),
    (
        (RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER, RuntimeServicePlane.LIVE),
        (RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER, RuntimeServicePlane.SERVING),
        (RuntimeServiceKind.LAB_JOBS_PUBLISHER, RuntimeServicePlane.RESEARCH),
        (RuntimeServiceKind.PROMOTIONS_PUBLISHER, RuntimeServicePlane.RESEARCH),
    ),
)
def test_authority_publishers_require_manifest_v2_and_no_capabilities(
    tmp_path: Path,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id=f"service-{kind.value}",
        kind=kind,
        plane=plane,
    )

    with pytest.raises(ValueError, match="schema.*v2|v2.*schema"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest.model_copy(update={"schema_version": 1}),),
            capability_env={manifest.service_id: {}},
        )
    with pytest.raises(ValueError, match="capability environment"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {"TUSHARE_TOKEN_MAIN": "forbidden"}},
        )


def test_feature_inputs_are_readonly_and_output_is_its_only_live_write_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="feature-live",
        kind=RuntimeServiceKind.FEATURE_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    settings["raw_spool_root"] = str(root / "live" / "features" / "raw")
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="read-only|writable|feature"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    ("setting_name", "bad_path"),
    (
        ("spool_root", "live/features"),
        ("quota_path", "live/market-minute/other/quota.sqlite3"),
    ),
)
def test_market_minute_paths_are_fixed_to_the_source_owner_root(
    tmp_path: Path,
    setting_name: str,
    bad_path: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="minute/source:primary",
        kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    settings[setting_name] = str(root / bad_path)
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="market-minute|quota_path|spool_root"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_market_minute_calendar_path_is_exact_content_generation(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="market-minute-source",
        kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = dict(manifest.settings)
    settings["calendar_path"] = str(
        root / "authorities" / "market-calendar" / "generations" / f"{'d' * 64}.json"
    )

    with pytest.raises(ValueError, match="calendar_path|fixed shared authority"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest.model_copy(update={"settings": settings}),),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    ("setting", "value", "match"),
    (
        ("rollout_mode", "published", "rollout_mode|candidate"),
        ("spool_root", "live/market-minute", "watchlist-quote|spool_root"),
    ),
)
def test_watchlist_quote_preflight_rejects_authority_or_failure_domain_escape(
    tmp_path: Path,
    setting: str,
    value: str,
    match: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="watchlist-quote-source",
        kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = dict(manifest.settings)
    settings[setting] = value if setting == "rollout_mode" else str(root / value)

    with pytest.raises(ValueError, match=match):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest.model_copy(update={"settings": settings}),),
            capability_env={manifest.service_id: {}},
        )


def test_candidate_bundle_rejects_snapshot_output_outside_live_plane(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    ).model_copy(
        update={
            "settings": {
                "strategy_id": "n_shape",
                "strategy_version": 1,
                "candidate_input_path": str(tmp_path / "input.json"),
                "snapshot_root": str(root / "serving" / "candidates"),
            }
        }
    )

    with pytest.raises(ValueError, match="owned by the live plane"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    "candidate_input_path",
    (
        "inside-live",
        "inside-control",
        "relative",
        "traversal",
    ),
)
def test_candidate_bundle_rejects_unsafe_readonly_input_path(
    tmp_path: Path,
    candidate_input_path: str,
) -> None:
    root = tmp_path / "runtime"
    if candidate_input_path == "inside-live":
        input_path = root / "live" / "inputs" / "n-shape.json"
        message = "read-only|writable|live"
    elif candidate_input_path == "inside-control":
        input_path = root / "control" / "inputs" / "n-shape.json"
        message = "read-only|writable|control"
    elif candidate_input_path == "relative":
        input_path = Path("inputs/n-shape.json")
        message = "absolute|normalized"
    else:
        input_path = tmp_path / "inputs" / ".." / "inputs" / "n-shape.json"
        message = "absolute|normalized"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    ).model_copy(
        update={
            "settings": {
                "strategy_id": "n_shape",
                "strategy_version": 1,
                "candidate_input_path": str(input_path),
                "snapshot_root": str(
                    root
                    / "live"
                    / "candidates"
                    / ("svc-" + hashlib.sha256(b"candidate-n-shape").hexdigest())
                ),
            }
        }
    )

    with pytest.raises(ValueError, match=message):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_installs_canonical_generation_with_systemd_instance_mapping(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    assert receipt.producer_commit == COMMIT
    assert len(receipt.generation_hash) == 64
    assert receipt.instance_mapping == {
        manifest.service_id: "svc-"
        + hashlib.sha256(manifest.service_id.encode("utf-8")).hexdigest()
        for manifest in manifests
    }
    dedicated_templates = {
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: "auction-universe",
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: "auction-match",
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: "market-minute",
        RuntimeServiceKind.FEATURE_LIVE: "feature",
        RuntimeServiceKind.CANDIDATE_PUBLISHER: "candidate",
        RuntimeServiceKind.STRATEGY_LIVE: "strategy",
        RuntimeServiceKind.SIGNAL_ROUTER: "signal-router",
        RuntimeServiceKind.NOTIFIER: "notifier",
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: "paper-constraint",
        RuntimeServiceKind.PAPER_BROKER: "paper-broker",
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: "runtime-health",
        RuntimeServiceKind.LAB_JOBS_PUBLISHER: "lab-jobs",
        RuntimeServiceKind.PROMOTIONS_PUBLISHER: "promotions",
    }
    assert receipt.unit_mapping == {
        manifest.service_id: (
            "rquant-runtime-"
            f"{dedicated_templates.get(manifest.service_kind, manifest.plane.value)}@"
            f"{receipt.instance_mapping[manifest.service_id]}.service"
        )
        for manifest in manifests
    }
    current = root / "current"
    assert current.is_symlink()
    assert os.readlink(current) == f"generations/{receipt.generation_hash}"
    generation = current.resolve(strict=True)
    assert generation.parent == root / "generations"
    assert _mode(generation) == 0o700
    assert (generation / "runtime.env").read_text() == (
        "APP_ENV=prod\n"
        "RQUANT_DISABLE_DOTENV=1\n"
        f"RQUANT_RUNTIME_COMMIT={COMMIT}\nRQUANT_RUNTIME_GENERATION={receipt.generation_hash}\n"
    )
    assert _mode(generation / "runtime.env") == 0o600

    for manifest in manifests:
        instance = receipt.instance_mapping[manifest.service_id]
        assert instance.startswith("svc-")
        assert len(instance) == 68
        manifest_path = generation / "manifests" / f"{instance}.json"
        assert _mode(manifest_path) == 0o600
        assert manifest_path.read_bytes() == json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    manifest_payload = b"".join(
        path.read_bytes() for path in sorted((generation / "manifests").iterdir())
    )
    for secret in (b"main-token", b"pushdeer-key", b"pushplus-token"):
        assert secret not in manifest_payload
        assert all(
            secret not in path.read_bytes() for path in generation.rglob("*") if path.is_file()
        )
    assert not (generation / "secrets").exists()
    assert not (generation / "credentials").exists()


def test_seals_generation_bound_credentials_outside_runtime_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    captured: dict[str, bytes] = {}
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda credentials: (
            captured.update(credentials) or _CredentialTransactionStub(dict(credentials))
        ),
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    expected_service_ids = {
        manifest.service_id
        for manifest in manifests
        if manifest.plane is RuntimeServicePlane.LIVE
        and manifest.service_kind
        not in {
            RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
            RuntimeServiceKind.STRATEGY_LIVE,
        }
    }
    assert set(captured) == {
        receipt.instance_mapping[service_id] for service_id in expected_service_ids
    }
    manifest_by_instance = {
        receipt.instance_mapping[manifest.service_id]: manifest for manifest in manifests
    }
    for instance, payload in captured.items():
        decoded = json.loads(payload)
        manifest = manifest_by_instance[instance]
        assert decoded["schema_version"] == 2
        assert decoded["service_id"] == manifest.service_id
        assert decoded["service_kind"] == manifest.service_kind.value
        assert decoded["instance_name"] == instance
        assert decoded["bundle_generation"] == receipt.generation_hash
        assert isinstance(decoded["capabilities"], dict)
    generation = (root / "current").resolve(strict=True)
    assert all(
        secret not in path.read_bytes()
        for secret in (b"main-token", b"pushdeer-key", b"pushplus-token")
        for path in generation.rglob("*")
        if path.is_file()
    )


def test_installer_creates_required_systemd_plane_and_control_directories(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="minute/source:primary",
        kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
    )

    install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {"TUSHARE_TOKEN_MAIN": "secret"}},
    )

    assert (root / "live").is_dir()
    assert (root / "control").is_dir()


def test_credential_sealer_failure_prevents_runtime_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    def fail(_credentials: object) -> None:
        raise RuntimeError("root sealer unavailable")

    monkeypatch.setattr("rquant.runtime_deployment_bundle._seal_runtime_credentials", fail)

    with pytest.raises(RuntimeError, match="root sealer"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()


@pytest.mark.parametrize("suffix", ("", "nested", "sibling"))
def test_candidate_bundle_requires_its_exclusive_instance_output_root(
    tmp_path: Path,
    suffix: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )
    expected = Path(manifest.settings["snapshot_root"])
    if suffix == "":
        unsafe = root / "live" / "candidates"
    elif suffix == "nested":
        unsafe = expected / "nested"
    else:
        unsafe = expected.parent / ("svc-" + "f" * 64)
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                "snapshot_root": str(unsafe),
            }
        }
    )

    with pytest.raises(ValueError, match="exclusive|instance"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_strategy_bundle_uses_dedicated_unit_and_exclusive_state_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    state_path = root / "live" / "strategies" / instance / "runner.sqlite3"
    assert Path(manifest.settings["runner_state_path"]) == state_path
    assert state_path.parent.is_dir()
    assert (root / "control" / "strategies" / instance).is_dir()
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-strategy@{instance}.service"
    )
    assert not (root / "current" / "credentials").exists()


@pytest.mark.parametrize(
    "field",
    (
        "calendar_path",
        "calendar_expected_commit",
        "calendar_content_sha256",
        "signal_bus_path",
        "routing_policy_fingerprint",
        "producer_instance_id",
        "producer_version",
    ),
)
def test_strategy_bundle_requires_complete_shadow_completion_authority(
    tmp_path: Path,
    field: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    payload = manifest.model_dump(mode="python")
    payload["settings"].pop(field)
    forged = RuntimeServiceManifest.model_validate(payload)

    with pytest.raises(ValueError, match=field.replace("_", ".?")):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(forged,),
            capability_env={forged.service_id: {}},
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (("producer_instance_id", "svc-" + "f" * 64), ("producer_version", "latest")),
)
def test_strategy_bundle_rejects_mutable_or_forged_producer_identity(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    payload = manifest.model_dump(mode="python")
    payload["settings"][field] = value
    forged = RuntimeServiceManifest.model_validate(payload)

    with pytest.raises(ValueError, match=field.replace("_", ".?")):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(forged,),
            capability_env={forged.service_id: {}},
        )


def test_rejects_legacy_plaintext_secret_generation(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    legacy = root / "generations" / ("f" * 64) / "secrets"
    legacy.mkdir(parents=True)
    (legacy / "svc.env").write_text("TUSHARE_TOKEN_MAIN=plaintext\n")
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="legacy plaintext"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )


@pytest.mark.parametrize("mutation", ("shared-root", "nested", "sibling"))
def test_strategy_bundle_rejects_nonexclusive_state_root(
    tmp_path: Path,
    mutation: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    expected = Path(manifest.settings["runner_state_path"])
    if mutation == "shared-root":
        unsafe = root / "live" / "strategies" / "runner.sqlite3"
    elif mutation == "nested":
        unsafe = expected.parent / "nested" / "runner.sqlite3"
    else:
        unsafe = expected.parent.parent / ("svc-" + "f" * 64) / "runner.sqlite3"
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                "runner_state_path": str(unsafe),
            }
        }
    )

    with pytest.raises(ValueError, match="exclusive|instance"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    "setting_name",
    (
        "feature_spool_root",
        "definition_registry_root",
        "candidate_snapshot_root",
        "paper_broker_path",
    ),
)
def test_strategy_bundle_rejects_readonly_input_inside_its_writable_root(
    tmp_path: Path,
    setting_name: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    own_root = Path(manifest.settings["runner_state_path"]).parent
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                setting_name: str(own_root / "forbidden"),
            }
        }
    )

    with pytest.raises(ValueError, match="read-only|writable|strategy"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_same_inputs_are_deterministic_regardless_of_manifest_order(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    second = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=tuple(reversed(manifests)),
        capability_env=dict(reversed(tuple(capabilities.items()))),
    )

    assert second == first
    assert len(tuple((root / "generations").iterdir())) == 1


def test_secret_leakage_check_allows_path_segments_matching_a_secret_value(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    capabilities["minute/source:primary"]["TUSHARE_TOKEN_MAIN"] = "secret"
    candidate = list(manifests)
    candidate[0] = candidate[0].model_copy(
        update={
            "settings": {
                **candidate[0].model_dump(mode="json")["settings"],
                "diagnostic_path": str(root / "secret" / "capture"),
            }
        }
    )

    validate_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=tuple(candidate),
        capability_env=capabilities,
        schema_bootstrap_reason="unit-test secret structure check",
    )


@pytest.mark.parametrize("leaked_value", ("main-token", "bWFpbi10b2tlbg==", "6d61696e2d746f6b656e"))
def test_secret_leakage_check_rejects_nested_raw_and_encoded_secret_values(
    tmp_path: Path,
    leaked_value: str,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    candidate = list(manifests)
    candidate[1] = candidate[1].model_copy(
        update={
            "settings": {
                **candidate[1].model_dump(mode="json")["settings"],
                "metadata": {"nested": {"value": leaked_value}},
            }
        }
    )

    with pytest.raises(ValueError, match="plaintext capability value"):
        validate_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=tuple(candidate),
            capability_env=capabilities,
            schema_bootstrap_reason="unit-test secret structure check",
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("commit", "commit"),
        ("duplicate", "duplicate"),
        ("wrong_plane", "plane"),
        ("unknown_env", "unknown capability environment"),
        ("serving_env", "cannot receive capability environment"),
        ("missing_mapping", "exactly match"),
        ("secret_in_manifest", "plaintext capability value"),
        ("wrong_path_owner", "owned by the live plane"),
    ),
)
def test_rejects_invalid_or_overprivileged_bundle(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    candidate = list(manifests)
    commit = COMMIT
    if mutation == "commit":
        candidate[0] = candidate[0].model_copy(update={"producer_commit": "b" * 40})
    elif mutation == "duplicate":
        candidate.append(candidate[0])
    elif mutation == "wrong_plane":
        candidate[0] = candidate[0].model_copy(update={"plane": RuntimeServicePlane.SERVING})
    elif mutation == "unknown_env":
        capabilities["minute/source:primary"]["AWS_SECRET_ACCESS_KEY"] = "nope"
    elif mutation == "serving_env":
        capabilities["serving-publisher"]["PUSHDEER_KEYS"] = "nope"
    elif mutation == "missing_mapping":
        capabilities.pop("notifier-admin")
    elif mutation == "secret_in_manifest":
        candidate[1] = candidate[1].model_copy(
            update={
                "settings": {
                    **candidate[1].model_dump(mode="json")["settings"],
                    "label": "main-token",
                }
            }
        )
    elif mutation == "wrong_path_owner":
        candidate[1] = candidate[1].model_copy(
            update={
                "settings": {
                    "spool_root": str(root / "serving" / "raw"),
                    "quota_path": str(root / "live" / "quota.sqlite"),
                }
            }
        )

    with pytest.raises(ValueError, match=message):
        install_runtime_deployment_bundle(
            root,
            producer_commit=commit,
            manifests=tuple(candidate),
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert not list(root.glob(".staging-*"))


def test_rejects_deployment_of_internal_paper_consumer(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    consumer = _manifest(
        root,
        service_id="paper-consumer",
        kind=RuntimeServiceKind.PAPER_CONSUMER,
        plane=RuntimeServicePlane.LIVE,
    )

    with pytest.raises(ValueError, match="paper consumer.*internal|internal.*paper consumer"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(consumer,),
            capability_env={consumer.service_id: {}},
        )


@pytest.mark.parametrize(
    "kind",
    (
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_BROKER,
    ),
)
def test_dedicated_runtime_services_require_manifest_v2(
    tmp_path: Path,
    kind: RuntimeServiceKind,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id=f"legacy.{kind.value}",
        kind=kind,
        plane=RuntimeServicePlane.LIVE,
    ).model_copy(update={"schema_version": 1})

    with pytest.raises(ValueError, match="schema.*v2|v2.*schema"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_signal_router_rejects_nested_source_path_inside_its_writable_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    router = _manifest(
        root,
        service_id="signal-router",
        kind=RuntimeServiceKind.SIGNAL_ROUTER,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = router.model_dump(mode="json")["settings"]
    settings.pop("runner_state_path")
    settings["sources"] = [
        {
            "source_id": "n-shape-v1",
            "runner_state_path": str(root / "live" / "signal-bus" / "unsafe-runner.sqlite3"),
            "expected_strategy_spec_fingerprint": "1" * 64,
            "expected_evaluator_contract_fingerprint": "2" * 64,
        }
    ]
    router = router.model_copy(update={"settings": settings})

    with pytest.raises(
        ValueError, match=r"sources\[0\].runner_state_path.*read-only|read-only.*sources"
    ):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(router,),
            capability_env={router.service_id: {}},
        )


def test_accepts_single_process_paper_broker_manifest(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    policy = {
        "account_id": "paper-main",
        "execution_lag_seconds": 60,
        "buy_quantity": 1000,
        "reduce_quantity": 500,
        "sell_quantity": 1000,
        "limit": 10,
    }
    broker_base = _manifest(
        root,
        service_id="paper-broker",
        kind=RuntimeServiceKind.PAPER_BROKER,
        plane=RuntimeServicePlane.LIVE,
    )
    broker = broker_base.model_copy(
        update={
            "settings": {
                **policy,
                **broker_base.model_dump(mode="json")["settings"],
                "initial_cash": "100000",
                "execution_cost_spec": paper_execution_cost_spec().model_dump(mode="json"),
                "raw_spool_root": str(root.parent / "market-minute"),
                "trade_calendar_path": str(root.parent / "calendar.json"),
                "trade_calendar_sha256": "f" * 64,
                "execution_constraint_root": str(root / "authorities" / "paper-execution"),
            }
        }
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(broker,),
        capability_env={broker.service_id: {}},
    )

    assert set(receipt.instance_mapping) == {broker.service_id}


def test_paper_constraint_authority_cannot_be_inside_broker_writable_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    broker = _manifest(
        root,
        service_id="paper-broker",
        kind=RuntimeServiceKind.PAPER_BROKER,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = broker.model_dump(mode="json")["settings"]
    instance = "svc-" + hashlib.sha256(broker.service_id.encode("utf-8")).hexdigest()
    settings["execution_constraint_root"] = str(
        root / "live" / "paper-brokers" / instance / "paper-execution"
    )
    broker = broker.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="execution_constraint_root.*read-only|read-only"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(broker,),
            capability_env={broker.service_id: {}},
        )


def test_paper_constraint_authority_must_use_the_systemd_readonly_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    broker = _manifest(
        root,
        service_id="paper-broker",
        kind=RuntimeServiceKind.PAPER_BROKER,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = broker.model_dump(mode="json")["settings"]
    settings["execution_constraint_root"] = str(tmp_path / "outside")
    broker = broker.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="paper-execution.*authority root|authority root"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(broker,),
            capability_env={broker.service_id: {}},
        )


@pytest.mark.parametrize(
    "kind",
    (RuntimeServiceKind.NOTIFIER, RuntimeServiceKind.PAPER_BROKER),
)
def test_owner_serving_authority_must_use_exclusive_instance_root(
    tmp_path: Path,
    kind: RuntimeServiceKind,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id=kind.value,
        kind=kind,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    settings["serving_authority_root"] = str(root / "live" / "shared-authority")
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="serving_authority_root.*exclusive"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_notifier_page_projection_surge_root_is_bound_to_operational_data(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="notifier-admin",
        kind=RuntimeServiceKind.NOTIFIER,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = manifest.model_dump(mode="python")["settings"]
    assert isinstance(settings, dict)
    settings["page_projection_database_path"] = str(
        root.parent / "operational" / "rquant_ro.duckdb"
    )
    settings["page_projection_surge_live_root"] = str(root.parent / "wrong-data" / "surge_live")
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="surge_live source"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    "kind",
    (
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        RuntimeServiceKind.SERVING_PUBLISHER,
        RuntimeServiceKind.SIGNAL_ROUTER,
    ),
)
def test_rejects_multiple_writers_for_singleton_runtime_kind(
    tmp_path: Path,
    kind: RuntimeServiceKind,
) -> None:
    root = tmp_path / "runtime"
    first = _manifest(
        root,
        service_id=f"{kind.value}.one",
        kind=kind,
        plane=(
            RuntimeServicePlane.SERVING
            if kind
            in {
                RuntimeServiceKind.SERVING_PUBLISHER,
                RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
            }
            else RuntimeServicePlane.RESEARCH
            if kind
            in {
                RuntimeServiceKind.LAB_JOBS_PUBLISHER,
                RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
                RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            }
            else RuntimeServicePlane.LIVE
        ),
    )
    second = first.model_copy(update={"service_id": f"{kind.value}.two"})

    with pytest.raises(ValueError, match="single.*writer|singleton"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(first, second),
            capability_env={first.service_id: {}, second.service_id: {}},
        )


@pytest.mark.parametrize(
    ("dataset_id", "bad_root"),
    (
        ("signals", "serving/signals"),
        ("paper_accounts", "live/notifications/paper"),
        ("runtime_health", "live/signal-bus/health"),
        ("lab_jobs", "control/lab"),
        ("promotions", "live/paper-brokers/promotions"),
        ("reference_slow_authority", "research/reference-slow"),
    ),
)
def test_serving_source_authorities_must_use_owner_readonly_roots(
    tmp_path: Path,
    dataset_id: str,
    bad_root: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="serving-publisher",
        kind=RuntimeServiceKind.SERVING_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    for authority in settings["source_authorities"]:
        if authority["dataset_id"] == dataset_id:
            authority["root"] = str(root / bad_root)
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="source authorit.*owner|read-only"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_serving_source_authorities_require_exact_dataset_set(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="serving-publisher",
        kind=RuntimeServiceKind.SERVING_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    settings["source_authorities"] = settings["source_authorities"][:-1]
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="exactly six"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    ("dataset_id", "forged_root"),
    (
        (
            "signals",
            Path("live") / "notifications" / ("svc-" + "f" * 64) / "serving-authority",
        ),
        (
            "paper_accounts",
            Path("live") / "paper-brokers" / ("svc-" + "f" * 64) / "serving-authority",
        ),
        ("runtime_health", Path("control") / "forged-runtime-health"),
        ("lab_jobs", Path("research") / "serving-authorities" / "forged-lab-jobs"),
        ("promotions", Path("research") / "serving-authorities" / "forged-promotions"),
        (
            "reference_slow_authority",
            Path("live") / "reference-slow" / "forged-serving-authority",
        ),
    ),
)
def test_serving_source_authorities_bind_exact_owner_service_identity(
    tmp_path: Path,
    dataset_id: str,
    forged_root: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="serving-publisher",
        kind=RuntimeServiceKind.SERVING_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
    )
    settings = manifest.model_dump(mode="json")["settings"]
    authority = next(
        item for item in settings["source_authorities"] if item["dataset_id"] == dataset_id
    )
    authority["root"] = str(root / forged_root)
    manifest = manifest.model_copy(update={"settings": settings})

    with pytest.raises(ValueError, match="owner|identity|exact"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_failure_before_publish_preserves_previous_current_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    previous_target = os.readlink(root / "current")
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    write = module._write_secure_file
    calls = 0

    def fail_during_staging(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated disk failure")
        write(path, payload)

    monkeypatch.setattr(module, "_write_secure_file", fail_during_staging)

    with pytest.raises(OSError, match="simulated disk failure"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == previous_target
    assert (root / previous_target).is_dir()
    assert (root / previous_target).name == first.generation_hash
    assert not list(root.glob(".staging-*"))


def test_failure_after_credentials_switch_restores_all_pointers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    visible_credentials: dict[str, bytes] = {}
    transactions: list[_CredentialTransactionStub] = []

    def seal(credentials: object) -> _CredentialTransactionStub:
        transaction = _CredentialTransactionStub(
            dict(credentials),  # type: ignore[arg-type]
            visible=visible_credentials,
        )
        transactions.append(transaction)
        return transaction

    monkeypatch.setattr(module, "_seal_runtime_credentials", seal)
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    previous_runtime_target = os.readlink(root / "current")
    previous_credentials = dict(visible_credentials)
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    monkeypatch.setattr(
        module,
        "_publish_current",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("failure between credential and runtime current")
        ),
    )

    with pytest.raises(OSError, match="between credential and runtime current"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == previous_runtime_target
    assert (root / previous_runtime_target).name == first.generation_hash
    assert visible_credentials == previous_credentials
    assert transactions[-1].rolled_back
    assert not transactions[-1].committed


def test_failure_after_runtime_pointer_replace_restores_runtime_and_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    visible_credentials: dict[str, bytes] = {}

    def seal(credentials: object) -> _CredentialTransactionStub:
        return _CredentialTransactionStub(
            dict(credentials),  # type: ignore[arg-type]
            visible=visible_credentials,
        )

    monkeypatch.setattr(module, "_seal_runtime_credentials", seal)
    install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    previous_runtime_target = os.readlink(root / "current")
    previous_credentials = dict(visible_credentials)
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    publish = module._publish_current

    def replace_then_fail(runtime_root: Path, *, generation_hash: str) -> None:
        publish(runtime_root, generation_hash=generation_hash)
        raise OSError("failure after runtime current replace")

    monkeypatch.setattr(module, "_publish_current", replace_then_fail)

    with pytest.raises(OSError, match="after runtime current replace"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == previous_runtime_target
    assert visible_credentials == previous_credentials


def test_restart_before_runtime_publish_recovers_exact_transaction_before_new_begin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    events: list[tuple[str, object]] = []

    def recover(**kwargs: object) -> _CredentialRecoveryStub:
        events.append(("recover", kwargs))
        return _CredentialRecoveryStub(
            outcome="rolled_back",
            transaction_id="c" * 64,
        )

    def seal(credentials: object) -> _CredentialTransactionStub:
        events.append(("begin", tuple(sorted(dict(credentials)))))  # type: ignore[arg-type]
        return _CredentialTransactionStub(dict(credentials))  # type: ignore[arg-type]

    monkeypatch.setattr(module, "_recover_runtime_credentials", recover, raising=False)
    monkeypatch.setattr(module, "_seal_runtime_credentials", seal)

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(changed, *manifests[1:]),
        capability_env=capabilities,
    )

    credential_instances = tuple(
        sorted(
            receipt.instance_mapping[service_id]
            for service_id in (
                "auction/source:primary",
                "minute/source:primary",
                "notifier-admin",
            )
        )
    )
    assert events == [
        (
            "recover",
            {
                "bundle_generation": receipt.generation_hash,
                "instances": credential_instances,
                "action": "rollback",
            },
        ),
        ("begin", credential_instances),
    ]
    assert os.readlink(root / "current") == f"generations/{receipt.generation_hash}"
    assert first.generation_hash != receipt.generation_hash


def test_restart_after_runtime_publish_commits_exact_transaction_and_skips_begin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    observed: dict[str, object] = {}

    def recover(**kwargs: object) -> _CredentialRecoveryStub:
        observed.update(kwargs)
        return _CredentialRecoveryStub(
            outcome="committed",
            transaction_id="c" * 64,
        )

    monkeypatch.setattr(module, "_recover_runtime_credentials", recover, raising=False)
    monkeypatch.setattr(
        module,
        "_seal_runtime_credentials",
        lambda _credentials: pytest.fail("replayed install must not begin a new transaction"),
    )

    replay = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    credential_instances = tuple(
        sorted(
            replay.instance_mapping[service_id]
            for service_id in (
                "auction/source:primary",
                "minute/source:primary",
                "notifier-admin",
            )
        )
    )
    assert observed == {
        "bundle_generation": first.generation_hash,
        "instances": credential_instances,
        "action": "commit",
    }
    assert replay.generation_hash == first.generation_hash
    assert os.readlink(root / "current") == f"generations/{first.generation_hash}"


def test_ambiguous_restart_recovery_fails_closed_before_new_begin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    monkeypatch.setattr(
        module,
        "_recover_runtime_credentials",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("ambiguous active credential transaction")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "_seal_runtime_credentials",
        lambda _credentials: pytest.fail("recovery failure must prevent begin"),
    )

    with pytest.raises(RuntimeError, match="recovery.*fail|failed.*recover|fail.closed"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    audit_files = tuple((root / "failed-deployments").glob("*.json"))
    assert len(audit_files) == 1
    audit = json.loads(audit_files[0].read_text())
    assert audit["status"] == "recovery_failed"
    assert audit["runtime_current"] is None


def test_first_publish_failure_leaves_no_visible_runtime_or_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    visible_credentials: dict[str, bytes] = {}
    monkeypatch.setattr(
        module,
        "_seal_runtime_credentials",
        lambda credentials: _CredentialTransactionStub(
            dict(credentials),  # type: ignore[arg-type]
            visible=visible_credentials,
        ),
    )
    monkeypatch.setattr(
        module,
        "_publish_current",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("first publish failed")),
    )

    with pytest.raises(OSError, match="first publish failed"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert visible_credentials == {}


def test_first_runtime_schema_install_requires_explicit_audited_bootstrap(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(RuntimeSchemaBootstrapRequiredError, match="explicit.*bootstrap"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not root.exists()


def test_schema_bootstrap_is_hash_bound_inside_immutable_generation(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    receipt = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="adopt existing runtime payload contracts",
    )

    generation = root / "generations" / receipt.generation_hash
    contract = json.loads((generation / "schema-contracts.json").read_text())
    audit = json.loads((generation / "schema-bootstrap.json").read_text())
    basis = json.loads((generation / "generation-basis.json").read_text())
    assert contract["producer_commit"] == COMMIT
    assert contract["content_hash"] == canonical_sha256(
        {key: value for key, value in contract.items() if key != "content_hash"}
    )
    assert audit["reason"] == "adopt existing runtime payload contracts"
    assert audit["contract_content_hash"] == contract["content_hash"]
    assert audit["content_hash"] == canonical_sha256(
        {key: value for key, value in audit.items() if key != "content_hash"}
    )
    assert canonical_sha256(basis) == receipt.generation_hash
    assert (
        basis["schema_contract_sha256"]
        == hashlib.sha256((generation / "schema-contracts.json").read_bytes()).hexdigest()
    )
    assert (
        basis["schema_bootstrap_sha256"]
        == hashlib.sha256((generation / "schema-bootstrap.json").read_bytes()).hexdigest()
    )


def test_same_schema_generation_replay_is_idempotent_without_bootstrap_bypass(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )

    replay = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    assert replay.generation_hash == first.generation_hash
    assert len(tuple((root / "generations").iterdir())) == 1


def test_bootstrap_argument_cannot_bypass_an_existing_schema_generation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )

    with pytest.raises(RuntimeSchemaBootstrapRequiredError, match="already bootstrapped"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
            schema_bootstrap_reason="silently ignore compatibility",
        )


def test_missing_or_tampered_previous_schema_contract_fails_before_publish(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )
    generation = root / "generations" / first.generation_hash
    contract_path = generation / "schema-contracts.json"
    original = contract_path.read_bytes()
    contract_path.unlink()

    with pytest.raises(RuntimeSchemaCompatibilityError, match="missing"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )
    assert os.readlink(root / "current") == f"generations/{first.generation_hash}"

    contract_path.write_bytes(original.replace(b"minute/source:primary", b"minute/source:tampered"))
    with pytest.raises(RuntimeSchemaCompatibilityError, match="invalid|canonical|contract"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )


def test_rehashed_previous_schema_contract_remains_bound_to_generation(
    tmp_path: Path,
) -> None:
    from rquant.runtime_schema_registry import RuntimeSchemaContractBundle

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )
    generation = root / "generations" / first.generation_hash
    contract_path = generation / "schema-contracts.json"
    contract = json.loads(contract_path.read_text())
    first_service = next(iter(contract["manifest_fingerprints"]))
    contract["manifest_fingerprints"][first_service] = "f" * 64
    contract["content_hash"] = canonical_sha256(
        {key: value for key, value in contract.items() if key != "content_hash"}
    )
    rewritten = RuntimeSchemaContractBundle.model_validate(contract)
    contract_path.write_text(rewritten.model_dump_json())
    bootstrap_path = generation / "schema-bootstrap.json"
    bootstrap = json.loads(bootstrap_path.read_text())
    bootstrap["contract_content_hash"] = rewritten.content_hash
    bootstrap["content_hash"] = canonical_sha256(
        {key: value for key, value in bootstrap.items() if key != "content_hash"}
    )
    bootstrap_path.write_text(
        json.dumps(bootstrap, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    )

    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    with pytest.raises(
        RuntimeSchemaCompatibilityError,
        match="hash-bound.*generation|generation.*hash-bound",
    ):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == f"generations/{first.generation_hash}"


def test_explicit_bootstrap_can_adopt_a_legacy_generation_without_schema_contract(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    legacy_hash = "1" * 64
    legacy = root / "generations" / legacy_hash
    legacy.mkdir(parents=True, mode=0o700)
    legacy.chmod(0o700)
    os.symlink(f"generations/{legacy_hash}", root / "current")
    manifests, capabilities = _bundle_inputs(root)

    receipt = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="reviewed legacy generation adoption",
    )

    audit = json.loads(
        (root / "generations" / receipt.generation_hash / "schema-bootstrap.json").read_text()
    )
    assert audit["previous_generation"] == f"generations/{legacy_hash}"


def test_legacy_v1_schema_contract_requires_explicit_audited_migration(
    tmp_path: Path,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )
    generation = root / "generations" / first.generation_hash
    legacy_payload = json.loads((generation / "schema-contracts.json").read_text())
    legacy_payload["schema_version"] = 1
    legacy_payload["content_hash"] = "0" * 64
    legacy_bytes = json.dumps(
        legacy_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    basis = json.loads((generation / "generation-basis.json").read_text())
    basis["schema_contract_sha256"] = hashlib.sha256(legacy_bytes).hexdigest()
    basis["schema_bootstrap_sha256"] = None
    rewritten_basis = module._RuntimeGenerationBasis.model_validate(basis)
    legacy_hash = canonical_sha256(rewritten_basis.model_dump(mode="python"))
    (generation / "schema-contracts.json").write_bytes(legacy_bytes)
    (generation / "schema-bootstrap.json").unlink()
    (generation / "generation-basis.json").write_bytes(rewritten_basis.model_dump_json().encode())
    generation.rename(root / "generations" / legacy_hash)
    (root / "current").unlink()
    (root / "current").symlink_to(Path("generations") / legacy_hash)

    candidate_manifests = (
        manifests[0].model_copy(update={"interval_seconds": 2}),
        *manifests[1:],
    )
    with pytest.raises(RuntimeSchemaCompatibilityError, match="legacy v1"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=candidate_manifests,
            capability_env=capabilities,
        )

    candidate = build_runtime_schema_contract_bundle(
        candidate_manifests,
        producer_commit=COMMIT,
    )
    reviews = tuple(
        RuntimeSchemaV1LifecycleReview(
            channel_id=channel.channel_id,
            field_name=field.name,
            introduced_in=field.introduced_in,
            deprecated_in=field.deprecated_in,
            removed_in=field.removed_in,
        )
        for channel in candidate.channels
        for field in channel.declaration.fields
    )
    authorization = RuntimeSchemaV1MigrationAuthorization(
        reason="reviewed every legacy field lifecycle",
        reviewed_lifecycles=reviews,
        migrated_at=datetime(2026, 8, 2, 1, 0, tzinfo=UTC),
    )
    migrated = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=candidate_manifests,
        capability_env=capabilities,
        schema_v1_migration=authorization,
    )

    assert migrated.previous_generation_hash == legacy_hash
    audits = tuple((root / "control" / "schema-migrations").glob("*.json"))
    assert len(audits) == 1
    audit = json.loads(audits[0].read_text())
    installed_contract = json.loads(
        (root / "generations" / migrated.generation_hash / "schema-contracts.json").read_text()
    )
    assert audit["status"] == "explicit_v1_migration"
    assert audit["legacy_payload_sha256"] == hashlib.sha256(legacy_bytes).hexdigest()
    assert audit["candidate_content_hash"] == installed_contract["content_hash"]


def test_broken_schema_contract_symlink_cannot_pose_as_legacy_bootstrap(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    legacy_hash = "1" * 64
    legacy = root / "generations" / legacy_hash
    legacy.mkdir(parents=True, mode=0o700)
    legacy.chmod(0o700)
    os.symlink("missing-contract.json", legacy / "schema-contracts.json")
    os.symlink(f"generations/{legacy_hash}", root / "current")
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(RuntimeSchemaCompatibilityError, match="unsafe"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
            schema_bootstrap_reason="must not bypass unsafe identity",
        )


def test_schema_transition_preflight_runs_before_credentials_or_current_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )
    monkeypatch.setattr(
        module,
        "_validate_runtime_schema_transition",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeSchemaCompatibilityError("new producer -> old consumer incompatible")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "_seal_runtime_credentials",
        lambda _credentials: pytest.fail("schema preflight must run before credential begin"),
    )

    with pytest.raises(RuntimeSchemaCompatibilityError, match="new producer"):
        _install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == f"generations/{first.generation_hash}"


def test_deployment_schema_rollout_is_persistent_idempotent_and_rolls_back_current(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    feature_manifest = _manifest(
        root,
        service_id="feature-live",
        kind=RuntimeServiceKind.FEATURE_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    manifests = (*manifests, feature_manifest)
    capabilities = {**capabilities, feature_manifest.service_id: {}}
    first = _install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="first reviewed bootstrap",
    )
    next_commit = "b" * 40
    candidate_manifests = tuple(
        manifest.model_copy(update={"producer_commit": next_commit}) for manifest in manifests
    )
    second = _install_runtime_deployment_bundle(
        root,
        producer_commit=next_commit,
        manifests=candidate_manifests,
        capability_env=capabilities,
    )
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)

    prepared = prepare_runtime_schema_rollout(
        root,
        previous_generation_id=first.generation_hash,
        target_generation_id=second.generation_hash,
        channel_id="runtime.market_minute.batch-envelope",
        started_at=started_at,
        deadline=started_at + timedelta(hours=1),
        consumer_ack_max_age_seconds=300,
    )
    replayed = prepare_runtime_schema_rollout(
        root,
        previous_generation_id=first.generation_hash,
        target_generation_id=second.generation_hash,
        channel_id="runtime.market_minute.batch-envelope",
        started_at=started_at,
        deadline=started_at + timedelta(hours=1),
        consumer_ack_max_age_seconds=300,
    )

    assert replayed == prepared
    authority, store = load_runtime_schema_rollout(root, plan_id=prepared.plan_id)
    assert authority.plan == prepared.plan
    assert store.get_state(prepared.plan_id).phase is RolloutPhase.PREPARE
    state = rollback_runtime_schema_rollout(
        root,
        plan_id=prepared.plan_id,
        expected_revision=0,
        reason="candidate process crashed",
        now=started_at + timedelta(seconds=1),
        operation_id="rollback-after-crash",
    )

    assert state.phase is RolloutPhase.ROLLBACK
    assert os.readlink(root / "current") == f"generations/{first.generation_hash}"
    reopened_authority, reopened_store = load_runtime_schema_rollout(
        root,
        plan_id=prepared.plan_id,
    )
    assert reopened_authority == authority
    assert reopened_store.get_state(prepared.plan_id) == state


def test_rollback_failure_is_fail_closed_and_preserves_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    monkeypatch.setattr(
        module,
        "_seal_runtime_credentials",
        lambda credentials: _CredentialTransactionStub(
            dict(credentials),  # type: ignore[arg-type]
            fail_rollback=True,
        ),
    )
    monkeypatch.setattr(
        module,
        "_publish_current",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("publish failed")),
    )

    with pytest.raises(RuntimeError, match="fail.closed|rollback"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    audit_files = tuple((root / "failed-deployments").glob("*.json"))
    assert len(audit_files) == 1
    audit = json.loads(audit_files[0].read_text())
    assert audit["status"] == "rollback_failed"
    assert audit["runtime_current"] is None
    assert audit["credential_rollback"] == "failed"


def test_upgrade_credential_rollback_failure_hides_runtime_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    monkeypatch.setattr(
        module,
        "_seal_runtime_credentials",
        lambda credentials: _CredentialTransactionStub(
            dict(credentials),  # type: ignore[arg-type]
            fail_rollback=True,
        ),
    )
    monkeypatch.setattr(
        module,
        "_publish_current",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("upgrade publish failed")),
    )

    with pytest.raises(RuntimeError, match="failed closed|fail.closed"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    audit_files = tuple((root / "failed-deployments").glob("*.json"))
    assert len(audit_files) == 1
    assert json.loads(audit_files[0].read_text())["runtime_current"] is None


def test_rejects_symlinked_runtime_parent_without_writing_outside(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(outside, target_is_directory=True)
    root = linked / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="symlink"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (outside / "runtime").exists()


def test_rejects_symlinked_plane_path_that_escapes_its_owner(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "live").symlink_to(outside, target_is_directory=True)
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="symlink"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert not tuple(outside.iterdir())
