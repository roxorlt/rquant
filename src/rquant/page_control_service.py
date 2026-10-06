"""Loopback HTTP service for page control commands."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
from collections.abc import Callable, Sequence
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from loguru import logger

from rquant.canvas_publication_receipt import (
    CANVAS_PUBLICATION_PROBE_NAMESPACE,
    CanvasPublicationKeyring,
    CanvasPublicationSigner,
    Ed25519CanvasPublicationKeyring,
    Ed25519CanvasPublicationSigner,
    SecureCanvasPublicationSigningClient,
)
from rquant.factor.page_control_backend import (
    FactorDefinitionPageControlBackend as RegistryFactorBackend,
)
from rquant.factor.registry import FactorDefinitionRegistry
from rquant.formula_market_page_backend import FormulaMarketPageBackend
from rquant.formula_market_private_config import load_private_formula_market_config
from rquant.formula_pool_definition import FormulaPoolDefinitionStore, FormulaPoolSaveBackend
from rquant.job_center_authority import resolve_current_job_center_authority_binding
from rquant.lab_daemon import load_lab_job_center_authority_manifest
from rquant.lab_page_control import build_lab_page_control_writer
from rquant.page_control import (
    DEFAULT_PAGE_CONTROL_SERVICE_ID,
    AckAlert,
    BackfillPlanPageControlBackend,
    DataAuditReportPageControlBackend,
    FactorDefinitionPageControlBackend,
    FormulaMarketPageControlBackend,
    FormulaPoolPageControlBackend,
    LabPageControlBackend,
    PageControlCommandConflictError,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    parse_page_control_command,
)
from rquant.pool_result_receipt import DailyWriterCapability, PublishedDailyScreenEvidence
from rquant.research_manifest import detect_verified_code_commit
from rquant.runtime_shadow_validation import _ed25519_signing_payload
from rquant.screen.query_contracts import ScreenQueryDefinition
from rquant.screen.query_history import ScreenQueryHistory, prepare_private_screen_outbox
from rquant.strict_json import canonical_json_bytes
from rquant.web.condition_alert_commands import ConditionRuleScopeResolver, condition_scope_resolver
from rquant.web.models.screen import ScreenRunData

if TYPE_CHECKING:
    from rquant.config import Settings
    from rquant.experiment_platform_commands import ExperimentPageControlBackend
    from rquant.paper_portfolio_commands import PaperPortfolioPageControlBackend
    from rquant.strategy_authoring import StrategyAuthoringPageControlBackend

PRODUCTION_CANVAS_SIGNER_COMMAND = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-canvas-publication-signer",
)


def _settings() -> Settings:
    """Read the process settings at call time, never at import time.

    This module is the `page_control` role's entry point. The wrapper hands its child an
    environment holding `LANG` / `LC_ALL` / `TZ` and nothing else, so a module-level
    `Settings` would make the import itself impossible there. A `settings` that a test has
    bound onto this module still wins, exactly as the old module-level name did.
    """

    bound = globals().get("settings")
    if bound is not None:
        return bound  # type: ignore[no-any-return]
    from rquant.config import get_settings

    return get_settings()


def __getattr__(name: str) -> object:
    """`rquant.page_control_service.settings` stays readable, built on first use."""

    if name == "settings":
        from rquant.config import get_settings

        return get_settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _IPv6ThreadingHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6


def _server_class_for_host(host: str) -> type[ThreadingHTTPServer]:
    if host == "::1":
        return _IPv6ThreadingHTTPServer
    if host in {"127.0.0.1", "localhost"}:
        return ThreadingHTTPServer
    raise ValueError("page control service must bind to loopback")


def _build_lab_backend() -> object | None:
    code_sha = detect_verified_code_commit(
        trusted_git_path=_settings().lab_trusted_git_path,
    )
    deployment_root = os.environ.get("RQUANT_RUNTIME_ROOT", "")
    if code_sha is None or not deployment_root:
        return None
    binding = resolve_current_job_center_authority_binding(
        Path(deployment_root),
        expected_code_sha=code_sha,
        runtime_root=_settings().lab_runtime_dir_resolved,
        lab_jobs_path=_settings().lab_jobs_path_resolved,
        command_spool_path=_settings().lab_job_command_dir_resolved,
        final_artifact_root=_settings().lab_final_artifact_dir_resolved,
    )
    manifest = load_lab_job_center_authority_manifest(
        binding.runtime_root / "job-center-authority.json",
        expected_code_sha=code_sha,
        expected_research_root=binding.runtime_root,
        expected_lab_jobs_path=binding.lab_jobs_path,
        expected_command_spool_path=binding.command_spool_path,
        expected_final_artifact_root=binding.final_artifact_root,
        expected_runtime_deployment_root=binding.runtime_deployment_root,
        expected_deployment_profile_id=binding.deployment_profile_id,
        expected_deployment_generation_hash=binding.deployment_generation_hash,
    )
    return build_lab_page_control_writer(manifest)


def build_page_control_service(
    *,
    outbox_path: Path | None = None,
    data_dir: Path | None = None,
    log_dir: Path | None = None,
    allowed_lab_export_roots: tuple[Path, ...] | None = None,
    lab_backend: LabPageControlBackend | None = None,
    experiment_backend: ExperimentPageControlBackend | None = None,
    backfill_plan_backend: BackfillPlanPageControlBackend | None = None,
    data_audit_report_backend: DataAuditReportPageControlBackend | None = None,
    formula_market_backend: FormulaMarketPageControlBackend | None = None,
    formula_pool_backend: FormulaPoolPageControlBackend | None = None,
    factor_definition_backend: FactorDefinitionPageControlBackend | None = None,
    strategy_authoring_backend: StrategyAuthoringPageControlBackend | None = None,
    paper_portfolio_backend: PaperPortfolioPageControlBackend | None = None,
    screen_query_executor: Callable[[ScreenQueryDefinition], ScreenRunData] | None = None,
    screen_query_cursor_key: bytes | None = None,
    condition_rule_scope: ConditionRuleScopeResolver | None = None,
    daily_writer_capability: Callable[[], DailyWriterCapability | None] | None = None,
    daily_run_evidence: Callable[[], tuple[PublishedDailyScreenEvidence, ...]] | None = None,
    load_default_lab_backend: bool = True,
    clock: Callable[[], datetime] | None = None,
    lease_seconds: int = 30,
    consumer_service_id: str = DEFAULT_PAGE_CONTROL_SERVICE_ID,
    consumer_instance_id: str | None = None,
    canvas_publication_signer: CanvasPublicationSigner | None = None,
    canvas_publication_keyring: CanvasPublicationKeyring | None = None,
) -> PageControlService:
    return build_page_control_service_with_dependencies(
        outbox_path=outbox_path,
        data_dir=data_dir,
        log_dir=log_dir,
        allowed_lab_export_roots=allowed_lab_export_roots,
        lab_backend=lab_backend,
        experiment_backend=experiment_backend,
        backfill_plan_backend=backfill_plan_backend,
        data_audit_report_backend=data_audit_report_backend,
        formula_market_backend=formula_market_backend,
        formula_pool_backend=formula_pool_backend,
        factor_definition_backend=factor_definition_backend,
        strategy_authoring_backend=strategy_authoring_backend,
        paper_portfolio_backend=paper_portfolio_backend,
        screen_query_executor=screen_query_executor,
        screen_query_cursor_key=screen_query_cursor_key,
        condition_rule_scope=condition_rule_scope,
        daily_writer_capability=daily_writer_capability,
        daily_run_evidence=daily_run_evidence,
        load_default_lab_backend=load_default_lab_backend,
        clock=clock,
        lease_seconds=lease_seconds,
        consumer_service_id=consumer_service_id,
        consumer_instance_id=consumer_instance_id,
        canvas_publication_signer=canvas_publication_signer,
        canvas_publication_keyring=canvas_publication_keyring,
    )


def build_page_control_service_with_dependencies(
    *,
    outbox_path: Path | None = None,
    data_dir: Path | None = None,
    log_dir: Path | None = None,
    allowed_lab_export_roots: tuple[Path, ...] | None = None,
    lab_backend: LabPageControlBackend | None = None,
    experiment_backend: ExperimentPageControlBackend | None = None,
    backfill_plan_backend: BackfillPlanPageControlBackend | None = None,
    data_audit_report_backend: DataAuditReportPageControlBackend | None = None,
    formula_market_backend: FormulaMarketPageControlBackend | None = None,
    formula_pool_backend: FormulaPoolPageControlBackend | None = None,
    factor_definition_backend: FactorDefinitionPageControlBackend | None = None,
    strategy_authoring_backend: StrategyAuthoringPageControlBackend | None = None,
    paper_portfolio_backend: PaperPortfolioPageControlBackend | None = None,
    screen_query_executor: Callable[[ScreenQueryDefinition], ScreenRunData] | None = None,
    screen_query_cursor_key: bytes | None = None,
    condition_rule_scope: ConditionRuleScopeResolver | None = None,
    daily_writer_capability: Callable[[], DailyWriterCapability | None] | None = None,
    daily_run_evidence: Callable[[], tuple[PublishedDailyScreenEvidence, ...]] | None = None,
    load_default_lab_backend: bool = True,
    clock: Callable[[], datetime] | None = None,
    lease_seconds: int = 30,
    consumer_service_id: str = DEFAULT_PAGE_CONTROL_SERVICE_ID,
    consumer_instance_id: str | None = None,
    canvas_publication_signer: CanvasPublicationSigner | None = None,
    canvas_publication_keyring: CanvasPublicationKeyring | None = None,
) -> PageControlService:
    if (canvas_publication_signer is None) != (canvas_publication_keyring is None):
        raise ValueError(
            "CanvasPublicationReceipt signer and public keyring must be provided together"
        )
    if allowed_lab_export_roots is None:
        configured_roots = os.environ.get("RQUANT_PAGE_CONTROL_ALLOWED_EXPORT_ROOTS", "")
        allowed_roots = tuple(
            Path(value.strip()) for value in configured_roots.split(os.pathsep) if value.strip()
        ) or (_settings().lab_runtime_dir_resolved / "exports",)
    else:
        allowed_roots = allowed_lab_export_roots
    if (screen_query_executor is None) != (screen_query_cursor_key is None):
        raise ValueError("private screening requires explicit executor and shared cursor material")
    path = Path(
        outbox_path
        or os.environ.get(
            "RQUANT_PAGE_CONTROL_OUTBOX", _settings().data_dir / "page-control.sqlite3"
        )
    )
    if screen_query_cursor_key is not None:
        prepare_private_screen_outbox(path)
    outbox = PageControlOutbox(path)
    screen_history = (
        None
        if screen_query_cursor_key is None
        else ScreenQueryHistory(outbox, cursor_key=screen_query_cursor_key)
    )
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=_settings().data_dir if data_dir is None else data_dir,
            log_dir=_settings().log_dir if log_dir is None else log_dir,
            allowed_lab_export_roots=allowed_roots,
            lab_backend=(
                lab_backend
                if lab_backend is not None or not load_default_lab_backend
                else _build_lab_backend()
            ),
            backfill_plan_backend=backfill_plan_backend,
            experiment_backend=experiment_backend,
            data_audit_report_backend=data_audit_report_backend,
            formula_market_backend=formula_market_backend,
            formula_pool_backend=formula_pool_backend,
            factor_definition_backend=factor_definition_backend,
            strategy_authoring_backend=strategy_authoring_backend,
            paper_portfolio_backend=paper_portfolio_backend,
            screen_query_history=screen_history,
            screen_query_executor=screen_query_executor,
            condition_rule_scope=condition_rule_scope,
            daily_writer_capability=daily_writer_capability,
            daily_run_evidence=daily_run_evidence,
            clock=clock,
            lease_seconds=lease_seconds,
            consumer_id=consumer_instance_id,
            consumer_service_id=consumer_service_id,
            canvas_publication_signer=canvas_publication_signer,
            canvas_publication_keyring=canvas_publication_keyring,
        ),
    )


def handler_for(service: PageControlService) -> type[BaseHTTPRequestHandler]:
    class PageControlHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {"/v1/commands", "/v1/commands/lookup"}:
                self.send_error(404)
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= content_length <= 1024 * 1024:
                    raise ValueError("request body must be between 1 byte and 1 MiB")
                payload = json.loads(self.rfile.read(content_length))
                command = parse_page_control_command(payload)
            except Exception as exc:
                self._write_json(
                    400,
                    {"error": f"{type(exc).__name__}: {exc}"},
                )
                return
            if self.path == "/v1/commands/lookup":
                if not isinstance(command, AckAlert):
                    self._write_json(400, {"error": "lookup requires ack_alert"})
                    return
                try:
                    receipt = service.lookup_ack_command(command)
                except ValueError:
                    self._write_json(409, {"error": "command conflict"})
                    return
                except Exception:
                    self._write_json(503, {"error": "lookup unavailable"})
                    return
                response = (
                    {"found": False}
                    if receipt is None
                    else {"found": True, "receipt": receipt.model_dump(mode="json")}
                )
            else:
                try:
                    response = service.submit(command).model_dump(mode="json")
                except PageControlCommandConflictError:
                    self._write_json(409, {"error": "command conflict"})
                    return
                except Exception as exc:
                    self._write_json(400, {"error": f"{type(exc).__name__}: {exc}"})
                    return
            self._write_json(200, response)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _write_json(self, status: int, payload: object) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return PageControlHandler


def build_parser() -> argparse.ArgumentParser:
    """The arguments the fixed root-owned runtime wrapper derives for this role.

    The values come from the two root-owned documents, never from the unit: `--manifest`
    identifies the authorised instance inside the generation, `--control-root` is the
    profile's root-owned prefix plus that label, and the two identity flags come from the
    current authority slot. This service only needs the runtime root and the commit, but it
    accepts and validates the whole set so that a mismatch fails loudly here rather than
    being silently ignored.
    """

    parser = argparse.ArgumentParser(description="Run the rQuant PageControl authority")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--control-root", required=True, type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument(
        "--formula-market-config",
        type=Path,
        help="owner-private local formula admission config; absent means disabled",
    )
    parser.add_argument(
        "--watchlist-socket",
        type=Path,
        help="owner-private local watchlist admission socket; absent means disabled",
    )
    parser.add_argument(
        "--price-rule-socket",
        type=Path,
        help="separate-UID private price rule socket; absent means disabled",
    )
    parser.add_argument(
        "--price-rule-web-uid",
        type=int,
        help="dedicated trusted Web UID for price rule admission",
    )
    parser.add_argument(
        "--price-rule-shared-gid",
        type=int,
        help="shared private socket GID for price rule admission",
    )
    parser.add_argument(
        "--condition-rule-serving-root",
        type=Path,
        help="explicit original Serving source for full-condition scope; absent disables enable",
    )
    parser.add_argument(
        "--condition-rule-activate",
        action="store_true",
        help="explicitly install the additive owned condition rule schema",
    )
    parser.add_argument(
        "--screen-query-config",
        type=Path,
        help="explicit owner-private screening config; absent means disabled",
    )
    parser.add_argument("--factor-archive-socket", type=Path)
    parser.add_argument("--factor-archive-registry", type=Path)
    parser.add_argument("--factor-archive-web-uid", type=int)
    parser.add_argument("--factor-archive-shared-gid", type=int)
    parser.add_argument("--factor-archive-editors")
    parser.add_argument("--factor-save-enabled", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_root: Path | None = None,
    expected_commit: str | None = None,
    ack_socket_path: Path | None = None,
    ack_serving_root: Path | None = None,
    watchlist_socket_path: Path | None = None,
    price_rule_socket_path: Path | None = None,
    price_rule_web_uid: int | None = None,
    price_rule_shared_gid: int | None = None,
    factor_archive_socket_path: Path | None = None,
    factor_archive_registry_path: Path | None = None,
    factor_archive_web_uid: int | None = None,
    factor_archive_shared_gid: int | None = None,
    factor_archive_editors: str | None = None,
    factor_save_enabled: bool = False,
    formula_market_config_path: Path | None = None,
    condition_rule_serving_root: Path | None = None,
    condition_rule_activate: bool = False,
    screen_query_config_path: Path | None = None,
) -> None:
    """Entry point. `argv` is what the runtime wrapper derived; keywords are for tests."""

    if argv is not None:
        arguments = build_parser().parse_args(list(argv))
        if screen_query_config_path is not None and arguments.screen_query_config is not None:
            raise ValueError("screen private config was supplied twice")
        screen_query_config_path = screen_query_config_path or arguments.screen_query_config
        condition_rule_serving_root = (
            condition_rule_serving_root or arguments.condition_rule_serving_root
        )
        condition_rule_activate = condition_rule_activate or arguments.condition_rule_activate
        expected_commit = expected_commit or arguments.expected_commit
        if formula_market_config_path is not None and arguments.formula_market_config is not None:
            raise ValueError("formula market config was supplied twice")
        formula_market_config_path = formula_market_config_path or arguments.formula_market_config
        if watchlist_socket_path is not None and arguments.watchlist_socket is not None:
            raise ValueError("watchlist socket was supplied twice")
        watchlist_socket_path = watchlist_socket_path or arguments.watchlist_socket
        if price_rule_socket_path is not None and arguments.price_rule_socket is not None:
            raise ValueError("price rule socket was supplied twice")
        if price_rule_web_uid is not None and arguments.price_rule_web_uid is not None:
            raise ValueError("price rule Web UID was supplied twice")
        if price_rule_shared_gid is not None and arguments.price_rule_shared_gid is not None:
            raise ValueError("price rule shared GID was supplied twice")
        price_rule_socket_path = price_rule_socket_path or arguments.price_rule_socket
        price_rule_web_uid = (
            price_rule_web_uid if price_rule_web_uid is not None else arguments.price_rule_web_uid
        )
        price_rule_shared_gid = (
            price_rule_shared_gid
            if price_rule_shared_gid is not None
            else arguments.price_rule_shared_gid
        )
        factor_archive_socket_path = factor_archive_socket_path or arguments.factor_archive_socket
        factor_archive_registry_path = (
            factor_archive_registry_path or arguments.factor_archive_registry
        )
        factor_archive_web_uid = (
            factor_archive_web_uid
            if factor_archive_web_uid is not None
            else arguments.factor_archive_web_uid
        )
        factor_archive_shared_gid = (
            factor_archive_shared_gid
            if factor_archive_shared_gid is not None
            else arguments.factor_archive_shared_gid
        )
        factor_archive_editors = factor_archive_editors or arguments.factor_archive_editors
        factor_save_enabled = factor_save_enabled or arguments.factor_save_enabled
    return _serve(
        runtime_root=runtime_root,
        expected_commit=expected_commit,
        ack_socket_path=ack_socket_path,
        ack_serving_root=ack_serving_root,
        watchlist_socket_path=watchlist_socket_path,
        price_rule_socket_path=price_rule_socket_path,
        price_rule_web_uid=price_rule_web_uid,
        price_rule_shared_gid=price_rule_shared_gid,
        factor_archive_socket_path=factor_archive_socket_path,
        factor_archive_registry_path=factor_archive_registry_path,
        factor_archive_web_uid=factor_archive_web_uid,
        factor_archive_shared_gid=factor_archive_shared_gid,
        factor_archive_editors=factor_archive_editors,
        factor_save_enabled=factor_save_enabled,
        formula_market_config_path=formula_market_config_path,
        condition_rule_serving_root=condition_rule_serving_root,
        condition_rule_activate=condition_rule_activate,
        screen_query_config_path=screen_query_config_path,
    )


def _serve(
    *,
    runtime_root: Path | None = None,
    expected_commit: str | None = None,
    ack_socket_path: Path | None = None,
    ack_serving_root: Path | None = None,
    watchlist_socket_path: Path | None = None,
    price_rule_socket_path: Path | None = None,
    price_rule_web_uid: int | None = None,
    price_rule_shared_gid: int | None = None,
    factor_archive_socket_path: Path | None = None,
    factor_archive_registry_path: Path | None = None,
    factor_archive_web_uid: int | None = None,
    factor_archive_shared_gid: int | None = None,
    factor_archive_editors: str | None = None,
    factor_save_enabled: bool = False,
    formula_market_config_path: Path | None = None,
    condition_rule_serving_root: Path | None = None,
    condition_rule_activate: bool = False,
    screen_query_config_path: Path | None = None,
) -> None:
    from rquant.runtime_deployment_profile import (
        LINUX_PRODUCTION_RUNTIME_ROOT,
        PRODUCTION_CANVAS_SIGNER_COMMAND,
        load_current_runtime_deployment_profile,
    )

    resolved_runtime_root = runtime_root or LINUX_PRODUCTION_RUNTIME_ROOT
    is_production_root = (
        Path(os.path.abspath(resolved_runtime_root)) == LINUX_PRODUCTION_RUNTIME_ROOT
    )
    profile = load_current_runtime_deployment_profile(resolved_runtime_root)
    resolved_commit = expected_commit or detect_verified_code_commit(
        trusted_git_path=_settings().lab_trusted_git_path,
    )
    if resolved_commit is None or profile.producer_commit != resolved_commit:
        raise ValueError("PageControl runtime profile commit is not the running commit")
    if is_production_root and profile.runtime_mode != "linux-production":
        raise ValueError("production PageControl entrypoint requires a linux-production profile")
    page_profile = profile.page_control
    if page_profile is None or page_profile.canvas_publication is None:
        raise ValueError("Canvas publication authority is missing from runtime profile")
    if page_profile.data_dir is None or page_profile.log_dir is None:
        raise ValueError("Canvas publication storage authority is missing from runtime profile")
    if page_profile.page_projection_canvas_catalog_root != page_profile.data_dir / "canvases":
        raise ValueError(
            "Canvas catalog root must remain derived from the PageControl data directory"
        )
    canvas_profile = page_profile.canvas_publication
    if profile.runtime_mode == "linux-production" and (
        canvas_profile.signer_command != PRODUCTION_CANVAS_SIGNER_COMMAND
    ):
        raise ValueError("production Canvas signer must use the fixed protected capability")
    keyring = Ed25519CanvasPublicationKeyring(
        active_key_id=canvas_profile.active_key_id,
        active_public_key=canvas_profile.active_public_key_pem.encode("utf-8"),
        previous_public_keys={
            key_id: public_key.encode("utf-8")
            for key_id, public_key in canvas_profile.previous_public_key_pems.items()
        },
    )
    signing_client = SecureCanvasPublicationSigningClient(
        command=canvas_profile.signer_command,
        key_id=canvas_profile.active_key_id,
        timeout_seconds=canvas_profile.timeout_seconds,
    )
    probe_body = canonical_json_bytes(
        {
            "contract": "serving-canvas-publication-startup-probe/v1",
            "profile_id": profile.profile_id,
            "producer_commit": profile.producer_commit,
            "consumer_service_id": canvas_profile.consumer_service_id,
            "consumer_instance_id": canvas_profile.consumer_instance_id,
        }
    )
    probe_payload = _ed25519_signing_payload(
        namespace=CANVAS_PUBLICATION_PROBE_NAMESPACE,
        payload=probe_body,
    )
    probe_signature = signing_client.sign(
        namespace=CANVAS_PUBLICATION_PROBE_NAMESPACE,
        payload=probe_payload,
    )
    if not keyring.verify_detached_payload(
        key_id=canvas_profile.active_key_id,
        payload=probe_payload,
        signature=probe_signature,
        require_active=True,
    ):
        raise RuntimeError("Canvas publication signer capability is not the active key")
    endpoint = urlsplit(page_profile.endpoint)
    host = endpoint.hostname or ""
    port = endpoint.port or 0
    if (
        endpoint.scheme != "http"
        or host not in {"127.0.0.1", "::1", "localhost"}
        or endpoint.path != "/v1/commands"
        or endpoint.query
        or endpoint.fragment
        or port <= 0
    ):
        raise ValueError("page control endpoint must be an explicit loopback command URL")
    formula_market_backend = None
    formula_pool_backend = None
    if formula_market_config_path is not None:
        formula_market_backend = FormulaMarketPageBackend(
            load_private_formula_market_config(formula_market_config_path)
        )
        formula_pool_backend = FormulaPoolSaveBackend(
            task_store=formula_market_backend.store,
            definitions=FormulaPoolDefinitionStore(
                definition_root=page_profile.data_dir / "formula_pools",
                rule_pool_root=page_profile.data_dir / "user_presets",
            ),
        )
    factor_fields = (
        factor_archive_socket_path,
        factor_archive_registry_path,
        factor_archive_web_uid,
        factor_archive_shared_gid,
        factor_archive_editors,
    )
    if any(value is not None for value in factor_fields) and not all(
        value is not None for value in factor_fields
    ):
        raise ValueError("factor archive listener requires socket, registry, IDs and editors")
    if factor_save_enabled and not all(value is not None for value in factor_fields):
        raise ValueError("factor save requires the private factor listener")
    factor_backend = None
    factor_editor_users: frozenset[str] = frozenset()
    if all(value is not None for value in factor_fields):
        assert factor_archive_registry_path is not None
        assert factor_archive_editors is not None
        names = tuple(name.strip() for name in factor_archive_editors.split(","))
        if not names or any(not name for name in names) or len(set(names)) != len(names):
            raise ValueError("factor archive editors must be distinct exact names")
        factor_editor_users = frozenset(names)
        factor_backend = RegistryFactorBackend(
            FactorDefinitionRegistry(factor_archive_registry_path)
        )
        factor_backend.identity()
    screen_config = None
    screen_executor = None
    if screen_query_config_path is not None:
        from rquant.screen.query_admission import (
            ScreenQueryExecutor,
            load_screen_query_private_config,
        )

        screen_config = load_screen_query_private_config(screen_query_config_path)
        others = (
            ack_socket_path,
            watchlist_socket_path,
            price_rule_socket_path,
            factor_archive_socket_path,
        )
        if any(
            other is not None and screen_config.socket_path.parent == other.parent
            for other in others
        ):
            raise ValueError("screen private endpoint requires a separate directory")
        screen_executor = ScreenQueryExecutor(screen_config)
    service = build_page_control_service(
        outbox_path=page_profile.outbox_path,
        data_dir=page_profile.data_dir,
        log_dir=page_profile.log_dir,
        allowed_lab_export_roots=(page_profile.data_dir / "exports",),
        formula_market_backend=formula_market_backend,
        formula_pool_backend=formula_pool_backend,
        condition_rule_scope=None
        if condition_rule_serving_root is None
        else condition_scope_resolver(condition_rule_serving_root),
        screen_query_executor=screen_executor,
        daily_writer_capability=None
        if screen_executor is None
        else screen_executor.daily_writer_capability,
        daily_run_evidence=None if screen_executor is None else screen_executor.daily_run_evidence,
        screen_query_cursor_key=None if screen_executor is None else screen_executor.cursor_key,
        factor_definition_backend=factor_backend,
        load_default_lab_backend=False,
        consumer_service_id=canvas_profile.consumer_service_id,
        consumer_instance_id=canvas_profile.consumer_instance_id,
        canvas_publication_signer=Ed25519CanvasPublicationSigner(
            key_id=canvas_profile.active_key_id,
            client=signing_client,
        ),
        canvas_publication_keyring=keyring,
    )
    if condition_rule_activate:
        if condition_rule_serving_root is None or price_rule_socket_path is None:
            raise ValueError(
                "condition installation requires the existing private peer and Serving source"
            )
        from datetime import UTC

        service.outbox.activate_condition_alert_rules(datetime.now(UTC))
    if (ack_socket_path is None) != (ack_serving_root is None):
        raise ValueError("ack socket and Serving root must be configured together")
    ack_server = None
    if ack_socket_path is not None and ack_serving_root is not None:
        try:
            from rquant.alert_ack_admission import AckAdmission, build_ack_admission_server

            ack_server = build_ack_admission_server(
                AckAdmission(service, ack_serving_root),
                socket_path=ack_socket_path,
            )
        except Exception:
            logger.exception("AckAlert admission listener disabled during startup")
    watchlist_server = None
    if watchlist_socket_path is not None:
        try:
            from rquant.watchlist_admission import (
                WatchlistAdmission,
                build_watchlist_admission_server,
            )

            watchlist_server = build_watchlist_admission_server(
                WatchlistAdmission(service), socket_path=watchlist_socket_path
            )
        except Exception:
            logger.exception("Watchlist admission listener disabled during startup")
    price_rule_server = None
    if any(
        value is not None
        for value in (price_rule_socket_path, price_rule_web_uid, price_rule_shared_gid)
    ):
        try:
            from rquant.price_alert_admission import (
                PriceAlertAdmission,
                build_price_alert_admission_server,
            )

            price_rule_server = build_price_alert_admission_server(
                PriceAlertAdmission(service),
                socket_path=price_rule_socket_path,
                trusted_web_uid=price_rule_web_uid,
                shared_gid=price_rule_shared_gid,
            )
        except Exception:
            logger.exception("Price rule admission listener disabled during startup")
    factor_archive_server = None
    if factor_backend is not None:
        try:
            from rquant.factor_definition_admission import (
                FactorDefinitionAdmission,
                build_factor_definition_admission_server,
            )

            factor_archive_server = build_factor_definition_admission_server(
                FactorDefinitionAdmission(
                    service, editor_users=factor_editor_users, save_enabled=factor_save_enabled
                ),
                socket_path=factor_archive_socket_path,
                trusted_web_uid=factor_archive_web_uid,
                shared_gid=factor_archive_shared_gid,
            )
        except Exception:
            logger.exception("Factor archive admission listener disabled during startup")
    screen_server = None
    server_class = _server_class_for_host(host)
    try:
        if screen_config is not None:
            from rquant.screen.query_admission import ScreenQueryPrivateServer

            screen_server = ScreenQueryPrivateServer(
                screen_config.socket_path,
                allowed_users=screen_config.allowed_users,
                trusted_web_uid=screen_config.trusted_web_uid,
                shared_gid=screen_config.shared_gid,
                control=service,
            )
        server = server_class((host, port), handler_for(service))
    except Exception:
        if screen_server is not None:
            screen_server.server_close()
        if ack_server is not None:
            ack_server.server_close()
        if watchlist_server is not None:
            watchlist_server.server_close()
        if price_rule_server is not None:
            price_rule_server.server_close()
        if factor_archive_server is not None:
            factor_archive_server.server_close()
        raise
    screen_thread = None
    screen_started = False
    ack_thread = None
    ack_started = False
    watchlist_thread = None
    watchlist_started = False
    price_rule_thread = None
    price_rule_started = False
    factor_archive_thread = None
    factor_archive_started = False
    try:
        if screen_server is not None:
            screen_thread = threading.Thread(target=screen_server.serve_forever, daemon=False)
            screen_thread.start()
            screen_started = True
        if ack_server is not None:
            ack_thread = threading.Thread(target=ack_server.serve_forever, daemon=True)
            ack_thread.start()
            ack_started = True
        if watchlist_server is not None:
            watchlist_thread = threading.Thread(target=watchlist_server.serve_forever, daemon=True)
            watchlist_thread.start()
            watchlist_started = True
        if price_rule_server is not None:
            price_rule_thread = threading.Thread(
                target=price_rule_server.serve_forever, daemon=True
            )
            price_rule_thread.start()
            price_rule_started = True
        if factor_archive_server is not None:
            factor_archive_thread = threading.Thread(
                target=factor_archive_server.serve_forever, daemon=True
            )
            factor_archive_thread.start()
            factor_archive_started = True
        server.serve_forever()
    finally:
        server.server_close()
        if screen_server is not None:
            if screen_started and screen_thread is not None:
                screen_server.shutdown()
                screen_thread.join()
            screen_server.server_close()
        if ack_server is not None:
            if ack_started and ack_thread is not None:
                ack_server.shutdown()
                ack_thread.join()
            ack_server.server_close()
        if watchlist_server is not None:
            if watchlist_started and watchlist_thread is not None:
                watchlist_server.shutdown()
                watchlist_thread.join()
            watchlist_server.server_close()
        if price_rule_server is not None:
            if price_rule_started and price_rule_thread is not None:
                price_rule_server.shutdown()
                price_rule_thread.join()
            price_rule_server.server_close()
        if factor_archive_server is not None:
            if factor_archive_started and factor_archive_thread is not None:
                factor_archive_server.shutdown()
                factor_archive_thread.join()
            factor_archive_server.server_close()


if __name__ == "__main__":
    main(sys.argv[1:])
