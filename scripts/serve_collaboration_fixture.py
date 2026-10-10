"""Serve the original collaboration owner for local synthetic browser tests."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sys
import tempfile
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from uuid import uuid4

from serve_web_fixture import _instant, _prepare_proxy_proof, running_clock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="synthetic Serving root to read")
    parser.add_argument(
        "--private-root", type=Path, required=True, help="new private owner directory"
    )
    parser.add_argument("--now", type=_instant, required=True, help="Serving clock start")
    parser.add_argument("--bind", required=True, help="IPv4 loopback host:port")
    parser.add_argument("--proxy-proof-file", type=Path, required=True)
    args = parser.parse_args(argv)
    host, port = args.bind.rsplit(":", 1)
    if host != "127.0.0.1" or not port.isdecimal() or not 1 <= int(port) <= 65535:
        parser.error("the synthetic collaboration fixture requires IPv4 loopback")
    private_root = args.private_root
    if not private_root.is_absolute() or private_root.resolve() != private_root:
        parser.error("--private-root must be an absolute canonical path")
    if args.proxy_proof_file.parent != private_root:
        parser.error("--proxy-proof-file must be inside the new private owner directory")
    private_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.environ.update(
        {
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "synthetic-test-token-0000000000000000",
            "DATA_DIR": str(private_root / "data"),
            "DUCKDB_PATH": str(private_root / "unused-primary.duckdb"),
            "PARQUET_DIR": str(private_root / "parquet"),
            "LOG_DIR": str(private_root / "logs"),
        }
    )
    server = None
    thread = None
    ipc_root = None

    def stop(_signal: int, _frame: FrameType | None) -> None:
        raise SystemExit(0)

    # Uvicorn replays SIGTERM after lifespan.close; let the owner finally run too.
    previous = signal.signal(signal.SIGTERM, stop)
    try:
        import uvicorn

        from rquant.collaboration_commands import PageControlRoleAuthority
        from rquant.collaboration_roles import RoleEntry, RoleState
        from rquant.experiment_registry import DateRange
        from rquant.factor.run_backend import FactorRunPageControlBackend
        from rquant.factor_definition_admission import (
            FactorDefinitionAdmission,
            build_factor_definition_admission_server,
        )
        from rquant.factor_run_admission import FactorRunAdmission
        from rquant.page_control import (
            PageControlConsumer,
            PageControlOutbox,
            PageControlService,
            PageControlStatus,
        )
        from rquant.portfolio_backtest_commands import (
            PortfolioCommandWriter,
            SubmitPortfolioBacktest,
        )
        from rquant.portfolio_backtest_source import (
            PortfolioExperimentProtocol,
            PortfolioRequestPreparer,
            PortfolioSourceData,
        )
        from rquant.research_catalog import ResearchCatalog
        from rquant.screen.query_history import prepare_private_screen_outbox
        from rquant.storage.duckdb import DuckDBStore
        from rquant.strategy_authoring import (
            StrategyAuthoringPageControlBackend,
            StrategyAuthoringStore,
        )
        from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
        from rquant.strategy_template_adapter import StrategyTemplateExecutionVersion
        from rquant.strategy_template_run_commands import RunStrategyTemplate
        from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
        from rquant.strategy_template_source import StrategyTemplateSourceData, TemplateRawDay
        from rquant.strategy_template_submission import (
            StrategyTemplateRunBackend,
            StrategyTemplateRunPreparer,
        )
        from rquant.web.app import create_app
        from rquant.web.collaboration_gateway import CollaborationGateway
        from rquant.web.settings import DEFAULT_BIND, WebSettings
        from tests.support.ai_assistance_fixture import (
            build_portfolio_foundation,
            private_directory,
        )
        from tests.unit.test_backtest_platform import config
        from tests.unit.test_backtest_preparation import source_data
        from tests.unit.test_factor_run_configuration import _configured
        from tests.unit.test_strategy_authoring import catalog as source_catalog
        from tests.unit.test_strategy_template_adapter import adapter_fixture

        _prepare_proxy_proof(args.proxy_proof_file)
        roles_path = private_root / "roles.json"
        state = RoleState.create(
            revision=1,
            users=(
                RoleEntry(username="admin", role="admin"),
                RoleEntry(username="alice", role="researcher"),
                RoleEntry(username="viewer", role="viewer"),
            ),
        )
        descriptor = os.open(
            roles_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(state.model_dump_json().encode())
            output.flush()
            os.fsync(output.fileno())
        outbox_path = private_root / "page-control.sqlite3"
        prepare_private_screen_outbox(outbox_path)
        outbox = PageControlOutbox(outbox_path)
        authority = PageControlRoleAuthority(mode="enforced", roles_path=roles_path)

        def operation_clock() -> datetime:
            return datetime.now(UTC)

        portfolio_source = source_data()
        foundation = build_portfolio_foundation(
            private_directory(private_root / "lab"), portfolio_source, clock=operation_clock
        )

        def original_portfolio_source(key: str, version: int) -> PortfolioSourceData:
            if (key, version) != (portfolio_source.source_key, portfolio_source.source_version):
                raise LookupError("original synthetic portfolio source is unavailable")
            return portfolio_source

        portfolio_preparer = PortfolioRequestPreparer(
            source_provider=original_portfolio_source,
            metadata_store_factory=lambda: DuckDBStore(foundation.metadata_path),
            catalog=ResearchCatalog(foundation.catalog_path),
            lake_root=foundation.lake_root,
            input_root=foundation.input_root,
            definitions=foundation.commands.definition_registry,
            experiments=foundation.commands.experiment_registry,
            protocol=foundation.protocol,
            code_commit=foundation.code_commit,
            clock=operation_clock,
        )
        strategy_root = private_directory(private_root / "template")
        store = StrategyAuthoringStore(
            strategy_root / "metadata.sqlite",
            definition_root=strategy_root / "definitions",
            producer_commit=foundation.code_commit,
            clock=operation_clock,
        )
        store.initialize()
        store, value, catalog, _ = adapter_fixture(strategy_root, existing_store=store)
        identity = store.identity()
        foundation.commands.template_directory = StrategyTemplateRuntimeDirectory(
            store, expected_identity=identity
        )
        template_source = StrategyTemplateSourceData(
            owner_id="alice",
            catalog=source_catalog(),
            portfolio=PortfolioSourceData(
                source_key="verified-screen",
                source_version=1,
                template=value.request,
                sources=value.sources,
                benchmarks={},
            ),
            days=tuple(
                TemplateRawDay(
                    trade_date=day.trade_date,
                    entry=day.entry.evidence,
                    index_closes=day.index_closes,
                    minutes=day.minutes,
                )
                for day in value.days
            ),
        )

        def original_template_source(
            owner: str, generation: str, version: StrategyTemplateExecutionVersion
        ) -> StrategyTemplateSourceData:
            if owner != "alice" or version != catalog.versions[0]:
                raise PermissionError(
                    "original synthetic template source has another owner or version"
                )
            return StrategyTemplateSourceData.model_validate(
                template_source.model_dump(mode="python")
                | {
                    "catalog": template_source.catalog.model_copy(
                        update={"generation_id": generation}
                    ),
                    "material_hash": None,
                }
            )

        template_preparer = StrategyTemplateRunPreparer(
            source_provider=original_template_source,
            metadata_store_factory=lambda: DuckDBStore(foundation.metadata_path),
            catalog=ResearchCatalog(foundation.catalog_path),
            lake_root=foundation.lake_root,
            input_root=private_directory(private_root / "template-inputs"),
            experiments=foundation.commands.experiment_registry,
            protocol=PortfolioExperimentProtocol(
                train_range=DateRange(start_date="2025-01-01", end_date="2025-01-31"),
                validation_range=DateRange(start_date="2025-02-01", end_date="2025-02-28"),
                frozen_outer_test_range=DateRange(start_date="2025-03-01", end_date="2025-03-31"),
            ),
            code_commit=store.producer_commit,
            clock=operation_clock,
        )
        template_backend = StrategyTemplateRunBackend(
            store,
            facade=foundation.commands,
            preparer=template_preparer,
            expected_identity=identity,
        )
        factor_root, factor_reference, factor_request = _configured(
            private_directory(private_root / "factor")
        )
        factor_backend = FactorRunPageControlBackend(
            factor_root, factor_reference, clock=operation_clock
        )
        consumer = PageControlConsumer(
            outbox=outbox,
            data_dir=private_root / "data",
            log_dir=private_root / "logs",
            clock=operation_clock,
            portfolio_backend=PortfolioCommandWriter(
                commands=foundation.commands, prepare=portfolio_preparer
            ),
            factor_run_backend=factor_backend,
            strategy_authoring_backend=StrategyAuthoringPageControlBackend(
                store, editor_users=("alice",), enabled=True, run_backend=template_backend
            ),
        )
        control = PageControlService(outbox=outbox, consumer=consumer, collaboration=authority)
        portfolio_request = SubmitPortfolioBacktest(
            command_id=str(uuid4()),
            requested_at=operation_clock(),
            actor_id="alice",
            config=config(),
        )
        control.submit_authorized(
            portfolio_request,
            authority.issue_authorization("alice", portfolio_request.model_dump(mode="json")),
        )
        factor_request = factor_request.model_copy(update={"requested_at": operation_clock()})
        FactorRunAdmission(control, run_users=frozenset({"alice"}), enabled=True).submit(
            factor_request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=factor_backend.configuration().registry_identity.instance_id,
        )
        head = catalog.versions[0].head
        strategy_request = RunStrategyTemplate(
            command_id=str(uuid4()),
            requested_at=operation_clock(),
            generation_id="generation-a",
            strategy_id=value.definition.logical_id,
            head=head,
            expected_head=head,
            start_date=value.request.days[0].trade_date,
            end_date=value.request.days[-1].trade_date,
            initial_cash=value.request.initial_cash,
        )
        StrategyAuthoringAdmission(
            control,
            source_catalog_provider=lambda owner, generation: source_catalog(
                owner=owner, generation=generation
            ),
        ).submit(
            strategy_request, authenticated_actor_id="alice", verified_metadata_identity=identity
        )
        receipts = [
            outbox.receipt(request.command_id)
            for request in (portfolio_request, factor_request, strategy_request)
        ]
        if any(
            receipt is None or receipt.status is not PageControlStatus.SUCCEEDED
            for receipt in receipts
        ):
            raise RuntimeError(
                "original synthetic audit submission did not reach its real queued receipt"
            )
        # These are actual accepted requests; no scheduler or worker executes them.
        print(
            json.dumps(
                {
                    "original_audit_submissions": [
                        receipt.model_dump(mode="json") for receipt in receipts
                    ]
                }
            ),
            flush=True,
        )
        # Repository proof paths can exceed macOS's Unix socket pathname limit.
        ipc_parent = (
            Path("/private/tmp") if sys.platform == "darwin" else Path(tempfile.gettempdir())
        )
        ipc_root = Path(tempfile.mkdtemp(prefix="rq-c15-", dir=ipc_parent.resolve()))
        socket_path = ipc_root / "ipc" / "collaboration.sock"
        # The accepted same-UID fixture uses the original explicit peer test slot.
        # It verifies real Unix framing and endpoint guards, not production UID separation.
        synthetic_web_uid = os.geteuid() + 10_000
        server = build_factor_definition_admission_server(
            FactorDefinitionAdmission(control, editor_users=frozenset(), save_enabled=False),
            socket_path=socket_path,
            trusted_web_uid=synthetic_web_uid,
            shared_gid=os.getegid(),
            peer_uid=lambda _connection: synthetic_web_uid,
        )
        assert server is not None
        gateway = CollaborationGateway(
            socket_path,
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: synthetic_web_uid,
        )
        settings = WebSettings(
            serving_root=args.root,
            bind=DEFAULT_BIND,
            ingress_socket_path=private_root / "web-private-fixture.sock",
            proxy_proof_file=args.proxy_proof_file,
            collaboration_mode="enforced",
        )
        app = create_app(settings, clock=running_clock(args.now), collaboration_gateway=gateway)
        if app.state.web.proxy_identity is None:
            raise ValueError("fixture proxy proof requires private, trusted parent directories")
        thread = threading.Thread(target=server.serve_forever, name="collaboration-fixture-owner")
        thread.start()

        uvicorn.run(
            app,
            host=host,
            port=int(port),
            workers=1,
            proxy_headers=False,
            server_header=False,
            access_log=False,
            log_level="warning",
        )
    finally:
        try:
            if server is not None:
                if thread is not None and thread.is_alive():
                    server.shutdown()
                    thread.join()
                server.server_close()
        finally:
            try:
                if ipc_root is not None:
                    shutil.rmtree(ipc_root)
            finally:
                try:
                    for directory, _, _ in os.walk(private_root, followlinks=False):
                        os.chmod(directory, 0o700)
                    shutil.rmtree(private_root)
                finally:
                    signal.signal(signal.SIGTERM, previous)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
