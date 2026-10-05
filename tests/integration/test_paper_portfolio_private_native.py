"""Root-only local UDS transport proof; test peer UID injection is explicit."""

import gc
import hashlib
import os
from pathlib import Path
import stat
import tempfile
import threading
from uuid import uuid4

import pytest

from rquant.page_control import PageControlStatus
from rquant.paper_operator_commands import SetPaperAccountPaused
from rquant.paper_portfolio_admission import (
    PaperPortfolioAdmissionClient, PaperPortfolioAdmissionRejectedError,
    build_paper_portfolio_admission_server,
)
from tests.unit.test_paper_portfolio_web import _WRITERS, setup
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def test_original_private_transport_owner_confirmation_recovery_and_cleanup(tmp_path: Path) -> None:
    server = None
    worker = None
    try:
        ctx = setup(tmp_path)
        gc.collect()
        broker = ctx.backend.research_backend.preparer.source_for(ctx.request.account_id, "alice").broker
        ledger = ctx.runtime.ledger_source_for(broker)
        paths = (ledger.path, ledger.path.with_name(ledger.path.name + "-wal"), ledger.anchor_path)
        before = tuple(hashlib.sha256(path.read_bytes()).hexdigest() if path and path.exists() else None for path in paths)
        # macOS cannot create a distinct OS identity here. Transport and filesystem
        # checks are real; only the trusted peer identity is supplied by the fixture.
        web_uid = os.geteuid() + 1
        with tempfile.TemporaryDirectory(prefix="pp-", dir="/private/tmp") as directory:
            socket_root = Path(directory)
            os.chown(socket_root, os.geteuid(), os.getegid(), follow_symlinks=False)
            socket_root.chmod(0o710)
            parent = socket_root.lstat()
            assert (parent.st_uid, parent.st_gid, stat.S_IMODE(parent.st_mode)) == (os.geteuid(), os.getegid(), 0o710)
            socket_path = socket_root / "paper.sock"
            server = build_paper_portfolio_admission_server(ctx.admission, socket_path=socket_path,
                trusted_web_uid=web_uid, shared_gid=os.getegid(), peer_uid=lambda _connection: web_uid)
            assert server is not None
            assert stat.S_IMODE(socket_path.stat().st_mode) == 0o660
            worker = threading.Thread(target=server.serve_forever, name="paper-private-test", daemon=False)
            worker.start()
            client = PaperPortfolioAdmissionClient(socket_path, expected_service_uid=os.geteuid(),
                shared_gid=os.getegid(), client_uid=lambda: web_uid)
            assert client.run_available(authenticated_actor_id="alice")
            with pytest.raises(PaperPortfolioAdmissionRejectedError):
                client.run_available(authenticated_actor_id="bob")
            current = ctx.runtime.operator.current()
            command = SetPaperAccountPaused(command_id=str(uuid4()), requested_at=EXECUTION_TIME,
                generation_id=ctx.manifest.generation_id, account_id=ctx.request.account_id,
                configuration_fingerprint=ctx.request.configuration_fingerprint,
                expected_sequence=current.sequence, expected_paused=current.paused, paused=True)
            assert client.lookup(command, authenticated_actor_id="alice") is None
            with pytest.raises(PaperPortfolioAdmissionRejectedError):
                client.submit(command, authenticated_actor_id="alice", verified_metadata_identity=ctx.runtime.state.identity())
            assert ctx.page.outbox.receipt(command.command_id) is None
            preview = client.prepare(command, authenticated_actor_id="alice", verified_metadata_identity=ctx.runtime.state.identity())
            assert preview.request == command and ctx.page.outbox.receipt(command.command_id) is None
            result = client.submit(command, authenticated_actor_id="alice", verified_metadata_identity=ctx.runtime.state.identity(), confirmation_id=preview.confirmation_id)
            assert result.receipt.status is PageControlStatus.SUCCEEDED and ctx.runtime.operator.current().status == "waiting"
            assert client.lookup(command, authenticated_actor_id="alice") == result
            assert client.resume(command, authenticated_actor_id="alice") == result
            with pytest.raises(PaperPortfolioAdmissionRejectedError):
                client.lookup(command, authenticated_actor_id="bob")
            with pytest.raises(PaperPortfolioAdmissionRejectedError):
                client.lookup(command.model_copy(update={"paused": False}), authenticated_actor_id="alice")
            with pytest.raises(PaperPortfolioAdmissionRejectedError):
                client._call("lookup", {"authenticated_actor_id": "alice", "command": command.model_dump(mode="json"), "path": "/untrusted/ledger"})
            after = tuple(hashlib.sha256(path.read_bytes()).hexdigest() if path and path.exists() else None for path in paths)
            assert before == after
            server.shutdown(); worker.join(timeout=3); server.server_close()
            assert not worker.is_alive() and not socket_path.exists()
            server, worker = None, None
        assert not socket_root.exists()
        print("REAL_AF_UNIX_AND_0660_PERMISSION=True; ORIGINAL_PRIVATE_TYPED_TRANSPORT=True; ORIGINAL_PAUSE_JOURNAL_AND_RECOVERY=True; OWNER_REJECT=True; LEDGER_DB_WAL_ANCHOR_UNCHANGED=True; THREAD_JOIN_AND_SOCKET_REMOVED=True; PEER_UID_INJECTED=True; DISTINCT_OS_UID_PROVEN=False")
    finally:
        if server is not None:
            server.shutdown()
            if worker is not None:
                worker.join(timeout=3)
                assert not worker.is_alive()
            server.server_close()
        while _WRITERS:
            _WRITERS.pop().close()
