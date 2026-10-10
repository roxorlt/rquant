"""Real private framing and cleanup preserve the independent run allowlist."""

import os
import tempfile
from pathlib import Path
from threading import Thread

import pytest

from rquant.factor.run_backend import FactorRunPageControlBackend
from rquant.factor_run_admission import (
    FactorRunAdmission,
    FactorRunAdmissionClient,
    FactorRunAdmissionRejectedError,
    FactorRunAdmissionUnavailableError,
    build_factor_run_admission_server,
)
from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
from tests.unit.test_factor_run_configuration import _configured
from tests.unit.test_factor_source_prepare import _AS_OF


def test_private_run_socket_allowlist_framing_and_cleanup(tmp_path: Path) -> None:
    root, reference, request = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path,
            log_dir=tmp_path,
            factor_run_backend=backend,
            clock=lambda: _AS_OF,
        ),
    )
    directory = Path(tempfile.mkdtemp(prefix="fre-", dir=tempfile.gettempdir()))
    os.chown(directory, os.geteuid(), os.getegid())
    directory.chmod(0o710)
    socket = directory / "run.sock"
    web_uid = os.geteuid() + 1
    server = build_factor_run_admission_server(
        FactorRunAdmission(service, run_users=frozenset({"alice"}), enabled=True),
        socket_path=socket,
        trusted_web_uid=web_uid,
        shared_gid=os.getegid(),
        peer_uid=lambda _: web_uid,
    )
    assert server is not None
    thread = Thread(target=server.serve_forever, daemon=True, name="factor-run-test-listener")
    thread.start()
    try:
        client = FactorRunAdmissionClient(
            socket,
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: web_uid,
        )
        with pytest.raises(FactorRunAdmissionRejectedError):
            client.lookup(request, authenticated_actor_id="editor-only")
        assert outbox.receipt(request.command_id) is None
        options = client.availability(authenticated_actor_id="alice")
        assert [item.label for item in options.pools] == [
            "全市场（剔除北交所、ST）",
            "沪深300",
            "中证1000",
            "创业板 + 科创板",
        ]
        assert [item.available for item in options.pools] == [True, False, False, False]
        result = client.submit(
            request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=backend.configuration().registry_identity.instance_id,
        )
        assert result.status == "submitted" and result.job_id is not None
        assert client.resume(request, authenticated_actor_id="alice") == result
        server.peer_uid = lambda _: -1
        with pytest.raises(FactorRunAdmissionUnavailableError):
            client.lookup(request, authenticated_actor_id="alice")
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()
        assert not thread.is_alive() and not socket.exists()
        directory.rmdir()
