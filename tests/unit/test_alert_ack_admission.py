"""A new acknowledgment passes one current, complete Serving generation."""

from __future__ import annotations

import http.client
import json
import os
import socket
import threading
from datetime import datetime, timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rquant.alert_ack import alert_event_at, alert_window_start, stable_alert_id
from rquant.alert_ack_admission import (
    AckAdmission,
    AckAdmissionClient,
    AckAdmissionRejectedError,
    build_ack_admission_server,
)
from rquant.page_control import AckAlert, PageControlService, PageControlStatus
from rquant.page_control_service import build_page_control_service, handler_for
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_alert_projection import AlertAckAuthoritySnapshot
from rquant.serving_publisher import ServingReader
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.alert_ack_read import AlertReadModel, _Event
from rquant.web.alert_ack_read import read_alert_ack as read_web_alert_ack
from rquant.web.envelope import ServingMeta, ServingState
from rquant.web.models.alert_ack import UnacknowledgedSummary
from rquant.web.serving import BorrowedGeneration
from tests.support.web_serving_fixture import (
    FIXTURE_BUILT_AT,
    _generation_ids,
    _monitor_events,
    _signal_bundle,
    _timeline_surge_events,
    build_web_fixture,
)

NOW = FIXTURE_BUILT_AT + timedelta(seconds=30)
ACTIVATED = FIXTURE_BUILT_AT - timedelta(days=1)
SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"


def _projection(name: str, rows: list[dict[str, object]]) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=name, available_at=FIXTURE_BUILT_AT, rows=tuple(rows)
    )


def _alert_projections(*, drop_monitor: bool = False) -> tuple[ServingProjectionPayload, ...]:
    first = alert_window_start(count_as_of=FIXTURE_BUILT_AT, activated_at=ACTIVATED)
    sources = {
        "signal": [record.signal for record in _signal_bundle(FIXTURE_BUILT_AT)[0]],
        "monitor_event": _monitor_events(),
        "surge_event": _timeline_surge_events("baseline"),
    }
    source_generation = _generation_ids("baseline", 0)["signals"]
    events: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for source, facts in sources.items():
        rows = sorted(
            (
                {
                    "source": source,
                    "alert_id": stable_alert_id(source, fact),
                    "occurred_at": alert_event_at(source, fact).isoformat(),
                    "confirmation_id": None,
                    "confirmed_at": None,
                    "eligible": True,
                }
                for fact in facts
            ),
            key=lambda row: str(row["alert_id"]),
        )
        if drop_monitor and source == "monitor_event":
            rows = []
        events.extend(rows)
        coverage.append(
            {
                "source": source,
                "state": "complete",
                "reason": None,
                "window_start": first.isoformat(),
                "window_end": FIXTURE_BUILT_AT.isoformat(),
                "count_as_of": FIXTURE_BUILT_AT.isoformat(),
                "source_generation_id": source_generation,
                "high_watermark": "verified-through-session",
                "row_count": len(rows),
                "row_digest": canonical_sha256(
                    {"contract": "alert-observed-rows/v1", "source": source, "rows": tuple(rows)}
                ),
            }
        )
    snapshot = AlertAckAuthoritySnapshot.create(activated_at=ACTIVATED, rows=[])
    return (
        _projection(
            "alert_ack_state",
            [
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "activated_at": ACTIVATED.isoformat(),
                    "row_count": snapshot.row_count,
                    "rows_sha256": snapshot.rows_sha256,
                }
            ],
        ),
        _projection("alert_ack", []),
        _projection("alert_event", events),
        _projection("alert_source_coverage", coverage),
        _projection(
            "alert_overview",
            [
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "unacknowledged_count": len(events),
                    "count_as_of": FIXTURE_BUILT_AT.isoformat(),
                    "activated_at": ACTIVATED.isoformat(),
                }
            ],
        ),
    )


def _setup(
    tmp_path: Path,
    *,
    drop_monitor: bool = False,
    activation: datetime | None = ACTIVATED,
) -> tuple[Path, PageControlService, AckAlert]:
    root = tmp_path / "serving"
    manifest = build_web_fixture(
        root, "baseline", signal_projections=_alert_projections(drop_monitor=drop_monitor)
    )
    service = build_page_control_service(
        outbox_path=tmp_path / "control" / "page-control.sqlite3",
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(tmp_path / "exports",),
        load_default_lab_backend=False,
        clock=lambda: NOW,
    )
    if activation is not None:
        service.outbox.activate_alert_ack(activation)
    event_id = stable_alert_id("monitor_event", _monitor_events()[0])
    command = AckAlert(
        command_id="ack-first",
        requested_at=NOW,
        generation_id=manifest.generation_id,
        alert_id=event_id,
        actor_id="researcher",
    )
    return root, service, command


def test_admission_uses_verified_generation_and_durable_retry(tmp_path: Path) -> None:
    root, service, command = _setup(tmp_path)
    admission = AckAdmission(service, root, clock=lambda: NOW, stale_after=timedelta(minutes=10))

    receipt = admission.admit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert isinstance(receipt.result, dict)
    assert receipt.result["confirmation_id"] == command.command_id
    assert admission.admit(command) == receipt
    assert service.lookup_ack_command(command) == receipt
    with pytest.raises(ValueError, match="different payload"):
        admission.admit(command.model_copy(update={"actor_id": "another-user"}))
    with pytest.raises(ValueError, match="verified Serving eligibility"):
        service.submit(command.model_copy(update={"command_id": "legacy-tcp"}))


def test_web_and_admission_share_the_same_generation_reader(tmp_path: Path) -> None:
    root, _service, command = _setup(tmp_path)
    with ServingReader(root).acquire_generation() as lease:
        cursor = lease.connection.cursor()
        try:
            borrowed = BorrowedGeneration(lease.manifest, lease.pointer, cursor, None)
            meta = ServingMeta(
                generation_id=lease.manifest.generation_id,
                built_at=lease.manifest.built_at,
                age_seconds=30,
                state=ServingState.READY,
                message=None,
                detail="verified",
            )
            alerts = read_web_alert_ack(
                borrowed, meta=meta, now=NOW, stale_after=timedelta(minutes=10)
            )
        finally:
            cursor.close()
    assert alerts.summary.state == "ready"
    assert alerts.summary.count == 4
    assert alerts.is_eligible("monitor_event", command.alert_id)
    assert alerts.status_for("monitor_event", command.alert_id).eligible


def test_shared_reader_excludes_event_after_verified_cutoff() -> None:
    alert_id = "b" * 64
    event = _Event(
        source="monitor_event",
        alert_id=alert_id,
        occurred_at=NOW + timedelta(seconds=1),
        confirmation_id=None,
        confirmed_at=None,
        eligible=True,
    )
    model = AlertReadModel(
        summary=UnacknowledgedSummary(state="ready", count=0, count_as_of=NOW),
        activated_at=ACTIVATED,
        events={("monitor_event", alert_id): event},
        acknowledgments={},
    )
    assert not model.is_eligible("monitor_event", alert_id)
    assert not model.status_for("monitor_event", alert_id).eligible


def test_admission_rejects_forged_source_and_activation_mismatch(tmp_path: Path) -> None:
    root, service, command = _setup(tmp_path, drop_monitor=True)
    admission = AckAdmission(service, root, clock=lambda: NOW, stale_after=timedelta(minutes=10))
    with pytest.raises(ValueError, match="eligib|source|coverage"):
        admission.admit(command)
    assert service.lookup_ack_command(command) is None

    root2, service2, command2 = _setup(
        tmp_path / "other", activation=ACTIVATED + timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="activation"):
        AckAdmission(service2, root2, clock=lambda: NOW, stale_after=timedelta(minutes=10)).admit(
            command2
        )
    assert service2.lookup_ack_command(command2) is None

    root3, service3, command3 = _setup(tmp_path / "missing", activation=None)
    with pytest.raises(ValueError, match="activation"):
        AckAdmission(service3, root3, clock=lambda: NOW).admit(command3)
    assert service3.lookup_ack_command(command3) is None


def test_pointer_change_before_decision_rejects_but_after_decision_allows(tmp_path: Path) -> None:
    root, service, command = _setup(tmp_path)
    publisher = build_web_fixture

    def switch() -> None:
        publisher(root, "baseline", sequence=1, signal_projections=_alert_projections())

    admission = AckAdmission(
        service,
        root,
        clock=lambda: NOW,
        stale_after=timedelta(minutes=10),
        before_final_pointer_check=switch,
    )
    with pytest.raises(ValueError, match="generation"):
        admission.admit(command)
    assert service.lookup_ack_command(command) is None

    root2, service2, command2 = _setup(tmp_path / "after")
    admission2 = AckAdmission(
        service2,
        root2,
        clock=lambda: NOW,
        stale_after=timedelta(minutes=10),
        after_final_pointer_check=lambda: publisher(
            root2, "baseline", sequence=1, signal_projections=_alert_projections()
        ),
    )
    assert admission2.admit(command2).status is PageControlStatus.SUCCEEDED
    assert ServingReader(root2).current_pointer().generation_id != command2.generation_id
    assert admission2.admit(command2).status is PageControlStatus.SUCCEEDED


def test_crash_after_decision_before_enqueue_has_no_receipt_and_retry_rechecks(
    tmp_path: Path,
) -> None:
    root, service, command = _setup(tmp_path)

    def crash() -> None:
        raise RuntimeError("simulated crash before enqueue")

    admission = AckAdmission(
        service,
        root,
        clock=lambda: NOW,
        stale_after=timedelta(minutes=10),
        after_final_pointer_check=crash,
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        admission.admit(command)
    assert service.lookup_ack_command(command) is None
    build_web_fixture(root, "baseline", sequence=1, signal_projections=_alert_projections())
    with pytest.raises(ValueError, match="generation"):
        AckAdmission(service, root, clock=lambda: NOW).admit(command)
    assert service.lookup_ack_command(command) is None


def test_old_tcp_command_endpoint_refuses_new_ack(tmp_path: Path) -> None:
    _root, service, command = _setup(tmp_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(service))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        connection.request(
            "POST",
            "/v1/commands",
            body=json.dumps(command.model_dump(mode="json")),
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        assert response.status == 400
        response.read()
        connection.close()
        assert service.lookup_ack_command(command) is None
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path: Path) -> None:
        super().__init__("localhost", timeout=2)
        self.socket_path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(str(self.socket_path))


def test_socket_rejects_other_uid_before_parsing_and_uses_private_modes(tmp_path: Path) -> None:
    root, service, command = _setup(tmp_path)
    admission = AckAdmission(service, root, clock=lambda: NOW, stale_after=timedelta(minutes=10))
    assert build_ack_admission_server(admission, socket_path=None) is None
    observed: list[str] = []

    def peer_uid(_connection: socket.socket) -> int:
        observed.append("checked")
        return os.geteuid() + 1

    with TemporaryDirectory(prefix="rqa-", dir=SHORT_TMP) as directory:
        socket_path = Path(directory) / "ack.sock"
        server = build_ack_admission_server(
            admission, socket_path=socket_path, trusted_uid=os.geteuid(), peer_uid=peer_uid
        )
        assert server is not None
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            assert socket_path.parent.stat().st_mode & 0o777 == 0o700
            assert socket_path.stat().st_mode & 0o777 == 0o600
            client = _UnixHTTPConnection(socket_path)
            with pytest.raises((OSError, http.client.RemoteDisconnected)):
                client.request("POST", "/v1/ack-admission", body="not-json")
                client.getresponse()
            assert observed == ["checked"]
            assert service.lookup_ack_command(command) is None
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)
        assert not socket_path.exists()


def test_socket_refuses_broad_or_symlinked_directory(tmp_path: Path) -> None:
    root, service, _command = _setup(tmp_path)
    admission = AckAdmission(service, root, clock=lambda: NOW)
    with TemporaryDirectory(prefix="rqa-", dir=SHORT_TMP) as directory:
        private = Path(directory)
        os.chmod(private, 0o750)
        with pytest.raises(ValueError, match="mode 0700"):
            build_ack_admission_server(admission, socket_path=private / "ack.sock")
        assert not (private / "ack.sock").exists()
        os.chmod(private, 0o700)
        target = private / "target"
        target.mkdir(mode=0o700)
        alias = private / "alias"
        alias.symlink_to(target, target_is_directory=True)
        with pytest.raises(ValueError, match="mode 0700"):
            build_ack_admission_server(admission, socket_path=alias / "ack.sock")
        assert not (target / "ack.sock").exists()


def test_private_socket_client_returns_durable_receipt(tmp_path: Path) -> None:
    root, service, command = _setup(tmp_path)
    admission = AckAdmission(service, root, clock=lambda: NOW, stale_after=timedelta(minutes=10))
    with TemporaryDirectory(prefix="rqa-", dir=SHORT_TMP) as directory:
        socket_path = Path(directory) / "ack.sock"
        server = build_ack_admission_server(admission, socket_path=socket_path)
        assert server is not None
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = AckAdmissionClient(socket_path)
            receipt = client.submit(command)
            assert receipt.status is PageControlStatus.SUCCEEDED
            assert client.submit(command) == receipt
            assert service.lookup_ack_command(command) == receipt
            with pytest.raises(AckAdmissionRejectedError):
                client.submit(
                    command.model_copy(update={"command_id": "other", "alert_id": "f" * 64})
                )
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)
