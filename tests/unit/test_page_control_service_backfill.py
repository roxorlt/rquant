"""The loopback service admits plan jobs only with an explicit trusted backend."""

from __future__ import annotations

import http.client
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path

from rquant.backfill_plan_page_backend import BackfillPlanPageBackend
from rquant.page_control import PageControlService, PageControlStatus
from rquant.page_control_service import build_page_control_service, handler_for
from tests.unit.test_backfill_plan_admission import NOW, _backend, _command
from tests.unit.test_backfill_plan_artifact import _snapshot


@contextmanager
def _http_service(service: PageControlService) -> Iterator[ThreadingHTTPServer]:
    with ThreadingHTTPServer(("127.0.0.1", 0), handler_for(service)) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join(timeout=3)
            assert not thread.is_alive()


def _post_command(server: ThreadingHTTPServer, command_id: str) -> dict[str, object]:
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
    try:
        connection.request(
            "POST",
            "/v1/commands",
            body=_command(command_id=command_id).model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        assert response.status == 200
        return json.loads(response.read())
    finally:
        connection.close()


def _build_service(
    tmp_path: Path, *, backfill_plan_backend: BackfillPlanPageBackend | None = None
) -> PageControlService:
    kwargs: dict[str, BackfillPlanPageBackend] = {}
    if backfill_plan_backend is not None:
        kwargs["backfill_plan_backend"] = backfill_plan_backend
    return build_page_control_service(
        outbox_path=tmp_path / "page-control.sqlite",
        data_dir=tmp_path / "page-data",
        log_dir=tmp_path / "page-logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        clock=lambda: NOW,
        **kwargs,
    )


def test_explicit_backend_queues_only_and_http_retry_keeps_task_id(tmp_path: Path) -> None:
    replica = _snapshot(tmp_path)
    backend = _backend(tmp_path, replica)
    command_id = "http-backfill-plan-0001"

    with _http_service(_build_service(tmp_path, backfill_plan_backend=backend)) as server:
        first = _post_command(server, command_id)
    with _http_service(_build_service(tmp_path, backfill_plan_backend=backend)) as server:
        replay = _post_command(server, command_id)

    assert first["status"] == PageControlStatus.SUCCEEDED.value
    assert replay == first
    result = first["result"]
    assert isinstance(result, dict)
    task_id = result["task_id"]
    assert isinstance(task_id, str)
    assert result == {"outcome": "task_queued", "task_id": task_id}
    admitted = backend.store.lookup_by_key(backend.idempotency_key(_command(command_id=command_id)))
    assert admitted is not None and admitted[1].task_id == task_id
    assert backend.store.status(task_id).status == "queued"
    assert backend.store.status(task_id).attempts == 0
    assert not list((tmp_path / "plans").glob("*.json"))


def test_default_service_rejects_plan_command_without_creating_task(tmp_path: Path) -> None:
    replica = _snapshot(tmp_path)
    backend = _backend(tmp_path, replica)
    command_id = "http-backfill-plan-disabled-0001"

    with _http_service(_build_service(tmp_path)) as server:
        receipt = _post_command(server, command_id)

    assert receipt["status"] == PageControlStatus.FAILED.value
    assert (
        backend.store.lookup_by_key(backend.idempotency_key(_command(command_id=command_id)))
        is None
    )
