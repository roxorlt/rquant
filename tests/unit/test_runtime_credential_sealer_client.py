from __future__ import annotations

import json
from subprocess import CompletedProcess

import pytest

from rquant.runtime_credential_sealer_client import (
    recover_runtime_credentials,
    seal_runtime_credentials,
)

TRANSACTION_ID = "c" * 64


def test_sends_credentials_only_over_stdin_to_fixed_root_owned_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials = {"svc-" + "a" * 64: b'{"token":"top-secret"}'}
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> CompletedProcess[bytes]:
        observed["command"] = command
        request = json.loads(kwargs["input"])
        observed.setdefault("requests", []).append(request)
        operation = request["operation"]
        return CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "schema_version": 2,
                    "operation": operation,
                    "transaction_id": TRANSACTION_ID,
                    "sealed_instances": sorted(credentials),
                }
            ).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        fake_run,
    )

    transaction = seal_runtime_credentials(credentials)
    transaction.commit()

    assert observed["command"] == [
        "/usr/bin/sudo",
        "-n",
        "/usr/local/libexec/rquant-runtime-credential-sealer",
    ]
    assert "top-secret" not in " ".join(observed["command"])
    requests = observed["requests"]
    assert requests[0]["schema_version"] == 2
    assert requests[0]["operation"] == "begin"
    assert set(requests[0]["credentials"]) == set(credentials)
    assert requests[1] == {
        "schema_version": 2,
        "operation": "commit",
        "transaction_id": TRANSACTION_ID,
    }


def test_transaction_can_rollback_through_the_fixed_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = "svc-" + "a" * 64
    requests: list[dict[str, object]] = []

    def fake_run(command: list[str], **kwargs: object) -> CompletedProcess[bytes]:
        request = json.loads(kwargs["input"])
        requests.append(request)
        return CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "schema_version": 2,
                    "operation": request["operation"],
                    "transaction_id": TRANSACTION_ID,
                    "sealed_instances": [instance],
                }
            ).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        fake_run,
    )

    transaction = seal_runtime_credentials({instance: b"payload"})
    transaction.rollback()

    assert [request["operation"] for request in requests] == ["begin", "rollback"]


def test_recovery_is_scoped_to_exact_generation_and_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instances = ("svc-" + "a" * 64, "svc-" + "b" * 64)
    generation = "d" * 64
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> CompletedProcess[bytes]:
        request = json.loads(kwargs["input"])
        observed.update(request)
        return CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "schema_version": 2,
                    "operation": "recover",
                    "bundle_generation": generation,
                    "sealed_instances": list(instances),
                    "outcome": "rolled_back",
                    "transaction_id": TRANSACTION_ID,
                }
            ).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        fake_run,
    )

    receipt = recover_runtime_credentials(
        bundle_generation=generation,
        instances=instances,
        action="rollback",
    )

    assert observed == {
        "schema_version": 2,
        "operation": "recover",
        "bundle_generation": generation,
        "instances": list(instances),
        "action": "rollback",
    }
    assert receipt.outcome == "rolled_back"
    assert receipt.transaction_id == TRANSACTION_ID


def test_recovery_rejects_receipt_for_another_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = "svc-" + "a" * 64
    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        lambda command, **_kwargs: CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "schema_version": 2,
                    "operation": "recover",
                    "bundle_generation": "e" * 64,
                    "sealed_instances": [instance],
                    "outcome": "committed",
                    "transaction_id": TRANSACTION_ID,
                }
            ).encode(),
            stderr=b"",
        ),
    )

    with pytest.raises(RuntimeError, match="generation|receipt"):
        recover_runtime_credentials(
            bundle_generation="d" * 64,
            instances=(instance,),
            action="commit",
        )


@pytest.mark.parametrize("mutation", ("failure", "wrong_receipt"))
def test_rejects_failed_or_incomplete_root_sealing(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    instance = "svc-" + "a" * 64
    if mutation == "failure":
        result = CompletedProcess([], 1, stdout=b"", stderr=b"denied")
        message = "failed"
    else:
        result = CompletedProcess(
            [],
            0,
            stdout=json.dumps(
                {
                    "schema_version": 2,
                    "operation": "begin",
                    "transaction_id": TRANSACTION_ID,
                    "sealed_instances": [],
                }
            ).encode(),
            stderr=b"",
        )
        message = "receipt"
    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        lambda *_args, **_kwargs: result,
    )

    with pytest.raises(RuntimeError, match=message):
        seal_runtime_credentials({instance: b"payload"})
