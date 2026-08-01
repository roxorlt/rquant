from __future__ import annotations

import json
from subprocess import CompletedProcess

import pytest

from rquant.runtime_credential_sealer_client import seal_runtime_credentials


def test_sends_credentials_only_over_stdin_to_fixed_root_owned_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials = {"svc-" + "a" * 64: b'{"token":"top-secret"}'}
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> CompletedProcess[bytes]:
        observed["command"] = command
        observed["input"] = kwargs["input"]
        return CompletedProcess(
            command,
            0,
            stdout=json.dumps({"sealed_instances": sorted(credentials)}).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        fake_run,
    )

    seal_runtime_credentials(credentials)

    assert observed["command"] == [
        "/usr/bin/sudo",
        "-n",
        "/usr/local/libexec/rquant-runtime-credential-sealer",
    ]
    assert "top-secret" not in " ".join(observed["command"])
    request = json.loads(observed["input"])
    assert request["schema_version"] == 1
    assert set(request["credentials"]) == set(credentials)


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
            stdout=b'{"sealed_instances":[]}',
            stderr=b"",
        )
        message = "receipt"
    monkeypatch.setattr(
        "rquant.runtime_credential_sealer_client.subprocess.run",
        lambda *_args, **_kwargs: result,
    )

    with pytest.raises(RuntimeError, match=message):
        seal_runtime_credentials({instance: b"payload"})
