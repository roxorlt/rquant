"""The Web editor sends only immutable commands to one fixed loopback authority."""

from __future__ import annotations

import http.client
import json
from collections.abc import Callable
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.web.settings import DEFAULT_PAGE_CONTROL_URL

_MAX_RECEIPT_BYTES = 16_384
PoolCommandTransport = Callable[[dict[str, object]], dict[str, object]]


class PoolCommandUnavailableError(RuntimeError):
    pass


class PoolCommandConflictError(RuntimeError):
    pass


class PoolCommandInvalidReceiptError(RuntimeError):
    pass


class PoolCommandWireReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_id: str
    status: Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
    enqueued_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None = None
    result: JsonValue | None = None
    error: str | None = None


class PoolCommandGateway:
    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_PAGE_CONTROL_URL,
        transport: PoolCommandTransport | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        if endpoint != DEFAULT_PAGE_CONTROL_URL:
            raise ValueError("pool editor requires the fixed loopback PageControl endpoint")
        self.transport = transport or self._post
        self.timeout_seconds = timeout_seconds

    def submit(self, payload: dict[str, object]) -> PoolCommandWireReceipt:
        try:
            response = self.transport(payload)
        except PoolCommandConflictError:
            raise
        except ValueError as error:
            raise PoolCommandConflictError(
                "command payload conflicts with existing command"
            ) from error
        except (OSError, TimeoutError, http.client.HTTPException) as error:
            raise PoolCommandUnavailableError("loopback PageControl is unavailable") from error
        try:
            receipt = PoolCommandWireReceipt.model_validate(response)
        except ValidationError as error:
            raise PoolCommandInvalidReceiptError("PageControl receipt is invalid") from error
        if receipt.command_id != payload.get("command_id"):
            raise PoolCommandInvalidReceiptError("PageControl command identity differs")
        return receipt

    def _post(self, payload: dict[str, object]) -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        connection = http.client.HTTPConnection("127.0.0.1", 8767, timeout=self.timeout_seconds)
        try:
            connection.request(
                "POST",
                "/v1/commands",
                body=body,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            response = connection.getresponse()
            if response.status == 400:
                raise PoolCommandConflictError("PageControl rejected the command")
            if response.status != 200:
                raise PoolCommandUnavailableError("PageControl did not return a successful receipt")
            if response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() != (
                "application/json"
            ):
                raise PoolCommandInvalidReceiptError("PageControl receipt is not JSON")
            raw = response.read(_MAX_RECEIPT_BYTES + 1)
            if len(raw) > _MAX_RECEIPT_BYTES:
                raise PoolCommandInvalidReceiptError("PageControl receipt exceeds the size limit")
            try:
                decoded = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise PoolCommandInvalidReceiptError("PageControl receipt is malformed") from error
            if not isinstance(decoded, dict):
                raise PoolCommandInvalidReceiptError("PageControl receipt is not an object")
            return decoded
        finally:
            connection.close()
