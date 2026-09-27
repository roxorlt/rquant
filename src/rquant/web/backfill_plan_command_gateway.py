"""Submit a bounded plan command to the fixed loopback PageControl authority."""

from __future__ import annotations

import http.client
import json
from collections.abc import Callable
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.web.settings import DEFAULT_PAGE_CONTROL_URL

_MAX_RECEIPT_BYTES = 16_384
BackfillPlanCommandTransport = Callable[[dict[str, object]], dict[str, object]]


class BackfillPlanCommandUnavailableError(RuntimeError):
    pass


class BackfillPlanCommandConflictError(RuntimeError):
    pass


class BackfillPlanCommandInvalidReceiptError(RuntimeError):
    pass


class BackfillPlanWireReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str
    status: Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
    enqueued_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None = None
    result: JsonValue | None = None
    error: str | None = None


class BackfillPlanCommandGateway:
    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_PAGE_CONTROL_URL,
        transport: BackfillPlanCommandTransport | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        if endpoint != DEFAULT_PAGE_CONTROL_URL:
            raise ValueError("backfill plan requires fixed loopback PageControl")
        self.transport = transport or self._post
        self.timeout_seconds = timeout_seconds

    def submit(self, payload: dict[str, object]) -> BackfillPlanWireReceipt:
        try:
            response = self.transport(payload)
        except BackfillPlanCommandConflictError:
            raise
        except ValueError as error:
            raise BackfillPlanCommandUnavailableError(
                "PageControl returned an unclassified failure"
            ) from error
        except (OSError, TimeoutError, http.client.HTTPException) as error:
            raise BackfillPlanCommandUnavailableError("PageControl is unavailable") from error
        try:
            receipt = BackfillPlanWireReceipt.model_validate(response)
        except (TypeError, ValidationError) as error:
            raise BackfillPlanCommandInvalidReceiptError(
                "PageControl receipt is invalid"
            ) from error
        if receipt.command_id != payload.get("command_id"):
            raise BackfillPlanCommandInvalidReceiptError("PageControl command identity differs")
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
            if response.status != 200:
                # PageControl currently returns 400 for both conflicts and runtime
                # errors; no HTTP error status here proves whether the effect ran.
                raise BackfillPlanCommandUnavailableError("PageControl did not return success")
            if response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() != (
                "application/json"
            ):
                raise BackfillPlanCommandInvalidReceiptError("PageControl receipt is not JSON")
            raw = response.read(_MAX_RECEIPT_BYTES + 1)
            if len(raw) > _MAX_RECEIPT_BYTES:
                raise BackfillPlanCommandInvalidReceiptError(
                    "PageControl receipt exceeds size limit"
                )
            try:
                decoded = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise BackfillPlanCommandInvalidReceiptError(
                    "PageControl receipt is malformed"
                ) from error
            if not isinstance(decoded, dict):
                raise BackfillPlanCommandInvalidReceiptError("PageControl receipt is not an object")
            return decoded
        finally:
            connection.close()
