"""Submit a bounded audit report command to the fixed loopback PageControl authority."""

from __future__ import annotations

import http.client
import json
from collections.abc import Callable
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.web.settings import DEFAULT_PAGE_CONTROL_URL

_MAX_RECEIPT_BYTES = 16_384
AuditReportCommandTransport = Callable[[dict[str, object]], dict[str, object]]


class AuditReportCommandUnavailableError(RuntimeError):
    pass


class AuditReportCommandConflictError(RuntimeError):
    pass


class AuditReportCommandInvalidReceiptError(RuntimeError):
    pass


class AuditReportWireReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str
    status: Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
    enqueued_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None = None
    result: JsonValue | None = None
    error: str | None = None


class AuditReportCommandGateway:
    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_PAGE_CONTROL_URL,
        transport: AuditReportCommandTransport | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        if endpoint != DEFAULT_PAGE_CONTROL_URL:
            raise ValueError("audit report requires fixed loopback PageControl")
        self.transport = transport or self._post
        self.timeout_seconds = timeout_seconds

    def submit(self, payload: dict[str, object]) -> AuditReportWireReceipt:
        try:
            response = self.transport(payload)
        except (
            AuditReportCommandConflictError,
            AuditReportCommandInvalidReceiptError,
            AuditReportCommandUnavailableError,
        ):
            raise
        except Exception as error:
            raise AuditReportCommandUnavailableError("PageControl result is unknown") from error
        try:
            receipt = AuditReportWireReceipt.model_validate(response)
        except (TypeError, ValidationError) as error:
            raise AuditReportCommandInvalidReceiptError("PageControl receipt is invalid") from error
        if receipt.command_id != payload.get("command_id"):
            raise AuditReportCommandInvalidReceiptError("PageControl command identity differs")
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
                # PageControl can use the same status for conflicts and failed effects.
                raise AuditReportCommandUnavailableError("PageControl result is unknown")
            if response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() != (
                "application/json"
            ):
                raise AuditReportCommandInvalidReceiptError("PageControl receipt is not JSON")
            raw = response.read(_MAX_RECEIPT_BYTES + 1)
            if len(raw) > _MAX_RECEIPT_BYTES:
                raise AuditReportCommandInvalidReceiptError(
                    "PageControl receipt exceeds size limit"
                )
            try:
                decoded = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AuditReportCommandInvalidReceiptError(
                    "PageControl receipt is malformed"
                ) from error
            if not isinstance(decoded, dict):
                raise AuditReportCommandInvalidReceiptError("PageControl receipt is not an object")
            return decoded
        finally:
            connection.close()
