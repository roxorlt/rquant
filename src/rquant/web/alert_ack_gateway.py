"""Read-only exact command recovery from the fixed loopback PageControl authority."""

from __future__ import annotations

import http.client
import json
from collections.abc import Callable

from pydantic import ValidationError

from rquant.page_control import AckAlert, PageControlReceipt
from rquant.web.settings import DEFAULT_PAGE_CONTROL_URL

_MAX_LOOKUP_BYTES = 16_384
AckLookupTransport = Callable[[dict[str, object]], dict[str, object]]


class AckLookupUnavailableError(RuntimeError):
    pass


class AckLookupConflictError(RuntimeError):
    pass


class AckLookupInvalidResponseError(RuntimeError):
    pass


class AckLookupGateway:
    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_PAGE_CONTROL_URL,
        transport: AckLookupTransport | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        if endpoint != DEFAULT_PAGE_CONTROL_URL:
            raise ValueError("alert lookup requires fixed loopback PageControl")
        self.transport = transport or self._post_lookup
        self.timeout_seconds = timeout_seconds

    def lookup(self, command: AckAlert) -> PageControlReceipt | None:
        try:
            response = self.transport(command.model_dump(mode="json"))
        except AckLookupConflictError:
            raise
        except ValueError as error:
            raise AckLookupConflictError("ack command conflicts with existing command") from error
        except (OSError, TimeoutError, http.client.HTTPException) as error:
            raise AckLookupUnavailableError("PageControl lookup unavailable") from error
        if response == {"found": False}:
            return None
        if not isinstance(response, dict) or set(response) != {"found", "receipt"}:
            raise AckLookupInvalidResponseError("PageControl lookup shape is invalid")
        if response["found"] is not True:
            raise AckLookupInvalidResponseError("PageControl lookup found state is invalid")
        try:
            receipt = PageControlReceipt.model_validate(response["receipt"])
        except ValidationError as error:
            raise AckLookupInvalidResponseError("PageControl lookup receipt is invalid") from error
        if receipt.command_id != command.command_id:
            raise AckLookupInvalidResponseError("PageControl command identity differs")
        return receipt

    def _post_lookup(self, payload: dict[str, object]) -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        connection = http.client.HTTPConnection("127.0.0.1", 8767, timeout=self.timeout_seconds)
        try:
            connection.request(
                "POST",
                "/v1/commands/lookup",
                body=body,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            response = connection.getresponse()
            if response.status == 409:
                raise AckLookupConflictError("PageControl command conflict")
            if response.status != 200:
                raise AckLookupUnavailableError("PageControl lookup did not return success")
            if response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() != (
                "application/json"
            ):
                raise AckLookupInvalidResponseError("PageControl lookup is not JSON")
            raw = response.read(_MAX_LOOKUP_BYTES + 1)
            if len(raw) > _MAX_LOOKUP_BYTES:
                raise AckLookupInvalidResponseError("PageControl lookup exceeds size limit")
            try:
                decoded = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AckLookupInvalidResponseError("PageControl lookup JSON is invalid") from error
            if not isinstance(decoded, dict):
                raise AckLookupInvalidResponseError("PageControl lookup is not an object")
            return decoded
        finally:
            connection.close()
