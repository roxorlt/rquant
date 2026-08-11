"""Closed application adapter from transport v1 to the existing SourceBroker API."""

from __future__ import annotations

from typing import cast

from pydantic import BaseModel, ValidationError

from rquant.source_broker import SourceBroker, SourceBrokerError
from rquant.source_broker_protocol import (
    SourceBrokerCallRequest,
    SourceBrokerCallResponse,
    SourceBrokerFinalizeRequest,
    SourceBrokerFinalizeResponse,
    SourceBrokerStartRequest,
    SourceBrokerStartResponse,
    SourceBrokerTransportError,
    SourceBrokerTransportRequest,
    SourceBrokerTransportResponse,
)


class SourceBrokerTransportAdapter:
    """Versioned finite dispatch; provider identity comes only from the signed plan."""

    def __init__(self, broker: SourceBroker) -> None:
        self._broker = broker

    def handle(
        self,
        request: SourceBrokerTransportRequest,
    ) -> SourceBrokerTransportResponse:
        if isinstance(request, SourceBrokerStartRequest):
            return SourceBrokerStartResponse.from_request(
                request=request,
                reservation=self._broker.start(request.plan),
            )
        if isinstance(request, SourceBrokerCallRequest):
            provider_request = self._daily_bars_provider_request(request)
            return SourceBrokerCallResponse.from_request(
                request=request,
                receipt=self._broker.call(
                    request.plan,
                    provider_request,
                    idempotency_key=request.idempotency_key,
                ),
            )
        if isinstance(request, SourceBrokerFinalizeRequest):
            return SourceBrokerFinalizeResponse.from_request(
                request=request,
                statement=self._broker.finalize(request.plan),
            )
        raise SourceBrokerTransportError("source broker transport operation is unsupported")

    def _daily_bars_provider_request(self, request: SourceBrokerCallRequest) -> BaseModel:
        plan = request.plan
        if plan.operation != "daily_bars":
            raise SourceBrokerTransportError(
                "transport call request does not match the signed plan operation"
            )
        try:
            registry = self._broker._provider_registry  # noqa: SLF001
            binding = registry.resolve(
                source=cast(str, plan.source),
                operation=plan.operation,
            )
            payload: dict[str, object] = {
                "trade_date": request.call_request.trade_date,
                "filters": (
                    None
                    if request.call_request.market is None
                    else {"market": request.call_request.market}
                ),
            }
            return binding.request_model.model_validate(payload)
        except (AttributeError, TypeError, ValidationError, SourceBrokerError) as exc:
            raise SourceBrokerTransportError(
                "daily bars transport request does not match the registered broker schema"
            ) from exc
