"""One borrowed Serving generation for the complete read-only strategy catalog."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.models.strategies import (
    StrategyCatalogData,
    StrategyCatalogItem,
    StrategyParameter,
)
from rquant.web.security import current_user
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/strategies")
_IDS = frozenset({"auction_gap", "growth_board_surge", "n_shape"})


@router.get("", response_model=Envelope[StrategyCatalogData], summary="已核验策略定义")
def list_strategies(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Envelope[StrategyCatalogData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看策略。")
        states = readers.table_states(borrowed.cursor) if borrowed is not None else {}
        catalog = states.get("strategy_catalog")
        parameters = states.get("strategy_catalog_parameter")
        if meta.state == "unavailable" or (
            (catalog is None or not catalog.available)
            and (parameters is None or not parameters.available)
        ):
            data = StrategyCatalogData(available=False, strategies=[])
        elif (
            catalog is None
            or parameters is None
            or not catalog.available
            or not parameters.available
            or catalog.row_count != 3
            or parameters.row_count > 32
        ):
            raise HTTPException(status_code=503, detail="策略目录暂时无法核验，请稍后重试。")
        else:
            rows = borrowed.cursor.execute(
                "SELECT strategy_id, name, version, registered_at "
                "FROM strategy_catalog ORDER BY strategy_id LIMIT 4"
            ).fetchall()
            parameter_rows = borrowed.cursor.execute(
                "SELECT strategy_id, parameter_key, label, display_value "
                "FROM strategy_catalog_parameter ORDER BY strategy_id, parameter_key LIMIT 33"
            ).fetchall()
            if (
                len(rows) != 3
                or {str(row[0]) for row in rows} != _IDS
                or len(parameter_rows) != parameters.row_count
                or len(parameter_rows) == 0
            ):
                raise HTTPException(status_code=503, detail="策略目录暂时无法核验，请稍后重试。")
            by_id: dict[str, list[StrategyParameter]] = {strategy_id: [] for strategy_id in _IDS}
            for strategy_id, key, label, value in parameter_rows:
                if strategy_id not in by_id:
                    raise HTTPException(
                        status_code=503, detail="策略目录暂时无法核验，请稍后重试。"
                    )
                by_id[strategy_id].append(
                    StrategyParameter(key=key, label=label, display_value=value)
                )
            if any(not values for values in by_id.values()):
                raise HTTPException(status_code=503, detail="策略目录暂时无法核验，请稍后重试。")
            data = StrategyCatalogData(
                available=True,
                strategies=[
                    StrategyCatalogItem(
                        strategy_id=strategy_id,
                        name=name,
                        version=version,
                        registered_at=registered_at,
                        parameters=by_id[strategy_id],
                    )
                    for strategy_id, name, version, registered_at in rows
                ],
            )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[StrategyCatalogData](data=data, serving=meta)
