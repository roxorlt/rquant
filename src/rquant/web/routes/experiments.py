"""Private experiment history from one borrowed immutable Serving generation."""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads
from rquant.web import readers
from rquant.web.envelope import Envelope, ServingState
from rquant.web.models.experiments import ExperimentItem, ExperimentListData
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/experiments")
_UNREADABLE = "实验记录暂时无法读取，请稍后重试。"
_CHANGED = "数据已更新，请重新查看实验记录。"
_MAX_CURSOR_CHARS = 512


class _Cursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["experiment_attempt_v1"] = "experiment_attempt_v1"
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    registered_at: datetime
    experiment_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_size: int = Field(ge=1, le=50)


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _encode(cursor: _Cursor, key: bytes) -> str:
    payload = canonical_json_bytes(cursor.model_dump(mode="json"))
    return f"{_segment(payload)}.{_segment(hmac.new(key, payload, hashlib.sha256).digest())}"


def _decode(token: str, key: bytes) -> _Cursor:
    try:
        payload_text, signature_text = token.split(".")
        payload = b64decode(
            payload_text + "=" * (-len(payload_text) % 4), altchars=b"-_", validate=True
        )
        signature = b64decode(
            signature_text + "=" * (-len(signature_text) % 4), altchars=b"-_", validate=True
        )
        if _segment(payload) != payload_text or _segment(signature) != signature_text:
            raise ValueError("cursor encoding is not canonical")
        if not hmac.compare_digest(hmac.new(key, payload, hashlib.sha256).digest(), signature):
            raise ValueError("cursor signature differs")
        decoded = _Cursor.model_validate(strict_canonical_json_loads(payload))
        if canonical_json_bytes(decoded.model_dump(mode="json")) != payload:
            raise ValueError("cursor payload is not canonical")
        return decoded
    except (Base64Error, UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=422, detail="分页已失效，请从首批重新查看。") from error


@router.get("", response_model=Envelope[ExperimentListData], summary="实验记录")
def list_experiments(
    request: Request,
    response: Response,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
    cursor: Annotated[str | None, Query(min_length=1, max_length=_MAX_CURSOR_CHARS)] = None,
) -> Envelope[ExperimentListData]:
    if cursor is not None and generation_id is None:
        raise HTTPException(status_code=422, detail="请从首批重新查看实验记录。")
    web = request.app.state.web
    boundary = _decode(cursor, web.cursor_key) if cursor is not None else None
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail=_CHANGED)
        if boundary is not None and (
            boundary.generation_id != meta.generation_id or boundary.page_size != limit
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        if borrowed is None or meta.state == ServingState.UNAVAILABLE:
            return Envelope[ExperimentListData](
                data=ExperimentListData(
                    available=False,
                    items=[],
                    retained_count=0,
                    truncated=False,
                    oldest_registered_at=None,
                    next_cursor=None,
                ),
                serving=meta,
            )
        try:
            states = readers.table_states(borrowed.cursor)
            attempts = states.get("experiment_attempt")
            window = states.get("experiment_attempt_window")
            available = (
                attempts is not None
                and window is not None
                and attempts.available
                and window.available
            )
            if (attempts is not None and attempts.available) != (
                window is not None and window.available
            ):
                raise ValueError("partial experiment projection")
            if not available:
                data = ExperimentListData(
                    available=False,
                    items=[],
                    retained_count=0,
                    truncated=False,
                    oldest_registered_at=None,
                    next_cursor=None,
                )
            else:
                assert attempts is not None and window is not None
                if window.row_count != 1 or not 0 <= attempts.row_count <= 500:
                    raise ValueError("invalid experiment window count")
                window_row = borrowed.cursor.execute(
                    "SELECT retained_count, truncated, oldest_registered_at "
                    "FROM experiment_attempt_window WHERE snapshot_key = 'current'"
                ).fetchone()
                if window_row is None:
                    raise ValueError("experiment window is missing")
                retained_count, truncated, oldest = window_row
                if (
                    type(retained_count) is not int
                    or retained_count != attempts.row_count
                    or type(truncated) is not bool
                    or (retained_count == 0) != (oldest is None)
                    or (truncated and retained_count != 500)
                ):
                    raise ValueError("experiment window conflicts with rows")
                oldest_row = borrowed.cursor.execute(
                    "SELECT registered_at FROM experiment_attempt "
                    "ORDER BY registered_at ASC, experiment_id ASC LIMIT 1"
                ).fetchone()
                if (None if oldest_row is None else oldest_row[0]) != oldest:
                    raise ValueError("experiment window coverage conflicts with rows")
                if boundary is None:
                    rows = borrowed.cursor.execute(
                        "SELECT experiment_id, hypothesis_family, registered_at, status, "
                        "completed_at, trade_count, net_return_pct, max_drawdown_pct, win_rate_pct "
                        "FROM experiment_attempt "
                        "ORDER BY registered_at DESC, experiment_id DESC LIMIT ?",
                        (limit + 1,),
                    ).fetchall()
                else:
                    rows = borrowed.cursor.execute(
                        "SELECT experiment_id, hypothesis_family, registered_at, status, "
                        "completed_at, trade_count, net_return_pct, max_drawdown_pct, win_rate_pct "
                        "FROM experiment_attempt "
                        "WHERE registered_at < ? OR "
                        "(registered_at = ? AND experiment_id < ?) "
                        "ORDER BY registered_at DESC, experiment_id DESC LIMIT ?",
                        (
                            boundary.registered_at,
                            boundary.registered_at,
                            boundary.experiment_id,
                            limit + 1,
                        ),
                    ).fetchall()
                items = [
                    ExperimentItem(
                        experiment_id=row[0],
                        hypothesis_family=row[1],
                        registered_at=row[2],
                        status=row[3],
                        completed_at=row[4],
                        trade_count=row[5],
                        net_return_pct=row[6],
                        max_drawdown_pct=row[7],
                        win_rate_pct=row[8],
                    )
                    for row in rows[:limit]
                ]
                next_cursor = None
                if len(rows) > limit:
                    last = items[-1]
                    next_cursor = _encode(
                        _Cursor(
                            generation_id=meta.generation_id,
                            registered_at=last.registered_at,
                            experiment_id=last.experiment_id,
                            page_size=limit,
                        ),
                        web.cursor_key,
                    )
                data = ExperimentListData(
                    available=True,
                    items=items,
                    retained_count=retained_count,
                    truncated=truncated,
                    oldest_registered_at=oldest,
                    next_cursor=next_cursor,
                )
        except HTTPException:
            raise
        except Exception as error:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from error
    return Envelope[ExperimentListData](data=data, serving=meta)
