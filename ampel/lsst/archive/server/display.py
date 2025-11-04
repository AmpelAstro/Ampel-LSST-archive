import io
from collections.abc import Sequence
from typing import Annotated, cast

from fastapi import (
    APIRouter,
    Query,
    status,
)
from fastapi.responses import RedirectResponse
from sqlalchemy import text
from sqlalchemy.sql.elements import ColumnElement

from ..alert_packet import Alert as LSSTAlert
from ..avro import extract_record
from ..db import get_blobs_with_condition, get_schema
from ..models import Alert
from .alert import AlertFromId
from .cutouts import make_cutout_plotly
from .db import AsyncSession
from .models import CutoutPlots
from .s3 import Bucket, get_range
from .settings import settings

router = APIRouter(tags=["display"])


@router.get(
    "/alert/{diaSourceId}/cutouts",
    response_model=CutoutPlots,
)
def display_cutouts(
    alert: AlertFromId, sigma: Annotated[None | float, Query(ge=0)] = None
):
    return {
        k: make_cutout_plotly(
            k, alert[f"cutout{k.capitalize()}"], sigma
        ).to_plotly_json()
        for k in ["template", "science", "difference"]
    }


@router.get("/roulette")
async def rien_de_la_plus(
    session: AsyncSession,
):
    """
    Redirect to a (somewhat) random alert cutout page, just for fun.
    """
    diaSourceId = await session.scalar(
        text("select id from alert TABLESAMPLE system_rows(1)")
    )
    return RedirectResponse(
        url=f"{settings.root_path}/display/alert/{diaSourceId}/cutouts?sigma=3",
        status_code=status.HTTP_303_SEE_OTHER,
    )


async def get_alerts_with_condition(
    session: AsyncSession,
    bucket: Bucket,
    conditions: "Sequence[ColumnElement[bool] | bool]",
):
    async for uri, start, end, schema_id in get_blobs_with_condition(
        session, conditions
    ):
        schema = await get_schema(session, schema_id)
        try:
            body = await get_range(bucket, uri, start, end)
            record = extract_record(io.BytesIO(await body.read()), schema)
            yield cast(LSSTAlert, record)
        except KeyError:
            continue


@router.get("/diaobject/{diaObjectId}")
async def get_alerts_for_diaobject(
    diaObjectId: int,
    session: AsyncSession,
    bucket: Bucket,
) -> list[LSSTAlert]:
    return [
        alert
        async for alert in get_alerts_with_condition(
            session, bucket, [Alert.diaobject_id == diaObjectId]
        )
    ]


@router.get("/ssobject/{ssObjectId}")
async def get_alerts_for_ssobject(
    ssObjectId: int,
    session: AsyncSession,
    bucket: Bucket,
) -> list[LSSTAlert]:
    return [
        alert
        async for alert in get_alerts_with_condition(
            session, bucket, [Alert.ssobject_id == ssObjectId]
        )
    ]
