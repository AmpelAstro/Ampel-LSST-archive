import io
from collections.abc import Sequence
from typing import Annotated, cast

import plotly.express as px
from astropy.time import Time
from fastapi import (
    APIRouter,
    Depends,
    Query,
)
from fastapi.responses import ORJSONResponse
from sqlalchemy import text
from sqlalchemy.sql.elements import ColumnElement

from ..alert_packet import Alert as LSSTAlert
from ..avro import extract_record
from ..db import get_blobs_with_condition, get_schema
from ..models import Alert
from .alert import AlertFromId
from .cutouts import make_cutout_plotly
from .db import AsyncSession
from .iceberg import Connection
from .models import AlertDisplay, CutoutPlots
from .s3 import Bucket, get_range

router = APIRouter(tags=["display"])


def _get_cutout_plots(
    alert: AlertFromId,
    sigma: Annotated[None | float, Query(ge=0)] = None,
) -> CutoutPlots:
    return CutoutPlots(
        **{
            k: make_cutout_plotly(
                k, alert[f"cutout{k.capitalize()}"], sigma
            ).to_plotly_json()
            for k in ["template", "science", "difference"]
        }
    )


CutoutPlotsFromId = Annotated[CutoutPlots, Depends(_get_cutout_plots)]

router.get(
    "/alert/{diaSourceId}/cutouts",
    response_model=CutoutPlots,
)(_get_cutout_plots)


@router.get(
    "/alert/{diaSourceId}",
)
def display_alert(alert: AlertFromId, cutouts: CutoutPlotsFromId):
    return AlertDisplay(
        alert={k: v for k, v in alert.items() if not k.startswith("cutout")},
        cutouts=cutouts,
    )


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
    diaSourceId = await session.scalar(
        text("select id from alert order by random() limit 1")
    )

    return str(diaSourceId)


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


@router.get("/diaobject/{diaObjectId}/summaryplots")
async def get_photopoints_for_diaobject(
    diaObjectId: int, connection: Connection
) -> ORJSONResponse:
    # append diaSource to history, unnest, uniqify, and project
    # there doesn't seem to be a way to push projections down through any list operation
    df = connection.execute(
        """
        select
            distinct on (visit)
            diaSourceId,
            visit,
            detector,
            midpointMjdTai,
            ra,
            dec,
            raErr,
            decErr,
            psfFlux,
            psfFluxErr,
            band
        from
            (
                select
                    unnest(diaSources, recursive := true)
                from
                    (
                        select
                            list_append(prvDiaSources, diaSource) as diaSources
                        from
                            alerts
                        where
                            diaSource.diaObjectId = ?
                    ) a
            ) b
        order by
            visit;
        """,
        (diaObjectId,),
    ).df()

    # emit calendar dates for plotting purposes
    df["epoch"] = Time(df["midpointMjdTai"], format="mjd", scale="tai").to_datetime()

    # NB: it would be easiest to pass diaSourceId in hover_data, but plotly
    # converts the content to doubles, losing precision in the process. pass in
    # a separate stringified list to bypass.
    diaSourceId = df["diaSourceId"]
    ids_for_groups = {
        band: diaSourceId[idx].to_numpy().astype(str).tolist()
        for band, idx in df.groupby("band").groups.items()
    }
    category_orders = {"band": "ugrizy"}

    lightcurve_fig = px.scatter(
        df,
        x="epoch",
        y="psfFlux",
        error_y="psfFluxErr",
        color="band",
        category_orders=category_orders,
        template="simple_white",
        hover_data=[
            "visit",
            "detector",
            "ra",
            "raErr",
            "dec",
            "decErr",
        ],
    )
    centroid_fig = px.scatter(
        df,
        x="ra",
        y="dec",
        error_x="raErr",
        error_y="decErr",
        color="band",
        category_orders=category_orders,
        template="simple_white",
        hover_data=[
            "midpointMjdTai",
            "visit",
            "detector",
            "psfFlux",
            "psfFluxErr",
        ],
    )
    centroid_fig.update_layout(yaxis_scaleanchor="x")

    return ORJSONResponse(
        content={
            "lightcurve": lightcurve_fig.to_plotly_json(),
            "centroid": centroid_fig.to_plotly_json(),
            "_ids_for_groups": [
                ids_for_groups[band]
                for band in category_orders["band"]
                if band in ids_for_groups
            ],
        }
    )


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
