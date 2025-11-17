import io
import itertools
from collections.abc import Generator, Sequence
from typing import Annotated, TypedDict, cast

import plotly.express as px
from astropy.table import Table
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


class Photopoint(TypedDict):
    id: int
    visit: int
    detector: int
    midpointMjdTai: float
    ra: float
    raErr: float | None
    dec: float
    decErr: float | None
    psfFlux: float | None
    psfFluxErr: float | None
    band: str | None


def _get_photopoints(alert: LSSTAlert) -> Generator[Photopoint, None, None]:
    for diaSource in itertools.chain(
        [alert["diaSource"]], alert.get("prvDiaSources") or []
    ):
        yield {
            "id": diaSource["diaSourceId"],
            "visit": diaSource["visit"],
            "detector": diaSource["detector"],
            "midpointMjdTai": diaSource["midpointMjdTai"],
            "ra": diaSource["ra"],
            "raErr": diaSource["raErr"],
            "dec": diaSource["dec"],
            "decErr": diaSource["decErr"],
            "psfFlux": diaSource["psfFlux"],
            "psfFluxErr": diaSource["psfFluxErr"],
            "band": diaSource["band"],
        }


@router.get("/diaobject/{diaObjectId}/summaryplots")
async def get_photopoints_for_diaobject(
    diaObjectId: int,
    session: AsyncSession,
    bucket: Bucket,
) -> ORJSONResponse:
    # deduplicate by visit, sort by time
    # FIXME: probably want to keep a thin column store of photopoints for performance
    # ~3 s for 21 alerts from localstack s3 is pretty slow

    alerts = [
        alert
        async for alert in get_alerts_with_condition(
            session, bucket, [Alert.diaobject_id == diaObjectId]
        )
    ]

    pps = Table(
        sorted(
            {
                pp["visit"]: pp for alert in alerts for pp in _get_photopoints(alert)
            }.values(),
            key=lambda pp: pp["midpointMjdTai"],
        )
    )
    pps["epoch"] = Time(pps["midpointMjdTai"], format="mjd", scale="tai").to_datetime()

    df = pps.to_pandas()
    # NB: it would be easiest to pass diaSourceId in hover_data, but plotly
    # converts the content to doubles, losing precision in the process. pass in
    # a separate stringified list to bypass.
    diaSourceId = df["id"]
    ids_for_groups = [
        diaSourceId[idx].to_numpy().astype(str).tolist()
        for idx in df.groupby("band").groups.values()
    ]

    lightcurve_fig = px.scatter(
        df,
        x="epoch",
        y="psfFlux",
        error_y="psfFluxErr",
        color="band",
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
            "_ids_for_groups": ids_for_groups,
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
