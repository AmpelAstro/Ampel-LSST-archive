from typing import Annotated

import numpy as np
import plotly.express as px
from astropy.time import Time
from duckdb import ColumnExpression, SQLExpression
from fastapi import (
    APIRouter,
    Depends,
    Query,
)
from fastapi.responses import ORJSONResponse
from hishel.fastapi import cache

from .alert import AlertFromId
from .cutouts import make_cutout_plotly
from .iceberg import AlertQuery, AlertRelation, Connection, flatten
from .metrics import REQ_TIME
from .models import AlertDisplay, CutoutPlots
from .settings import settings

router = APIRouter(tags=["display"])


def _get_cutout_plots(
    alert: AlertFromId,
    sigma: Annotated[None | float, Query(ge=0)] = None,
) -> CutoutPlots:
    return CutoutPlots(
        **{
            k: make_cutout_plotly(
                f"{k} {alert['diaSource']['band']}",
                alert[f"cutout{k.capitalize()}"],
                sigma,
            ).to_plotly_json()
            for k in ["template", "science", "difference"]
        }
    )


CutoutPlotsFromId = Annotated[CutoutPlots, Depends(_get_cutout_plots)]

router.get(
    "/alert/{diaSourceId}/cutouts",
    response_model=CutoutPlots,
)(_get_cutout_plots)

cache_response = cache(max_age=settings.cache_max_age, public=True)


@router.get(
    "/alert/{diaSourceId}",
    dependencies=[cache_response],
)
def display_alert(alert: AlertFromId, cutouts: CutoutPlotsFromId):
    return AlertDisplay(
        alert={k: v for k, v in alert.items() if not k.startswith("cutout")},
        cutouts=cutouts,
    )


@REQ_TIME.labels("get_photopoints_for_diaobject").time()
@router.get("/diaobject/{diaObjectId}/summaryplots")
def get_photopoints_for_diaobject(
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
    # compute offsets in arcsec from weighted mean position
    for coord in "ra", "dec":
        weights = 1 / df[f"{coord}Err"] ** 2
        center = (df[coord] * weights).sum() / weights.sum()
        df[f"{coord}Offset"] = (df[coord] - center) * 3600  # arcsec
        df[f"{coord}OffsetErr"] = df[f"{coord}Err"] * 3600

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
        labels={
            "epoch": "UTC date",
            "psfFlux": "PSF Flux (nJy)",
        },
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
        x="raOffset",
        y="decOffset",
        labels={
            "raOffset": "RA Offset (arcsec)",
            "decOffset": "Dec Offset (arcsec)",
        },
        error_x="raOffsetErr",
        error_y="decOffsetErr",
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
    # ensure symmetric axes
    max_xoffset = max(
        np.abs((df["raOffset"] + df["raOffsetErr"]).max()),
        np.abs((df["raOffset"] - df["raOffsetErr"]).min()),
    )
    max_yoffset = max(
        np.abs((df["decOffset"] + df["decOffsetErr"]).max()),
        np.abs((df["decOffset"] - df["decOffsetErr"]).min()),
    )
    centroid_fig.update_layout(
        yaxis_scaleanchor="x",
        xaxis_range=[-max_xoffset, max_xoffset],
        yaxis_range=[-max_yoffset, max_yoffset],
    )

    # return as ORJSONResponse to avoid serializability check (we already know
    # ORJSON can handle numpy arrays)
    response = ORJSONResponse(
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
    # manually call cache dependency; fastapi doesn't do this when you return a
    # response directly
    cache_response.dependency(response)
    return response


@router.get("/diaobject/{diaObjectId}/templates", dependencies=[cache_response])
def get_bandpass_templates(diaObjectId: int, connection: Connection):
    # find alert for latest visit in each band for this diaObjectId
    ids = connection.execute(
        "select any_value(diaSourceId order by diaSource.visit desc) as diaSourceId from alerts where diaSource.diaObjectId=? group by diaSource.band",
        [diaObjectId],
    ).fetchall()
    template_plots = {}
    for params in ids:
        # NB: execute one query per band to avoid pulling all blobs into memory;
        # duckdb iceberg plugin doesn't push down any column filters except
        # _single_ range and equality
        band, blob = connection.execute(
            "select diaSource.band, cutoutTemplate from alerts where diaSourceId=? limit 1",
            params,
        ).fetchone()
        template_plots[band] = make_cutout_plotly(
            f"template {band}",
            blob,
            significance_threshold=None,
        ).to_plotly_json()
    return template_plots


@REQ_TIME.labels("query_alerts").time()
@router.post("/alerts/query")
def query_alerts(query: AlertQuery, alerts: AlertRelation):
    """Execute an arbitrary query against the alerts table"""
    return query.flatten(alerts)


@router.get("/nights/list", dependencies=[cache_response])
async def list_nights(
    alerts: AlertRelation,
):
    return flatten(
        alerts.select(
            SQLExpression("diaSource.visit // 100000 as night").alias("night")
        )
        .aggregate([ColumnExpression("night"), SQLExpression("count(*) as alerts")])
        .order("night desc")
    )
