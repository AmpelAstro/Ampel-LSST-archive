from fastapi import (
    APIRouter,
)
from fastapi.responses import PlainTextResponse

from .alert import AlertFromId
from .cutouts import render_cutout_plots

router = APIRouter(tags=["display"])


@router.get(
    "/alert/{diaSourceId}/cutouts",
)
def display_cutouts(alert: AlertFromId):
    return PlainTextResponse(
        render_cutout_plots(
            {
                k: alert[f"cutout{k.capitalize()}"]
                for k in ["template", "science", "difference"]
            }
        ),
        media_type="image/svg+xml",
    )
