from fastapi import (
    APIRouter,
    status,
)
from fastapi.responses import PlainTextResponse, RedirectResponse
from sqlalchemy import text

from .alert import AlertFromId
from .cutouts import render_cutout_plots
from .db import AsyncSession
from .settings import settings

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
        url=f"{settings.root_path}/display/alert/{diaSourceId}/cutouts",
        status_code=status.HTTP_303_SEE_OTHER,
    )
