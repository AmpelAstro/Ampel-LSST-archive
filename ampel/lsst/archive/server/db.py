from functools import lru_cache

from fastapi import Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg2.errors import QueryCanceled  # type: ignore[import]
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from sqlmodel import create_engine

from ampel.lsst.t0.ArchiveUpdater import ArchiveUpdater

from .settings import settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(str(settings.archive_uri))


async def handle_operationalerror(request: Request, exc: OperationalError):
    if isinstance(exc.orig, QueryCanceled):
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content=jsonable_encoder(
                {
                    "detail": {
                        "msg": f"Query canceled after {settings.default_statement_timeout} s"
                    }
                }
            ),
            headers={"retry-after": str(2 * settings.default_statement_timeout)},
        )
    raise exc
