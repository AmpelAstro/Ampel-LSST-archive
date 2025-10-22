from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated

from asyncpg.exceptions import QueryCanceledError
from fastapi import Depends, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
)
from sqlalchemy.ext.asyncio import AsyncSession as _AsyncSession

from .settings import PostgresDsn, settings


def uri_with_driver(driver: str) -> str:
    uri = settings.archive_uri
    scheme = f"{uri.scheme.split('+')[0]}+{driver}"
    return str(
        PostgresDsn.build(
            scheme=scheme,
            hosts=uri.hosts(),
            path=uri.path[1:] if uri.path else None,
            query=uri.query,
            fragment=uri.fragment,
        )
    )


@lru_cache(maxsize=1)
def get_engine() -> AsyncEngine:
    timeout_ms = 1000 * settings.default_statement_timeout
    return create_async_engine(
        uri_with_driver("asyncpg"),
        connect_args={"server_settings": {"statement_timeout": f"{timeout_ms}"}},
    )


@asynccontextmanager
async def get_session() -> AsyncGenerator[_AsyncSession, None]:
    async with (
        _AsyncSession(get_engine(), expire_on_commit=False) as session,
        session.begin(),
    ):
        yield session


# NB: Depends does the same thing as asynccontextmanager, so unwrap to get the
# underlying function
AsyncSession = Annotated[_AsyncSession, Depends(get_session().func)]


async def handle_querycancelederror(request: Request, exc: QueryCanceledError):
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
