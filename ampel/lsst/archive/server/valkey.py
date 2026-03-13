import asyncio
from datetime import timedelta
from functools import cache
from typing import Annotated

from fastapi import Depends
from glide import (
    ExpirySet,
    ExpiryType,
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
)

from .settings import settings


def async_cache(async_function):
    @cache
    def wrapper(*args, **kwargs):
        coro = async_function(*args, **kwargs)
        return asyncio.ensure_future(coro)

    return wrapper


# @async_cache
async def get_valkey_client():
    addresses = [
        NodeAddress(
            host=settings.valkey_url.host, port=settings.valkey_url.port or 6379
        )
    ]
    client_config = GlideClientConfiguration(addresses)
    return await GlideClient.create(client_config)


get_valkey_client.cache_clear = lambda: None

Valkey = Annotated[GlideClient, Depends(get_valkey_client)]

STREAM_TTL = ExpirySet(ExpiryType.SEC, timedelta(days=1))
CHUNK_CLAIM_TTL = ExpirySet(ExpiryType.SEC, timedelta(minutes=5))
KEEP_TTL = ExpirySet(ExpiryType.KEEP_TTL, None)
