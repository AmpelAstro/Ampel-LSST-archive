from datetime import timedelta
from typing import Annotated

from async_lru import alru_cache
from fastapi import Depends
from glide import (
    ExpirySet,
    ExpiryType,
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
)

from .settings import settings

cache = alru_cache(None)


@cache
async def get_valkey_client():
    addresses = [
        NodeAddress(
            host=settings.valkey_url.host, port=settings.valkey_url.port or 6379
        )
    ]
    client_config = GlideClientConfiguration(addresses)
    return await GlideClient.create(client_config)


Valkey = Annotated[GlideClient, Depends(get_valkey_client)]

STREAM_TTL = ExpirySet(ExpiryType.SEC, timedelta(days=1))
CHUNK_CLAIM_TTL = ExpirySet(ExpiryType.SEC, timedelta(minutes=5))
KEEP_TTL = ExpirySet(ExpiryType.KEEP_TTL, None)
