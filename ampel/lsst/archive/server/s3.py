from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Annotated
from urllib.parse import urlsplit

import aioboto3
from aiobotocore.response import StreamingBody
from async_lru import alru_cache
from fastapi import Depends
from prometheus_async.aio import time

from .metrics import REQ_TIME
from .settings import settings

if TYPE_CHECKING:
    from types_aiobotocore_s3.service_resource import Bucket as _Bucket
    from types_aiobotocore_s3.service_resource import Object, S3ServiceResource


@alru_cache(maxsize=1)
async def get_s3_client() -> "S3ServiceResource":
    session = aioboto3.Session()
    # fake `async with`: https://github.com/terricain/aioboto3/issues/197#issuecomment-756992493
    return await session.resource(
        "s3",
        endpoint_url=str(settings.s3_endpoint_url)
        if settings.s3_endpoint_url
        else None,
        verify=not settings.s3_insecure,
    ).__aenter__()


async def get_s3_bucket() -> "_Bucket":
    resource = await get_s3_client()
    return await resource.Bucket(settings.s3_bucket)


Bucket = Annotated["_Bucket", Depends(get_s3_bucket)]


async def get_stream(bucket: "_Bucket", key: str) -> "StreamingBody":
    response = await (await bucket.Object(key)).get()
    if response["ResponseMetadata"]["HTTPStatusCode"] <= 400:  # noqa: PLR2004
        return response["Body"]
    raise KeyError


@time(REQ_TIME.labels("get_range"))
async def get_range(
    bucket: "_Bucket", key: str, start: int, end: int
) -> "StreamingBody":
    obj = await bucket.Object(key)
    try:
        response = await obj.get(Range=f"bytes={start}-{end}")
    except bucket.meta.client.exceptions.NoSuchKey as err:
        raise KeyError(f"bucket {bucket.name} has no key {key}") from err
    if response["ResponseMetadata"]["HTTPStatusCode"] <= 400:  # noqa: PLR2004
        return response["Body"]
    raise KeyError


async def chunk_object(obj: "Object", chunk_length: int) -> AsyncGenerator[bytes, None]:
    """Async generator to get file chunk."""

    content_length = await obj.content_length

    for offset in range(0, content_length, chunk_length):
        end = min(offset + chunk_length - 1, content_length - 1)
        response = await obj.get(Range=f"bytes={offset}-{end}")
        assert response["ResponseMetadata"]["HTTPStatusCode"] < 300  # noqa: PLR2004
        async with response["Body"] as stream:
            yield await stream.read()


def get_url_for_key(bucket: "_Bucket", key: str) -> str:
    return f"{settings.s3_endpoint_url or ''}/{bucket.name}/{key}"


def get_key_for_url(bucket: "_Bucket", uri: str) -> str:
    path = urlsplit(uri).path.split("/")
    assert path[-2] == bucket.name
    return path[-1]
