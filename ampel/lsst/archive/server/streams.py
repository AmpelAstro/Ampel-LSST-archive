import datetime
from typing import Annotated

import sqlalchemy as sa
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)
from fastapi.responses import RedirectResponse, StreamingResponse
from sqlalchemy import delete
from sqlmodel import select

from ..models import ResultBlob, ResultGroup
from .db import (
    AsyncSession,
)
from .models import ChunkCount, StreamDescription
from .s3 import Bucket, chunk_object
from .settings import settings

router = APIRouter(tags=["stream"])


async def get_group(resume_token: str, session: AsyncSession) -> ResultGroup:
    group = await session.scalar(
        select(ResultGroup).filter(
            ResultGroup.name == resume_token,  # type: ignore[arg-type]
        )
    )
    if not group:
        raise HTTPException(status_code=404, detail="Stream not found") from None
    if group.error is None:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={"msg": "queue-populating query has not yet finished"},
        )
    if group.error:
        raise HTTPException(
            status_code=status.HTTP_424_FAILED_DEPENDENCY,
            detail={"msg": group.msg},
        )
    return group


GroupFromToken = Annotated[ResultGroup, Depends(get_group)]


async def get_blob(
    chunk_id: int, session: AsyncSession, group: GroupFromToken
) -> ResultBlob:
    blob = await session.scalar(
        select(ResultBlob)
        .filter(
            ResultBlob.group_id == group.id,  # type: ignore[arg-type]
            ResultBlob.id == chunk_id,  # type: ignore[arg-type]
        )
        .with_for_update(nowait=True)
    )
    if not blob:
        raise HTTPException(status_code=404, detail="Blob not found") from None
    return blob


BlobFromChunk = Annotated[ResultBlob, Depends(get_blob)]


@router.get(
    "/{resume_token}",
    # response_model=StreamDescription,
    response_model_exclude_none=True,
    responses={
        status.HTTP_423_LOCKED: {"description": "Query has not finished"},
        status.HTTP_424_FAILED_DEPENDENCY: {"description": "Query failed"},
    },
)
async def stream_get(session: AsyncSession, group: GroupFromToken) -> StreamDescription:
    pending = ResultBlob.issued.is_(None).label("pending")
    q = (
        select(
            pending,
            sa.func.count().label("chunks"),
            sa.func.sum(ResultBlob.count).label("items"),
            sa.func.sum(ResultBlob.size).label("bytes"),
        )
        .where(
            ResultBlob.group_id == group.id  # type: ignore[arg-type]
        )
        .group_by(pending)
    )
    p, r = (ChunkCount(chunks=0, items=0, bytes=0) for _ in range(2))
    for row in await session.execute(q):
        if row.pending:
            p = ChunkCount(
                chunks=row.chunks,
                items=row.items,
                bytes=row.bytes,
            )
        else:
            r = ChunkCount(
                chunks=row.chunks,
                items=row.items,
                bytes=row.bytes,
            )
    return StreamDescription(
        post=f"{settings.root_path}/{group.name}/chunk",
        chunk_size=group.chunk_size,
        remaining=p,
        pending=r,
        started_at=group.created,
        finished_at=group.resolved,
    )


@router.post(
    "/{resume_token}/fetch",
    tags=["stream"],
    response_model_exclude_none=True,
    responses={
        status.HTTP_204_NO_CONTENT: {"description": "No more chunks available"},
        status.HTTP_303_SEE_OTHER: {"description": "URL of claimed chunk"},
        status.HTTP_423_LOCKED: {"description": "Query has not finished"},
        status.HTTP_424_FAILED_DEPENDENCY: {"description": "Query failed"},
    },
    response_class=RedirectResponse,
    status_code=status.HTTP_303_SEE_OTHER,
)
async def stream_claim_chunk(
    session: AsyncSession,
    group: GroupFromToken,
):
    """
    Get the next available chunk of alerts from the given stream. This chunk will
    be reserved until explicitly released or deleted.
    """
    blob = await session.scalar(
        select(ResultBlob)
        .filter(
            ResultBlob.group_id == group.id,  # type: ignore[arg-type]
            ResultBlob.issued.is_(None),  # type: ignore[union-attr]
        )
        .with_for_update(skip_locked=True)
    )
    if not blob:
        raise HTTPException(status_code=status.HTTP_204_NO_CONTENT)
    blob.issued = datetime.datetime.now(datetime.UTC)
    session.add(blob)

    return f"{settings.root_path}/stream/{group.name}/chunk/{blob.id}"


@router.get(
    "/{resume_token}/chunk/{chunk_id}",
    tags=["stream"],
)
async def stream_get_chunk(
    blob: BlobFromChunk,
    session: AsyncSession,
    bucket: Bucket,
) -> StreamingResponse:
    """
    Download a chunk of alerts
    """
    obj = await bucket.Object(blob.uri)

    return StreamingResponse(
        chunk_object(obj, settings.stream_chunk_bytes),
        media_type=await obj.content_type,
    )


@router.delete(
    "/{resume_token}/chunk/{chunk_id}",
    tags=["stream"],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stream_delete_chunk(
    blob: BlobFromChunk,
    session: AsyncSession,
    bucket: Bucket,
):
    """
    Delete a consumed chunk
    """
    await (await bucket.Object(blob.uri)).delete()
    await session.delete(blob)


@router.post(
    "/{resume_token}/chunk/{chunk_id}/release",
    tags=["stream"],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stream_release_chunk(
    blob: BlobFromChunk,
    session: AsyncSession,
):
    """
    Mark the given chunk as unconsumed.
    """
    blob.issued = None
    session.add(blob)


@router.delete(
    "/{resume_token}",
    tags=["stream"],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stream_delete(resume_token: str, session: AsyncSession, bucket: Bucket):
    group = (
        await session.execute(
            select(ResultGroup).filter(
                ResultGroup.name == resume_token,  # type: ignore[arg-type]
            )
        )
    ).scalar()
    if group:
        # bulk delete blobs from S3 in groups of 1000
        async for uris in (
            await session.stream_scalars(
                select(ResultBlob.uri)
                .filter(
                    ResultBlob.group_id == group.id,  # type: ignore[arg-type]
                )
                .execution_options(yield_per=1000)
            )
        ).partitions():
            response = await bucket.meta.client.delete_objects(
                Bucket=bucket.name, Delete={"Objects": [{"Key": key} for key in uris]}
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] < 300  # noqa: PLR2004

        await session.execute(
            delete(ResultBlob).where(
                ResultBlob.group_id == group.id,  # type: ignore[arg-type]
            )
        )
        await session.delete(group)
        await session.commit()
    else:
        raise HTTPException(status_code=404, detail=f"Stream {resume_token} not found")
