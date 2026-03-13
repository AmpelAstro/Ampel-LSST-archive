import datetime
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)
from fastapi.responses import RedirectResponse, StreamingResponse
from .models import ChunkCount, StreamDescription, StreamRecord
from .settings import settings
from .valkey import Valkey
from .iceberg import Connection, flatten
from glide import ListDirection

router = APIRouter(tags=["stream"])


def stream_key(resume_token: str) -> str:
    return f"stream:{resume_token}"


def stream_chunks_key(resume_token: str) -> str:
    return f"stream:{resume_token}:chunks"


def stream_chunk_pending_key(resume_token: str, chunk_id: int) -> str:
    return f"stream:{resume_token}:chunk:{chunk_id}:pending"


def stream_table(resume_token: str) -> str:
    return f"stream_{resume_token}"


async def get_stream(resume_token: str, valkey: Valkey) -> StreamRecord:
    info = await valkey.get(stream_key(resume_token))
    if info is None:
        raise HTTPException(status_code=404, detail="Stream not found")
    if info == "pending":
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={"msg": "queue-populating query has not yet finished"},
        )
    if info == "error":
        raise HTTPException(
            status_code=status.HTTP_424_FAILED_DEPENDENCY,
            detail={"msg": "queue-populating query failed"},
        )
    return StreamRecord.model_validate_json(info)


StreamRecordFromToken = Annotated[StreamRecord, Depends(get_stream)]


@router.get(
    "/{resume_token}",
    response_model=StreamDescription,
    response_model_exclude_none=True,
    responses={
        status.HTTP_423_LOCKED: {"description": "Query has not finished"},
        status.HTTP_424_FAILED_DEPENDENCY: {"description": "Query failed"},
    },
)
async def stream_get(
    resume_token: str,
    stream: StreamRecordFromToken,
    valkey: Valkey,
) -> StreamDescription:
    remaining_chunks = (await valkey.llen(f"stream:{resume_token}:chunks")) or 0
    pending_chunks = (await valkey.llen(f"stream:{resume_token}:chunks:pending")) or 0
    return StreamDescription(
        post=f"{settings.root_path}/stream/{resume_token}/chunk",
        chunk_size=stream.chunk_size,
        items=stream.items,
        remaining=remaining_chunks,
        pending=pending_chunks,
        started_at=stream.started_at,
        finished_at=stream.finished_at,
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
    resume_token: str,
    stream: StreamRecordFromToken,
    valkey: Valkey,
):
    """
    Get the next available chunk of alerts from the given stream. This chunk will
    be reserved until explicitly released or deleted.
    """
    chunk_id = await valkey.lmove(
        f"stream:{resume_token}:chunks",
        f"stream:{resume_token}:chunks:pending",
        ListDirection.LEFT,
        ListDirection.LEFT,
    )
    if chunk_id is None:
        raise HTTPException(
            status_code=status.HTTP_204_NO_CONTENT, detail="No more chunks available"
        )

    return f"{settings.root_path}/stream/{resume_token}/chunk/{chunk_id.decode()}"


@router.get(
    "/{resume_token}/chunk/{chunk_id}",
    tags=["stream"],
)
def stream_get_chunk(
    resume_token: str,
    chunk_id: int,
    stream: StreamRecordFromToken,
    cursor: Connection,
    valkey: Valkey,
) -> StreamingResponse:
    """
    Download a chunk of alerts
    """
    relation = cursor.sql(
        f"select * from {stream_table(resume_token)} offset {chunk_id * stream.chunk_size} limit {stream.chunk_size};"
    )

    return flatten(relation)


@router.delete(
    "/{resume_token}/chunk/{chunk_id}",
    tags=["stream"],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stream_delete_chunk(
    resume_token: str,
    chunk_id: int,
    valkey: Valkey,
):
    """
    Delete a consumed chunk
    """
    await valkey.lrem(f"stream:{resume_token}:chunks:pending", 0, str(chunk_id))


@router.post(
    "/{resume_token}/chunk/{chunk_id}/release",
    tags=["stream"],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stream_release_chunk(
    resume_token: str,
    chunk_id: int,
    valkey: Valkey,
):
    """
    Mark the given chunk as unconsumed.
    """
    await valkey.lrem(f"stream:{resume_token}:chunks:pending", 0, str(chunk_id))
    await valkey.lpush(f"stream:{resume_token}:chunks", [str(chunk_id)])


@router.delete(
    "/{resume_token}",
    tags=["stream"],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stream_delete(
    resume_token: str,
    stream: StreamRecordFromToken,
    cursor: Connection,
    valkey: Valkey,
):
    cursor.sql(f"drop table if exists {stream_table(resume_token)}")
    await valkey.delete(
        [
            f"stream:{resume_token}",
            f"stream:{resume_token}:chunks",
            f"stream:{resume_token}:chunks:pending",
        ]
    )
