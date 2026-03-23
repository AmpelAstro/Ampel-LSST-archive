import datetime
from typing import Annotated

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    status,
)
from fastapi.responses import RedirectResponse, StreamingResponse
from glide import ListDirection
from starlette.concurrency import run_in_threadpool

from .iceberg import (
    AlertRelation,
    Connection,
    StreamQuery,
    flatten,
    table_name_token,
)
from .models import StreamDescription, StreamRecord
from .settings import settings
from .valkey import STREAM_TTL, Valkey

router = APIRouter(tags=["stream"])


def stream_key(resume_token: str) -> str:
    return f"stream:{resume_token}"


def stream_chunks_key(resume_token: str) -> str:
    return f"stream:{resume_token}:chunks"


def stream_chunk_pending_key(resume_token: str, chunk_id: int) -> str:
    return f"stream:{resume_token}:chunk:{chunk_id}:pending"


def stream_table(resume_token: str) -> str:
    return f"stream_{resume_token}"

async def create_stream_from_query(
    query: Annotated[StreamQuery, Body()],
    alert_relation: AlertRelation,
    cursor: Connection,
    tasks: BackgroundTasks,
    valkey: Valkey,
):
    """
    Create a stream of alerts from the given query. The resulting resume_token
    can be used to read the stream concurrently from multiple clients.
    """
    token = table_name_token()
    key = f"stream:{token}"
    table = f"stream_{token}"

    def get_num_rows() -> int:
        return cursor.sql(f"select count(*) from {table};").fetchone()[0]  # type: ignore[index]

    # create stream in the background
    async def create_stream() -> None:
        t0 = datetime.datetime.now(tz=datetime.UTC)
        await valkey.set(key, "pending", expiry=STREAM_TTL)
        await run_in_threadpool(query.persist_to, alert_relation, table)
        t1 = datetime.datetime.now(tz=datetime.UTC)
        record = StreamRecord(
            chunk_size=query.chunk_size,
            started_at=t0,
            finished_at=t1,
            expires_at=t1 + query.ttl,
            items=await run_in_threadpool(get_num_rows),
        )
        nchunks = (record.items + query.chunk_size - 1) // query.chunk_size
        await valkey.set(key, record.model_dump_json())
        await valkey.lpush(f"{key}:chunks", [str(i) for i in range(nchunks)])

    tasks.add_task(create_stream)

    return {"resume_token": token}

async def purge_expired_streams(valkey: Valkey, cursor: Connection):
    """
    Delete expired streams and their associated tables. This should be run
    periodically as a background task.
    """
    valkey_cursor = b"0"
    while True:
        valkey_cursor, keys = await valkey.scan(valkey_cursor, match="stream:*")
        for k in keys:
            if k.count(b":") != 1:
                continue
            info = await valkey.get(k)
            if info is None or info == "pending" or info == "error":
                continue
            record = StreamRecord.model_validate_json(info)
            if record.expires_at < datetime.datetime.now(tz=datetime.UTC):
                resume_token = k.decode().rsplit(":", maxsplit=1)[-1]
                await valkey.delete(
                    [f"stream:{resume_token}", f"stream:{resume_token}:chunks", f"stream:{resume_token}:chunks:pending"]
                )
                cursor.sql(f"drop table if exists stream_{resume_token};")
        if valkey_cursor == b"0":
            break

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
        post=f"{settings.root_path}/stream/{resume_token}/fetch",
        chunk_size=stream.chunk_size,
        items=stream.items,
        remaining=remaining_chunks,
        pending=pending_chunks,
        started_at=stream.started_at,
        finished_at=stream.finished_at,
        expires_at=stream.expires_at,
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
