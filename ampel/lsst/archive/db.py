import base64
import datetime
import hashlib
import io
import json
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import TYPE_CHECKING, Any

import fastavro
from fastavro.types import Schema
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import join, select

from .avro import extract_record, pack_blocks, pack_records
from .models import Alert, AvroBlob, AvroSchema, BaseBlob, ResultBlob, ResultGroup
from .server.db import get_session
from .server.s3 import get_range

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine
    from sqlalchemy.sql.elements import ColumnElement
    from types_aiobotocore_s3.service_resource import Object

    from .server.s3 import Bucket


AVRO_SCHEMAS: dict[int, Schema] = {}


async def ensure_schema(engine: "AsyncEngine", schema_id: int, content: str) -> Schema:
    if schema_id not in AVRO_SCHEMAS:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            schema = (
                await session.execute(
                    select(AvroSchema).where(AvroSchema.id == schema_id)
                )
            ).scalar()
            if schema is None:
                await session.execute(
                    insert(AvroSchema).values(id=schema_id, content=content)
                )
                await session.commit()
            else:
                content = schema.content
        AVRO_SCHEMAS[schema_id] = fastavro.parse_schema(json.loads(content))
    return AVRO_SCHEMAS[schema_id]


async def get_schema(session: "AsyncSession", schema_id: int) -> Schema:
    if schema_id not in AVRO_SCHEMAS:
        schema = (
            await session.execute(select(AvroSchema).where(AvroSchema.id == schema_id))
        ).scalar()
        if schema is None:
            raise KeyError(f"No schema with id {schema_id}")
        AVRO_SCHEMAS[schema_id] = fastavro.parse_schema(json.loads(schema.content))
    return AVRO_SCHEMAS[schema_id]


async def store_alert_chunk(
    bucket: "Bucket", session: "AsyncSession", record: BaseBlob, blob: bytes
) -> "Object":
    obj = await bucket.Object(record.uri)
    s3_response = await obj.put(
        Body=blob,
        ContentMD5=base64.b64encode(hashlib.md5(blob).digest()).decode("utf-8"),
        ContentType="application/avro",
        Metadata={
            "schema-id": str(record.schema_id),
            "count": str(record.count),
        },
    )
    assert 200 <= s3_response["ResponseMetadata"]["HTTPStatusCode"] < 300  # noqa: PLR2004

    session.add(record)
    try:
        await session.flush()
    except:
        await obj.delete()
        raise

    return obj


async def insert_alert_chunk(
    engine: "AsyncEngine",
    bucket: "Bucket",
    schema_id: int,
    key: str,
    alerts: Sequence[dict],
    on_complete: None | Callable[[], Any] = None,
):
    async with (
        AsyncSession(engine, expire_on_commit=False) as session,
        session.begin() as transaction,
    ):
        schema = await get_schema(session, schema_id)

        blob, ranges = pack_records(schema, alerts)
        name = f"{key}.avro"

        blob_record = AvroBlob(
            schema_id=schema_id,
            uri=name,
            count=len(alerts),
            size=len(blob),
            refcount=0,
        )
        obj = await store_alert_chunk(bucket, session, blob_record, blob)

        try:
            insert_stmt = insert(Alert)
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    Alert.id,  # type: ignore[list-item]
                ],
                set_={
                    k: insert_stmt.excluded[k]
                    for k in ("avro_blob_id", "avro_blob_start", "avro_blob_end")
                },
            )

            await session.execute(
                stmt,
                params=[
                    Alert.from_alert_packet(
                        alert, blob_record.id, start, end
                    ).model_dump()
                    for alert, (start, end) in zip(alerts, ranges, strict=True)
                ],
            )
            await session.flush()
            await transaction.commit()
            if on_complete is not None:
                on_complete()
        except:
            await transaction.rollback()
            await obj.delete()
            raise


async def get_blobs_with_condition(
    session: "AsyncSession",
    conditions: "Sequence[ColumnElement[bool] | bool]",
) -> AsyncGenerator[tuple[str, int, int, int], None]:
    async for blob in await session.stream(
        select(
            AvroBlob.uri,
            Alert.avro_blob_start,
            Alert.avro_blob_end,
            AvroBlob.schema_id,
        )
        .select_from(
            join(
                Alert,
                AvroBlob,
                Alert.avro_blob_id == AvroBlob.id,  # type: ignore[arg-type]
            )
        )
        .where(*conditions)
    ):
        uri, start, end, schema_id = blob
        yield (uri, start, end, schema_id)


async def store_search_results(
    session: "AsyncSession",
    bucket: "Bucket",
    group: ResultGroup,
    conditions: "Sequence[ColumnElement[bool] | bool]",
):
    bodies: list[bytes] = []
    last_schema_id = None
    chunk_size = group.chunk_size
    count = 1
    chunk = 0

    async def flush():
        blob = pack_blocks(await get_schema(session, schema_id), bodies)
        return await store_alert_chunk(
            bucket,
            session,
            ResultBlob(
                schema_id=last_schema_id if last_schema_id is not None else schema_id,
                group_id=group.id,
                uri=f"group/{group.name}/{chunk:020d}.avro",
                count=len(bodies),
                size=len(blob),
            ),
            blob,
        )

    async for uri, start, end, schema_id in get_blobs_with_condition(
        session, conditions
    ):
        body = await get_range(bucket, uri, start, end)
        bodies.append(await body.read())
        if (
            last_schema_id is not None and schema_id != last_schema_id
        ) or count >= chunk_size:
            await flush()

            bodies.clear()
            count = 1
            chunk += 1
        last_schema_id = schema_id

    if bodies:
        await flush()


async def populate_chunks(
    bucket: "Bucket", group: ResultGroup, conditions: "Sequence[ColumnElement[bool]]"
):
    async with get_session() as task_session:
        try:
            await store_search_results(
                task_session,
                bucket,
                group,
                conditions,
            )
            group.error = False
            group.resolved = datetime.datetime.now(datetime.UTC)
        except Exception as e:
            group.error = True
            group.msg = str(e)
        task_session.add(group)


async def get_alert_from_s3(
    id: int,
    session: "AsyncSession",
    bucket: "Bucket",
) -> dict | None:
    async for uri, start, end, schema_id in get_blobs_with_condition(
        session,
        [Alert.id == id],
    ):
        schema = await get_schema(session, schema_id)
        body = await get_range(bucket, uri, start, end)
        try:
            record = extract_record(io.BytesIO(await body.read()), schema)
        except KeyError:
            return None
        assert isinstance(record, dict)
        return record
    return None
