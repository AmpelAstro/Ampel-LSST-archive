import base64
import hashlib
import io
import json
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import fastavro
from fastavro.types import Schema
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import join, select

from .avro import extract_record, pack_records
from .models import Alert, AvroBlob, AvroSchema
from .server.s3 import get_range

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine
    from sqlalchemy.sql.elements import ColumnElement

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


@asynccontextmanager
async def _rollback_on_exception(
    engine: "AsyncEngine",
    on_exception: None | Callable[[], Any] = None,
    on_complete: None | Callable[[], Any] = None,
) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(engine) as session:
        try:
            yield session
            await session.flush()
            if on_complete is not None:
                on_complete()
            await session.commit()
        except:
            if on_exception is not None:
                on_exception()
            await session.rollback()
            raise


async def insert_alert_chunk(
    engine: "AsyncEngine",
    bucket: "Bucket",
    schema_id: int,
    key: str,
    alerts: Sequence[dict],
    on_complete: None | Callable[[], Any] = None,
):
    async with AsyncSession(engine) as session:
        schema = await get_schema(session, schema_id)

    blob, ranges = pack_records(schema, alerts)
    name = f"{key}.avro"
    md5 = base64.b64encode(hashlib.md5(blob).digest()).decode("utf-8")

    obj = await bucket.Object(name)

    s3_response = await obj.put(
        Body=blob,
        ContentMD5=md5,
        ContentType="application/avro",
        Metadata={
            "schema-id": str(schema_id),
            "count": str(len(ranges)),
        },
    )
    assert 200 <= s3_response["ResponseMetadata"]["HTTPStatusCode"] < 300  # noqa: PLR2004

    async with _rollback_on_exception(
        engine,
        on_exception=obj.delete,
        on_complete=on_complete,
    ) as session:
        blob_record = AvroBlob(
            schema_id=schema_id,
            uri=name,
            count=len(alerts),
            size=len(blob),
            refcount=0,
        )
        session.add(blob_record)
        await session.flush()

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
                Alert.from_alert_packet(alert, blob_record.id, start, end).model_dump()
                for alert, (start, end) in zip(alerts, ranges, strict=True)
            ],
        )


async def get_blobs_with_condition(
    session: "AsyncSession",
    conditions: "Sequence[ColumnElement[bool] | bool]",
) -> AsyncGenerator[tuple[str, int, int, int], None]:
    for blob in await session.execute(
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
