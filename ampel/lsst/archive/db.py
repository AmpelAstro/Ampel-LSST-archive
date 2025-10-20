import base64
import hashlib
import json
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import fastavro
from fastavro.types import Schema
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, join, select

from .avro import extract_record, pack_records
from .models import Alert, AvroBlob, AvroSchema
from .server.s3 import get_range

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket
    from sqlalchemy import Engine


AVRO_SCHEMAS: dict[int, Schema] = {}


def ensure_schema(engine: "Engine", schema_id: int, content: str) -> Schema:
    if schema_id not in AVRO_SCHEMAS:
        with Session(engine) as session:
            schema = session.exec(
                select(AvroSchema).where(AvroSchema.id == schema_id)
            ).first()
            if schema is None:
                session.exec(insert(AvroSchema).values(id=schema_id, content=content))
                session.commit()
            else:
                content = schema.content
        AVRO_SCHEMAS[schema_id] = fastavro.parse_schema(json.loads(content))
    return AVRO_SCHEMAS[schema_id]


def get_schema(engine: "Engine", schema_id: int) -> Schema:
    if schema_id not in AVRO_SCHEMAS:
        with Session(engine) as session:
            schema = session.exec(
                select(AvroSchema).where(AvroSchema.id == schema_id)
            ).first()
            if schema is None:
                raise KeyError(f"No schema with id {schema_id}")
        AVRO_SCHEMAS[schema_id] = fastavro.parse_schema(json.loads(schema.content))
    return AVRO_SCHEMAS[schema_id]


@contextmanager
def _rollback_on_exception(
    engine: "Engine",
    on_exception: None | Callable[[], Any] = None,
    on_complete: None | Callable[[], Any] = None,
) -> Generator[Session, None, None]:
    with Session(engine) as session:
        try:
            yield session
            session.flush()
            if on_complete is not None:
                on_complete()
            session.commit()
        except:
            if on_exception is not None:
                on_exception()
            session.rollback()
            raise


def insert_alert_chunk(
    engine: "Engine",
    bucket: "Bucket",
    schema_id: int,
    key: str,
    alerts: Sequence[dict],
    on_complete: None | Callable[[], Any] = None,
):
    schema = get_schema(engine, schema_id)

    blob, ranges = pack_records(schema, alerts)
    name = f"{key}.avro"
    md5 = base64.b64encode(hashlib.md5(blob).digest()).decode("utf-8")

    obj = bucket.Object(name)

    s3_response = obj.put(
        Body=blob,
        ContentMD5=md5,
        ContentType="application/avro",
        Metadata={
            "schema-id": str(schema_id),
            "count": str(len(ranges)),
        },
    )
    assert 200 <= s3_response["ResponseMetadata"]["HTTPStatusCode"] < 300  # noqa: PLR2004

    with _rollback_on_exception(
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
        session.flush()

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

        session.exec(
            stmt,
            params=[
                Alert.from_alert_packet(alert, blob_record.id, start, end).model_dump()
                for alert, (start, end) in zip(alerts, ranges, strict=True)
            ],
        )


def get_alert_from_s3(
    id: int,
    engine: "Engine",
    bucket: "Bucket",
) -> dict | None:
    with Session(engine) as session:
        blob = session.exec(
            select(
                AvroBlob.uri,
                Alert.avro_blob_start,
                Alert.avro_blob_end,
            )
            .select_from(
                join(
                    Alert,
                    AvroBlob,
                    Alert.avro_blob_id == AvroBlob.id,  # type: ignore[arg-type]
                )
            )
            .where(Alert.id == id)
        ).first()
        if blob is None:
            return None
        uri, start, end = blob
        try:
            record = extract_record(*get_range(bucket, uri, start, end))
            assert isinstance(record, dict)
            return record
        except KeyError:
            return None
