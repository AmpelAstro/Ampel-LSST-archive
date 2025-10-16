import base64
import hashlib
import json
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import fastavro
from astropy import units as u
from astropy_healpix import lonlat_to_healpix
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, join, select

from .avro import extract_record, pack_records
from .models import Alert, AvroBlob, AvroSchema
from .server.s3 import get_range

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket
    from sqlalchemy import Engine

NSIDE = 1 << 16

AVRO_SCHEMAS: dict[str, dict] = {}


def ensure_schema(engine: "Engine", schema_id: int, content: str) -> dict:
    if schema_id not in AVRO_SCHEMAS:
        with Session(engine) as session:
            schema = session.exec(
                select(AvroSchema).filter(AvroSchema.id == schema_id)
            ).first()
            if schema is None:
                session.exec(insert(AvroSchema).values(id=schema_id, content=content))
                session.commit()
            else:
                content = schema.content
        AVRO_SCHEMAS[schema_id] = fastavro.parse_schema(json.loads(content))
    return AVRO_SCHEMAS[schema_id]


def get_schema(engine: "Engine", schema_id: int) -> dict:
    if schema_id not in AVRO_SCHEMAS:
        with Session(engine) as session:
            schema = session.exec(
                select(AvroSchema).filter(AvroSchema.id == schema_id)
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


def _alert_values(alert: dict, blob_id: int, blob_start: int, blob_end: int) -> dict:
    diaSource = alert["diaSource"]
    return Alert(
        id=diaSource["diaSourceId"],
        object_id=diaSource["diaObjectId"],
        midpointMjdTai=diaSource["midpointMjdTai"],
        ra=diaSource["ra"],
        dec=diaSource["dec"],
        hpx=int(
            lonlat_to_healpix(
                diaSource["ra"] * u.deg,
                diaSource["dec"] * u.deg,
                nside=NSIDE,
                order="nested",
            )
        ),
        avro_blob_id=blob_id,
        avro_blob_start=blob_start,
        avro_blob_end=blob_end,
    ).model_dump()


def insert_alert_chunk(
    engine: "Engine",
    bucket: "Bucket",
    schema_id: int,
    key: str,
    alerts: Iterable[dict],
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
        (blob_id,) = session.exec(
            insert(AvroBlob)
            .values(
                schema_id=schema_id,
                uri=name,
                count=len(alerts),
                size=len(blob),
                refcount=0,
            )
            .returning(AvroBlob.id)
        ).first()

        session.exec(
            insert(Alert)
            .values(
                [
                    _alert_values(alert, blob_id, start, end)
                    for alert, (start, end) in zip(alerts, ranges, strict=False)
                ]
            )
            .on_conflict_do_nothing(index_elements=[Alert.id])
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
            .select_from(join(Alert, AvroBlob, Alert.avro_blob_id == AvroBlob.id))
            .where(Alert.id == id)
        ).first()
        if blob is None:
            return None
        uri, start, end = blob
        try:
            return extract_record(*get_range(bucket, uri, start, end))
        except KeyError:
            return None
