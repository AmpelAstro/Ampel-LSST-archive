import base64
import hashlib
import json
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING

import fastavro
from astropy import units as u
from astropy_healpix import lonlat_to_healpix
from sqlmodel import Session, insert, select

from .avro import pack_records
from .models import Alert, AvroBlob, AvroSchema

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, Object
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
    engine: "Engine", s3_object: "Object"
) -> Generator[Session, None, None]:
    with Session(engine) as session:
        try:
            yield session
            session.commit()
        except:
            s3_object.delete()
            session.rollback()
            raise


def insert_alert_chunk(
    engine: "Engine",
    bucket: "Bucket",
    schema_id: int,
    key: str,
    alerts: Iterable[dict],
):
    schema = get_schema(engine, schema_id)

    blob, ranges = pack_records(schema, alerts)
    name = f"{key}.avro"
    md5 = base64.b64encode(hashlib.md5(blob).digest()).decode("utf-8")

    obj = bucket.Object(name)

    s3_response = obj.put(
        Body=blob,
        ContentMD5=md5,
        Metadata={"schema-id": str(schema_id)},
    )
    assert 200 <= s3_response["ResponseMetadata"]["HTTPStatusCode"] < 300  # noqa: PLR2004

    with _rollback_on_exception(engine, obj) as session:
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

        for alert, (start, end) in zip(alerts, ranges):
            diaSource = alert["diaSource"]
            session.add(
                Alert(
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
                    avro_blob_start=start,
                    avro_blob_end=end,
                )
            )
