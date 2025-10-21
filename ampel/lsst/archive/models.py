from datetime import datetime

import astropy.units as u
from astropy_healpix import lonlat_to_healpix
from sqlalchemy import BigInteger
from sqlmodel import Field, SQLModel

NSIDE = 1 << 16


class BaseBlob(SQLModel):
    id: int = Field(default=None, primary_key=True, sa_type=BigInteger)
    schema_id: int = Field(foreign_key="avroschema.id")
    uri: str
    count: int
    size: int


class AvroBlob(BaseBlob, table=True):
    """
    SQLModel class for the AvroBlob table.
    """

    refcount: int


class AvroSchema(SQLModel, table=True):
    """
    SQLModel class for the AvroSchema table.
    """

    id: int = Field(default=None, primary_key=True)
    content: str  # JSON schema as string


class ResultGroup(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: str = Field(unique=True)
    chunk_size: int


class ResultBlob(BaseBlob, table=True):
    group_id: int = Field(foreign_key="resultgroup.id")
    issued: datetime | None = Field(default=None)


class Alert(SQLModel, table=True):
    """
    SQLModel class for the AlertArchive table.
    """

    id: int = Field(
        default=None, primary_key=True, sa_type=BigInteger, description="diaSourceId"
    )
    object_id: int = Field(sa_type=BigInteger, description="diaObjectId")
    midpointMjdTai: float
    ra: float
    dec: float
    hpx: int = Field(..., sa_type=BigInteger)
    avro_blob_id: int = Field(foreign_key="avroblob.id")
    avro_blob_start: int
    avro_blob_end: int

    @classmethod
    def from_alert_packet(
        cls, alert: dict, blob_id: int, blob_start: int, blob_end: int
    ) -> "Alert":
        diaSource = alert["diaSource"]
        return cls(
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
        )
