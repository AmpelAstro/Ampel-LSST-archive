from datetime import UTC, datetime
from typing import TYPE_CHECKING

import astropy.units as u
from astropy_healpix import lonlat_to_healpix
from sqlalchemy import TIMESTAMP, BigInteger
from sqlmodel import Field, SQLModel

if TYPE_CHECKING:
    from .alert_packet import MPCORB
    from .alert_packet import Alert as LSSTAlert
    from .alert_packet import DIAObject as DIA
    from .alert_packet import SSSource as SSS

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
    error: None | bool = Field(default=None)
    msg: None | str = Field(default=None)
    created: datetime = Field(
        default_factory=lambda: datetime.now(UTC), sa_type=TIMESTAMP(timezone=True)
    )
    resolved: None | datetime = Field(default=None, sa_type=TIMESTAMP(timezone=True))


class ResultBlob(BaseBlob, table=True):
    group_id: int = Field(foreign_key="resultgroup.id")
    issued: datetime | None = Field(default=None, sa_type=TIMESTAMP(timezone=True))


class Alert(SQLModel, table=True):
    """
    SQLModel class for the AlertArchive table.
    """

    id: int = Field(
        default=None, primary_key=True, sa_type=BigInteger, description="diaSourceId"
    )
    diaobject_id: int = Field(
        sa_type=BigInteger, description="diaObjectId", foreign_key="diaobject.id"
    )
    ssobject_id: int = Field(
        sa_type=BigInteger, description="ssObjectId", foreign_key="ssobject.id"
    )
    midpointMjdTai: float
    ra: float
    dec: float
    hpx: int = Field(..., sa_type=BigInteger)
    avro_blob_id: int = Field(foreign_key="avroblob.id")
    avro_blob_start: int
    avro_blob_end: int

    @classmethod
    def from_record(
        cls, alert: "LSSTAlert", blob_id: int, blob_start: int, blob_end: int
    ) -> "Alert":
        diaSource = alert["diaSource"]
        return cls(
            id=diaSource["diaSourceId"],
            diaobject_id=diaSource["diaObjectId"] or None,
            ssobject_id=diaSource["ssObjectId"] or None,
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


class DIAObject(SQLModel, table=True):
    """
    SQLModel class for the DIAObject table.
    """

    id: int = Field(
        default=None, primary_key=True, sa_type=BigInteger, description="diaObjectId"
    )
    validityStartMjdTai: float
    firstDiaSourceMjdTai: float | None
    lastDiaSourceMjdTai: float | None
    nDiaSources: int
    ra: float
    dec: float
    hpx: int = Field(..., sa_type=BigInteger)

    @classmethod
    def from_record(cls, diaobject: "DIA") -> "DIAObject":
        return cls(
            id=diaobject["diaObjectId"],
            validityStartMjdTai=diaobject["validityStartMjdTai"],
            firstDiaSourceMjdTai=diaobject["firstDiaSourceMjdTai"],
            lastDiaSourceMjdTai=diaobject["lastDiaSourceMjdTai"],
            nDiaSources=diaobject["nDiaSources"],
            ra=diaobject["ra"],
            dec=diaobject["dec"],
            hpx=int(
                lonlat_to_healpix(
                    diaobject["ra"] * u.deg,
                    diaobject["dec"] * u.deg,
                    nside=NSIDE,
                    order="nested",
                )
            ),
        )


class SSObject(SQLModel, table=True):
    """
    SQLModel class for the SSObject table.
    """

    id: int = Field(
        default=None, primary_key=True, sa_type=BigInteger, description="ssSourceId"
    )
    designation: str | None = Field(default=None, description="MPC designation")

    @classmethod
    def from_record(cls, sssource: "SSS", mpcorb: "None | MPCORB") -> "SSObject":
        return cls(
            id=sssource["ssObjectId"],
            designation=mpcorb["mpcDesignation"] if mpcorb is not None else None,
        )
