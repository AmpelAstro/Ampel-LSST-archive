from sqlmodel import SQLModel, Field
from sqlalchemy import BigInteger


class AvroBlob(SQLModel, table=True):
    """
    SQLModel class for the AvroBlob table.
    """

    id: int = Field(default=None, primary_key=True, sa_type=BigInteger)
    schema_id: int = Field(foreign_key="avroschema.id")
    uri: str
    count: int
    refcount: int
    size: int


class AvroSchema(SQLModel, table=True):
    """
    SQLModel class for the AvroSchema table.
    """

    id: int = Field(default=None, primary_key=True)
    content: str  # JSON schema as string


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
