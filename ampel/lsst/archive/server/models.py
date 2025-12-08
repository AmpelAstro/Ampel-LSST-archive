import math
from base64 import b64encode
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from astropy.time import Time
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    PlainSerializer,
    field_validator,
    model_validator,
)

from ..models import NSIDE
from ..types import FilterClause


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Stream(BaseModel):
    path: str
    chunk_size: int


class ChunkCount(BaseModel):
    items: int
    chunks: int
    bytes: int


class StreamDescription(BaseModel):
    post: str = Field(description="URL to post to in order to get the next chunk")
    chunk_size: int
    remaining: ChunkCount = Field(description="Unconsumed items")
    pending: ChunkCount = Field(description="Items reserved but not yet consumed")
    started_at: datetime = Field(description="Timestamp when query was issued")
    finished_at: datetime | None = Field(
        default=None,
        description="Timestamp when query finished (null if still running)",
    )


class PlotlyFigure(BaseModel):
    data: JsonValue
    layout: JsonValue
    frames: list[JsonValue] | None = None


class CutoutPlots(BaseModel):
    template: PlotlyFigure
    science: PlotlyFigure
    difference: PlotlyFigure


class AlertDisplay(BaseModel):
    alert: JsonValue
    cutouts: CutoutPlots


class AstropyTime(BaseModel):
    """
    A time representation compatible with astropy.time.Time
    """

    val: str | float | int
    val2: None | float | int = None
    format: None | str = None
    scale: None | Literal["tai", "tcb", "tcg", "tdb", "tt", "ut1", "utc"] = None
    precision: None | int = None
    in_subfmt: None | str = None
    out_subfmt: None | str = None
    location: None | tuple[float, float, float] = None

    def to_astropy_time(self) -> Time:
        return Time(
            val=self.val,
            val2=self.val2,
            format=self.format,
            scale=self.scale,
            precision=self.precision,
            in_subfmt=self.in_subfmt,
            out_subfmt=self.out_subfmt,
            location=self.location,
        )

    def mjd_tai(self) -> float:
        return float(Time(self.to_astropy_time(), scale="tai").mjd)

    @model_validator(mode="before")
    def parse_astropy_time(cls, data: Any) -> Any:
        if isinstance(data, str | float | int):
            return {"val": data}
        return data

    @model_validator(mode="after")
    def validate_astropy_time(self) -> Self:
        self.to_astropy_time()
        return self


class Topic(BaseModel):
    description: str = Field(..., description="Informative string for this topic")
    candids: list[int] = Field(
        ..., description="IPAC candidate ids to associate with this topic"
    )


class TopicDescription(BaseModel):
    topic: str
    description: str = Field(..., description="Informative string for this topic")
    size: int


class TopicQuery(StrictModel):
    topic: str
    chunk_size: int = Field(
        100, ge=100, le=10000, description="Number of alerts per chunk"
    )
    start: int | None = Field(None, ge=0)
    stop: int | None = Field(None, ge=1)
    step: int | None = Field(None, gt=0)


class ConeConstraint(StrictModel):
    ra: float = Field(
        ..., description="Right ascension of field center in degrees (J2000)"
    )
    dec: float = Field(
        ..., description="Declination of field center in degrees (J2000)"
    )
    radius: float = Field(
        ..., gt=0, lt=10, description="Radius of search cone in degrees"
    )


class TimeConstraint(StrictModel):
    lt: float | None = Field(None, alias="$lt")
    gt: float | None = Field(None, alias="$gt")


class StrictTimeConstraint(TimeConstraint):
    lt: float = Field(..., alias="$lt")
    gt: float = Field(..., alias="$gt")


class CandidateFilterable(StrictModel):
    candidate: FilterClause | None = None


class AlertQuery(CandidateFilterable):
    cone: ConeConstraint | None = None
    jd: TimeConstraint = TimeConstraint()  # type: ignore[call-arg]
    candidate: FilterClause | None = None
    chunk_size: int = Field(
        100, ge=0, le=10000, description="Number of alerts per chunk"
    )

    @model_validator(mode="before")
    @classmethod
    def at_least_one_constraint(cls, values: Any):
        if isinstance(values, dict) and not {"cone", "jd"}.intersection(values.keys()):
            raise ValueError("At least one constraint (cone or jd) must be specified")
        return values


class ObjectQuery(CandidateFilterable):
    objectId: str | list[str]
    jd: TimeConstraint = TimeConstraint()  # type: ignore[call-arg]
    candidate: FilterClause | None = None
    chunk_size: int = Field(
        100, ge=0, le=10000, description="Number of alerts per chunk"
    )


class AlertChunkQueryBase(StrictModel):
    """Options for queries that will return a chunk of alerts"""

    latest: bool = Field(
        False, description="Return only the latest alert for each objectId"
    )
    with_history: bool = False
    with_cutouts: bool = False
    chunk_size: int = Field(
        100, gt=0, le=10000, description="Number of alerts to return per page"
    )
    resume_token: str | None = Field(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    )


class MapQueryBase(CandidateFilterable):
    jd: StrictTimeConstraint


class HEALpixMapRegion(StrictModel):
    nside: int = Field(..., gt=0, le=NSIDE)
    pixels: list[int]

    @field_validator("nside")
    @classmethod
    def power_of_two(cls, nside):
        if not math.log2(nside).is_integer():
            raise ValueError("nside must be a power of 2")
        return nside


class HEALpixMapQuery(AlertChunkQueryBase, MapQueryBase, HEALpixMapRegion): ...


class HEALpixRegionQueryBase(MapQueryBase):
    regions: list[HEALpixMapRegion]


class HEALpixRegionQuery(AlertChunkQueryBase, HEALpixRegionQueryBase): ...


class HEALpixRegionCountQuery(HEALpixRegionQueryBase): ...


# NB: ser_json_bytes="base64" uses a URL-safe alphabet, whereas b64encode
# uses +/ to represent 62 and 63. Use a serialization function to emit
# strings that can be properly decoded with b64decode. See:
# https://github.com/pydantic/pydantic/issues/7000
StampData = Annotated[
    bytes,
    PlainSerializer(lambda v: b64encode(v), return_type=str, when_used="json"),
]


class AlertBase(BaseModel):
    diaSourceId: int


class AlertCutouts(AlertBase):
    cutoutScience: None | StampData = None
    cutoutTemplate: None | StampData = None
    cutoutDifference: None | StampData = None


class AsyncResult(BaseModel):
    resume_token: str
