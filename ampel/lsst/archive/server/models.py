import math
from base64 import b64encode
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
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


# Generated from tests/test-data/schema_3.3.json
# 1. Convert avro to json-schema with https://json-schema-validator.herokuapp.com/avro.jsp
# 2. Convert json-schema to pydantic with datamodel-codegen --input schema_3.3.json --output alert --use-schema-description


class Candidate(BaseModel):
    """
    avro alert schema
    """

    jd: float
    fid: int
    pid: int
    diffmaglim: float | None = None
    pdiffimfilename: str | None = None
    programpi: str | None = None
    programid: Literal[1, 2, 3]
    candid: int
    isdiffpos: str
    tblid: int
    nid: int | None = None
    rcid: int | None = None
    field: int | None = None
    xpos: float | None = None
    ypos: float | None = None
    ra: float
    dec: float
    magpsf: float
    sigmapsf: float
    chipsf: float | None = None
    magap: float | None = None
    sigmagap: float | None = None
    distnr: float | None = None
    magnr: float | None = None
    sigmagnr: float | None = None
    chinr: float | None = None
    sharpnr: float | None = None
    sky: float | None = None
    magdiff: float | None = None
    fwhm: float | None = None
    classtar: float | None = None
    mindtoedge: float | None = None
    magfromlim: float | None = None
    seeratio: float | None = None
    aimage: float | None = None
    bimage: float | None = None
    aimagerat: float | None = None
    bimagerat: float | None = None
    elong: float | None = None
    nneg: int | None = None
    nbad: int | None = None
    rb: float | None = None
    ssdistnr: float | None = None
    ssmagnr: float | None = None
    ssnamenr: str | None = None
    sumrat: float | None = None
    magapbig: float | None = None
    sigmagapbig: float | None = None
    ranr: float
    decnr: float
    sgmag1: float | None = None
    srmag1: float | None = None
    simag1: float | None = None
    szmag1: float | None = None
    sgscore1: float | None = None
    distpsnr1: float | None = None
    ndethist: int
    ncovhist: int
    jdstarthist: float | None = None
    jdendhist: float | None = None
    scorr: float | None = None
    tooflag: int | None = None
    objectidps1: int | None = None
    objectidps2: int | None = None
    sgmag2: float | None = None
    srmag2: float | None = None
    simag2: float | None = None
    szmag2: float | None = None
    sgscore2: float | None = None
    distpsnr2: float | None = None
    objectidps3: int | None = None
    sgmag3: float | None = None
    srmag3: float | None = None
    simag3: float | None = None
    szmag3: float | None = None
    sgscore3: float | None = None
    distpsnr3: float | None = None
    nmtchps: int
    rfid: int
    jdstartref: float
    jdendref: float
    nframesref: int
    rbversion: str | None = None
    dsnrms: float | None = None
    ssnrms: float | None = None
    dsdiff: float | None = None
    magzpsci: float | None = None
    magzpsciunc: float | None = None
    magzpscirms: float | None = None
    nmatches: int | None = None
    clrcoeff: float | None = None
    clrcounc: float | None = None
    zpclrcov: float | None = None
    zpmed: float | None = None
    clrmed: float | None = None
    clrrms: float | None = None
    neargaia: float | None = None
    neargaiabright: float | None = None
    maggaia: float | None = None
    maggaiabright: float | None = None
    exptime: float | None = None
    drb: float | None = None
    drbversion: str | None = None


class PrvCandidate(BaseModel):
    """
    avro alert schema
    """

    jd: float
    fid: int
    pid: int
    diffmaglim: float | None = None
    pdiffimfilename: str | None = None
    programpi: str | None = None
    programid: int
    candid: int | None = None
    isdiffpos: str | None = None
    tblid: int | None = None
    nid: int | None = None
    rcid: int | None = None
    field: int | None = None
    xpos: float | None = None
    ypos: float | None = None
    ra: float | None = None
    dec: float | None = None
    magpsf: float | None = None
    sigmapsf: float | None = None
    chipsf: float | None = None
    magap: float | None = None
    sigmagap: float | None = None
    distnr: float | None = None
    magnr: float | None = None
    sigmagnr: float | None = None
    chinr: float | None = None
    sharpnr: float | None = None
    sky: float | None = None
    magdiff: float | None = None
    fwhm: float | None = None
    classtar: float | None = None
    mindtoedge: float | None = None
    magfromlim: float | None = None
    seeratio: float | None = None
    aimage: float | None = None
    bimage: float | None = None
    aimagerat: float | None = None
    bimagerat: float | None = None
    elong: float | None = None
    nneg: int | None = None
    nbad: int | None = None
    rb: float | None = None
    ssdistnr: float | None = None
    ssmagnr: float | None = None
    ssnamenr: str | None = None
    sumrat: float | None = None
    magapbig: float | None = None
    sigmagapbig: float | None = None
    ranr: float | None = None
    decnr: float | None = None
    scorr: float | None = None
    magzpsci: float | None = None
    magzpsciunc: float | None = None
    magzpscirms: float | None = None
    clrcoeff: float | None = None
    clrcounc: float | None = None
    rbversion: str | None = None


class FPHist(BaseModel):
    field: int | None = None
    rcid: int | None = None
    fid: int
    pid: int
    rfid: int
    sciinpseeing: float | None = None
    scibckgnd: float | None = None
    scisigpix: float | None = None
    magzpsci: float | None = None
    magzpsciunc: float | None = None
    magzpscirms: float | None = None
    clrcoeff: float | None = None
    clrcounc: float | None = None
    exptime: float | None = None
    adpctdif1: float | None = None
    adpctdif2: float | None = None
    diffmaglim: float | None = None
    programid: int
    jd: float
    forcediffimflux: float | None = None
    forcediffimfluxunc: float | None = None
    procstatus: str | None = None
    distnr: float | None = None
    ranr: float
    decnr: float
    magnr: float | None = None
    sigmagnr: float | None = None
    chinr: float | None = None
    sharpnr: float | None = None


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
    diaObjectId: int
    model_config = ConfigDict(
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )


class AlertCutouts(AlertBase):
    cutoutScience: None | StampData = None
    cutoutTemplate: None | StampData = None
    cutoutDifference: None | StampData = None


class Alert_33(AlertCutouts):
    """
    avro alert schema for LSST (www.lsst.caltech.edu)
    """

    schemavsn: (
        Literal["1.9"]
        | Literal["2.0"]
        | Literal["3.0"]
        | Literal["3.1"]
        | Literal["3.2"]
        | Literal["3.3"]
    )
    publisher: str = "Ampel"
    candidate: Candidate
    prv_candidates: list[PrvCandidate] | None = None


class Alert_402(Alert_33):
    schemavsn: Literal["4.02"]  # type: ignore[assignment]
    fp_hists: list[FPHist] | None = None


Alert = Alert_33 | Alert_402


class AsyncResult(BaseModel):
    resume_token: str


class AlertChunk(BaseModel):
    resume_token: str
    chunk: int | None = None
    alerts: list[Alert]
    remaining: ChunkCount
    pending: ChunkCount
    model_config = ConfigDict(
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )


class AlertCount(BaseModel):
    count: int
