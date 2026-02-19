from fastapi import (
    FastAPI,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from zstd_asgi import ZstdMiddleware

from .alert import CutoutsFromId
from .display import router as display_router
from .iceberg import AlertQuery, AlertRelation, get_refs
from .models import AlertCutouts
from .settings import settings

# from .tokens import (
#     AuthToken,
#     verify_access_token,
#     verify_write_token,
# )
# from .tokens import (
#     router as token_router,
# )


DESCRIPTION = """
Query LSST alerts issued by IPAC

## Authorization

Some endpoints require an authorization token.
You can create a *LSST archive access token* using the "Archive tokens" tab on the [Ampel dashboard](https://ampel.zeuthen.desy.de/live/dashboard/tokens).
These tokens are persistent, and associated with your GitHub username.
"""

app = FastAPI(
    title="LSST Alert Archive Service",
    description=DESCRIPTION,
    version="3.1.0",
    root_path=settings.root_path,
    default_response_class=ORJSONResponse,
    openapi_tags=[
        {"name": "alerts", "description": "Retrieve alerts"},
        {
            "name": "photopoints",
            "description": "Retrieve de-duplicated detections and upper limits",
        },
        {"name": "cutouts", "description": "Retrieve image cutouts for alerts"},
        {"name": "search", "description": "Search for alerts"},
        {"name": "stream", "description": "Read a result set concurrently"},
        {
            "name": "topic",
            "description": "A topic is a persistent collection of alerts, specified by candidate id. This can be used e.g. to store a pre-selected sample of alerts for analysis.",
        },
        {
            "name": "tokens",
            "description": "Manage persistent authentication tokens",
            "externalDocs": {
                "description": "Authentication dashboard",
                "url": "https://ampel.zeuthen.desy.de/live/dashboard/tokens",
            },
        },
    ],
)

app.add_middleware(ZstdMiddleware, minimum_size=1000)
app.add_middleware(GZipMiddleware, minimum_size=1000)
if settings.allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(display_router, prefix="/display")


@app.get(
    "/alert/{diaSourceId}/cutouts",
)
def get_alert_cutouts(cutouts: CutoutsFromId) -> AlertCutouts:
    """
    Get image cutouts for the given alert.
    """
    return AlertCutouts.model_validate(cutouts)


app.get("/refs")(get_refs)

'''
@app.get(
    "/object/{objectId}/alerts",
    tags=["alerts"],
    response_model=list[Alert],
    response_model_exclude_none=True,
)
def get_alerts_for_object(
    objectId: str = Path(..., description="LSST object name"),
    jd_start: Optional[float] = Query(
        None, description="minimum Julian Date of observation"
    ),
    jd_end: Optional[float] = Query(
        None, description="maximum Julian Date of observation"
    ),
    with_history: bool = Query(
        False, description="Include previous detections and upper limits"
    ),
    limit: Optional[int] = Query(
        None,
        description="Maximum number of alerts to return",
    ),
    start: int = Query(0, description="Return alerts starting at index"),
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: AuthToken = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
):
    """
    Get all alerts for the given object.
    """
    chunk_id, alerts = archive.get_alerts_for_object(
        objectId,
        jd_start=jd_start,
        jd_end=jd_end,
        programid=programid,
        with_history=with_history,
        limit=limit,
        start=start,
    )
    return alerts


@app.get(
    "/object/{objectId}/photopoints",
    tags=["photopoints"],
    response_model=Alert,  # type: ignore[arg-type]
    response_model_exclude_none=True,
)
def get_photopoints_for_object(
    objectId: str = Path(..., description="LSST object name"),
    jd_start: Optional[float] = Query(
        None, description="minimum Julian Date of observation"
    ),
    jd_end: Optional[float] = Query(
        None, description="maximum Julian Date of observation"
    ),
    upper_limits: bool = Query(True, description="include upper limits"),
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: AuthToken = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
):
    """
    Get all detections and upper limits for the given object, consolidated into
    a single alert packet.
    """
    return archive.get_photopoints_for_object(
        objectId,
        programid=programid,
        jd_start=jd_start,
        jd_end=jd_end,
        include_upper_limits=upper_limits,
    )


@app.get(
    "/alerts/time_range",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_time_range(
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    with_history: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: bool = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk, alerts = archive.get_alerts_in_time_range(
        jd_start=jd_start,
        jd_end=jd_end,
        programid=programid,
        with_history=with_history,
        group_name=resume_token,
        block_size=chunk_size,
        max_blocks=1,
    )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )
'''

"""
async def group_from_query(
    session: AsyncSession,
    bucket: Bucket,
    tasks: BackgroundTasks,
    conditions: "Sequence[ColumnElement[bool]]",
) -> ResultGroup:
    resume_token = secrets.token_urlsafe(32)

    group = ResultGroup(name=resume_token, chunk_size=1000)
    session.add(group)
    await session.flush()

    tasks.add_task(populate_chunks, bucket, group, conditions)

    return group


@app.get(
    "/alerts/cone_search",
    tags=["search"],
    response_class=RedirectResponse,
    status_code=status.HTTP_303_SEE_OTHER,
)
async def get_alerts_in_cone(
    ra: Annotated[
        float, Query(description="Right ascension of field center in degrees (J2000)")
    ],
    dec: Annotated[
        float, Query(description="Declination of field center in degrees (J2000)")
    ],
    radius: Annotated[float, Query(description="radius of search field in degrees")],
    start: Annotated[AstropyTime, Query(description="Start time for the search")],
    end: Annotated[AstropyTime, Query(description="End time for the search")],
    session: AsyncSession,
    bucket: Bucket,
    tasks: BackgroundTasks,
) -> str:
    group = await group_from_query(
        session,
        bucket,
        tasks,
        [
            *cone_search_condition(ra=ra, dec=dec, radius=radius),
            *time_range_condition(start.mjd_tai(), end.mjd_tai()),
        ],
    )

    return f"{settings.root_path}/stream/{group.name}"
"""

'''
@app.get("/alerts/sample")
def get_random_alerts(
    count: int = Query(1, ge=1, le=10_000),
    with_history: bool = Query(False),
    engine: sqlalchemy.Engine = Depends(get_engine),
):
    """
    Get a sample of random alerts to test random-access throughput
    """
    alerts, dt = archive.get_random_alerts(count=count, with_history=with_history)
    return {
        "dt": dt,
        "alerts": len(alerts),
        "prv_candidates": sum(len(alert["prv_candidates"]) for alert in alerts),
    }


@app.get(
    "/objects/cone_search",
    tags=["search"],
)
def get_objects_in_cone(
    ra: float = Query(
        ..., description="Right ascension of field center in degrees (J2000)"
    ),
    dec: float = Query(
        ..., description="Declination of field center in degrees (J2000)"
    ),
    radius: float = Query(..., description="radius of search field in degrees"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: bool = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
) -> list[str]:
    return list(
        archive.get_objects_in_cone(
            ra=ra,
            dec=dec,
            radius=radius,
            jd_start=jd_start,
            jd_end=jd_end,
            programid=programid,
        )
    )


@app.get(
    "/alerts/healpix",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_healpix_pixel(
    nside: Literal[
        "1",
        "2",
        "4",
        "8",
        "16",
        "32",
        "64",
        "128",
        "256",
        "512",
        "1024",
        "2048",
        "4096",
        "8192",
    ] = Query("64", description="NSide of (nested) HEALpix grid"),
    ipix: list[int] = Query(..., description="Pixel index"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    latest: bool = Query(
        False, description="Return only the latest alert for each objectId"
    ),
    with_history: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: bool = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk, alerts = archive.get_alerts_in_healpix(
        pixels={int(nside): ipix},
        jd_start=jd_start,
        jd_end=jd_end,
        latest=latest,
        programid=programid,
        with_history=with_history,
        group_name=resume_token,
        block_size=chunk_size,
        max_blocks=1,
    )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )


@app.post(
    "/alerts/healpix/skymap",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_healpix_map(
    query: Union[HEALpixMapQuery, HEALpixRegionQuery],
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: bool = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    resume_token = query.resume_token or secrets.token_urlsafe(32)
    if query.resume_token:
        chunk, alerts = archive.get_chunk_from_queue(
            query.resume_token, with_history=query.with_history
        )
    else:
        if isinstance(query, HEALpixRegionQuery):
            regions = {region.nside: region.pixels for region in query.regions}
        else:
            regions = deres(query.nside, query.pixels)
        chunk, alerts = archive.get_alerts_in_healpix(
            pixels=regions,
            jd_start=query.jd.gt,
            jd_end=query.jd.lt,
            latest=query.latest,
            programid=programid,
            candidate_filter=query.candidate,
            with_history=query.with_history,
            group_name=resume_token,
            block_size=query.chunk_size,
            max_blocks=1,
        )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )


@app.post(
    "/alerts/healpix/skymap/count",
    tags=["search"],
    response_model=AlertCount,
    response_model_exclude_none=True,
)
def count_alerts_in_healpix_map(
    query: HEALpixRegionCountQuery,
    engine: sqlalchemy.Engine = Depends(get_engine),
    # auth: bool = Depends(verify_access_token),
    # programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertCount:
    return AlertCount(
        count=archive.count_alerts_in_healpix(
            pixels={region.nside: region.pixels for region in query.regions},
            jd_start=query.jd.gt,
            jd_end=query.jd.lt,
            programid=programid,
            candidate_filter=query.candidate,
        )
    )
'''

'''
@app.post("/topics/", tags=["topic"], status_code=201)
def create_topic(
    topic: Topic,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
):
    """
    Create a new persistent collection of alerts
    """
    name = secrets.token_urlsafe()
    try:
        archive.create_topic(name, topic.candids, topic.description)
    except sqlalchemy.exc.IntegrityError:
        raise HTTPException(
            status_code=400,
            detail={
                "msg": "Topic did not match any alerts. Are you sure these are valid candidate ids?",
                "topic": jsonable_encoder(topic),
            },
        ) from None
    return name


@app.get("/topic/{topic}", tags=["topic"], response_model=TopicDescription)
def get_topic(
    topic: str,
    archive: ArchiveDB = Depends(get_archive),
):
    return {"topic": topic, **archive.get_topic_info(topic)}


@app.post(
    "/streams/from_topic",
    tags=["topic", "stream"],
    response_model=StreamDescription,
    status_code=201,
)
def create_stream_from_topic(
    query: TopicQuery,
    archive: ArchiveDB = Depends(get_archive),
):
    """
    Create a stream of alerts from the given persistent topic.  The resulting
    resume_token can be used to read the stream concurrently from multiple clients.
    """
    resume_token = secrets.token_urlsafe()
    try:
        archive.create_read_queue_from_topic(
            query.topic,
            resume_token,
            query.chunk_size,
            slice(query.start, query.stop, query.step),
        )
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Topic not found") from None
    stream_info = get_stream_info(resume_token, archive)
    return {"resume_token": resume_token, **stream_info}


@app.post(
    "/streams/from_query",
    tags=["search", "stream"],
    response_model=Stream,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_stream_from_query(
    tasks: BackgroundTasks,
    query: Union[AlertQuery, ObjectQuery, HEALpixRegionQuery] = Body(
        ...,
        examples={  # type: ignore[arg-type]
            "cone": {
                "summary": "Cone search",
                "value": {
                    "cone": {"ra": 158.068431, "dec": 47.0497302, "radius": 3.5},
                },
            },
            "object": {
                "summary": "LSST-ID search",
                "description": "Retrieve alerts for all ObjectIds provided",
                "value": {
                    "objectId": ["ZTF19aapreis", "ZTF19aatubsj"],
                    "jd": {"$gt": 2458550.5, "$lt": 2459550.5},
                },
            },
            "healpix": {
                "summary": "HEALpix search",
                "description": "search regions of a HEALpix map (in narrow time range)",
                "value": {
                    "regions": [{"nside": 64, "pixels": [5924, 5925, 5926, 5927]}],
                    "jd": {"$gt": 2459308.72, "$lt": 2459308.73},
                },
            },
            "filtered": {
                "summary": "Epoch search",
                "description": "Search for all candidates in an epoch range that fulfill criteria",
                "value": {
                    "jd": {"$gt": 2459550.5, "$lt": 2459551.5},
                    "candidate": {
                        "drb": {"$gt": 0.999},
                        "magpsf": {"$lt": 15},
                        "ndethist": {"$gt": 0, "$lte": 10},
                        "fid": 1,
                    },
                },
            },
        },
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
):
    """
    Create a stream of alerts from the given query. The resulting resume_token
    can be used to read the stream concurrently from multiple clients.
    """
    try:
        if isinstance(query, AlertQuery):
            if query.cone:
                condition, order = archive._cone_search_condition(  # noqa: SLF001
                    ra=query.cone.ra,
                    dec=query.cone.dec,
                    radius=query.cone.radius,
                    programid=programid,
                    jd_min=query.jd.gt,
                    jd_max=query.jd.lt,
                    candidate_filter=query.candidate,
                )
            else:
                condition, order = archive._time_range_condition(  # noqa: SLF001
                    programid=programid,
                    jd_start=query.jd.gt,
                    jd_end=query.jd.lt,
                    candidate_filter=query.candidate,
                )
        elif isinstance(query, ObjectQuery):
            condition, order = archive._object_search_condition(  # noqa: SLF001
                objectId=query.objectId,
                programid=programid,
                jd_start=query.jd.gt,
                jd_end=query.jd.lt,
                candidate_filter=query.candidate,
            )
        else:
            pixels: dict[int, list[int]] = {}
            for region in query.regions:
                pixels[region.nside] = pixels.get(region.nside, []) + region.pixels
            condition, order = archive._healpix_search_condition(  # noqa: SLF001
                pixels=pixels,
                jd_min=query.jd.gt,
                jd_max=query.jd.lt,
                latest=query.latest,
                programid=programid,
                candidate_filter=query.candidate,
            )
    except NoSuchColumnError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"msg": f"unknown candidate field {exc.args[0]}"},
        ) from None

    name = secrets.token_urlsafe(32)
    try:
        conn = archive._engine.connect()  # noqa: SLF001
    except sqlalchemy.exc.TimeoutError as exc:
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"msg": str(exc)}
        ) from None

    conn.execute(
        sqlalchemy.text(f"set statement_timeout={settings.stream_query_timeout*1000};")
    )
    group_id = archive._create_read_queue(conn, name, query.chunk_size)  # noqa: SLF001
    conn.commit()

    # create stream in the background
    def create_stream() -> None:
        try:
            archive._fill_read_queue(conn, condition, order, group_id, query.chunk_size)  # noqa: SLF001
            conn.commit()
        finally:
            conn.close()

    tasks.add_task(create_stream)

    return {"resume_token": name, "chunk_size": query.chunk_size}
'''

# Collect metrics for all endpoints except /metrics
instrumentator = Instrumentator(
    excluded_handlers=["/metrics"],
).instrument(app)

# If we are mounted under a (non-stripped) prefix path, create a potemkin root
# router and mount the actual root as a sub-application. This has no effect
# other than to prefix the paths of all routes with the root path.
if settings.root_path:
    wrapper = FastAPI(debug=True)
    wrapper.mount(settings.root_path, app)
    app = wrapper

# Expose metrics at the root
instrumentator.expose(app)


@app.get("/health", tags=["health"])
def health_check(alerts: AlertRelation):
    assert (
        len(
            AlertQuery(
                include=["diaSourceId"], condition="diaSourceId > 0", limit=1
            ).flatten(alerts)
        )
        == 1
    )
    return {"status": "healthy"}
