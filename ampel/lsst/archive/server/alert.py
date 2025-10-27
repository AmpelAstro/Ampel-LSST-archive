import io
from typing import Annotated, cast

from fastapi import Depends, HTTPException, status
from prometheus_async.aio import time

from ..alert_packet import Alert as LSSTAlert
from ..avro import extract_record
from ..db import get_blobs_with_condition, get_schema
from ..models import Alert
from .db import (
    AsyncSession,
)
from .metrics import REQ_TIME
from .s3 import Bucket, get_range


@time(REQ_TIME.labels("get_alert_from_s3"))
async def get_alert_from_s3(
    diaSourceId: int,
    session: AsyncSession,
    bucket: Bucket,
) -> dict:
    async for uri, start, end, schema_id in get_blobs_with_condition(
        session,
        [Alert.id == diaSourceId],
    ):
        schema = await get_schema(session, schema_id)
        body = await get_range(bucket, uri, start, end)
        content = await time(REQ_TIME.labels("read_body"))(body.read)()
        record = extract_record(io.BytesIO(content), schema)
        return cast(LSSTAlert, record)
    raise HTTPException(
        status.HTTP_404_NOT_FOUND, detail={"msg": f"{diaSourceId=} not found"}
    )


AlertFromId = Annotated[LSSTAlert, Depends(get_alert_from_s3)]
