import io
from typing import Annotated

from fastapi import Depends, HTTPException, status

from ..avro import extract_record
from ..db import get_blobs_with_condition, get_schema
from ..models import Alert
from .db import (
    AsyncSession,
)
from .s3 import Bucket, get_range


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
        record = extract_record(io.BytesIO(await body.read()), schema)
        assert isinstance(record, dict)
        return record
    raise HTTPException(
        status.HTTP_404_NOT_FOUND, detail={"msg": f"{diaSourceId=} not found"}
    )


AlertFromId = Annotated[dict, Depends(get_alert_from_s3)]
