from collections.abc import Sequence
from typing import Annotated, cast

from duckdb import Expression
from fastapi import Depends, HTTPException, status

from ..alert_packet import Alert as LSSTAlert
from .iceberg import (
    AlertRelation,
    ColumnExpression,
    SQLExpression,
    StarExpression,
    flatten,
)
from .metrics import REQ_TIME

REQ_TIME.labels("get_alert_from_iceberg").time()


def _get_alert_from_iceberg(
    diaSourceId: int,
    alerts: AlertRelation,
    columns: Sequence[Expression] = (StarExpression(),),
) -> LSSTAlert:
    if (
        record := next(
            iter(
                flatten(
                    alerts.select(*columns)
                    .filter(SQLExpression(f"diaSourceId = {diaSourceId}"))
                    .limit(1)
                )
            )
        )
    ) is not None:
        return cast(LSSTAlert, record)
    raise HTTPException(
        status.HTTP_404_NOT_FOUND, detail={"msg": f"no alert with {diaSourceId=}"}
    )


def get_alert_from_iceberg(
    diaSourceId: int,
    alerts: AlertRelation,
) -> LSSTAlert:
    return _get_alert_from_iceberg(
        diaSourceId,
        alerts,
    )


def get_cutouts_from_iceberg(
    diaSourceId: int,
    alerts: AlertRelation,
) -> LSSTAlert:
    return _get_alert_from_iceberg(
        diaSourceId,
        alerts,
        columns=[
            ColumnExpression("diaSourceId"),
            ColumnExpression("cutoutScience"),
            ColumnExpression("cutoutTemplate"),
            ColumnExpression("cutoutDifference"),
        ],
    )


AlertFromId = Annotated[LSSTAlert, Depends(get_alert_from_iceberg)]

CutoutsFromId = Annotated[LSSTAlert, Depends(get_cutouts_from_iceberg)]
