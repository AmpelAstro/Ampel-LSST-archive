from typing import Annotated, cast

from fastapi import Depends, HTTPException, status

from ..alert_packet import Alert as LSSTAlert
from .iceberg import Connection
from .metrics import REQ_TIME

REQ_TIME.labels("get_alert_from_iceberg").time()


def get_alert_from_iceberg(
    diaSourceId: int,
    connection: Connection,
) -> LSSTAlert:
    if (
        record := next(
            iter(
                connection.execute(
                    "select * from alerts where diaSourceId = ? limit 1;",
                    (diaSourceId,),
                )
                .fetch_arrow_table()
                .to_pylist()
            ),
            None,
        )
    ) is not None:
        return cast(LSSTAlert, record)
    raise HTTPException(
        status.HTTP_404_NOT_FOUND, detail={"msg": f"no alert with {diaSourceId=}"}
    )


AlertFromId = Annotated[LSSTAlert, Depends(get_alert_from_iceberg)]
