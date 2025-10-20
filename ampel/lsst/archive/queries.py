from typing import TYPE_CHECKING

from sqlalchemy import and_, func, or_

from .healpix_cone_search import ranges_for_cone
from .models import NSIDE, Alert

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import ColumnElement
    from sqlalchemy.orm.attributes import InstrumentedAttribute


def cone_search_condition(
    ra: float, dec: float, radius: float
) -> "Sequence[ColumnElement[bool]]":
    """
    Get a SQLAlchemy condition for healpix cone search

    :returns: SQLAlchemy condition
    """
    center = func.ll_to_earth(dec, ra)
    loc = func.ll_to_earth(Alert.dec, Alert.ra)
    pix: InstrumentedAttribute = Alert.hpx  # type: ignore[assignment]

    nside, ranges = ranges_for_cone(ra, dec, radius, max_nside=NSIDE)
    scale = (NSIDE // nside) ** 2
    return [
        or_(
            *[
                and_(pix >= int(left * scale), pix < int(right * scale))
                for left, right in ranges
            ]
        ),
        func.earth_distance(center, loc) < radius,
    ]


def time_range_condition(
    start_epoch: float, end_epoch: float
) -> "Sequence[ColumnElement[bool]]":
    """
    Get a SQLAlchemy condition for time range search

    :returns: SQLAlchemy condition
    """
    epoch: InstrumentedAttribute = Alert.midpointMjdTai  # type: ignore[assignment]

    return (
        epoch >= start_epoch,
        epoch < end_epoch,
    )
