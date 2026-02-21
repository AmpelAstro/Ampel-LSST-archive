from contextlib import contextmanager
from pathlib import Path

import pytest

from ampel.lsst.archive.server.iceberg import (
    AlertQuery,
    get_cursor,
    get_duckdb,
    get_relation,
)
from ampel.lsst.archive.server.models import ConeConstraint, HEALpixRegionQuery


@pytest.fixture
def healpix_region_query():
    with open(Path(__file__).parent / "test-data" / "healpix_region_query.json") as f:
        return HEALpixRegionQuery.model_validate_json(f.read())


@pytest.fixture
def relation(_mock_iceberg):
    with contextmanager(get_cursor)(get_duckdb()) as cursor:
        yield get_relation(cursor)


def test_healpix_region_query(
    healpix_region_query: HEALpixRegionQuery,
    relation,
):
    query = AlertQuery(
        include=["diaSourceId"],
        condition=None,
        location=healpix_region_query,
    )

    result = query.flatten(relation)
    assert len(result) == 0


def test_cone_search(
    relation,
):
    rows = AlertQuery(
        include=["diaSource.ra", "diaSource.dec"],
        condition=None,
    ).flatten(relation)
    assert len(rows) > 0

    cone = ConeConstraint.model_validate({**rows[0], "radius": 1 / 3600})
    query = AlertQuery(
        include=["diaSourceId"],
        condition=None,
        location=cone,
    )

    result = query.flatten(relation)
    assert len(result) == len(rows)
