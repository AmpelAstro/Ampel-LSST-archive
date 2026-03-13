import pytest
from fastapi import status

from ampel.lsst.archive.server.iceberg import AlertQuery, StreamQuery, table_name_token


def test_persistence(alert_relation, cursor, ensure_table_dirs):
    query = AlertQuery(
        include=["diaSourceId"],
        condition=None,
        limit=1,
    )

    table_name = "persistent_output"
    # XXX HACK: duckdb fileio doesn't create directory structure
    ensure_table_dirs("lsst", table_name)
    query.persist_to(alert_relation, table_name)

    persistent_relation = cursor.sql(f"from {table_name}")
    rows = AlertQuery(
        include=["diaSourceId"],
        condition=None,
        limit=1,
    ).flatten(persistent_relation)
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_create_stream(integration_client, mocker, ensure_table_dirs):
    name = table_name_token()
    mocker.patch("ampel.lsst.archive.server.app.table_name_token", return_value=name)

    query = StreamQuery(
        include=["diaSourceId"],
        condition=None,
        limit=1,
    )
    ensure_table_dirs("lsst", f"stream_{name}")

    response = await integration_client.post(
        "/streams/from_query",
        json=query.model_dump(),
    )
    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json().keys() == {"resume_token"}
