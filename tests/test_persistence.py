import pytest
import pytest_asyncio
from fastapi import status

from ampel.lsst.archive.server.iceberg import AlertQuery, StreamQuery, table_name_token
from ampel.lsst.archive.server.models import StreamDescription


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


@pytest_asyncio.fixture
async def stream_token(integration_client, mocker, ensure_table_dirs):
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
    return response.json()["resume_token"]


@pytest.mark.asyncio
async def test_create_stream(integration_client, stream_token):
    response = await integration_client.get(f"/stream/{stream_token}")
    assert response.status_code == status.HTTP_200_OK
    desc = StreamDescription.model_validate(response.json())
    assert desc.items == 1
    assert desc.remaining == 1
    assert desc.pending == 0


@pytest.mark.asyncio
async def test_get_chunk(integration_client, stream_token):
    async def description():
        response = await integration_client.get(f"/stream/{stream_token}")
        assert response.status_code == status.HTTP_200_OK
        return StreamDescription.model_validate(response.json())

    # claim a chunk
    response = await integration_client.post(
        f"/stream/{stream_token}/fetch", follow_redirects=False
    )
    assert response.status_code == status.HTTP_303_SEE_OTHER
    location = response.headers["location"]
    assert location == f"/stream/{stream_token}/chunk/0"

    # chunk is claimed
    desc = await description()
    assert desc.pending == 1

    response = await integration_client.post(
        f"/stream/{stream_token}/fetch", follow_redirects=False
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, (
        "no more unclaimed chunks"
    )

    # get the chunk
    response = await integration_client.get(location)
    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert isinstance(payload, list)
    assert len(payload) == 1

    # release the chunk
    assert (
        await integration_client.post(f"/stream/{stream_token}/chunk/0/release")
    ).status_code == status.HTTP_204_NO_CONTENT

    desc = await description()
    assert desc.pending == 0, "chunk is no longer pending"

    await integration_client.post(
        f"/stream/{stream_token}/fetch", follow_redirects=False
    )

    assert (
        await integration_client.delete(f"/stream/{stream_token}/chunk/0")
    ).status_code == status.HTTP_204_NO_CONTENT

    desc = await description()
    assert desc.remaining == 0, "no chunks left"
    assert desc.pending == 0, "no chunks pending"
