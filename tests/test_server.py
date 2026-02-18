import httpx
import pytest
from fastapi import status

from ampel.lsst.archive.server.app import app


@pytest.fixture
def integration_client(_mock_iceberg):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    )


@pytest.mark.asyncio
async def test_display(integration_client):
    response = await integration_client.post(
        "/display/alerts/query",
        json={"condition": "true", "include": ["diaSourceId"]},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 98
